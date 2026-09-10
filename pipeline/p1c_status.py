"""Immutable, accepted-only P1C Reup status publication."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import uuid
from typing import Mapping

from pipeline.dubvi_engine_contract import (
    CONTRACT_VERSION,
    canonical_json_bytes,
    parse_json_document,
    validate_engine_status_event,
)


class ReupStatusError(RuntimeError):
    """Raised when accepted evidence cannot be safely published or reused."""


MAX_REUP_STATUS_EVENT_BYTES = 64 * 1024


def publish_accepted_status(
    status_root: Path,
    job: Mapping[str, object],
    *,
    occurred_at_utc: str | None = None,
) -> Path:
    """Publish or validate Reup sequence-one ``accepted`` evidence only.

    The routine never starts a worker, acquires a lease, or writes any status
    beyond the one immutable acceptance event.
    """

    attempt_number, parent_job_id = _runtime_lineage(job)
    job_id = str(job.get("reup_job_id", ""))
    dispatch_id = str(job.get("dispatch_id", ""))
    if not job_id or not dispatch_id:
        raise ReupStatusError("accepted status requires a validated job identity")
    event: dict[str, object] = {
        "contract_version": CONTRACT_VERSION,
        "message_kind": "engine_status_event",
        "event_id": str(uuid.uuid4()),
        "engine_kind": "reup",
        "engine_job_id": job_id,
        "dispatch_id": dispatch_id,
        "correlation_id": dispatch_id,
        "sequence": 1,
        "attempt_number": attempt_number,
        "event_kind": "accepted",
        "state": "accepted",
        "occurred_at_utc": occurred_at_utc or _now_utc_millis(),
    }
    if parent_job_id is not None:
        event["parent_engine_job_id"] = parent_job_id
    document = validate_engine_status_event(event)
    payload = canonical_json_bytes(document)
    root = _require_status_root(status_root)
    directory = _require_status_subdirectory(root, "reup", job_id)
    final = directory / "event-000001.json"
    _assert_under(root, final)
    if final.exists() or final.is_symlink():
        _validate_existing(final, job)
        return final
    directory.mkdir(parents=True, exist_ok=True)
    stage = directory / f".event-000001.{document['event_id']}.part"
    _stage_exact(stage, payload)
    try:
        os.link(stage, final)
    except FileExistsError:
        if _matches(final, payload):
            _safe_remove_owned_stage(stage)
            return final
        _safe_remove_owned_stage(stage)
        raise ReupStatusError("ENGINE_STATUS_CONFLICT: racing accepted status differs")
    except OSError as error:
        raise ReupStatusError(f"non-replacing accepted status publication failed: {error}") from error
    if not _matches(final, payload):
        raise ReupStatusError("published accepted status failed verification")
    _safe_remove_owned_stage(stage)
    return final


def _validate_existing(path: Path, job: Mapping[str, object]) -> None:
    if path.is_symlink() or not path.is_file():
        raise ReupStatusError("existing accepted status is not a regular file")
    size = path.stat().st_size
    if size <= 0 or size > MAX_REUP_STATUS_EVENT_BYTES:
        raise ReupStatusError("existing accepted status has an unsafe size")
    try:
        payload = path.read_bytes()
        if len(payload) != size:
            raise ReupStatusError("existing accepted status changed while read")
        document = validate_engine_status_event(parse_json_document(payload))
    except Exception as error:
        raise ReupStatusError("existing accepted status is invalid") from error
    expected = {
        "engine_kind": "reup",
        "engine_job_id": job["reup_job_id"],
        "dispatch_id": job["dispatch_id"],
        "correlation_id": job["dispatch_id"],
        "attempt_number": job["attempt_number"],
        "event_kind": "accepted",
        "state": "accepted",
        "sequence": 1,
    }
    if any(document.get(field) != value for field, value in expected.items()):
        raise ReupStatusError("existing accepted status conflicts with canonical job identity")
    if document.get("parent_engine_job_id") != job.get("parent_reup_job_id"):
        raise ReupStatusError("existing accepted status conflicts with retry lineage")
    if payload != canonical_json_bytes(document):
        raise ReupStatusError("existing accepted status is not canonical bytes")


def _stage_exact(path: Path, payload: bytes) -> None:
    if path.exists() or path.is_symlink():
        if _matches(path, payload):
            return
        raise ReupStatusError("existing accepted status stage differs")
    try:
        with path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError:
        if _matches(path, payload):
            return
        raise ReupStatusError("racing accepted status stage differs")
    if not _matches(path, payload):
        raise ReupStatusError("accepted status stage failed verification")


def _safe_remove_owned_stage(path: Path) -> None:
    """Best-effort removal of only this invocation's exact hidden stage."""

    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def _matches(path: Path, expected: bytes) -> bool:
    if path.is_symlink() or not path.is_file() or path.stat().st_size != len(expected):
        return False
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.digest() == hashlib.sha256(expected).digest()


def _now_utc_millis() -> str:
    value = datetime.now(timezone.utc)
    return value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{value.microsecond // 1000:03d}Z"


def _runtime_lineage(job: Mapping[str, object]) -> tuple[int, str | None]:
    if "attempt_number" not in job:
        raise ReupStatusError("canonical Reup job requires attempt_number")
    attempt = job["attempt_number"]
    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
        raise ReupStatusError("canonical Reup attempt_number must be positive")
    parent = job.get("parent_reup_job_id")
    if attempt == 1:
        if parent is not None:
            raise ReupStatusError("first canonical Reup job must not have a parent")
        return attempt, None
    if not isinstance(parent, str) or not parent:
        raise ReupStatusError("retry canonical Reup job requires parent_reup_job_id")
    return attempt, parent


def _require_status_root(status_root: Path) -> Path:
    if status_root.exists() or status_root.is_symlink():
        if status_root.is_symlink() or not status_root.is_dir():
            raise ReupStatusError("status_root must be a real non-symlink directory")
    else:
        try:
            status_root.mkdir(parents=True, exist_ok=False)
        except OSError as error:
            raise ReupStatusError(f"cannot create status_root: {error}") from error
        if status_root.is_symlink() or not status_root.is_dir():
            raise ReupStatusError("created status_root is not a real directory")
    return status_root.resolve()


def _require_status_subdirectory(root: Path, *parts: str) -> Path:
    directory = root.joinpath(*parts)
    _assert_under(root, directory)
    if directory.exists() or directory.is_symlink():
        if directory.is_symlink() or not directory.is_dir():
            raise ReupStatusError("status directory must be a real contained directory")
    else:
        try:
            directory.mkdir(parents=True, exist_ok=False)
        except OSError as error:
            raise ReupStatusError(f"cannot create status directory: {error}") from error
        if directory.is_symlink() or not directory.is_dir():
            raise ReupStatusError("created status directory is not real")
    _assert_under(root, directory)
    return directory


def _assert_under(root: Path, path: Path) -> None:
    try:
        path.resolve(strict=False).relative_to(root.resolve(strict=False))
    except ValueError as error:
        raise ReupStatusError("status path escapes configured status root") from error
