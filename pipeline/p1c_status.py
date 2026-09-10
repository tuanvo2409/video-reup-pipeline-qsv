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

    job_id = str(job["reup_job_id"])
    dispatch_id = str(job["dispatch_id"])
    event: dict[str, object] = {
        "contract_version": CONTRACT_VERSION,
        "message_kind": "engine_status_event",
        "event_id": str(uuid.uuid4()),
        "engine_kind": "reup",
        "engine_job_id": job_id,
        "dispatch_id": dispatch_id,
        "correlation_id": dispatch_id,
        "sequence": 1,
        "attempt_number": job["attempt_number"],
        "event_kind": "accepted",
        "state": "accepted",
        "occurred_at_utc": occurred_at_utc or _now_utc_millis(),
    }
    if "parent_reup_job_id" in job:
        event["parent_engine_job_id"] = job["parent_reup_job_id"]
    document = validate_engine_status_event(event)
    payload = canonical_json_bytes(document)
    directory = status_root / "reup" / job_id
    final = directory / "event-000001.json"
    _assert_under(status_root, final)
    if final.exists() or final.is_symlink():
        _validate_existing(final, job)
        return final
    directory.mkdir(parents=True, exist_ok=True)
    stage = directory / f".event-000001.{document['event_id']}.part"
    _stage_exact(stage, payload)
    try:
        os.link(stage, final)
    except FileExistsError:
        _validate_existing(final, job)
        return final
    except OSError as error:
        raise ReupStatusError(f"non-replacing accepted status publication failed: {error}") from error
    if not _matches(final, payload):
        raise ReupStatusError("published accepted status failed verification")
    stage.unlink(missing_ok=True)
    return final


def _validate_existing(path: Path, job: Mapping[str, object]) -> None:
    if path.is_symlink() or not path.is_file():
        raise ReupStatusError("existing accepted status is not a regular file")
    try:
        document = validate_engine_status_event(parse_json_document(path.read_bytes()))
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


def _assert_under(root: Path, path: Path) -> None:
    try:
        path.resolve(strict=False).relative_to(root.resolve(strict=False))
    except ValueError as error:
        raise ReupStatusError("status path escapes configured status root") from error
