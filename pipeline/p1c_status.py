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


def inspect_existing_accepted_status(status_root: Path, job: Mapping[str, object]) -> Path | None:
    """Return only valid, pre-existing sequence-one acceptance evidence.

    This inspection is deliberately side-effect free so queue-style intake can
    advance past completed immutable envelopes without creating a status root,
    status directory, stage, or event.  A present but invalid record is an
    authority conflict, never a reason to skip ahead.
    """

    _runtime_lineage(job)
    job_id = str(job.get("reup_job_id", ""))
    dispatch_id = str(job.get("dispatch_id", ""))
    if not job_id or not dispatch_id:
        raise ReupStatusError("accepted status requires a validated job identity")
    if not status_root.exists() and not status_root.is_symlink():
        return None
    if status_root.is_symlink() or not status_root.is_dir():
        raise ReupStatusError("status_root must be a real non-symlink directory")
    root = status_root.resolve()
    final = root / "reup" / job_id / "event-000001.json"
    _assert_under(root, final)
    if not final.exists() and not final.is_symlink():
        return None
    _validate_existing(final, job)
    return final


def load_status_event(
    status_root: Path,
    job: Mapping[str, object],
    sequence: int,
) -> dict[str, object]:
    """Load one exact immutable Reup status event without side effects."""

    attempt_number, parent_job_id = _runtime_lineage(job)
    job_id = str(job.get("reup_job_id", ""))
    dispatch_id = str(job.get("dispatch_id", ""))
    if not job_id or not dispatch_id:
        raise ReupStatusError("status event requires a validated job identity")
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1:
        raise ReupStatusError("status sequence must be positive")
    if not status_root.exists() or status_root.is_symlink() or not status_root.is_dir():
        raise ReupStatusError("status_root must be an existing real directory")
    root = status_root.resolve()
    path = root / "reup" / job_id / f"event-{sequence:06d}.json"
    _assert_under(root, path)
    if path.is_symlink() or not path.is_file():
        raise ReupStatusError("requested Reup status event is absent or unsafe")
    size = path.stat().st_size
    if size <= 0 or size > MAX_REUP_STATUS_EVENT_BYTES:
        raise ReupStatusError("Reup status event has an unsafe size")
    payload = path.read_bytes()
    if len(payload) != size:
        raise ReupStatusError("Reup status event changed while read")
    try:
        document = validate_engine_status_event(parse_json_document(payload))
    except Exception as error:
        raise ReupStatusError("Reup status event is invalid") from error
    expected = {
        "engine_kind": "reup",
        "engine_job_id": job_id,
        "dispatch_id": dispatch_id,
        "correlation_id": dispatch_id,
        "attempt_number": attempt_number,
        "sequence": sequence,
    }
    if any(document.get(field) != value for field, value in expected.items()):
        raise ReupStatusError("Reup status event conflicts with canonical job identity")
    if document.get("parent_engine_job_id") != parent_job_id:
        raise ReupStatusError("Reup status event conflicts with retry lineage")
    if payload != canonical_json_bytes(document):
        raise ReupStatusError("Reup status event is not canonical bytes")
    expected_pairs = {
        1: {("accepted", "accepted")},
        2: {("started", "running")},
        3: {
            ("output_published", "running"),
            ("failed", "failed"),
            ("lease_lost", "running"),
        },
        4: {("handoff_published", "running")},
        5: {("succeeded", "succeeded")},
    }
    if sequence not in expected_pairs or (document["event_kind"], document["state"]) not in expected_pairs[sequence]:
        raise ReupStatusError("Reup status sequence has an illegal event kind")
    return document


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

    return _publish_status(status_root, job, sequence=1, event_kind="accepted", state="accepted", occurred_at_utc=occurred_at_utc)


def publish_started_status(status_root: Path, job: Mapping[str, object], lease_id: str, *, occurred_at_utc: str | None = None) -> Path:
    return _publish_status(status_root, job, sequence=2, event_kind="started", state="running", lease_id=lease_id, occurred_at_utc=occurred_at_utc)


def publish_lease_lost_status(status_root: Path, job: Mapping[str, object], lease_id: str, *, occurred_at_utc: str | None = None) -> Path:
    return _publish_status(status_root, job, sequence=3, event_kind="lease_lost", state="running", lease_id=lease_id, occurred_at_utc=occurred_at_utc)


def publish_output_published_status(status_root: Path, job: Mapping[str, object], output_fingerprint: str, *, occurred_at_utc: str | None = None) -> Path:
    return _publish_status(status_root, job, sequence=3, event_kind="output_published", state="running", output_fingerprint=output_fingerprint, occurred_at_utc=occurred_at_utc)


def publish_failed_status(status_root: Path, job: Mapping[str, object], error_classification: str, diagnostic_summary: str, *, occurred_at_utc: str | None = None) -> Path:
    return _publish_status(status_root, job, sequence=3, event_kind="failed", state="failed", error_classification=error_classification, diagnostic_summary=diagnostic_summary[:2048], occurred_at_utc=occurred_at_utc)


def publish_handoff_published_status(status_root: Path, job: Mapping[str, object], output_fingerprint: str, handoff_id: str, handoff_ref: str, *, occurred_at_utc: str | None = None) -> Path:
    return _publish_status(status_root, job, sequence=4, event_kind="handoff_published", state="running", output_fingerprint=output_fingerprint, handoff_id=handoff_id, handoff_ref=handoff_ref, occurred_at_utc=occurred_at_utc)


def publish_succeeded_status(status_root: Path, job: Mapping[str, object], output_fingerprint: str, handoff_id: str, handoff_ref: str, *, occurred_at_utc: str | None = None) -> Path:
    return _publish_status(status_root, job, sequence=5, event_kind="succeeded", state="succeeded", output_fingerprint=output_fingerprint, handoff_id=handoff_id, handoff_ref=handoff_ref, occurred_at_utc=occurred_at_utc)


def _publish_status(status_root: Path, job: Mapping[str, object], *, sequence: int, event_kind: str, state: str, lease_id: str | None = None, output_fingerprint: str | None = None, handoff_id: str | None = None, handoff_ref: str | None = None, error_classification: str | None = None, diagnostic_summary: str | None = None, occurred_at_utc: str | None = None) -> Path:
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
        "sequence": sequence,
        "attempt_number": attempt_number,
        "event_kind": event_kind,
        "state": state,
        "occurred_at_utc": occurred_at_utc or _now_utc_millis(),
    }
    if parent_job_id is not None:
        event["parent_engine_job_id"] = parent_job_id
    if lease_id is not None:
        event["lease_id"] = lease_id
    for field, value in (("output_fingerprint", output_fingerprint), ("handoff_id", handoff_id), ("handoff_ref", handoff_ref), ("error_classification", error_classification), ("diagnostic_summary", diagnostic_summary)):
        if value is not None:
            event[field] = value
    document = validate_engine_status_event(event)
    payload = canonical_json_bytes(document)
    root = _require_status_root(status_root)
    directory = _require_status_subdirectory(root, "reup", job_id)
    if sequence == 2:
        previous = directory / "event-000001.json"
        if not previous.exists() and not previous.is_symlink():
            raise ReupStatusError("started status requires valid accepted status")
        _validate_existing(previous, job)
    elif sequence == 3:
        previous = directory / "event-000002.json"
        if not previous.exists() and not previous.is_symlink():
            raise ReupStatusError("lease_lost status requires valid started status")
        _validate_existing(previous, job, sequence=2, event_kind="started", state="running", lease_id=lease_id)
    elif sequence == 4:
        previous = directory / "event-000003.json"
        if not previous.exists() and not previous.is_symlink():
            raise ReupStatusError("handoff_published requires sequence-3 output evidence")
        _validate_existing(previous, job, sequence=3, event_kind="output_published", state="running", output_fingerprint=output_fingerprint)
    elif sequence == 5:
        previous = directory / "event-000004.json"
        if not previous.exists() and not previous.is_symlink():
            raise ReupStatusError("succeeded requires sequence-4 handoff evidence")
        _validate_existing(previous, job, sequence=4, event_kind="handoff_published", state="running", output_fingerprint=output_fingerprint, handoff_id=handoff_id, handoff_ref=handoff_ref)
    final = directory / f"event-{sequence:06d}.json"
    _assert_under(root, final)
    if final.exists() or final.is_symlink():
        _validate_existing(final, job, sequence=sequence, event_kind=event_kind, state=state, lease_id=lease_id, output_fingerprint=output_fingerprint, handoff_id=handoff_id, handoff_ref=handoff_ref)
        return final
    directory.mkdir(parents=True, exist_ok=True)
    stage = directory / f".event-{sequence:06d}.{document['event_id']}.part"
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


def _validate_existing(path: Path, job: Mapping[str, object], *, sequence: int = 1, event_kind: str = "accepted", state: str = "accepted", lease_id: str | None = None, output_fingerprint: str | None = None, handoff_id: str | None = None, handoff_ref: str | None = None) -> None:
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
        "event_kind": event_kind,
        "state": state,
        "sequence": sequence,
    }
    if any(document.get(field) != value for field, value in expected.items()):
        raise ReupStatusError("existing accepted status conflicts with canonical job identity")
    if document.get("parent_engine_job_id") != job.get("parent_reup_job_id"):
        raise ReupStatusError("existing accepted status conflicts with retry lineage")
    if lease_id is not None and document.get("lease_id") != lease_id:
        raise ReupStatusError("existing status conflicts with lease identity")
    for field, value in (("output_fingerprint", output_fingerprint), ("handoff_id", handoff_id), ("handoff_ref", handoff_ref)):
        if value is not None and document.get(field) != value:
            raise ReupStatusError(f"existing status conflicts with {field}")
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
