"""Side-effect-free P1C Reup envelope validation and accepted acknowledgement."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from pipeline.dubvi_engine_contract import (
    ContractValidationError,
    parse_json_document,
    validate_control_plane_to_reup_job,
)
from pipeline.p1c_status import (
    ReupStatusError,
    inspect_existing_accepted_status,
    publish_accepted_status,
)


class ReupIntakeError(RuntimeError):
    """Raised when canonical Reup intake cannot fail closed."""


MAX_REUP_JOB_ENVELOPE_BYTES = 64 * 1024
_TARGET_PLATFORM_RE = re.compile(r"[a-z0-9]+(?:[a-z0-9-]*[a-z0-9])?")


@dataclass(frozen=True)
class AcceptedReupJob:
    """One verified envelope whose accepted status is durably visible."""

    envelope_path: Path
    media_path: Path
    status_path: Path
    document: Mapping[str, object]


def iter_reup_envelopes(input_root: Path) -> tuple[Path, ...]:
    """Return only final canonical envelopes in deterministic lexical order."""

    root = _require_input_root(input_root)
    paths = [
        path for path in root.rglob("dubvi-reup-job-*.job.json")
        if path.is_file() and not path.is_symlink() and path.name.startswith("dubvi-reup-job-")
    ]
    return tuple(sorted(paths, key=lambda path: path.relative_to(root).as_posix()))


def accept_next_reup_job(
    input_root: Path,
    status_root: Path,
    *,
    occurred_at_utc: str | None = None,
) -> AcceptedReupJob | None:
    """Accept at most one pending canonical envelope in deterministic order."""

    root = _require_input_root(input_root)
    for envelope in iter_reup_envelopes(root):
        document = load_reup_envelope(envelope, root)
        try:
            existing = inspect_existing_accepted_status(status_root, document)
        except ReupStatusError as error:
            raise ReupIntakeError(f"canonical accepted status failed: {error}") from error
        if existing is not None:
            continue
        return accept_reup_envelope(envelope, root, status_root, occurred_at_utc=occurred_at_utc)
    return None


def accept_reup_envelope(
    envelope_path: Path,
    input_root: Path,
    status_root: Path,
    *,
    occurred_at_utc: str | None = None,
) -> AcceptedReupJob:
    """Validate an immutable CP envelope and publish only its accepted ACK."""

    root = _require_input_root(input_root)
    document = load_reup_envelope(envelope_path, root)
    media_path = envelope_path.parent / str(document["media_name"])
    _validate_media(media_path, envelope_path.parent, document)
    try:
        status_path = publish_accepted_status(status_root, document, occurred_at_utc=occurred_at_utc)
    except ReupStatusError as error:
        raise ReupIntakeError(f"canonical accepted status failed: {error}") from error
    return AcceptedReupJob(envelope_path, media_path, status_path, document)


def load_reup_envelope(envelope_path: Path, input_root: Path) -> dict[str, object]:
    """Parse and validate one final canonical Reup envelope without side effects."""

    root = _require_input_root(input_root)
    _assert_under(root, envelope_path)
    if envelope_path.is_symlink() or not envelope_path.is_file():
        raise ReupIntakeError("canonical Reup envelope must be a regular final file")
    size = envelope_path.stat().st_size
    if size <= 0 or size > MAX_REUP_JOB_ENVELOPE_BYTES:
        raise ReupIntakeError("canonical Reup envelope has an unsafe size")
    try:
        payload = envelope_path.read_bytes()
        if len(payload) != size:
            raise ReupIntakeError("canonical Reup envelope changed while read")
        document = validate_control_plane_to_reup_job(parse_json_document(payload))
    except ContractValidationError as error:
        raise ReupIntakeError(f"invalid Control Plane Reup envelope: {error}") from error
    _validate_runtime_lineage(document)
    profile = _safe_profile_component(document["reup_profile"])
    platform = _safe_target_platform(document["target_platform"])
    expected = f"dubvi-reup-job-{document['reup_job_id']}.job.json"
    if envelope_path.name != expected:
        raise ReupIntakeError("envelope filename does not match its explicit reup_job_id")
    expected_directory = root / profile / platform
    if envelope_path.parent.resolve() != expected_directory.resolve():
        raise ReupIntakeError("envelope path contradicts its authoritative profile/platform layout")
    return document


def _require_input_root(input_root: Path) -> Path:
    """Return only an existing real input root; canonical intake never creates it."""

    if input_root.is_symlink() or not input_root.is_dir():
        raise ReupIntakeError("configured Reup input root must be an existing non-symlink directory")
    return input_root.resolve()


def _validate_runtime_lineage(document: Mapping[str, object]) -> None:
    """Apply CP3's stricter runtime lineage rules above generic CP1 grammar."""

    if "attempt_number" not in document:
        raise ReupIntakeError("canonical Reup envelope requires attempt_number")
    attempt = document["attempt_number"]
    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
        raise ReupIntakeError("canonical Reup envelope attempt_number must be positive")
    parent_present = "parent_reup_job_id" in document
    if attempt == 1 and parent_present:
        raise ReupIntakeError("first canonical Reup attempt must not carry parent_reup_job_id")
    if attempt > 1 and not parent_present:
        raise ReupIntakeError("retry canonical Reup attempt requires parent_reup_job_id")


def _safe_profile_component(value: object) -> str:
    if not isinstance(value, str) or not value or value != value.strip() or value in {".", ".."}:
        raise ReupIntakeError("reup_profile must be one nonblank safe path component")
    if "\x00" in value or any(token in value for token in ("/", "\\", ":")) or Path(value).is_absolute():
        raise ReupIntakeError("reup_profile must be one non-absolute safe path component")
    return value


def _safe_target_platform(value: object) -> str:
    if not isinstance(value, str) or _TARGET_PLATFORM_RE.fullmatch(value) is None:
        raise ReupIntakeError("target_platform must be a lowercase canonical safe component")
    return value


def _validate_media(media: Path, directory: Path, document: Mapping[str, object]) -> None:
    if media.parent.resolve() != directory.resolve() or media.is_symlink() or not media.is_file():
        raise ReupIntakeError("envelope media is absent, symlinked, or outside its envelope directory")
    if not media.name.startswith("dubvi-") or media.stat().st_size != document["media_size"]:
        raise ReupIntakeError("envelope media name or size does not match canonical job")
    digest = hashlib.sha256()
    with media.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    if "sha256:" + digest.hexdigest() != document["source_fingerprint"]:
        raise ReupIntakeError("envelope media fingerprint does not match canonical job")


def _assert_under(root: Path, path: Path) -> None:
    try:
        path.resolve(strict=False).relative_to(root.resolve(strict=False))
    except ValueError as error:
        raise ReupIntakeError("envelope path escapes configured input root") from error
