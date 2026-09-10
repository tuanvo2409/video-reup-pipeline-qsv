"""Side-effect-free P1C Reup envelope validation and accepted acknowledgement."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from pipeline.dubvi_engine_contract import (
    ContractValidationError,
    parse_json_document,
    validate_control_plane_to_reup_job,
)
from pipeline.p1c_status import ReupStatusError, publish_accepted_status


class ReupIntakeError(RuntimeError):
    """Raised when canonical Reup intake cannot fail closed."""


@dataclass(frozen=True)
class AcceptedReupJob:
    """One verified envelope whose accepted status is durably visible."""

    envelope_path: Path
    media_path: Path
    status_path: Path
    document: Mapping[str, object]


def iter_reup_envelopes(input_root: Path) -> tuple[Path, ...]:
    """Return only final canonical envelopes in deterministic lexical order."""

    if not input_root.is_dir() or input_root.is_symlink():
        raise ReupIntakeError("configured Reup input root must be an existing non-symlink directory")
    paths = [
        path for path in input_root.rglob("dubvi-reup-job-*.job.json")
        if path.is_file() and not path.is_symlink() and path.name.startswith("dubvi-reup-job-")
    ]
    return tuple(sorted(paths, key=lambda path: path.relative_to(input_root).as_posix()))


def accept_next_reup_job(
    input_root: Path,
    status_root: Path,
    *,
    occurred_at_utc: str | None = None,
) -> AcceptedReupJob | None:
    """Accept at most the first deterministic canonical envelope once."""

    envelopes = iter_reup_envelopes(input_root)
    if not envelopes:
        return None
    return accept_reup_envelope(envelopes[0], input_root, status_root, occurred_at_utc=occurred_at_utc)


def accept_reup_envelope(
    envelope_path: Path,
    input_root: Path,
    status_root: Path,
    *,
    occurred_at_utc: str | None = None,
) -> AcceptedReupJob:
    """Validate an immutable CP envelope and publish only its accepted ACK."""

    document = load_reup_envelope(envelope_path, input_root)
    media_path = envelope_path.parent / str(document["media_name"])
    _validate_media(media_path, envelope_path.parent, document)
    try:
        status_path = publish_accepted_status(status_root, document, occurred_at_utc=occurred_at_utc)
    except ReupStatusError as error:
        raise ReupIntakeError(f"canonical accepted status failed: {error}") from error
    return AcceptedReupJob(envelope_path, media_path, status_path, document)


def load_reup_envelope(envelope_path: Path, input_root: Path) -> dict[str, object]:
    """Parse and validate one final canonical Reup envelope without side effects."""

    _assert_under(input_root, envelope_path)
    if envelope_path.is_symlink() or not envelope_path.is_file():
        raise ReupIntakeError("canonical Reup envelope must be a regular final file")
    try:
        document = validate_control_plane_to_reup_job(parse_json_document(envelope_path.read_bytes()))
    except ContractValidationError as error:
        raise ReupIntakeError(f"invalid Control Plane Reup envelope: {error}") from error
    expected = f"dubvi-reup-job-{document['reup_job_id']}.job.json"
    if envelope_path.name != expected:
        raise ReupIntakeError("envelope filename does not match its explicit reup_job_id")
    expected_directory = input_root / str(document["reup_profile"]) / str(document["target_platform"])
    if envelope_path.parent.resolve() != expected_directory.resolve():
        raise ReupIntakeError("envelope path contradicts its authoritative profile/platform layout")
    return document


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
