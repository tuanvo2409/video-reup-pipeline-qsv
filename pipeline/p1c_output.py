"""Verified, attempt-scoped Reup output publication."""

from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


class OutputPublicationError(RuntimeError):
    pass


@dataclass(frozen=True)
class PublishedReupOutput:
    final_path: Path
    fingerprint: str
    size: int
    stage_path: Path
    reused_existing: bool = False


_OWNED_STAGE_RE = re.compile(r"^\.output\.([0-9a-f]{64})\.part$")


def publish_reup_output(candidate: Path, job: Mapping[str, object], output_root: Path) -> PublishedReupOutput:
    job_id = _text(job.get("reup_job_id"), "reup_job_id")
    root = _real_root(output_root)
    final_dir = root / "p1c" / job_id
    _ensure_contained(root, final_dir)
    final_dir.mkdir(parents=True, exist_ok=True)
    final = final_dir / "output.mp4"
    if candidate.is_symlink() or not candidate.is_file() or candidate.stat().st_size <= 0:
        raise OutputPublicationError("processor candidate is missing, empty, or symlinked")
    _ensure_contained(root, candidate)
    fingerprint, size = _hash_file(candidate)
    stage = final_dir / f".output.{fingerprint[7:]}.part"
    _stage_copy(candidate, stage, fingerprint, size)
    if final.exists() or final.is_symlink():
        if _matches(final, fingerprint, size):
            return PublishedReupOutput(final, fingerprint, size, stage, True)
        raise OutputPublicationError("FINAL_PATH_CONFLICT")
    try:
        os.link(stage, final)
    except FileExistsError:
        if _matches(final, fingerprint, size):
            return PublishedReupOutput(final, fingerprint, size, stage, True)
        raise OutputPublicationError("FINAL_PATH_CONFLICT")
    except OSError as error:
        raise OutputPublicationError(f"output publication failed: {error}") from error
    if not _matches(final, fingerprint, size):
        raise OutputPublicationError("published output failed verification")
    return PublishedReupOutput(final, fingerprint, size, stage, False)


def recover_prepared_reup_output(job: Mapping[str, object], output_root: Path) -> PublishedReupOutput | None:
    """Recover one exact attempt-owned output preparation without rerendering."""

    job_id = _text(job.get("reup_job_id"), "reup_job_id")
    root = _existing_root(output_root)
    if root is None:
        return None
    final_dir = root / "p1c" / job_id
    _ensure_contained(root, final_dir)
    if not final_dir.exists() and not final_dir.is_symlink():
        return None
    if final_dir.is_symlink() or not final_dir.is_dir():
        raise OutputPublicationError("attempt output directory must be real")
    valid: list[tuple[Path, str, int]] = []
    for path in final_dir.iterdir():
        match = _OWNED_STAGE_RE.fullmatch(path.name)
        if match is None or path.is_symlink() or not path.is_file() or path.stat().st_size <= 0:
            continue
        fingerprint, size = _hash_file(path)
        if fingerprint[7:] != match.group(1):
            continue
        valid.append((path, fingerprint, size))
    if len(valid) > 1:
        raise OutputPublicationError("multiple valid owned output stages exist")
    if not valid:
        return None
    stage, fingerprint, size = valid[0]
    final = final_dir / "output.mp4"
    if final.exists() or final.is_symlink():
        if _matches(final, fingerprint, size):
            return PublishedReupOutput(final, fingerprint, size, stage, True)
        raise OutputPublicationError("FINAL_PATH_CONFLICT")
    try:
        os.link(stage, final)
    except FileExistsError:
        if _matches(final, fingerprint, size):
            return PublishedReupOutput(final, fingerprint, size, stage, True)
        raise OutputPublicationError("FINAL_PATH_CONFLICT")
    except OSError as error:
        raise OutputPublicationError(f"output publication failed: {error}") from error
    if not _matches(final, fingerprint, size):
        raise OutputPublicationError("recovered output failed verification")
    return PublishedReupOutput(final, fingerprint, size, stage, False)


def verify_reup_output(output_path: Path, output_fingerprint: str) -> tuple[str, int]:
    """Verify one canonical published output without creating or changing files."""

    if output_path.is_symlink() or not output_path.is_file() or output_path.stat().st_size <= 0:
        raise OutputPublicationError("canonical Reup output is absent or unsafe")
    actual, size = _hash_file(output_path)
    if actual != output_fingerprint:
        raise OutputPublicationError("canonical Reup output fingerprint differs")
    return actual, size


def cleanup_published_output_stage(published: PublishedReupOutput) -> None:
    try:
        published.stage_path.unlink(missing_ok=True)
    except OSError:
        pass


def _real_root(root: Path) -> Path:
    if root.exists() or root.is_symlink():
        if root.is_symlink() or not root.is_dir():
            raise OutputPublicationError("REUP_OUTPUT_DIR must be a real directory")
    else:
        root.mkdir(parents=True, exist_ok=False)
    return root.resolve()


def _existing_root(root: Path) -> Path | None:
    if not root.exists() and not root.is_symlink():
        return None
    if root.is_symlink() or not root.is_dir():
        raise OutputPublicationError("REUP_OUTPUT_DIR must be a real directory")
    return root.resolve()


def _stage_copy(source: Path, stage: Path, expected_fp: str, expected_size: int) -> None:
    if stage.exists() or stage.is_symlink():
        if _matches(stage, expected_fp, expected_size):
            return
        raise OutputPublicationError("existing output stage differs")
    digest = hashlib.sha256(); size = 0
    try:
        with source.open("rb") as source_handle, stage.open("xb") as stage_handle:
            while chunk := source_handle.read(1024 * 1024):
                digest.update(chunk); size += len(chunk); stage_handle.write(chunk)
            stage_handle.flush(); os.fsync(stage_handle.fileno())
    except OSError as error:
        raise OutputPublicationError(f"output staging failed: {error}") from error
    if size != expected_size or "sha256:" + digest.hexdigest() != expected_fp or not _matches(stage, expected_fp, expected_size):
        raise OutputPublicationError("output stage verification failed")


def _hash_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256(); size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024): digest.update(chunk); size += len(chunk)
    return "sha256:" + digest.hexdigest(), size


def _matches(path: Path, fingerprint: str, size: int) -> bool:
    return not path.is_symlink() and path.is_file() and path.stat().st_size == size and _hash_file(path)[0] == fingerprint


def _ensure_contained(root: Path, path: Path) -> None:
    try: path.resolve(strict=False).relative_to(root.resolve())
    except ValueError as error: raise OutputPublicationError("path escapes configured output root") from error


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip() or "/" in value or "\\" in value or value in {".", ".."}:
        raise OutputPublicationError(f"{field} is not a safe identifier")
    return value
