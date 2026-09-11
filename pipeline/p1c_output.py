"""Verified, attempt-scoped Reup output publication."""

from __future__ import annotations

import hashlib
import os
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
