"""Immutable Reup-to-Translator handoff v2 publication."""

from __future__ import annotations

import hashlib
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from pipeline.dubvi_engine_contract import canonical_json_bytes, parse_json_document, validate_reup_to_translator_handoff


class HandoffV2Error(RuntimeError):
    pass


@dataclass(frozen=True)
class PublishedHandoffV2:
    handoff_id: str
    video_path: Path
    sidecar_path: Path
    handoff_ref: str
    output_fingerprint: str


def publish_reup_handoff(output_path: Path, job: Mapping[str, object], media_root: Path, *, output_fingerprint: str, created_at_utc: str | None = None) -> PublishedHandoffV2:
    root = _root(media_root)
    profile = _safe(job.get("reup_profile"), "reup_profile")
    job_id = _safe(job.get("reup_job_id"), "reup_job_id")
    if output_path.is_symlink() or not output_path.is_file() or output_path.stat().st_size <= 0 or _hash(output_path)[0] != output_fingerprint:
        raise HandoffV2Error("canonical Reup output is not verified")
    directory = root / profile; _under(root, directory); directory.mkdir(parents=True, exist_ok=True)
    video = directory / f"reup-{job_id}.mp4"; sidecar = directory / f"reup-{job_id}.meta.json"
    _under(root, video); _under(root, sidecar)
    if sidecar.exists() or sidecar.is_symlink():
        document = _read_sidecar(sidecar)
        if _existing_matches(document, job, profile, job_id, output_fingerprint, video):
            return PublishedHandoffV2(str(document["handoff_id"]), video, sidecar, f"{profile}/{sidecar.name}", output_fingerprint)
        raise HandoffV2Error("FINAL_PATH_CONFLICT")
    if video.exists() or video.is_symlink():
        if video.is_symlink() or not video.is_file() or _hash(video)[0] != output_fingerprint:
            raise HandoffV2Error("FINAL_PATH_CONFLICT")
    else:
        _copy_link(output_path, video, output_fingerprint)
    handoff_id = str(uuid.uuid4())
    payload: dict[str, object] = {
        "handoff_schema_version": 2, "handoff_status": "complete", "handoff_id": handoff_id,
        "candidate_id": job["candidate_id"], "schedule_id": job["schedule_id"], "dispatch_id": job["dispatch_id"],
        "reup_job_id": job_id, "correlation_id": job["dispatch_id"], "channel_id": job["channel_id"],
        "channel_slug": job["channel_slug"], "reup_profile": profile, "target_platform": job["target_platform"],
        "localization_profile": job.get("localization_profile"), "policy_profile": job.get("policy_profile"),
        "source_fingerprint": job["source_fingerprint"], "reup_output_fingerprint": output_fingerprint,
        "created_at_utc": created_at_utc or _now(),
    }
    validated = validate_reup_to_translator_handoff(payload); bytes_ = canonical_json_bytes(validated)
    stage = sidecar.with_name(f".{sidecar.name}.{handoff_id}.part")
    try:
        with stage.open("xb") as handle: handle.write(bytes_); handle.flush(); os.fsync(handle.fileno())
        if stage.read_bytes() != bytes_: raise HandoffV2Error("handoff sidecar stage verification failed")
        try: os.link(stage, sidecar)
        except FileExistsError:
            existing = _read_sidecar(sidecar)
            if existing != validated: raise HandoffV2Error("FINAL_PATH_CONFLICT")
            handoff_id = str(existing["handoff_id"])
        return PublishedHandoffV2(handoff_id, video, sidecar, f"{profile}/{sidecar.name}", output_fingerprint)
    except HandoffV2Error: raise
    except OSError as error: raise HandoffV2Error(f"handoff sidecar publication failed: {error}") from error
    finally:
        stage.unlink(missing_ok=True)


def _read_sidecar(path: Path) -> dict[str, object]:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 64 * 1024: raise HandoffV2Error("invalid handoff sidecar")
    try: return validate_reup_to_translator_handoff(parse_json_document(path.read_bytes()))
    except Exception as error: raise HandoffV2Error("invalid handoff sidecar") from error


def _existing_matches(document: Mapping[str, object], job: Mapping[str, object], profile: str, job_id: str, output_fingerprint: str, video: Path) -> bool:
    expected = {
        "candidate_id": job.get("candidate_id"),
        "schedule_id": job.get("schedule_id"),
        "dispatch_id": job.get("dispatch_id"),
        "reup_job_id": job_id,
        "correlation_id": job.get("dispatch_id"),
        "channel_id": job.get("channel_id"),
        "channel_slug": job.get("channel_slug"),
        "reup_profile": profile,
        "target_platform": job.get("target_platform"),
        "localization_profile": job.get("localization_profile"),
        "policy_profile": job.get("policy_profile"),
        "source_fingerprint": job.get("source_fingerprint"),
        "reup_output_fingerprint": output_fingerprint,
    }
    if any(document.get(field) != value for field, value in expected.items()):
        return False
    return not video.is_symlink() and video.is_file() and video.stat().st_size > 0 and _hash(video)[0] == output_fingerprint


def _copy_link(source: Path, final: Path, fingerprint: str) -> None:
    stage = final.with_name(f".{final.name}.{fingerprint[7:19]}.part")
    if not stage.exists():
        with source.open("rb") as src, stage.open("xb") as dst:
            while chunk := src.read(1024 * 1024): dst.write(chunk)
            dst.flush(); os.fsync(dst.fileno())
    if _hash(stage)[0] != fingerprint: raise HandoffV2Error("handoff video stage mismatch")
    try: os.link(stage, final)
    except FileExistsError:
        if _hash(final)[0] != fingerprint: raise HandoffV2Error("FINAL_PATH_CONFLICT")
    finally: stage.unlink(missing_ok=True)


def _hash(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256(); size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024): digest.update(chunk); size += len(chunk)
    return "sha256:" + digest.hexdigest(), size


def _root(path: Path) -> Path:
    if path.exists() or path.is_symlink():
        if path.is_symlink() or not path.is_dir(): raise HandoffV2Error("DUBVI_MEDIA_DIR must be a real directory")
    else: path.mkdir(parents=True, exist_ok=False)
    return path.resolve()


def _under(root: Path, path: Path) -> None:
    try: path.resolve(strict=False).relative_to(root.resolve())
    except ValueError as error: raise HandoffV2Error("handoff path escapes media root") from error


def _safe(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip() or "/" in value or "\\" in value: raise HandoffV2Error(f"{field} is unsafe")
    return value


def _now() -> str:
    from datetime import datetime, timezone
    value = datetime.now(timezone.utc); return value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{value.microsecond // 1000:03d}Z"
