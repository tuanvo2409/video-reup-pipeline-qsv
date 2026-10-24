import hashlib
import json
import logging
import os
import tempfile
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from pipeline.config import config

logger = logging.getLogger("VideoPipeline.CacheManager")

CACHE_FILE = config.processing_dir.parent / "processed_history.json"
TRANSFORM_SIGNATURE_SCHEMA = "v2"
_LOCK = threading.Lock()


@contextmanager
def _ledger_lock():
    """Serialize ledger operations within this process and across processes."""
    with _LOCK:
        lock_path = CACHE_FILE.with_suffix(CACHE_FILE.suffix + ".lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with open(lock_path, "a+b") as lock_file:
            if os.name == "nt":
                import msvcrt

                lock_file.seek(0, os.SEEK_END)
                if lock_file.tell() == 0:
                    lock_file.write(b"0")
                    lock_file.flush()
                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)
                try:
                    yield
                finally:
                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _unique_recovery_path(directory: Path, filename: str) -> Path:
    candidate = directory / filename
    if not candidate.exists():
        return candidate
    stem = candidate.stem
    suffix = candidate.suffix
    index = 1
    while True:
        candidate = directory / f"{stem}.recovery{index}{suffix}"
        if not candidate.exists():
            return candidate
        index += 1

def get_file_md5_quick(file_path: Path) -> str:
    """Compute MD5 checksum using Python 3.11+ stdlib file_digest."""
    with open(file_path, "rb") as f:
        return hashlib.file_digest(f, "md5").hexdigest()


def _signature_value(value: Any) -> Any:
    """Normalize signature inputs, including selected asset content identity."""
    if isinstance(value, Path):
        normalized = {"path": str(value)}
        if value.is_file():
            digest = hashlib.sha256()
            try:
                with value.open("rb") as asset_file:
                    for chunk in iter(lambda: asset_file.read(1024 * 1024), b""):
                        digest.update(chunk)
            except OSError:
                normalized["unreadable"] = True
            else:
                normalized["sha256"] = digest.hexdigest()
        elif value.exists() and not value.is_dir():
            normalized["unreadable"] = True
        return normalized
    if isinstance(value, dict):
        return {str(key): _signature_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_signature_value(item) for item in value]
    if isinstance(value, set):
        return sorted((_signature_value(item) for item in value), key=str)
    return value


def make_transform_signature(
    profile_name: str,
    platform_name: str,
    transform_options: Dict[str, Any],
    config_obj: Any,
) -> str:
    payload = {
        "schema": TRANSFORM_SIGNATURE_SCHEMA,
        "profile": profile_name,
        "platform": platform_name,
        "options": _signature_value(transform_options),
        "config": {
            name: _signature_value(getattr(config_obj, name, None))
            for name in (
                "layout_mode",
                "enable_trim",
                "enable_smart_scenes",
                "scene_threshold",
                "enable_invisible_mask",
                "invisible_mask_opacity",
                "enable_hflip",
                "enable_mask",
                "mask_bottom_ratio",
                "mask_color",
                "enable_frame",
                "frame_border_width",
                "frame_border_color",
                "enable_grain",
                "grain_strength",
                "enable_jitter",
                "jitter_contrast_range",
                "jitter_saturation_range",
                "jitter_brightness_range",
                "enable_tempo_shift",
                "tempo_min",
                "tempo_max",
                "target_width",
                "target_height",
                "target_fps",
                "preferred_encoders",
                "scale_flags",
                "qsv_global_quality",
                "cpu_crf",
                "cpu_preset",
                "bgm_volume",
                "audio_sample_rate",
                "audio_channels",
                "audio_bitrate",
                "enable_mascot",
            )
        },
    }
    encoded = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()

def _load_cache_unlocked() -> Dict[str, Any]:
    if not CACHE_FILE.exists():
        return {}
    with open(CACHE_FILE, "r", encoding="utf-8") as f:
        cache = json.load(f)
    if not isinstance(cache, dict):
        raise ValueError("cache ledger must contain a JSON object")
    return cache


def load_cache() -> Dict[str, Any]:
    """Load the processed history JSON cache."""
    with _ledger_lock():
        try:
            return _load_cache_unlocked()
        except Exception as e:
            logger.warning(f"Failed to read cache file ({e}). Initializing empty cache.")
            return {}

def _save_cache_unlocked(cache_data: Dict[str, Any]) -> None:
    temp_cache_name = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=CACHE_FILE.parent,
            prefix=f".{CACHE_FILE.name}.",
            suffix=".tmp",
            delete=False,
        ) as f:
            temp_cache_name = f.name
            json.dump(cache_data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_cache_name, CACHE_FILE)
    finally:
        if temp_cache_name:
            Path(temp_cache_name).unlink(missing_ok=True)


def save_cache(cache_data: Dict[str, Any]) -> None:
    """Save the cache dictionary to processed_history.json atomically."""
    with _ledger_lock():
        _save_cache_unlocked(cache_data)

def is_already_processed(
    file_path: Path,
    transform_signature: Optional[str] = None,
    output_root: Optional[Path] = None,
) -> Optional[Dict[str, Any]]:
    """Return a verified cache entry for the requested transform, if present."""
    if not file_path.exists():
        return None
    original_md5 = get_file_md5_quick(file_path)
    cached_info = load_cache().get(original_md5)
    if not isinstance(cached_info, dict):
        return None
    if cached_info.get("original_md5") != original_md5:
        return None
    if cached_info.get("transform_signature_schema") != TRANSFORM_SIGNATURE_SCHEMA:
        return None
    if not transform_signature or cached_info.get("transform_signature") != transform_signature:
        return None
    output_path_value = cached_info.get("output_path")
    output_md5 = cached_info.get("output_md5")
    if not isinstance(output_path_value, str) or not isinstance(output_md5, str):
        return None
    output_path = Path(output_path_value)
    if not output_path.is_absolute():
        return None
    root = (output_root or config.output_dir).resolve()
    try:
        resolved_output = output_path.resolve(strict=True)
        resolved_output.relative_to(root)
    except (FileNotFoundError, OSError, ValueError):
        return None
    if output_path.is_symlink() or not output_path.is_file():
        return None
    try:
        if get_file_md5_quick(output_path) != output_md5:
            return None
    except OSError:
        return None
    return cached_info

def record_processed_video(
    original_md5: str,
    original_name: str,
    output_path: Path,
    output_md5: str,
    duration_s: float,
    vpdq_score: Optional[float] = None,
    vpdq_status: Optional[str] = None,
    transform_signature: Optional[str] = None,
) -> None:
    """Record a newly processed video into the cache ledger."""
    with _ledger_lock():
        cache = _load_cache_unlocked()
        existing = cache.get(original_md5, {})
        runs = existing.get("runs_count", 0) + 1
        cache[original_md5] = {
            "original_filename": original_name,
            "output_filename": output_path.name,
            "output_path": str(output_path.resolve()),
            "processed_at": datetime.now().isoformat(timespec="seconds"),
            "original_md5": original_md5,
            "output_md5": output_md5,
            "duration_seconds": round(duration_s, 2),
            "vpdq_score": vpdq_score,
            "vpdq_status": vpdq_status,
            "runs_count": runs,
            "transform_signature_schema": TRANSFORM_SIGNATURE_SCHEMA,
            "transform_signature": transform_signature,
        }
        _save_cache_unlocked(cache)
    logger.info(f"[Cache Recorded] '{original_name}' (MD5: {original_md5[:8]}) saved to history ledger.")

def cleanup_stale_processing() -> int:
    """Clean up leftover tmp_ files and recover interrupted files on startup."""
    if not config.processing_dir.exists():
        return 0
    cleaned_count = 0
    cleaned_count += _cleanup_proven_p1c_orphans()
    for item in config.processing_dir.iterdir():
        if item.is_file():
            if item.name.startswith("tmp_"):
                config.failed_dir.mkdir(parents=True, exist_ok=True)
                recovery_path = _unique_recovery_path(config.failed_dir, item.name)
                try:
                    # ponytail: open-file protection is platform/filesystem-specific; use job-owned locks/leases for cross-platform certainty.
                    item.rename(recovery_path)
                except PermissionError:
                    logger.warning(f"[Startup Stale Cleanup] Skipping active file: '{item.name}'")
                    continue
                logger.warning(f"[Auto-Recovery] Quarantined stale temporary file: '{recovery_path.name}'")
            else:
                recovery_path = _unique_recovery_path(config.input_dir, item.name)
                try:
                    item.rename(recovery_path)
                except PermissionError:
                    logger.warning(f"[Startup Stale Cleanup] Skipping active file: '{item.name}'")
                    continue
                logger.info(f"[Auto-Recovery] Recovered interrupted file back to input/: '{recovery_path.name}'")
            cleaned_count += 1
    if cleaned_count > 0:
        logger.info(f"[Startup Stale Cleanup] Cleaned / recovered {cleaned_count} leftover file(s).")
    return cleaned_count


def _cleanup_proven_p1c_orphans() -> int:
    """Recover only attempt directories with validated terminal status evidence."""
    status_root = getattr(config, "engine_status_dir", None)
    if status_root is None:
        return 0
    status_root = Path(status_root)
    cleaned_count = 0
    for container_name in ("p1c-attempts", "p1c-source"):
        container = config.processing_dir / container_name
        if not container.is_dir() or container.is_symlink():
            continue
        for attempt in container.iterdir():
            if not attempt.is_dir() or attempt.is_symlink() or not _has_terminal_p1c_evidence(status_root, attempt.name):
                continue
            recovery_dir = config.failed_dir / "p1c-orphans"
            recovery_dir.mkdir(parents=True, exist_ok=True)
            destination = _unique_recovery_path(recovery_dir, attempt.name)
            if destination.exists():
                logger.warning("[P1C Recovery] Leaving orphan '%s': recovery destination exists", attempt.name)
                continue
            try:
                attempt.rename(destination)
            except OSError:
                logger.warning("[P1C Recovery] Could not recover proven orphan '%s'", attempt.name)
                continue
            cleaned_count += 1
            logger.warning("[P1C Recovery] Recovered proven orphan '%s'", attempt.name)
    return cleaned_count


def _has_terminal_p1c_evidence(status_root: Path, job_id: str) -> bool:
    """Accept bounded terminal evidence for exactly one canonical job id."""
    event_dir = status_root / "reup" / job_id
    for sequence, event_kind, state in ((3, "failed", "failed"), (5, "succeeded", "succeeded")):
        path = event_dir / f"event-{sequence:06d}.json"
        try:
            if path.is_symlink() or not path.is_file() or path.stat().st_size > 64 * 1024:
                continue
            with path.open("r", encoding="utf-8") as handle:
                event = json.load(handle)
        except (OSError, ValueError):
            continue
        if (
            isinstance(event, dict)
            and event.get("engine_job_id") == job_id
            and event.get("sequence") == sequence
            and event.get("event_kind") == event_kind
            and event.get("state") == state
        ):
            return True
    return False

def clear_cache_history() -> int:
    """Clear all records in the cache ledger."""
    with _ledger_lock():
        if CACHE_FILE.exists():
            CACHE_FILE.unlink()
            logger.info("[Cache Cleared] Cache history ledger cleared successfully.")
            return 1
    return 0

def get_cache_stats() -> Dict[str, Any]:
    """Get high level summary of cache statistics."""
    cache = load_cache()
    return {
        "total_unique_processed": len(cache),
        "history_file": str(CACHE_FILE),
        "file_size_bytes": CACHE_FILE.stat().st_size if CACHE_FILE.exists() else 0,
    }
