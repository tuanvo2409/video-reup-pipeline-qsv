import hashlib
import json
import logging
import os
import shutil
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from pipeline.config import config

logger = logging.getLogger("VideoPipeline.CacheManager")

CACHE_FILE = config.processing_dir.parent / "processed_history.json"
_LOCK = threading.Lock()

def get_file_md5_quick(file_path: Path) -> str:
    """Compute MD5 checksum of a file efficiently."""
    hasher = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def load_cache() -> Dict[str, Any]:
    """Load the processed history JSON cache."""
    with _LOCK:
        if not CACHE_FILE.exists():
            return {}
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to read cache file ({e}). Initializing empty cache.")
            return {}

def save_cache(cache_data: Dict[str, Any]) -> None:
    """Save the cache dictionary to processed_history.json atomically."""
    with _LOCK:
        temp_cache = CACHE_FILE.with_suffix(".tmp")
        try:
            with open(temp_cache, "w", encoding="utf-8") as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            shutil.move(str(temp_cache), str(CACHE_FILE))
        except Exception as e:
            logger.error(f"Failed to write cache file: {e}")
            if temp_cache.exists():
                temp_cache.unlink(missing_ok=True)

def is_already_processed(file_path: Path) -> Optional[Dict[str, Any]]:
    """
    Check if a file has already been successfully processed.
    Matches by original MD5 hash to prevent duplicate rendering.
    """
    if not file_path.exists():
        return None
    
    file_md5 = get_file_md5_quick(file_path)
    cache = load_cache()
    return cache.get(file_md5)

def record_processed_video(
    original_md5: str,
    original_name: str,
    output_path: Path,
    output_md5: str,
    duration_s: float,
    vpdq_score: Optional[float] = None,
    vpdq_status: Optional[str] = None,
) -> None:
    """Record a newly processed video into the cache ledger."""
    cache = load_cache()
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
    }
    save_cache(cache)
    logger.info(f"[Cache Recorded] '{original_name}' (MD5: {original_md5[:8]}) saved to history ledger.")

def cleanup_stale_processing() -> int:
    """
    Scans the processing/ directory on startup to clean up leftover tmp_ files
    from interrupted or crashed runs, preventing stuck locks or stalled queues.
    Returns the number of files recovered or cleaned.
    """
    if not config.processing_dir.exists():
        return 0
    
    cleaned_count = 0
    for item in config.processing_dir.iterdir():
        if item.is_file():
            if item.name.startswith("tmp_"):
                item.unlink(missing_ok=True)
                cleaned_count += 1
            else:
                target_input = config.input_dir / item.name
                shutil.move(str(item), str(target_input))
                logger.info(f"[Auto-Recovery] Recovered interrupted file back to input/: '{item.name}'")
                cleaned_count += 1
                
    if cleaned_count > 0:
        logger.info(f"[Startup Stale Cleanup] Cleaned / recovered {cleaned_count} leftover file(s).")
    return cleaned_count

def clear_cache_history() -> int:
    """Clear all records in the cache ledger."""
    with _LOCK:
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
