import argparse
import errno
import logging
import shutil
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional

from pipeline.config import config
from pipeline.hardware import detect_best_encoder
from pipeline.processor import process_single_video, ProcessResult
from pipeline.vpdq_scorer import score_video_pair
from pipeline.platform_presets import PLATFORM_PRESETS
from pipeline.cache_manager import (
    cleanup_stale_processing,
    clear_cache_history,
    get_cache_stats,
    _unique_recovery_path,
)
from pipeline.input_rules import is_legacy_media_input
from pipeline.p1c_lease_client import LeaseBridgeClient, LeaseBridgeUnavailable

# Setup root logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(config.log_file, encoding="utf-8"),
    ]
)
logger = logging.getLogger("VideoPipeline.Watcher")


def _positive_workers(value: str) -> int:
    workers = int(value)
    if workers <= 0:
        raise argparse.ArgumentTypeError("workers must be a positive integer")
    return workers


def _contain_future_failure(source_path: Path, error: BaseException) -> ProcessResult:
    """Contain one legacy item failure without overwriting another recovery."""
    if source_path.is_file() and not source_path.is_symlink():
        config.failed_dir.mkdir(parents=True, exist_ok=True)
        recovery_path = _unique_recovery_path(config.failed_dir, source_path.name)
        if recovery_path.exists():
            logger.warning("[WATCHER] Skipped recovery for '%s': destination exists", source_path.name)
        else:
            try:
                shutil.move(str(source_path), str(recovery_path))
                logger.warning("[WATCHER] Quarantined failed source '%s'", recovery_path.name)
            except OSError:
                logger.warning("[WATCHER] Could not quarantine failed source '%s'", source_path.name)
    logger.warning("[WATCHER] Item '%s' failed (%s)", source_path.name, type(error).__name__)
    return ProcessResult(
        success=False,
        input_file=source_path,
        error_message=f"worker failure: {type(error).__name__}",
    )


@contextmanager
def _watcher_instance_lock():
    lock_path = config.processing_dir.parent / "watcher.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_file = lock_path.open("a+b")
    acquired = False
    try:
        if sys.platform == "win32":
            import msvcrt

            lock_file.seek(0, 2)
            if lock_file.tell() == 0:
                lock_file.write(b"0")
                lock_file.flush()
            lock_file.seek(0)
            try:
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            except PermissionError:
                logger.warning("Another legacy watcher instance is already running; exiting.")
                yield False
                return
        else:
            import fcntl

            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as error:
                if error.errno not in (errno.EACCES, errno.EAGAIN):
                    raise
                logger.warning("Another legacy watcher instance is already running; exiting.")
                yield False
                return
        acquired = True
        yield True
    finally:
        if acquired:
            if sys.platform == "win32":
                import msvcrt

                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        lock_file.close()


@contextmanager
def acquire_heavy_media_lease():
    """Acquire HEAVY_MEDIA lease from Control Plane if available, protecting CPU/GPU limits.

    Gracefully falls back to local execution if Control Plane bridge is not configured.
    """
    client: Optional[LeaseBridgeClient] = None
    try:
        client = LeaseBridgeClient.from_environment()
    except LeaseBridgeUnavailable:
        sibling = Path(__file__).resolve().parents[1] / "dubvi-control-plane"
        if (sibling / "src" / "dubvi_control_plane").is_dir():
            try:
                client = LeaseBridgeClient(sibling)
            except LeaseBridgeUnavailable:
                client = None

    if client is None:
        logger.info("[LEASE] Control plane bridge not configured; proceeding with local lock.")
        yield True
        return

    job_id = str(uuid.uuid4())
    dispatch_id = str(uuid.uuid4())
    job = {"reup_job_id": job_id, "dispatch_id": dispatch_id}

    try:
        admission = client.acquire(job)
    except LeaseBridgeUnavailable as exc:
        logger.warning("[LEASE] Control plane bridge unavailable (%s); proceeding with local lock.", exc)
        yield True
        return

    if not admission.granted or admission.lease is None:
        logger.warning("[LEASE] HEAVY_MEDIA capacity unavailable (active lease exists); deferring execution.")
        yield False
        return

    lease_id = str(admission.lease["lease_id"])
    logger.info("[LEASE] Acquired HEAVY_MEDIA lease %s (heartbeat every %ss)", lease_id, admission.heartbeat_seconds)

    stop_event = threading.Event()
    heartbeat_interval = max(1, admission.heartbeat_seconds)

    def _heartbeat_loop():
        while not stop_event.wait(timeout=heartbeat_interval):
            try:
                renewed = client.heartbeat(job, lease_id)
                if renewed.lease is None:
                    logger.warning("[LEASE] HEAVY_MEDIA lease renewal rejected.")
                    break
            except LeaseBridgeUnavailable as hb_err:
                logger.warning("[LEASE] HEAVY_MEDIA heartbeat error: %s", hb_err)
                break

    heartbeat_thread = threading.Thread(target=_heartbeat_loop, daemon=True, name="lease-heartbeat")
    heartbeat_thread.start()

    try:
        yield True
    finally:
        stop_event.set()
        heartbeat_thread.join(timeout=2.0)
        try:
            client.release(job, lease_id)
            logger.info("[LEASE] Released HEAVY_MEDIA lease %s", lease_id)
        except LeaseBridgeUnavailable as rel_err:
            logger.warning("[LEASE] Failed to release HEAVY_MEDIA lease %s: %s", lease_id, rel_err)


def scan_input_files() -> List[Path]:
    """Scan the input directory and all channel subdirectories for supported video files."""
    if not config.input_dir.exists():
        return []
    found: List[Path] = []
    for item in config.input_dir.iterdir():
        if is_legacy_media_input(item, config.supported_extensions):
            found.append(item)
        elif item.is_dir() and not item.name.startswith("."):
            for sub_item in item.rglob("*"):
                if is_legacy_media_input(sub_item, config.supported_extensions):
                    found.append(sub_item)
    return found

def run_batch(
    max_workers: int = 2,
    override_profile: Optional[str] = None,
    target_platform: Optional[str] = None,
    override_encoder: Optional[str] = None,
    override_mode: Optional[str] = None,
    enable_mask: Optional[bool] = None,
    mask_ratio: Optional[float] = None,
    enable_frame: Optional[bool] = None,
    enable_invisible_mask: Optional[bool] = None,
    invisible_mask_opacity: Optional[float] = None,
    enable_hflip: Optional[bool] = None,
    enable_grain: Optional[bool] = None,
    enable_jitter: Optional[bool] = None,
    enable_smart_scenes: Optional[bool] = None,
    enable_tempo: Optional[bool] = None,
    custom_tempo: Optional[float] = None,
    custom_zoom: Optional[float] = None,
    enable_trim: Optional[bool] = None,
    custom_trim_start: Optional[float] = None,
    custom_trim_end: Optional[float] = None,
    send_to_dubvi: Optional[bool] = None,
    force_reprocess: bool = False,
) -> None:
    """Process all available videos in the input directory and sub-channels in parallel with Smart Cache."""
    config.ensure_dirs()
    cleanup_stale_processing()

    files = scan_input_files()

    if not files:
        logger.info(f"No video files found in '{config.input_dir}'.")
        return

    layout = override_mode or config.layout_mode
    plat_str = target_platform or "Universal"
    logger.info(f"=== Starting Multi-Channel Batch: {len(files)} files | Platform: {plat_str} | Workers: {max_workers} ===")
    start_batch_time = time.time()

    results: List[ProcessResult] = []
    with acquire_heavy_media_lease() as lease_granted:
        if not lease_granted:
            return
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    process_single_video,
                    f,
                    override_profile,
                    target_platform,
                    override_encoder,
                    override_mode,
                    enable_mask,
                    mask_ratio,
                    enable_frame,
                    enable_invisible_mask,
                    invisible_mask_opacity,
                    enable_hflip,
                    enable_grain,
                    enable_jitter,
                    enable_smart_scenes,
                    enable_tempo,
                    custom_tempo,
                    custom_zoom,
                    enable_trim,
                    custom_trim_start,
                    custom_trim_end,
                    send_to_dubvi,
                    force_reprocess,
                ): f
                for f in files
            }

            for future in as_completed(futures):
                source_path = futures[future]
                try:
                    res = future.result()
                except Exception as error:
                    res = _contain_future_failure(source_path, error)
                results.append(res)
                status_str = "SUCCESS" if res.success else "FAILED"
                if res.cache_hit:
                    status_str = "CACHE_HIT"
                dubvi_tag = " -> [DUBVI Ready]" if res.dubvi_forwarded else ""
                logger.info(
                    f"[{status_str}] [{res.profile_name} | {res.platform_name}] Finished '{res.input_file.name}' in {res.duration_seconds:.2f}s | "
                    f"{res.vpdq_score_info or ''}{dubvi_tag}"
                )

    total_time = time.time() - start_batch_time
    success_count = sum(1 for r in results if r.success)
    failed_count = len(results) - success_count

    logger.info("=== Multi-Channel Batch Summary ===")
    logger.info(f"Total: {len(results)} | Success: {success_count} | Failed: {failed_count}")
    logger.info(f"Total Time: {total_time:.2f}s | Avg Time: {total_time/max(1, len(results)):.2f}s/video")

def run_watch_daemon(
    poll_interval: int = 3,
    max_workers: int = 2,
    override_profile: Optional[str] = None,
    target_platform: Optional[str] = None,
    override_encoder: Optional[str] = None,
    override_mode: Optional[str] = None,
    enable_mask: Optional[bool] = None,
    mask_ratio: Optional[float] = None,
    enable_frame: Optional[bool] = None,
    enable_invisible_mask: Optional[bool] = None,
    invisible_mask_opacity: Optional[float] = None,
    enable_hflip: Optional[bool] = None,
    enable_grain: Optional[bool] = None,
    enable_jitter: Optional[bool] = None,
    enable_smart_scenes: Optional[bool] = None,
    enable_tempo: Optional[bool] = None,
    custom_tempo: Optional[float] = None,
    custom_zoom: Optional[float] = None,
    enable_trim: Optional[bool] = None,
    custom_trim_start: Optional[float] = None,
    custom_trim_end: Optional[float] = None,
    send_to_dubvi: Optional[bool] = None,
    force_reprocess: bool = False,
) -> None:
    """Watch the input directory and all channel subdirectories continuously."""
    config.ensure_dirs()
    cleanup_stale_processing()

    layout = override_mode or config.layout_mode
    logger.info(f"=== Multi-Channel Daemon Started (Polling every {poll_interval}s | Workers: {max_workers} | Mode: {layout}) ===")
    logger.info(f"Drop video files into '{config.input_dir.resolve()}' or subfolders to process.")
    logger.info("Press Ctrl+C to stop.")

    try:
        while True:
            files = scan_input_files()
            if files:
                logger.info(f"Detected {len(files)} new file(s) across channels. Dispatching to queue...")
                with acquire_heavy_media_lease() as lease_granted:
                    if not lease_granted:
                        time.sleep(poll_interval)
                        continue
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        futures = {
                            executor.submit(
                                process_single_video,
                                f,
                                override_profile,
                                target_platform,
                                override_encoder,
                                override_mode,
                                enable_mask,
                                mask_ratio,
                                enable_frame,
                                enable_invisible_mask,
                                invisible_mask_opacity,
                                enable_hflip,
                                enable_grain,
                                enable_jitter,
                                enable_smart_scenes,
                                enable_tempo,
                                custom_tempo,
                                custom_zoom,
                                enable_trim,
                                custom_trim_start,
                                custom_trim_end,
                                send_to_dubvi,
                                force_reprocess,
                            ): f
                            for f in files
                        }
                        for f in as_completed(futures):
                            source_path = futures[f]
                            try:
                                f.result()
                            except Exception as error:
                                _contain_future_failure(source_path, error)
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("Daemon watcher stopped by user.")

def main():
    parser = argparse.ArgumentParser(
        description="High-Performance Multi-Platform Video Pipeline with Smart Cache, PySceneDetect, sampled-frame dHash similarity heuristic, and Intel QSV"
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Run continuously in watcher mode, processing files as they are placed in input/ or subfolders"
    )
    parser.add_argument(
        "--batch",
        action="store_true",
        help="Run once over all files in input/ and subfolders, then exit"
    )
    parser.add_argument(
        "--platform",
        type=str,
        default=None,
        choices=["universal", "tiktok", "facebook", "shorts", "instagram"],
        help="Target platform preset: 'universal', 'tiktok', 'facebook', 'shorts', 'instagram'"
    )
    parser.add_argument(
        "--profile",
        type=str,
        default=None,
        help="Force a specific channel profile name for all processed videos"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-rendering video even if it was already processed in cache history"
    )
    parser.add_argument(
        "--clear-cache",
        action="store_true",
        help="Clear the processed_history.json ledger"
    )
    parser.add_argument(
        "--cache-stats",
        action="store_true",
        help="Display cache history ledger statistics"
    )
    parser.add_argument(
        "--mode",
        type=str,
        default=None,
        choices=["crop_fill", "blur_pip"],
        help="Layout mode: 'crop_fill' (Zero black bars center zoom) or 'blur_pip' (Smart blur background PiP)"
    )
    parser.add_argument(
        "--workers",
        type=_positive_workers,
        default=config.max_workers,
        help=f"Number of parallel video workers (default: {config.max_workers})"
    )
    parser.add_argument(
        "--encoder",
        type=str,
        default=None,
        choices=["hevc_qsv", "h264_qsv", "libx264", "libx265"],
        help="Force a specific video encoder"
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=None,
        help="Set exact video+audio speedup multiplier (e.g. 1.15)"
    )
    parser.add_argument(
        "--zoom",
        type=float,
        default=None,
        help="Custom zoom factor to center-fill (e.g. 1.03 for 3%% zoom)"
    )
    parser.add_argument(
        "--hflip",
        action="store_true",
        help="Enable horizontal mirroring / flip"
    )
    parser.add_argument(
        "--to-dubvi",
        action="store_true",
        help="Publish rendered video and complete metadata sidecar to configured DUBVI_MEDIA_DIR"
    )
    parser.add_argument(
        "--no-scenes",
        action="store_true",
        help="Disable PySceneDetect nonlinear speed modulation"
    )
    parser.add_argument(
        "--inv-opacity",
        type=float,
        default=None,
        help="Opacity for invisible watermark mask (default 0.02 = 98%% invisible)"
    )
    parser.add_argument(
        "--no-inv-mask",
        action="store_true",
        help="Disable invisible watermark mask overlay"
    )
    parser.add_argument(
        "--trim-start",
        type=float,
        default=None,
        help="Custom seconds to trim from video start (e.g. 1.0)"
    )
    parser.add_argument(
        "--trim-end",
        type=float,
        default=None,
        help="Custom seconds to trim from video end (e.g. 1.0)"
    )
    parser.add_argument(
        "--no-trim",
        action="store_true",
        help="Disable head & tail trimming"
    )
    parser.add_argument(
        "--mask",
        action="store_true",
        help="Enable bottom subtitle masking box"
    )
    parser.add_argument(
        "--mask-ratio",
        type=float,
        default=None,
        help="Custom subtitle mask height ratio (e.g. 0.05 for 5%% height)"
    )
    parser.add_argument(
        "--frame",
        action="store_true",
        help="Enable black border frame"
    )
    parser.add_argument(
        "--no-grain",
        action="store_true",
        help="Disable film grain / noise injection"
    )
    parser.add_argument(
        "--no-jitter",
        action="store_true",
        help="Disable color jitter (EQ shifts)"
    )
    parser.add_argument(
        "--no-speed",
        action="store_true",
        help="Disable video/audio speedup (keep 1.0x)"
    )
    parser.add_argument(
        "--check-hw",
        action="store_true",
        help="Test and display detected hardware acceleration capabilities"
    )
    parser.add_argument(
        "--score",
        nargs=2,
        metavar=("ORIGINAL", "PROCESSED"),
        help="Calculate sampled-frame dHash similarity heuristic between two videos and exit"
    )

    args = parser.parse_args()

    if args.clear_cache:
        clear_cache_history()
        return

    if args.cache_stats:
        stats = get_cache_stats()
        print("\n[Smart Cache Statistics]")
        print(f"• Total Unique Processed Videos: {stats['total_unique_processed']}")
        print(f"• History Ledger File: {stats['history_file']}")
        print(f"• Size: {stats['file_size_bytes']} bytes\n")
        return

    if args.score:
        res = score_video_pair(Path(args.score[0]), Path(args.score[1]))
        print(f"\n[Sampled-frame dHash similarity heuristic]")
        print(f"• Original: {res.get('original_file', args.score[0])}")
        print(f"• Processed: {res.get('processed_file', args.score[1])}")
        print(f"• Similarity: {res.get('similarity_percent', 0.0)}%")
        print(f"• Status: {res.get('status', 'UNKNOWN')}")
        print(f"• Verdict: {res.get('verdict', res.get('message', 'UNKNOWN'))}\n")
        return

    if args.check_hw:
        encoder = detect_best_encoder()
        print(f"\n[Hardware Check] Best Detected Encoder: {encoder}")
        return

    # Parse boolean flags
    enable_mask = True if args.mask else None
    enable_frame = True if args.frame else False
    enable_inv_mask = False if args.no_inv_mask else None
    enable_hflip = True if args.hflip else None
    enable_grain = False if args.no_grain else None
    enable_jitter = False if args.no_jitter else None
    enable_tempo = False if args.no_speed else None
    enable_smart_scenes = False if args.no_scenes else None
    enable_trim = False if args.no_trim else None
    send_to_dubvi = True if args.to_dubvi else None

    params = {
        "max_workers": args.workers,
        "override_profile": args.profile,
        "target_platform": args.platform,
        "override_encoder": args.encoder,
        "override_mode": args.mode,
        "enable_mask": enable_mask,
        "mask_ratio": args.mask_ratio,
        "enable_frame": enable_frame,
        "enable_invisible_mask": enable_inv_mask,
        "invisible_mask_opacity": args.inv_opacity,
        "enable_hflip": enable_hflip,
        "enable_grain": enable_grain,
        "enable_jitter": enable_jitter,
        "enable_smart_scenes": enable_smart_scenes,
        "enable_tempo": enable_tempo,
        "custom_tempo": args.speed,
        "custom_zoom": args.zoom,
        "enable_trim": enable_trim,
        "custom_trim_start": args.trim_start,
        "custom_trim_end": args.trim_end,
        "send_to_dubvi": send_to_dubvi,
        "force_reprocess": args.force,
    }

    with _watcher_instance_lock() as acquired:
        if not acquired:
            return
        if args.watch:
            run_watch_daemon(**params)
        else:
            run_batch(**params)

if __name__ == "__main__":
    main()
