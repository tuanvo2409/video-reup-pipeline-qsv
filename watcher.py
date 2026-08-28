import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional

from pipeline.config import config
from pipeline.hardware import detect_best_encoder
from pipeline.processor import process_single_video, ProcessResult
from pipeline.vpdq_scorer import score_video_pair
from pipeline.cache_manager import (
    cleanup_stale_processing,
    clear_cache_history,
    get_cache_stats,
)

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

def scan_input_files() -> List[Path]:
    """Scan the input directory for supported video files."""
    if not config.input_dir.exists():
        return []
    return [
        p for p in config.input_dir.iterdir()
        if p.is_file() and p.suffix.lower() in config.supported_extensions
    ]

def run_batch(
    max_workers: int = 2,
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
    """Process all available videos in the input directory in parallel with Smart Cache."""
    config.ensure_dirs()
    # Startup Stale Cache & Temp Cleanup
    cleanup_stale_processing()

    files = scan_input_files()

    if not files:
        logger.info(f"No video files found in '{config.input_dir}'.")
        return

    layout = override_mode or config.layout_mode
    logger.info(f"=== Starting Batch Processing: {len(files)} files | Workers: {max_workers} | Mode: {layout} ===")
    start_batch_time = time.time()

    results: List[ProcessResult] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                process_single_video,
                f,
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
            res = future.result()
            results.append(res)
            status_str = "SUCCESS" if res.success else "FAILED"
            if res.cache_hit:
                status_str = "CACHE_HIT"
            dubvi_tag = " -> [DUBVI Ready]" if res.dubvi_forwarded else ""
            logger.info(f"[{status_str}] Finished '{res.input_file.name}' in {res.duration_seconds:.2f}s | {res.vpdq_score_info or ''}{dubvi_tag}")

    total_time = time.time() - start_batch_time
    success_count = sum(1 for r in results if r.success)
    failed_count = len(results) - success_count

    logger.info("=== Batch Summary ===")
    logger.info(f"Total: {len(results)} | Success: {success_count} | Failed: {failed_count}")
    logger.info(f"Total Time: {total_time:.2f}s | Avg Time: {total_time/max(1, len(results)):.2f}s/video")

def run_watch_daemon(
    poll_interval: int = 3,
    max_workers: int = 2,
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
    """Watch the input directory continuously and process files as they arrive."""
    config.ensure_dirs()
    cleanup_stale_processing()

    layout = override_mode or config.layout_mode
    logger.info(f"=== Daemon Watcher Started (Polling every {poll_interval}s | Workers: {max_workers} | Mode: {layout}) ===")
    logger.info(f"Drop video files into '{config.input_dir.resolve()}' to process.")
    logger.info("Press Ctrl+C to stop.")

    try:
        while True:
            files = scan_input_files()
            if files:
                logger.info(f"Detected {len(files)} new file(s). Dispatching to queue...")
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [
                        executor.submit(
                            process_single_video,
                            f,
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
                        )
                        for f in files
                    ]
                    for f in as_completed(futures):
                        _ = f.result()
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("Daemon watcher stopped by user.")

def main():
    parser = argparse.ArgumentParser(
        description="High-Performance Video Pipeline with Smart Cache, PySceneDetect, vPDQ, and Intel QSV"
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Run continuously in watcher mode, processing files as they are placed in input/"
    )
    parser.add_argument(
        "--batch",
        action="store_true",
        help="Run once over all files in input/ and exit"
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
        type=int,
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
        "--tempo",
        type=float,
        default=None,
        help="Alias for --speed"
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
        help="Auto copy rendered video to DUBVI media directory (C:/Users/vmath/Videos/douyin)"
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
        help="Calculate Meta vPDQ similarity score between two videos and exit"
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
        print(f"\n[Meta vPDQ Assessment]")
        print(f"• Original: {res['original_file']}")
        print(f"• Processed: {res['processed_file']}")
        print(f"• Similarity: {res['similarity_percent']}%")
        print(f"• Status: {res['status']}")
        print(f"• Verdict: {res['verdict']}\n")
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
    speed_val = args.speed if args.speed is not None else args.tempo

    params = {
        "max_workers": args.workers,
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
        "custom_tempo": speed_val,
        "custom_zoom": args.zoom,
        "enable_trim": enable_trim,
        "custom_trim_start": args.trim_start,
        "custom_trim_end": args.trim_end,
        "send_to_dubvi": send_to_dubvi,
        "force_reprocess": args.force,
    }

    if args.watch:
        run_watch_daemon(**params)
    else:
        run_batch(**params)

if __name__ == "__main__":
    main()
