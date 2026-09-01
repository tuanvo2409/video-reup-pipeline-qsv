import json
import logging
import random
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Dict, Any

from pipeline.config import config
from pipeline.hardware import detect_best_encoder
from pipeline.ffmpeg_builder import FFmpegCommandBuilder
from pipeline.scene_manager import detect_scenes, assign_scene_speeds
from pipeline.vpdq_scorer import score_video_pair
from pipeline.profile_manager import get_channel_profile, ChannelProfile
from pipeline.platform_presets import get_platform_preset, PlatformPreset, PLATFORM_PRESETS
from pipeline.cache_manager import (
    is_already_processed,
    record_processed_video,
    get_file_md5_quick,
)

logger = logging.getLogger("VideoPipeline.Processor")

@dataclass
class ProcessResult:
    """Result summary of video processing."""
    success: bool
    input_file: Path
    output_file: Optional[Path] = None
    duration_seconds: float = 0.0
    original_md5: Optional[str] = None
    output_md5: Optional[str] = None
    encoder_used: Optional[str] = None
    layout_mode: Optional[str] = None
    trimmed_info: Optional[str] = None
    zoom_info: Optional[str] = None
    speed_info: Optional[str] = None
    vpdq_score_info: Optional[str] = None
    profile_name: str = "default"
    platform_name: str = "universal"
    cache_hit: bool = False
    dubvi_forwarded: bool = False
    error_message: Optional[str] = None

def get_video_duration(file_path: Path) -> float:
    """Get the duration of a video in seconds using ffprobe."""
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        str(file_path)
    ]
    try:
        res = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
            creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0
        )
        if res.returncode == 0:
            return float(res.stdout.strip())
    except Exception as e:
        logger.warning(f"Failed to get duration for {file_path.name}: {e}")
    return 0.0

def pick_random_asset(folder: Optional[Path], valid_extensions: tuple) -> Optional[Path]:
    """Select a random file from a folder matching the extensions."""
    if not folder or not folder.exists():
        return None
    files = [f for f in folder.iterdir() if f.is_file() and f.suffix.lower() in valid_extensions]
    return random.choice(files) if files else None

def process_single_video(
    source_path: Path,
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
) -> ProcessResult:
    """
    Process a single video with Multi-Profile and Platform-Adaptive Preset support.
    """
    start_time = time.time()
    config.ensure_dirs()

    # Determine channel profile from subfolder or override
    profile_name = "default"
    detected_platform = target_platform or "universal"

    if override_profile:
        profile_name = override_profile
    else:
        try:
            rel = source_path.relative_to(config.input_dir)
            if len(rel.parts) > 2:
                profile_name = rel.parts[0]
                detected_platform = rel.parts[1]
            elif len(rel.parts) == 2:
                profile_name = rel.parts[0]
        except Exception:
            profile_name = "default"

    profile: ChannelProfile = get_channel_profile(profile_name)
    platform_preset: PlatformPreset = get_platform_preset(detected_platform)

    # 1. Smart Cache Lookup
    cache_key_file = source_path
    if not force_reprocess and source_path.exists():
        cached_info = is_already_processed(source_path)
        if cached_info:
            out_file_path = Path(cached_info["output_path"])
            logger.info(
                f"[CACHE HIT] [{profile_name} | {platform_preset.name}] '{source_path.name}' was already processed on {cached_info['processed_at']}. "
                f"Skipping render. (Use --force to re-render fresh variations)."
            )
            source_path.unlink(missing_ok=True)
            return ProcessResult(
                success=True,
                input_file=source_path,
                output_file=out_file_path if out_file_path.exists() else None,
                duration_seconds=0.0,
                original_md5=cached_info["original_md5"],
                output_md5=cached_info["output_md5"],
                profile_name=profile_name,
                platform_name=platform_preset.name,
                cache_hit=True,
                vpdq_score_info=f"Cached vPDQ: {cached_info.get('vpdq_score')}% [{cached_info.get('vpdq_status')}]",
            )

    # 2. Move from input to processing
    temp_filename = f"{profile_name}_{platform_preset.name}_{source_path.name}"
    temp_processing_path = config.processing_dir / temp_filename
    try:
        shutil.move(str(source_path), str(temp_processing_path))
    except Exception as e:
        logger.error(f"Failed to move {source_path.name} to processing: {e}")
        return ProcessResult(
            success=False,
            input_file=source_path,
            profile_name=profile_name,
            platform_name=platform_preset.name,
            error_message=f"I/O move error: {e}"
        )

    original_md5 = get_file_md5_quick(temp_processing_path)
    total_duration = get_video_duration(temp_processing_path)

    # Calculate Head & Tail Trimming (Cỡ 1s)
    do_trim = config.enable_trim if enable_trim is None else enable_trim
    trim_start = 0.0
    trim_end = None
    trimmed_str = "None"

    if do_trim and total_duration > 3.0:
        trim_start = custom_trim_start if custom_trim_start is not None else round(random.uniform(*profile.trim_start_range), 3)
        trim_end_offset = custom_trim_end if custom_trim_end is not None else round(random.uniform(*profile.trim_end_range), 3)
        trim_end = max(1.0, round(total_duration - trim_end_offset, 3))
        trimmed_str = f"Head: {trim_start}s | Tail: {trim_end_offset}s"

    # PySceneDetect Nonlinear Scene Speed Scheduling (Using Profile Speed Range)
    use_smart_scenes = config.enable_smart_scenes if enable_smart_scenes is None else enable_smart_scenes
    scene_schedule = None
    speed_info_str = "Speed: 1.15x (Uniform)"

    if use_smart_scenes and total_duration > 5.0:
        raw_scenes = detect_scenes(temp_processing_path, threshold=config.scene_threshold)
        if len(raw_scenes) > 1:
            adjusted_scenes = [
                (max(trim_start, s_start), min(trim_end if trim_end is not None else total_duration, s_end))
                for s_start, s_end in raw_scenes
                if min(trim_end if trim_end is not None else total_duration, s_end) - max(trim_start, s_start) >= 0.4
            ]
            if adjusted_scenes:
                scene_schedule = assign_scene_speeds(
                    adjusted_scenes,
                    speed_min=profile.speed_range[0],
                    speed_max=profile.speed_range[1]
                )
                speeds_sample = [f"{s[2]}x" for s in scene_schedule[:3]]
                speed_info_str = f"Smart Scenes: {len(scene_schedule)} cuts ({', '.join(speeds_sample)}...)"

    # Output paths (Categorized by Channel Profile folder)
    target_out_dir = config.output_dir / profile_name if profile_name != "default" else config.output_dir
    target_out_dir.mkdir(parents=True, exist_ok=True)
    
    suffix_tag = f"_{platform_preset.name}" if platform_preset.name != "universal" else ""
    output_filename = f"processed_{source_path.stem}{suffix_tag}.mp4"
    final_output_path = target_out_dir / output_filename
    meta_output_path = target_out_dir / f"processed_{source_path.stem}{suffix_tag}.meta.json"
    temp_output_path = config.processing_dir / f"tmp_{temp_filename}"

    # Pick channel-specific or global assets
    mascot_file = profile.mascot_path or pick_random_asset(config.mascots_dir, (".webm", ".mov", ".png"))
    hook_file = pick_random_asset(profile.hooks_dir, config.supported_extensions) or pick_random_asset(config.hooks_dir, config.supported_extensions)
    outro_file = pick_random_asset(profile.outros_dir, config.supported_extensions) or pick_random_asset(config.outros_dir, config.supported_extensions)
    bgm_file = pick_random_asset(profile.bgm_dir, config.supported_audio_extensions) or pick_random_asset(config.bgm_dir, config.supported_audio_extensions)
    frame_file = pick_random_asset(config.frames_dir, config.supported_image_extensions)
    watermark_file = profile.watermark_path or pick_random_asset(config.watermarks_dir, config.supported_image_extensions) or (
        config.default_watermark if config.default_watermark.exists() else None
    )

    encoder = override_encoder or detect_best_encoder()
    layout = override_mode or config.layout_mode
    effective_hflip = enable_hflip if enable_hflip is not None else profile.enable_hflip
    effective_inv_opacity = invisible_mask_opacity if invisible_mask_opacity is not None else profile.invisible_mask_opacity

    # Build command with PlatformPreset
    builder = FFmpegCommandBuilder(
        input_path=temp_processing_path,
        output_path=temp_output_path,
        mascot_path=mascot_file,
        hook_path=hook_file,
        outro_path=outro_file,
        bgm_path=bgm_file,
        frame_path=frame_file,
        invisible_mask_path=watermark_file,
        layout_mode=layout,
        encoder=encoder,
        enable_mask=enable_mask,
        mask_ratio=mask_ratio,
        enable_frame=enable_frame,
        enable_invisible_mask=enable_invisible_mask,
        invisible_mask_opacity=effective_inv_opacity,
        enable_hflip=effective_hflip,
        enable_grain=enable_grain,
        enable_jitter=enable_jitter,
        enable_tempo=enable_tempo,
        custom_tempo=custom_tempo,
        custom_zoom=custom_zoom,
        trim_start=trim_start,
        trim_end=trim_end,
        scene_schedule=scene_schedule,
        platform_preset=platform_preset,
    )

    logger.info(
        f"Processing [{profile_name} | {platform_preset.name}] '{source_path.name}' [Mode: {layout} | "
        f"Encoder: {encoder} | {speed_info_str} | Zoom: {builder.zoom:.3f}x | "
        f"HFlip: {builder.enable_hflip} | Trim: {trimmed_str}]"
    )

    # Direct Hardware Execution
    cmd = builder.build_command()
    success = False
    last_error = ""

    try:
        process = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0
        )
        if process.returncode == 0 and temp_output_path.exists() and temp_output_path.stat().st_size > 0:
            success = True
        else:
            last_error = process.stderr[-1000:] if process.stderr else "Unknown render error"
            if temp_output_path.exists():
                temp_output_path.unlink()
    except Exception as e:
        last_error = str(e)

    duration = time.time() - start_time

    if success:
        vpdq_info_str = "vPDQ: Skipped"
        vpdq_score_val = None
        vpdq_status_val = None

        if config.enable_vpdq_scoring:
            try:
                score_res = score_video_pair(temp_processing_path, temp_output_path)
                vpdq_score_val = score_res["similarity_percent"]
                vpdq_status_val = score_res["status"]
                vpdq_info_str = f"vPDQ Match: {vpdq_score_val}% [{vpdq_status_val}]"
            except Exception as e:
                logger.warning(f"Failed to score vPDQ: {e}")

        shutil.move(str(temp_output_path), str(final_output_path))
        temp_processing_path.unlink(missing_ok=True)

        new_md5 = get_file_md5_quick(final_output_path)

        # Write unique metadata receipt for this video
        meta_data = {
            "channel_profile": profile_name,
            "target_platform": platform_preset.name,
            "original_filename": source_path.name,
            "output_filename": final_output_path.name,
            "original_md5": original_md5,
            "output_md5": new_md5,
            "duration_seconds": round(duration, 2),
            "zoom_factor": round(builder.zoom, 4),
            "horizontal_flip": builder.enable_hflip,
            "invisible_mask_opacity": effective_inv_opacity,
            "loudnorm_target_lufs": platform_preset.target_lufs,
            "vpdq_similarity_percent": vpdq_score_val,
            "vpdq_status": vpdq_status_val,
            "processed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        try:
            with open(meta_output_path, "w", encoding="utf-8") as mf:
                json.dump(meta_data, mf, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"Failed to write metadata json: {e}")

        # Record in Processed Cache History Ledger
        record_processed_video(
            original_md5=original_md5,
            original_name=source_path.name,
            output_path=final_output_path,
            output_md5=new_md5,
            duration_s=duration,
            vpdq_score=vpdq_score_val,
            vpdq_status=vpdq_status_val,
        )

        # Auto forward to DUBVI media folder if requested
        do_send_dubvi = config.auto_send_to_dubvi if send_to_dubvi is None else send_to_dubvi
        dubvi_forwarded = False
        if do_send_dubvi:
            try:
                target_dubvi_dir = config.dubvi_media_dir / profile_name if profile_name != "default" else config.dubvi_media_dir
                target_dubvi_dir.mkdir(parents=True, exist_ok=True)
                dest_dubvi_file = target_dubvi_dir / final_output_path.name
                shutil.copy2(str(final_output_path), str(dest_dubvi_file))
                dubvi_forwarded = True
                logger.info(f"-> [DUBVI Bridge] Auto-copied to DUBVI folder: '{profile_name}/{dest_dubvi_file.name}'")
            except Exception as e:
                logger.warning(f"Failed to copy to DUBVI media dir: {e}")

        logger.info(
            f"Successfully processed: [{profile_name} | {platform_preset.name}] '{final_output_path.name}' in {duration:.2f}s "
            f"[{vpdq_info_str} | Orig MD5: {original_md5[:8]} -> New MD5: {new_md5[:8]}]"
        )
        return ProcessResult(
            success=True,
            input_file=source_path,
            output_file=final_output_path,
            duration_seconds=duration,
            original_md5=original_md5,
            output_md5=new_md5,
            encoder_used=encoder,
            layout_mode=layout,
            trimmed_info=trimmed_str,
            zoom_info=f"Zoom: {builder.zoom:.3f}x",
            speed_info=speed_info_str,
            vpdq_score_info=vpdq_info_str,
            profile_name=profile_name,
            platform_name=platform_preset.name,
            dubvi_forwarded=dubvi_forwarded,
        )
    else:
        failed_dir = config.failed_dir / profile_name if profile_name != "default" else config.failed_dir
        failed_dir.mkdir(parents=True, exist_ok=True)
        failed_path = failed_dir / temp_processing_path.name
        shutil.move(str(temp_processing_path), str(failed_path))
        logger.error(f"Failed to process '{source_path.name}'. Moved to failed/. Error: {last_error}")
        return ProcessResult(
            success=False,
            input_file=source_path,
            duration_seconds=duration,
            original_md5=original_md5,
            encoder_used=encoder,
            layout_mode=layout,
            trimmed_info=trimmed_str,
            zoom_info=f"Zoom: {builder.zoom:.3f}x",
            speed_info=speed_info_str,
            profile_name=profile_name,
            platform_name=platform_preset.name,
            error_message=last_error,
        )
