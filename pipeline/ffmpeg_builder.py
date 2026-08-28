import random
from pathlib import Path
from typing import List, Optional, Tuple
from pipeline.config import config
from pipeline.hardware import detect_best_encoder, get_encoder_args

class FFmpegCommandBuilder:
    """Builds robust, single-pass FFmpeg commands with PySceneDetect nonlinear speedup, vPDQ perturbation, and Intel QSV."""

    def __init__(
        self,
        input_path: Path,
        output_path: Path,
        mascot_path: Optional[Path] = None,
        hook_path: Optional[Path] = None,
        outro_path: Optional[Path] = None,
        bgm_path: Optional[Path] = None,
        frame_path: Optional[Path] = None,
        invisible_mask_path: Optional[Path] = None,
        layout_mode: Optional[str] = None,
        encoder: Optional[str] = None,
        enable_mask: Optional[bool] = None,
        mask_ratio: Optional[float] = None,
        enable_frame: Optional[bool] = None,
        enable_invisible_mask: Optional[bool] = None,
        invisible_mask_opacity: Optional[float] = None,
        enable_hflip: Optional[bool] = None,
        enable_grain: Optional[bool] = None,
        enable_jitter: Optional[bool] = None,
        enable_tempo: Optional[bool] = None,
        custom_tempo: Optional[float] = None,
        custom_zoom: Optional[float] = None,
        trim_start: float = 0.0,
        trim_end: Optional[float] = None,
        scene_schedule: Optional[List[Tuple[float, float, float]]] = None,
    ):
        self.input_path = input_path
        self.output_path = output_path
        self.mascot_path = mascot_path
        self.hook_path = hook_path
        self.outro_path = outro_path
        self.bgm_path = bgm_path
        self.frame_path = frame_path
        self.invisible_mask_path = invisible_mask_path or (
            config.default_watermark if config.default_watermark.exists() else None
        )

        self.layout_mode = layout_mode or config.layout_mode
        self.encoder = encoder or detect_best_encoder()

        self.enable_mask = config.enable_mask if enable_mask is None else enable_mask
        self.mask_ratio = mask_ratio if mask_ratio is not None else config.mask_bottom_ratio
        self.enable_frame = config.enable_frame if enable_frame is None else enable_frame
        self.enable_hflip = config.enable_hflip if enable_hflip is None else enable_hflip

        self.enable_invisible_mask = (
            config.enable_invisible_mask
            if enable_invisible_mask is None
            else enable_invisible_mask
        )
        self.invisible_mask_opacity = (
            invisible_mask_opacity
            if invisible_mask_opacity is not None
            else config.invisible_mask_opacity
        )

        self.enable_grain = config.enable_grain if enable_grain is None else enable_grain
        self.enable_jitter = config.enable_jitter if enable_jitter is None else enable_jitter
        self.enable_tempo = config.enable_tempo_shift if enable_tempo is None else enable_tempo

        self.trim_start = trim_start
        self.trim_end = trim_end
        self.scene_schedule = scene_schedule

        # Randomize subtle zoom factor (1.02x to 1.05x) centered
        if custom_zoom:
            self.zoom = custom_zoom
        else:
            self.zoom = round(random.uniform(config.zoom_min, config.zoom_max), 3)

        # Video & Audio Speedup (1.10x to 1.20x)
        if custom_tempo:
            self.speed = custom_tempo
        elif self.enable_tempo:
            self.speed = round(random.uniform(config.tempo_min, config.tempo_max), 3)
        else:
            self.speed = 1.0

        # Randomize color jitter values
        if self.enable_jitter:
            self.contrast = round(random.uniform(*config.jitter_contrast_range), 3)
            self.saturation = round(random.uniform(*config.jitter_saturation_range), 3)
            self.brightness = round(random.uniform(*config.jitter_brightness_range), 3)
        else:
            self.contrast, self.saturation, self.brightness = 1.0, 1.0, 0.0

    def build_command(self) -> List[str]:
        """Construct the complete single-pass FFmpeg command line arguments list."""
        cmd: List[str] = ["ffmpeg", "-y", "-hide_banner"]

        inputs: List[Path] = [self.input_path]
        mascot_idx: Optional[int] = None
        hook_idx: Optional[int] = None
        outro_idx: Optional[int] = None
        bgm_idx: Optional[int] = None
        frame_idx: Optional[int] = None
        wm_idx: Optional[int] = None

        # Input 0: Main Video
        cmd.extend(["-i", str(self.input_path)])

        if self.mascot_path and self.mascot_path.exists() and config.enable_mascot:
            mascot_idx = len(inputs)
            inputs.append(self.mascot_path)
            cmd.extend(["-stream_loop", "-1", "-i", str(self.mascot_path)])

        if self.hook_path and self.hook_path.exists():
            hook_idx = len(inputs)
            inputs.append(self.hook_path)
            cmd.extend(["-i", str(self.hook_path)])

        if self.outro_path and self.outro_path.exists():
            outro_idx = len(inputs)
            inputs.append(self.outro_path)
            cmd.extend(["-i", str(self.outro_path)])

        if self.bgm_path and self.bgm_path.exists():
            bgm_idx = len(inputs)
            inputs.append(self.bgm_path)
            cmd.extend(["-stream_loop", "-1", "-i", str(self.bgm_path)])

        if self.frame_path and self.frame_path.exists() and self.enable_frame:
            frame_idx = len(inputs)
            inputs.append(self.frame_path)
            cmd.extend(["-i", str(self.frame_path)])

        if self.invisible_mask_path and self.invisible_mask_path.exists() and self.enable_invisible_mask:
            wm_idx = len(inputs)
            inputs.append(self.invisible_mask_path)
            cmd.extend(["-loop", "1", "-i", str(self.invisible_mask_path)])

        filter_complex_parts: List[str] = []

        # ----------------------------------------------------------------------
        # 0. PySceneDetect Nonlinear Speed Modulation or Global Trim+Speedup
        # ----------------------------------------------------------------------
        if self.scene_schedule and len(self.scene_schedule) > 1:
            # Segment-by-segment nonlinear temporal warping
            scene_v_tags = []
            scene_a_tags = []
            for idx, (s_start, s_end, s_spd) in enumerate(self.scene_schedule):
                v_tag = f"[sc_v_{idx}]"
                a_tag = f"[sc_a_{idx}]"
                filter_complex_parts.append(
                    f"[0:v]trim=start={s_start:.3f}:end={s_end:.3f},setpts=(PTS-STARTPTS)/{s_spd:.4f}{v_tag};"
                    f"[0:a]atrim=start={s_start:.3f}:end={s_end:.3f},asetpts=PTS-STARTPTS,atempo={s_spd:.4f}{a_tag}"
                )
                scene_v_tags.append(v_tag)
                scene_a_tags.append(a_tag)

            concat_scene_inputs = "".join([f"{v}{a}" for v, a in zip(scene_v_tags, scene_a_tags)])
            filter_complex_parts.append(
                f"{concat_scene_inputs}concat=n={len(self.scene_schedule)}:v=1:a=1[v_trimmed][a_trimmed]"
            )
        else:
            # Standard Global Head & Tail Trimming + Uniform Speedup
            v_trim_filters = []
            a_trim_filters = []

            if self.trim_start > 0 or self.trim_end is not None:
                trim_v_opts = [f"start={self.trim_start:.3f}"]
                trim_a_opts = [f"start={self.trim_start:.3f}"]
                if self.trim_end is not None and self.trim_end > self.trim_start:
                    trim_v_opts.append(f"end={self.trim_end:.3f}")
                    trim_a_opts.append(f"end={self.trim_end:.3f}")

                v_trim_filters.append(f"trim={':'.join(trim_v_opts)}")
                a_trim_filters.append(f"atrim={':'.join(trim_a_opts)}")

            if self.speed != 1.0:
                v_trim_filters.append(f"setpts=(PTS-STARTPTS)/{self.speed:.4f}")
                a_trim_filters.append("asetpts=PTS-STARTPTS")
                a_trim_filters.append(f"atempo={self.speed:.4f}")
            else:
                v_trim_filters.append("setpts=PTS-STARTPTS")
                a_trim_filters.append("asetpts=PTS-STARTPTS")

            filter_complex_parts.append(
                f"[0:v]{','.join(v_trim_filters)}[v_trimmed];"
                f"[0:a]{','.join(a_trim_filters)}[a_trimmed]"
            )

        v_in_label = "v_trimmed"
        a_in_label = "a_trimmed"

        # ----------------------------------------------------------------------
        # 1. Main Video Layout & Visual Anti-Fingerprint
        # ----------------------------------------------------------------------
        post_filters = []
        if self.enable_hflip:
            post_filters.append("hflip")
        if self.enable_jitter:
            post_filters.append(
                f"eq=contrast={self.contrast}:brightness={self.brightness}:saturation={self.saturation}"
            )
        if self.enable_grain:
            post_filters.append(f"noise=alls={config.grain_strength}:allf=t+u")

        # Custom Border lines ONLY if explicitly enabled
        if self.enable_frame and frame_idx is None and config.frame_border_width > 0:
            bw = config.frame_border_width
            bc = config.frame_border_color
            post_filters.append(
                f"drawbox=x=0:y=0:w=iw:h={bw}:color={bc}:t=fill,"
                f"drawbox=x=0:y=ih-{bw}:w=iw:h={bw}:color={bc}:t=fill,"
                f"drawbox=x=0:y=0:w={bw}:h=ih:color={bc}:t=fill,"
                f"drawbox=x=iw-{bw}:y=0:w={bw}:h=ih:color={bc}:t=fill"
            )

        post_filters_str = ("," + ",".join(post_filters)) if post_filters else ""

        if self.layout_mode == "blur_pip":
            # Smart Blur Background PiP mode
            filter_complex_parts.append(
                f"[{v_in_label}]split=2[v_bg_src][v_fg_src];"
                f"[v_bg_src]scale={config.target_width}:{config.target_height}:force_original_aspect_ratio=increase:flags={config.scale_flags},"
                f"crop={config.target_width}:{config.target_height},boxblur=25:5[v_bg];"
                f"[v_fg_src]scale={config.target_width}:-2:flags={config.scale_flags}[v_fg_raw]"
            )
            fg_label = "[v_fg_raw]"
            if self.enable_mask and self.mask_ratio > 0:
                mask_y = 1.0 - self.mask_ratio
                filter_complex_parts.append(
                    f"[v_fg_raw]drawbox=x=0:y=ih*{mask_y:.2f}:w=iw:h=ih*{self.mask_ratio:.2f}:color={config.mask_color}:t=fill[v_fg_masked]"
                )
                fg_label = "[v_fg_masked]"

            filter_complex_parts.append(
                f"[v_bg]{fg_label}overlay=(W-w)/2:(H-h)/2,fps={config.target_fps}{post_filters_str},format=yuv420p[v_main_base]"
            )
        else:
            # Default crop_fill: Zoom slightly + Scale to fill 1080x1920 + Center crop (NO black borders!)
            scaled_w = int(config.target_width * self.zoom)
            scaled_h = int(config.target_height * self.zoom)

            filters = [
                f"scale={scaled_w}:{scaled_h}:force_original_aspect_ratio=increase:flags={config.scale_flags}",
                f"crop={config.target_width}:{config.target_height}:(in_w-out_w)/2:(in_h-out_h)/2",
            ]
            if self.enable_mask and self.mask_ratio > 0:
                mask_y = 1.0 - self.mask_ratio
                filters.append(
                    f"drawbox=x=0:y=ih*{mask_y:.2f}:w=iw:h=ih*{self.mask_ratio:.2f}:color={config.mask_color}:t=fill"
                )
            filters.extend([f"fps={config.target_fps}"])
            if post_filters:
                filters.extend(post_filters)
            filters.append("format=yuv420p")

            filter_complex_parts.append(f"[{v_in_label}]{','.join(filters)}[v_main_base]")

        current_main_v = "[v_main_base]"

        # ----------------------------------------------------------------------
        # 2. Invisible Watermark Mask Overlay (Mờ 98% phá vỡ nhận diện AI)
        # ----------------------------------------------------------------------
        if wm_idx is not None:
            filter_complex_parts.append(
                f"[{wm_idx}:v]scale={config.target_width}:{config.target_height}:flags={config.scale_flags},"
                f"format=rgba,colorchannelmixer=aa={self.invisible_mask_opacity:.3f}[v_wm];"
                f"{current_main_v}[v_wm]overlay=0:0:format=auto:shortest=1[v_with_wm]"
            )
            current_main_v = "[v_with_wm]"

        # Custom Frame Overlay
        if frame_idx is not None and self.enable_frame:
            filter_complex_parts.append(
                f"[{frame_idx}:v]scale={config.target_width}:{config.target_height}[v_frame_scaled];"
                f"{current_main_v}[v_frame_scaled]overlay=0:0[v_framed]"
            )
            current_main_v = "[v_framed]"

        # Mascot Overlay
        if mascot_idx is not None:
            filter_complex_parts.append(
                f"[{mascot_idx}:v]scale=280:-1[v_mascot];"
                f"{current_main_v}[v_mascot]overlay=x=main_w-overlay_w-30:y=main_h*0.82-overlay_h-20:format=auto:shortest=1[v_main_overlay]"
            )
            current_main_v = "[v_main_overlay]"

        # ----------------------------------------------------------------------
        # 3. Audio Transformations & BGM Layering
        # ----------------------------------------------------------------------
        audio_filters = [
            f"aresample={config.audio_sample_rate}",
            "aformat=sample_fmts=fltp:channel_layouts=stereo"
        ]

        filter_complex_parts.append(f"[{a_in_label}]{','.join(audio_filters)}[a_main_processed]")
        current_main_a = "[a_main_processed]"

        if bgm_idx is not None:
            filter_complex_parts.append(
                f"[{bgm_idx}:a]aresample={config.audio_sample_rate},aformat=sample_fmts=fltp:channel_layouts=stereo[a_bgm_norm];"
                f"[a_main_processed][a_bgm_norm]amix=inputs=2:weights=1.0 {config.bgm_volume:.2f}:duration=first:dropout_transition=2[a_main_mixed]"
            )
            current_main_a = "[a_main_mixed]"

        # ----------------------------------------------------------------------
        # 4. Handle Hooks and Outros Concatenation
        # ----------------------------------------------------------------------
        segments_v = []
        segments_a = []

        if hook_idx is not None:
            filter_complex_parts.append(
                f"[{hook_idx}:v]scale={config.target_width}:{config.target_height}:flags={config.scale_flags},fps={config.target_fps},format=yuv420p[v_hook];"
                f"[{hook_idx}:a]aresample={config.audio_sample_rate},aformat=sample_fmts=fltp:channel_layouts=stereo[a_hook]"
            )
            segments_v.append("[v_hook]")
            segments_a.append("[a_hook]")

        segments_v.append(current_main_v)
        segments_a.append(current_main_a)

        if outro_idx is not None:
            filter_complex_parts.append(
                f"[{outro_idx}:v]scale={config.target_width}:{config.target_height}:flags={config.scale_flags},fps={config.target_fps},format=yuv420p[v_outro];"
                f"[{outro_idx}:a]aresample={config.audio_sample_rate},aformat=sample_fmts=fltp:channel_layouts=stereo[a_outro]"
            )
            segments_v.append("[v_outro]")
            segments_a.append("[a_outro]")

        final_v_label = "[final_v]"
        final_a_label = "[final_a]"

        if len(segments_v) > 1:
            concat_inputs = "".join([f"{v}{a}" for v, a in zip(segments_v, segments_a)])
            filter_complex_parts.append(
                f"{concat_inputs}concat=n={len(segments_v)}:v=1:a=1{final_v_label}{final_a_label}"
            )
        else:
            final_v_label = current_main_v
            final_a_label = current_main_a

        # Join full filter_complex
        full_filter_complex = ";".join(filter_complex_parts)
        cmd.extend(["-filter_complex", full_filter_complex])

        # Map streams
        cmd.extend(["-map", final_v_label, "-map", final_a_label])

        # Video Encoder options
        cmd.extend(get_encoder_args(self.encoder))

        # Audio encoder options
        cmd.extend([
            "-c:a", "aac",
            "-b:a", config.audio_bitrate,
            "-ar", str(config.audio_sample_rate),
            "-ac", str(config.audio_channels)
        ])

        # Wipe Metadata
        cmd.extend(["-map_metadata", "-1", "-map_chapters", "-1"])

        # Output path
        cmd.append(str(self.output_path))

        return cmd
