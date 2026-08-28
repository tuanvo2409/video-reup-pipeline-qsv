import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Literal

# Base workspace directory
BASE_DIR = Path(__file__).resolve().parent.parent

@dataclass
class PipelineConfig:
    """System configuration for video pipeline processing."""
    # Directory paths
    input_dir: Path = BASE_DIR / "input"
    processing_dir: Path = BASE_DIR / "processing"
    # Output directory - Updated to user's desired folder
    output_dir: Path = Path(r"C:\Users\vmath\Downloads\douyinnnnnnnnnnn\video reup raw")
    failed_dir: Path = BASE_DIR / "failed"
    assets_dir: Path = BASE_DIR / "assets"
    mascots_dir: Path = BASE_DIR / "assets" / "mascots"
    hooks_dir: Path = BASE_DIR / "assets" / "hooks"
    outros_dir: Path = BASE_DIR / "assets" / "outros"
    bgm_dir: Path = BASE_DIR / "assets" / "bgm"
    frames_dir: Path = BASE_DIR / "assets" / "frames"
    watermarks_dir: Path = BASE_DIR / "assets" / "watermarks"
    default_watermark: Path = BASE_DIR / "assets" / "watermarks" / "default_invisible_mask.jpg"
    log_file: Path = BASE_DIR / "system.log"

    # DUBVI Bridge Directory (Giai đoạn 2)
    dubvi_media_dir: Path = Path(r"C:\Users\vmath\Videos\douyin")
    auto_send_to_dubvi: bool = False

    # Video output resolution (9:16 vertical standard)
    target_width: int = 1080
    target_height: int = 1920
    target_fps: int = 30

    # Layout Mode: 'crop_fill' (Zoom to fill 9:16 edge-to-edge, center focused, NO black bars)
    layout_mode: str = "crop_fill"

    # Zoom factor (Tự động zoom nhẹ 1.02x - 1.05x để lấp đầy 100% màn hình, không viền đen)
    zoom_min: float = 1.02
    zoom_max: float = 1.05

    # Head & Tail Temporal Trim (Cắt đầu và đuôi video cỡ 1s)
    enable_trim: bool = True
    trim_start_range: tuple = (0.8, 1.2)   # Cắt đầu cỡ 1s (0.8s - 1.2s)
    trim_end_range: tuple = (0.8, 1.2)     # Cắt đuôi cỡ 1s (0.8s - 1.2s)

    # Video & Audio Speedup
    enable_tempo_shift: bool = True
    tempo_min: float = 1.10                # Tốc độ tối thiểu 1.10x
    tempo_max: float = 1.20                # Tốc độ tối đa 1.20x

    # PySceneDetect / Smart Nonlinear Scene Speedup (Bẻ tốc độ phi tuyến tính theo từng cảnh)
    enable_smart_scenes: bool = True
    scene_threshold: float = 0.28

    # Invisible Watermark Mask (Lớp phủ ảnh vô hình mờ 98% phá vỡ ma trận AI)
    enable_invisible_mask: bool = True
    invisible_mask_opacity: float = 0.02   # Độ đậm 2% (Mờ 98% mắt người không thể thấy)

    # Horizontal Flip (Lật gương ngang tùy chọn)
    enable_hflip: bool = False

    # Subtitle mask (Mặc định tắt để giữ toàn vẹn hình ảnh)
    enable_mask: bool = False
    mask_bottom_ratio: float = 0.05
    mask_color: str = "black@0.85"

    # Khung viền viền đen (Mặc định TẮT)
    enable_frame: bool = False
    frame_border_width: int = 0
    frame_border_color: str = "black"

    # Visual Perturbations (Anti-pHash)
    enable_grain: bool = True
    grain_strength: int = 2          # 1 to 5 (subtle micro-grain)
    enable_jitter: bool = True
    jitter_contrast_range: tuple = (0.98, 1.03)
    jitter_saturation_range: tuple = (0.98, 1.04)
    jitter_brightness_range: tuple = (-0.01, 0.015)

    # Meta vPDQ & pHash Scorer (Tự động chấm điểm sau khi render)
    enable_vpdq_scoring: bool = True

    # Scaling flags
    scale_flags: str = "fast_bilinear"

    # Hardware & Encoders
    preferred_encoders: List[str] = field(
        default_factory=lambda: ["hevc_qsv", "h264_qsv", "libx264"]
    )
    max_workers: int = 2  # Optimized for 2-core i3-1115G4 + 20GB RAM
    qsv_global_quality: int = 25
    cpu_crf: int = 23
    cpu_preset: str = "veryfast"

    # Audio Settings
    bgm_volume: float = 0.08
    audio_sample_rate: int = 44100
    audio_channels: int = 2
    audio_bitrate: str = "192k"

    # Assets toggle
    enable_mascot: bool = True

    # Supported file extensions
    supported_extensions: tuple = (".mp4", ".mov", ".mkv", ".avi", ".webm", ".ts")
    supported_audio_extensions: tuple = (".mp3", ".wav", ".aac", ".m4a", ".ogg", ".flac")
    supported_image_extensions: tuple = (".png", ".webp", ".jpg", ".jpeg")

    def ensure_dirs(self) -> None:
        """Ensure all required directories exist."""
        for d in [
            self.input_dir,
            self.processing_dir,
            self.output_dir,
            self.failed_dir,
            self.mascots_dir,
            self.hooks_dir,
            self.outros_dir,
            self.bgm_dir,
            self.frames_dir,
            self.watermarks_dir,
            self.dubvi_media_dir,
        ]:
            d.mkdir(parents=True, exist_ok=True)

# Default global instance
config = PipelineConfig()
