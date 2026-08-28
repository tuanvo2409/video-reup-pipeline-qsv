import subprocess
import logging
from typing import Tuple, List, Optional
from pipeline.config import config

logger = logging.getLogger("VideoPipeline.Hardware")

_CACHED_ENCODER: Optional[str] = None

def test_encoder(encoder_name: str) -> bool:
    """Test if an encoder works properly by encoding 1 test frame."""
    cmd = [
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
        "-f", "lavfi", "-i", "testsrc=duration=0.1:size=320x240:rate=10",
        "-c:v", encoder_name,
        "-f", "null", "-"
    ]
    try:
        res = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=5,
            creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0
        )
        return res.returncode == 0
    except Exception as e:
        logger.debug(f"Encoder test failed for {encoder_name}: {e}")
        return False

def detect_best_encoder() -> str:
    """Detect and return the best available video encoder on the system."""
    global _CACHED_ENCODER
    if _CACHED_ENCODER is not None:
        return _CACHED_ENCODER

    logger.info("Detecting hardware acceleration capabilities...")
    for enc in config.preferred_encoders:
        logger.debug(f"Testing encoder: {enc}")
        if test_encoder(enc):
            _CACHED_ENCODER = enc
            logger.info(f"Selected video encoder: {_CACHED_ENCODER}")
            return _CACHED_ENCODER

    _CACHED_ENCODER = "libx264"
    logger.warning("No hardware encoder available. Falling back to CPU: libx264")
    return _CACHED_ENCODER

def get_encoder_args(encoder: str) -> List[str]:
    """Return optimal FFmpeg encoding arguments for the specified encoder."""
    if encoder == "hevc_qsv":
        # Intel QuickSync HEVC
        return [
            "-c:v", "hevc_qsv",
            "-global_quality", str(config.qsv_global_quality),
            "-preset", "medium",
            "-pix_fmt", "nv12"
        ]
    elif encoder == "h264_qsv":
        # Intel QuickSync H.264
        return [
            "-c:v", "h264_qsv",
            "-global_quality", str(config.qsv_global_quality),
            "-preset", "medium",
            "-pix_fmt", "nv12"
        ]
    elif encoder == "libx265":
        # CPU HEVC
        return [
            "-c:v", "libx265",
            "-crf", str(config.cpu_crf),
            "-preset", config.cpu_preset,
            "-pix_fmt", "yuv420p"
        ]
    else:
        # Default CPU H.264
        return [
            "-c:v", "libx264",
            "-crf", str(config.cpu_crf),
            "-preset", config.cpu_preset,
            "-pix_fmt", "yuv420p"
        ]
