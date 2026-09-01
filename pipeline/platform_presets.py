from dataclasses import dataclass
from typing import Optional, Tuple, Dict, Any

@dataclass
class PlatformPreset:
    """Configuration preset tailored for specific social media platform algorithms."""
    name: str
    target_lufs: float = -14.0       # EBU R128 integrated loudness target
    true_peak: float = -1.0          # True peak limit in dBFS
    speed_multiplier_bias: float = 0.0
    unsharp_filter: Optional[str] = None
    contrast_bias: float = 1.0
    saturation_bias: float = 1.0
    brightness_bias: float = 0.0
    colorbalance_filter: Optional[str] = None
    safe_zone_top_ratio: float = 0.0
    safe_zone_bottom_ratio: float = 0.0
    max_duration_seconds: Optional[float] = None
    grain_strength_bias: int = 0
    enable_hflip_default: bool = False

# Official Platform Profiles based on creator guidelines and broadcast standards
PLATFORM_PRESETS: Dict[str, PlatformPreset] = {
    "universal": PlatformPreset(
        name="universal",
        target_lufs=-14.0,
        true_peak=-1.0,
        unsharp_filter="unsharp=lx=5:ly=5:la=0.6:cx=3:cy=3:ca=0.3",
        contrast_bias=1.02,
        saturation_bias=1.03,
        safe_zone_bottom_ratio=0.15,
        max_duration_seconds=58.5,
    ),
    "tiktok": PlatformPreset(
        name="tiktok",
        target_lufs=-14.0,
        true_peak=-1.0,
        speed_multiplier_bias=0.03,
        unsharp_filter="unsharp=lx=5:ly=5:la=0.8:cx=3:cy=3:ca=0.4",
        contrast_bias=1.02,
        saturation_bias=1.03,
        safe_zone_bottom_ratio=0.22,
        enable_hflip_default=True,
    ),
    "facebook": PlatformPreset(
        name="facebook",
        target_lufs=-16.0,
        true_peak=-1.5,
        speed_multiplier_bias=-0.02,
        contrast_bias=1.06,
        saturation_bias=1.02,
        brightness_bias=-0.01,
        safe_zone_top_ratio=0.12,
        safe_zone_bottom_ratio=0.15,
        grain_strength_bias=1,
    ),
    "shorts": PlatformPreset(
        name="shorts",
        target_lufs=-14.0,
        true_peak=-1.0,
        speed_multiplier_bias=0.01,
        unsharp_filter="unsharp=lx=3:ly=3:la=0.5:cx=3:cy=3:ca=0.3",
        contrast_bias=1.01,
        saturation_bias=1.06,
        brightness_bias=0.015,
        safe_zone_bottom_ratio=0.12,
        max_duration_seconds=58.5,
    ),
    "instagram": PlatformPreset(
        name="instagram",
        target_lufs=-14.0,
        true_peak=-1.0,
        speed_multiplier_bias=-0.03,
        contrast_bias=1.02,
        saturation_bias=1.02,
        colorbalance_filter="colorbalance=rs=0.02:gs=0.01:bs=-0.02",
        safe_zone_bottom_ratio=0.10,
    ),
}

def get_platform_preset(platform_name: Optional[str]) -> PlatformPreset:
    """Retrieve the platform preset or default to universal."""
    if not platform_name:
        return PLATFORM_PRESETS["universal"]
    key = platform_name.lower().strip()
    if key in ["fb", "facebook", "reels", "fb_reels"]:
        return PLATFORM_PRESETS["facebook"]
    elif key in ["tt", "tiktok"]:
        return PLATFORM_PRESETS["tiktok"]
    elif key in ["yt", "yt_shorts", "shorts", "youtube"]:
        return PLATFORM_PRESETS["shorts"]
    elif key in ["ig", "instagram", "insta"]:
        return PLATFORM_PRESETS["instagram"]
    return PLATFORM_PRESETS.get(key, PLATFORM_PRESETS["universal"])
