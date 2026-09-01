import hashlib
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Dict, Any, Tuple

from pipeline.config import config

logger = logging.getLogger("VideoPipeline.ProfileManager")

PROFILES_DIR = config.assets_dir / "profiles"

@dataclass
class ChannelProfile:
    """Configuration profile for a specific channel or page."""
    name: str
    speed_range: Tuple[float, float] = (1.10, 1.20)
    zoom_range: Tuple[float, float] = (1.02, 1.05)
    trim_start_range: Tuple[float, float] = (0.8, 1.2)
    trim_end_range: Tuple[float, float] = (0.8, 1.2)
    invisible_mask_opacity: float = 0.02
    grain_strength: int = 2
    jitter_contrast_range: Tuple[float, float] = (0.98, 1.03)
    jitter_saturation_range: Tuple[float, float] = (0.98, 1.04)
    jitter_brightness_range: Tuple[float, float] = (-0.01, 0.015)
    enable_hflip: bool = False
    # Profile specific asset paths
    profile_dir: Optional[Path] = None
    mascot_path: Optional[Path] = None
    watermark_path: Optional[Path] = None
    bgm_dir: Optional[Path] = None
    hooks_dir: Optional[Path] = None
    outros_dir: Optional[Path] = None

def _find_first_asset(folder: Path, extensions: Tuple[str, ...]) -> Optional[Path]:
    """Find the first matching asset file in a directory."""
    if not folder.exists():
        return None
    for ext in extensions:
        matches = list(folder.glob(f"*{ext}"))
        if matches:
            return matches[0]
    return None

def get_channel_profile(profile_name: str = "default") -> ChannelProfile:
    """
    Load or dynamically synthesize a unique fingerprint configuration profile
    for a specific channel/page name.
    """
    clean_name = profile_name.strip() if profile_name else "default"
    prof_folder = PROFILES_DIR / clean_name
    
    # 1. Base profile with dynamic entropy seed derived from profile name hash
    # Ensures each channel gets distinct baseline ranges even with zero configuration!
    name_hash = int(hashlib.md5(clean_name.encode("utf-8")).hexdigest()[:8], 16)
    
    # Slight deterministic variations per profile (e.g. +/- 0.02 speed, +/- 0.01 zoom, grain 1-4)
    speed_offset = ((name_hash % 7) - 3) * 0.008  # -0.024 to +0.024
    zoom_offset = ((name_hash % 5) - 2) * 0.005   # -0.010 to +0.010
    grain_val = 1 + (name_hash % 4)               # 1, 2, 3, or 4
    hflip_default = bool((name_hash >> 3) & 1) if clean_name != "default" else False
    
    base_speed = (
        round(max(1.06, config.tempo_min + speed_offset), 3),
        round(min(1.25, config.tempo_max + speed_offset), 3)
    )
    base_zoom = (
        round(max(1.015, config.zoom_min + zoom_offset), 3),
        round(min(1.065, config.zoom_max + zoom_offset), 3)
    )

    profile = ChannelProfile(
        name=clean_name,
        speed_range=base_speed,
        zoom_range=base_zoom,
        trim_start_range=config.trim_start_range,
        trim_end_range=config.trim_end_range,
        invisible_mask_opacity=config.invisible_mask_opacity,
        grain_strength=grain_val,
        jitter_contrast_range=config.jitter_contrast_range,
        jitter_saturation_range=config.jitter_saturation_range,
        jitter_brightness_range=config.jitter_brightness_range,
        enable_hflip=hflip_default,
        profile_dir=prof_folder if prof_folder.exists() else None,
    )

    # 2. Check for profile-specific JSON configuration override
    if prof_folder.exists():
        json_file = prof_folder / "profile.json"
        if json_file.exists():
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if "speed_range" in data:
                        profile.speed_range = tuple(data["speed_range"])
                    if "zoom_range" in data:
                        profile.zoom_range = tuple(data["zoom_range"])
                    if "grain_strength" in data:
                        profile.grain_strength = int(data["grain_strength"])
                    if "enable_hflip" in data:
                        profile.enable_hflip = bool(data["enable_hflip"])
                    if "invisible_mask_opacity" in data:
                        profile.invisible_mask_opacity = float(data["invisible_mask_opacity"])
            except Exception as e:
                logger.warning(f"Failed to parse profile.json for '{clean_name}': {e}")

        # Resolve profile-specific assets
        profile.mascot_path = _find_first_asset(prof_folder / "mascots", config.supported_image_extensions) or _find_first_asset(prof_folder, (".png", ".webp"))
        profile.watermark_path = _find_first_asset(prof_folder / "watermarks", config.supported_image_extensions)
        profile.bgm_dir = prof_folder / "bgm" if (prof_folder / "bgm").exists() else None
        profile.hooks_dir = prof_folder / "hooks" if (prof_folder / "hooks").exists() else None
        profile.outros_dir = prof_folder / "outros" if (prof_folder / "outros").exists() else None

    return profile
