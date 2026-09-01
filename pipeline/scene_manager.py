import logging
import random
import re
import subprocess
from pathlib import Path
from typing import List, Tuple

logger = logging.getLogger("VideoPipeline.SceneManager")

def detect_scenes(
    video_path: Path,
    threshold: float = 0.30,
    min_scene_seconds: float = 1.2
) -> List[Tuple[float, float]]:
    """
    Ultra-fast native scene cut detector using FFmpeg filtergraph.
    Runs at 10x-15x realtime without requiring heavy external dependencies.
    Returns list of (start_time, end_time) in seconds.
    """
    cmd = [
        "ffmpeg", "-v", "info",
        "-i", str(video_path),
        "-vf", f"select=gt(scene\\,{threshold:.2f}),showinfo",
        "-f", "null", "-"
    ]

    cut_timestamps = [0.0]
    total_duration = 0.0

    try:
        process = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=120,
            creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0
        )

        # Parse duration
        dur_match = re.search(r"Duration:\s*(\d+):(\d+):(\d+\.\d+)", process.stderr)
        if dur_match:
            h, m, s = map(float, dur_match.groups())
            total_duration = h * 3600 + m * 60 + s

        # Parse pts_time from showinfo
        for line in process.stderr.splitlines():
            if "pts_time:" in line:
                m = re.search(r"pts_time:\s*([\d\.]+)", line)
                if m:
                    t = float(m.group(1))
                    # Only add if it's at least min_scene_seconds away from previous cut
                    if t - cut_timestamps[-1] >= min_scene_seconds:
                        cut_timestamps.append(round(t, 3))

    except Exception as e:
        logger.warning(f"Scene detection error for {video_path.name}: {e}")

    if total_duration <= 0.0:
        # Fallback: estimate from last cut timestamp or default 60s
        total_duration = max(cut_timestamps[-1] + 2.0, 60.0)

    if cut_timestamps[-1] < total_duration:
        cut_timestamps.append(round(total_duration, 3))

    # Convert timestamps to interval tuples: [(0.0, t1), (t1, t2), ...]
    scenes: List[Tuple[float, float]] = []
    for i in range(len(cut_timestamps) - 1):
        start = cut_timestamps[i]
        end = cut_timestamps[i + 1]
        if end > start:
            scenes.append((start, end))

    if not scenes:
        scenes = [(0.0, total_duration)]

    logger.info(f"Detected {len(scenes)} distinct scene(s) in '{video_path.name}'.")
    return scenes

def assign_scene_speeds(
    scenes: List[Tuple[float, float]],
    speed_min: float = 1.10,
    speed_max: float = 1.20
) -> List[Tuple[float, float, float]]:
    """
    Assign a randomized speed factor to each individual scene.
    Returns list of (start_time, end_time, speed_factor).
    """
    scheduled_scenes: List[Tuple[float, float, float]] = []
    for start, end in scenes:
        speed = round(random.uniform(speed_min, speed_max), 3)
        scheduled_scenes.append((start, end, speed))
    return scheduled_scenes
