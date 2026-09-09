from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from pipeline.config import PipelineConfig
from pipeline.dubvi_handoff import (
    HANDOFF_SCHEMA_VERSION,
    HandoffPublicationError,
    publish_dubvi_handoff,
)


class P0ConfigAndHandoffTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _metadata(self, video_name: str) -> dict[str, object]:
        return {
            "channel_profile": "channel-a",
            "target_platform": "shorts",
            "original_filename": "source.mp4",
            "output_filename": video_name,
            "original_md5": "original-md5",
            "output_md5": "output-md5",
            "duration_seconds": 42.0,
            "zoom_factor": 1.03,
            "encoder_used": "libx264",
            "layout_mode": "crop_fill",
            "vpdq_similarity_percent": 12.5,
            "vpdq_status": "PASSED",
            "processed_at": "2026-09-09 12:00:00",
        }

    def test_portable_output_and_bridge_configuration(self) -> None:
        output_dir = self.root / "configured-output"
        bridge_dir = self.root / "configured-bridge"
        with patch.dict(
            os.environ,
            {"REUP_OUTPUT_DIR": str(output_dir), "DUBVI_MEDIA_DIR": str(bridge_dir)},
            clear=False,
        ):
            config = PipelineConfig(
                input_dir=self.root / "input",
                processing_dir=self.root / "processing",
                failed_dir=self.root / "failed",
                assets_dir=self.root / "assets",
                mascots_dir=self.root / "assets" / "mascots",
                hooks_dir=self.root / "assets" / "hooks",
                outros_dir=self.root / "assets" / "outros",
                bgm_dir=self.root / "assets" / "bgm",
                frames_dir=self.root / "assets" / "frames",
                watermarks_dir=self.root / "assets" / "watermarks",
                default_watermark=self.root / "assets" / "watermarks" / "default.jpg",
                log_file=self.root / "system.log",
            )

        self.assertEqual(output_dir, config.output_dir)
        self.assertEqual(bridge_dir, config.dubvi_media_dir)
        self.assertNotIn("vmath", str(config.output_dir).lower())
        self.assertNotIn("vmath", str(config.dubvi_media_dir).lower())

        config.ensure_dirs()
        self.assertTrue(config.output_dir.is_dir())
        self.assertFalse(config.dubvi_media_dir.exists())
        config.ensure_dirs(include_dubvi=True)
        self.assertTrue(config.dubvi_media_dir.is_dir())

    def test_successful_handoff_publishes_video_and_complete_sidecar(self) -> None:
        source = self.root / "processed_clip.mp4"
        source.write_bytes(b"rendered-video")
        bridge_dir = self.root / "bridge"

        publication = publish_dubvi_handoff(source, self._metadata(source.name), bridge_dir)

        self.assertTrue(publication.video_path.is_file())
        self.assertTrue(publication.metadata_path.is_file())
        sidecar = json.loads(publication.metadata_path.read_text(encoding="utf-8"))
        self.assertTrue({
            "handoff_schema_version",
            "handoff_status",
            "channel_profile",
            "target_platform",
            "original_filename",
            "output_filename",
            "original_md5",
            "output_md5",
            "vpdq_similarity_percent",
            "vpdq_status",
            "processed_at",
        }.issubset(sidecar))
        self.assertEqual(HANDOFF_SCHEMA_VERSION, sidecar["handoff_schema_version"])
        self.assertEqual("complete", sidecar["handoff_status"])
        self.assertEqual("channel-a", sidecar["channel_profile"])
        self.assertEqual("shorts", sidecar["target_platform"])
        self.assertEqual(source.read_bytes(), publication.video_path.read_bytes())

    def test_failed_handoff_keeps_production_output_and_publishes_no_completed_pair(self) -> None:
        source = self.root / "processed_clip.mp4"
        source.write_bytes(b"rendered-video")
        bridge_dir = self.root / "bridge"

        with patch("pipeline.dubvi_handoff.shutil.copy2", side_effect=OSError("simulated copy failure")):
            with self.assertRaises(HandoffPublicationError):
                publish_dubvi_handoff(source, self._metadata(source.name), bridge_dir)

        self.assertTrue(source.is_file())
        self.assertFalse((bridge_dir / source.name).exists())
        self.assertFalse((bridge_dir / f"{source.stem}.meta.json").exists())
        self.assertEqual([], list(bridge_dir.iterdir()))


if __name__ == "__main__":
    unittest.main()
