from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from pipeline.ffmpeg_builder import FFmpegCommandBuilder
from pipeline.platform_presets import PlatformPreset, get_platform_preset
from pipeline.profile_manager import ChannelProfile
from pipeline import processor
from pipeline.config import PipelineConfig


class TestFFmpegBuilderProfile(unittest.TestCase):
    def test_channel_profile_forwarding_and_ranges(self):
        """Verify that passing ChannelProfile overrides zoom, speed, jitter, and grain."""
        profile = ChannelProfile(
            name="custom_profile",
            zoom_range=(1.08, 1.10),
            speed_range=(1.25, 1.30),
            jitter_contrast_range=(1.04, 1.06),
            jitter_saturation_range=(1.02, 1.04),
            jitter_brightness_range=(0.01, 0.02),
            grain_strength=5,
        )
        builder = FFmpegCommandBuilder(
            input_path=Path("input.mp4"),
            output_path=Path("output.mp4"),
            channel_profile=profile,
            enable_tempo=True,
            enable_jitter=True,
            enable_grain=True,
        )

        self.assertGreaterEqual(builder.zoom, 1.08)
        self.assertLessEqual(builder.zoom, 1.10)

        self.assertGreaterEqual(builder.speed, 1.25)
        self.assertLessEqual(builder.speed, 1.30)

        preset = builder.platform_preset
        expected_min_c = round(1.04 * preset.contrast_bias, 3)
        expected_max_c = round(1.06 * preset.contrast_bias, 3)
        self.assertGreaterEqual(builder.contrast, expected_min_c)
        self.assertLessEqual(builder.contrast, expected_max_c)

        expected_min_s = round(1.02 * preset.saturation_bias, 3)
        expected_max_s = round(1.04 * preset.saturation_bias, 3)
        self.assertGreaterEqual(builder.saturation, expected_min_s)
        self.assertLessEqual(builder.saturation, expected_max_s)

        expected_min_b = round(0.01 + preset.brightness_bias, 3)
        expected_max_b = round(0.02 + preset.brightness_bias, 3)
        self.assertGreaterEqual(builder.brightness, expected_min_b)
        self.assertLessEqual(builder.brightness, expected_max_b)

        cmd = builder.build_command()
        cmd_str = " ".join(cmd)
        self.assertIn("noise=alls=5:allf=t+u", cmd_str)

    def test_custom_zoom_and_tempo_override_channel_profile(self):
        """Custom zoom and custom tempo must override ChannelProfile ranges."""
        profile = ChannelProfile(
            name="custom_profile",
            zoom_range=(1.08, 1.10),
            speed_range=(1.25, 1.30),
        )
        builder = FFmpegCommandBuilder(
            input_path=Path("input.mp4"),
            output_path=Path("output.mp4"),
            channel_profile=profile,
            custom_zoom=1.05,
            custom_tempo=1.12,
        )
        self.assertEqual(builder.zoom, 1.05)
        self.assertEqual(builder.speed, 1.12)

    def test_platform_preset_max_duration_clamping(self):
        """Verify max_duration_seconds clamps trim_end."""
        preset = PlatformPreset(
            name="shorts_test",
            max_duration_seconds=58.5,
        )

        # Case 1: trim_end exceeds max duration
        builder1 = FFmpegCommandBuilder(
            input_path=Path("input.mp4"),
            output_path=Path("output.mp4"),
            platform_preset=preset,
            trim_start=10.0,
            trim_end=80.0,
        )
        self.assertEqual(builder1.trim_end, 68.5)

        # Case 2: trim_end is within max duration
        builder2 = FFmpegCommandBuilder(
            input_path=Path("input.mp4"),
            output_path=Path("output.mp4"),
            platform_preset=preset,
            trim_start=10.0,
            trim_end=50.0,
        )
        self.assertEqual(builder2.trim_end, 50.0)

        # Case 3: trim_end is None
        builder3 = FFmpegCommandBuilder(
            input_path=Path("input.mp4"),
            output_path=Path("output.mp4"),
            platform_preset=preset,
            trim_start=5.0,
            trim_end=None,
        )
        self.assertEqual(builder3.trim_end, 63.5)

    def test_processor_forwards_channel_profile_and_clamps_scenes(self):
        """Verify processor passes channel_profile to builder and clamps trim_end and scene cuts safely."""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            output_dir = root / "output"
            processing_dir = root / "processing"
            input_dir.mkdir()
            output_dir.mkdir()
            processing_dir.mkdir()

            config = PipelineConfig(
                input_dir=input_dir,
                output_dir=output_dir,
                processing_dir=processing_dir,
            )
            config.enable_trim = True
            config.enable_smart_scenes = True

            source = input_dir / "clip.mp4"
            source.write_bytes(b"dummy_content")

            test_profile = ChannelProfile(
                name="test_prof",
                speed_range=(1.25, 1.30),
                zoom_range=(1.08, 1.10),
                trim_start_range=(5.0, 5.0),
                trim_end_range=(4.5, 4.5),  # On 10s video, trim_end=5.5 -> diff=0.5 < 1.0s, must reset!
            )

            builder_captured_kwargs = {}

            def fake_builder_init(*args, **kwargs):
                builder_captured_kwargs.update(kwargs)
                mock_b = MagicMock()
                mock_b.zoom = 1.0
                mock_b.enable_hflip = False
                mock_b.output_path = kwargs.get("output_path")
                mock_b.build_command.return_value = ["ffmpeg", "-version"]
                return mock_b

            fake_scenes = [(0.0, 4.0), (4.0, 8.0), (8.0, 15.0)]

            with patch.object(processor, "config", config), \
                 patch.object(processor, "get_channel_profile", return_value=test_profile), \
                 patch.object(processor, "get_video_duration", return_value=10.0), \
                 patch.object(processor, "detect_scenes", return_value=fake_scenes), \
                 patch.object(processor, "FFmpegCommandBuilder", side_effect=fake_builder_init), \
                 patch("subprocess.run") as mock_run:

                def side_effect_run(*args, **kwargs):
                    out_path = builder_captured_kwargs.get("output_path")
                    if out_path:
                        out_path.write_bytes(b"rendered")
                    res = MagicMock()
                    res.returncode = 0
                    res.stderr = ""
                    res.stdout = ""
                    return res

                mock_run.side_effect = side_effect_run

                result = processor.process_single_video(source, force_reprocess=True)
                self.assertTrue(result.success)

                # Verify channel_profile was forwarded
                self.assertIn("channel_profile", builder_captured_kwargs)
                self.assertEqual(builder_captured_kwargs["channel_profile"], test_profile)

                # Verify trim_start and trim_end were reset because diff was 0.5 < 1.0s
                self.assertEqual(builder_captured_kwargs["trim_start"], 0.0)
                self.assertEqual(builder_captured_kwargs["trim_end"], 10.0)

                # Verify scene_schedule cuts did not exceed 10.0s
                schedule = builder_captured_kwargs.get("scene_schedule")
                self.assertIsNotNone(schedule)
                for s_start, s_end, _ in schedule:
                    self.assertGreaterEqual(s_start, 0.0)
                    self.assertLessEqual(s_end, 10.0)
                    self.assertGreater(s_end, s_start)


if __name__ == "__main__":
    unittest.main()
