from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import MagicMock, patch

from pipeline import cache_manager, processor
from pipeline.config import PipelineConfig
from pipeline.dubvi_handoff import HandoffPublicationError


class ProcessorCacheTests(unittest.TestCase):
    def _render_config(self, root: Path) -> PipelineConfig:
        config = PipelineConfig(
            input_dir=root / "input",
            output_dir=root / "output",
            processing_dir=root / "processing",
            failed_dir=root / "failed",
            dubvi_media_dir=root / "dubvi",
        )
        config.input_dir.mkdir()
        config.output_dir.mkdir()
        config.processing_dir.mkdir()
        config.ensure_dirs = MagicMock()
        config.enable_vpdq_scoring = False
        config.enable_trim = False
        config.enable_smart_scenes = False
        config.enable_jitter = False
        config.enable_tempo_shift = False
        return config

    def _pipeline_patches(self, builder_holder: dict, mascot_path: Path | None = None):
        profile = SimpleNamespace(
            trim_start_range=(0.8, 1.2),
            trim_end_range=(0.8, 1.2),
            speed_range=(1.1, 1.2),
            mascot_path=mascot_path,
            hooks_dir=None,
            outros_dir=None,
            bgm_dir=None,
            watermark_path=None,
            invisible_mask_opacity=0.02,
            enable_hflip=False,
        )
        platform = SimpleNamespace(name="universal", target_lufs=-14.0)

        def make_builder(**kwargs):
            builder = MagicMock()
            builder.zoom = 1.0
            builder.enable_hflip = False
            builder.output_path = kwargs["output_path"]
            builder.build_command.return_value = ["fake-ffmpeg"]
            builder_holder["builder"] = builder
            builder_holder["build_count"] = builder_holder.get("build_count", 0) + 1
            return builder

        def fake_run(*args, **kwargs):
            builder_holder["builder"].output_path.write_bytes(b"rendered-video")
            return SimpleNamespace(returncode=0, stderr="", stdout="")

        return patch.multiple(
            processor,
            get_channel_profile=MagicMock(return_value=profile),
            get_platform_preset=MagicMock(return_value=platform),
            get_video_duration=MagicMock(return_value=1.0),
            pick_random_asset=MagicMock(return_value=None),
            detect_best_encoder=MagicMock(return_value="libx264"),
            FFmpegCommandBuilder=MagicMock(side_effect=make_builder),
            subprocess=SimpleNamespace(run=fake_run, PIPE=object()),
        )

    def test_verified_cache_hit_leaves_raw_source_available(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            output_dir = root / "output"
            input_dir.mkdir()
            output_dir.mkdir()
            source = input_dir / "clip.mp4"
            output = output_dir / "processed_clip.mp4"
            source.write_bytes(b"raw")
            output.write_bytes(b"rendered")
            source_md5 = hashlib.md5(b"raw").hexdigest()
            output_md5 = hashlib.md5(b"rendered").hexdigest()
            signature = "verified-signature"
            cache_file = root / "history.json"
            cache_file.write_text(
                json.dumps(
                    {
                        source_md5: {
                            "original_md5": source_md5,
                            "output_path": str(output),
                            "output_md5": output_md5,
                            "transform_signature": signature,
                            "transform_signature_schema": "v2",
                            "processed_at": "now",
                            "vpdq_score": None,
                            "vpdq_status": None,
                        }
                    }
                ),
                encoding="utf-8",
            )
            config = PipelineConfig(input_dir=input_dir, output_dir=output_dir, processing_dir=root / "processing")
            config.enable_trim = False
            config.enable_smart_scenes = False
            config.enable_jitter = False
            config.enable_tempo_shift = False
            config.zoom_min = config.zoom_max = 1.03
            with patch.object(processor, "config", config), patch.object(cache_manager, "CACHE_FILE", cache_file), patch.object(
                processor, "_cache_transform_signature", return_value=signature
            ):
                result = processor.process_single_video(source)

            self.assertTrue(result.success)
            self.assertEqual(output, result.output_file)
            self.assertTrue(source.exists())

    def test_send_to_dubvi_bypasses_cache_and_reaches_handoff(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self._render_config(root)
            source = config.input_dir / "clip.mp4"
            source.write_bytes(b"raw")
            builder_holder = {}
            with patch.object(processor, "config", config), self._pipeline_patches(builder_holder), patch.object(
                processor,
                "is_already_processed",
                return_value={"output_path": str(config.output_dir / "cached.mp4")},
            ) as cache_lookup, patch.object(processor, "record_processed_video"), patch.object(
                processor,
                "publish_dubvi_handoff",
                side_effect=HandoffPublicationError("handoff reached from normal processing"),
            ) as publish:
                result = processor.process_single_video(source, send_to_dubvi=True)

            self.assertTrue(result.success)
            self.assertFalse(result.cache_hit)
            self.assertIn("handoff reached", result.handoff_error)
            cache_lookup.assert_not_called()
            publish.assert_called_once()

    def test_processor_rerenders_when_signature_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self._render_config(root)
            source = config.input_dir / "clip.mp4"
            source.write_bytes(b"raw")
            builder_holder = {}
            history = root / "history.json"
            with patch.object(processor, "config", config), self._pipeline_patches(builder_holder), patch.object(
                cache_manager, "CACHE_FILE", history
            ):
                first = processor.process_single_video(source)
                self.assertTrue(first.success)

                source.write_bytes(b"raw")
                config.mask_bottom_ratio = 0.10
                second = processor.process_single_video(source)

            self.assertTrue(second.success)
            self.assertFalse(second.cache_hit)
            self.assertEqual(2, builder_holder["build_count"])

    def test_processor_cache_hits_when_config_is_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self._render_config(root)
            source = config.input_dir / "clip.mp4"
            source.write_bytes(b"raw")
            builder_holder = {}
            history = root / "history.json"
            with patch.object(processor, "config", config), self._pipeline_patches(builder_holder), patch.object(
                cache_manager, "CACHE_FILE", history
            ):
                deterministic = {"custom_zoom": 1.03, "custom_tempo": 1.15}
                first = processor.process_single_video(source, **deterministic)
                self.assertTrue(first.success)

                source.write_bytes(b"raw")
                second = processor.process_single_video(source, **deterministic)

            self.assertTrue(second.success)
            self.assertTrue(second.cache_hit)
            self.assertEqual(1, builder_holder["build_count"])

    def test_processor_bypasses_cache_when_random_transform_is_unsignatured(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self._render_config(root)
            config.enable_jitter = True
            source = config.input_dir / "clip.mp4"
            source.write_bytes(b"raw")
            builder_holder = {}
            history = root / "history.json"
            with patch.object(processor, "config", config), self._pipeline_patches(builder_holder), patch.object(
                cache_manager, "CACHE_FILE", history
            ):
                first = processor.process_single_video(source, custom_zoom=1.03, custom_tempo=1.15)
                self.assertTrue(first.success)

                source.write_bytes(b"raw")
                second = processor.process_single_video(source, custom_zoom=1.03, custom_tempo=1.15)

            self.assertTrue(second.success)
            self.assertFalse(second.cache_hit)
            self.assertEqual(2, builder_holder["build_count"])

    def test_processor_rerenders_when_mascot_asset_bytes_change(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self._render_config(root)
            source = config.input_dir / "clip.mp4"
            source.write_bytes(b"raw")
            mascot = root / "mascot.png"
            mascot.write_bytes(b"mascot-v1")
            builder_holder = {}
            history = root / "history.json"
            with patch.object(processor, "config", config), self._pipeline_patches(
                builder_holder, mascot_path=mascot
            ), patch.object(cache_manager, "CACHE_FILE", history):
                first = processor.process_single_video(source)
                self.assertTrue(first.success)

                source.write_bytes(b"raw")
                mascot.write_bytes(b"mascot-v2")
                second = processor.process_single_video(source)

            self.assertTrue(second.success)
            self.assertFalse(second.cache_hit)
            self.assertEqual(2, builder_holder["build_count"])

    def test_ledger_failure_preserves_rendered_output_and_raw_input(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self._render_config(root)
            source = config.input_dir / "clip.mp4"
            source.write_bytes(b"raw")
            builder_holder = {}
            with patch.object(processor, "config", config), self._pipeline_patches(builder_holder), patch.object(
                processor,
                "record_processed_video",
                side_effect=OSError("ledger unavailable"),
            ), patch.object(processor, "publish_dubvi_handoff") as publish:
                result = processor.process_single_video(source)

            self.assertFalse(result.success)
            self.assertIn("ledger", result.error_message.lower())
            self.assertIsNotNone(result.output_file)
            self.assertTrue(result.output_file.is_file())
            self.assertEqual([b"raw"], [path.read_bytes() for path in config.failed_dir.iterdir()])
            publish.assert_not_called()

    def test_ledger_failure_move_failure_uses_cleanup_safe_raw_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self._render_config(root)
            source = config.input_dir / "clip.mp4"
            source.write_bytes(b"raw")
            config.failed_dir.mkdir()
            existing_recovery = config.failed_dir / "default_universal_clip.mp4"
            existing_recovery.write_bytes(b"existing")
            builder_holder = {}
            real_move = processor.shutil.move

            def fail_raw_preservation(source_path, destination_path):
                if Path(source_path).parent == config.processing_dir and Path(destination_path).parent == config.failed_dir:
                    raise OSError("simulated preservation move failure")
                return real_move(source_path, destination_path)

            with patch.object(processor, "config", config), patch.object(cache_manager, "config", config), self._pipeline_patches(
                builder_holder
            ), patch.object(processor, "record_processed_video", side_effect=OSError("ledger unavailable")), patch.object(
                processor.shutil, "move", side_effect=fail_raw_preservation
            ):
                result = processor.process_single_video(source)

            self.assertFalse(result.success)
            self.assertIn("ledger", result.error_message.lower())
            self.assertIn("preserved", result.error_message.lower())
            self.assertTrue(result.output_file.is_file())
            self.assertEqual(
                {b"existing", b"raw"},
                {path.read_bytes() for path in config.failed_dir.iterdir()},
            )

            with patch.object(cache_manager, "config", config):
                cache_manager.cleanup_stale_processing()
            self.assertEqual(
                {b"existing", b"raw"},
                {path.read_bytes() for path in config.failed_dir.iterdir()},
            )


if __name__ == "__main__":
    unittest.main()
