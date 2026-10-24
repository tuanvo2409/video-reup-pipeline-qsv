from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
import tempfile
import threading
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from pipeline import cache_manager
from pipeline.config import PipelineConfig
from pipeline.p1c_status import publish_accepted_status, publish_failed_status, publish_started_status


def _record_from_process(cache_file: str, output_path: str, original_md5: str, barrier: object) -> None:
    cache_manager.CACHE_FILE = Path(cache_file)
    barrier.wait(timeout=10)
    cache_manager.record_processed_video(
        original_md5,
        "source.mp4",
        Path(output_path),
        "out",
        1.0,
    )


def _acquire_ledger_lock(cache_file: str, result_queue: object) -> None:
    cache_manager.CACHE_FILE = Path(cache_file)
    try:
        with cache_manager._ledger_lock():
            result_queue.put("acquired")
    except Exception as error:
        result_queue.put(f"error: {error}")


def _hold_watcher_instance_lock(lock_path: str, ready_event: object, release_event: object) -> None:
    path = Path(lock_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as lock_file:
        if os.name == "nt":
            import msvcrt

            lock_file.seek(0, os.SEEK_END)
            if lock_file.tell() == 0:
                lock_file.write(b"0")
                lock_file.flush()
            lock_file.seek(0)
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)
            try:
                ready_event.set()
                release_event.wait(timeout=10)
            finally:
                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                ready_event.set()
                release_event.wait(timeout=10)
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _crash_while_holding_ledger_lock(cache_file: str, ready_event: object) -> None:
    cache_manager.CACHE_FILE = Path(cache_file)
    with cache_manager._ledger_lock():
        ready_event.set()
        os._exit(0)


class CacheManagerTests(unittest.TestCase):
    def _write_cache(self, cache_file: Path, entry: dict) -> None:
        cache_file.write_text(json.dumps(entry), encoding="utf-8")

    def test_cache_hit_requires_existing_regular_output_and_matching_hash(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "source.mp4"
            output = root / "output" / "render.mp4"
            output.parent.mkdir()
            source.write_bytes(b"raw")
            output.write_bytes(b"rendered")
            source_md5 = hashlib.md5(b"raw").hexdigest()
            output_md5 = hashlib.md5(b"rendered").hexdigest()
            entry = {
                "original_md5": source_md5,
                "output_path": str(output),
                "output_md5": output_md5,
                "transform_signature": "sig",
                "transform_signature_schema": "v2",
            }
            cache_file = root / "history.json"
            self._write_cache(cache_file, {source_md5: entry})

            with patch.object(cache_manager, "CACHE_FILE", cache_file):
                self.assertEqual(
                    entry,
                    cache_manager.is_already_processed(
                        source,
                        transform_signature="sig",
                        output_root=output.parent,
                    ),
                )
                output.write_bytes(b"corrupt")
                self.assertIsNone(
                    cache_manager.is_already_processed(
                        source,
                        transform_signature="sig",
                        output_root=output.parent,
                    )
                )
                output.unlink()
                self.assertIsNone(
                    cache_manager.is_already_processed(
                        source,
                        transform_signature="sig",
                        output_root=output.parent,
                    )
                )

    def test_legacy_or_wrong_scope_entry_is_a_miss(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "source.mp4"
            output = root / "render.mp4"
            source.write_bytes(b"raw")
            output.write_bytes(b"rendered")
            source_md5 = hashlib.md5(b"raw").hexdigest()
            output_md5 = hashlib.md5(b"rendered").hexdigest()
            cache_file = root / "history.json"
            self._write_cache(
                cache_file,
                {
                    source_md5: {
                        "original_md5": source_md5,
                        "output_path": str(output),
                        "output_md5": output_md5,
                    }
                },
            )
            with patch.object(cache_manager, "CACHE_FILE", cache_file):
                self.assertIsNone(
                    cache_manager.is_already_processed(
                        source,
                        transform_signature="profile-a/platform-a",
                        output_root=root,
                    )
                )

    def test_signature_changes_for_profile_or_platform(self) -> None:
        config = type("Config", (), {
            "layout_mode": "crop_fill",
            "enable_trim": True,
            "enable_smart_scenes": True,
            "enable_invisible_mask": True,
            "enable_hflip": False,
            "enable_mask": False,
            "enable_frame": False,
            "enable_grain": True,
            "enable_jitter": True,
            "enable_tempo_shift": True,
            "invisible_mask_opacity": 0.02,
            "target_width": 1080,
            "target_height": 1920,
            "target_fps": 30,
            "preferred_encoders": ["libx264"],
            "mask_bottom_ratio": 0.05,
            "grain_strength": 2,
            "audio_sample_rate": 44100,
            "audio_channels": 2,
            "audio_bitrate": "192k",
        })()
        signature_a = cache_manager.make_transform_signature("page-a", "tiktok", {}, config)
        signature_b = cache_manager.make_transform_signature("page-b", "tiktok", {}, config)
        signature_c = cache_manager.make_transform_signature("page-a", "facebook", {}, config)
        self.assertNotEqual(signature_a, signature_b)
        self.assertNotEqual(signature_a, signature_c)

    def test_cache_signature_changes_for_mask_grain_audio_encoder(self) -> None:
        config = type("Config", (), {
            "layout_mode": "crop_fill",
            "enable_trim": True,
            "enable_smart_scenes": True,
            "enable_invisible_mask": True,
            "enable_hflip": False,
            "enable_mask": True,
            "enable_frame": False,
            "enable_grain": True,
            "enable_jitter": True,
            "enable_tempo_shift": True,
            "invisible_mask_opacity": 0.02,
            "target_width": 1080,
            "target_height": 1920,
            "target_fps": 30,
            "preferred_encoders": ["libx264"],
            "mask_bottom_ratio": 0.05,
            "grain_strength": 2,
            "audio_sample_rate": 44100,
            "audio_channels": 2,
            "audio_bitrate": "192k",
        })()
        base_options = {"override_encoder": "libx264"}
        baseline = cache_manager.make_transform_signature("page-a", "tiktok", base_options, config)

        config.mask_bottom_ratio = 0.10
        self.assertNotEqual(
            baseline,
            cache_manager.make_transform_signature("page-a", "tiktok", base_options, config),
        )
        config.mask_bottom_ratio = 0.05

        config.grain_strength = 4
        self.assertNotEqual(
            baseline,
            cache_manager.make_transform_signature("page-a", "tiktok", base_options, config),
        )
        config.grain_strength = 2

        config.audio_bitrate = "256k"
        self.assertNotEqual(
            baseline,
            cache_manager.make_transform_signature("page-a", "tiktok", base_options, config),
        )
        config.audio_bitrate = "192k"

        changed_encoder = {"override_encoder": "h264_qsv"}
        self.assertNotEqual(
            baseline,
            cache_manager.make_transform_signature("page-a", "tiktok", changed_encoder, config),
        )

    def test_changed_mascot_asset_bytes_invalidates_cache(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            mascot = root / "mascot.png"
            mascot.write_bytes(b"mascot-v1")
            config = type("Config", (), {
                "layout_mode": "crop_fill",
                "enable_trim": True,
                "enable_smart_scenes": True,
                "enable_invisible_mask": True,
                "enable_hflip": False,
                "enable_mask": False,
                "enable_frame": False,
                "enable_grain": True,
                "enable_jitter": True,
                "enable_tempo_shift": True,
                "invisible_mask_opacity": 0.02,
                "target_width": 1080,
                "target_height": 1920,
                "target_fps": 30,
                "preferred_encoders": ["libx264"],
                "mask_bottom_ratio": 0.05,
                "grain_strength": 2,
                "audio_sample_rate": 44100,
                "audio_channels": 2,
                "audio_bitrate": "192k",
            })()
            options = {"mascot_path": mascot}
            first = cache_manager.make_transform_signature("page-a", "tiktok", options, config)
            mascot.write_bytes(b"mascot-v2")
            second = cache_manager.make_transform_signature("page-a", "tiktok", options, config)
            self.assertNotEqual(first, second)

    def test_old_signature_entry_is_a_cache_miss(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "source.mp4"
            output = root / "render.mp4"
            source.write_bytes(b"raw")
            output.write_bytes(b"rendered")
            source_md5 = hashlib.md5(b"raw").hexdigest()
            output_md5 = hashlib.md5(b"rendered").hexdigest()
            cache_file = root / "history.json"
            self._write_cache(
                cache_file,
                {source_md5: {
                    "original_md5": source_md5,
                    "output_path": str(output),
                    "output_md5": output_md5,
                    "transform_signature": "legacy-signature",
                }},
            )
            with patch.object(cache_manager, "CACHE_FILE", cache_file):
                self.assertIsNone(
                    cache_manager.is_already_processed(
                        source,
                        transform_signature="legacy-signature",
                        output_root=root,
                    )
                )

    def test_concurrent_records_preserve_distinct_keys_and_increment_runs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            cache_file = root / "history.json"
            output = root / "render.mp4"
            output.write_bytes(b"rendered")
            barrier = threading.Barrier(2)

            def coordinated_load() -> dict:
                snapshot = json.loads(cache_file.read_text(encoding="utf-8")) if cache_file.exists() else {}
                barrier.wait()
                return snapshot

            def coordinated_save(cache: dict) -> None:
                cache_file.write_text(json.dumps(cache), encoding="utf-8")

            def record(md5: str) -> None:
                cache_manager.record_processed_video(md5, "source.mp4", output, "out", 1.0)

            with patch.object(cache_manager, "CACHE_FILE", cache_file), patch.object(
                cache_manager, "load_cache", side_effect=coordinated_load
            ), patch.object(cache_manager, "save_cache", side_effect=coordinated_save):
                threads = [threading.Thread(target=record, args=(md5,)) for md5 in ("one", "two")]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()

                cache = json.loads(cache_file.read_text(encoding="utf-8"))
                self.assertEqual({"one", "two"}, set(cache))

            with patch.object(cache_manager, "CACHE_FILE", cache_file):
                cache_manager.record_processed_video("one", "source.mp4", output, "out", 1.0)
                cache = json.loads(cache_file.read_text(encoding="utf-8"))
                self.assertEqual(2, cache["one"]["runs_count"])

    def test_record_failure_keeps_previous_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            cache_file = root / "history.json"
            previous = {"old": {"runs_count": 1}}
            cache_file.write_text(json.dumps(previous), encoding="utf-8")
            previous_bytes = cache_file.read_bytes()
            output = root / "render.mp4"
            output.write_bytes(b"rendered")
            with patch.object(cache_manager, "CACHE_FILE", cache_file), patch.object(
                cache_manager.os, "replace", side_effect=OSError("disk full")
            ):
                with self.assertRaises(OSError):
                    cache_manager.record_processed_video("new", "source.mp4", output, "out", 1.0)
            self.assertEqual(previous_bytes, cache_file.read_bytes())
            self.assertEqual(previous, json.loads(cache_file.read_text(encoding="utf-8")))

    def test_independent_process_records_preserve_distinct_keys_and_same_key_runs(self) -> None:
        context = multiprocessing.get_context("spawn")
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            cache_file = root / "history.json"
            output = root / "render.mp4"
            output.write_bytes(b"rendered")

            for original_md5s, expected_keys, expected_runs in [
                (("one", "two"), {"one", "two"}, {"one": 1, "two": 1}),
                (("same", "same"), {"same"}, {"same": 2}),
            ]:
                cache_file.unlink(missing_ok=True)
                barrier = context.Barrier(2)
                processes = [context.Process(
                    target=_record_from_process,
                    args=(str(cache_file), str(output), original_md5, barrier),
                ) for original_md5 in original_md5s]
                try:
                    for process in processes:
                        process.start()
                    for process in processes:
                        process.join(timeout=15)
                    for process in processes:
                        if process.is_alive():
                            process.terminate()
                            process.join(timeout=5)
                        self.assertEqual(0, process.exitcode)
                finally:
                    for process in processes:
                        if process.is_alive():
                            process.terminate()
                            process.join(timeout=5)

                cache = json.loads(cache_file.read_text(encoding="utf-8"))
                self.assertEqual(expected_keys, set(cache))
                self.assertEqual(expected_runs, {key: cache[key]["runs_count"] for key in expected_keys})

    def test_child_crash_releases_ledger_lock_for_next_process(self) -> None:
        context = multiprocessing.get_context("spawn")
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_file = Path(temp_dir) / "history.json"
            ready_event = context.Event()
            crashed = context.Process(
                target=_crash_while_holding_ledger_lock,
                args=(str(cache_file), ready_event),
            )
            try:
                crashed.start()
                self.assertTrue(ready_event.wait(timeout=10))
                crashed.join(timeout=10)
                self.assertFalse(crashed.is_alive())

                result_queue = context.Queue()
                successor = context.Process(
                    target=_acquire_ledger_lock,
                    args=(str(cache_file), result_queue),
                )
                try:
                    successor.start()
                    successor.join(timeout=10)
                    self.assertFalse(successor.is_alive())
                    self.assertEqual(0, successor.exitcode)
                    self.assertEqual("acquired", result_queue.get(timeout=2))
                finally:
                    if successor.is_alive():
                        successor.terminate()
                        successor.join(timeout=5)
            finally:
                if crashed.is_alive():
                    crashed.terminate()
                    crashed.join(timeout=5)

    def test_second_watcher_cli_fails_fast_while_instance_lock_is_owned(self) -> None:
        context = multiprocessing.get_context("spawn")
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = PipelineConfig(
                input_dir=root / "input",
                processing_dir=root / "processing",
                failed_dir=root / "failed",
            )
            config.input_dir.mkdir()
            config.processing_dir.mkdir()
            config.failed_dir.mkdir()
            lock_path = config.processing_dir.parent / "watcher.lock"
            ready_event = context.Event()
            release_event = context.Event()
            owner = context.Process(
                target=_hold_watcher_instance_lock,
                args=(str(lock_path), ready_event, release_event),
            )
            owner.start()
            try:
                self.assertTrue(ready_event.wait(timeout=10))

                import watcher

                with patch.object(watcher, "config", config), patch.object(
                    watcher.sys, "argv", ["watcher.py", "--batch"]
                ), patch.object(watcher, "run_batch") as run_batch:
                    watcher_thread = threading.Thread(target=watcher.main)
                    watcher_thread.start()
                    watcher_thread.join(timeout=1)
                    self.assertFalse(watcher_thread.is_alive())
                    run_batch.assert_not_called()

                    release_event.set()
                    owner.join(timeout=10)
                    self.assertFalse(owner.is_alive())
                    watcher.main()
                    run_batch.assert_called_once()
            finally:
                release_event.set()
                owner.join(timeout=10)
                if owner.is_alive():
                    owner.terminate()
                    owner.join(timeout=5)
            self.assertEqual(0, owner.exitcode)

    def test_stale_tmp_files_are_quarantined_instead_of_deleted(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = PipelineConfig(
                input_dir=root / "input",
                processing_dir=root / "processing",
                failed_dir=root / "failed",
            )
            config.input_dir.mkdir()
            config.processing_dir.mkdir()
            stale_tmp = config.processing_dir / "tmp_raw.mp4"
            stale_tmp.write_bytes(b"raw")

            with patch.object(cache_manager, "config", config):
                self.assertEqual(1, cache_manager.cleanup_stale_processing())

            self.assertFalse(stale_tmp.exists())
            self.assertEqual(b"raw", (config.failed_dir / stale_tmp.name).read_bytes())

    def test_active_legacy_processing_files_are_not_moved_during_startup_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = PipelineConfig(
                input_dir=root / "input",
                processing_dir=root / "processing",
                failed_dir=root / "failed",
            )
            config.input_dir.mkdir()
            config.processing_dir.mkdir()
            active_input = config.processing_dir / "legacy_input.mp4"
            active_tmp = config.processing_dir / "tmp_legacy_output.mp4"
            active_input.write_bytes(b"live-input")
            active_tmp.write_bytes(b"live-output")

            processor_ready = threading.Event()
            release_processor = threading.Event()
            processor_errors: list[BaseException] = []

            def hold_legacy_files_open() -> None:
                try:
                    with active_input.open("rb"), active_tmp.open("rb"):
                        processor_ready.set()
                        if not release_processor.wait(timeout=5):
                            raise TimeoutError("test processor was not released")
                except BaseException as error:
                    processor_errors.append(error)

            processor = threading.Thread(target=hold_legacy_files_open)
            processor.start()
            self.assertTrue(processor_ready.wait(timeout=5))
            try:
                with patch.object(cache_manager, "config", config):
                    self.assertEqual(0, cache_manager.cleanup_stale_processing())

                self.assertTrue(active_input.is_file())
                self.assertTrue(active_tmp.is_file())
                self.assertEqual(b"live-input", active_input.read_bytes())
                self.assertEqual(b"live-output", active_tmp.read_bytes())
                self.assertFalse((config.input_dir / active_input.name).exists())
                self.assertFalse((config.failed_dir / active_tmp.name).exists())
            finally:
                release_processor.set()
                processor.join(timeout=5)

            self.assertFalse(processor.is_alive())
            self.assertEqual([], processor_errors)

    def test_stale_non_tmp_files_use_collision_safe_input_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = PipelineConfig(
                input_dir=root / "input",
                processing_dir=root / "processing",
                failed_dir=root / "failed",
            )
            config.input_dir.mkdir()
            config.processing_dir.mkdir()
            existing = config.input_dir / "clip.mp4"
            existing.write_bytes(b"existing")
            interrupted = config.processing_dir / existing.name
            interrupted.write_bytes(b"interrupted")

            with patch.object(cache_manager, "config", config):
                self.assertEqual(1, cache_manager.cleanup_stale_processing())

            self.assertEqual(b"existing", existing.read_bytes())
            self.assertEqual(b"interrupted", (config.input_dir / "clip.recovery1.mp4").read_bytes())

    def test_cleanup_keeps_active_attempt_and_handles_proven_orphan(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            processing = root / "processing"
            input_dir = root / "input"
            failed_dir = root / "failed"
            status_root = root / "status"
            processing.mkdir()
            input_dir.mkdir()

            def make_job() -> dict[str, object]:
                return {
                    "reup_job_id": str(uuid.uuid4()),
                    "dispatch_id": str(uuid.uuid4()),
                    "attempt_number": 1,
                }

            active_job = make_job()
            orphan_job = make_job()
            active_source = processing / "p1c-attempts" / active_job["reup_job_id"] / "source-active.mp4"
            orphan_source = processing / "p1c-attempts" / orphan_job["reup_job_id"] / "source-orphan.mp4"
            active_source.parent.mkdir(parents=True)
            orphan_source.parent.mkdir(parents=True)
            active_source.write_bytes(b"active")
            orphan_source.write_bytes(b"orphan")
            publish_accepted_status(status_root, active_job)
            publish_started_status(status_root, active_job, str(uuid.uuid4()))
            publish_accepted_status(status_root, orphan_job)
            publish_started_status(status_root, orphan_job, str(uuid.uuid4()))
            publish_failed_status(status_root, orphan_job, "PROCESSING_FAILED", "bounded failure")
            config = PipelineConfig(
                input_dir=input_dir,
                processing_dir=processing,
                failed_dir=failed_dir,
                engine_status_dir=status_root,
            )

            with patch.object(cache_manager, "config", config):
                cache_manager.cleanup_stale_processing()

            self.assertTrue(active_source.exists())
            self.assertFalse(orphan_source.exists())
            recovered = list(input_dir.rglob("source-orphan.mp4")) + list(failed_dir.rglob("source-orphan.mp4"))
            self.assertEqual([b"orphan"], [path.read_bytes() for path in recovered])


if __name__ == "__main__":
    unittest.main()
