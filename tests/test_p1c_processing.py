"""CP5 processing adapter, lifecycle, recovery, and child-boundary tests."""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import threading
import unittest
import uuid
from unittest import mock

from pipeline import cache_manager
from pipeline import p1c_processing
from pipeline import processor as legacy_processor
from pipeline.config import config
from pipeline.config import PipelineConfig
from pipeline.dubvi_engine_contract import canonical_json_bytes
from pipeline.p1c_intake import AcceptedReupJob
from pipeline.p1c_output import OutputPublicationError, publish_reup_output
from pipeline.p1c_status import publish_accepted_status, publish_started_status
from pipeline.platform_presets import get_platform_preset


def _job(source: Path) -> dict[str, object]:
    return {
        "candidate_id": str(uuid.uuid4()), "schedule_id": str(uuid.uuid4()),
        "dispatch_id": str(uuid.uuid4()), "reup_job_id": str(uuid.uuid4()),
        "channel_id": str(uuid.uuid4()), "channel_slug": "home-vi",
        "reup_profile": "home", "target_platform": "tiktok",
        "media_name": source.name, "media_size": source.stat().st_size,
        "source_fingerprint": "sha256:" + hashlib.sha256(source.read_bytes()).hexdigest(),
        "source_provenance_ref": "candidate:placeholder", "created_at_utc": "2026-09-10T10:00:00.000Z",
        "attempt_number": 1, "localization_profile": "vi-default", "policy_profile": "default",
    }


class P1CProcessingTests(unittest.TestCase):
    def test_facebook_reels_contract_uses_uncapped_facebook_preset(self) -> None:
        preset = get_platform_preset("facebook-reels")
        self.assertEqual("facebook", preset.name)
        self.assertIsNone(preset.max_duration_seconds)

    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.input_root = self.root / "input" / "home" / "tiktok"
        self.input_root.mkdir(parents=True)
        self.source = self.input_root / "dubvi-source.mp4"
        self.source.write_bytes(b"stable-source")
        self.job = _job(self.source)
        self.job["source_provenance_ref"] = f"candidate:{self.job['candidate_id']}"
        self.envelope = self.input_root / f"dubvi-reup-job-{self.job['reup_job_id']}.job.json"
        self.envelope.write_bytes(canonical_json_bytes({
            "contract_version": 1, "message_kind": "control_plane_to_reup_job", "envelope_status": "complete",
            "dispatch_id": self.job["dispatch_id"], "reup_job_id": self.job["reup_job_id"],
            "correlation_id": self.job["dispatch_id"], "candidate_id": self.job["candidate_id"],
            "schedule_id": self.job["schedule_id"], "channel_id": self.job["channel_id"],
            "channel_slug": self.job["channel_slug"], "reup_profile": self.job["reup_profile"],
            "target_platform": self.job["target_platform"], "media_name": self.source.name,
            "media_size": self.job["media_size"], "source_fingerprint": self.job["source_fingerprint"],
            "source_provenance_ref": self.job["source_provenance_ref"], "created_at_utc": self.job["created_at_utc"],
            "attempt_number": 1, "localization_profile": "vi-default", "policy_profile": "default",
        }))
        self.status_root = self.root / "status"
        publish_accepted_status(self.status_root, self.job, occurred_at_utc="2026-09-10T10:01:00.000Z")
        self.lease_id = str(uuid.uuid4())
        publish_started_status(self.status_root, self.job, self.lease_id, occurred_at_utc="2026-09-10T10:02:00.000Z")
        self.accepted = AcceptedReupJob(self.envelope, self.source, self.status_root / "reup" / self.job["reup_job_id"] / "event-000001.json", self.job)
        self.output_root = self.root / "output"
        self.output_root.mkdir()
        self.media_root = self.root / "media"
        self.processing_root = self.root / "processing"

    def _prepare_output(self, payload: bytes = b"processed"):
        candidate = self.output_root / "legacy-output.mp4"
        candidate.write_bytes(payload)
        return publish_reup_output(candidate, self.job, self.output_root)

    def _complete_successful_attempt(self):
        calls = 0

        def processor(path: Path, **kwargs: object) -> object:
            nonlocal calls
            calls += 1
            candidate = self.output_root / "legacy-output.mp4"
            candidate.write_bytes(b"processed")
            return SimpleNamespace(success=True, output_file=str(candidate))

        with mock.patch.object(config, "processing_dir", self.processing_root):
            result = p1c_processing.process_reup_attempt(
                self.accepted, self.status_root, processor=processor,
                output_root=self.output_root, media_root=self.media_root,
            )
        self.assertEqual("succeeded", result.outcome)
        event_dir = self.status_root / "reup" / self.job["reup_job_id"]
        output_path = self.output_root / "p1c" / self.job["reup_job_id"] / "output.mp4"
        handoff_dir = self.media_root / self.job["reup_profile"]
        return calls, event_dir, output_path, handoff_dir

    def test_processing_uses_attempt_copy_exact_legacy_call_and_publishes_1_to_5(self) -> None:
        calls: list[tuple[Path, dict[str, object]]] = []

        def processor(path: Path, **kwargs: object) -> object:
            calls.append((Path(path), kwargs))
            candidate = self.output_root / "legacy-output.mp4"
            candidate.write_bytes(Path(path).read_bytes() + b"-processed")
            return SimpleNamespace(success=True, output_file=str(candidate))

        with mock.patch.object(config, "processing_dir", self.processing_root):
            result = p1c_processing.process_reup_attempt(
                self.accepted, self.status_root, processor=processor,
                output_root=self.output_root, media_root=self.media_root,
                now_utc="2026-09-10T10:03:00.000Z",
            )
        self.assertEqual("succeeded", result.outcome)
        self.assertEqual(b"stable-source", self.source.read_bytes())
        self.assertEqual(1, len(calls))
        scratch, kwargs = calls[0]
        self.assertTrue(str(scratch).startswith(str(self.processing_root / "p1c-attempts" / self.job["reup_job_id"])))
        self.assertEqual({
            "override_profile": "home", "target_platform": "tiktok",
            "send_to_dubvi": False, "force_reprocess": True,
            "enable_trim": False, "enable_tempo": False,
            "enable_smart_scenes": False, "custom_tempo": 1.0,
            "output_root": self.output_root,
            "working_dir": self.processing_root / "p1c-attempts" / self.job["reup_job_id"],
        }, kwargs)
        events = [json.loads((self.status_root / "reup" / self.job["reup_job_id"] / f"event-{number:06d}.json").read_text(encoding="utf-8")) for number in range(1, 6)]
        self.assertEqual(["accepted", "started", "output_published", "handoff_published", "succeeded"], [event["event_kind"] for event in events])
        self.assertFalse(scratch.exists())

    def test_paused_attempt_files_are_private_from_stale_processing_cleanup(self) -> None:
        processor_started = threading.Event()
        release_processor = threading.Event()
        processor_calls: list[Path] = []
        result_holder: list[object] = []

        def processor(path: Path, **kwargs: object) -> object:
            processor_calls.append(path)
            live_tmp = path.parent / "tmp_live_output.mp4"
            live_tmp.write_bytes(b"live-output")
            self.assertEqual(path.parent, kwargs["working_dir"])
            processor_started.set()
            if not release_processor.wait(timeout=5):
                raise TimeoutError("test processor was not released")
            candidate = self.output_root / "legacy-output.mp4"
            candidate.write_bytes(b"processed")
            return SimpleNamespace(success=True, output_file=str(candidate))

        def run_attempt() -> None:
            result_holder.append(p1c_processing.process_reup_attempt(
                self.accepted,
                self.status_root,
                processor=processor,
                output_root=self.output_root,
                media_root=self.media_root,
            ))

        with mock.patch.object(config, "processing_dir", self.processing_root):
            worker = threading.Thread(target=run_attempt)
            worker.start()
            self.assertTrue(processor_started.wait(timeout=5))
            scratch = processor_calls[0]
            live_tmp = scratch.parent / "tmp_live_output.mp4"
            self.assertEqual(self.processing_root / "p1c-attempts" / self.job["reup_job_id"], scratch.parent)
            self.assertEqual(b"stable-source", scratch.read_bytes())
            self.assertEqual(b"live-output", live_tmp.read_bytes())

            with mock.patch.object(cache_manager, "config", config):
                self.assertEqual(0, cache_manager.cleanup_stale_processing())

            self.assertTrue(scratch.is_file())
            self.assertTrue(live_tmp.is_file())
            self.assertEqual(b"stable-source", scratch.read_bytes())
            self.assertEqual(b"live-output", live_tmp.read_bytes())
            release_processor.set()
            worker.join(timeout=5)

        self.assertFalse(worker.is_alive())
        self.assertEqual("succeeded", result_holder[0].outcome)

    def test_processor_failure_is_terminal_and_diagnostic_is_bounded(self) -> None:
        def processor(*args: object, **kwargs: object) -> object:
            return SimpleNamespace(success=False, error_message="x" * 10000)

        with mock.patch.object(config, "processing_dir", self.processing_root):
            result = p1c_processing.process_reup_attempt(self.accepted, self.status_root, processor=processor, output_root=self.output_root, media_root=self.media_root)
        self.assertEqual("failed", result.outcome)
        event = json.loads((self.status_root / "reup" / self.job["reup_job_id"] / "event-000003.json").read_text(encoding="utf-8"))
        self.assertEqual("PROCESSING_FAILED", event["error_classification"])
        self.assertLessEqual(len(event["diagnostic_summary"]), 2048)

    def test_source_integrity_failure_is_durable_and_never_starts_processor(self) -> None:
        self.source.write_bytes(b"tampered-source")
        processor = mock.Mock()
        with mock.patch.object(config, "processing_dir", self.processing_root):
            result = p1c_processing.process_reup_attempt(self.accepted, self.status_root, processor=processor, output_root=self.output_root, media_root=self.media_root)
        self.assertEqual("failed", result.outcome)
        processor.assert_not_called()
        event = json.loads((self.status_root / "reup" / self.job["reup_job_id"] / "event-000003.json").read_text(encoding="utf-8"))
        self.assertEqual("PROCESSING_FAILED", event["error_classification"])

    def test_real_processor_boundary_honors_output_root_override(self) -> None:
        override_root = self.root / "alternate-output"
        seen: dict[str, object] = {}

        def processor(path: Path, **kwargs: object) -> object:
            seen.update(kwargs)
            candidate = override_root / "legacy-output.mp4"
            candidate.parent.mkdir(parents=True, exist_ok=True)
            candidate.write_bytes(b"processed")
            return SimpleNamespace(success=True, output_file=str(candidate))

        with mock.patch.object(config, "processing_dir", self.processing_root):
            result = p1c_processing.process_reup_attempt(
                self.accepted, self.status_root, processor=processor,
                output_root=override_root, media_root=self.media_root,
            )

        self.assertEqual("succeeded", result.outcome)
        self.assertEqual(override_root, seen["output_root"])
        self.assertTrue((override_root / "p1c" / self.job["reup_job_id"] / "output.mp4").is_file())

    def test_final_basename_collision_preserves_existing_bytes(self) -> None:
        first = self._prepare_output(payload=b"first-output")
        candidate = self.output_root / "replacement.mp4"
        candidate.write_bytes(b"different-output")

        with self.assertRaisesRegex(OutputPublicationError, "FINAL_PATH_CONFLICT"):
            publish_reup_output(candidate, self.job, self.output_root)

        self.assertEqual(b"first-output", first.final_path.read_bytes())

    def test_failed_basename_collision_preserves_existing_bytes(self) -> None:
        runtime = PipelineConfig(
            input_dir=self.root / "legacy-input",
            output_dir=self.root / "legacy-output",
            processing_dir=self.root / "legacy-processing",
            failed_dir=self.root / "legacy-failed",
        )
        runtime.input_dir.mkdir()
        runtime.output_dir.mkdir()
        runtime.processing_dir.mkdir()
        runtime.failed_dir.mkdir()
        runtime.ensure_dirs = mock.Mock()
        source = runtime.input_dir / "clip.mp4"
        source.write_bytes(b"raw")
        collision = runtime.failed_dir / "default_universal_clip.mp4"
        collision.write_bytes(b"original-failed-bytes")
        profile = SimpleNamespace(
            mascot_path=None, hooks_dir=None, outros_dir=None, bgm_dir=None,
            watermark_path=None, invisible_mask_opacity=0.02, enable_hflip=False,
            trim_start_range=(0.8, 1.2), trim_end_range=(0.8, 1.2), speed_range=(1.1, 1.2),
        )
        platform = SimpleNamespace(name="universal", target_lufs=-14.0)

        with mock.patch.object(legacy_processor, "config", runtime), mock.patch.object(
            legacy_processor, "get_channel_profile", return_value=profile
        ), mock.patch.object(legacy_processor, "get_platform_preset", return_value=platform), mock.patch.object(
            legacy_processor, "pick_random_asset", return_value=None
        ), mock.patch.object(legacy_processor, "detect_best_encoder", return_value="libx264"), mock.patch.object(
            legacy_processor, "get_video_duration", return_value=1.0
        ), mock.patch.object(legacy_processor, "FFmpegCommandBuilder") as builder_type, mock.patch.object(
            legacy_processor, "subprocess", SimpleNamespace(
                run=lambda *args, **kwargs: SimpleNamespace(returncode=1, stderr="render failed", stdout=""),
                PIPE=object(),
            )
        ):
            builder_type.return_value.zoom = 1.03
            builder_type.return_value.enable_hflip = False
            builder_type.return_value.build_command.return_value = ["fake-ffmpeg"]
            result = legacy_processor.process_single_video(
                source,
                enable_trim=False,
                enable_smart_scenes=False,
                enable_jitter=False,
                enable_tempo=False,
                custom_zoom=1.03,
                custom_tempo=1.15,
                force_reprocess=True,
            )

        self.assertFalse(result.success)
        self.assertEqual(b"original-failed-bytes", collision.read_bytes())
        self.assertGreaterEqual(len(list(runtime.failed_dir.glob("*.mp4"))), 2)

    def test_copy_mutation_after_source_check_refuses_processing(self) -> None:
        real_copyfile = shutil.copyfile

        def mutate_scratch(source: Path, scratch: Path) -> Path:
            result = real_copyfile(source, scratch)
            scratch.write_bytes(b"mutated-after-copy")
            return result

        processor = mock.Mock()
        with mock.patch.object(config, "processing_dir", self.processing_root), mock.patch.object(
            p1c_processing.shutil, "copyfile", side_effect=mutate_scratch
        ):
            result = p1c_processing.process_reup_attempt(
                self.accepted, self.status_root, processor=processor,
                output_root=self.output_root, media_root=self.media_root,
            )

        self.assertEqual("failed", result.outcome)
        processor.assert_not_called()
        self.assertEqual(b"stable-source", self.source.read_bytes())
        event = json.loads((self.status_root / "reup" / self.job["reup_job_id"] / "event-000003.json").read_text(encoding="utf-8"))
        self.assertEqual("PROCESSING_FAILED", event["error_classification"])
        self.assertLessEqual(len(event["diagnostic_summary"]), 2048)

    def test_output_oserror_publishes_bounded_failure(self) -> None:
        def processor(path: Path, **kwargs: object) -> object:
            candidate = self.output_root / "legacy-output.mp4"
            candidate.write_bytes(b"processed")
            return SimpleNamespace(success=True, output_file=str(candidate))

        with mock.patch.object(config, "processing_dir", self.processing_root), mock.patch.object(
            p1c_processing, "publish_reup_output", side_effect=OSError("output disk failure")
        ):
            result = p1c_processing.process_reup_attempt(
                self.accepted, self.status_root, processor=processor,
                output_root=self.output_root, media_root=self.media_root,
            )

        self.assertEqual("failed", result.outcome)
        event = json.loads((self.status_root / "reup" / self.job["reup_job_id"] / "event-000003.json").read_text(encoding="utf-8"))
        self.assertEqual("failed", event["event_kind"])
        self.assertLessEqual(len(event["diagnostic_summary"]), 2048)

    def test_handoff_oserror_leaves_recoverable_evidence(self) -> None:
        def processor(path: Path, **kwargs: object) -> object:
            candidate = self.output_root / "legacy-output.mp4"
            candidate.write_bytes(b"processed")
            return SimpleNamespace(success=True, output_file=str(candidate))

        with mock.patch.object(config, "processing_dir", self.processing_root), mock.patch.object(
            p1c_processing, "publish_reup_handoff", side_effect=OSError("handoff disk failure")
        ):
            result = p1c_processing.process_reup_attempt(
                self.accepted, self.status_root, processor=processor,
                output_root=self.output_root, media_root=self.media_root,
            )

        self.assertEqual("handoff_pending", result.outcome)
        self.assertIsNotNone(result.output)
        assert result.output is not None
        self.assertTrue(result.output.final_path.is_file())
        event = json.loads((self.status_root / "reup" / self.job["reup_job_id"] / "event-000003.json").read_text(encoding="utf-8"))
        self.assertEqual("output_published", event["event_kind"])

    def test_missing_event3_recovers_exact_stage_and_final_without_rerendering(self) -> None:
        prepared = self._prepare_output()
        processor = mock.Mock()
        result = p1c_processing.recover_reup_completion(
            self.job, self.status_root, output_root=self.output_root, media_root=self.media_root,
        )
        self.assertEqual("succeeded", result)
        processor.assert_not_called()
        event_dir = self.status_root / "reup" / self.job["reup_job_id"]
        events = [json.loads((event_dir / f"event-{number:06d}.json").read_text(encoding="utf-8")) for number in (3, 4, 5)]
        self.assertEqual(["output_published", "handoff_published", "succeeded"], [event["event_kind"] for event in events])
        self.assertEqual(prepared.fingerprint, events[0]["output_fingerprint"])
        self.assertEqual([], list(prepared.final_path.parent.glob(".output.*.part")))

    def test_missing_event3_recovers_stage_only_by_linking_final(self) -> None:
        prepared = self._prepare_output()
        prepared.final_path.unlink()
        result = p1c_processing.recover_reup_completion(
            self.job, self.status_root, output_root=self.output_root, media_root=self.media_root,
        )
        self.assertEqual("succeeded", result)
        self.assertEqual(b"processed", prepared.final_path.read_bytes())
        self.assertTrue((self.status_root / "reup" / self.job["reup_job_id"] / "event-000003.json").is_file())

    def test_final_without_stage_or_event3_does_not_infer_output_published(self) -> None:
        prepared = self._prepare_output()
        prepared.stage_path.unlink()
        result = p1c_processing.recover_reup_completion(
            self.job, self.status_root, output_root=self.output_root, media_root=self.media_root,
        )
        self.assertEqual("reconciliation_required", result)
        self.assertFalse((self.status_root / "reup" / self.job["reup_job_id"] / "event-000003.json").exists())

    def test_missing_event3_rejects_bad_stage_multiple_stages_and_differing_final(self) -> None:
        final_dir = self.output_root / "p1c" / self.job["reup_job_id"]
        final_dir.mkdir(parents=True)
        bad_name = final_dir / (".output." + "0" * 64 + ".part")
        bad_name.write_bytes(b"bad")
        self.assertEqual("reconciliation_required", p1c_processing.recover_reup_completion(self.job, self.status_root, output_root=self.output_root, media_root=self.media_root))
        first = b"one"
        second = b"two"
        (final_dir / f".output.{hashlib.sha256(first).hexdigest()}.part").write_bytes(first)
        (final_dir / f".output.{hashlib.sha256(second).hexdigest()}.part").write_bytes(second)
        self.assertEqual("reconciliation_required", p1c_processing.recover_reup_completion(self.job, self.status_root, output_root=self.output_root, media_root=self.media_root))
        for path in final_dir.glob(".output.*.part"):
            path.unlink()
        prepared = self._prepare_output()
        prepared.final_path.unlink()
        prepared.final_path.write_bytes(b"different-final")
        self.assertEqual("reconciliation_required", p1c_processing.recover_reup_completion(self.job, self.status_root, output_root=self.output_root, media_root=self.media_root))
        self.assertEqual(b"different-final", prepared.final_path.read_bytes())

    def test_event5_recovery_requires_exact_output_handoff_and_chain(self) -> None:
        calls, event_dir, output_path, handoff_dir = self._complete_successful_attempt()
        self.assertEqual("succeeded", p1c_processing.recover_reup_completion(self.job, self.status_root, output_root=self.output_root, media_root=self.media_root))
        self.assertEqual(1, calls)
        output_path.unlink()
        self.assertEqual("reconciliation_required", p1c_processing.recover_reup_completion(self.job, self.status_root, output_root=self.output_root, media_root=self.media_root))

    def test_event5_recovery_rejects_hash_and_missing_video(self) -> None:
        _, _, output_path, handoff_dir = self._complete_successful_attempt()
        output_path.write_bytes(b"different-output")
        self.assertEqual("reconciliation_required", p1c_processing.recover_reup_completion(self.job, self.status_root, output_root=self.output_root, media_root=self.media_root))

        output_path.write_bytes(b"processed")
        video = handoff_dir / f"reup-{self.job['reup_job_id']}.mp4"
        video.unlink()
        self.assertEqual("reconciliation_required", p1c_processing.recover_reup_completion(self.job, self.status_root, output_root=self.output_root, media_root=self.media_root))

    def test_event5_recovery_rejects_missing_sidecar(self) -> None:
        _, _, _, handoff_dir = self._complete_successful_attempt()
        sidecar = handoff_dir / f"reup-{self.job['reup_job_id']}.meta.json"
        sidecar.unlink()
        self.assertEqual("reconciliation_required", p1c_processing.recover_reup_completion(self.job, self.status_root, output_root=self.output_root, media_root=self.media_root))

    def test_event5_recovery_rejects_sidecar_handoff_id_mismatch(self) -> None:
        _, _, _, handoff_dir = self._complete_successful_attempt()
        sidecar = handoff_dir / f"reup-{self.job['reup_job_id']}.meta.json"
        document = json.loads(sidecar.read_text(encoding="utf-8"))
        document["handoff_id"] = str(uuid.uuid4())
        sidecar.write_bytes(canonical_json_bytes(document))
        self.assertEqual("reconciliation_required", p1c_processing.recover_reup_completion(self.job, self.status_root, output_root=self.output_root, media_root=self.media_root))

    def test_event5_recovery_rejects_missing_event4(self) -> None:
        _, event_dir, _, _ = self._complete_successful_attempt()
        (event_dir / "event-000004.json").unlink()
        self.assertEqual("reconciliation_required", p1c_processing.recover_reup_completion(self.job, self.status_root, output_root=self.output_root, media_root=self.media_root))

    def test_event4_recovery_rejects_missing_event3(self) -> None:
        _, event_dir, _, _ = self._complete_successful_attempt()
        (event_dir / "event-000003.json").unlink()
        self.assertEqual("reconciliation_required", p1c_processing.recover_reup_completion(self.job, self.status_root, output_root=self.output_root, media_root=self.media_root))

    def test_handoff_failure_is_recovered_without_rerendering(self) -> None:
        self.media_root.write_text("not-a-directory", encoding="utf-8")
        calls = 0

        def processor(path: Path, **kwargs: object) -> object:
            nonlocal calls
            calls += 1
            candidate = self.output_root / "legacy-output.mp4"
            candidate.write_bytes(b"processed")
            return SimpleNamespace(success=True, output_file=str(candidate))

        with mock.patch.object(config, "processing_dir", self.processing_root):
            result = p1c_processing.process_reup_attempt(self.accepted, self.status_root, processor=processor, output_root=self.output_root, media_root=self.media_root)
        self.assertEqual("handoff_pending", result.outcome)
        self.assertEqual(1, calls)
        event = json.loads((self.status_root / "reup" / self.job["reup_job_id"] / "event-000003.json").read_text(encoding="utf-8"))
        self.media_root.unlink()
        recovered = p1c_processing.recover_reup_completion(self.job, self.status_root, output_root=self.output_root, media_root=self.media_root)
        self.assertEqual("succeeded", recovered)
        self.assertEqual(1, calls)
        self.assertEqual("output_published", event["event_kind"])
        self.assertEqual([], list((self.output_root / "p1c" / self.job["reup_job_id"]).glob(".output.*.part")))

    def test_recovery_completes_existing_handoff_published_event_without_rerendering(self) -> None:
        calls = 0

        def processor(path: Path, **kwargs: object) -> object:
            nonlocal calls
            calls += 1
            candidate = self.output_root / "legacy-output.mp4"
            candidate.write_bytes(b"processed")
            return SimpleNamespace(success=True, output_file=str(candidate))

        with mock.patch.object(config, "processing_dir", self.processing_root):
            result = p1c_processing.process_reup_attempt(self.accepted, self.status_root, processor=processor, output_root=self.output_root, media_root=self.media_root)
        self.assertEqual("succeeded", result.outcome)
        event_dir = self.status_root / "reup" / self.job["reup_job_id"]
        (event_dir / "event-000005.json").unlink()
        self.assertEqual("succeeded", p1c_processing.recover_reup_completion(self.job, self.status_root, output_root=self.output_root, media_root=self.media_root))
        self.assertEqual(1, calls)
        self.assertTrue((event_dir / "event-000005.json").is_file())

    def test_child_boundary_refuses_processing_without_started_evidence(self) -> None:
        self.status_root.joinpath("reup", self.job["reup_job_id"], "event-000002.json").unlink()
        with mock.patch.dict("sys.modules", {"pipeline.processor": mock.Mock()}):
            self.assertEqual(2, p1c_processing.main([str(self.input_root.parent.parent), str(self.envelope), str(self.status_root)]))


if __name__ == "__main__":
    unittest.main()
