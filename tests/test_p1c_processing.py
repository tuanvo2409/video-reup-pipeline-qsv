"""CP5 processing adapter, lifecycle, recovery, and child-boundary tests."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
import uuid
from unittest import mock

from pipeline import p1c_processing
from pipeline.config import config
from pipeline.dubvi_engine_contract import canonical_json_bytes
from pipeline.p1c_intake import AcceptedReupJob
from pipeline.p1c_status import publish_accepted_status, publish_started_status


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
        self.assertTrue(str(scratch).startswith(str(self.processing_root / "p1c-source" / self.job["reup_job_id"])))
        self.assertEqual({"override_profile": "home", "target_platform": "tiktok", "send_to_dubvi": False, "force_reprocess": True}, kwargs)
        events = [json.loads((self.status_root / "reup" / self.job["reup_job_id"] / f"event-{number:06d}.json").read_text(encoding="utf-8")) for number in range(1, 6)]
        self.assertEqual(["accepted", "started", "output_published", "handoff_published", "succeeded"], [event["event_kind"] for event in events])
        self.assertFalse(scratch.exists())

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
