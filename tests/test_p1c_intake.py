"""Offline CP3 canonical Reup intake regressions; no processor invocation."""

from __future__ import annotations

import hashlib
import json
import sys
import unittest
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.dubvi_engine_contract import canonical_json_bytes, validate_engine_status_event
from pipeline.input_rules import is_legacy_media_input
from pipeline.p1c_intake import ReupIntakeError, accept_next_reup_job


class P1CReupIntakeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "input"
        self.target = self.root / "profile" / "tiktok"
        self.target.mkdir(parents=True)
        self.status = Path(self.temporary.name) / "status"
        self.media = self.target / "dubvi-media.mp4"
        self.media.write_bytes(b"canonical-media")
        self.job_id = str(uuid.uuid4())
        self.dispatch_id = str(uuid.uuid4())
        self.candidate_id = str(uuid.uuid4())
        self.schedule_id = str(uuid.uuid4())
        self.channel_id = str(uuid.uuid4())

    def _write_envelope(self, **changes: object) -> Path:
        document: dict[str, object] = {
            "contract_version": 1, "message_kind": "control_plane_to_reup_job", "envelope_status": "complete",
            "dispatch_id": self.dispatch_id, "reup_job_id": self.job_id, "correlation_id": self.dispatch_id,
            "candidate_id": self.candidate_id, "schedule_id": self.schedule_id, "channel_id": self.channel_id,
            "channel_slug": "channel", "reup_profile": "profile", "target_platform": "tiktok",
            "media_name": self.media.name, "media_size": self.media.stat().st_size,
            "source_fingerprint": "sha256:" + hashlib.sha256(self.media.read_bytes()).hexdigest(),
            "source_provenance_ref": f"candidate:{self.candidate_id}",
            "created_at_utc": "2026-09-10T13:00:00.000Z", "attempt_number": 1,
        }
        document.update(changes)
        path = self.target / f"dubvi-reup-job-{self.job_id}.job.json"
        path.write_bytes(canonical_json_bytes(document))
        return path

    def test_accepts_one_valid_job_and_reuses_exact_status(self) -> None:
        self._write_envelope()
        first = accept_next_reup_job(self.root, self.status, occurred_at_utc="2026-09-10T13:01:00.000Z")
        assert first is not None
        second = accept_next_reup_job(self.root, self.status, occurred_at_utc="2026-09-10T13:02:00.000Z")
        assert second is not None
        self.assertEqual(first.status_path, second.status_path)
        event = validate_engine_status_event(json.loads(first.status_path.read_text(encoding="utf-8")))
        self.assertEqual(("accepted", "accepted", 1), (event["event_kind"], event["state"], event["sequence"]))
        self.assertEqual([], list(first.status_path.parent.glob("event-000002.json")))

    def test_rejects_media_fingerprint_mismatch_and_legacy_namespace_is_fenced(self) -> None:
        self._write_envelope()
        self.media.write_bytes(b"tampered")
        with self.assertRaises(ReupIntakeError):
            accept_next_reup_job(self.root, self.status)
        self.assertFalse(is_legacy_media_input(self.media, (".mp4",)))
        manual = self.target / "manual.mp4"
        manual.write_bytes(b"manual")
        self.assertTrue(is_legacy_media_input(manual, (".mp4",)))

    def test_canonical_intake_never_imports_or_calls_the_processor(self) -> None:
        source = (ROOT / "pipeline" / "p1c_intake.py").read_text(encoding="utf-8")
        self.assertNotIn("pipeline.processor", source)
        self.assertNotIn("process_single_video", source)

    def test_differing_existing_status_never_overwrites(self) -> None:
        self._write_envelope()
        event_path = self.status / "reup" / self.job_id / "event-000001.json"
        event_path.parent.mkdir(parents=True)
        event_path.write_text("{}", encoding="utf-8")
        with self.assertRaises(ReupIntakeError):
            accept_next_reup_job(self.root, self.status)
        self.assertEqual("{}", event_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
