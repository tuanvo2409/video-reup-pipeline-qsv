"""Offline CP3 canonical Reup intake regressions; no processor invocation."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import unittest
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.dubvi_engine_contract import canonical_json_bytes, validate_engine_status_event
from pipeline.input_rules import is_legacy_media_input
from pipeline.p1c_intake import ReupIntakeError, accept_next_reup_job, accept_reup_envelope


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

    def test_runtime_lineage_and_path_components_fail_before_status_write(self) -> None:
        cases = (
            ("missing-attempt", {"attempt_number": None}),
            ("first-with-parent", {"parent_reup_job_id": str(uuid.uuid4())}),
            ("retry-without-parent", {"attempt_number": 2}),
            ("profile-traversal", {"reup_profile": "foo/../profile"}),
            ("profile-dot", {"reup_profile": "profile/."}),
            ("profile-backslash", {"reup_profile": "foo\\bar"}),
            ("platform-unsafe", {"target_platform": "../tiktok"}),
        )
        for name, changes in cases:
            with self.subTest(name=name):
                self._write_envelope(**changes)
                path = self.target / f"dubvi-reup-job-{self.job_id}.job.json"
                if changes.get("attempt_number") is None:
                    document = json.loads(path.read_text(encoding="utf-8"))
                    document.pop("attempt_number")
                    path.write_bytes(canonical_json_bytes(document))
                with self.assertRaises(ReupIntakeError):
                    accept_next_reup_job(self.root, self.status)
                self.assertFalse(self.status.exists())

    def test_oversized_envelope_and_invalid_status_root_fail_closed(self) -> None:
        path = self._write_envelope()
        path.write_bytes(b"{" + b" " * (64 * 1024) + b"}")
        with self.assertRaises(ReupIntakeError):
            accept_next_reup_job(self.root, self.status)
        self.assertFalse(self.status.exists())
        self._write_envelope()
        self.status.write_text("not a directory", encoding="utf-8")
        with self.assertRaises(ReupIntakeError):
            accept_next_reup_job(self.root, self.status)
        self.assertTrue(self.status.is_file())

    def test_symlink_roots_are_rejected_when_supported(self) -> None:
        envelope = self._write_envelope()
        target = Path(self.temporary.name) / "real-status"
        target.mkdir()
        try:
            os.symlink(target, self.status, target_is_directory=True)
        except (NotImplementedError, OSError):
            self.skipTest("directory symlinks are unavailable in this test environment")
        with self.assertRaises(ReupIntakeError):
            accept_next_reup_job(self.root, self.status)
        linked_root = Path(self.temporary.name) / "linked-input"
        os.symlink(self.root, linked_root, target_is_directory=True)
        with self.assertRaises(ReupIntakeError):
            accept_reup_envelope(envelope, linked_root, Path(self.temporary.name) / "unused-status")

    def test_retry_status_copies_exact_parent_job_id(self) -> None:
        parent = str(uuid.uuid4())
        self._write_envelope(attempt_number=2, parent_reup_job_id=parent)
        accepted = accept_next_reup_job(self.root, self.status, occurred_at_utc="2026-09-10T13:01:00.000Z")
        assert accepted is not None
        event = validate_engine_status_event(json.loads(accepted.status_path.read_text(encoding="utf-8")))
        self.assertEqual(2, event["attempt_number"])
        self.assertEqual(parent, event["parent_engine_job_id"])

    def test_legacy_filter_fences_root_and_nested_canonical_media(self) -> None:
        nested = self.target / "dubvi-nested.mp4"
        nested.write_bytes(b"x")
        ordinary = self.target / "ordinary.mp4"
        ordinary.write_bytes(b"x")
        self.assertFalse(is_legacy_media_input(self.media, (".mp4",)))
        self.assertFalse(is_legacy_media_input(nested, (".mp4",)))
        self.assertTrue(is_legacy_media_input(ordinary, (".mp4",)))

    def test_direct_intake_requires_real_input_root(self) -> None:
        envelope = self._write_envelope()
        missing = Path(self.temporary.name) / "missing-root"
        with self.assertRaises(ReupIntakeError):
            accept_reup_envelope(envelope, missing, self.status)
        file_root = Path(self.temporary.name) / "file-root"
        file_root.write_text("not a directory", encoding="utf-8")
        with self.assertRaises(ReupIntakeError):
            accept_reup_envelope(envelope, file_root, self.status)
        self.assertFalse(self.status.exists())

    def test_status_race_requires_exact_canonical_bytes(self) -> None:
        self._write_envelope()
        from pipeline import p1c_status

        def different_winner(stage: str | os.PathLike[str], final: str | os.PathLike[str]) -> None:
            document = json.loads(Path(stage).read_text(encoding="utf-8"))
            document["event_id"] = str(uuid.uuid4())
            document["occurred_at_utc"] = "2026-09-10T13:02:00.000Z"
            Path(final).write_bytes(canonical_json_bytes(document))
            raise FileExistsError

        with mock.patch.object(p1c_status.os, "link", side_effect=different_winner):
            with self.assertRaises(ReupIntakeError):
                accept_next_reup_job(self.root, self.status, occurred_at_utc="2026-09-10T13:01:00.000Z")
        final = self.status / "reup" / self.job_id / "event-000001.json"
        winner = json.loads(final.read_text(encoding="utf-8"))
        self.assertEqual("2026-09-10T13:02:00.000Z", winner["occurred_at_utc"])
        self.assertEqual([], list(final.parent.glob("event-000002.json")))

    def test_status_race_reuses_only_exact_bytes(self) -> None:
        self._write_envelope()
        from pipeline import p1c_status

        def exact_winner(stage: str | os.PathLike[str], final: str | os.PathLike[str]) -> None:
            Path(final).write_bytes(Path(stage).read_bytes())
            raise FileExistsError

        with mock.patch.object(p1c_status.os, "link", side_effect=exact_winner):
            accepted = accept_next_reup_job(self.root, self.status, occurred_at_utc="2026-09-10T13:01:00.000Z")
        assert accepted is not None
        self.assertEqual([], list(accepted.status_path.parent.glob("event-000002.json")))


if __name__ == "__main__":
    unittest.main()
