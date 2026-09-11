"""CP5 verified output publication regressions."""

from __future__ import annotations

import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
import uuid

from pipeline.p1c_output import OutputPublicationError, publish_reup_output


class P1COutputPublicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.output_root = self.root / "output"
        self.output_root.mkdir()
        self.candidate = self.output_root / "legacy-result.mp4"
        self.candidate.write_bytes(b"verified-output")
        self.job = {"reup_job_id": str(uuid.uuid4())}

    def test_publishes_stream_verified_output_without_replacing_final(self) -> None:
        published = publish_reup_output(self.candidate, self.job, self.output_root)
        self.assertEqual(self.output_root / "p1c" / self.job["reup_job_id"] / "output.mp4", published.final_path)
        self.assertEqual("sha256:" + hashlib.sha256(b"verified-output").hexdigest(), published.fingerprint)
        self.assertEqual(b"verified-output", published.final_path.read_bytes())
        self.assertTrue(published.stage_path.is_file())

    def test_matching_final_is_idempotent_and_different_final_is_conflict(self) -> None:
        first = publish_reup_output(self.candidate, self.job, self.output_root)
        second = publish_reup_output(self.candidate, self.job, self.output_root)
        self.assertTrue(second.reused_existing)
        self.assertEqual(first.fingerprint, second.fingerprint)
        first.final_path.unlink()
        first.final_path.write_bytes(b"unrelated-final")
        with self.assertRaisesRegex(OutputPublicationError, "FINAL_PATH_CONFLICT"):
            publish_reup_output(self.candidate, self.job, self.output_root)
        self.assertEqual(b"unrelated-final", first.final_path.read_bytes())

    def test_candidate_must_be_inside_output_root_and_final_is_not_overwritten(self) -> None:
        outside = self.root / "outside.mp4"
        outside.write_bytes(b"outside")
        with self.assertRaises(OutputPublicationError):
            publish_reup_output(outside, self.job, self.output_root)


if __name__ == "__main__":
    unittest.main()
