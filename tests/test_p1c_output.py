"""CP5 verified output publication regressions."""

from __future__ import annotations

import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
import uuid

from pipeline.p1c_output import OutputPublicationError, cleanup_published_output_stage, publish_reup_output, recover_prepared_reup_output


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

    def test_recovery_reuses_matching_final_and_exact_owned_stage(self) -> None:
        prepared = publish_reup_output(self.candidate, self.job, self.output_root)
        recovered = recover_prepared_reup_output(self.job, self.output_root)
        assert recovered is not None
        self.assertTrue(recovered.reused_existing)
        self.assertEqual(prepared.fingerprint, recovered.fingerprint)
        self.assertEqual(b"verified-output", recovered.final_path.read_bytes())

    def test_recovery_links_stage_only_to_canonical_final(self) -> None:
        prepared = publish_reup_output(self.candidate, self.job, self.output_root)
        prepared.final_path.unlink()
        recovered = recover_prepared_reup_output(self.job, self.output_root)
        assert recovered is not None
        self.assertFalse(recovered.reused_existing)
        self.assertEqual(b"verified-output", recovered.final_path.read_bytes())

    def test_final_without_owned_stage_is_not_recovery_evidence(self) -> None:
        prepared = publish_reup_output(self.candidate, self.job, self.output_root)
        cleanup_published_output_stage(prepared)
        self.assertIsNone(recover_prepared_reup_output(self.job, self.output_root))

    def test_invalid_stage_digest_is_ignored_and_two_valid_stages_fail_closed(self) -> None:
        final_dir = self.output_root / "p1c" / self.job["reup_job_id"]
        final_dir.mkdir(parents=True)
        invalid_name = final_dir / (".output." + "0" * 64 + ".part")
        invalid_name.write_bytes(b"wrong-digest")
        self.assertIsNone(recover_prepared_reup_output(self.job, self.output_root))
        first = b"first-stage"
        second = b"second-stage"
        first_fp = hashlib.sha256(first).hexdigest()
        second_fp = hashlib.sha256(second).hexdigest()
        (final_dir / f".output.{first_fp}.part").write_bytes(first)
        (final_dir / f".output.{second_fp}.part").write_bytes(second)
        with self.assertRaisesRegex(OutputPublicationError, "multiple valid"):
            recover_prepared_reup_output(self.job, self.output_root)

    def test_valid_stage_and_different_final_never_overwrite_final(self) -> None:
        prepared = publish_reup_output(self.candidate, self.job, self.output_root)
        prepared.final_path.unlink()
        prepared.final_path.write_bytes(b"unrelated-final")
        with self.assertRaisesRegex(OutputPublicationError, "FINAL_PATH_CONFLICT"):
            recover_prepared_reup_output(self.job, self.output_root)
        self.assertEqual(b"unrelated-final", prepared.final_path.read_bytes())


if __name__ == "__main__":
    unittest.main()
