"""External Reup input-root configuration and canonical discovery regressions."""

from __future__ import annotations

import sys
import unittest
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.config import BASE_DIR, PipelineConfig
from pipeline.p1c_intake import ReupIntakeError, iter_reup_envelopes
from pipeline.p1c_worker import run_canonical_once


class ExternalInputRootTests(unittest.TestCase):
    def test_default_input_root_uses_repo_fallback(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            self.assertEqual(BASE_DIR / "input", PipelineConfig().input_dir)

    def test_absolute_input_root_is_used_exactly(self) -> None:
        with TemporaryDirectory() as temporary:
            external = Path(temporary) / "canonical-input"
            with patch.dict("os.environ", {"DUBVI_REUP_INPUT_DIR": str(external)}):
                self.assertEqual(external, PipelineConfig().input_dir)

    def test_relative_input_root_is_preserved_as_path(self) -> None:
        with patch.dict("os.environ", {"DUBVI_REUP_INPUT_DIR": "runtime-input"}):
            self.assertEqual(Path("runtime-input"), PipelineConfig().input_dir)

    def test_envelope_discovery_uses_external_root(self) -> None:
        with TemporaryDirectory() as temporary:
            external = Path(temporary) / "canonical-input"
            envelope = external / "profile" / "tiktok" / "dubvi-reup-job-example.job.json"
            envelope.parent.mkdir(parents=True)
            envelope.write_text("{}", encoding="utf-8")
            self.assertEqual((envelope,), iter_reup_envelopes(external))

    def test_missing_external_root_does_not_fall_back(self) -> None:
        with TemporaryDirectory() as temporary:
            external = Path(temporary) / "does-not-exist"
            with patch.dict("os.environ", {"DUBVI_REUP_INPUT_DIR": str(external)}):
                configured = PipelineConfig().input_dir
            self.assertEqual(external, configured)
            self.assertNotEqual(BASE_DIR / "input", configured)

    def test_requested_missing_job_fails_closed_in_external_root(self) -> None:
        with TemporaryDirectory() as temporary:
            external = Path(temporary) / "canonical-input"
            status = Path(temporary) / "status"
            external.mkdir()
            from pipeline import p1c_worker

            with patch.object(p1c_worker.config, "input_dir", external), patch.object(
                p1c_worker.config, "engine_status_dir", status
            ):
                with self.assertRaisesRegex(ReupIntakeError, "requested Reup job envelope was not found"):
                    run_canonical_once(reup_job_id=str(uuid.uuid4()))


if __name__ == "__main__":
    unittest.main()
