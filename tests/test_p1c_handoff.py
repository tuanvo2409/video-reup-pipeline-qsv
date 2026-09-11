"""CP5 Reup-to-Translator handoff v2 publication regressions."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
import uuid

from pipeline.dubvi_engine_contract import canonical_json_bytes, validate_reup_to_translator_handoff
from pipeline.p1c_handoff import HandoffV2Error, publish_reup_handoff


def _job() -> dict[str, object]:
    return {
        "candidate_id": str(uuid.uuid4()),
        "schedule_id": str(uuid.uuid4()),
        "dispatch_id": str(uuid.uuid4()),
        "reup_job_id": str(uuid.uuid4()),
        "channel_id": str(uuid.uuid4()),
        "channel_slug": "home-vi",
        "reup_profile": "home",
        "target_platform": "tiktok",
        "localization_profile": "vi-default",
        "policy_profile": "default",
        "source_fingerprint": "sha256:" + "1" * 64,
    }


class P1CHandoffV2Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.output = self.root / "output.mp4"
        self.output.write_bytes(b"published-video")
        self.job = _job()
        self.fingerprint = "sha256:" + hashlib.sha256(self.output.read_bytes()).hexdigest()

    def test_publishes_contract_valid_v2_sidecar_last_and_reuses_same_handoff(self) -> None:
        first = publish_reup_handoff(
            self.output, self.job, self.root / "media", output_fingerprint=self.fingerprint,
            created_at_utc="2026-09-10T10:00:00.000Z",
        )
        document = validate_reup_to_translator_handoff(json.loads(first.sidecar_path.read_text(encoding="utf-8")))
        self.assertEqual(2, document["handoff_schema_version"])
        self.assertNotIn("translator_job_id", document)
        self.assertEqual(self.fingerprint, document["reup_output_fingerprint"])
        second = publish_reup_handoff(
            self.output, self.job, self.root / "media", output_fingerprint=self.fingerprint,
            created_at_utc="2026-09-10T11:00:00.000Z",
        )
        self.assertEqual(first.handoff_id, second.handoff_id)
        self.assertEqual(first.sidecar_path.read_bytes(), second.sidecar_path.read_bytes())

    def test_translator_repository_validator_accepts_published_sidecar(self) -> None:
        published = publish_reup_handoff(self.output, self.job, self.root / "media", output_fingerprint=self.fingerprint)
        translator_contract_path = Path(__file__).resolve().parents[2] / "douyin-vi-translator" / "local-worker" / "dubvi_engine_contract.py"
        specification = importlib.util.spec_from_file_location("translator_contract_cp5", translator_contract_path)
        self.assertIsNotNone(specification)
        assert specification is not None and specification.loader is not None
        module = importlib.util.module_from_spec(specification)
        specification.loader.exec_module(module)
        validated = module.validate_reup_to_translator_handoff(json.loads(published.sidecar_path.read_text(encoding="utf-8")))
        self.assertEqual(2, validated["handoff_schema_version"])
        self.assertNotIn("translator_job_id", validated)

    def test_different_existing_video_is_never_overwritten(self) -> None:
        media = self.root / "media" / self.job["reup_profile"]
        media.mkdir(parents=True)
        final = media / f"reup-{self.job['reup_job_id']}.mp4"
        final.write_bytes(b"unrelated")
        with self.assertRaisesRegex(HandoffV2Error, "FINAL_PATH_CONFLICT"):
            publish_reup_handoff(self.output, self.job, self.root / "media", output_fingerprint=self.fingerprint)
        self.assertEqual(b"unrelated", final.read_bytes())

    def test_existing_sidecar_requires_matching_full_identity_and_video(self) -> None:
        first = publish_reup_handoff(self.output, self.job, self.root / "media", output_fingerprint=self.fingerprint)
        sidecar = json.loads(first.sidecar_path.read_text(encoding="utf-8"))
        sidecar["channel_id"] = str(uuid.uuid4())
        first.sidecar_path.write_bytes(canonical_json_bytes(sidecar))
        with self.assertRaisesRegex(HandoffV2Error, "FINAL_PATH_CONFLICT"):
            publish_reup_handoff(self.output, self.job, self.root / "media", output_fingerprint=self.fingerprint)


if __name__ == "__main__":
    unittest.main()
