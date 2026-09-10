from __future__ import annotations

import hashlib
from pathlib import Path
import unittest

from pipeline import dubvi_engine_contract as contract


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPOSITORY_ROOT / "tests" / "fixtures" / "p1c_engine_contract"


class ReupEngineContractTests(unittest.TestCase):
    def _fixture(self, name: str) -> dict[str, object]:
        return contract.parse_json_document((FIXTURE_ROOT / name).read_bytes())

    def test_vendored_module_and_fixture_manifest_are_pinned(self) -> None:
        manifest = self._fixture("contract-manifest.json")
        module_hash = hashlib.sha256((REPOSITORY_ROOT / "pipeline" / "dubvi_engine_contract.py").read_bytes()).hexdigest()
        self.assertEqual(contract.CONTRACT_IMPLEMENTATION_VERSION, manifest["contract_implementation_version"])
        self.assertEqual(module_hash, manifest["module_sha256"])
        listed = manifest["fixtures"]
        self.assertEqual(
            set(listed),
            {path.name for path in FIXTURE_ROOT.glob("*.json") if path.name != "contract-manifest.json"},
        )
        for name, expected in listed.items():
            self.assertEqual(expected, hashlib.sha256((FIXTURE_ROOT / name).read_bytes()).hexdigest())

    def test_all_shared_valid_and_invalid_vectors_use_generic_validator(self) -> None:
        for path in sorted(FIXTURE_ROOT.glob("*.valid.json")):
            with self.subTest(path=path.name):
                contract.validate_document(contract.parse_json_document(path.read_bytes()))
        for path in sorted(FIXTURE_ROOT.glob("*.invalid.json")):
            with self.subTest(path=path.name):
                with self.assertRaises(contract.ContractValidationError):
                    contract.validate_document(contract.parse_json_document(path.read_bytes()))

    def test_reup_envelopes_and_events_are_accepted(self) -> None:
        for name in (
            "cp_to_reup_job.valid.json",
            "cp_to_reup_job.retry.valid.json",
            "reup.accepted.valid.json",
            "reup.running.valid.json",
            "reup.failed.valid.json",
            "reup.succeeded.valid.json",
        ):
            with self.subTest(name=name):
                contract.validate_document(self._fixture(name))

    def test_v2_handoff_and_translator_envelope_are_understood(self) -> None:
        contract.validate_document(self._fixture("reup_to_translator_handoff_v2.valid.json"))
        contract.validate_document(self._fixture("cp_to_translator_job.valid.json"))
        contract.validate_document(self._fixture("cp_to_translator_job.retry.valid.json"))

    def test_invalid_identity_and_reference_fail_closed(self) -> None:
        for name in (
            "cp_to_reup_job.bad_correlation.invalid.json",
            "cp_to_reup_job.bad_fingerprint.invalid.json",
            "cp_to_translator_job.absolute_ref.invalid.json",
            "handoff_v2.translator_job_id.invalid.json",
            "status.illegal_state_pair.invalid.json",
        ):
            with self.subTest(name=name):
                with self.assertRaises(contract.ContractValidationError):
                    contract.validate_document(self._fixture(name))


if __name__ == "__main__":
    unittest.main()
