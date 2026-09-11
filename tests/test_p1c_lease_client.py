from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.p1c_lease_client import LeaseBridgeUnavailable, _validate_response


JOB = "11111111-1111-4111-8111-111111111111"


def payload(*, acquired="2026-09-11T00:00:00.000Z", heartbeat="2026-09-11T00:00:15.000Z", expires="2026-09-11T00:01:00.000Z", state="active", released_at=None):
    lease = {"lease_id": "22222222-2222-4222-8222-222222222222", "resource_class": "HEAVY_MEDIA", "owner": f"reup:{JOB}", "job_id": JOB, "state": state, "acquired_at": acquired, "heartbeat_at": heartbeat, "expires_at": expires, "released_at": released_at}
    return {"bridge_version": 1, "action": "acquire" if state == "active" else "release", "ok": True, "engine_kind": "reup", "timing": {"heartbeat_seconds": 15, "ttl_seconds": 60}, "lease": lease, "granted": True}


class LeaseClientResponseTests(unittest.TestCase):
    def test_canonical_timestamps_are_accepted(self):
        self.assertEqual(15, _validate_response(payload(), "acquire", JOB).heartbeat_seconds)

    def test_malformed_timestamps_are_rejected(self):
        cases = (
            {"acquired": "garbageZ"}, {"acquired": "2026-99-99T00:00:00.000Z"},
            {"acquired": "2026-09-11T00:00:00Z"}, {"acquired": "2026-09-11T00:00:00.00Z"},
            {"acquired": "2026-09-11T00:00:00.000+00:00"},
            {"heartbeat": "2026-09-10T23:59:59.000Z"}, {"expires": "2026-09-11T00:00:15.000Z"},
        )
        for change in cases:
            with self.subTest(change=change), self.assertRaises(LeaseBridgeUnavailable):
                _validate_response({**payload(), "lease": {**payload()["lease"], **change}}, "acquire", JOB)

    def test_released_lease_requires_canonical_released_at(self):
        for value in (None, "garbageZ", "2026-09-11T00:00:15Z"):
            with self.subTest(value=value), self.assertRaises(LeaseBridgeUnavailable):
                _validate_response(payload(state="released", released_at=value), "release", JOB)
        valid = _validate_response(payload(state="released", released_at="2026-09-11T00:00:30.000Z"), "release", JOB)
        self.assertEqual("released", valid.lease["state"])


if __name__ == "__main__":
    unittest.main()
