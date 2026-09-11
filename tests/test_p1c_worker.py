"""CP4 worker shell tests with deterministic fake leases and work handles."""

from __future__ import annotations

import hashlib
import sys
import unittest
from unittest.mock import patch
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline.p1c_lease_client import LeaseBridgeResponse, LeaseBridgeUnavailable
from pipeline.p1c_status import ReupStatusError, publish_accepted_status
from pipeline.p1c_worker import run_lease_aware_work


JOB_ID = "11111111-1111-4111-8111-111111111111"; DISPATCH_ID = "22222222-2222-4222-8222-222222222222"; LEASE_ID = "33333333-3333-4333-8333-333333333333"


def job() -> dict[str, object]:
    return {"contract_version": 1, "message_kind": "control_plane_to_reup_job", "envelope_status": "complete", "dispatch_id": DISPATCH_ID, "reup_job_id": JOB_ID, "correlation_id": DISPATCH_ID, "candidate_id": "44444444-4444-4444-8444-444444444444", "schedule_id": "55555555-5555-4555-8555-555555555555", "channel_id": "66666666-6666-4666-8666-666666666666", "channel_slug": "channel", "reup_profile": "profile", "target_platform": "tiktok", "media_name": "dubvi-media.mp4", "media_size": 1, "source_fingerprint": "sha256:" + "0" * 64, "source_provenance_ref": "candidate:44444444-4444-4444-8444-444444444444", "created_at_utc": "2026-09-11T00:00:00.000Z", "attempt_number": 1}


def response(action: str, *, granted: bool | None = None) -> LeaseBridgeResponse:
    lease = {"lease_id": LEASE_ID, "resource_class": "HEAVY_MEDIA", "owner": f"reup:{JOB_ID}", "job_id": JOB_ID, "state": "released" if action == "release" else "active", "acquired_at": "2026-09-11T00:00:00.000Z", "heartbeat_at": "2026-09-11T00:00:00.000Z", "expires_at": "2026-09-11T00:01:00.000Z", "released_at": None}
    return LeaseBridgeResponse(action, granted, 15, 60, lease if granted is not False else None)


class Handle:
    def __init__(self, running: bool = False): self.running = running; self.terminated = False
    def poll(self): return None if self.running else 0
    def terminate(self): self.terminated = True
    def wait(self, timeout=None): return 0


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory(); self.addCleanup(self.temp.cleanup); self.root = Path(self.temp.name); self.job = job(); publish_accepted_status(self.root, self.job)

    def test_capacity_denial_never_starts_work_or_started_event(self):
        class Client:
            def acquire(_, job): return response("acquire", granted=False)
        called = []
        result = run_lease_aware_work(self.job, self.root, Client(), lambda job: called.append(job))
        self.assertEqual("capacity_unavailable", result.outcome); self.assertEqual([], called)
        self.assertFalse((self.root / "reup" / JOB_ID / "event-000002.json").exists())

    def test_missing_accepted_evidence_never_acquires(self):
        class Client:
            def acquire(_ , job): raise AssertionError("acquire must not run")
        result = run_lease_aware_work(self.job, self.root / "missing", Client(), lambda job: (_ for _ in ()).throw(AssertionError()))
        self.assertEqual("accepted_status_unavailable", result.outcome)

    def test_started_publication_failure_releases_exact_acquired_lease(self):
        released = []
        class Client:
            def acquire(_, job): return response("acquire", granted=True)
            def release(_, job, lease_id): released.append(lease_id); return response("release")
        with patch("pipeline.p1c_worker.publish_started_status", side_effect=ReupStatusError("conflict")):
            result = run_lease_aware_work(self.job, self.root, Client(), lambda job: (_ for _ in ()).throw(AssertionError()))
        self.assertEqual("started_status_unavailable", result.outcome); self.assertEqual([LEASE_ID], released)

    def test_start_work_exception_releases_and_never_heartbeats(self):
        released = []
        class Client:
            def acquire(_, job): return response("acquire", granted=True)
            def release(_, job, lease_id): released.append(lease_id); return response("release")
            def heartbeat(_, job, lease_id): raise AssertionError("heartbeat must not run")
        def start(_): raise RuntimeError("start failed")
        result = run_lease_aware_work(self.job, self.root, Client(), start)
        self.assertEqual("work_start_failed", result.outcome); self.assertEqual([LEASE_ID], released)

    def test_cleanup_failure_is_explicit(self):
        class Client:
            def acquire(_, job): return response("acquire", granted=True)
            def release(_, job, lease_id): raise LeaseBridgeUnavailable("release down")
        with patch("pipeline.p1c_worker.publish_started_status", side_effect=ReupStatusError("conflict")):
            result = run_lease_aware_work(self.job, self.root, Client(), lambda job: None)
        self.assertEqual("started_status_unavailable_release_unconfirmed", result.outcome)

    def test_started_then_completion_releases_owned_lease(self):
        class Client:
            def acquire(_, job): return response("acquire", granted=True)
            def release(_, job, lease_id): return response("release")
        handle = Handle(); result = run_lease_aware_work(self.job, self.root, Client(), lambda job: handle)
        self.assertEqual("work_completed_lease_released", result.outcome)
        self.assertTrue((self.root / "reup" / JOB_ID / "event-000002.json").is_file())

    def test_heartbeat_loss_terminates_only_owned_handle(self):
        class Client:
            def acquire(_, job): return response("acquire", granted=True)
            def heartbeat(_, job, lease_id): raise LeaseBridgeUnavailable("down")
        handle = Handle(running=True); ticks = iter((0.0, 15.0, 15.0))
        result = run_lease_aware_work(self.job, self.root, Client(), lambda job: handle, monotonic=lambda: next(ticks), sleep=lambda _: None)
        self.assertEqual("lease_lost", result.outcome); self.assertTrue(handle.terminated)
        self.assertTrue((self.root / "reup" / JOB_ID / "event-000003.json").is_file())


if __name__ == "__main__": unittest.main()
