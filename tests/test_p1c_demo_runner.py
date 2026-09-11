from __future__ import annotations

import hashlib
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, patch

from pipeline import p1c_worker
from pipeline.config import config
from pipeline.dubvi_engine_contract import canonical_json_bytes
from pipeline.p1c_intake import accept_reup_envelope
from pipeline.p1c_lease_client import LeaseBridgeResponse
from pipeline.p1c_status import (
    publish_accepted_status,
    publish_failed_status,
    publish_handoff_published_status,
    publish_lease_lost_status,
    publish_output_published_status,
    publish_started_status,
    publish_succeeded_status,
)


class Handle:
    def poll(self):
        return 0

    def terminate(self):
        raise AssertionError("completed child must not be terminated")

    def wait(self, timeout=None):
        return 0


class Client:
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.released: list[str] = []
        self.lease_id = str(uuid.uuid4())

    def acquire(self, job):
        return LeaseBridgeResponse(
            "acquire", True, 15, 60,
            {
                "lease_id": self.lease_id,
                "resource_class": "HEAVY_MEDIA",
                "owner": f"reup:{self.job_id}",
                "job_id": self.job_id,
                "state": "active",
                "acquired_at": "2026-09-11T00:00:00.000Z",
                "heartbeat_at": "2026-09-11T00:00:00.000Z",
                "expires_at": "2026-09-11T00:01:00.000Z",
                "released_at": None,
            },
        )

    def release(self, job, lease_id):
        self.released.append(lease_id)
        return LeaseBridgeResponse(
            "release", None, 15, 60,
            {
                "lease_id": lease_id,
                "resource_class": "HEAVY_MEDIA",
                "owner": f"reup:{self.job_id}",
                "job_id": self.job_id,
                "state": "released",
                "acquired_at": "2026-09-11T00:00:00.000Z",
                "heartbeat_at": "2026-09-11T00:00:00.000Z",
                "expires_at": "2026-09-11T00:01:00.000Z",
                "released_at": "2026-09-11T00:00:30.000Z",
            },
        )


class ReupDemoRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.input_root = root / "input"
        self.target = self.input_root / "profile1" / "tiktok"
        self.target.mkdir(parents=True)
        self.status_root = root / "status"
        self.status_root.mkdir()
        self.output_root = root / "output"
        self.output_root.mkdir()
        self.media_root = root / "dubvi-media"
        self.media_root.mkdir()

    def _write_job(self, number: int) -> tuple[Path, dict[str, object]]:
        ids = {
            "candidate_id": str(uuid.UUID(int=number, version=4)),
            "schedule_id": str(uuid.UUID(int=100 + number, version=4)),
            "dispatch_id": str(uuid.UUID(int=200 + number, version=4)),
            "reup_job_id": str(uuid.UUID(int=300 + number, version=4)),
            "channel_id": str(uuid.UUID(int=400 + number, version=4)),
        }
        profile = f"profile{number}"
        target = self.input_root / profile / "tiktok"
        target.mkdir(parents=True, exist_ok=True)
        media = target / "dubvi-media.mp4"
        payload = f"media-{number}".encode()
        media.write_bytes(payload)
        document: dict[str, object] = {
            "contract_version": 1,
            "message_kind": "control_plane_to_reup_job",
            "envelope_status": "complete",
            **ids,
            "correlation_id": ids["dispatch_id"],
            "channel_slug": "demo-channel",
            "reup_profile": profile,
            "target_platform": "tiktok",
            "media_name": media.name,
            "media_size": len(payload),
            "source_fingerprint": "sha256:" + hashlib.sha256(payload).hexdigest(),
            "source_provenance_ref": f"candidate:{ids['candidate_id']}",
            "created_at_utc": "2026-09-11T00:00:00.000Z",
            "attempt_number": 1,
        }
        path = target / f"dubvi-reup-job-{ids['reup_job_id']}.job.json"
        path.write_bytes(canonical_json_bytes(document))
        return path, document

    def _configured(self):
        return patch.multiple(
            config,
            input_dir=self.input_root,
            engine_status_dir=self.status_root,
            output_dir=self.output_root,
            dubvi_media_dir=self.media_root,
        )

    def _accept(self, path: Path, job: dict[str, object]) -> None:
        accept_reup_envelope(path, self.input_root, self.status_root)

    def _start(self, job: dict[str, object]) -> str:
        lease_id = str(uuid.uuid4())
        publish_started_status(self.status_root, job, lease_id)
        return lease_id

    def test_new_envelope_is_accepted_then_sent_to_lease_worker(self) -> None:
        path, job = self._write_job(1)
        client = Client(job["reup_job_id"])
        with self._configured(), \
             patch.object(p1c_worker.LeaseBridgeClient, "from_environment", return_value=client), \
             patch.object(p1c_worker, "launch_processing_child", return_value=Handle()) as launch:
            report = p1c_worker.run_canonical_once()
        self.assertEqual(job["reup_job_id"], report["reup_job_id"])
        self.assertTrue((self.status_root / "reup" / job["reup_job_id"] / "event-000001.json").is_file())
        launch.assert_called_once_with(self.input_root, path, self.status_root)
        self.assertEqual([client.lease_id], client.released)

    def test_accepted_without_start_is_selected_before_later_job(self) -> None:
        first_path, first = self._write_job(1)
        self._write_job(2)
        self._accept(first_path, first)
        client = Client(first["reup_job_id"])
        with self._configured(), \
             patch.object(p1c_worker.LeaseBridgeClient, "from_environment", return_value=client), \
             patch.object(p1c_worker, "launch_processing_child", return_value=Handle()) as launch:
            report = p1c_worker.run_canonical_once()
        self.assertEqual(first["reup_job_id"], report["reup_job_id"])
        self.assertEqual(first_path, launch.call_args.args[1])

    def test_started_without_terminal_skips_to_later_job(self) -> None:
        first_path, first = self._write_job(1)
        second_path, second = self._write_job(2)
        self._accept(first_path, first)
        self._start(first)
        client = Client(second["reup_job_id"])
        with self._configured(), \
             patch.object(p1c_worker.LeaseBridgeClient, "from_environment", return_value=client), \
             patch.object(p1c_worker, "launch_processing_child", return_value=Handle()) as launch:
            report = p1c_worker.run_canonical_once()
        self.assertEqual(second["reup_job_id"], report["reup_job_id"])
        self.assertEqual(second_path, launch.call_args.args[1])
        self.assertTrue((self.status_root / "reup" / first["reup_job_id"] / "event-000002.json").is_file())

    def test_terminal_old_job_advances_to_new_job(self) -> None:
        first_path, first = self._write_job(1)
        second_path, second = self._write_job(2)
        self._accept(first_path, first)
        self._start(first)
        publish_failed_status(self.status_root, first, "PROCESSING_FAILED", "old")
        client = Client(second["reup_job_id"])
        with self._configured(), \
             patch.object(p1c_worker.LeaseBridgeClient, "from_environment", return_value=client), \
             patch.object(p1c_worker, "launch_processing_child", return_value=Handle()) as launch:
            report = p1c_worker.run_canonical_once()
        self.assertEqual(second["reup_job_id"], report["reup_job_id"])
        self.assertEqual(second_path, launch.call_args.args[1])

    def test_output_published_uses_recovery_without_launching_processor(self) -> None:
        path, job = self._write_job(1)
        self._accept(path, job)
        self._start(job)
        fingerprint = job["source_fingerprint"]
        publish_output_published_status(self.status_root, job, fingerprint)
        with self._configured(), \
             patch.object(p1c_worker, "recover_reup_completion", return_value="succeeded") as recover, \
             patch.object(p1c_worker, "launch_processing_child") as launch:
            report = p1c_worker.run_canonical_once()
        self.assertEqual("succeeded", report["outcome"])
        recover.assert_called_once()
        launch.assert_not_called()

    def test_event4_and_event5_rediscovery_are_recovery_only(self) -> None:
        for number, terminal in enumerate(("event4", "event5"), start=1):
            with self.subTest(terminal=terminal):
                path, job = self._write_job(number)
                self._accept(path, job)
                lease_id = self._start(job)
                fingerprint = job["source_fingerprint"]
                publish_output_published_status(self.status_root, job, fingerprint)
                handoff_id = str(uuid.uuid4())
                handoff_ref = f"profile{number}/reup.meta.json"
                publish_handoff_published_status(self.status_root, job, fingerprint, handoff_id, handoff_ref)
                if terminal == "event5":
                    publish_succeeded_status(self.status_root, job, fingerprint, handoff_id, handoff_ref)
                with self._configured(), \
                     patch.object(p1c_worker, "recover_reup_completion", return_value="succeeded") as recover, \
                     patch.object(p1c_worker, "launch_processing_child") as launch:
                    report = p1c_worker.run_canonical_once(
                        reup_job_id=job["reup_job_id"] if terminal == "event5" else None
                    )
                self.assertEqual("succeeded", report["outcome"])
                recover.assert_called_once()
                launch.assert_not_called()

    def test_completed_event5_is_skipped_for_automatic_queue_progress(self) -> None:
        first_path, first = self._write_job(1)
        second_path, second = self._write_job(2)
        self._accept(first_path, first)
        self._start(first)
        fingerprint = first["source_fingerprint"]
        publish_output_published_status(self.status_root, first, fingerprint)
        handoff_id = str(uuid.uuid4())
        handoff_ref = "profile1/reup.meta.json"
        publish_handoff_published_status(self.status_root, first, fingerprint, handoff_id, handoff_ref)
        publish_succeeded_status(self.status_root, first, fingerprint, handoff_id, handoff_ref)
        client = Client(second["reup_job_id"])
        with self._configured(), \
             patch.object(p1c_worker, "recover_reup_completion", return_value="succeeded") as recover, \
             patch.object(p1c_worker.LeaseBridgeClient, "from_environment", return_value=client), \
             patch.object(p1c_worker, "launch_processing_child", return_value=Handle()) as launch:
            report = p1c_worker.run_canonical_once()
        self.assertEqual(second["reup_job_id"], report["reup_job_id"])
        self.assertEqual(second_path, launch.call_args.args[1])
        recover.assert_called_once()

    def test_explicit_failed_job_is_not_reprocessed(self) -> None:
        path, job = self._write_job(1)
        self._accept(path, job)
        self._start(job)
        publish_failed_status(self.status_root, job, "PROCESSING_FAILED", "failed")
        with self._configured(), \
             patch.object(p1c_worker.LeaseBridgeClient, "from_environment") as bridge, \
             patch.object(p1c_worker, "launch_processing_child") as launch:
            report = p1c_worker.run_canonical_once(reup_job_id=job["reup_job_id"])
        self.assertEqual("failed", report["outcome"])
        bridge.assert_not_called()
        launch.assert_not_called()

    def test_lease_lost_old_job_is_not_rerun(self) -> None:
        first_path, first = self._write_job(1)
        second_path, second = self._write_job(2)
        self._accept(first_path, first)
        lease_id = self._start(first)
        publish_lease_lost_status(self.status_root, first, lease_id)
        client = Client(second["reup_job_id"])
        with self._configured(), \
             patch.object(p1c_worker.LeaseBridgeClient, "from_environment", return_value=client), \
             patch.object(p1c_worker, "launch_processing_child", return_value=Handle()) as launch:
            report = p1c_worker.run_canonical_once()
        self.assertEqual(second["reup_job_id"], report["reup_job_id"])
        self.assertEqual(second_path, launch.call_args.args[1])


if __name__ == "__main__":
    unittest.main()
