from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline.p1c_lease_client import LeaseBridgeResponse, LeaseBridgeUnavailable
import watcher


class WatcherHeavyMediaLeaseTests(unittest.TestCase):
    def test_lease_acquired_and_released_successfully(self):
        fake_admission = LeaseBridgeResponse(
            action="acquire",
            granted=True,
            heartbeat_seconds=15,
            ttl_seconds=60,
            lease={"lease_id": "test-lease-123"},
        )
        fake_release = LeaseBridgeResponse(
            action="release",
            granted=None,
            heartbeat_seconds=15,
            ttl_seconds=60,
            lease={"lease_id": "test-lease-123", "released_at": "2026-09-29T12:00:00Z"},
        )
        client = MagicMock()
        client.acquire.return_value = fake_admission
        client.release.return_value = fake_release

        with patch("watcher.LeaseBridgeClient.from_environment", return_value=client):
            with watcher.acquire_heavy_media_lease() as granted:
                self.assertTrue(granted)
                client.acquire.assert_called_once()
            client.release.assert_called_once()

    def test_lease_capacity_unavailable_yields_false(self):
        fake_admission = LeaseBridgeResponse(
            action="acquire",
            granted=False,
            heartbeat_seconds=15,
            ttl_seconds=60,
            lease=None,
        )
        client = MagicMock()
        client.acquire.return_value = fake_admission

        with patch("watcher.LeaseBridgeClient.from_environment", return_value=client):
            with watcher.acquire_heavy_media_lease() as granted:
                self.assertFalse(granted)
                client.acquire.assert_called_once()
            client.release.assert_not_called()

    def test_lease_bridge_unavailable_gracefully_yields_true(self):
        with patch.object(
            watcher.LeaseBridgeClient,
            "from_environment",
            side_effect=LeaseBridgeUnavailable("not configured"),
        ), patch.object(
            watcher.Path,
            "is_dir",
            return_value=False,
        ):
            with watcher.acquire_heavy_media_lease() as granted:
                self.assertTrue(granted)


if __name__ == "__main__":
    unittest.main()
