from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline.processor import ProcessResult
import watcher


class WatcherErrorIsolationTests(unittest.TestCase):
    def setUp(self):
        self.config = SimpleNamespace(
            layout_mode="crop_fill",
            input_dir=ROOT / "input",
            ensure_dirs=MagicMock(),
        )

    def test_batch_continues_after_one_future_raises(self):
        bad = Path("bad.mp4")
        good = Path("good.mp4")

        def process(path, *args):
            if path == bad:
                raise RuntimeError("one item failed")
            return ProcessResult(success=True, input_file=path)

        with patch.object(watcher, "config", self.config), patch.object(
            watcher, "cleanup_stale_processing"
        ), patch.object(watcher, "scan_input_files", return_value=[bad, good]), patch.object(
            watcher, "process_single_video", side_effect=process
        ) as process_mock:
            watcher.run_batch(max_workers=2)

        self.assertEqual({bad, good}, {call.args[0] for call in process_mock.call_args_list})

    def test_daemon_polls_again_after_one_future_raises(self):
        bad = Path("bad.mp4")
        scans = [[bad], []]

        with patch.object(watcher, "config", self.config), patch.object(
            watcher, "cleanup_stale_processing"
        ), patch.object(watcher, "scan_input_files", side_effect=scans) as scan_mock, patch.object(
            watcher, "process_single_video", side_effect=RuntimeError("one item failed")
        ), patch.object(watcher.time, "sleep", side_effect=[None, KeyboardInterrupt]):
            watcher.run_watch_daemon(poll_interval=0, max_workers=1)

        self.assertEqual(2, scan_mock.call_count)


if __name__ == "__main__":
    unittest.main()
