from __future__ import annotations

import os
import json
import subprocess
import sys
import unittest
from contextlib import nullcontext
from unittest.mock import patch
from pathlib import Path

from pipeline.config import PipelineConfig


class ExecutionLimitTests(unittest.TestCase):
    def test_profile_environment_prefers_h264_qsv_and_one_worker(self) -> None:
        with patch.dict(
            os.environ,
            {"DUBVI_REUP_ENCODER": "h264_qsv", "DUBVI_REUP_WORKERS": "1"},
            clear=False,
        ):
            config = PipelineConfig()

        self.assertEqual(["h264_qsv", "hevc_qsv", "libx264"], config.preferred_encoders)
        self.assertEqual(1, config.max_workers)

    def test_disposable_output_root_relocates_processing_failed_and_cache_paths(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        with self.subTest("override"):
            runtime = self._fresh_runtime_config(repo_root, "C:/dubvi-disposable-runtime")
            runtime_root = Path("C:/dubvi-disposable-runtime")
            self.assertEqual({
                "processing": str(runtime_root / "processing"),
                "failed": str(runtime_root / "failed"),
                "cache": str(runtime_root / "processed_history.json"),
                "output": str(runtime_root),
            }, runtime)

        with self.subTest("legacy defaults"):
            runtime = self._fresh_runtime_config(repo_root, None)
            self.assertEqual({
                "processing": str(repo_root / "processing"),
                "failed": str(repo_root / "failed"),
                "cache": str(repo_root / "processed_history.json"),
                "output": str(repo_root / "output"),
            }, runtime)

    def test_workers_zero_is_rejected_during_argument_parsing(self) -> None:
        import watcher

        with patch.object(watcher.sys, "argv", ["watcher.py", "--batch", "--workers", "0"]), patch.object(
            watcher, "_watcher_instance_lock", return_value=nullcontext(True)
        ), patch.object(watcher, "run_batch") as run_batch:
            with self.assertRaises(SystemExit) as raised:
                watcher.main()

        self.assertEqual(2, raised.exception.code)
        run_batch.assert_not_called()

    @staticmethod
    def _fresh_runtime_config(repo_root: Path, output_root: str | None):
        env = os.environ.copy()
        env.pop("REUP_OUTPUT_DIR", None)
        if output_root is not None:
            env["REUP_OUTPUT_DIR"] = output_root
        script = """
import json
from pipeline.config import config
from pipeline.cache_manager import CACHE_FILE
print(json.dumps({
    'processing': str(config.processing_dir),
    'failed': str(config.failed_dir),
    'cache': str(CACHE_FILE),
    'output': str(config.output_dir),
}))
"""
        process = subprocess.run(
            [sys.executable, "-c", script],
            cwd=repo_root,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
        return json.loads(process.stdout)


if __name__ == "__main__":
    unittest.main()
