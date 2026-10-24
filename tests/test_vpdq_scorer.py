from __future__ import annotations

import unittest
import io
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from pipeline.vpdq_scorer import score_video_pair
import watcher


class SampledFrameDHashTests(unittest.TestCase):
    def test_user_facing_labels_do_not_claim_meta_vpdq_or_posting_safety(self) -> None:
        repo_root = Path(__file__).parents[1]
        for path in (repo_root / "watcher.py", repo_root / "pipeline" / "config.py"):
            text = path.read_text(encoding="utf-8").lower()
            self.assertNotIn("meta vpdq", text, path.name)
            self.assertNotIn("safe to post", text, path.name)
            self.assertNotIn("posting safety", text, path.name)

    def test_frame_extraction_failure_remains_unknown_without_posting_safety_claim(self) -> None:
        with patch("pipeline.vpdq_scorer.extract_frame_thumbnails", return_value=[]):
            result = score_video_pair(Path("original.mp4"), Path("processed.mp4"))

        self.assertEqual(0.0, result["similarity_percent"])
        self.assertEqual("UNKNOWN", result["status"])
        self.assertIn("Could not extract sufficient frames", result["message"])
        self.assertNotIn("posting safety", result["message"].lower())

    def test_low_similarity_passed_band_uses_advisory_verdict(self) -> None:
        original_frame = bytes(256)
        processed_frame = bytes(range(16)) * 16
        with patch(
            "pipeline.vpdq_scorer.extract_frame_thumbnails",
            side_effect=[[original_frame], [processed_frame]],
        ):
            result = score_video_pair(Path("original.mp4"), Path("processed.mp4"))

        self.assertEqual(0.0, result["similarity_percent"])
        self.assertEqual("PASSED", result["status"])
        self.assertIn("heuristic only", result["verdict"].lower())
        self.assertNotIn("safe to post", result["verdict"].lower())

    def test_score_cli_prints_unknown_result_without_key_error(self) -> None:
        result = {
            "similarity_percent": 0.0,
            "status": "UNKNOWN",
            "message": "Could not extract sufficient frames for hashing.",
        }
        output = io.StringIO()
        with patch.object(watcher, "score_video_pair", return_value=result), patch.object(
            watcher.sys, "argv", ["watcher.py", "--score", "original.mp4", "processed.mp4"]
        ), redirect_stdout(output):
            watcher.main()

        self.assertIn("UNKNOWN", output.getvalue())


if __name__ == "__main__":
    unittest.main()
