from __future__ import annotations

import unittest
from pipeline.trust_classifier import COMMERCIAL_KEYWORDS, PURE_TRUST_PATTERNS, classify_video_intent


class TestTrustClassifier(unittest.TestCase):
    def test_pure_trust_content(self):
        res = classify_video_intent(desc="当代大学生寝室日常", asr_transcript="我和室友的日常生活")
        self.assertTrue(res["is_trust"])
        self.assertEqual(res["verdict"], "APPROVED_PURE_TRUST")

    def test_commercial_content_rejected(self):
        res = classify_video_intent(desc="点击左下角小黄车", asr_transcript="好物推荐，大家快买这个同款")
        self.assertFalse(res["is_trust"])
        self.assertEqual(res["verdict"], "REJECT_COMMERCIAL_KEYWORDS")

    def test_ad_metadata_rejected(self):
        res = classify_video_intent(desc="Video thường", asr_transcript="Không có gì", extra_metadata={"is_ads": True})
        self.assertFalse(res["is_trust"])
        self.assertEqual(res["verdict"], "REJECT_COMMERCIAL_ADS")


if __name__ == "__main__":
    unittest.main()
