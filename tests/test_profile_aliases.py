"""Approved two-page Reup profile alias and propagation regressions."""

from pathlib import Path
import unittest

from pipeline.config import config
from pipeline.profile_manager import get_channel_profile


class ApprovedProfileAliasTests(unittest.TestCase):
    def test_page_aliases_resolve_exact_recovery_assets(self) -> None:
        page_a = get_channel_profile("page_goc_tro_bat_on")
        page_b = get_channel_profile("page_giai_cuu_chuong_lon")
        self.assertEqual("page_goc tro bat on", page_a.profile_dir.name)
        self.assertEqual("mask_hang_den.png", page_a.watermark_path.name)
        self.assertEqual("mask_hang_trang.jpg", page_b.watermark_path.name)
        self.assertNotEqual(page_a.watermark_path, page_b.watermark_path)

    def test_aliases_share_safe_baseline_and_legacy_ids_keep_folder_lookup(self) -> None:
        page_a = get_channel_profile("page_goc_tro_bat_on")
        page_b = get_channel_profile("page_giai_cuu_chuong_lon")
        legacy = get_channel_profile("page_goc tro bat on")
        self.assertEqual(page_a.speed_range, page_b.speed_range)
        self.assertEqual((1.08, 1.15), page_a.speed_range)
        self.assertEqual(page_a.zoom_range, page_b.zoom_range)
        self.assertFalse(page_a.enable_hflip)
        self.assertEqual("page_goc tro bat on", legacy.profile_dir.name)
        self.assertEqual("mask_hang_den.png", legacy.watermark_path.name)

    def test_default_profile_keeps_legacy_defaults(self) -> None:
        profile = get_channel_profile("default")
        self.assertEqual("default", profile.name)
        self.assertFalse(profile.enable_hflip)

    def test_mapping_is_explicit_and_assets_are_present(self) -> None:
        for profile_id, record in config.approved_profile_assets.items():
            asset_dir = config.assets_dir / record["directory"]
            watermark = asset_dir / record["watermark"]
            self.assertTrue(asset_dir.is_dir(), profile_id)
            self.assertTrue(watermark.is_file(), profile_id)


if __name__ == "__main__":
    unittest.main()
