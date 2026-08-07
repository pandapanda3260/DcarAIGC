from __future__ import annotations

import unittest
from unittest import mock

from v8 import release_management as releases  # type: ignore[import-not-found]
from v8.matcher_dsl import V5_2_POINT_SPEC  # type: ignore[import-not-found]


class ReleaseManagementV52Test(unittest.TestCase):
    def test_release_contract_targets_exact_v5_2_point_set(self) -> None:
        self.assertEqual(releases.SOURCE_RELEASE_ID, "evaluation-v8__selling-points-v5.1")
        self.assertEqual(releases.TARGET_RELEASE_ID, "evaluation-v8__selling-points-v5.2")
        self.assertEqual(releases.POINT_IDS, frozenset(V5_2_POINT_SPEC))
        self.assertEqual(len(releases.POINT_IDS), 28)
        self.assertFalse({"C1", "C2", "C3", "C4", "M7"} & releases.POINT_IDS)

    def test_freeze_verification_uses_current_evidence_contract(self) -> None:
        components = {
            "detail_raw_sha256": None,
            "text_sha256": "1" * 64,
            "media_sha256": None,
            "asr_sha256": None,
            "ocr_sha256": None,
            "comments_version_sha256": None,
            "manual_evidence_sha256": None,
        }
        with mock.patch.object(
            releases,
            "_current_evidence_state",
            return_value=({}, components, "2" * 64),
        ):
            frozen, frozen_sha, current, current_sha = (
                releases._freeze_v1_and_current_evidence_state(mock.Mock(), 1)
            )

        self.assertEqual(frozen, components)
        self.assertIsNot(frozen, components)
        self.assertIs(current, components)
        self.assertEqual(frozen_sha, "2" * 64)
        self.assertEqual(current_sha, "2" * 64)


if __name__ == "__main__":
    unittest.main()
