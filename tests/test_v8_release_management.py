"""The v5.1→v5.2 taxonomy release lifecycle is retired (schema v16).

``evaluation-v8__selling-points-v5.2`` shipped in 2026-08 and
``evaluation-v9__selling-points-v5.2`` has been the active release since.
Schema v16 then deleted the manual review domain that this lifecycle's
freeze/verify chain attests over column by column (``review_id`` and
``pending_review`` are part of every projection hash it compares), so the
chain can never run again.

The previous 12 behavioural tests covered exactly that chain.  They are
replaced by a boundary test pinning the retirement guard: the module now fails
closed with an explicit message instead of an opaque "schema columns are
incomplete", and the guard sits in the single ``_require_v9`` entry every
public operation passes through.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from v8 import release_management as management
from v8.release_management import (
    RELEASE_LIFECYCLE_RETIRED_MESSAGE,
    ReleaseManagementError,
)
from v8.storage import connect, initialize_database


class ReleaseManagementRetirementTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.db = self.root / "release.sqlite3"
        with connect(self.db) as connection:
            initialize_database(connection)

    def test_status_fails_closed_with_an_explicit_retirement_message(self) -> None:
        with connect(self.db) as connection:
            with self.assertRaises(ReleaseManagementError) as raised:
                management._require_v9(connection)
        self.assertEqual(str(raised.exception), RELEASE_LIFECYCLE_RETIRED_MESSAGE)
        self.assertIn("retired", str(raised.exception))

    def test_the_frozen_target_identifiers_are_still_readable(self) -> None:
        # 冻结的历史 manifest/receipt 按名字引用这些常量，保留供审计追溯
        self.assertEqual(
            management.TARGET_RELEASE_ID, "evaluation-v8__selling-points-v5.2"
        )
        self.assertEqual(management.TARGET_TAXONOMY_VERSION, "selling-points-v5.2")
        self.assertEqual(
            management.SOURCE_RELEASE_ID, "evaluation-v8__selling-points-v5.1"
        )


if __name__ == "__main__":
    unittest.main()
