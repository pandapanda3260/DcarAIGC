from __future__ import annotations

import json
import sqlite3
import unittest
from unittest.mock import patch

from tests import test_v8_transport_natural_due as due_fixtures
from v8 import durable_runs, providers, tikhub_scan
from v8.storage import connect, transaction
from v8.transport_due_candidates import list_primary_due_candidates
from v8.transport_natural_due import NaturalDueError, validate_natural_due_request


AT = due_fixtures.AT
REFERENCE = due_fixtures.REFERENCE


class TransportDueCandidatesTest(unittest.TestCase):
    # Reuse fixture helpers without inheriting and rerunning the source suite.
    setUp = due_fixtures.TransportNaturalDueTest.setUp
    account_row = due_fixtures.TransportNaturalDueTest.account_row
    roster = due_fixtures.TransportNaturalDueTest.roster
    activate = due_fixtures.TransportNaturalDueTest.activate
    activation_identity = due_fixtures.TransportNaturalDueTest.activation_identity
    claim_round = due_fixtures.TransportNaturalDueTest.claim_round
    _scan_claims = due_fixtures.TransportNaturalDueTest._scan_claims

    def round_identity(self, snapshot, *, key, at):
        identity = due_fixtures.TransportNaturalDueTest.round_identity(
            self, snapshot, key=key, at=at
        )
        with connect(self.db) as connection:
            identity["eligible_identity_ids"] = [
                row[0]
                for row in connection.execute(
                    "SELECT account_identity_id FROM account_roster_members "
                    "WHERE snapshot_id=? ORDER BY account_identity_id",
                    (snapshot["id"],),
                )
            ]
        return identity

    def _posts_claims(self):
        claims = self._scan_claims()
        self._set_reference(claims[2], self.identity_id, REFERENCE)
        return claims

    def _set_reference(self, claim, identity_id, reference):
        with connect(self.db) as connection, transaction(connection):
            durable_runs.checkpoint(connection, claim, {"reference": reference}, now=AT)
            connection.execute(
                "INSERT INTO account_provider_references(account_identity_id,provider,"
                "reference_kind,reference_value,created_at,updated_at) "
                "VALUES (?,'TikHub','sec_user_id',?,?,?)",
                (identity_id, reference, AT, AT),
            )

    def _enumerate(self, *, at=AT):
        # SQLite itself rejects writes, in addition to checking unchanged data.
        with sqlite3.connect(self.db.as_uri() + "?mode=ro", uri=True) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only=ON")
            before = list(connection.iterdump())
            with (
                patch.object(
                    durable_runs, "claim_run", side_effect=AssertionError("claim")
                ),
                patch.object(
                    tikhub_scan, "_provider_call", side_effect=AssertionError("network")
                ),
                patch.object(
                    providers, "_load_key", side_effect=AssertionError("credentials")
                ),
            ):
                result = list_primary_due_candidates(connection, at=at)
            self.assertEqual(connection.total_changes, 0)
            self.assertEqual(list(connection.iterdump()), before)
            return result

    def test_deterministic_order_full_proof_and_read_only(self):
        second_account = self.account_row("099999999999")
        _parent, _parent_identity, child, identity = self._posts_claims()
        with connect(self.db) as connection:
            second_identity = dict(
                connection.execute(
                    "SELECT * FROM account_platform_identities WHERE account_id=?",
                    (second_account["id"],),
                ).fetchone()
            )
        state = durable_runs.get_run(child.scheduler_run_id, db_path=self.db)[
            "details"
        ]["checkpoint"]
        second_child = durable_runs.claim_run(
            "tikhub_reconcile",
            {
                **identity,
                "account_id": second_account["id"],
                "identity_id": second_identity["id"],
                "uid": second_identity["uid"],
            },
            db_path=self.db,
            now=AT,
            initial_checkpoint=state,
        )
        assert second_child is not None
        self._set_reference(second_child, second_identity["id"], REFERENCE + "s")

        first = self._enumerate()

        self.assertEqual(first, self._enumerate())
        self.assertEqual(
            [item["proof"]["account_uid"] for item in first],
            ["099999999999", "100000000001"],
        )
        self.assertEqual(
            first[0]["scope"].scheduler_run_id, second_child.scheduler_run_id
        )
        for candidate in first:
            self.assertEqual(
                set(candidate), {"scope", "request_identity", "stage", "proof"}
            )
            self.assertEqual(candidate["stage"], "discovery")
            self.assertEqual(candidate["proof"]["operation"], "douyin_user_posts")
            self.assertEqual(
                candidate["proof"]["request_document"],
                candidate["request_identity"].document,
            )
            with connect(self.db) as connection:
                self.assertEqual(
                    candidate["proof"],
                    validate_natural_due_request(
                        connection,
                        scope=candidate["scope"],
                        request_identity=candidate["request_identity"],
                        stage="discovery",
                        at=AT,
                    ),
                )

    def test_history_unrelated_and_nonrunning_rows_are_excluded(self):
        _parent, _parent_identity, child, identity = self._posts_claims()
        for job, purpose in (
            ("history_recovery", "history"),
            ("unrelated", "reconcile"),
        ):
            durable_runs.claim_run(
                job, {**identity, "purpose": purpose}, db_path=self.db, now=AT
            )
        self.assertEqual(len(self._enumerate()), 1)
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                "UPDATE scheduler_runs SET status='partial',details_json='{}' WHERE id=?",
                (child.scheduler_run_id,),
            )
        self.assertEqual(self._enumerate(), [])

    def test_missing_reference_and_pending_raw_are_not_list_candidates(self):
        _parent, _parent_identity, child, _identity = self._scan_claims()
        self.assertEqual(self._enumerate(), [])
        self._set_reference(child, self.identity_id, REFERENCE)
        with connect(self.db) as connection, transaction(connection):
            durable_runs.checkpoint(
                connection, child, {"pending_raw": {"raw_response_id": 1}}, now=AT
            )
        self.assertEqual(self._enumerate(), [])

    def test_future_and_previous_day_runs_are_ineligible(self):
        _parent, _parent_identity, child, _identity = self._posts_claims()
        self.assertEqual(self._enumerate(at="2026-08-30T00:12:00Z"), [])
        with connect(self.db) as connection, transaction(connection):
            details = durable_runs.assert_owner(connection, child)
            details["next_resume_at"] = "2026-08-29T00:30:00Z"
            connection.execute(
                "UPDATE scheduler_runs SET details_json=? WHERE id=?",
                (json.dumps(details), child.scheduler_run_id),
            )
        self.assertEqual(self._enumerate(), [])

    def test_forged_cursor_fails_closed(self):
        _parent, _parent_identity, child, _identity = self._posts_claims()
        with connect(self.db) as connection, transaction(connection):
            durable_runs.checkpoint(
                connection, child, {"cursor": {"forged": 1}}, now=AT
            )
        with self.assertRaises(NaturalDueError) as caught:
            self._enumerate()
        self.assertEqual(caught.exception.code, "natural_due_cursor_invalid")

    def test_forged_owner_fails_closed(self):
        _parent, _parent_identity, child, _identity = self._posts_claims()
        with connect(self.db) as connection, transaction(connection):
            details = durable_runs.assert_owner(connection, child)
            details["owner"]["token"] = "forged-owner"
            connection.execute(
                "UPDATE scheduler_runs SET details_json=? WHERE id=?",
                (json.dumps(details), child.scheduler_run_id),
            )
        with self.assertRaises(NaturalDueError) as caught:
            self._enumerate()
        self.assertEqual(caught.exception.code, "natural_due_owner_invalid")

    def test_forged_reference_is_not_silently_dropped(self):
        _parent, _parent_identity, child, _identity = self._posts_claims()
        with connect(self.db) as connection, transaction(connection):
            durable_runs.checkpoint(
                connection, child, {"reference": REFERENCE + "forged"}, now=AT
            )
        with self.assertRaises(NaturalDueError) as caught:
            self._enumerate()
        self.assertEqual(caught.exception.code, "natural_due_reference_invalid")
