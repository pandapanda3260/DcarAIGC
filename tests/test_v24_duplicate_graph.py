from __future__ import annotations

import json
import time
import unittest
from unittest.mock import patch

from v8 import duplicate_graph as graph
from v8 import duplicate_index as index
from v8.duplicates import FINGERPRINT_VERSION


def fingerprint(cid, *, media=(), text=None):
    return {"content_id": cid, "fingerprint_id": cid, "source_sha256": str(cid),
            "input_revision": 1, "input_status": "available", "fingerprint_version": FINGERPRINT_VERSION,
            "media_sha256_json": json.dumps(media), "frame_phashes_json": "[]", "text_sha256": text,
            "text_simhash": None, "asr_simhash": None, "ocr_simhash": None,
            "published_at": f"2026-01-{cid:02d}", "imported_at": "2026-01-01"}


class DuplicateGraphTest(unittest.TestCase):
    def test_direct_chain_is_projected_to_canonical_without_fabricating_edge(self):
        rows = {cid: fingerprint(cid, media=media) for cid, media in ((1, ["a"]), (2, ["a", "b"]), (3, ["b"]))}
        prepared = index.prepare_fingerprints(rows)
        results = {1: {2: index.compare_prepared(prepared[1], prepared[2])},
                   2: {3: index.compare_prepared(prepared[2], prepared[3])}}
        snapshot = {"generation_id": "test", "pointers": rows, "edges": [], "members": {}}
        delta = graph.build_component_delta(snapshot, results)
        self.assertEqual(set(delta["edges"]), {(1, 2), (2, 3)})
        self.assertEqual(set(delta["projections"]), {(2, 1), (3, 1)})
        evidence = json.loads(delta["projections"][(3, 1)]["evidence_json"])
        self.assertNotIn("cluster_members", evidence)
        self.assertEqual((evidence["best_edge"]["left"], evidence["best_edge"]["right"]), (2, 3))

    def test_checkpoint_preserves_negative_progress_and_reuses_pairs(self):
        rows = {cid: fingerprint(cid, text=str(cid)) for cid in range(601)}
        works = [{"content_id": 0, "target_input_revision": 1}]
        candidates = list(range(1, 601))
        digest = graph.work_digest(0, rows, candidates, 1)
        snapshot = {"fingerprints": rows, "candidates": {0: candidates}, "digests": {0: digest}, "prior": {}}
        first = graph.compute_graph_delta(snapshot, works, time.monotonic() - 1)
        self.assertEqual(first["compared_pairs"], 512)
        self.assertFalse(first["works"][0]["complete"])
        snapshot["prior"] = {0: {other: {"input_snapshot_digest": digest, "comparison": value}
                                 for other, value in first["works"][0]["records"].items()}}
        second = graph.compute_graph_delta(snapshot, works)
        self.assertEqual(second["compared_pairs"], 88)
        self.assertTrue(second["works"][0]["complete"])

    def test_batch_pair_only_computed_once(self):
        rows = {1: fingerprint(1, text="equal"), 2: fingerprint(2, text="equal")}
        works = [{"content_id": cid, "target_input_revision": 1} for cid in rows]
        snapshot = {"fingerprints": rows, "candidates": {1: [2], 2: [1]},
                    "digests": {1: "a", 2: "b"}, "prior": {}}
        result = graph.compute_graph_delta(snapshot, works)
        self.assertEqual(result["compared_pairs"], 1)
        self.assertTrue(all(row["complete"] for row in result["works"].values()))


if __name__ == "__main__":
    unittest.main()
