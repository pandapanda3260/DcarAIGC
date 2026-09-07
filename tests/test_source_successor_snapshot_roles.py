"""Exercise real builder/receiver private-reference classification with files."""
from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

from v8 import account_code_successor, capture_code_successor, runtime_source_successor


ROOT = Path(__file__).resolve().parents[1]


def load_module(name, path):
    specification = importlib.util.spec_from_file_location(name, path)
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[name] = module
    specification.loader.exec_module(module)
    return module


builder = load_module("source_roles_builder", ROOT / "scripts/build_server_snapshot.py")
installer = load_module("source_roles_installer", ROOT / "deploy/server/install_snapshot.py")


class SourceSuccessorSnapshotRolesTest(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.project = self.root / "writer-project"
        self.project.mkdir()
        self.private = self.root / "private-evidence"
        self.private.mkdir(mode=0o700)

    def manifest(self, contract=runtime_source_successor.PROOF):
        roles = (runtime_source_successor.PRIVATE_ROLES
                 if contract == runtime_source_successor.PROOF
                 else capture_code_successor.PRIVATE_ROLES)
        references = []
        for role in sorted(roles):
            path = self.private / (role + ".json")
            body = (json.dumps({"fixture": True, "role": role}) + "\n").encode()
            path.write_bytes(body)
            path.chmod(0o600)
            references.append({"role": role, "path": str(path),
                "sha256": hashlib.sha256(body).hexdigest(), "byte_size": len(body)})
        deployment = {"deployment_id": 1, "receipt_sha256": "a" * 64, "evidence": {}}
        proof = {"contract": contract, "private_references": references}
        directory = builder._private_deployment_directory(
            deployment, project_root=self.project, code_successor=proof
        )
        return {"writer_project_root": str(self.project), "deployment_readiness": deployment,
            "code_successor": proof, "private_deployment_references": directory}

    def test_source_seven_roles_cross_real_builder_and_replica_without_private_files(self):
        manifest = self.manifest()
        self.assertEqual(len(manifest["private_deployment_references"]["references"]), 7)
        self.assertIn("code_successor.source_tree", {
            row["role"] for row in manifest["private_deployment_references"]["references"]
        })
        # Mac private envelopes are referenced, never uploaded or read by Ubuntu.
        for path in self.private.iterdir():
            path.unlink()
        index = installer._private_deployment_reference_index(manifest)
        self.assertEqual(len(index), 7)

    def test_all_previous_proof_contracts_retain_exact_six_roles(self):
        for contract in (capture_code_successor.PROOF, capture_code_successor.PROOF_V2,
                         account_code_successor.PROOF):
            with self.subTest(contract=contract):
                manifest = self.manifest(contract)
                self.assertEqual(len(installer._private_deployment_reference_index(manifest)), 6)
                bad = copy.deepcopy(manifest)
                extra = {**bad["code_successor"]["private_references"][0], "role": "source_tree"}
                bad["code_successor"]["private_references"].append(extra)
                with self.assertRaises(installer.SnapshotInstallError):
                    installer._private_deployment_reference_index(bad)

    def test_missing_duplicate_or_unbound_source_roles_are_rejected(self):
        original = self.manifest()
        for mutation in ("missing", "duplicate", "directory-missing", "hash", "size"):
            with self.subTest(mutation=mutation):
                manifest = copy.deepcopy(original)
                proof = manifest["code_successor"]["private_references"]
                directory = manifest["private_deployment_references"]["references"]
                if mutation == "missing":
                    proof[:] = [row for row in proof if row["role"] != "source_tree"]
                elif mutation == "duplicate":
                    proof.append(copy.deepcopy(proof[0]))
                elif mutation == "directory-missing":
                    directory[:] = [row for row in directory if row["role"] != "code_successor.source_tree"]
                elif mutation == "hash":
                    next(row for row in directory if row["role"] == "code_successor.source_tree")["sha256"] = "f" * 64
                else:
                    next(row for row in directory if row["role"] == "code_successor.source_tree")["byte_size"] += 1
                with self.assertRaises(installer.SnapshotInstallError):
                    installer._private_deployment_reference_index(manifest)


if __name__ == "__main__":
    unittest.main()
