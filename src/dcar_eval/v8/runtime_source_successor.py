"""Reviewed source-isolation successor; existing user/paid authority is retained.

Source directories and source manifests are immutable release inputs. The data
project, formal database, writer lease, activation and original acceptance stay
unchanged. Empty review pins fail closed and cannot be supplied by a caller.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Mapping

from . import account_code_successor as account, capture_authorizations as auth, forward_recovery

PLAN = "writer-source-isolation-successor-plan-v1"
DECISION = "writer-source-isolation-successor-decision-v1"
PROOF = "writer-source-isolation-successor-proof-v1"
TRANSITION = "writer-source-isolation-20260907-v1"
SOURCE_CONTRACT = "writer-source-tree-v1"
PRIVATE_ROLES = frozenset({"plan", "previous_build", "source_archive", "source_tree", "full_checks", "build", "runtime"})
PARENT_BUILD_SHA256 = "419753a1df3fd6c866864d75c081d3e989da75932fa279c6b29e9975d5583175"
MODULE = "src/dcar_eval/v8/runtime_source_successor.py"
_LOADED_SOURCE = Path(__file__).read_bytes()
# Populated only from the independently reviewed final release delta.
SOURCE_TRANSITIONS: dict[str, tuple[str | None, str | None]] = {
    'app/web/app/components/AppShell.tsx': ('b5db3fbd560c2ebe5ae98ec1b2dcba062994121889081d7f86e2ea5977e2e98e', 'febbe4fe394804a771c38e570da1ae31fa25cdc33a9049738e13a04ff9668f97'),
    'app/web/app/components/ContentUpdateJobsProvider.tsx': ('8015fe92563b34dcbc373d51b9d8d5b5867e17a1dd2b8b33197910421ab84f9b', 'a0b890e3debab4b4e93dbf10d01dd679cb513892837b6e9015f0cb526c11c84c'),
    'app/web/app/contents/ContentsPage.tsx': ('1de8115fa9dc7848cb973af3b15fc7914c0047710a95de0be78cf06aea380e3b', '19c56e7c891241b5157249128bc5ff003cf3ba175fa7d8b1e857f495cd59db1b'),
    'app/web/app/contents/contentUpdateJobs.ts': ('caa0be6ee3aa5a0a866c4dd154affcc1e4f8d8bad3fdb4ba90350c551e6214c2', 'f0cf87840cec6b57b2d4c01e86d85faf54c20962de8fb17aebce1575002f00ef'),
    'app/web/app/globals.css': ('2ab6cfe824759df46ed1abe6136a1c370d55b0de45f1944d9a17915c4c18340a', 'dcc006cb5f736ffa5b73bc8947fd4bee20c29a4dd5155018849a7a896cf61ff6'),
    'app/web/app/lib/contentThumbnailServer.ts': ('5fb416896d207181200e12a9831fbeaa7a6ba0dbc04654a51bf3127f178a7272', '861f5131a9c5a8f3dc0acb2aea8632b00c39e2894f089a5d499928357a6d43bc'),
    'app/web/app/lib/queries.ts': ('ee0c744dbc67362cdae0ef56f53cbe44657ee810acaf0fe4b6dd532d8ad76658', '1202d3ac519c6c85ee91d7f3aca13a0b3c2de24ffecc940291ca2d7387d008cb'),
    'app/web/app/lib/serviceStatus.ts': ('e90d27c963e159e5d9c10f6ba53fb8d279d7a326912272a93e9b905e1cfb982f', '8063b8ccc56c36050b5ca75d81c60c669b4b308ac3f59d06643030c09d54540f'),
    'app/web/app/users/UsersPage.module.css': ('8777df57cd699e72ece853b8ce4bcf54b75fe1157108af33fa0003bccd9e81f7', '70c4ad35e5f070520f99781331b864eb21d9f4dd10a2d39e7bd0a12a9d0ebbf2'),
    'app/web/app/users/UsersPage.tsx': ('119c7527b0df60247dc010a6e1cfa1db2ce59d7adce4c77b389c73f207d7b912', '1cc4d233e21be5c0a3ee264ce3e4e3ec214e57a58ab98be48d165320e7b4fe82'),
    'app/web/server/content_thumbnails.py': ('ff4d3283876e56b18e85c1af5902f70c9474b839fd46a59352aa2bd8d08de9ca', '55714de8e90c3c1e0609de2ae5209cc8859727fdc07ef81591df7611c7c043ae'),
    'app/web/tests/content-thumbnails.test.mjs': ('a84023e5fdd5a008b2e90fce71979f234104c637c29da7981b12496d610bd6c0', 'ac743107ab97d58558adde33edcd5d602a2d62c9ab1a21d0f3e18ef01d46b3f9'),
    'app/web/tests/content-update-jobs.test.mjs': ('74aa92273f20aed362e3a5ef8d8f9e164dd60fa0efe5a6836a5cc07a83209bd8', '079fd8f6c4445de92d9c65231ec3e1bd15fbba6f7ecda7ed842358be79159396'),
    'app/web/tests/page-header.test.mjs': ('538d9dbc171bfadacd14a6c60d382687519b5851b2838103a6b2d40efe9667e7', '5dcfeed23526b146eae4b9712f60ad5d2e2dbc849017f429c004ded27f14d26c'),
    'app/web/tests/rendered-html.test.mjs': ('82dc1b641e7496d6dcc5ea83feb08e895baf66d6a25d8016b15e700e04f719b4', '6eced814342c8b3801326643a024689dd32729547f0cd2e35fd26c22972f0fa6'),
    'app/web/tests/service-status.test.mjs': ('29c3d588e2ef90d726ebac5b295bb2e9a0e143a5975f63846e339565637bc365', '91e526e5e02d6a1ef50584fadb40ba6dc16f6fb49722a1a0c26c5196d507266f'),
    'deploy/macos/README.md': ('499889d2d1419c96917162f37fc364e4215e76f7c41a6964edb3f52c928b343e', '4ceee851663ab7a534e94eb01cfe7a88f30dcaf97a19b8a9cad617edae6a2b5e'),
    'deploy/macos/publish_snapshot.py': ('4ecc0dd9939684b47ef9c761b823b28bf5bb10d4b135aad7d9744f56c8bf7c0d', 'c1099490971198d83a2b54de9ed473578e12a25b43faf3a19a286673c28030aa'),
    'deploy/macos/render_snapshot_publisher.py': ('87f56cebddee2df16b058dfe9d180bab4135aa1964d559c8038f79356ab6f500', 'de7bf3bfcf2565365e52f82b33273a33edd8158ae543e09fb8c96387b5ccecb5'),
    'deploy/macos/run_snapshot_publisher.sh': ('fc91e2696ed7aa5fdee0a766f95eae3168d3359b9da31885439864806b860348', 'a1140b3a5355ee71df1ba023796ce6d7a93cc7c6a1f0cfe3fe4dad446b6f49b9'),
    'deploy/macos/run_writer_worker.sh': ('9460a7396130c4d1845c876a11bbb7cb0d2379f9236f44ffc1fa4d1f34d89eb4', 'c16804736363bfadea36f4e835999ff4f9c1cc1b92f8603db7cb36c47ff9e24b'),
    'deploy/server/install_snapshot.py': ('0e3bc1c38c23faee8a895bebcb62ee6f6151ef5198fe5c40e0b9d3da32007b8e', '91d5e6f3d811594c7e8ea0908acb99ae6bb4b84e6ae688e8259710cce47f1edd'),
    'deploy/server/systemd/dcar-web.service': ('f3e7cffc89fb3c681ac0321417325b53775e9b8aefc780ef0d00a2d7b83cbb75', 'ee971c9255a5a60d342b3e1a1f8534033c414f4ff4062a9955a2db6bfb6b2f88'),
    'scripts/issue_v20_code_successor.py': ('fccd9965419893c6304f386e764be8a478d538f3ce27ae75bfc2c1d2b4ea928a', '09fbb450e5fc030289efadefc95ad8f7de44630fa020e1ba1e0ecce8c35279aa'),
    'scripts/seal_r0_receipts.py': ('a19efaf07994c23977bb55704bdec90a41dcdc5c91df4da6ab9a1541dd74de0d', 'a62683c81ccd003d24b73af5aabea6b67f7a8516aeac8eef415b7b4238242503'),
    'scripts/writer_database_safety.py': ('fac2795abc44f57468fad8aa49d1c0e69919989daf3e11ce5fb6b0bbf9d092ce', 'a9973c640e0635b4a5c762e3ea8893a84193db9ab2555df463c6d3c42128f668'),
    'src/dcar_eval/project_paths.py': ('712b154f825be7f9c42088714c8dcb3769120bf6724e56f6aadc0d8051d1ecc2', '0ea49ca970fa8448732e1284a7d7e913ff6d1ac95c47f98fd4cdb8f38f598182'),
    'src/dcar_eval/v8/account_code_successor.py': ('00c576dc7a82ca917794e90e4abfa8a70243cede36e7009569b35df49260ba76', 'c3a2e005e7545dd9f40f7f854599b6e0ad12c12cb52e0df3d633bff9efeb2f79'),
    'src/dcar_eval/v8/api.py': ('a49aad7a5b5e3267539043343386519ae65534b95c19bab92a15cef251d289a2', '9e25490fd77c49c9dfc7bde9f7b0fcb7f4fd3bd81010582ac9ae4aa651be33ce'),
    'src/dcar_eval/v8/artifact_paths.py': ('2da08694f99135f754d2a0fd3ca62f6a37f5173c457d31f4b69d0ecf20731220', '7351d16c8041c75bf0330ef4b8f58165103908d65ec72be1d4285699fc331d78'),
    'src/dcar_eval/v8/audience_rate.py': ('9174f1c4222f7d1a3fb6b08ddf1a8d89105a7f70dae75ed179d84c96c01394b8', 'b7cc1f15f23b36410f6cc88f14901e89eece8968858ac4856f42092a78ecd9a1'),
    'src/dcar_eval/v8/capture_code_successor.py': ('3b85febc853c0bef0ccc259cbb0828998f4bacf0d62343c079db5f43f497cc41', '44fd726f5086abe575f1c1ea3fee63a6dc3856e046003ce2941d0c06613f5743'),
    'src/dcar_eval/v8/capture_release.py': ('2a76d5391a593d40cbff6fa119249764415865e98c0b24521a0b4e8020658bb5', 'dad336ed3f7be15e90e1f83e0414f6859859ba5f0c5a91c5cb5304c1153f2bea'),
    'src/dcar_eval/v8/capture_release_commands.py': ('5ea063776b35e70e08c00b89908a1f5b6069dd64d7bc739367606a61238bf272', 'fb7a5ea34a13ed9fde68f524f4746a59d0570c6181621931a0aefcc6b35891d3'),
    'src/dcar_eval/v8/contracts.py': ('2aac3c7957a6bd725da41d2d34f192249457600cd33224bb29627ae0d1ddfb99', '933327baac24ca2eb46752cbc874b20d68717e6f45c352a04d90c47376c1b9f8'),
    'src/dcar_eval/v8/duplicates.py': ('e3cfeb975c54193d3dd719d4072c2506db2eb097b3115c3d6256687c91c35816', '2cf18eddde5d14665894eaeb6f9e41f876633e49ff298d2efac003120acd11f6'),
    'src/dcar_eval/v8/forward_recovery.py': ('2c718705b901ddb443e581c5fb7ce1773b052f030c8d4a04a5e9e4992aee934f', '36d7e77e4063f7b28cda6bb67f048ecc123dbe4395c91fa92771201c7a3f4cae'),
    'src/dcar_eval/v8/media.py': ('7d5644445db5899dcf7d5c098fe5b48804c4834960cb05814a9ea2a238bb7b85', '07cbd1ffc169a2cbb02b7a287a1e0877265757470ed406912e1180434c9e4770'),
    'src/dcar_eval/v8/media_consumer_proofs.py': ('e638986e3f48bd86297041f0e6c298390965dc9447ecefbca710ddd0ea5dbeb8', '1b4217746c142c29ef105d4ab779f764fd23f4430835e9533b055dd48c08d8fb'),
    'src/dcar_eval/v8/raw_archive.py': ('2c1ae94d745486a157ca589eea8b694b213db83de659445a72b25b3bf9521630', 'edc09ca5e002bf656f29302c2151b04208169b03fc4f78f8561acb1ae06ccb5d'),
    'src/dcar_eval/v8/release_management.py': ('ddba6ab15fb8991438403f3f7febeca0e9f6e1472e379b7dfff15bbc886241a7', '6e50d05f8c0e19b227de9c586fe6bd2e747fa0cfd1780a253a74a75307a94bf4'),
    'src/dcar_eval/v8/release_management_v5_1.py': ('f5ce7d6045fd33cb1e2077da628fd6f1ba17e9120da86881441485dc5a91442c', 'f4001daef28b5831235541df1d0b5bd20f00f94bc97a02893ac9de2c23c67083'),
    'src/dcar_eval/v8/runtime_database.py': ('ef72e8f561eb2d171fb6988ee1d9524111a175a80f7c81693045d226bce4ea62', '3e5eef60394004fee39d4d2d3b7bcd0c3bde818ab4d0ebac603426bf40747215'),
    'src/dcar_eval/v8/runtime_paths.py': (None, 'd6a8e2eed70334a762960c363e2b152076d9d0e683a6818d069f16bf0593aad6'),
    'src/dcar_eval/v8/selling_point_offline.py': ('ba6572f81e140520455bb67fe7a862c754fcc9d405155722ee0e43760d80f24b', '912c647ce6ca75c061cdea629f2275a526d5d59389e5f09ab63ff7104c850842'),
    'src/dcar_eval/v8/snapshot_sync.py': (None, '02615aa94e4c22f040820f2e886396207107c40582f23c63f8c2a5c75c205f64'),
    'src/dcar_eval/v8/spu_catalog_import.py': ('32f4714b63fa33b68cd570c3242829e3ad7fc2a98064e625877962a2bb8304a5', '529870e3d3fd9fbd2aacc2e1f6ba67fedbac855fd400f16627713c300d8c94e5'),
    'src/dcar_eval/v8/storage.py': ('0b80ddbbfb0a5d26678d14d90300108ff63fce3b3c3740ab930e7d2eb6954ea9', '31fac55f9fdf7bca796a97659e1d652cc252b360052813713ec5dd3f74af5307'),
    'tests/fixtures/account_status_schema19_reviewed_sources.tar.gz': (None, '9c96c56221b3c4f2904021563d41976e63917cbbe76b90536f9ea206b195c79f'),
    'tests/test_auth_deployment_contract.py': ('bb5d1e76a07758a29a70ee5f760104dfa3e807934a31f3a2d6d98812b4bef296', 'd6e69076a14f0e40ca190bc93547465434e5580c8f42fc647003e56edcb0a67b'),
    'tests/test_dcar_auth_gateway.py': ('3d64db42fa077783ab50dc9fd092879a958a9fb74fe599f998bd1f60fa904a9d', '36bae3dd400a2ab221af8e91769d077b48f78c99018ca30a6d0234c43f1651ac'),
    'tests/test_issue_v20_deployment_receipt.py': ('044a85c6d060688884b64b139f9ac828b714b63cda65c8b5ecbe166887bd280a', '2a11056ffe11a996c63d1e93bbbbb081cee1228f04ab7673658b11f97666a7be'),
    'tests/test_macos_source_runtime_wrappers.py': (None, '976d8bae340fc0663d495cc91a683d62cce8b87e3f1c54aedc6baff1d1eaa823'),
    'tests/test_matrix_snapshot_publisher.py': ('42089c6624a1054a046bada231217f20a9fd9b49c72d47e2d0a74524106aeb5e', '766978bc8d3161e858992710d380af7fffc151b71253e24bef64f8b63e46c5b0'),
    'tests/test_server_schema_upgrade.py': ('53479a7f0d32bbbe72c8aa25dc78b374a3c53bab973db9b3830233743a3760b0', '6a216819d6414725d99a0c567ae7e05a47ff57b2a2eeaeab65e3ecde2d9f9ce5'),
    'tests/test_server_snapshot_deployment.py': ('85c5e9de373763eea93b133cf9b7bafbe9765085231b22fa3fe16bac92afe086', '041fed539534c5f4d858fb9e5b7ae987ddd685330865dba51ea3380fce334139'),
    'tests/test_source_successor_snapshot_roles.py': (None, 'd2777fbe4aa620798a2f1c0306b700cb9fc618d41ef6c2dc66918bd896c85549'),
    'tests/test_v10_schema_migration.py': ('cae4ff92efe54c96e2495e88591d1a51817f939bb29b8d4a8443e0256f2f82c7', 'bf2635d1286693faa10d6f6a49b09ade3340e684fec15bf3202c1b1bfef8204a'),
    'tests/test_v12_schema_migration.py': ('96958adcf83428e33e250c35068ec6b4e78cb01d5716ae6925e9d0eabea8336b', '52ff9a6e49df36e19d81bcbaaf62f20e1a2f068d3cfe01003d875c2269f51d00'),
    'tests/test_v13_schema_migration.py': ('a6f3ec2afc4b65d7b160d421e5ae7307afcd6c9dbbe6143fae705698842ecb9d', '2f814048d84ce02b83355092e45cadec637b8cbcdf541e20c835134a659d4c00'),
    'tests/test_v20_release_tools.py': ('edef35590c44b5ac0234e02121aa90249cff92864940d0cbb326f98dce49c4e6', 'a37eb11d9cbe8c72336f395657416c651bd267c13267949c1673134d8ab66208'),
    'tests/test_v8_account_code_release_integration.py': ('da2a3e7aeba2f11b8348320029208882a91575627d70b7674ad844a869748c89', 'aa998c83f461678dec10967f129c1982157eb62e3a8b591f6e1779c124d25fec'),
    'tests/test_v8_account_code_successor.py': ('c69fb726c46893d5ae5ec572e459340697f58ef75683b916d0df83844c015c56', '4e50e721756fc2c83167e97b86599fa8d4ec4dda3a29895c3631a424b1d09658'),
    'tests/test_v8_forward_build_successor.py': ('1479596a1a586157e2ac442bc20e553d5cd64b26bbc3305f5261d4504ca4b6d0', 'f186f0834ad0ebdcf001d2d2f1692475c8c939218135d818e1362b4c68364799'),
    'tests/test_v8_runtime_paths.py': (None, '0e34f1420298a4af819ad12db7a60cfc01646118c08123152ed6f385520388c7'),
    'tests/test_v8_runtime_source_successor.py': (None, '2c4ec4d5b456599b0a082d28f4a48bba28cad603760a7173cdf8bc7259d562eb'),
    'tests/test_v8_service_status_freshness.py': (None, 'fb01abdff03534694fa21c791be247b951478251f373e797e9d1f92b8825ca67'),
    'tests/test_v8_snapshot_sync.py': (None, '737e75a25a88e21685961597c8725c687027cf1a0c2307a70023342cf1fbf64e'),
    'tests/test_v8_source_preimport.py': (None, '74f336b2baa2b105d00d47a3a23608e0015a190ce17e9731ccd68ef177671007'),
    'tests/test_web_thumbnail_deployment_contract.py': (None, '4d5bebfbb6d6d2d2426918c65c00a2c2118f3e4714511a1cdaf42ddaaa7e1b55'),
    'tests/test_web_thumbnail_stream_security.py': (None, '8ddefaa1ed3d2c2a0ebc43e749d06475aa39fcc39bbca95e925a421b3c7eb6d3'),
}
REQUIRED_CHECKS = frozenset({"writer_source_backend", "writer_source_frontend", "writer_source_lint",
    "writer_source_typecheck", "writer_source_ruff", "writer_source_mypy", "writer_source_successor",
    "writer_source_bootstrap", "writer_source_publisher"})
MAX_SOURCE_BYTES = 512 * 1024 * 1024
MAX_MANIFEST_BYTES = 16 * 1024 * 1024


def _code():
    from . import capture_code_successor
    return capture_code_successor


def require(value: bool, message: str) -> None:
    if not value:
        raise auth.AuthorizationError("writer_source_successor: " + message)


def _sha(body: bytes | None) -> str | None:
    return hashlib.sha256(body).hexdigest() if body is not None else None


def _name(name: Any) -> str:
    require(isinstance(name, str) and bool(name) and not any(c in name for c in ("\0", "\n", "\r", "\\")),
            "invalid source name")
    require(not name.startswith("/") and all(p not in ("", ".", "..", ".git") for p in name.split("/")),
            "source path escapes its release")
    return name


def _git(root: Path, *args: str) -> bytes:
    from .runtime_paths import verified_git
    return verified_git(root, *args)


def _file(path: Path, *, private: bool = False, limit: int = MAX_SOURCE_BYTES) -> tuple[bytes, int]:
    before = path.lstat()
    require(path.is_absolute() and path.resolve(strict=True) == path and stat.S_ISREG(before.st_mode)
            and before.st_nlink == 1 and before.st_uid == os.geteuid() and not before.st_mode & 0o022
            and (not private or stat.S_IMODE(before.st_mode) == 0o600) and 0 <= before.st_size <= limit,
            "source or receipt file is unsafe")
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as stream:
        body = stream.read(limit + 1)
        opened = os.fstat(stream.fileno())
    after = path.lstat()
    def identity(value):
        return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)
    require(identity(before) == identity(opened) == identity(after) and len(body) == before.st_size,
            "source or receipt changed while reading")
    return body, stat.S_IMODE(before.st_mode)


def _private(reference: Mapping[str, Any]) -> dict[str, Any]:
    body, _ = _file(Path(reference["path"]), private=True, limit=MAX_MANIFEST_BYTES)
    require(_sha(body) == reference.get("sha256")
            and reference.get("byte_size", reference.get("size", len(body))) == len(body),
            "private source reference changed")
    def unique(pairs):
        result = dict(pairs)
        require(len(result) == len(pairs), "duplicate source receipt field")
        return result
    value = json.loads(body, object_pairs_hook=unique)
    require(isinstance(value, dict), "source receipt is not an object")
    return value


def _records(source: Path) -> list[dict[str, Any]]:
    require(source.is_absolute() and source.resolve(strict=True) == source and source.is_dir(),
            "source root is not a canonical directory")
    names = sorted(set(_git(source, "ls-files", "--cached", "--others", "--exclude-standard", "-z").split(b"\0")) - {b""})
    records = []
    total = 0
    for encoded in names:
        name = _name(os.fsdecode(encoded))
        path = source / name
        # Deleted tracked files are represented by the exact Git patch.
        if not path.exists() and not path.is_symlink():
            continue
        body, mode = _file(path, limit=MAX_SOURCE_BYTES - total)
        total += len(body)
        records.append({"path": name, "sha256": _sha(body), "byte_size": len(body), "mode": mode})
    # Ignored executable files may shadow an imported module even though Git
    # status remains clean. Include them in the fail-closed inventory check.
    listed = {row["path"] for row in records}
    for relative in ("src", "scripts", "deploy", "config"):
        folder = source / relative
        if not folder.is_dir():
            continue
        for directory, folders, files in os.walk(folder, followlinks=False):
            for name in folders:
                require(not (Path(directory) / name).is_symlink(), "source directory is a symlink")
            for name in files:
                path = Path(directory) / name
                if path.suffix in {".py", ".pyc", ".so", ".sh", ".json", ".yaml", ".yml", ".toml"}:
                    require(path.relative_to(source).as_posix() in listed, "unlisted executable or configuration source")
    return records


def verify_source_tree(*, source_root: Path, reference: Mapping[str, Any], git: Mapping[str, Any]) -> dict[str, Any]:
    sealer, _ = _code()._tools()
    manifest = _private(reference)
    require(manifest.get("contract") == SOURCE_CONTRACT and manifest.get("source_root") == str(source_root)
            and manifest.get("git") == git and isinstance(manifest.get("files"), list), "source tree binding differs")
    before = sealer._git_record(source_root, allow_working_tree=True)
    require(before == git and manifest["files"] == _records(source_root)
            and sealer._git_record(source_root, allow_working_tree=True) == before,
            "frozen source tree changed")
    return manifest


def verify_bootstrap(*, project_root: Path, source_root: Path, build_receipt: Path | None = None,
                     mode: str) -> dict[str, Any]:
    """Read-only launch preflight. No database connection or provider operation."""
    from .runtime_database import load_installed_writer_contract
    require(mode in {"writer", "publisher"}, "unknown bootstrap mode")
    require(project_root.is_absolute() and project_root.resolve(strict=True) == project_root
            and source_root.is_absolute() and source_root.resolve(strict=True) == source_root
            and source_root != project_root, "source and data roots must be distinct canonical paths")
    installed = load_installed_writer_contract(required=True)
    require(installed is not None and installed.project_root == project_root, "bootstrap data root differs from installed writer")
    assert installed is not None
    environment = installed.payload.get("EnvironmentVariables", {})
    require(isinstance(environment, dict), "installed writer environment is invalid")
    assert isinstance(environment, dict)
    require(environment.get("DCAR_WRITER_SOURCE_ROOT") == str(source_root)
            and os.environ.get("DCAR_PROJECT_ROOT") == str(project_root)
            and os.environ.get("DCAR_WRITER_SOURCE_ROOT") == str(source_root), "bootstrap source environment is not installed")
    selected = Path(environment.get("DCAR_LOADED_BUILD_RECEIPT", ""))
    require(selected.is_absolute() and (build_receipt is None or build_receipt == selected), "bootstrap build is not installed")
    sealer, _ = _code()._tools()
    body, _ = _file(selected, private=True, limit=MAX_MANIFEST_BYTES)
    build = sealer._read_receipt(selected, contract_version=sealer.SEALED_BUILD_CONTRACT)
    plan = _private(build["code_successor_plan"])
    require(plan.get("contract") == PLAN and plan.get("transition") == TRANSITION
            and plan.get("project_root") == str(project_root) and plan.get("source_root") == str(source_root)
            and plan.get("git") == build.get("git") and build.get("status") == "succeeded"
            and build.get("schema_contract") == sealer._schema_contract(20, 20), "bootstrap build/plan source differs")
    require(plan.get("changes") == approved_changes(), "bootstrap source review differs")
    verify_source_tree(source_root=source_root, reference=plan["source_tree"], git=plan["git"])
    require(_sha(_file(selected, private=True, limit=MAX_MANIFEST_BYTES)[0]) == _sha(body), "installed build changed during bootstrap")
    return {"status": "verified", "project_root": str(project_root), "source_root": str(source_root),
            "build_sha256": _sha(body), "source_tree_sha256": plan["source_tree"]["sha256"], "mode": mode}


def approved_changes() -> dict[str, dict[str, str | None]]:
    require(account._digest(PARENT_BUILD_SHA256) and bool(SOURCE_TRANSITIONS) and MODULE not in SOURCE_TRANSITIONS,
            "source isolation review is not frozen")
    result = {}
    for name, pair in SOURCE_TRANSITIONS.items():
        _name(name)
        require(len(pair) == 2 and pair[0] != pair[1] and all(x is None or account._digest(x) for x in pair),
                "source isolation review has invalid hashes")
        result[name] = {"before_sha256": pair[0], "after_sha256": pair[1]}
    result[MODULE] = {"before_sha256": None, "after_sha256": _sha(_LOADED_SOURCE)}
    return dict(sorted(result.items()))


def _tree(project: Path, head: str) -> dict[str, tuple[str, str]]:
    result = {}
    for item in _git(project, "ls-tree", "-rz", "--full-tree", head).split(b"\0"):
        if not item:
            continue
        entry, path = item.split(b"\t", 1)
        mode, kind, oid = entry.decode("ascii").split()
        require(kind == "blob" and mode in {"100644", "100755"}, "source tree contains links or submodules")
        result[_name(os.fsdecode(path))] = (mode, oid)
    return result


def source_delta(project: Path, parent: Mapping[str, Any], current: Mapping[str, Any], *, live: bool = True) -> dict[str, Any]:
    left_tree, right_tree = (_tree(project, value["git"]["head"]) for value in (parent, current))
    left, right = forward_recovery._successor_archive(parent), forward_recovery._successor_archive(current, live=live)
    old_modes = {name: int(row[0], 8) & 0o777 for name, row in left_tree.items()}
    old_modes.update({row["path"]: row["mode"] for row in parent["git"]["working_tree"]["untracked_files"]})
    if live:
        for name, mode in old_modes.items():
            path = project / name
            if path.exists():
                require(not path.is_symlink() and stat.S_IMODE(path.stat().st_mode) == mode,
                        "source isolation cannot change source modes")
    changes = {}
    for name in sorted(set(left_tree) | set(right_tree) | set(left) | set(right)):
        if name not in left and name not in right and left_tree.get(name) == right_tree.get(name):
            continue
        def body(tree, overlay):
            if name in overlay:
                return overlay[name]
            return _git(project, "cat-file", "blob", tree[name][1]) if name in tree else None
        old, new = body(left_tree, left), body(right_tree, right)
        if old != new:
            changes[name] = {"before_sha256": _sha(old), "after_sha256": _sha(new)}
    require(changes == approved_changes(), "source differs from exact reviewed isolation transition")
    return changes


def _parent(reference: Mapping[str, Any], proof: Mapping[str, Any]) -> None:
    require(reference.get("sha256") == PARENT_BUILD_SHA256 and proof.get("build_reference") == reference
            and proof.get("contract") == account.PROOF and proof.get("decision_receipt") is not None
            and proof.get("proof_sha256") == auth.digest({k: v for k, v in proof.items() if k != "proof_sha256"}),
            "source isolation parent is not the released account generation")


def _fields(plan: Mapping[str, Any], parent: Mapping[str, Any], *, project_root: Path) -> None:
    _parent(plan["installed_parent"], parent)
    require(plan.get("contract") == PLAN and plan.get("transition") == TRANSITION
            and plan.get("project_root") == str(project_root) and plan.get("changes") == approved_changes()
            and plan.get("required_checks") == sorted(REQUIRED_CHECKS), "source isolation plan scope differs")
    source = Path(plan["source_root"])
    require(source.is_absolute() and source != project_root and ".." not in source.parts
            and plan["source_tree"]["path"] != str(source), "source isolation roots are invalid")
    old = parent["plan_payload"]
    for key in ("previous_build", "source_deployment", "source_decision_sha256", "active", "release", "operations", "manifest", "origin_runtime_bindings"):
        require(plan.get(key) == old.get(key), "source isolation changed inherited " + key)
    require(plan.get("business_e2e") == "deferred_by_user" and plan.get("transport_qualification") == "not_verified",
            "source isolation cannot invent acceptance")


@contextmanager
def using_plan(connection, reference, *, project_root: Path, at: str, historical: bool = False):
    from .runtime_paths import source_root
    from .source_routing import parse_time
    code = _code()
    sealer, contract = code._tools()
    require(not historical, "this explicit source isolation transition cannot be reused as another parent")
    plan = _private(reference)
    source = Path(plan["source_root"])
    require(source_root(project_root) == source and parse_time(plan["issued_at"]) <= parse_time(at),
            "source isolation root or time differs")
    verify_source_tree(source_root=source, reference=plan["source_tree"], git=plan["git"])
    parent_ref = contract.verified_reference(plan["installed_parent"], project_root=project_root)
    parent = code.current_proof(connection, project_root=project_root, build_path=Path(parent_ref["path"]), at=at, _historical=True)
    require(parent is not None, "source isolation parent proof is absent")
    _fields(plan, parent, project_root=project_root)
    previous_ref = contract.verified_reference(plan["previous_build"], project_root=project_root)
    previous = sealer._read_receipt(Path(previous_ref["path"]), contract_version=sealer.SEALED_BUILD_CONTRACT)
    parent_build = sealer._read_receipt(Path(parent_ref["path"]), contract_version=sealer.SEALED_BUILD_CONTRACT)
    archive = contract.verified_reference(plan["source_archive"], project_root=project_root)
    require(source_delta(source, parent_build, {"git": plan["git"], "source_archive": archive}) == plan["changes"],
            "source isolation archive delta changed")
    checked = {"project_root": str(project_root), "previous_build": previous, "plan": plan,
        "reference": reference, "historical": False, "installed_parent_proof": parent}
    token = code._CHECKED.set(checked)
    try:
        tests_ref = contract.verified_reference(plan["full_checks"], project_root=project_root)
        tests = sealer._read_receipt(Path(tests_ref["path"]), contract_version=sealer.TEST_RESULTS_CONTRACT)
        parent_tests_ref = contract.verified_reference(parent_build["test_results_receipt"], project_root=project_root)
        parent_tests = sealer._read_receipt(Path(parent_tests_ref["path"]), contract_version=sealer.TEST_RESULTS_CONTRACT)
        sealer._verify_test_results_payload(project_root, tests)
        require(tests.get("git") == plan["git"] and tests.get("status") == "passed"
                and REQUIRED_CHECKS <= set(tests.get("results", {}))
                and all(tests["results"].get(k) == v for k, v in parent_tests["results"].items()),
                "source isolation tests or parent evidence changed")
        deployment = contract.validate_deployment_receipt(connection,
            deployment_id=plan["source_deployment"]["deployment_id"], project_root=project_root, require_accepted=True)
        decision = deployment["release_decision"]
        require(deployment["receipt_sha256"] == plan["source_deployment"]["receipt_sha256"]
                and decision["decision_sha256"] == plan["source_decision_sha256"]
                and decision["runtime_bindings"] == plan["origin_runtime_bindings"]
                and decision["operations"] == plan["operations"]
                and decision["transport_manifest"] == plan["manifest"] == forward_recovery._route(),
                "source isolation changed original release authority")
        checked["deployment"] = deployment
        checked["account_control"] = account.control_postcheck(connection, plan=plan, deployment=deployment, at=at)
        yield checked
    finally:
        code._CHECKED.reset(token)


def prepare_plan(connection, *, project_root: Path, previous_build: Path, evidence_dir: Path,
                 tests: Mapping[str, Path], actor: str, reason: str, at: str):
    from .runtime_database import require_current_process_writer_lock
    from .runtime_paths import source_root
    require_current_process_writer_lock(connection)
    require(connection.in_transaction and bool(actor.strip()) and bool(reason.strip()), "writer transaction and release actor are required")
    code = _code()
    sealer, _ = code._tools()
    approved_changes()
    parent_ref = code._ref(previous_build)
    installed = code._installed_build_path()
    require(installed is not None and code._ref(installed) == parent_ref, "source isolation parent is not installed")
    parent = code.current_proof(connection, project_root=project_root, build_path=previous_build, at=at, _historical=True)
    require(parent is not None, "released account parent is absent")
    _parent(parent_ref, parent)
    source = source_root(project_root)
    require(source != project_root, "source isolation requires a separate frozen tree")
    active, release = code._current_control(connection, at)
    account.control_precheck(parent["plan_payload"], active, release)
    folder = sealer._private_evidence_parent(evidence_dir, project_root)
    sealer._create_evidence_dir(evidence_dir, folder)
    git = sealer._git_record(source, allow_working_tree=True)
    archive = evidence_dir / sealer.WORKING_TREE_ARCHIVE
    sealer._write_source_archive(source, archive, git)
    archive_ref = sealer._source_archive_record(archive, git)
    parent_build = sealer._read_receipt(previous_build, contract_version=sealer.SEALED_BUILD_CONTRACT)
    changes = source_delta(source, parent_build, {"git": git, "source_archive": archive_ref})
    manifest = {"contract": SOURCE_CONTRACT, "source_root": str(source), "git": git, "files": _records(source)}
    manifest_path = evidence_dir / "writer-source-tree-v1.json"
    sealer._write_exclusive(manifest_path, manifest)
    source_tree = code._ref(manifest_path)
    verify_source_tree(source_root=source, reference=source_tree, git=git)
    checks = sealer._test_results_payload(project_root, paths=tests, git_record=git)
    checks_path = evidence_dir / sealer.TEST_RESULTS_FILENAME
    sealer._write_exclusive(checks_path, sealer._envelope(sealer.TEST_RESULTS_CONTRACT, checks))
    old = parent["plan_payload"]
    plan = {key: old[key] for key in ("previous_build", "source_deployment", "source_decision_sha256",
        "active", "release", "operations", "manifest", "origin_runtime_bindings")}
    plan.update(contract=PLAN, transition=TRANSITION, project_root=str(project_root), source_root=str(source),
        source_tree=source_tree, installed_parent=parent_ref, git=git, source_archive=archive_ref,
        full_checks=code._ref(checks_path), changes=changes, required_checks=sorted(REQUIRED_CHECKS),
        actor=actor, reason=reason, issued_at=at, business_e2e="deferred_by_user", transport_qualification="not_verified")
    path = evidence_dir / "code-successor-plan.json"
    sealer._write_exclusive(path, plan)
    reference = code._ref(path)
    with account.runtime_context(connection, build=parent_build, build_ref=parent_ref, at=at), using_plan(
            connection, reference, project_root=project_root, at=at):
        pass
    return reference


def validate_portable(connection: sqlite3.Connection, proof: Mapping[str, Any], *, deployment: Mapping[str, Any], at: str) -> dict[str, Any]:
    """Validate the unchanged authority and new source decision in replica ledger."""
    from .source_routing import parse_time
    from .transport_receipts import _digest as transport_digest
    code = _code()
    require(proof.get("contract") == PROOF and proof.get("proof_sha256") == auth.digest({k: v for k, v in proof.items() if k != "proof_sha256"}),
            "portable source proof digest differs")
    plan, parent = proof["plan_payload"], proof["installed_parent_proof"]
    _fields(plan, parent, project_root=Path(plan["project_root"]))
    body = (json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()
    require(_sha(body) == proof["plan_reference"]["sha256"] and len(body) == proof["plan_reference"]["byte_size"]
            and proof["build_reference"]["sha256"] == proof["runtime_bindings"]["build_sha256"]
            and proof["runtime_reference"]["sha256"] == proof["runtime_bindings"]["runtime_sha256"], "portable source identities differ")
    refs = {"plan": proof["plan_reference"], "previous_build": plan["installed_parent"], "source_archive": plan["source_archive"],
        "source_tree": plan["source_tree"], "full_checks": plan["full_checks"], "build": proof["build_reference"], "runtime": proof["runtime_reference"]}
    seen = set()
    for entry in proof["private_references"]:
        role = entry["role"]
        require(role in refs and role not in seen and type(entry["byte_size"]) is int and entry["byte_size"] > 0
                and all(entry[k] == refs[role][k] for k in ("path", "sha256")), "portable source reference differs")
        seen.add(role)
    require(seen == set(refs), "portable source reference missing")
    row = code._decision_row(connection, proof["runtime_bindings"]["build_sha256"])
    receipt = proof["decision_receipt"]
    require(row is not None and isinstance(receipt, dict) and json.loads(row["details_json"]) == receipt
            and row["status"] == "succeeded" and row["id"] == receipt["receipt_id"]
            and receipt["payload_sha256"] == transport_digest(receipt["payload"])
            and receipt["self_sha256"] == transport_digest({k: v for k, v in receipt.items() if k not in {"self_sha256", "mirror"}}),
            "portable source decision is not the immutable ledger record")
    require(receipt["payload"] == code._decision_payload(plan, plan_sha=proof["plan_reference"]["sha256"],
        build_sha=proof["build_reference"]["sha256"], runtime_sha=proof["runtime_reference"]["sha256"], at=receipt["recorded_at"])
        and parse_time(plan["issued_at"]) <= parse_time(receipt["recorded_at"]) <= parse_time(at), "portable source decision changed")
    decision = deployment["release_decision"]
    require(deployment["status"] == "accepted" and proof["source_deployment_sha256"] == deployment["receipt_sha256"]
            and plan["source_deployment"] == {"deployment_id": deployment["deployment_id"], "receipt_sha256": deployment["receipt_sha256"]}
            and plan["source_decision_sha256"] == decision["decision_sha256"] and plan["operations"] == decision["operations"]
            and proof["origin_runtime_bindings"] == plan["origin_runtime_bindings"] == decision["runtime_bindings"]
            and proof["manifest"] == plan["manifest"] == decision["transport_manifest"]
            and proof["runtime_bindings"]["config_sha256"] == proof["origin_runtime_bindings"]["config_sha256"],
            "portable source proof changed original authority")
    token = account._RUNTIME.set({"plan": plan, "runtime": proof["runtime_bindings"]})
    try:
        code.validate_portable(connection, parent, deployment=deployment, at=at)
        control = account.control_postcheck(connection, plan=plan, deployment=deployment, at=at, portable=True)
        require(proof["active"] == control["active"] and proof["release_event_id"] == control["release"]["id"]
                and proof["release_event_hash"] == control["release"]["event_hash"] and proof.get("roster_successor") == control["roster_successor"],
                "portable source proof changed roster authority")
        from .capture_activation_release import validate_installed_activation_successor
        from .profile_activations import activation_at
        active = activation_at(connection, at)
        require(active is not None, "portable source activation is absent")
        assert active is not None
        validate_installed_activation_successor(connection, source_deployment=deployment, current_active=active,
            runtime_bindings=proof["origin_runtime_bindings"], manifest=proof["manifest"], at=at, portable=True)
        return dict(proof)
    finally:
        account._RUNTIME.reset(token)
