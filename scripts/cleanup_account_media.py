#!/usr/bin/env python3
"""Plan/apply bounded media unlinking after an independently installed DB cleanup.

Standard library only. Never writes a database, removes directories, follows
symlinks, or discovers files by recursively walking a cache. The caller must
keep writers stopped between final planning and application.
"""
from __future__ import annotations

import argparse
import collections
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import sys

VERSION = "account-media-cleanup-v1"
MAX_JSON = 16 * 1024 * 1024
PATH_KEYS = {"path", "paths", "file", "files", "filename", "directory", "directories"}


class Unsafe(ValueError):
    pass


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def load(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write_json(path, value):
    path = Path(path)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(encoded(value) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())


@contextlib.contextmanager
def readonly(path):
    path = Path(path).absolute()
    if not path.is_file() or path.is_symlink():
        raise Unsafe("database must be an existing regular non-symlink file")
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    connection.execute("BEGIN")
    try:
        yield connection
    finally:
        connection.rollback()
        connection.close()


def id_set(values):
    if not isinstance(values, list) or any(type(v) is not int or v <= 0 for v in values):
        raise Unsafe("scope IDs must be positive integer arrays")
    if len(set(values)) != len(values):
        raise Unsafe("scope IDs must not repeat")
    return set(values)


def scope_sets(scope):
    result = {k: id_set(scope[k]) for k in (
        "deleted_content_ids", "deleted_account_ids", "retained_content_ids", "retained_account_ids")}
    for noun in ("content", "account"):
        if result[f"deleted_{noun}_ids"] & result[f"retained_{noun}_ids"]:
            raise Unsafe("deleted and retained scopes overlap")
    if not result["deleted_content_ids"] or not result["deleted_account_ids"]:
        raise Unsafe("empty deletion scope")
    return result


def verify_installed(connection, sets):
    for table, noun in (("content_items", "content"), ("accounts", "account")):
        actual = {r[0] for r in connection.execute(f"SELECT id FROM {table}")}
        if actual != sets[f"retained_{noun}_ids"] or actual & sets[f"deleted_{noun}_ids"]:
            raise Unsafe(f"installed {table} ID set differs from validated scope")


class Boundary:
    def __init__(self, root):
        self.root = Path(root).absolute()
        if self.root != self.root.resolve(strict=True):
            raise Unsafe("project root must be canonical and must not contain symlinks")
        self.allowed = (self.root / "data/cache", self.root / "reports")

    def path(self, value):
        if not isinstance(value, str) or not value or any(c in value for c in ("\0", "\n", "\r", "\\")):
            raise Unsafe("invalid local path")
        p = Path(value)
        if ".." in p.parts:
            raise Unsafe("parent traversal is forbidden")
        p = p if p.is_absolute() else self.root / p
        if p == self.root or not any(p.is_relative_to(a) and p != a for a in self.allowed):
            raise Unsafe("path outside project data/cache and reports")
        return p

    def info(self, path):
        p = self.path(str(path))
        current = self.root
        for part in p.relative_to(self.root).parts:
            current /= part
            info = current.lstat()
            if stat.S_ISLNK(info.st_mode):
                raise Unsafe("symlink component")
            if current != p and not stat.S_ISDIR(info.st_mode):
                raise Unsafe("non-directory parent")
        return info

    @contextlib.contextmanager
    def parent_fd(self, path):
        p = self.path(str(path))
        fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            for part in p.relative_to(self.root).parts[:-1]:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = child
            yield fd, p.name
        finally:
            os.close(fd)

    def read_json(self, path):
        with self.parent_fd(path) as (directory, name):
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
            with os.fdopen(fd, "rb") as stream:
                before = os.fstat(stream.fileno())
                if not stat.S_ISREG(before.st_mode) or before.st_size > MAX_JSON:
                    raise Unsafe("manifest is nonregular or too large")
                body = stream.read(MAX_JSON + 1)
                if identity(before) != identity(os.fstat(stream.fileno())):
                    raise Unsafe("manifest changed while read")
        return json.loads(body), hashlib.sha256(body).hexdigest()


def identity(info):
    return {"dev": info.st_dev, "inode": info.st_ino, "size": info.st_size,
            "mtime_ns": info.st_mtime_ns, "nlink": info.st_nlink,
            "allocated_bytes": getattr(info, "st_blocks", 0) * 512}


def strings(value, key=""):
    if isinstance(value, dict):
        for k, v in value.items():
            yield from strings(v, str(k))
    elif isinstance(value, list):
        for v in value:
            yield from strings(v, key)
    elif isinstance(value, str):
        yield key, value


def local_references(value, boundary, parent=None):
    """Conservatively protect both project-relative and manifest-relative forms."""
    result = set()
    for key, text in strings(value):
        if not text or text.startswith(("http://", "https://", "data:", "app://")):
            continue
        path_key = key in PATH_KEYS or key.endswith(("_path", "_paths", "_root", "_dir", "_directory"))
        prefixes = tuple(str(p) + "/" for p in boundary.allowed) + ("data/cache/", "reports/")
        recognizable = text.startswith(prefixes)
        if not recognizable and not path_key:
            continue
        if text.startswith("/") and not any(
                text == str(p) or text.startswith(str(p) + "/") for p in boundary.allowed):
            continue  # External archives/config/input roots cannot be candidates.
        choices = [text]
        if parent is not None and not Path(text).is_absolute():
            choices.append(str(parent / text))
        accepted = False
        for choice in choices:
            try:
                result.add(boundary.path(choice))
                accepted = True
            except Unsafe:
                pass
        if not accepted and (recognizable or ".." in Path(text).parts):
            raise Unsafe("ambiguous or traversing retained path reference")
    return result


def manifest_path(path, kind=""):
    return path.suffix.lower() == ".json" and ("manifest" in kind or "manifest" in path.name or path.name == "frames.json")


def qident(value):
    return "\"" + value.replace("\"", "\"\"") + "\""


def retained_references(connection, boundary):
    paths = set()
    manifests = set()
    # All explicit path columns and structured JSON columns, across all tables.
    # No provider blob decoding and no unstructured text/content-body matching.
    row_hash = hashlib.sha256()
    tables = [r[0] for r in connection.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
    for table in tables:
        columns = [r[1] for r in connection.execute(f"PRAGMA table_info({qident(table)})")]
        selected = [n for n in columns if n in PATH_KEYS or n.endswith(("_path", "_paths", "_root", "_dir", "_json"))]
        if not selected:
            continue
        # Sort a row-digest multiset so physical row order/install VACUUM is irrelevant.
        digests = []
        for row in connection.execute(f"SELECT {','.join(map(qident, selected))} FROM {qident(table)}"):
            obj = {}
            for name, value in zip(selected, row):
                if not value or not isinstance(value, str):
                    continue
                if name.endswith("_json"):
                    try:
                        obj[name] = json.loads(value)
                    except json.JSONDecodeError:
                        if "data/cache/" in value or "reports/" in value:
                            raise Unsafe(f"unparseable retained path JSON in {table}.{name}")
                else:
                    obj[name] = value
            refs = local_references(obj, boundary)
            paths.update(refs)
            if refs:
                digests.append(digest(sorted(map(str, refs))))
        row_hash.update(encoded([table, sorted(digests)]))
    for row in connection.execute("SELECT local_path,artifact_type FROM evidence_artifacts"):
        p = boundary.path(row[0])
        paths.add(p)
        if manifest_path(p, row[1]):
            manifests.add(p)
    manifest_hashes = {}
    queue = list(manifests)
    while queue:
        p = queue.pop()
        if str(p) in manifest_hashes:
            continue
        # Fail closed if a retained manifest cannot explain its references.
        try:
            body, sha = boundary.read_json(p)
        except (OSError, ValueError) as exc:
            raise Unsafe(f"retained manifest unavailable: {p.name}: {type(exc).__name__}") from exc
        manifest_hashes[str(p)] = sha
        refs = local_references(body, boundary, p.parent)
        paths.update(refs)
        queue.extend(x for x in refs if manifest_path(x) and str(x) not in manifest_hashes)
    return paths, {"path_digest": digest(sorted(map(str, paths))),
                   "database_reference_digest": row_hash.hexdigest(),
                   "manifest_digest": digest(manifest_hashes), "path_count": len(paths),
                   "manifest_count": len(manifest_hashes)}


def protected(path, references):
    return path in references or any(parent in references for parent in path.parents)


def make_plan(source_db, installed_db, scope, project_root):
    sets = scope_sets(scope)
    boundary = Boundary(project_root)
    with readonly(installed_db) as target:
        verify_installed(target, sets)
        references, binding = retained_references(target, boundary)
    with readonly(source_db) as source:
        source_accounts = {r[0] for r in source.execute("SELECT id FROM accounts")}
        ownership = dict(source.execute("SELECT id,account_id FROM content_items"))
        actual_deleted = {i for i, a in ownership.items() if a in sets["deleted_account_ids"]}
        if not sets["deleted_account_ids"] <= source_accounts or actual_deleted != sets["deleted_content_ids"]:
            raise Unsafe("deleted IDs are not exactly the selected source accounts and their contents")
        if set(ownership) - actual_deleted != sets["retained_content_ids"]:
            raise Unsafe("retained content set does not partition the source")
        paths = {}
        for r in source.execute("SELECT content_id,artifact_type,local_path FROM evidence_artifacts"):
            if r[0] in actual_deleted:
                paths.setdefault(boundary.path(r[2]), set()).add(r[1])
    skipped = []
    candidates = set(paths)
    old_manifests = [p for p, kinds in paths.items() if any(manifest_path(p, k) for k in kinds)]
    blocked_parents = set()
    seen = set()
    while old_manifests:
        p = old_manifests.pop()
        if p in seen or protected(p, references):
            continue
        seen.add(p)
        try:
            body, _ = boundary.read_json(p)
            children = local_references(body, boundary, p.parent)
            for child in children:
                if child != p and child.is_relative_to(p.parent):
                    candidates.add(child)
                    if manifest_path(child):
                        old_manifests.append(child)
        except (OSError, ValueError) as exc:
            blocked_parents.add(p.parent)
            skipped.append({"path": str(p), "reason": "unreadable_old_manifest", "error": type(exc).__name__})
    entries = []
    inode_seen = set()
    for p in sorted(candidates):
        reason = None
        if protected(p, references):
            reason = "retained_reference_or_directory"
        elif any(p.is_relative_to(parent) for parent in blocked_parents):
            reason = "uncertain_old_manifest_scope"
        if reason:
            skipped.append({"path": str(p), "reason": reason})
            continue
        try:
            info = boundary.info(p)
            if not stat.S_ISREG(info.st_mode):
                reason = "directory_or_nonregular_not_removed"
            elif info.st_nlink != 1:
                reason = "hardlink_count_not_one"
            elif (info.st_dev, info.st_ino) in inode_seen:
                reason = "duplicate_inode"
            if not reason:
                inode_seen.add((info.st_dev, info.st_ino))
                entries.append({"path": str(p), **identity(info)})
        except FileNotFoundError:
            reason = "missing"
        except (OSError, Unsafe) as exc:
            reason = "unsafe_path_" + type(exc).__name__
        if reason:
            skipped.append({"path": str(p), "reason": reason})
    return {"version": VERSION, "project_root": str(boundary.root), "scope": scope,
            "scope_sha256": digest(scope), "source_db": str(Path(source_db).absolute()),
            "installed_reference_binding": binding, "entries": entries, "skipped": skipped,
            "summary": {"files": len(entries), "logical_bytes": sum(x["size"] for x in entries),
                        "allocated_bytes": sum(x["allocated_bytes"] for x in entries),
                        "skip_reasons": dict(collections.Counter(x["reason"] for x in skipped))},
            "limits": ["No directory removal or recursive cache walk.",
                       "Only explicit manifest child paths within that manifest directory are additional candidates.",
                       "All nlink != 1 files are retained.",
                       "Unlinked allocated bytes are observations, not guaranteed APFS freed space."]}


def emit(stream, value):
    stream.write(encoded(value) + b"\n")


def sync(stream):
    stream.flush()
    os.fsync(stream.fileno())


def apply_plan(manifest, manifest_sha256, installed_db, project_root, journal, batch_size=128):
    raw = Path(manifest).read_bytes()
    if hashlib.sha256(raw).hexdigest() != manifest_sha256:
        raise Unsafe("manifest SHA256 mismatch")
    plan = json.loads(raw)
    if plan.get("version") != VERSION or plan["scope_sha256"] != digest(plan["scope"]):
        raise Unsafe("manifest contract/scope digest mismatch")
    boundary = Boundary(project_root)
    if str(boundary.root) != plan["project_root"]:
        raise Unsafe("project root differs from approved manifest")
    sets = scope_sets(plan["scope"])
    with readonly(installed_db) as target:
        verify_installed(target, sets)
        references, binding = retained_references(target, boundary)
        if binding != plan["installed_reference_binding"]:
            raise Unsafe("installed references changed; create a fresh plan")
        # Holding a read snapshot does not stop writers. Caller owns the stopped-writer window.
        entries = plan["entries"]
        if len({x["path"] for x in entries}) != len(entries):
            raise Unsafe("duplicate candidate paths")
        for entry in entries:
            p = boundary.path(entry["path"])
            if protected(p, references) or entry["nlink"] != 1:
                raise Unsafe("manifest contains a protected or hardlinked candidate")
        fd = os.open(journal, os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "r+b") as log:
            fcntl.flock(log.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            log_info = os.fstat(log.fileno())
            if not stat.S_ISREG(log_info.st_mode) or log_info.st_nlink != 1:
                raise Unsafe("journal must be a regular file with one link")
            content = log.read()
            truncate_at = None
            if content and not content.endswith(b"\n"):
                end = content.rfind(b"\n") + 1
                if not end:
                    raise Unsafe("existing journal has no complete header")
                truncate_at = end
                content = content[:end]
            events = [json.loads(line) for line in content.splitlines()]
            header = {"event": "header", "version": VERSION, "manifest_sha256": manifest_sha256,
                      "scope_sha256": plan["scope_sha256"]}
            if events and events[0] != header:
                raise Unsafe("journal belongs to another manifest")
            if truncate_at is not None:
                log.truncate(truncate_at)
            if not events:
                emit(log, header)
                sync(log)
            completed = {e["index"]: e for e in events if e.get("event") == "result"}
            todo = [i for i in range(len(entries)) if i not in completed]
            for offset in range(0, len(todo), batch_size):
                indices = todo[offset:offset + batch_size]
                emit(log, {"event": "intent", "indices": indices})
                sync(log)  # Durable intent before any unlink in this bounded batch.
                for i in indices:
                    entry = entries[i]
                    status = "changed_or_unsafe"
                    try:
                        with boundary.parent_fd(entry["path"]) as (directory, name):
                            info = os.stat(name, dir_fd=directory, follow_symlinks=False)
                            if stat.S_ISREG(info.st_mode) and identity(info) == {k: entry[k] for k in identity(info)}:
                                os.unlink(name, dir_fd=directory)
                                status = "unlinked"
                    except FileNotFoundError:
                        status = "already_missing"
                    except (OSError, Unsafe):
                        status = "changed_or_unsafe"
                    event = {"event": "result", "index": i, "status": status}
                    emit(log, event)
                    completed[i] = event
                sync(log)
            counts = collections.Counter(e["status"] for e in completed.values())
            unlinked = [entries[i] for i, e in completed.items() if e["status"] == "unlinked"]
            result = {"manifest_sha256": manifest_sha256, "complete": len(completed) == len(entries),
                      "counts": dict(counts), "observed_unlinked_logical_bytes": sum(e["size"] for e in unlinked),
                      "observed_unlinked_allocated_bytes": sum(e["allocated_bytes"] for e in unlinked),
                      "planned_skips": len(plan["skipped"]),
                      "note": "Already-missing files are not claimed as freed space. Journal resumes idempotently."}
            emit(log, {"event": "summary", **result})
            sync(log)
            return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("plan")
    for name in ("source-db", "installed-db", "scope-json", "project-root", "output"):
        p.add_argument("--" + name, required=True)
    p = sub.add_parser("apply")
    for name in ("manifest", "manifest-sha256", "installed-db", "project-root", "journal"):
        p.add_argument("--" + name, required=True)
    p.add_argument("--writers-stopped", action="store_true", required=True,
                   help="Caller asserts the controlled stopped-writer window is active")
    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            plan = make_plan(args.source_db, args.installed_db, load(args.scope_json), args.project_root)
            write_json(args.output, plan)
            result = {"manifest": args.output, "sha256": hashlib.sha256(Path(args.output).read_bytes()).hexdigest(), **plan["summary"]}
        else:
            result = apply_plan(args.manifest, args.manifest_sha256, args.installed_db, args.project_root, args.journal)
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except (OSError, ValueError, KeyError, sqlite3.Error) as exc:
        print(json.dumps({"error": type(exc).__name__, "message": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
