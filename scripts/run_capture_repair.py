#!/usr/bin/env python3
"""Run one frozen repair via the installed wrapper; restore launchd in finally.

This coordinator never imports business modules or constructs a loaded build ID.
Preparing/activating a release remains the existing installer's responsibility.
"""
from __future__ import annotations
import argparse
from contextlib import contextmanager
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import plistlib
import signal
import socket
import sqlite3
import stat
import subprocess
import sys
import time
import urllib.request

LABEL = "cn.tj.dcar.writer-worker"


def require(value, message):
    if not value:
        raise RuntimeError(message)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def record(path, value):
    path = Path(path)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as output:
        json.dump(value, output, ensure_ascii=False, indent=2)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())


def installed_plist(path):
    require(path.is_absolute() and path.resolve() == path and path.is_file(), "unsafe installed plist")
    value = plistlib.loads(path.read_bytes())
    env = value["EnvironmentVariables"]
    source = Path(env["DCAR_WRITER_SOURCE_ROOT"])
    require(value["Label"] == LABEL and value["WorkingDirectory"] == env["DCAR_PROJECT_ROOT"]
        and value["ProgramArguments"] == [str(source / "deploy/macos/run_writer_worker.sh")]
        and not env.get("DCAR_LOADED_BUILD_ID"), "installed Writer bootstrap contract differs")
    return value


def bootstrap_environment(plist, plan, plan_sha):
    # Keep system session paths, never caller-provided business/key overrides.
    env = {key: value for key, value in os.environ.items() if key in {
        "PATH", "HOME", "USER", "LOGNAME", "TMPDIR", "LANG", "LC_ALL", "SHELL", "__CF_USER_TEXT_ENCODING"}}
    configured = plist["EnvironmentVariables"]
    require(not configured.get("DCAR_LOADED_BUILD_ID") and not configured.get("TIKHUB_API_KEY")
        and not configured.get("TIKHUB_API_BASE"), "plist contains forbidden derived or secret values")
    env.update(configured)
    env.update(DCAR_WRITER_ENTRY="repair", DCAR_WRITER_REPAIR_PLAN=str(plan),
        DCAR_WRITER_REPAIR_PLAN_SHA256=plan_sha)
    return env


def health():
    with urllib.request.urlopen("http://127.0.0.1:8766/api/v8/health", timeout=15) as response:
        return json.load(response)


def alive(pid):
    return subprocess.run(["/bin/ps", "-p", str(pid), "-o", "pid="], capture_output=True).returncode == 0


def port_open():
    with socket.socket() as client:
        client.settimeout(1)
        return client.connect_ex(("127.0.0.1", 8766)) == 0


def lock_free(path):
    with Path(path).open("r") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(stream, fcntl.LOCK_UN)


def inflight(database, boot_at):
    with sqlite3.connect(Path(database).as_uri() + "?mode=ro", uri=True, timeout=5) as connection:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        attempts = connection.execute("SELECT count(*) FROM fetch_attempts WHERE response_finished_at IS NULL AND julianday(request_started_at)>=julianday(?)", (boot_at,)).fetchone()[0]
        slots = connection.execute("SELECT count(*) FROM fetch_slots WHERE status='running'").fetchone()[0]
        sends = connection.execute("SELECT count(*) FROM paid_provider_dispatch_events WHERE id IN (SELECT max(id) FROM paid_provider_dispatch_events GROUP BY dispatch_id) AND event_type='send_marked' AND julianday(created_at)>=julianday(?)", (boot_at,)).fetchone()[0]
        leases = connection.execute("SELECT count(*) FROM capture_work_items WHERE state IN ('running','leased') AND julianday(lease_expires_at)>julianday('now')").fetchone()[0]
        return {"unfinished_requests": attempts, "running_slots": slots, "unsettled_sends": sends, "live_leases": leases}


def controlled_stop(pid, source, database, boot_at, evidence):
    """Reuse the established no-in-flight stop boundary if graceful drain stalls."""
    command = subprocess.check_output(["/bin/ps", "-p", str(pid), "-o", "command="], text=True)
    require(str(source) in command and "uvicorn v8.api:app" in command, "Writer PID identity changed")
    stopped = False
    killed = False
    try:
        os.kill(pid, signal.SIGSTOP)
        stopped = True
        time.sleep(0.2)
        require("T" in subprocess.check_output(["/bin/ps", "-p", str(pid), "-o", "stat="], text=True), "Writer threads did not freeze")
        rows = []
        for line in subprocess.check_output(["/bin/ps", "-axo", "pid=,ppid=,comm="], text=True).splitlines():
            fields = line.strip().split(None, 2)
            if len(fields) == 3:
                rows.append((int(fields[0]), int(fields[1]), fields[2]))
        descendants = {pid}
        children = []
        while True:
            added = [row for row in rows if row[1] in descendants and row[0] not in descendants]
            if not added:
                break
            descendants.update(row[0] for row in added)
            children.extend(added)
        require(all(Path(row[2]).name in {"git", "caffeinate"} or (Path(row[2]).name == "<defunct>"
            and "Z" in subprocess.check_output(["/bin/ps", "-p", str(row[0]), "-o", "stat="], text=True)) for row in children),
            "Writer has an active media or unreviewed child")
        state = inflight(database, boot_at)
        require(state["unfinished_requests"] == state["running_slots"] == state["unsettled_sends"] == 0,
            "paid request still in flight; controlled stop refused")
        record(evidence / "controlled-stop.json", {"pid": pid, **state,
            "method": "freeze-threads-confirm-no-sent-request-then-terminate", "lease_records_preserved": True,
            "children": [{"pid": row[0], "kind": Path(row[2]).name} for row in children]})
        os.kill(pid, signal.SIGKILL)
        killed = True
    finally:
        if stopped and not killed and alive(pid):
            os.kill(pid, signal.SIGCONT)


def stop_writer(plist_path, plist, evidence):
    before = health()
    pid = before["media_consumers"]["pid"]
    env = plist["EnvironmentVariables"]
    require(before["status"] == "ok" and before["writer_lock"]["held"]
        and before["media_consumers"]["current_code_matches_loaded"], "current Writer is not a verified running source")
    listener = subprocess.check_output(["/usr/sbin/lsof", "-tiTCP:8766", "-sTCP:LISTEN"], text=True).strip()
    require(listener == str(pid), "health PID differs from port owner")
    boot_at = before["media_consumers"]["started_at"]
    record(evidence / "before-stop.json", before)
    os.kill(pid, signal.SIGINT)
    deadline = time.monotonic() + 120
    while alive(pid) and time.monotonic() < deadline:
        time.sleep(1)
    if alive(pid):
        controlled_stop(pid, env["DCAR_WRITER_SOURCE_ROOT"], env["DCAR_V8_DB"], boot_at, evidence)
    result = subprocess.run(["/bin/launchctl", "bootout", "gui/" + str(os.getuid()), str(plist_path)], capture_output=True, text=True)
    require(result.returncode == 0, "launchctl bootout failed: " + result.stderr[:300])
    deadline = time.monotonic() + 180
    while (alive(pid) or inflight(env["DCAR_V8_DB"], boot_at)["live_leases"]) and time.monotonic() < deadline:
        time.sleep(1)
    require(not alive(pid) and not port_open(), "old Writer has not exited")
    require(inflight(env["DCAR_V8_DB"], boot_at)["live_leases"] == 0, "old durable lease is still live")
    lock_free(env["DCAR_WRITER_LOCK"])
    record(evidence / "writer-stopped.json", {"old_pid": pid, "port_8766_closed": True, "writer_lock_free": True})
    return before


def start_writer(plist_path, expected, evidence):
    result = subprocess.run(["/bin/launchctl", "bootstrap", "gui/" + str(os.getuid()), str(plist_path)], capture_output=True, text=True)
    require(result.returncode == 0, "launchctl bootstrap failed: " + result.stderr[:300])
    deadline = time.monotonic() + 180
    last_error = "health not available"
    while time.monotonic() < deadline:
        try:
            current = health()
            require(current["status"] == "ok" and current["writer_lock"]["held"]
                and current["media_consumers"]["current_code_matches_loaded"]
                and current["database_state"]["user_version"] == 23, "restored Writer health differs")
            identity = current["runtime_database_identity"]
            require(identity["inode"] == expected["runtime_database_identity"]["inode"], "restored Writer database differs")
            record(evidence / "writer-started.json", current)
            return current
        except Exception as error:
            last_error = type(error).__name__ + ": " + str(error)[:250]
            time.sleep(1)
    raise RuntimeError("Writer did not recover: " + last_error)


def restore_predecessor_gates(plan, evidence):
    """The pre-repair S16 has no CLI selector: use its real running API."""
    base = "http://127.0.0.1:8766/api/v8/internal/current-activation-hold/commands"
    submissions = []
    for operation in plan["publish_operations"]:
        body = {"command_id": "repair-rollback:" + plan["repair_run_id"] + ":" + operation,
            "command": "capture_release", "parameters": {"action": "operation_publish", "operation": operation}}
        request = urllib.request.Request(base, data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        for attempt in range(2):
            try:
                with urllib.request.urlopen(request, timeout=60) as response:
                    submitted = json.load(response)
                break
            except (OSError, TimeoutError):
                if attempt:
                    raise
                # Same command identity is safe after an uncertain response.
        submissions.append({"operation": operation, "command_id": body["command_id"], "response": submitted})
        record(evidence / "rollback-gates.json", {"status": "submitted", "records": submissions,
            "provider_calls": 0, "entry": "predecessor-service-current-hold-control"})
    # These commands wake the dedicated control executor, not the daily queue.
    deadline = time.monotonic() + 900
    pending = {item["response"]["run_id"]: item for item in submissions}
    while pending and time.monotonic() < deadline:
        for run_id, item in list(pending.items()):
            with urllib.request.urlopen(base + "/" + str(run_id), timeout=15) as response:
                status = json.load(response)
            item["terminal"] = status
            require(status["status"] != "failed", "predecessor operation publication failed: " + item["operation"])
            if status["status"] == "succeeded":
                del pending[run_id]
        if pending:
            time.sleep(2)
    record(evidence / "rollback-gates.json", {"status": "pending" if pending else "succeeded",
        "records": submissions, "provider_calls": 0, "entry": "predecessor-service-current-hold-control"})
    require(not pending, "predecessor gate restoration did not finish within its bounded window")
    return submissions


def _group_exists(group_id):
    try:
        os.killpg(group_id, 0)
        return True
    except ProcessLookupError:
        return False


def _run_repair_process(argv, *, cwd, env, output):
    # caffeinate is the direct child; Python can outlive it after interruption.
    # Own a process group and wait for every member before returning to cleanup.
    process = subprocess.Popen(argv, cwd=cwd, env=env, stdout=output,
        stderr=subprocess.STDOUT, start_new_session=True)
    try:
        code = process.wait()
    except BaseException:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            process.poll()  # reap the direct child even when its descendant stays alive
            if not _group_exists(process.pid):
                break
            time.sleep(0.2)
        require(not _group_exists(process.pid),
            "repair process group still running; preserve freeze until its ledger settles")
        raise
    deadline = time.monotonic() + 300
    while _group_exists(process.pid) and time.monotonic() < deadline:
        time.sleep(0.2)
    require(not _group_exists(process.pid), "repair descendants have not exited")
    return code


def execute(args):
    plan_path = args.plan.resolve(strict=True)
    require(not args.plan.is_symlink() and digest(plan_path) == args.plan_sha256, "frozen plan differs")
    plist_path = args.plist
    plist = installed_plist(plist_path)
    evidence = args.evidence.resolve()
    evidence.mkdir(mode=0o700, parents=True, exist_ok=False)
    original = plist_path.read_bytes()
    record(evidence / "coordinator.json", {"plan_sha256": args.plan_sha256, "original_plist_sha256": hashlib.sha256(original).hexdigest(),
        "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat()})
    (evidence / "writer.before.plist").write_bytes(original)
    (evidence / "writer.before.plist").chmod(0o600)
    freeze = Path(plist["WorkingDirectory"]) / "runtime/operator-freeze.lock"
    fd = os.open(freeze, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as output:
        json.dump({"purpose": "bounded-capture-repair", "evidence": str(evidence), "plan_sha256": args.plan_sha256}, output)
    owned = (freeze.stat().st_dev, freeze.stat().st_ino, digest(freeze))
    before = None
    unloaded = False
    failure = None
    try:
        before = stop_writer(plist_path, plist, evidence)
        unloaded = True
        if args.install_proposal:
            target_plan = json.loads(plan_path.read_text())
            source = Path(target_plan["source_root"])
            python = Path(plist["WorkingDirectory"]) / ".venv/bin/python"
            env = {key: value for key, value in os.environ.items() if not key.startswith(("DCAR_", "TIKHUB_"))}
            env.update(PYTHONDONTWRITEBYTECODE="1", PYTHONPATH=str(source / "src/dcar_eval"))
            with (evidence / "install.log").open("w") as output:
                process = subprocess.run([str(python), "-B", str(source / "scripts/install_four_platform_flow.py"), "activate",
                    "--proposal", str(args.install_proposal), "--output", str(evidence / "writer-installed.json")],
                    cwd=source, env=env, stdout=output, stderr=subprocess.STDOUT)
            require(process.returncode == 0, "release activation failed; see install.log")
        current = installed_plist(plist_path)
        env = bootstrap_environment(current, plan_path, args.plan_sha256)
        with (evidence / "repair.log").open("w") as output:
            code = _run_repair_process(current["ProgramArguments"], cwd=current["WorkingDirectory"], env=env, output=output)
        require(code == 0, "sealed repair failed; see repair.log")
    except BaseException as error:
        failure = error
    finally:
        # Even a failed stop can have successfully unloaded launchd. Query the
        # service instead of assuming an exception means it is still running.
        registered = subprocess.run(["/bin/launchctl", "print", "gui/" + str(os.getuid()) + "/" + LABEL], capture_output=True).returncode == 0
        if unloaded or not registered:
            try:
                lock_free(plist["EnvironmentVariables"]["DCAR_WRITER_LOCK"])
            except BaseException as cleanup_error:
                record(evidence / "recovery-pending.json", {"status": "writer_lock_still_held",
                    "freeze_preserved": True, "second_writer_started": False,
                    "original_error": type(failure).__name__ if failure else None,
                    "cleanup_error": type(cleanup_error).__name__})
                raise RuntimeError("repair cleanup is waiting for its real Writer owner; see recovery-pending.json") from cleanup_error
        if failure is not None and (unloaded or not registered):
            plist_path.write_bytes(original)
            plist_path.chmod(0o600)
            record(evidence / "rollback.json", {"restored_predecessor_plist": True, "data_deleted": False,
                "gate_restoration": "requires predecessor operation publisher if new decision was already issued"})
        require(freeze.exists() and (freeze.stat().st_dev, freeze.stat().st_ino, digest(freeze)) == owned,
            "operator freeze ownership changed")
        freeze.unlink()
        if unloaded or not registered:
            if before is None:
                before = json.loads((evidence / "before-stop.json").read_text())
            start_writer(plist_path, before, evidence)
            if failure is not None:
                restore_predecessor_gates(json.loads(plan_path.read_text()), evidence)
        elif failure is not None:
            # A rejected controlled stop resumed the original Writer. Do not
            # create a second service and do not remove someone else's lock.
            record(evidence / "stop-aborted.json", {"original_service_preserved": True, "reason": type(failure).__name__})
    if failure is not None:
        raise failure
    print(json.dumps({"status": "completed", "service_restored": True, "evidence": str(evidence)}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--plist", type=Path, default=Path.home() / "Library/LaunchAgents" / (LABEL + ".plist"))
    parser.add_argument("--install-proposal", type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    execute(args)


if __name__ == "__main__":
    main()
