# macOS designated writer and snapshot publisher

This directory defines the single macOS writer for the DcarAIGC production
topology. The worker owns scheduled provider calls, media processing,
incremental evaluation, and report jobs. It runs a loopback-only API on
`127.0.0.1:8766`; the normal local UI/API remains on `127.0.0.1:4173` and
`127.0.0.1:8765`.

Nothing in this directory is installed or loaded automatically. Both plist
templates are disabled by default. Each renderer never calls `launchctl`.

## Safety contract

- There must be exactly one scheduled writer. The Ubuntu read replica keeps
  `DCAR_SCHEDULER_ENABLED=0` and `DCAR_STARTUP_CATCHUP_ENABLED=0`.
- The writer uses `DCAR_SCHEDULER_ENABLED=1` and
  `DCAR_STARTUP_CATCHUP_ENABLED=1`, but the only supported startup mode is
  `report_only`. Startup catch-up may create or retry due `daily_report` and
  `weekly_report` occurrences. It never runs `daily_capture`, media download,
  media processing, or `daily_media_cutoff`, so it cannot spend provider money.
- The current `daily_capture` provider ceiling is **USD 8 per scheduled day**.
  Do not enable the LaunchAgent until the operator has explicitly authorized
  that recurring ceiling and added the acknowledgement to `writer.env`.
- The TikHub API key is never stored in the plist or `writer.env`. The latter
  contains only `TIKHUB_API_KEY_FILE`, pointing to the existing external key
  file, which itself contains `TIKHUB_API_KEY=...` and must have mode `0400` or
  `0600`.
- The Mac must stay powered, connected to the network, and awake through the
  scheduled window. The wrapper uses `caffeinate -s`, which prevents idle
  system sleep while the Mac is on AC power. Closing a laptop lid, losing AC
  power, rebooting, or manually sleeping the Mac can still skip a run.
- This is a per-user LaunchAgent, so the designated macOS account must be
  logged into its GUI session after a reboot.
- An operator freeze lock at `runtime/operator-freeze.lock` blocks startup.
  Do not remove it without completing the corresponding recovery procedure.

## 1. Prepare the external writer configuration

Do this only after the recurring USD 8 ceiling has been approved. Copy the
example outside the repository, edit the absolute key-file path, and protect
it. Set `DCAR_DAILY_COST_AUTHORIZATION` to
`I_ACKNOWLEDGE_DAILY_PROVIDER_LIMIT_USD_8` only after that approval. The example
leaves it blank so a copy alone cannot authorize spend. Do not paste the API
key into this file.

```sh
install -d -m 0700 "$HOME/Library/Application Support/DcarAIGC"
install -d -m 0700 "$HOME/Library/Logs/DcarAIGC"
install -m 0600 deploy/macos/writer.env.example \
  "$HOME/Library/Application Support/DcarAIGC/writer.env"
chmod 0600 /absolute/path/outside/the/repository/TikHub.env.local
```

Review both files locally. The key file must remain outside the checkout. The
worker preflight rejects symlinks, permissive file modes, direct
`TIKHUB_API_KEY` environment values, missing cost acknowledgement, a missing
formal database, and any attempt to use port 8765.

## 2. Render without loading

Run the side-effect-free check first, then render a new disabled plist. The
renderer refuses to overwrite an existing file and never runs `launchctl`.

```sh
python3 deploy/macos/render_launch_agent.py \
  --project-root "$PWD" \
  --check

python3 deploy/macos/render_launch_agent.py \
  --project-root "$PWD" \
  --output "$HOME/Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"

plutil -lint "$HOME/Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
plutil -p "$HOME/Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
```

Before enabling, verify all of the following:

1. the Mac is on AC power and configured not to sleep;
2. `.venv/bin/python`, `mlx-whisper`, Homebrew `ffmpeg`/`ffprobe`, and
   `/usr/bin/swiftc` are present; the LaunchAgent supplies their explicit PATH;
3. `app/data/dcar_insight.sqlite3` is the intended formal writer database;
4. no process is already listening on 8766;
5. the Ubuntu scheduler and startup catch-up both report disabled;
6. the recurring provider ceiling has explicit operator approval;
7. there is no operator freeze lock.

## 3. Explicitly enable and wait for first start

These commands are intentionally not run by the renderer. Execute them only
after the review above. `Disabled=true` in the template requires the explicit
`launchctl enable` step.

```sh
label="cn.tj.dcar.writer-worker"
domain="gui/$(id -u)"
plist="$HOME/Library/LaunchAgents/$label.plist"

launchctl enable "$domain/$label"
launchctl bootstrap "$domain" "$plist"
```

`RunAtLoad=true` makes `bootstrap` start the worker. Do not immediately follow
it with `kickstart -k`: that kills the just-started process and can leave the
job waiting for the 300-second throttle interval. Instead, give the first
process time to finish its preflight and bind the health port:

```sh
for attempt in $(seq 1 60); do
  if curl -fsS -o /dev/null http://127.0.0.1:8766/api/v8/health 2>/dev/null; then
    break
  fi
  sleep 1
done
```

Verify separation and runtime state without invoking a provider:

```sh
lsof -nP -iTCP:8765 -sTCP:LISTEN
lsof -nP -iTCP:8766 -sTCP:LISTEN
curl -fsS http://127.0.0.1:8766/api/v8/health
curl -fsS http://127.0.0.1:8766/api/v8/scheduler | python3 -m json.tool
launchctl print "gui/$(id -u)/cn.tj.dcar.writer-worker"
```

The worker scheduler must report requested and enabled. Startup catch-up must
report `mode=report_only`, requested/enabled, and eventually `status=succeeded`;
every result must be a `daily_report` or `weekly_report`. A capture or media job
in that result list is a deployment-blocking safety violation. Do not manually
execute `daily_capture` as a smoke test; wait for the authorized scheduled
window or use existing mocked tests.

### Deliberate restart of an already loaded worker

Use `kickstart -k` only when intentionally restarting an existing loaded job,
not as part of first installation or as a retry while its first process is
still starting:

```sh
label="cn.tj.dcar.writer-worker"
domain="gui/$(id -u)"
launchctl kickstart -k "$domain/$label"
```

For a plist update, use the bootout/render/review/bootstrap sequence below so
launchd loads the new definition. After either kind of deliberate restart,
wait for the health port in the same way as the first start.

## Stop, disable, and uninstall

Stopping or uninstalling the LaunchAgent does not delete the database, reports,
cache, external key file, external `writer.env`, or logs.

```sh
label="cn.tj.dcar.writer-worker"
domain="gui/$(id -u)"
plist="$HOME/Library/LaunchAgents/$label.plist"

launchctl bootout "$domain" "$plist"
launchctl disable "$domain/$label"
mkdir -p "$HOME/.Trash/DcarAIGC-launchagents"
mv "$plist" "$HOME/.Trash/DcarAIGC-launchagents/$label.plist"
```

Keep the external configuration until the service is deliberately retired.
For a plist update, boot out the old job, move the old plist aside, render and
review a new one, then repeat the explicit enable/bootstrap sequence.

## Daily read-replica publisher (default disabled)

The second LaunchAgent is scheduled for 09:00 Asia/Shanghai, after the 08:00
daily report window. It does not run a scheduler or provider call. Before it can
publish, it requires all of the following:

1. the loopback writer on port 8766 is healthy and points at the formal DB;
   its health identity must exactly match the DB and must be report v8.6,
   schema 13 / `scheduler-run-attempt-history`, active evaluation-v9 on the
   published selling-points-v5.2 taxonomy, including the DB-frozen matcher SHA;
2. the writer scheduler is enabled and holds the single-writer lock, while
   startup catch-up has succeeded in `report_only` mode and returned only
   `daily_report`/`weekly_report` results;
3. today's 02:00 `daily_capture` row has a terminal status and completion time;
4. today's 07:30 `daily_media_cutoff` succeeded with a valid completion time;
5. the DB/WAL storage and newest content pass the configured freshness gates;
6. a consistent SQLite online-backup snapshot and every referenced artifact
   pass the snapshot builder's checks;
7. a dedicated SSH alias, strict known-host match, dedicated identity, remote
   free-space gate, and all three rsync dry runs pass.

The 08:00 `daily_report` result is not a publication gate: a failed or unavailable
report remains visible as that status in the snapshot and is not silently turned
into success. Any existing manual review or release gate still applies.

The publisher uploads into one unique, versioned staging tree:

```text
/var/lib/dcar-aigc/incoming/<snapshot-id>/
  bundle/
  artifacts/cache/
  artifacts/reports/
```

It never rsyncs directly into the active cache or report roots and never uses
`--delete`. The server verifier hashes the staged version first. With the API
stopped, the installer backs up every differing active artifact before replacing
it and restores those exact bytes together with the DB on failure or rollback.
An identity mismatch in the installed API health response is an install failure
and triggers that same automatic DB/artifact rollback.
The installer is the only component allowed to promote a verified generation.
For unchanged bytes, rsync may hard-link from the active root into staging via
`--link-dest`; active files are only references and are never changed by rsync.

### 1. Prepare the external publisher configuration

Copy the non-secret example outside the repository and protect it. It contains
only deployment coordinates and limits. Do not add a password, private key,
provider key, or BasicAuth credential.

```sh
install -d -m 0700 "$HOME/Library/Application Support/DcarAIGC"
install -d -m 0700 "$HOME/Library/Logs/DcarAIGC"
install -m 0600 deploy/macos/publisher.env.example \
  "$HOME/Library/Application Support/DcarAIGC/publisher.env"
```

Use a dedicated SSH alias. Seed `~/.ssh/known_hosts` through a trusted channel
before the first run; do not use `accept-new` or disable host-key checking.

```sshconfig
Host dcar-prod
  HostName <production-host>
  User <snapshot-publisher-user>
  IdentityFile ~/.ssh/id_ed25519_dcar_prod
  IdentitiesOnly yes
  BatchMode yes
  PasswordAuthentication no
  KbdInteractiveAuthentication no
  StrictHostKeyChecking yes
  UserKnownHostsFile ~/.ssh/known_hosts
```

Keep the identity external to this repository with mode `0600`. On the server,
grant `NOPASSWD` only for the fixed Python interpreter plus
`deploy/server/install_snapshot.py verify|install --bundle` below the fixed
incoming root. Validate the sudoers rule with `visudo`; do not grant a shell or
general passwordless sudo to the publisher account.

The snapshot uses the explicit `thin-server-v1` policy. The capacity gate counts
only the included small evidence/report set, the database bundle, and the
default 5 GiB reserve. Large binary media is listed as optional reuse and is
never transferred; the installer reuses it only when the active file has the
same path, size, and SHA-256. The publisher also parses
`rsync --dry-run --stats` for cache, reports, and bundle, checks free space
again, and checks the reserve once more before remote verification. Any failed
gate stops before the active generation changes.

### 2. Run side-effect-free local checks

The plist check only validates the disabled 09:00 job definition. The publisher
`--check` additionally validates the external file and current writer state; it
does not build a snapshot and does not invoke SSH or rsync.

```sh
python3 deploy/macos/render_snapshot_publisher.py \
  --project-root "$PWD" \
  --check

.venv/bin/python deploy/macos/publish_snapshot.py \
  --project-root "$PWD" \
  --env-file "$HOME/Library/Application Support/DcarAIGC/publisher.env" \
  --db "$PWD/app/data/dcar_insight.sqlite3" \
  --legacy-db "$PWD/app/data/web_mvp.sqlite3" \
  --check
```

The local writer must already be running for the second command. A successful
result explicitly reports `no_snapshot_built=true` and
`no_ssh_attempted=true`.

### 3. Render, review, and explicitly enable

Rendering still leaves the job disabled and unloaded. The commands below are
operator steps; this repository does not execute them automatically.

```sh
python3 deploy/macos/render_snapshot_publisher.py \
  --project-root "$PWD" \
  --output "$HOME/Library/LaunchAgents/cn.tj.dcar.snapshot-publisher.plist"

plutil -lint \
  "$HOME/Library/LaunchAgents/cn.tj.dcar.snapshot-publisher.plist"
plutil -p \
  "$HOME/Library/LaunchAgents/cn.tj.dcar.snapshot-publisher.plist"
```

Only after the server installer, sudo rule, SSH alias, capacity, and rollback
have been reviewed, enable and bootstrap the job. Do not `kickstart` it merely
as a smoke test because that performs a real publication.

```sh
label="cn.tj.dcar.snapshot-publisher"
domain="gui/$(id -u)"
plist="$HOME/Library/LaunchAgents/$label.plist"

launchctl enable "$domain/$label"
launchctl bootstrap "$domain" "$plist"
```

The writer keeps `DCAR_STARTUP_CATCHUP_ENABLED=1` exclusively for report-only
catch-up. The publisher and Ubuntu replica both keep scheduler and catch-up set
to `0`; enabling the publisher does not broaden provider authorization. Paid
capture and media processing remain scheduled or explicitly operator-invoked
writer work and are never triggered by startup catch-up.

### Failure handling and monitoring

A failed local build leaves no partial named snapshot. A failed remote space,
rsync, verify, or install step never changes the local formal DB and does not
delete local snapshots or server history. A unique remote staging directory may
remain for investigation. The server installer owns active-generation rollback;
never manually copy staged files over an active root.

There is deliberately no publisher-side `rm` or automatic remote cleanup in
this change. `--link-dest` prevents unchanged artifacts from consuming another
full 40.7 GB, but each successful staging tree still retains its database
bundle and changed bytes. Before enabling unattended daily publication, monitor
`/var/lib/dcar-aigc` capacity and use this retention policy: keep the active
staging tree, the two previous successful trees, and failed trees until their
incident is closed. Pruning must be a separate root-owned, receipt-aware server
operation that refuses to remove the active snapshot or rollback history; do
not add a wildcard `rm` cron job. Such a garbage collector is not installed or
enabled by this deployment.

```sh
launchctl print "gui/$(id -u)/cn.tj.dcar.snapshot-publisher"
tail -n 200 "$HOME/Library/Logs/DcarAIGC/snapshot-publisher.stdout.log"
tail -n 200 "$HOME/Library/Logs/DcarAIGC/snapshot-publisher.stderr.log"
```

After success, inspect the local `publisher-receipt.json` under that day's
snapshot and the server's active-snapshot receipt. The local receipt records
both capture and media-cutoff states plus the three dry-run sizes. Confirm the
online overview, yesterday, and this-week windows before treating the
publication as complete.

To disable and remove only this LaunchAgent (without deleting snapshots,
configuration, logs, or server data):

```sh
label="cn.tj.dcar.snapshot-publisher"
domain="gui/$(id -u)"
plist="$HOME/Library/LaunchAgents/$label.plist"

launchctl bootout "$domain" "$plist"
launchctl disable "$domain/$label"
mkdir -p "$HOME/.Trash/DcarAIGC-launchagents"
mv "$plist" "$HOME/.Trash/DcarAIGC-launchagents/$label.plist"
```
