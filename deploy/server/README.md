# DcarAIGC read-replica deployment

The production topology has one writer and one read replica:

- the designated macOS worker owns the formal SQLite database, provider calls,
  media processing, evaluations, and scheduled reports;
- the Ubuntu server serves the authenticated UI/API from a verified snapshot;
- the Ubuntu scheduler and startup catch-up always remain disabled.

Do not enable capture on the Ubuntu host. Its API image deliberately excludes
the macOS Vision OCR and local media-analysis runtime, and a second scheduler
would also create two writers and duplicate provider spend.

## Persistent server layout

Create the persistent directories before the first release. The API service
only reads the active DB, reports, and cache. The publisher writes only below
`incoming`; the root-run host installer writes active data while the API is
stopped.

```sh
sudo install -d -o root -g dcar-aigc -m 0750 \
  /var/lib/dcar-aigc/db \
  /var/lib/dcar-aigc/reports \
  /var/lib/dcar-aigc/cache
sudo install -d -o dcar-aigc -g dcar-aigc -m 0750 \
  /var/lib/dcar-aigc/incoming
sudo install -d -o dcar-aigc -g dcar-aigc -m 0700 \
  /var/lib/dcar-aigc/auth
sudo install -d -o root -g root -m 0755 /var/lib/dcar-aigc/runtime
```

The systemd units bind the persistent directories into the versioned release
tree. Code releases must never carry or overwrite `app/data`, `data/cache`, or
`reports`.

For the Compose alternative, its unprivileged container user is uid/gid
`10001`; grant that identity read access only. The container filesystem and the
DB, report, and cache binds are all read-only. Read-only API connections use
SQLite URI read-only/immutable mode plus `query_only`; they do not create
WAL/SHM files. Only the stopped-service host installer updates active data.

## Server environment

Install `deploy/server/dcar.env.example` as `/etc/dcar-aigc/dcar.env`, owned by
root and mode `0600`. These production gates are mandatory:

```text
DCAR_READ_ONLY=1
DCAR_SCHEDULER_ENABLED=0
DCAR_STARTUP_CATCHUP_ENABLED=0
```

Do not configure `TIKHUB_API_KEY` or `TIKHUB_API_KEY_FILE` on the read replica.
If a future designated writer uses a credential file, keep it outside the
repository with restrictive permissions and point `TIKHUB_API_KEY_FILE` to it.

## Build and install a code release

Use a versioned directory under `/var/www/dcar-aigc/releases` and update the
`current` symlink only after backend tests and the public-path web build pass.
The browser-facing values must be present at build time; systemd runtime
variables cannot rewrite an already-built client bundle.

```sh
cd /var/www/dcar-aigc/releases/<release>
/var/www/dcar-aigc/runtime/python-3.12.13/bin/python3 -m venv .venv
.venv/bin/pip install -r deploy/server/requirements-api.txt

# Required empty bind targets. Never copy active data into a code release.
install -d -m 0750 app/data data/cache reports

cd app/web
npm ci
DCAR_WEB_BASE_PATH=/dcar \
NEXT_PUBLIC_DCAR_API_BASE=/dcar \
node ./node_modules/vinext/dist/cli.js build
```

If the configured package mirror cannot serve an exact pinned version, populate
the new release from a separately verified Linux wheelhouse or make a physical
copy of the previous release's already verified `site-packages`, then run
`.venv/bin/python -m pip check` and save `pip freeze` in the release. Never use
a symlink or hard-link clone that lets a later install mutate the rollback
environment.

Keep each release's `.venv` inside that release. Installing a new dependency
must not mutate the running or rollback release. After temporary-port API/Web/
authentication smoke checks pass, stop the authentication gateway, Web and API
first, then switch `current` atomically:

```sh
sudo systemctl stop dcar-auth dcar-web dcar-api
ln -s /var/www/dcar-aigc/releases/<release> /var/www/dcar-aigc/current.next
mv -Tf /var/www/dcar-aigc/current.next /var/www/dcar-aigc/current
```

Record the previous symlink target and unit files before this step. If any
service or the public-path smoke check fails, stop the three new services, restore
that exact symlink and the prior units, reload systemd, and start the previous
three services. Before a code-only rollback, verify that the previous release supports
the active SQLite `user_version`; otherwise roll back its matching data snapshot
at the same time.

Install the units and the Nginx include, then reload their configurations:

```sh
sudo install -m 0644 deploy/server/systemd/dcar-api.service \
  /etc/systemd/system/dcar-api.service
sudo install -m 0644 deploy/server/systemd/dcar-web.service \
  /etc/systemd/system/dcar-web.service
sudo install -m 0644 deploy/server/systemd/dcar-auth.service \
  /etc/systemd/system/dcar-auth.service
sudo systemctl daemon-reload
sudo systemctl enable dcar-api dcar-web dcar-auth
sudo systemctl start dcar-api dcar-web dcar-auth
curl -fsS http://127.0.0.1:4173/dcar/auth/health
sudo nginx -t
sudo systemctl reload nginx
```

`dcar-api.service` intentionally refuses to start until the first verified
`dcar_insight.sqlite3` has been installed.

The Compose file is an alternative loopback-only runtime. Its web build now
defaults to `/dcar`, its API runs read-only with both schedulers disabled, and
its health check reads the overview database rather than returning a shallow
process-only response.

## Build a consistent snapshot on the macOS writer

Run this only after today's 02:00 capture has reached a terminal state and the
07:30 media cutoff has succeeded. The script uses SQLite's online backup API,
so it never copies a live WAL database directly. It then requires
`quick_check=ok`, zero foreign key violations, the expected schema, and exact
hashes for report and visible evidence files. It also freezes one
`dcar-runtime-identity-v1` value from the backup DB and refuses to build unless
that identity is report v8.6, schema 13 / `scheduler-run-attempt-history`, and
the active `evaluation-v9__selling-points-v5.2` release on the published v5.2
taxonomy. The matcher SHA-256 is read from that release row, never hard-coded.

```sh
cd /Users/mark/Documents/DcarAIGC
snapshot_dir="/private/tmp/dcar-snapshot-$(date -u +%Y%m%dT%H%M%SZ)"

.venv/bin/python scripts/build_server_snapshot.py \
  --project-root "$PWD" \
  --db app/data/dcar_insight.sqlite3 \
  --legacy-db app/data/web_mvp.sqlite3 \
  --expected-user-version 13 \
  --output "$snapshot_dir"
```

The output contains:

- consistent `databases/dcar_insight.sqlite3` and optional legacy DB backups;
- `manifest.json` plus its separate SHA-256;
- the exact report/schema/release/taxonomy/matcher runtime identity;
- NUL-delimited `cache-files-from0` and `reports-files-from0` lists.

The manifest uses the explicit `thin-server-v1` policy. Databases, legacy DB,
reports, the two HMAC salt files required by the read-only API, and small
JSON/text evidence are included and transferred. Large
binary evidence is listed separately as optional reuse: it is never transferred
by this publisher, and is served only when the active file at the same path has
the exact recorded size and SHA-256. Missing or mismatched optional evidence is
explicitly omitted; it is not deleted or overwritten. Unregistered cache files
outside these database-backed evidence paths are not published.

Remote capacity gates use only included artifact bytes plus the bundle and the
configured free-space reserve. Optional large-byte totals are reported for
audit but never inflate the required staging capacity.

## Stage one versioned snapshot

The examples below assume an operator-configured SSH alias named `dcar-prod`.
Use an SSH identity dedicated to this server with `IdentitiesOnly yes`; never
put a password or private key in this repository.

The 09:00 disabled-by-default publisher is documented in
`deploy/macos/README.md`. It performs the freshness, SSH, free-space, and rsync
dry-run gates automatically. The layout below documents its server contract;
do not rsync artifacts directly into active cache or report roots.

Read the snapshot ID without interpolating untrusted shell output:

```sh
snapshot_id="$($PWD/.venv/bin/python -c \
  'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["snapshot_id"])' \
  "$snapshot_dir/manifest.json")"
remote_stage="/var/lib/dcar-aigc/incoming/$snapshot_id"
```

Create one new, immutable staging tree. A snapshot ID must never be reused:

```sh
ssh dcar-prod \
  "test ! -e '$remote_stage' && install -d -m 0750 \
    '$remote_stage' \
    '$remote_stage/artifacts' \
    '$remote_stage/artifacts/cache' \
    '$remote_stage/artifacts/reports' \
    '$remote_stage/bundle'"
```

Synchronize the exact hash-listed artifacts into that version only. There is
deliberately no `--delete`, and these commands must be preceded by the included
thin-set and `rsync --dry-run --stats` capacity checks used by the macOS
publisher:

```sh
rsync -a --checksum --delay-updates --from0 \
  --link-dest=/var/lib/dcar-aigc/cache \
  --files-from="$snapshot_dir/cache-files-from0" \
  data/cache/ "dcar-prod:$remote_stage/artifacts/cache/"

rsync -a --checksum --delay-updates --from0 \
  --link-dest=/var/lib/dcar-aigc/reports \
  --files-from="$snapshot_dir/reports-files-from0" \
  reports/ "dcar-prod:$remote_stage/artifacts/reports/"
```

The active roots above are read-only link sources. Rsync never uses them as a
destination, and only hash-identical files are hard-linked into staging.

Then transfer the database bundle inside the same version:

```sh
rsync -a --delay-updates "$snapshot_dir/" \
  "dcar-prod:$remote_stage/bundle/"
```

The verifier validates the thin policy and checks only included staged files;
optional-reuse files are checked against active storage and never required in
staging. During promotion, every differing included active artifact is copied
into the same snapshot history before atomic replacement. The installer then replaces
the DB, starts the API, and restores both DB and artifact bytes if smoke checks
fail. Activated databases and included files are normalized to
`root:dcar-aigc` mode `0640`, with managed directories mode `0750`, so the
read-only API cannot be broken by macOS source files arriving as `0600`. No
publisher or operator may overwrite an active artifact beforehand.

## Verify and atomically install

The verifier hashes the staged database and every listed persistent artifact
before stopping the API. Installation then:

1. obtains `/var/lib/dcar-aigc/runtime/snapshot-install.lock`;
2. verifies the staged DB and included artifact hashes before stopping the API;
3. stops only `dcar-api.service`;
4. preserves the current DB set and every changing artifact in snapshot history;
5. atomically replaces only included staged artifacts and DB files while the
   API is down;
6. starts the API and reads health, overview, and scheduler endpoints;
7. restores the previous DB and artifact bytes if any later step fails.

```sh
cd /var/www/dcar-aigc/current
sudo /var/www/dcar-aigc/current/.venv/bin/python \
  deploy/server/install_snapshot.py verify --bundle "$remote_stage/bundle"

sudo /var/www/dcar-aigc/current/.venv/bin/python \
  deploy/server/install_snapshot.py install --bundle "$remote_stage/bundle"
```

Successful installation writes a non-secret receipt to
`/var/lib/dcar-aigc/runtime/active-snapshot.json`. The API smoke check compares
the active DB SHA-256, schema version, content count, latest published date, and
the full runtime identity with the manifest. It also requires read-only mode,
scheduler requested/enabled both false, and startup catch-up unrequested.

Restart the authentication gateway and Web together when the code release
changed; a data-only snapshot does not require rebuilding the Web bundle or
restarting either service.

The publisher does not automatically delete `incoming/<snapshot-id>` after a
successful install. `--link-dest` hard-links unchanged artifacts, so those
files do not consume another full copy, but bundle DBs and changed bytes still
accumulate. Monitor capacity and retain the active staging tree, two previous
successful trees, and unresolved failed trees. Pruning must be performed by a
future root-owned, receipt-aware server command that protects the active ID and
rollback history. Do not use wildcard `rm` or an unaudited cleanup cron; no such
cleanup is installed by this deployment.

## Manual rollback

The history directory named for an installed snapshot contains the databases
and changed artifact bytes that were active immediately before that snapshot
was installed. To undo a specific installation:

```sh
cd /var/www/dcar-aigc/current
sudo /var/www/dcar-aigc/current/.venv/bin/python \
  deploy/server/install_snapshot.py rollback \
  --snapshot-id <installed-snapshot-id>
```

Omitting `--snapshot-id` selects the newest eligible snapshot history. Before
rollback, the installer makes a separate safety backup of the current DBs and
affected artifacts. A failed rollback restores that safety backup and restarts
the API.

## Public Nginx path

Install `deploy/server/nginx/dcar-proxy.conf` inside the existing TLS server
block. Nginx sends all `/dcar/` traffic to the authentication gateway on 4173;
the gateway keeps opaque, revocable Sessions in `/var/lib/dcar-aigc/auth`, then
forwards authenticated Web traffic to 4174 and API traffic to 8765. All three
ports stay bound to server loopback. The existing htpasswd file remains the
account source, but browser HTTP Basic authentication is no longer used. See `AUTH.md` for the
complete login, logout and acceptance contract.

Post-install acceptance must cover:

- `GET /dcar/api/v8/overview` returns the expected latest published date;
- yesterday and this-week windows match the writer snapshot;
- `GET /dcar/api/v8/scheduler` reports both scheduling gates off;
- one report download passes its registered hash; omitted large evidence returns
  a clear unavailable response instead of a server error;
- write endpoints are rejected by read-replica mode.
