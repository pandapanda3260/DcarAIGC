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
sudo install -d -o root -g dcar-aigc -m 0751 /var/lib/dcar-aigc
sudo install -d -o root -g dcar-aigc -m 0750 \
  /var/lib/dcar-aigc/db \
  /var/lib/dcar-aigc/reports \
  /var/lib/dcar-aigc/cache
sudo install -d -o dcar-aigc -g dcar-aigc -m 0750 \
  /var/lib/dcar-aigc/incoming
sudo install -d -o dcar-aigc -g dcar-aigc -m 0700 \
  /var/lib/dcar-aigc/auth
sudo id -u dcar-douyin >/dev/null 2>&1 \
  || sudo useradd --system --user-group --home-dir /nonexistent \
    --shell /usr/sbin/nologin dcar-douyin
sudo install -d -o dcar-douyin -g dcar-douyin -m 0700 \
  /var/lib/dcar-aigc/douyin-control
sudo install -d -o root -g root -m 0700 \
  /var/backups/dcar-aigc/douyin-control \
  /var/backups/dcar-aigc/auth \
  /etc/dcar-aigc/credentials
sudo install -d -o dcar-aigc -g dcar-aigc -m 0700 /var/lib/dcar-aigc/auth-smoke
sudo install -d -o root -g root -m 0755 /var/lib/dcar-aigc/runtime
```

The authentication gateway reads two root-only credential files through
systemd `LoadCredential`: `/etc/dcar-aigc/credentials/sms-tencent` (Tencent
Cloud SMS, `KEY=VALUE` lines — the `TENCENT_SMS_*` section of `dcar.env.local`
copied verbatim: `TENCENT_SMS_SECRET_ID`, `TENCENT_SMS_SECRET_KEY`,
`TENCENT_SMS_SDK_APP_ID`, `TENCENT_SMS_SIGN_NAME`, `TENCENT_SMS_TEMPLATE_ID`,
optional `TENCENT_SMS_REGION` and `TENCENT_SMS_CODE_TTL_MINUTES`; quotes are
accepted, unrelated keys are ignored) and `/etc/dcar-aigc/credentials/auth-pepper` (at least 32
random hex characters, for example `python3 -c 'import secrets; print(secrets.token_hex(32))'`).
Install both as `root:root` mode `0600` before the first release that ships the
SQLite account store; see `AUTH.md`.

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

The Douyin control plane ships fail-closed in stage 0. The application defaults
to these values when `/etc/dcar-aigc/douyin-stage1.env` is absent or omits them:

```text
DOUYIN_AUTHORIZATION_ENABLED=0
DCAR_DOUYIN_PROVIDER=disabled
DCAR_DOUYIN_PROXY_URL=http://127.0.0.1:4176
```

Create independent random Edge, Machine, Fernet-keyring, and open-id HMAC
credentials below `/etc/dcar-aigc/credentials`, owned by root and mode `0600`.
The Edge credential is shared only by `dcar-auth` and the control plane; the
Machine credential is only for `/internal/v1/*`. The credential shown in
earlier screenshots is considered compromised. Rotate it in the Douyin console,
then install only the replacement as
`/etc/dcar-aigc/credentials/douyin-client-secret`, owned by root and mode `0600`,
before installing the Stage-1 control unit. The unit exposes it to 4175 only as
the systemd credential file
`/run/credentials/dcar-douyin-control.service/douyin-client-secret`; the secret
itself is not an environment value and the `proxy` account cannot read the
source file. Keep real authorization and
the real provider disabled until the provider implementation and canary gates
pass.

## Restricted Douyin OpenAPI egress

4175 retains `IPAddressDeny=any` plus `IPAddressAllow=localhost`. Its only
network path to Douyin is an independently sandboxed Squid process on
`127.0.0.1:4176`. Squid accepts CONNECT only for the exact destination
`open.douyin.com:443`; subdomains, IP literals, other domains, non-CONNECT
methods, and other ports are denied. It does not decrypt TLS or cache/log
request metadata.

Install the Ubuntu Squid package, the dedicated configuration, and its unit
before restarting 4175:

```sh
sudo apt-get update
sudo apt-get install --yes squid
sudo install -d -o root -g root -m 0755 /etc/squid
sudo install -o root -g root -m 0644 \
  deploy/server/squid/dcar-douyin-egress.conf \
  /etc/squid/dcar-douyin-egress.conf
sudo install -o root -g root -m 0644 \
  deploy/server/systemd/dcar-douyin-egress.service \
  /etc/systemd/system/dcar-douyin-egress.service
sudo /usr/sbin/squid -k parse -f /etc/squid/dcar-douyin-egress.conf
sudo systemctl daemon-reload
sudo systemctl enable --now dcar-douyin-egress.service
```

The control unit uses the explicit
`DCAR_DOUYIN_PROXY_URL=http://127.0.0.1:4176` contract. Provider code must use
that value explicitly with ambient proxy discovery disabled; it must never
fall back to a direct connection when the proxy is unavailable. The base unit
loads the optional root-owned `/etc/dcar-aigc/douyin-stage1.env`. Each gate is
defined in only one place so the file is the single operational source of truth;
do not duplicate the same variables with `Environment=` in the unit. With the
file absent, application defaults remain `DOUYIN_AUTHORIZATION_ENABLED=0` and
`DCAR_DOUYIN_PROVIDER=disabled`, so installing the proxy alone cannot enable a
real authorization or provider call.

## Build and install a code release

Use a versioned directory under `/var/www/dcar-aigc/releases` and update the
`current` symlink only after backend tests and the public-path web build pass.
The browser-facing values must be present at build time; systemd runtime
variables cannot rewrite an already-built client bundle.

```sh
sudo install -d -o root -g root -m 0755 \
  /var/www/dcar-aigc /var/www/dcar-aigc/releases /var/www/dcar-aigc/runtime
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
must not mutate the running or rollback release. For an auth-only upgrade,
physically copy the current production release, then overlay only the reviewed
auth/Web delta: production business/schema19 code may be newer than the branch
baseline. Do not replace that code with a stale full repository archive. The
code release does not invoke the snapshot installer or restart the macOS writer.

The executable `deploy/server/libexec/dcar-auth-release.py` is the single auth
cutover entrypoint. It checks the candidate on temporary ports while production
is still running, then takes `snapshot-install.lock` using nonblocking
`fcntl.flock(LOCK_EX | LOCK_NB)`, checks `current` again, stops services and the
backup timer, makes a verified pre-migration backup, migrates/imports, provisions
the first superadmin, makes a post-migration backup, installs the auth/backup
units and both helpers, switches `current` atomically, and starts/checks services.
It holds `/var/lib/dcar-aigc/runtime/snapshot-install.lock` through this critical
section, matching the snapshot installer. A busy lock causes no service changes.

The helper also checks disk headroom before smoke and again under the lock,
before any service is stopped. For each actual filesystem containing the auth
DB/sidecars, backups, audit log, receipt/saved units or installed helpers, it
budgets migration journals, schema growth, both backups and atomic file writes,
then requires **at least 2 GiB and 3% of total capacity still available**. Budgets
sharing a device are added, and root-reserved blocks do not count. The two
measurements are saved as `disk_preflight` / `disk_locked` in the receipt.
Rollback/restore use the same checks with recovery-specific budgets. There is
no environment variable or CLI flag to skip or lower this floor. A disk at 96%
usage is not automatically rejected if its remaining capacity satisfies the
actual budget; do not delete historical data or rollback releases just to reach
an arbitrary 10% threshold. If the check fails, keep production running, add
capacity or review precisely which generated artifacts are safe to relocate,
and rerun preflight. Candidate cloning/build capacity must be checked before
preparing the candidate; this helper sees that already-allocated space.

The temporary-port Auth smoke must run through a transient systemd unit so the
root-only credentials reach the `dcar-aigc` user exactly as in production, and
it must use a scratch account store, never the live one:

```sh
REL=/var/www/dcar-aigc/releases/<release>
sudo install -d -o dcar-aigc -g dcar-aigc -m 0700 /var/lib/dcar-aigc/auth-smoke
sudo install -d -o root -g root -m 0700 /var/backups/dcar-aigc/auth
sudo systemd-run --wait --pipe --collect --unit dcar-sms-test \
  -p User=dcar-aigc -p Group=dcar-aigc \
  -p LoadCredential=sms-tencent:/etc/dcar-aigc/credentials/sms-tencent \
  -E PYTHONPATH="$REL/src/dcar_eval" \
  "$REL/.venv/bin/python" -m dcar_auth.admin sms-test <verification-phone> \
  --sms-credentials-file /run/credentials/dcar-sms-test.service/sms-tencent
sudo systemd-run --unit dcar-auth-smoke -p User=dcar-aigc -p Group=dcar-aigc \
  -p LoadCredential=sms-tencent:/etc/dcar-aigc/credentials/sms-tencent \
  -p LoadCredential=auth-pepper:/etc/dcar-aigc/credentials/auth-pepper \
  -E PYTHONPATH=$REL/src/dcar_eval -E DCAR_AUTH_BASE_PATH=/dcar \
  -E DCAR_AUTH_WEB_UPSTREAM=http://127.0.0.1:<web-smoke-port> \
  -E DCAR_AUTH_API_UPSTREAM=http://127.0.0.1:<api-smoke-port> \
  -E DCAR_AUTH_SESSION_DB=/var/lib/dcar-aigc/auth-smoke/sessions.sqlite3 \
  -E DCAR_AUTH_LOGIN_TEMPLATE=$REL/deploy/server/nginx/login.html \
  -E DCAR_AUTH_SECURE_COOKIE=1 -E DCAR_AUTH_SMS_PROVIDER=tencent \
  $REL/.venv/bin/python -m uvicorn dcar_auth.gateway:app \
  --app-dir $REL/src/dcar_eval --host 127.0.0.1 --port 14173 --workers 1
curl -fsS http://127.0.0.1:14173/dcar/auth/health
curl -fsS http://127.0.0.1:14173/dcar/login | grep -c '<form '     # expect 3
curl -sS -o /dev/null -w '%{http_code}\n' -X POST \
  -H 'Origin: http://127.0.0.1:14173' -H 'X-Dcar-Request: code' \
  --data 'phone=13000000000&purpose=login' http://127.0.0.1:14173/dcar/auth/code   # expect 404, no SMS
```

Before cutover, also run `tests/run_auth_integration_browser_smoke.sh` against
the candidate build/scratch stores: password/code login, reset and old-session
revocation, disabled account 403 banner, and operator/admin/superadmin route and
mutation controls. API/Web/Control temporary-port processes must be running and
verified independently. Keep them and the scratch Auth service alive through
the helper's pre-smoke; these are not production ports. A real Tencent send and
phone receipt must already have passed with the production signature/template.

Before cutover, **do not run new account CLI commands against the live old
database**. Only explicit `migrate` changes the schema; normal commands such as
`list`, `set-phone` and `allow-phone` check schema/health and reject an old or
missing database with a "run migrate first" error. `sms-test` does not open the
account database. Use it for the real receipt test, and keep other candidate
experiments on scratch databases. Leave formal migration/import/bootstrap to
`dcar-auth-release deploy`, which coordinates it with stop/backup/rollback.

Use a new receipt filename for every attempt. Schema 0 imports htpasswd once;
schema 1/2 migrates to schema 3 without re-importing its existing accounts; schema 3 is
validated without re-import. New registrations receive `new_user` and must be
explicitly authorized by an administrator before any business access. All paths below are server paths:

```sh
REL=/var/www/dcar-aigc/releases/<release>
RECEIPT=/var/lib/dcar-aigc/runtime/auth-release-<UTC-timestamp>.json
sudo "$REL/.venv/bin/python" "$REL/deploy/server/libexec/dcar-auth-release.py" deploy \
  --candidate-release "$REL" --receipt "$RECEIPT" --superadmin dcar \
  --pre-smoke-url http://127.0.0.1:14173/dcar/auth/health \
  --pre-smoke-url http://127.0.0.1:<api-smoke-port>/health \
  --pre-smoke-url http://127.0.0.1:<web-smoke-port>/dcar/overview \
  --post-smoke-url http://127.0.0.1:4173/dcar/auth/health \
  --post-smoke-url https://<public-host>/dcar/login
sudo systemctl stop dcar-auth-smoke
```

Existing session fingerprints are unchanged by the import, so browsers that were
logged in before the release do not need to log in again. Every CLI change and
every change made on the user-management page is appended to
`/var/lib/dcar-aigc/auth/auth-changes.log`; version-2 intent is durable before
the database transaction commits, and a commit marker supplies the completion
time. `changes --since <ISO time>` emits normalized committed/conservative
records. Exit 3 requires reconciliation, not retrying or ignoring the warning.
Only after `deploy` returns success, bind the verified operator phone with `set-phone dcar <phone>`
and `allow-phone <phone> --note <operator>` using the CLI below. Do not guess a
phone or send registration SMS to a person whose number has not been confirmed.
Do not insert these account writes between the deploy helper's migration and
its successful final smoke. A corrupt or partially written change log fails
closed; stop account writers and use the evidence-preserving offline procedure
in [the operations manual](../../docs/v8/运行与备份手册.md#认证安全变更日志损坏处置).

The receipt and its sibling `.files` directory retain the previous symlink,
four old service units, prior auth helper files, their modes/ownership/hashes,
and verified before/after backups. This auth overlay does not change Nginx or
the API/Web/Control units. On a failed pre-switch migration it restores old
units and leaves htpasswd untouched. A schema-1/2 binary cannot safely run a schema-3
database, so that failure stays stopped until the rollback below completes.
For a trusted database and an unsuccessful cutover, use the same receipt:

```sh
sudo "$REL/.venv/bin/python" "$REL/deploy/server/libexec/dcar-auth-release.py" rollback \
  --candidate-release "$REL" --receipt "$RECEIPT" \
  --post-smoke-url https://<public-host>/dcar/login
```

Rollback is phase-aware. Before `current` switched, it never exports htpasswd.
After a schema-0 cutover it exports current active users with business access
and current password hashes (excluding `new_user`), refuses an empty export,
installs it atomically, and preserves schema-3 tables for a future upgrade.
A schema-3 previous release uses the current DB. A schema-1/2 previous release
requires restoration of the receipt's verified backup at that version, first
preserves the current DB, revokes sessions/challenges,
restores old units/code, and exits 3 with services stopped for account-change
reconciliation. Corruption or suspected compromise is not a code-only rollback:
use the offline restore procedure in `docs/v8/运行与备份手册.md`; never export an
untrusted current database into htpasswd.

The following unit/Nginx installation block is for first server provisioning,
not an auth-overlay release or its rollback. Existing servers use the helper
above so that their prior units remain recoverable:

```sh
sudo install -m 0644 deploy/server/systemd/dcar-api.service \
  /etc/systemd/system/dcar-api.service
sudo install -m 0644 deploy/server/systemd/dcar-web.service \
  /etc/systemd/system/dcar-web.service
sudo install -m 0644 deploy/server/systemd/dcar-auth.service \
  /etc/systemd/system/dcar-auth.service
sudo install -m 0644 deploy/server/systemd/dcar-douyin-control.service \
  /etc/systemd/system/dcar-douyin-control.service
sudo install -m 0644 deploy/server/systemd/dcar-douyin-egress.service \
  /etc/systemd/system/dcar-douyin-egress.service
sudo install -m 0644 deploy/server/systemd/dcar-douyin-vault-backup.service \
  /etc/systemd/system/dcar-douyin-vault-backup.service
sudo install -m 0644 deploy/server/systemd/dcar-douyin-vault-backup.timer \
  /etc/systemd/system/dcar-douyin-vault-backup.timer
sudo install -m 0755 deploy/server/libexec/dcar-douyin-vault-backup.py \
  /usr/local/libexec/dcar-douyin-vault-backup
sudo install -m 0644 deploy/server/systemd/dcar-auth-backup.service \
  /etc/systemd/system/dcar-auth-backup.service
sudo install -m 0644 deploy/server/systemd/dcar-auth-backup.timer \
  /etc/systemd/system/dcar-auth-backup.timer
sudo install -m 0755 deploy/server/libexec/dcar-auth-backup.py \
  /usr/local/libexec/dcar-auth-backup
sudo install -m 0755 deploy/server/libexec/dcar-auth-release.py \
  /usr/local/libexec/dcar-auth-release
sudo systemctl daemon-reload
sudo systemctl enable dcar-api dcar-web dcar-douyin-egress \
  dcar-douyin-control dcar-auth \
  dcar-douyin-vault-backup.timer dcar-auth-backup.timer
sudo systemctl start dcar-api dcar-web dcar-douyin-egress \
  dcar-douyin-control dcar-auth \
  dcar-douyin-vault-backup.timer dcar-auth-backup.timer
curl -fsS http://127.0.0.1:4173/dcar/auth/health
curl -fsS -H "X-Dcar-Machine-Key: $(sudo cat /etc/dcar-aigc/credentials/douyin-machine-key)" \
  http://127.0.0.1:4175/internal/v1/health
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

Run this only after the Matrix-first frozen scan/report dependency receipts
pass the publisher gate (or the explicit first-day cutover receipt). The
retired 02:00/07:30 jobs are not substitutes. The script uses SQLite's online backup API,
so it never copies a live WAL database directly. It then requires
`quick_check=ok`, zero foreign key violations, the expected schema, and exact
hashes for report and visible evidence files. It also freezes one
`dcar-runtime-identity-v1` value from the backup DB and refuses to build unless
that identity is report v8.9, schema 19 /
`dual-acquisition-profile-roster-v1`, and
the active `evaluation-v9__selling-points-v5.2` release on the published v5.2
taxonomy. The matcher SHA-256 is read from that release row, never hard-coded.

```sh
cd /Users/mark/Projects/DcarAIGC
snapshot_dir="/private/tmp/dcar-snapshot-$(date -u +%Y%m%dT%H%M%SZ)"

.venv/bin/python scripts/build_server_snapshot.py \
  --project-root "$PWD" \
  --db app/data/dcar_insight.sqlite3 \
  --legacy-db app/data/web_mvp.sqlite3 \
  --expected-user-version 19 \
  --output "$snapshot_dir"
```

The output contains:

- consistent `databases/dcar_insight.sqlite3` and optional legacy DB backups;
- `manifest.json` plus its separate SHA-256;
- the exact report/schema/release/taxonomy/matcher runtime identity;
- NUL-delimited `cache-files-from0` and `reports-files-from0` lists.

The manifest uses the explicit `thin-server-v2` policy. Databases, legacy DB,
reports, the two HMAC salt files required by the read-only API, and small
JSON/text evidence are included and transferred. Large
binary evidence is listed separately as optional reuse: it is never transferred
by this publisher, and is served only when the active file at the same path has
the exact recorded size and SHA-256. Missing or mismatched optional evidence is
explicitly omitted; it is not deleted or overwritten. Unregistered cache files
outside these database-backed evidence paths are not published. All newly
managed originals are instead listed in `managed_originals-v1`: none are
transferred or opportunistically reused. Their exact manifest, preview,
completion and lifecycle proofs remain required. An unknown missing required
file still fails the build. Signed database and JSON paths are not rewritten;
the read-only API resolves only exact members of the installed, hash-bound
manifest. Legacy v1 bundles are historical rollback evidence, not new builds.

The normal publisher/install path is 19→19 only. The schema-19 deployment
requires an installed schema18/report-v8.8 compatibility release whose
installer recognizes the exact 18→19 contract, followed by the sealed new
release's `seal-schema-upgrade --from-schema 18 --to-schema 19` and
`schema-upgrade --from-schema 18 --to-schema 19` operations. The historical
17→18 pair remains supported only by its own exact seal and transition
contracts; neither pair accepts skipped or widened source/target versions. A
schema17 source is a blocker for the 18→19 operation, not an invitation to skip
a gate. The v2 bundle is sealed into the paired transaction.
The paired transaction owns the existing snapshot-install lock, stops the
four API/Web/Auth/Control services, and restores code, configuration, databases,
artifacts and the previous active receipt together on failure. An unsettled
transition blocks normal installation/publication; do not manually replace
`current` or clear its marker. A successful source seal is not a successful
deployment: the production runtime and read-only smoke checks must pass.

Remote capacity gates use only included artifact bytes plus the bundle and the
configured free-space reserve. Optional large-byte totals are reported for
audit but never inflate the required staging capacity.

## Stage one versioned snapshot

The examples below assume an operator-configured SSH alias named `dcar-prod`.
Use an SSH identity dedicated to this server with `IdentitiesOnly yes`; never
put a password or private key in this repository.

The unattended publisher is documented in `deploy/macos/README.md`. It runs at
login, at 09:00, and performs hourly reconciliation. It performs the freshness,
SSH, free-space, and rsync dry-run gates automatically. The layout below
documents its server contract; do not rsync artifacts directly into active
cache or report roots.

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

Restart Control, the authentication gateway, and Web together when the code
release changed; a data-only snapshot does not require rebuilding the Web bundle
or restarting those services.

Before a new automatic publish is built, the publisher invokes the root-owned
`prune` command. It reads the validated active receipt while holding the
snapshot install lock, always protects the active snapshot ID, and retains at
most three valid snapshot-ID directories independently under `incoming` and
`snapshot-history`. Invalid names, files, symlinks, and `rollback-*` safety
backups are never deletion candidates. This bounds daily storage growth while
preserving the active tree and two rollback/staging points.

The same fail-closed cleanup can be inspected manually without wildcard paths:

```sh
sudo /var/www/dcar-aigc/current/.venv/bin/python \
  deploy/server/install_snapshot.py prune \
  --incoming-root /var/lib/dcar-aigc/incoming \
  --retain-count 3
```

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

Install `deploy/server/nginx/dcar-http.conf` in the Nginx `http` context and
`deploy/server/nginx/dcar-proxy.conf` inside the existing TLS server block.
Nginx sends all `/dcar/` traffic, including the exact quiet Douyin callback, to
the authentication gateway on 4173. The gateway keeps opaque, revocable
Sessions in `/var/lib/dcar-aigc/auth`, then forwards authenticated Web traffic
to 4174, API traffic to 8765, and bounded Douyin control traffic to 4175. All
four ports stay bound to server loopback. The callback location disables access
logging, raises error logging to `crit`, rate-limits GET, and never bypasses the
gateway. Accounts, phone bindings, the operator admission list, verification
codes and sessions all live in the gateway's SQLite store
`/var/lib/dcar-aigc/auth/sessions.sqlite3`; the old htpasswd file is only an
import source for the first release and a rollback target. See `AUTH.md` for the
complete login, registration, recovery, backup and acceptance contract.

## Douyin authorization operations

Each new scan must start from the exact business-account row. If the callback
reports an authorization conflict, first open the Douyin authorization
management page and check for a historical `pending_match` record occupying the
same Douyin uid. Invalidate that historical record, return to the intended
account row, and start a fresh scan. Never bypass the conflict or reassign the
record directly in SQLite.

## Douyin Vault backup

The Vault deliberately uses SQLite rollback-journal (`journal_mode=DELETE`),
not WAL. The service enforces this at startup and sets `synchronous=EXTRA` on
every connection. The root-run backup helper opens the source read-only, waits
for an ordinary shared lock, uses SQLite `Connection.backup()`, and validates
`quick_check`, foreign keys, schema, tables, and encrypted BLOB columns before
atomically installing a mode-`0600` backup and manifest. It also works while
4175 is stopped. It fails closed if WAL/SHM or a hot rollback journal is present.

Before enabling the timer, run one offline backup and verify the timer:

```sh
sudo systemctl stop dcar-douyin-control
sudo systemctl start dcar-douyin-vault-backup.service
sudo systemctl status dcar-douyin-vault-backup.service --no-pager
sudo systemctl start dcar-douyin-control
sudo systemctl enable --now dcar-douyin-vault-backup.timer
sudo systemctl list-timers dcar-douyin-vault-backup.timer --no-pager
```

## Authentication store backup

The account store shares the Vault's rules: rollback journal only
(`journal_mode=DELETE`), never WAL. `/usr/local/libexec/dcar-auth-backup`
mirrors the Vault helper (read-only source, online `Connection.backup()`,
`synchronous=EXTRA`, `quick_check`, `user_version`, required tables, sha256
dedupe, atomic install of a mode-`0600` backup plus manifest) and additionally
prunes pairs older than `--keep-days` while always keeping the newest valid
pair. Backups contain password hashes, phone numbers and session digests but
never the pepper, so a leaked backup cannot be used to brute-force codes.

Run one backup by hand, verify it, and rehearse a restore before relying on the
timer:

```sh
sudo systemctl start dcar-auth-backup.service
sudo systemctl status dcar-auth-backup.service --no-pager
sudo /usr/local/libexec/dcar-auth-backup --verify \
  "$(ls -t /var/backups/dcar-aigc/auth/dcar-auth-*.sqlite3 | head -1)" --expect-user-version 3
sudo systemctl enable --now dcar-auth-backup.timer
sudo systemctl list-timers dcar-auth-backup.timer --no-pager
```

For a trusted restore, use the `dcar-auth-release.py restore` command in
`docs/v8/运行与备份手册.md`; do not copy or move a backup over the live database
by hand. The helper checks disk headroom, holds the shared snapshot lock,
stops the timer/services, verifies the selected pair, preserves the current
database and sidecars, restores/migrates, and revokes sessions/challenges. Its
expected exit code is 3: services remain stopped until identity reconciliation.
Use the original `auth-changes.log`, the backup time, and the restore receipt to
reconcile deletions, role/status changes and password resets before restarting.
If the store or credentials may have been exposed, also rotate `auth-pepper`
and the Tencent SecretKey, disable accounts, and re-enable each only after an
out-of-band identity check and password reset. For a damaged audit log, follow
the runbook's evidence-preserving tail-repair procedure; never delete middle
records to make validation pass.

## Restricted Mac tunnel

The Mac sync channel uses its own `dcar-douyin-sync` SSH account, key and alias;
it must not reuse the `dcar-prod` publisher account or key. Install the checked
sshd Match block and a reviewed public key (replace the placeholder in the
example before installation):

```sh
sudo id -u dcar-douyin-sync >/dev/null 2>&1 \
  || sudo useradd --system --create-home \
    --home-dir /var/lib/dcar-douyin-sync \
    --shell /usr/sbin/nologin dcar-douyin-sync
sudo install -d -o dcar-douyin-sync -g dcar-douyin-sync -m 0700 \
  /var/lib/dcar-douyin-sync/.ssh
sudo install -o dcar-douyin-sync -g dcar-douyin-sync -m 0600 \
  /tmp/dcar-douyin-sync.authorized_keys \
  /var/lib/dcar-douyin-sync/.ssh/authorized_keys
sudo install -o root -g root -m 0644 \
  deploy/server/ssh/60-dcar-douyin-sync.conf \
  /etc/ssh/sshd_config.d/60-dcar-douyin-sync.conf
sudo sshd -t
sudo systemctl reload ssh
```

The installed `authorized_keys` entry must contain
`command="/usr/sbin/nologin",restrict,port-forwarding,permitopen="127.0.0.1:4175"`.
The account shell and forced command deny shell/session requests. The Match
block permits only client-local forwarding to `127.0.0.1:4175`; it denies PTY,
X11, agent, remote/dynamic destination, tunnel-device and password access.
Every HTTP request through the permitted tunnel must still carry the separate
Machine credential; SSH authentication does not replace application
authentication.

Validate the effective Match block before allowing the Mac to connect:

```sh
sudo sshd -T -C user=dcar-douyin-sync,host=localhost,addr=127.0.0.1 \
  | grep -E '^(allowtcpforwarding|permitopen|permittty|x11forwarding|allowagentforwarding|forcecommand) '
```

Acceptance requires `allowtcpforwarding local`, exactly
`permitopen 127.0.0.1:4175`, and all interactive capabilities disabled. A
normal `ssh dcar-douyin-sync-prod` shell request must fail, while the documented
Mac `ssh -N` tunnel and Machine-authenticated health request succeed. A forward
to any other destination or port must fail.

Rollback starts on the Mac: stop and disable its tunnel LaunchAgent first.
Then lock the server account and recoverably remove the Match block before
reloading sshd:

```sh
sudo usermod -L dcar-douyin-sync
sudo install -d -o root -g root -m 0700 /root/dcar-ssh-rollback
sudo mv /etc/ssh/sshd_config.d/60-dcar-douyin-sync.conf \
  /root/dcar-ssh-rollback/60-dcar-douyin-sync.conf
sudo sshd -t
sudo systemctl reload ssh
```

Keep the account home and `authorized_keys` until the rollback has been
accepted so the change remains recoverable.

## Douyin egress acceptance and rollback

Before enabling the real provider, verify that Squid is bound only to loopback,
the exact Douyin tunnel succeeds, and every negative case fails closed:

```sh
sudo /usr/sbin/squid -k parse -f /etc/squid/dcar-douyin-egress.conf
sudo systemctl status dcar-douyin-egress dcar-douyin-control --no-pager
sudo ss -ltnp | grep '127.0.0.1:4176'
curl --fail --silent --show-error --head \
  --proxy http://127.0.0.1:4176 https://open.douyin.com/
! curl --fail --silent --show-error --head \
  --proxy http://127.0.0.1:4176 https://example.com/
! curl --fail --silent --show-error --insecure --head \
  --proxy http://127.0.0.1:4176 https://1.1.1.1/
```

Also stop `dcar-douyin-egress` temporarily and confirm a real-provider request
fails without making a direct connection, then restore the proxy before further
testing. Do not enable the provider merely to smoke the infrastructure; the
default-disabled health and page-render smoke remain valid without a Douyin
request.

To roll this egress layer back, first set
`DOUYIN_AUTHORIZATION_ENABLED=0` and `DCAR_DOUYIN_PROVIDER=disabled`, restart
4175, restore the previous `dcar-douyin-control.service`, and reload systemd.
Only then disable `dcar-douyin-egress`, remove its unit/configuration, and
restart 4175 again. Removing Squid before disabling the provider is not an
acceptable rollback because it can strand in-flight refresh or video-list
requests. Revoke the replacement Client Secret in the Douyin console if the
rollback is caused by suspected credential exposure.

Post-install acceptance must cover:

- `GET /dcar/api/v8/overview` returns the expected latest published date;
- yesterday and this-week windows match the writer snapshot;
- `GET /dcar/api/v8/scheduler` reports both scheduling gates off;
- one report download passes its registered hash; omitted large evidence returns
  a clear unavailable response instead of a server error;
- write endpoints are rejected by read-replica mode;
- `GET /dcar/accounts/douyin-authorization` renders through the authenticated
  gateway; a new authorization is entered from one account row and its POST body
  locks the exact account id plus Douyin uid, while the general management page
  exposes no unified scan action;
- a targeted `POST /dcar/api/douyin/oauth/start` returns 409 while the production
  stage-0 flag is disabled and no request reaches `open.douyin.com`;
- Squid listens only on `127.0.0.1:4176`, permits only
  `CONNECT open.douyin.com:443`, and denies subdomains, IP literals, other
  domains, methods, and ports;
- stopping Squid makes the real provider fail closed instead of connecting
  directly;
- `sudo -u dcar-douyin test -r /var/lib/dcar-aigc/auth/sessions.sqlite3`,
  `sudo -u dcar-douyin test -r /etc/dcar-aigc/credentials/auth-pepper` and the
  equivalent read-replica DB check all fail;
- the active snapshot receipt, database SHA-256, schema version, content count,
  and latest published timestamp are unchanged by the code release;
- an offline Vault backup passes while 4175 is stopped.
