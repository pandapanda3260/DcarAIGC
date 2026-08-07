# DcarAIGC server deployment (phase 1)

This Compose project runs the current UI and API only. It deliberately disables
the scheduler and startup catch-up, and does not install the macOS-only media
ASR/OCR stack.

The services publish only on the server loopback interface. Access them with two
SSH tunnels:

```sh
ssh -L 4173:127.0.0.1:4173 -L 8765:127.0.0.1:8765 <server>
```

Before starting, create the bind-mount directories and make the writable ones
available to container uid/gid `10001`:

```sh
sudo install -d -o 10001 -g 10001 \
  /var/lib/dcar-aigc/db \
  /var/lib/dcar-aigc/reports \
  /var/lib/dcar-aigc/cache
sudo install -d -o root -g root /var/lib/dcar-aigc/runtime
```

Copy consistent SQLite backup files to the `db` directory before the first
start. Do not copy a live SQLite database without its backup procedure.

Start and verify from the repository root:

```sh
docker compose -f deploy/server/compose.yml build
docker compose -f deploy/server/compose.yml up -d
docker compose -f deploy/server/compose.yml ps
```

If the target host cannot reach the container registry, use the isolated
systemd units under `deploy/server/systemd/` with private Python 3.12.13 and
Node 22.13.1 runtimes under `/var/www/dcar-aigc/runtime`. The services use
systemd bind mounts so persistent data remains under `/var/lib/dcar-aigc`
while application paths still resolve inside the release tree.

The web build intentionally keeps
`NEXT_PUBLIC_DCAR_API_BASE=http://127.0.0.1:8765`, matching the dual-tunnel
access model by default.

For the authenticated public path on the existing Origin domain, build with:

```sh
DCAR_WEB_BASE_PATH=/dcar \
NEXT_PUBLIC_DCAR_API_BASE=/dcar \
node ./node_modules/vinext/dist/cli.js build
```

Install `deploy/server/nginx/dcar-proxy.conf` as an include inside the existing
TLS server block. Both the web UI and API then share the `/dcar/` BasicAuth
protection space; the API remains bound to server loopback.
