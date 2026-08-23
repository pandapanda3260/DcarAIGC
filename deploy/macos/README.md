# macOS 指定 writer 与 snapshot publisher

该目录定义 DcarAIGC 正式拓扑中唯一允许运行调度的 macOS writer。writer 监听 `127.0.0.1:8766`，负责供应商抓取、媒体处理、增量评估和报告任务。日常 UI/API 继续使用 4173/8765，且 8765 固定不启用 scheduler。

writer renderer 只生成 disabled-by-default plist，永不调用 `launchctl`；writer 仍按生效日人工启用。snapshot publisher plist 是已授权的无人值守任务，但它的 renderer 同样只渲染，必须经过一次本机门禁后由部署流程显式 bootstrap。

## 安全合同

- 同一时刻只能有一个 scheduled writer。Ubuntu 副本和 8765 均必须保持 `DCAR_SCHEDULER_ENABLED=0` 和 `DCAR_STARTUP_CATCHUP_ENABLED=0`。
- writer 使用 `DCAR_SCHEDULER_ENABLED=1` 和 `DCAR_STARTUP_CATCHUP_ENABLED=1`，但 startup catch-up 严格为 `report_only`：只可创建/重试 `daily_report` 和 `weekly_report`，不运行 capture、media 或 cutoff，不产生供应商费用。
- 每日 capture 的 task 总额硬顶为 USD 8。只有运营者明确批准循环成本并在 `writer.env` 写入固定 acknowledgement 后才能启用。
- TikHub API key 不得进入 plist 或 `writer.env`。`writer.env` 只允许 `TIKHUB_API_KEY_FILE`、`DCAR_DAILY_COST_AUTHORIZATION`、空行和注释；其他条目都会 exit 78。
- `DCAR_DAILY_CAPTURE_RECONCILE_FROM` 只能由 plist 继承，必须是真实且规范的 `YYYY-MM-DD`。不得将它写进 `writer.env`。
- scheduler 锁在 `$HOME/Library/Application Support/DcarAIGC/runtime/writer-worker.lock`，不在 checkout 里。`writer_lock.held=true` 只证明唯一 scheduler 进程锁已持有，不证明数据库没有其他非调度写进者。
- `runtime/operator-freeze.lock` 存在时不得启动 writer。正式库只能在停服、freeze、已验证备份下走 v15→v16 离线 candidate 流程，运行时不自动迁移。
- wrapper 使用 `caffeinate -s`，只能在接交流电时防止系统空闲睡眠。合盖、断电、人工睡眠或重启仍可以跳过 Cron。
- 这是 per-user LaunchAgent，Mac 重启后指定账号必须登录 GUI session。

## 1. 准备项目外 writer 配置

只在 USD 8 循环上限已批准后执行。复制示例到项目外，修改绝对 key-file 路径，并保护文件。`DCAR_DAILY_COST_AUTHORIZATION` 只能在审批后设为 `I_ACKNOWLEDGE_DAILY_PROVIDER_LIMIT_USD_8`。

```sh
install -d -m 0700 "$HOME/Library/Application Support/DcarAIGC"
install -d -m 0700 "$HOME/Library/Application Support/DcarAIGC/runtime"
install -d -m 0700 "$HOME/Library/Logs/DcarAIGC"
install -m 0600 deploy/macos/writer.env.example \
  "$HOME/Library/Application Support/DcarAIGC/writer.env"
chmod 0600 /absolute/path/outside/the/repository/TikHub.env.local
```

key 文件必须是项目外的普通非 symlink 文件，mode 只能是 0400 或 0600，内含 `TIKHUB_API_KEY=...`。wrapper 会在导出任何付费环境前完成该校验和 reconcile 日期校验。

## 2. 只渲染，不加载

用 `D` 表示这次新调度生效的北京自然日。renderer 对已存在输出使用 `open("xb")` 拒绝覆盖；如果需要顺延 D 或更新 plist，先将旧文件移到备份路径，再重新渲染。

```sh
D=2026-08-21

python3 deploy/macos/render_launch_agent.py \
  --project-root "$PWD" \
  --reconcile-from "$D" \
  --check

python3 deploy/macos/render_launch_agent.py \
  --project-root "$PWD" \
  --reconcile-from "$D" \
  --output "$HOME/Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"

plutil -lint "$HOME/Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
plutil -p "$HOME/Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
```

渲染前后必须核对：

1. Mac 接交流电、网络正常，且计划窗口内不睡眠。
2. `.venv/bin/python`、`mlx-whisper`、Homebrew `ffmpeg`/`ffprobe` 和 `/usr/bin/swiftc` 存在。
3. `app/data/dcar_insight.sqlite3` 已是通过离线 candidate 切换和回执验证的 v16 正式库。
4. 8766 无既有监听者，Ubuntu 调度/catch-up 为关，8765 无调度。
5. 循环成本已批准，且 operator freeze lock 尚未提前解除。
6. plist 中 `DCAR_DAILY_CAPTURE_RECONCILE_FROM=D`，writer lock 是项目外路径，不含 API key。

## 3. D 日启用时序

所有实施、测试、备份、迁移、plist 复核必须在 **D 日 01:00 前**完成。未按时完成时，D 自动顺延到下一自然日；先归档已渲染 plist，再用新日期重新渲染。禁止在 D 日 02:00 之后“临时修好就启动”。

计划在 **D 日 00:09** 做最后一次 v16 identity、备份/安装回执、plist 和端口门禁复核，全部通过后才按回执解除 canonical operator freeze；任一项不通过则保留 freeze 并顺延 D。

计划的首次 bootstrap 时刻是 **D 日 00:10**。plist 是 `RunAtLoad=true`，bootstrap 会立即启动 writer；小时 reconcile 也通过显式 `next_run_time=now` 立即检查。00:10 尚未到当天 02:00 业务槽，因此返回 `before_today_slot` 且不产生供应商费用。02:00 后 bootstrap 则会立即尝试当天真实付费抓取。

只在 freeze 解除、正式 v16 身份和所有门禁验收后执行：

```sh
label="cn.tj.dcar.writer-worker"
domain="gui/$(id -u)"
plist="$HOME/Library/LaunchAgents/$label.plist"

launchctl enable "$domain/$label"
launchctl bootstrap "$domain" "$plist"
```

`bootstrap` 后不要紧接 `kickstart -k`；这会杀掉刚启动的进程，并可能触发 300 秒 throttle。等待 health：

```sh
for attempt in $(seq 1 60); do
  if curl -fsS -o /dev/null http://127.0.0.1:8766/api/v8/health 2>/dev/null; then
    break
  fi
  sleep 1
done
```

在不调用供应商的情况下验证：

```sh
lsof -nP -iTCP:8765 -sTCP:LISTEN
lsof -nP -iTCP:8766 -sTCP:LISTEN
curl -fsS http://127.0.0.1:8766/api/v8/health
curl -fsS http://127.0.0.1:8766/api/v8/scheduler | python3 -m json.tool
launchctl print "gui/$(id -u)/cn.tj.dcar.writer-worker"
tail -n 200 "$HOME/Library/Logs/DcarAIGC/writer-worker.stderr.log"
```

scheduler 必须报告 requested/enabled；`daily_capture_reconcile` 必须是 `mode=current_day_only`、`effective_from=D`、`interval_seconds=3600`。以 D=`2026-08-21` 的正式库只读基线计算，`startup_catchup` 必须在观察窗内结束为 `status=succeeded`、`error=null`，并精确返回 12 个结果（10 个 `daily_report`、2 个 `weekly_report`，每项仅允许 `succeeded` 或 `partial`）；不能只检查“results 里没有供应商任务”，因为 `running` 时空 results 也会误通过。稳定观察至少 650 秒，除了 PID/health 稳定，stderr 还必须没有 preflight exit 78、日期拒绝或 KeepAlive/ThrottleInterval 崩溃循环。若 00:20:50 仍为 `running`，或出现 `failed`/`deferred`/结果集漂移，必须在 01:00 前 bootout + disable、恢复 freeze 并顺延 D，禁止让该进程继续进入 02:00 付费槽。

禁止手工执行 `daily_capture` 作为烟测；等待已授权的自然 02:00 槽。该槽结束后，状态可以是 `succeeded` 或 `partial`，但还必须通过独立质量门：discovery ≥90%、当天选中 cohort ≥60%、数组计数一致、provider 无阻断。`provider_usage` ledger 是权威账本：details 上报小计、ledger 和声明预算必须是有限值，成本非负、预算为正，且同时满足 `ledger >= details 上报小计`、`ledger <= 声明预算`、`ledger <= USD 8`。两者精确相等只作诊断，不影响 `passed`。3,000 是选中 cohort 上限，不是全库覆盖率。

`budget_blocked` 仍是 `failed`，不得为了通过本次验收提高 USD 8 上限或自动重跑该终态。只有 `interrupted` 会由小时 reconcile 以同一 task ID 和剩余预算自动续跑；显式 `operator_retry` 保留为诊断入口。

### 睡眠/唤醒语义

02:00 Cron 负责低延迟，每小时 reconcile 负责当天正确性。Mac 在任一 Cron 时刻睡眠、当天稍后唤醒时，只要 writer 仍运行，reconcile 会按依赖顺序补齐当天已到时的 capture、media、cutoff 和报告槽；它不会追回昨天或更早的 capture。遗漏的日报/周报由独立的小时 report reconcile 补齐，publisher 在完整链路终态前保持 fail-closed。

## 故意重启、更新、停用和卸载

只有已加载且明确要重启的 writer 才使用：

```sh
label="cn.tj.dcar.writer-worker"
domain="gui/$(id -u)"
launchctl kickstart -k "$domain/$label"
```

plist 更新必须走 bootout→归档旧 plist→用新日期渲染→复核→bootstrap。停用/卸载不会删除 DB、reports、cache、key 文件、`writer.env` 或日志：

```sh
label="cn.tj.dcar.writer-worker"
domain="gui/$(id -u)"
plist="$HOME/Library/LaunchAgents/$label.plist"

launchctl bootout "$domain" "$plist"
launchctl disable "$domain/$label"
mkdir -p "$HOME/.Trash/DcarAIGC-launchagents"
mv "$plist" "$HOME/.Trash/DcarAIGC-launchagents/$label.plist"
```

## snapshot publisher：无人值守自动发布

publisher 在登录时启动一次、每天 09:00 启动一次，并每小时 reconcile。09:00 前的自动调用只返回 no-op；同一北京自然日成功发布后，后续调用通过项目外 `snapshot_root/automatic-publisher-state.json` 原子成功状态返回 no-op。任务不是 `KeepAlive` 服务，不运行 scheduler、catch-up 或任何供应商调用，也不继承 TikHub key。

09:00 只是当天首次检查，不是假定所有上游工作已完成。每次自动调用必须先在本机通过以下门禁，任一未完成都会在构建快照和连接 SSH 之前退出，等待下一小时重新检查：

1. 唯一 writer 正常并持有 scheduler lock；startup catch-up 必须为 report-only 且不在运行中。若启动时曾 failed/deferred，小时 report reconcile 修复后的当天实际报告槽才是发布依据。
2. 当天 02:00 capture 为 `succeeded` 或 `partial`；`failed`、`skipped` 均不可发布。
3. 当天 02:20 download、03:00 processing 和 07:30 cutoff 全部为 `succeeded`。
4. 当天 08:00 daily report 为 `succeeded` 或 `partial`；周一 08:30 weekly report 也必须为 `succeeded` 或 `partial`。
5. report v8.7、schema 16 / `remove-manual-review`、evaluation/taxonomy identity、正式 DB、内容新鲜度均一致。
6. SSH 专用 alias、strict known_hosts、远端有界保留、空间、bundle 校验、安装和安装后 smoke check 全部通过。保留命令始终保护 active snapshot，并分别只保留最近 3 个 incoming/history 快照目录。

`daily_capture=partial` 可发布，但质量门只作为诊断，不得用来放宽上述终态、身份、新鲜度或 USD 8 预算合同。远端发布失败时保留本地快照和远端 incoming 供审计，下一次自动执行仍从全套门禁开始；成功后本地只保留最近 3 个自动快照目录。

项目外配置沿用 `publisher.env.example`，权限必须是 0400 或 0600。首次安装：

```sh
label="cn.tj.dcar.snapshot-publisher"
domain="gui/$(id -u)"
plist="$HOME/Library/LaunchAgents/$label.plist"

install -d -m 0700 "$HOME/Library/Application Support/DcarAIGC"
install -d -m 0700 "$HOME/Library/Logs/DcarAIGC"

python3 deploy/macos/render_snapshot_publisher.py \
  --project-root "$PWD" --check

DCAR_PROJECT_ROOT="$PWD" \
DCAR_PUBLISHER_ENV_FILE="$HOME/Library/Application Support/DcarAIGC/publisher.env" \
DCAR_V8_DB="$PWD/app/data/dcar_insight.sqlite3" \
DCAR_LEGACY_DB="$PWD/app/data/web_mvp.sqlite3" \
DCAR_READ_ONLY=1 DCAR_SCHEDULER_ENABLED=0 DCAR_STARTUP_CATCHUP_ENABLED=0 \
  deploy/macos/run_snapshot_publisher.sh --check

python3 deploy/macos/render_snapshot_publisher.py \
  --project-root "$PWD" --output "$plist"
plutil -lint "$plist"
launchctl enable "$domain/$label"
launchctl bootstrap "$domain" "$plist"
```

不要在安装后执行 `kickstart -k`。更新 plist 时先 `bootout`，将旧 plist 移到备份路径，再重新渲染和 bootstrap；renderer 会拒绝覆盖已有文件。验收：

```sh
launchctl print-disabled "$domain" | grep "$label"
launchctl print "$domain/$label"
tail -n 200 "$HOME/Library/Logs/DcarAIGC/snapshot-publisher.stdout.log"
tail -n 200 "$HOME/Library/Logs/DcarAIGC/snapshot-publisher.stderr.log"
```

## Douyin OpenAPI sync tunnel

This is a separate, no-shell SSH channel for the future OpenAPI sync. It does
not read `writer.env`, does not use the writer or publisher SSH alias, and does
not run anything from `.venv`. The server must first install the dedicated
`dcar-douyin-sync` user, restricted key and sshd Match block described in
`deploy/server/README.md`.

Create a dedicated key and alias. Verify the server host-key fingerprint over
an independent trusted channel before adding it to the standard
`~/.ssh/known_hosts`; do not accept a first-use prompt from the LaunchAgent.

```sshconfig
Host dcar-douyin-sync-prod
    HostName your.server.example
    User dcar-douyin-sync
    IdentityFile ~/.ssh/id_ed25519_dcar_douyin_sync
    IdentitiesOnly yes
```

Install the independent Machine credential and strict sync environment outside
the repository. The env parser accepts only the three keys in the example,
rejects duplicates/unknown entries, and fixes the local listener to
`127.0.0.1:14175`. The remote destination is hard-coded as
`127.0.0.1:4175`.

```sh
install -d -m 0700 "$HOME/Library/Application Support/DcarAIGC"
install -d -m 0700 "$HOME/Library/Application Support/DcarAIGC/runtime"
install -d -m 0700 "$HOME/Library/Logs/DcarAIGC"
install -m 0600 deploy/macos/douyin-sync.env.example \
  "$HOME/Library/Application Support/DcarAIGC/douyin-sync.env"
install -m 0600 /secure/input/douyin-machine-key \
  "$HOME/Library/Application Support/DcarAIGC/douyin-machine-key"
```

Edit only the alias and absolute Machine-key-file path in `douyin-sync.env`.
Render and inspect the disabled-by-default LaunchAgent, then start it through
the bounded start script:

```sh
label="cn.tj.dcar.douyin-sync-tunnel"
plist="$HOME/Library/LaunchAgents/$label.plist"

python3 deploy/macos/render_douyin_sync_tunnel.py \
  --project-root "$PWD" --check
python3 deploy/macos/render_douyin_sync_tunnel.py \
  --project-root "$PWD" --output "$plist"
plutil -lint "$plist"
deploy/macos/start_douyin_sync_tunnel.sh
```

The foreground LaunchAgent process uses `ExitOnForwardFailure=yes`, strict
known_hosts, a dedicated safe IdentityFile, `ServerAliveInterval=30`,
`ServerAliveCountMax=3`, and exactly
`-L 127.0.0.1:14175:127.0.0.1:4175`. Preflight rejects an alias that declares
any other local, remote or dynamic forward. The control socket is stored in the
project-external runtime directory.

Acceptance is read-only and does not call Douyin or the writer:

```sh
DCAR_DOUYIN_SYNC_ENV_FILE="$HOME/Library/Application Support/DcarAIGC/douyin-sync.env" \
  deploy/macos/check_douyin_sync_tunnel.sh
lsof -nP -iTCP:14175 -sTCP:LISTEN
launchctl print "gui/$(id -u)/cn.tj.dcar.douyin-sync-tunnel"
```

The health script first checks the SSH control connection, then calls only
`/internal/v1/health` through loopback with the Machine credential supplied on
curl configuration stdin (not its process arguments). Also verify a direct
shell request and a forward to any destination other than
`127.0.0.1:4175` fail on the server.

Rollback and uninstall are recoverable and do not touch the writer plist,
writer env, writer venv, Machine key, private SSH key or known_hosts:

```sh
label="cn.tj.dcar.douyin-sync-tunnel"
plist="$HOME/Library/LaunchAgents/$label.plist"
deploy/macos/stop_douyin_sync_tunnel.sh
mkdir -p "$HOME/.Trash/DcarAIGC-launchagents"
mv "$plist" "$HOME/.Trash/DcarAIGC-launchagents/$label.plist"
```

After Mac rollback is complete, follow the server-side rollback sequence to
lock the dedicated account and recoverably remove its sshd Match block.
