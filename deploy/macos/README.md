# macOS 指定 writer 与 snapshot publisher

该目录定义 DcarAIGC 正式拓扑中唯一允许运行调度的 macOS writer。writer 监听 `127.0.0.1:8766`，负责供应商抓取、媒体处理、增量评估和报告任务。日常 UI/API 继续使用 4173/8765，且 8765 固定不启用 scheduler。

仓库不会自动安装、加载或启用任何 LaunchAgent。renderer 只生成 disabled-by-default plist，永不调用 `launchctl`。

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

`budget_blocked` 仍是 `failed`，不得为了通过本次验收提高 USD 8 上限或开启自动重试；后续只能按显式授权的 `operator_retry` 流程处理。

### 睡眠/唤醒语义

02:00 Cron 负责低延迟，每小时 reconcile 负责当天正确性。Mac 在 02:00 睡眠、当天稍后唤醒时，只要 writer 仍运行，reconcile 会补当天 02:00 槽；它不会追回昨天或更早槽。白天补抓也不会紧接着追跑 02:20/03:00/07:30、日/周报或 publisher。

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

## snapshot publisher：已停用、未加载

snapshot publisher 不属于本轮恢复范围。当前 LaunchAgent 已停用且未加载；不得 render/load/enable/bootstrap/kickstart，不得把它当作 writer 验收的一部分。

源码仍保留，但 publisher 与 server installer 共用的发布身份已有三组明确漂移：

1. `EXPECTED_DATABASE_SCHEMA_VERSION = 13`，而当前代码 schema 为 16。
2. `EXPECTED_DATABASE_SCHEMA_MIGRATION = "scheduler-run-attempt-history"`，而当前为 `remove-manual-review`。
3. `EXPECTED_REPORT_VERSION = "dcar-content-operations-report-v8.6"`，而 `pyproject.toml` 和当前报告合同为 v8.7。

这三项未在独立 publisher 升级中同步、测试和端到端验证前，publisher 不可用。本轮只保留已知失败的单测证据，不修改 publisher 逻辑，也不通过放宽身份门禁消红。
