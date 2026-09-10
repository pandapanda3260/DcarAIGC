# macOS 指定 writer 与 snapshot publisher

> 当前发布基线是 schema19 / report v8.9，数据库迁移身份为
> `dual-acquisition-profile-roster-v1`。系统以 `matrix_hybrid_v1` 和
> `tikhub_managed_v1` 两个采集 profile 运行，生效 activation 决定当日来源族。
> schema17→18、schema18 和 report v8.8 仅作为历史/兼容合同保留。下文原
> `daily_capture`、02:20/03:00/07:30 和固定12项启动回补的运行记录仅用于
> 识别历史v17回执，不再作为新作业的启动、补跑或发布依据。当前作业、
> 名册成员门和 profile-day 来源规则由 `v8.pipeline`、`profile_activations`
> 与相应名单来源族统一执行。正式切换必须使用受控 profile 冷切流程；
> 本文件示例不是自动迁移授权。

该目录定义 DcarAIGC 正式拓扑中唯一允许运行调度的 macOS writer。writer 监听 `127.0.0.1:8766`，同时承担本地正式 API、供应商抓取、媒体处理、增量评估和报告任务。日常 4173 网关经 4174 Web 连接 8766；8765 只保留给 operator freeze 下的只读快照。

writer renderer 只生成 disabled-by-default plist，永不调用 `launchctl`；writer 仍按生效日人工启用。snapshot publisher plist 是已授权的无人值守任务，但它的 renderer 同样只渲染，必须经过一次本机门禁后由部署流程显式 bootstrap。

## 安全合同

- 同一时刻只能有一个 scheduled writer。Ubuntu 副本和 freeze 8765 均必须保持 `DCAR_SCHEDULER_ENABLED=0` 和 `DCAR_STARTUP_CATCHUP_ENABLED=0`。
- writer 使用 `DCAR_SCHEDULER_ENABLED=1` 和 `DCAR_STARTUP_CATCHUP_ENABLED=1`，但 startup catch-up 严格为 `report_only`：只可创建/重试 `daily_report` 和 `weekly_report`，不运行 capture、media 或 cutoff，不产生供应商费用。
- TikHub全部作业共享北京时间日总额 USD 100；reconcile/detail/metrics/comments/history 五类额度均为 USD 100。分类额度不能突破全局硬顶，单任务上限也不能替代全局额度。只有运营者明确批准循环成本并在 `writer.env` 写入固定 acknowledgement 后才能启用。
- TikHub API base/key 不得进入 plist 或 `writer.env`。`writer.env` 只允许 `TIKHUB_API_KEY_FILE`、`DCAR_DAILY_COST_AUTHORIZATION`、空行和注释；其他条目都会 exit 78。`TIKHUB_API_KEY_FILE` 指向外部 `dcar.env.local`，wrapper 只导出文件路径，不会把其中其他服务的凭据注入 writer 环境。
- `DCAR_DAILY_CAPTURE_RECONCILE_FROM` 只能由 plist 继承，必须是真实且规范的 `YYYY-MM-DD`。不得将它写进 `writer.env`。
- plist 只保存项目外的 `DCAR_LOADED_BUILD_RECEIPT` 路径，禁止直接保存 `DCAR_LOADED_BUILD_ID`。wrapper 校验该 0600 单链接回执后，以整个文件的 SHA-256 派生实际加载的 build ID。
- scheduler 锁在 `$HOME/Library/Application Support/DcarAIGC/runtime/writer-worker.lock`，不在 checkout 里。`writer_lock.held=true` 只证明唯一 scheduler 进程锁已持有，不证明数据库没有其他非调度写进者。
- `runtime/operator-freeze.lock` 存在时不得启动 writer。当前正式升级只允许在停服、freeze、已验证备份下走精确的封版18→19流程；17→18安装路径仅供历史兼容，旧15→16工具合同仅供历史恢复，运行时不自动迁移。
- wrapper 使用 `caffeinate -s`，只能在接交流电时防止系统空闲睡眠。合盖、断电、人工睡眠或重启仍可以跳过 Cron。
- 这是 per-user LaunchAgent，Mac 重启后指定账号必须登录 GUI session。

本地 UI 不另设 LaunchAgent。登录后用唯一的 `dcar-live-ui` SCREEN 运行 `scripts/start_web_mvp.sh`，日志写到 `$HOME/Library/Logs/DcarAIGC/local-ui.log`（该日志文件必须以 `umask 077` 创建：本地验证码通过 `DCAR_AUTH_SMS_PROVIDER=log` 打印在其中，手机号已脱敏）；脚本首次启动会把 `runtime/auth/users.htpasswd` 的旧账号一次性导入 `runtime/auth/sessions.sqlite3`，并生成 `runtime/auth/pepper`；新账号在登录页用准入名单内的手机号注册，名单用 `PYTHONPATH=src/dcar_eval python3 -m uv run --frozen python -m dcar_auth.admin --db runtime/auth/sessions.sqlite3 allow-phone <手机号>` 维护；旧 `dcar-view-ui`/`dcar-read-api` 必须停用。该脚本只启动 4173/4174，并在启动前验证 8766 正式 writer 合同，不会启动、停止或重启 8766。

## 1. 准备项目外 writer 配置

只在 USD 100 循环上限已批准后执行。复制示例到项目外，修改绝对 key-file 路径，并保护文件。`DCAR_DAILY_COST_AUTHORIZATION` 只能在审批后设为 `I_ACKNOWLEDGE_DAILY_PROVIDER_LIMIT_USD_100`。

```sh
install -d -m 0700 "$HOME/Library/Application Support/DcarAIGC"
install -d -m 0700 "$HOME/Library/Application Support/DcarAIGC/runtime"
install -d -m 0700 "$HOME/Library/Logs/DcarAIGC"
install -m 0600 deploy/macos/writer.env.example \
  "$HOME/Library/Application Support/DcarAIGC/writer.env"
chmod 0600 /absolute/path/outside/the/repository/dcar.env.local
```

TikHub 配置文件必须是项目外的普通非 symlink 文件，mode 只能是 0400 或 0600，并且精确包含一条 `TIKHUB_API_BASE=https://api.tikhub.dev` 或 `TIKHUB_API_BASE=https://api.tikhub.io`，以及一条 `TIKHUB_API_KEY=...`。选定 route 只以该受保护文件为准；wrapper 拒绝进程环境直接传入 `TIKHUB_API_BASE`，不会在两个域名之间自动 fallback，并会在导出任何付费环境前完成 route、凭据和 reconcile 日期校验。

## 2. 只渲染，不加载

用 `D` 表示这次新调度生效的北京自然日。renderer 对已存在输出使用 `open("xb")` 拒绝覆盖；如果需要顺延 D 或更新 plist，先将旧文件移到备份路径，再重新渲染。

```sh
D=2026-09-01
LOADED_BUILD_RECEIPT="$HOME/Library/Application Support/DcarAIGC/evidence/sealed-build.json"

python3 deploy/macos/render_launch_agent.py \
  --project-root "$PWD" \
  --reconcile-from "$D" \
  --loaded-build-receipt "$LOADED_BUILD_RECEIPT" \
  --check

python3 deploy/macos/render_launch_agent.py \
  --project-root "$PWD" \
  --reconcile-from "$D" \
  --loaded-build-receipt "$LOADED_BUILD_RECEIPT" \
  --output "$HOME/Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"

plutil -lint "$HOME/Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
plutil -p "$HOME/Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
```

渲染前后必须核对：

1. Mac 接交流电、网络正常，且计划窗口内不睡眠。
2. `.venv/bin/python`、`mlx-whisper`、Homebrew `ffmpeg`/`ffprobe` 和 `/usr/bin/swiftc` 存在。
3. 已安装 plist 的 `DCAR_V8_DB` 指向项目外 canonical 路径，且数据库已是通过封版18→19切换和回执验证的 schema19 正式库。
4. 8766 无既有监听者，Ubuntu 调度/catch-up 为关，8765 无调度。
5. 循环成本已批准，且 operator freeze lock 尚未提前解除。
6. plist 中 `DCAR_DAILY_CAPTURE_RECONCILE_FROM=D`，writer lock 与 loaded build receipt 都是项目外路径，不含 API key 或 `DCAR_LOADED_BUILD_ID`。

## 3. D 日启用时序

本次修复原审批窗口是北京时间 **2026-09-01 00:00～00:30**，但实际在 8 月 31 日 13:55 已提前启用，`pipeline_reconcile` 因而创建了 8 月 31 日的付费补偿轮次；不得将这次执行记录为“按窗口部署”。下面的时间点保留为原审批流程和复盘依据，不是已经满足的回执。后续上线必须在测试完成后只做一次必要重启，不得为纯测试提交反复重启正在执行正式轮次的 writer。

00:00 后先在 writer 仍运行时用普通只读连接核对正式库；禁止使用 `immutable=1`，否则 WAL 正在写时可能读到不一致状态：

```sh
DATA_ROOT="$HOME/Library/Application Support/DcarAIGC/data"
DB_PATH="$DATA_ROOT/dcar_insight.sqlite3" .venv/bin/python - <<'PY'
import os
import sqlite3
from pathlib import Path

path = Path(os.environ["DB_PATH"]).resolve()
connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
connection.execute("BEGIN")
mismatch_sql = """
SELECT * FROM (
  SELECT 'run_without_attempt' AS mismatch, id
  FROM (
    SELECT id FROM scheduler_runs WHERE status='running'
    EXCEPT
    SELECT scheduler_run_id AS id
    FROM scheduler_run_attempts WHERE status='running'
  )
  UNION ALL
  SELECT 'attempt_without_run' AS mismatch, id
  FROM (
    SELECT scheduler_run_id AS id
    FROM scheduler_run_attempts WHERE status='running'
    EXCEPT
    SELECT id FROM scheduler_runs WHERE status='running'
  )
)
"""
runs = connection.execute(
    "SELECT COUNT(*) FROM scheduler_runs WHERE status='running'"
).fetchone()[0]
attempts = connection.execute(
    "SELECT COUNT(*) FROM scheduler_run_attempts WHERE status='running'"
).fetchone()[0]
mismatches = connection.execute(mismatch_sql).fetchall()
quick_check = connection.execute("PRAGMA quick_check").fetchone()[0]
print({"running_runs": runs, "running_attempts": attempts,
       "mismatches": mismatches, "quick_check": quick_check})
if runs != attempts or mismatches or quick_check != "ok":
    raise SystemExit("writer restart preflight failed")
PY
```

两类 running 数量必须相等、两向 mismatch 查询必须返回 0 行、`quick_check` 必须为 `ok`。否则启动时的 `recover_interrupted_scheduler_runs()` 会失败，本窗口禁止继续。

门禁通过后按“bootout → 归档旧 plist → 用 `D=2026-09-01` 重新渲染并 `plutil -lint` → enable/bootstrap”执行。不要在 bootstrap 后紧接 `kickstart -k`。在这次 2026-09-01 历史执行中，正式库已是 schema18，因此当时没有 schema 迁移，也不补 8 月 28～31 日数据；这不是当前 schema19 发布步骤。

```sh
label="cn.tj.dcar.writer-worker"
domain="gui/$(id -u)"
plist="$HOME/Library/LaunchAgents/$label.plist"

launchctl enable "$domain/$label"
launchctl bootstrap "$domain" "$plist"
```

等待 health：

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

scheduler 必须报告 requested/enabled，`registered_job_ids` 必须包含 `pipeline_reconcile` 且不含 `history_recovery`；`pipeline_reconcile` 必须是 `mode=current_day_only`、`enabled=true`、`interval_seconds=3600`、`paid_round_cutoff=20:00`。`startup_catchup` 保持 `report_only`，允许补过去缺失的日报/周报，但不得产生供应商费用。补偿、报告、汇总和 OpenAPI 健康任务使用独立的 `control` executor；电脑从睡眠恢复后，长抓取不得占满报告执行通道，日报、周报和 07:30 汇总也不得因超过一小时而丢弃。页面报告和自动报告共用报告专用锁；供应商抓取和内容任务不持有该锁。进程内 SQLite 写事务只在 `BEGIN IMMEDIATE` 到 commit/rollback 期间由另一把 RLock 串行，不串行整个任务，也不覆盖跨进程写入。

00:30 前确认 writer PID/health 稳定、stderr 没有新的 `database is locked` 或父 `pipeline_round:*` 收尾异常，并确认旧 `history_scan_catalog`、`history_recovery`、跨日 `tikhub_reconcile` 和跨日 `matrix_works_scan` 没有被续跑。自动 `content_pipeline`、`comments_refresh` 和指标队列也排除历史内容；运营上只允许在另行明确授权后使用 `range_backfill` 产生历史抓取费用。旧库中的 history 账本记录仍保留，不以删除记录伪造零费用。

不要手工补 8 月 28～31 日，也不要手工触发历史 catalog。真实端到端验收使用自然轮次：02:00 账号指标、02:10 Matrix 作品扫描、03:00 TikHub 对账，随后观察续跑、07:30 汇总、08:00 日报和 09:00 起 publisher。全员 TikHub 对账实测超过 9 小时，并不被报告专用锁串行；08:00 后完成的扫描仍不能进入当天冻结输入，因此正常日报也可能长期带明确原因 `partial`。provider 费用只以 `provider_usage` ledger 的北京时间当日增量为准。

同一次小时补偿的各 key 共用该次 `reconcile_at`。这只表示各父 `pipeline_round:*` attempt 的 `started_at` 记录同一补偿起点；Matrix/TikHub 子扫描有自己的 claim 时间，父 attempt 的 `started_at/completed_at` 不能直接用来计算真实运行耗时。

### 睡眠/唤醒语义

Cron 负责低延迟，每小时 `pipeline_reconcile` 负责北京时间当天正确性：每个 registration 只选当前已到点的最后一个槽，再按计划时刻排序。20:00 后不新建付费补偿轮次；已经创建的当天 partial 仍可续跑。跨过午夜后不续跑昨天的供应商子扫描。遗漏的日报/周报由独立的小时 `report_reconcile` 补齐，publisher 在允许终态前保持 fail-closed。

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

publisher 在登录时启动一次、每天 09:00 启动一次，并每小时 reconcile。09:00 前的自动调用只返回 no-op；同一北京自然日会重新读取 Writer，只有报告、采集和调度观察与已发布证据完全相同时才 no-op，后续新内容、指标或任务状态会继续发布。项目外 `snapshot_root/automatic-publisher-state-v2.json` 保存原子成功状态，`publisher-status.json` 记录最近执行结果及真实拒因，后者不充当授权或成功证明。任务不是 `KeepAlive` 服务，不运行 scheduler、catch-up 或任何供应商调用，也不继承 TikHub key。

09:00 只是当天首次检查，不是假定所有上游工作已完成。Writer 配置 `reconcile_from` 后，发布统一使用该业务日下界：首日没有到期日报、报告缺失或失败、采集不完整均记录为真实的 `observed/partial` 快照，用户可以看到新数据和失败状态；快照发布成功不代表日报完整。只选择下界后的日报及完整周期均在下界后的周报，不创建历史任务。未配置下界的旧安装保留原完整当日依赖合同。所有模式保留以下完整性门禁：

1. 唯一 Writer 正常、scheduler 真正运行且持有同一把锁；startup catch-up 可以关闭或尚未完成，启用时仍禁止包含非报告工作，实际报告任务与不可变 attempt 才是发布依据。
2. 已存在的原生 profile-day scan/day receipt 必须可核验，且绑定当时的 activation、来源族、冻结名单与扫描；缺失或未完成要如实显示，损坏或来源绑定不符则拒绝发布。不以旧 capture 成功、最新作品时间或空队列代替覆盖。
3. 已成功或 partial 的应发日报、周报继续验证冻结 scope/input hash、原始页及全部报告文件。冻结策略按已支持的原版本验证，冻结 anchor 精确绑定原 attempt；后来重试或策略升级不改写原事实。失败的报告也必须与真实终态 attempt 一致。
4. 观察证据在 SQLite 一致性备份完成后按分离快照重新封存，再精确复验；不会把备份前的旧观察套到更新的数据字节上。未到期、缺失或失败的报告不伪造成功回执。
5. report v8.9、schema19 / `dual-acquisition-profile-roster-v1`、profile-day scope、evaluation/taxonomy identity、正式DB与thin-server-v2/media-retention-v1合同一致。
6. SSH 专用 alias、strict known_hosts、远端有界保留、空间、bundle 校验、安装和安装后 smoke check 全部通过。保留命令始终保护 active snapshot，并分别只保留最近 3 个 incoming/history 快照目录。

明确标注原因的 partial 快照可发布，但不得放宽身份、冻结证据、文件完整性或费用合同。未结算的 schema-upgrade 仍阻断普通发布；17→18 过渡只保留为历史兼容。远端发布失败时保留本地快照和远端 incoming，下一次自动执行仍从全套门禁开始；成功后本地只保留最近 3 个自动快照目录。快照目录保留与媒体原件 72 小时策略是两个独立合同。

schema19 跨 profile 冷切只使用 `python -m v8.profile_control begin/complete`
和原生 activation/receipt 链；同 profile 名单更新由 writer 进程内排期次日
activation。正式库要求 `DCAR_WRITER_LOCK` 与已安装 writer plist 的项目外锁
配置一致，锁文件必须已经存在；writer 持锁、配置缺失或指向替代锁时拒绝
执行。不要新建项目内锁冒充停写，也不要对正式库传 `--at` 伪造执行时间。
旧 `v8.pipeline_cutover --activate-only`/cutover CLI 仅保留给 schema18 历史合同。

### 新原件生命周期激活

维护窗口先核真实归档根/容量、accepted名册、唯一运行消费者与预算。正式数据库
路径固定使用已安装 writer plist 的项目外 `DCAR_V8_DB`；隔离checkout拒绝
激活正式库。`python -m v8.media_consumer_proofs --db <db> activate` 只追加
模式回执，不下载或删除原件；传 `--mode enrollment_only`、本次固定
`--activation-id`、封版 `--release` 和逐条 `--canary-content-id`。

在真实新canary完成证据、复制归档和全包恢复验证后，用同模块的 `capture`
子命令配合项目外 `--publisher-env`，实际读取8766和SSH远端。它验证Mac
启动时固定的源码hash、当前源码未变、实际库路径和schema，以及服务器
18→19成功回执、当前只读API/安装manifest/代码hash。17→18回执只用于历史
兼容验证。capture只写0600的hash绑定
证明文件，不自动激活；同封版Mac进程重启后必须重新capture。源码改变则
先停止释放并完成新的代码/服务器配对证明，不能只重采health冒充同一封版。

将该文件用 `activate --mode enrollment_only --consumer-receipt <path>`
附到同一激活记录，才可受控释放canary热原件并真实恢复。然后以
`--mode active --consumer-receipt <path> --canary-bundle <id>` 进入自动模式；
所有固定canary必须有真实成功run/attempt、完整成员恢复和释放回执。
服务器不是schema19、任一消费者未就绪或证明缺失时拒绝释放，不用布尔值
代签成功。旧文件永不补登记。72小时从真实首次归档验证T起算，恢复或
重启不重置；到期后精确删除原件，无回收区。未通过完成门满14天只进入
受保护人工待办；首次自然到期删除须另行观察，不以模拟时钟代替。

项目外配置沿用 `publisher.env.example`，权限必须是 0400 或 0600。publisher 的 SSH 私钥必须放在后台可读的项目外目录（推荐 `$HOME/Library/Application Support/DcarAIGC/credentials/`），不得放在受 TCC 限制的 `Documents`。SSH alias 必须使用该 `IdentityFile`、`IdentitiesOnly yes` 和 `IdentityAgent none`。首次安装：

```sh
label="cn.tj.dcar.snapshot-publisher"
domain="gui/$(id -u)"
plist="$HOME/Library/LaunchAgents/$label.plist"
data_root="$HOME/Library/Application Support/DcarAIGC/data"

install -d -m 0700 "$HOME/Library/Application Support/DcarAIGC"
install -d -m 0700 "$HOME/Library/Logs/DcarAIGC"

python3 deploy/macos/render_snapshot_publisher.py \
  --project-root "$PWD" --check

DCAR_PROJECT_ROOT="$PWD" \
DCAR_PUBLISHER_ENV_FILE="$HOME/Library/Application Support/DcarAIGC/publisher.env" \
DCAR_V8_DB="$data_root/dcar_insight.sqlite3" \
DCAR_LEGACY_DB="$data_root/web_mvp.sqlite3" \
DCAR_READ_ONLY=1 DCAR_SCHEDULER_ENABLED=0 DCAR_STARTUP_CATCHUP_ENABLED=0 \
  deploy/macos/run_snapshot_publisher.sh --check

# 使用与 LaunchAgent 相同的 wrapper 执行真实 SSH 只读检查；
# 不构建快照、不 rsync、不安装、不写本地 publisher state。
DCAR_PROJECT_ROOT="$PWD" \
DCAR_PUBLISHER_ENV_FILE="$HOME/Library/Application Support/DcarAIGC/publisher.env" \
DCAR_READ_ONLY=1 DCAR_SCHEDULER_ENABLED=0 DCAR_STARTUP_CATCHUP_ENABLED=0 \
  deploy/macos/run_snapshot_publisher.sh --remote-check

python3 deploy/macos/render_snapshot_publisher.py \
  --project-root "$PWD" --output "$plist"
plutil -lint "$plist"
launchctl enable "$domain/$label"
launchctl bootstrap "$domain" "$plist"
```

`--check`、自动发布和新的手工发布属于 `formal_read`：它们按已安装 writer
LaunchAgent 的数据库 device/inode 身份读取正式库，只 read/stat 已安装的 writer
lock，并要求 8766 同时报告同一把已持有的锁；publisher 不获取 writer flock。
`--remote-check` 属于 `remote_only`，不要求或读取本机数据库和 writer lock。

每个新快照在首次 SSH 前都会以 `O_EXCL`、`0600` 创建
`snapshot-source-receipt.json`，绑定正式库、已观察 writer lock、manifest、发布证据
和 artifact root/path 摘要。只有已存在且自校验通过的该回执才能续传；续传不再读取
实时正式库或 8766：

```sh
DCAR_PROJECT_ROOT="$PWD" \
DCAR_PUBLISHER_ENV_FILE="$HOME/Library/Application Support/DcarAIGC/publisher.env" \
DCAR_READ_ONLY=1 DCAR_SCHEDULER_ENABLED=0 DCAR_STARTUP_CATCHUP_ENABLED=0 \
  deploy/macos/run_snapshot_publisher.sh \
  --resume-staged-snapshot 20260829T010000Z-0123456789ab
```

安装前的发布意图会原子写入 `snapshot-publisher-pending.json`。安装后只有远端 active receipt、snapshot ID、DB SHA、schema/runtime 和 API 全部一致才写成功回执；下一小时会自动区分“新版已健康”、“旧版已恢复”和“未知远端状态”。只有确认的安装不匹配才触发 installer rollback；SSH/远端 API 瞬时失败保留 pending，不回滚可能健康的新版。

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
