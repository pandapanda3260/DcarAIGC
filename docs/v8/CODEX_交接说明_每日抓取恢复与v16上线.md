# CODEX 交接说明：每日抓取恢复 + v16 安全上线

> 更新时间：2026-08-18
> 当前项目根：`/Users/mark/Projects/DcarAIGC`
> 性质：代码集成 + 离线 v16 切换交接；D 日激活与自然 02:00 验收尚未到时

## 2026-08-18 执行回执摘要

- 本地分支 `codex/daily-capture-recovery-v6`，checkpoint commit 为 `9fad164c314098c9941c72471db00b5f6763df6e`，未 push。
- 唯一可写项目根已同卷原子移到 `/Users/mark/Projects/DcarAIGC`；旧 `~/Documents/DcarAIGC` 路径不再存在，`.venv` 已按 `uv.lock` 重建。
- 正式库已通过 receipt-driven 原子安装切到 schema v16 / `remove-manual-review`，SHA-256 为 `9f6df4f1b2603b6cf17393d8ce23cd88e3b7b2f2f840b659bd9124e0da823fec`；`quick_check` / `integrity_check` / FK 均通过。
- 原 v15 DB/WAL/SHM 保留在 `app/data/backups/v15-before-v16-20260818-1900/`；项目外已验证 v15 备份、backup receipt、r2 migration receipt 和 install receipt 位于 `~/Library/Application Support/DcarAIGC/migrations/v15-v16-20260818-9fad164/`。
- 首份 candidate 因人工核对使 SHM mtime 变化而被 installer 正确拒绝；它作为失败审计保留。第二份 candidate 在回执生成后立即原子安装成功。
- `provider_usage` 仍为 20,712 行 / USD 28.492，历史 `daily_capture` 仍只有 6 个槽；移根、备份、迁移和渲染全程零供应商增量。
- D=`2026-08-21` 的 writer plist 已渲染到 `~/Library/LaunchAgents/cn.tj.dcar.writer-worker.plist`，仍 `Disabled=true`、未加载；publisher 仍 disabled/unloaded 且未重装。operator freeze 仍保留，不得提前解除。
- 剩余时间门只有：D 日 00:09 最后复核与解冻、00:10 enable + bootstrap、自然 02:00 质量验收，以及后续白名单日期的有界内容恢复。

## 0. 先读结论

Claude 的方向大体正确，但“54 个 schema 红灯全是陈旧 fixture”不成立。当时与 T0 相关的精确数量是 **55**：

- `test_install_writer_database_candidate`：40 个 error。
- `test_v12_schema_migration`：7 个。
- `test_v13_schema_migration`：7 个。
- `test_v8_spu_llm`：1 个。

v12/v13/SPU 部分确实是 fixture 和写死版本断言漂移；但 installer 的 40 个 error 不能全部归类为 fixture。将“v16 结构 + 伪造 `PRAGMA user_version=11`”换成真实 v11 源 fixture 后，还暴露了 installer 自身只验证历史 v11→v13 增量、无法表达当前迁移边界的生产合同漂移。迁移守卫不应放宽；正确做法是使用真实历史源并严格验证声明的新增、删除、重建、manifest append、业务投影和 identity。

初始交接时，Gate 0、全量回归、正式库迁移、LaunchAgent 加载、自然 02:00 验收和有界内容恢复都未完成。现在以文首“2026-08-18 执行回执摘要”为准：Gate 0、回归、迁根、离线 v16 切换和 disabled plist 渲染已完成；加载、自然 02:00 验收和内容恢复仍受 D 日时间门约束。

## 1. 切换前正式基线（审计留存）

本次重新扫描得到的正式库身份为：

- 路径：`app/data/dcar_insight.sqlite3`。
- 大小：453,197,824 B。
- SHA-256：`00693e225650299ae8205250983738c779149f3f9dca99163b3426b56dc3f9b8`。
- `PRAGMA user_version=15`，最后 migration 为 `spu-llm-assist`。
- WAL 为 0 B。
- `provider_usage`：20,712 行，`max(id)=20712`，累计 USD 28.492000，最后一条为 `2026-08-17T13:17:19Z`。
- `daily_capture` 只有 6 个历史 run，最新精确槽为北京时间 2026-08-11 02:00。
- 6 个 attempt 的 `invocation_source` 全是 `legacy_migration`，正式库还没有一条真正 `scheduled` 里程。
- canonical operator freeze lock 仍在 `runtime/operator-freeze.lock`，mode 0600。

代码侧已是 schema v16 / `remove-manual-review`，报告合同为 v8.7。上述数值是离线切换前的 v15 审计锚点；当前正式 v16 身份以文首执行回执摘要和项目外 install receipt 为准。

实施人在任何正式步骤前必须重新采样 schema identity、SHA、WAL、`provider_usage` 和 DB holder；上述数值只是 2026-08-18 的基线，不能替代当场验证。

## 2. 当前实现状态

### T0：schema fixture 与 installer 合同

已完成：

- 增加 `tests/schema_fixture.py`，以真实历史 schema 生成测试源，不再用“新 schema 手工 drop 一部分对象 + 改 pragma”伪造老库。
- 更新 v12、v13、SPU LLM 的 manifest/版本断言。
- 不放宽 v14 leftover 守卫，不 skip，不删断言。
- 验证真实历史链上已声明的对象新增/删除/重建、migration append、投影、计数、序列和 v16 identity。

相关迁移模块扩展测试为 82 个通过。这只证明 T0 目标组，不等于全量回归已通过。

### T2/T4：正式库运行时失败式停止

当前 `api.py` 已实现：

- 当 `db_path` 命中 canonical formal DB 时，只读检查 schema compatibility，不调用 `initialize_database()`。
- 正式库仍为 v15 时，API 拒绝启动，不会创建 WAL/SHM 后再迁移。
- 临时 DB/测试 DB 仍允许 `initialize_database()` 完整初始化与迁移。
- freeze 检查已位于 lifespan 外层、早于 scheduler lock 文件写入。

### T3：离线 v15→v16 candidate

`scripts/migrate_v8_schema.py` 已存在，当前 CLI 参数为：

```bash
.venv/bin/python scripts/migrate_v8_schema.py prepare-backup --help

.venv/bin/python scripts/migrate_v8_schema.py prepare-backup \
  --source-db "$PWD/app/data/dcar_insight.sqlite3" \
  --backup /absolute/path/outside/project/dcar_insight_v15_backup.sqlite3 \
  --expected-source-sha256 <formal-v15-sha256> \
  --from 15 \
  --freeze-lock "$PWD/runtime/operator-freeze.lock" \
  --migration-lock /absolute/path/outside/project/v16-locks/dcar-v16-migration.lock \
  --receipt /absolute/path/outside/project/v15-backup-receipt.json

.venv/bin/python scripts/migrate_v8_schema.py build-candidate --help

.venv/bin/python scripts/migrate_v8_schema.py build-candidate \
  --source-db "$PWD/app/data/dcar_insight.sqlite3" \
  --candidate /absolute/path/outside/project/dcar_insight_v16_candidate.sqlite3 \
  --expected-source-sha256 <formal-v15-sha256> \
  --from 15 \
  --to 16 \
  --freeze-lock "$PWD/runtime/operator-freeze.lock" \
  --migration-lock /absolute/path/outside/project/v16-locks/dcar-v16-migration.lock \
  --backup-receipt /absolute/path/outside/project/v15-backup-receipt.json \
  --receipt /absolute/path/outside/project/v16-migration-receipt.json
```

migration lock 的项目外父目录必须由当前用户持有且权限精确为 `0700`；只预建目录，不预建 lock 文件。首次 `prepare-backup` 要求 lock 路径不存在，并以 `O_EXCL` 创建带固定 magic 的 lock；后续 backup/migration receipt 绑定其 inode、SHA 和 `0600` 权限，未知既有文件绝不截断。若首次执行在回执生成前崩溃，只能人工核验并移除孤儿 lock 后重跑，不得覆盖或编辑。`prepare-backup` 只读冻结的正式 v15 源，使用 SQLite backup API 生成项目外备份、执行独立 scratch restore 验证并写 O_EXCL backup receipt；禁止操作者手写 JSON。`build-candidate` 再通过私有 staging 迁移并校验 v16，发布项目外 candidate 和 O_EXCL migration receipt；两步都不就地改正式库。原无子命令的 candidate 参数仍兼容，但正式执行统一使用显式子命令。

`scripts/install_writer_database_candidate.py` 已收敛为 **migration-receipt 驱动的 v15→v16 原子安装**：重新校验 source/candidate/receipt/SHA/identity，备份旧 DB 与 sidecar，任一故障都恢复原 v15。当前 CLI 为：

```bash
.venv/bin/python scripts/install_writer_database_candidate.py --help

.venv/bin/python scripts/install_writer_database_candidate.py \
  --formal-db "$PWD/app/data/dcar_insight.sqlite3" \
  --candidate /absolute/path/outside/project/dcar_insight_v16_candidate.sqlite3 \
  --migration-receipt /absolute/path/outside/project/v16-migration-receipt.json \
  --expected-migration-receipt-sha256 <migration-receipt-sha256> \
  --backup-dir "$PWD/app/data/backups/v15-before-v16-YYYYMMDD-HHMMSS" \
  --receipt /absolute/path/outside/project/v16-install-receipt.json \
  --freeze-lock "$PWD/runtime/operator-freeze.lock"
```

`--backup-dir` 只允许 `app/data/backups/` 下的新直接子目录，candidate、migration receipt 与 install receipt 必须在项目根之外且不可覆盖。正式执行仍必须等待目标与故障注入测试全绿，并现场以 `--help` 复核；不复用旧 v11 示例参数。

反向 schema 恢复入口为 `scripts/restore_writer_database_backup.py`。它严格消费原始 `dcar-v16-offline-backup-v1` 回执，验证 freeze、无 holder、当前正式 v16 SHA、v15 backup 身份和同一 migration lock；原子安装 v15，并把原 v16 DB/sidecar 保留在新的 `app/data/backups/` 直接子目录。所有 durable checkpoint 的故障注入都必须回到字节相同的原 v16：

```bash
.venv/bin/python scripts/restore_writer_database_backup.py \
  --formal-db "$PWD/app/data/dcar_insight.sqlite3" \
  --expected-formal-v16-sha256 <formal-v16-sha256> \
  --backup-receipt /absolute/path/outside/project/v15-backup-receipt.json \
  --expected-backup-receipt-sha256 <backup-receipt-sha256> \
  --rollback-dir "$PWD/app/data/backups/v16-before-v15-restore-YYYYMMDD-HHMMSS" \
  --receipt /absolute/path/outside/project/v15-restore-receipt.json \
  --freeze-lock "$PWD/runtime/operator-freeze.lock"
```

### T5：current-day guard、小时 reconcile、状态和质量门

当前 scheduler 已实现：

- `current_day_daily_capture_guard()` 显式要求 `db_path`、`reports_root`、`capture_call_override` 和 `effective_from`，不依赖 `DCAR_V8_DB` 或 `DEFAULT_DB` 默认值。
- 只计算北京时间当天 02:00 精确槽，不用 `latest_occurrence()`，不扫描昨天/历史缺口。
- 已有任意状态的当天槽都返回 `already_attempted`，小时保障不自动 retry。
- 02:00 Cron 与 `daily_capture_reconcile` 共用同一 guard。reconcile 每小时一次，显式 `next_run_time=now`、`coalesce=True`、`max_instances=1`、`misfire_grace_time=None`。
- reconcile 不加入 `JOBS`，不生成独立 scheduler run；真正执行只记 `job_id=daily_capture`。
- `/api/v8/scheduler` 返回 `daily_capture_reconcile: {mode=current_day_only, enabled, effective_from, interval_seconds=3600}`。

状态语义：

- `succeeded`：所有最终待执行对象完成。
- `partial`：有真实成功，也有局部未解决项。`partial` 是终态，包括 `operator_retry` 在内都不重跑同一槽。
- `failed`：全局 auth/balance/budget 阻断、没有任何有效成功或顶层异常。
- `skipped`：没有 enabled 身份等无任务情形。

上线质量门与 scheduler run 状态分离，不篡改 run：

- `len(discovery) == monitored_accounts`，且 succeeded ≥90%。
- `len(content_updates) == monitored_contents`，且 `succeeded + already_succeeded` ≥60%。
- `blocked_providers == []`。
- details 成本与 `provider_usage` ledger 一致，且 task 总额≤USD 8。

2026-08-11 的实测样本为 178/183 discovery 成功、2189/3000 content 成功、`blocked_providers=[]`、USD 6.151，可通过 90%/60% 门。`eligible_contents=61,718`，因此 3,000 只是当天 selected cohort 上限（4.86%），60% 不是全库覆盖率。

“02:00 exact slot”来自业务槽计算，不是回调必须在 02:00:00 到达。Cron 负责低延迟，小时 reconcile 负责当天正确性。如果白天才补到 capture，它不追跑 media download/process/cutoff、日/周报或 publisher。

### T6/T7：预算与 range 合同加固

- capture 已强制 `task_id` / `task_max_amount` 同时存在或同时缺失；不允许 task budget 在缺 cap、错 task 或放大 cap 时进入 provider callback。
- task 总额硬顶在 `_reserve_budget()` 的同一 `BEGIN IMMEDIATE` 事务中跨 operation 累计；per-operation batch 不会放大 task 总额。
- range `_iso()` 已保留微秒，不会丢掉 `23:59:59.999999` 边界。
- 非 `--full-history` 必须显式传 `--start`；`--as-of` 也已为必填，必须是实际采集截面。
- 正式 DB mutation 要求有效 operator freeze，campaign contract 在首次 provider/写库前验证。

当前 CLI 必须现场通过以下命令复核：

```bash
PYTHONPATH=src/dcar_eval:. .venv/bin/python -m v8.range_backfill --help
```

### T8：部署和 Web 类型

- `types.ts` 的 `latest_capture_run.status` 已包含 `partial` 和 `interrupted`，不增加页面提醒或状态 UI。
- `scripts/start_web_mvp.sh` 对非零 scheduler、非零 startup catch-up、非空 reconcile 日期都 exit 78，8765 固定不调度。
- writer plist 已增加 `DCAR_DAILY_CAPTURE_RECONCILE_FROM`，writer lock 改为 `$HOME/Library/Application Support/DcarAIGC/runtime/writer-worker.lock`。
- renderer 已增加必填 `--reconcile-from`，严格验证规范且真实的 `YYYY-MM-DD`，并保持 `open("xb")` 拒绝覆盖。
- wrapper 只从 plist 继承日期，不从 `writer.env` 解析。它会先 unset/校验，再在成本授权和key preflight 后导出给 writer。

部署与类型目标测试已通过，但尚未执行任何 `launchctl enable/bootstrap`。

## 3. 不能做的事

- 不增加页面上的“抓取已停止/数据过期”banner、badge、toast 或其他提醒。
- 不补写历史 `daily_capture` 调度槽，不写占位 `skipped/succeeded`，不伪造调度成功。
- 不自动 retry `partial`，不让小时 reconcile retry 已有 `failed/interrupted` 槽。
- 不为 media、cutoff、日报、周报或 publisher 增加小时追赶。
- 不修改 3,000 条 selected cohort 算法。
- 不增加 DB 表/列/状态枚举或 scheduler migration。
- 不让 8765 成为第二个 scheduler。
- 不通过 skip、删断言或放宽 schema/publisher 守卫消红。
- 不启用或修改 publisher，不发布 Ubuntu snapshot。
- 不用 `git reset --hard`，不覆盖用户已有 dirty hunk，不 push。
- 不为赶 D 日而跳过 freeze、备份、schema、测试或时序门。

## 4. Gate 0：停止移动目标并创建本地基线提交

原交接的 `git add src tests config scripts deploy docs app/web/app app/api pyproject.toml` 有明确漏项：它不包含根 `README.md`、`AGENTS.md`、`archive/legacy_scripts/`、`app/web/tests/`，也容易遗漏新增的 T0/T3/T9 文件或已移入 archive 的配对删除。不得照抄该命令。

Gate 0 只能在所有并行 agent 停止写入后执行：

1. 记录 HEAD、branch、`git status --porcelain=v1`、tracked binary diff、untracked 清单和目标文件 SHA到项目外回执目录。
2. 重新采样正式 DB schema/SHA/WAL/用量，记录 operator freeze identity，不打开可写连接。
3. 从 `git status --short` 中逐个审核并创建显式 allowlist。至少要复核根 `README.md`、`AGENTS.md`、`archive/legacy_scripts/`、`app/api/`、`app/web/app/`、`app/web/tests/`、`config/`、`deploy/`、`docs/`、`pyproject.toml`、`scripts/`、`src/`、`tests/`，包括删除与移动的成对路径。
4. **禁止 `git add -A` 和无审核的整目录 staging**。明确排除 `.venv/`、`outputs/`、`runtime/`、正式 DB/sidecar、reports 生成物、凭据、缓存和其他非源码文件。
5. 用 `git add -- <reviewed-file> ...` 只 stage allowlist，然后执行：

```bash
git diff --cached --name-status
git diff --cached --check
git status --short
```

6. 逐项核对 staged 列表与 allowlist，确认无生成物/凭据/数据库，再做本地 checkpoint commit。不 push。
7. 提交后重跑 T0/T2–T8 目标测试，确认工作树只剩明确的生成物白名单。

## 5. 代码与副本验收门

所有 Python 测试必须设 `DCAR_TEST_DENY_FORMAL_DB=1`，使用临时 DB、临时 reports 和 fake provider。

最少覆盖：

- 真实 v11/v12/v13/v15 迁移 fixture，不伪造 pragma + 新结构。
- formal DB schema 不匹配时不调用 `initialize_database()`，DB/WAL/SHM 零变化。
- freeze 早于 scheduler lock 文件和任何正式写入。
- v15→v16 candidate 成功、各 durable checkpoint 故障注入、receipt 不可覆盖、源库零变化。
- migration-receipt 驱动的原子安装和每个切换故障点的完整 v15 恢复。
- current-day guard 在生效日前、01:59、历史缺口、已有六种状态、跨夜、长睡眠场景下的零历史追跑。
- 6 个业务 Cron + 1 个 reconcile，启动首次立即、无限 misfire 宽限、coalesce 和单实例。
- Cron/reconcile 并发时动作 1 次、run 1 行、attempt 1 行，另一个返回重复跳过；同时断言 attempt 部分唯一索引、终态触发器和 append-only。
- 178/183 + 2189/3000 通过质量门；1/3000、数组长度不等、provider blocked、ledger 不等或超预算均失败。质量失败不改 scheduler run 状态。
- task 预算跨 operation 总额封顶、并发预占、unbilled 释放、缺 cap/错 task/放大 cap 在 provider callback 前失败。
- range 微秒保真、非 full-history 缺 `--start` 拒绝、`--as-of` 必填、首次 mutation 前合同验证和 formal freeze。
- `startup_catchup` 严格 report-only，8765 对三个 writer 配置 fail-loud，plist/wrapper 日期不走 `writer.env`。

全量命令：

```bash
DCAR_TEST_DENY_FORMAL_DB=1 PYTHONPATH=src/dcar_eval:. \
  .venv/bin/python -m unittest discover -s tests -p 'test_*.py'
```

全量结果必须与新 checkpoint 基线比较，不得直接沿用 Claude 的“1200 / 82 红”移动快照。publisher 的三处合同漂移可作为已登记的独立失败，但不能混入其他新失败后一并忽略。

## 6. 正式切换门和回滚

正式切换只能在 Gate 0 与副本故障注入全绿后进行：

1. 保持 operator freeze，停止 8765、8766、range/backfill 和其他直连 CLI。
2. 在 Mac 上核对 4173/4174/8765/8766、`launchctl`、PID 和 `lsof` DB/WAL/SHM holder；采样 `provider_usage` 两次，非预期增长必须停止。
3. 产生任何 backup receipt 前，先归档旧 plist，并把唯一可写项目根从 `~/Documents/DcarAIGC` 同卷原子移到 `/Users/mark/Projects/DcarAIGC`；不复制出第二份 DB/checkout。新根运行 `uv sync --frozen` 并重跑目标测试。
4. 之后所有 `$PWD` 必须是最终 canonical root。backup receipt 绑定正式 DB 绝对路径，所以回执生成后禁止再迁根，否则受测的 reverse restore 会正确拒绝。
5. 确保 WAL 为空/不存在；先创建当前用户独占、`0700` 的项目外 lock 目录，再使用 `migrate_v8_schema.py prepare-backup` 创建项目外 v15 备份、执行独立 scratch restore 验证并生成不可覆盖回执；禁止手工创建 lock 文件或手写回执。
6. 用 `migrate_v8_schema.py build-candidate` 生成同文件系统的项目外 v16 candidate 和 migration receipt，证明正式 v15 源全程不变。
7. `tests.test_restore_writer_database_backup` 的 reverse-restore 全 checkpoint 故障注入必须全绿，证明失败时回到字节相同的原 v16。
8. 用整合后的 migration-receipt 安装器原子切换，核对 v16 identity、回执、保留的 v15 DB/sidecar 和无 holder。
9. 完成新根/.venv/plist 校验后才进入 D 日激活。

回滚分界：

- 正式库仍为 v15：可回滚代码或项目根，但保留 freeze 和已验证备份。
- 正式库已是 v16：只回滚代码不能恢复服务，旧代码会拒绝 v16。唯一 schema 回退路径是在 freeze 下运行受测的 `restore_writer_database_backup.py`，由原始 backup receipt 驱动原子恢复；禁止手工替换 DB/sidecar。
- 已完成的有界内容恢复、供应商费用和审计记录不通过删表/删行回滚。

## 7. D 日 writer 激活

规则是硬约束：所有实施、备份、迁移、测试和 plist 复核必须在 **D 日 01:00 前**完成。超时则 D 顺延一天，先归档已渲染 plist，再用新 D 渲染。禁止在 02:00 后临时启动。

当前 renderer 命令：

```bash
D=2026-08-21

python3 deploy/macos/render_launch_agent.py \
  --project-root "$PWD" \
  --reconcile-from "$D" \
  --check

python3 deploy/macos/render_launch_agent.py \
  --project-root "$PWD" \
  --reconcile-from "$D" \
  --output "$HOME/Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
```

plist 为 `RunAtLoad=true`，bootstrap 会立即启动，reconcile 也会立即检查。计划在 **D 日 00:09** 复核 v16 identity、备份/安装回执、plist 和端口，全通过才按回执解除 freeze；任一门失败都保留 freeze 并顺延 D。然后在 **D 日 00:10** `enable + bootstrap`；此时仍早于 02:00，guard 返回 `before_today_slot`，不发生付费抓取。不在首次 bootstrap 后执行 `kickstart -k`。

观察至少 650 秒，必须同时满足：

- 8765/8766 分离，8766 health 稳定。
- scheduler requested/enabled，唯一 scheduler lock 持有。
- `startup_catchup.mode=report_only`，results 只包含日/周报。
- reconcile 为 `current_day_only`、`effective_from=D`、`interval_seconds=3600`。
- stderr 无 preflight exit 78、无日期/成本/凭据拒绝、无 KeepAlive + 300 秒 throttle 崩溃循环。
- 观察窗内 `provider_usage` 无增长。

禁止付费烟测。等待自然 02:00 后，用 scheduler run/attempt、`quality_gate`、provider ledger 和日志验收。状态为 `succeeded` 或 `partial` 只是必要条件，还必须通过 90%/60% 质量门。

## 8. 历史槽不补，只做有界内容恢复

不补写历史 scheduler 槽是已拍板的决定。有界内容恢复与 scheduler 收据是两件事：它可以恢复内容，但不伪造当天调度成功。

已授权日期只有：

- `2026-08-17`
- `2026-08-18`
- `2026-08-19`

合同：

- 每日独立 task ID：`v16-gap-YYYYMMDD-range-v1`。
- 每日 `--max-amount 4.00`，task 级跨 operation 总额硬顶。
- `--max-pages 8`、`--workers 1`，三日总授权 USD 12.00。
- `2026-08-16` 及之前不补，`2026-08-20` 及之后不自动扩日。
- 每个 phase 都重复显式传 `--start`、`--end`、`--as-of`、`--task-id`、`--max-amount 4.00`、`--max-pages 8`、`--workers 1`、`--db`。
- end 使用北京当日 `23:59:59.999999+08:00`，`--as-of` 使用所有 phase 共享的实际执行截面，不伪造历史指标时点。
- 任何 phase 前先运行 `PYTHONPATH=src/dcar_eval:. .venv/bin/python -m v8.range_backfill --help`，以当前 CLI 为准。
- 正式 DB mutation 期间保持 operator freeze；每个 phase 之间人工核账和质量验收。

这批恢复不追跑过去的 02:20/03:00/07:30、日报、周报或 publisher 槽。已产生的内容、付费用量和审计不在回滚时删除。

## 9. publisher 独立阻断项

snapshot publisher 当前已停用、未加载。本轮不 render/load/enable/bootstrap/kickstart，不修 publisher 逻辑。

已知三处合同漂移：

1. schema expected 13 vs 当前代码 16。
2. migration expected `scheduler-run-attempt-history` vs 当前 `remove-manual-review`。
3. report expected v8.6 vs `pyproject.toml` / 当前合同 v8.7。

该组只能作为后续独立升级：同步 macOS publisher、server installer、fixture 和端到端身份后才能启用。不能在本轮通过删门禁或改期望值强行消红。

## 10. 完成定义

只有以下全部完成，才能声称“每日抓取已恢复”：

1. Gate 0 明确 allowlist 的本地 checkpoint 提交完成，无生成物/凭据/数据库被 stage，不 push。
2. 目标测试、全量回归和副本故障注入通过，除已登记 publisher 漂移外无新失败。
3. 正式 v15 备份可恢复，v16 candidate 与 migration receipt 验证通过，原子安装回执完整。
4. D 日 00:10 writer 加载通过 650 秒零付费/preflight 观察，无崩溃循环。
5. 自然 02:00 实际 run/attempt 唯一，质量门通过，供应商 ledger 不超授权。
6. 有界内容恢复只在 2026-08-17/18/19 内执行并逐 phase 核账；不新增历史 scheduler 槽，不追跑下游作业。
7. publisher 仍保持停用/未加载，三处漂移作为独立后续任务。

任一门失败都停在当前阶段，保留 freeze、备份、candidate、receipt、日志和测试证据，不为赶日期扩大授权。
