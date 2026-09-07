# 视频抓取 v2.5 分步闭环执行记录

状态：**实施中：2026-09-06 上午已修复昨夜辅助脚本未收尾问题，64项历史本地欠账清零；正式 Writer 安装 a0b6a51，scheduler paused、current HOLD generation3 生效。命令4105正在执行固定20个按需真实样本，尚无最终route结论；不得把少量已成功样本等同于完整验收。普通付费仍关闭，schema20、全部operation资格及独立archive卷等仍未完成。** 本记录区分离线代码通过、生产安装、真实付费验证和完整业务日验收；不能以其中一项替代其余项。

## 范围与基线

- 批准方案：[v2.5 需求说明](/Users/mark/Documents/DcarAIGC/文档/视频数据抓取链路专项审计与改造需求说明_v2.5_2026-09-05.md)，SHA-256 `4d01f69ee90223f8884e88116c73e12be4c363d2e0780942311c22078c290bee`。
- 实施工作树：`/Users/mark/Projects/DcarAIGC-worktrees/video-capture-v25`，分支 `codex/video-capture-v25-implementation`。不覆盖原工作树的并发改动。
- Phase 0 冻结记录：[baseline receipt](</Users/mark/Library/Application Support/DcarAIGC/rollouts/video-capture-v25-20260905T125300/phase0-baseline-receipt-v1.json>)。运行状态须在实际安装前重新核验，不把历史冻结记录当成实时健康证明。
- 本轮 Step 2B-2/2B-3/2B-4 仅修改候选代码及测试、文档；未访问或修改正式数据库，未启动服务，未向供应商发起真实请求，真实费用 **USD 0**。

## 已完成代码步骤

| 步骤 | 结果 | 提交 |
| --- | --- | --- |
| Current activation hold | 同 activation 的显式 hold 控制及 readiness 保护 | `35df472` |
| 抓取传输与 raw 证据 | 流式传输、压缩 raw、不可覆盖付费身份与回放证据 | `32e1028` |
| 来源 policy-v2 | detail view 占位值、statistics share、legacy operation 三项止损 | `8058cd6` |
| Step 2B-2 故障域与预算 | 五域隔离、预算上限、一次性补偿和严格恢复证据 | `d64e0fe` |
| Step 2B-3 有界本地补偿 | 本地 materialization 优先、控制/执行分离、独立 executor | `0551c95` |

### Step 2B-2：故障隔离与费用上限

生产模块：`provider_budget.py`、`fault_recovery.py`、`capture.py`、`provider_recovery.py`、`provider_transport.py`、`douyin_openapi_sync.py`、`scheduler.py`、`pipeline.py`、`range_backfill.py`、`scan_terminals.py`、`scripts/run_full_history_cache_batches.py`、`config/source_routing_matrix_first_v2.json`。

- Provider、operation、storage、authorization、paid identity 五个故障域分别持久化；相同证据指纹复用 generation。单个 transport unknown 不再开启全局 provider circuit。
- 发现/指标自动桶分别为 USD 30/15，历史 repair 默认为 0；自动总额 50、显式事故授权绝对上限 100，禁止隐式借桶。
- 补偿必须绑定原始终态结算、本地不可恢复业务缺口、原 paid identity、sequence=1 和一次性授权；普通 sequence=0 不可携带授权绕过购买限制。
- Operation 恢复核验实际样本与 raw，storage 恢复实际执行本地幂等写、hash/readback 和容量验证；不接受调用方单独填写成功计数或布尔值解锁。
- 保留旧 scan-terminal-v1 收据语义，新阻断分类使用 v2。

发现并修复：新预算/故障码未进入历史补抓阻断集合造成空转；无 HTTP 429 的业务限流码被误归字段合同；合法 `media_source_refresh` raw 无法通过来源 stage 校验。修复均保留对应原业务断言并增加负向保护。

验证分组（有交集，不合并成唯一测试总数）：

- 账务、抓取、恢复：123 项通过；新增业务限流及补偿类型修正各自定向复测通过。
- 调度、队列、授权、hold/readiness、传输：271 项通过。
- 四个历史补抓模块：130 项通过。
- Publisher + cache batch：66 项通过；最后 batch 修改后 44 项复测通过。
- Source routing：37 项通过。
- 改动 Python 文件 Ruff、`git diff --check` 通过；10 个生产模块定向 Mypy 通过。
- 早期扩展回归 841 项曾在六个兼容模块发现失败，已在上述对应模块逐一修复复测；没有把它描述为重新跑过且全部通过的最终全量回归。

### Step 2B-3：有界本地补偿与独立执行器

生产模块：`pipeline.py`、`tikhub_scan.py`、`runtime_receipts.py`、`reconcile_control.py`、`api.py`。

1. `resume_local_materialization` 仅接受已有、经 raw/manifest/冻结身份校验的 pending materialization；没有 provider callback、替换 cursor 或替换 scope 参数，绝不进入 `_reference`/`_raw`/网络路径。
2. 每次最多 50 个工作单元，共享 60 秒 monotonic 领取期限。截止后不领取新工作；已开始的 fsync/事务不能被强行截断，真实耗时和已处理数量照实记账，不宣称进程可在恰好 60 秒被抢占。
3. 本地 item 进度逐项 checkpoint；未完成保留 continuation，已有 cursor 是旧链路冻结的 provider continuation，重放不改写它，pending 未清前不允许购买下一页。
4. Reconcile 顺序为：本地 raw 入库欠账 → 当日轮次登记/子任务收据 → 其余父任务收尾 → 运行收据。历史 raw 可零费用恢复，历史购买仍关闭。
5. Reconcile 不同步执行付费页、媒体分析或账号指标刷新；新轮次持久化为 `execution_queued`，由原执行队列接续。同一个未变化的 queued parent 不反复新增 attempt。
6. 调度每 5 分钟一个有界 reconcile slice；report/reconcile 各独立 1 worker，control 保留给控制与授权任务。已注册的采集、报告任务不删除。
7. Scan/day receipt 也计入预算；耗尽时保留欠账。Slice 成功仅表示本次控制动作完成，`summary.data_complete` 和业务 readiness 仍由真实覆盖收据决定。

验证与问题闭环：

- Pipeline + scheduler + API + runtime receipts + work budget：263 项通过。首次发现一个测试用相同 tick 连续构造两个不同故障，被新的重复调用保护拦截；改用两个独立 tick，保留原故障断言，重跑同组通过。
- 原 TikHub scan + 新 local-only materialization：49 项通过。
- 新真实测试库联动回放：1 项通过。先注入 SQLite 本地入库失败，形成真实 pending/raw/child，再经真实 pipeline 重放写入 detail/metrics；Provider 回调 0 次，`provider_usage` 全行与金额不变、付费 dispatch 事件不变；slice 成功但无完整覆盖 receipt 时仍 not-ready。
- 本地重放预检可能消耗截止时间的问题已修复：预检后、claim 前再次检查 deadline，已过期时 attempt/usage 不增加。
- Ruff、定向 Mypy 和 `git diff --check` 通过。此处测试库联动不是用户要求的线上真实端到端实跑。

### Step 2B-4：阻断队列冻结、本地路径保留及积压分区

代码完成，提交 `c1b007b`。生产模块：`pipeline.py`、新增 `work_readiness.py`、`provider_updates.py`、`providers.py`。没有 DDL、前端或生产运行态变更。

1. 新队列和同日 pending 的恢复都在 claim 前按真实 operation、content/account identity、stage 和 window 判断 readiness。一次 planner pass 共用只读预算检查及 unresolved-ledger 索引，不为每条内容重扫费用账本；最终发送边界仍独立验权。
2. 阻断按 scope + readiness generation 只追加首次审计收据；重复 tick 不新增 queue attempt、usage 或 paid marker。interval drain 收据同北京日复用，START→SEALED 保留首次收据、不篡改历史。
3. 抖音 metrics 的 detail_counts/statistics 可分组执行，未执行组记录 `deferred_groups` 并保留 pending。已入库的成功组不再持续唤醒仍被阻断的兄弟组；故障恢复后原 durable run 可继续，不重买成功组。
4. 完全付费阻断时仍运行显式 cache-only 的 raw replay、本地下载/处理与恢复申请，不申请新的付费队列 attempt。缺缓存、缺探测 raw、XHS probe 后 video lifetime 都在 budget/claim 前停止；XHS image 派生及可信零评论派生保留。评论保留既有 `cache_only_page_unavailable` stop reason。
5. 冻结 item 只存紧凑身份；恢复从 DB 读当前 platform/content，不改原 cycle/identity。历史禁用欠账、过期业务日、被替换 activation 和媒体恢复前置仍走已有本地收尾规则。
6. 新 `queue_backlog_summary` 以 `(queue kind, content)` 为单位，将 runnable 和 blocked 互斥计数；部分可执行指标内容归 runnable，阻断 operation 另计。包含已 claimed pending 和媒体前置，不删 discovery/expected 分母；查询本身不写审计行。`pipeline_summary` 先筛 durable contract 再取最近 500 行，避免阻断收据淹没真实任务。

验证与修复：

- Pipeline、scheduler、API、runtime receipts、readiness/backlog、provider updates、媒体前置及本地 materialization 联测 **289/289** 通过（29.653 秒）。
- 末次 cache-only + 原评论分页联测 **12/12** 通过（0.547 秒）；provider 本地子项另有 8 项完整定向测试通过。测试分组可能重叠，不累加为唯一总数。
- 加强后的 3 项队列测试再通过：重复 tick 的队列 attempts、全 usage、全 paid events 逐行不变；local runner 确实继续运行；已成功 metrics 组不重买；恢复原 run 清空 pending。
- 早期 107 项联测发现正常 batch 摘要多输出空 `blocked_work` 的兼容错误。已改为只在有阻断时增加字段，原精确格式断言保留，单项及上述 289 项复测通过。
- 新增 backlog/只读测试 4 项、readiness 模块 6 项通过（包含在 289 项中）；还验证 500 条非 durable 收据不改变 discovery coverage。
- 改动文件 Ruff、`git diff --check` 与四个生产源模块定向 Mypy 通过。尚未运行最终全仓完整回归，也未部署或进行真实供应商 E2E。

### Step 2C-1：请求路由冻结与诊断 HOLD 绑定基础

代码完成，随本节提交；这是双 permit 的基础步骤，**不是已可执行的真实诊断许可，更不是 operation qualification 通过**。生产普通发送仍不得恢复。

- 修复已确认的配置时序问题：旧路径在 claim/send 后的调用闭包中才通过 `_tikhub_url()` 重读配置。现在九个 TikHub adapter 付费入口先冻结非敏感 route manifest，在 claim 前、send 前分别复核，将相同绑定写入 usage、dispatch scope、不可覆盖 send claim；闭包只使用冻结 api_base。即便 send 后配置变化，本次 URL 也不会改变。
- Manifest 固定 api_base、host、route id、HTTP stack、route generation 和 config SHA，不读取或记录 API key。响应 transport receipt 逐字段核对冻结值。paid scope identity 及不可覆盖 claim 文件名不加入 route，跨域名不能制造新购买身份。
- 测试注入的 provider callback 明确不作为 live transport qualification；缺少 manifest 的兼容 fixture 不能被未来诊断资格链当成有效样本。
- `read_current_diagnostic_hold` 仅从 schema19 DB 推导 START、activation/roster、当前 build generation、build/runtime/config/price/budget prerequisite、actor 和 Matrix 高水位；既有 terminal attempt、self-hash、expiry 及完整 CAS 都必须通过。SEALED、build advance、缺失/过期/损坏收据或 Matrix 增量即拒绝。该函数本身不签发许可、不开放 drain。
- 仍缺：自然 due 的诊断 campaign/rank、member permit、writer 内 operator runner、诊断 tail 的严格核销及 20/200 实际资格收据。不能直接以当前 helper 或健康探针替代这些门。

验证（分组有交集，不累加）：

- Providers、provider updates、transport、TikHub scan、capture：163 项通过（14.032 秒）。
- Current hold、HOLD binding、paid dispatch、budget、provider recovery、scan terminals：111 项通过；Matrix fence 补充后 HOLD binding 5 项再次通过。
- 新路由边界 4 项：claim 前漂移零 usage/slot/attempt/marker；等待网络槽期间漂移归 `not_sent` 且零实际发送；send 后漂移仍发送冻结 URL；同 identity 换 route 仍被不可覆盖 claim 拒绝，异常后 context 清理。
- Config/transport/paid identity/raw evidence：50 项通过；Capture + providers：98 项通过；Range backfill + config + queue readiness：39 项通过。
- 四项路由边界与五项 HOLD binding 最终联合 9 项通过；改动文件 Ruff、定向 Mypy、diff-check 通过。Mypy 的 `tikhub_config.py` 单独检查，避免此项目双 PYTHONPATH 名称造成重复模块诊断；不删除或忽略类型错误。
- 本步骤未访问正式 DB、未启动服务或真实供应商请求，真实费用仍 USD 0。

外部核对：[TikHub 官方域名公告](https://docs.tikhub.io/4579297m0) 明确中国大陆使用 `.dev`、其他地区使用 `.io`；[官方 health 文档](https://docs.tikhub.io/237673542e0) 明确它只测存活、不测依赖。本次只读取文档，没有调用健康或付费接口。实际 route 仍必须经过方案规定的互斥样本资格门，禁止运行期 fallback。

### Step 2C-2：自然到期来源、不可变诊断收据及主组冻结

本节代码和定向联合验证完成，随本节提交。只新增诊断控制基础模块，不修改 DDL、业务页面或生产运行态；**没有签发可发送的 member permit，没有解锁生产发送**。

- `transport_natural_due.py` 从现有 durable run 的真实 owner、activation/roster、冻结 candidate/pending/item、账号轮次、评论已持久化 cursor、扫描父子关系重新构造完整请求。请求参数、cursor、时间窗、due bucket 和 sequence 必须全部一致；直接/manual owner、历史购买、未来/过期工作及已完成本地工作不能冒充诊断样本。
- 自然排序时间取 queue 的 `created_for` 或 cron parent 的 `scheduled_at`，另存 `source_scheduled_for=scan:<hash>`。不能把 scheduler 表中的幂等键误当时间排序，也不能让 caller 自填时间。
- `transport_receipts.py` 复用 schema19 scheduler run + 单次 terminal attempt，并绑定不可覆盖 0600 镜像。写入先 fsync/读回镜像，再同步终态；同 key 同 payload 幂等，缺镜像、篡改、冲突和不同字节的 rollback orphan 均拒绝。纯控制 receipt 不产生 usage/fetch/paid dispatch。
- `transport_cohort.py` 在 HOLD/build generation 内冻结每个名册 Douyin 账号最近一份 hash 完整且固定路由页形态有效的 raw，以 **entity bytes** 的 nearest-rank P75（含并列）选大页账号。缺 raw 单列，不从名册外补样；损坏证据不静默回退；新增 raw 不重选旧 cohort。
- `transport_campaign.py` 冻结唯一 `.dev + urllib-stream-v1`、20 页主组、正常单价/预算、开始高水位和按先后次序选取的规则；期限为 24 小时与 HOLD prerequisites 最早 expiry 的较早者。重复调用返回同一 campaign，不重置失败分母。该 API 不支持凭空签发 host/stack 对照组或 qualification。

已发现的问题与处理：历史 raw 的 `local_path` 可能相对 `PROJECT_ROOT` 存储，cohort 不能按当前 cwd 读取；已按 capture 的正式读取规则修复并增加相对路径测试。首轮自然到期测试有一项 fixture 试图删除不可变 roster 行，触发真实 DB trigger；已改为关闭账号有效性来验证 active-member 边界，保留原拒绝断言和生产 trigger。另一项短期 budget prerequisite 测试曾复用旧 artifact identity 修改 expiry，被不可变收据正确拒绝；已用新 artifact identity 构造新收据，不放松任何生产约束。

验证：新增收据/大页/campaign/自然到期与原 HOLD、route、paid identity、durable runs 联测 **50/50 通过（1.234 秒）**；新增 4 个生产模块定向 Mypy 通过，新增 8 个源/测试文件 Ruff 与 diff-check 通过。早期 Ruff 检出测试文件未使用的 import，已删除无用 import 后复测通过；不删除测试或断言。未重复全仓回归，未访问正式 DB，未运行真实供应商请求，真实费用 USD 0。

下一闭环是 member permit、真实队列枚举/排名与 writer 内命名 operator runner；不可将本节记录或 callback fixture 当作真实 20/200 transport qualification。

### Step 2C-3：实际 writer 归属、固定成员许可与逐次校验

代码和定向验证完成，随本节提交。Step 2C-2 提交为 `045d572`。**本步骤仍不接通生产发送，也不是线上端到端结果。**

- `runtime_database.py` 在已有 `acquire_writer_lock` 生命周期中登记真实进程 PID、数据库 inode、已持有 fd 与 lock inode。新校验拒绝其他 DB、其他 PID、释放/替换锁和仅维护用途的 formal-mutation lease，不接受 caller 的 `held=true` 或观察到别人持锁作为 writer 权限。原 API health 格式不变。
- `transport_due_candidates.py` 只读枚举当前 running 的 Douyin 自然扫描子任务，重推 owner、名册、cursor、reference 和完整自然到期证据，按真实到期时间/UID/cursor/paid identity 排序。不创建任务、不自行删去已有费用的候选；明确不就绪项过滤，损坏的有效范围证据报错。
- `transport_members.py` 由同 campaign 的实名 operator durable owner 和当前 writer 批量冻结前 20 个大页 cohort 成员，保存 HOLD/路由/预算/actor/issue/expiry 绑定。少于 20 项不签发；前排 scope 已使用时整批拒绝，不能看完结果再拿第 21 项补位。整批 savepoint 防止镜像异常被 caller 捕获后提交半批许可。
- send 前校验重读 DB、0600 镜像、当前 HOLD/build、实际 writer、operator owner 和完整 natural-due scope。后续 rank 必须有先前 rank 的同 START、同 member 终态 dispatch 链；失败保留原 rank。当前只有 issuer/verifier，尚需 capture/dispatch 边界及 writer runner 接线，不能直接把 helper 当作 drain 例外。

验证与修复：

- 成员/枚举/自然到期/writer/runtime/收据/campaign/HOLD/dispatch 联测 **53/53 通过（1.934 秒）**；3 个生产模块定向 Mypy、7 个改动源/测试文件 Ruff 和 diff-check 通过。
- 4 个成员测试使用真实 20 账号、等字节 P75 raw、HOLD、campaign、自然扫描和实际已安装契约式 writer lease，不 mock 核心校验器。验证固定 ranks、幂等、rank2 不抢跑、无 writer/19 项时零许可，以及请求/游标/配置/owner/HOLD 漂移拒绝和 rank21 不补位。
- 首次 53 项虽返回 OK，但原 `ApiInstalledWriterLeaseTest` 的路径假文件令真实后台控制 executor 报 `file is not a database`。已给该生命周期测试初始化真正的空 schema19 DB，不静音 logger、不禁用 executor、不删除断言；重跑同 53 项通过且无该异常。
- 未访问正式 DB、未启动服务或真实供应商调用，真实费用 USD 0；无 DDL、前端、配置或依赖变更。

### Step 2C-4：诊断许可接入真实发送边界

代码完成，随本节提交；仍是候选代码的离线闭环，**没有生产安装、真实 provider 请求或 operation qualification**。

- `capture.py` 的 reserve 与获取 network slot 后的 send 两个事务都重验 START/member/自然 due/owner/writer lease/实际 paused scheduler/route；usage、dispatch 与不可覆盖 send claim 绑定同一成员及请求身份。等待期间 owner、expiry、scheduler 或 reservation 变化会终结为 `not_sent`，费用预占释放，不创建 fetch attempt 或发送 marker。
- `paid_dispatch.py` 独立验证同一诊断许可；普通路径不接受诊断 scope，仍必须通过当前 RELEASE。
- 修复真实 schema19 fixture 暴露的兼容错误：原 `permit_event_id=START` 被 `trg_paid_dispatch_events_binding` 拒绝。**不修改 DDL、不虚构 RELEASE、不打开 HOLD**。该既有 FK 只保存当前 START 紧邻、同 activation、hash 链验证通过的真实历史 RELEASE；`diagnostic_member` 显式写 `authorization_kind=current_hold_diagnostic_permit_v1`、START id/hash、历史 anchor id/hash/role、campaign/rank/arm。当前发送权限来自 START 成员许可，绝不来自旧 RELEASE。所有诊断读取和前序 rank 校验同步使用这两个不同语义，schema20 再使用独立授权列。
- `provider_budget.py` 只允许完整成员验证通过的请求对 operation transport 故障做有界诊断；不关闭故障，不免预算。逐类检查全部 open operation faults，避免新 transport 掩盖旧 field/rate 故障；余额、账号鉴权、存储及原价格/额度仍阻断。
- 本步骤没新增业务 UI、接口路由、依赖或数据库结构；既有触发器完整保留。

验证与修复：

- 诊断 capture/budget + 普通 capture/budget/drain/dispatch + HOLD/member 联测 **129/129** 通过（9.024 秒）。其中新增 14 项诊断测试；测试回调使用真实临时 schema19 库、真实 writer lease、实际 paused APScheduler 与字节 fixture，**不是线上 E2E**。
- 正向测试核对原始字节、usage/dispatch/send claim 一致且只发送一次；发送后 schema 对象与 drain 历史不变，状态仍 `draining`，普通请求仍拒绝。负向覆盖过期、scheduler 恢复、source owner 丢失、篡改、重复/提前 rank，以及 operation/provider/auth/storage 阻断。
- 测试初次故障断言比较了写接口的 `id` 与读接口的 `receipt_id`，已改为前后读取同一完整 fault state 做严格相等比较；原“诊断不清故障”断言不降低，129 项复测通过。
- 9 个改动 Python 文件 Ruff、6 个生产模块定向 Mypy、`git diff --check` 通过。尚未运行最终全仓完整回归。真实费用 **USD 0**；正式库及服务未触碰。

### Step 2C-5：固定成员单页执行与本地入库

代码完成，随本节提交；新增 `transport_execution.py` 及 8 项集成测试。该执行原语仅接受已经签发的 member id、原 operator claim 和实际 paused scheduler，不能传账号、cursor、route、provider 回调或临时预算。**它尚不是生产入口、完整 campaign coordinator 或 qualification verifier**。

- 从 durable member 重建请求和当前 source owner；发送前复核上一个已提交 manifest 的真实文件/hash，以及 `_page_key` 与规范 cursor 对应关系。HTTP 调用只使用成员冻结的 reference/cursor。
- 每个成员只调用一次既有 provider adapter/capture；随后使用既有 `_apply` 与本地 materialization child。`has_more` 只产生单页 yield，不额外购买下一页。重复成员、raw 待物化或丢失 owner 不可重买。
- 返回 effective starts、dispatch terminal、完整 transport receipt、raw id、本地入库结果及全 scan complete，各语义分开；固定 `qualified=False`，不把 20 页 fixture 宣称为 operation 资格通过。
- 传输失败保留原 rank、原 cursor 和 quarantine；业务 HTTP 402 保留完整错误 raw，并阻断下一成员。物化失败保留 raw/child，可走已有零费用恢复链。

验证与修复：

- Executor + 诊断边界 + 既有 TikHub scan/local/reconcile materialization 联测 **71/71** 通过（14.056 秒）；随后修复无 Content-Length 返回口径并补测试，executor **8/8** 通过。分组有重叠，不累加。
- 20 个固定成员经过真实 parser、capture、数据库和 materializer，仅 HTTP 字节使用 fixture：20 次发送、20 条视频、20 个内容的四项互动字段匹配，原始字节逐页 readback，全部 source attempt 正确收尾；测试账本 USD 0.020，**真实 provider 调用 0、真实费用 USD 0**。
- 首测物化未通过的根因为 fixture 将抓取时间固定在 9 月 6 日，而 metrics recording clock 未固定；统一 fixture 时钟，保留“不允许抓取晚于入库”的生产约束。另修正测试 raw Path 类型及既有最终 `live_applied` 状态断言。
- 已修复代码问题：旧 manifest 必须在发送前验证；非规范 cursor 在购买前拒绝；完整业务失败从 dispatch 回读错误 raw id；完整回执区分无 Content-Length、gzip 校验和路由拒绝状态。
- 改动源/测试 Ruff、生产模块 Mypy 通过。尝试将源与测试同次送入 Mypy 遇到仓库 `v8`/`dcar_eval.v8` 双模块名冲突；改用项目既有生产模块定向命令通过，没有添加类型忽略或改弱测试。最终全仓验证尚未执行。
- 未改 DDL/配置/依赖/业务页面，未修改正式库或重启服务。

### Step 2C-6：自然轮次准备与固定整批协调

代码完成，随本节提交。新增 `transport_preparation.py`、`transport_runner.py` 及 9 项测试；没有新增启动入口、HTTP endpoint、自动任务或放量动作。

- 准备器必须持实际 writer lease、原 operator owner 和 paused scheduler。新建轮次严格采用现有调度的“每个 registration 最近一个已到期 slot”，不补造过去每小时轮次；已有自然 parent 保留首次冻结的名册/链接，实际领取仅为当前启用账号与冻结名册的交集。
- 枚举完整自然库存，不传入手选 20 个账号，不执行 UID 查询或 provider 调用。仅使用已有精确账号引用；终态、未来、历史、待本地物化和引用缺失明确排除；遇其他 live owner 整个准备事务回滚。
- 准备和签发完整 20-rank batch 在同一事务内完成，第一笔发送前全部 member 已持久化。固定次序逐页执行；失败不替换、不重置分母。收到余额等硬阻断后停止，保留全部 20 个冻结成员及实际已执行前缀。
- 所有本批取得、但尚未结束的 source/parent attempts 都按原 owner 收尾为 `partial`，不伪造 scan complete；operator 的 `succeeded` 只表示命令已收尾，结果另有 `sample_complete`、unknown 数量和 `qualified=False`。异常中止不自动换 owner 或重建成员。
- 整批结果写入不可变 `campaign_terminal` 收据及私有 mirror。费用由真实 usage 账本汇总，unknown 保留保守全额、数量单列；终态重复调用只回读原收据，不新增尝试或发送。最终资格与严格 seal tail 尚未接入，不能恢复普通付费。

验证与修复：

- Preparation/runner/member/enumerator/executor 联测 **28/28** 通过（10.529 秒），含新增 9 项。准备器典型 fixture 为 3 个真实到期 registration × 20 账号 = 60 个候选，再冻结前 20 个；其余 owner 正确收尾，无遗留 running attempt。
- 整批测试覆盖正常 20 页、首 rank 截断保留 unknown 并继续固定样本、402 后只发送 1 次且所有 owner 收尾、样本不足时全部准备回滚，以及终态重复调用零写入。
- 修复已确认的冻结名册漂移：最初对已有 parent 错用当前 enabled ID 集重建，现按现有 pipeline 语义保留首次 eligible IDs；新增启用账号不能挤入旧轮次，已禁用账号不重领，旧 links 不变。
- 4 个改动 Python 文件 Ruff、2 个生产模块隔离缓存 Mypy、`git diff --check` 通过。并行类型检查曾触发共享 Mypy 缓存内部 AssertionError，隔离缓存重跑通过，没有修改类型规则。
- 全部请求都是临时测试环境的 HTTP 字节 fixture；账本金额仅为模拟值。真实 provider 调用及真实费用仍为 **0 / USD 0**，正式库、服务、DDL/配置/依赖未触碰。

### Step 2C-7：诊断历史证据与完整 START 水位

新增只读 `transport_evidence.py`：逐成员核验不可变 campaign/member、历史 START 谱系、唯一 dispatch、usage/slot/attempt、规范 paid identity、真实 O_EXCL send claim，以及完整 raw 或截断 quarantine 的文件/hash/字节数。核验的是发送当时的许可窗口，不要求已结束的 owner 再次 running，也不因读取时许可过期而抹掉历史证据。该 reader 不授予发送、SEAL、RELEASE 或 qualification。

- 完整成功和完整 HTTP 错误分别保留事实与费用；`billing_unknown` 即使 mutable usage state 被改写也不视为已结算。未发送成员必须为零 attempts/费用且无发送 marker。
- `paid_drain.py` 同时识别真实 budget-v3 和原 v2 的在途请求；补齐 TikHub works/accounts parent 与 diagnostic operator。START 新冻结 raw、dispatch event、scheduler attempt 的真实 MAX(id)，覆盖旧 run 在 START 后新开 attempt 的情况；schema18 缺少 dispatch 表时该水位为零。既有 receipt 不回写，未改 DDL。
- 本步仍保留 `_verify_sealable` 原严格拒绝；新增 reader 尚未接成尾部豁免，不能误开普通流量。

验证与修复：

- Evidence 10 项测试通过；与 baseline、executor、runner、ordinary drain/dispatch 联测 **41/41** 通过（12.767 秒）。覆盖过期历史回读、完整 402、截断 unknown、防篡改、未发送、账务金额不匹配及只读零写入。后补 scheduler attempt 水位另由 baseline 5 项复测。
- 首轮发现 raw 加载对象的属性层级错误及 `fetch_attempts.completed_at` 不存在；按现有 `receipt.stored_*` 和 `response_finished_at` 修正，保留全部断言，重跑通过。另对 canonical request 重算 paid/execution hash，核对整条 dispatch 的资源身份不漂移。
- 4 文件 Ruff、2 生产模块隔离缓存 Mypy、`git diff --check` 通过。
- 仅对正式库执行一次 `sqlite3 -readonly` 查询（`PRAGMA query_only=ON`）：schema19、最新 drain 事件 id9/type release、activation2、2026-09-05T03:03:55Z。这仅证明当次账本没有当前 HOLD START，**不代表服务健康或允许发送**。没有正式库写入、服务操作或 provider 调用，真实费用 **USD 0**。

下一步：保守 `charged_unverified` 结算、原 owner/本地物化终态及精确 tail 集合核验；再接严格 SEAL 和 operation qualification。

### Step 2C-8：保守结算、任务收尾与严格 HOLD 尾部

新增 `transport_accounting.py`、`transport_owner_evidence.py`、`transport_tail.py`，接入既有 `_verify_sealable` 的 current-HOLD 分支。普通 drain 规则保留；没有 DDL、依赖、UI、自动任务或生产启动变更。

- 诊断 unknown 仅在实际 writer、paused scheduler、当前未封存 START 和原 campaign 收尾后，追加不可变 `accounting_terminal` 收据及 mirror，并将 usage 标为 `charged_unverified`。原 amount、billed reservation、fetch attempt 的未知状态、批次费用、slot/paid identity guard 全部保留，不伪装供应商 billed、不发补偿授权。预算摘要单列未核实数量和金额，仍占用原预算。
- 读取结算时重新核对 member/terminal/raw 或 quarantine/O_EXCL 证据、原 ledger snapshot 及 mirror；仅改 mutable state 不能通过。重复调用回读同一收据，无新费用/记录；证据写入失败回滚本次事务。
- owner 核验涵盖 operator、全部自然 parent/source（含未选中库存）、本地 materializer 的原身份与终态，以及成功页的 manifest、raw、eligible 内容。`has_more` 可保持 partial，完整 HTTP 错误或截断可收尾，但不虚构完整采集或 qualification。
- current-HOLD 尾部以 START 水位后的实际 usage、fetch attempt、raw、dispatch、paid run/attempt、materialization run/attempt **逐集合相等**为门，不按诊断标签豁免。零费用派生 detail/metrics 也必须有同页来源、内容/operation/slot、hash、零费 attempt 和明确物化终态；未知/孤立/在途记录均拒绝。
- 原 frozen 在途若结束为 `billing_unknown`，current-HOLD 不再仅因网络终态就接受。成功、完整错误、hash-verified partial/zero-body 诊断只能在 accounted terminal 后进入尾部证明；此证明不是 route/operation qualification，不能单独执行业务放量。

发现问题及修复：

1. 跨秒物化时 child 的既有 `completed_at` 是传入 slice 的逻辑时间，不能用它约束真实 derived fetch 完成时间。改用已验证 source 链的最终真实完成时间，起点为 child 链最早开始；递增时钟集成测试通过。
2. 初版集合遗漏本地 materialization run/attempt，可能漏掉孤立 running owner；现独立计集合，不把本地任务误标付费。新建孤立 child、START 前旧 child 的新 attempt 均阻断。
3. 初版 owner 核验拒绝所有后续 attempt，导致现有零费用本地恢复完成后仍不能 seal。现只接受同页 pending/raw/manifest、progress-v1、无新增 usage/dispatch 的完整 source/child 续接链；保留原付费 attempt 和原 campaign `materialized=false`。真实临时库复现失败→本地恢复成功，付费表逐行不变、HTTP fixture 仍20次，exact paid attempts 65 / materialization attempts 21；无 child 进展的额外 attempt、付费 suffix 仍拒绝。
4. 测试 fixture 原先在 START 之后插入时间戳较早的“历史” raw，会违反真实 ID 水位。调整为通过 pre-HOLD hook 真正先插历史行，未改生产条件或降低断言。修复测试入口名称/事务设置及新 list 类型标注。
5. 原始文件相对路径按 capture 的 PROJECT_ROOT 解析，并进一步核对 transport storage receipt 与 DB 的 raw id/path/hash/size，避免依赖调用进程 cwd。

验证：

- 最终 accounting/owner/tail/evidence/baseline/receipt 定向联测 **44/44** 通过（56.385 秒）。
- 既有预算与人工 billing reconciliation 联测 **105/105** 通过；HOLD/ordinary drain/hold binding/cohort/campaign **51/51** 通过；跨秒及本地集合修复后的 tail+HOLD/ordinary **40/40** 通过；后续 local recovery+owner/tail **16/16** 通过。分组重叠，不累加为唯一测试数，未重复跑全仓回归。
- 14 个改动 Python 文件 Ruff、7 个生产模块 Mypy 通过；最后补 list annotation 后，owner/tail 定向 Mypy 复测通过。`git diff --check` 通过。
- 全部为临时库、真实 parser/materialization、HTTP 字节 fixture；不是线上端到端。本步正式库读取/写入及 provider 请求均为0，真实费用 **USD 0**。

并行运行边界：认证任务已告知 4174 使用 `runtime/web/build-20260905-P6JAuV` 固定快照、4173 认证独立。本任务未验证或重启这两个服务，不覆盖该快照；候选 API/Writer 后续安装须单独同步 8766 上游状态。不能把前端可用当成业务数据已恢复。

下一步：固定 primary 20 页的 route 判定、后续 control arms / 每 operation 200 个自然 starts 的 qualification 及显式 writer operator 入口；仍不能启动普通付费或宣称 Phase1 完成。

### Step 2C-9：固定 primary 样本判定与范围收敛

新增 `transport_verdict.py` 及 5 项定向测试，收据种类增加 `route_verdict`。按原 20 个成员、完整响应、本地物化和保守入账生成 passed / incomplete / failed，不改变失败分母、不等同 operation qualification、不开放普通付费。重复调用回读同一不可变结果。

验证：`DCAR_TEST_DENY_FORMAL_DB=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:src/dcar_eval:. /Users/mark/Projects/DcarAIGC/.venv/bin/python -m unittest tests.test_v8_transport_verdict`，**5/5 通过（10.141 秒）**；新增/改动三个文件 Ruff 通过。没有生产写入或真实请求，费用 USD 0。

响应用户的范围收敛要求：暂停扩展通用控制组/资格框架，先复用既有 Writer 命令队列完成显式 primary 入口，随后验证最小真实链路。已有定向结果不重复跑；全仓回归留到最终交付一次集中执行。普通付费门和既定失败分母保持不变。

运行边界复核：8766 当前为另一任务的独立只读副本 API，不是 Writer；不能因有监听端口就宣称抓取恢复，也不直接停止或覆盖它。

### Step 2C-10：复用 Writer 队列接通 primary 执行

仅修改 `api.py`、`profile_control.py`、`transport_runner.py` 和 Writer wrapper，增加两个定向测试文件；不引入新服务、DDL、普通付费放量或页面变化。

- 既有 durable command API 增加 `transport_primary`，唯一用户参数是当前 `drain_id`。路由、成员、费用、raw 路径和 scheduler 均不可由请求覆盖。
- Writer 队列在真实 writer lease、真实 paused scheduler 下冻结 cohort/campaign，事务内创建并保存唯一原 operator claim，再调用已有 runner。新命令不能接管已有 campaign；同命令重复提交只读已完成结果，不再次购买。
- 执行后自动保守结算本批 unknown 并记录 route verdict；命令执行完成与路线通过明确分开。即使 route passed，`ordinary_paid_authorized=false`，不解除 HOLD。
- `DCAR_SCHEDULER_START_PAUSED=1` 只允许 writable scheduler 模式，启动注册但暂停的调度器，禁用 startup catchup。默认启动仍为原来的 `scheduler.start()`，不改变现有默认行为。

验证（本步只跑相关检查，未跑全仓回归）：

1. `DCAR_TEST_DENY_FORMAL_DB=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:src/dcar_eval:. /Users/mark/Projects/DcarAIGC/.venv/bin/python -m unittest tests.test_v8_transport_operator_command`：**4/4 通过，4.802 秒**，覆盖实际 parser/临时库物化、重复购买防护、unknown 保守结算、缺失/运行中 scheduler 拒绝、请求越界拒绝。
2. `PYTHONPATH=src/dcar_eval .venv/bin/python -m unittest tests.test_v8_transport_operator_api`：**4/4 通过**；既有 `tests.test_v8_api.ApiStartupSafetyTest.test_scheduler_installs_reconcile_and_reports_status`：**1/1 通过**。
3. 与第1项同环境运行 `tests.test_v8_current_activation_hold.CurrentActivationHoldTest.test_restarted_release_does_not_replay_first_claim_time`：**1/1 通过，0.073 秒**。
4. 修改 Python 源码/测试的 Ruff、三个生产模块独立缓存 Mypy、`bash -n deploy/macos/run_writer_worker.sh`、`git diff --check` 均通过。

问题处理：沿用既有队列 claim 会与 runner 的 DurableClaim 不匹配，适配器现创建并持久绑定原 operator，未新增并行调度框架。显式暂停与默认启动分支独立，既有默认启动断言原样通过。验证期间未发现未修复的本步代码失败。

真实环境仅只读核验：8766 PID 4762 是独立 `read_only_replica`；正式库 schema19，最近 drain 仍为事件9（release、activation2），无本轮诊断 START。安装 Writer 仍指向原项目及旧 `sealed-evidence-v5` 构建，不能把候选测试通过当作已部署。正式库写入、付费调用均为0，本步真实费用 **USD 0**。

下一闭环只做候选安装与固定 primary 实跑。先保留另一任务的只读 API/Web 与原工作树修改，沿既有安装/冻结合同切换 Writer；不要在未安装候选、未建立当前 HOLD 时直接启动旧普通调度器。控制组/200次资格扩展仍暂停，只有真实主组结果决定是否需要继续。

### 安装门禁检查：真实失败保留，未安装候选

本轮只启动了一次全量后端检查，后续只重跑失败组，未重复全仓回归。检查基线为 `3576313`。

| 检查 | 实际结果 |
| --- | --- |
| `DCAR_TEST_DENY_FORMAL_DB=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:src/dcar_eval:. .venv/bin/python -m unittest discover -s tests` | 2,895 项，598.301 秒，exit 1；30 failures、54 errors、1 skipped（原有跳过，未新增跳过） |
| `npm test`（app/web，包含完整 build） | 构建通过，77/77 测试通过，exit 0 |
| `npm run lint`（app/web） | 通过 |
| `npm exec --yes --package=node@22.13.1 -- node ./node_modules/typescript/bin/tsc --noEmit` | 通过 |
| `ruff check src tests scripts deploy` | 通过 |
| `MYPYPATH=src/dcar_eval mypy --cache-dir <private> --follow-imports=silent src/dcar_eval/v8` | 既有 v8 生产门禁，105 个模块通过 |
| 额外 `mypy --explicit-package-bases src/dcar_eval scripts deploy` | **未通过**，101 errors / 16 files；包括旧工具类型债和动态导入路径，不算通过，不新增 ignore、不顺带改旧工具 |

全量后端原始日志：`/tmp/dcar-backend-install-check.COuWx5/unittest-discover.log`。构建/前端/静态检查执行摘要保存在 `/Users/mark/Library/Application Support/DcarAIGC/rollouts/video-capture-v25-install.x3zUg6/`；文件明确为已完成命令的摘要而非原始转录。该目录**没有通过的 backend 收据、没有 sealed build、不是发布成功记录**。

已修复并验证：

1. `tests/test_prepare_v9_freeze.py` 的全局 Thread mock 把新增 Writer 控制线程计入 catchup，导致 3 条失败。现只 mock 名为 `dcar-startup-catchup` 的线程，其他线程正常创建；原启动次数/开关断言全部保留。`tests.test_prepare_v9_freeze.ApiLifespanSwitchTest` **4/4 通过（0.293 秒）**，Ruff 通过。
2. 独立工作树缺少 Git 不跟踪的冻结历史样本、历史报告、legacy DB 副本及已配置的 gmssl 离线依赖。用 APFS 独立克隆补齐测试所需旧目录，没有把测试 data 链接到正式 data，没有复制 v8 的 246 GiB 运行缓存，没有改正式库或源文件。原测试生成的0字节 `app/data/web_mvp.sqlite3` 保留为 `app/data/web_mvp.empty-after-initial-tests.sqlite3`，可恢复。未改依赖声明或锁文件。
3. 仅重跑缺资源的12个模块：`test_build_rnote_three_proposition_report`、`test_collect_douyin_by_uid`、`test_rebuild_channel_evaluation_v4`、`test_restructure_channel_report_v5`、`test_restructure_channel_report_v6_tikhub`、`test_v7_contract`、`test_v8_migration`、`test_v8_migration_baseline`、`test_web_api`、`test_workflow_corpus`、`test_workflow_evaluation`、`test_workflow_reporting`，**68/68 通过（5.958 秒）**。环境与上表后端命令一致，不删除测试、不降低断言。

当前未关闭的发布阻断：

- 原始失败中有 **64 条错误/失败记录（含 subtest，不等同64个独立问题）**来自 `test_run_paid_source_refresh_canary`。单项复现确认是 `provider_budget.py` 的 `Historical spend requires a consumed sequence-1 authorization`。
- `run_paid_source_refresh_canary.py` 已明确退役 live HTTP，仅保留四个注入 transport 的离线历史测试；其 `_usage_policy_evidence` 仍调用当前 v3 的 `check_reservation`，而 fixture 与精确 scope 断言仍基于 v2。不能通过伪造 sequence-1 前件、改 purpose、加测试环境豁免或恢复生产 v2 预算来修复。
- 本轮没有改该脚本、预算门禁或这些失败断言；没有把后端检查记为通过。下一步应仅迁移该离线测试合同到现行 v3 授权语义，保留费用、防重购、崩溃窗口与零网络恢复断言，不再扩展生产抓取功能。完成前不能封存通过的构建或安装 Writer。

运行状态：8766 PID 4762 仍为原只读副本；已告知并行认证任务本轮不切换它。没有改 LaunchAgent、凭据、正式数据库或生产运行目录，没有停任何现有服务，没有执行真实视频抓取。**真实端到端未完成，新增真实抓取费用 USD 0；本任务尚未完成交付。**

### 旧离线 canary v3 合同闭环（2026-09-05）

当前后端阻断已定向修复，尚不等于全仓重新通过或已部署。

- 仅修改 `scripts/run_paid_source_refresh_canary.py`、对应测试及新增 `tests/paid_compensation_fixture.py`。生产预算、capture、DDL、配置和服务未变。
- 临时 schema18 fixture 明确构造合成的原始未知请求，经既有 preview/reconcile 隔离 API 结算，再走真实 gap/authorize；实际 claim 消费唯一 sequence-1 授权。原请求 due bucket 不依赖含授权的 DB hash，避免循环身份；task/budget 仍绑定最终源 hash。
- 基线六张 allowed 表的断言不变；scheduler 只允许与新 usage 原子对应的一条 consumption，内存复制只读基线后调用现行 v3 consume/check_reservation 逐字段重放，所有旧行仍逐行不变。允许合法 consumed_at≤reserved_at≤sent_at，不要求同秒。
- 保留零网络恢复、SIGKILL、费用、一次发送、旧文件/行/sequence 守恒断言；没有恢复 v2、改 purpose、增加测试环境预算豁免或伪造线上结算。

验证结果：

1. 原模块 `python -m unittest tests.test_run_paid_source_refresh_canary`：**133/133 通过，34.573秒**；原始日志 `/tmp/dcar-canary-v3.hwyGiF`。
2. 新增三个 v3 专项：授权消费与原行守恒、额外 scheduler 行拒绝、已消费授权在 fresh clone 中禁止再买：**3/3通过，0.798秒**。
3. 三项新测试与 `tests.test_v8_provider_budget tests.test_v8_billing_reconciliation` 联测共86项，唯一失败是新测试插入意外 scheduler 行时漏填 started_at，未进入待验证分支；补全 schema 必填时间后上述3项重新通过。其余83项通过，不将这次86项命令记录为全绿。
4. 修改的三个 Python 文件 Ruff、`git diff --check` 通过。

测试基线先遇到非法 stage CHECK 和 live-reader 的 schema19 门；现使用合法 detail stage、隔离只读连接及既有结算API，没有改生产schema门。临时账务为合成测试证据，不是供应商结算；真实请求/费用仍0/USD0。

下一步：保留共享项目已提交成果形成集成候选，集中检查后按既有安装合同执行固定 primary 实跑；不扩展通用诊断框架。正式数据库、8766只读服务、认证服务未动。

### 集成检查闭环与运行交接（2026-09-05 23:12 BJT）

- `6c6c4cc` 全量后端：**3,077项，0 failures / 0 errors，1项原有skip，629.210秒，exit0**。原始日志 `/tmp/dcar-v25-integrated-backend.tkyNLC`；未删除测试、修改断言或将skip计作通过。
- 随后纳入其他已交付任务的原样改动：auth schema3 `0990fbf`；媒体 `564b8fa` 对应源文件及已完成的页头统一/三筛选删除，按共享文件原字节保存为 `d597925`，未push。集成候选 `baf6b12` 自动合并，无冲突；只验证实际新增范围，不重复全仓后端。
- 集成后 auth/API/media/paused-operator 九模块 **283/283通过，26.353秒**：`/tmp/dcar-v25-final-delta.p2INaB`。首轮因生成私有日志时把umask077带进测试进程，使原auth导出测试期望0640实际0600；仅将测试进程恢复既有022，日志仍0600，源码和权限断言未改。失败日志 `/tmp/dcar-v25-final-delta.nL624X` 保留。
- `baf6b12` 前端完整build及 **99/99测试**、Lint、Node22 TypeScript、**6/6媒体浏览器测试**通过；四份原始日志在 `/tmp/dcar-merged-frontend.vTBhuU/`。未操作正式4173/4174。
- 全仓Ruff通过（`/tmp/dcar-v25-final-static.UbfYiY/ruff.log`）。按实际运行包名 `MYPYPATH=src/dcar_eval mypy --follow-imports=silent -p v8 -p dcar_auth` **111模块通过**（`/tmp/dcar-v25-final-mypy.magG3Y`）；初次同时传两个目录遇重复模块名，改用包入口，不增加ignore。旧工具全目录类型债仍不属于本次通过范围。
- 运行状态已漂移：另一任务“恢复正常服务模式”于22:43启动正式Writer PID38805，核心仍旧build，8766已不是只读副本。该任务已明确交接，不再重启/改调度/改activation；当前publisher未加载。认证4173使用独立schema3构建、媒体4174使用独立构建，抓取安装不替换这两个服务，不回退认证库。
- 23:07只调用一次免费的 `.dev /api/v1/tikhub/user/get_user_info`：HTTP200、provider code200、余额USD49.0499。没有视频请求或正式账务/配置写入；这不等同provider抓取、raw物化或端到端通过。
- `mode-b-begin`仍PAUSED；旧`automation`配置不存在，未重建。当前只见内置数据卷，无独立archive卷，后续HOLD_SEAL容量门仍未满足，不伪造容量收据。

下一闭环：将通过的候选以原安装root安装成**可读写、scheduler paused**的8766，建立current HOLD并执行固定primary真实链路。普通paid不提前恢复，纯UI后续增量继续由独立任务发布，不再让本次抓取候选追随页面改动。

### 正式安装与首轮真实诊断（2026-09-05 23:21–23:32 BJT）

- 正式 checkout 已安装 `f93d191`；schema19、activation2 / `tikhub_managed_v1`、正式库原 inode 保持不变。安装前在独占 Writer lease 下创建一致备份；没有替换数据库。8766 可读写、scheduler paused、catchup disabled；4173 schema3 认证与4174独立UI构建未改。TikHub base 按已批准主组改为 `.dev`，凭据内容未输出。
- 原始证据目录：`/Users/mark/Library/Application Support/DcarAIGC/rollouts/video-capture-v25-live.HSO4IQ/`。有效 build 在 `sealed-final`，SHA `c5ddfa311a2cb09ef36f061af126667a84d6f8831a84d4538a9529a1eb65ac72`；此前两次 seal 分别因并行UI修改checkout/runtime被真实拒绝，未绕过检查、未安装失败收据。
- 实际 HTTP `HOLD_BEGIN` run3759成功，drain `video-v25-current-20260905-1`、START event10；五项 prerequisite 取自真实构建、安装回读、11条免费供应商价格元数据和批准预算。`readyz` 的 control_readiness=true、data_readiness=false，普通 paid 保持 draining。
- 实际 HTTP `transport_primary` run3765已终结，但 **route=INCOMPLETE、network starts=0、USD0，不是通过**。固定20个成员全部保留，rank1在 reserve 前被 `provider_circuit_open` 拒绝；未取第21个补位、未重置 campaign 或 paid identity。终态 receipt4089、verdict4090及结果在 `primary-result.json`。
- 根因已由正式只读证据确认：legacy circuit run918 指向 usage89608 / fetch125273，是旧 `douyin_uid_profile` 的 `IncompleteRead`，HTTP NULL、`billing_unknown`；旧全局投影在 diagnostic operation 例外前无条件拒绝。
- 当前最小修复仅对有原 usage+slot/attempt 证据的该 legacy transport 投影允许完整双 permit/member 验证的诊断；独立检查全部 open provider-hard，保留 operation/account/storage/预算及发送前重验。普通请求仍阻断，不关闭 circuit、不改旧 unknown、不退回旧传输栈。
- 修复的四模块定向回归 **89/89，9.755秒通过**（`/tmp/dcar-v25-legacy-transport.Bc2oc9`）；其中新增7项含多组负向子案例，原断言未改。测试证明历史行字节不变、无许可不发送、余额等硬故障不绕过、等待中故障出现仅释放未发送预占。未重复全量回归。

下一步仅作受控 build advance 和新代自然到期主组实跑；旧零发送结果永久保留。完整放量所需独立 archive 卷、全部 operation qualification、schema20/业务日验收仍未完成，不能以首轮执行命令成功替代。

### legacy修复安装闭环与自然到期接续（2026-09-05 23:38 BJT）

- 候选与正式主线代码HEAD均为 `f1f2cf68b01f06d27e12f59f7c2a70836826f102`；这一步只改 `provider_budget.py`、对应诊断预算测试和本执行记录。定向89项通过，Ruff及Mypy通过，日志 `/tmp/dcar-v25-legacy-static.dxjiHs/`。没有重复全量回归或增加ignore。
- 有效新build位于上述证据目录的 `sealed-legacy-fix/`，SHA `fcb2c08abd46ff3ca7b5544613e02385afdd9e33924dd57792c6611d4524cc25`；runtime SHA `22d952a6e42451c6b389d57a9109881a24ebec8c5f35c269d9d8f38fa6f9ad4e`。两次输入门分别因命令填写的完整HEAD不符、定向静态日志权限0644被拒绝；改为实际 `git rev-parse` 结果、将这两份新日志收紧为0600后通过，未改变测试内容或门禁。
- 实际HTTP `hold_build_advance` run4094成功，event4095 / generation2；没有新增activation、release或删除旧诊断。五项新代prerequisite已经按真实安装回读注册；build/runtime/config有效至03:35:38 BJT，price/budget至03:36:53 BJT。注册时只在停止Writer、确认零正式库句柄并持独占Writer lease后执行现有注册函数，无并行写库。
- 最新8766 PID83548：health=ok、read_only=false、writer lock held；loaded_build匹配上述SHA；control_readiness=true、data_readiness=false、paid_dispatch_state=draining。scheduler paused、catchup disabled，4173/4174未操作，publisher未启动。
- 正式只读计数仍为 usage89609、fetch_attempts125273、raw116696、paid_events13770、running_attempts0，与首轮诊断前一致。**本任务本轮新增真实视频network start=0、USD0**，不能宣称真实传输通过。
- 原20个零发送permit也不重签：既有preparation/自然due校验会在下一北京日排除旧日source。下一实际新轮次是 **2026-09-06 02:10 BJT 的 `tikhub_works_scan`**；当前不提前提交空campaign，不新增绕过旧scope的代码。
- 已通过应用创建一次性heartbeat“视频抓取固定样本验收”，02:11 BJT接续本任务；固定command_id `video-v25-primary-20260906-2`，最多20个自然成员/总额USD0.020。执行前核对实际build/generation/权限有效期；已存在命令只读取，不重复购买。明确禁止自动HOLD_RELEASE、切profile、恢复普通scheduler、启动publisher或全量放量。旧 `mode-b-begin` 保持PAUSED。
- 外部阻断：已向用户请求独立持久archive卷挂载路径；未收到前不得生成虚假capacity receipt或开放全量。后续全部operation200样本、schema20演练/发布、业务日+D+1仍未完成，不是仅等这一轮20样本就能宣称交付。

本节为候选工作树中的运行日志增量，暂未提交/同步正式root，以保持已安装build封版不漂移；下一次受控构建再一并保存。

### 按需执行与历史本地回放修复（2026-09-06）

- 用户最新指令“不用等固定时间，有任务及时处理”覆盖此前02:11等待安排。旧一次性heartbeat已PAUSED。仅诊断发现增加 `transport_primary_on_demand`、`source=operator`、`due_kind=on_demand`：以已提交 durable command 的真实attempt开始时间冻结30天业务窗口，保留原cron实现。不是伪造cron时间，不重签原20个permit，也不重用原paid scope；固定20、两项许可、单并发、费用上限和最终发送重验不变。
- `transport_preparation.py`、`transport_natural_due.py`、`transport_owner_evidence.py`完成最小接线；新增4项测试覆盖午夜立即执行、旧/新scope不交叉、伪命令拒绝，以及等网络slot时command owner漂移导致0发送。第一轮测试发现新验证器误要求读取事务，已改为同等run/attempt/token/identity的只读校验；没有改变旧测试。
- 真实执行既有local-only replay时，3个任务2315/2330/3193均被旧 `_owned_scope` 的current epoch门终结为 `profile_superseded`，processed=0；原始证据 `local-replay-20260906-result.json`。provider_usage/fetch_attempts/paid_events全行hash前后相等，新增费用0；不能把这次任务终态称为物化成功。
- `tikhub_scan.py`仅为充分校验的pending raw绑定私有local replay上下文（DB真实路径、原run、scope hash），核对历史epoch/冻结名册/当前账号身份和原raw，不要求旧epoch仍是当前付费epoch；`_raw`明确拒绝该上下文进入发现网络。普通路径仍拒绝旧epoch，disabled账号不恢复。针对被旧门误终结的failed/profile_superseded，在同一事务保留旧attempt不变并追加新的local-only attempt；`pipeline.py`将此类真实pending重新纳入有界回放，其他failed不恢复。
- 合并定向 **90/90，48.034秒，通过**，原始日志 `/tmp/dcar-v25-immediate.TnyOo9`；5个生产模块Mypy和改动文件Ruff通过，日志 `/tmp/dcar-v25-immediate-static.BYkr0E/`。6处Optional类型收窄错误通过等价else分支修复，未增加ignore或降低断言；不重复整仓回归。
- 使用正式库只读backup与原3页raw做隔离副本验证：分别 **20/20、10/10、5/5** 项本地物化成功，remaining=0；paid ledger全行hash完全不变。首轮副本因相对raw路径解析到候选root而失败，已仅在副本中将路径解析到安装root，原库/raw/hash不改；失败日志保留，最终日志 `local-epoch-clone-2.log`。副本路径 `/var/folders/cv/f0j7r6zj0h1dykhg_l8bnl800000gn/T/dcar-local-epoch-clone-qyhfnd6d/clone.sqlite3`。
- 下一动作：安装这一受控构建、BUILD_ADVANCE；先恢复正式库35项本地欠账，再由现有HTTP命令立即执行固定primary。尚未调用真实视频接口，不能提前写route或operation qualified。

### 2026-09-06 上午：修复未收尾的运行步骤

- 用户追问暂停原因后，读取仍可回收的执行结果：主仓已 fast-forward 至 `881fa97152e439b242a79848ea701e942f8e0bb7`，但 `resume_verified_epoch_debt.py` 在真实本地回放后断言失败，后续封版及 Writer 重启未执行。不是等待用户确认，也不是任务完成；昨夜没有持续跑到验收。
- 根因是部署辅助脚本把预期集合写死为原3个run，而 `_replay_materialization_debt` 合法枚举了更早的4个pending。50工作单元上限正常生效：2247/2255/2273/2293/2315分别成功9/1/15/4/20项，2330成功1项后让出，剩9项。初次失败结果及日志原样保留，没有删测试、改断言或覆盖失败收据。
- 只读重新确认正式库pending只剩2330/3193后，新增一次性辅助脚本 `remaining-local-epoch-replay-v1`，使用原 `resume_local_materialization` 分别恢复9/5项；预先精确验证2个run，事后严格验证14项、remaining为空、所有派生attempt零计费、两张付费账本全行hash不变。累计64项完成，本地pending清零，新增provider请求/费用为0。证据：私有部署目录 `remaining-local-epoch-replay-result.json`、`remaining-local-epoch-replay.log`；原失败证据 `verified-local-epoch-replay-result.json` 仍保留。
- 复用既有全量结果及881fa97的90项增量结果完成安装封版，未重复全量回归；`sealed-immediate` build SHA为 `91e16527a6f13a4d1439cd8192973c0da581333493e0c1f11c42650ad2cfff28`，runtime SHA为 `ecbed1e68a041db6d6f96ddf40e3d6128eb53de51997a7c52e569da70e8ab66a`。仅更新外部LaunchAgent的receipt路径并恢复8766，scheduler仍paused、catchup disabled。实际 `/api/v8/health` 为ok、read_only=false、Writer锁held，PID61835；`/api/v8/readyz` loaded_build与封版一致，control_readiness=true、data_readiness=false，不能宣称业务数据完整。
- BUILD_ADVANCE前的正式库只读校验复现另一项阻断：`transport_tail._derived_evidence` 只认可当前诊断campaign的source raw，因此拒绝历史raw的合法本地回放，错误为 `Derived raw has no verified diagnostic source`。尚未提交新BUILD_ADVANCE/primary，也没有付费调用。正在候选工作树修复这一最小兼容缺口，不修改HWM、不删历史attempt、不白名单任意零金额行。
- 只读候选校验进一步定位到历史逻辑时间与真实响应时间的差异：派生raw116702 / attempt125279真实完成于16:15:00Z，原source显式`now`固定的`completed_at`为16:14:58Z。不能把后者当成真实写入上界或重写历史时间。历史回放专用证明使用严格原始raw/冻结manifest/全部终态owner/progress/slot/零付费及exact全集，实际响应时间不得超过本次验证时刻；原诊断member的完成时间门不改。这些历史时间不作为传输延迟或新鲜度资格的证据。
- 此兼容修复只改`transport_tail.py`并新增历史回放测试。6项新测试+8项原tail测试全部通过（14/14、18.624秒）；Ruff/Mypy通过，原始日志`/tmp/dcar-historical-tail.kbtMfw/{tests,ruff,mypy}.log`。测试覆盖误终结后两段恢复、来源/manifest篡改、无依据的零金额usage及local attempt、实际完成晚于逻辑时间、实际完成晚于验证时刻拒绝；未改原测试断言。
- 正式库只读最终验证于2026-09-06 09:47:02 BJT通过：`total_changes=0`，26条新增派生raw与26条免费fetch attempt精确核对，新增usage=0、dispatch=0；原301个诊断预备run与313个post-HWM source attempt、8个本地物化attempt完整归属。`qualified=false`保持不变。证据`historical-tail-readonly-2.log`，第1次失败日志保留；仅候选代码解析相对raw时指向安装root，没有改DB路径、证据字节或hash。下一步安装此修复并推进generation3，再发最多20个真实受控样本。
- 修复提交`a0b6a518314ade1e76a7dbfdd3e29fb010d9b4e0`已fast-forward到正式root，工作树干净。`sealed-local-tail` build SHA `f888b396a23a23d4c95d133438084f7ee79c74c11bebbb26e0ef39d857946f50`、runtime SHA `ed3aa478090d5075572de955f95efb9c0a3e110b5cd6b7db0155b96724f22595`，复用已通过的历史全量结果和本轮14项/只读结果，未重复全量。启动回读health=ok、Writer锁held、loaded_build准确；新prerequisite注册期间增加EXIT恢复Writer兜底，避免再次因辅助步骤失败停在未重启状态。
- 实际HTTP BUILD_ADVANCE命令4101成功，generation3；config SHA `1fa8d627231d155d4fec1e53946be0d92f17f83e92a4f4e6ec0fdbb95a29055f`，build/runtime/config有效至13:51:38 BJT，price/budget有效至13:53:19 BJT。没有生成新activation或HOLD_RELEASE。
- 已提交且仅提交一次`video-v25-primary-on-demand-20260906-3`（run4105，campaign4107），固定20个原始成员、USD0.020上限。只读观察进程仅GET该命令并核对账本，不会重发、补位、切换路由或开启普通调度；终态写`local-tail-primary-terminal.json`。运行结果待终态后追加，预占金额不提前写成实付费用。
- `video-v25-primary-on-demand-20260906-3`已终态成功完成命令本身，但route verdict失败：20个effective starts、17个usable pages、19个complete responses、1个transport uncertain / charged_unverified，保守入账18,000 microusd（USD0.018）；ordinary paid仍未授权，operation未qualified，verdict给出的唯一下一步为`run_disjoint_control_arms`。证据：正式库receipt 4447/4449、`local-tail-primary-terminal.json`。
- 候选分支补齐固定控制臂实现，允许在BUILD_ADVANCE后的当前generation执行`transport_control`并引用同一HOLD lineage的上一代failed primary verdict；控制臂限定为`control_io=https://api.tikhub.io + urllib-stream-v1`与`control_legacy=https://api.tikhub.dev + urllib-legacy-v1`，样本只取未使用/未permit的自然到期scope，不补位、不解锁ordinary paid、不自动fallback。新增/更新61项transport相关测试全部通过，Ruff通过，Mypy通过。

## 可复现验证命令（历史及本轮）

在上述实施工作树执行，Python 使用现有受管环境；测试显式禁止正式库。

```sh
export DCAR_TEST_DENY_FORMAL_DB=1
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH=src:src/dcar_eval:.
export MYPYPATH=src/dcar_eval
DCAR_AUDIT_PYTHON=/Users/mark/Projects/DcarAIGC/.venv/bin/python

"$DCAR_AUDIT_PYTHON" -m unittest tests.test_v8_fault_recovery tests.test_v8_provider_budget tests.test_v8_capture tests.test_v8_provider_recovery tests.test_v8_billing_reconciliation
"$DCAR_AUDIT_PYTHON" -m unittest tests.test_v8_pipeline tests.test_v8_scheduler tests.test_v8_api tests.test_v8_runtime_receipts tests.test_v8_reconcile_control
"$DCAR_AUDIT_PYTHON" -m unittest tests.test_v8_tikhub_scan tests.test_v8_local_materialization tests.test_v8_reconcile_materialization
"$DCAR_AUDIT_PYTHON" -m unittest tests.test_v8_pipeline tests.test_v8_pipeline_work_readiness tests.test_v8_queue_backlog tests.test_v8_work_readiness tests.test_v8_provider_updates tests.test_v8_pipeline_media_lifecycle tests.test_v8_reconcile_materialization tests.test_v8_api tests.test_v8_scheduler tests.test_v8_runtime_receipts
"$DCAR_AUDIT_PYTHON" -m unittest tests.test_v8_provider_local_only tests.test_v8_comment_paging_live
"$DCAR_AUDIT_PYTHON" -m unittest tests.test_v8_request_transport_binding tests.test_v8_transport_hold_binding
"$DCAR_AUDIT_PYTHON" -m unittest tests.test_v8_transport_receipts tests.test_v8_transport_cohort tests.test_v8_transport_campaign tests.test_v8_transport_natural_due tests.test_v8_transport_hold_binding tests.test_v8_request_transport_binding
"$DCAR_AUDIT_PYTHON" -m unittest tests.test_v8_transport_members tests.test_v8_transport_due_candidates tests.test_v8_transport_natural_due tests.test_v8_diagnostic_writer_lease tests.test_v8_runtime_database tests.test_v8_transport_receipts tests.test_v8_transport_campaign tests.test_v8_transport_hold_binding tests.test_v8_paid_dispatch
"$DCAR_AUDIT_PYTHON" -m unittest tests.test_v8_diagnostic_budget_boundary tests.test_v8_diagnostic_capture_boundary tests.test_v8_provider_budget tests.test_v8_paid_dispatch tests.test_v8_transport_hold_binding tests.test_v8_transport_members tests.test_v8_capture tests.test_v8_paid_drain
"$DCAR_AUDIT_PYTHON" -m unittest tests.test_v8_transport_execution tests.test_v8_diagnostic_capture_boundary tests.test_v8_diagnostic_budget_boundary tests.test_v8_tikhub_scan tests.test_v8_local_materialization tests.test_v8_reconcile_materialization
"$DCAR_AUDIT_PYTHON" -m unittest tests.test_v8_transport_runner tests.test_v8_transport_preparation tests.test_v8_transport_members tests.test_v8_transport_due_candidates tests.test_v8_transport_execution
"$DCAR_AUDIT_PYTHON" -m unittest tests.test_v8_transport_evidence tests.test_v8_diagnostic_drain_baseline tests.test_v8_transport_execution tests.test_v8_transport_runner tests.test_v8_paid_drain tests.test_v8_paid_dispatch
"$DCAR_AUDIT_PYTHON" -m unittest tests.test_v8_transport_accounting tests.test_v8_transport_owner_evidence tests.test_v8_transport_tail tests.test_v8_transport_evidence tests.test_v8_diagnostic_drain_baseline tests.test_v8_transport_receipts
"$DCAR_AUDIT_PYTHON" -m unittest tests.test_v8_transport_tail tests.test_v8_current_activation_hold tests.test_v8_paid_drain tests.test_v8_transport_hold_binding tests.test_v8_transport_cohort tests.test_v8_transport_campaign

/Users/mark/Projects/DcarAIGC/.venv/bin/ruff check src/dcar_eval/v8/pipeline.py src/dcar_eval/v8/tikhub_scan.py src/dcar_eval/v8/reconcile_control.py src/dcar_eval/v8/runtime_receipts.py
/Users/mark/Projects/DcarAIGC/.venv/bin/mypy --follow-imports=silent src/dcar_eval/v8/pipeline.py src/dcar_eval/v8/tikhub_scan.py src/dcar_eval/v8/reconcile_control.py src/dcar_eval/v8/runtime_receipts.py
/Users/mark/Projects/DcarAIGC/.venv/bin/mypy --follow-imports=silent src/dcar_eval/v8/pipeline.py src/dcar_eval/v8/provider_updates.py src/dcar_eval/v8/providers.py src/dcar_eval/v8/work_readiness.py
git diff --check
```

## 下一步与未完成门

- 当前代码闭环：route verdict、显式writer命令入口及legacy transport诊断兼容已安装；current HOLD generation2生效。下一步仅固定primary自然到期实跑，不继续扩展通用qualification模块。普通付费仍关闭。
- Phase 1 尚未完整：安装、HOLD_BEGIN与BUILD_ADVANCE已验证，但transport qualification、HOLD_SEAL/RELEASE、独立归档卷/容量收据及真实受控样本仍未完成，不能启动普通付费流量。
- Schema20 演练、配对发布、shadow、canary、完整业务日与 D+1 尚未执行。
- 安装前全量后端3,077项命令通过（含1项原有skip）；后续集成增量283项、前端99项、媒体浏览器6项、Lint、Web类型及v8/auth 111模块类型门禁通过，详见上述原始日志。全目录探索性 Mypy 仍有历史工具错误；不把生产边界通过夸大为所有旧脚本通过。
- 线上真实端到端、真实传输质量/费用和业务数据完整性验收尚未完成。后续真实请求只能使用已授权环境、预算及合法 permit，不能以 fixture 代替。

本文件是持续执行记录，不是最终完成报告，不解除任何生产安全门。
