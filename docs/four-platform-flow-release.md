# Schema23 四平台主线的本地发布

## 当前 R4：已启用采集时段内的发现补漏

实际前代为已安装 R3（build 文件 SHA `1fd3e82c546ab7534bf2c791f0d131f7ff7c376ed8940ea69886000c8c0244bc`）。
scope=`bounded_forward_discovery_coverage_recovery`。`CODE_ONLY_REPAIR_FILES` 精确限制
本代八文件：`capture_runtime.py`、新 `capture_discovery_recovery.py`、`providers.py`、
`capture_day_coverage.py`、`test_v23_discovery_recovery.py` 和本发布模块、发布范围测试、本文档。

正常发现保留最近 72 小时重复检查。中断时从可验证的完整覆盖位置补齐缺口，
单次自动回补和每日低频复查均以最近 30 天为上限；超出范围、分页未完成和失败
保留待补事实，不冒充完整覆盖。旧成功水位不能直接证明旧时段完整，新规则以
真实覆盖证据为准。作品 ID 继续去重，发现时原指标来源保留。

所有发现窗口进一步限制在有证据的自动采集已启用时段。新账号启用之前和暂停
期间不默认扩入；补齐既有 forward discovery 覆盖缺口并不重新授权完整历史补采。
本代沿用 `local_writer_forward_flow_only` 和 `historical_backfill_authorized=false`，
新用户原始指令、本代源码和新 scope 一并封存，操作集合、价格、预算、账本、
已安装 schema23 及原 v3 索引保持原样。冻结从 R3 source 复制，只覆盖本代八文件，
不能将工作区的其他未提交修改带入正式 Writer。

五项检查绑定最终冻结源码：`flow_execution` 运行新的窗口/恢复/已启用时段过滤及
既有滚动调度定向测试；`flow_metrics_reports` 重跑发现携带指标、指标补充路由
和按已启用区间判定日覆盖的用例，保护旧报告和指标配置。`flow_schema`、`flow_frontend` 仅在前代实际检查
回执、输出 hash 和全部相关依赖一致时继承。`flow_release` 运行当前精确范围反例
和真实冻结 R3 → R4 临时库继承测试，验证原 migration/index、正式权限范围和
无数据库写入；不得手工造 passed 或修改旧冻结验证器。

准备仍用原 schema22 `--parent-build`、原 cleanup `--parent-install`、首次
schema23 `--migration`，指定实际 R3 `--code-predecessor-build` 和原 v3
`--inherited-index-install`；不运行 migration/index 安装。通过当前 installer
和已封存 `run_capture_repair.py` 协调器安装，采用 `publish_probe`、`works=[]`，
只重新绑定当时仍 open 的原 operation。该阶段供应商发送必须为零；协调器负责
唯一 Writer 排空、安装、新入口签回并恢复普通服务，失败时恢复前代与许可。
正常服务恢复后分别验证来源 hash、DB inode、Writer 锁、调度状态和实际新发现
窗口，代码检查通过本身不表示业务覆盖已经补齐。

以下 R3 及更早章节保留为历史，不作为本次固定样本重试或历史补采指令。

## 前代 R3：已证实未扣费的固定样本单次重试

实际前代为已安装 R2（build 文件 SHA `a18ffb475c71097f0dc69e1f05fdf6853b6b2ce26489a26935fe855b5fb4a156`）。
scope=`bounded_verified_unbilled_fixed_retry`，精确八文件由 CODE_ONLY_REPAIR_FILES
固定：三个执行模块、两个定向测试及三个发布合同/测试/文档文件。
本轮仅补齐固定 KS 阶段失败后的合法重试承载，不扩大历史采集或日常调度范围。

R2 的该次快手完整 HTTP 400 正文明确表示请求未扣费并可重试，原金额为零。
R3 在验证原始响应、原工作归属与实际未扣费证据后，调用既有 record_settlement
写入真实零金额结算，复用原 paired grant，重构确切固定工作并只发送一次 sequence 1
重试。旧失败、原请求、原 raw 和费用事实保留；正文缺失、费用未知、已收费或
已经消耗重试的情况不能冒充该路径资格。重试与 normal stage 不能在同一执行切片
重复列入，不等待 daily due/FIFO。最终仍经过原 A/B、作者归属、当前许可和真实预算。

provider_budget、usage_settlements、provider_transport、严格 clean EOF 判断、schema、
媒体、指标和报告代码保持 R2 原样。补偿模块只增加此精确工作承载，不改变通用
结算或发放规则。此前成功的其他固定请求及四平台媒体/分析结果保留，不重复采集。

flow_execution 只执行新重试临时库用例及薄入口受影响用例，核验实际运行前后 SHA；
未变的 R2 claim/bootstrap 结果按依赖字节一致性继承，不重跑整组。flow_schema、
flow_metrics_reports、flow_frontend 仅在各依赖文件和前代回执/输出 hash 都可验证时
继承。flow_release 新跑四项精确范围反例，同时比较全部发布函数 AST 与 R2 相同，
只允许本代 required/scope/allowlist 常量变更，再继承 R2 已执行的真实冻结继承
测试证据；若函数或依赖发生变化则必须重新执行受影响的继承用例。prepare 仍完整
验证 R3 当前 source/build/authorization 和实际前代链，不能拿前代测试替代安装校验。

冻结、检查、prepare 完成后通过既有 installer 和受封存 Writer 入口安装，签回原有
24 项本地 operation 许可（供应商请求为零），仅执行此 KS 追加重试并读回真实结果。
成功即结束；再次失败保留确切失败事实，不开启下一轮重试或追加发布。

## 前代 R2：完整实体恢复和四平台固定样本

实际前代是已安装 R1b，不能用未安装的候选作为前代。
scope=`bounded_content_entity_recovery_and_fixed_sample_execution`。
本轮修复已完整到达但 HTTP framing 失败的正文识别和隔离实体恢复，记录可追溯的
追加恢复证据；旧 transport receipt、失败 disposition、原发送和费用事实保持原样。
快手既有完整实体先做本地恢复，不新增详情请求；小红书无响应正文，修复错误字段后
走真实定向重试。R2 同时修复 `python -m` 与 canonical module 重复加载导致的
ContextVar 分裂，并以新的 fixed helper 执行四平台固定样本。

R1b 已在正式 Writer 内签回 24 个 operation，供应商发送为零；四个精准 probe
因上述上下文分裂在领取前失败。因此 R1b 的发布结果不代表 P0 已通过真库发送验收。
R2 使用同一受封存 shell 和协调器，保持唯一 Writer 锁及原 A/B 真实账本，
不等待日常 FIFO，不扩大到历史补采。固定任务仍核验作品身份、作者归属、当前
operation 权限和实际发送预算；普通队列到期与历史筛选不作为 repair 任务的前置。

本代 CODE_ONLY_REPAIR_FILES 精确列出受影响文件，继承 R1b 其他源码；
CODE_ONLY_REQUIRED_SOURCE 额外要求 recovery 和 fixed helper 存在，同时保留 R1
已封存入口和 timing 文件要求。旧 REQUIRED_SOURCE、CODE_REPAIR_FILES 与前代
验证器保持原义。provider_budget、schema、路由配置、价格、结算、补偿和 profile
路径不在本轮改动范围。

五项发布检查分别绑定最终源码，不能把旧 passed 改名：

- flow_execution：核验已有 transport/recovery 合并测试的真实回执、日志及每个文件
  的运行前后 SHA，再汇入 fixed helper 和 canonical CLI 的实际测试。最终受影响
  业务和测试文件必须全部被对应回执覆盖；已通过且源码未变的测试不重复运行。
- flow_metrics_reports：只跑当前 CSV/SPU/API 读口四项及冻结报告依赖五项，配合
  recovery 测试中的真实指标来源验证，确认新恢复事实可读且旧 v2/v3/v4 合同不变。
  metric_field_facts 已变，不能直接复用 R1b 的整项指标 passed。
- flow_schema、flow_frontend：仅在各依赖文件与 R1b 字节一致、原报告与输出 hash
  可验证时继承；不执行 DDL 或重跑无关前端回归。
- flow_release：运行本代精确范围反例和一次临时库的 frozen R1b → R2 继承验证，
  保留原 migration/index 并检查无数据库写入。不得修改旧冻结验证器使测试通过。

R2 的 transport 源码发生变化，须重新冻结完整 source、build、local-flow
authorization 和当前 operator proof；继承原 schema23 migration、v3 index-install
以及历史根证据，不重冻结账号名单或 source-operation 快照。安装后在已封存入口中
锁外准备一次 PreparedInheritance，为原有 24 个 operation 批量发布并验证当前
decision、continuity permit、readiness/gate 配对；此阶段供应商请求仍为零。
只看 gate 的祖先 build_receipt_sha256 不能判断当前绑定，须使用当前决策链字段。
本地许可全部通过后才进入恢复与固定采样，读回实际 raw、指标、媒体和分析结果；
视频号不支持的可信 VV 不作为采集失败。R2 回滚使用 R1b 已封存 repair 入口签回
原有 24 项许可，保留新增或未知费用、失败及恢复证据，随后恢复普通服务。

## 前代 R1：发送边界证据修复和定向 Writer 入口

实际前代是已安装 S16。scope=`bounded_repair_dispatch_and_paid_boundary_preflight`。
R1 只封存锁外静态证据准备、短事务内当前权限/身份/预算复验、真实发送计时与
已安装 shell 的精确 repair work 入口。协调器读取原 plist 环境，在普通 Writer
停止后运行同一受封存 shell，执行结束或失败均恢复普通 Writer；真正发送仍持有
既有 WRITER 独占锁并经过原 A/B 账本。新入口及 timing 模块由 CODE_ONLY_REQUIRED_SOURCE 强制要求；旧首次迁移的
REQUIRED_SOURCE 保持原义，不能用新文件要求重写旧来源。

CODE_ONLY_REPAIR_FILES 精确列出本轮改动，旧 CODE_REPAIR_FILES 保持原样。
不改 provider_transport.py、tikhub_config.py、路由、价格、结算或 schema；
完整响应读取修复、隔离实体恢复和完整固定采样命令属于后续独立 R2。
继承原 schema23 migration 和 v3 index-install，不重复 DDL。新 source、build 和
local-flow authorization 必须分别绑定最终测试源码，不能把前代 passed 原样改名。
未受影响的结构、指标和前端检查只在其依赖文件一致且原回执 hash 可验证时继承；
发送边界、bootstrap/精准执行和发布继承分别执行必要定向检查。

停机安装后，在真实 Writer 入口中复用同一锁外 PreparedInheritance，为停机前
已开启的 24 个 operation 批量调用既有 publish_operation_gate 并验证新 decision、
readiness 和 gate 配对；供应商请求为零，不提交 24 条排队命令，不扩大原 scope，
也不清除 transport fault。所有当前许可通过后才恢复日常调度。
原 activation 4 仅有 account_cleanup metadata，当前 preparation → four-platform
operator 分支不要求重冻结旧名单或 source-operation snapshot。历史根证据保留，
新执行版本由本代 source/build/authorization proof 验证；业务样本结果另行验收。

R1 回滚到 S16 时，旧 shell 没有 repair 分支，不能调用本轮外置 worker 或手工
注入已加载 build 身份。恢复原 S16 plist 后 bootstrap 原 shell/service，确认原
build 和正式 DB inode，再使用原 Writer 的
`POST /api/v8/internal/current-activation-hold/commands` 提交原 open operation：
`command=capture_release`，`parameters={action:operation_publish,operation:<op>}`。
每个 operation 使用稳定的 `repair-r1-rollback-s16:<run>:<op>` command_id，超时后
查询并复用既有命令，不重复提交新身份。该接口提交后唤醒独立控制 executor，
不要求自然到期；S16 没有批量发布接口，需逐项签发并通过 GET 同路径 `/{run_id}`
读回。这 24 次原控制命令没有供应商请求，亦不清旧失败、未知费用或故障。
R2 回滚到 R1 才能使用 R1 已封存的 repair 入口批量刷新许可。

## 前代 S16：同一工作复用已验证继承证据

S15 实际样本在发送前因 `batch_reservation_expired` 阻断，发送次数为零。根因为领取、预留、发送三个边界分别重复准备同套全链证据，发送前准备时间超过预留时限。
本次仅让同一工作、同一线程、同一数据库内的现有 PreparedInheritance 延续到发送边界；每次事务前后仍核验文件代际和数据库精确读集合，发送前权限、资格、预算、报价和归属检查保持原样。
不延长任何时限，不新增跨任务缓存，不改调度并发、账本或数据库结构。scope=`single_work_inheritance_reuse_through_paid_send`，精确八文件由 CODE_ONLY_REPAIR_FILES 限定。
只验证受影响的同工作生命周期和真实 A/B 预留/发送反例及发布范围；未变的结构、指标、前端继承有效检查，不重复完整回归。


## 前代 S15：领取重校验与单项异常隔离

实际前代为已安装 S14-r2。此次 scope 为 `work_claim_preflight_and_rolling_failure_isolation`。
只将普通单条和统计批次领取阶段的继承重校验移出写锁，保留锁内变化检查、当前权限、身份和预算校验；
滚动执行逐项保留失败并继续原并发与16项上限内补槽。单个工作失败不能把整轮成功状态冒充为完整通过。
不延长租约或媒体确认TTL，不新增并发，不跳过账本，也不改数据库结构。

精确七文件由 CODE_ONLY_REPAIR_FILES 固定：两个运行模块、两个定向测试与三个发布合同/测试/文档文件。
只跑领取边界、原租约及滚动调度相关测试和必要发布合同检查。结构、指标读口及前端源码不变，按文件证据继承有效检查，
不重复全量回归、DDL或历史补采。切换保留当前UI、Auth与8768读服务，只更新Writer和现有helper绑定。
两个之前从未发送且已过期的样本，仅在本次修复安装后通过现有明确确认入口重新发起各一次采集。

以下章节均为历史记录，不构成本次重复执行指令。

## 前代 S14：身份与手动更新审查缺口

实际安装前代为 S13。此次 scope 为 `identity_and_manual_update_review_fixes`，
仅修复抖音 modal_id 链接解析与冲突判断、缺原始 UID 时保留已有账号归属，
以及快手 manual_update 持久表达评论受限并在完成时返回 partial。
不新增评论能力，不变更付款、调度、数据库结构或历史报告。

本代精确九文件：发布模块、本文、发布范围测试；content_identity.py、operations.py、
capture_runtime.py、capture_commands.py；test_v23_identity_audit_regressions.py、
test_v23_manual_update_limitations.py。完整路径由 CODE_ONLY_REPAIR_FILES 固定。
继承 S13 build、原 schema23 迁移与原 v3 索引回执，不重复 DDL。

只执行受影响身份、manual_update、当前 CSV/SPU 读取及发布范围定向回归。
未改前端和其他业务模块按字节一致性复用前代证据，不重跑完整回归。
运行切换保留当前前端、原数据库与既有持久队列；不以额外性能采样作为交付条件。

以下 S13 及更早章节均为历史记录，不作为本次范围或重复执行的指令。

## 前代 S13：CSV 与 SPU 指标口径收敛

本次实际运行前代是已安装 **S12（v12）**，scope 固定为
`current_csv_and_spu_metric_policy`。只修复当前内容 CSV 导出与 SPU 受众统计两个
读取入口，使 schema20–23 使用与当前 API 相同的指标字段事实；schema19 保留明确的
旧 v2 合同。可信有效值下降到零仍为有效值，后续空值不能刷新或覆盖原有效来源；
视频号不可用 VV 在 CSV 留空，也不能放大 SPU 曝光汇总。旧报告和冻结输入继续按原版本重放。

本代 `CODE_ONLY_REPAIR_FILES` 精确收窄为以下六个文件，不沿用 S12 的较宽清单：

- `src/dcar_eval/v8/four_platform_flow_release.py`（发布模块 `MODULE`）
- `docs/four-platform-flow-release.md`
- `tests/test_four_platform_flow_release.py`
- `src/dcar_eval/v8/operations.py`
- `src/dcar_eval/v8/spu_audience.py`
- `tests/test_v23_current_metric_readers.py`

历史 `CODE_REPAIR_FILES`、全部继承验证函数、来源保护和权限逻辑保持原样。
前代必须按自身封存验证器验证；本代只允许上述六文件中的精确差异，其他文件、
删除或权限变化继续拒绝。不得将未安装候选当作前代。

这是 schema23 原库上的纯代码后继：`--code-predecessor-build` 指向实际安装 S12
build，`--inherited-index-install` 继承已安装的原 v3 索引收据。`--parent-build`
仍指原 schema22 祖先，`--migration` 仍用首次 v2 schema23 迁移收据。
不运行迁移或索引安装器，不传新的 `--index-install`，不重复 DDL，不补历史数据，
也不附带规划、媒体、控制队列或性能修复。

冻结后的五组完整检查由发布操作者统一执行；指标组须包含
`tests.test_v23_current_metric_readers`，覆盖 schema20–23 的 CSV、两类 SPU 汇总与
API 一致、有效零值与空值保留、视频号 VV 不可用，以及 schema19 原口径。
发布范围的小测试是 `tests.test_four_platform_flow_release.FourPlatformCodeChainGuardTest`。
代码和临时测试通过不代表服务已切换；实际来源绑定、控制结果与业务验收另记。

下文保留最初迁移及历次后继的历史流程。它们的安装、索引和旧 scope 说明不能作为
本次重复执行的步骤；本次以以上实际 S12 前代和六文件纯代码范围为准。

本入口继承**当前实际安装的 schema22 Writer build**（包含已有代码后继），新增 schema23 迁移与操作授权。原 schema21/22 迁移、账号采集策略、运行激活、付费台账和操作授权对象保持原义。历史账号/作品/指标补齐、远端数据库与 Publisher 切换不在本授权内。

当前指标字段口径是不可变 `source-routing-operation-field-v4`。新报告使用 `dcar-content-operations-report-v8.10`，冻结四个平台、指标策略内容与摘要、受众分平台定标状态。旧 v2/v3、旧报告合同与输入快照继续按原版本重放。视频号可信 VV 不可用，不进入自动补采缺项；报表仍单列曝光不完整。小红书 VV 保持不适用。

新报告的 `platform_content_id_aliases` 列为 JSON 字符串数组，来自 scope 截止前的身份行证据。快手数字 photo_id 与 URL eid 可凭这份冻结证据对应；截止后的身份新增或合并不能修改旧报告导出。

## 输入与代码冻结

所有路径取自实际 Writer plist 和本次外部发布目录。父 build 必须是 plist 当前指向的 build，不能以最早 schema22 build 替代当前代码后继。`FLOW_PARENT_INSTALL` 仍是原始 cleanup install receipt。

```sh
FLOW_PYTHON=/absolute/python3.12
FLOW_CHECKOUT=/absolute/final-reviewed-worktree
FLOW_PARENT_BUILD=/absolute/current-installed-schema22-build.json
FLOW_PARENT_INSTALL=/absolute/original-cleanup-install.json
FLOW_INSTALLED_PLIST=/absolute/Library/LaunchAgents/cn.tj.dcar.writer-worker.plist
FLOW_DATA=/absolute/data-project-root
FLOW_DB=/absolute/formal.sqlite3
FLOW_SOURCE=/absolute/new-independent-schema23-source
FLOW_EVIDENCE=/absolute/private-four-platform-release
FLOW_INSTRUCTION=/absolute/private-actual-user-instruction.txt
FLOW_THREAD=actual-authorization-thread-id
mkdir -m 700 "$FLOW_EVIDENCE"

"$FLOW_PYTHON" "$FLOW_CHECKOUT/scripts/prepare_four_platform_flow_release.py" freeze \
  --parent-build "$FLOW_PARENT_BUILD" --source-root "$FLOW_SOURCE" \
  --source-tree "$FLOW_EVIDENCE/source-tree.json"
```

冻结包括已跟踪与未忽略文件、权限、Git 身份与逐文件 SHA。父源码也必须与父 build 封存清单一致。后续命令从冻结树执行；检查报告及日志必须放在源码外。

## 五类检查

同一 `source-tree.json` 必须有 `flow_schema`、`flow_execution`、`flow_metrics_reports`、`flow_frontend`、`flow_release` 五类成功报告。以下命令不调用付费服务，测试只使用临时库与样本响应。

```sh
"$FLOW_PYTHON" "$FLOW_SOURCE/scripts/prepare_four_platform_flow_release.py" check \
  --parent-build "$FLOW_PARENT_BUILD" --source-tree "$FLOW_EVIDENCE/source-tree.json" \
  --name flow_schema --output "$FLOW_EVIDENCE/flow-schema.json" -- \
  "$FLOW_PYTHON" -m unittest tests.test_v23_capture_plan_reuse tests.test_v23_content_identity \
  tests.test_v23_account_identity_repair tests.test_four_platform_flow_release

"$FLOW_PYTHON" "$FLOW_SOURCE/scripts/prepare_four_platform_flow_release.py" check \
  --parent-build "$FLOW_PARENT_BUILD" --source-tree "$FLOW_EVIDENCE/source-tree.json" \
  --name flow_execution --output "$FLOW_EVIDENCE/flow-execution.json" -- \
  "$FLOW_PYTHON" -m unittest tests.test_v23_account_identity_api tests.test_v23_account_identity_repair \
  tests.test_v23_account_capture_status tests.test_v23_content_identity_api tests.test_v23_installed_account_import \
  tests.test_v23_media_source_refresh tests.test_v23_shared_provider_flow tests.test_v23_provider_updates \
  tests.test_v23_capture_queue_fairness tests.test_v23_metric_cycle_context tests.test_capture_work_index_install \
  tests.test_v23_runtime_evidence_context tests.test_v23_capture_authority_preflight \
  tests.test_v23_shared_request_ownership tests.test_v8_capture_commands \
  tests.test_v23_preparation_recovery tests.test_v23_preparation_business_day tests.test_v8_preparation_profile_reuse \
  tests.test_v23_runtime_read_api tests.test_v8_capture_rolling_scheduler tests.test_v8_capture_runtime_lease \
  tests.test_v8_new_platform_capture_integration tests.test_v8_new_platform_contracts \
  tests.test_v8_wechat_video_crypto tests.test_v8_media_work_queue tests.test_v8_local_content_analysis \
  tests.test_v8_wechat_mixed_search tests.test_v8_resolver_parser_replay \
  tests.test_v8_preparation_never_sent_recovery tests.test_v8_unsent_admission_refresh

"$FLOW_PYTHON" "$FLOW_SOURCE/scripts/prepare_four_platform_flow_release.py" check \
  --parent-build "$FLOW_PARENT_BUILD" --source-tree "$FLOW_EVIDENCE/source-tree.json" \
  --name flow_metrics_reports --output "$FLOW_EVIDENCE/flow-metrics-reports.json" -- \
  "$FLOW_PYTHON" -m unittest tests.test_v23_current_metric_policy tests.test_v21_operation_field_policy \
  tests.test_v22_new_platform_metric_policy tests.test_report_metric_validity tests.test_v8_audience_rate \
  tests.test_v8_insights tests.test_v8_report_inputs tests.test_v8_contract tests.test_v8_reports tests.test_v8_source_routing \
  tests.test_v23_report_dependencies tests.test_v8_pipeline_cutover_receipts

"$FLOW_PYTHON" "$FLOW_SOURCE/scripts/prepare_four_platform_flow_release.py" check \
  --parent-build "$FLOW_PARENT_BUILD" --source-tree "$FLOW_EVIDENCE/source-tree.json" \
  --name flow_frontend --output "$FLOW_EVIDENCE/flow-frontend.json" -- \
  npm --prefix "$FLOW_SOURCE/app/web" test

"$FLOW_PYTHON" "$FLOW_SOURCE/scripts/prepare_four_platform_flow_release.py" check \
  --parent-build "$FLOW_PARENT_BUILD" --source-tree "$FLOW_EVIDENCE/source-tree.json" \
  --name flow_release --output "$FLOW_EVIDENCE/flow-release.json" -- \
  "$FLOW_PYTHON" -m unittest tests.test_four_platform_flow_release tests.test_account_intake_release \
  tests.test_v23_installed_account_import tests.test_v23_runtime_read_api tests.test_account_intake_code_successor
```

需要先在冻结 Web 项目安装 lockfile 对应依赖。`flow_execution` 包含真实临时 schema23 lifespan、暂停状态的 worker 注册、业务查询和冻结 XLSX 下载；`flow_metrics_reports` 还验证新 v8.10 依赖及迁移后旧 v8.9 scope/修订继续按原版本读取。这些仅读取已冻结报告依赖，不构成 Publisher 或远端安装授权。最终检查前不得再改源码；改动后重新冻结与检查。命令成功只证明代码/临时库合同，不能登记实际四平台采集成功。

## 安装、授权与切换

先停止 Writer 并卸载 Publisher。安装器必须获得已安装 Writer 的真实维护锁，先验证原 schema22 build 与安装路径，再创建独立完整 SQLite 备份，校验备份与父证明，最后在原 inode 上事务迁移到 schema23。迁移不创建回补任务、不发送请求、不开放付费闸门。

```sh
"$FLOW_PYTHON" "$FLOW_SOURCE/scripts/install_four_platform_flow.py" install \
  --database "$FLOW_DB" --project-root "$FLOW_DATA" --installed-plist "$FLOW_INSTALLED_PLIST" \
  --parent-build "$FLOW_PARENT_BUILD" --parent-install "$FLOW_PARENT_INSTALL" \
  --source-tree "$FLOW_EVIDENCE/source-tree.json" --output-dir "$FLOW_EVIDENCE/migration" \
  --check-report "flow_schema=$FLOW_EVIDENCE/flow-schema.json" \
  --check-report "flow_execution=$FLOW_EVIDENCE/flow-execution.json" \
  --check-report "flow_metrics_reports=$FLOW_EVIDENCE/flow-metrics-reports.json" \
  --check-report "flow_frontend=$FLOW_EVIDENCE/flow-frontend.json" \
  --check-report "flow_release=$FLOW_EVIDENCE/flow-release.json"

"$FLOW_PYTHON" "$FLOW_SOURCE/scripts/prepare_four_platform_flow_release.py" prepare \
  --parent-build "$FLOW_PARENT_BUILD" --parent-install "$FLOW_PARENT_INSTALL" \
  --installed-plist "$FLOW_INSTALLED_PLIST" --migration "$FLOW_EVIDENCE/migration/migration-install.json" \
  --evidence-root "$FLOW_EVIDENCE/prepared" --approve-local-capture \
  --user-instruction-file "$FLOW_INSTRUCTION" --source-thread-id "$FLOW_THREAD" \
  --actor actual-operator --reason approved-four-platform-forward-repair \
  --check-report "flow_schema=$FLOW_EVIDENCE/flow-schema.json" \
  --check-report "flow_execution=$FLOW_EVIDENCE/flow-execution.json" \
  --check-report "flow_metrics_reports=$FLOW_EVIDENCE/flow-metrics-reports.json" \
  --check-report "flow_frontend=$FLOW_EVIDENCE/flow-frontend.json" \
  --check-report "flow_release=$FLOW_EVIDENCE/flow-release.json"

"$FLOW_PYTHON" "$FLOW_SOURCE/scripts/install_four_platform_flow.py" activate \
  --proposal "$FLOW_EVIDENCE/prepared/install-proposal.json" \
  --output "$FLOW_EVIDENCE/writer-installed.json"
```

`prepare` 读取真实用户指令私有文件，绑定新源码、迁移、正式 inode、现有 activation 和 v4 指标策略；同时对 proposed plist 执行真实 bootstrap。`activate` 再验证后只替换本地 Writer plist，保持服务停止。随后由已获授权的操作者启动 Writer，确认数据 API 返回 schema23、实际 loaded build、原数据库 inode，再逐操作验证 readiness、价格、预算、发送与原始回包。新增视频号评论操作只存在于 schema23 授权集合，旧 schema22 证明不扩权。

## 失败与运行验收

### 已安装 v10 的规划、媒体公平性及未发送恢复修复（v12）

v10 已实际安装，v12 必须直接继承该 build、原 schema23 迁移及已安装索引，
不能把未安装的 v7、v8、v9 当作运行前代，也不重复 DDL。新的代码范围为
`planner_media_fairness_and_unsent_preparation`；受限清单保留历史后继兼容路径，
本次冻结清单及与实际 v10 的逐文件差异必须另外精确核对。

运行复核确认两个独立问题。规划入队/再考虑阶段仍会在写事务内调用未使用规划缓存的
目录策略验证，必须为每个阶段分别在锁外准备完整继承证据，在事务边界内继续核对实时
文件代际、数据库读集、目录/准备引用、权限、期限及原计划 CAS。准备完成后该阶段的
逻辑时间、资料重用证明和入队时间必须一致；跨日的旧计划只延后到下轮，不能在锁内
重算、篡改历史计划或放宽证据条件。

媒体排序原先全局先取等待下载的内容，跨轮只处理一项时，其他平台缺来源的内容可被
长期挡住。改为先轮转平台，再在该平台内优先下载，并保留最早待办补位和原到期排序。
轮转游标按实际领取后追加的 durable attempt ID 推进；旧 run 复用原 ID 或墙钟回退时也
按真实领取顺序处理。每五个成功领取的工作给一次普通待办机会，优先当前不是等待下载
的内容：未尝试内容按原等待时间，已尝试内容按最后真实 attempt 从旧到新轮转。
历史只读取实际领取次数与次序，不能推断历史媒体阶段；退避未领取不消费兜底位。
每轮仅一次聚合读取历史，本轮用内存队列推进，不新增状态表、DDL、强制工作或付费。

账号准备的未发送恢复必须按当前步骤的 operation、原 work/slot/window/batch/scope
验证完整 reserved→not_sent、零请求/原始响应/费用、released_unsent 和无活跃领取。
前序步骤已经成功的 resolver/profile 原始证据必须保留并继续复用，不得按整个 intake
要求全无 raw，也不得重建整个准备任务。schema22 保持原窄合同，schema23 四平台走
同一单步证明。未来仅预算预占记录存在、状态/金额/计费日全匹配且唯一问题是 TTL
过期时返回可重试 batch_reservation_expired；其他状态或归属异常仍 HOLD。历史恢复
还须核对原报价、金额、计费日及实际关闭时已到期；不能把历史“过期或变化”错误
一律解释为过期。未来 TTL 分流须在原有身份和 scope 核验之后、实际发送领取之前。
清理分支
仍须先证明没有实际发送才退回 pending，不延长 TTL 或改变费用事实。

v11 初始冻结候选的发布测试发现旧断言仍禁止 capture_runtime 变更；该候选从未安装。
v12 适配当前精确修复范围，并保留 providers、共享请求所有权及原权限边界断言。
原失败回执必须保留，不能覆盖成通过。

回归须覆盖真实冻结继承链的规划阶段、锁内无冷预检、并发变更拒绝与跨日延后，
以及四平台各准备步骤纯未发送恢复、前序成功响应保留、TTL-only 可重试与错绑定 HOLD，
以及同平台持续下载的单项预算、最早普通待办轮转、退避保留兜底位、旧 run 重试、
墙钟回退、四平台有限轮转和原媒体恢复合同。
源码、五组检查、运行 Writer、Web 来源绑定、相同原 24 项许可和实际业务阶段分别验收。
实际日志已确认存在长 normal 持锁及完成事务排队；没有任务标签的长事务不能仅凭
代码相似性归因给某个函数，隔离测试改善也不能直接作为正式运行 p95 通过结论。

### 已安装 v6 的计划类型、身份投影、媒体与继承语义修复（v10）

v10 直接继承实际安装的 v6 build。v7、v8、v9 已准备但未安装，其收据保留为未执行证据，
不能将它作为已安装前代。原 v3 单索引收据、原 v2 schema23 迁移、
原 schema22/schema23 备份及所有前代冻结源码保持原字节，不运行迁移或索引安装器。
已安装 v6 保留 v5 的写锁外权限证明、媒体单例 raw 查找、C3 详情/指标物理 owner，
并已修复同秒追加计划使最新覆盖回执读取报错的问题。本次仅对继承结构读集补充
正常目录版本递增的等价语义处理，其他已验证逻辑保留，也不纳入另一个任务尚未安装的账号分类后继。

本次覆盖率修复区分目录采集计划与账号准备计划，解决合法准备计划被送入目录计划
验证器后误报 `catalog_plan_invalid`。新 source-v2 绑定保留全部输入引用；仅有明确
合同、有效哈希及稳定 work 归属的 `account-preparation-plan-v1` 可按其类型处理，
不能过滤未知合同、损坏目录计划或坏准备计划来伪造完整覆盖。旧 source-v1 的冻结
算法和验证语义保持原样，不改旧回执、不回退 revision；所有权限验证继续严格。

本次控制流程修复把 operation publish/renew 及维护资格段的完整不可变继承检查
放到 SQLite 写事务之前，进入短事务后仍检查文件代际和数据库读集，使用重新取得
的时钟验证租约、TTL、预算和 HOLD 等实时条件。完整证明不得在事务内冷启动，
失效证明不得继续执行；保留分段耗时证据。该改动不修改任何付款原语、额度或授权范围，
不重排已排队控制命令，也不删除或重建未完成任务。

账号主页准备成功后，目录身份状态必须与同一份已验证的账号、平台和 UID 一致。
普通导入仍不能把 UID 当作已验证身份；成功准备的原子落库负责完成目录投影。
已有可信资料由原目录 reconciliation 的证据验证结果修复旧投影，不购买新资料，
不改身份定位、频率或昵称，不重跑历史内容。统计读取仍要求已验证目录；重复执行
没有状态变化时不写入，损坏证据或主体冲突不能升级身份。

实际视频号详情还暴露 HTTP CDN 例外遗漏：同一份已验证响应中的媒体 URL、token、
解密 key、作者和作品均完整，但 `wxapp.tc.qq.com` 的数字路径 `stodownload`
地址被旧 URL 规则拒绝。本轮同步媒体 URL 判断和规范化，增加这一精确 host/path
的 HTTP 例外，不泛放腾讯域名，不改签名查询或强制改为 HTTPS；材料仍须同源。

先冻结独立 v10 源码并完成五组检查。v7、v8、v9 均只有准备回执，没有实际安装；
直接运行前代仍为 v6。原 `--parent-build` 仍指向已验证的 schema22
祖先，`--migration` 仍是原始迁移收据；纯代码参数是：

```sh
--code-predecessor-build "$FLOW_INSTALLED_V6_BUILD" \
--inherited-index-install "$FLOW_INSTALLED_V3_INDEX_INSTALL"
```

不要传 `--index-install`。准备器先用实际 v6 源码自己的完整验证器检查带索引的原库，
成功后才能读取前代已验证的 migration/index 引用。新 build 使用
`four-platform-flow-code-only-successor-v1`，scope 为
`catalog_plans_identity_media_and_inheritance_semantics`，绑定实际 v6 build、前代证明摘要、
十八条专属代码/测试/文档路径中的精确差异，强制发布准备阶段 `database_writes=0` 和
`schema_migration_repeated=false`。原索引引用继续绑定 v3，原迁移继续绑定 v2。

本次代码清单仅含发布模块、prepare 脚本、本文档、发布测试、`capture_day_coverage.py`、
`capture_release_commands.py`、`pipeline.py`、`test_v23_catalog_plan_types.py` 和
`test_v23_release_command_preflight.py`，以及 `account_intake.py`、
`account_directory_reconciliation.py`、`account_capture_eligibility.py` 和
`test_v23_directory_identity_projection.py`，另含 `schema_v23.py`、
`runtime_evidence_context.py`、`test_v23_inheritance_catalog_revision.py`，以及
`media.py`、`test_v23_wechat_http_media_source.py`。
正常目录版本递增不能使结构证明失效：新结构查询直接验证原有合法性条件；
已验证冻结祖先的旧结构查询通过限定来源的语义投影保留同一条件。
缺行、非法版本、非零投影深度以及其他查询的真实数据变化仍拒绝；不改迁移 DDL。
已完成准备的小红书身份仍须验证原资料链，
升级目录标签不能绕过损坏或缺失的证据；没有准备来源的旧已验证目录保留兼容行为。原单索引
`CODE_REPAIR_FILES` 保持不变。额外文件、删除/权限变化、原迁移或索引改写、
前代源码漂移、坏继承引用、循环/过深链和额外 DDL 都拒绝。

回归使用真实冻结 v6 校验器及完整临时 schema23 带索引数据库，核对直接前代、
原 migration/index 字节、业务表及 inode 不变，并覆盖重新封装的坏引用和额外 DDL。
覆盖状态测试须证明旧 v1 回执不变、新 v2 对合法准备计划分类而保留完整输入约束，
损坏/未知类型不能绕过验证。控制预检测试须验证锁外完整检查、事务内实时权限与
失效回滚，且不触发供应商请求。身份投影测试须覆盖四平台准备后内容可见、未验证
输入仍不可见、旧可信状态恢复、暂停账号历史、重复无写入及已有采集计划可继续。
继承证明测试须覆盖正常目录版本递增、冻结祖先兼容、非法状态、非结构调用者、
其他读集变化及真实临时 schema23 A/B 边界，不能只测查询字符串。
媒体回归须从实际响应形状经现有解析器、解密材料选择到来源登记/选择，覆盖精确
HTTP host/path 与签名保留，并拒绝相似域、无关路径或跨素材拼接，不调用供应商。
实际安装后分别检查 health、data freshness、
控制命令进度与正常采集；准备成功和临时库测试不表示已切换或业务链完成。

### 已迁移 schema23 的代码后继

运行验收发现普通 FIFO 会把新平台发现与已确认媒体刷新排在大量旧任务之后；普通四平台和准备四平台分别轮转，已确认媒体刷新另占有界名额。总并发仍为 4，每次最多 16 提交，媒体最多 2 个名额；报价保持 1 小时，过期不自动续费。指标周期复用同批已验证的不可变计划，减少写锁内逐作品、逐指标组的重复校验。

这个后继保留原始 schema23 迁移收据、schema22 完整备份和历史证明，不重复执行 `install`。先按上文用原 schema22 父 build 冻结新的独立源码、完成全部五类检查；检查及原始 schema23 前驱都必须保持字节一致。确认原始 schema23 Writer 无在途请求及有效租约后停机。

在原 `prepare` 命令中额外传入 `--code-predecessor-build /absolute/current-original-schema23/build.json`，`--migration` 继续指第一次 22→23 的迁移收据。准备器用该前驱自己的封存验证器核实原迁移、授权和完整源码，再校验受限代码差异；新 build 记录 `code_predecessor`，新五类检查绑定当前源码。只允许队列选择、指标计划复用、媒体报价状态及其测试/发布入口，以及下文唯一受控性能索引；不能修改表结构、旧迁移证明、价格、路由、预算或付款原语。

任务表随队列增长仍缺少按作品查询的索引。单靠重复计算复用的三轮写锁 p95 降幅约 77%～81%，未稳定达到 80%；最终版本再增加下面唯一索引，所有即时资格 SQL 保留。

```sql
CREATE INDEX idx_capture_work_content_cycle ON capture_work_items(content_id,operation,state,account_id) WHERE content_id IS NOT NULL
```

索引使用独立 `scripts/install_capture_work_index.py install`，必须在旧 schema23 Writer 零在途、零有效租约且已停机时取得原维护锁。输入包括原 schema22 parent build/install、本次 frozen source-tree、五类 checks、已安装原始 schema23 `--code-predecessor-build` 和新的 `--output-dir`。安装器新建独立完整 schema23 备份，用原始 schema23 自己的严格 verifier 核查备份；事务内只建这个精确索引，逐表摘要证明业务记录未变，保留原迁移 payload、receipt 和 user_version=23。

`schema_v23.objects()` 仍返回所有对象；原结构验证仅允许从其输入中移除这一个名字和 SQL 完全一致的索引，再验证原完整结构 hash。新 build 另外强制验证实际结构等于备份结构加这个精确索引，绑定原 inode、备份 hash、源码和五类检查。任何其他 DDL 仍拒绝。旧 schema23 verifier 只在真实封存的 schema23 备份上运行，不伪造新库结构。

上述 `prepare` 还必须传 `--index-install /absolute/index-maintenance/index-install.json`。原 `--migration` 始终指第一次 22→23 收据，不再次执行 schema 迁移。若索引已提交但外部收据未保存，先保持停机，运行同脚本 `recover-receipt --output-dir ...`；若放弃后继且 plist 仍是原始 schema23，运行 `rollback --output-dir ...`，只删除该索引并保留后来业务记录，不恢复整个备份文件。

后续 `activate` 仍只原子替换本地 Writer plist，不修改业务记录。启动后重新发行相同本地范围内、当前源码绑定的操作许可，再做真实任务验收；不把该代码后继视为新的历史补齐或远端发布授权。

若代码后继启动失败且尚未重新放行新的工作，可在零在途、零有效租约状态下恢复前驱 schema23 的 plist；若已装性能索引，须先用专用 rollback 撤销唯一索引，再重启原 Writer。数据库保持 schema23，绝不能退回 schema22 Writer或用迁移前备份覆盖业务写入。

迁移事务失败会回滚 SQL。若事务已成功、外部收据写入失败，保持 Writer 停止，执行：

```sh
"$FLOW_PYTHON" "$FLOW_SOURCE/scripts/install_four_platform_flow.py" recover-receipt \
  --output-dir "$FLOW_EVIDENCE/migration"
```

恢复命令校验父源码、备份 SHA、原 plist、正式 inode 与 schema23 内部迁移证明，只补写匹配收据；已有有效冲突收据会被拒绝。不提供覆盖正式库的自动恢复入口，避免抹掉迁移后的业务写入。

启动时旧 schema22 verifier 只读取被封存的 schema22 备份，并继续核对正式原 inode；schema23 verifier 核对自己的结构证明及保留的 schema21/22 收据表摘要。备份按文件代际缓存完整 SHA，大小、inode、mtime、ctime、权限或所有者变化即失效。旧证明不会收到伪装成 schema22 的新连接。

实际验收必须分别记录四平台的信息补齐、身份、分页链接、作者归属、指标、媒体下载、内容分析、报告与入库结果。无样本、无可信 VV、无受众定标分别如实记载；不能把编译成功、临时测试或旧全局整日资格当作四平台采集验收通过。保留 72 小时前向发现、30 天本地分析及独立媒体刷新待办边界。
