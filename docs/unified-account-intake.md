# 四平台统一账号接入：本地实现与交接

本次实现把后台新增、Excel 汇总导入、旧内部导入包装器和存量目录补齐汇入同一个账号接入服务。账号资料先保存为可追溯请求，再由受预算、接口资格和执行授权约束的准备任务查询主页、核对身份、归档完整返回、绑定账号并计算采集资格。接收成功不等于已抓到作品或指标。

本文中的候选库数字是首次离线验证结果，不代表当前运行状态。用户随后授权启用本机完整自动化；实际安装、增量导入及运行验收以 `/Users/mark/Library/Application Support/DcarAIGC/account-intake/20260912-v1` 中的最新实测回执为准。只操作本机，Publisher 保持停用，远端数据库和发布仍未获授权。不能用旧候选库覆盖持续更新的正式本地库。

## 当前候选库结果

本地证据目录：
`/Users/mark/Documents/ChatGPT/DcarAIGC/outputs/01a08b5c-account-summary/enrichment/local-import/unified-intake-20260912`

| 口径 | 数量 | 含义 |
| --- | ---: | --- |
| 保留的目录记录 | 663 | 与补齐前目录逐列一致，保留备注、原始导入时间和人工状态 |
| 原有具备资料条件的账号 | 159 | 已有身份和平台采集定位证据；仍需执行授权、接口资格及预算 |
| 已接收准备请求 | 478 | 已写本地候选库的统一接入请求 |
| 可排出准备任务 | 472 | 影子规划生成任务；当前均因未安装准备策略而没有发请求 |
| 已接收但标识类型不支持 | 6 | 视频号 5 个 32 位十六进制标识、1 个 wxid；不能冒充 finder UID 或 sph 账号标识 |
| 缺少可用定位资料 | 25 | 现有表中没有可以准确定位主页的信息，保留目录及明确原因 |
| 无效占位标识冲突 | 1 | 保留原目录，没有强行创建身份 |

总数：159 + 478 + 25 + 1 = 663；478 = 472 + 6。此前重点提出的 263 个已有 UID 的抖音/小红书账号全部可以排准备任务，尚未真实补齐主页或抓取内容。

`queue-verification.json` 记录任务及 6 个类型问题；`directory-provenance-verification.json` 记录 663 行目录不变；`final-idempotent-import.json` 记录重复导入 0 次数据库写入。逐行异常保留在这些本地受控文件中，不在源码文档复制手机号或证件信息。

## 服务与字段契约

- `src/dcar_eval/v8/account_intake.py` 提供 `submit_account_intake(connection, *, request_key, value, source, at)`、`preparation_inputs(connection)` 和 `apply_prepared_profile(connection, intake_id, normalized_profile, raw_response_id, at)`。调用者管理事务；服务不联网、不自动迁移、不提交事务。
- `value` 使用 `platform`、`uid`、`display_account_id`、`profile_url`、类型明确的 `references` 及人工资料。账号 ID、UID、手机号和证件号码必须是文本，数字 JSON 输入拒绝而非先转浮点数。UID 与展示号保持不同字段。
- `source` 保留原 Excel 行及批注来源、来源类型、原始输入或目录行。来源请求使用稳定 `request_key`；完全相同重试返回原结果，修改同一 key 的输入或来源报冲突。新资料用新请求键。
- `input_sha256` 对应实际持久化的 `input_json`；原始提交哈希另行保存。`preparation_key` 仅由定位资料生成，同一个身份的重复来源共用准备成果。
- 平台 + UID 优先，平台 + 展示号其次；只有确认身份一致才绑定。昵称、手机号、操作员姓名都不是合并依据；跨平台保留独立身份。
- 空白、无效值及“待核实”资料不覆盖已有有效人工值。运营人员、开卡人、证件使用人、持卡人分别保存。准备返回不能覆盖其查询期间发生的人工修改。
- 只有适配器核对完整响应后才建立真实 `accounts` / `account_platform_identities` 绑定。UID 不全时可先有目录或请求；不生成假 UID。

`account_summary_import.py` 兼容原汇总导入函数；旧 schema21 的收据流程保持原意。网页 schema22 新增使用异步接入；旧 HTTP 批量导入接口继续返回 410，不重新开放旧入口。

### 平台定位规则

| 平台 | 支持的输入和身份依据 |
| --- | --- |
| 抖音 | 数字 UID、账号展示号、官方主页中的 sec_user_id；支持的官方短链在 API 写事务外展开。返回 UID 与已给定 sec_user_id 必须一致 |
| 小红书 | UID、展示号或官方主页；展示号搜索只接受准确匹配，完整主页再次确认 UID |
| 快手 | 数字 UID、主页 eid 或支持的短链；eid 是定位引用，先取候选再用数字 UID 复查，不把 eid 填到 UID。仅有展示号时保留原资料与定位不足原因，当前适配器不把数字展示号猜成 UID |
| 视频号 | finder UID 或 sph 账号标识；转换结果只是候选，随后 channel_info 反查 sph，再以完整主页的 contact.username 确认 finder。作品分享链接、wxid 和来源不明的 32 位字符串不能当作账号主页标识 |

短链展开由 `normalize_submission_input` 复用原有安全展开能力，发生在 API 数据库写事务之外。离线 Excel/目录脚本不联网，遇到需要展开或不支持的标识会保存具体原因，不能跳过身份核对。技术定位缺项与运营状态分开，没有新增人工“等待核验”状态。

### 原始响应及任务恢复

`account_intake_requests` 是准备任务的正式目标。`capture_work_items`、`fetch_slots`、请求批次和完整 `provider_raw_responses` 都记录 `intake_request_id`；多步骤的每次真实返回分别归档。

原始响应的 `account_id` 保持首次归档时的 NULL，不改写不可变响应索引。验证后的账号通过 intake 绑定关联，`account_provider_references` 保存平台、真实 `source_raw_response_id` 和引用类型。视频号转换→反查→主页、快手 eid→数字 UID 复查等链路保留所有响应 ID；最后一页成功不能代替前置证据。

准备执行复用现有 paid request、预算、限流、资格、原始响应归档和恢复机制。无法确认上次发送结果时保留原请求恢复，不直接再付费。修改定位资料后旧请求不能覆盖新输入。

## 页面语义与人工状态

页面根据 intake 结果和最后准备任务的 `state/reason/due_at` 显示“待接入”“正在准备”“准备失败”“主页准备完成”。未安装准备策略、价格未核实或暂缺预算显示“待接入”；身份不符或接口合同失败显示具体失败原因，不能永远显示“准备中”。

“主页准备完成”只说明定位资料和证据已齐备，后续仍按有效采集计划执行。日更、周更、暂停、未标记和历史停止自动采集字段目前都不作为排除名单的条件。没有观察期、连续天数判断或自动改暂停逻辑；人工状态保留原值。以后改变暂停语义需另一次明确规则变更。

存量 `directory_backfill` 只核对既有目录身份并提交准备请求，不重写 `raw_json`、`updated_at`、原始导入时间或来源说明。

目录核对是每次采集规划前的固定步骤，包含缺少 UID/展示号/主页的旧目录。定位内容不变时复用原请求，不重复创建账号或购买主页查询；修改定位、消除冲突或适配器增加能力后重新进入准备流程。旧任务返回必须仍与当前目录定位一致，才能保存结果。接口发送结果和账单不明时沿用原请求恢复，不绕过付费保护重复发送。

## 本地候选迁移和可重入补齐

所有路径必须为绝对、非符号链接路径；输出文件和备份必须是新路径。先在独立候选库验证，不能用候选 SQLite 文件直接替换正式数据库，因为原 Writer 权限绑定数据库 inode。

```sh
INTAKE_PYTHON=/Users/mark/Projects/DcarAIGC/.venv/bin/python
INTAKE_CHECKOUT=/Users/mark/Projects/DcarAIGC-worktrees/local-account-import-20260911
INTAKE_OUTPUT=/absolute/private/intake-candidate
mkdir -p "$INTAKE_OUTPUT"

"$INTAKE_PYTHON" "$INTAKE_CHECKOUT/scripts/migrate_account_intake.py" \
  --source /absolute/source-schema21.sqlite3 \
  --backup "$INTAKE_OUTPUT/source.backup.sqlite3" \
  --candidate "$INTAKE_OUTPUT/accounts.schema22.sqlite3" \
  --report "$INTAKE_OUTPUT/migration.json"

"$INTAKE_PYTHON" "$INTAKE_CHECKOUT/scripts/prepare_account_directory.py" \
  --db "$INTAKE_OUTPUT/accounts.schema22.sqlite3" \
  --evidence-root "$INTAKE_OUTPUT" --report "$INTAKE_OUTPUT/dry-run.json"

"$INTAKE_PYTHON" "$INTAKE_CHECKOUT/scripts/prepare_account_directory.py" \
  --db "$INTAKE_OUTPUT/accounts.schema22.sqlite3" \
  --evidence-root "$INTAKE_OUTPUT" --report "$INTAKE_OUTPUT/import.json" \
  --backup "$INTAKE_OUTPUT/before-intake.sqlite3" --apply
```

目录脚本默认为 dry-run：schema21 只迁到内存副本模拟。`--apply` 要求独立 schema22 候选及新备份。脚本先跳过已有资格的记录，逐行记录 accepted/no_locator/conflict，保留所有目录。再次执行使用新报告和备份文件路径；同一来源请求不会新增重复账号或准备请求。

## 后续统一上线与配对验收

**发布链的必要代码兼容已补齐；以下配对验收仍是后续实际切换的前置项：**

1. 核对 `account_cleanup_snapshot.validate` 对原清理收据的继承、schema22 和原 schema21 两份迁移证明；确认快照生成与发布证据来自同一冻结数据库。
2. 验证 Publisher 与 Writer 指向同一冻结源码，发布配置显式固定 schema22；远端 `server/install_snapshot` 必须完成明确的 schema21→22 转换，与只读库和 Web/API 同步配对。
3. 在最终源码上同时验证 Writer、Publisher、远端读库以及对应回滚策略，确认现有快照同步不会因 schema22 中断。

实际切换前完成上述检查，再使用新的 schema22 链。旧 `prepare_account_catalog_capture_release.py` 绑定 schema21 和旧策略，不能直接复用或改写旧 V2 收据。

### 冻结源码和生成实测材料

`prepare_account_intake_release.py freeze` 复制独立 Git 源码树，包含最终已跟踪和未忽略文件；`check` 必须从冻结树中的脚本运行，两个报告共享同一个源码清单。源码、检查日志或清单发生变化，后续预检即拒绝。

```sh
# 下列均为后续执行示例；先填入实际已安装父 build / 原始 install receipt。
INTAKE_PARENT_BUILD=/absolute/installed-schema21-build.json
INTAKE_PARENT_INSTALL=/absolute/original-cleanup-install.json
INTAKE_SOURCE=/absolute/independent-frozen-schema22-source
INTAKE_RELEASE=/absolute/private/schema22-release
mkdir -p "$INTAKE_RELEASE"

"$INTAKE_PYTHON" "$INTAKE_CHECKOUT/scripts/prepare_account_intake_release.py" freeze \
  --parent-build "$INTAKE_PARENT_BUILD" --source-root "$INTAKE_SOURCE" \
  --source-tree "$INTAKE_RELEASE/source-tree.json"

"$INTAKE_PYTHON" "$INTAKE_SOURCE/scripts/prepare_account_intake_release.py" check \
  --parent-build "$INTAKE_PARENT_BUILD" --source-tree "$INTAKE_RELEASE/source-tree.json" \
  --name intake_schema --output "$INTAKE_RELEASE/intake-schema.json" -- \
  "$INTAKE_PYTHON" -m unittest tests.test_v8_schema_v22 tests.test_v8_schema_v22_maintenance \
  tests.test_account_intake_install tests.test_account_intake_release \
  tests.test_account_intake_snapshot_builder tests.test_account_intake_snapshot_deployment

"$INTAKE_PYTHON" "$INTAKE_SOURCE/scripts/prepare_account_intake_release.py" check \
  --parent-build "$INTAKE_PARENT_BUILD" --source-tree "$INTAKE_RELEASE/source-tree.json" \
  --name intake_execution --output "$INTAKE_RELEASE/intake-execution.json" -- \
  "$INTAKE_PYTHON" -m unittest tests.test_v8_account_intake tests.test_v8_account_intake_api \
  tests.test_v8_account_preparation tests.test_v8_intake_capture tests.test_v8_preparation_recovery \
  tests.test_v8_platform_adapters tests.test_v8_new_platform_capture_integration \
  tests.test_account_intake_publication tests.test_v8_intake_reference_repair
```

上述重点检查以外，最终上线还须完成本节前述发布/远端配对检查及页面构建，不将两个重点报告当作远端部署成功的证明。

### 受控正式迁移和新 Writer 提案

前置兼容完成并获得统一上线授权后，停止 Writer 并确认进程和数据库句柄退出，再调用独立安装入口。安装脚本取得现有 `formal-mutation` 租约、备份并校验旧库，原 inode、权限和历史数据均保留；不替换 plist、不启动服务、不发平台请求。

```sh
"$INTAKE_PYTHON" "$INTAKE_SOURCE/scripts/install_account_intake.py" install \
  --database /absolute/formal.sqlite3 --project-root /absolute/data-project \
  --installed-plist /absolute/cn.tj.dcar.writer-worker.plist \
  --parent-build "$INTAKE_PARENT_BUILD" --parent-install "$INTAKE_PARENT_INSTALL" \
  --source-tree "$INTAKE_RELEASE/source-tree.json" \
  --check-report "intake_schema=$INTAKE_RELEASE/intake-schema.json" \
  --check-report "intake_execution=$INTAKE_RELEASE/intake-execution.json" \
  --output-dir "$INTAKE_RELEASE/installation"

"$INTAKE_PYTHON" "$INTAKE_SOURCE/scripts/prepare_account_intake_release.py" prepare \
  --parent-build "$INTAKE_PARENT_BUILD" --parent-install "$INTAKE_PARENT_INSTALL" \
  --installed-plist /absolute/cn.tj.dcar.writer-worker.plist \
  --migration "$INTAKE_RELEASE/installation/migration-install.json" \
  --check-report "intake_schema=$INTAKE_RELEASE/intake-schema.json" \
  --check-report "intake_execution=$INTAKE_RELEASE/intake-execution.json" \
  --publisher-plist /absolute/cn.tj.dcar.snapshot-publisher.plist \
  --evidence-root "$INTAKE_RELEASE/proposal" --actor /actual/operator \
  --reason '统一上线 schema22 账号接入' --approve-production-rollout
```

`prepare` 验证未改写的 schema21 父证据、schema22 迁移源哈希、同一数据库 inode、完整源码及实测报告，然后生成新的 sealed build、source plan、`writer.before.plist`、`writer.next.plist` 和安装步骤文件；使用临时 home 运行真实 bootstrap 校验，已安装 plist 保持原样。传入 `--publisher-plist` 时，还会读取它绑定的私有 env，生成 Publisher 的 before/next plist 与 before/next env，只将源码目录和预期 schema 从 21 配对到 22；省略时提案明确标记 `publisher.status=not_prepared`，不能据此直接切换全系统。显式批准参数只供后续获授权的执行使用，本轮未对真实安装执行。

正式切换时，复核 Writer/Publisher 的旧 plist、发布 env 和全部哈希，配套采用生成的 next 文件，保留原 cleanup install receipt。启动后核对实际加载 build、schema22、账号数、目录来源、Web/API 和快照/远端数据。随后分别完成各 preparation/discovery/metrics operation 的资格、价格、预算及执行授权，才允许真实请求；策略 V3 不绕过这些门禁。

若迁移成功但安装收据写出中断，可执行 `install_account_intake.py recover-receipt --output-dir ...`，只对原安装证据恢复。若需退回且尚未发生任何后续业务写入，可在 Writer 停止时执行同脚本 `rollback --output-dir ...`；它先备份 schema22，并验证完整表内容仍等于迁移后状态后才恢复 schema21，同样保留 inode。存在后续写入时拒绝回滚，保留现场，不能覆盖新数据或直接用旧代码打开新库。

## 验证范围

本地测试覆盖：主键/关联/历史保留、文本标识精度、跨平台不合并、UID/展示号冲突、重复来源零写入、目录 provenance 不变、完整多步 raw 证据绑定、准备恢复和付费门禁、API 幂等及技术状态、四平台表单、schema22 事务回滚、真实临时 Writer 维护租约、旧证据继承和新源码 bootstrap。

最终本地证据目录中的 `final-backend-regression.log`、`final-release-regression.log`、`final-web-tests.log`、`final-web-types.log`、`final-web-build.log`、`final-api-verification.json` 和 `completion-verification.json` 记录最终测试/页面/候选数据口径。仅有有限样本真实响应（历史平台集成材料中的 19 份实体 / 20 次尝试，16 份有效）；263 个账号批量补齐、全名单持续抓取监控和线上安装尚未验收。分页异常见 `integrated-live-evidence-replay` 及平台集成报告。
