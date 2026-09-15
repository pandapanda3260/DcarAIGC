# 最新账号汇总表的本地增量导入

本次只更新独立本地数据库。主仓库现有未提交改动、安装中的 Writer 数据库、发布器及线上环境未修改。

独立代码：`/Users/mark/Projects/DcarAIGC-worktrees/local-account-import-20260911`，分支 `codex/local-account-import-20260911`，基于 `cc69581`。改动保留在该工作树中。

输入：`/Users/mark/Documents/ChatGPT/DcarAIGC/outputs/01a08b5c-account-summary/enrichment/账号信息_抓取补全.xlsx`。

输入 SHA256：`42f693e8cc60ed6294a13e363bb7ac0b4cb62f027fb2e891368165294b2f5ef9`。

配套抓取核验记录：同目录 `enriched_normalized.json`。导入器读取实际 Excel 的单元格、文本类型、约数格式和批注，并逐行检查核验记录与 Excel 一致；不把核验文件当作 Excel 的替代品。

最新进展：在下述全状态规则基础上，另从本地完整响应恢复 2 个账号凭据，当前 159 / 663 条满足采集条件。详细导入、备份与监控修复见文末。

## 本次结果

- Excel 840 条账号：新增目录 371、更新目录 262、整行待核实 207。新目录包括 242 个新主体、127 条只有显示 ID 的目录、2 个已有主体补目录。
- 另有 52 条手机卡资料，未伪造成账号；完整内容、来源和备注保留在私有导入回执中。
- 原目录 30 条没有改动，最终目录 663 条。主体从 308 增至 550，身份从 275 增至 517。新表未匹配的 44 个原主体全部保留；其中原本不在目录中的主体不会被凭空赋予平台身份。
- 已导入的 633 条中，249 条仍有部分字段待核实；有效字段已导入，未明确字段保留库内有效值。完整异常明细 456 行（207 整行待核实 + 249 部分字段待核实）。
- 三条整行身份冲突是 Excel 第 88、571、617 行：同平台显示 ID 重复且 UID 无法独立确认。另 204 条缺少可安全建档的定位标识。未按名称或手机号合并。
- 重放同一文件：新增 0、更新 0、未变更 633，SQL 写入数 0。仍返回同一批 207 条待核实和 52 条资料，便于持续核对。

## 字段及迁移

平台、UID、显示 ID、运营人员、手机号、账号名称、更新状态、分类和明确实名状态复用现有三张表：`accounts`、`account_platform_identities`、`account_directory_rows`。

完整 16 字段、开卡人、证件号码、持卡人、接单状态、实名来源、原始批注、核验来源、旧值与导入回执保存在既有 `account_directory_rows.raw_json.account_summary` 中。不同人员字段独立保存，标识和联系方式均使用文本。数据来源详情通过现有账号列表 API 的 `account_summary` 返回，页面增加只读“资料”入口。

本次没有 DDL 迁移；数据库保持 schema 21，完整 sqlite_master 结构与迁移回执不变。无需新增 SQL 迁移文件。导入器会检查现有结构，不在运行时偷偷升级旧库。

粉丝以带来源的 Excel 观察值展示，保留约数含义；已有供应商采集指标和原始证据未改写。页面同时保留不同的旧采集记录。明确更新状态按新表更新，但新主体 `enabled=0`、目录 `uid_unverified`；不创建任务、不建立采集通道、不改变已有采集准入状态。

## 本地文件和验证

所有数据文件位于：

`/Users/mark/Documents/ChatGPT/DcarAIGC/outputs/01a08b5c-account-summary/enrichment/local-import/`

- `accounts.local.sqlite3`：已导入的本地库。
- `accounts.before-import.sqlite3`：导入前完整备份；通过 SQLite backup API 从本地一致性副本生成，包含当时已提交 WAL 数据。
- `clone-receipt.json`：本机源数据库只读备份来源、时间和 SHA256。
- `import-result.json`：892 行完整去向、字段差异、保留资料和备份回执。
- `待核实明细.csv`：按源行号列出的整行与字段异常；请使用“从文本/CSV 导入”并将 UID/账号 ID 列设为文本，避免 Excel 自动改写长数字。
- `replay-verification.json`：重复执行零写入。
- `database-verification.json`：原主键与关联保留、120 张历史业务表逐表内容哈希一致、SQLite/FK 检查通过。
- `field-verification.json`：892 行逐一核对，633 条完整字段、文本标识、批注与来源验证通过。

本地验收地址 `http://127.0.0.1:4183/accounts`，账号 API 只读、无调度，网关仅开放账号读取；其他写请求拒绝。使用现有真实 AccountsPage 与实际本地数据库。媒体归档没有复制到本次工作树，所以该验收入口不代表整站、媒体库或线上健康验收；它也不执行发布或抓取。

## 后续上线步骤（本次未执行）

1. 将本工作树改动合入准备上线的代码，先处理与当时最新代码的冲突；不要覆盖主仓库其他任务改动。核心文件为导入模块、账号读模型、前端账号资料及配套脚本/测试。
2. 上线窗口内，按既有运维流程暂停写入和快照发布，从届时最新数据库取得一致性离线候选。不要直接用本次本地库替换届时数据库，否则会丢失本次备份之后的新内容。
3. 在离线候选上先运行以下 dry-run，复核 UID/显示 ID 冲突和变更数量，再加 `--apply --backup /absolute/new-before-import.sqlite3` 执行。同文件重放不会新增账号或改时间戳。脚本拒绝已安装 Writer 的同一文件或硬链接，必须使用独立候选文件。

```bash
PYTHONPATH=src/dcar_eval python scripts/import_account_summary.py \
  --db /absolute/candidate.sqlite3 \
  --xlsx /absolute/账号信息_抓取补全.xlsx \
  --metadata /absolute/enriched_normalized.json \
  --report /absolute/dry-run.json
```

4. 用 `audit_account_summary_import.py --before ... --after ... --report ...` 核对历史数据，再用 `verify_account_summary_fields.py --db ... --before ... --xlsx ... --metadata ... --result ... --output ...` 核对每行字段。schema 21 无需 DDL；若届时结构改变，应先适配并重新验收。
5. 按既有数据库安装/发布流程上线候选和代码，然后检查账号列表、资料弹窗、账号状态及历史关联。系统自动依据唯一平台 UID、定位及真实凭据判断采集条件，不设置人工核验待办；导入资料本身不代替这些证据。异常明细中无法唯一定位的账号保留具体缺项，不按名称或手机号猜测。

当前本地复核可运行 24 项核心导入测试、3 项 Excel 解析测试、7 项目录读模型测试及 42 项前端账号测试。TypeScript 检查和本地生产构建已通过。生产构建仅生成本机文件，没有部署。

## 重新启动本地验收入口

在本工作树 `app/web` 使用现有依赖，运行本地 Node 22.13+ 的 `node node_modules/vinext/dist/cli.js start --hostname 127.0.0.1 --port 4184`；如源码变化，先本地 build。不要运行 `start_web_mvp.sh`，它会接入当前安装的正式 Writer。

另一个终端在本工作树运行：

```bash
python scripts/preview_account_summary.py \
  --db /Users/mark/Documents/ChatGPT/DcarAIGC/outputs/01a08b5c-account-summary/enrichment/local-import/accounts.local.sqlite3 \
  --evidence-root /Users/mark/Projects/DcarAIGC \
  --port 4183 --web-port 4184
```

预览绑定 loopback、检查目标不是正式库、读取账号并拒绝其他业务操作；浏览器禁止加载外部头像。不要通过反向代理对外暴露这个临时本地验收入口。Python 可使用主项目已有 `.venv/bin/python`。Excel 导入脚本只用标准库，无需安装 openpyxl。

## 本地核验文件的根目录

独立数据库保留原始核验记录中的路径和 SHA256。部分旧响应使用相对路径，因此只复制 SQLite 文件到工作树并不足以读取对应证据。本地预览必须显式传入 `--evidence-root`，本次为 `/Users/mark/Projects/DcarAIGC`。该参数选择本机已经存在的文件，不复制、不改写、不补抓原始响应，也不操作该目录的数据库。

启动脚本在首次导入 `v8` 之前同时设置已有路径解析器需要的代码根和证据根，并检查所有旧读取器实际使用同一证据根。仅设置 `DCAR_PROJECT_ROOT` 不够；`v8.__init__` 会提前加载并固定存储根目录。这里的双根环境参数仅用于路径解析，不是 Writer 授权，不创建或修改安装回执；预览仍使用独立数据库、只读模式、关闭调度与外网连接。预览的账号采集状态按当前本地证据计算，正式只读副本仍使用已发布快照。

用以下脚本复核全部目录及四个曾因路径缺失而被排除的原始响应。脚本检查原文件的大小、SHA256、UID 与抓取定位值是否属于同一个账号对象，使用现有读取器的文件安全规则；不会跳过校验或因读不到文件而自动放行。统计不写死为 62，后续数据变化会返回届时结果。

```bash
python scripts/verify_local_account_evidence.py \
  --db /Users/mark/Documents/ChatGPT/DcarAIGC/outputs/01a08b5c-account-summary/enrichment/local-import/accounts.local.sqlite3 \
  --evidence-root /Users/mark/Projects/DcarAIGC \
  --raw-response-id 319 --raw-response-id 185 --raw-response-id 138 --raw-response-id 63 \
  --report /Users/mark/Documents/ChatGPT/DcarAIGC/outputs/01a08b5c-account-summary/enrichment/local-import/capture-audit/local-evidence-verification.json
```

在取消运营状态筛选后的首轮复核为 663 条目录、157 条满足账号采集条件；四份原始文件的校验和身份绑定均通过，数据库前后 SHA256 一致、SQL 写入数为 0。这只证明账号条件和本地证据可读取，不证明已经开始或成功完成抓取。健康接口另显示 `evidence_root`、`code_root`、`capture_status_source=current_local_evidence`，便于确认入口实际配置。

后续上线需按既有部署流程保留/打包所需核验文件，复核部署目标中的实际路径和哈希；不能把本机路径直接写成线上路径，也不能仅上传 SQLite 文件后默认证据齐全。本地预览脚本及其双根设置不替代正式安装回执、发布流程或采集授权。


## 当前人工状态与采集规则（2026-09-11）

日更、周更、暂停、未标记均参与同一套自动采集资格判断；旧 `accounts.enabled` 开关不用于筛选账号。没有观察期、截止时间、按停更天数自动暂停或自动改状态。规则保持到另行明确修改。

人工状态保存只更新标签，不改变 `enabled`、采集计划、历史名单或业务关联。标签不要求定位证据齐全，只要准确绑定账号即可保存；采集仍要求平台支持、唯一身份和真实定位证据。实际运行时由既有计划器按资格维护旧开关投影。新增暂停账号在主页身份及凭据有效时同样进入后续计划。

新增人工操作回执使用 `account-operating-label-v2`，约束操作前后采集开关不变；旧 v1 回执仍按原规则严格校验，原文不修改。重放旧操作不重复写数据，并重新显示当前采集资格。日覆盖统计在新政策下忽略人工标签变化，旧政策下的历史覆盖哈希算法保留。

状态规则修改当时的全量只读复核：663 条目录、157 条合格（较原规则增加 95 条暂停账号）、506 条存在技术阻断。阻断为定位缺失 129、平台未支持 203、身份缺失 38、主页证据缺失 136。原人工标签仍为日更 201、周更 35、暂停 210、未标记 217。不存在因为暂停或未标记而被排除的账号。本轮数据库零写入，SHA256 不变；没有启动真实抓取或监控。

本轮验收文件在 `local-import/capture-audit/`：

- `all-status-evidence-verification.json`：全量资格、原始证据和数据库零写入核对。
- `all-status-api-verification.json`：7 页全部 663 条及 633 条导入资料逐字段核对、人工状态与采集原因计数。
- `all-status-browser-verification.json`：实际页面 9 个账号检查，包含两条暂停但可采集账号及缺定位账号，浏览器运行错误为 0。
- `paused-account-still-eligible.png`：暂停标签保留、没有错误停采提示的页面。

后续统一上线还需同步安装新政策 `account-catalog-automatic-capture-policy-v2`。旧安装回执及旧计划只授权旧政策，不能把新代码直接指向旧回执启动。应基于届时实际安装代次准备新的源码、审批及检查/构建回执，按既有发布流程安装后生成新 catalog plan；不要修改旧回执或绕过校验。旧副本中因暂停排除的记录会显示“采集计划尚未按全量名单规则更新”，直到新计划发布。新旧政策在同一日切换不会被误算为同一范围的完整覆盖。本次没有执行上述上线操作。

最终定向验证：214 项后台测试、26 项前端账号测试、TypeScript 检查、本地生产构建全部通过。全量 API 读取与页面抽查分别验收，不视为实际抓取完成。更广泛 API 套件在基础版本已有 7 个失败，之前的基础/候选对照保留在 `api-baseline-comparison.json`；本次未把它们算作通过，也未做无关修复。


## 继续处理：完整凭据恢复与主页指标（最新）

从前期 `sample-profiles.json` 恢复 2 个账号完整凭据：目录 234（抖音，账号 426，“95加满”）和目录 401（小红书，账号 592，“AI车与你”）。只按平台＋UID 精确绑定，原人工标签分别保留未标记和日更。对保存的完整 payload 重新编码后，字节长度和 SHA256 与当时 transport receipt 完全一致，才落盘原始实体及来源副本。没有将 slim 摘要、仅格式正确的 UID 或人工“已核验”标签当成原始抓取证据。

当前目录仍为 663 条，满足资格 **159 条（158 抖音、1 小红书）**，剩余 504 条：缺定位 128、缺主页证据 135、缺身份 38、平台未支持 203。身份标识、手机号、人员信息和人工标签均未改动。121 张非目标数据表逐表内容哈希一致，既有原始响应和定位关联完整保留；数据库结构不变。

导入前备份为 `local-import/accounts.before-profile-evidence.sqlite3`。新增 2 条 `provider_raw_responses` 和 2 条 `account_provider_references`，共 4 次 SQL 写入；原始凭据文件保存在 `local-import/profile-evidence/`。来源明确标记 `local_profile_evidence_import`，没有伪造付费任务、采集发生次数或费用回执。同一输入再次执行返回未变更 2、写入 0，不增加账号或重复凭据。

新增脚本 `scripts/import_account_profile_evidence.py` 默认 dry-run，仅允许独立候选库，禁用网络。上线前在届时最新的独立离线候选上运行（所有路径换成目标机实际路径）：

```bash
python scripts/import_account_profile_evidence.py \
  --db /absolute/candidate.sqlite3 \
  --evidence-root /absolute/existing-evidence-root \
  --input /absolute/sample-profiles.json \
  --evidence-dir /absolute/imported-profile-evidence \
  --report /absolute/profile-evidence-dry-run.json
```

确认 dry-run 后以新报告路径加 `--apply --backup /absolute/new-before-evidence.sqlite3`。必须同时携带完整输入响应及导入后的原始证据文件；不能只迁 SQLite。然后运行 `verify_account_profile_evidence_import.py --before ... --after ... --import-report ... --report ...`，核对全部非目标数据和关联保留。脚本未授权或执行线上发布，正式政策与源码回执仍按前述统一上线流程准备。

同时修复 7 个已有抖音账号的主页指标旧 HOLD 永久占位：沿用原有每 6 小时周期，仅对真正到期、身份绑定相同且晚于旧 HOLD 的新周期生成新工作身份。同周期幂等，旧 HOLD、旧 paid identity、费用不删除、不改写、不重发。runnable、running、provider_blocked 等普通非终态工作仍有互斥，供应商、预算、付费身份及运行权限检查保留。这是后续周期的调度修复，不代表已解锁或补跑旧请求。

真实页面验收还修复独立预览忽略 SQLite 已提交 WAL 数据的问题：仅 `live_capture_status` 模式启用已有 WAL 只读连接，正式 sealed replica 仍保持 immutable 读取。API 验收现在逐账号比较页面响应与直接资格计算，能发现“库已更新、页面仍显示旧结果”。

本轮验收记录在 `capture-audit/`：`profile-evidence-import.json`、`profile-evidence-replay.json`、`profile-evidence-database-verification.json`、`profile-evidence-readiness.json`、`profile-evidence-api-verification.json`、`profile-evidence-browser-verification.json` 和 `profile-metric-hold-verification.json`。全部 7 页 663 条、633 条导入资料逐字段及资格一致性通过，浏览器实际检查 11 个账号，包含新恢复的两个账号，运行错误为 0。扩大回归 269 项通过，后续 WAL/空来源关联场景的定向测试也通过；本轮没有前端源码改动，沿用上轮已验收构建。

当前仅本地候选与只读预览完成；未启动真实抓取、未解除历史 HOLD、未部署。159 表示账号具备技术条件，不等同于每个采集阶段已请求成功。历史旧计划与新政策切换时仍需按实际安装代次处理任务接续；不能将旧政策任务直接解释为新政策任务。
