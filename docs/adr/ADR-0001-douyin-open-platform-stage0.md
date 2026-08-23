# ADR-0001：抖音开放平台阶段 0 安全与技术底座

- 状态：已接受，允许按 0A → 0B → 0C → 0D 实施
- 日期：2026-08-23
- 基线提交：`43e8bfb7c186cdc7a739a555205f4eb2bc7090db`
- 实施分支：`codex/douyin-open-platform-stage0-v9`
- 实施工作树：`/private/tmp/dcar-douyin-stage0-v9`

## 1. 目标与结论

阶段 0 建立一个默认不能连接真实抖音网络的 OAuth 控制面，为阶段 1 接入抖音
开放平台准备以下底座：

1. 认证网关后的抖音控制路由；
2. 账号最小投影与授权确认页面；
3. 单次消费、绑定浏览器会话的 OAuth 状态机；
4. 独立加密 Token Vault；
5. 可在控制服务停机时执行的 SQLite 在线备份；
6. systemd、Nginx、Compose、受限 SSH 通道与可回滚发布合同。

阶段 0 不调用抖音真实 OAuth、作品列表、Webhook、下载或删除接口，不修改正式业务
库，也不改变当前抓取、发布与分析链路。生产环境固定
`DOUYIN_AUTHORIZATION_ENABLED=0`；真实 Client Secret 在完成轮换前不得安装到服务器。

## 2. 已核验基线

### 2.1 Git 与工作区

- 本 ADR 的起点、本地 `origin/main` 记录及远端 `main` 在核验时均为
  `43e8bfb7c186cdc7a739a555205f4eb2bc7090db`。
- 原工作区 `/Users/mark/Projects/DcarAIGC` 存在其他任务的 16 个 tracked 修改和
  5 个 untracked 文件；这些文件不得被本阶段 stash、reset、clean、暂存、提交或移动。
- 阶段 0 只允许在本文头部列出的独立 worktree 修改。
- 旧备份分支不得 push；不得从未清洗的旧提交建立阶段 0 分支。

### 2.2 当前运行态

- Mac 的 4173、4174、8765、8766 正在监听；writer 使用正式可写 schema 16。
  阶段 0 不修改活动 `.venv`、LaunchAgent、writer 配置或 Mac 正式库。
- publisher 已加载但最近因当日 `daily_capture=failed` 而 fail-closed。该数据质量门
  与阶段 0 代码实现分离，不在本阶段顺手修复。
- Ubuntu 当前 release 为 `20260822T085800Z-43e8bfb`；API、Web、Auth active，
  8765 是 schema 16 的只读副本且 `quick_check=ok`，4175 尚未部署。
- `snapshot-install.lock` 核验时无 holder；这一观测不能替代发布当场取锁。
- 干净归档上的阶段 0 基线目标组共 79 项测试通过。

### 2.3 抖音应用状态与密钥处置

- 2026-08-23 在开放平台控制台复核时，应用状态已显示为“正式应用/已完成”，
  不再按“测试应用”设计阶段 1 门禁。
- Client Secret 曾在截图及控制台页面中明文出现，按已泄露处理。
- 当前控制台可见页面没有自助重置入口；删除应用不是可接受的轮换方法。
- 阶段 0 不读取、不复制、不提交、不部署该 Secret。真实 OAuth 保持关闭。
- 在阶段 1 canary 前，必须通过开放平台提供的重置入口或官方支持完成轮换，并仅将
  新 Secret 通过 systemd credential 安装。未完成轮换时，任何真实授权请求均为硬失败。

## 3. 范围边界

### 3.1 阶段 0 必须交付

- 4173：抖音路由、认证、同源 POST、可信头、callback 清洁失败路径、短超时上游、
  base path Location 修复和 bypass 拒绝。
- 4175：自渲染页面、账号搜索、Mock OAuth、状态机、授权确认、Token Vault、审计和
  内部 health。
- Vault：MultiFernet 密文、open_id HMAC 指纹、SQLite DELETE journal、每连接安全
  PRAGMA、显式关闭、单 writer。
- 运维：停机也可用的 root 备份 helper、systemd timer、Nginx、Compose、凭据、受限
  SSH 端口转发、发布和回滚说明。
- 自动化：安全、路由、状态机、数据库、备份、部署合同和真实本地 Mock 闭环测试。

### 3.2 阶段 0 明确不做

- 不连接 `open.douyin.com`，不执行真实 OAuth 或 Token 刷新。
- 不调用 `video.list`、Webhook、作品统计、媒体下载或删除。
- 不修改 `v8/storage.py`、`v8/api.py`、`v8/providers.py`、`v8/capture.py`、
  `v8/scheduler.py`、writer 或 React Web 页面。
- 不改正式库 schema；4175 不 import `dcar_eval.v8.*`，也不获得正式库写权限。
- 不把 OpenAPI 临时塞入现有 `call_override`。阶段 1 先参数化 provider、
  adapter version、price 与账号解析，再建立 `AccountDiscoveryAdapter`。
- 不建设匿名 callback、receipt Cookie、请求签名信封、nonce/replay 表、Ed25519 账号
  目录、第二套 release/runtime、自研 SNI relay 或 writer 重放表。

## 4. 架构与信任边界

```text
浏览器 / 抖音顶层回跳
        |
        | HTTPS，Cookie 只到网关
        v
Nginx -> dcar-auth 4173
             |-- 4174 Web
             |-- 8765 只读 API
             `-- 4175 dcar-douyin-control
                       |-- 只读调用 8765 /api/v8/accounts/search
                       `-- /var/lib/dcar-aigc/douyin-control/vault.sqlite3

Mac writer -- 受限 SSH tunnel --> 127.0.0.1:4175/internal/v1/*
```

固定决策：

- callback 走普通已认证网关链路，不开第二条匿名公网入口。
- 浏览器永远不直连 4175；4175 只监听 loopback。
- 4173 到 4175 使用独立静态 Edge Key。网关先删除客户端全部 `X-Dcar-*`，再注入
  可信身份。4175 不信任浏览器同名头。
- 内部 `/internal/v1/*` 只接受 Machine Key，其他路由只接受 Edge Key；两种凭据互斥。
- `dcar-douyin` 独立系统用户仅用于代码/Vault 权限隔离，不加入 `dcar-aigc` 组；它必须
  无法读取 htpasswd、Auth session DB 和业务副本库。
- 本阶段继续使用同一 release 树和 venv，不创建第二套 Python runtime 或镜像。

## 5. 网关合同

### 5.1 路由优先级和默认行为

抖音边界路由必须排在通用 `/api` 和 Web 路由之前，只匹配精确路径或以 `/` 开始的
子路径，不允许 `/douyin-evil` 之类的相邻前缀命中。

- `douyin_upstream` 为可选配置，默认未配置。
- 未配置时：已认证请求返回 404，不尝试连接；未认证仍先执行现有认证规则。
- `DCAR_AUTH_BYPASS=1` 时，所有抖音控制路由由 4173 直接返回 403；4175 也拒绝
  `temporary-bypass`，形成两层防线。
- Douyin 边界只允许 GET 和 POST；HEAD、PUT、PATCH、DELETE、OPTIONS 在认证、读取
  请求体、生成可信头和访问上游之前统一返回 405。callback 进一步只允许 GET。
- 4175 使用独立 HTTP client，connect/read/write/pool 均采用秒级短超时；不得复用
  8765 的长任务 client。

### 5.2 可信头

进入 Douyin 分支后，4173 按以下固定顺序处理：

1. 认证会话；
2. 对 POST 执行同源与 marker 校验；
3. 删除浏览器传入的全部 `X-Dcar-*`；
4. 注入 `X-Dcar-Authenticated-User`；
5. 注入 `X-Dcar-Session-Binding`，值为当前 Session token 的 SHA-256；
6. 注入 `X-Dcar-Verified-Action`，值为该路由固定 action；
7. 注入从 credential 文件读取的 `X-Dcar-Edge-Key`。

4173 必须丢弃 4175 响应中的全部 `Set-Cookie`，包括重复头和非 Dcar 名称；4175
不能创建、覆盖或删除网关 Session Cookie，也不能通过其他 Cookie 改变浏览器状态。

Nginx 的通用 `/dcar/` location 也必须清空 `Authorization`、
`X-Dcar-Authenticated-User`、`X-Dcar-Session-Binding` 和 `X-Dcar-Edge-Key`，不能只依赖
Python 网关清理。

### 5.3 同源 POST

所有浏览器 Douyin POST 必须使用与路径唯一对应的 marker：

| 路径 | `X-Dcar-Request` |
| --- | --- |
| `/api/douyin/accounts/search` | `douyin-accounts-search` |
| `/api/douyin/oauth/start` | `douyin-oauth-start` |
| `/api/douyin/oauth/confirm` | `douyin-oauth-confirm` |
| `/api/douyin/oauth/reject` | `douyin-oauth-reject` |
| `/api/douyin/authorizations/unbind` | `douyin-authorization-unbind` |

4173 自己复用现有 `_same_origin_post` 语义校验 Origin/可信代理信息，验证后才生成
`X-Dcar-Verified-Action`。4175 只信该可信 action，不信浏览器 marker。

### 5.4 请求体上限

- Douyin POST 缺少 `Content-Length`：411。
- 声明长度超过 64 KiB：413。
- 合法长度：4173 有界缓冲后以原始 bytes 转发。
- 非法 `Content-Length` 或协议层长度冲突交给 Nginx/Uvicorn/h11 拒绝；应用层不实现
  一个不可达的“声明长度与实际长度不符”分支。

### 5.5 callback

- callback 是已登录用户的普通顶层 GET。
- 若 Fetch Metadata 存在，要求 `Sec-Fetch-Mode=navigate`、
  `Sec-Fetch-Dest=document`，`Sec-Fetch-Site` 只能为 `cross-site` 或 `none`。
- 未登录 callback 不把原 query 编入 `return_to`，固定 303 到：
  `/dcar/login?notice=douyin-session-required&return_to=/dcar/douyin`。
- login 模板仅从固定 notice 枚举显示“登录状态已失效，请重新登录后再次发起授权”。
- code/state 不得进入 login URL、HTML、Referrer 或日志。
- callback 的应用响应始终 303 离开带 query 的 URL，并带 `Cache-Control: no-store` 与
  `Referrer-Policy: no-referrer`。

### 5.6 base path 与 Location

- 4175 `redirect_slashes=False`，应用生成的站内跳转必须使用配置
  `PUBLIC_BASE_PATH` 拼接相对地址。
- 4173 的 `_public_location` 对 API/Douyin 这类剥前缀上游返回的、指向上游自身的
  绝对 Location 补回公开 base path，且不得重复 `/dcar/dcar`。
- 外部绝对 URL 和已经正确的相对 URL 不改写。
- 测试必须同时覆盖空 base path 与 `/dcar`。

## 6. 公开和内部路由

| 公开路径 | 方法 | 4175 上游路径 | 固定结果 |
| --- | --- | --- | --- |
| `/dcar/douyin` | GET | `/douyin` | 账号选择/授权状态页 |
| `/dcar/api/douyin/accounts/search` | POST | `/api/douyin/accounts/search` | 最小账号投影 |
| `/dcar/api/douyin/oauth/start` | POST | `/api/douyin/oauth/start` | `{ "authorize_url": "..." }` |
| `/dcar/oauth/douyin/callback` | GET | `/oauth/douyin/callback` | 校验、换 Mock Token、303 |
| `/dcar/douyin/confirm` | GET | `/douyin/confirm` | 当前 pending 授权确认页 |
| `/dcar/api/douyin/oauth/confirm` | POST | `/api/douyin/oauth/confirm` | 事务确认 |
| `/dcar/api/douyin/oauth/reject` | POST | `/api/douyin/oauth/reject` | 擦除候选 |
| `/dcar/api/douyin/authorizations` | GET | `/api/douyin/authorizations` | 当前用户最小状态 |
| `/dcar/api/douyin/authorizations/unbind` | POST | `/api/douyin/authorizations/unbind` | `authorization_id + expected_version` 乐观解绑 |

内部仅暴露 `GET /internal/v1/health`，只接受 Machine Key。阶段 0 不实现同步写入接口。

Mock OAuth 提供方是独立的 loopback 测试服务，只由测试 fixture 和本地端到端实跑启动；
4175 产品路由中不得出现 `/mock/*`。生产固定 `provider_mode=disabled`、
`DOUYIN_AUTHORIZATION_ENABLED=0`，不读取 Client Secret，也不创建 OAuth HTTP client。

## 7. 账号搜索合同

浏览器请求 JSON 只允许：

- `query`：用户原始输入，0 到 100 字符；不 trim、不写日志；
- `page`：整数且至少 1；
- `page_size`：整数且最多 50。

4175 服务端调用只读 `POST /api/v8/accounts/search`，原样透传 `query`，固定强制
`platform=douyin`。列表需要翻页，不能假定少于 100 个账号。

8765 的真实响应字段为 `platforms`，顶层还可能带 `phone` 和
`pending_platform_identities`。4175 只允许返回：

- `account_id`、`operator_name`、`enabled`；
- 已正式建档的 Douyin `uid` 与 `nickname`。

必须删除 phone、pending identity 和其他平台身份。接口不暴露 identity id，Vault
有意只保存 `(account_id, platform_uid)`。start 与 confirm 都必须重新查询并验证账号仍
存在、enabled、平台为 Douyin 且 uid 匹配，不信任页面旧快照。

## 8. OAuth 状态机

### 8.1 状态

1. `created`：同源 start 创建；只保存 state 摘要，绑定 username、session binding、
   account_id、platform_uid、requested scopes 和过期时间。
2. `exchanging`：callback 使用条件更新单次消费；要求 username/session binding 均与
   start 一致。并发回调只有一个能进入。
3. `pending_confirmation`：code 立即换 Mock Token，候选 Token 立即加密；userinfo
   失败只降低昵称/头像展示，不销毁已换得 Token。
4. `confirmed`：confirm 在单个 `BEGIN IMMEDIATE` 事务中 upsert active authorization，
   清除候选密文。
5. `failed`、`rejected`、`expired`：终态；立即擦除候选密文，不可继续消费。

state 原文和 code 永不入库、日志或审计。过期 pending 由定时清理归入 `expired` 并擦除
密文。start 返回 JSON，由页面执行 `window.location`；不得让 fetch 跟随跨域 303。

### 8.2 授权唯一性与重新授权

- 新 open_id + 未占用目标：新建 active。
- 相同 open_id + 相同 `(account_id, platform_uid)`：保留 authorization row id，替换
  Token/到期/scopes，`version += 1`，`renew_count = 0`。
- 相同 open_id + 不同目标：返回换绑冲突，旧 active 不变。
- 不同 open_id + 已占用目标：返回占用冲突，旧 active 不变。
- 只有显式 unbind 成功后，才允许绑定到新目标。
授权记录包含：access/refresh 到期时间、renew_count、scopes、key_version、version、刷新
租约和 `needs_reauthorization`。阶段 0 只建立这些字段和加密存储，没有实现刷新、
`renew_refresh_token`、租约竞争、到期调度或自动重新授权逻辑。官方 Token 生命周期限制
及“同一 open_id 只刷新一次”的执行逻辑属于阶段 1：access 15 天、refresh 30 天、
refresh 最多续 5 次、最长 195 天后必须重新授权。

## 9. Vault 模型与加密合同

Vault 只含三张业务表：

### 9.1 `oauth_states`

至少包含：`state_digest` 主键、`bound_username`、`session_binding`、`account_id`、
`platform_uid`、`requested_scopes_json`、`status`、`expires_at`、候选 token 密文、
候选 open_id 指纹、候选公开资料、失败 reason code、创建/更新时间。

候选 Token 明文 envelope 必须包含 `record_id=state_digest`；解密后用常量时间比较，防止
不同记录间替换密文。

### 9.2 `douyin_authorizations`

至少包含：稳定 row id、`open_id_fingerprint UNIQUE`、`bound_username`、`account_id`、
`platform_uid`、access/refresh token 密文、`access_expires_at`、`refresh_expires_at`、
`renew_count`、`scopes_json`、`key_version`、`version`、刷新租约、
`needs_reauthorization`、状态和时间戳。

另加 `(account_id, platform_uid)` 的 active 唯一约束，数据库约束和事务内显式冲突检查
共同保证不隐式换绑。

### 9.3 `audit_events`

只保存时间、actor、action、结果、reason code、authorization/state 的不可逆短指纹和
request id。禁止保存 code、state 原文、Token、Client Secret、query、Cookie、Edge Key、
Machine Key 或用户手机号。

### 9.4 密钥

- Token 使用 `MultiFernet` 认证加密；阶段 0 已验证多 key 解密和显式 `rotate()` 原语，
  但 Vault 尚未实现“旧 key 解密后用 primary key 懒重加密”的读写路径，该逻辑属于
  阶段 1。
- Fernet keyring 每行固定为 `<正整数版本>:<Fernet key>`，首行是当前写 key，版本不得
  重复；候选密文绑定 `record_id=state_digest, kind=oauth_candidate`，正式 access/refresh
  密文分别绑定 `record_id=authorization_id, kind=access|refresh`。
- open_id 索引用独立 HMAC-SHA256 key，不复用 Fernet key。
- Edge Key、Machine Key、Fernet keyring、open_id HMAC key、Client Secret 五类凭据分文件、
  分用途、0600，通过 `LoadCredential=` 注入。
- Client Secret 只在真实 OAuth flag 开启时才是必需 credential；阶段 0 生产不得安装。
- 日志脱敏由控制模块自己完成；绝不修改 `capture.py` 的 TikHub 信封脱敏器。

## 10. SQLite 固定合同

Vault 体量小且只有单 writer，固定使用 rollback journal `DELETE`，禁止 WAL。

### 10.1 启动 preflight

4175 在建表、写入和进入 ready 之前：

1. `sqlite3.connect(..., timeout=10.0)`；
2. `PRAGMA busy_timeout=10000`；
3. 执行 `PRAGMA journal_mode=DELETE`；
4. 断言返回值小写后严格等于 `delete`。

WAL 标记会持久写入数据库文件，因此即使 DELETE 是 SQLite 默认值也必须执行该纠偏。
若 WAL 正被占用而无法转换，服务拒绝启动，不执行 DDL，也不提供 ready。

### 10.2 每一个 VaultStore 连接

每次新建连接、事务开始前均设置并读取断言：

- `PRAGMA journal_mode=DELETE` -> `delete`；
- `PRAGMA synchronous=EXTRA` -> `3`；
- `PRAGMA foreign_keys=ON` -> `1`；
- `PRAGMA busy_timeout=10000` -> `10000`；
- `PRAGMA locking_mode=NORMAL` -> `normal`。

`synchronous` 是连接级设置，不能只在初始化连接上设置。标准
`with sqlite3.Connection` 只负责提交/回滚，不会关闭连接；实现必须通过明确 factory 与
`closing`/`finally` 在成功和异常路径都 close。

写事务固定 `BEGIN IMMEDIATE`，保持短小；网络调用不得发生在持锁期间。4175 单 worker。
clean close 后不得留下 `-wal`/`-shm`，DELETE 事务中的短暂 `-journal` 合法。

## 11. 备份合同

### 11.1 helper 与权限

- root-owned 标准库 Python helper 固定安装到 `/usr/local/libexec`，不得从 current、venv
  或 `PYTHONPATH` 运行，不加载任何业务凭据。
- source 目录只读；backup 目录是唯一读写路径；helper 只保留
  `CAP_DAC_READ_SEARCH`，启用 `PrivateNetwork` 并只允许 `AF_UNIX`。
- backup service 与 control 无 `Wants`、`After` 或 `Requires` 关系，必须在 4175 inactive
  时独立成功。
- timer 每小时运行；本阶段不自动删除历史备份。

### 11.2 单次 Online Backup

1. source 以 URI `mode=ro`、`timeout=10.0` 打开并要求 journal 为 DELETE；不得使用
   `immutable=1`。
2. target 同目录以 `O_EXCL` 创建 `.partial`，连接 `timeout=10.0`；backup 前设置并断言
   DELETE，设置 `synchronous=EXTRA`。
3. 只调用一次 `source.backup(target, pages=4096, sleep=0.01)`；SQLite/Python 自带
   BUSY/LOCKED 等待，不写额外 for/while 重试器。
4. `connect timeout` 不是整个 backup 的总时限；systemd oneshot
   `TimeoutStartSec=60s` 是唯一总时限。失败或被杀只允许留下未发布 partial。
5. 关闭写连接后重新以 RO 打开 target，验证 `quick_check=ok`、
   `foreign_key_check` 为空、journal=delete、schema/表计数、密文 canary、无 sidecar、0600。
   重开连接不得验证 `synchronous=3`，因为它是连接级设置。
6. fsync 文件和目录，写 SHA-256 manifest；仅内容变化时 `os.replace` 原子发布 final。

源库若为 WAL 或需要 hot-journal recovery，helper fail closed，绝不修改 source。

### 11.3 硬验收

- 写入加密 canary，clean stop 4175，确认无进程 FD 且无 `-wal/-shm/-journal`。
- 4175 保持 inactive 时运行备份必须成功。
- target 含 canary 密文且完整性通过，source SHA-256 与 mtime 不变。
- 短暂 EXCLUSIVE 写锁释放后，backup 通过内建等待成功。
- 持续锁由 systemd 60 秒终止，且不得出现 final。

## 12. 服务、网络和文件权限

### 12.1 control unit

- `User=dcar-douyin`、`Group=dcar-douyin`；
- `ConditionPathIsDirectory=/var/lib/dcar-aigc/douyin-control`；
- `After=Wants=network-online.target dcar-api.service`；
- `RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6`；
- `IPAddressDeny=any`、`IPAddressAllow=localhost`；
- `--no-access-log`，只监听 `127.0.0.1:4175`；
- Vault 目录由 root 预建为 `dcar-douyin:dcar-douyin 0700`，是唯一业务 RW 目录。

`dcar-auth.service` 增加对 control 的 `After/Wants`，保留现有 htpasswd condition 和 Auth
session RW 路径。`ConditionPathIsDirectory` 只负责早失败；验收仍检查
`ConditionResult=yes`、active 和 loopback health。

### 12.2 Nginx

- callback 仍代理到 4173，只在精确 location 关闭 access log，并设置
  `error_log ... crit`、限速和只允许 GET。
- `limit_req_zone` 放在 http 级 include，不放进 server block include。
- 通用和 callback location 均清除新增可信头。
- 回滚先恢复 Nginx 配置并 reload，再停止 4175，避免中间 502 把 query 写进 error log。

### 12.3 Compose

- control 以独立 UID 10002 复用 API 镜像，发布端口仅
  `127.0.0.1:4175:4175`；4173 仍是唯一公网入口。
- 构建产物目录必须至少 0755、文件至少 0644/可执行文件 0755，并以 UID 10002 实际
  import control 包和 `cryptography`。
- systemd 与 Compose 不得同时运行同一环境。

### 12.4 Mac 机器通道

- 专用 SSH key 使用 `restrict,port-forwarding,permitopen="127.0.0.1:4175"`，禁止 shell、
  agent/X11 转发和其他目标。
- `/internal/v1/*` 只接受 Machine Key；阶段 0 只有 health。
- SSH 22 可达是部署和未来 Mac 同步的硬前置。

## 13. 发布合同

阶段 0 使用现有 `/var/www/dcar-aigc/releases`、同一 runtime 和每 release venv。新增
`cryptography` 必须先在 Mac 更新 pinned `pyproject.toml`、`uv.lock` 和
`deploy/server/requirements-api.txt`，服务器使用 wheelhouse/候选 venv，不在生产临时
联网装包。

`/var/www/dcar-aigc`、`/var/www/dcar-aigc/releases` 与只存放解释器的
`/var/www/dcar-aigc/runtime` 固定为 `root:root 0755`，使彼此不同组的 `dcar-aigc` 与
`dcar-douyin` 都只能穿越并读取发布代码及共享解释器；不得用把 `dcar-douyin` 加入
`dcar-aigc` 组来绕过目录权限。业务库、htpasswd、Vault 与凭据仍分别保留原有的最小
权限边界。

`/var/lib/dcar-aigc` 固定为 `root:dcar-aigc 0751`：`dcar-douyin` 只能穿越到其已知的
`douyin-control` 子目录，不能列出父目录；Vault 子目录继续为
`dcar-douyin:dcar-douyin 0700`。服务账号必须用独立同名主组创建。

固定发布顺序：

1. 候选 release/venv 完成测试；服务器候选只做 health、页面和权限 smoke。生产 flag=0，
   `oauth/start` 预期 409，不能声称生产完成 Mock OAuth 闭环。
2. 确认 publisher 当前无进程。发布脚本用 `flock -n` 获取
   `snapshot-install.lock`；取不到时在停止服务前退出。
3. 持锁复核 schema16、active snapshot、业务 DB SHA；安装目录、credential、unit。
4. 依次停止 Auth、Control、Web、API，原子切换 `current`，依次启动 API、Control、Web、
   Auth。
5. 服务健康后执行 `nginx -t` 并 reload；验证公网 health、页面、账号搜索和 start=409。
6. 复核业务 DB SHA、active snapshot 和 8765 只读状态未变，记录 receipt 后释放锁。

snapshot installer 使用阻塞 `flock(LOCK_EX)`；若自动 publisher 在发布持锁期间触发，
它会等待而不是失败。阶段 0 发布流程不调用 installer，自动 publisher 照常运行，靠锁
串行；因此发布窗口必须短。

回滚必须在同一持锁窗口内恢复旧 Nginx 配置、旧 `current`、旧 units/credentials，按
API、Web、Auth 原合同完成 smoke。不得只回滚 4175 或只回滚按钮。

### 13.1 生产认证验收状态

2026-08-23 已删除生产临时 `DCAR_AUTH_BYPASS=1` drop-in，恢复正常账号登录。
未登录 HTTPS smoke 已验证：页面返回 302、API 返回 401，callback 返回 303
到不含 code/state 的固定 notice，响应具有 `no-store` 和 `no-referrer`。另使用
服务器端创建并随即撤销的短期生产 Session 完成真实 HTTPS smoke：
`/auth/session`、Douyin 页面和账号搜索均成功，带跨站导航 Fetch Metadata 的
callback 抵达 4175，并因 provider flag=0 按预期跳转到 `oauth-disabled`。

当前 Chrome 没有可复用的 Dcar 登录态，验收过程也没有代用户输入密码，因此
“真实浏览器从外站执行顶层 GET，并由 SameSite=Lax 自动携带 Session Cookie”
仍是唯一未完成的生产验收项，必须在阶段 1 真实 OAuth 开闸前完成。阶段 0 的
provider flag 仍为 0，以上 smoke 不证明有效 state 消费或真实抖音 OAuth；二者仍留给
阶段 1 canary。

## 14. 工作包与放行条件

### 0A：基线、ADR、安全

- 建立独立 worktree，保留原工作区；
- 固化本 ADR、威胁模型、路由、状态机、数据库、发布/回滚合同；
- 运行 redacted secret scan；
- 记录 Secret 轮换的前置门禁，确保阶段 0 不安装旧 Secret。

放行：原工作区零触碰；worktree diff 仅为 ADR；文档校验和 secret scan 通过。

### 0B：网关与路径

- 实现边界路由、bypass 403、同源、可信头、短超时 client；
- 实现 callback 未登录清洁跳转和 login notice；
- 修复剥前缀上游绝对 Location；
- 同时覆盖空 base 与 `/dcar`。

放行：网关/部署合同相关测试全绿；未配置上游、伪造头、callback、canary 日志和路径
回归均通过。

### 0C：控制面、Vault 与备份

固定实施顺序：

1. 更新并锁定 `cryptography` 依赖；
2. 实现 ready 前 DELETE preflight 和每连接 PRAGMA；
3. 实现三表 DAO、加密和状态机；
4. 实现页面、账号投影、loopback Mock OAuth client 与 test-only provider fixture；
5. 实现单次 backup helper；
6. 运行单元、集成、并发和停机备份测试。

放行：真实抖音零连接；state 重放/错会话/冲突、密文替换、旧 WAL 纠偏、每连接
sync=3、显式 close、短锁、持续锁、4175 停机备份全部通过；生产 flag=0。

### 0D：部署、发布与文档

- systemd、Nginx、Compose、SSH、credentials、持久目录和 timer；
- 更新部署合同测试与 server README/AUTH；
- 候选验证、受锁切换、生产 smoke 和完整回滚演练。

放行：生产 health/页面/search 通过、start=409；正式业务 DB SHA 和 active snapshot 不变；
旧 release 回滚 smoke 通过。

## 15. 必测矩阵

- Gateway：路径边界、bypass、认证顺序、GET/POST 方法白名单、marker/Origin、清头/注头、
  丢弃 4175 的全部 Set-Cookie、callback、Location、上游短超时。
- Body：缺 Content-Length=411、超过 64KiB=413、上限内 bytes 不变。
- Account search：空/中文/空格/%/_/100 字符原样传递；101=422 且 8765 零调用；最小投影。
- OAuth：单次 state、并发 callback、错用户/会话、过期/reject、userinfo 降级、重新授权和
  唯一性冲突。
- Vault：新库、持久 WAL 转 DELETE、占用 WAL 拒绝、每连接 sync=3、显式 close、
  `BEGIN IMMEDIATE`、密文跨记录替换失败。
- Backup：运行中、clean stop、短锁、60 秒持续锁、hot journal fail closed、manifest、
  hash、权限、无 secret、source 不变。
- Deployment：systemd analyze、Condition、UID 反向权限、Compose import、Nginx 日志、
  SSH permitopen、发布与回滚。

不得删除测试、skip、新增宽松分支、降低现有断言或把 Mock 结果表述成真实抖音成功。

## 16. 阶段 1 硬入口

### 16.1 真实 OAuth 硬入口

只有以下条件全部满足，才允许打开真实 OAuth：

1. 已泄露 Client Secret 完成轮换，新 Secret 只通过 credential 安装；
2. 精确 callback URI 已登记；`user_info`、`video.list`、
   `renew_refresh_token` 与 `trial.whitelist` 所需能力均已获批；
3. 正向代理域名 ACL、DNS、证书、超时与审计完成，不直接删除 systemd 禁网；
4. 完成上一节尚未完成的真实 HTTPS 浏览器登录与跨站顶层回跳 smoke；
5. 阶段 1 首版不显示真实头像，只使用本地占位图并把 CSP 收紧为
   `img-src 'self' data:`；不保存或渲染 provider avatar URL。若未来确需头像代理，必须
   另立 ADR 处理 SSRF、域名白名单、大小/类型限制和缓存，不能恢复浏览器直连外域；
6. 实现 access/refresh 到期调度、open_id 刷新租约、`renew_refresh_token` 次数与 195 天
   上限、`needs_reauthorization` 状态，以及旧 key 解密后的 primary-key 懒重加密；
7. 增加 root-only 运维 CLI，用于离职操作员场景下审计式解绑：参数必须包含
   `authorization_id`、`expected_version`、`actor` 和 `reason`，原子擦除 Token、置为
   `unbound`、版本加一并记录 `authorization_admin_unbind`。CLI 不直接转移所有者；新
   操作员必须重新完成 OAuth，不允许任意已登录浏览器操作员接管他人授权；
8. 以 Vault schema v2 把 active 授权唯一约束迁移为仅
   `platform_uid WHERE status='active'`，并补全冲突矩阵：owner 不同为
   `owner_conflict`；同 open_id/同 UID 但 account_id 漂移为 `target_changed`；同 open_id
   但 UID 改变为 `open_id_rebind_conflict`；不同 open_id 占用相同 active UID 为
   `account_binding_conflict`；只有 unbound 记录才能由新操作员重新 OAuth 认领；
9. callback 用户名/Session 不匹配必须在事务外留下不含敏感值的 `security_rejected`
   审计；新 start 遇到 `exchanging` 流程必须返回 409，不得直接 supersede；
10. trust boundary 对非 ASCII credential/action 头稳定返回 403，不得让
    `compare_digest` 的 `TypeError` 变成 500；
11. 单个自有白名单账号完成授权、刷新、重新授权、账号匹配、撤销 canary；
12. 先完成 `AccountDiscoveryAdapter` 前置参数化，`call_override` 仍只用于 fixture；
13. 明确 open_id 到 `(account_id, platform_uid)` 的绑定及 DiscoveryItem 映射；
14. Mac writer 继续是正式业务库唯一 writer。

### 16.2 阶段 1 运维与测试债

以下项目不等同于真实 OAuth 安全门禁，但必须在阶段 1 跟踪关闭，且在完成前不得在交付
总结中宣称已经实现：

- backup helper 在可捕获异常路径的 `finally` 中，只清理由本次进程创建的 `.partial`
  及其 sidecar；SIGKILL 遗留仍允许由运维清理。timer 增加
  `RandomizedDelaySec=10m`。
- E2E host 参数化并默认使用与生产一致的 `127.0.0.1`，`::1` 仅作为可选地址族测试。
- 源码权限合同不再依赖 checkout umask；Dockerfile 显式规范构建产物权限，并继续保留
  UID 10002 的真实镜像 import 测试，不得简单删除断言。

作品身份合同提前冻结但不在阶段 0 实现：`video_id` 必须为 6–24 位数字，作为
`platform_content_id`；canonical URL 为 `https://www.douyin.com/video/{video_id}`；
`media_type -> content_type`；`item_id` 不持久化；置顶作品不参与时间窗口停止判断。

## 17. 参考依据

- [OAuth 2.0 Security Best Current Practice (RFC 9700)](https://www.rfc-editor.org/rfc/rfc9700)
- [抖音 Web OAuth 2.0](https://open.douyin.com/platform/resource/docs/develop/permission/web/oauth2)
- [抖音开放平台 FAQ](https://open.douyin.com/platform/resource/docs/common-question/faq)
- [抖音获取用户公开信息](https://open.douyin.com/platform/resource/docs/openapi/account-management/get-account-open-info)
- [抖音查询授权账号视频列表](https://open.douyin.com/platform/resource/docs/openapi/video-management/douyin/search-video/account-video-list)
- [SQLite Write-Ahead Logging](https://www.sqlite.org/wal.html)
- [SQLite PRAGMA](https://sqlite.org/pragma.html)
- [Python sqlite3 backup](https://docs.python.org/3.12/library/sqlite3.html#sqlite3.Connection.backup)
- [Fernet / MultiFernet](https://cryptography.io/en/latest/fernet/)
- [OpenSSH authorized_keys options](https://man.openbsd.org/sshd.8#AUTHORIZED_KEYS_FILE_FORMAT)
