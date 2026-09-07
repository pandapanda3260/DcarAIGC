# Dcar Sentinel 登录与退出

## 唯一认证链路

浏览器只访问认证网关：本地 `127.0.0.1:4173`，服务器 `/dcar/`。网关负责：

- 用自己的 SQLite 账号库校验账号密码登录，并提供手机号验证码登录、手机号注册与找回密码；
- 把随机 Session ID 写入 HttpOnly Cookie，只在同一 SQLite 中保存其摘要；
- 未登录页面跳到登录页，未登录 API 返回 401；
- 登录后把 Web 请求转到 4174；本地 API 转到 8766，服务器只读 API 转到 8765；抖音控制请求转到 4175；
- 退出时删除服务端 Session 并清 Cookie，旧 Cookie 无法再次使用。

## 账号库

账号（含角色）、手机号绑定、运营人员准入名单、验证码 / 找回票据、失败限速记录、删除墓碑和 Session 全部存放在
`DCAR_AUTH_SESSION_DB`（服务器 `/var/lib/dcar-aigc/auth/sessions.sqlite3`，本地
`runtime/auth/sessions.sqlite3`），`PRAGMA user_version=3`，固定 `journal_mode=DELETE`。网关启动时
把 `user_version` 0/1/2 原地升到 3。schema 2→3 在单个事务中重建 `auth_users` 的角色约束，保留已有账号角色、密码、时间戳及其他表记录；新账号默认 `new_user`。正式切换仍必须先停止认证写入并创建验证通过的备份，不能靠网关启动绕过发布流程；更高版本拒绝启动。
htpasswd 文件不再被网关读取，只作为首次发布的一次性导入源和回滚目标。

- 密码哈希：`passlib sha512_crypt`，100000 轮；登录时旧账号存在与否用同轮数的 dummy 哈希抹平时差。
- 验证码：6 位数字、5 分钟有效、单次使用、错 5 次作废；库里只存 `HMAC-SHA256(pepper, …)`，pepper 来自
  `DCAR_AUTH_PEPPER_FILE`（服务器 `LoadCredential=auth-pepper`）。同一手机号同一用途只有状态为 `sent`
  且最新的一条生效；发送失败（`rejected`）或结果未知（`unknown`，超时 / 传输错 / 响应不可解析）的验证码永不生效，
  也不遮蔽之前已送达的验证码。发送不重试（腾讯云 SendSms 无幂等）。
- 验证码 / 票据记录只作废不删除：改密（页面找回、`set-password`）、改手机号、停用、`revoke-challenges` 只写
  `invalidated_at / invalidated_reason`（`password_changed / phone_changed / user_disabled / user_deleted / revoked`），
  作废行不再生效也不会让更早的记录重新生效；物理删除只有 24 小时保留期清理，所以限频台账与费用上限不会被账号操作重置。
- 发送限频：同手机号 60 秒 1 条、1 小时 5 条、24 小时 10 条；同 IP 1 小时 10 条、24 小时 30 条；全局 24 小时
  `DCAR_AUTH_SMS_DAILY_CAP`（默认 300）条。四种发送状态全部计入。
- 失败限速：`auth_failures` 表按 `user:` / `phone:` / `ip:` 键计数（沿用 `DCAR_AUTH_THROTTLE_*` 配置，默认 10
  分钟 8 次）；未注册 / 未授权 / 已停用 / 验证码错误都计入；成功只清账号键，IP 键永不因成功清除。
- 密码规则：8–64 位任意字符，拒绝内置常见密码表命中与包含用户名 / 手机号的密码。
- 用户名 `^[A-Za-z0-9_]{4,32}$`，大小写不敏感唯一，`temporary-bypass` 保留；手机号 `^1[3-9][0-9]{9}$`。
- 账号状态 `active / disabled`：停用账号的 Session 与验证码立即撤销，密码登录、验证码登录、发码、找回全部 403。
- 注册门控是**手机号准入名单**（`allow-phone`）：准入只允许注册，不代表业务授权。页面注册出来的账号一律是新用户（`new_user`），须由管理员在用户权限页明确授权后才能访问业务；从名单移除不影响既有账号，撤权用角色降为新用户、`disable-user` 或页面删除；`set-phone` / 页面改手机号时旧手机号同事务退出名单。

### 角色与用户管理

四级角色存在 `auth_users.role`：`superadmin`（超级管理员）、`admin`（管理员）、`operator`（运营人员）、`new_user`（新用户，最低等级）。
每个请求都从库里重新读角色，网关是唯一的拦截点，角色不透传给上游。

- 新用户登录或注册后只进入 `/pending-approval` 等待授权页，不加载业务导航或数据；业务页面跳转到该页，API、媒体、导出和 RSC 返回 `403 approval_required`，用户管理接口仍返回 `403 forbidden`。管理员授权后点击“刷新权限”即可进入；已登录用户被降为新用户后，下次业务请求被拒绝，前端清除缓存并跳到等待页。迁移不会自动改变任何已有账号角色。
- 运营人员只能进 AIGC 数据统计各页与 API；`/users` 页面对其 303 到 `/overview`，`GET /auth/users` 与两个写接口 403
  `{"code":"forbidden"}`。管理员与超级管理员可进"用户管理 / 用户权限"页，看到全部账号的注册信息与权限等级。
- 能授予的最高角色 = 自己的角色；只能修改 / 删除等级不高于自己的账号（管理员碰不到超级管理员）；不能改自己的
  角色、不能删自己、不能在本页改自己的密码（走登录页找回或 CLI `set-password`）；至少保留一个 active 超级管理员。
- 首个超级管理员与"零超管"恢复只能走离线 CLI：`set-role`，或空库首次导入的 `import-htpasswd --bootstrap-superadmin`；已有超级管理员可以在页面授予他人超级管理员。
- 页面删除 = 撤销访问资格：同一事务写墓碑（`auth_deleted_users`，用户名永久保留，不能再注册或导入）、把该手机号移出
  准入名单、删除全部 Session、作废全部验证码与票据（`user_deleted`），再删账号行。
- 页面改手机号：旧手机号退出准入名单并作废其注册验证码；页面重置密码：目标账号全部 Session 立即失效。
- 写接口在事务内重新核验发起者的 Session（存在、未过期、指纹相符、账号 active、角色达标），不符返回 401
  `session_revoked` 并清 Cookie；库故障统一 503 `storage_unavailable`。
- 安全变更日志 `auth-changes.log`（与库同目录、0600、拒绝符号/硬链接、追加写、fsync，不随备份恢复）使用
  `dcar-auth-change-v2`：数据库提交前持久化 intent，提交后追加 commit marker；intent 写失败阻止事务。
  schema 3 延续该日志格式并兼容 schema 2 历史记录；`after` 包含目标新角色/状态等可审计值，不含哈希、密码、验证码。提交标记缺失归为 `conservative`，旧日志归为
  `legacy_conservative`；`changes --since` 按 commit 时间过滤已确认项，保守项始终返回，CLI exit 3 明确要求核实。
  路径可用 `DCAR_AUTH_CHANGE_LOG` 覆盖。恢复保持网关停止，先核实日志及相关账号；日志不能自动重建新密码或证明身份。
  残尾或中间坏行都会阻止后续安全变更，不能通过跳过坏行解决；按[日志损坏处置](../../docs/v8/运行与备份手册.md#认证安全变更日志损坏处置)
  停写、留存完整原件与哈希，再使用离线工具处理允许修复的残尾并完成人工身份对账。

页面接口（都在 `DCAR_AUTH_BYPASS=1` 时 404；写接口要求 `Content-Type: application/json`、`Content-Length`、
同源标记头 `X-Dcar-Request`；错误响应 `{"detail": 中文, "code": snake_case}`）：

| 路径 | 方法 / marker | 请求体 | 成功 |
|---|---|---|---|
| `/auth/users` | GET | — | `{actor:{username,role}, items:[{username,phone,role,status,created_at,password_updated_at}]}`（最新注册在前） |
| `/auth/users/update` | POST `user-update` | `{username, phone, role, password}`（四个字段都必须是字符串；`phone` 为空会清除绑定，`password` 为空不修改密码） | `{item}` |
| `/auth/users/delete` | POST `user-delete` | `{username}` | `{}` |

`/auth/session` 在登录态下多返回 `role`（bypass 模式没有该字段）。

### 离线管理工具

```sh
sudo -u dcar-aigc env PYTHONPATH=/var/www/dcar-aigc/current/src/dcar_eval \
  /var/www/dcar-aigc/current/.venv/bin/python -m dcar_auth.admin \
  --db /var/lib/dcar-aigc/auth/sessions.sqlite3 <子命令>
```

| 子命令 | 作用 |
|---|---|
| `migrate` | 建表 / 补列并把 `user_version` 升到 3（幂等；schema 2→3 保留账号和会话） |
| `import-htpasswd --source PATH [--bootstrap-superadmin]` | 须先显式 `migrate`，仅 schema 3 空库可用；跳过 `#` 注释停用行，拒绝空行；严格校验用户名与完整 SHA-512 crypt 格式/轮数，无重复/保留名/墓碑；任一失败整体拒绝；默认全部为运营人员，显式 bootstrap 时在同一事务把首个有效账号设为超管 |
| `set-role USERNAME superadmin\|admin\|operator\|new_user` | 设置权限等级或恢复首个超管；最后一个 active 超级管理员不能降级 |
| `delete-user USERNAME` | 删除账号（墓碑 + 撤准入 + 删 Session + 作废验证码）；最后一个超级管理员不能删 |
| `changes [--since ISO]` | 按 JSON 行输出标准化变更；保守项始终输出且退出码 3，坏行或不安全日志拒绝读取 |
| `export-htpasswd --output PATH` | 导出有业务权限的 active 账号（排除新用户，防止旧版回滚越权；原子写入，0640）；默认拒绝空导出；仅已切换的 schema-0 可信代码回滚使用 |
| `allow-phone PHONE [--note] [--remove]` | 维护准入名单 |
| `set-phone USERNAME PHONE` | 绑定 / 改绑手机号；同事务作废该账号全部验证码与票据，旧手机号退出准入名单 |
| `set-password USERNAME` | 交互设置密码，同事务撤销该账号全部 Session 与验证码 |
| `disable-user` / `enable-user USERNAME` | 停用 / 启用；停用同事务撤销 Session 与验证码 |
| `revoke-sessions [--username]` / `revoke-challenges [--username]` | 撤销会话 / 作废验证码与票据（默认全部；记录保留供限频计数） |
| `list` | 用户名、手机号、权限、状态、创建与改密时间、活跃 Session 数；没有超级管理员时在 stderr 提示 |
| `sms-test PHONE --sms-credentials-file PATH` | 真实发送一条测试验证码，验证签名、AK 与模板 |

除显式 `migrate` 外，普通账号 CLI 只校验数据库版本/健康，不隐式建库或迁移；旧版本或缺失的库会提示先迁移。
`sms-test` 不打开账号库，可以在发布前执行。生产旧库的迁移、导入与首个超管设置统一交给 release helper；
不要提前手工运行 `migrate` 来绕过发布检查。`set-phone dcar` 与 `allow-phone` 只在 `deploy` 成功后执行。

### 短信通道

`DCAR_AUTH_SMS_PROVIDER=tencent`（生产默认；`DCAR_AUTH_SECURE_COOKIE=1` 时禁止 `log`）通过 httpx 直接调用腾讯云短信
`SendSms`（`sms.tencentcloudapi.com`，API 3.0 版本 `2021-01-11`，`TC3-HMAC-SHA256` 签名，与官方 SDK 相同的
`SignedHeaders=content-type;host` 构造），参数放在 JSON body 里（`PhoneNumberSet` 为 `+86` 开头的 E.164 号码），
URL 不含手机号与验证码。凭据文件 `DCAR_AUTH_SMS_CREDENTIALS_FILE`（服务器 `LoadCredential=sms-tencent`）为 `KEY=VALUE`
（允许引号、`export`、注释与无关键，可直接复制 `dcar.env.local` 的腾讯云短信一节）：必填
`TENCENT_SMS_SECRET_ID`、`TENCENT_SMS_SECRET_KEY`、`TENCENT_SMS_SDK_APP_ID`、`TENCENT_SMS_SIGN_NAME`、`TENCENT_SMS_TEMPLATE_ID`；
可选 `TENCENT_SMS_REGION`（默认 `ap-guangzhou`）与 `TENCENT_SMS_CODE_TTL_MINUTES`（模板第二个变量"N 分钟内有效"，
填了必须等于 5，即验证码真实有效期；模板只有一个变量时不填）。模板变量顺序固定为 `{1}`=验证码、`{2}`=分钟数。
CAM 子账号只授予 `sms:SendSms`。短信侧的单号限频码（`LimitExceeded.PhoneNumber*` / `DeliveryFrequencyLimit`）
对用户返回 429，`InvalidParameterValue.IncorrectPhoneNumber` 返回 400，其它 `Error` / 非 `Ok` 状态返回 503。
网关把 `httpx` / `httpcore` 日志固定在 WARNING，短信只记录一条
`sms challenge=<id> purpose=<用途> status=<状态> provider_code=<Code>`，永不记录手机号、验证码与凭据。
本地开发用 `DCAR_AUTH_SMS_PROVIDER=log`，验证码打印在网关日志中（手机号脱敏）。

### 登录页接口

登录页（`deploy/server/nginx/login.html`）在同一张卡片内切换登录 / 注册 / 找回三个视图。除既有的
`POST /auth/login`（合同不变；账号停用时 403 `{"detail":"该账号已停用"}`）外，网关新增五个表单接口，都要求
`Content-Type: application/x-www-form-urlencoded`、`Content-Length`（缺失 411、超过 16KB 413）、同源标记头
`X-Dcar-Request`，失败响应为 `{"detail": 中文, "code": snake_case}`：

| 路径 | marker | 字段 | 成功 |
|---|---|---|---|
| `/auth/code` | `code` | `phone`, `purpose`(register / login / reset) | `{}`，验证码已发出 |
| `/auth/login/code` | `login-code` | `phone`, `code`, `remember`, `return_to` | `{redirect_to}` + Session Cookie |
| `/auth/register` | `register` | `username`, `phone`, `password`, `code`, `return_to` | `{redirect_to}` + 30 天 Session |
| `/auth/reset/verify` | `reset-verify` | `phone`, `code` | `{reset_token}`（10 分钟、单次） |
| `/auth/reset/confirm` | `reset-confirm` | `reset_token`, `password`, `return_to` | `{redirect_to}` + 30 天 Session；该账号其它 Session 与票据全部失效 |

处理顺序固定为：廉价校验 → 限速与廉价凭证预检（验证码比对并计次；ticket 存在性）→ 昂贵密码哈希 →
带 `rowcount` / 相等性守卫的最终 `BEGIN IMMEDIATE` 事务。`DCAR_AUTH_BYPASS=1` 时五个新接口返回 404。
客户端 IP 只在设置了 `DCAR_AUTH_BASE_PATH`（即有可信反向代理）时取 `X-Real-IP`。
`/auth/users*` 三个用户管理接口的处理顺序固定为：bypass 404 → 405 → 同源 marker → 415 → 411 / 413 → 400（结构）→
401 → 403（角色）→ 字段规则 → 密码哈希 → 事务（事务内再复核 Session 与双方角色）。

4174、本地 8766、服务器 8765 和 4175 只是回环地址上的内部上游，不是浏览器入口；systemd 模式不
对公网发布，Compose 也只把 4175 绑定到宿主回环。真实 OpenAPI 调用只能由 4175
经 `127.0.0.1:4176` 的 `dcar-douyin-egress` 发出；该 Squid 只允许精确的
`CONNECT open.douyin.com:443`，不允许子域名、IP literal、其他域名或端口。4173 删除浏览器传入的全部
`X-Dcar-*`，再用独立 Edge credential 向 4175 注入已认证身份、会话绑定和已验证
action。Douyin 边界只允许 GET/POST，并丢弃 4175 响应里的全部 `Set-Cookie`，因此
Control 不能覆盖 Dcar Session 或设置其他浏览器 Cookie。该边界防止外部访问绕过网关，
不把同机 root 进程视为不可信租户。

服务器业务数据库保持只读，认证 Session 单独存放，不写入业务库；本地 4173 是可信操作台，连接可写的 8766 正式库。线上账号文件已
只读核验为 SHA-512 crypt（`$6$`）；网关启动时也会校验格式，不符合时直接
拒绝启动，不会静默跳过认证。核验过程只看账号数量和哈希前缀，不记录或
输出实际账号与哈希。

## 本地开发

执行：

```sh
scripts/start_web_mvp.sh
```

首次运行会生成 `runtime/auth/pepper`，把账号库迁移到 `user_version=3`，并把已被 Git 忽略的旧账号文件
`runtime/auth/users.htpasswd`（若存在且账号库为空）一次性导入
`runtime/auth/sessions.sqlite3`，用 `import-htpasswd --bootstrap-superadmin` 在同一事务把首个有效账号设为超级管理员（中断或日志失败不会留下半完成导入；库里已有账号但没有超级管理员时启动脚本会
打印 `set-role` 命令提示）。此后打开 `http://127.0.0.1:4173` 会先进入
登录页；新账号用准入名单内的手机号在页面注册（本地验证码打印在终端），注册后默认为“新用户”，仅可见等待授权页；点击侧边栏退出按钮会撤销当前 Session 并返回 `/login`。

本地端口分工：认证网关 4173、Web 上游 4174、正式 API/writer 8766；8765 仅在 operator freeze 下作为只读 viewer。服务器仍使用只读 API 8765。抖音控制面为 4175，受限出网代理为 4176；4176 只绑定服务器回环，不是通用正向代理。

### 临时免登录

需要短时间关闭账号密码登录时，只给认证网关设置
`DCAR_AUTH_BYPASS=1`。本地可执行：

```sh
DCAR_AUTH_BYPASS=1 scripts/start_web_mvp.sh
```

服务器使用 systemd drop-in 设置同一变量后只重启认证网关。此模式不会修改或
删除账号文件、密码哈希和已有 Session；撤掉该变量并重启网关即可恢复登录。
免登录期间，所有能访问页面地址的人都可直接进入，使用完应及时恢复。
抖音控制路由是例外：bypass 模式下 4173 和 4175 都固定返回 403，不能在免登录
状态下发起、确认、拒绝或解绑授权。

## 服务器部署

先建立认证服务的持久目录、备份目录与凭据目录；首次发布时旧账号文件只需让服务账号可读一次（导入用）：

```sh
sudo install -d -o dcar-aigc -g dcar-aigc -m 0700 /var/lib/dcar-aigc/auth
sudo id -u dcar-douyin >/dev/null 2>&1 \
  || sudo useradd --system --home-dir /nonexistent --shell /usr/sbin/nologin dcar-douyin
sudo install -d -o dcar-douyin -g dcar-douyin -m 0700 \
  /var/lib/dcar-aigc/douyin-control
sudo install -d -o root -g root -m 0700 \
  /var/backups/dcar-aigc/douyin-control /var/backups/dcar-aigc/auth /etc/dcar-aigc/credentials
sudo install -o root -g root -m 0600 sms-tencent auth-pepper /etc/dcar-aigc/credentials/
sudo -u dcar-aigc test -r /etc/nginx/.htpasswd-dcar \
  || { sudo chgrp dcar-aigc /etc/nginx/.htpasswd-dcar; sudo chmod 0640 /etc/nginx/.htpasswd-dcar; }
```

`dcar-auth.service` 通过 `LoadCredential` 读取 `sms-tencent` 与 `auth-pepper`，不再引用 htpasswd；
首次发布由 release helper 锁内执行 `migrate`、`import-htpasswd`、首个超级管理员设置；`set-phone`、`allow-phone` 放在
helper 成功后。发布前先跑独立的 `sms-test` 并确认真实收件，不使用账号 CLI 改旧正式库。
临时端口 smoke（`systemd-run` + `LoadCredential`）、备份定时器安装与回滚前的 `export-htpasswd`
都写在 `README.md`「Build and install a code release」。

发布工具在 smoke 前与锁内停服前分别核验磁盘：按实际文件系统合并 DB/sidecar、迁移增量、迁移前后备份、日志、receipt/旧 unit
和 helper 原子安装预算，完成预算后必须同时保留至少 2 GiB 和总容量 3%，不计 root 保留块。回滚/恢复按其安全副本大小另算。
检查失败不停止服务，不提供环境变量/CLI 隐式绕过，也不自动清理数据；先扩容或审查可安全迁移的生成物，再重跑。

在 root-only 的 `/etc/dcar-aigc/credentials` 中安装 Edge、Machine、Fernet
keyring、open-id HMAC 四个随机凭据，均为 root:root 0600。上线 Stage-1 unit 前，
必须先在抖音控制台轮换截图中暴露过的值，并把唯一的新 Client Secret 安装为
`/etc/dcar-aigc/credentials/douyin-client-secret`（root:root 0600）。4175 只通过
systemd `LoadCredential` 读取它，Squid 的 `proxy` 用户不能读取凭据源。基础 unit
只读取 root-owned 的 `/etc/dcar-aigc/douyin-stage1.env`，不再用 `Environment=`
重复定义两个开关；文件缺失时应用默认仍为 `DOUYIN_AUTHORIZATION_ENABLED=0` 和
`DCAR_DOUYIN_PROVIDER=disabled`。安装 `dcar-douyin-egress`、五个常驻 unit、
备份 service/timer 和备份 helper 后，再加载 Nginx：

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now dcar-api dcar-web dcar-douyin-egress \
  dcar-douyin-control dcar-auth \
  dcar-douyin-vault-backup.timer
curl -fsS http://127.0.0.1:4173/dcar/auth/health
sudo nginx -t && sudo systemctl reload nginx
```

`dcar-douyin` 不加入 `dcar-aigc` 组。部署验收必须确认它不能读取 htpasswd、
认证 Session DB 或只读业务副本库。浏览器 Douyin POST 必须由 4173 校验同源并
转换成固定可信 action；4175 不接受浏览器直接伪造的身份或 action 头。
4175 保留 `IPAddressDeny=any`/`IPAddressAllow=localhost`，并只按显式
`DCAR_DOUYIN_PROXY_URL=http://127.0.0.1:4176` 使用代理；应用禁止读取 ambient
proxy 环境变量，也禁止代理失败后退回直连。

账号新增走登录页注册（准入名单门控），改密走登录页找回、用户权限页（管理员改他人）或 CLI `set-password`，
角色与删除走用户权限页或 CLI `set-role` / `delete-user`；`htpasswd -5` 不再是账号变更途径。

Compose 运行时账号库位于 `/var/lib/dcar-aigc/auth-compose-sessions/sessions.sqlite3`，
凭据副本以 uid 10001 可读的方式单独存放，与 systemd 服务账号权限互不混用：

```sh
sudo install -d -o root -g root -m 0700 /var/lib/dcar-aigc/auth-compose-credentials
sudo install -d -o 10001 -g 10001 -m 0700 /var/lib/dcar-aigc/auth-compose-sessions
sudo install -o 10001 -g 10001 -m 0600 \
  /etc/dcar-aigc/credentials/sms-tencent /etc/dcar-aigc/credentials/auth-pepper \
  /var/lib/dcar-aigc/auth-compose-credentials/
DCAR_AUTH_CREDENTIAL_ROOT=/var/lib/dcar-aigc/auth-compose-credentials \
DCAR_AUTH_SESSION_ROOT=/var/lib/dcar-aigc/auth-compose-sessions \
  docker compose -f deploy/server/compose.yml up -d
```

Compose 形态的迁移、停启、镜像回滚合同：`docker compose stop auth control web api` →
`docker tag dcar-aigc-api:local dcar-aigc-api:prev && docker tag dcar-aigc-web:local dcar-aigc-web:prev` →
`docker compose build api web` → 先以新镜像显式运行 `docker compose run --rm --no-deps auth python -m dcar_auth.admin
--db /var/lib/dcar-aigc/auth/sessions.sqlite3 migrate` → 首次 schema 0 来源再运行导入（`docker compose run --rm --no-deps
-v <htpasswd 副本>:/tmp/users.htpasswd:ro auth python -m dcar_auth.admin --db
/var/lib/dcar-aigc/auth/sessions.sqlite3 import-htpasswd --source /tmp/users.htpasswd --bootstrap-superadmin`；
已有 schema 1/2 账号不可重新导入）→ 确认超管/账号 → `docker compose up -d` 并验收健康 → 再同法执行
`set-phone` / `allow-phone` / `list`。回滚：停四个服务 → 以新镜像
`export-htpasswd` → 把导出复制为 `${DCAR_AUTH_CREDENTIAL_ROOT}/users.htpasswd`（0600，10001:10001）→
`prev` 标签打回 `local` → 恢复上一版 `compose.yml`（含 htpasswd bind）→ `docker compose up -d`。
宿主机备份定时器用 drop-in 把 `--source` 与 `ReadOnlyPaths` 指向 compose 的库路径。

改密、改手机号、停用账号后，该账号的全部 Session 在同一事务内删除、验证码与票据在同一事务内作废。不要把实际账号、
密码、哈希、pepper 或短信凭据提交到仓库。

## 验收

- 未登录访问 `/dcar/selling-points`，跳到带安全 `return_to` 的登录页；
- 错误密码返回 401，正确密码回到原页面；
- 未带 Cookie 请求 `/dcar/api/` 返回 401；
- 点击退出后 URL 为 `/dcar/login`；
- 浏览器后退或手工重放退出前 Cookie，受保护页面仍要求登录；
- 连续失败达到限制后返回 429；
- 准入名单内的手机号可以注册并直接登录，名单外手机号发码返回 403 且腾讯云无发送记录；
- 验证码登录成功；同一验证码第二次使用 401；
- 找回密码两步走完后直接登录，另一浏览器里该账号的旧会话被踢回登录页；
- `disable-user` 后该账号所有入口 403，原会话立即失效；`export-htpasswd` 不含该账号；
- 运营人员登录后侧栏没有"用户管理"分组，直接访问 `/dcar/users` 回到总览，`/dcar/auth/users` 403；管理员能改 / 删运营人员
  但看不到超级管理员的修改与删除按钮（接口层同样 403）；管理员的角色下拉里没有"超级管理员"；
- 在用户权限页删除一个已登录的运营人员：其浏览器下一次请求跳回登录页，用同一手机号发注册验证码返回 403，用原用户名注册返回 409；
- 在用户权限页重置某人密码：其旧会话立即失效，新密码可登录；`set-role` 把最后一个超级管理员降级被拒绝；
- 发布前已登录的浏览器无需重新登录，旧 htpasswd 账号的密码登录仍可用；
- `dcar-auth-backup.service` 手工运行一次产出备份与 manifest，`--verify` 通过；
- 已登录访问 `/dcar/accounts/douyin-authorization` 可看到授权管理页；新授权只能从账号
  列表具体行进入，链接与 start body 同时锁定 account id 和 Douyin uid，通用管理页不
  提供统一扫码；
- Douyin 路由的 HEAD/PUT/PATCH/DELETE/OPTIONS 返回 405，4175 返回的任意
  `Set-Cookie` 都不会到达浏览器或覆盖当前 Session；
- 阶段 0 flag 关闭时，带完整目标的发起授权固定返回 409，真实抖音网络零请求；
- `dcar-douyin-egress` 只监听 `127.0.0.1:4176`，只允许
  `CONNECT open.douyin.com:443`；example.com、IP literal、非 CONNECT 和其他端口
  都返回拒绝；
- 停止 `dcar-douyin-egress` 后，真实 provider 请求 fail closed，不能绕过代理直连；
- 未登录 callback 303 到不含 code/state 的固定登录地址；登录后 callback 的 state
  必须绑定原用户名和原 Session，且只能消费一次；
- `dcar-douyin` 对 htpasswd、Auth Session DB、业务副本库的反向读权限测试失败；
- `nginx -t`、认证网关测试、Web 构建和真实浏览器流程均通过。

回滚代码版本时，先恢复并 reload 前一版 Nginx，再把 API、Web、Control、Auth 四个
服务一起切换和重启；不要只回滚按钮、控制面或 Nginx 配置。
若同时回滚受限出网层，必须先关闭真实 OAuth/provider、恢复上一版 Control unit 并
重启 4175，之后才可停止和删除 `dcar-douyin-egress`。疑似 Client Secret 泄露时还要
在抖音控制台撤销该值，不能只删除服务器文件。
