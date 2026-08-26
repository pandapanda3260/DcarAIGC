# Dcar Sentinel 登录与退出

## 唯一认证链路

浏览器只访问认证网关：本地 `127.0.0.1:4173`，服务器 `/dcar/`。网关负责：

- 使用现有 htpasswd 账号校验登录；
- 把随机 Session ID 写入 HttpOnly Cookie，只在独立 SQLite 中保存其摘要；
- 未登录页面跳到登录页，未登录 API 返回 401；
- 登录后把 Web 请求转到 4174；本地 API 转到 8766，服务器只读 API 转到 8765；抖音控制请求转到 4175；
- 退出时删除服务端 Session 并清 Cookie，旧 Cookie 无法再次使用。

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

首次运行会在终端要求创建一个本地账号和密码，并保存到已被 Git 忽略的
`runtime/auth/users.htpasswd`。此后打开 `http://127.0.0.1:4173` 会先进入
登录页；点击侧边栏退出按钮会撤销当前 Session 并返回 `/login`。

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

先建立认证服务的持久目录，并允许服务账号读取现有账号文件：

```sh
sudo install -d -o dcar-aigc -g dcar-aigc -m 0700 /var/lib/dcar-aigc/auth
sudo id -u dcar-douyin >/dev/null 2>&1 \
  || sudo useradd --system --home-dir /nonexistent --shell /usr/sbin/nologin dcar-douyin
sudo install -d -o dcar-douyin -g dcar-douyin -m 0700 \
  /var/lib/dcar-aigc/douyin-control
sudo install -d -o root -g root -m 0700 \
  /var/backups/dcar-aigc/douyin-control /etc/dcar-aigc/credentials
sudo -u dcar-aigc test -r /etc/nginx/.htpasswd-dcar \
  || { sudo chgrp dcar-aigc /etc/nginx/.htpasswd-dcar; sudo chmod 0640 /etc/nginx/.htpasswd-dcar; }
```

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

账号新增或改密继续使用 SHA-512 crypt：

```sh
sudo htpasswd -5 /etc/nginx/.htpasswd-dcar <用户名>
```

Compose 运行时把账号只读副本和可写 Session 物理分开，避免容器通过可写
目录改动账号源，也避免与 systemd 服务账号权限混用：

```sh
sudo install -d -o root -g root -m 0700 /var/lib/dcar-aigc/auth-compose-credentials
sudo install -d -o 10001 -g 10001 -m 0700 /var/lib/dcar-aigc/auth-compose-sessions
sudo install -o 10001 -g 10001 -m 0600 \
  /etc/nginx/.htpasswd-dcar \
  /var/lib/dcar-aigc/auth-compose-credentials/users.htpasswd
DCAR_AUTH_CREDENTIAL_ROOT=/var/lib/dcar-aigc/auth-compose-credentials \
DCAR_AUTH_SESSION_ROOT=/var/lib/dcar-aigc/auth-compose-sessions \
  docker compose -f deploy/server/compose.yml up -d
```

不要让容器直接修改服务器账号源；账号变更后重新生成该只读副本。

改密或删除账号后，该账号的旧 Session 会在下次请求时失效。不要把实际账号、
密码或哈希提交到仓库。

## 验收

- 未登录访问 `/dcar/selling-points`，跳到带安全 `return_to` 的登录页；
- 错误密码返回 401，正确密码回到原页面；
- 未带 Cookie 请求 `/dcar/api/` 返回 401；
- 点击退出后 URL 为 `/dcar/login`；
- 浏览器后退或手工重放退出前 Cookie，受保护页面仍要求登录；
- 连续失败达到限制后返回 429；
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
