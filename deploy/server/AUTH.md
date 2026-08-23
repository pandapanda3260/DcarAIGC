# Dcar Sentinel 登录与退出

## 唯一认证链路

浏览器只访问认证网关：本地 `127.0.0.1:4173`，服务器 `/dcar/`。网关负责：

- 使用现有 htpasswd 账号校验登录；
- 把随机 Session ID 写入 HttpOnly Cookie，只在独立 SQLite 中保存其摘要；
- 未登录页面跳到登录页，未登录 API 返回 401；
- 登录后把 Web 请求转到 4174、API 请求转到 8765、抖音控制请求转到 4175；
- 退出时删除服务端 Session 并清 Cookie，旧 Cookie 无法再次使用。

4174、8765 和 4175 只是回环地址上的内部上游，不是浏览器入口；systemd 模式不
对公网发布，Compose 也只把 4175 绑定到宿主回环。4173 删除浏览器传入的全部
`X-Dcar-*`，再用独立 Edge credential 向 4175 注入已认证身份、会话绑定和已验证
action。Douyin 边界只允许 GET/POST，并丢弃 4175 响应里的全部 `Set-Cookie`，因此
Control 不能覆盖 Dcar Session 或设置其他浏览器 Cookie。该边界防止外部访问绕过网关，
不把同机 root 进程视为不可信租户。

业务数据库保持只读，认证 Session 单独存放，不写入业务库。线上账号文件已
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

端口分工：认证网关 4173、Web 上游 4174、抖音控制面 4175、只读 API 8765。

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
keyring、open-id HMAC 四个随机凭据，均为 root:root 0600。阶段 0 固定关闭真实
OAuth（`DOUYIN_AUTHORIZATION_ENABLED=0`），不安装 Client Secret。安装四个常驻
unit、备份 service/timer 和备份 helper 后，再加载 Nginx：

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now dcar-api dcar-web dcar-douyin-control dcar-auth \
  dcar-douyin-vault-backup.timer
curl -fsS http://127.0.0.1:4173/dcar/auth/health
sudo nginx -t && sudo systemctl reload nginx
```

`dcar-douyin` 不加入 `dcar-aigc` 组。部署验收必须确认它不能读取 htpasswd、
认证 Session DB 或只读业务副本库。浏览器 Douyin POST 必须由 4173 校验同源并
转换成固定可信 action；4175 不接受浏览器直接伪造的身份或 action 头。

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
- 已登录访问 `/dcar/douyin` 可看到控制页，账号搜索只返回正式 Douyin 身份投影；
- Douyin 路由的 HEAD/PUT/PATCH/DELETE/OPTIONS 返回 405，4175 返回的任意
  `Set-Cookie` 都不会到达浏览器或覆盖当前 Session；
- 阶段 0 发起授权固定返回 409，真实抖音网络零请求；
- 未登录 callback 303 到不含 code/state 的固定登录地址；登录后 callback 的 state
  必须绑定原用户名和原 Session，且只能消费一次；
- `dcar-douyin` 对 htpasswd、Auth Session DB、业务副本库的反向读权限测试失败；
- `nginx -t`、认证网关测试、Web 构建和真实浏览器流程均通过。

回滚代码版本时，先恢复并 reload 前一版 Nginx，再把 API、Web、Control、Auth 四个
服务一起切换和重启；不要只回滚按钮、控制面或 Nginx 配置。
