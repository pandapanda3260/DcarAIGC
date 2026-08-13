# Dcar Sentinel 登录与退出

## 唯一认证链路

浏览器只访问认证网关：本地 `127.0.0.1:4173`，服务器 `/dcar/`。网关负责：

- 使用现有 htpasswd 账号校验登录；
- 把随机 Session ID 写入 HttpOnly Cookie，只在独立 SQLite 中保存其摘要；
- 未登录页面跳到登录页，未登录 API 返回 401；
- 登录后把 Web 请求转到 4174、API 请求转到 8765；
- 退出时删除服务端 Session 并清 Cookie，旧 Cookie 无法再次使用。

4174 和 8765 只是回环地址上的内部上游，不对公网或容器宿主发布；它们不是
浏览器入口。该边界防止外部访问绕过网关，不把同机高权限进程视为不可信租户。

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

端口分工：认证网关 4173、Web 上游 4174、只读 API 8765。

## 服务器部署

先建立认证服务的持久目录，并允许服务账号读取现有账号文件：

```sh
sudo install -d -o dcar-aigc -g dcar-aigc -m 0700 /var/lib/dcar-aigc/auth
sudo -u dcar-aigc test -r /etc/nginx/.htpasswd-dcar \
  || { sudo chgrp dcar-aigc /etc/nginx/.htpasswd-dcar; sudo chmod 0640 /etc/nginx/.htpasswd-dcar; }
```

安装三个 systemd unit 后启动 API、Web、认证网关，再加载 Nginx：

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now dcar-api dcar-web dcar-auth
curl -fsS http://127.0.0.1:4173/dcar/auth/health
sudo nginx -t && sudo systemctl reload nginx
```

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
- `nginx -t`、认证网关测试、Web 构建和真实浏览器流程均通过。

回滚代码版本时，三个服务必须一起切换和重启；不要只回滚按钮或 Nginx 配置。
