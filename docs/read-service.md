# 独立读服务运行说明

独立读服务将账号翻页和部分统计查询从现有 writer API 进程移出。writer 继续处理所有写入、采集、调度与作业控制；读服务使用同一正式 SQLite 数据库的 WAL，只读打开，不复制 scheduler，也不迁移数据库。

## 启动与鉴权分流

使用当前系统已安装的 Python 环境。`DCAR_PROJECT_ROOT` 和 `DCAR_V8_DB` 必须与已安装 writer LaunchAgent 的正式数据库身份一致。候选代码可以位于独立目录，脚本会从自身所在目录加载代码。

```bash
export DCAR_PROJECT_ROOT=/Users/mark/Projects/DcarAIGC
export DCAR_V8_DB='/Users/mark/Library/Application Support/DcarAIGC/data/dcar_insight.sqlite3'
export DCAR_READ_API_KEY_FILE=/Users/mark/Projects/DcarAIGC/runtime/read-api/gateway.key
export DCAR_READ_PYTHON=/Users/mark/Projects/DcarAIGC/.venv/bin/python
export DCAR_READ_API_PORT=8768

scripts/start_read_api.sh --check
scripts/start_read_api.sh
```

`--check` 先验证 `FORMAL_READ`、数据库文件身份和 schema，再初始化或核验共享密钥；不会监听端口。密钥仅在文件缺失时生成，权限为 `0600`，不会打印或覆盖已有文件。读服务和网关使用同一个密钥文件。首次启动前检查 `8768` 未占用；不要停止其他服务腾出端口。

脚本通过 `env -i` 只传递列出的运行变量，不读取 writer 环境文件，不继承供应商密钥、代理变量、调度激活信息或 writer lock。固定设置 `DCAR_READ_ONLY=1`、`DCAR_SCHEDULER_ENABLED=0`、`DCAR_STARTUP_CATCHUP_ENABLED=0`、`DCAR_LLM_DISABLED=1`。

模块入口固定绑定 `127.0.0.1`、一个 worker，并关闭 Uvicorn `proxy_headers`。后者避免网关转发的真实客户端地址改变读服务的 loopback 检查。若直接运行 Uvicorn，必须保留同样设置：

```bash
python -m uvicorn v8.read_api:create_app --factory \
  --host 127.0.0.1 --port 8768 --workers 1 --no-proxy-headers
```

直接运行 Uvicorn 不提供脚本的环境清理和密钥初始化；正常运维使用启动脚本。模块没有可用于绕过正式数据库身份检查的环境变量。测试只通过进程内显式 `ReadApiConfig(test_fixture=True)` 注入临时数据库。

读服务验收后，在鉴权网关中配置：

```text
DCAR_AUTH_API_UPSTREAM=http://127.0.0.1:8766
DCAR_AUTH_READ_API_UPSTREAM=http://127.0.0.1:8768
DCAR_AUTH_READ_API_KEY_FILE=/Users/mark/Projects/DcarAIGC/runtime/read-api/gateway.key
```

本说明只定义配置合同，具体 LaunchAgent、发布目录和环境文件由部署步骤管理。本机新增服务 label 使用 `cn.tj.dcar.live-read-api`，避免与历史 `cn.tj.dcar.read-api` 混淆。writer 不需要为读服务重启或增加 worker。

网关先执行现有登录、角色、账号管理与新用户权限检查，再按精确路径和方法分流。用户伪造的 `X-Dcar-Read-Key`、`X-Dcar-Read-Scope` 会被剥离，网关注入可信密钥及“用户 + 角色”的固定长度权限范围。读服务只接受 loopback 且密钥匹配的请求；响应缓存包含权限范围，不跨身份复用。

分流白名单定义在 [`read_contract.py`](../src/dcar_eval/v8/read_contract.py)：

- `POST /api/v8/accounts/search`：旧客户端默认得到完整 Account；目录版本 2 且显式 `compact: true` 时启用列表精简合同，并返回 `list_contract_version: 1`。保留标准 writer 的已发布采集状态口径，不包含显式 `account_capture_live_status=True` 的特殊预览扫描分支。
- `GET /api/v8/accounts/directory/{directory_row_id}`：返回对应目录行的完整 Account；未找到为 `404`。标识采用 `directory_id`，不能把负 Account ID 代入。
- `POST /api/v8/contents/search`：复用既有查询参数和返回结构，读取本机媒体可用性。
- `GET /api/v8/overview`、`GET /api/v8/selling-points`：复用现有正式统计和新鲜度函数。
- `GET /api/v8/spu-audience/assets`、`GET /api/v8/spu-audience/stats`：保留 `window`、`platform` 校验与统计口径。

未列入的读取和所有写入仍发送至 writer，包括 `/api/v8/health`、scheduler、任务/媒体处理状态、导出和采集控制。读服务不挂载 writer router，不进入 writer API lifespan，不启动 provider、scheduler、迁移或 writer lock。

## 缓存、新鲜度和更新传播

缓存位于独立读服务进程内，使用 LRU，最多 128 条、总响应正文最多 16 MiB；超大结果直接返回但不保存。最多 16 个不同键在途、4 个实际计算任务。同键并发共用一次计算。等待计算槽位超过 5 秒或等待已有计算超过 60 秒时返回 `503` 和 `Retry-After: 1`；已成功的数据不会被错误或空占位结果写入缓存。

数据库连接使用 `mode=ro`、`PRAGMA query_only=ON`，并通过 `live_wal_read_only_connections()` 读取已提交 WAL。`immutable=1` 不用于本机活库。列表查询在短读事务中完成总数、当前页与投影，避免同一响应混合不同提交。

版本检测每 5 秒最多检查一次持久只读连接的 `PRAGMA data_version`。它只触发业务域语义复核，不直接成为结果缓存键。小账号元数据表按实际字段计算摘要；大表只做可以使用前导索引的独立 `MAX` 探针，避免组合 `COUNT/MAX` 扫描。多个业务域复用同一轮表摘要。无关 heartbeat 写入不会因 WAL 文件 mtime 改变而清空结果缓存。

这些探针不是事务日志，不能捕获所有保留最大值的中间行修改、删除或同秒覆盖。后端结果从计算开始最多保存 **30 秒**，命中不会延长期限；此严格 TTL 为上述未捕获变化提供兜底。后台写入一般在下一次 5 秒检测窗口被观察到，若探针未改变则到结果 TTL 截止重新读取。这里的边界描述的是读服务缓存，不代表浏览器已经刷新；前端仍需在自己的新鲜期、写入完成失效或显式刷新时发起读取。

网关收到成功的业务写入响应后，会向读服务发送定向失效通知，并强制相关域跳过检测窗口。失效发生于计算期间时，旧 generation 的结果不能重新填回缓存。若通知失败，写入成功结果仍正常交付，响应带 `X-Dcar-Read-Invalidation: deferred`，依靠检测窗口和硬 TTL 收敛。异步任务的“已接收”不等于数据已完成发布；后台提交仍由数据库检测和后续前端刷新覆盖。

每次请求检查数据库 inode；文件替换后重新执行正式身份与 schema 验证，关闭旧只读锚点、打开新文件，并清除旧 epoch 缓存。`X-Dcar-Data-Revision` 包含响应正文摘要，所以探针相同但重新计算结果已变时也会返回不同版本。

所有业务响应继续使用 HTTP `Cache-Control: private, no-store`，确保浏览器每次网络请求经过网关鉴权。它不等于禁用 React Query 或读服务进程缓存。此次没有引入共享 HTTP/CDN 缓存、Redis 或新框架。

## 验收与排查

读服务健康路由为 `GET /internal/read/health`，需 `X-Dcar-Read-Key`。它仅报告自身只读连接和进程是否就绪；不能替代 writer health、scheduler 或采集完成验收。不要把密钥放进聊天、URL、命令历史或检查日志；验收程序应从密钥文件读取并以请求头发送。

业务响应提供 `Server-Timing`：`revision`、`queue`、`compute`、`serialize`、`total`，账号路由另有查询阶段；缓存状态为 `miss`、`hit`、`coalesced`。读服务为每次请求生成独立的 `X-Dcar-Read-Request-Id`，不信任客户端传来的同名值，便于把响应与计时对应。网关附加 `gateway_proxy`，其范围包含上游等待，不能再与 reader total 相加作为总时延。

验收至少包含：

1. 原 writer 与 reader 使用相同查询条件、同一数据库提交对照返回值；剔除明确的生成时间字段后比较。账号检查全部页、筛选总数、目录顺序和完整详情；内容检查指标、标签、媒体可用性；概览检查原有 `data_freshness` 与自然日口径。
2. 浏览器实测首次翻页、缓存回翻、相邻页预取、后台刷新期间交互和详情加载。缓存命中速度不代表首次冷查询已优化。
3. 无登录、新用户、operator、管理员分别验权限，包含伪造 read headers、直接访问 reader、账户变更和退出登录。
4. 测同键并发、写入后刷新、计算中失效、缓存超过 30 秒、WAL 提交、数据库替换、读服务故障，以及 scheduler 仍只有原 writer 一份。

可重复的临时数据库测试（不访问正式库）：

```bash
PYTHONPATH=src/dcar_eval DCAR_TEST_DENY_FORMAL_DB=1 \
  /Users/mark/Projects/DcarAIGC/.venv/bin/python -m unittest \
  tests.test_v8_read_service tests.test_v8_account_listing \
  tests.test_dcar_auth_gateway tests.test_dcar_auth_account_boundary -q
```

## 回滚

1. 从网关配置中同时移除 `DCAR_AUTH_READ_API_UPSTREAM` 和 `DCAR_AUTH_READ_API_KEY_FILE`，仅重启网关。所有现有读取恢复至 `DCAR_AUTH_API_UPSTREAM`；写入路径始终未变。
2. 验证网关业务路由、writer health、scheduler 和页面数据。若同时回滚前端，使用配套旧构建；新前端在旧服务返回完整列表时应使用兼容流程，不再依赖新详情端点。
3. 停止独立 reader 的托管进程。保留密钥、日志和测试记录便于恢复，不修改或恢复 SQLite、不重启 writer、不删除 WAL/SHM 文件。

服务故障时网关不会自动把重型查询回压到 writer；确认回滚配置后再恢复旧路径。

设计参考：[FastAPI 后台重计算边界](https://fastapi.tiangolo.com/tutorial/background-tasks/#caveat)、[Uvicorn 每 worker 独立 lifespan](https://uvicorn.dev/concepts/lifespan/)、[SQLite WAL 并发与限制](https://sqlite.org/wal.html)、[HTTP 私有缓存语义](https://www.rfc-editor.org/rfc/rfc9111.html)。
