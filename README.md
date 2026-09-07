# DCar Insight v8

本地单用户内容运营工作台，管理账号、内容、卖点标准和不可变数据报告。系统对抖音和小红书执行账号发现、详情、实时指标、评论与本地媒体证据处理；视频号和快手首版支持人工导入。

## 当前正式基线

- 代码 schema：SQLite v19 / `dual-acquisition-profile-roster-v1`；采集 profile 为 `matrix_hybrid_v1` 或 `tikhub_managed_v1`
- 正式数据库：以已安装 writer plist 的 `DCAR_V8_DB` 为唯一权威，默认位于 `$HOME/Library/Application Support/DcarAIGC/data/dcar_insight.sqlite3`；schema18→19 必须走受控封版迁移，API 运行时不会自动迁移正式库
- 报告合同：当前写入与发布使用 `config/report_contract_v8_9.json`；v8.8 及更早合同只用于验证历史只读报告
- 评估发布：`evaluation-v9__selling-points-v5.2`，taxonomy 为 `selling-points-v5.2`
- Web：概览、任务列表、任务详情、账号、内容、卖点六类真实路由
- 历史：v7 报告及 5 个 revision 只读保留，不再作为 v8 当前状态

## 目录

- `src/dcar_eval/v8/`：v8 存储、迁移、采集、媒体、评估、报告、调度和 API
- `src/dcar_eval/` 其他模块：冻结的历史评估链及 v8 复用的媒体处理器
- `data/inputs/`：输入链接和UID清单
- `data/cache/`：可复用采集、视频、ASR、OCR和评论缓存
- `data/processed/`：结构化中间结果
- `reports/runs/v8/`：v8 任务不可变 revision 产物
- `reports/current/`、`reports/archive/`：v7 历史报告
- `tests/`：自动化回归测试
- `app/data/`：开发与测试状态、历史迁移基线及本地遗留文件；不是已安装 writer 的正式数据库权威路径
- `app/web/`：v8 Web 应用
- `docs/v8/`：实施记录、合同和运行说明

## 本地启动

首次运行：

```bash
python3 -m uv sync --frozen
npm --prefix app/web ci
```

启动：

```bash
scripts/start_web_mvp.sh
```

首次启动会在终端创建本地登录账号。打开 `http://127.0.0.1:4173` 后先登录；浏览器 API 也走同一个认证入口。内部 Web 为 4174，正式 API 与唯一 scheduler 为 8766，不应绕过 4173 作为日常入口。

`scripts/start_web_mvp.sh` 只启动 Web 与认证网关，不启动第二个 API。正常模式会先验证 8766 正连接已安装 writer plist 指定的正式数据库、持有 scheduler lock、报告运行时就绪、真实注册了当天 `pipeline_reconcile`，且没有注册 `history_recovery`；任一条件不满足都失败式停止，不回退旧数据。8765 仅保留给 operator freeze 期间的只读快照 viewer。

Web 默认先构建并运行 production 版本，避免日常访问承担开发态即时编译和 HMR 开销。只有前端开发时才使用 `DCAR_WEB_MODE=dev scripts/start_web_mvp.sh`。

本地 4173 是可信操作台：页面上的显式新增、编辑、刷新和报告操作会写正式库；仅浏览或刷新卖点页不会触发供应商调用。每日调度仍只由 macOS 指定 writer 在 8766 运行。

完整验证和备份流程见 `docs/v8/运行与备份手册.md`。
