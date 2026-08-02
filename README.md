# DCar Insight v8

本地单用户内容运营工作台，管理账号、内容、卖点标准和不可变数据报告。系统对抖音和小红书执行账号发现、详情、实时指标、评论与本地媒体证据处理；视频号和快手首版支持人工导入。

## 当前正式基线

- 数据库：`app/data/dcar_insight.sqlite3`（SQLite schema v3）
- 报告合同：`config/report_contract_v8.json`
- 卖点标准：数据库中最新 `published` taxonomy（迁移初始版本为 `selling-points-v5.0`）
- Web：概览、任务、账号、内容、卖点五个页面
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
- `app/data/`：本地 SQLite 状态
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

打开 `http://127.0.0.1:4173`；本地 API 固定使用 `http://127.0.0.1:8765`。调度器默认启用，启动时执行有界补跑；临时禁用可在启动命令前设置 `DCAR_SCHEDULER_ENABLED=0`。

完整验证和备份流程见 `docs/v8/运行与备份手册.md`。
