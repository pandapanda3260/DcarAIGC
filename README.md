# DCar 内容评估工作流

本项目用于对抖音和小红书内容进行业务卖点、内容汽车性、互动受众汽车性与懂车帝拉新潜力评估。

## 当前正式基线

- 规则：`config/business_selling_points_v4_final.json`
- 判断标准：`config/懂车帝内容评估判断标准与流程_v4_终版.md`
- 报告：`reports/current/双渠道结构化结论报告_v6.2_TikHub_2026-08-02.md`
- Web MVP：本地、单用户、缓存优先；默认禁止刷新付费采集接口。

## 目录

- `src/dcar_eval/`：正式采集、媒体解析、评分与报告代码
- `data/inputs/`：输入链接和UID清单
- `data/cache/`：可复用采集、视频、ASR、OCR和评论缓存
- `data/processed/`：结构化中间结果
- `reports/current/`：当前正式报告
- `reports/archive/`：历史报告
- `tests/`：自动化回归测试
- `app/api/`：本地任务服务
- `app/web/`：Web MVP
- `docs/migration/`：资产清单、迁移映射和回归结果

## 本地启动

运行 `scripts/start_web_mvp.sh`，然后打开终端显示的本地地址。网页任务服务固定使用 `http://127.0.0.1:8765`。

