# DCar Insight Web

本地双渠道内容评估界面。页面读取 `public/data/latest-report.json` 作为离线报告快照，并通过 `http://127.0.0.1:8765` 连接本地任务服务。

当前版本只开放输入校验和缓存回归，不会自动刷新付费采集接口。

从项目根目录运行 `scripts/start_web_mvp.sh` 可同时启动网页和任务服务。

