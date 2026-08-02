# Bright Data 小红书渠道验收说明

核验日期：2026-07-19

## 结论

当前结论是：**Bright Data 自助渠道 No-Go；定制销售渠道 Conditional Go**。

Bright Data 官方确实有 Xiaohongshu MCP 宣传页，明确宣称可获取公开的笔记内容、互动数据、话题标签和评论。但使用当前账号的真实 API Key 进行 Rapid 和 Pro 两条路径实测，P001 都被 Bright Data 服务端按 `robots.txt` 规则拒绝，没有取得标题、正文或任何评论。官方应用内支持助手进一步确认：当前没有公开的小红书结构化 Scraper API、Dataset 或 `dataset_id`，需要定制爬虫或销售开通。

## 已经实际验证的事项

1. Bright Data Remote MCP 服务器可达；未带 Token 时返回 HTTP `401 Unauthorized`。
2. 已从用户登录的 Bright Data 控制台安全读取 API Key，不在报告或日志中保存原值。
3. 真实 MCP `initialize` 返回 HTTP `200`，成功协商协议 `2025-06-18` 并建立 Session。
4. Rapid 模式真实返回 5 个工具：`ask_brightdata_assistant`、`search_engine`、`scrape_as_markdown`、`search_engine_batch`、`scrape_batch`；无小红书专用工具。
5. Rapid 调用 `scrape_as_markdown(P001)` 返回 `bad_endpoint`，原因是目标站点不允许 immediate access 模式。
6. Pro 模式真实返回 74 个工具，含完整浏览器操作工具，但仍无小红书专用工具。
7. Pro 调用 `scraping_browser_navigate(P001)` 返回 `brob/robots.txt restriction`，服务端明确要求联系 account manager 获取 full access。
8. 两次 P001 调用都是 MCP 业务结果 HTTP `200`，但数据结果为失败；不能把 HTTP 200 记成采集成功。
9. 当前免费账号显示 5,000/5,000 额度，但免费额度不会自动开通小红书目标权限。
10. 控制台还有“填写姓名并接受许可协议/隐私政策”的开户确认框。本次未替用户接受条款，也未绑卡或购买任何服务。

## 对宣传说法的核对

| 说法 | 核对结果 |
|---|---|
| 可采集笔记正文 | 官方小红书 MCP 页明确声明支持 |
| 可采集评论 | 宣传页声明支持，但当前账号的 Rapid 和 Pro 都在打开笔记前被拒绝 |
| 可采集点赞、分享、话题标签 | 官方页明确声明支持 |
| 有小红书专用 Scraper API | 实际工具清单无对应工具；应用内支持助手确认当前无公开产品 |
| 有现成小红书 Dataset | 应用内支持助手确认当前无公开 Dataset 或 `dataset_id` |
| 可稳定取图片和商品链接 | 小红书官方页未给出对应字段，待实测 |
| `$1/1000` 或 `$0.75/1000` | 不能作为小红书当前价格；公开 MCP/Web Scraper 按量价显示为 `$1.5/1000 results/records` |
| 现成数据集 `$250` 起 | 通用社媒评论数据集有该量级，但未证实包含小红书，不建议现在购买 |

## 当前不建议购买

Rapid 和 Pro 都已实测失败，所以现在不应购买 Dataset、套餐或流量。如果后续 Bright Data 销售或 Scraper Studio 团队提供小红书定制通道，先索取一次 P001 成功响应，确认评论字段和分页后再决定。

项目已准备安全探针 `probe_brightdata_mcp.py` 和凭据模板 `brightdata.env.example`。如果未来拿到已开通小红书权限的 Token：

1. 将模板复制到 `/Users/mark/Documents/key/DcarKey/brightdata.env`，写入：

   ```text
   BRIGHT_DATA_API_TOKEN=你的Token
   ```

2. 将文件权限设为仅当前用户可读：

   ```bash
   chmod 600 /Users/mark/Documents/key/DcarKey/brightdata.env
   ```

3. 不要在聊天中粘贴 Token。凭据目录位于 Dcar 项目之外，且文件权限应保持为 `600`。
4. 先只执行连通性和工具清单探针，不抓取笔记：

   ```bash
   cd /Users/mark/Documents/DcarAIGC
   python3 probe_brightdata_mcp.py
   ```

   探针不打印 Token、完整接口 URL 或服务端错误正文；它只把脱敏后的工具名、说明和必填参数写入 `brightdata_mcp_tools.json`。

## P001 单篇验收结果

验收对象使用 `pilot_sample_10_blind.csv` 中的 P001。

| 验收项 | 结果 |
|---|---|
| MCP 认证与会话 | 通过 |
| Rapid 页面抓取 | 失败：`bad_endpoint/robots.txt` |
| Pro 浏览器导航 | 失败：`brob/robots.txt restriction` |
| 标题/正文 | 0，未取得 |
| 评论文本 | 0，未取得 |
| 有效独立外部评论用户 | 0（不是笔记真实 0 评论，而是渠道未取到） |
| 评论分页 | 无法测试 |
| 三命题数据合同映射 | 不通过 |

## 正式启用门槛

P001 必须同时满足：

1. 真实取得标题或正文，不是仅返回页面框架。
2. 真实取得评论文本，不是仅返回“评论数”。
3. 有效独立外部评论用户不少于 20 人。
4. 评论去重和分页结果稳定。
5. 可以把原始返回稳定映射到现有三命题数据合同。
6. 单篇请求数、流量和费用可记录。

P001 已在第 1、2 项失败，因此不扩到 10 篇，也不购买数据集。只有 Bright Data 提供定制权限并让 P001 完整通过后，才重启 5 篇汽车 + 5 篇非汽车抽检。

## 官方参考

- https://brightdata.com/ai/mcp-server/xiaohongshu
- https://docs.brightdata.com/ai/mcp-server/remote/quickstart
- https://docs.brightdata.com/ai/mcp-server/tools
- https://brightdata.com/pricing/mcp-server
- https://brightdata.com/pricing/web-scraper
- https://docs.brightdata.com/datasets/scrapers/scrapers-library/overview
- https://brightdata.com/products/datasets/social-media/comments
