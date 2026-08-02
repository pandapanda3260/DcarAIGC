# 抖音 UID 最近作品采集说明 v1

更新时间：2026-08-01

## 结论

可以通过抖音数字 UID 取到账号的最近公开作品。本次 4 个测试 UID 均校验成功，合计取得 60 条作品，60 个作品 ID 全部唯一，作品作者 UID 与请求 UID 的错配数为 0。

| UID | 账号 | 账号显示作品数 | 本次取得 | 是否还有更早作品 |
|---|---|---:|---:|---|
| 1619994549436234 | 懂车小宇 | 220 | 20 | 是 |
| 7634495945283077157 | 汽车零件有话说 | 62 | 21 | 是 |
| 7644454447171503163 | 拾光放映厅 | 15 | 12 | 否 |
| 7497203819535008825 | 车纪元 | 7 | 7 | 否 |

60 条标准化记录中，48 条为视频，12 条为图文。某个账号的接口实际返回 21 条，比请求的 20 条多 1 条；脚本保留服务端实际返回的完整首页。

## 使用方法

首次使用时安装唯一额外依赖：

```bash
cd /Users/mark/Documents/DcarAIGC
python3 -m pip install --target .douyin_deps -r requirements-douyin.txt
```

采集或复用缓存：

```bash
python3 collect_douyin_by_uid.py \
  1619994549436234 \
  7634495945283077157 \
  7644454447171503163 \
  7497203819535008825
```

默认优先复用已有缓存，不再请求抖音。需要重新拉取时才使用 `--refresh`：

```bash
python3 collect_douyin_by_uid.py --refresh 1619994549436234
```

## 文件与字段

- `douyin_cache/douyin_posts.jsonl`：全部标准化作品，后续“卖点”打标建议直接读取此文件。
- `douyin_cache/douyin_posts.csv`：便于人工检查和导入表格。
- `douyin_cache/douyin_accounts.jsonl`：账号名、作品数、`has_more` 等账号级信息。
- `douyin_cache/collection_summary.json`：成功数、失败数、唯一作品数和作者错配数。
- `douyin_cache/accounts/<uid>/profile_raw.json`：账号原始响应。
- `douyin_cache/accounts/<uid>/posts_page_001_raw.json`：最近首页作品原始响应。

标准化作品主要字段包括：`uid`、`account_name`、`aweme_id`、`desc`、`create_time_cn`、`content_type`、点赞/评论/收藏/分享数、`share_url`、`cover_url`、`video_url`、`image_urls`。

## 实现链路

1. 申请临时游客 `ttwid`，仅存在于进程内存。
2. 根据数字 UID 请求公开账号资料，取得 `sec_uid`。
3. 用 `sec_uid` 请求账号最近作品首页。
4. 对账号 UID 和每条作品的作者 UID 做一致性校验后才写入缓存。

脚本在本地生成网页请求所需的 `a_bogus` 签名。`douyin_abogus.py` 的实现源自 JoeanAmier/TikTokDownloader 和 Evil0ctal/Douyin_TikTok_Download_API，文件头已保留来源及许可说明。如果将脚本对外分发或用于商业系统，需另行复核 GPL 许可兼容性。

## 边界与生产建议

- 本次验证的是“最近首页”，不是完整历史。前两个账号 `has_more=true`，不能把 20/21 条当成全部作品。
- 公开网页接口属非官方开放 API 的工程化使用，参数、签名和风控策略可能变化；缓存、低频请求和重试不能消除这一风险。
- `cover_url`、`video_url` 和 `image_urls` 多为带签名的 CDN 链接，可能过期。如果后续卖点判定需要长期分析视觉内容，应在取到后及时下载并建立本地媒体索引。
- 如果需要稳定地批量翻页几百个账号，可换用 TikHub App V3：它目前提供“数字 UID 转 `sec_user_id`”和“用户作品列表”接口。正式接入前仍需用有效 token 做小额测试。
- 抖音官方 `/video/list/` 需要账号 OAuth 授权及 `video.list` 权限，适合自有账号合规接入，不适合直接查任意未授权 UID。

## 参考资料

- 抖音开放平台：<https://open.douyin.com/platform/resource/docs/openapi/video-management/douyin/search-video/account-video-list>
- TikHub UID 转 `sec_user_id`：<https://docs.tikhub.io/293991352e0>
- TikHub 用户资料（UID）：<https://docs.tikhub.io/244469113e0>
- TikHub App V3 用户作品：<https://docs.tikhub.io/186826223e0>
- 签名实现来源：<https://github.com/Evil0ctal/Douyin_TikTok_Download_API>
- 原始实现来源：<https://github.com/JoeanAmier/TikTokDownloader>
