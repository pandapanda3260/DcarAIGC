# 快手、视频号自动内容采集接入（2026-09-12，本地）

此前这两个平台直接被标为不支持，无法进入现有作品发现、详情和指标任务。此候选补齐适配、调度、原始响应保存、指标落库、重放、费用及身份校验。暂停等人工标签仍不排除账号。没有观察期或自动停更判断。

工作目录为 `/Users/mark/Projects/DcarAIGC-worktrees/platform-capture-20260912`，分支 `codex/platform-capture-20260912`。基于 `cc6958152d18ea006c9bec1541752367c1d468e4` 加本轮前的账号导入补丁。原账号任务正在独立目录实施 schema22/统一入库，需把本轮内容采集差异合入其最终候选，不能覆盖它的整个 runtime、storage 或账号导入代码。

## 实现与字段边界

- 快手：数字 UID 查询主页、`fetch_user_post_v2` 原样保留字符串 `pcursor`，`fetch_one_video` 获取详情和五项作品指标。Python 整数转文本保持完整标识；请求 ID、返回 ID、作者 UID 均校验。HTTP `*.kwaicdn.com` 媒体地址按原签名保留。
- 视频号：短号只能先产生 finder 候选，再用账号信息中 `视频号ID` 或实际返回的 `Channels ID` 反查完全相等，最后查询该 finder 的主页。作品列表使用 POST body 的 `last_buffer`，**向后翻页以 `continueFlag` 为准，不能使用 `upContinueFlag`**。
- 视频号详情没有分享 URL 时，内部已核验的平台作品 ID 可正常落库；用户输入和其他平台不放宽。复抓空链接不抹除旧有效链接。
- 视频号主页聚合零和作品播放量零采用保守的非权威状态，规范值留空、原值在原始响应保留，不将未知信息覆盖成零。播放量处理依据真实有互动但返回零播放的样本，官方文档未保证零值代表真实零；其他作品指标的明确零保留。主页 `feedsCount` 对应作品总数，不是播放量。
- 评论暂未注册为这两个平台的自动任务。视频号加密媒体仅保留同次响应中的地址、密钥证据，未实现视频文件解密；不能宣称视频下载、播放或视频内容分析已完成。

## 验证口径

所有业务库只读。本轮使用临时测试数据库验证实际发现→详情→指标落库、账号主键/作者对应、重复执行不重复请求与插入，以及有完整主页的暂停账号生成四种任务。缺少正式发送凭据时保持 `provider_blocked`，没有伪造线上授权。

少量真实供应商请求只写本地私有文件。证据目录：

`/Users/mark/Documents/ChatGPT/DcarAIGC/outputs/01a08b5c-account-summary/enrichment/local-import/platform-integration-20260912`

`live-evidence-verification.json` 对 19 份完整实体的字节长度、SHA256、JSON 内容全部核对通过。20 次有界业务请求中 16 份响应通过操作契约，2 份业务失败、1 份非法占位输入被接口拒绝、1 次传输失败。采样器已增加输入格式检查，防止再次发送占位值；不确定请求保留 attempt 记录，重放不自动重发。

- 快手样本：5 页、100 个不同作品，详情和互动指标匹配同一作者。第 6 页传输失败，**没有完成该账号全量翻页**。
- 视频号样本：2 个账号完成短号转换→反向核对→主页→作品首屏→详情，各首屏 15 条。一个账号的续页请求完整保留 488 字符游标、服务端回显相同，但供应商返回业务错误，**不能把 15 条当该账号全部作品，也不能标为完整监控成功**。
- 本轮没有对全部 203 个账号做真实抓取验收。身份缺失、接口失败和平台功能接入是不同问题；统一账号准备流程负责逐条补齐身份和重试可恢复错误。

离线复核（输出必须使用新的文件名）：

```sh
PYTHONDONTWRITEBYTECODE=1 python scripts/verify_account_platform_probes.py \
  --evidence /absolute/platform-integration-20260912/probes \
  --report /absolute/new-verification.json
```

## 后续统一上线

1. 在统一账号候选中合入专属内容文件和相关测试；`capture_runtime.py`、`operations.py`、账号资格、预算、覆盖率和政策仅合并本轮差异，保留另一任务的 schema22 和 `profile_prepare` 改动。候选本身无需修改旧 schema18/21 DDL；schema22 由统一入库任务迁移。
2. 复用保存的完整实体和反查链，按平台＋UID 绑定；不要只迁 SQLite 而遗漏原始证据。导入前备份最终目标库，重跑应不创建重复账号。
3. 生成最终源码、来源路由 V3、账号政策 V3（含统一准备规则）、构建及真实安装凭据。快手/视频号各注册 `user_profile`、`user_posts`、`video_detail`、`video_statistics` 四种操作；正常价格与现有总预算、限流、发送前校验一起生效。
4. `capture_authorizations` 仍要求每个新操作的真实当前授权。旧 `CONTINUITY_OPERATIONS` 和历史凭据不扩大；新平台只维护明确安装的 operator release。不能使用旧抖音/小红书凭据伪装新平台已经获准发送。本次没有发布任何新 gate。
5. 用户统一上线后，按实际业务队列验证两个平台的完整分页和持续指标更新，分别统计完整、未完成、请求失败和身份缺失；采样成功不等于 203 个账号全部成功。

接口依据：TikHub 官方[快手文档](https://docs.tikhub.io/467698477e0)、[视频号作品列表](https://docs.tikhub.io/472974841e0)、[详情](https://docs.tikhub.io/472974842e0)、[主页](https://docs.tikhub.io/472974845e0)。本地价格接口核对快手主页/列表 USD 0.01，详情 USD 0.001；视频号 USD 0.01。原始价格查询记录保存在同一证据目录。
