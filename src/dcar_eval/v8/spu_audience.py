"""SPU × 目标人群 × 用车场景：规则资产、内容关联与统计。

设计基线：`docs/SPU人群场景关联与统计方案_v0.1.md`。本模块实现其中的
规则链路（A 链，association-v1）与首期统计（发布条数 + 曝光量）：

- 规则资产：款型级 SPU 主数据（含车系兜底节点）、P1–P8 目标人群、
  S1–S11 用车场景、SPU→人群 与 人群→场景 两张映射表；
- 内容关联：别名召回 → 语境消歧 → 评分 → 款型细化 → 场景直判 →
  人群归因（内容显式 > 规则先验）→ 一致性标记；
- 统计：三维明细为基座，单维上卷派生；曝光沿用 v8.5 最新快照口径，
  并按"已归类曝光 ÷ 有效总曝光 ≥ 90%"决定曝光数值是否发布。

与方案文档的两处实现修正（落地时发现的口径缺口，待回写方案）：

1. 场景评分补了 36 分语义基础分（方案原文"来源分+频次分"最高只有
   50 分，永远到不了 60 分确认线）；
2. 人群显式信号需要命中 ≥2 个不同词才走 content_explicit 通道，避免
   单个泛化成本词把大量内容错归到 P1。

自动关联只处理证据等级 V2/V3 的内容：SQL 预过滤最新有效评估的
evidence_level，非 V2/V3 与无评估内容不进入循环，按"非V2/V3跳过"计数
（2026-08-15 Mark 确认口径）。执行通道是系统能力而非一次性脚本：
CLI `scripts/run_spu_association.py`（默认试算，--apply 实跑）与 API 后台
任务共用 run_association——批量预取证据等级与 ASR/OCR 路径、每批一个
事务、进度写回 spu_association_runs，服务重启时孤儿 running 行自动失效。

LLM 双轨（B 链，2026-08-16 落地，无人工复核版）：规则链跑完后，对仍未
解决的内容（车型缺失/灰区/款型未细化/场景或人群缺失）经 ``llm_hook``
调用大模型补空——规则优先、闭集选择、原文证据逐字校验、置信度门槛，
详见 `llm_assist` 模块 docstring；key 缺失或调用失败自动降级为纯规则，
刷新流程不受影响。LLM 行 rule_version='spu-llm-v1'，可一键失效回滚。
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from .storage import DEFAULT_DB, PROJECT_ROOT, connect, now_utc, transaction

LOGGER = logging.getLogger(__name__)

ASSOCIATION_RULE_VERSION = "spu-association-v2"
ASSET_SEED_VERSION = "spu-assets-v0.2"
SHANGHAI = ZoneInfo("Asia/Shanghai")

NONE_KEY = "__none__"
CONFIRM_THRESHOLD = 75
GRAY_THRESHOLD = 60
SCENE_CONFIRM_THRESHOLD = 60
SCENE_BASE_SCORE = 36
EXPLICIT_SIGNAL_MIN_HITS = 2
LOW_SAMPLE_THRESHOLD = 5
SQL_ID_CHUNK = 800  # IN (...) 分块上限，避开 SQLITE_MAX_VARIABLE_NUMBER
EXPOSURE_COVERAGE_THRESHOLD = 90.0

STAT_WINDOWS = ("all", "yesterday", "this_week", "last_week")
STAT_PLATFORMS = ("douyin", "xiaohongshu")

_DOMAIN_TABLES = (
    "spu_catalog",
    "spu_alias",
    "audience_dim",
    "scene_dim",
    "spu_audience_map",
    "audience_scene_map",
    "content_spu_links",
    "content_scene_links",
    "content_audience_links",
    "spu_association_runs",
)


class SpuAudienceError(RuntimeError):
    """Raised when the association domain cannot satisfy a request."""


# ---------------------------------------------------------------------------
# 种子资产（方案 §3；全部标注"待评审"，页面与后续迭代可改）
# ---------------------------------------------------------------------------

SEED_BASIS = "v0.1 种子：按 价格带×能源×车身×定位 规则初配，待评审"

AUDIENCE_SEEDS: Tuple[Dict[str, Any], ...] = (
    {
        "code": "P1",
        "label": "生活精算师",
        "definition": "关注油耗、保养、残值等全周期成本，拒付品牌溢价",
        "signals": ["油耗", "电耗", "养车成本", "用车成本", "保值率", "落地多少", "一公里", "每公里"],
    },
    {
        "code": "P2",
        "label": "摩登青年客",
        "definition": "重性能、配置、智能化，视车为社交表达",
        "signals": ["零百", "弹射", "改色", "氛围感", "运动感", "推背感"],
    },
    {
        "code": "P3",
        "label": "潮奢精质族",
        "definition": "追求高端品质与品牌调性",
        "signals": ["豪华感", "行政", "气场", "商务范", "质感拉满"],
    },
    {
        "code": "P4",
        "label": "优价品质流",
        "definition": "预算内追求品质均衡",
        "signals": ["性价比", "同价位", "值不值", "预算有限", "高性价比"],
    },
    {
        "code": "P5",
        "label": "温馨生活家",
        "definition": "家庭用户；全家出行、亲子、长途返乡",
        "signals": ["宝妈", "带娃", "二胎", "家用", "六座", "奶爸", "一家人", "家庭用车"],
    },
    {
        "code": "P6",
        "label": "科技先锋派",
        "definition": "智驾、智能座舱尝鲜",
        "signals": ["智驾", "城市NOA", "激光雷达", "车机", "智能座舱", "辅助驾驶", "自动泊车"],
    },
    {
        "code": "P7",
        "label": "野趣体验官",
        "definition": "越野、露营户外玩家；含硬核探险与露营漫游",
        "signals": ["越野", "穿越", "露营", "车宿", "床车", "自驾进藏", "老炮"],
    },
    {
        "code": "P8",
        "label": "实干奋斗者",
        "definition": "营运与创富用途",
        "signals": ["跑网约", "跑滴滴", "拉货", "摆摊", "回本", "营运", "进货"],
    },
)

SCENE_SEEDS: Tuple[Dict[str, Any], ...] = (
    {"code": "S1", "label": "通勤代步", "definition": "日常上下班与市区代步", "triggers": ["通勤", "上下班", "代步", "市区开", "买菜车"], "negatives": []},
    {"code": "S2", "label": "家庭亲子", "definition": "带娃与全家出行", "triggers": ["带娃", "亲子", "全家出行", "二胎", "儿童座椅", "一家人"], "negatives": []},
    {"code": "S3", "label": "自驾长途", "definition": "自驾游与长途高速", "triggers": ["自驾游", "自驾", "长途", "跑高速", "返乡", "川藏", "环线"], "negatives": []},
    {"code": "S4", "label": "越野穿越", "definition": "真实越野与脱困穿越", "triggers": ["越野", "穿越", "脱困", "爬坡", "沙漠", "差速锁", "涉水"], "negatives": ["越野风格", "越野套件", "越野外观"]},
    {"code": "S5", "label": "露营车宿", "definition": "露营、车宿与户外用电", "triggers": ["露营", "车宿", "床车", "外放电", "天幕", "帐篷"], "negatives": []},
    {
        "code": "S6",
        "label": "商务接待",
        "definition": "商务接待、客户接送与行政用车",
        "triggers": ["商务", "接待", "行政", "客户接送"],
        "negatives": [],
    },
    {"code": "S7", "label": "网约营运", "definition": "网约车与营运接单", "triggers": ["网约车", "跑滴滴", "营运", "接单", "跑单"], "negatives": []},
    {
        "code": "S8",
        "label": "拉货创富",
        "definition": "经营载货、货运、摆摊与小生意",
        "triggers": ["拉货", "载货", "货运", "商用运输", "摆摊", "进货", "小生意", "创业"],
        "negatives": [],
    },
    {"code": "S9", "label": "宠物出行", "definition": "携宠出行与宠物模式", "triggers": ["宠物", "带狗", "猫咪", "宠物模式", "毛孩子"], "negatives": []},
    {"code": "S10", "label": "改装玩车", "definition": "改装与玩车社群", "triggers": ["改装", "爆改", "姿态", "刷ECU", "车友聚会", "轮毂"], "negatives": []},
    {"code": "S11", "label": "新手练手", "definition": "新手第一台车与练手", "triggers": ["新手", "练手", "刚拿驾照", "第一台车", "新手司机"], "negatives": []},
)

AUDIENCE_SCENE_SEEDS: Tuple[Tuple[str, str, str], ...] = (
    ("P1", "S1", "core"), ("P1", "S3", "related"), ("P1", "S11", "related"),
    ("P2", "S10", "core"), ("P2", "S1", "related"), ("P2", "S3", "related"),
    ("P3", "S6", "core"), ("P3", "S3", "related"),
    ("P4", "S1", "core"), ("P4", "S2", "core"), ("P4", "S3", "related"), ("P4", "S11", "related"),
    ("P5", "S2", "core"), ("P5", "S3", "core"), ("P5", "S1", "related"), ("P5", "S9", "related"),
    ("P6", "S1", "core"), ("P6", "S3", "core"), ("P6", "S2", "related"),
    ("P7", "S4", "core"), ("P7", "S5", "core"), ("P7", "S3", "related"), ("P7", "S10", "related"),
    ("P8", "S7", "core"), ("P8", "S8", "core"), ("P8", "S1", "related"),
)

# v0.1 把「商务接待」和「经营载货」合并成 S6，导致 P3 豪华车系的
# core 场景文案带上「载货」。v0.2 保留 S6 编号表示商务接待，并把载货
# 语义收敛到已有的 S8，避免新增 S 码破坏历史链接和前端闭集。旧种子
# 指纹只用于安全升级：如果用户已人工改过该行，ensure_assets 不会覆盖。
_LEGACY_SCENE_SEEDS: Tuple[Dict[str, Any], ...] = (
    {
        "code": "S6",
        "label": "商务接待载货",
        "definition": "商务接待与经营载货",
        "triggers": ["商务", "接待", "行政", "载货", "商用"],
        "negatives": [],
    },
    {
        "code": "S8",
        "label": "拉货创富",
        "definition": "拉货、摆摊与小生意",
        "triggers": ["拉货", "摆摊", "进货", "小生意", "创业"],
        "negatives": [],
    },
)

_SCENE_SEED_BY_CODE = {str(scene["code"]): scene for scene in SCENE_SEEDS}


def _scene_matches_seed(scene: Dict[str, Any], seed: Dict[str, Any]) -> bool:
    """仅识别完整的历史种子指纹，避免覆盖页面上的人工修订。"""

    return (
        str(scene.get("code") or "") == str(seed["code"])
        and str(scene.get("label") or "") == str(seed["label"])
        and str(scene.get("definition") or "") == str(seed["definition"])
        and list(scene.get("triggers") or []) == list(seed["triggers"])
        and list(scene.get("negatives") or []) == list(seed["negatives"])
    )


def _canonicalize_legacy_scene(scene: Dict[str, Any]) -> Dict[str, Any]:
    """读取旧库时即时投影 v0.2 语义，不要求先对正式库执行写迁移。"""

    for legacy in _LEGACY_SCENE_SEEDS:
        if _scene_matches_seed(scene, legacy):
            current = _SCENE_SEED_BY_CODE[str(legacy["code"])]
            return {
                **scene,
                "label": str(current["label"]),
                "definition": str(current["definition"]),
                "triggers": list(current["triggers"]),
                "negatives": list(current["negatives"]),
            }
    return scene


def _upgrade_legacy_scene_seeds(connection: sqlite3.Connection) -> int:
    """将未经人工修订的 v0.1 场景种子幂等升级到 v0.2。"""

    changed = 0
    for legacy in _LEGACY_SCENE_SEEDS:
        row = connection.execute(
            "SELECT * FROM scene_dim WHERE code=?", (legacy["code"],)
        ).fetchone()
        if row is None:
            continue
        candidate = dict(row)
        candidate["triggers"] = _decode_list(candidate.get("trigger_words_json"))
        candidate["negatives"] = _decode_list(candidate.get("negative_words_json"))
        if not _scene_matches_seed(candidate, legacy):
            continue
        current = _SCENE_SEED_BY_CODE[str(legacy["code"])]
        cursor = connection.execute(
            """
            UPDATE scene_dim
            SET label=?, definition=?, trigger_words_json=?, negative_words_json=?
            WHERE code=?
            """,
            (
                current["label"],
                current["definition"],
                json.dumps(current["triggers"], ensure_ascii=False),
                json.dumps(current["negatives"], ensure_ascii=False),
                current["code"],
            ),
        )
        changed += int(cursor.rowcount)
    return changed

# 车系种子：slug, 品牌, 车系, 车身, 能源, 指导价带(万, 粗略), primary人群,
# secondary人群(可空), 别名列表[(词, 类型, 歧义)]。价格带仅用于款型价格锚，
# 数值为粗略种子，需人工修订。
_S = "series"
SPU_SERIES_SEEDS: Tuple[Tuple[str, str, str, str, str, float, float, str, str, Tuple[Tuple[str, str, int], ...]], ...] = (
    ("byd__qin-plus", "比亚迪", "秦PLUS", "轿车", "phev", 7.0, 14.0, "P1", "P4", (("秦PLUS", "official", 0), ("秦plus", "official", 0), ("秦", "official", 1))),
    ("byd__song-plus", "比亚迪", "宋PLUS", "SUV", "phev", 13.0, 20.0, "P5", "P1", (("宋PLUS", "official", 0), ("宋plus", "official", 0), ("宋", "official", 1))),
    ("byd__seagull", "比亚迪", "海鸥", "轿车", "ev", 6.0, 9.0, "P1", "", (("海鸥", "official", 1),)),
    ("byd__dolphin", "比亚迪", "海豚", "轿车", "ev", 9.0, 13.0, "P1", "", (("海豚", "official", 1),)),
    ("byd__han", "比亚迪", "汉", "轿车", "phev", 16.0, 30.0, "P3", "P6", (("比亚迪汉", "official", 0), ("汉EV", "official", 0), ("汉DM", "official", 0), ("汉", "official", 1))),
    ("byd__tang", "比亚迪", "唐", "SUV", "phev", 17.0, 30.0, "P5", "", (("比亚迪唐", "official", 0), ("唐DM", "official", 0), ("唐", "official", 1))),
    ("tesla__model-3", "特斯拉", "Model 3", "轿车", "ev", 23.0, 34.0, "P6", "P2", (("Model 3", "official", 0), ("model3", "official", 0), ("毛豆3", "nickname", 0))),
    ("tesla__model-y", "特斯拉", "Model Y", "SUV", "ev", 24.0, 35.0, "P6", "P5", (("Model Y", "official", 0), ("modely", "official", 0), ("毛豆Y", "nickname", 0))),
    ("lixiang__l6", "理想", "理想L6", "SUV", "erev", 24.0, 28.0, "P5", "P6", (("理想L6", "official", 0), ("L6", "model_code", 1))),
    ("lixiang__l7", "理想", "理想L7", "SUV", "erev", 28.0, 34.0, "P5", "P6", (("理想L7", "official", 0), ("L7", "model_code", 1))),
    ("lixiang__l9", "理想", "理想L9", "SUV", "erev", 40.0, 46.0, "P5", "P3", (("理想L9", "official", 0), ("L9", "model_code", 1))),
    ("aito__m7", "问界", "问界M7", "SUV", "erev", 24.0, 33.0, "P5", "P6", (("问界M7", "official", 0), ("AITO M7", "official", 0), ("M7", "model_code", 1))),
    ("aito__m9", "问界", "问界M9", "SUV", "erev", 46.0, 57.0, "P3", "P6", (("问界M9", "official", 0), ("M9", "model_code", 1))),
    ("xiaomi__su7", "小米", "小米SU7", "轿车", "ev", 21.0, 30.0, "P2", "P6", (("小米SU7", "official", 0), ("SU7", "official", 0), ("米时捷", "nickname", 0))),
    ("tank__300", "坦克", "坦克300", "SUV", "ice", 20.0, 25.0, "P7", "P2", (("坦克300", "official", 0), ("坦克三百", "nickname", 0))),
    ("haval__h6", "哈弗", "哈弗H6", "SUV", "ice", 10.0, 15.0, "P4", "", (("哈弗H6", "official", 0), ("H6", "model_code", 1))),
    ("haval__dagou", "哈弗", "哈弗大狗", "SUV", "ice", 12.0, 16.0, "P7", "P4", (("哈弗大狗", "official", 0), ("大狗", "nickname", 1))),
    ("wuling__hongguang-miniev", "五菱", "宏光MINIEV", "微型车", "ev", 3.0, 7.0, "P1", "P8", (("宏光MINIEV", "official", 0), ("宏光mini", "official", 0), ("MINIEV", "official", 0))),
    ("geely__xingyue-l", "吉利", "星越L", "SUV", "ice", 14.0, 19.0, "P4", "", (("星越L", "official", 0),)),
    ("zeekr__001", "极氪", "极氪001", "轿车", "ev", 26.0, 33.0, "P2", "P6", (("极氪001", "official", 0),)),
    ("changan__cs75-plus", "长安", "CS75 PLUS", "SUV", "ice", 11.0, 15.0, "P4", "", (("CS75PLUS", "official", 0), ("CS75 PLUS", "official", 0), ("CS75", "official", 0))),
    ("nio__es6", "蔚来", "蔚来ES6", "SUV", "ev", 33.0, 40.0, "P3", "P6", (("蔚来ES6", "official", 0), ("ES6", "model_code", 1))),
    ("xpeng__mona-m03", "小鹏", "MONA M03", "轿车", "ev", 12.0, 16.0, "P1", "P6", (("MONA M03", "official", 0), ("M03", "official", 0), ("mona", "official", 0))),
    ("xpeng__g6", "小鹏", "小鹏G6", "SUV", "ev", 18.0, 25.0, "P6", "", (("小鹏G6", "official", 0), ("G6", "model_code", 1))),
    ("benz__c-class", "奔驰", "奔驰C级", "轿车", "ice", 33.0, 38.0, "P3", "", (("奔驰C级", "official", 0), ("奔驰C", "official", 0), ("C级", "official", 1))),
    ("benz__glc", "奔驰", "奔驰GLC", "SUV", "ice", 42.0, 53.0, "P3", "", (("奔驰GLC", "official", 0), ("GLC", "official", 0))),
    ("bmw__3-series", "宝马", "宝马3系", "轿车", "ice", 30.0, 40.0, "P2", "P3", (("宝马3系", "official", 0), ("三系", "nickname", 1), ("325", "model_code", 1))),
    ("bmw__5-series", "宝马", "宝马5系", "轿车", "ice", 44.0, 55.0, "P3", "", (("宝马5系", "official", 0), ("五系", "nickname", 1))),
    ("bmw__x3", "宝马", "宝马X3", "SUV", "ice", 40.0, 48.0, "P3", "", (("宝马X3", "official", 0), ("X3", "model_code", 1))),
    ("audi__a4l", "奥迪", "奥迪A4L", "轿车", "ice", 32.0, 40.0, "P3", "P2", (("奥迪A4L", "official", 0), ("A4L", "official", 0))),
    ("audi__a6l", "奥迪", "奥迪A6L", "轿车", "ice", 42.0, 55.0, "P3", "", (("奥迪A6L", "official", 0), ("A6L", "official", 0))),
    ("audi__q5l", "奥迪", "奥迪Q5L", "SUV", "ice", 40.0, 48.0, "P3", "", (("奥迪Q5L", "official", 0), ("Q5L", "official", 0))),
    ("vw__lavida", "大众", "朗逸", "轿车", "ice", 8.0, 13.0, "P1", "", (("朗逸", "official", 0),)),
    ("vw__sagitar", "大众", "速腾", "轿车", "ice", 10.0, 15.0, "P1", "P4", (("速腾", "official", 0),)),
    ("vw__tiguan-l", "大众", "途观L", "SUV", "ice", 17.0, 26.0, "P5", "P4", (("途观L", "official", 0), ("途观", "official", 0))),
    ("toyota__corolla", "丰田", "卡罗拉", "轿车", "hev", 9.0, 14.0, "P1", "", (("卡罗拉", "official", 0),)),
    ("toyota__camry", "丰田", "凯美瑞", "轿车", "hev", 17.0, 25.0, "P4", "P3", (("凯美瑞", "official", 0),)),
    ("toyota__highlander", "丰田", "汉兰达", "SUV", "hev", 25.0, 33.0, "P5", "", (("汉兰达", "official", 0), ("大汉", "nickname", 1))),
    ("toyota__prado", "丰田", "普拉多", "SUV", "ice", 45.0, 56.0, "P7", "P3", (("普拉多", "official", 0), ("霸道", "nickname", 1))),
    ("toyota__land-cruiser", "丰田", "兰德酷路泽", "SUV", "ice", 60.0, 80.0, "P7", "P3", (("兰德酷路泽", "official", 0), ("陆巡", "nickname", 0), ("LC300", "model_code", 0))),
    ("honda__civic", "本田", "思域", "轿车", "ice", 12.0, 17.0, "P2", "P1", (("思域", "official", 0),)),
    ("honda__accord", "本田", "雅阁", "轿车", "ice", 17.0, 23.0, "P4", "P3", (("雅阁", "official", 0),)),
    ("honda__crv", "本田", "CR-V", "SUV", "ice", 18.0, 25.0, "P5", "P4", (("CR-V", "official", 0), ("CRV", "official", 0))),
    ("nissan__sylphy", "日产", "轩逸", "轿车", "ice", 8.0, 13.0, "P1", "", (("轩逸", "official", 0),)),
)

# 购车语境词（消歧条件 c 与语境加分共用）
CONTEXT_WORDS: Tuple[str, ...] = (
    "试驾", "续航", "油耗", "电耗", "落地价", "提车", "内饰", "百公里", "指导价",
    "优惠", "订车", "选车", "买车", "配置", "底盘", "空间", "方向盘", "车机",
    "充电", "保养", "二手车", "新车", "上市", "颗粒", "马力", "变速箱",
)

# 购车语境加分词（语境分 +10 的判断子集）
PURCHASE_WORDS: Tuple[str, ...] = ("落地价", "指导价", "优惠", "提车", "订车", "对比", "试驾", "报价")

# 款型细化：年款样式
YEAR_TOKEN_RE = re.compile(r"(20\d{2})\s*款")
PRICE_TOKEN_RE = re.compile(r"(\d{1,3}(?:\.\d{1,2})?)\s*万")


def domain_ready(connection: sqlite3.Connection) -> bool:
    """判断数据库是否已具备 v14 关联域表（只读副本可能仍是 v13）。"""

    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name IN ({})".format(
            ",".join("?" for _ in _DOMAIN_TABLES)
        ),
        _DOMAIN_TABLES,
    ).fetchall()
    return len(rows) == len(_DOMAIN_TABLES)


def _require_domain(connection: sqlite3.Connection) -> None:
    if not domain_ready(connection):
        raise SpuAudienceError(
            "当前数据库版本太旧，车型人群功能暂时不可用。请先更新本地数据后再试。"
        )


# ---------------------------------------------------------------------------
# 种子写入（幂等，仅补缺不覆盖人工修改）
# ---------------------------------------------------------------------------

def ensure_assets(connection: sqlite3.Connection) -> None:
    _require_domain(connection)
    captured_at = now_utc()
    for audience in AUDIENCE_SEEDS:
        connection.execute(
            """
            INSERT OR IGNORE INTO audience_dim(code, label, definition, explicit_signals_json, ref_mapping_json)
            VALUES (?, ?, ?, ?, '{}')
            """,
            (
                audience["code"],
                audience["label"],
                audience["definition"],
                json.dumps(audience["signals"], ensure_ascii=False),
            ),
        )
    for scene in SCENE_SEEDS:
        connection.execute(
            """
            INSERT OR IGNORE INTO scene_dim(code, label, definition, trigger_words_json, negative_words_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                scene["code"],
                scene["label"],
                scene["definition"],
                json.dumps(scene["triggers"], ensure_ascii=False),
                json.dumps(scene["negatives"], ensure_ascii=False),
            ),
        )
    _upgrade_legacy_scene_seeds(connection)
    for audience_code, scene_code, tier in AUDIENCE_SCENE_SEEDS:
        connection.execute(
            """
            INSERT OR IGNORE INTO audience_scene_map(audience_code, scene_code, tier, basis)
            VALUES (?, ?, ?, ?)
            """,
            (audience_code, scene_code, tier, SEED_BASIS),
        )
    for slug, brand, series, body_style, powertrain, low, high, primary, secondary, aliases in SPU_SERIES_SEEDS:
        connection.execute(
            """
            INSERT OR IGNORE INTO spu_catalog(
                spu_id, brand, series, series_slug, trim_label, is_series_node,
                model_year, powertrain, body_style, price_low, price_high,
                external_ref, enabled, created_at, updated_at
            ) VALUES (?, ?, ?, ?, NULL, 1, NULL, ?, ?, ?, ?, '', 1, ?, ?)
            """,
            (slug, brand, series, slug, powertrain, body_style, low, high, captured_at, captured_at),
        )
        for alias, alias_type, ambiguous in aliases:
            connection.execute(
                """
                INSERT OR IGNORE INTO spu_alias(alias, alias_type, spu_scope, spu_id, ambiguous, enabled)
                VALUES (?, ?, 'series', ?, ?, 1)
                """,
                (alias, alias_type, slug, ambiguous),
            )
        if primary:
            connection.execute(
                """
                INSERT OR IGNORE INTO spu_audience_map(scope, scope_key, audience_code, role, weight, basis)
                VALUES ('series', ?, ?, 'primary', 1.0, ?)
                """,
                (slug, primary, SEED_BASIS),
            )
        if secondary:
            connection.execute(
                """
                INSERT OR IGNORE INTO spu_audience_map(scope, scope_key, audience_code, role, weight, basis)
                VALUES ('series', ?, ?, 'secondary', 0.5, ?)
                """,
                (slug, secondary, SEED_BASIS),
            )


# ---------------------------------------------------------------------------
# 资产读取
# ---------------------------------------------------------------------------

def _decode_list(value: Any) -> List[str]:
    try:
        decoded = json.loads(str(value or "[]"))
    except (json.JSONDecodeError, TypeError, ValueError):
        return []
    return [str(item) for item in decoded] if isinstance(decoded, list) else []


def _load_assets(connection: sqlite3.Connection) -> Dict[str, Any]:
    audiences = [dict(row) for row in connection.execute(
        "SELECT * FROM audience_dim WHERE enabled=1 ORDER BY code"
    )]
    for audience in audiences:
        audience["signals"] = _decode_list(audience.pop("explicit_signals_json"))
        audience.pop("ref_mapping_json", None)
    scenes = [dict(row) for row in connection.execute(
        "SELECT * FROM scene_dim WHERE enabled=1 ORDER BY code"
    )]
    for scene in scenes:
        scene["triggers"] = _decode_list(scene.pop("trigger_words_json"))
        scene["negatives"] = _decode_list(scene.pop("negative_words_json"))
        scene.update(_canonicalize_legacy_scene(scene))
    catalog = {
        str(row["spu_id"]): dict(row)
        for row in connection.execute("SELECT * FROM spu_catalog WHERE enabled=1")
    }
    aliases = [
        dict(row)
        for row in connection.execute(
            """
            SELECT sa.* FROM spu_alias sa
            JOIN spu_catalog sc ON sc.spu_id=sa.spu_id
            WHERE sa.enabled=1 AND sc.enabled=1
            """
        )
    ]
    audience_map: Dict[Tuple[str, str], Dict[str, str]] = {}
    for row in connection.execute("SELECT * FROM spu_audience_map"):
        key = (str(row["scope"]), str(row["scope_key"]))
        audience_map.setdefault(key, {})[str(row["role"])] = str(row["audience_code"])
    scene_map: Dict[str, Dict[str, set]] = {}
    for row in connection.execute("SELECT * FROM audience_scene_map"):
        entry = scene_map.setdefault(str(row["audience_code"]), {"core": set(), "related": set()})
        entry[str(row["tier"])].add(str(row["scene_code"]))
    return {
        "audiences": audiences,
        "scenes": scenes,
        "catalog": catalog,
        "aliases": aliases,
        "audience_map": audience_map,
        "scene_map": scene_map,
    }


# ---------------------------------------------------------------------------
# 证据文本装配（与 evaluation 的 artifact 读取口径一致）
# ---------------------------------------------------------------------------

def _resolved_path(local_path: str) -> Path:
    path = Path(local_path)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _read_json_file(path: Optional[Path]) -> Dict[str, Any]:
    if path is None or not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _eligible_v23_contents(
    connection: sqlite3.Connection,
    *,
    limit: Optional[int] = None,
    since: Optional[str] = None,
    window: Optional[str] = None,
) -> List[sqlite3.Row]:
    """一次窗口查询取出 V2/V3 已发布内容——关联的唯一处理范围。

    非 V2/V3 与无评估内容不进入返回集，也就不进入关联循环。
    ``since`` 为增量口径：只取最新有效评估时间不早于该时刻的内容
    （新进内容、重评估内容），供自动补算使用。
    ``window`` 为范围重算口径：只取发布时间落在统计窗口（昨天/本周/上周，
    上海时区，与页面统计口径同源 ``_window_bounds``）内的内容。
    """

    limit_sql = f"LIMIT {int(limit)}" if limit else ""
    since_sql = "AND le.evaluated_at >= ?" if since else ""
    parameters: List[Any] = [since] if since else []
    window_sql = ""
    if window and window != "all":
        bounds = _window_bounds(window)
        if bounds is not None:
            window_sql = "AND c.published_at >= ? AND c.published_at < ?"
            parameters.extend(bounds)
    return connection.execute(
        f"""
        WITH latest_eval AS (
            SELECT content_id, evidence_level, evaluated_at,
                   ROW_NUMBER() OVER (
                       PARTITION BY content_id
                       ORDER BY evaluated_at DESC, id DESC
                   ) selector_rank
            FROM evaluation_versions
            WHERE invalidated_at IS NULL
        )
        SELECT c.id, c.title, c.body, le.evidence_level
        FROM content_items c
        JOIN latest_eval le ON le.content_id=c.id AND le.selector_rank=1
        WHERE c.published_at IS NOT NULL AND le.evidence_level IN ('V2','V3')
          {since_sql}
          {window_sql}
        ORDER BY c.id
        {limit_sql}
        """,
        parameters,
    ).fetchall()


def resolve_incremental_since(connection: sqlite3.Connection) -> Optional[str]:
    """增量补算的起点：最近一次成功刷新的开始时刻；从未成功过则返回 None（走全量）。"""

    row = connection.execute(
        "SELECT started_at FROM spu_association_runs WHERE status='succeeded' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return str(row["started_at"]) if row is not None else None


def _artifact_paths_by_content(
    connection: sqlite3.Connection,
) -> Dict[int, Dict[str, str]]:
    """批量预取每条内容最新可用的 ASR/OCR 证据文件路径，替代逐条点查。"""

    mapping: Dict[int, Dict[str, str]] = {}
    for row in connection.execute(
        """
        SELECT content_id, artifact_kind, local_path FROM (
            SELECT ea.content_id, ea.local_path,
                   CASE WHEN ea.artifact_type IN ('asr','transcript','media_transcript')
                        THEN 'asr' ELSE 'ocr' END artifact_kind,
                   ROW_NUMBER() OVER (
                       PARTITION BY ea.content_id,
                           CASE WHEN ea.artifact_type IN ('asr','transcript','media_transcript')
                                THEN 'asr' ELSE 'ocr' END
                       ORDER BY ea.id DESC
                   ) selector_rank
            FROM evidence_artifacts ea
            WHERE ea.status='available' AND ea.sha256 IS NOT NULL
              AND ea.artifact_type IN
                  ('asr','transcript','media_transcript','ocr','media_ocr')
        ) WHERE selector_rank=1
        """
    ):
        if row["local_path"]:
            entry = mapping.setdefault(int(row["content_id"]), {})
            entry[str(row["artifact_kind"])] = str(row["local_path"])
    return mapping


def _assemble_texts(
    content: sqlite3.Row | Mapping[str, Any], paths: Mapping[str, str]
) -> Dict[str, str]:
    asr_payload = (
        _read_json_file(_resolved_path(paths["asr"])) if "asr" in paths else {}
    )
    ocr_payload = (
        _read_json_file(_resolved_path(paths["ocr"])) if "ocr" in paths else {}
    )
    return {
        "title": str(content["title"] or ""),
        "body": str(content["body"] or ""),
        "asr": str(asr_payload.get("text") or ""),
        "ocr": str(ocr_payload.get("combined_text") or ""),
    }


# ---------------------------------------------------------------------------
# 匹配核心（纯函数，便于定标与单测）
# ---------------------------------------------------------------------------

_CHANNEL_ORDER = ("asr", "ocr", "title", "body")


def _occurrences(text: str, word: str) -> int:
    if not word:
        return 0
    return text.lower().count(word.lower())


def _channel_hits(texts: Dict[str, str], word: str) -> Dict[str, int]:
    hits: Dict[str, int] = {}
    for channel in _CHANNEL_ORDER:
        count = _occurrences(texts.get(channel, ""), word)
        if count:
            hits[channel] = count
    return hits


def _source_score(channels: Sequence[str], evidence_level: str) -> int:
    if "asr" in channels or "ocr" in channels:
        return 30 if evidence_level == "V3" else 24
    if "title" in channels:
        return 18
    if "body" in channels:
        return 12
    return 0


def _frequency_score(total: int) -> int:
    if total >= 3:
        return 20
    if total == 2:
        return 14
    if total == 1:
        return 8
    return 0


def _evidence_cap(evidence_level: str) -> int:
    return 100 if evidence_level == "V3" else 90


def _window_has_qualifier(text: str, position: int, length: int) -> bool:
    lowered = text.lower()
    start = max(0, position - 5)
    end = min(len(lowered), position + length + 5)
    window = lowered[start:end]
    qualifiers = ("dm-i", "dmi", "ev", "plus", "pro", "max", "冠军版", "款", "混动", "四驱")
    return any(token in window for token in qualifiers)


def match_series(
    texts: Dict[str, str],
    evidence_level: str,
    assets: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """别名召回 → 语境消歧 → 车系评分；返回按分数排序的车系候选。"""

    catalog = assets["catalog"]
    full_text = "\n".join(texts.get(channel, "") for channel in _CHANNEL_ORDER)
    context_hits = sum(1 for word in CONTEXT_WORDS if _occurrences(full_text, word))
    per_series: Dict[str, Dict[str, Any]] = {}
    for alias_row in assets["aliases"]:
        if str(alias_row["spu_scope"]) != "series":
            continue
        alias = str(alias_row["alias"])
        spu = catalog.get(str(alias_row["spu_id"]))
        if spu is None:
            continue
        hits = _channel_hits(texts, alias)
        if not hits:
            continue
        if int(alias_row["ambiguous"]):
            brand = str(spu["brand"])
            confirmed = False
            if brand and _occurrences(full_text, brand):
                confirmed = True
            if not confirmed:
                for channel in hits:
                    channel_text = texts.get(channel, "")
                    position = channel_text.lower().find(alias.lower())
                    if position >= 0 and _window_has_qualifier(channel_text, position, len(alias)):
                        confirmed = True
                        break
            if not confirmed and context_hits >= 2:
                confirmed = True
            if not confirmed:
                continue
        slug = str(spu["series_slug"])
        entry = per_series.setdefault(
            slug,
            {"series_slug": slug, "brand": str(spu["brand"]), "series": str(spu["series"]),
             "channels": {}, "aliases": [], "total": 0},
        )
        entry["aliases"].append(alias)
        for channel, count in hits.items():
            entry["channels"][channel] = entry["channels"].get(channel, 0) + count
            entry["total"] += count

    candidates: List[Dict[str, Any]] = []
    for slug, entry in per_series.items():
        channels = list(entry["channels"])
        source = _source_score(channels, evidence_level)
        frequency = _frequency_score(int(entry["total"]))
        if len(per_series) == 1:
            uniqueness = 25
        elif "title" in channels:
            uniqueness = 18
        else:
            uniqueness = 8
        context = 0
        if entry["brand"] and _occurrences(full_text, entry["brand"]):
            context += 15
        if any(_occurrences(full_text, word) for word in PURCHASE_WORDS):
            context += 10
        context = min(context, 25)
        score = min(source + frequency + uniqueness + context, _evidence_cap(evidence_level))
        entry["score"] = score
        entry["evidence"] = {
            "aliases": sorted(set(entry["aliases"])),
            "channels": entry["channels"],
            "source": source,
            "frequency": frequency,
            "uniqueness": uniqueness,
            "context": context,
        }
        candidates.append(entry)
    candidates.sort(key=lambda item: (-int(item["score"]), str(item["series_slug"])))
    return candidates


def resolve_trim(
    texts: Dict[str, str],
    series_slug: str,
    assets: Dict[str, Any],
) -> Optional[str]:
    """款型细化：款型别名 / 年款 / 价格锚 三类信号，唯一最优才落款型。"""

    catalog = assets["catalog"]
    trims = [
        spu for spu in catalog.values()
        if str(spu["series_slug"]) == series_slug and not int(spu["is_series_node"])
    ]
    if not trims:
        return None
    full_text = "\n".join(texts.get(channel, "") for channel in _CHANNEL_ORDER)
    trim_alias_hits: Dict[str, int] = {}
    for alias_row in assets["aliases"]:
        if str(alias_row["spu_scope"]) != "trim":
            continue
        spu = catalog.get(str(alias_row["spu_id"]))
        if spu is None or str(spu["series_slug"]) != series_slug:
            continue
        if _occurrences(full_text, str(alias_row["alias"])):
            trim_alias_hits[str(spu["spu_id"])] = trim_alias_hits.get(str(spu["spu_id"]), 0) + 1
    years = {int(match) for match in YEAR_TOKEN_RE.findall(full_text)}
    prices = [float(match) for match in PRICE_TOKEN_RE.findall(full_text)]
    scored: List[Tuple[int, str]] = []
    for spu in trims:
        signals = 0
        spu_id = str(spu["spu_id"])
        alias_signal = bool(trim_alias_hits.get(spu_id))
        year_signal = (
            spu["model_year"] is not None and int(spu["model_year"]) in years
        )
        low = spu["price_low"]
        high = spu["price_high"]
        price_signal = bool(
            low is not None
            and high is not None
            and prices
            and any(float(low) * 0.9 <= price <= float(high) * 1.1 for price in prices)
        )
        if alias_signal:
            signals += 1
        if year_signal:
            signals += 1
        if price_signal:
            signals += 1
        # 别名是强信号；没有别名时，年款与价格必须同时成立。价格或年款
        # 单独命中只代表大致区间，不能把车系内容猜成某个具体款型。
        if alias_signal or (year_signal and price_signal):
            scored.append((signals, spu_id))
    if not scored:
        return None
    scored.sort(key=lambda item: (-item[0], item[1]))
    if len(scored) > 1 and scored[0][0] == scored[1][0]:
        return None
    return scored[0][1]


def match_scenes(
    texts: Dict[str, str],
    evidence_level: str,
    assets: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """场景直判：36 分语义基础分 + 来源分 + 频次分，负向词收紧确认门槛。"""

    results: List[Dict[str, Any]] = []
    full_text = "\n".join(texts.get(channel, "") for channel in _CHANNEL_ORDER)
    for scene in assets["scenes"]:
        hit_words: Dict[str, Dict[str, int]] = {}
        for word in scene["triggers"]:
            hits = _channel_hits(texts, word)
            if hits:
                hit_words[word] = hits
        if not hit_words:
            continue
        negatives_present = any(_occurrences(full_text, word) for word in scene["negatives"])
        if negatives_present and len(hit_words) < 2:
            continue
        channels = sorted({channel for hits in hit_words.values() for channel in hits})
        total = sum(sum(hits.values()) for hits in hit_words.values())
        score = min(
            SCENE_BASE_SCORE + _source_score(channels, evidence_level) + _frequency_score(total),
            _evidence_cap(evidence_level),
        )
        if score < SCENE_CONFIRM_THRESHOLD:
            continue
        results.append({
            "scene_code": str(scene["code"]),
            "score": score,
            "evidence": {"words": sorted(hit_words), "channels": channels, "total": total},
        })
    results.sort(key=lambda item: (-int(item["score"]), str(item["scene_code"])))
    return results


def attribute_audience(
    texts: Dict[str, str],
    primary_spu: Optional[Dict[str, Any]],
    assets: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """人群归因：内容显式（≥2 个不同信号词）优先，其次主SPU规则先验。"""

    full_text = "\n".join(texts.get(channel, "") for channel in _CHANNEL_ORDER)
    explicit: List[Tuple[int, str, List[str]]] = []
    for audience in assets["audiences"]:
        words = [word for word in audience["signals"] if _occurrences(full_text, word)]
        if len(words) >= EXPLICIT_SIGNAL_MIN_HITS:
            explicit.append((len(words), str(audience["code"]), words))
    explicit.sort(key=lambda item: (-item[0], item[1]))

    prior_code: Optional[str] = None
    if primary_spu is not None:
        audience_map = assets["audience_map"]
        trim_entry = audience_map.get(("trim", str(primary_spu["spu_id"])))
        series_entry = audience_map.get(("series", str(primary_spu["series_slug"])))
        entry = trim_entry or series_entry or {}
        prior_code = entry.get("primary")

    if explicit:
        code = explicit[0][1]
        return {
            "audience_code": code,
            "source": "content_explicit",
            "conflict": bool(prior_code and prior_code != code),
            "evidence": {"signals": explicit[0][2], "rule_prior": prior_code},
        }
    if prior_code:
        return {
            "audience_code": prior_code,
            "source": "rule_prior",
            "conflict": False,
            "evidence": {"rule_prior": prior_code},
        }
    return None


# ---------------------------------------------------------------------------
# 关联执行与落库
# ---------------------------------------------------------------------------

def _persist_content_links(
    connection: sqlite3.Connection,
    content_id: int,
    series_candidates: List[Dict[str, Any]],
    trim_by_slug: Dict[str, Optional[str]],
    scene_rows: List[Dict[str, Any]],
    audience_row: Optional[Dict[str, Any]],
    consistency_flag: bool,
    captured_at: str,
) -> None:
    for table in ("content_spu_links", "content_scene_links", "content_audience_links"):
        connection.execute(
            f"UPDATE {table} SET invalidated_at=? WHERE content_id=? AND invalidated_at IS NULL",
            (captured_at, content_id),
        )
    primary_assigned = False
    for candidate in series_candidates:
        score = int(candidate["score"])
        if score < GRAY_THRESHOLD:
            continue
        status = "confirmed" if score >= CONFIRM_THRESHOLD else "gray"
        slug = str(candidate["series_slug"])
        trim_id = trim_by_slug.get(slug) if status == "confirmed" else None
        spu_id = trim_id or slug
        resolved_level = "trim" if trim_id else "series"
        is_primary = 0
        if status == "confirmed" and not primary_assigned:
            is_primary = 1
            primary_assigned = True
        connection.execute(
            """
            INSERT INTO content_spu_links(
                content_id, spu_id, resolved_level, is_primary, status, score,
                evidence_json, rule_version, created_at, invalidated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            (
                content_id, spu_id, resolved_level, is_primary, status, score,
                json.dumps(candidate["evidence"], ensure_ascii=False),
                ASSOCIATION_RULE_VERSION, captured_at,
            ),
        )
    for scene in scene_rows:
        connection.execute(
            """
            INSERT INTO content_scene_links(
                content_id, scene_code, score, evidence_json, rule_version, created_at, invalidated_at
            ) VALUES (?, ?, ?, ?, ?, ?, NULL)
            """,
            (
                content_id, scene["scene_code"], int(scene["score"]),
                json.dumps(scene["evidence"], ensure_ascii=False),
                ASSOCIATION_RULE_VERSION, captured_at,
            ),
        )
    if audience_row is not None:
        connection.execute(
            """
            INSERT INTO content_audience_links(
                content_id, audience_code, source, conflict_flag, consistency_flag,
                rule_version, created_at, invalidated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            (
                content_id, audience_row["audience_code"], audience_row["source"],
                1 if audience_row["conflict"] else 0, 1 if consistency_flag else 0,
                ASSOCIATION_RULE_VERSION, captured_at,
            ),
        )


def associate_single_content(content_id: int, *, db_path: Path = DEFAULT_DB) -> Dict[str, Any]:
    """对单条内容即时补算三标签（内容「更新数据」后调用；无运行记录，轻量幂等）。

    内容不是 V2/V3 时清掉其旧关联行并返回 skipped；出错由调用方兜底，
    不应阻断主流程。
    """

    with connect(db_path) as connection:
        _require_domain(connection)
        content = connection.execute(
            "SELECT id, title, body FROM content_items WHERE id=? AND published_at IS NOT NULL",
            (content_id,),
        ).fetchone()
        if content is None:
            return {"content_id": content_id, "status": "skipped", "reason": "内容不存在或缺发布日期"}
        level_row = connection.execute(
            """
            SELECT evidence_level FROM evaluation_versions
            WHERE content_id=? AND invalidated_at IS NULL
            ORDER BY evaluated_at DESC, id DESC LIMIT 1
            """,
            (content_id,),
        ).fetchone()
        evidence_level = str(level_row["evidence_level"]) if level_row is not None else None
        if evidence_level not in {"V2", "V3"}:
            with transaction(connection):
                _persist_content_links(connection, int(content["id"]), [], {}, [], None, False, now_utc())
            return {"content_id": content_id, "status": "skipped", "reason": "证据等级不足（非V2/V3）"}
        assets = _load_assets(connection)
        paths: Dict[str, str] = {}
        for kind, artifact_types in (("asr", ("asr", "transcript", "media_transcript")), ("ocr", ("ocr", "media_ocr"))):
            placeholders = ",".join("?" for _ in artifact_types)
            row = connection.execute(
                f"""
                SELECT local_path FROM evidence_artifacts
                WHERE content_id=? AND artifact_type IN ({placeholders})
                  AND status='available' AND sha256 IS NOT NULL
                ORDER BY id DESC LIMIT 1
                """,
                (content_id, *artifact_types),
            ).fetchone()
            if row is not None and row["local_path"]:
                paths[kind] = str(row["local_path"])
        texts = _assemble_texts(content, paths)
        series_candidates = match_series(texts, evidence_level, assets)
        trim_by_slug: Dict[str, Optional[str]] = {}
        for candidate in series_candidates:
            if int(candidate["score"]) >= CONFIRM_THRESHOLD:
                trim_by_slug[str(candidate["series_slug"])] = resolve_trim(
                    texts, str(candidate["series_slug"]), assets
                )
        scene_rows = match_scenes(texts, evidence_level, assets)
        primary_candidate = next(
            (item for item in series_candidates if int(item["score"]) >= CONFIRM_THRESHOLD),
            None,
        )
        primary_spu: Optional[Dict[str, Any]] = None
        if primary_candidate is not None:
            slug = str(primary_candidate["series_slug"])
            primary_spu = {"spu_id": trim_by_slug.get(slug) or slug, "series_slug": slug}
        audience_row = attribute_audience(texts, primary_spu, assets)
        consistency_flag = False
        if audience_row is not None and scene_rows:
            allowed_entry = assets["scene_map"].get(
                str(audience_row["audience_code"]), {"core": set(), "related": set()}
            )
            allowed_codes = set(allowed_entry["core"]) | set(allowed_entry["related"])
            consistency_flag = any(
                scene["scene_code"] not in allowed_codes for scene in scene_rows
            )
        with transaction(connection):
            _persist_content_links(
                connection, int(content["id"]), series_candidates, trim_by_slug,
                scene_rows, audience_row, consistency_flag, now_utc(),
            )
        # 规则落库后 LLM 即时补空（单条同步，失败静默降级，不影响主流程）
        llm_filled = 0
        hook = default_llm_hook(concurrency=1)
        if hook is not None:
            gray_slugs = sorted({
                str(item["series_slug"]) for item in series_candidates
                if GRAY_THRESHOLD <= int(item["score"]) < CONFIRM_THRESHOLD
            })
            primary_slug = (
                str(primary_candidate["series_slug"])
                if primary_candidate is not None else None
            )
            target = {
                "content_id": int(content["id"]),
                "evidence_level": evidence_level,
                "title": str(content["title"] or ""),
                "body": str(content["body"] or ""),
                "artifact_paths": paths,
                "spu_state": (
                    "confirmed" if primary_candidate is not None
                    else ("gray" if gray_slugs else "none")
                ),
                "primary_slug": primary_slug,
                "trim_resolved": bool(primary_slug and trim_by_slug.get(primary_slug)),
                "gray_slugs": gray_slugs,
                "scene_codes": [str(scene["scene_code"]) for scene in scene_rows],
                "has_audience": audience_row is not None,
            }
            try:
                llm_summary = hook(connection, None, [target], assets)
                llm_filled = sum(
                    int(llm_summary.get(key) or 0)
                    for key in (
                        "spu_filled", "gray_upgraded", "gray_overridden",
                        "trim_refined", "scene_filled", "audience_filled",
                    )
                )
            except Exception:  # noqa: BLE001 —— 降级路径，绝不阻断内容更新
                LOGGER.exception("内容 %s LLM 补充失败（规则结果不受影响）", content_id)
        return {
            "content_id": content_id,
            "status": "associated",
            "spu_linked": primary_candidate is not None,
            "scene_count": len(scene_rows),
            "llm_filled": llm_filled,
        }


def recover_orphan_association_runs(*, db_path: Path = DEFAULT_DB) -> int:
    """服务启动时调用：进程内不可能有存活的关联任务，running 行都是孤儿。"""

    with connect(db_path) as connection:
        if not domain_ready(connection):
            return 0
        with transaction(connection):
            cursor = connection.execute(
                """
                UPDATE spu_association_runs
                SET status='failed', finished_at=?,
                    summary_json='{"note":"服务重启，任务已中断，可重新发起"}'
                WHERE status='running'
                """,
                (now_utc(),),
            )
            return int(cursor.rowcount)


def start_association_run(*, db_path: Path = DEFAULT_DB) -> int:
    """创建 running 运行记录并做并发守卫；返回 run_id。"""

    with connect(db_path) as connection:
        _require_domain(connection)
        with transaction(connection):
            running = connection.execute(
                "SELECT id FROM spu_association_runs WHERE status='running' ORDER BY id LIMIT 1"
            ).fetchone()
            if running is not None:
                raise SpuAudienceError(
                    f"已有数据刷新任务（第 {int(running['id'])} 次）在运行中，请等它完成后再发起"
                )
            ensure_assets(connection)
            cursor = connection.execute(
                """
                INSERT INTO spu_association_runs(started_at, status, rule_version)
                VALUES (?, 'running', ?)
                """,
                (now_utc(), ASSOCIATION_RULE_VERSION),
            )
            return int(cursor.lastrowid or 0)


def dry_run_summary(
    *, db_path: Path = DEFAULT_DB, limit: Optional[int] = None,
    window: Optional[str] = None,
) -> Dict[str, Any]:
    """试算：只统计 V2/V3 处理范围与词表规模，不写任何数据。"""

    with connect(db_path) as connection:
        _require_domain(connection)
        assets = _load_assets(connection)
        published_total = int(connection.execute(
            "SELECT COUNT(*) FROM content_items WHERE published_at IS NOT NULL"
        ).fetchone()[0])
        eligible = _eligible_v23_contents(connection, limit=limit, window=window)
        series_count = sum(
            1 for spu in assets["catalog"].values() if int(spu["is_series_node"])
        )
        return {
            "published_total": published_total,
            "eligible_v23": len(eligible),
            "skipped_not_v23": (
                published_total - len(eligible) if limit is None else None
            ),
            "series_count": series_count,
            "alias_count": len(assets["aliases"]),
            "scene_count": len(assets["scenes"]),
            "estimated_minutes": round(len(eligible) * 0.012 / 60, 1),
        }


def _invalidate_stale_links(
    connection: sqlite3.Connection, eligible_ids: Sequence[int]
) -> None:
    """把已脱离 V2/V3 范围（证据降级等）内容的历史关联行一次性失效。"""

    with transaction(connection):
        connection.execute(
            "CREATE TEMP TABLE IF NOT EXISTS _assoc_eligible(id INTEGER PRIMARY KEY)"
        )
        connection.execute("DELETE FROM _assoc_eligible")
        connection.executemany(
            "INSERT OR IGNORE INTO _assoc_eligible(id) VALUES (?)",
            [(int(value),) for value in eligible_ids],
        )
        stamp = now_utc()
        for table in (
            "content_spu_links", "content_scene_links", "content_audience_links"
        ):
            connection.execute(
                f"""
                UPDATE {table} SET invalidated_at=?
                WHERE invalidated_at IS NULL
                  AND content_id NOT IN (SELECT id FROM _assoc_eligible)
                """,
                (stamp,),
            )
        connection.execute("DROP TABLE _assoc_eligible")


def run_association(
    *,
    db_path: Path = DEFAULT_DB,
    limit: Optional[int] = None,
    since: Optional[str] = None,
    scope_window: Optional[str] = None,
    run_id: Optional[int] = None,
    batch_size: int = 500,
    progress_callback: Optional[Any] = None,
    llm_hook: Optional[Any] = None,
) -> Dict[str, Any]:
    """系统级关联执行通道：只迭代 V2/V3，批量预取，分批提交，进度可查。

    - ``run_id`` 缺省时自建运行记录（CLI/同步路径）；API 后台任务先
      ``start_association_run`` 再传入 run_id；
    - 每 ``batch_size`` 条一个事务，进度写入 ``spu_association_runs.summary_json``；
    - 全量运行（limit 为空）结束后，把已脱离 V2/V3 范围内容的历史关联行
      统一失效；limit 试跑与窗口重算不做清理，避免误伤范围外数据；
    - ``scope_window``（yesterday/this_week/last_week）：只重算发布时间落在
      该统计窗口内的 V2/V3 内容，窗口口径与页面统计选择器同源；'all'/None
      即全量；
    - ``llm_hook`` 非空时，规则链结束后对未解决内容执行 LLM 补空
      （B 链）：hook(connection, run_id, targets, assets) → 摘要 dict，写入
      summary_json.llm；hook 抛错只记结论、不影响规则结果与运行状态。
    """

    if scope_window == "all":
        scope_window = None
    if run_id is None:
        run_id = start_association_run(db_path=db_path)
    with connect(db_path) as connection:
        _require_domain(connection)
        assets = _load_assets(connection)
        scene_map = assets["scene_map"]
        published_total = int(connection.execute(
            "SELECT COUNT(*) FROM content_items WHERE published_at IS NOT NULL"
        ).fetchone()[0])
        if since:
            mode = "incremental"
        elif scope_window:
            mode = f"window:{scope_window}"
        else:
            mode = "full"
        contents = _eligible_v23_contents(
            connection, limit=limit, since=since, window=scope_window
        )
        artifact_paths = _artifact_paths_by_content(connection)
        llm_targets: Optional[List[Dict[str, Any]]] = (
            [] if llm_hook is not None else None
        )
        counters = {
            "contents_total": len(contents),
            "spu_linked": 0,
            "trim_resolved": 0,
            "gray_count": 0,
            "scene_linked": 0,
            "audience_linked": 0,
            "insufficient_evidence": (
                published_total - len(contents)
                if limit is None and mode == "full"
                else 0
            ),
        }
        processed = 0

        def _progress_json() -> str:
            return json.dumps(
                {
                    "processed": processed,
                    "eligible": len(contents),
                    "published_total": published_total,
                    "mode": mode,
                },
                ensure_ascii=False,
            )

        def _flush(batch: List[Tuple[Any, ...]]) -> None:
            nonlocal processed
            with transaction(connection):
                for item in batch:
                    _persist_content_links(connection, *item)
                processed += len(batch)
                connection.execute(
                    "UPDATE spu_association_runs SET summary_json=? WHERE id=?",
                    (_progress_json(), run_id),
                )
            if progress_callback is not None:
                progress_callback(processed, len(contents))

        try:
            batch: List[Tuple[Any, ...]] = []
            for content in contents:
                content_id = int(content["id"])
                evidence_level = str(content["evidence_level"])
                texts = _assemble_texts(content, artifact_paths.get(content_id, {}))
                series_candidates = match_series(texts, evidence_level, assets)
                trim_by_slug: Dict[str, Optional[str]] = {}
                for candidate in series_candidates:
                    if int(candidate["score"]) >= CONFIRM_THRESHOLD:
                        trim_by_slug[str(candidate["series_slug"])] = resolve_trim(
                            texts, str(candidate["series_slug"]), assets
                        )
                scene_rows = match_scenes(texts, evidence_level, assets)
                primary_candidate = next(
                    (
                        item for item in series_candidates
                        if int(item["score"]) >= CONFIRM_THRESHOLD
                    ),
                    None,
                )
                primary_spu: Optional[Dict[str, Any]] = None
                if primary_candidate is not None:
                    slug = str(primary_candidate["series_slug"])
                    primary_spu = {
                        "spu_id": trim_by_slug.get(slug) or slug,
                        "series_slug": slug,
                    }
                audience_row = attribute_audience(texts, primary_spu, assets)
                consistency_flag = False
                if audience_row is not None and scene_rows:
                    allowed_entry = scene_map.get(
                        str(audience_row["audience_code"]),
                        {"core": set(), "related": set()},
                    )
                    allowed_codes = (
                        set(allowed_entry["core"]) | set(allowed_entry["related"])
                    )
                    consistency_flag = any(
                        scene["scene_code"] not in allowed_codes
                        for scene in scene_rows
                    )
                if primary_candidate is not None:
                    counters["spu_linked"] += 1
                    slug = str(primary_candidate["series_slug"])
                    if trim_by_slug.get(slug):
                        counters["trim_resolved"] += 1
                if any(
                    GRAY_THRESHOLD <= int(item["score"]) < CONFIRM_THRESHOLD
                    for item in series_candidates
                ):
                    counters["gray_count"] += 1
                if scene_rows:
                    counters["scene_linked"] += 1
                if audience_row is not None:
                    counters["audience_linked"] += 1
                if llm_targets is not None:
                    gray_slugs = sorted({
                        str(item["series_slug"]) for item in series_candidates
                        if GRAY_THRESHOLD <= int(item["score"]) < CONFIRM_THRESHOLD
                    })
                    primary_slug = (
                        str(primary_candidate["series_slug"])
                        if primary_candidate is not None else None
                    )
                    llm_targets.append({
                        "content_id": content_id,
                        "evidence_level": evidence_level,
                        "title": str(content["title"] or ""),
                        "body": str(content["body"] or ""),
                        "artifact_paths": artifact_paths.get(content_id, {}),
                        "spu_state": (
                            "confirmed" if primary_candidate is not None
                            else ("gray" if gray_slugs else "none")
                        ),
                        "primary_slug": primary_slug,
                        "trim_resolved": bool(
                            primary_slug and trim_by_slug.get(primary_slug)
                        ),
                        "gray_slugs": gray_slugs,
                        "scene_codes": [
                            str(scene["scene_code"]) for scene in scene_rows
                        ],
                        "has_audience": audience_row is not None,
                    })
                batch.append((
                    content_id, series_candidates, trim_by_slug, scene_rows,
                    audience_row, consistency_flag, now_utc(),
                ))
                if len(batch) >= max(1, batch_size):
                    _flush(batch)
                    batch = []
            if batch:
                _flush(batch)
            if limit is None and mode == "full":
                _invalidate_stale_links(
                    connection, [int(row["id"]) for row in contents]
                )
        except Exception:
            with transaction(connection):
                connection.execute(
                    """
                    UPDATE spu_association_runs
                    SET finished_at=?, status='failed', summary_json=?
                    WHERE id=?
                    """,
                    (now_utc(), _progress_json(), run_id),
                )
            raise
        llm_summary: Optional[Dict[str, Any]] = None
        if llm_hook is not None:
            # LLM 补充是降级路径：任何异常只记入摘要，不影响规则结果与运行状态。
            try:
                llm_summary = llm_hook(connection, run_id, llm_targets or [], assets)
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("LLM 补充执行失败（规则结果不受影响）run_id=%s", run_id)
                llm_summary = {"enabled": True, "error": str(exc)[:300]}
        final_summary = json.loads(_progress_json())
        if llm_summary is not None:
            final_summary["llm"] = llm_summary
        with transaction(connection):
            connection.execute(
                """
                UPDATE spu_association_runs
                SET finished_at=?, status='succeeded',
                    contents_total=?, spu_linked=?, trim_resolved=?, gray_count=?,
                    scene_linked=?, audience_linked=?, insufficient_evidence=?,
                    summary_json=?
                WHERE id=?
                """,
                (
                    now_utc(), counters["contents_total"], counters["spu_linked"],
                    counters["trim_resolved"], counters["gray_count"],
                    counters["scene_linked"], counters["audience_linked"],
                    counters["insufficient_evidence"],
                    json.dumps(final_summary, ensure_ascii=False), run_id,
                ),
            )
        result: Dict[str, Any] = {"run_id": run_id, **counters}
        if llm_summary is not None:
            result["llm"] = llm_summary
        return result


def default_llm_hook(
    *,
    limit: Optional[int] = None,
    concurrency: Optional[int] = None,
    progress_callback: Optional[Any] = None,
) -> Optional[Any]:
    """构造标准 LLM 补充钩子；key 缺失或被禁用时返回 None（纯规则运行）。

    返回的 hook 满足 run_association 的契约：
    ``hook(connection, run_id, targets, assets) -> 摘要 dict``。
    """

    from . import llm_assist

    if not llm_assist.llm_available():
        return None

    def _hook(
        connection: sqlite3.Connection,
        run_id: Optional[int],
        targets: List[Dict[str, Any]],
        assets: Dict[str, Any],
    ) -> Dict[str, Any]:
        return llm_assist.run_llm_pass(
            connection,
            run_id=run_id,
            targets=targets,
            assets=assets,
            assemble_texts=lambda target: _assemble_texts(
                target, target.get("artifact_paths") or {}
            ),
            now_utc=now_utc,
            transaction=transaction,
            limit=limit,
            concurrency=concurrency,
            progress_callback=progress_callback,
        )

    return _hook


# ---------------------------------------------------------------------------
# 内容列表标签
# ---------------------------------------------------------------------------

def content_labels(
    connection: sqlite3.Connection, content_ids: Sequence[int]
) -> Dict[int, Dict[str, Any]]:
    """为内容列表批量取 SPU/人群/场景 标签；域未就绪时返回空。"""

    ids = [int(value) for value in content_ids]
    if not ids or not domain_ready(connection):
        return {}
    labels: Dict[int, Dict[str, Any]] = {
        content_id: {
            "spu": None,
            "spu_secondary_count": 0,
            "spu_gray_count": 0,
            "audience": None,
            "scenes": [],
        }
        for content_id in ids
    }
    for start_index in range(0, len(ids), SQL_ID_CHUNK):
        chunk = ids[start_index:start_index + SQL_ID_CHUNK]
        placeholders = ",".join("?" for _ in chunk)
        for row in connection.execute(
            f"""
            SELECT l.content_id, l.spu_id, l.resolved_level, l.status, l.is_primary,
                   l.score, l.evidence_json,
                   c.brand, c.series, c.trim_label
            FROM content_spu_links l JOIN spu_catalog c ON c.spu_id=l.spu_id
            WHERE l.content_id IN ({placeholders}) AND l.invalidated_at IS NULL
            ORDER BY l.is_primary DESC, l.score DESC
            """,
                chunk,
            ):
            entry = labels[int(row["content_id"])]
            if int(row["is_primary"]) and entry["spu"] is None:
                try:
                    evidence = json.loads(str(row["evidence_json"] or "{}"))
                except (json.JSONDecodeError, TypeError, ValueError):
                    evidence = {}
                aliases = evidence.get("aliases") if isinstance(evidence, dict) else None
                entry["spu"] = {
                    "spu_id": str(row["spu_id"]),
                    "series": str(row["series"]),
                    "brand": str(row["brand"]),
                    "trim_label": row["trim_label"],
                    "resolved_level": str(row["resolved_level"]),
                    "score": int(row["score"]),
                    "matched_aliases": (
                        [str(alias) for alias in aliases[:3]]
                        if isinstance(aliases, list)
                        else []
                    ),
                }
            elif str(row["status"]) == "confirmed":
                entry["spu_secondary_count"] += 1
            elif str(row["status"]) == "gray":
                entry["spu_gray_count"] += 1
        for row in connection.execute(
            f"""
            SELECT l.content_id, l.audience_code, l.source, a.label
            FROM content_audience_links l JOIN audience_dim a ON a.code=l.audience_code
            WHERE l.content_id IN ({placeholders}) AND l.invalidated_at IS NULL
            """,
                chunk,
            ):
            labels[int(row["content_id"])]["audience"] = {
                "code": str(row["audience_code"]),
                "label": str(row["label"]),
                "source": str(row["source"]),
            }
        for row in connection.execute(
            f"""
            SELECT l.content_id, l.scene_code, s.label, s.definition,
                   s.trigger_words_json, s.negative_words_json
            FROM content_scene_links l JOIN scene_dim s ON s.code=l.scene_code
            WHERE l.content_id IN ({placeholders}) AND l.invalidated_at IS NULL
            ORDER BY l.score DESC, l.scene_code
            """,
                chunk,
            ):
            scene = _canonicalize_legacy_scene({
                "code": str(row["scene_code"]),
                "label": str(row["label"]),
                "definition": str(row["definition"] or ""),
                "triggers": _decode_list(row["trigger_words_json"]),
                "negatives": _decode_list(row["negative_words_json"]),
            })
            labels[int(row["content_id"])]["scenes"].append(
                {"code": str(row["scene_code"]), "label": str(scene["label"])}
            )
    return labels


# ---------------------------------------------------------------------------
# 资产管理（页面 CRUD）
# ---------------------------------------------------------------------------

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _slugify(brand: str, series: str) -> str:
    base = _SLUG_RE.sub("-", f"{brand}-{series}".lower()).strip("-")
    if not base:
        base = "spu"
    return base


def list_assets(*, db_path: Path = DEFAULT_DB, read_only: bool = False) -> Dict[str, Any]:
    with connect(db_path, read_only=read_only) as connection:
        if not domain_ready(connection):
            return {"ready": False, "spu": [], "audiences": [], "scenes": [], "audience_scene_map": [], "last_run": None}
        assets = _load_assets(connection)
        alias_by_spu: Dict[str, List[Dict[str, Any]]] = {}
        for alias in assets["aliases"]:
            alias_by_spu.setdefault(str(alias["spu_id"]), []).append({
                "alias": str(alias["alias"]),
                "alias_type": str(alias["alias_type"]),
                "ambiguous": bool(alias["ambiguous"]),
            })
        audience_map = assets["audience_map"]
        spu_rows: List[Dict[str, Any]] = []
        for spu in sorted(
            assets["catalog"].values(),
            key=lambda item: (str(item["brand"]), str(item["series_slug"]), int(item["is_series_node"]) * -1, str(item["spu_id"])),
        ):
            scope_key = ("series", str(spu["series_slug"])) if int(spu["is_series_node"]) else ("trim", str(spu["spu_id"]))
            mapped = audience_map.get(scope_key) or audience_map.get(("series", str(spu["series_slug"]))) or {}
            spu_rows.append({
                "spu_id": str(spu["spu_id"]),
                "brand": str(spu["brand"]),
                "series": str(spu["series"]),
                "series_slug": str(spu["series_slug"]),
                "trim_label": spu["trim_label"],
                "is_series_node": bool(spu["is_series_node"]),
                "model_year": spu["model_year"],
                "powertrain": str(spu["powertrain"] or ""),
                "body_style": str(spu["body_style"] or ""),
                "price_low": spu["price_low"],
                "price_high": spu["price_high"],
                "audience_primary": mapped.get("primary"),
                "audience_secondary": mapped.get("secondary"),
                "aliases": alias_by_spu.get(str(spu["spu_id"]), []),
            })
        last_run_row = connection.execute(
            "SELECT * FROM spu_association_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
        since = resolve_incremental_since(connection)
        if since is None:
            stale_row = connection.execute(
                """
                SELECT COUNT(DISTINCT ev.content_id) FROM evaluation_versions ev
                JOIN content_items c ON c.id=ev.content_id
                WHERE ev.invalidated_at IS NULL AND ev.evidence_level IN ('V2','V3')
                  AND c.published_at IS NOT NULL
                """
            ).fetchone()
        else:
            stale_row = connection.execute(
                """
                SELECT COUNT(DISTINCT ev.content_id) FROM evaluation_versions ev
                JOIN content_items c ON c.id=ev.content_id
                WHERE ev.invalidated_at IS NULL AND ev.evidence_level IN ('V2','V3')
                  AND c.published_at IS NOT NULL AND ev.evaluated_at >= ?
                """,
                (since,),
            ).fetchone()
        stale_content_count = int(stale_row[0]) if stale_row is not None else 0
        last_run: Optional[Dict[str, Any]] = None
        if last_run_row is not None:
            last_run = dict(last_run_row)
            try:
                last_run["summary"] = json.loads(
                    str(last_run.pop("summary_json") or "{}")
                )
            except (json.JSONDecodeError, TypeError, ValueError):
                last_run["summary"] = {}
        scene_map_rows = [
            {"audience_code": code, "core": sorted(entry["core"]), "related": sorted(entry["related"])}
            for code, entry in sorted(assets["scene_map"].items())
        ]
        return {
            "ready": True,
            "rule_version": ASSOCIATION_RULE_VERSION,
            "seed_version": ASSET_SEED_VERSION,
            "spu": spu_rows,
            "audiences": assets["audiences"],
            "scenes": assets["scenes"],
            "audience_scene_map": scene_map_rows,
            "last_run": last_run,
            "stale_content_count": stale_content_count,
        }


def upsert_spu(payload: Dict[str, Any], *, db_path: Path = DEFAULT_DB) -> Dict[str, Any]:
    """新增或更新一个 SPU（车系或款型），并同步别名与人群映射。"""

    brand = str(payload.get("brand") or "").strip()
    series = str(payload.get("series") or "").strip()
    if not brand or not series:
        raise SpuAudienceError("品牌与车系不能为空")
    trim_label = str(payload.get("trim_label") or "").strip() or None
    aliases = payload.get("aliases") or []
    captured_at = now_utc()
    with connect(db_path) as connection:
        _require_domain(connection)
        with transaction(connection):
            ensure_assets(connection)
            existing_series = connection.execute(
                "SELECT series_slug FROM spu_catalog WHERE brand=? AND series=? LIMIT 1",
                (brand, series),
            ).fetchone()
            series_slug = (
                str(existing_series["series_slug"]) if existing_series is not None
                else _slugify(brand, series)
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO spu_catalog(
                    spu_id, brand, series, series_slug, trim_label, is_series_node,
                    model_year, powertrain, body_style, price_low, price_high,
                    external_ref, enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, NULL, 1, NULL, '', '', NULL, NULL, '', 1, ?, ?)
                """,
                (series_slug, brand, series, series_slug, captured_at, captured_at),
            )
            spu_id = series_slug
            if trim_label is not None:
                spu_id = str(payload.get("spu_id") or "").strip() or (
                    f"{series_slug}__{_SLUG_RE.sub('-', trim_label.lower()).strip('-') or 'trim'}"
                )
                connection.execute(
                    """
                    INSERT INTO spu_catalog(
                        spu_id, brand, series, series_slug, trim_label, is_series_node,
                        model_year, powertrain, body_style, price_low, price_high,
                        external_ref, enabled, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, '', 1, ?, ?)
                    ON CONFLICT(spu_id) DO UPDATE SET
                        trim_label=excluded.trim_label,
                        model_year=excluded.model_year,
                        powertrain=excluded.powertrain,
                        body_style=excluded.body_style,
                        price_low=excluded.price_low,
                        price_high=excluded.price_high,
                        enabled=1,
                        updated_at=excluded.updated_at
                    """,
                    (
                        spu_id, brand, series, series_slug, trim_label,
                        payload.get("model_year"),
                        str(payload.get("powertrain") or ""),
                        str(payload.get("body_style") or ""),
                        payload.get("price_low"), payload.get("price_high"),
                        captured_at, captured_at,
                    ),
                )
            else:
                connection.execute(
                    """
                    UPDATE spu_catalog SET powertrain=?, body_style=?, price_low=?, price_high=?,
                        enabled=?, updated_at=?
                    WHERE spu_id=?
                    """,
                    (
                        str(payload.get("powertrain") or ""),
                        str(payload.get("body_style") or ""),
                        payload.get("price_low"), payload.get("price_high"),
                        0 if payload.get("enabled") is False else 1,
                        captured_at, series_slug,
                    ),
                )
            if isinstance(aliases, list):
                connection.execute("DELETE FROM spu_alias WHERE spu_id=?", (spu_id,))
                for alias in aliases:
                    word = str((alias or {}).get("alias") or "").strip()
                    if not word:
                        continue
                    connection.execute(
                        """
                        INSERT OR IGNORE INTO spu_alias(alias, alias_type, spu_scope, spu_id, ambiguous, enabled)
                        VALUES (?, ?, ?, ?, ?, 1)
                        """,
                        (
                            word,
                            str((alias or {}).get("alias_type") or "official"),
                            "series" if trim_label is None else "trim",
                            spu_id,
                            1 if (alias or {}).get("ambiguous") else 0,
                        ),
                    )
            scope = "series" if trim_label is None else "trim"
            scope_key = series_slug if trim_label is None else spu_id
            for role in ("primary", "secondary"):
                code = str(payload.get(f"audience_{role}") or "").strip()
                connection.execute(
                    "DELETE FROM spu_audience_map WHERE scope=? AND scope_key=? AND role=?",
                    (scope, scope_key, role),
                )
                if code:
                    connection.execute(
                        """
                        INSERT INTO spu_audience_map(scope, scope_key, audience_code, role, weight, basis)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (scope, scope_key, code, role, 1.0 if role == "primary" else 0.5,
                         str(payload.get("basis") or "页面配置")),
                    )
    return {"spu_id": spu_id, "series_slug": series_slug}


# ---------------------------------------------------------------------------
# 统计（三维明细 + 单维上卷 + 覆盖率 + gap）
# ---------------------------------------------------------------------------

def _window_bounds(window: str) -> Optional[Tuple[str, str]]:
    if window == "all":
        return None
    current = datetime.now(SHANGHAI)
    today = datetime.combine(current.date(), time.min, tzinfo=SHANGHAI)
    this_week = today - timedelta(days=today.weekday())
    if window == "yesterday":
        bounds = (today - timedelta(days=1), today)
    elif window == "this_week":
        bounds = (this_week, current)
    elif window == "last_week":
        bounds = (this_week - timedelta(days=7), this_week)
    else:
        raise SpuAudienceError(f"不支持的统计窗口：{window}")

    def _utc(value: datetime) -> str:
        return (
            value.astimezone(timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        )

    return (_utc(bounds[0]), _utc(bounds[1]))


def build_stats(
    *,
    db_path: Path = DEFAULT_DB,
    window: str = "all",
    platform: str = "",
    read_only: bool = False,
) -> Dict[str, Any]:
    if window not in STAT_WINDOWS:
        raise SpuAudienceError(f"不支持的统计窗口：{window}")
    if platform and platform not in STAT_PLATFORMS:
        raise SpuAudienceError(f"不支持的统计平台：{platform}")
    with connect(db_path, read_only=read_only) as connection:
        if not domain_ready(connection):
            return {"ready": False}
        assets = _load_assets(connection)
        bounds = _window_bounds(window)
        where = ["c.published_at IS NOT NULL"]
        parameters: List[Any] = []
        if bounds is not None:
            where.append("c.published_at >= ? AND c.published_at < ?")
            parameters.extend(bounds)
        if platform:
            where.append("c.platform=?")
            parameters.append(platform)
        contents = connection.execute(
            f"""
            SELECT c.id, c.platform, ms.view_count
            FROM content_items c
            LEFT JOIN content_metric_snapshots ms ON ms.id=(
                SELECT ms2.id FROM content_metric_snapshots ms2
                WHERE ms2.content_id=c.id ORDER BY ms2.captured_at DESC, ms2.id DESC LIMIT 1
            )
            WHERE {' AND '.join(where)}
            """,
            parameters,
        ).fetchall()
        content_ids = [int(row["id"]) for row in contents]
        labels = content_labels(connection, content_ids)
        catalog = assets["catalog"]
        audience_labels = {str(item["code"]): str(item["label"]) for item in assets["audiences"]}
        scene_labels = {str(item["code"]): str(item["label"]) for item in assets["scenes"]}
        scene_map = assets["scene_map"]
        audience_map = assets["audience_map"]

        posts_total = len(contents)
        views_total_valid = 0
        classified_views = 0
        spu_count = audience_count = scene_count = trim_count = 0
        detail: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        series_posts: Dict[str, int] = {}
        series_scene_posts: Dict[Tuple[str, str], int] = {}
        overflow: Dict[Tuple[str, str], int] = {}
        for row in contents:
            content_id = int(row["id"])
            view = int(row["view_count"]) if row["view_count"] is not None and int(row["view_count"]) > 0 else 0
            views_total_valid += view
            entry = labels.get(content_id) or {"spu": None, "audience": None, "scenes": []}
            spu = entry.get("spu")
            audience = entry.get("audience")
            scenes = entry.get("scenes") or []
            spu_key = str(spu["spu_id"]) if spu else NONE_KEY
            audience_key = str(audience["code"]) if audience else NONE_KEY
            scene_keys = [str(scene["code"]) for scene in scenes] or [NONE_KEY]
            if spu:
                spu_count += 1
                classified_views += view
                if str(spu["resolved_level"]) == "trim":
                    trim_count += 1
                slug = str(catalog.get(spu_key, {}).get("series_slug") or spu_key)
                series_posts[slug] = series_posts.get(slug, 0) + 1
                for scene_key in scene_keys:
                    if scene_key != NONE_KEY:
                        series_scene_posts[(slug, scene_key)] = series_scene_posts.get((slug, scene_key), 0) + 1
            if audience:
                audience_count += 1
                allowed_entry = scene_map.get(audience_key, {"core": set(), "related": set()})
                allowed = set(allowed_entry["core"]) | set(allowed_entry["related"])
                for scene_key in scene_keys:
                    if scene_key != NONE_KEY and scene_key not in allowed:
                        overflow[(audience_key, scene_key)] = overflow.get((audience_key, scene_key), 0) + 1
            if scene_keys != [NONE_KEY]:
                scene_count += 1
            for scene_key in scene_keys:
                key = (spu_key, audience_key, scene_key)
                bucket = detail.setdefault(key, {"posts": 0, "views": 0})
                bucket["posts"] += 1
                bucket["views"] += view

        def _spu_label(spu_id: str) -> Dict[str, Any]:
            if spu_id == NONE_KEY:
                return {"spu_id": NONE_KEY, "label": "未归类", "series": None, "trim_label": None}
            spu = catalog.get(spu_id)
            if spu is None:
                return {"spu_id": spu_id, "label": spu_id, "series": None, "trim_label": None}
            series = str(spu["series"])
            trim = spu["trim_label"]
            return {
                "spu_id": spu_id,
                "label": f"{series} · {trim}" if trim else f"{series}（未细化）",
                "series": series,
                "trim_label": trim,
            }

        exposure_share = (
            round(classified_views * 100 / views_total_valid, 2) if views_total_valid else None
        )
        views_status = (
            "not_applicable" if posts_total == 0
            else "missing" if views_total_valid == 0
            else "available" if exposure_share is not None and exposure_share >= EXPOSURE_COVERAGE_THRESHOLD
            else "below_threshold"
        )
        detail_rows = []
        for (spu_key, audience_key, scene_key), bucket in detail.items():
            detail_rows.append({
                "spu": _spu_label(spu_key),
                "audience": {
                    "code": audience_key,
                    "label": audience_labels.get(audience_key, "未归类") if audience_key != NONE_KEY else "未归类",
                },
                "scene": {
                    "code": scene_key,
                    "label": scene_labels.get(scene_key, "未识别") if scene_key != NONE_KEY else "未识别",
                },
                "posts": bucket["posts"],
                "views": bucket["views"] if views_status == "available" else None,
                "low_sample": bucket["posts"] < LOW_SAMPLE_THRESHOLD,
            })
        detail_rows.sort(key=lambda item: (-int(item["posts"]), str(item["spu"]["spu_id"])))

        # 单维上卷按内容去重计算（场景多值只在场景维度内展开，口径 D4）；
        # 同步累计分平台桶，供车型库行内的 抖音/小红书 条数占比与曝光占比。
        def _new_rollup_bucket() -> Dict[str, Any]:
            return {
                "posts": 0,
                "views": 0,
                "channels": {p: {"posts": 0, "views": 0} for p in STAT_PLATFORMS},
            }

        channel_totals: Dict[str, Dict[str, int]] = {
            p: {"posts": 0, "valid_views": 0, "classified_views": 0}
            for p in STAT_PLATFORMS
        }
        spu_rollup: Dict[str, Dict[str, Any]] = {}
        audience_rollup: Dict[str, Dict[str, Any]] = {}
        scene_rollup: Dict[str, Dict[str, Any]] = {}
        for row in contents:
            content_id = int(row["id"])
            content_platform = str(row["platform"])
            view = int(row["view_count"]) if row["view_count"] is not None and int(row["view_count"]) > 0 else 0
            entry = labels.get(content_id) or {"spu": None, "audience": None, "scenes": []}
            spu_key = str(entry["spu"]["spu_id"]) if entry.get("spu") else NONE_KEY
            audience_key = str(entry["audience"]["code"]) if entry.get("audience") else NONE_KEY
            channel_total = channel_totals.get(content_platform)
            if channel_total is not None:
                channel_total["posts"] += 1
                channel_total["valid_views"] += view
                if entry.get("spu"):
                    channel_total["classified_views"] += view

            def _accumulate(target: Dict[str, Dict[str, Any]], key: str) -> None:
                bucket = target.setdefault(key, _new_rollup_bucket())
                bucket["posts"] += 1
                bucket["views"] += view
                channel_bucket = bucket["channels"].get(content_platform)
                if channel_bucket is not None:
                    channel_bucket["posts"] += 1
                    channel_bucket["views"] += view

            _accumulate(spu_rollup, spu_key)
            _accumulate(audience_rollup, audience_key)
            scenes = entry.get("scenes") or []
            for scene in scenes or [{"code": NONE_KEY}]:
                _accumulate(scene_rollup, str(scene["code"]))

        channel_meta: Dict[str, Dict[str, Any]] = {}
        for p, total in channel_totals.items():
            share = (
                round(total["classified_views"] * 100 / total["valid_views"], 2)
                if total["valid_views"]
                else None
            )
            channel_meta[p] = {
                "posts": total["posts"],
                "valid_views": total["valid_views"],
                "classified_share": share,
                "views_published": bool(
                    total["valid_views"]
                    and share is not None
                    and share >= EXPOSURE_COVERAGE_THRESHOLD
                ),
            }

        def _rollup_rows(source: Dict[str, Dict[str, Any]], kind: str) -> List[Dict[str, Any]]:
            rows = []
            for key, bucket in source.items():
                if kind == "spu":
                    label = _spu_label(key)["label"]
                elif kind == "audience":
                    label = audience_labels.get(key, "未归类") if key != NONE_KEY else "未归类"
                else:
                    label = scene_labels.get(key, "未识别") if key != NONE_KEY else "未识别"
                channels: Dict[str, Dict[str, Any]] = {}
                for p in STAT_PLATFORMS:
                    channel_bucket = bucket["channels"][p]
                    meta = channel_meta[p]
                    post_share = (
                        round(channel_bucket["posts"] * 100 / meta["posts"], 1)
                        if meta["posts"]
                        else None
                    )
                    view_share = (
                        round(channel_bucket["views"] * 100 / meta["valid_views"], 1)
                        if meta["views_published"] and meta["valid_views"]
                        else None
                    )
                    channels[p] = {
                        "posts": channel_bucket["posts"],
                        "views": channel_bucket["views"] if meta["views_published"] else None,
                        "post_share": post_share,
                        "view_share": view_share,
                        "post_denominator": meta["posts"],
                        "view_denominator": meta["valid_views"],
                    }
                rows.append({
                    "key": key,
                    "label": label,
                    "posts": bucket["posts"],
                    "views": bucket["views"] if views_status == "available" else None,
                    "channels": channels,
                })
            rows.sort(key=lambda item: (item["key"] == NONE_KEY, -int(item["posts"]), str(item["key"])))
            return rows

        gaps_missing: List[Dict[str, Any]] = []
        for slug, posts in sorted(series_posts.items(), key=lambda item: -item[1])[:20]:
            entry = audience_map.get(("series", slug)) or {}
            primary = entry.get("primary")
            if not primary:
                continue
            core_scenes = scene_map.get(primary, {"core": set()})["core"]
            series_label = next(
                (str(spu["series"]) for spu in catalog.values() if str(spu["series_slug"]) == slug),
                slug,
            )
            for scene_code in sorted(core_scenes):
                if not series_scene_posts.get((slug, scene_code)):
                    gaps_missing.append({
                        "series": series_label,
                        "series_posts": posts,
                        "audience": {"code": primary, "label": audience_labels.get(primary, primary)},
                        "scene": {"code": scene_code, "label": scene_labels.get(scene_code, scene_code)},
                    })
        overflow_rows = [
            {
                "audience": {"code": audience_key, "label": audience_labels.get(audience_key, audience_key)},
                "scene": {"code": scene_key, "label": scene_labels.get(scene_key, scene_key)},
                "posts": count,
            }
            for (audience_key, scene_key), count in sorted(overflow.items(), key=lambda item: -item[1])[:20]
        ]

        def _percentage(numerator: int) -> Optional[float]:
            return round(numerator * 100 / posts_total, 1) if posts_total else None

        return {
            "ready": True,
            "window": window,
            "platform": platform or "all",
            "rule_version": ASSOCIATION_RULE_VERSION,
            "totals": {
                "posts": posts_total,
                "valid_exposure_views": views_total_valid,
            },
            "coverage": {
                "spu_percentage": _percentage(spu_count),
                "audience_percentage": _percentage(audience_count),
                "scene_percentage": _percentage(scene_count),
                "trim_percentage": round(trim_count * 100 / spu_count, 1) if spu_count else None,
            },
            "exposure_gate": {
                "classified_share": exposure_share,
                "threshold": EXPOSURE_COVERAGE_THRESHOLD,
                "status": views_status,
            },
            "channel_totals": channel_meta,
            "detail": detail_rows,
            "spu_rollup": _rollup_rows(spu_rollup, "spu"),
            "audience_rollup": _rollup_rows(audience_rollup, "audience"),
            "scene_rollup": _rollup_rows(scene_rollup, "scene"),
            "gaps": {"missing": gaps_missing, "overflow": overflow_rows},
            "footnotes": [
                "一条内容可以对应多个场景，所以各场景条数相加可能大于内容总数；每个场景内同一内容只计算一次。",
                "曝光量使用每条内容最近保存的阅读或播放累计值，只统计曝光量大于 0 的内容；完成分类的曝光低于 90% 时不显示曝光占比。",
                "一条内容只统计一个主要车型；对比多个车系时，其他车系仍会保留在识别明细中。",
                "系统只自动处理资料较完整、可以评估的内容；资料不足或尚未评估的内容会显示为未归类。",
            ],
        }
