#!/usr/bin/env python3

import unittest

from label_douyin_video_evidence_v3 import evidence_level, match_points


def row(desc: str = "", uid: str = "test") -> dict[str, str]:
    return {"desc": desc, "uid": uid, "aweme_id": "test"}


def asr(text: str) -> dict:
    return {"status": "success", "text": text, "avg_logprob": -0.1}


def ocr(text: str = "") -> dict:
    return {"status": "success", "combined_text": text}


class V3RulesTest(unittest.TestCase):
    def test_caption_only_is_v1_and_capped(self) -> None:
        level, _ = evidence_level(False, {}, {}, "奔驰GLC降价了")
        self.assertEqual(level, "V1")
        matches = match_points(row("奔驰GLC现价21.88万"), {}, {}, level)
        self.assertTrue(matches)
        self.assertLess(matches[0]["score"], 75)

    def test_video_available_but_label_only_in_caption_is_still_capped(self) -> None:
        speech = "今天只聊一个与汽车价格无关的生活故事，视频里没有报价和优惠信息。"
        level, _ = evidence_level(True, asr(speech), {}, "奥迪A4L现在11.99万")
        self.assertEqual(level, "V2")
        matches = match_points(row("奥迪A4L现在11.99万"), asr(speech), {}, level)
        x3 = next(item for item in matches if item["id"] == "X3")
        self.assertLess(x3["score"], 75)

    def test_asr_price_with_platform_benefit_is_x3(self) -> None:
        text = "奥迪A4L现在11.99万元，在懂车帝还能享受五年免息优惠，买车更省钱。"
        level, _ = evidence_level(True, asr(text), {}, "")
        self.assertEqual(level, "V2")
        matches = match_points(row(), asr(text), {}, level)
        self.assertEqual(matches[0]["id"], "X3")
        self.assertGreaterEqual(matches[0]["score"], 75)

    def test_historical_price_is_not_current_price_selling_point(self) -> None:
        text = "这辆车当年50万落地，是父亲奋斗多年的体面，如今想起仍很怀念。"
        level, _ = evidence_level(True, asr(text), ocr("当年50万落地"), "")
        ids = [item["id"] for item in match_points(row(), asr(text), ocr("当年50万落地"), level)]
        self.assertNotIn("X3", ids)

    def test_mileage_is_not_car_price(self) -> None:
        text = "我现在开了8万公里，变速箱油有人说6万公里换，也有人说10万公里再换。"
        level, _ = evidence_level(True, asr(text), ocr("8万公里"), "")
        ids = [item["id"] for item in match_points(row(), asr(text), ocr("8万公里"), level)]
        self.assertNotIn("X3", ids)

    def test_story_amount_is_not_current_car_price(self) -> None:
        text = "你现在手里有十万奖金，朋友又找你借五十万，生活突然变得很荒诞。"
        level, _ = evidence_level(True, asr(text), ocr("十万奖金"), "")
        ids = [item["id"] for item in match_points(row(), asr(text), ocr("十万奖金"), level)]
        self.assertNotIn("X3", ids)

    def test_long_ocr_digit_noise_is_not_current_car_price(self) -> None:
        text = "大多数人的油车会开八到十年，电车老款价格直接腰斩，但没必要追新。"
        level, _ = evidence_level(True, asr(text), ocr("200000000888298080000 价格直接腰斩"), "")
        ids = [item["id"] for item in match_points(row(), asr(text), ocr("200000000888298080000 价格直接腰斩"), level)]
        self.assertNotIn("X3", ids)

    def test_bare_process_number_is_not_current_car_price(self) -> None:
        text = "签合同前先打全款，现在12123可以快速办理过户流程。"
        level, _ = evidence_level(True, asr("卖车过户指南"), ocr(text), "")
        ids = [item["id"] for item in match_points(row(), asr("卖车过户指南"), ocr(text), level)]
        self.assertNotIn("X3", ids)

    def test_bare_ocr_noise_after_price_words_is_not_current_car_price(self) -> None:
        text = "普通人的油车可以开八到十年，老款价格是直接腰斩 -310136，不必追新。"
        level, _ = evidence_level(True, asr("油车能开多久"), ocr(text), "")
        ids = [item["id"] for item in match_points(row(), asr("油车能开多久"), ocr(text), level)]
        self.assertNotIn("X3", ids)

    def test_unverified_future_price_is_not_x3(self) -> None:
        text = "27款桑塔纳即将发布，官方渲染图曝光，据说只要5万起步，配置可能很高。"
        level, _ = evidence_level(True, asr(text), ocr("官方渲染图"), "")
        ids = [item["id"] for item in match_points(row(), asr(text), ocr("官方渲染图"), level)]
        self.assertNotIn("X3", ids)

    def test_salary_based_recommendation_prefers_x4_over_prices(self) -> None:
        text = "你的月薪适合买什么车？月薪一万推荐轩逸，月薪两万推荐雅阁，按预算和通勤场景选车。"
        level, _ = evidence_level(True, asr(text), ocr("月薪一万 轩逸 12万；月薪两万 雅阁 18万"), "")
        matches = match_points(row(), asr(text), ocr("月薪一万 轩逸 12万；月薪两万 雅阁 18万"), level)
        self.assertEqual(matches[0]["id"], "X4")

    def test_used_car_hashtag_routes_same_listing_prices_to_e6(self) -> None:
        speech = "在懂车帝看同一二手车源在两个平台价格不同，查询市场行情和历史价格才能减少信息差。"
        level, _ = evidence_level(True, asr(speech), ocr("懂车帝 同一车源不同价格"), "#二手车数据")
        matches = match_points(row("#二手车数据"), asr(speech), ocr("懂车帝 同一车源不同价格"), level)
        self.assertEqual(matches[0]["id"], "E6")

    def test_used_car_market_content_without_dcar_is_not_e6(self) -> None:
        speech = "同一二手车源在两个平台价格不同，查询市场行情和历史价格才能减少信息差。"
        level, _ = evidence_level(True, asr(speech), ocr("同一车源不同价格"), "#二手车数据")
        ids = [item["id"] for item in match_points(row("#二手车数据"), asr(speech), ocr("同一车源不同价格"), level)]
        self.assertNotIn("E6", ids)

    def test_sales_ranking_is_not_x2_test_or_reputation(self) -> None:
        speech = "新能源MPV销量榜更新，最新月销量排行显示多款车型销量下滑。"
        level, _ = evidence_level(True, asr(speech), ocr("懂车帝 销量榜"), "")
        ids = [item["id"] for item in match_points(row(), asr(speech), ocr("懂车帝 销量榜"), level)]
        self.assertNotIn("X2", ids)

    def test_generic_app_navigation_does_not_trigger_x4(self) -> None:
        speech = "加油站员工把92号汽油加进了高性能车，车主要求处理加错油的问题。"
        ui_ocr = "发现 商城 推荐 热点 政府补贴 买车补贴 选车 价格"
        level, _ = evidence_level(True, asr(speech), ocr(ui_ocr), "")
        ids = [item["id"] for item in match_points(row(), asr(speech), ocr(ui_ocr), level)]
        self.assertNotIn("X4", ids)

    def test_used_car_navigation_tab_does_not_override_new_car_price(self) -> None:
        speech = "奔驰C级现在一口价19.9万，在懂车帝还能看两年免息。"
        ui_ocr = "车系首页 参数配置 19.49万 询底价 文章 视频 二手车"
        level, _ = evidence_level(True, asr(speech), ocr(ui_ocr), "")
        matches = match_points(row(), asr(speech), ocr(ui_ocr), level)
        self.assertEqual(matches[0]["id"], "X3")

    def test_distant_how_to_choose_phrase_in_life_story_does_not_trigger_x4(self) -> None:
        speech = "下个月工资还要还房贷。你从小被教会的不是怎么选择，而是把生活撑过去。后来买车也背上了贷款。"
        level, _ = evidence_level(True, asr(speech), ocr("人生故事"), "")
        ids = [item["id"] for item in match_points(row(), asr(speech), ocr("人生故事"), level)]
        self.assertNotIn("X4", ids)

    def test_single_deposit_in_emotional_story_is_not_x8(self) -> None:
        speech = "为了结婚我交了定金买车，后来感情结束，这辆车成了青春的纪念。"
        level, _ = evidence_level(True, asr(speech), ocr("青春故事"), "")
        ids = [item["id"] for item in match_points(row(), asr(speech), ocr("青春故事"), level)]
        self.assertNotIn("X8", ids)

    def test_unverified_configuration_reduction_is_not_x7(self) -> None:
        speech = "这款车改款以后可能会减配，外观变化不大，上市后再看。"
        level, _ = evidence_level(True, asr(speech), ocr("新车改款"), "")
        ids = [item["id"] for item in match_points(row(), asr(speech), ocr("新车改款"), level)]
        self.assertNotIn("X7", ids)

    def test_single_cost_phrase_in_emotional_story_is_not_x6(self) -> None:
        speech = "工作最难的时候，用车成本让我压力很大，但家人的支持让我坚持了下来。"
        level, _ = evidence_level(True, asr(speech), ocr("生活故事"), "")
        ids = [item["id"] for item in match_points(row(), asr(speech), ocr("生活故事"), level)]
        self.assertNotIn("X6", ids)

    def test_community_navigation_tab_alone_is_not_c3(self) -> None:
        speech = "这款车现在优惠两万元，落地价格已经进入十五万区间。"
        level, _ = evidence_level(True, asr(speech), ocr("首页 选车 车友圈 我的"), "")
        ids = [item["id"] for item in match_points(row(), asr(speech), ocr("首页 选车 车友圈 我的"), level)]
        self.assertNotIn("C3", ids)

    def test_actual_dcar_community_campaign_is_c3(self) -> None:
        speech = "来懂车帝去玩车社区参加电车还能这么玩主题改装大赛，分享你的改装作品。"
        level, _ = evidence_level(True, asr(speech), ocr("动漫主题改装"), "")
        matches = match_points(row(), asr(speech), ocr("动漫主题改装"), level)
        self.assertEqual(matches[0]["id"], "C3")
        self.assertEqual(matches[0]["source"], "ASR")

    def test_charging_explanation_uses_actual_video_evidence_for_c1(self) -> None:
        speech = "120千瓦公共快充会分流，800V液冷超充速度更快，但超时占位会收费。"
        level, _ = evidence_level(True, asr(speech), ocr("不同电桩"), "不同充电桩的区别")
        matches = match_points(row("不同充电桩的区别"), asr(speech), ocr("不同电桩"), level)
        c1 = next(item for item in matches if item["id"] == "C1")
        self.assertEqual(c1["source"], "ASR")

    def test_component_comparison_is_c1(self) -> None:
        speech = "机械把手断电和结冰时也能打开，隐藏电动把手风阻更低，但要注意故障风险。"
        level, _ = evidence_level(True, asr(speech), ocr("机械把手vs电动把手"), "")
        ids = [item["id"] for item in match_points(row(), asr(speech), ocr("机械把手vs电动把手"), level)]
        self.assertIn("C1", ids)

    def test_stray_used_car_hashtag_does_not_override_new_car_price(self) -> None:
        speech = "全新奥迪A4L现车优惠后只要十九万，裸车价格已经公布。"
        level, _ = evidence_level(True, asr(speech), ocr("全新奥迪A4L 19万"), "#二手车 #奥迪A4L")
        matches = match_points(row("#二手车 #奥迪A4L"), asr(speech), ocr("全新奥迪A4L 19万"), level)
        self.assertEqual(matches[0]["id"], "X3")

    def test_used_car_contrast_does_not_override_current_new_car_price(self) -> None:
        speech = "汉兰达新车现在只要20万，三年前二手车价格都比这个高，在懂车帝还能了解最新降价信息。"
        level, _ = evidence_level(True, asr(speech), ocr("汉兰达新车 20万"), "")
        matches = match_points(row(), asr(speech), ocr("汉兰达新车 20万"), level)
        self.assertEqual(matches[0]["id"], "X3")

    def test_new_car_guide_with_used_car_contrast_is_not_used_recommendation(self) -> None:
        speech = "家庭第一台一手新车怎么选？国产新车按预算推荐。买二手车会担心事故车，但新车有官方质保。"
        level, _ = evidence_level(True, asr(speech), ocr("国产一手车 家庭新车推荐"), "")
        ids = [item["id"] for item in match_points(row(), asr(speech), ocr("国产一手车 家庭新车推荐"), level)]
        self.assertNotIn("E4", ids)

    def test_generic_warning_about_reports_is_not_e5(self) -> None:
        speech = "买二手车不要只看车商提供的检测报告，那些报告可能不能完全反映车况。"
        level, _ = evidence_level(True, asr(speech), ocr("二手车避坑"), "#懂车帝二手车")
        ids = [item["id"] for item in match_points(row("#懂车帝二手车"), asr(speech), ocr("二手车避坑"), level)]
        self.assertNotIn("E5", ids)

    def test_used_budget_range_before_budget_hits_e3(self) -> None:
        speech = "买二手车，2-3万预算推荐这三款代步车型，车源多而且维修方便。"
        level, _ = evidence_level(True, asr(speech), ocr("2-3万预算"), "")
        ids = [item["id"] for item in match_points(row(), asr(speech), ocr("2-3万预算"), level)]
        self.assertTrue({"E3", "E4"}.intersection(ids))

    def test_generic_maintenance_routes_to_c1_not_ai(self) -> None:
        text = "汽车保养时机油和滤芯怎么换，今天讲清楚常见故障和维修避坑方法。"
        level, _ = evidence_level(True, asr(text), ocr("机油滤芯更换"), "")
        matches = match_points(row(), asr(text), ocr("机油滤芯更换"), level)
        ids = [item["id"] for item in matches]
        self.assertIn("C1", ids)
        self.assertNotIn("M3", ids)

    def test_used_car_tutorial_is_c1_not_e5_without_platform_report(self) -> None:
        text = "买二手车如何识别事故车和泡水车，先看发动机和底盘，再检查维修痕迹。"
        level, _ = evidence_level(True, asr(text), ocr("事故车识别"), "")
        ids = [item["id"] for item in match_points(row(), asr(text), ocr("事故车识别"), level)]
        self.assertIn("C1", ids)
        self.assertNotIn("E5", ids)

    def test_emotional_story_with_repair_word_is_not_c1(self) -> None:
        text = "那年爸爸为了家里修了一夜发动机，后来这台旧车成了我童年最深的回忆和体面。"
        level, _ = evidence_level(True, asr(text), ocr("爸爸的旧车"), "")
        ids = [item["id"] for item in match_points(row(), asr(text), ocr("爸爸的旧车"), level)]
        self.assertNotIn("C1", ids)

    def test_emotional_story_with_car_key_is_not_c1(self) -> None:
        text = "妈妈离开后，我在抽屉里找到那把旧车钥匙，想起小时候她送我上学的日子。"
        level, _ = evidence_level(True, asr(text), ocr("人生副本"), "#情感 #治愈")
        ids = [item["id"] for item in match_points(row("#情感 #治愈"), asr(text), ocr("人生副本"), level)]
        self.assertNotIn("C1", ids)

    def test_emotional_back_seat_story_is_not_c2(self) -> None:
        text = "婆媳关系里每次都默认你坐后排，后排宽敞只是他们劝你的理由。"
        level, _ = evidence_level(True, asr(text), ocr("情感短片"), "#情感 #婚姻")
        ids = [item["id"] for item in match_points(row("#情感 #婚姻"), asr(text), ocr("情感短片"), level)]
        self.assertNotIn("C2", ids)

    def test_story_with_actual_dcar_model_detail_task_can_be_c2(self) -> None:
        text = "故事里的水电工打开懂车帝，查看后备箱尺寸和车主体验，确认工具能不能放下。"
        level, _ = evidence_level(True, asr(text), ocr("人生副本"), "#情感")
        ids = [item["id"] for item in match_points(row("#情感"), asr(text), ocr("人生副本"), level)]
        self.assertIn("C2", ids)

    def test_multidimensional_comparison_is_x1(self) -> None:
        text = "奥迪A4L和特斯拉Model 3怎么选？全面对比空间、动力、配置和长期成本。"
        level, _ = evidence_level(True, asr(text), ocr("A4L VS Model 3"), "")
        matches = match_points(row(), asr(text), ocr("A4L VS Model 3"), level)
        self.assertEqual(matches[0]["id"], "X1")

    def test_single_model_feature_list_is_not_x1(self) -> None:
        text = "全面对比一下这辆车的空间、动力、配置和长期成本，看看值不值得买。"
        level, _ = evidence_level(True, asr(text), ocr("空间 动力 配置"), "")
        ids = [item["id"] for item in match_points(row(), asr(text), ocr("空间 动力 配置"), level)]
        self.assertNotIn("X1", ids)

    def test_comparison_navigation_ui_alone_is_not_x1(self) -> None:
        speech = "奔驰C级现在一口价19.9万，动力足、底盘稳，懂车帝还有两年免息。"
        ui_ocr = "同级对比 宝马3系 空间 动力 配置 二手车"
        level, _ = evidence_level(True, asr(speech), ocr(ui_ocr), "")
        matches = match_points(row(), asr(speech), ocr(ui_ocr), level)
        self.assertEqual(matches[0]["id"], "X3")

    def test_query_real_transaction_price_capability_is_x3(self) -> None:
        speech = "买家用MPV前别只看排面，现在用懂车帝查真实成交价和二排空间。"
        level, _ = evidence_level(True, asr(speech), ocr("家用MPV"), "")
        matches = match_points(row(), asr(speech), ocr("家用MPV"), level)
        self.assertEqual(matches[0]["id"], "X3")

    def test_ai_label_requires_ai_xiaodong(self) -> None:
        text = "仪表盘亮故障灯后要先停车检查，避免继续行驶造成危险。"
        level, _ = evidence_level(True, asr(text), ocr("故障灯"), "")
        ids = [item["id"] for item in match_points(row(), asr(text), ocr("故障灯"), level)]
        self.assertNotIn("M5", ids)


if __name__ == "__main__":
    unittest.main()
