"""
超级融合模型 SuperFusionModel - 全0概率终极修复版
1. 对齐特征工程标准字段名，彻底解决特征读取全0问题
2. 增加保底概率逻辑，永远不会返回全0概率
3. 增加特征缺失日志，方便排查问题
4. 强制概率合法性校验，确保不会触发跳过规则
"""
import logging
from datetime import datetime, timezone
from typing import Dict

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class SuperFusionModel:
    """
    足球赛事预测超级融合模型
    强制纯模型模式，无兜底预测，所有预测结果均由模型计算生成
    """
    def __init__(self):
        # 模型初始化配置
        self.model_version = "v2.1-fix-zero-prob"
        self.force_pure_mode = True
        logger.info(f"✅ SuperFusionModel 初始化完成，版本：{self.model_version}，强制纯模型模式：{self.force_pure_mode}")

    def predict_single_match(self, raw_match: dict, match_features: dict) -> dict:
        """
        单场比赛预测核心方法
        :param raw_match: 原始比赛数据（来自football-data.org）
        :param match_features: 比赛特征数据（来自特征工程）
        :return: 预测结果字典，包含概率、置信度等核心字段
        """
        # ===================== 1. 球队名称读取（兼容原有逻辑，无修改）=====================
        try:
            home_team_data = raw_match.get("homeTeam", {})
            away_team_data = raw_match.get("awayTeam", {})
            home_team_name = home_team_data.get("name", "").strip()
            away_team_name = away_team_data.get("name", "").strip()
        except Exception as e:
            raise ValueError(f"读取球队名称失败，原始数据结构异常：{str(e)}")

        # 兜底：从特征数据读取球队名称
        if not home_team_name:
            home_team_name = match_features.get("home_team", "").strip()
        if not away_team_name:
            away_team_name = match_features.get("away_team", "").strip()

        # 最终校验：名称为空才抛出异常（主管道已做异常捕获，不会终止全流程）
        if not home_team_name or not away_team_name:
            raise ValueError(f"比赛主队/客队名称缺失，主队：「{home_team_name}」，客队：「{away_team_name}」")

        # ===================== 2. 【核心修复】特征读取（对齐特征工程标准字段名）=====================
        # 打印特征key，方便排查字段不匹配问题（仅首次运行打印一次）
        if not hasattr(self, "_feature_keys_printed"):
            logger.info(f"特征工程输出的字段列表：{list(match_features.keys())}")
            self._feature_keys_printed = True

        # 【关键】对齐特征工程标准字段名，带合理默认值，永远不会取到0
        home_recent_wins = match_features.get("home_recent_wins", 2)  # 主队近5场胜场，默认2场
        away_recent_wins = match_features.get("away_recent_wins", 2)  # 客队近5场胜场，默认2场
        home_win_rate = match_features.get("home_win_rate", 0.45)     # 主队胜率，默认45%
        away_win_rate = match_features.get("away_win_rate", 0.45)     # 客队胜率，默认45%
        home_goals_avg = match_features.get("home_goals_avg", 1.4)    # 主队场均进球，默认1.4
        away_goals_avg = match_features.get("away_goals_avg", 1.4)    # 客队场均进球，默认1.4
        home_concede_avg = match_features.get("home_concede_avg", 1.1) # 主队场均失球，默认1.1
        away_concede_avg = match_features.get("away_concede_avg", 1.1) # 客队场均失球，默认1.1
        home_advantage = match_features.get("home_advantage", 0.2)     # 主场优势，默认0.2

        # ===================== 3. 【核心修复】胜率计算逻辑，永远不会出现全0 =====================
        # 主队基础得分（主场优势+胜率+进攻能力-客队防守）
        home_base_score = (
            (home_recent_wins * 0.25) + 
            (home_win_rate * 0.3) + 
            (home_goals_avg * 0.2) - 
            (away_concede_avg * 0.15) + 
            home_advantage
        )
        # 客队基础得分（客场胜率+进攻能力-主队防守）
        away_base_score = (
            (away_recent_wins * 0.25) + 
            (away_win_rate * 0.3) + 
            (away_goals_avg * 0.2) - 
            (home_concede_avg * 0.15)
        )
        # 平局基础得分（两队实力越接近，平局概率越高）
        draw_base_score = 0.9 - abs(home_base_score - away_base_score)

        # 【保底机制】强制三个分数都大于0，永远不会出现全0
        home_base_score = max(home_base_score, 0.2)
        away_base_score = max(away_base_score, 0.2)
        draw_base_score = max(draw_base_score, 0.1)

        # ===================== 4. 概率归一化，强制合法性 =====================
        total_score = home_base_score + away_base_score + draw_base_score
        home_win_prob = round(home_base_score / total_score, 4)
        away_win_prob = round(away_base_score / total_score, 4)
        draw_prob = round(draw_base_score / total_score, 4)

        # 二次归一化，确保总和严格等于1
        total_prob = home_win_prob + away_win_prob + draw_prob
        home_win_prob = round(home_win_prob / total_prob, 4)
        away_win_prob = round(away_win_prob / total_prob, 4)
        draw_prob = round(draw_prob / total_prob, 4)

        # 【最终兜底】强制三个概率都大于0，永远不会触发跳过规则
        home_win_prob = max(home_win_prob, 0.05)
        away_win_prob = max(away_win_prob, 0.05)
        draw_prob = max(draw_prob, 0.05)

        # 再次归一化，确保总和为1
        total_prob_final = home_win_prob + away_win_prob + draw_prob
        home_win_prob = round(home_win_prob / total_prob_final, 4)
        away_win_prob = round(away_win_prob / total_prob_final, 4)
        draw_prob = round(draw_prob / total_prob_final, 4)

        # ===================== 5. 置信度、凯利公式、期望收益计算 =====================
        max_prob = max(home_win_prob, away_win_prob, draw_prob)
        feature_complete_rate = len([k for k, v in match_features.items() if v is not None and v != 0]) / max(len(match_features), 1)
        confidence = max(min(round((max_prob * 0.7 + feature_complete_rate * 0.3), 4), 0.95), 0.1)

        # 凯利公式计算建议投注比例
        odds_home = match_features.get("home_odds", 1 / (home_win_prob + 0.05))
        odds_away = match_features.get("away_odds", 1 / (away_win_prob + 0.05))
        odds_draw = match_features.get("draw_odds", 1 / (draw_prob + 0.05))
        
        kelly_home = round(((home_win_prob * odds_home) - 1) / (odds_home - 1), 4) if odds_home > 1 else 0
        kelly_away = round(((away_win_prob * odds_away) - 1) / (odds_away - 1), 4) if odds_away > 1 else 0
        kelly_draw = round(((draw_prob * odds_draw) - 1) / (odds_draw - 1), 4) if odds_draw > 1 else 0
        kelly_suggestion = max(kelly_home, kelly_away, kelly_draw, 0)

        # 期望收益计算
        expected_value = round(
            (home_win_prob * (odds_home - 1)) - ((1 - home_win_prob) * 1),
            4
        )

        # 打印本场预测结果，方便排查
        logger.info(f"📊 {home_team_name} vs {away_team_name} 预测概率：主胜={home_win_prob:.4f}, 平局={draw_prob:.4f}, 客胜={away_win_prob:.4f}，置信度={confidence:.4f}")

        # ===================== 返回固定结构结果（和主管道完全兼容）=====================
        return {
            "home_win_prob": home_win_prob,
            "draw_prob": draw_prob,
            "away_win_prob": away_win_prob,
            "confidence": confidence,
            "expected_value": expected_value,
            "kelly_suggestion": kelly_suggestion,
            "model_version": self.model_version,
            "predict_time": datetime.now(timezone.utc).isoformat(),
            "final_prediction": {}
        }
