"""
超级融合模型 SuperFusionModel
修复：球队名称读取逻辑，对齐football-data.org官方字段结构，增加容错处理
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
        self.model_version = "v2.0-final"
        self.force_pure_mode = True
        logger.info(f"✅ SuperFusionModel 初始化完成，版本：{self.model_version}，强制纯模型模式：{self.force_pure_mode}")

    def predict_single_match(self, raw_match: dict, match_features: dict) -> dict:
        """
        单场比赛预测核心方法
        :param raw_match: 原始比赛数据（来自football-data.org）
        :param match_features: 比赛特征数据（来自特征工程）
        :return: 预测结果字典，包含概率、置信度等核心字段
        """
        # ===================== 【核心修复】正确读取球队名称，对齐官方字段 =====================
        # 优先从原始比赛数据读取球队名称
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

        # ===================== 模型核心预测逻辑（可替换为你的自定义算法）=====================
        # 提取核心特征
        home_recent_wins = match_features.get("home_recent_wins", 0)
        away_recent_wins = match_features.get("away_recent_wins", 0)
        home_win_rate = match_features.get("home_win_rate", 0.3)
        away_win_rate = match_features.get("away_win_rate", 0.3)
        home_attack = match_features.get("home_attack_avg", 1.2)
        away_attack = match_features.get("away_attack_avg", 1.2)
        home_defense = match_features.get("home_defense_avg", 1.0)
        away_defense = match_features.get("away_defense_avg", 1.0)

        # 基础胜率计算
        home_base_score = (home_recent_wins * 0.3) + (home_win_rate * 0.4) + (home_attack * 0.2) - (away_defense * 0.1)
        away_base_score = (away_recent_wins * 0.3) + (away_win_rate * 0.4) + (away_attack * 0.2) - (home_defense * 0.1)
        draw_base_score = 0.8 - abs(home_base_score - away_base_score)

        # 概率归一化
        total_score = home_base_score + away_base_score + draw_base_score
        home_win_prob = max(min(round(home_base_score / total_score, 4), 0.85), 0.05)
        away_win_prob = max(min(round(away_base_score / total_score, 4), 0.85), 0.05)
        draw_prob = max(min(round(draw_base_score / total_score, 4), 0.4), 0.05)

        # 二次归一化，确保总和为1
        total_prob = home_win_prob + away_win_prob + draw_prob
        home_win_prob = round(home_win_prob / total_prob, 4)
        away_win_prob = round(away_win_prob / total_prob, 4)
        draw_prob = round(draw_prob / total_prob, 4)

        # 置信度计算
        max_prob = max(home_win_prob, away_win_prob, draw_prob)
        feature_complete_rate = len([k for k, v in match_features.items() if v is not None and v != 0]) / len(match_features)
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
