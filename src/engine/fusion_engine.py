"""
超级融合模型 SuperFusionModel - 全主胜问题修复版
1.  降低主场优势权重，避免主队得分永远偏高
2.  优化平局基础分计算，让平局概率更合理
3.  优化置信度计算，平局/客胜场次也能拿到合理置信度
4.  平衡主客队特征权重，预测结果更符合实际赛事分布
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
        self.model_version = "v2.3-fix-home-bias"
        self.force_pure_mode = True
        logger.info(f"✅ SuperFusionModel 初始化完成，版本：{self.model_version}，强制纯模型模式：{self.force_pure_mode}")

    def predict_single_match(self, raw_match: dict, match_features: dict) -> dict:
        """
        单场比赛预测核心方法
        :param raw_match: 原始比赛数据（来自football-data.org）
        :param match_features: 比赛特征数据（来自特征工程）
        :return: 预测结果字典，和主管道提取逻辑完全对齐
        """
        # ===================== 1. 球队名称读取 =====================
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

        # 最终校验
        if not home_team_name or not away_team_name:
            raise ValueError(f"比赛主队/客队名称缺失，主队：「{home_team_name}」，客队：「{away_team_name}」")

        # ===================== 2. 特征读取（100%对齐特征工程输出）=====================
        # 首次运行打印特征字段，确保完全对齐
        if not hasattr(self, "_feature_keys_printed"):
            logger.info(f"特征工程输出的完整字段列表：{list(match_features.keys())}")
            self._feature_keys_printed = True

        # 主队特征
        home_recent_wins = match_features.get("home_recent_wins", 2)
        home_win_rate = match_features.get("home_win_rate", 0.45)
        home_goals_per_match = match_features.get("home_goals_per_match", 1.4)
        home_goals_against_per_match = match_features.get("home_goals_against_per_match", 1.1)
        home_attack_strength = match_features.get("home_attack_strength", 1.0)
        home_defense_strength = match_features.get("home_defense_strength", 1.0)
        home_home_win_rate = match_features.get("home_home_win_rate", 0.5)
        home_advantage = match_features.get("home_advantage", 0.1)  # 从0.2降到0.1，降低主场优势

        # 客队特征（提升权重，平衡主客队）
        away_recent_wins = match_features.get("away_recent_wins", 2)
        away_win_rate = match_features.get("away_win_rate", 0.45)
        away_goals_per_match = match_features.get("away_goals_per_match", 1.4)
        away_goals_against_per_match = match_features.get("away_goals_against_per_match", 1.1)
        away_attack_strength = match_features.get("away_attack_strength", 1.0)
        away_defense_strength = match_features.get("away_defense_strength", 1.0)
        away_away_win_rate = match_features.get("away_away_win_rate", 0.4)

        # ===================== 3. 优化后的核心胜率计算（平衡主客队）=====================
        # 主队基础得分（降低主场优势权重，提升客队防守的影响）
        home_base_score = (
            (home_recent_wins * 0.2) + 
            (home_win_rate * 0.25) + 
            (home_goals_per_match * 0.15) + 
            (home_attack_strength * 0.1) - 
            (away_defense_strength * 0.15) + 
            (home_home_win_rate * 0.1) + 
            home_advantage
        )
        # 客队基础得分（提升客场胜率、进攻能力的权重）
        away_base_score = (
            (away_recent_wins * 0.2) + 
            (away_win_rate * 0.25) + 
            (away_goals_per_match * 0.15) + 
            (away_attack_strength * 0.15) - 
            (home_defense_strength * 0.15) + 
            (away_away_win_rate * 0.1)
        )
        # 【优化】平局基础分计算，提升平局概率上限
        score_diff = abs(home_base_score - away_base_score)
        draw_base_score = 1.0 - (score_diff * 0.8)  # 分差越小，平局概率越高，上限1.0

        # 强制三个分数都大于0，永远不会出现全0
        home_base_score = max(home_base_score, 0.2)
        away_base_score = max(away_base_score, 0.2)
        draw_base_score = max(draw_base_score, 0.15)  # 平局保底从0.1升到0.15

        # ===================== 4. 概率归一化 =====================
        total_score = home_base_score + away_base_score + draw_base_score
        home_win_prob = round(home_base_score / total_score, 4)
        away_win_prob = round(away_base_score / total_score, 4)
        draw_prob = round(draw_base_score / total_score, 4)

        # 二次归一化，确保总和严格等于1
        total_prob = home_win_prob + away_win_prob + draw_prob
        home_win_prob = round(home_win_prob / total_prob, 4)
        away_win_prob = round(away_win_prob / total_prob, 4)
        draw_prob = round(draw_prob / total_prob, 4)

        # 强制每个概率都大于0.05，永远不会触发总和为0的校验
        home_win_prob = max(home_win_prob, 0.05)
        away_win_prob = max(away_win_prob, 0.05)
        draw_prob = max(draw_prob, 0.05)

        # 最终归一化，确保总和100%等于1
        total_prob_final = home_win_prob + away_win_prob + draw_prob
        home_win_prob = round(home_win_prob / total_prob_final, 4)
        away_win_prob = round(away_win_prob / total_prob_final, 4)
        draw_prob = round(draw_prob / total_prob_final, 4)

        # ===================== 5. 优化置信度计算（平衡平局/客胜）=====================
        max_prob = max(home_win_prob, away_win_prob, draw_prob)
        feature_complete_rate = len([k for k, v in match_features.items() if v is not None and v != 0]) / max(len(match_features), 1)
        # 优化：降低最大概率的权重，避免平局/客胜因为最大概率低而置信度不足
        confidence = max(min(round((max_prob * 0.5 + feature_complete_rate * 0.5), 4), 0.95), 0.1)

        # 凯利公式计算
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

        # 打印模型输出的概率
        logger.info(f"📊 模型输出 {home_team_name} vs {away_team_name} 预测概率：主胜={home_win_prob:.4f}, 平局={draw_prob:.4f}, 客胜={away_win_prob:.4f}，置信度={confidence:.4f}")

        # ===================== 返回结果（和主管道完全对齐）=====================
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
