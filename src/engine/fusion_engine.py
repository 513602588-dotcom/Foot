"""
超级融合模型 SuperFusionModel - 模型结果合理性修复版
1.  优化平局概率计算，解决固定值异常问题
2.  平衡主客队权重，避免主胜过度偏高
3.  优化置信度计算，更贴合真实预测可靠性
4.  保留所有原有逻辑，完全兼容主管道
"""
import logging
from datetime import datetime, timezone
from typing import Dict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class SuperFusionModel:
    def __init__(self):
        self.model_version = "v2.4-fix-result-reasonable"
        self.force_pure_mode = True
        logger.info(f"✅ SuperFusionModel 初始化完成，版本：{self.model_version}，强制纯模型模式：{self.force_pure_mode}")

    def predict_single_match(self, raw_match: dict, match_features: dict) -> dict:
        # 读取球队名称
        try:
            home_team_data = raw_match.get("homeTeam", {})
            away_team_data = raw_match.get("awayTeam", {})
            home_team_name = home_team_data.get("name", "").strip()
            away_team_name = away_team_data.get("name", "").strip()
        except Exception as e:
            raise ValueError(f"读取球队名称失败：{str(e)}")

        if not home_team_name:
            home_team_name = match_features.get("home_team", "").strip()
        if not away_team_name:
            away_team_name = match_features.get("away_team", "").strip()

        if not home_team_name or not away_team_name:
            raise ValueError(f"主队/客队名称缺失，主队：「{home_team_name}」，客队：「{away_team_name}」")

        # 打印特征字段（仅首次运行）
        if not hasattr(self, "_feature_keys_printed"):
            logger.info(f"特征工程输出的完整字段列表：{list(match_features.keys())}")
            self._feature_keys_printed = True

        # 主队特征读取
        home_recent_wins = match_features.get("home_recent_wins", 2)
        home_win_rate = match_features.get("home_win_rate", 0.45)
        home_goals_per_match = match_features.get("home_goals_per_match", 1.4)
        home_goals_against_per_match = match_features.get("home_goals_against_per_match", 1.1)
        home_attack_strength = match_features.get("home_attack_strength", 1.0)
        home_defense_strength = match_features.get("home_defense_strength", 1.0)
        home_home_win_rate = match_features.get("home_home_win_rate", 0.5)
        home_advantage = match_features.get("home_advantage", 0.08)  # 主场优势从0.1进一步降到0.08，平衡主客队

        # 客队特征读取（权重进一步提升，平衡主客队）
        away_recent_wins = match_features.get("away_recent_wins", 2)
        away_win_rate = match_features.get("away_win_rate", 0.45)
        away_goals_per_match = match_features.get("away_goals_per_match", 1.4)
        away_goals_against_per_match = match_features.get("away_goals_against_per_match", 1.1)
        away_attack_strength = match_features.get("away_attack_strength", 1.0)
        away_defense_strength = match_features.get("away_defense_strength", 1.0)
        away_away_win_rate = match_features.get("away_away_win_rate", 0.4)

        # ===================== 【核心修复】合理的基础得分计算 =====================
        # 主队基础得分：平衡各项权重，降低主场优势影响
        home_base_score = (
            (home_recent_wins * 0.18) + 
            (home_win_rate * 0.22) + 
            (home_goals_per_match * 0.15) + 
            (home_attack_strength * 0.12) - 
            (away_defense_strength * 0.15) + 
            (home_home_win_rate * 0.1) + 
            home_advantage
        )
        # 客队基础得分：提升客场特征权重，平衡主客队
        away_base_score = (
            (away_recent_wins * 0.18) + 
            (away_win_rate * 0.22) + 
            (away_goals_per_match * 0.15) + 
            (away_attack_strength * 0.15) - 
            (home_defense_strength * 0.15) + 
            (away_away_win_rate * 0.15)
        )

        # ===================== 【核心修复】平局概率计算优化，解决固定值问题 =====================
        score_diff = abs(home_base_score - away_base_score)
        # 平局基础分优化：分差越小，平局概率越高，无强制保底，避免固定值
        draw_base_score = max(0.1, 1.3 - (score_diff * 0.75))

        # 强制三个分数都大于0.1，避免极端值
        home_base_score = max(home_base_score, 0.2)
        away_base_score = max(away_base_score, 0.2)
        draw_base_score = max(draw_base_score, 0.1)

        # 概率归一化
        total_score = home_base_score + away_base_score + draw_base_score
        home_win_prob = round(home_base_score / total_score, 4)
        away_win_prob = round(away_base_score / total_score, 4)
        draw_prob = round(draw_base_score / total_score, 4)

        # 二次归一化，确保总和严格等于1
        total_prob = home_win_prob + away_win_prob + draw_prob
        home_win_prob = round(home_win_prob / total_prob, 4)
        away_win_prob = round(away_win_prob / total_prob, 4)
        draw_prob = round(draw_prob / total_prob, 4)

        # 强制每个概率都大于0.05，避免极端0值
        home_win_prob = max(home_win_prob, 0.05)
        away_win_prob = max(away_win_prob, 0.05)
        draw_prob = max(draw_prob, 0.05)

        # 最终归一化，确保总和100%等于1
        total_prob_final = home_win_prob + away_win_prob + draw_prob
        home_win_prob = round(home_win_prob / total_prob_final, 4)
        away_win_prob = round(away_win_prob / total_prob_final, 4)
        draw_prob = round(draw_prob / total_prob_final, 4)

        # ===================== 【修复】置信度计算优化 =====================
        max_prob = max(home_win_prob, away_win_prob, draw_prob)
        feature_complete_rate = len([k for k, v in match_features.items() if v is not None and v != 0]) / max(len(match_features), 1)
        # 平衡最大概率和特征完整度，避免置信度过低
        confidence = max(min(round((max_prob * 0.6 + feature_complete_rate * 0.4), 4), 0.95), 0.1)

        # 赔率&凯利&EV计算（主管道会覆盖，这里保留兜底）
        odds_home = max(1.1, (1 / home_win_prob) * 0.93)
        odds_draw = max(1.1, (1 / draw_prob) * 0.93)
        odds_away = max(1.1, (1 / away_win_prob) * 0.93)
        
        max_odds = odds_home if max_prob == home_win_prob else odds_away if max_prob == away_win_prob else odds_draw
        expected_value = round((max_prob * (max_odds - 1)) - ((1 - max_prob) * 1), 4)
        
        if max_odds > 1 and expected_value > 0:
            kelly_suggestion = round(((max_prob * max_odds) - 1) / (max_odds - 1), 4)
            kelly_suggestion = max(min(kelly_suggestion, 1), 0)
        else:
            kelly_suggestion = 0.0

        # 打印模型输出
        logger.info(f"📊 模型输出 {home_team_name} vs {away_team_name} 预测概率：主胜={home_win_prob:.4f}, 平局={draw_prob:.4f}, 客胜={away_win_prob:.4f}，置信度={confidence:.4f}")

        # 返回结果（完全兼容主管道）
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
