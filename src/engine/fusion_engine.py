"""
超级融合模型 - 综合Poisson、Elo、ML、xG等所有信息
这是最终的预测引擎，使用加权融合获得最高准确率
"""

import numpy as np
import pandas as pd
from typing import Dict, Tuple, List
import json
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SuperFusionModel:
    """超级融合预测模型"""
    
    def __init__(self):
        self.weights = {
            'poisson': 0.20,
            'elo': 0.15,
            'xgboost': 0.25,
            'dnn': 0.25,
            'xg_model': 0.10,
            'home_bias': 0.05,
        }
        self.meta_learner = None
        self.poisson_stats = {}
        self.elo_ratings = {}
        self.xg_stats = {}
    
    def set_weights(self, weights: Dict[str, float]):
        """配置模型权重"""
        total = sum(weights.values())
        self.weights = {k: v/total for k, v in weights.items()}
        logger.info(f"Updated weights: {self.weights}")
    
    def load_meta_learner(self, ml_models):
        """加载机器学习模型"""
        self.meta_learner = ml_models
    
    def predict_single_match(self, match_data: Dict, features: pd.Series) -> Dict:
        """
        预测单场比赛
        
        Args:
            match_data: 比赛基础数据 (home_team, away_team, odds等)
            features: 特征向量 (来自FeatureEngineer)
        
        Returns:
            预测结果字典
        """
        
        home_team = match_data.get('home_team')
        away_team = match_data.get('away_team')
        
        results = {
            'home_team': home_team,
            'away_team': away_team,
            'match_date': match_data.get('date'),
            'detailed_predictions': {},
            'final_prediction': {},
            'confidence': 0,
            'recommended_bet': None,
            'expected_value': 0
        }
        
        try:
            # 1. Poisson模型预测
            poisson_probs = self._predict_poisson(features)
            results['detailed_predictions']['poisson'] = poisson_probs
            
            # 2. Elo模型预测
            elo_probs = self._predict_elo(features)
            results['detailed_predictions']['elo'] = elo_probs
            
            # 3. xG统计模型
            xg_probs = self._predict_xg_model(features)
            results['detailed_predictions']['xg_model'] = xg_probs
            
            # 4. 机器学习模型（如果可用）
            ml_probs = self._predict_ml(features)
            if ml_probs:
                results['detailed_predictions']['ml'] = ml_probs
            
            # 5. 主场偏差调整
            home_bias = self._calculate_home_bias(features)
            results['detailed_predictions']['home_bias'] = home_bias
            
            # 6. 融合所有预测
            final_probs = self._fuse_predictions(
                poisson_probs,
                elo_probs,
                xg_probs,
                ml_probs,
                home_bias
            )
            
            results['final_prediction'] = {
                'win_prob': round(final_probs[0] * 100, 2),
                'draw_prob': round(final_probs[1] * 100, 2),
                'loss_prob': round(final_probs[2] * 100, 2)
            }
            
            # 7. 计算置信度
            max_prob = max(final_probs)
            results['confidence'] = round(max_prob * 100, 2)
            
            # 8. 赔率分析
            if 'odds_win' in match_data and match_data['odds_win']:
                odds_win = float(match_data['odds_win'])
                implied_prob = 1 / odds_win
                ev = (final_probs[0] - implied_prob) * odds_win
                kelly = self._kelly_criterion(final_probs[0], odds_win)
                
                results['expected_value'] = round(ev * 100, 2)
                results['kelly_stake'] = round(kelly * 100, 2)
                results['odds_value'] = 'GOOD' if ev > 0.05 else 'FAIR' if ev > 0 else 'BAD'
            
            # 9. 推荐决策
            results['recommended_bet'] = self._make_recommendation(
                final_probs,
                results['expected_value'],
                match_data
            )
            
            # 10. 解释性说明
            results['reasoning'] = self._generate_reasoning(
                results['detailed_predictions'],
                features,
                final_probs
            )
            
        except Exception as e:
            logger.error(f"Error in prediction: {e}")
            results['error'] = str(e)
        
        return results
    
    def _predict_poisson(self, features: pd.Series) -> Tuple[float, float, float]:
        """基于特征的Poisson模型预测"""
        # 从特征计算xG
        h_attack = features.get('h_attack_strength', 1.0)
        a_defense = features.get('a_defense_strength', 1.0)
        a_attack = features.get('a_attack_strength', 1.0)
        h_defense = features.get('h_defense_strength', 1.0)
        
        lambda_h = h_attack * a_defense * 1.2  # 主场加成
        lambda_a = a_attack * h_defense
        
        # 计算概率
        from scipy.stats import poisson
        win_prob = 0
        draw_prob = 0
        loss_prob = 0
        
        for h_goals in range(0, 8):
            for a_goals in range(0, 8):
                p = poisson.pmf(h_goals, lambda_h) * poisson.pmf(a_goals, lambda_a)
                if h_goals > a_goals:
                    win_prob += p
                elif h_goals == a_goals:
                    draw_prob += p
                else:
                    loss_prob += p
        
        # 正则化
        total = win_prob + draw_prob + loss_prob
        return (win_prob/total, draw_prob/total, loss_prob/total)
    
    def _predict_elo(self, features: pd.Series) -> Tuple[float, float, float]:
        """基于形态的Elo型预测"""
        # 使用球队最近状态作为Elo代理
        h_form = features.get('h_win_rate', 0.5) * 0.6 + features.get('h_draw_rate', 0.3) * 0.3
        a_form = features.get('a_win_rate', 0.5) * 0.6 + features.get('a_draw_rate', 0.3) * 0.3
        
        # 基于form差异计算概率
        form_diff = h_form - a_form
        expected_h = 1 / (1 + 10**(-form_diff / 0.6))  # 缩放因子
        
        # 平手率相关于两队实力接近度
        draw_rate = 0.3 * (1 - abs(form_diff))
        
        win_prob = expected_h * (1 - draw_rate)
        draw_prob = draw_rate
        loss_prob = (1 - expected_h) * (1 - draw_rate)
        
        # 正则化
        total = win_prob + draw_prob + loss_prob
        return (win_prob/total, draw_prob/total, loss_prob/total)
    
    def _predict_xg_model(self, features: pd.Series) -> Tuple[float, float, float]:
        """基于xG的预测模型"""
        h_xg = features.get('h_xg_per_match', 1.4)
        a_xg = features.get('a_xg_per_match', 1.1)
        
        # xG转化为得分概率
        h_conv = 0.08 + features.get('h_attack_strength', 1.0) * 0.02
        a_conv = 0.08 + features.get('a_attack_strength', 1.0) * 0.02
        
        h_expected_goals = h_xg * h_conv
        a_expected_goals = a_xg * a_conv
        
        # 使用Poisson分布
        from scipy.stats import poisson
        win_prob = 0
        draw_prob = 0
        loss_prob = 0
        
        for h_goals in range(0, 8):
            for a_goals in range(0, 8):
                p = poisson.pmf(h_goals, h_expected_goals) * poisson.pmf(a_goals, a_expected_goals)
                if h_goals > a_goals:
                    win_prob += p
                elif h_goals == a_goals:
                    draw_prob += p
                else:
                    loss_prob += p
        
        total = win_prob + draw_prob + loss_prob
        return (win_prob/total, draw_prob/total, loss_prob/total)
    
    def _predict_ml(self, features: pd.Series) -> Tuple[float, float, float]:
        """机器学习模型预测"""
        if not self.meta_learner:
            return None
        
        try:
            X = pd.DataFrame([features])
            win_prob, draw_prob, loss_prob = self.meta_learner.predict(X)
            return (win_prob[0], draw_prob[0], loss_prob[0])
        except Exception as e:
            logger.warning(f"ML prediction failed: {e}")
            return None
    
    def _calculate_home_bias(self, features: pd.Series) -> Dict:
        """主场优势调整"""
        h_home_record = features.get('h_win_rate', 0.5)  # 简化：使用主队胜率
        
        # 主场加成在5-10%之间
        home_boost = min(0.10, max(0.05, (h_home_record - 0.45) * 0.2))
        
        return {
            'home_boost': round(home_boost * 100, 2),
            'away_penalty': round(home_boost * 0.7 * 100, 2)
        }
    
    def _fuse_predictions(self, poisson_probs, elo_probs, xg_probs, ml_probs, home_bias) -> np.ndarray:
        """
        融合所有预测模型
        """
        # 初始化融合结果
        fused = np.array([0.0, 0.0, 0.0])
        weight_sum = 0
        
        # Poisson权重
        fused += np.array(poisson_probs) * self.weights['poisson']
        weight_sum += self.weights['poisson']
        
        # Elo权重
        fused += np.array(elo_probs) * self.weights['elo']
        weight_sum += self.weights['elo']
        
        # xG模型权重
        fused += np.array(xg_probs) * self.weights['xg_model']
        weight_sum += self.weights['xg_model']
        
        # ML模型权重
        if ml_probs:
            fused += np.array(ml_probs) * (self.weights['xgboost'] + self.weights['dnn'])
            weight_sum += self.weights['xgboost'] + self.weights['dnn']
        
        # 主场加成
        home_boost = home_bias['home_boost'] / 100
        fused[0] *= (1 + home_boost * 0.5)  # 主胜提升
        fused[2] *= (1 - home_boost * 0.3)  # 客负降低
        
        # 正则化
        fused = fused / fused.sum()
        
        return fused
    
    def _kelly_criterion(self, win_prob: float, odds: float) -> float:
        """Kelly准则 - 最优投注比例"""
        if odds <= 1:
            return 0
        b = odds - 1
        p = win_prob
        q = 1 - p
        f = (p * b - q) / b
        return max(0, min(f, 0.3))  # 限制在0-30%
    
    def _make_recommendation(self, probs: np.ndarray, ev: float, match_data: Dict) -> str:
        """生成投注推荐"""
        win_prob = probs[0]
        draw_prob = probs[1]
        loss_prob = probs[2]
        
        # 确定比赛方向
        if max(probs) < 0.35:
            return "SKIP"  # 不确定
        
        # 根据EV和概率推荐
        if win_prob > 0.55 and ev > 5:
            return "BET_WIN"
        elif draw_prob > 0.35 and ev > 5:
            return "BET_DRAW"
        elif loss_prob > 0.55:
            return "BET_LOSS"
        elif max(probs) > 0.45:
            return "MONITOR"
        else:
            return "SKIP"
    
    def _generate_reasoning(self, predictions: Dict, features: pd.Series, probs: np.ndarray) -> str:
        """生成预测理由"""
        reasons = []
        
        # 最大概率来源
        model_probs = {
            'Poisson': predictions.get('poisson', (0,0,0))[0],
            'Elo': predictions.get('elo', (0,0,0))[0],
            'xG': predictions.get('xg_model', (0,0,0))[0],
        }
        
        top_model = max(model_probs, key=model_probs.get)
        reasons.append(f"{top_model}模型看好主胜")
        
        # 形态分析
        if features.get('h_winning_streak', 0) >= 3:
            reasons.append("主队连胜气势")
        
        if features.get('a_injury_severity', 0) > 5:
            reasons.append("客队伤兵满地")
        
        # 历史对战
        h2h_rate = features.get('h2h_home_win_rate', 0.5)
        if h2h_rate > 0.6:
            reasons.append("历史对阵占优")
        
        return " | ".join(reasons[:3]) if reasons else "综合多项因素"


class BatchPredictor:
    """批量预测工具"""
    
    def __init__(self, fusion_model: SuperFusionModel):
        self.fusion_model = fusion_model
    
    def predict_matches(self, matches_df: pd.DataFrame, features_df: pd.DataFrame) -> List[Dict]:
        """批量预测多场比赛"""
        results = []
        
        for idx, match in matches_df.iterrows():
            if idx < len(features_df):
                features = features_df.iloc[idx]
                prediction = self.fusion_model.predict_single_match(
                    match.to_dict(),
                    features
                )
                results.append(prediction)
        
        return results
    
    def export_results(self, results: List[Dict], output_path: str):
        """导出预测结果为JSON"""
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        logger.info(f"Results exported to {output_path}")
