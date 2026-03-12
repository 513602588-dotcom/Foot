"""
超级融合模型 - 综合Poisson、Elo、ML、xG等所有信息
【适配版】100%对齐原版核心架构，兼容已跑通的主管道、GitHub部署流程
修复：主场重复加成、特征字段不匹配、平局概率异常、EV计算对齐原版
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
    """超级融合预测模型（原版架构适配版）"""
    
    def __init__(self):
        # 完全对齐原版权重配置，总和100%
        self.weights = {
            'poisson': 0.20,
            'elo': 0.15,
            'xgboost': 0.25,
            'dnn': 0.25,
            'xg_model': 0.10,
            'home_bias': 0.05,
        }
        # 权重归一化，确保总和1
        total_weight = sum(self.weights.values())
        self.weights = {k: round(v/total_weight, 4) for k, v in self.weights.items()}
        self.meta_learner = None
        self.force_pure_mode = True  # 兼容你之前的纯模型模式开关
        self.model_version = "v2.0-Pro-适配版"
        logger.info(f"✅ SuperFusionModel 初始化完成，版本：{self.model_version}，权重配置：{self.weights}")
    
    def set_weights(self, weights: Dict[str, float]):
        """配置模型权重（对齐原版接口）"""
        total = sum(weights.values())
        self.weights = {k: round(v/total, 4) for k, v in weights.items()}
        logger.info(f"✅ 权重已更新：{self.weights}")
    
    def load_meta_learner(self, ml_models):
        """加载机器学习模型（对齐原版接口，兼容无模型场景）"""
        if ml_models:
            self.meta_learner = ml_models
            logger.info("✅ 机器学习模型加载成功")
        else:
            logger.warning("⚠️ 未传入有效ML模型，XGBoost+DNN权重将自动重新分配")
    
    def predict_single_match(self, match_data: Dict, features: pd.Series) -> Dict:
        """
        预测单场比赛（完全对齐原版入参/出参，兼容你的主管道）
        Args:
            match_data: 比赛基础数据（带真实赔率）
            features: 特征工程输出的特征向量
        Returns:
            预测结果字典，完全兼容主管道读取
        """
        # 兼容你主管道的字段名
        home_team = match_data.get('home_team', features.get('home_team', ''))
        away_team = match_data.get('away_team', features.get('away_team', ''))
        
        # 出参完全兼容你之前的主管道，不用改主管道代码
        results = {
            'home_team': home_team,
            'away_team': away_team,
            'match_date': match_data.get('date'),
            'detailed_predictions': {},
            'final_prediction': {},
            'confidence': 0.6,
            'recommended_bet': None,
            'expected_value': 0,
            'kelly_stake': 0,
            # 兼容你主管道的字段读取
            'home_win_prob': 0.0,
            'draw_prob': 0.0,
            'away_win_prob': 0.0,
            'model_version': self.model_version
        }
        
        try:
            # 1. Poisson模型预测（对齐原版，移除重复主场加成）
            poisson_probs = self._predict_poisson(features)
            results['detailed_predictions']['poisson'] = poisson_probs
            
            # 2. Elo模型预测（对齐原版，修复平局概率逻辑）
            elo_probs = self._predict_elo(features)
            results['detailed_predictions']['elo'] = elo_probs
            
            # 3. xG统计模型（对齐原版，适配特征字段）
            xg_probs = self._predict_xg_model(features)
            results['detailed_predictions']['xg_model'] = xg_probs
            
            # 4. 机器学习模型（对齐原版，兼容无模型场景）
            ml_probs = self._predict_ml(features)
            if ml_probs:
                results['detailed_predictions']['ml'] = ml_probs
            
            # 5. 主场偏差调整（对齐原版，仅在这里加一次主场优势）
            home_bias = self._calculate_home_bias(features)
            results['detailed_predictions']['home_bias'] = home_bias
            
            # 6. 融合所有预测（对齐原版加权融合逻辑）
            final_probs = self._fuse_predictions(
                poisson_probs,
                elo_probs,
                xg_probs,
                ml_probs,
                home_bias
            )
            
            # 兼容主管道的字段读取，不用改主管道
            home_win_prob, draw_prob, away_win_prob = final_probs
            results['home_win_prob'] = home_win_prob
            results['draw_prob'] = draw_prob
            results['away_win_prob'] = away_win_prob
            
            results['final_prediction'] = {
                'win_prob': round(home_win_prob * 100, 2),
                'draw_prob': round(draw_prob * 100, 2),
                'loss_prob': round(away_win_prob * 100, 2)
            }
            
            # 7. 计算置信度（对齐原版逻辑，优化合理性）
            max_prob = max(final_probs)
            feature_complete_rate = len([k for k, v in features.items() if pd.notna(v) and v != 0]) / max(len(features), 1)
            results['confidence'] = round(max_prob * 0.7 + feature_complete_rate * 0.3, 4)
            
            # 8. 赔率分析&EV&凯利计算（100%对齐原版value.py的公式）
            odds_win = match_data.get('odds_win', None)
            odds_draw = match_data.get('odds_draw', None)
            odds_away = match_data.get('odds_away', None)
            
            if odds_win and odds_draw and odds_away and float(odds_win) > 1 and float(odds_draw) > 1 and float(odds_away) > 1:
                odds_win = float(odds_win)
                odds_draw = float(odds_draw)
                odds_away = float(odds_away)
                
                # 按最大概率取对应赔率
                max_index = np.argmax(final_probs)
                if max_index == 0:
                    use_odds = odds_win
                    use_prob = home_win_prob
                elif max_index == 1:
                    use_odds = odds_draw
                    use_prob = draw_prob
                else:
                    use_odds = odds_away
                    use_prob = away_win_prob
                
                # 【100%对齐原版value.py】EV计算公式
                expected_value = (use_prob * use_odds) - 1.0
                kelly = self._kelly_criterion(use_prob, use_odds)
                
                results['expected_value'] = round(expected_value, 4)
                results['kelly_stake'] = round(kelly, 4)
                results['odds_value'] = 'GOOD' if expected_value > 0.05 else 'FAIR' if expected_value > 0 else 'BAD'
                results['used_odds'] = use_odds
            else:
                # 无真实赔率时的兜底，不报错
                results['expected_value'] = 0.0
                results['kelly_stake'] = 0.0
                results['odds_value'] = 'NO_ODDS'
                logger.warning(f"⚠️ {home_team} vs {away_team} 无有效赔率，EV&凯利使用默认值")
            
            # 9. 推荐决策（对齐原版逻辑）
            results['recommended_bet'] = self._make_recommendation(
                final_probs,
                results['expected_value'],
                match_data
            )
            
            # 10. 解释性说明（对齐原版逻辑）
            results['reasoning'] = self._generate_reasoning(
                results['detailed_predictions'],
                features,
                final_probs
            )
            
            # 打印日志，和你之前的主管道完全兼容
            logger.info(f"📊 模型输出 {home_team} vs {away_team} 预测概率：主胜={home_win_prob:.4f}, 平局={draw_prob:.4f}, 客胜={away_win_prob:.4f}，置信度={results['confidence']:.4f}")

        except Exception as e:
            logger.error(f"❌ {home_team} vs {away_team} 预测失败：{e}", exc_info=True)
            results['error'] = str(e)
        
        return results
    
    def _predict_poisson(self, features: pd.Series) -> Tuple[float, float, float]:
        """基于特征的Poisson模型预测【对齐原版，移除重复主场加成】"""
        # 【适配特征字段】完全对齐你feature_engineering.py输出的h_xxx/a_xxx字段
        h_attack = features.get('h_attack_strength', 1.0)
        a_defense = features.get('a_defense_strength', 1.0)
        a_attack = features.get('a_attack_strength', 1.0)
        h_defense = features.get('h_defense_strength', 1.0)
        
        # 【对齐原版】主场加成仅在融合阶段加，这里不重复叠加，避免主胜过度放大
        lambda_h = h_attack * a_defense
        lambda_a = a_attack * h_defense
        
        # 对齐原版，用scipy.stats.poisson计算概率
        from scipy.stats import poisson
        win_prob = 0.0
        draw_prob = 0.0
        loss_prob = 0.0
        
        # 扩大进球范围到0-10，提升概率准确性
        for h_goals in range(0, 11):
            for a_goals in range(0, 11):
                p = poisson.pmf(h_goals, lambda_h) * poisson.pmf(a_goals, lambda_a)
                if h_goals > a_goals:
                    win_prob += p
                elif h_goals == a_goals:
                    draw_prob += p
                else:
                    loss_prob += p
        
        # 正则化，确保总和1
        total = win_prob + draw_prob + loss_prob
        return (round(win_prob/total, 4), round(draw_prob/total, 4), round(loss_prob/total, 4))
    
    def _predict_elo(self, features: pd.Series) -> Tuple[float, float, float]:
        """基于形态的Elo型预测【对齐原版，修复平局概率逻辑】"""
        # 【适配特征字段】对齐feature_engineering.py输出的字段
        h_win_rate = features.get('h_win_rate', 0.5)
        h_draw_rate = features.get('h_draw_rate', 0.3)
        a_win_rate = features.get('a_win_rate', 0.5)
        a_draw_rate = features.get('a_draw_rate', 0.3)
        
        # 对齐原版form计算逻辑
        h_form = h_win_rate * 0.6 + h_draw_rate * 0.3
        a_form = a_win_rate * 0.6 + a_draw_rate * 0.3
        
        # 对齐原版Elo预期胜率计算
        form_diff = h_form - a_form
        expected_h = 1 / (1 + 10**(-form_diff / 0.6))
        
        # 【对齐原版，修复平局概率】避免固定值、极端值，符合真实联赛情况
        base_draw_rate = 0.25
        draw_boost = 0.15 * (1 - abs(form_diff))
        draw_rate = min(0.45, max(0.1, base_draw_rate + draw_boost))
        
        win_prob = expected_h * (1 - draw_rate)
        draw_prob = draw_rate
        loss_prob = (1 - expected_h) * (1 - draw_rate)
        
        # 正则化
        total = win_prob + draw_prob + loss_prob
        return (round(win_prob/total, 4), round(draw_prob/total, 4), round(loss_prob/total, 4))
    
    def _predict_xg_model(self, features: pd.Series) -> Tuple[float, float, float]:
        """基于xG的预测模型【对齐原版，适配特征字段】"""
        # 【适配特征字段】对齐feature_engineering.py输出的字段，无xG用进球兜底
        h_xg = features.get('h_xg_per_match', features.get('h_goals_per_match', 1.4))
        a_xg = features.get('a_xg_per_match', features.get('a_goals_per_match', 1.1))
        h_attack_strength = features.get('h_attack_strength', 1.0)
        a_attack_strength = features.get('a_attack_strength', 1.0)
        
        # 对齐原版xG转换逻辑
        h_conv = 0.08 + h_attack_strength * 0.02
        a_conv = 0.08 + a_attack_strength * 0.02
        
        h_expected_goals = h_xg * h_conv
        a_expected_goals = a_xg * a_conv
        
        # 对齐原版Poisson计算
        from scipy.stats import poisson
        win_prob = 0.0
        draw_prob = 0.0
        loss_prob = 0.0
        
        for h_goals in range(0, 11):
            for a_goals in range(0, 11):
                p = poisson.pmf(h_goals, h_expected_goals) * poisson.pmf(a_goals, a_expected_goals)
                if h_goals > a_goals:
                    win_prob += p
                elif h_goals == a_goals:
                    draw_prob += p
                else:
                    loss_prob += p
        
        total = win_prob + draw_prob + loss_prob
        return (round(win_prob/total, 4), round(draw_prob/total, 4), round(loss_prob/total, 4))
    
    def _predict_ml(self, features: pd.Series) -> Tuple[float, float, float]:
        """机器学习模型预测【对齐原版，兼容无模型场景】"""
        if not self.meta_learner or self.force_pure_mode:
            return None
        
        try:
            X = pd.DataFrame([features])
            win_prob, draw_prob, loss_prob = self.meta_learner.predict(X)
            return (round(win_prob[0], 4), round(draw_prob[0], 4), round(loss_prob[0], 4))
        except Exception as e:
            logger.warning(f"⚠️ ML预测失败，已禁用：{e}")
            self.meta_learner = None
            return None
    
    def _calculate_home_bias(self, features: pd.Series) -> Dict:
        """主场优势调整【对齐原版，仅在这里加一次主场优势】"""
        # 【适配特征字段】对齐feature_engineering.py输出的字段
        h_home_win_rate = features.get('h_home_win_rate', features.get('h_win_rate', 0.5))
        
        # 对齐原版，主场加成2%-8%，和主场胜率正相关
        home_boost = min(0.08, max(0.02, (h_home_win_rate - 0.4) * 0.15))
        
        return {
            'home_boost': round(home_boost * 100, 2),
            'away_penalty': round(home_boost * 0.7 * 100, 2)
        }
    
    def _fuse_predictions(self, poisson_probs, elo_probs, xg_probs, ml_probs, home_bias) -> np.ndarray:
        """融合所有预测模型【100%对齐原版加权融合逻辑】"""
        fused = np.array([0.0, 0.0, 0.0])
        used_weight = 0.0
        
        # 1. Poisson权重
        fused += np.array(poisson_probs) * self.weights['poisson']
        used_weight += self.weights['poisson']
        
        # 2. Elo权重
        fused += np.array(elo_probs) * self.weights['elo']
        used_weight += self.weights['elo']
        
        # 3. xG模型权重
        fused += np.array(xg_probs) * self.weights['xg_model']
        used_weight += self.weights['xg_model']
        
        # 4. ML模型权重（仅当ML可用且非纯模型模式时生效）
        if ml_probs and not self.force_pure_mode:
            ml_total_weight = self.weights['xgboost'] + self.weights['dnn']
            fused += np.array(ml_probs) * ml_total_weight
            used_weight += ml_total_weight
        
        # 权重归一化，避免ML缺失导致的权重失真
        if used_weight > 0:
            fused = fused / used_weight
        
        # 5. 主场加成（对齐原版，仅在这里加一次，避免重复叠加）
        home_boost = home_bias['home_boost'] / 100
        fused[0] = min(0.95, fused[0] + home_boost)
        fused[2] = max(0.05, fused[2] - home_boost * 0.5)
        
        # 最终正则化，确保总和严格等于1
        fused = fused / fused.sum()
        return np.round(fused, 4)
    
    def _kelly_criterion(self, win_prob: float, odds: float) -> float:
        """Kelly准则 - 最优投注比例【100%对齐原版value.py】"""
        if odds <= 1:
            return 0.0
        b = odds - 1
        p = win_prob
        q = 1 - p
        f = (p * b - q) / b
        return max(0.0, min(f, 0.3))  # 对齐原版，限制0-30%
    
    def _make_recommendation(self, probs: np.ndarray, ev: float, match_data: Dict) -> str:
        """生成投注推荐【对齐原版逻辑】"""
        max_prob = max(probs)
        if max_prob < 0.35:
            return "SKIP"
        
        if ev > 0.05:
            return "BET"
        elif max_prob > 0.55:
            return "MONITOR"
        else:
            return "SKIP"
    
    def _generate_reasoning(self, predictions: Dict, features: pd.Series, probs: np.ndarray) -> str:
        """生成预测理由【对齐原版逻辑】"""
        reasons = []
        result_map = ["主胜", "平局", "客胜"]
        top_result = result_map[np.argmax(probs)]
        
        # 最大概率来源
        model_probs = {
            'Poisson': predictions.get('poisson', (0,0,0))[np.argmax(probs)],
            'Elo': predictions.get('elo', (0,0,0))[np.argmax(probs)],
            'xG模型': predictions.get('xg_model', (0,0,0))[np.argmax(probs)],
        }
        top_model = max(model_probs, key=model_probs.get)
        reasons.append(f"{top_model}模型看好{top_result}")
        
        # 形态分析
        h_wins = features.get('h_winning_streak', 0)
        a_wins = features.get('a_winning_streak', 0)
        if h_wins >= 3:
            reasons.append(f"主队{h_wins}连胜状态火热")
        if a_wins >= 3:
            reasons.append(f"客队{a_wins}连胜状态出色")
        
        return " | ".join(reasons[:3]) if reasons else "综合球队攻防、近期状态多维度分析"
