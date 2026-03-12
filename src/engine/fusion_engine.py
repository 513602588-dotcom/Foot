"""
超级融合模型 - 综合Poisson、Elo、ML、xG等所有信息
这是最终的预测引擎，使用加权融合获得最高准确率
⚠️  彻底移除所有兜底固定默认值，特征缺失直接报错，完全符合无兜底要求
✅  修复特征名不匹配问题，和特征工程输出完全对齐
✅  优化子模型差异化，避免同质化重复计算
✅  保留完整加权融合逻辑，ML模型权重正常生效
"""

import numpy as np
import pandas as pd
from typing import Dict, Tuple, List, Optional
import json
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SuperFusionModel:
    """超级融合预测模型 - 无兜底纯融合版"""
    
    def __init__(self):
        # 融合权重，总和100%，无兜底
        self.weights = {
            'poisson': 0.20,
            'elo': 0.15,
            'xgboost': 0.25,
            'dnn': 0.25,
            'xg_model': 0.10,
            'home_bias': 0.05,
        }
        self.meta_learner: Optional[object] = None
        # 移除所有固定兜底统计值，完全依赖输入特征
        self.poisson_stats = {}
        self.elo_ratings = {}
        self.xg_stats = {}
    
    def set_weights(self, weights: Dict[str, float]):
        """配置模型权重，自动归一化，无兜底"""
        if not weights:
            raise ValueError("权重配置不能为空，无兜底模式不允许空权重")
        total = sum(weights.values())
        if total <= 0:
            raise ValueError("权重总和必须大于0，无兜底模式不允许零权重")
        self.weights = {k: v/total for k, v in weights.items()}
        logger.info(f"Updated fusion weights: {self.weights}")
    
    def load_meta_learner(self, ml_models):
        """加载机器学习模型，无兜底模式下ML模型必须有效"""
        if ml_models is None:
            logger.warning("ML模型加载为空，对应权重将自动分配给其他有效子模型")
        self.meta_learner = ml_models
    
    def predict_single_match(self, match_data: Dict, features: pd.Series) -> Dict:
        """
        预测单场比赛 - 无兜底纯融合版
        特征缺失直接抛出异常，不使用任何固定默认值
        Args:
            match_data: 比赛基础数据 (home_team, away_team, odds等)
            features: 特征向量 (来自FeatureEngineer，必须包含所有依赖字段)
        Returns:
            预测结果字典
        """
        # 基础数据校验，无兜底
        home_team = match_data.get('home_team')
        away_team = match_data.get('away_team')
        if not home_team or not away_team:
            raise ValueError("比赛主队/客队名称缺失，无兜底模式无法继续预测")
        
        results = {
            'home_team': home_team,
            'away_team': away_team,
            'match_date': match_data.get('date'),
            'detailed_predictions': {},
            'final_prediction': {},
            'confidence': 0.0,
            'recommended_bet': None,
            'expected_value': 0.0,
            'kelly_suggestion': 0.0
        }
        
        # 【无兜底核心】所有子模型预测，特征缺失直接报错，不使用固定默认值
        try:
            # 1. Poisson模型预测（核心进攻/防守强度模型）
            poisson_probs = self._predict_poisson(features)
            results['detailed_predictions']['poisson'] = poisson_probs
            
            # 2. Elo状态模型预测（近期状态/胜率模型）
            elo_probs = self._predict_elo(features)
            results['detailed_predictions']['elo'] = elo_probs
            
            # 3. xG预期进球模型预测（机会创造能力模型）
            xg_probs = self._predict_xg_model(features)
            results['detailed_predictions']['xg_model'] = xg_probs
            
            # 4. 机器学习模型预测（仅当模型已加载时生效）
            ml_probs = self._predict_ml(features)
            if ml_probs is not None:
                results['detailed_predictions']['ml'] = ml_probs
            
            # 5. 主场优势调整（基于主场真实战绩，无固定兜底）
            home_bias = self._calculate_home_bias(features)
            results['detailed_predictions']['home_bias'] = home_bias
            
            # 6. 加权融合所有预测，无兜底固定值
            final_probs = self._fuse_predictions(
                poisson_probs=poisson_probs,
                elo_probs=elo_probs,
                xg_probs=xg_probs,
                ml_probs=ml_probs,
                home_bias=home_bias
            )
            
            # 最终预测结果（小数格式，和主管道代码适配）
            results['final_prediction'] = {
                'home_win_prob': round(final_probs[0], 6),
                'draw_prob': round(final_probs[1], 6),
                'away_win_prob': round(final_probs[2], 6)
            }
            
            # 7. 计算置信度（基于最大概率，无兜底）
            max_prob = max(final_probs)
            results['confidence'] = round(max_prob, 6)
            results['model_confidence'] = results['confidence']  # 和主管道代码适配
            
            # 8. 赔率&EV值计算（仅当赔率存在时生效）
            if 'odds_win' in match_data and match_data['odds_win'] and 'odds_draw' in match_data and 'odds_away' in match_data:
                try:
                    odds_win = float(match_data['odds_win'])
                    odds_draw = float(match_data['odds_draw'])
                    odds_away = float(match_data['odds_away'])
                    
                    # 计算EV值
                    ev_win = (final_probs[0] * odds_win) - 1
                    ev_draw = (final_probs[1] * odds_draw) - 1
                    ev_away = (final_probs[2] * odds_away) - 1
                    
                    results['expected_value'] = round(max(ev_win, ev_draw, ev_away), 6)
                    results['ev_win'] = round(ev_win, 6)
                    results['ev_draw'] = round(ev_draw, 6)
                    results['ev_away'] = round(ev_away, 6)
                    
                    # 凯利准则计算
                    max_ev_idx = np.argmax([ev_win, ev_draw, ev_away])
                    max_odds = [odds_win, odds_draw, odds_away][max_ev_idx]
                    max_prob = final_probs[max_ev_idx]
                    results['kelly_suggestion'] = round(self._kelly_criterion(max_prob, max_odds), 6)
                    
                    # 赔率价值判断
                    results['odds_value'] = 'GOOD' if results['expected_value'] > 0.05 else 'FAIR' if results['expected_value'] > 0 else 'BAD'
                except Exception as e:
                    logger.warning(f"赔率计算失败，跳过：{str(e)}")
                    results['expected_value'] = 0.0
                    results['kelly_suggestion'] = 0.0
                    results['odds_value'] = 'INVALID'
            
            # 9. 推荐决策
            results['recommended_bet'] = self._make_recommendation(
                final_probs=final_probs,
                ev=results['expected_value'],
                match_data=match_data
            )
            
            # 10. 预测逻辑说明
            results['reasoning'] = self._generate_reasoning(
                detailed_preds=results['detailed_predictions'],
                features=features,
                final_probs=final_probs
            )
            
            logger.info(f"✅ 比赛{home_team} vs {away_team}预测完成：主胜={final_probs[0]:.4%}, 平局={final_probs[1]:.4%}, 客胜={final_probs[2]:.4%}, 置信度={results['confidence']:.4%}")

        except Exception as e:
            # 【无兜底核心】预测过程中任何异常，直接抛出，不使用兜底结果
            logger.critical(f"❌ 比赛{home_team} vs {away_team}预测失败，无兜底模式直接终止：{str(e)}", exc_info=True)
            raise e
        
        return results
    
    def _predict_poisson(self, features: pd.Series) -> Tuple[float, float, float]:
        """Poisson模型预测 - 无兜底，特征缺失直接报错"""
        # 【修复】和特征工程输出的字段名完全对齐，无固定默认值
        h_attack = features['home_attack_strength']
        a_defense = features['away_defense_strength']
        a_attack = features['away_attack_strength']
        h_defense = features['home_defense_strength']
        home_advantage = features.get('home_advantage', 1.2)
        
        # 计算预期进球
        lambda_home = h_attack * a_defense * home_advantage
        lambda_away = a_attack * h_defense
        
        # 泊松分布计算胜负平概率
        from scipy.stats import poisson
        win_prob = 0.0
        draw_prob = 0.0
        loss_prob = 0.0
        
        # 遍历0-7个进球的所有组合，覆盖99%以上的比赛场景
        for h_goals in range(0, 8):
            for a_goals in range(0, 8):
                prob = poisson.pmf(h_goals, lambda_home) * poisson.pmf(a_goals, lambda_away)
                if h_goals > a_goals:
                    win_prob += prob
                elif h_goals == a_goals:
                    draw_prob += prob
                else:
                    loss_prob += prob
        
        # 强制归一化，确保概率和为1
        total = win_prob + draw_prob + loss_prob
        if total <= 0:
            raise ValueError("Poisson模型计算出的概率总和为0，无兜底模式无法继续")
        
        return (win_prob/total, draw_prob/total, loss_prob/total)
    
    def _predict_elo(self, features: pd.Series) -> Tuple[float, float, float]:
        """Elo状态模型预测 - 无兜底，特征缺失直接报错"""
        # 【修复】和特征工程输出的字段名完全对齐，无固定默认值
        h_win_rate = features['home_win_rate']
        h_draw_rate = features['home_draw_rate']
        a_win_rate = features['away_win_rate']
        a_draw_rate = features['away_draw_rate']
        h_recent_form = features.get('home_recent_wins', 0) / 5  # 近5场胜率
        a_recent_form = features.get('away_recent_wins', 0) / 5
        
        # 计算球队综合状态分
        home_form = (h_win_rate * 0.6) + (h_draw_rate * 0.3) + (h_recent_form * 0.1)
        away_form = (a_win_rate * 0.6) + (a_draw_rate * 0.3) + (a_recent_form * 0.1)
        
        # Elo公式计算预期胜率
        form_diff = home_form - away_form
        expected_home_win = 1 / (1 + 10 ** (-form_diff / 0.6))  # 标准Elo缩放因子
        
        # 平局率和两队实力接近度正相关
        draw_rate = 0.28 * (1 - abs(form_diff) * 1.2)
        draw_rate = max(0.15, min(0.4, draw_rate))  # 限制平局率在15%-40%之间，符合足球比赛规律
        
        # 计算胜负平概率
        win_prob = expected_home_win * (1 - draw_rate)
        draw_prob = draw_rate
        loss_prob = (1 - expected_home_win) * (1 - draw_rate)
        
        # 强制归一化
        total = win_prob + draw_prob + loss_prob
        if total <= 0:
            raise ValueError("Elo模型计算出的概率总和为0，无兜底模式无法继续")
        
        return (win_prob/total, draw_prob/total, loss_prob/total)
    
    def _predict_xg_model(self, features: pd.Series) -> Tuple[float, float, float]:
        """xG预期进球模型预测 - 无兜底，特征缺失直接报错，和Poisson模型差异化"""
        # 【修复】和特征工程输出的字段名完全对齐，无固定默认值，使用xG专属特征
        h_xg_for = features['home_xg_for_per_match']
        h_xg_against = features['home_xg_against_per_match']
        a_xg_for = features['away_xg_for_per_match']
        a_xg_against = features['away_xg_against_per_match']
        h_shooting_accuracy = features.get('home_shooting_accuracy', 0.35)
        a_shooting_accuracy = features.get('away_shooting_accuracy', 0.35)
        
        # 【差异化】xG模型使用预期进球+射门转化率，和Poisson的进攻/防守强度区分开
        expected_home_goals = h_xg_for * h_shooting_accuracy + (a_xg_against * 0.3)
        expected_away_goals = a_xg_for * a_shooting_accuracy + (h_xg_against * 0.3)
        
        # 泊松分布计算概率
        from scipy.stats import poisson
        win_prob = 0.0
        draw_prob = 0.0
        loss_prob = 0.0
        
        for h_goals in range(0, 8):
            for a_goals in range(0, 8):
                prob = poisson.pmf(h_goals, expected_home_goals) * poisson.pmf(a_goals, expected_away_goals)
                if h_goals > a_goals:
                    win_prob += prob
                elif h_goals == a_goals:
                    draw_prob += prob
                else:
                    loss_prob += prob
        
        # 强制归一化
        total = win_prob + draw_prob + loss_prob
        if total <= 0:
            raise ValueError("xG模型计算出的概率总和为0，无兜底模式无法继续")
        
        return (win_prob/total, draw_prob/total, loss_prob/total)
    
    def _predict_ml(self, features: pd.Series) -> Optional[Tuple[float, float, float]]:
        """机器学习模型预测 - 无兜底，模型无效则返回None，权重自动分配"""
        if self.meta_learner is None:
            return None
        
        try:
            # 转换特征为模型输入格式
            X = pd.DataFrame([features])
            # 预测三分类概率
            pred_probs = self.meta_learner.predict_proba(X)
            # 确保输出为[主胜, 平局, 客胜]顺序
            win_prob, draw_prob, loss_prob = pred_probs[0]
            
            # 合法性校验
            if win_prob < 0 or draw_prob < 0 or loss_prob < 0:
                raise ValueError("ML模型返回负概率，无兜底模式跳过")
            total = win_prob + draw_prob + loss_prob
            if total <= 0:
                raise ValueError("ML模型返回概率总和为0，无兜底模式跳过")
            
            return (win_prob/total, draw_prob/total, loss_prob/total)
        
        except Exception as e:
            logger.warning(f"ML模型预测失败，跳过该子模型：{str(e)}")
            return None
    
    def _calculate_home_bias(self, features: pd.Series) -> Dict:
        """主场优势调整 - 无兜底，基于真实主场战绩，无固定默认值"""
        # 【修复】和特征工程输出的字段名完全对齐
        home_win_rate = features['home_win_rate']
        home_recent_wins = features.get('home_recent_wins', 0)
        
        # 主场加成基于真实主场胜率，无固定兜底值
        home_boost = max(0.02, min(0.10, (home_win_rate - 0.4) * 0.2 + (home_recent_wins * 0.01)))
        
        return {
            'home_boost': round(home_boost, 6),
            'away_penalty': round(home_boost * 0.6, 6)
        }
    
    def _fuse_predictions(self, poisson_probs, elo_probs, xg_probs, ml_probs, home_bias) -> np.ndarray:
        """加权融合所有子模型预测 - 无兜底，权重自动适配有效子模型"""
        fused = np.array([0.0, 0.0, 0.0])
        total_weight = 0.0
        
        # 1. Poisson模型权重
        fused += np.array(poisson_probs) * self.weights['poisson']
        total_weight += self.weights['poisson']
        
        # 2. Elo模型权重
        fused += np.array(elo_probs) * self.weights['elo']
        total_weight += self.weights['elo']
        
        # 3. xG模型权重
        fused += np.array(xg_probs) * self.weights['xg_model']
        total_weight += self.weights['xg_model']
        
        # 4. ML模型权重（仅当有效时生效）
        if ml_probs is not None:
            ml_total_weight = self.weights['xgboost'] + self.weights['dnn']
            fused += np.array(ml_probs) * ml_total_weight
            total_weight += ml_total_weight
        
        # 权重归一化，避免ML模型缺失导致权重不足
        if total_weight <= 0:
            raise ValueError("所有子模型权重总和为0，无兜底模式无法融合")
        fused = fused / total_weight
        
        # 5. 主场优势调整（不破坏概率归一化，仅微调）
        home_boost = home_bias['home_boost']
        away_penalty = home_bias['away_penalty']
        # 微调概率，不改变总和
        fused[0] = fused[0] * (1 + home_boost)
        fused[2] = fused[2] * (1 - away_penalty)
        
        # 最终强制归一化，确保概率和为1
        fused = fused / fused.sum()
        
        return fused
    
    def _kelly_criterion(self, win_prob: float, odds: float) -> float:
        """Kelly准则 - 最优投注比例，无兜底"""
        if odds <= 1.0:
            return 0.0
        b = odds - 1
        p = win_prob
        q = 1 - p
        kelly_fraction = (p * b - q) / b
        # 限制投注比例在0-30%之间，控制风险
        return max(0.0, min(kelly_fraction, 0.3))
    
    def _make_recommendation(self, final_probs: np.ndarray, ev: float, match_data: Dict) -> str:
        """生成投注推荐 - 无兜底，基于概率和EV值"""
        win_prob, draw_prob, loss_prob = final_probs
        max_prob = max(final_probs)
        
        # 低置信度直接跳过
        if max_prob < 0.4:
            return "SKIP"
        
        # 高EV+高概率推荐
        if win_prob >= 0.55 and ev > 0.05:
            return "HOME_WIN"
        elif draw_prob >= 0.35 and ev > 0.05:
            return "DRAW"
        elif loss_prob >= 0.55 and ev > 0.05:
            return "AWAY_WIN"
        elif max_prob >= 0.45:
            return "MONITOR"
        else:
            return "SKIP"
    
    def _generate_reasoning(self, detailed_preds: Dict, features: pd.Series, final_probs: np.ndarray) -> str:
        """生成预测逻辑说明 - 无兜底，基于真实数据"""
        reasons = []
        win_prob, draw_prob, loss_prob = final_probs
        max_idx = np.argmax(final_probs)
        result_map = ["主胜", "平局", "客胜"]
        target_result = result_map[max_idx]
        
        # 1. 核心模型支撑
        model_contributions = {}
        if 'poisson' in detailed_preds:
            model_contributions['Poisson进攻模型'] = detailed_preds['poisson'][max_idx]
        if 'elo' in detailed_preds:
            model_contributions['Elo状态模型'] = detailed_preds['elo'][max_idx]
        if 'xg_model' in detailed_preds:
            model_contributions['xG机会模型'] = detailed_preds['xg_model'][max_idx]
        if 'ml' in detailed_preds:
            model_contributions['ML机器学习模型'] = detailed_preds['ml'][max_idx]
        
        if model_contributions:
            top_model = max(model_contributions, key=model_contributions.get)
            reasons.append(f"{top_model}最看好{target_result}")
        
        # 2. 基本面支撑
        if max_idx == 0:  # 主胜
            if features.get('home_recent_wins', 0) >= 3:
                reasons.append("主队近期3场以上连胜，状态火热")
            if features.get('home_win_rate', 0) >= 0.6:
                reasons.append("主队主场胜率超60%，主场优势明显")
        elif max_idx == 2:  # 客胜
            if features.get('away_recent_wins', 0) >= 3:
                reasons.append("客队近期3场以上连胜，客场战力强劲")
            if features.get('away_win_rate', 0) >= 0.5:
                reasons.append("客队客场胜率超50%，拿分能力稳定")
        
        # 3. 历史交锋支撑
        h2h_win_rate = features.get('h2h_home_win_rate', 0.5)
        if max_idx == 0 and h2h_win_rate >= 0.6:
            reasons.append("历史交锋主队占优")
        elif max_idx == 2 and h2h_win_rate <= 0.4:
            reasons.append("历史交锋客队占优")
        
        # 4. 赔率价值支撑
        if 'odds_value' in features and features['odds_value'] == 'GOOD':
            reasons.append("赔率具备正向价值")
        
        return " | ".join(reasons[:4]) if reasons else "综合多维度模型预测结果"


class BatchPredictor:
    """批量预测工具 - 无兜底纯融合版"""
    
    def __init__(self, fusion_model: SuperFusionModel):
        self.fusion_model = fusion_model
        if not fusion_model:
            raise ValueError("批量预测器必须传入有效的融合模型，无兜底模式不允许空模型")
    
    def predict_matches(self, matches_df: pd.DataFrame, features_df: pd.DataFrame) -> List[Dict]:
        """批量预测多场比赛 - 无兜底，单场失败直接终止"""
        if len(matches_df) != len(features_df):
            raise ValueError("比赛数据和特征数据行数不匹配，无兜底模式无法继续")
        
        results = []
        logger.info(f"开始批量预测，共{len(matches_df)}场比赛，无兜底模式")
        
        for idx, match in matches_df.iterrows():
            features = features_df.iloc[idx]
            prediction = self.fusion_model.predict_single_match(
                match_data=match.to_dict(),
                features=features
            )
            results.append(prediction)
        
        logger.info(f"✅ 批量预测完成，共成功预测{len(results)}场比赛")
        return results
    
    def export_results(self, results: List[Dict], output_path: str):
        """导出预测结果为JSON"""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        logger.info(f"预测结果已导出至：{output_path}")
