"""
预测准确度优化和性能分析工具
包括回测、模型评估、特征重要性分析等
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import json
import logging
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, log_loss, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ModelEvaluator:
    """模型评估工具"""
    
    @staticmethod
    def evaluate_predictions(y_true: np.ndarray, y_pred: np.ndarray, y_pred_proba: np.ndarray = None) -> Dict:
        """
        评估预测性能
        
        Args:
            y_true: 真实标签 (0=loss, 1=draw, 2=win)
            y_pred: 预测标签
            y_pred_proba: 预测概率
        
        Returns:
            性能指标字典
        """
        metrics = {
            'accuracy': accuracy_score(y_true, y_pred),
            'precision_macro': precision_score(y_true, y_pred, average='macro', zero_division=0),
            'recall_macro': recall_score(y_true, y_pred, average='macro', zero_division=0),
            'f1_macro': f1_score(y_true, y_pred, average='macro', zero_division=0),
        }
        
        # 多类AUC
        if y_pred_proba is not None and len(np.unique(y_true)) > 1:
            try:
                metrics['auc_micro'] = roc_auc_score(y_true, y_pred_proba[:, 1:], multi_class='ovr', average='micro')
                metrics['logloss'] = log_loss(y_true, y_pred_proba)
            except Exception as e:
                logger.warning(f"Could not compute AUC/logloss: {e}")
        
        # 混淆矩阵
        cm = confusion_matrix(y_true, y_pred)
        metrics['confusion_matrix'] = cm.tolist()
        
        return metrics
    
    @staticmethod
    def get_performance_by_confidence(predictions: List[Dict]) -> Dict:
        """按置信度分层的性能分析"""
        
        confidence_bins = {
            'high': [],      # >= 70%
            'medium': [],    # 50-70%
            'low': []        # < 50%
        }
        
        for pred in predictions:
            conf = pred.get('confidence', 0)
            
            if conf >= 70:
                confidence_bins['high'].append(pred)
            elif conf >= 50:
                confidence_bins['medium'].append(pred)
            else:
                confidence_bins['low'].append(pred)
        
        analysis = {}
        for level, preds in confidence_bins.items():
            analysis[level] = {
                'count': len(preds),
                'avg_confidence': np.mean([p.get('confidence', 0) for p in preds]) if preds else 0,
                'avg_ev': np.mean([p.get('expected_value', 0) for p in preds]) if preds else 0,
            }
        
        return analysis
    
    @staticmethod
    def calculate_betting_performance(predictions: List[Dict], results: List[Dict]) -> Dict:
        """计算投注性能"""
        
        if len(predictions) != len(results):
            logger.warning("Predictions and results length mismatch")
            return {}
        
        kelly_stakes = []
        returns = []
        wins = 0
        losses = 0
        
        for pred, result in zip(predictions, results):
            try:
                kelly_stake = pred.get('kelly_stake', 0) / 100
                actual_result = result.get('result')  # 'win', 'draw', 'loss'
                odds = pred.get('odds_win', 1)
                recommended = pred.get('recommended_bet', '')
                
                if recommended == 'BET_WIN' and actual_result == 'win':
                    profit = kelly_stake * (odds - 1)
                    wins += 1
                elif recommended == 'BET_WIN' and actual_result != 'win':
                    profit = -kelly_stake
                    losses += 1
                else:
                    profit = 0
                
                kelly_stakes.append(kelly_stake)
                returns.append(profit)
                
            except Exception as e:
                logger.warning(f"Error calculating return: {e}")
                continue
        
        total_staked = sum(kelly_stakes)
        total_profit = sum(returns)
        
        performance = {
            'total_bets': len([r for r in returns if r != 0]),
            'wins': wins,
            'losses': losses,
            'win_rate': wins / (wins + losses) if (wins + losses) > 0 else 0,
            'total_staked': total_staked,
            'total_profit': total_profit,
            'roi': total_profit / total_staked * 100 if total_staked > 0 else 0,
            'profit_per_bet': np.mean([r for r in returns if r != 0]) if any(r != 0 for r in returns) else 0,
        }
        
        return performance


class Backtester:
    """回测工具"""
    
    def __init__(self, initial_bankroll: float = 1000):
        self.initial_bankroll = initial_bankroll
        self.bankroll = initial_bankroll
        self.trades = []
    
    def backtest_kelly(self, predictions: List[Dict], results: List[Dict]) -> Dict:
        """
        Kelly准则回测
        
        Args:
            predictions: 预测列表
            results: 比赛结果列表
        
        Returns:
            回测统计
        """
        logger.info("Running Kelly backtest...")
        
        self.bankroll = self.initial_bankroll
        
        for pred, result in zip(predictions, results):
            try:
                kelly_stake = pred.get('kelly_stake', 0) / 100 * self.bankroll
                odds = pred.get('odds_win', 1)
                recommendation = pred.get('recommended_bet', '')
                actual_result = result.get('result', '')
                
                if recommendation != 'BET_WIN':
                    continue
                
                if kelly_stake > self.bankroll:
                    kelly_stake = self.bankroll
                
                # 比赛结果
                if actual_result == 'win':
                    profit = kelly_stake * (odds - 1)
                    self.bankroll += profit
                    status = 'WIN'
                else:
                    self.bankroll -= kelly_stake
                    status = 'LOSS'
                
                self.trades.append({
                    'match': f"{pred.get('home_team')} vs {pred.get('away_team')}",
                    'stake': kelly_stake,
                    'odds': odds,
                    'result': status,
                    'profit': profit if status == 'WIN' else -kelly_stake,
                    'bankroll': self.bankroll
                })
                
            except Exception as e:
                logger.warning(f"Error in backtest trade: {e}")
                continue
        
        stats = self._calculate_backtest_stats()
        logger.info(f"✅ Backtest completed")
        logger.info(f"   Initial: ${self.initial_bankroll:.2f}")
        logger.info(f"   Final: ${self.bankroll:.2f}")
        logger.info(f"   Return: {stats['total_return']:.2f}%")
        
        return stats
    
    def backtest_fixed_stake(self, predictions: List[Dict], results: List[Dict], stake: float = 10) -> Dict:
        """
        固定赌注回测
        
        Args:
            predictions: 预测列表
            results: 比赛结果列表
            stake: 固定赌注金额
        
        Returns:
            回测统计
        """
        logger.info("Running fixed stake backtest...")
        
        self.bankroll = self.initial_bankroll
        
        for pred, result in zip(predictions, results):
            try:
                odds = pred.get('odds_win', 1)
                recommendation = pred.get('recommended_bet', '')
                actual_result = result.get('result', '')
                
                if recommendation != 'BET_WIN':
                    continue
                
                if stake > self.bankroll:
                    continue
                
                if actual_result == 'win':
                    profit = stake * (odds - 1)
                    self.bankroll += profit
                    status = 'WIN'
                else:
                    self.bankroll -= stake
                    profit = -stake
                    status = 'LOSS'
                
                self.trades.append({
                    'match': f"{pred.get('home_team')} vs {pred.get('away_team')}",
                    'stake': stake,
                    'odds': odds,
                    'result': status,
                    'profit': profit,
                    'bankroll': self.bankroll
                })
                
            except Exception as e:
                logger.warning(f"Error in backtest trade: {e}")
                continue
        
        stats = self._calculate_backtest_stats()
        logger.info(f"✅ Backtest completed")
        logger.info(f"   Initial: ${self.initial_bankroll:.2f}")
        logger.info(f"   Final: ${self.bankroll:.2f}")
        logger.info(f"   Return: {stats['total_return']:.2f}%")
        
        return stats
    
    def _calculate_backtest_stats(self) -> Dict:
        """计算回测统计"""
        
        if not self.trades:
            return {
                'total_trades': 0,
                'wins': 0,
                'losses': 0,
                'win_rate': 0,
                'total_profit': 0,
                'total_return': 0,
                'max_drawdown': 0,
                'sharpe_ratio': 0,
            }
        
        df = pd.DataFrame(self.trades)
        
        wins = len(df[df['result'] == 'WIN'])
        losses = len(df[df['result'] == 'LOSS'])
        total_profit = sum([t['profit'] for t in self.trades])
        
        # 最大回撤
        cumulative = df['bankroll'].cumsum()
        running_max = cumulative.expanding().max()
        drawdown = (cumulative - running_max) / running_max
        max_drawdown = drawdown.min() if len(drawdown) > 0 else 0
        
        # Sharpe比率 (简化版)
        returns = [t['profit'] for t in self.trades]
        if len(returns) > 1 and np.std(returns) > 0:
            sharpe = np.mean(returns) / np.std(returns) * np.sqrt(252)
        else:
            sharpe = 0
        
        stats = {
            'total_trades': len(self.trades),
            'wins': wins,
            'losses': losses,
            'win_rate': wins / len(self.trades) if self.trades else 0,
            'total_profit': total_profit,
            'total_return': (self.bankroll - self.initial_bankroll) / self.initial_bankroll * 100,
            'avg_profit_per_trade': np.mean([t['profit'] for t in self.trades]),
            'max_drawdown': max_drawdown * 100,
            'sharpe_ratio': sharpe,
        }
        
        return stats
    
    def plot_equity_curve(self, output_path: str = "backtest_equity.png"):
        """绘制权益曲线"""
        
        if not self.trades:
            logger.warning("No trades to plot")
            return
        
        df = pd.DataFrame(self.trades)
        
        plt.figure(figsize=(12, 6))
        plt.plot(df.index, df['bankroll'], marker='o', linewidth=2, markersize=4)
        plt.axhline(y=self.initial_bankroll, color='r', linestyle='--', label='Initial')
        plt.xlabel('Trade Number')
        plt.ylabel('Bankroll ($)')
        plt.title('Equity Curve - Kelly Backtest')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(output_path, dpi=100)
        logger.info(f"Equity curve saved to {output_path}")


class FeatureImportanceAnalyzer:
    """特征重要性分析"""
    
    @staticmethod
    def analyze_xgboost_importance(model) -> Dict:
        """分析XGBoost特征重要性"""
        
        try:
            importance_df = pd.DataFrame({
                'feature': model.feature_names if hasattr(model, 'feature_names') else [],
                'importance': model.feature_importances_
            }).sort_values('importance', ascending=False)
            
            return importance_df.head(20).to_dict('records')
        except Exception as e:
            logger.error(f"Error analyzing feature importance: {e}")
            return {}


class ModelCalibration:
    """模型校准工具"""
    
    @staticmethod
    def calibrate_probabilities(y_true: np.ndarray, y_pred_proba: np.ndarray) -> float:
        """
        计算概率校准等级 (ECE - Expected Calibration Error)
        
        值越小越好，0表示完全校准
        """
        
        n_bins = 10
        bin_sums = np.zeros(n_bins)
        bin_true = np.zeros(n_bins)
        bin_total = np.zeros(n_bins)
        
        # 将预测分组到bins
        for i in range(len(y_pred_proba)):
            bin_idx = int(y_pred_proba[i] * n_bins)
            if bin_idx == n_bins:
                bin_idx = n_bins - 1
            
            bin_sums[bin_idx] += y_pred_proba[i]
            bin_true[bin_idx] += y_true[i]
            bin_total[bin_idx] += 1
        
        # 计算ECE
        ece = 0
        for i in range(n_bins):
            if bin_total[i] > 0:
                bin_accuracy = bin_true[i] / bin_total[i]
                bin_confidence = bin_sums[i] / bin_total[i]
                ece += np.abs(bin_accuracy - bin_confidence) * (bin_total[i] / len(y_true))
        
        return ece


class PerformanceReport:
    """生成性能报告"""
    
    @staticmethod
    def generate_report(
        predictions: List[Dict],
        results: List[Dict],
        output_path: str = "performance_report.json"
    ) -> Dict:
        """
        生成完整的性能报告
        """
        
        logger.info("Generating performance report...")
        
        # 模型评估
        evaluator = ModelEvaluator()
        y_pred = np.array([p.get('recommended_bet', 'BET_WIN') == 'BET_WIN' for p in predictions], dtype=int)
        y_true = np.array([r.get('result', '') == 'win' for r in results], dtype=int)
        
        metrics = evaluator.evaluate_predictions(y_true, y_pred)
        
        # 投注性能
        betting_perf = evaluator.calculate_betting_performance(predictions, results)
        
        # 置信度分析
        confidence_analysis = evaluator.get_performance_by_confidence(predictions)
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'total_predictions': len(predictions),
            'model_metrics': metrics,
            'betting_performance': betting_perf,
            'confidence_analysis': confidence_analysis,
            'top_predictions': sorted(predictions, key=lambda x: x.get('expected_value', 0), reverse=True)[:10],
        }
        
        # 保存报告
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        
        logger.info(f"Report saved to {output_path}")
        
        return report


# 示例使用
if __name__ == "__main__":
    # 示例数据
    y_true = np.array([1, 1, 0, 1, 0])
    y_pred = np.array([1, 1, 1, 0, 0])
    y_pred_proba = np.array([
        [0.1, 0.7, 0.2],
        [0.05, 0.8, 0.15],
        [0.6, 0.2, 0.2],
        [0.4, 0.3, 0.3],
        [0.7, 0.2, 0.1]
    ])
    
    evaluator = ModelEvaluator()
    metrics = evaluator.evaluate_predictions(y_true, y_pred, y_pred_proba)
    print(f"Model metrics: {metrics}")
    
    # 回测示例
    predictions = [
        {'kelly_stake': 5, 'odds_win': 2.5, 'recommended_bet': 'BET_WIN', 'home_team': 'A', 'away_team': 'B'},
        {'kelly_stake': 3, 'odds_win': 1.8, 'recommended_bet': 'BET_WIN', 'home_team': 'C', 'away_team': 'D'},
    ]
    results = [
        {'result': 'win'},
        {'result': 'loss'},
    ]
    
    backtester = Backtester()
    stats = backtester.backtest_kelly(predictions, results)
    print(f"Backtest stats: {stats}")
