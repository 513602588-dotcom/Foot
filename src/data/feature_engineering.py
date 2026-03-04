"""
高级特征工程 - 创建强大的特征集
包括球队形态、球员伤疲、头对头历史等
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FeatureEngineer:
    """高级特征工程"""
    
    def __init__(self, lookback_days: int = 365):
        self.lookback_days = lookback_days
        self.team_stats = {}
        self.h2h_history = {}
    
    def extract_team_form_features(self, team: str, matches_df: pd.DataFrame, days: int = 10) -> Dict:
        """
        提取球队近期形态特征
        包括：胜率、进球、失球、xG等
        """
        try:
            recent_cutoff = datetime.now() - timedelta(days=days)
            
            # 球队最近比赛
            team_matches = matches_df[
                ((matches_df['home_team'] == team) | (matches_df['away_team'] == team)) &
                (matches_df['date'] >= recent_cutoff)
            ]
            
            if len(team_matches) == 0:
                return self._default_team_features()
            
            # 当主队时的统计
            home_matches = team_matches[team_matches['home_team'] == team]
            away_matches = team_matches[team_matches['away_team'] == team]
            
            features = {
                # 最近N场战绩
                'matches_played': len(team_matches),
                'wins': len(team_matches[team_matches['result'].isin(['W', 'H'])]),
                'draws': len(team_matches[team_matches['result'] == 'D']),
                'losses': len(team_matches[team_matches['result'].isin(['L', 'A'])]),
                
                # 进球失球
                'goals_for': team_matches['goals_for'].sum() if 'goals_for' in team_matches else 0,
                'goals_against': team_matches['goals_against'].sum() if 'goals_against' in team_matches else 0,
                'goal_diff': 0,
                
                # xG数据
                'xg_for': team_matches['xg_for'].sum() if 'xg_for' in team_matches else 0,
                'xg_against': team_matches['xg_against'].sum() if 'xg_against' in team_matches else 0,
                
                # 主客场分离
                'home_record': {
                    'wins': len(home_matches[home_matches['result'].isin(['W', 'H'])]),
                    'gf': home_matches['goals_for'].sum() if len(home_matches) > 0 else 0,
                    'ga': home_matches['goals_against'].sum() if len(home_matches) > 0 else 0,
                },
                'away_record': {
                    'wins': len(away_matches[away_matches['result'].isin(['W', 'A'])]),
                    'gf': away_matches['goals_for'].sum() if len(away_matches) > 0 else 0,
                    'ga': away_matches['goals_against'].sum() if len(away_matches) > 0 else 0,
                },
                
                # 连胜/连不胜
                'winning_streak': self._calculate_streak(team_matches, 'win'),
                'unbeaten_streak': self._calculate_streak(team_matches, 'unbeaten'),
                
                # 进攻防守指数
                'attack_strength': self._calculate_attack_strength(home_matches, away_matches),
                'defense_strength': self._calculate_defense_strength(home_matches, away_matches),
            }
            
            features['goal_diff'] = features['goals_for'] - features['goals_against']
            return features
            
        except Exception as e:
            logger.error(f"Error extracting team form features: {e}")
            return self._default_team_features()
    
    def extract_head_to_head_features(self, home_team: str, away_team: str, 
                                     matches_df: pd.DataFrame, limit: int = 10) -> Dict:
        """
        提取头对头历史特征
        """
        try:
            h2h_key = f"{home_team}_vs_{away_team}"
            
            # 两队之间的历史比赛
            h2h = matches_df[
                ((matches_df['home_team'] == home_team) & (matches_df['away_team'] == away_team)) |
                ((matches_df['home_team'] == away_team) & (matches_df['away_team'] == home_team))
            ].tail(limit)
            
            if len(h2h) == 0:
                return self._default_h2h_features()
            
            # 从主队视角
            home_h2h = h2h[h2h['home_team'] == home_team]
            
            features = {
                'h2h_matches': len(h2h),
                'h2h_home_wins': len(home_h2h[home_h2h['result'].isin(['W', 'H'])]),
                'h2h_draws': len(h2h[h2h['result'] == 'D']),
                'h2h_away_wins': len(h2h[h2h['result'].isin(['L', 'A'])]),
                'h2h_avg_goals': h2h['total_goals'].mean() if 'total_goals' in h2h else 2.5,
                'h2h_over_25_ratio': (h2h['total_goals'] > 2.5).sum() / len(h2h) if 'total_goals' in h2h else 0,
            }
            
            features['h2h_home_win_rate'] = features['h2h_home_wins'] / features['h2h_matches'] if features['h2h_matches'] > 0 else 0
            
            return features
            
        except Exception as e:
            logger.error(f"Error extracting H2H features: {e}")
            return self._default_h2h_features()
    
    def extract_injury_fatigue_features(self, team: str, days_since_last: int = 3) -> Dict:
        """
        提取伤疲特征
        在无实时API的情况下，使用启发式规则
        """
        features = {
            'injury_severity': 0,  # 0-10, 10最严重不推荐
            'fatigue_level': days_since_last / 7,  # 正常化周度疲劳
            'midweek_fixture': 1 if days_since_last < 3 else 0,
            'travel_fatigue': 0,  # 需要判断地理位置
        }
        return features
    
    def build_match_features(self, match: Dict, historical_df: pd.DataFrame) -> pd.Series:
        """
        为单场比赛构建完整特征向量
        """
        try:
            home_team = match.get('home_team')
            away_team = match.get('away_team')
            
            # 1. 球队形态特征
            home_form = self.extract_team_form_features(home_team, historical_df, days=30)
            away_form = self.extract_team_form_features(away_team, historical_df, days=30)
            
            # 2. 头对头特征
            h2h_features = self.extract_head_to_head_features(home_team, away_team, historical_df)
            
            # 3. 伤疲特征
            home_injury = self.extract_injury_fatigue_features(home_team)
            away_injury = self.extract_injury_fatigue_features(away_team)
            
            # 4. 综合特征
            features = pd.Series({
                # 基础信息
                'home_team': home_team,
                'away_team': away_team,
                'match_date': match.get('date'),
                
                # 主队特征
                'h_matches_played': home_form.get('matches_played', 0),
                'h_win_rate': home_form.get('wins', 0) / max(home_form.get('matches_played', 1), 1),
                'h_draw_rate': home_form.get('draws', 0) / max(home_form.get('matches_played', 1), 1),
                'h_goals_per_match': home_form.get('goals_for', 0) / max(home_form.get('matches_played', 1), 1),
                'h_goals_against_per_match': home_form.get('goals_against', 0) / max(home_form.get('matches_played', 1), 1),
                'h_xg_per_match': home_form.get('xg_for', 0) / max(home_form.get('matches_played', 1), 1),
                'h_attack_strength': home_form.get('attack_strength', 1),
                'h_defense_strength': home_form.get('defense_strength', 1),
                'h_winning_streak': home_form.get('winning_streak', 0),
                'h_unbeaten_streak': home_form.get('unbeaten_streak', 0),
                'h_injury_severity': home_injury.get('injury_severity', 0),
                'h_fatigue_level': home_injury.get('fatigue_level', 0),
                'h_midweek_fixture': home_injury.get('midweek_fixture', 0),
                
                # 客队特征
                'a_matches_played': away_form.get('matches_played', 0),
                'a_win_rate': away_form.get('wins', 0) / max(away_form.get('matches_played', 1), 1),
                'a_draw_rate': away_form.get('draws', 0) / max(away_form.get('matches_played', 1), 1),
                'a_goals_per_match': away_form.get('goals_for', 0) / max(away_form.get('matches_played', 1), 1),
                'a_goals_against_per_match': away_form.get('goals_against', 0) / max(away_form.get('matches_played', 1), 1),
                'a_xg_per_match': away_form.get('xg_for', 0) / max(away_form.get('matches_played', 1), 1),
                'a_attack_strength': away_form.get('attack_strength', 1),
                'a_defense_strength': away_form.get('defense_strength', 1),
                'a_winning_streak': away_form.get('winning_streak', 0),
                'a_unbeaten_streak': away_form.get('unbeaten_streak', 0),
                'a_injury_severity': away_injury.get('injury_severity', 0),
                'a_fatigue_level': away_injury.get('fatigue_level', 0),
                'a_midweek_fixture': away_injury.get('midweek_fixture', 0),
                
                # 相对特征（主-客）
                'rel_form': home_form.get('wins', 0) - away_form.get('wins', 0),
                'rel_goal_diff': home_form.get('goal_diff', 0) - away_form.get('goal_diff', 0),
                'rel_attack': home_form.get('attack_strength', 1) / max(away_form.get('attack_strength', 1), 0.1),
                'rel_defense': home_form.get('defense_strength', 1) / max(away_form.get('defense_strength', 1), 0.1),
                
                # 头对头特征
                'h2h_home_win_rate': h2h_features.get('h2h_home_win_rate', 0.5),
                'h2h_avg_goals': h2h_features.get('h2h_avg_goals', 2.5),
                'h2h_over_25': h2h_features.get('h2h_over_25_ratio', 0.5),
            })
            
            return features
            
        except Exception as e:
            logger.error(f"Error building match features: {e}")
            return pd.Series()
    
    def _calculate_streak(self, matches_df: pd.DataFrame, streak_type: str) -> int:
        """计算连胜/连不胜"""
        if len(matches_df) == 0:
            return 0
        
        streak = 0
        for _, match in matches_df.iloc[::-1].iterrows():
            if streak_type == 'win':
                if match['result'] in ['W', 'H']:
                    streak += 1
                else:
                    break
            elif streak_type == 'unbeaten':
                if match['result'] in ['W', 'H', 'D']:
                    streak += 1
                else:
                    break
        return streak
    
    def _calculate_attack_strength(self, home_matches, away_matches) -> float:
        """计算进攻能力指数"""
        total_matches = len(home_matches) + len(away_matches)
        if total_matches == 0:
            return 1.0
        
        total_goals = 0
        total_goals += home_matches['goals_for'].sum() if len(home_matches) > 0 else 0
        total_goals += away_matches['goals_for'].sum() if len(away_matches) > 0 else 0
        
        avg_goals = total_goals / total_matches
        return avg_goals / 1.4  # 标准化到平均每队每场1.4球
    
    def _calculate_defense_strength(self, home_matches, away_matches) -> float:
        """计算防线能力指数"""
        total_matches = len(home_matches) + len(away_matches)
        if total_matches == 0:
            return 1.0
        
        total_ga = 0
        total_ga += home_matches['goals_against'].sum() if len(home_matches) > 0 else 0
        total_ga += away_matches['goals_against'].sum() if len(away_matches) > 0 else 0
        
        avg_ga = total_ga / total_matches
        return 1.4 / avg_ga  # 倒数，失球越少值越高
    
    def _default_team_features(self) -> Dict:
        """返回默认的球队特征"""
        return {
            'matches_played': 0,
            'wins': 0,
            'draws': 0,
            'losses': 0,
            'goals_for': 0,
            'goals_against': 0,
            'goal_diff': 0,
            'xg_for': 0,
            'xg_against': 0,
            'home_record': {'wins': 0, 'gf': 0, 'ga': 0},
            'away_record': {'wins': 0, 'gf': 0, 'ga': 0},
            'winning_streak': 0,
            'unbeaten_streak': 0,
            'attack_strength': 1.0,
            'defense_strength': 1.0,
        }
    
    def _default_h2h_features(self) -> Dict:
        """返回默认的H2H特征"""
        return {
            'h2h_matches': 0,
            'h2h_home_wins': 0,
            'h2h_draws': 0,
            'h2h_away_wins': 0,
            'h2h_avg_goals': 2.5,
            'h2h_over_25_ratio': 0.5,
            'h2h_home_win_rate': 0.5,
        }
