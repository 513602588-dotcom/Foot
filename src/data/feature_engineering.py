"""
高级特征工程 - 【修复版】100%对齐原版设计，解决时区不匹配、全默认特征问题
✅ 原版字段规范100%保留，和融合模型完全对齐
✅ 统一时区处理，彻底解决历史比赛筛选失败问题
✅ 原版容错逻辑完全保留，无任何设计偏离
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def convert_utc_date(date_str: str) -> datetime:
    """【修复】统一返回带UTC时区的datetime，彻底解决时区不匹配问题"""
    if not date_str or not isinstance(date_str, str):
        return datetime.now(timezone.utc) - timedelta(days=365)
    try:
        if date_str.endswith("Z"):
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(date_str)
        # 确保返回带UTC时区的datetime
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception as e:
        logger.warning(f"时间转换失败：{date_str}，错误：{str(e)}")
        return datetime.now(timezone.utc) - timedelta(days=365)


def parse_match_result(match: Dict, is_home: bool) -> Tuple[str, int, int]:
    """【原版函数100%保留】比赛结果解析"""
    default_result = 'D'
    default_gf = 0
    default_ga = 0

    try:
        score = match.get("score", {})
        full_time = score.get("fullTime", {})
        home_goals = full_time.get("home", 0) if full_time.get("home") is not None else 0
        away_goals = full_time.get("away", 0) if full_time.get("away") is not None else 0

        if is_home:
            gf = home_goals
            ga = away_goals
        else:
            gf = away_goals
            ga = home_goals

        if gf > ga:
            result = 'W'
        elif gf == ga:
            result = 'D'
        else:
            result = 'L'

        return result, gf, ga

    except Exception as e:
        logger.warning(f"比赛结果解析失败：{str(e)}")
        return default_result, default_gf, default_ga


class FeatureEngineer:
    """【原版核心类100%保留，仅修复时区bug】"""
    
    def __init__(self, lookback_days: int = 365):
        self.lookback_days = lookback_days
        self.team_stats = {}
        self.h2h_history = {}
    
    def extract_team_form_features(self, team_name: str, matches_df: pd.DataFrame, days: int = 30) -> Dict:
        """【修复核心】统一时区，确保能正确筛选球队历史比赛"""
        try:
            now_utc = datetime.now(timezone.utc)
            recent_cutoff = now_utc - timedelta(days=days)
            
            # 强制校验必填字段，不存在直接返回默认特征
            required_cols = ['home_team_name', 'away_team_name', 'match_date']
            if not all(col in matches_df.columns for col in required_cols):
                logger.warning(f"⚠️ 历史数据缺少必填字段，返回球队{team_name}的默认特征")
                return self._default_team_features()
            
            if matches_df.empty:
                return self._default_team_features()
            
            # 【修复】确保match_date是带UTC时区的datetime，和recent_cutoff时区统一
            if not pd.api.types.is_datetime64tz_dtype(matches_df['match_date']):
                matches_df['match_date'] = pd.to_datetime(matches_df['match_date'], utc=True)
            
            # 筛选球队最近比赛，原版逻辑完全保留
            team_matches = matches_df[
                ((matches_df['home_team_name'] == team_name) | (matches_df['away_team_name'] == team_name)) &
                (matches_df['match_date'] >= recent_cutoff)
            ].copy()
            
            if len(team_matches) == 0:
                logger.warning(f"⚠️ 球队{team_name}未找到最近{days}天的历史比赛，返回默认特征")
                return self._default_team_features()
            
            logger.info(f"✅ 球队{team_name}找到{len(team_matches)}场历史比赛，开始计算特征")
            
            # 拆分主客场比赛，原版逻辑完全保留
            home_matches = team_matches[team_matches['home_team_name'] == team_name].copy()
            away_matches = team_matches[team_matches['away_team_name'] == team_name].copy()
            home_matches_count = len(home_matches)
            away_matches_count = len(away_matches)
            
            # 计算球队视角的进球/失球/结果，原版逻辑完全保留
            team_matches['is_home'] = team_matches['home_team_name'] == team_name
            team_matches[['result', 'goals_for', 'goals_against']] = team_matches.apply(
                lambda x: parse_match_result(x, x['is_home']),
                axis=1, result_type='expand'
            )
            
            if home_matches_count > 0:
                home_matches[['result', 'goals_for', 'goals_against']] = home_matches.apply(
                    lambda x: parse_match_result(x, True),
                    axis=1, result_type='expand'
                )
            if away_matches_count > 0:
                away_matches[['result', 'goals_for', 'goals_against']] = away_matches.apply(
                    lambda x: parse_match_result(x, False),
                    axis=1, result_type='expand'
                )
            
            # 【原版特征字段100%保留，和融合模型完全对齐】
            features = {
                'matches_played': len(team_matches),
                'recent_wins': len(team_matches[team_matches['result'] == 'W']),
                'recent_draws': len(team_matches[team_matches['result'] == 'D']),
                'recent_losses': len(team_matches[team_matches['result'] == 'L']),
                'goals_for': team_matches['goals_for'].sum(),
                'goals_against': team_matches['goals_against'].sum(),
                'goal_diff': 0,
                'xg_for': team_matches['xg_for'].sum() if 'xg_for' in team_matches.columns else team_matches['goals_for'].sum(),
                'xg_against': team_matches['xg_against'].sum() if 'xg_against' in team_matches.columns else team_matches['goals_against'].sum(),
                'home_matches_count': home_matches_count,
                'home_wins': len(home_matches[home_matches['result'] == 'W']) if home_matches_count > 0 else 0,
                'home_gf': home_matches['goals_for'].sum() if home_matches_count > 0 else 0,
                'home_ga': home_matches['goals_against'].sum() if home_matches_count > 0 else 0,
                'away_matches_count': away_matches_count,
                'away_wins': len(away_matches[away_matches['result'] == 'W']) if away_matches_count > 0 else 0,
                'away_gf': away_matches['goals_for'].sum() if away_matches_count > 0 else 0,
                'away_ga': away_matches['goals_against'].sum() if away_matches_count > 0 else 0,
                'winning_streak': self._calculate_streak(team_matches, 'win'),
                'unbeaten_streak': self._calculate_streak(team_matches, 'unbeaten'),
                'attack_strength': self._calculate_attack_strength(home_matches, away_matches, home_matches_count, away_matches_count),
                'defense_strength': self._calculate_defense_strength(home_matches, away_matches, home_matches_count, away_matches_count),
                'shooting_accuracy': self._calculate_shooting_accuracy(team_matches),
            }
            
            features['goal_diff'] = features['goals_for'] - features['goals_against']
            return features
            
        except Exception as e:
            logger.error(f"提取球队{team_name}形态特征失败：{e}", exc_info=False)
            return self._default_team_features()
    
    def extract_head_to_head_features(self, home_team: str, away_team: str, 
                                     matches_df: pd.DataFrame, limit: int = 10) -> Dict:
        """【原版逻辑100%保留】提取历史交锋特征"""
        try:
            required_cols = ['home_team_name', 'away_team_name', 'match_date']
            if not all(col in matches_df.columns for col in required_cols):
                return self._default_h2h_features()
            
            if matches_df.empty:
                return self._default_h2h_features()
            
            # 【修复】统一时区
            if not pd.api.types.is_datetime64tz_dtype(matches_df['match_date']):
                matches_df['match_date'] = pd.to_datetime(matches_df['match_date'], utc=True)
            
            h2h_matches = matches_df[
                ((matches_df['home_team_name'] == home_team) & (matches_df['away_team_name'] == away_team)) |
                ((matches_df['home_team_name'] == away_team) & (matches_df['away_team_name'] == home_team))
            ].tail(limit).copy()
            
            if len(h2h_matches) == 0:
                return self._default_h2h_features()
            
            h2h_matches[['home_result', 'home_gf', 'home_ga']] = h2h_matches.apply(
                lambda x: parse_match_result(x, x['home_team_name'] == home_team),
                axis=1, result_type='expand'
            )
            h2h_matches['total_goals'] = h2h_matches['home_gf'] + h2h_matches['home_ga']
            
            features = {
                'h2h_matches': len(h2h_matches),
                'h2h_home_wins': len(h2h_matches[h2h_matches['home_result'] == 'W']),
                'h2h_draws': len(h2h_matches[h2h_matches['home_result'] == 'D']),
                'h2h_away_wins': len(h2h_matches[h2h_matches['home_result'] == 'L']),
                'h2h_avg_goals': h2h_matches['total_goals'].mean(),
                'h2h_over_25_ratio': (h2h_matches['total_goals'] > 2.5).sum() / len(h2h_matches),
            }
            
            features['h2h_home_win_rate'] = features['h2h_home_wins'] / features['h2h_matches']
            return features
            
        except Exception as e:
            logger.error(f"提取{home_team} vs {away_team} H2H特征失败：{e}", exc_info=False)
            return self._default_h2h_features()
    
    def extract_injury_fatigue_features(self, team: str, last_match_date: datetime = None) -> Dict:
        """【原版逻辑100%保留】提取伤疲特征"""
        try:
            now_utc = datetime.now(timezone.utc)
            if last_match_date is None:
                last_match_date = now_utc - timedelta(days=4)
            
            # 统一时区
            if last_match_date.tzinfo is None:
                last_match_date = last_match_date.replace(tzinfo=timezone.utc)
            
            days_since_last = (now_utc - last_match_date).days
            features = {
                'injury_severity': 0,
                'fatigue_level': min(days_since_last / 7, 1.5),
                'midweek_fixture': 1 if days_since_last < 4 else 0,
                'travel_fatigue': 0,
            }
            return features
        except Exception as e:
            logger.error(f"提取{team}伤疲特征失败：{e}", exc_info=False)
            return {
                'injury_severity': 0,
                'fatigue_level': 1.0,
                'midweek_fixture': 0,
                'travel_fatigue': 0,
            }
    
    def build_match_features(self, match: Dict, historical_df: pd.DataFrame) -> pd.Series:
        """【原版逻辑100%保留，仅修复队名提取bug】构建单场比赛特征"""
        try:
            # 强制提取主队/客队名称，彻底解决"未知主队"问题
            home_team_name = match.get("home_team", "")
            away_team_name = match.get("away_team", "")
            match_date = convert_utc_date(match.get("date", ""))
            match_id = match.get("match_id", 0)
            competition_code = match.get("competition_code", "")
            
            if not home_team_name or not away_team_name:
                logger.warning(f"⚠️ 比赛{match_id}缺少主队/客队名称，跳过")
                return pd.Series()
            
            # 提取特征，原版逻辑完全保留
            home_form = self.extract_team_form_features(home_team_name, historical_df, days=30)
            away_form = self.extract_team_form_features(away_team_name, historical_df, days=30)
            h2h_features = self.extract_head_to_head_features(home_team_name, away_team_name, historical_df)
            
            # 提取最近比赛时间，原版逻辑完全保留
            home_last_match = None
            away_last_match = None
            if not historical_df.empty and 'home_team_name' in historical_df.columns:
                home_matches = historical_df[
                    (historical_df['home_team_name'] == home_team_name) | 
                    (historical_df['away_team_name'] == home_team_name)
                ]
                if len(home_matches) > 0:
                    home_last_match = home_matches['match_date'].max()
                
                away_matches = historical_df[
                    (historical_df['home_team_name'] == away_team_name) | 
                    (historical_df['away_team_name'] == away_team_name)
                ]
                if len(away_matches) > 0:
                    away_last_match = away_matches['match_date'].max()
            
            home_injury = self.extract_injury_fatigue_features(home_team_name, home_last_match)
            away_injury = self.extract_injury_fatigue_features(away_team_name, away_last_match)
            
            # 【原版特征字段100%保留，和融合模型完全对齐，无任何修改】
            features = pd.Series({
                'match_id': match_id,
                'home_team': home_team_name,
                'away_team': away_team_name,
                'match_date': match_date,
                'competition_code': competition_code,
                'home_recent_wins': home_form.get('recent_wins', 0),
                'home_matches_played': home_form.get('matches_played', 0),
                'home_win_rate': home_form.get('recent_wins', 0) / max(home_form.get('matches_played', 1), 1),
                'home_draw_rate': home_form.get('recent_draws', 0) / max(home_form.get('matches_played', 1), 1),
                'home_loss_rate': home_form.get('recent_losses', 0) / max(home_form.get('matches_played', 1), 1),
                'home_goals_per_match': home_form.get('goals_for', 0) / max(home_form.get('matches_played', 1), 1),
                'home_goals_against_per_match': home_form.get('goals_against', 0) / max(home_form.get('matches_played', 1), 1),
                'home_goal_diff_per_match': home_form.get('goal_diff', 0) / max(home_form.get('matches_played', 1), 1),
                'home_xg_for_per_match': home_form.get('xg_for', 0) / max(home_form.get('matches_played', 1), 1),
                'home_xg_against_per_match': home_form.get('xg_against', 0) / max(home_form.get('matches_played', 1), 1),
                'home_shooting_accuracy': home_form.get('shooting_accuracy', 0.35),
                'home_attack_strength': home_form.get('attack_strength', 1.0),
                'home_defense_strength': home_form.get('defense_strength', 1.0),
                'home_winning_streak': home_form.get('winning_streak', 0),
                'home_unbeaten_streak': home_form.get('unbeaten_streak', 0),
                'home_home_win_rate': home_form.get('home_wins', 0) / max(home_form.get('home_matches_count', 1), 1),
                'home_injury_severity': home_injury.get('injury_severity', 0),
                'home_fatigue_level': home_injury.get('fatigue_level', 1.0),
                'home_midweek_fixture': home_injury.get('midweek_fixture', 0),
                'home_advantage': max(1.1, min(1.5, (home_form.get('home_wins', 0)/max(home_form.get('home_matches_count',1),1)) / max(home_form.get('away_wins',0)/max(home_form.get('away_matches_count',1),1), 0.5))),
                'away_recent_wins': away_form.get('recent_wins', 0),
                'away_matches_played': away_form.get('matches_played', 0),
                'away_win_rate': away_form.get('recent_wins', 0) / max(away_form.get('matches_played', 1), 1),
                'away_draw_rate': away_form.get('recent_draws', 0) / max(away_form.get('matches_played', 1), 1),
                'away_loss_rate': away_form.get('recent_losses', 0) / max(away_form.get('matches_played', 1), 1),
                'away_goals_per_match': away_form.get('goals_for', 0) / max(away_form.get('matches_played', 1), 1),
                'away_goals_against_per_match': away_form.get('goals_against', 0) / max(away_form.get('matches_played', 1), 1),
                'away_goal_diff_per_match': away_form.get('goal_diff', 0) / max(away_form.get('matches_played', 1), 1),
                'away_xg_for_per_match': away_form.get('xg_for', 0) / max(away_form.get('matches_played', 1), 1),
                'away_xg_against_per_match': away_form.get('xg_against', 0) / max(away_form.get('matches_played', 1), 1),
                'away_shooting_accuracy': away_form.get('shooting_accuracy', 0.35),
                'away_attack_strength': away_form.get('attack_strength', 1.0),
                'away_defense_strength': away_form.get('defense_strength', 1.0),
                'away_winning_streak': away_form.get('winning_streak', 0),
                'away_unbeaten_streak': away_form.get('unbeaten_streak', 0),
                'away_away_win_rate': away_form.get('away_wins', 0) / max(away_form.get('away_matches_count', 1), 1),
                'away_injury_severity': away_injury.get('injury_severity', 0),
                'away_fatigue_level': away_injury.get('fatigue_level', 1.0),
                'away_midweek_fixture': away_injury.get('midweek_fixture', 0),
                'rel_win_rate_diff': (home_form.get('recent_wins', 0) / max(home_form.get('matches_played', 1), 1)) - (away_form.get('recent_wins', 0) / max(away_form.get('matches_played', 1), 1)),
                'rel_goal_diff': home_form.get('goal_diff', 0) - away_form.get('goal_diff', 0),
                'rel_attack_strength': home_form.get('attack_strength', 1.0) / max(away_form.get('attack_strength', 1.0), 0.1),
                'rel_defense_strength': home_form.get('defense_strength', 1.0) / max(away_form.get('defense_strength', 1.0), 0.1),
                'rel_streak_diff': home_form.get('winning_streak', 0) - away_form.get('winning_streak', 0),
                'rel_fatigue_diff': home_injury.get('fatigue_level', 1.0) - away_injury.get('fatigue_level', 1.0),
                'h2h_home_win_rate': h2h_features.get('h2h_home_win_rate', 0.5),
                'h2h_draw_rate': h2h_features.get('h2h_draws', 0) / max(h2h_features.get('h2h_matches', 1), 1),
                'h2h_away_win_rate': h2h_features.get('h2h_away_wins', 0) / max(h2h_features.get('h2h_matches', 1), 1),
                'h2h_avg_goals': h2h_features.get('h2h_avg_goals', 2.5),
                'h2h_over_25_ratio': h2h_features.get('h2h_over_25_ratio', 0.5),
            })
            
            return features
            
        except Exception as e:
            logger.error(f"构建比赛{match.get('match_id', '未知')}特征失败：{e}", exc_info=False)
            return pd.Series()
    
    def _calculate_streak(self, matches_df: pd.DataFrame, streak_type: str) -> int:
        """【原版函数100%保留】计算连胜/连不胜"""
        if len(matches_df) == 0:
            return 0
        
        streak = 0
        for _, match in matches_df.sort_values('match_date', ascending=False).iterrows():
            if streak_type == 'win':
                if match['result'] == 'W':
                    streak += 1
                else:
                    break
            elif streak_type == 'unbeaten':
                if match['result'] in ['W', 'D']:
                    streak += 1
                else:
                    break
        return streak
    
    def _calculate_attack_strength(self, home_matches: pd.DataFrame, away_matches: pd.DataFrame, home_count: int, away_count: int) -> float:
        """【原版函数100%保留】计算进攻强度"""
        total_matches = home_count + away_count
        if total_matches == 0:
            return 1.0
        
        total_goals = 0
        total_goals += home_matches['goals_for'].sum() if home_count > 0 else 0
        total_goals += away_matches['goals_for'].sum() if away_count > 0 else 0
        
        avg_goals_per_match = total_goals / total_matches
        return avg_goals_per_match / 1.4
    
    def _calculate_defense_strength(self, home_matches: pd.DataFrame, away_matches: pd.DataFrame, home_count: int, away_count: int) -> float:
        """【原版函数100%保留】计算防守强度"""
        total_matches = home_count + away_count
        if total_matches == 0:
            return 1.0
        
        total_goals_against = 0
        total_goals_against += home_matches['goals_against'].sum() if home_count > 0 else 0
        total_goals_against += away_matches['goals_against'].sum() if away_count > 0 else 0
        
        avg_ga_per_match = total_goals_against / total_matches
        return 1.4 / max(avg_ga_per_match, 0.1)
    
    def _calculate_shooting_accuracy(self, team_matches: pd.DataFrame) -> float:
        """【原版函数100%保留】计算射门准确率"""
        if len(team_matches) == 0:
            return 0.35
        if 'shots' in team_matches.columns and 'goals_for' in team_matches.columns:
            total_shots = team_matches['shots'].sum()
            total_goals = team_matches['goals_for'].sum()
            if total_shots > 0:
                return min(0.6, max(0.1, total_goals / total_shots))
        return 0.35
    
    def _default_team_features(self) -> Dict:
        """【原版函数100%保留】默认球队特征"""
        return {
            'matches_played': 0,
            'recent_wins': 0,
            'recent_draws': 0,
            'recent_losses': 0,
            'goals_for': 0,
            'goals_against': 0,
            'goal_diff': 0,
            'xg_for': 0,
            'xg_against': 0,
            'home_matches_count': 0,
            'home_wins': 0,
            'home_gf': 0,
            'home_ga': 0,
            'away_matches_count': 0,
            'away_wins': 0,
            'away_gf': 0,
            'away_ga': 0,
            'winning_streak': 0,
            'unbeaten_streak': 0,
            'attack_strength': 1.0,
            'defense_strength': 1.0,
            'shooting_accuracy': 0.35,
        }
    
    def _default_h2h_features(self) -> Dict:
        """【原版函数100%保留】默认H2H特征"""
        return {
            'h2h_matches': 0,
            'h2h_home_wins': 0,
            'h2h_draws': 0,
            'h2h_away_wins': 0,
            'h2h_avg_goals': 2.5,
            'h2h_over_25_ratio': 0.5,
            'h2h_home_win_rate': 0.5,
        }


# ===================== 【原版入口函数100%保留】=====================
def build_features_dataset(matches: List[Dict], historical_matches: List[Dict] = None) -> pd.DataFrame:
    """主管道调用入口，100%兼容原版代码"""
    if historical_matches is None:
        historical_matches = []
    
    engineer = FeatureEngineer()
    feature_list = []
    success_count = 0
    fail_count = 0

    logger.info(f"开始为{len(matches)}场比赛构建高级特征")

    # 历史数据标准化
    historical_df = pd.DataFrame()
    if len(historical_matches) > 0:
        try:
            historical_df = pd.DataFrame(historical_matches)
            logger.info(f"原始历史数据形状：{historical_df.shape}")
            
            # 强制生成home_team_name
            if 'homeTeam' in historical_df.columns:
                historical_df['home_team_name'] = historical_df['homeTeam'].apply(
                    lambda x: x.get('name', '') if isinstance(x, dict) else str(x)
                )
            elif 'home_team' in historical_df.columns:
                historical_df['home_team_name'] = historical_df['home_team']
            else:
                historical_df['home_team_name'] = ''
            
            # 强制生成away_team_name
            if 'awayTeam' in historical_df.columns:
                historical_df['away_team_name'] = historical_df['awayTeam'].apply(
                    lambda x: x.get('name', '') if isinstance(x, dict) else str(x)
                )
            elif 'away_team' in historical_df.columns:
                historical_df['away_team_name'] = historical_df['away_team']
            else:
                historical_df['away_team_name'] = ''
            
            # 强制生成带UTC时区的match_date
            if 'utcDate' in historical_df.columns:
                historical_df['match_date'] = pd.to_datetime(historical_df['utcDate'], utc=True)
            elif 'date' in historical_df.columns:
                historical_df['match_date'] = pd.to_datetime(historical_df['date'], utc=True)
            else:
                historical_df['match_date'] = pd.NaT
            
            # 过滤无效数据
            historical_df = historical_df[
                (historical_df['home_team_name'] != '') & 
                (historical_df['away_team_name'] != '') &
                (~historical_df['match_date'].isna())
            ].reset_index(drop=True)
            
            logger.info(f"✅ 历史数据标准化完成，共{len(historical_df)}条有效记录")
            logger.info(f"历史数据字段：{list(historical_df.columns)}")
        
        except Exception as e:
            logger.error(f"历史数据标准化失败：{e}", exc_info=True)
            historical_df = pd.DataFrame()

    # 构建单场比赛特征
    for match in matches:
        match_dict = {
            "home_team": match.get("homeTeam", {}).get("name", ""),
            "away_team": match.get("awayTeam", {}).get("name", ""),
            "date": match.get("utcDate", ""),
            "match_id": match.get("id", ""),
            "competition_code": match.get("competition", {}).get("code", "")
        }
        match_features = engineer.build_match_features(match_dict, historical_df)
        if match_features is not None and not match_features.empty:
            feature_list.append(match_features)
            success_count += 1
        else:
            fail_count += 1

    logger.info(f"特征构建完成：成功{success_count}场，失败{fail_count}场")

    if len(feature_list) == 0:
        logger.warning("未提取到任何有效特征")
        return pd.DataFrame()

    features_df = pd.DataFrame(feature_list)
    logger.info(f"最终特征数据集形状：{features_df.shape}")
    logger.info(f"特征唯一值均值：{features_df.nunique().mean():.2f}")
    return features_df
