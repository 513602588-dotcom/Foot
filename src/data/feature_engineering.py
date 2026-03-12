"""
高级特征工程 - 字段名完全适配版
彻底解决字段名不匹配导致的KeyError，保留所有原有的高级特征逻辑
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def convert_utc_date(date_str: str) -> datetime:
    """统一转换API返回的UTC时间字符串，彻底解决类型不匹配报错"""
    if not date_str or not isinstance(date_str, str):
        return datetime.now(timezone.utc) - timedelta(days=365)
    try:
        if date_str.endswith("Z"):
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return datetime.fromisoformat(date_str)
    except Exception as e:
        logger.warning(f"时间转换失败：{date_str}，错误：{str(e)}")
        return datetime.now(timezone.utc) - timedelta(days=365)


def parse_match_result(match: Dict, is_home: bool) -> Tuple[str, int, int]:
    """从API返回的比分数据中，解析比赛结果、进球、失球，解决KeyError"""
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

        # 解析比赛结果
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
    """高级特征工程 - 字段名完全适配模型预测代码"""
    
    def __init__(self, lookback_days: int = 365):
        self.lookback_days = lookback_days
        self.team_stats = {}
        self.h2h_history = {}
    
    def extract_team_form_features(self, team_name: str, matches_df: pd.DataFrame, days: int = 30) -> Dict:
        """
        提取球队近期形态特征，字段名完全适配模型预测代码
        包括：胜率、进球、失球、xG、连胜/连不胜、攻防强度等
        """
        try:
            now_utc = datetime.now(timezone.utc)
            recent_cutoff = now_utc - timedelta(days=days)
            
            # 修复：历史数据为空时，直接返回默认特征，避免报错
            if matches_df.empty:
                return self._default_team_features()
            
            # 筛选球队最近比赛
            team_matches = matches_df[
                ((matches_df['home_team_name'] == team_name) | (matches_df['away_team_name'] == team_name)) &
                (matches_df['match_date'] >= recent_cutoff)
            ].copy()
            
            if len(team_matches) == 0:
                return self._default_team_features()
            
            # 拆分主客场比赛
            home_matches = team_matches[team_matches['home_team_name'] == team_name].copy()
            away_matches = team_matches[team_matches['away_team_name'] == team_name].copy()
            
            # 计算球队视角的进球/失球/结果
            team_matches['is_home'] = team_matches['home_team_name'] == team_name
            team_matches[['result', 'goals_for', 'goals_against']] = team_matches.apply(
                lambda x: parse_match_result(x, x['is_home']),
                axis=1, result_type='expand'
            )
            
            # 主客场数据补充
            if len(home_matches) > 0:
                home_matches[['result', 'goals_for', 'goals_against']] = home_matches.apply(
                    lambda x: parse_match_result(x, True),
                    axis=1, result_type='expand'
                )
            if len(away_matches) > 0:
                away_matches[['result', 'goals_for', 'goals_against']] = away_matches.apply(
                    lambda x: parse_match_result(x, False),
                    axis=1, result_type='expand'
                )
            
            # 核心特征计算，字段名和模型预测代码完全匹配
            features = {
                # 最近N场基础战绩
                'matches_played': len(team_matches),
                'recent_wins': len(team_matches[team_matches['result'] == 'W']),
                'recent_draws': len(team_matches[team_matches['result'] == 'D']),
                'recent_losses': len(team_matches[team_matches['result'] == 'L']),
                
                # 进球失球核心数据
                'goals_for': team_matches['goals_for'].sum(),
                'goals_against': team_matches['goals_against'].sum(),
                'goal_diff': 0,
                
                # xG数据（兼容API返回的xG字段，无数据则用实际进球替代）
                'xg_for': team_matches['xg_for'].sum() if 'xg_for' in team_matches.columns else team_matches['goals_for'].sum(),
                'xg_against': team_matches['xg_against'].sum() if 'xg_against' in team_matches.columns else team_matches['goals_against'].sum(),
                
                # 主客场分离战绩
                'home_record': {
                    'wins': len(home_matches[home_matches['result'] == 'W']) if len(home_matches) > 0 else 0,
                    'gf': home_matches['goals_for'].sum() if len(home_matches) > 0 else 0,
                    'ga': home_matches['goals_against'].sum() if len(home_matches) > 0 else 0,
                },
                'away_record': {
                    'wins': len(away_matches[away_matches['result'] == 'W']) if len(away_matches) > 0 else 0,
                    'gf': away_matches['goals_for'].sum() if len(away_matches) > 0 else 0,
                    'ga': away_matches['goals_against'].sum() if len(away_matches) > 0 else 0,
                },
                
                # 连胜/连不胜特征
                'winning_streak': self._calculate_streak(team_matches, 'win'),
                'unbeaten_streak': self._calculate_streak(team_matches, 'unbeaten'),
                
                # 进攻防守强度指数
                'attack_strength': self._calculate_attack_strength(home_matches, away_matches),
                'defense_strength': self._calculate_defense_strength(home_matches, away_matches),
            }
            
            features['goal_diff'] = features['goals_for'] - features['goals_against']
            return features
            
        except Exception as e:
            logger.error(f"提取球队{team_name}形态特征失败：{e}", exc_info=False)
            return self._default_team_features()
    
    def extract_head_to_head_features(self, home_team: str, away_team: str, 
                                     matches_df: pd.DataFrame, limit: int = 10) -> Dict:
        """
        提取两队历史交锋（H2H）特征
        """
        try:
            # 修复：历史数据为空时，直接返回默认特征
            if matches_df.empty:
                return self._default_h2h_features()
            
            # 筛选两队历史比赛
            h2h_matches = matches_df[
                ((matches_df['home_team_name'] == home_team) & (matches_df['away_team_name'] == away_team)) |
                ((matches_df['home_team_name'] == away_team) & (matches_df['away_team_name'] == home_team))
            ].tail(limit).copy()
            
            if len(h2h_matches) == 0:
                return self._default_h2h_features()
            
            # 从主队视角计算结果
            h2h_matches[['home_result', 'home_gf', 'home_ga']] = h2h_matches.apply(
                lambda x: parse_match_result(x, x['home_team_name'] == home_team),
                axis=1, result_type='expand'
            )
            h2h_matches['total_goals'] = h2h_matches['home_gf'] + h2h_matches['home_ga']
            
            # 核心特征计算
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
        """
        提取球队伤疲特征
        无实时伤停API时，使用比赛间隔计算疲劳度，启发式规则
        """
        try:
            now_utc = datetime.now(timezone.utc)
            if last_match_date is None:
                last_match_date = now_utc - timedelta(days=4)
            
            days_since_last = (now_utc - last_match_date).days
            features = {
                'injury_severity': 0,  # 0-10，数值越高伤停越严重，后续可接入伤停API修改
                'fatigue_level': min(days_since_last / 7, 1.5),  # 归一化周度疲劳，3天内比赛疲劳拉满
                'midweek_fixture': 1 if days_since_last < 4 else 0,
                'travel_fatigue': 0,  # 后续可接入主客场地理位置数据补充
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
        """
        为单场比赛构建完整特征向量
        字段名和模型预测代码完全匹配，彻底解决KeyError
        """
        try:
            # 适配API数据结构，提取基础信息
            home_team_data = match.get('homeTeam', {})
            away_team_data = match.get('awayTeam', {})
            home_team_name = home_team_data.get('name', home_team_data.get('shortName', '未知主队'))
            away_team_name = away_team_data.get('name', away_team_data.get('shortName', '未知客队'))
            match_date = convert_utc_date(match.get('utcDate', ''))
            
            # 1. 提取主队/客队近期形态特征
            home_form = self.extract_team_form_features(home_team_name, historical_df, days=30)
            away_form = self.extract_team_form_features(away_team_name, historical_df, days=30)
            
            # 2. 提取两队历史交锋特征
            h2h_features = self.extract_head_to_head_features(home_team_name, away_team_name, historical_df)
            
            # 3. 提取伤疲特征（取两队最近一场比赛时间计算）
            home_last_match = historical_df[
                (historical_df['home_team_name'] == home_team_name) | 
                (historical_df['away_team_name'] == home_team_name)
            ]['match_date'].max() if not historical_df.empty else None
            away_last_match = historical_df[
                (historical_df['home_team_name'] == away_team_name) | 
                (historical_df['away_team_name'] == away_team_name)
            ]['match_date'].max() if not historical_df.empty else None
            
            home_injury = self.extract_injury_fatigue_features(home_team_name, home_last_match)
            away_injury = self.extract_injury_fatigue_features(away_team_name, away_last_match)
            
            # 4. 合并所有特征，字段名和模型预测代码完全匹配
            features = pd.Series({
                # 基础信息
                'match_id': match.get('id', 0),
                'home_team': home_team_name,
                'away_team': away_team_name,
                'match_date': match_date,
                'competition_code': match.get('competition', {}).get('code', ''),
                
                # 主队核心特征（和模型代码里的字段名完全一致）
                'home_recent_wins': home_form.get('recent_wins', 0),
                'home_matches_played': home_form.get('matches_played', 0),
                'home_win_rate': home_form.get('recent_wins', 0) / max(home_form.get('matches_played', 1), 1),
                'home_draw_rate': home_form.get('recent_draws', 0) / max(home_form.get('matches_played', 1), 1),
                'home_loss_rate': home_form.get('recent_losses', 0) / max(home_form.get('matches_played', 1), 1),
                'home_goals_per_match': home_form.get('goals_for', 0) / max(home_form.get('matches_played', 1), 1),
                'home_goals_against_per_match': home_form.get('goals_against', 0) / max(home_form.get('matches_played', 1), 1),
                'home_goal_diff_per_match': home_form.get('goal_diff', 0) / max(home_form.get('matches_played', 1), 1),
                'home_xg_per_match': home_form.get('xg_for', 0) / max(home_form.get('matches_played', 1), 1),
                'home_xg_against_per_match': home_form.get('xg_against', 0) / max(home_form.get('matches_played', 1), 1),
                'home_attack_strength': home_form.get('attack_strength', 1.0),
                'home_defense_strength': home_form.get('defense_strength', 1.0),
                'home_winning_streak': home_form.get('winning_streak', 0),
                'home_unbeaten_streak': home_form.get('unbeaten_streak', 0),
                'home_home_win_rate': home_form.get('home_record', {}).get('wins', 0) / max(len(home_form.get('home_record', {})), 1),
                'home_injury_severity': home_injury.get('injury_severity', 0),
                'home_fatigue_level': home_injury.get('fatigue_level', 1.0),
                'home_midweek_fixture': home_injury.get('midweek_fixture', 0),
                
                # 客队核心特征（和模型代码里的字段名完全一致）
                'away_recent_wins': away_form.get('recent_wins', 0),
                'away_matches_played': away_form.get('matches_played', 0),
                'away_win_rate': away_form.get('recent_wins', 0) / max(away_form.get('matches_played', 1), 1),
                'away_draw_rate': away_form.get('recent_draws', 0) / max(away_form.get('matches_played', 1), 1),
                'away_loss_rate': away_form.get('recent_losses', 0) / max(away_form.get('matches_played', 1), 1),
                'away_goals_per_match': away_form.get('goals_for', 0) / max(away_form.get('matches_played', 1), 1),
                'away_goals_against_per_match': away_form.get('goals_against', 0) / max(away_form.get('matches_played', 1), 1),
                'away_goal_diff_per_match': away_form.get('goal_diff', 0) / max(away_form.get('matches_played', 1), 1),
                'away_xg_per_match': away_form.get('xg_for', 0) / max(away_form.get('matches_played', 1), 1),
                'away_xg_against_per_match': away_form.get('xg_against', 0) / max(away_form.get('matches_played', 1), 1),
                'away_attack_strength': away_form.get('attack_strength', 1.0),
                'away_defense_strength': away_form.get('defense_strength', 1.0),
                'away_winning_streak': away_form.get('winning_streak', 0),
                'away_unbeaten_streak': away_form.get('unbeaten_streak', 0),
                'away_away_win_rate': away_form.get('away_record', {}).get('wins', 0) / max(len(away_form.get('away_record', {})), 1),
                'away_injury_severity': away_injury.get('injury_severity', 0),
                'away_fatigue_level': away_injury.get('fatigue_level', 1.0),
                'away_midweek_fixture': away_injury.get('midweek_fixture', 0),
                
                # 两队相对特征（核心预测特征）
                'rel_win_rate_diff': (home_form.get('recent_wins', 0) / max(home_form.get('matches_played', 1), 1)) - (away_form.get('recent_wins', 0) / max(away_form.get('matches_played', 1), 1)),
                'rel_goal_diff': home_form.get('goal_diff', 0) - away_form.get('goal_diff', 0),
                'rel_attack_strength': home_form.get('attack_strength', 1.0) / max(away_form.get('attack_strength', 1.0), 0.1),
                'rel_defense_strength': home_form.get('defense_strength', 1.0) / max(away_form.get('defense_strength', 1.0), 0.1),
                'rel_streak_diff': home_form.get('winning_streak', 0) - away_form.get('winning_streak', 0),
                'rel_fatigue_diff': home_injury.get('fatigue_level', 1.0) - away_injury.get('fatigue_level', 1.0),
                
                # 历史交锋特征
                'h2h_home_win_rate': h2h_features.get('h2h_home_win_rate', 0.5),
                'h2h_draw_rate': h2h_features.get('h2h_draws', 0) / max(h2h_features.get('h2h_matches', 1), 1),
                'h2h_away_win_rate': h2h_features.get('h2h_away_wins', 0) / max(h2h_features.get('h2h_matches', 1), 1),
                'h2h_avg_goals': h2h_features.get('h2h_avg_goals', 2.5),
                'h2h_over_25_ratio': h2h_features.get('h2h_over_25_ratio', 0.5),
            })
            
            return features
            
        except Exception as e:
            logger.error(f"构建比赛{match.get('id', '未知')}特征失败：{e}", exc_info=False)
            return pd.Series()
    
    def _calculate_streak(self, matches_df: pd.DataFrame, streak_type: str) -> int:
        """计算连胜/连不胜，适配修改后的result字段"""
        if len(matches_df) == 0:
            return 0
        
        streak = 0
        # 按时间倒序，从最近一场开始算
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
    
    def _calculate_attack_strength(self, home_matches: pd.DataFrame, away_matches: pd.DataFrame) -> float:
        """计算进攻能力指数，数值越高进攻越强，标准化到联赛平均水平"""
        total_matches = len(home_matches) + len(away_matches)
        if total_matches == 0:
            return 1.0
        
        total_goals = 0
        total_goals += home_matches['goals_for'].sum() if len(home_matches) > 0 else 0
        total_goals += away_matches['goals_for'].sum() if len(away_matches) > 0 else 0
        
        avg_goals_per_match = total_goals / total_matches
        return avg_goals_per_match / 1.4  # 标准化到五大联赛平均每队每场1.4球
    
    def _calculate_defense_strength(self, home_matches: pd.DataFrame, away_matches: pd.DataFrame) -> float:
        """计算防线能力指数，数值越高防守越强，标准化到联赛平均水平"""
        total_matches = len(home_matches) + len(away_matches)
        if total_matches == 0:
            return 1.0
        
        total_goals_against = 0
        total_goals_against += home_matches['goals_against'].sum() if len(home_matches) > 0 else 0
        total_goals_against += away_matches['goals_against'].sum() if len(away_matches) > 0 else 0
        
        avg_ga_per_match = total_goals_against / total_matches
        # 倒数处理，失球越少，防守强度数值越高
        return 1.4 / max(avg_ga_per_match, 0.1)
    
    def _default_team_features(self) -> Dict:
        """默认球队特征，容错兜底"""
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
            'home_record': {'wins': 0, 'gf': 0, 'ga': 0},
            'away_record': {'wins': 0, 'gf': 0, 'ga': 0},
            'winning_streak': 0,
            'unbeaten_streak': 0,
            'attack_strength': 1.0,
            'defense_strength': 1.0,
        }
    
    def _default_h2h_features(self) -> Dict:
        """默认H2H特征，容错兜底"""
        return {
            'h2h_matches': 0,
            'h2h_home_wins': 0,
            'h2h_draws': 0,
            'h2h_away_wins': 0,
            'h2h_avg_goals': 2.5,
            'h2h_over_25_ratio': 0.5,
            'h2h_home_win_rate': 0.5,
        }


# ===================== 主管道兼容入口函数（无需修改主管道）=====================
def build_features_dataset(matches: List[Dict], historical_matches: List[Dict] = None) -> pd.DataFrame:
    """
    主管道调用入口，和之前的代码完全兼容
    输入：API返回的比赛列表、历史比赛列表
    输出：完整特征数据集DataFrame，直接喂给预测模型
    """
    if historical_matches is None:
        historical_matches = []
    
    # 初始化特征工程实例
    engineer = FeatureEngineer()
    feature_list = []
    success_count = 0
    fail_count = 0

    logger.info(f"开始为{len(matches)}场比赛构建高级特征")

    # 预处理历史比赛数据，转为DataFrame，适配API结构
    historical_df = pd.DataFrame()
    if len(historical_matches) > 0:
        try:
            historical_df = pd.DataFrame(historical_matches)
            # 统一历史数据字段名，和特征工程适配
            if 'homeTeam' in historical_df.columns:
                historical_df['home_team_name'] = historical_df['homeTeam'].apply(lambda x: x.get('name', '') if isinstance(x, dict) else '')
            if 'awayTeam' in historical_df.columns:
                historical_df['away_team_name'] = historical_df['awayTeam'].apply(lambda x: x.get('name', '') if isinstance(x, dict) else '')
            if 'utcDate' in historical_df.columns:
                historical_df['match_date'] = historical_df['utcDate'].apply(convert_utc_date)
            logger.info(f"历史数据预处理完成，共{len(historical_df)}条记录")
        except Exception as e:
            logger.warning(f"历史数据预处理失败：{e}，使用空历史数据")
            historical_df = pd.DataFrame()

    # 为每场比赛构建特征
    for match in matches:
        match_features = engineer.build_match_features(match, historical_df)
        # 修复Pandas真值模糊报错，使用正确的非空判断
        if match_features is not None and not match_features.empty:
            feature_list.append(match_features)
            success_count += 1
        else:
            fail_count += 1

    logger.info(f"特征构建完成：成功{success_count}场，失败{fail_count}场")

    # 无有效特征时返回空DataFrame，避免后续报错
    if len(feature_list) == 0:
        logger.warning("未提取到任何有效特征")
        return pd.DataFrame()

    # 转换为完整特征数据集
    features_df = pd.DataFrame(feature_list)
    logger.info(f"最终高级特征数据集形状：{features_df.shape}")
    return features_df
