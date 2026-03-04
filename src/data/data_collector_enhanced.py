"""
增强的数据收集模块
集成多个API和数据源，并存储到本地数据库
"""

import requests
import pandas as pd
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging
import sqlite3
from pathlib import Path
import asyncio
import aiohttp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataCollector:
    """主数据收集器"""
    
    def __init__(self, db_path: str = "data/football.db"):
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self):
        """初始化SQLite数据库"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 比赛表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS matches (
                    match_id TEXT PRIMARY KEY,
                    date TEXT,
                    league TEXT,
                    home_team TEXT,
                    away_team TEXT,
                    home_goals INTEGER,
                    away_goals INTEGER,
                    home_xg REAL,
                    away_xg REAL,
                    status TEXT,
                    odds_win REAL,
                    odds_draw REAL,
                    odds_lose REAL,
                    created_at TEXT,
                    updated_at TEXT
                )
            ''')
            
            # 球队表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS teams (
                    team_id TEXT PRIMARY KEY,
                    team_name TEXT,
                    league TEXT,
                    country TEXT,
                    founded_year INTEGER,
                    elo_rating REAL,
                    last_updated TEXT
                )
            ''')
            
            # 球队统计表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS team_stats (
                    stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    team_id TEXT,
                    date TEXT,
                    matches_played INTEGER,
                    wins INTEGER,
                    draws INTEGER,
                    losses INTEGER,
                    goals_for INTEGER,
                    goals_against INTEGER,
                    xg_for REAL,
                    xg_against REAL,
                    points INTEGER,
                    FOREIGN KEY (team_id) REFERENCES teams(team_id)
                )
            ''')
            
            # 预测结果表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS predictions (
                    prediction_id TEXT PRIMARY KEY,
                    match_id TEXT,
                    model_name TEXT,
                    win_prob REAL,
                    draw_prob REAL,
                    loss_prob REAL,
                    confidence REAL,
                    recommendation TEXT,
                    expected_value REAL,
                    kelly_stake REAL,
                    created_at TEXT,
                    result TEXT,
                    FOREIGN KEY (match_id) REFERENCES matches(match_id)
                )
            ''')
            
            conn.commit()
            logger.info(f"Database initialized at {self.db_path}")
    
    def save_match(self, match_data: Dict):
        """保存或更新比赛数据"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                now = datetime.now().isoformat()
                
                cursor.execute('''
                    INSERT OR REPLACE INTO matches 
                    (match_id, date, league, home_team, away_team, home_goals, away_goals,
                     home_xg, away_xg, status, odds_win, odds_draw, odds_lose, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    match_data.get('id'),
                    match_data.get('date'),
                    match_data.get('league'),
                    match_data.get('home_team'),
                    match_data.get('away_team'),
                    match_data.get('home_goals'),
                    match_data.get('away_goals'),
                    match_data.get('home_xg'),
                    match_data.get('away_xg'),
                    match_data.get('status'),
                    match_data.get('odds_win'),
                    match_data.get('odds_draw'),
                    match_data.get('odds_lose'),
                    now,
                    now
                ))
                
                conn.commit()
                logger.info(f"Match saved: {match_data.get('home_team')} vs {match_data.get('away_team')}")
                
        except Exception as e:
            logger.error(f"Error saving match: {e}")
    
    def save_matches_batch(self, matches: List[Dict]):
        """批量保存比赛数据"""
        for match in matches:
            self.save_match(match)
        logger.info(f"Batch saved {len(matches)} matches")
    
    def save_prediction(self, prediction_data: Dict):
        """保存预测结果"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                now = datetime.now().isoformat()
                pred_id = f"{prediction_data.get('match_id')}_{prediction_data.get('model_name')}_{now}"
                
                cursor.execute('''
                    INSERT INTO predictions
                    (prediction_id, match_id, model_name, win_prob, draw_prob, loss_prob,
                     confidence, recommendation, expected_value, kelly_stake, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    pred_id,
                    prediction_data.get('match_id'),
                    prediction_data.get('model_name'),
                    prediction_data.get('win_prob'),
                    prediction_data.get('draw_prob'),
                    prediction_data.get('loss_prob'),
                    prediction_data.get('confidence'),
                    prediction_data.get('recommendation'),
                    prediction_data.get('expected_value'),
                    prediction_data.get('kelly_stake'),
                    now
                ))
                
                conn.commit()
                logger.info(f"Prediction saved for match {prediction_data.get('match_id')}")
                
        except Exception as e:
            logger.error(f"Error saving prediction: {e}")
    
    def get_matches(self, league: Optional[str] = None, days_ahead: int = 7) -> pd.DataFrame:
        """获取即将进行的比赛"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = "SELECT * FROM matches WHERE status = 'SCHEDULED'"
                params = []
                
                if league:
                    query += " AND league = ?"
                    params.append(league)
                
                query += " ORDER BY date ASC"
                
                df = pd.read_sql_query(query, conn, params=params)
                logger.info(f"Retrieved {len(df)} upcoming matches")
                return df
                
        except Exception as e:
            logger.error(f"Error retrieving matches: {e}")
            return pd.DataFrame()
    
    def get_team_history(self, team: str, limit: int = 20) -> pd.DataFrame:
        """获取球队最近比赛历史"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = '''
                    SELECT * FROM matches 
                    WHERE (home_team = ? OR away_team = ?) AND status = 'FINISHED'
                    ORDER BY date DESC LIMIT ?
                '''
                df = pd.read_sql_query(query, conn, params=(team, team, limit))
                logger.info(f"Retrieved {len(df)} historical matches for {team}")
                return df
                
        except Exception as e:
            logger.error(f"Error retrieving team history: {e}")
            return pd.DataFrame()
    
    def export_to_csv(self, query_type: str = "matches", output_path: str = "data/export.csv"):
        """导出数据为CSV"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                if query_type == "matches":
                    df = pd.read_sql_query("SELECT * FROM matches", conn)
                elif query_type == "predictions":
                    df = pd.read_sql_query("SELECT * FROM predictions", conn)
                else:
                    df = pd.read_sql_query("SELECT * FROM team_stats", conn)
                
                df.to_csv(output_path, index=False, encoding='utf-8')
                logger.info(f"Data exported to {output_path}")
                return output_path
                
        except Exception as e:
            logger.error(f"Error exporting data: {e}")
            return None


class WebScraper:
    """网页爬虫 - 用于获取额外数据"""
    
    @staticmethod
    async def fetch_json(url: str, headers: Dict = None, timeout: int = 10) -> Optional[Dict]:
        """异步获取JSON数据"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=timeout) as response:
                    if response.status == 200:
                        return await response.json()
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
        return None
    
    @staticmethod
    def fetch_json_sync(url: str, headers: Dict = None, timeout: int = 10) -> Optional[Dict]:
        """同步获取JSON数据"""
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
        return None


class HistoricalDataLoader:
    """历史数据加载器"""
    
    @staticmethod
    def load_from_csv(csv_path: str) -> pd.DataFrame:
        """从CSV加载历史数据"""
        try:
            df = pd.read_csv(csv_path)
            logger.info(f"Loaded {len(df)} records from {csv_path}")
            return df
        except Exception as e:
            logger.error(f"Error loading CSV: {e}")
            return pd.DataFrame()
    
    @staticmethod
    def load_from_json(json_path: str) -> List[Dict]:
        """从JSON加载历史数据"""
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.info(f"Loaded {len(data)} records from {json_path}")
            return data
        except Exception as e:
            logger.error(f"Error loading JSON: {e}")
            return []
    
    @staticmethod
    def create_dataframe_from_site_data(picks_path: str = "site/data/picks.json") -> pd.DataFrame:
        """从site/data目录创建DataFrame"""
        try:
            data = HistoricalDataLoader.load_from_json(picks_path)
            
            records = []
            for pick in data:
                records.append({
                    'date': pick.get('date'),
                    'home_team': pick.get('home'),
                    'away_team': pick.get('away'),
                    'odds_win': pick.get('odds', {}).get('H'),
                    'odds_draw': pick.get('odds', {}).get('D'),
                    'odds_lose': pick.get('odds', {}).get('A'),
                    'prob_win': pick.get('prob', {}).get('H'),
                    'prob_draw': pick.get('prob', {}).get('D'),
                    'prob_loss': pick.get('prob', {}).get('A'),
                    'ev_home': pick.get('ev_home'),
                    'kelly_home': pick.get('kelly_home'),
                })
            
            df = pd.DataFrame(records)
            logger.info(f"Created DataFrame with {len(df)} records")
            return df
            
        except Exception as e:
            logger.error(f"Error creating DataFrame: {e}")
            return pd.DataFrame()


class CacheManager:
    """缓存管理器 - 避免重复API调用"""
    
    def __init__(self, cache_dir: str = "data/cache", ttl_hours: int = 6):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.ttl_seconds = ttl_hours * 3600
    
    def get(self, key: str) -> Optional[Dict]:
        """从缓存获取数据"""
        cache_file = self.cache_dir / f"{key}.json"
        
        if cache_file.exists():
            # 检查是否过期
            mtime = cache_file.stat().st_mtime
            if datetime.now().timestamp() - mtime < self.ttl_seconds:
                try:
                    with open(cache_file, 'r') as f:
                        return json.load(f)
                except Exception as e:
                    logger.warning(f"Error reading cache: {e}")
        
        return None
    
    def set(self, key: str, data: Dict):
        """将数据保存到缓存"""
        try:
            cache_file = self.cache_dir / f"{key}.json"
            with open(cache_file, 'w') as f:
                json.dump(data, f)
            logger.info(f"Cached {key}")
        except Exception as e:
            logger.error(f"Error writing cache: {e}")
    
    def clear(self):
        """清空缓存"""
        try:
            for f in self.cache_dir.glob("*.json"):
                f.unlink()
            logger.info("Cache cleared")
        except Exception as e:
            logger.error(f"Error clearing cache: {e}")


# 示例用法
if __name__ == "__main__":
    # 创建收集器
    collector = DataCollector()
    
    # 示例：保存比赛数据
    sample_match = {
        'id': 'match_001',
        'date': '2024-03-15',
        'league': 'PL',
        'home_team': 'Manchester United',
        'away_team': 'Liverpool',
        'home_goals': None,
        'away_goals': None,
        'home_xg': 1.8,
        'away_xg': 1.5,
        'status': 'SCHEDULED',
        'odds_win': 2.50,
        'odds_draw': 3.20,
        'odds_lose': 2.80
    }
    collector.save_match(sample_match)
    
    # 加载历史数据
    df = HistoricalDataLoader.create_dataframe_from_site_data()
    print(f"DataFrame shape: {df.shape}")
    print(df.head())
