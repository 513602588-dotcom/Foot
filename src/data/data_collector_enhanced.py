"""
增强版足球数据采集器
补全FootballDataCollector类，解决导入错误，兼容主管道调用逻辑
"""
import sqlite3
import json
from datetime import datetime
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)


class FootballDataCollector:
    """足球赛事数据采集与缓存类，和主管道调用逻辑完全兼容"""
    
    def __init__(self, db_path: str = "data/football.db"):
        self.db_path = db_path
        self._init_database()
        logger.info(f"FootballDataCollector 初始化完成，数据库路径：{db_path}")
    
    def _init_database(self):
        """初始化数据库表结构"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 赛事表，兼容API返回的字段结构
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS matches (
                    match_id INTEGER PRIMARY KEY,
                    competition_code TEXT,
                    home_team_id INTEGER,
                    home_team_name TEXT,
                    away_team_id INTEGER,
                    away_team_name TEXT,
                    match_utc_date TEXT,
                    status TEXT,
                    matchday INTEGER,
                    home_goals INTEGER,
                    away_goals INTEGER,
                    raw_data TEXT,
                    created_time TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 球队表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS teams (
                    team_id INTEGER PRIMARY KEY,
                    team_name TEXT,
                    short_name TEXT,
                    tla TEXT,
                    competition_code TEXT,
                    created_time TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("数据库表结构初始化完成")
        
        except Exception as e:
            logger.error(f"数据库初始化失败：{str(e)}")
    
    def save_matches(self, matches: List[Dict], competition_code: str):
        """
        保存赛事数据到数据库，主管道直接调用的核心方法
        :param matches: API返回的赛事列表
        :param competition_code: 联赛编码
        """
        if len(matches) == 0:
            logger.warning(f"无{competition_code}联赛赛事数据，跳过保存")
            return
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            saved_count = 0
            for match in matches:
                try:
                    match_id = match.get("id", 0)
                    if match_id == 0:
                        continue
                    
                    home_team = match.get("homeTeam", {})
                    away_team = match.get("awayTeam", {})
                    score = match.get("score", {}).get("fullTime", {})
                    
                    # 插入/更新赛事数据
                    cursor.execute('''
                        INSERT OR REPLACE INTO matches 
                        (match_id, competition_code, home_team_id, home_team_name, away_team_id, away_team_name, match_utc_date, status, matchday, home_goals, away_goals, raw_data)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        match_id,
                        competition_code,
                        home_team.get("id", 0),
                        home_team.get("name", ""),
                        away_team.get("id", 0),
                        away_team.get("name", ""),
                        match.get("utcDate", ""),
                        match.get("status", ""),
                        match.get("matchday", 0),
                        score.get("home", 0) if score.get("home") is not None else 0,
                        score.get("away", 0) if score.get("away") is not None else 0,
                        json.dumps(match, ensure_ascii=False)
                    ))
                    saved_count += 1
                    
                    # 保存球队信息
                    self._save_team(cursor, home_team, competition_code)
                    self._save_team(cursor, away_team, competition_code)
                
                except Exception as e:
                    logger.warning(f"保存赛事{match.get('id', '未知')}失败：{str(e)}")
                    continue
            
            conn.commit()
            conn.close()
            logger.info(f"✅ {competition_code}联赛赛事保存完成，共保存{saved_count}条记录")
        
        except Exception as e:
            logger.error(f"保存{competition_code}联赛赛事失败：{str(e)}")
    
    def _save_team(self, cursor, team_data: Dict, competition_code: str):
        """保存球队信息，内部方法"""
        try:
            team_id = team_data.get("id", 0)
            if team_id == 0:
                return
            
            cursor.execute('''
                INSERT OR REPLACE INTO teams 
                (team_id, team_name, short_name, tla, competition_code)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                team_id,
                team_data.get("name", ""),
                team_data.get("shortName", ""),
                team_data.get("tla", ""),
                competition_code
            ))
        except Exception as e:
            logger.warning(f"保存球队{team_data.get('name', '未知')}失败：{str(e)}")
    
    def get_historical_matches(self, competition_code: str = None, limit: int = 100) -> List[Dict]:
        """获取历史赛事数据，供特征工程调用"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            if competition_code:
                cursor.execute('''
                    SELECT raw_data FROM matches 
                    WHERE competition_code = ? AND status = 'FINISHED'
                    ORDER BY match_utc_date DESC LIMIT ?
                ''', (competition_code, limit))
            else:
                cursor.execute('''
                    SELECT raw_data FROM matches 
                    WHERE status = 'FINISHED'
                    ORDER BY match_utc_date DESC LIMIT ?
                ''', (limit,))
            
            results = cursor.fetchall()
            conn.close()
            
            # 解析原始数据
            historical_matches = []
            for row in results:
                try:
                    match_data = json.loads(row[0])
                    historical_matches.append(match_data)
                except:
                    continue
            
            logger.info(f"获取到{len(historical_matches)}条历史赛事数据")
            return historical_matches
        
        except Exception as e:
            logger.error(f"获取历史赛事数据失败：{str(e)}")
            return []
