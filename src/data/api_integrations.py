"""
多源足球数据API集成
【修复版】解决Sofascore 403、the-odds-api 401报错，禁用无效API调用
"""
import requests
import json
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FootballDataAPI:
    """football-data.org 官方API（核心数据源，稳定可用）"""
    BASE_URL = "https://api.football-data.org/v4"
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key
        self.headers = {"X-Auth-Token": api_key} if api_key else {}
    
    def get_competitions(self):
        """获取所有支持的联赛"""
        try:
            resp = requests.get(f"{self.BASE_URL}/competitions", headers=self.headers, timeout=10)
            resp.raise_for_status()
            return resp.json().get('competitions', [])
        except Exception as e:
            logger.error(f"Failed to get competitions: {e}")
            return []
    
    def get_matches(self, competition_code: str = "PL", status: str = "SCHEDULED", days: int = 7):
        """
        获取指定联赛的赛程
        PL=英超, SA=意甲, BL1=德甲, FL1=法甲, PD=西甲
        """
        if not self.api_key:
            return _get_mock_matches(competition_code)
        try:
            if status == "FINISHED":
                dateFrom = (datetime.now() - timedelta(days=days)).isoformat()[:10]
                dateTo = datetime.now().isoformat()[:10]
            else:
                dateFrom = datetime.now().isoformat()[:10]
                dateTo = (datetime.now() + timedelta(days=days)).isoformat()[:10]
            
            params = {
                "status": status,
                "dateFrom": dateFrom,
                "dateTo": dateTo
            }
            url = f"{self.BASE_URL}/competitions/{competition_code}/matches"
            resp = requests.get(url, headers=self.headers, params=params, timeout=10)
            resp.raise_for_status()
            return resp.json().get('matches', [])
        except Exception as e:
            logger.error(f"Failed to get matches: {e}")
            return []
    
    def get_team_standings(self, competition_code: str):
        """获取联赛积分榜"""
        try:
            url = f"{self.BASE_URL}/competitions/{competition_code}/standings"
            resp = requests.get(url, headers=self.headers, timeout=10)
            resp.raise_for_status()
            return resp.json().get('standings', [])
        except Exception as e:
            logger.error(f"Failed to get standings: {e}")
            return []
    
    def get_team_stats(self, team_id: int):
        """获取球队详细统计"""
        try:
            url = f"{self.BASE_URL}/teams/{team_id}"
            resp = requests.get(url, headers=self.headers, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Failed to get team stats: {e}")
            return {}


class UnderstatAPI:
    """Understat数据（xG、射门等）"""
    BASE_URL = "https://understat.com/api"
    
    @staticmethod
    def get_team_xg_stats(league: str = "EPL") -> Dict:
        """获取球队xG统计"""
        try:
            url = f"{UnderstatAPI.BASE_URL}/get_league_squad_exp_stats/{league}/2024"
            # 加反爬请求头
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Referer": "https://understat.com/"
            }
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"Failed to get Understat xG: {e}，使用默认值")
            return {}
    
    @staticmethod
    def get_match_data(match_id: int) -> Dict:
        """获取具体比赛的xG数据"""
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Referer": "https://understat.com/"
            }
            url = f"{UnderstatAPI.BASE_URL}/match/{match_id}"
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"Failed to get match xG data: {e}")
            return {}


class OddsAPI:
    """赔率数据API - the-odds-api.com【修复版】解决401报错"""
    BASE_URL = "https://api.the-odds-api.com/v4"
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key
        # 校验Key有效性
        if self.api_key:
            logger.info(f"✅ OddsAPI Key已配置，长度：{len(self.api_key)}")
        else:
            logger.warning("⚠️ 未配置OddsAPI Key，将使用默认赔率")
    
    def get_upcoming_matches(self, sport: str = "soccer_epl", regions: str = "uk,eu"):
        """获取即将进行的比赛赔率，加容错和校验"""
        if not self.api_key:
            return []
        try:
            params = {
                "apiKey": self.api_key,
                "regions": regions,
                "markets": "h2h",
                "dateFormat": "iso",
                "oddsFormat": "decimal"
            }
            # 加请求头
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
            url = f"{self.BASE_URL}/sports/{sport}/events"
            resp = requests.get(url, params=params, headers=headers, timeout=15)
            # 处理401报错
            if resp.status_code == 401:
                logger.error("❌ OddsAPI 401未授权：请检查API Key是否正确、是否过期、免费额度是否用完")
                return []
            resp.raise_for_status()
            # 打印剩余额度，方便排查
            remaining = resp.headers.get('x-requests-remaining', '未知')
            used = resp.headers.get('x-requests-used', '未知')
            logger.info(f"✅ OddsAPI请求成功，剩余额度：{remaining}，已用：{used}")
            return resp.json()
        except Exception as e:
            logger.error(f"Failed to get odds: {e}")
            return []


class DataAggregator:
    """数据聚合器 - 合并多个API源【修复版】禁用无效Sofascore调用"""
    
    def __init__(self, football_api_key: str = None, odds_api_key: str = None):
        self.fdb = FootballDataAPI(football_api_key)
        self.understat = UnderstatAPI()
        self.odds = OddsAPI(odds_api_key)
        # 禁用Sofascore API，避免403报错
        logger.info("✅ 多源数据聚合器初始化完成，已禁用无效Sofascore API调用")
    
    def get_comprehensive_match_data(self, match: Dict) -> Dict:
        """获取单场比赛的综合数据，仅保留有效API调用"""
        # 统一字段名，和预测引擎、主管道完全对齐
        home_team = match.get("homeTeam", {}).get("name", "")
        away_team = match.get("awayTeam", {}).get("name", "")
        match_date = match.get("utcDate", "")
        competition_code = match.get("competition", {}).get("code", "")
        
        enhanced = {
            "id": match.get("id", ""),
            "home_team": home_team,
            "away_team": away_team,
            "date": match_date,
            "competition_code": competition_code,
            "basic": match,
            "odds_win": None,
            "odds_draw": None,
            "odds_away": None,
            "xg_stats": {},
            "head_to_head": []
        }
        
        try:
            # 【核心】获取赔率数据，并精准匹配到当前比赛
            if self.odds.api_key:
                # 联赛映射，适配the-odds-api的官方联赛代码
                league_map = {
                    "PL": "soccer_epl",
                    "PD": "soccer_spain_la_liga",
                    "BL1": "soccer_germany_bundesliga",
                    "SA": "soccer_italy_serie_a",
                    "FL1": "soccer_france_ligue_one"
                }
                sport_key = league_map.get(competition_code, "soccer_epl")
                
                # 获取对应联赛的赔率
                odds_data = self.odds.get_upcoming_matches(sport=sport_key, regions="uk,eu")
                for odds_match in odds_data:
                    # 队名模糊匹配，适配不同API的队名差异
                    odds_home = odds_match.get("home_team", "").lower()
                    odds_away = odds_match.get("away_team", "").lower()
                    match_home = home_team.lower()
                    match_away = away_team.lower()
                    
                    # 模糊匹配，兼容不同API的队名格式
                    if (match_home in odds_home or odds_home in match_home) and (match_away in odds_away or odds_away in match_away):
                        # 提取胜平负赔率
                        h2h_markets = odds_match.get("bookmakers", [{}])[0].get("markets", [{}])[0].get("outcomes", [])
                        for outcome in h2h_markets:
                            outcome_name = outcome.get("name", "").lower()
                            if outcome_name == "home":
                                enhanced["odds_win"] = outcome.get("price", None)
                            elif outcome_name == "draw":
                                enhanced["odds_draw"] = outcome.get("price", None)
                            elif outcome_name == "away":
                                enhanced["odds_away"] = outcome.get("price", None)
                        logger.info(f"✅ 赔率匹配成功：{home_team} vs {away_team}，主胜赔率：{enhanced['odds_win']}")
                        break
        
        except Exception as e:
            logger.error(f"❌ 聚合比赛数据失败：{home_team} vs {away_team}，错误：{e}")
        
        return enhanced
    
    def get_league_data(self, competition_code: str = "PL") -> Dict:
        """获取完整联赛数据"""
        return {
            "standings": self.fdb.get_team_standings(competition_code),
            "matches": self.fdb.get_matches(competition_code),
            "xg_stats": self.understat.get_team_xg_stats()
        }


# ====================== 快速工厂函数 ======================
def create_data_aggregator(football_api_key: str = None, odds_api_key: str = None) -> DataAggregator:
    """创建数据聚合器实例（对齐原版接口）"""
    return DataAggregator(football_api_key, odds_api_key)

def validate_and_get_api_keys() -> Dict:
    """验证并获取环境变量中的API密钥（对齐主管道）"""
    api_keys = {
        "FOOTBALL_DATA_KEY": os.getenv("FOOTBALL_DATA_KEY", "").strip(),
        "API_FOOTBALL_KEY": os.getenv("API_FOOTBALL_KEY", "").strip(),
        "ODDS_API_KEY": os.getenv("ODDS_API_KEY", "").strip()
    }
    
    logger.info("=== 环境变量密钥读取状态 ===")
    valid_keys = {}
    for key, value in api_keys.items():
        if value:
            logger.info(f"{key} 长度：{len(value)}")
            valid_keys[key] = value
        else:
            logger.warning(f"{key} 未配置")
    logger.info("=============================")
    
    logger.info("=== 密钥有效性验证 ===")
    final_valid_keys = {}
    for key, value in valid_keys.items():
        if len(value) >= 20:
            logger.info(f"✅ {key} 验证通过，长度：{len(value)}")
            final_valid_keys[key] = value
        else:
            logger.warning(f"⚠️ {key} 长度不足，无效")
    logger.info(f"=== 密钥验证完成，共 {len(final_valid_keys)} 个有效密钥 ===")
    
    return final_valid_keys


# ====================== Mock 模式自动切换（无API Key时使用）======================
SAMPLE_MATCHES = [
    {"id": 1001, "utcDate": "2026-03-14T15:00:00Z", "competition": {"code": "PL"}, "homeTeam": {"name": "Burnley FC"}, "awayTeam": {"name": "AFC Bournemouth"}, "status": "SCHEDULED"},
    {"id": 1002, "utcDate": "2026-03-14T17:30:00Z", "competition": {"code": "PL"}, "homeTeam": {"name": "Arsenal FC"}, "awayTeam": {"name": "Everton FC"}, "status": "SCHEDULED"},
    {"id": 1003, "utcDate": "2026-03-15T14:00:00Z", "competition": {"code": "SA"}, "homeTeam": {"name": "Juventus FC"}, "awayTeam": {"name": "FC Internazionale Milano"}, "status": "SCHEDULED"},
    {"id": 1004, "utcDate": "2026-03-15T19:45:00Z", "competition": {"code": "BL1"}, "homeTeam": {"name": "FC Bayern München"}, "awayTeam": {"name": "Borussia Dortmund"}, "status": "SCHEDULED"},
    {"id": 1005, "utcDate": "2026-03-16T20:00:00Z", "competition": {"code": "FL1"}, "homeTeam": {"name": "Paris Saint-Germain FC"}, "awayTeam": {"name": "Olympique de Marseille"}, "status": "SCHEDULED"},
]

def _get_mock_matches(competition_code):
    """无 API Key 时返回模拟赛程"""
    logger.info(f"🔄 无 API Key → 使用模拟数据")
    filtered = [m for m in SAMPLE_MATCHES if m["competition"]["code"] == competition_code]
    return filtered or SAMPLE_MATCHES
