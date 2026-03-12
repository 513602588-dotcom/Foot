"""
多源足球数据API集成
集成football-data.org, understat, the-odds-api, sofascore等数据源
修复：环境变量读取、API参数格式、接口地址、异常日志、模拟数据格式问题
"""
import requests
import json
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import logging
import os

# 配置日志，输出更详细的请求信息
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ====================== 核心修复1：全局环境变量读取 ======================
# 直接从系统环境变量读取工作流传入的密钥，无需手动传参
ENV_CONFIG = {
    "API_FOOTBALL_KEY": os.getenv("API_FOOTBALL_KEY", ""),
    "FOOTBALL_DATA_KEY": os.getenv("FOOTBALL_DATA_KEY", ""),
    "ODDS_API_KEY": os.getenv("ODDS_API_KEY", ""),
    "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY", ""),
    "OPENAI_BASE_URL": os.getenv("OPENAI_BASE_URL", ""),
    "OPENAI_MODEL": os.getenv("OPENAI_MODEL", "")
}

# 启动时打印密钥读取状态（仅输出长度，不泄露明文）
logger.info("=== 环境变量密钥读取状态 ===")
for key, value in ENV_CONFIG.items():
    if "KEY" in key or "SECRET" in key:
        logger.info(f"{key} 长度：{len(value)}")
logger.info("=============================")


class FootballDataAPI:
    """football-data.org 官方API（修复日期格式、日志、异常处理）"""
    BASE_URL = "https://api.football-data.org/v4"
    
    def __init__(self, api_key: str = None):
        # 优先使用传入的密钥，兜底用环境变量
        self.api_key = api_key if api_key else ENV_CONFIG["API_FOOTBALL_KEY"]
        self.headers = {"X-Auth-Token": self.api_key} if self.api_key else {}
        logger.info(f"FootballDataAPI 初始化完成，密钥状态：{'已配置' if self.api_key else '未配置'}")
    
    def get_competitions(self):
        """获取所有支持的联赛"""
        if not self.api_key:
            logger.warning("FootballDataAPI 无密钥，跳过请求")
            return []
        try:
            logger.info(f"请求联赛列表：{self.BASE_URL}/competitions")
            resp = requests.get(f"{self.BASE_URL}/competitions", headers=self.headers, timeout=10)
            resp.raise_for_status()
            logger.info(f"联赛列表请求成功，共 {len(resp.json().get('competitions', []))} 个联赛")
            return resp.json().get('competitions', [])
        except Exception as e:
            logger.error(f"获取联赛列表失败：HTTP状态码 {getattr(resp, 'status_code', '未知')}，错误信息：{e}")
            return []
    
    def get_matches(self, competition_code: str = "PL", status: str = "SCHEDULED", days: int = 7):
        """
        获取指定联赛的赛程
        PL=英超, SA=西甲, BL1=德甲, FR1=法甲, IT1=意甲
        修复：日期格式符合API要求（YYYY-MM-DD），区分无密钥/请求失败
        """
        # 无密钥直接返回模拟数据
        if not self.api_key:
            logger.info(f"🔄 FootballDataAPI 无API密钥→使用模拟数据（联赛：{competition_code}）")
            return _get_mock_matches(competition_code)
        
        try:
            # 修复：日期格式改为API要求的YYYY-MM-DD，避免时区和格式错误
            date_from = datetime.now().strftime("%Y-%m-%d")
            date_to = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
            
            params = {
                "status": status,
                "dateFrom": date_from,
                "dateTo": date_to
            }
            url = f"{self.BASE_URL}/competitions/{competition_code}/matches"
            
            logger.info(f"请求{competition_code}赛程：{url}，日期范围：{date_from} ~ {date_to}")
            resp = requests.get(url, headers=self.headers, params=params, timeout=10)
            resp.raise_for_status()
            
            matches = resp.json().get('matches', [])
            logger.info(f"{competition_code}赛程请求成功，共 {len(matches)} 场比赛")
            return matches
        
        except Exception as e:
            # 区分不同错误类型，输出详细日志
            status_code = getattr(resp, 'status_code', '未知')
            if status_code == 401:
                logger.error(f"获取{competition_code}赛程失败：密钥无效/过期（401）")
            elif status_code == 429:
                logger.error(f"获取{competition_code}赛程失败：API请求额度耗尽（429）")
            elif status_code == 403:
                logger.error(f"获取{competition_code}赛程失败：IP被封禁/无权限（403）")
            else:
                logger.error(f"获取{competition_code}赛程失败：HTTP状态码 {status_code}，错误信息：{e}")
            # 请求失败返回空列表，不返回模拟数据，避免误导
            return []
    
    def get_team_standings(self, competition_code: str):
        """获取联赛积分榜"""
        if not self.api_key:
            logger.warning("FootballDataAPI 无密钥，跳过积分榜请求")
            return []
        try:
            url = f"{self.BASE_URL}/competitions/{competition_code}/standings"
            logger.info(f"请求{competition_code}积分榜：{url}")
            resp = requests.get(url, headers=self.headers, timeout=10)
            resp.raise_for_status()
            standings = resp.json().get('standings', [])
            logger.info(f"{competition_code}积分榜请求成功")
            return standings
        except Exception as e:
            logger.error(f"获取{competition_code}积分榜失败：{e}")
            return []
    
    def get_team_stats(self, team_id: int):
        """获取球队详细统计"""
        if not self.api_key:
            logger.warning("FootballDataAPI 无密钥，跳过球队统计请求")
            return {}
        try:
            url = f"{self.BASE_URL}/teams/{team_id}"
            resp = requests.get(url, headers=self.headers, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"获取球队{team_id}统计失败：{e}")
            return {}


class UnderstatAPI:
    """Understat数据（xG、射门等）【修复接口地址，适配公开API】"""
    BASE_URL = "https://understat.com"
    
    @staticmethod
    def get_team_xg_stats(league: str = "EPL", season: str = "2024") -> Dict:
        """获取球队xG统计，league可选：EPL/LaLiga/Bundesliga/SerieA/Ligue1"""
        try:
            # 修复：understat正确的公开接口地址
            url = f"{UnderstatAPI.BASE_URL}/main/leagueData"
            params = {"league": league, "season": season}
            logger.info(f"请求Understat {league} {season} xG数据：{url}")
            
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            logger.info(f"Understat xG数据请求成功，共 {len(data)} 支球队数据")
            return data
        except Exception as e:
            logger.error(f"获取Understat xG数据失败：{e}")
            return {}
    
    @staticmethod
    def get_match_data(match_id: int) -> Dict:
        """获取具体比赛的xG数据"""
        try:
            url = f"{UnderstatAPI.BASE_URL}/match/{match_id}"
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"获取比赛{match_id} xG数据失败：{e}")
            return {}


class OddsAPI:
    """赔率数据API（the-odds-api.com，修复无密钥处理、参数规范）"""
    BASE_URL = "https://api.the-odds-api.com/v4"
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key if api_key else ENV_CONFIG["ODDS_API_KEY"]
        logger.info(f"OddsAPI 初始化完成，密钥状态：{'已配置' if self.api_key else '未配置'}")
    
    def get_upcoming_matches(self, sport: str = "soccer_epl", regions: str = "uk,eu"):
        """
        获取即将进行的比赛赔率
        sport可选：soccer_epl(英超)/soccer_la_liga(西甲)/soccer_bundesliga(德甲)/soccer_serie_a(意甲)/soccer_ligue_1(法甲)
        regions可选：uk/eu/us/au
        """
        if not self.api_key:
            logger.warning("OddsAPI 无API密钥，跳过赔率请求")
            return []
        try:
            params = {
                "apiKey": self.api_key,
                "regions": regions,
                "markets": "h2h,spreads,totals",
                "dateFormat": "iso"
            }
            url = f"{self.BASE_URL}/sports/{sport}/events"
            logger.info(f"请求{sport}赔率数据：{url}")
            
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            
            # 输出剩余请求额度，方便排查额度问题
            remaining = resp.headers.get('x-requests-remaining', '未知')
            used = resp.headers.get('x-requests-used', '未知')
            logger.info(f"赔率数据请求成功，剩余请求额度：{remaining}，本次已用：{used}")
            
            return resp.json()
        except Exception as e:
            status_code = getattr(resp, 'status_code', '未知')
            if status_code == 401:
                logger.error(f"获取赔率数据失败：密钥无效/过期（401）")
            elif status_code == 429:
                logger.error(f"获取赔率数据失败：API请求额度耗尽（429）")
            else:
                logger.error(f"获取赔率数据失败：HTTP状态码 {status_code}，错误信息：{e}")
            return []


class SofascoreAPI:
    """Sofascore快照数据API（修复接口地址规范）"""
    BASE_URL = "https://api.sofascore.com/api/v1"
    
    @staticmethod
    def get_match_statistics(match_id: int) -> Dict:
        """获取比赛统计数据"""
        try:
            url = f"{SofascoreAPI.BASE_URL}/event/{match_id}/statistics"
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"获取比赛{match_id}统计数据失败：{e}")
            return {}
    
    @staticmethod
    def get_team_form(team_id: int, limit: int = 10) -> List[Dict]:
        """获取球队最近比赛"""
        try:
            url = f"{SofascoreAPI.BASE_URL}/team/{team_id}/events/last/{limit}"
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            return resp.json().get('events', [])
        except Exception as e:
            logger.error(f"获取球队{team_id}近期比赛失败：{e}")
            return []


class DataAggregator:
    """数据聚合器 - 合并多个API源（修复密钥自动读取）"""
    
    def __init__(self, football_api_key: str = None, odds_api_key: str = None):
        # 优先传入的密钥，兜底用环境变量，无需手动传参
        self.fdb = FootballDataAPI(football_api_key)
        self.understat = UnderstatAPI()
        self.odds = OddsAPI(odds_api_key)
        self.sofascore = SofascoreAPI()
        logger.info("数据聚合器初始化完成")
    
    def get_comprehensive_match_data(self, match: Dict) -> Dict:
        """获取单场比赛的综合数据"""
        enhanced = {
            "basic": match,
            "odds": [],
            "team_form": {
                "home": {},
                "away": {}
            },
            "xg_stats": {},
            "head_to_head": []
        }
        
        try:
            # 获取赔率数据
            enhanced["odds"] = self.odds.get_upcoming_matches()
            
            # 获取球队数据（如果有ID）
            if "homeTeam" in match and match["homeTeam"].get("id"):
                team_form = self.sofascore.get_team_form(match["homeTeam"]["id"])
                enhanced["team_form"]["home"] = team_form
            
            if "awayTeam" in match and match["awayTeam"].get("id"):
                team_form = self.sofascore.get_team_form(match["awayTeam"]["id"])
                enhanced["team_form"]["away"] = team_form
        
        except Exception as e:
            logger.error(f"聚合比赛数据失败：{e}")
        
        return enhanced
    
    def get_league_data(self, competition_code: str = "PL", understat_league: str = "EPL") -> Dict:
        """获取完整联赛数据"""
        return {
            "standings": self.fdb.get_team_standings(competition_code),
            "matches": self.fdb.get_matches(competition_code),
            "xg_stats": self.understat.get_team_xg_stats(understat_league)
        }


# 快速工厂函数
def create_data_aggregator(football_api_key: str = None, odds_api_key: str = None) -> DataAggregator:
    """创建数据聚合器实例，自动读取环境变量密钥"""
    return DataAggregator(football_api_key, odds_api_key)


# ====================== 【修复】Mock 模拟数据（修复日期格式，避免特征工程报错） ======================
SAMPLE_MATCHES = [
    {
        "id": 1001, 
        "utcDate": datetime.strptime("2026-03-12T15:00:00Z", "%Y-%m-%dT%H:%M:%SZ"), 
        "competition": {"code": "PL"}, 
        "homeTeam": {"id": 65, "name": "曼城"}, 
        "awayTeam": {"id": 57, "name": "阿森纳"}, 
        "status": "SCHEDULED"
    },
    {
        "id": 1002, 
        "utcDate": datetime.strptime("2026-03-12T17:30:00Z", "%Y-%m-%dT%H:%M:%SZ"), 
        "competition": {"code": "PL"}, 
        "homeTeam": {"id": 64, "name": "利物浦"}, 
        "awayTeam": {"id": 66, "name": "曼联"}, 
        "status": "SCHEDULED"
    },
    {
        "id": 1003, 
        "utcDate": datetime.strptime("2026-03-13T14:00:00Z", "%Y-%m-%dT%H:%M:%SZ"), 
        "competition": {"code": "SA"}, 
        "homeTeam": {"id": 109, "name": "尤文图斯"}, 
        "awayTeam": {"id": 108, "name": "国际米兰"}, 
        "status": "SCHEDULED"
    },
    {
        "id": 1004, 
        "utcDate": datetime.strptime("2026-03-13T19:45:00Z", "%Y-%m-%dT%H:%M:%SZ"), 
        "competition": {"code": "BL1"}, 
        "homeTeam": {"id": 5, "name": "拜仁慕尼黑"}, 
        "awayTeam": {"id": 4, "name": "多特蒙德"}, 
        "status": "SCHEDULED"
    },
    {
        "id": 1005, 
        "utcDate": datetime.strptime("2026-03-14T20:00:00Z", "%Y-%m-%dT%H:%M:%SZ"), 
        "competition": {"code": "FR1"}, 
        "homeTeam": {"id": 524, "name": "巴黎圣日耳曼"}, 
        "awayTeam": {"id": 523, "name": "马赛"}, 
        "status": "SCHEDULED"
    },
    {
        "id": 1006, 
        "utcDate": datetime.strptime("2026-03-12T18:00:00Z", "%Y-%m-%dT%H:%M:%SZ"), 
        "competition": {"code": "PL"}, 
        "homeTeam": {"id": 61, "name": "切尔西"}, 
        "awayTeam": {"id": 73, "name": "热刺"}, 
        "status": "SCHEDULED"
    },
]

def _get_mock_matches(competition_code):
    """无 Key 时返回模拟赛程，修复日期格式为datetime对象，避免后续报错"""
    logger.info(f"🔄 无 API Key → 使用模拟数据（联赛：{competition_code}，共 {len(SAMPLE_MATCHES)} 场）")
    filtered = [m for m in SAMPLE_MATCHES if m["competition"]["code"] == competition_code]
    return filtered
