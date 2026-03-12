"""
多源足球数据API集成 - 修复版（完全对齐football-data.org v4官方规范）
"""
import requests
import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 环境变量读取
ENV_CONFIG = {
    "FOOTBALL_DATA_KEY": os.getenv("FOOTBALL_DATA_KEY", ""),
    "API_FOOTBALL_KEY": os.getenv("API_FOOTBALL_KEY", ""),
    "ODDS_API_KEY": os.getenv("ODDS_API_KEY", "")
}

# 启动密钥状态校验
logger.info("=== 环境变量密钥读取状态 ===")
for key, value in ENV_CONFIG.items():
    if "KEY" in key:
        logger.info(f"{key} 长度：{len(value)}")
logger.info("=============================")


class FootballDataAPI:
    """
    football-data.org 官方API v4 完全合规版
    官方规范联赛代码：
    - PL: 英超
    - PD: 西甲
    - BL1: 德甲
    - SA: 意甲
    - FL1: 法甲
    官方规范status值（必须全大写）：
    SCHEDULED, LIVE, IN_PLAY, PAUSED, FINISHED, POSTPONED, SUSPENDED, CANCELLED
    """
    BASE_URL = "https://api.football-data.org/v4"
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key if api_key else ENV_CONFIG["FOOTBALL_DATA_KEY"]
        self.headers = {
            "X-Auth-Token": self.api_key.strip(),
            "Accept": "application/json"
        } if self.api_key else {}
        logger.info(f"FootballDataAPI 初始化完成，密钥状态：{'已配置' if self.api_key else '未配置'}")
    
    def get_matches(
        self, 
        competition_code: str = "PL", 
        status: str = "SCHEDULED", 
        days: int = 7
    ) -> List[Dict]:
        """
        获取指定联赛的赛程（完全对齐官方API规范，修复400参数错误）
        """
        # 无密钥直接返回模拟数据，避免管道中断
        if not self.api_key:
            logger.info(f"无FOOTBALL_DATA_KEY，返回模拟赛程数据（联赛：{competition_code}）")
            return self._get_mock_matches(competition_code)
        
        try:
            # 修复1：使用UTC标准日期，避免时区导致的参数错误
            now_utc = datetime.now(timezone.utc)
            date_from = now_utc.strftime("%Y-%m-%d")
            date_to = (now_utc + timedelta(days=days)).strftime("%Y-%m-%d")
            
            # 修复2：强制参数合规，去除空格、统一大写，避免400错误
            params = {
                "status": status.strip().upper(),
                "dateFrom": date_from,
                "dateTo": date_to
            }
            competition_code = competition_code.strip()
            url = f"{self.BASE_URL}/competitions/{competition_code}/matches"
            
            # 打印完整请求信息，方便后续排查
            logger.info(f"发起API请求：URL={url}，参数={params}")
            resp = requests.get(
                url, 
                headers=self.headers, 
                params=params, 
                timeout=15
            )
            
            # 修复3：完整打印API返回的错误详情，不再盲猜问题
            if resp.status_code != 200:
                logger.error(f"API请求失败！HTTP状态码：{resp.status_code}")
                logger.error(f"API返回错误详情：{resp.text}")
                resp.raise_for_status()
            
            # 解析返回数据
            result = resp.json()
            matches = result.get("matches", [])
            logger.info(f"✅ {competition_code} 赛程获取成功，共 {len(matches)} 场比赛")
            return matches
        
        except Exception as e:
            logger.error(f"❌ 获取{competition_code}赛程失败，异常：{str(e)}")
            return []
    
    def _get_mock_matches(self, competition_code: str) -> List[Dict]:
        """模拟赛程数据，兜底用，确保管道不会因为API问题终止"""
        mock_data = [
            {
                "id": 1001,
                "utcDate": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat(),
                "competition": {"code": "PL", "name": "英超"},
                "homeTeam": {"id": 65, "name": "曼城", "shortName": "曼城"},
                "awayTeam": {"id": 57, "name": "阿森纳", "shortName": "阿森纳"},
                "status": "SCHEDULED",
                "matchday": 30
            },
            {
                "id": 1002,
                "utcDate": (datetime.now(timezone.utc) + timedelta(days=2)).isoformat(),
                "competition": {"code": "PD", "name": "西甲"},
                "homeTeam": {"id": 109, "name": "皇家马德里", "shortName": "皇马"},
                "awayTeam": {"id": 108, "name": "巴塞罗那", "shortName": "巴萨"},
                "status": "SCHEDULED",
                "matchday": 30
            },
            {
                "id": 1003,
                "utcDate": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat(),
                "competition": {"code": "BL1", "name": "德甲"},
                "homeTeam": {"id": 5, "name": "拜仁慕尼黑", "shortName": "拜仁"},
                "awayTeam": {"id": 4, "name": "多特蒙德", "shortName": "多特"},
                "status": "SCHEDULED",
                "matchday": 28
            },
            {
                "id": 1004,
                "utcDate": (datetime.now(timezone.utc) + timedelta(days=3)).isoformat(),
                "competition": {"code": "SA", "name": "意甲"},
                "homeTeam": {"id": 61, "name": "尤文图斯", "shortName": "尤文"},
                "awayTeam": {"id": 73, "name": "国际米兰", "shortName": "国米"},
                "status": "SCHEDULED",
                "matchday": 30
            },
            {
                "id": 1005,
                "utcDate": (datetime.now(timezone.utc) + timedelta(days=2)).isoformat(),
                "competition": {"code": "FL1", "name": "法甲"},
                "homeTeam": {"id": 524, "name": "巴黎圣日耳曼", "shortName": "巴黎"},
                "awayTeam": {"id": 523, "name": "马赛", "shortName": "马赛"},
                "status": "SCHEDULED",
                "matchday": 29
            }
        ]
        # 过滤对应联赛的模拟数据
        filtered = [m for m in mock_data if m["competition"]["code"] == competition_code]
        return filtered if filtered else mock_data[:2]


class OddsAPI:
    """赔率数据API（兼容原有逻辑）"""
    BASE_URL = "https://api.the-odds-api.com/v4"
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key if api_key else ENV_CONFIG["ODDS_API_KEY"]
        logger.info(f"OddsAPI 初始化完成，密钥状态：{'已配置' if self.api_key else '未配置'}")
    
    def get_upcoming_matches(self, sport: str = "soccer_epl") -> List[Dict]:
        if not self.api_key:
            logger.warning("无ODDS_API_KEY，跳过赔率请求")
            return []
        try:
            params = {
                "apiKey": self.api_key.strip(),
                "regions": "uk,eu",
                "markets": "h2h",
                "dateFormat": "iso"
            }
            resp = requests.get(f"{self.BASE_URL}/sports/{sport}/events", params=params, timeout=10)
            resp.raise_for_status()
            logger.info(f"✅ {sport} 赔率数据获取成功")
            return resp.json()
        except Exception as e:
            logger.error(f"❌ 获取赔率数据失败：{str(e)}")
            return []


class DataAggregator:
    """数据聚合器，和原有主管道完全兼容"""
    def __init__(self):
        self.fdb_api = FootballDataAPI()
        self.odds_api = OddsAPI()
        logger.info("数据聚合器初始化完成")
    
    def get_all_upcoming_matches(self, competitions: List[str] = ["PL", "PD", "BL1", "SA", "FL1"]) -> List[Dict]:
        """获取所有指定联赛的即将进行的比赛"""
        all_matches = []
        for comp in competitions:
            matches = self.fdb_api.get_matches(competition_code=comp)
            all_matches.extend(matches)
        
        logger.info(f"✅ 所有联赛共获取到 {len(all_matches)} 场比赛")
        return all_matches


# 导出实例，方便主管道调用
def create_data_aggregator() -> DataAggregator:
    return DataAggregator()
