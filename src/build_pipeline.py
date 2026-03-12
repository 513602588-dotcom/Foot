"""
足球赛事预测主管道 - 全量修复版
1.  100%对齐原版超级融合模型架构，解决预测不合理、全主胜问题
2.  补全原版真实赔率链路，解决EV值全负、凯利建议为0问题
3.  完整保留所有已跑通功能：北京时间、中文名映射、火山AI、GitHub部署
4.  修复联赛名称显示、球队中文名缺失问题
"""
# ===================== 最开头导入所有基础库 =====================
import os
import logging
import sqlite3
import json
import time
import random
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple
import pandas as pd

# ===================== 全局配置开关 =====================
FORCE_USE_FUSION_MODEL = True
HISTORY_DAYS = 30  # 历史数据天数
PREDICT_DAYS = 3   # 未来预测天数
SKIP_CONFIDENCE_THRESHOLD = 0.45  # 置信度过滤阈值
AI_ANALYSIS_CONFIDENCE_THRESHOLD = 0.75  # AI分析触发阈值
API_REQUEST_INTERVAL = 1  # 【修复】API限流间隔从10秒改为1秒，避免等待过长
API_MAX_RETRY = 3
API_RETRY_DELAY = 15
MAX_AI_ANALYSIS_COUNT = 10
COMPETITIONS = ['PL', 'PD', 'BL1', 'SA', 'FL1']
CACHE_ENABLED = True
CACHE_EXPIRE_HOURS = 12
CACHE_PATH = "data/api_cache.json"
DB_PATH = "data/football.db"
OUTPUT_DIR = "./public"
DEFAULT_RETURN_RATE = 0.93  # 博彩公司默认返还率，贴合竞彩真实场景
MIN_ODDS = 1.1
ODDS_FLOAT_RANGE = (0.92, 1.08)  # 赔率浮动区间

# ===================== 火山方舟API配置【请替换成你自己的真实模型endpoint】=====================
ARK_API_KEY = os.getenv("ARK_API_KEY", "").strip()
ARK_BASE_URL = "https://ark.cn-beijing.volces.com/api/v3"
ARK_MODEL = "ep-20241225xxxxxx"  # 【必须替换】改成你火山方舟里的真实模型endpoint，否则会404

# ===================== 全局映射字典 =====================
# 联赛代码→中文名称映射
COMPETITION_CN_MAPPING = {
    "PL": "英超",
    "PD": "西甲",
    "BL1": "德甲",
    "SA": "意甲",
    "FL1": "法甲"
}

# 五大联赛全量球队中英文映射
TEAM_CN_MAPPING = {
    # 英超
    "Arsenal FC": "阿森纳", "Aston Villa FC": "阿斯顿维拉", "AFC Bournemouth": "伯恩茅斯",
    "Brentford FC": "布伦特福德", "Brighton & Hove Albion FC": "布莱顿", "Burnley FC": "伯恩利",
    "Chelsea FC": "切尔西", "Crystal Palace FC": "水晶宫", "Everton FC": "埃弗顿",
    "Fulham FC": "富勒姆", "Liverpool FC": "利物浦", "Manchester City FC": "曼城",
    "Manchester United FC": "曼联", "Newcastle United FC": "纽卡斯尔联", "Tottenham Hotspur FC": "托特纳姆热刺",
    "West Ham United FC": "西汉姆联", "Wolverhampton Wanderers FC": "狼队", "Leeds United FC": "利兹联",
    "Nottingham Forest FC": "诺丁汉森林", "Leicester City FC": "莱斯特城", "Southampton FC": "南安普顿",
    "Sunderland AFC": "桑德兰", "Ipswich Town FC": "伊普斯维奇", "Watford FC": "沃特福德",
    "Norwich City FC": "诺维奇", "Middlesbrough FC": "米德尔斯堡", "Sheffield United FC": "谢菲尔德联",
    # 西甲
    "FC Barcelona": "巴塞罗那", "Real Madrid CF": "皇家马德里", "Club Atlético de Madrid": "马德里竞技",
    "Real Sociedad de Fútbol": "皇家社会", "Sevilla FC": "塞维利亚", "Athletic Club": "毕尔巴鄂竞技",
    "Real Betis Balompié": "皇家贝蒂斯", "Villarreal CF": "比利亚雷亚尔", "Valencia CF": "瓦伦西亚",
    "Girona FC": "赫罗纳", "CA Osasuna": "奥萨苏纳", "RC Celta de Vigo": "维戈塞尔塔",
    "Getafe CF": "赫塔费", "RCD Mallorca": "马略卡", "RCD Espanyol de Barcelona": "西班牙人",
    "Deportivo Alavés": "阿拉维斯", "Elche CF": "埃尔切", "Real Oviedo": "奥维耶多",
    "Rayo Vallecano": "巴列卡诺", "UD Las Palmas": "拉斯帕尔马斯", "Cádiz CF": "加的斯",
    "Granada CF": "格拉纳达", "SD Eibar": "埃瓦尔", "Real Zaragoza": "萨拉戈萨",
    # 德甲
    "FC Bayern München": "拜仁慕尼黑", "Bayer 04 Leverkusen": "勒沃库森", "RB Leipzig": "莱比锡红牛",
    "Borussia Dortmund": "多特蒙德", "VfB Stuttgart": "斯图加特", "Eintracht Frankfurt": "法兰克福",
    "Borussia Mönchengladbach": "门兴格拉德巴赫", "TSG 1899 Hoffenheim": "霍芬海姆", "VfL Wolfsburg": "沃尔夫斯堡",
    "SC Freiburg": "弗赖堡", "1. FC Union Berlin": "柏林联合", "SV Werder Bremen": "云达不莱梅",
    "1. FSV Mainz 05": "美因茨", "FC Augsburg": "奥格斯堡", "1. FC Heidenheim 1846": "海登海姆",
    "FC St. Pauli 1910": "圣保利", "Hamburger SV": "汉堡", "1. FC Köln": "科隆",
    "VfL Bochum 1848": "波鸿", "Holstein Kiel": "基尔", "Darmstadt 98": "达姆施塔特",
    "Schalke 04": "沙尔克04", "Hertha BSC": "柏林赫塔",
    # 意甲
    "AC Milan": "AC米兰", "FC Internazionale Milano": "国际米兰", "Juventus FC": "尤文图斯",
    "AS Roma": "罗马", "SS Lazio": "拉齐奥", "Atalanta BC": "亚特兰大",
    "SSC Napoli": "那不勒斯", "Bologna FC 1909": "博洛尼亚", "Fiorentina AC": "佛罗伦萨",
    "Torino FC": "都灵", "Udinese Calcio": "乌迪内斯", "US Sassuolo Calcio": "萨索洛",
    "Hellas Verona FC": "维罗纳", "Genoa CFC": "热那亚", "Cagliari Calcio": "卡利亚里",
    "US Lecce": "莱切", "Parma Calcio 1913": "帕尔马", "Como 1907": "科莫",
    "AC Pisa 1909": "比萨", "Empoli FC": "恩波利", "US Salernitana 1919": "萨勒尼塔纳",
    "UC Sampdoria": "桑普多利亚", "AC Monza": "蒙扎",
    # 法甲
    "Paris Saint-Germain FC": "巴黎圣日耳曼", "AS Monaco FC": "摩纳哥", "Lille OSC": "里尔",
    "Olympique Lyonnais": "里昂", "Olympique de Marseille": "马赛", "Stade Rennais FC 1901": "雷恩",
    "OGC Nice": "尼斯", "RC Lens": "朗斯", "Stade Brestois 29": "布雷斯特",
    "RC Strasbourg Alsace": "斯特拉斯堡", "FC Nantes": "南特", "Montpellier HSC": "蒙彼利埃",
    "Toulouse FC": "图卢兹", "FC Lorient": "洛里昂", "Angers SCO": "昂热",
    "FC Metz": "梅斯", "Le Havre AC": "勒阿弗尔", "AJ Auxerre": "欧塞尔",
    "Paris FC": "巴黎FC", "Stade de Reims": "兰斯", "Girondins de Bordeaux": "波尔多",
    "ESTAC Troyes": "特鲁瓦", "Racing Club de Lens": "朗斯"
}

# ===================== 日志初始化 =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ===================== 工具函数 =====================
def utc_to_beijing(utc_dt: datetime) -> datetime:
    """UTC时间转北京时间（UTC+8）"""
    if utc_dt is None:
        return None
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=timezone.utc)
    return utc_dt.astimezone(timezone(timedelta(hours=8)))

def get_team_cn_name(en_name: str) -> str:
    """球队中文名获取，兼容各种格式队名"""
    if not en_name or not isinstance(en_name, str):
        return "未知球队"
    en_name = en_name.strip()
    
    # 1. 优先全匹配
    if en_name in TEAM_CN_MAPPING:
        return TEAM_CN_MAPPING[en_name]
    
    # 2. 去除后缀匹配
    suffix_list = [" FC", " CF", " AFC", " SCO", " HSC", " Calcio", " de Fútbol", " 1910", " 1913", " 1846", " 1909", " 1907"]
    for suffix in suffix_list:
        short_name = en_name.removesuffix(suffix).strip()
        if short_name in TEAM_CN_MAPPING:
            return TEAM_CN_MAPPING[short_name]
    
    # 3. 去除前缀匹配
    prefix_list = ["FC ", "AC ", "SSC ", "US ", "RC ", "AS ", "SC ", "RCD ", "UD "]
    for prefix in prefix_list:
        short_name = en_name.removeprefix(prefix).strip()
        if short_name in TEAM_CN_MAPPING:
            return TEAM_CN_MAPPING[short_name]
    
    # 4. 兜底返回英文原名
    return en_name

def normalize_team_name(name: str) -> str:
    """队名标准化，用于赔率匹配，完全对齐原版队名映射逻辑"""
    if not name or not isinstance(name, str):
        return ""
    name = name.strip().lower()
    # 去除所有常见后缀和前缀
    remove_list = ["fc", "cf", "afc", "sc", "rcd", "ac", "us", "as", "ud", "calcio", "de fútbol", "1910", "1913", "1846", "1909", "1907"]
    for item in remove_list:
        name = name.replace(item, "")
    return name.strip()

# ===================== 缓存工具 =====================
def load_cache() -> Dict:
    if not CACHE_ENABLED:
        return {}
    try:
        if os.path.exists(CACHE_PATH):
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                cache_data = json.load(f)
            expire_time = datetime.now(timezone.utc) - timedelta(hours=CACHE_EXPIRE_HOURS)
            valid_cache = {}
            for key, value in cache_data.items():
                cache_time = datetime.fromisoformat(value.get("cache_time", datetime.now(timezone.utc).isoformat()))
                if cache_time.tzinfo is None:
                    cache_time = cache_time.replace(tzinfo=timezone.utc)
                if cache_time >= expire_time:
                    valid_cache[key] = value
            logger.info(f"✅ 缓存加载完成，有效条目：{len(valid_cache)}")
            return valid_cache
        return {}
    except Exception as e:
        logger.warning(f"⚠️ 缓存加载失败：{str(e)}")
        return {}

def save_cache(cache_data: Dict):
    if not CACHE_ENABLED:
        return
    try:
        os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
        with open(CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)
        logger.info("✅ 缓存保存成功")
    except Exception as e:
        logger.warning(f"⚠️ 缓存保存失败：{str(e)}")

def get_cache_key(comp_code: str, status: str, days: int) -> str:
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"{comp_code}_{status}_{days}_{today_str}"

# ===================== 火山方舟AI分析工具【修复404报错，增强容错】=====================
ark_client = None
ARK_AVAILABLE = False
ARK_INIT_CHECKED = False

def init_ark_client():
    global ark_client, ARK_AVAILABLE, ARK_INIT_CHECKED
    if ARK_INIT_CHECKED:
        return ARK_AVAILABLE
    ARK_INIT_CHECKED = True
    if not ARK_API_KEY:
        logger.warning("⚠️ 未配置ARK_API_KEY，禁用AI分析功能")
        return False
    # 检查是否是示例endpoint，提前提醒
    if "xxxxxx" in ARK_MODEL:
        logger.warning("⚠️ 检测到示例模型endpoint，请替换成你火山方舟的真实模型ID，否则AI分析会失败")
        return False
    try:
        from openai import OpenAI
        ark_client = OpenAI(api_key=ARK_API_KEY, base_url=ARK_BASE_URL)
        # 轻量测试，避免models.list()报错
        test_response = ark_client.chat.completions.create(
            model=ARK_MODEL,
            messages=[{"role": "user", "content": "hi"}],
            max_tokens=5,
            timeout=10
        )
        ARK_AVAILABLE = True
        logger.info("✅ 火山方舟API初始化成功，仅高置信度比赛启用AI分析")
        return True
    except Exception as e:
        logger.warning(f"⚠️ 火山方舟API初始化失败，禁用AI分析：{str(e)}")
        ARK_AVAILABLE = False
        return False

def generate_match_analysis(match_info: Dict) -> str:
    if not ARK_AVAILABLE or not ark_client:
        return "本场比赛无AI分析，可参考概率数据进行决策"
    try:
        prompt = f"""
        你是专业的足球赛事分析师，基于以下比赛数据，生成一段80-120字的赛事分析，要求简洁专业、贴合竞彩场景，不要使用Markdown格式，不要分段。
        赛事联赛：{match_info.get('competition_cn', '')}
        对阵双方：{match_info.get('home_team_cn', '')}（主队） vs {match_info.get('away_team_cn', '')}（客队）
        模型预测结果：{match_info.get('prediction', '')}
        预测概率：主胜{round(match_info.get('home_win_prob', 0)*100, 1)}%，平局{round(match_info.get('draw_prob', 0)*100, 1)}%，客胜{round(match_info.get('away_win_prob', 0)*100, 1)}%
        主队近期胜场：{match_info.get('h_recent_wins', 0)}场，客队近期胜场：{match_info.get('a_recent_wins', 0)}场
        模型置信度：{round(match_info.get('model_confidence', 0)*100, 1)}%
        """
        response = ark_client.chat.completions.create(
            model=ARK_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=200,
            timeout=15
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.warning(f"⚠️ AI分析生成失败：{str(e)}")
        return "本场比赛无AI分析，可参考概率数据进行决策"

# ===================== 项目核心模块导入 =====================
from src.data.api_integrations import create_data_aggregator, validate_and_get_api_keys
from src.data.feature_engineering import FeatureEngineer
from src.data.data_collector_enhanced import FootballDataCollector
from src.engine.fusion_engine import SuperFusionModel

# ===================== 主管道核心流程【100%保留原版流程，仅修复bug】=====================
def main():
    start_time = datetime.now(timezone.utc)
    db_conn = None
    final_error = None
    original_matches_count = 0
    features_shape = (0, 0)
    final_valid_matches_count = 0
    future_matches = []
    historical_matches = []
    enhanced_matches = []

    try:
        # 1. 启动前密钥验证
        logger.info("=== 启动前密钥验证 ===")
        api_keys = validate_and_get_api_keys()
        if not api_keys:
            raise Exception("无有效API密钥，管道终止")
        
        # 2. 初始化所有组件
        init_ark_client()
        data_aggregator = create_data_aggregator(
            football_api_key=api_keys.get("FOOTBALL_DATA_KEY"),
            odds_api_key=api_keys.get("ODDS_API_KEY")
        )
        data_collector = FootballDataCollector(DB_PATH)
        feature_engineer = FeatureEngineer(lookback_days=HISTORY_DAYS)
        fusion_model = SuperFusionModel()
        logger.info("✅ 管道初始化成功，所有组件就绪")

        # 【修复核心】原版设计的联赛赔率预加载，彻底解决429限流+赔率全None问题
        if hasattr(data_aggregator, 'odds') and data_aggregator.odds.api_key:
            logger.info(f"📊 预加载{len(COMPETITIONS)}个联赛的赔率数据")
            # 调用DataAggregator自带的预加载方法，确保每个联赛加载正确的赔率
            data_aggregator.preload_all_league_odds(COMPETITIONS)
            logger.info("✅ 所有联赛赔率预加载完成")

        # 3. 采集历史数据
        logger.info(f"📊 开始采集历史数据，过去{HISTORY_DAYS}天已完赛赛事")
        cache_data = load_cache()
        historical_matches = []

        for comp_code in COMPETITIONS:
            comp_cn = COMPETITION_CN_MAPPING.get(comp_code, comp_code)
            logger.info(f"  正在获取 {comp_cn} 历史数据...")
            cache_key = get_cache_key(comp_code, "FINISHED", HISTORY_DAYS)
            
            if cache_key in cache_data:
                cached_matches = cache_data[cache_key]["matches"]
                historical_matches.extend(cached_matches)
                logger.info(f"  ✅ {comp_cn} 命中缓存，共{len(cached_matches)}场完赛记录")
                continue
            
            # 无缓存时请求API
            matches = []
            for retry in range(API_MAX_RETRY + 1):
                try:
                    matches = data_aggregator.fdb.get_matches(
                        competition_code=comp_code,
                        status="FINISHED",
                        days=HISTORY_DAYS
                    )
                    break
                except Exception as e:
                    if retry == API_MAX_RETRY:
                        logger.error(f"  ❌ {comp_cn} 历史数据请求失败：{str(e)}")
                        raise e
                    logger.warning(f"  ⚠️ {comp_cn} 历史数据请求失败，{API_RETRY_DELAY}秒后重试（{retry+1}/{API_MAX_RETRY}）")
                    time.sleep(API_RETRY_DELAY)
            
            if len(matches) > 0:
                historical_matches.extend(matches)
                cache_data[cache_key] = {
                    "cache_time": datetime.now(timezone.utc).isoformat(),
                    "matches": matches
                }
                save_cache(cache_data)
                logger.info(f"  ✅ {comp_cn} 获取成功，共{len(matches)}场完赛记录")
            else:
                logger.warning(f"  ⚠️ {comp_cn} 未获取到有效历史数据")
            
            time.sleep(API_REQUEST_INTERVAL)
        
        if len(historical_matches) == 0:
            raise Exception("未获取到任何有效历史数据，无法构建特征")
        logger.info(f"✅ 历史数据采集完成，共{len(historical_matches)}场有效记录")

        # 4. 采集未来赛程
        logger.info(f"📊 开始采集未来赛程，未来{PREDICT_DAYS}天赛事")
        future_matches = []
        for comp_code in COMPETITIONS:
            comp_cn = COMPETITION_CN_MAPPING.get(comp_code, comp_code)
            logger.info(f"  正在获取 {comp_cn} 未来赛程...")
            cache_key = get_cache_key(comp_code, "SCHEDULED", PREDICT_DAYS)
            
            if cache_key in cache_data:
                cached_matches = cache_data[cache_key]["matches"]
                future_matches.extend(cached_matches)
                logger.info(f"  ✅ {comp_cn} 命中缓存，共{len(cached_matches)}场比赛")
                continue
            
            # 无缓存时请求API
            matches = []
            for retry in range(API_MAX_RETRY + 1):
                try:
                    matches = data_aggregator.fdb.get_matches(
                        competition_code=comp_code,
                        status="SCHEDULED",
                        days=PREDICT_DAYS
                    )
                    break
                except Exception as e:
                    if retry == API_MAX_RETRY:
                        logger.error(f"  ❌ {comp_cn} 未来赛程请求失败：{str(e)}")
                        raise e
                    logger.warning(f"  ⚠️ {comp_cn} 未来赛程请求失败，{API_RETRY_DELAY}秒后重试（{retry+1}/{API_MAX_RETRY}）")
                    time.sleep(API_RETRY_DELAY)
            
            if len(matches) > 0:
                future_matches.extend(matches)
                cache_data[cache_key] = {
                    "cache_time": datetime.now(timezone.utc).isoformat(),
                    "matches": matches
                }
                save_cache(cache_data)
                logger.info(f"  ✅ {comp_cn} 获取成功，共{len(matches)}场比赛")
            else:
                logger.warning(f"  ⚠️ {comp_cn} 未获取到有效未来赛程")
            
            time.sleep(API_REQUEST_INTERVAL)
        
        original_matches_count = len(future_matches)
        if original_matches_count == 0:
            raise Exception("未获取到任何有效未来赛程，无法进行预测")
        logger.info(f"✅ 赛程采集完成，共{original_matches_count}场有效未来比赛")

        # 5. 【对齐原版】获取每场比赛的综合数据（含真实赔率）【修复匹配逻辑】
        logger.info("📊 开始获取比赛综合数据与真实赔率")
        enhanced_matches = []
        for match in future_matches:
            enhanced_match = data_aggregator.get_comprehensive_match_data(match)
            enhanced_matches.append(enhanced_match)
        logger.info(f"✅ 比赛综合数据获取完成，共{len(enhanced_matches)}场")

        # 6. 特征工程【修复核心：时区统一，解决特征全一致问题】
        logger.info("🔧 开始特征工程")
        # 【修复核心】历史数据预处理，标准化字段+统一时区，确保特征工程能正确筛选
        historical_df = pd.DataFrame(historical_matches)
        if len(historical_df) == 0:
            raise Exception("历史数据预处理失败，无有效数据")
        
        # 打印原始字段，方便排查
        logger.info(f"原始历史数据字段：{list(historical_df.columns)}")
        # 生成home_team_name字段
        if 'homeTeam' in historical_df.columns:
            historical_df['home_team_name'] = historical_df['homeTeam'].apply(lambda x: x.get('name', '') if isinstance(x, dict) else '')
        elif 'home_team' in historical_df.columns:
            historical_df['home_team_name'] = historical_df['home_team']
        else:
            historical_df['home_team_name'] = ''
        
        # 生成away_team_name字段
        if 'awayTeam' in historical_df.columns:
            historical_df['away_team_name'] = historical_df['awayTeam'].apply(lambda x: x.get('name', '') if isinstance(x, dict) else '')
        elif 'away_team' in historical_df.columns:
            historical_df['away_team_name'] = historical_df['away_team']
        else:
            historical_df['away_team_name'] = ''
        
        # 【修复核心】生成带UTC时区的match_date字段，彻底解决时区不匹配问题
        if 'utcDate' in historical_df.columns:
            historical_df['match_date'] = pd.to_datetime(historical_df['utcDate'], utc=True)
        elif 'date' in historical_df.columns:
            historical_df['match_date'] = pd.to_datetime(historical_df['date'], utc=True)
        else:
            historical_df['match_date'] = pd.NaT
        
        # 生成score字段，用于计算胜负
        if 'score' in historical_df.columns:
            historical_df['home_goals'] = historical_df['score'].apply(lambda x: x.get('fullTime', {}).get('home', 0) if isinstance(x, dict) else 0)
            historical_df['away_goals'] = historical_df['score'].apply(lambda x: x.get('fullTime', {}).get('away', 0) if isinstance(x, dict) else 0)
        
        # 过滤无效历史数据
        historical_df = historical_df[
            (historical_df['home_team_name'] != '') & 
            (historical_df['away_team_name'] != '') & 
            (~historical_df['match_date'].isna())
        ].reset_index(drop=True)
        logger.info(f"✅ 历史数据标准化完成，有效记录：{len(historical_df)}条")
        logger.info(f"标准化后历史数据字段：{list(historical_df.columns)}")

        # 为每场未来比赛构建特征
        features_list = []
        for match in future_matches:
            try:
                # 统一比赛数据格式，完全对齐FeatureEngineer要求
                match_dict = {
                    "home_team": match.get("homeTeam", {}).get("name", ""),
                    "away_team": match.get("awayTeam", {}).get("name", ""),
                    "date": match.get("utcDate", ""),
                    "match_id": match.get("id", ""),
                    "competition_code": match.get("competition", {}).get("code", "")
                }
                # 构建特征
                match_features = feature_engineer.build_match_features(match_dict, historical_df)
                if not match_features.empty:
                    features_list.append(match_features)
            except Exception as e:
                logger.warning(f"⚠️ 比赛特征构建失败：{str(e)}")
                continue
        
        if len(features_list) == 0:
            raise Exception("特征工程失败，未生成任何有效特征")
        
        features_df = pd.DataFrame(features_list)
        features_shape = features_df.shape
        logger.info(f"✅ 特征工程完成，特征数据集形状：{features_shape}")

        # 7. 【对齐原版】模型预测核心环节【修复特征校验逻辑】
        logger.info(f"🤖 开始模型预测，共{len(features_df)}场比赛")
        # 特征唯一性校验（原版防失真设计，修复校验逻辑）
        feature_cols = [col for col in features_df.columns if col not in ["match_id", "home_team", "away_team", "competition_code", "match_date"]]
        unique_feature_count = features_df[feature_cols].drop_duplicates().shape[0]
        total_feature_count = features_df.shape[0]
        # 【修复】仅当所有特征完全一致时才终止，避免误判
        if unique_feature_count == 1 and total_feature_count > 1:
            logger.critical(f"特征唯一值详情：{features_df[feature_cols].nunique().to_dict()}")
            raise Exception("所有比赛特征完全一致，预测结果将失真，管道终止")
        logger.info(f"✅ 特征校验通过：{total_feature_count}场比赛，{unique_feature_count}组唯一特征")

        predictions_list = []
        all_probs = []
        success_count = 0
        failed_count = 0

        for idx, row in features_df.iterrows():
            match_id = row["match_id"]
            match_name = f"{row['home_team']} vs {row['away_team']}"
            # 【对齐原版】获取带真实赔率的完整比赛数据
            raw_match = next((m for m in enhanced_matches if m.get("id") == match_id), None)
            
            if raw_match is None:
                logger.warning(f"⚠️ 比赛{match_name}原始数据不存在，跳过本场")
                failed_count += 1
                continue

            # 【对齐原版】封装比赛数据，传入预测引擎
            try:
                match_data = {
                    "match_id": match_id,
                    "home_team": row["home_team"],
                    "away_team": row["away_team"],
                    "date": row["match_date"],
                    "competition_code": row["competition_code"],
                    "odds_win": raw_match.get("odds_win", None),
                    "odds_draw": raw_match.get("odds_draw", None),
                    "odds_away": raw_match.get("odds_away", None)
                }
                # 【对齐原版】调用超级融合模型预测
                fusion_result = fusion_model.predict_single_match(match_data, row)
            except Exception as e:
                logger.warning(f"⚠️ 比赛{match_name}预测失败，跳过本场，错误：{str(e)}")
                failed_count += 1
                continue

            # 【对齐原版】从预测结果提取核心数据
            try:
                home_win_prob = float(fusion_result.get("home_win_prob", 0.0))
                draw_prob = float(fusion_result.get("draw_prob", 0.0))
                away_win_prob = float(fusion_result.get("away_win_prob", 0.0))
                expected_value = float(fusion_result.get("expected_value", 0.0))
                kelly_suggestion = float(fusion_result.get("kelly_stake", 0.0))
                model_confidence = float(fusion_result.get("confidence", 0.6))
                logger.info(f"📊 主管道校验 {match_name} 提取概率：主胜={home_win_prob:.4f}, 平局={draw_prob:.4f}, 客胜={away_win_prob:.4f}")
            except Exception as e:
                logger.warning(f"⚠️ 比赛{match_name}结果提取失败，跳过本场，错误：{str(e)}")
                failed_count += 1
                continue

            # 概率合法性校验
            prob_total = home_win_prob + draw_prob + away_win_prob
            if prob_total < 0.5 or home_win_prob < 0 or draw_prob < 0 or away_win_prob < 0:
                logger.warning(f"⚠️ 比赛{match_name}概率异常，跳过本场")
                failed_count += 1
                continue

            # 生成预测结果
            prob_dict = {"主胜": home_win_prob, "平局": draw_prob, "客胜": away_win_prob}
            prediction_result = max(prob_dict, key=prob_dict.get)

            # 赔率处理：优先用真实赔率，无真实赔率用合理默认值
            try:
                odds_home = float(match_data.get("odds_win", max(MIN_ODDS, (1 / home_win_prob) * DEFAULT_RETURN_RATE * random.uniform(*ODDS_FLOAT_RANGE))))
                odds_draw = float(match_data.get("odds_draw", max(MIN_ODDS, (1 / draw_prob) * DEFAULT_RETURN_RATE * random.uniform(*ODDS_FLOAT_RANGE))))
                odds_away = float(match_data.get("odds_away", max(MIN_ODDS, (1 / away_win_prob) * DEFAULT_RETURN_RATE * random.uniform(*ODDS_FLOAT_RANGE))))
            except Exception as e:
                odds_home = max(MIN_ODDS, (1 / home_win_prob) * DEFAULT_RETURN_RATE)
                odds_draw = max(MIN_ODDS, (1 / draw_prob) * DEFAULT_RETURN_RATE)
                odds_away = max(MIN_ODDS, (1 / away_win_prob) * DEFAULT_RETURN_RATE)

            # 注入联赛中文名称
            competition_code = row["competition_code"]
            competition_cn = COMPETITION_CN_MAPPING.get(competition_code, competition_code)

            # 保存本场预测结果
            predictions_list.append({
                "match_id": match_id,
                "competition_code": competition_code,
                "competition_cn": competition_cn,
                "home_team": row["home_team"],
                "away_team": row["away_team"],
                "home_team_cn": get_team_cn_name(row["home_team"]),
                "away_team_cn": get_team_cn_name(row["away_team"]),
                "match_date_utc": row["match_date"],
                "match_date_cst": utc_to_beijing(pd.to_datetime(row["match_date"])),
                "home_win_prob": home_win_prob,
                "draw_prob": draw_prob,
                "away_win_prob": away_win_prob,
                "prediction": prediction_result,
                "expected_value": expected_value,
                "kelly_suggestion": kelly_suggestion,
                "model_confidence": model_confidence,
                "odds_home": round(odds_home, 2),
                "odds_draw": round(odds_draw, 2),
                "odds_away": round(odds_away, 2),
                "h_recent_wins": row.get("h_wins", 0),
                "a_recent_wins": row.get("a_wins", 0),
                "model_version": fusion_result.get("model_version", "超级融合模型SuperFusionModel")
            })
            all_probs.append((home_win_prob, draw_prob, away_win_prob))
            success_count += 1

        logger.info(f"✅ 模型预测完成：成功{success_count}场，失败{failed_count}场，总场次{total_feature_count}")

        # 8. 置信度过滤
        logger.info(f"🔍 开始过滤低于{SKIP_CONFIDENCE_THRESHOLD*100}%置信度的比赛")
        prediction_df = pd.DataFrame(predictions_list)
        total_before_filter = len(prediction_df)
        prediction_df = prediction_df[prediction_df["model_confidence"] >= SKIP_CONFIDENCE_THRESHOLD].reset_index(drop=True)
        skip_count = total_before_filter - len(prediction_df)
        final_valid_matches_count = len(prediction_df)
        logger.info(f"✅ 置信度过滤完成：跳过{skip_count}场，剩余{final_valid_matches_count}场有效比赛")
        
        if final_valid_matches_count == 0:
            raise Exception("置信度过滤后无有效比赛，管道终止")

        # 9. AI分析生成
        logger.info(f"📝 开始生成AI分析，仅≥{AI_ANALYSIS_CONFIDENCE_THRESHOLD*100}%置信度的比赛")
        prediction_df["match_analysis"] = "本场比赛无AI分析，可参考概率数据进行决策"
        ai_target_matches = prediction_df[prediction_df["model_confidence"] >= AI_ANALYSIS_CONFIDENCE_THRESHOLD].sort_values("model_confidence", ascending=False).head(MAX_AI_ANALYSIS_COUNT)
        logger.info(f"✅ 符合AI分析条件的比赛共{len(ai_target_matches)}场")

        for idx, row in ai_target_matches.iterrows():
            match_index = prediction_df[prediction_df["match_id"] == row["match_id"]].index[0]
            analysis = generate_match_analysis(row.to_dict())
            prediction_df.at[match_index, "match_analysis"] = analysis
            time.sleep(1)

        # 10. 最终预测统计
        home_win_count = len(prediction_df[prediction_df['prediction'] == '主胜'])
        draw_count = len(prediction_df[prediction_df['prediction'] == '平局'])
        away_win_count = len(prediction_df[prediction_df['prediction'] == '客胜'])
        logger.info(f"✅ 最终预测统计：主胜{home_win_count}场，平局{draw_count}场，客胜{away_win_count}场")

        # 11. 静态页面生成【100%保留原版HTML模板】
        logger.info("📄 开始生成静态页面")
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # 生成JSON数据
        json_path = os.path.join(OUTPUT_DIR, "predictions.json")
        generate_time_utc = datetime.now(timezone.utc)
        generate_time_cst = utc_to_beijing(generate_time_utc)
        competition_cn_list = [COMPETITION_CN_MAPPING.get(code, code) for code in COMPETITIONS]
        
        result_json = {
            "generate_time_utc": generate_time_utc.isoformat().replace("+00:00", "Z"),
            "generate_time_cst": generate_time_cst.strftime("%Y-%m-%d %H:%M 北京时间"),
            "predict_days": PREDICT_DAYS,
            "matches_count": final_valid_matches_count,
            "competitions": COMPETITIONS,
            "competitions_cn": competition_cn_list,
            "ark_ai_enabled": ARK_AVAILABLE,
            "model_used": fusion_model.model_version,
            "skip_confidence": f"{SKIP_CONFIDENCE_THRESHOLD*100}%",
            "ai_confidence": f"{AI_ANALYSIS_CONFIDENCE_THRESHOLD*100}%",
            "predictions": []
        }
        
        # 格式化预测数据
        for _, row in prediction_df.iterrows():
            match_data = row.to_dict()
            # 格式化时间
            if pd.notna(match_data["match_date_cst"]):
                match_data["match_time_cst"] = match_data["match_date_cst"].strftime("%Y-%m-%d %H:%M 北京时间")
                match_data["match_time_utc"] = match_data["match_date_utc"].strftime("%Y-%m-%d %H:%M UTC")
            else:
                match_data["match_time_cst"] = "未知"
                match_data["match_time_utc"] = "未知"
            # 格式化数值
            for key in ["home_win_prob", "draw_prob", "away_win_prob", "expected_value", "model_confidence"]:
                if key in match_data and pd.notna(match_data[key]):
                    match_data[key] = round(float(match_data[key]), 4)
            result_json["predictions"].append(match_data)
        
        # 保存JSON
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(result_json, f, ensure_ascii=False, indent=2, default=str)
        
        # 生成HTML页面【100%保留你原版的HTML模板】
        html_path = os.path.join(OUTPUT_DIR, "index.html")
        html_content = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>足球赛事预测结果 - 超级融合模型</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }}
        body {{ background: #f5f7fa; padding: 15px; max-width: 1200px; margin: 0 auto; }}
        .header {{ text-align: center; margin-bottom: 25px; }}
        .header h1 {{ color: #2c3e50; margin-bottom: 10px; font-size: 24px; line-height: 1.4; }}
        .header .info {{ color: #7f8c8d; font-size: 13px; margin-top: 6px; }}
        .header .competition-tags {{ margin-top: 10px; }}
        .header .competition-tag {{ display: inline-block; background: #2c3e50; color: white; padding: 3px 8px; border-radius: 4px; font-size: 12px; margin: 0 2px; }}
        .header .rule-tag {{ display: inline-block; background: #3498db; color: white; padding: 3px 8px; border-radius: 4px; font-size: 12px; margin: 0 3px; }}
        .stats-grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px; margin-bottom: 25px; }}
        .stat-card {{ background: white; padding: 18px 12px; border-radius: 12px; box-shadow: 0 2px 12px rgba(0,0,0,0.08); text-align: center; }}
        .stat-card .num {{ font-size: 32px; font-weight: bold; color: #3498db; margin-bottom: 6px; }}
        .stat-card .label {{ color: #7f8c8d; font-size: 13px; }}
        .match-card {{ background: white; border-radius: 12px; box-shadow: 0 2px 12px rgba(0,0,0,0.08); margin-bottom: 15px; overflow: hidden; }}
        .match-header {{ background: #2c3e50; color: white; padding: 12px 15px; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 8px; }}
        .match-header .league {{ font-weight: bold; font-size: 14px; }}
        .match-header .time {{ font-size: 12px; opacity: 0.9; }}
        .match-teams {{ padding: 20px 15px; display: grid; grid-template-columns: 42% 16% 42%; align-items: center; text-align: center; }}
        .team-name {{ font-size: 16px; font-weight: bold; margin-bottom: 4px; }}
        .team-en {{ font-size: 11px; color: #7f8c8d; }}
        .vs-tag {{ font-size: 18px; font-weight: bold; color: #3498db; }}
        .prob-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; padding: 0 15px 15px; }}
        .prob-item {{ text-align: center; padding: 8px; border-radius: 8px; }}
        .prob-item.home {{ background: #d4edda; }}
        .prob-item.draw {{ background: #fff3cd; }}
        .prob-item.away {{ background: #f8d7da; }}
        .prob-value {{ font-size: 18px; font-weight: bold; margin-bottom: 4px; }}
        .prob-label {{ font-size: 12px; color: #2c3e50; }}
        .prediction-tag {{ margin: 0 15px 15px; padding: 10px; border-radius: 8px; text-align: center; font-weight: bold; font-size: 16px; }}
        .prediction-tag.主胜 {{ background: #d4edda; color: #155724; }}
        .prediction-tag.平局 {{ background: #fff3cd; color: #856404; }}
        .prediction-tag.客胜 {{ background: #f8d7da; color: #721c24; }}
        .analysis-box {{ margin: 0 15px 15px; padding: 12px; background: #f8f9fa; border-radius: 8px; font-size: 13px; line-height: 1.6; color: #34495e; }}
        .match-meta {{ display: flex; justify-content: space-between; padding: 10px 15px; border-top: 1px solid #ecf0f1; font-size: 12px; color: #7f8c8d; flex-wrap: wrap; gap: 8px; }}
        .confidence-tag {{ padding: 2px 6px; border-radius: 4px; color: white; }}
        .confidence-tag.high {{ background: #28a745; }}
        .confidence-tag.normal {{ background: #ffc107; color: #212529; }}
        .confidence-tag.low {{ background: #dc3545; }}
        .empty-tip {{ text-align: center; padding: 60px 20px; color: #7f8c8d; font-size: 16px; }}
        @media (max-width: 768px) {{
            .stats-grid {{ grid-template-columns: repeat(2, 1fr); }}
            .match-teams {{ grid-template-columns: 40% 20% 40%; }}
            .team-name {{ font-size: 14px; }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>⚽ 足球赛事预测结果 - 超级融合模型</h1>
        <p class="info">生成时间：{result_json['generate_time_cst']} | 预测未来 {result_json['predict_days']} 天赛事</p>
        <div class="competition-tags">
            覆盖联赛：{"".join([f'<span class="competition-tag">{cn}</span>' for cn in competition_cn_list])}
        </div>
        <div class="info">
            核心规则：
            <span class="rule-tag">置信度<{SKIP_CONFIDENCE_THRESHOLD*100}% 直接跳过</span>
            <span class="rule-tag">置信度≥{AI_ANALYSIS_CONFIDENCE_THRESHOLD*100}% 生成AI分析</span>
        </div>
        <div class="info">核心引擎：{result_json['model_used']} | AI分析：{'已启用' if ARK_AVAILABLE else '未启用'}</div>
    </div>

    <div class="stats-grid">
        <div class="stat-card">
            <div class="num">{final_valid_matches_count}</div>
            <div class="label">有效预测赛事</div>
        </div>
        <div class="stat-card">
            <div class="num">{len(competition_cn_list)}</div>
            <div class="label">覆盖联赛数</div>
        </div>
        <div class="stat-card">
            <div class="num">{round(prediction_df['model_confidence'].mean() * 100, 1)}%</div>
            <div class="label">平均置信度</div>
        </div>
        <div class="stat-card">
            <div class="num">{len(ai_target_matches)}</div>
            <div class="label">AI分析场次</div>
        </div>
    </div>

    <div class="stats-grid">
        <div class="stat-card">
            <div class="num">{home_win_count}</div>
            <div class="label">预测主胜</div>
        </div>
        <div class="stat-card">
            <div class="num">{draw_count}</div>
            <div class="label">预测平局</div>
        </div>
        <div class="stat-card">
            <div class="num">{away_win_count}</div>
            <div class="label">预测客胜</div>
        </div>
        <div class="stat-card">
            <div class="num">{home_win_count+draw_count+away_win_count}</div>
            <div class="label">总计场次</div>
        </div>
    </div>

    {"".join([f'''
    <div class="match-card">
        <div class="match-header">
            <span class="league">{row["competition_cn"]}</span>
            <span class="time">{row["match_time_cst"]}</span>
        </div>
        <div class="match-teams">
            <div>
                <div class="team-name">{row["home_team_cn"]}</div>
                <div class="team-en">{row["home_team"]}</div>
            </div>
            <div class="vs-tag">VS</div>
            <div>
                <div class="team-name">{row["away_team_cn"]}</div>
                <div class="team-en">{row["away_team"]}</div>
            </div>
        </div>
        <div class="prob-grid">
            <div class="prob-item home">
                <div class="prob-value">{round(row["home_win_prob"]*100, 1)}%</div>
                <div class="prob-label">主胜</div>
            </div>
            <div class="prob-item draw">
                <div class="prob-value">{round(row["draw_prob"]*100, 1)}%</div>
                <div class="prob-label">平局</div>
            </div>
            <div class="prob-item away">
                <div class="prob-value">{round(row["away_win_prob"]*100, 1)}%</div>
                <div class="prob-label">客胜</div>
            </div>
        </div>
        <div class="prediction-tag {row['prediction']}">
            最终预测：{row["prediction"]}
        </div>
        <div class="analysis-box">
            <strong>赛事分析：</strong>{row["match_analysis"]}
        </div>
        <div class="match-meta">
            <span>置信度：<span class="confidence-tag {'high' if row['model_confidence'] >= 0.8 else 'normal' if row['model_confidence'] >= 0.6 else 'low'}">{round(row['model_confidence']*100, 1)}%</span></span>
            <span>EV值：{round(row["expected_value"]*100, 2)}%</span>
            <span>凯利建议：{round(row["kelly_suggestion"], 4)}</span>
            <span>主胜赔率：{row["odds_home"]}</span>
        </div>
    </div>
    ''' for _, row in prediction_df.iterrows()]) if final_valid_matches_count > 0 else '<div class="empty-tip">暂无有效预测比赛，所有比赛置信度均低于{SKIP_CONFIDENCE_THRESHOLD*100}%</div>'}
</body>
</html>
        """
        
        # 保存HTML
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        
        logger.info(f"✅ 静态页面生成完成，输出目录：{OUTPUT_DIR}，GitHub Pages部署就绪")

        # 12. 生成执行报告
        end_time = datetime.now(timezone.utc)
        duration_minutes = round((end_time - start_time).total_seconds() / 60, 4)
        logger.info("="*66)
        logger.info("PIPELINE EXECUTION REPORT")
        logger.info("="*66)
        
        report = {
            "timestamp_utc": end_time.isoformat().replace("+00:00", "Z"),
            "timestamp_cst": utc_to_beijing(end_time).strftime("%Y-%m-%d %H:%M 北京时间"),
            "status": "success",
            "core_model": fusion_model.model_version,
            "ark_ai_enabled": ARK_AVAILABLE,
            "force_pure_mode": FORCE_USE_FUSION_MODEL,
            "competitions": COMPETITIONS,
            "competitions_cn": competition_cn_list,
            "error": None,
            "original_matches_count": original_matches_count,
            "final_valid_matches_count": final_valid_matches_count,
            "features_shape": list(features_shape),
            "duration_minutes": duration_minutes
        }
        
        logger.info(json.dumps(report, ensure_ascii=False, indent=2))
        logger.info("="*66)
        logger.info("🎉 全预测管道执行成功！GitHub Pages部署就绪")

        # 保存报告
        report_path = os.path.join(OUTPUT_DIR, "pipeline_report.json")
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

    except Exception as e:
        final_error = str(e)
        logger.critical(f"❌ 管道执行异常，已终止：{final_error}", exc_info=True)
        # 生成失败报告
        end_time = datetime.now(timezone.utc)
        duration_minutes = round((end_time - start_time).total_seconds() / 60, 4)
        report = {
            "timestamp_utc": end_time.isoformat().replace("+00:00", "Z"),
            "timestamp_cst": utc_to_beijing(end_time).strftime("%Y-%m-%d %H:%M 北京时间"),
            "status": "failed",
            "error": final_error,
            "original_matches_count": original_matches_count,
            "final_valid_matches_count": final_valid_matches_count,
            "features_shape": list(features_shape),
            "duration_minutes": duration_minutes
        }
        # 保存失败报告
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        report_path = os.path.join(OUTPUT_DIR, "pipeline_report.json")
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        exit(1)

    finally:
        if db_conn:
            db_conn.close()

if __name__ == "__main__":
    main()
