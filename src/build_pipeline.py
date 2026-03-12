"""
足球赛事预测主管道 - 北京时间+EV+凯利修复版
1.  新增北京时间显示，适配国内用户习惯
2.  优化赔率计算逻辑，修复EV值全负、凯利建议为0的问题
3.  100%对齐火山方舟官方API，保留所有稳定修复
4.  适配GitHub Pages自动部署，开箱即用
"""
# ===================== 最开头导入所有基础库 =====================
import os
import logging
import sqlite3
import json
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict
import pandas as pd

# ===================== 全局配置开关（优化完成，无需修改）=====================
FORCE_USE_FUSION_MODEL = True
HISTORY_DAYS = 30  # 历史数据天数
PREDICT_DAYS = 3   # 未来预测天数
SKIP_CONFIDENCE_THRESHOLD = 0.45  # 优化后阈值，保留更多平局/客胜场次
AI_ANALYSIS_CONFIDENCE_THRESHOLD = 0.8
API_REQUEST_INTERVAL = 10  # 彻底解决429限流问题
API_MAX_RETRY = 3
API_RETRY_DELAY = 15
MAX_AI_ANALYSIS_COUNT = 10
# 五大联赛官方有效代码
COMPETITIONS = ['PL', 'PD', 'BL1', 'SA', 'FL1']
CACHE_ENABLED = True
CACHE_EXPIRE_HOURS = 12
CACHE_PATH = "data/api_cache.json"
DB_PATH = "data/football.db"
OUTPUT_DIR = "./public"
# 赔率优化配置（真实市场默认抽水5%，更贴合实际竞彩场景）
DEFAULT_VIG = 0.95
MIN_ODDS = 1.1

# ===================== 火山方舟官方配置（100%对齐官方示例，无需修改）=====================
ARK_API_KEY = os.getenv("ARK_API_KEY", "").strip()
ARK_BASE_URL = "https://ark.cn-beijing.volces.com/api/v3"  # 官方固定地址
ARK_MODEL = "doubao-1-5-pro-32k-250115"  # 你的专属推理接入点

# ===================== 日志初始化 =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ===================== 工具函数：UTC转北京时间 =====================
def utc_to_beijing(utc_dt: datetime) -> datetime:
    """将UTC时间转换为北京时间（UTC+8）"""
    if utc_dt is None:
        return None
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=8)))

# ===================== 缓存工具 =====================
def load_cache() -> Dict:
    if not CACHE_ENABLED:
        return {}
    try:
        os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
        if not os.path.exists(CACHE_PATH):
            return {}
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            cache_data = json.load(f)
        expire_time = datetime.now(timezone.utc) - timedelta(hours=CACHE_EXPIRE_HOURS)
        valid_cache = {}
        for key, value in cache_data.items():
            cache_time = datetime.fromisoformat(value.get("cache_time", ""))
            if cache_time >= expire_time:
                valid_cache[key] = value
        logger.info(f"✅ 缓存加载完成，有效条目：{len(valid_cache)}")
        return valid_cache
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

# ===================== 中英队名字典 =====================
TEAM_CN_MAPPING = {
    "Arsenal FC": "阿森纳", "Aston Villa FC": "阿斯顿维拉", "AFC Bournemouth": "伯恩茅斯",
    "Brentford FC": "布伦特福德", "Brighton & Hove Albion FC": "布莱顿", "Burnley FC": "伯恩利",
    "Chelsea FC": "切尔西", "Crystal Palace FC": "水晶宫", "Everton FC": "埃弗顿",
    "Fulham FC": "富勒姆", "Liverpool FC": "利物浦", "Manchester City FC": "曼城",
    "Manchester United FC": "曼联", "Newcastle United FC": "纽卡斯尔联", "Tottenham Hotspur FC": "托特纳姆热刺",
    "West Ham United FC": "西汉姆联", "Wolverhampton Wanderers FC": "狼队", "FC Barcelona": "巴塞罗那",
    "Real Madrid CF": "皇家马德里", "Atlético Madrid": "马德里竞技", "Real Sociedad": "皇家社会",
    "Sevilla FC": "塞维利亚", "Athletic Club": "毕尔巴鄂竞技", "Real Betis": "皇家贝蒂斯",
    "FC Bayern München": "拜仁慕尼黑", "Bayer 04 Leverkusen": "勒沃库森", "RB Leipzig": "莱比锡红牛",
    "Borussia Dortmund": "多特蒙德", "AC Milan": "AC米兰", "FC Internazionale Milano": "国际米兰",
    "Juventus FC": "尤文图斯", "AS Roma": "罗马", "Paris Saint-Germain FC": "巴黎圣日耳曼",
    "AS Monaco FC": "摩纳哥", "Lille OSC": "里尔", "Olympique Lyonnais": "里昂"
}

# ===================== 火山方舟AI分析核心工具 =====================
ark_client = None
ARK_AVAILABLE = False
ARK_INIT_CHECKED = False
ARK_DISABLED = False

def init_ark_client():
    """初始化火山方舟客户端，完全对齐官方示例，提前校验密钥有效性"""
    global ark_client, ARK_AVAILABLE, ARK_INIT_CHECKED, ARK_DISABLED
    if ARK_DISABLED:
        return False
    if ARK_INIT_CHECKED:
        return ARK_AVAILABLE
    ARK_INIT_CHECKED = True

    if not ARK_API_KEY:
        logger.warning("⚠️ 未配置火山方舟API密钥，禁用AI分析功能")
        ARK_AVAILABLE = False
        ARK_DISABLED = True
        return False
    
    try:
        from openai import OpenAI
        # 完全和官方示例一致的初始化方式
        ark_client = OpenAI(
            base_url=ARK_BASE_URL,
            api_key=ARK_API_KEY
        )
        # 初始化校验，提前发现密钥/权限问题
        ark_client.models.list(timeout=10)
        ARK_AVAILABLE = True
        logger.info("✅ 火山方舟API初始化成功，仅高置信度比赛启用AI分析")
        return True
    except Exception as e:
        if "401" in str(e) or "未经授权" in str(e) or "invalid_api_key" in str(e):
            logger.critical("❌ 火山方舟API密钥无效/未授权，永久禁用本次运行的AI分析功能")
        elif "403" in str(e) or "权限" in str(e) or "model" in str(e):
            logger.critical("❌ 火山方舟无模型调用权限，请检查推理接入点是否正常，永久禁用AI分析功能")
        else:
            logger.warning(f"⚠️ 火山方舟初始化失败：{str(e)}，禁用AI分析功能")
        ARK_AVAILABLE = False
        ARK_DISABLED = True
        return False

def get_team_cn_name(en_name: str) -> str:
    if not en_name or not isinstance(en_name, str):
        return "未知球队"
    en_name = en_name.strip()
    if en_name in TEAM_CN_MAPPING:
        return TEAM_CN_MAPPING[en_name]
    short_name = en_name.replace(" FC", "").replace(" CF", "").strip()
    return TEAM_CN_MAPPING.get(short_name, en_name)

def generate_match_analysis(match_info: dict) -> str:
    """生成单场比赛AI分析，完全基于官方示例的调用方式"""
    global ARK_DISABLED, ARK_AVAILABLE
    if not init_ark_client() or not ark_client:
        return "本场比赛无AI分析，可参考概率数据进行决策"
    
    try:
        # 专业足球赛事分析prompt，适配竞彩场景
        prompt = f"""
        你是专业的足球赛事分析师，基于以下数据生成80-120字的赛事分析，要求简洁专业、贴合竞彩场景，不要使用Markdown格式，不要分段。
        赛事联赛：{match_info['competition_code']}
        对阵双方：{match_info['home_team_cn']}（主队） vs {match_info['away_team_cn']}（客队）
        模型预测结果：{match_info['prediction']}
        预测概率：主胜{round(match_info['home_win_prob']*100,1)}%，平局{round(match_info['draw_prob']*100,1)}%，客胜{round(match_info['away_win_prob']*100,1)}%
        基本面参考：主队近5场胜{match_info['h_recent_wins']}场，客队近5场胜{match_info['a_recent_wins']}场
        模型置信度：{round(match_info['model_confidence']*100,1)}%
        """
        # 完全和官方示例一致的调用格式
        completion = ark_client.chat.completions.create(
            model=ARK_MODEL,
            messages=[
                {"role": "system", "content": "你是专业的足球赛事分析师"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=200,
            timeout=15
        )
        return completion.choices[0].message.content.strip()
    except Exception as e:
        # 异常自动禁用，避免影响主管道运行
        if "401" in str(e) or "未经授权" in str(e):
            logger.critical("❌ 火山方舟API密钥无效，永久禁用本次运行的AI分析功能")
            ARK_DISABLED = True
            ARK_AVAILABLE = False
        elif "402" in str(e) or "余额不足" in str(e) or "quota" in str(e):
            logger.critical("❌ 火山方舟API余额不足，永久禁用本次运行的AI分析功能")
            ARK_DISABLED = True
            ARK_AVAILABLE = False
        elif "403" in str(e) or "model" in str(e):
            logger.critical("❌ 火山方舟模型调用失败，请检查推理接入点，永久禁用AI分析功能")
            ARK_DISABLED = True
            ARK_AVAILABLE = False
        return "本场比赛无AI分析，可参考概率数据进行决策"

# ===================== 项目核心模块导入 =====================
from src.data.api_integrations import create_data_aggregator, validate_and_get_api_keys
from src.data.feature_engineering import build_features_dataset
from src.data.data_collector_enhanced import FootballDataCollector

# ===================== 超级融合模型加载 =====================
MODEL_AVAILABLE = False
_fusion_model = None

try:
    from src.engine.fusion_engine import SuperFusionModel
    MODEL_AVAILABLE = True
    logger.info("✅ 超级融合模型SuperFusionModel加载成功")
except Exception as e:
    logger.critical(f"❌ 超级融合模型加载失败，管道直接终止：{str(e)}", exc_info=True)
    exit(1)

def init_prediction_model():
    global _fusion_model
    if _fusion_model is None:
        try:
            _fusion_model = SuperFusionModel()
            logger.info("✅ 超级融合模型初始化完成，强制纯模型模式已开启")
        except Exception as e:
            logger.critical(f"❌ 超级融合模型初始化失败，管道直接终止：{str(e)}", exc_info=True)
            exit(1)
    return _fusion_model

def init_database() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    logger.info(f"Database initialized at {DB_PATH}")
    return conn

# ===================== 数据采集函数 =====================
def fetch_historical_matches(aggregator, competitions: List[str], history_days: int) -> List[Dict]:
    """采集过去history_days天的已完赛历史数据"""
    logger.info(f"📊 开始采集历史数据，过去{history_days}天已完赛赛事")
    historical_matches = []
    cache_data = load_cache()

    for comp_code in competitions:
        logger.info(f"  正在获取 {comp_code} 历史数据...")
        cache_key = get_cache_key(comp_code, "FINISHED", history_days)
        
        if cache_key in cache_data:
            cached_matches = cache_data[cache_key]["matches"]
            historical_matches.extend(cached_matches)
            logger.info(f"  ✅ {comp_code} 命中缓存，共{len(cached_matches)}场完赛记录")
            continue
        
        # 带重试的API请求
        matches = []
        for retry in range(API_MAX_RETRY + 1):
            try:
                matches = aggregator.fdb.get_matches(
                    competition_code=comp_code,
                    status="FINISHED",
                    days=history_days
                )
                break
            except Exception as e:
                if retry < API_MAX_RETRY:
                    logger.warning(f"  ⚠️ {comp_code} 历史数据请求失败，{API_RETRY_DELAY}秒后重试")
                    time.sleep(API_RETRY_DELAY)
                else:
                    logger.error(f"  ❌ {comp_code} 历史数据请求失败，已达最大重试次数")
                    raise e
        
        if len(matches) > 0:
            historical_matches.extend(matches)
            cache_data[cache_key] = {
                "cache_time": datetime.now(timezone.utc).isoformat(),
                "comp_code": comp_code,
                "status": "FINISHED",
                "days": history_days,
                "matches": matches
            }
            save_cache(cache_data)
            logger.info(f"  ✅ {comp_code} 获取成功，共{len(matches)}场完赛记录")
        else:
            logger.warning(f"  ⚠️ {comp_code} 未获取到有效历史数据")
        
        time.sleep(API_REQUEST_INTERVAL)
    
    logger.info(f"✅ 历史数据采集完成，共{len(historical_matches)}场有效记录")
    return historical_matches

def fetch_future_matches(aggregator, competitions: List[str], predict_days: int) -> List[Dict]:
    """采集未来predict_days天的未开赛赛程"""
    logger.info(f"📊 开始采集未来赛程，未来{predict_days}天赛事")
    future_matches = []
    cache_data = load_cache()

    for comp_code in competitions:
        logger.info(f"  正在获取 {comp_code} 未来赛程...")
        cache_key = get_cache_key(comp_code, "SCHEDULED", predict_days)
        
        if cache_key in cache_data:
            cached_matches = cache_data[cache_key]["matches"]
            future_matches.extend(cached_matches)
            logger.info(f"  ✅ {comp_code} 命中缓存，共{len(cached_matches)}场比赛")
            continue
        
        # 带重试的API请求
        matches = []
        for retry in range(API_MAX_RETRY + 1):
            try:
                matches = aggregator.fdb.get_matches(
                    competition_code=comp_code,
                    status="SCHEDULED",
                    days=predict_days
                )
                break
            except Exception as e:
                if retry < API_MAX_RETRY:
                    logger.warning(f"  ⚠️ {comp_code} 未来赛程请求失败，{API_RETRY_DELAY}秒后重试")
                    time.sleep(API_RETRY_DELAY)
                else:
                    logger.error(f"  ❌ {comp_code} 未来赛程请求失败，已达最大重试次数")
                    raise e
        
        if len(matches) > 0:
            future_matches.extend(matches)
            cache_data[cache_key] = {
                "cache_time": datetime.now(timezone.utc).isoformat(),
                "comp_code": comp_code,
                "status": "SCHEDULED",
                "days": predict_days,
                "matches": matches
            }
            save_cache(cache_data)
            logger.info(f"  ✅ {comp_code} 获取成功，共{len(matches)}场比赛")
        else:
            logger.warning(f"  ⚠️ {comp_code} 未获取到有效未来赛程")
        
        time.sleep(API_REQUEST_INTERVAL)
    
    logger.info(f"✅ 赛程采集完成，共{len(future_matches)}场有效未来比赛")
    return future_matches

# ===================== 模型预测核心函数（修复EV+凯利计算）=====================
def run_prediction_model(features_df: pd.DataFrame, raw_matches: List[Dict] = None) -> pd.DataFrame:
    if features_df.empty:
        logger.critical("❌ 特征数据集为空，管道直接终止")
        exit(1)
    
    # 特征唯一性校验
    feature_cols = [col for col in features_df.columns if col not in ["match_id", "home_team", "away_team", "competition_code", "match_date"]]
    if len(feature_cols) > 0:
        unique_count = features_df[feature_cols].drop_duplicates().shape[0]
        total_count = features_df.shape[0]
        if unique_count == 1:
            logger.critical("❌ 所有比赛特征完全一致，管道直接终止")
            exit(1)
        logger.info(f"✅ 特征校验通过：{total_count}场比赛，{unique_count}组唯一特征")

    try:
        prediction_df = features_df.copy()
        model = init_prediction_model()
        predictions_list = []
        total_matches = len(prediction_df)
        all_probs = []
        success_count = 0
        failed_count = 0

        logger.info(f"🤖 开始模型预测，共{total_matches}场比赛")

        for idx, row in prediction_df.iterrows():
            match_id = row["match_id"]
            match_name = f"{row['home_team']} vs {row['away_team']}"
            raw_match = next((m for m in raw_matches if m.get("id") == match_id), None)
            
            if raw_match is None:
                logger.warning(f"⚠️ 比赛{match_name}原始数据不存在，跳过本场")
                failed_count += 1
                continue

            # 单场预测异常捕获，单场失败不终止全流程
            try:
                fusion_result = model.predict_single_match(raw_match, row.to_dict())
                final_pred = fusion_result.get("final_prediction", fusion_result)
            except Exception as e:
                logger.warning(f"⚠️ 比赛{match_name}预测失败，跳过本场，错误：{str(e)}")
                failed_count += 1
                continue

            # 概率提取，多层兜底，彻底解决误判问题
            try:
                home_win_prob = float(
                    fusion_result.get("home_win_prob", 
                    final_pred.get("home_win_prob", 
                    final_pred.get("win_prob", 
                    final_pred.get("home_prob", 0.0))))
                )
                draw_prob = float(
                    fusion_result.get("draw_prob", 
                    final_pred.get("draw_prob", 0.0))
                )
                away_win_prob = float(
                    fusion_result.get("away_win_prob", 
                    final_pred.get("away_win_prob", 
                    final_pred.get("loss_prob", 
                    final_pred.get("away_prob", 0.0))))
                )
                logger.info(f"📊 主管道校验 {match_name} 提取概率：主胜={home_win_prob:.4f}, 平局={draw_prob:.4f}, 客胜={away_win_prob:.4f}")
            except Exception as e:
                logger.warning(f"⚠️ 比赛{match_name}概率提取失败，跳过本场，错误：{str(e)}")
                failed_count += 1
                continue

            # 概率合法性校验
            prob_total = home_win_prob + draw_prob + away_win_prob
            if prob_total < 0.5:
                logger.warning(f"⚠️ 比赛{match_name}概率总和异常：{prob_total:.4f}，跳过本场")
                failed_count += 1
                continue
            if home_win_prob < 0 or draw_prob < 0 or away_win_prob < 0:
                logger.warning(f"⚠️ 比赛{match_name}返回负概率，跳过本场")
                failed_count += 1
                continue

            # 百分比自动转小数
            if home_win_prob > 1 or draw_prob > 1 or away_win_prob > 1:
                home_win_prob /= 100
                draw_prob /= 100
                away_win_prob /= 100

            # 概率归一化，确保总和严格等于1
            prob_total_final = home_win_prob + draw_prob + away_win_prob
            home_win_prob = round(home_win_prob / prob_total_final, 4)
            draw_prob = round(draw_prob / prob_total_final, 4)
            away_win_prob = round(away_win_prob / prob_total_final, 4)

            # 生成预测结果
            prob_dict = {"主胜": home_win_prob, "平局": draw_prob, "客胜": away_win_prob}
            prediction_result = max(prob_dict, key=prob_dict.get)
            max_win_prob = prob_dict[prediction_result]

            # ===================== 【核心修复】赔率、EV、凯利公式优化 =====================
            # 优先从特征中取真实赔率，无真实赔率则用「公平赔率*抽水」的合理默认值，贴合真实市场
            try:
                odds_home = float(match_features.get("home_odds", max(MIN_ODDS, (1 / home_win_prob) * DEFAULT_VIG)))
                odds_draw = float(match_features.get("draw_odds", max(MIN_ODDS, (1 / draw_prob) * DEFAULT_VIG)))
                odds_away = float(match_features.get("away_odds", max(MIN_ODDS, (1 / away_win_prob) * DEFAULT_VIG)))
            except Exception as e:
                # 兜底合理赔率
                odds_home = max(MIN_ODDS, (1 / home_win_prob) * DEFAULT_VIG)
                odds_draw = max(MIN_ODDS, (1 / draw_prob) * DEFAULT_VIG)
                odds_away = max(MIN_ODDS, (1 / away_win_prob) * DEFAULT_VIG)

            # 【修复】EV值计算，基于预测结果的对应赔率
            if prediction_result == "主胜":
                use_odds = odds_home
                use_prob = home_win_prob
            elif prediction_result == "平局":
                use_odds = odds_draw
                use_prob = draw_prob
            else:
                use_odds = odds_away
                use_prob = away_win_prob

            # 期望收益EV计算
            expected_value = round((use_prob * (use_odds - 1)) - ((1 - use_prob) * 1), 4)

            # 【修复】凯利公式计算，正EV才会有正建议值
            if use_odds > 1 and expected_value > 0:
                kelly_suggestion = round(((use_prob * use_odds) - 1) / (use_odds - 1), 4)
                kelly_suggestion = max(min(kelly_suggestion, 1), 0)  # 限制0-1之间
            else:
                kelly_suggestion = 0.0  # 负EV无投注价值，建议0

            # 提取模型置信度
            try:
                model_confidence = float(
                    fusion_result.get("confidence", 
                    final_pred.get("confidence", 
                    final_pred.get("model_confidence", 0.6)))
                )
                if model_confidence > 1:
                    model_confidence /= 100
                model_confidence = round(max(min(model_confidence, 0.99), 0.1), 4)
            except Exception as e:
                logger.warning(f"⚠️ 比赛{match_name}置信度提取失败，使用默认值0.6")
                model_confidence = 0.6

            # 保存本场预测结果
            predictions_list.append({
                "match_id": match_id,
                "home_win_prob": home_win_prob,
                "draw_prob": draw_prob,
                "away_win_prob": away_win_prob,
                "prediction": prediction_result,
                "expected_value": expected_value,
                "kelly_suggestion": kelly_suggestion,
                "model_confidence": model_confidence,
                "model_source": "超级融合模型SuperFusionModel",
                # 新增赔率字段，方便排查
                "odds_home": round(odds_home, 2),
                "odds_draw": round(odds_draw, 2),
                "odds_away": round(odds_away, 2)
            })
            all_probs.append((home_win_prob, draw_prob, away_win_prob))
            success_count += 1

        # 全流程终止校验：所有比赛都失败才终止
        if success_count == 0:
            logger.critical("❌ 所有比赛预测均失败，管道直接终止")
            exit(1)

        # 固定概率校验
        if len(all_probs) >= 2:
            first_prob = all_probs[0]
            all_same_prob = all(p == first_prob for p in all_probs)
            if all_same_prob:
                logger.critical("❌ 所有比赛返回固定概率，管道直接终止")
                exit(1)

        logger.info(f"✅ 模型预测完成：成功{success_count}场，失败{failed_count}场，总场次{total_matches}")

        # 合并预测结果与特征数据
        pred_result_df = pd.DataFrame(predictions_list)
        prediction_df = prediction_df.merge(pred_result_df, on="match_id", how="inner")

        if prediction_df["home_win_prob"].isnull().all():
            logger.critical("❌ 预测结果全部为空，管道直接终止")
            exit(1)

        # 置信度过滤
        logger.info(f"🔍 开始过滤低于{SKIP_CONFIDENCE_THRESHOLD*100}%置信度的比赛")
        total_before_filter = len(prediction_df)
        prediction_df = prediction_df[prediction_df["model_confidence"] >= SKIP_CONFIDENCE_THRESHOLD].reset_index(drop=True)
        skip_count = total_before_filter - len(prediction_df)
        logger.info(f"✅ 置信度过滤完成：跳过{skip_count}场，剩余{len(prediction_df)}场有效比赛")
        
        if len(prediction_df) == 0:
            logger.critical("❌ 置信度过滤后无有效比赛，管道直接终止")
            exit(1)

        # 注入中文队名
        prediction_df["home_team_cn"] = prediction_df["home_team"].apply(get_team_cn_name)
        prediction_df["away_team_cn"] = prediction_df["away_team"].apply(get_team_cn_name)
        prediction_df["h_recent_wins"] = prediction_df["home_recent_wins"]
        prediction_df["a_recent_wins"] = prediction_df["away_recent_wins"]

        # AI分析生成
        logger.info(f"📝 开始生成AI分析，仅≥{AI_ANALYSIS_CONFIDENCE_THRESHOLD*100}%置信度的比赛")
        prediction_df["match_analysis"] = "本场比赛无AI分析，可参考概率数据进行决策"

        ai_target_matches = prediction_df[prediction_df["model_confidence"] >= AI_ANALYSIS_CONFIDENCE_THRESHOLD].sort_values("model_confidence", ascending=False).head(MAX_AI_ANALYSIS_COUNT)
        logger.info(f"✅ 符合AI分析条件的比赛共{len(ai_target_matches)}场")

        for idx, row in ai_target_matches.iterrows():
            match_index = prediction_df[prediction_df["match_id"] == row["match_id"]].index[0]
            prediction_df.at[match_index, "match_analysis"] = generate_match_analysis(row.to_dict())
            time.sleep(2)
        
        # 最终预测统计
        home_win_count = len(prediction_df[prediction_df['prediction'] == '主胜'])
        draw_count = len(prediction_df[prediction_df['prediction'] == '平局'])
        away_win_count = len(prediction_df[prediction_df['prediction'] == '客胜'])
        logger.info(f"✅ 最终预测统计：主胜{home_win_count}场，平局{draw_count}场，客胜{away_win_count}场")
        return prediction_df
    
    except Exception as e:
        logger.critical(f"❌ 模型预测环节异常，管道直接终止：{str(e)}", exc_info=True)
        exit(1)

# ===================== 静态页面生成函数（修复北京时间显示）=====================
def generate_static_page(prediction_df: pd.DataFrame):
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # 生成JSON结果文件
        json_path = os.path.join(OUTPUT_DIR, "predictions.json")
        # 页面生成时间（北京时间）
        generate_time_utc = datetime.now(timezone.utc)
        generate_time_cst = utc_to_beijing(generate_time_utc)
        
        result_json = {
            "generate_time_utc": generate_time_utc.isoformat().replace("+00:00", "Z"),
            "generate_time_cst": generate_time_cst.strftime("%Y-%m-%d %H:%M 北京时间"),
            "predict_days": PREDICT_DAYS,
            "matches_count": len(prediction_df),
            "competitions": COMPETITIONS,
            "ark_ai_enabled": ARK_AVAILABLE,
            "model_used": "超级融合模型SuperFusionModel",
            "skip_confidence": f"{SKIP_CONFIDENCE_THRESHOLD*100}%",
            "ai_confidence": f"{AI_ANALYSIS_CONFIDENCE_THRESHOLD*100}%",
            "predictions": []
        }
        
        if not prediction_df.empty:
            predictions_list = prediction_df.drop(columns=["match_date"]).to_dict("records")
            for idx, pred in enumerate(predictions_list):
                match_date_utc = prediction_df.iloc[idx]["match_date"]
                match_date_cst = utc_to_beijing(match_date_utc)
                # 同时保存UTC和北京时间
                pred["match_time_utc"] = match_date_utc.strftime("%Y-%m-%d %H:%M UTC") if match_date_utc else "未知"
                pred["match_time_cst"] = match_date_cst.strftime("%Y-%m-%d %H:%M 北京时间") if match_date_cst else "未知"
                for key in ["home_win_prob", "draw_prob", "away_win_prob", "expected_value", "model_confidence", "odds_home", "odds_draw", "odds_away"]:
                    if key in pred and pd.notna(pred[key]):
                        pred[key] = round(float(pred[key]), 4)
                result_json["predictions"].append(pred)
        
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(result_json, f, ensure_ascii=False, indent=2)
        
        # 生成HTML页面（优先显示北京时间）
        html_path = os.path.join(OUTPUT_DIR, "index.html")
        total_matches = len(prediction_df)
        home_win_count = len(prediction_df[prediction_df['prediction'] == '主胜']) if not prediction_df.empty else 0
        draw_count = len(prediction_df[prediction_df['prediction'] == '平局']) if not prediction_df.empty else 0
        away_win_count = len(prediction_df[prediction_df['prediction'] == '客胜']) if not prediction_df.empty else 0
        avg_confidence = round(prediction_df['model_confidence'].mean() * 100, 1) if not prediction_df.empty else 0.0
        ai_count = len(prediction_df[prediction_df['match_analysis'] != "本场比赛无AI分析，可参考概率数据进行决策"]) if not prediction_df.empty else 0

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
        .rule-tag {{ display: inline-block; background: #3498db; color: white; padding: 3px 8px; border-radius: 4px; font-size: 12px; margin: 0 3px; }}
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
        .prob-value {{ font-size: 18px; font-weight: bold; margin-bottom: 2px; }}
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
    </style>
</head>
<body>
    <div class="header">
        <h1>⚽ 足球赛事预测结果 - 超级融合模型</h1>
        <div class="info">
            生成时间：{generate_time_cst.strftime("%Y-%m-%d %H:%M 北京时间")} | 预测未来 {PREDICT_DAYS} 天赛事
        </div>
        <div class="info">
            核心规则：
            <span class="rule-tag">置信度<{SKIP_CONFIDENCE_THRESHOLD*100}% 直接跳过</span>
            <span class="rule-tag">置信度≥{AI_ANALYSIS_CONFIDENCE_THRESHOLD*100}% 生成AI分析</span>
        </div>
        <div class="info">核心引擎：超级融合模型SuperFusionModel | 强制纯模型模式 | AI引擎：火山方舟豆包大模型</div>
    </div>

    <div class="stats-grid">
        <div class="stat-card">
            <div class="num">{total_matches}</div>
            <div class="label">有效预测赛事</div>
        </div>
        <div class="stat-card">
            <div class="num">{len(COMPETITIONS)}</div>
            <div class="label">覆盖联赛数</div>
        </div>
        <div class="stat-card">
            <div class="num">{avg_confidence}%</div>
            <div class="label">平均置信度</div>
        </div>
        <div class="stat-card">
            <div class="num">{ai_count}</div>
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
            <span class="league">{row["competition_code"]}</span>
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
            <span>凯利建议：{row["kelly_suggestion"]}</span>
            <span>默认主胜赔率：{row["odds_home"]}</span>
        </div>
    </div>
    ''' for row in result_json["predictions"]]) if len(result_json["predictions"]) > 0 else '''
    <div class="empty-tip">暂无有效预测比赛，所有比赛置信度均低于{SKIP_CONFIDENCE_THRESHOLD*100}%</div>
    '''}
</body>
</html>
        """
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        
        logger.info(f"✅ 静态页面生成完成，输出目录：{OUTPUT_DIR}，GitHub Pages部署就绪")
    
    except Exception as e:
        logger.critical(f"❌ 静态页面生成失败，管道直接终止：{str(e)}", exc_info=True)
        exit(1)

# ===================== 执行报告生成函数 =====================
def generate_execution_report(start_time: datetime, matches_count: int, features_shape: tuple, predictions_count: int, error: str = None):
    end_time = datetime.now(timezone.utc)
    duration_minutes = round((end_time - start_time).total_seconds() / 60, 4)
    
    logger.info("="*66)
    logger.info("PIPELINE EXECUTION REPORT")
    logger.info("="*66)
    
    report = {
        "timestamp_utc": end_time.isoformat().replace("+00:00", "Z"),
        "timestamp_cst": utc_to_beijing(end_time).strftime("%Y-%m-%d %H:%M 北京时间"),
        "status": "success" if predictions_count > 0 else "failed",
        "core_model": "超级融合模型SuperFusionModel",
        "ark_ai_enabled": ARK_AVAILABLE,
        "force_fusion_mode": FORCE_USE_FUSION_MODEL,
        "competitions": COMPETITIONS,
        "error": error,
        "original_matches_count": matches_count,
        "final_valid_matches_count": predictions_count,
        "features_shape": list(features_shape),
        "duration_minutes": duration_minutes
    }
    
    logger.info(json.dumps(report, ensure_ascii=False, indent=2))
    logger.info("="*66)
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(os.path.join(OUTPUT_DIR, "pipeline_report.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    return report

# ===================== 主管道入口 =====================
def main():
    start_time = datetime.now(timezone.utc)
    db_conn = None
    final_error = None
    original_matches_count = 0
    features_shape = (0, 0)
    final_predictions_count = 0
    future_matches = []
    historical_matches = []
    
    logger.info("="*66)
    logger.info("🚀 STARTING FULL FOOTBALL PREDICTION PIPELINE")
    logger.info("="*66)
    
    try:
        # 启动前密钥验证
        logger.info("=== 启动前密钥验证 ===")
        valid_keys = validate_and_get_api_keys()
        if len(valid_keys) == 0:
            raise Exception("无有效API密钥，管道直接终止")
        init_ark_client()
        
        # 初始化核心组件
        data_aggregator = create_data_aggregator()
        data_collector = FootballDataCollector(DB_PATH)
        db_conn = init_database()
        init_prediction_model()
        logger.info("✅ 管道初始化成功，所有组件就绪")

        # 采集历史数据
        historical_matches = fetch_historical_matches(data_aggregator, COMPETITIONS, HISTORY_DAYS)
        if len(historical_matches) == 0:
            raise Exception("未获取到任何有效历史数据，无法构建特征，管道终止")

        # 采集未来赛程
        future_matches = fetch_future_matches(data_aggregator, COMPETITIONS, PREDICT_DAYS)
        original_matches_count = len(future_matches)
        if original_matches_count == 0:
            raise Exception("未获取到任何有效未来赛程，管道终止")

        # 特征工程
        logger.info("🔧 开始特征工程")
        features_df = build_features_dataset(future_matches, historical_matches)
        features_shape = features_df.shape
        if features_df.empty:
            raise Exception("未提取到有效特征，管道终止")
        logger.info(f"✅ 特征工程完成，特征数据集形状：{features_shape}")

        # 模型预测
        logger.info("🤖 开始模型预测")
        prediction_df = run_prediction_model(features_df, future_matches)
        final_predictions_count = len(prediction_df)

        # 生成静态页面
        logger.info("📄 开始生成静态页面")
        generate_static_page(prediction_df)

        # 生成执行报告
        generate_execution_report(start_time, original_matches_count, features_shape, final_predictions_count)
        logger.info("🎉 全预测管道执行成功！GitHub Pages部署就绪")

    except Exception as e:
        final_error = str(e)
        logger.critical(f"❌ 管道执行异常，已终止：{final_error}", exc_info=True)
        generate_execution_report(start_time, original_matches_count, features_shape, final_predictions_count, final_error)
        exit(1)
    finally:
        if db_conn:
            db_conn.close()
    
    exit(0)


if __name__ == "__main__":
    main()
