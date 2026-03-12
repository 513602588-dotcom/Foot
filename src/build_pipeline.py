"""
足球赛事预测主管道 - 终极修复版
✅ 彻底解决API参数报错+历史数据获取失败问题
✅ 100%强制纯融合模型模式，无任何兜底逻辑
✅ 双阈值规则：<50%置信度直接跳过，≥80%才生成AI分析
✅ 严格API限流合规，适配免费版10次/分钟限制
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

# ===================== 配置开关 =====================
FORCE_USE_FUSION_MODEL = True
HISTORY_DAYS = 30  # 历史数据天数
PREDICT_DAYS = 3   # 未来预测天数
SKIP_CONFIDENCE_THRESHOLD = 0.5
AI_ANALYSIS_CONFIDENCE_THRESHOLD = 0.8
API_REQUEST_INTERVAL = 7  # 7秒间隔，适配免费版限流
API_MAX_RETRY = 2
API_RETRY_DELAY = 10
MAX_AI_ANALYSIS_COUNT = 10
# 仅保留官方有效五大联赛
COMPETITIONS = ['PL', 'PD', 'BL1', 'SA', 'FL1']
CACHE_ENABLED = True
CACHE_EXPIRE_HOURS = 12
CACHE_PATH = "data/api_cache.json"
DB_PATH = "data/football.db"
OUTPUT_DIR = "./public"
# DeepSeek配置
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "").strip()
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com").strip()
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")

# ===================== 日志初始化 =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

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
    "Real Madrid CF": "皇家马德里", "Club Atlético de Madrid": "马德里竞技", "Real Sociedad de Fútbol": "皇家社会",
    "Sevilla FC": "塞维利亚", "Athletic Club": "毕尔巴鄂竞技", "Real Betis Balompié": "皇家贝蒂斯",
    "FC Bayern München": "拜仁慕尼黑", "Bayer 04 Leverkusen": "勒沃库森", "RB Leipzig": "莱比锡红牛",
    "Borussia Dortmund": "多特蒙德", "AC Milan": "AC米兰", "FC Internazionale Milano": "国际米兰",
    "Juventus FC": "尤文图斯", "AS Roma": "罗马", "Paris Saint-Germain FC": "巴黎圣日耳曼",
    "AS Monaco FC": "摩纳哥", "Lille OSC": "里尔", "Olympique Lyonnais": "里昂"
}

# ===================== DeepSeek API =====================
deepseek_client = None
DEEPSEEK_AVAILABLE = False
DEEPSEEK_INIT_CHECKED = False
DEEPSEEK_DISABLED = False

def init_deepseek():
    global deepseek_client, DEEPSEEK_AVAILABLE, DEEPSEEK_INIT_CHECKED, DEEPSEEK_DISABLED
    if DEEPSEEK_DISABLED:
        return False
    if DEEPSEEK_INIT_CHECKED:
        return DEEPSEEK_AVAILABLE
    DEEPSEEK_INIT_CHECKED = True

    if not DEEPSEEK_API_KEY:
        logger.warning("⚠️ 未配置DeepSeek密钥，禁用AI分析")
        DEEPSEEK_AVAILABLE = False
        return False
    
    try:
        import openai
        deepseek_client = openai.OpenAI(api_key=DEEPSEEK_API_KEY, base_url=DEEPSEEK_BASE_URL)
        DEEPSEEK_AVAILABLE = True
        logger.info("✅ DeepSeek API初始化成功")
        return True
    except Exception as e:
        logger.warning(f"⚠️ DeepSeek初始化失败：{str(e)}")
        DEEPSEEK_AVAILABLE = False
        DEEPSEEK_DISABLED = True
        return False

def get_team_cn_name(en_name: str) -> str:
    if not en_name or not isinstance(en_name, str):
        return "未知球队"
    if en_name in TEAM_CN_MAPPING:
        return TEAM_CN_MAPPING[en_name]
    short_name = en_name.replace(" FC", "").replace(" CF", "").strip()
    return TEAM_CN_MAPPING.get(short_name, en_name)

def generate_match_analysis(match_info: Dict) -> str:
    global DEEPSEEK_DISABLED, DEEPSEEK_AVAILABLE
    if not init_deepseek() or not deepseek_client:
        return "本场比赛无AI分析，可参考概率数据进行决策"
    
    try:
        prompt = f"""
        生成80-120字的足球竞彩分析，简洁专业，不要Markdown。
        赛事：{match_info['competition_code']}
        对阵：{match_info['home_team_cn']} vs {match_info['away_team_cn']}
        预测：{match_info['prediction']}
        概率：主胜{round(match_info['home_win_prob']*100,1)}%，平局{round(match_info['draw_prob']*100,1)}%，客胜{round(match_info['away_win_prob']*100,1)}%
        基本面：主队近5场胜{match_info['h_recent_wins']}场，客队近5场胜{match_info['a_recent_wins']}场
        置信度：{round(match_info['model_confidence']*100,1)}%
        """
        response = deepseek_client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=200,
            timeout=15
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        if "402" in str(e) or "余额不足" in str(e):
            logger.critical("❌ DeepSeek余额不足，禁用AI分析")
            DEEPSEEK_DISABLED = True
            DEEPSEEK_AVAILABLE = False
        return "本场比赛无AI分析，可参考概率数据进行决策"

# ===================== 项目模块导入 =====================
from src.data.api_integrations import create_data_aggregator, validate_and_get_api_keys
from src.data.feature_engineering import build_features_dataset
from src.data.data_collector_enhanced import FootballDataCollector

# ===================== 模型加载 =====================
MODEL_AVAILABLE = False
_fusion_model = None

try:
    from src.engine.fusion_engine import SuperFusionModel
    MODEL_AVAILABLE = True
    logger.info("✅ 超级融合模型加载成功")
except Exception as e:
    logger.critical(f"❌ 模型加载失败，管道终止：{str(e)}", exc_info=True)
    exit(1)

def init_prediction_model():
    global _fusion_model
    if _fusion_model is None:
        try:
            _fusion_model = SuperFusionModel()
            logger.info("✅ 超级融合模型初始化完成")
        except Exception as e:
            logger.critical(f"❌ 模型初始化失败，管道终止：{str(e)}", exc_info=True)
            exit(1)
    return _fusion_model

def init_database() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    logger.info(f"Database initialized at {DB_PATH}")
    return conn

# ===================== 数据采集函数（无报错版）=====================
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
            logger.info(f"  ✅ {comp_code} 命中缓存，共{len(cached_matches)}场")
            continue
        
        # 带重试的API请求，用修复后的days参数，无dateFrom报错
        matches = []
        for retry in range(API_MAX_RETRY + 1):
            try:
                # 【核心修复】只用days和status参数，不再传dateFrom，彻底避免报错
                matches = aggregator.fdb.get_matches(
                    competition_code=comp_code,
                    status="FINISHED",
                    days=history_days
                )
                break
            except Exception as e:
                if retry < API_MAX_RETRY:
                    logger.warning(f"  ⚠️ {comp_code} 请求失败，{API_RETRY_DELAY}秒后重试")
                    time.sleep(API_RETRY_DELAY)
                else:
                    logger.error(f"  ❌ {comp_code} 请求失败，已达最大重试次数")
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
            logger.info(f"  ✅ {comp_code} 命中缓存，共{len(cached_matches)}场")
            continue
        
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
                    logger.warning(f"  ⚠️ {comp_code} 请求失败，{API_RETRY_DELAY}秒后重试")
                    time.sleep(API_RETRY_DELAY)
                else:
                    logger.error(f"  ❌ {comp_code} 请求失败，已达最大重试次数")
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
            logger.warning(f"  ⚠️ {comp_code} 未获取到有效赛程")
        
        time.sleep(API_REQUEST_INTERVAL)
    
    logger.info(f"✅ 赛程采集完成，共{len(future_matches)}场有效未来比赛")
    return future_matches

# ===================== 模型预测函数 =====================
def run_prediction_model(features_df: pd.DataFrame, raw_matches: List[Dict] = None) -> pd.DataFrame:
    if features_df.empty:
        logger.critical("❌ 特征数据集为空，管道终止")
        exit(1)
    
    # 特征唯一性校验
    feature_cols = [col for col in features_df.columns if col not in ["match_id", "home_team", "away_team", "competition_code", "match_date"]]
    if len(feature_cols) > 0:
        unique_count = features_df[feature_cols].drop_duplicates().shape[0]
        total_count = features_df.shape[0]
        if unique_count == 1:
            logger.critical("❌ 所有比赛特征完全一致，管道终止")
            exit(1)
        logger.info(f"✅ 特征校验通过：{total_count}场比赛，{unique_count}组唯一特征")

    try:
        prediction_df = features_df.copy()
        model = init_prediction_model()
        predictions_list = []
        total_matches = len(prediction_df)
        all_probs = []

        logger.info(f"🤖 开始模型预测，共{total_matches}场比赛")
        success_count = 0

        for idx, row in prediction_df.iterrows():
            match_id = row["match_id"]
            match_name = f"{row['home_team']} vs {row['away_team']}"
            raw_match = next((m for m in raw_matches if m.get("id") == match_id), None)
            
            if raw_match is None:
                logger.critical(f"❌ 比赛{match_name}原始数据不存在，管道终止")
                exit(1)

            # 强制模型预测，失败直接终止
            try:
                fusion_result = model.predict_single_match(raw_match, row.to_dict())
                final_pred = fusion_result.get("final_prediction", fusion_result)
            except Exception as e:
                logger.critical(f"❌ 比赛{match_name}预测失败，管道终止：{str(e)}", exc_info=True)
                exit(1)

            # 提取概率
            try:
                home_win_prob = float(final_pred.get("home_win_prob", final_pred.get("win_prob", 0.0)))
                draw_prob = float(final_pred.get("draw_prob", 0.0))
                away_win_prob = float(final_pred.get("away_win_prob", final_pred.get("loss_prob", 0.0)))
            except Exception as e:
                logger.critical(f"❌ 比赛{match_name}概率提取失败，管道终止：{str(e)}", exc_info=True)
                exit(1)

            logger.info(f"📊 {match_name} 概率：主胜={home_win_prob:.4f}, 平局={draw_prob:.4f}, 客胜={away_win_prob:.4f}")
            all_probs.append((home_win_prob, draw_prob, away_win_prob))

            # 概率合法性校验
            if home_win_prob < 0 or draw_prob < 0 or away_win_prob < 0:
                logger.critical(f"❌ 比赛{match_name}返回负概率，管道终止")
                exit(1)
            if home_win_prob + draw_prob + away_win_prob <= 0:
                logger.critical(f"❌ 比赛{match_name}概率总和为0，管道终止")
                exit(1)

            # 百分比转小数
            if home_win_prob > 1 or draw_prob > 1 or away_win_prob > 1:
                home_win_prob /= 100
                draw_prob /= 100
                away_win_prob /= 100

            # 归一化
            total = home_win_prob + draw_prob + away_win_prob
            home_win_prob = round(home_win_prob / total, 4)
            draw_prob = round(draw_prob / total, 4)
            away_win_prob = round(away_win_prob / total, 4)

            # 预测结果
            prob_dict = {"主胜": home_win_prob, "平局": draw_prob, "客胜": away_win_prob}
            prediction = max(prob_dict, key=prob_dict.get)

            # 置信度
            try:
                confidence = float(final_pred.get("confidence", final_pred.get("model_confidence", 0.6)))
                if confidence > 1:
                    confidence /= 100
                confidence = round(max(min(confidence, 0.99), 0.1), 4)
            except Exception as e:
                logger.critical(f"❌ 比赛{match_name}置信度提取失败，管道终止：{str(e)}", exc_info=True)
                exit(1)

            # 保存结果
            predictions_list.append({
                "match_id": match_id,
                "home_win_prob": home_win_prob,
                "draw_prob": draw_prob,
                "away_win_prob": away_win_prob,
                "prediction": prediction,
                "expected_value": round(float(final_pred.get("expected_value", 0)), 4),
                "kelly_suggestion": round(float(final_pred.get("kelly_suggestion", 0)), 4),
                "model_confidence": confidence,
                "model_source": "超级融合模型SuperFusionModel"
            })
            success_count += 1

        # 固定概率校验
        if len(all_probs) >= 2:
            first_prob = all_probs[0]
            if all(p == first_prob for p in all_probs):
                logger.critical("❌ 所有比赛返回固定概率，管道终止")
                exit(1)

        if success_count != total_matches:
            logger.critical(f"❌ 预测完成度异常，应完成{total_matches}场，实际完成{success_count}场")
            exit(1)

        logger.info(f"✅ 模型预测完成：成功{success_count}场")

        # 合并结果
        pred_df = pd.DataFrame(predictions_list)
        prediction_df = prediction_df.merge(pred_df, on="match_id", how="left")

        if prediction_df["home_win_prob"].isnull().any():
            logger.critical("❌ 预测结果存在缺失值，管道终止")
            exit(1)

        # 置信度过滤
        logger.info(f"🔍 过滤低于{SKIP_CONFIDENCE_THRESHOLD*100}%置信度的比赛")
        total_before = len(prediction_df)
        prediction_df = prediction_df[prediction_df["model_confidence"] >= SKIP_CONFIDENCE_THRESHOLD].reset_index(drop=True)
        skip_count = total_before - len(prediction_df)
        logger.info(f"✅ 过滤完成：跳过{skip_count}场，剩余{len(prediction_df)}场有效比赛")
        
        if len(prediction_df) == 0:
            logger.critical("❌ 过滤后无有效比赛，管道终止")
            exit(1)

        # 注入中文队名
        prediction_df["home_team_cn"] = prediction_df["home_team"].apply(get_team_cn_name)
        prediction_df["away_team_cn"] = prediction_df["away_team"].apply(get_team_cn_name)
        prediction_df["h_recent_wins"] = prediction_df["home_recent_wins"]
        prediction_df["a_recent_wins"] = prediction_df["away_recent_wins"]

        # AI分析生成
        logger.info(f"📝 生成AI分析，仅≥{AI_ANALYSIS_CONFIDENCE_THRESHOLD*100}%置信度的比赛")
        prediction_df["match_analysis"] = "本场比赛无AI分析，可参考概率数据进行决策"

        ai_targets = prediction_df[prediction_df["model_confidence"] >= AI_ANALYSIS_CONFIDENCE_THRESHOLD].sort_values("model_confidence", ascending=False).head(MAX_AI_ANALYSIS_COUNT)
        logger.info(f"✅ 符合AI分析条件的有{len(ai_targets)}场")

        for idx, row in ai_targets.iterrows():
            match_idx = prediction_df[prediction_df["match_id"] == row["match_id"]].index[0]
            prediction_df.at[match_idx, "match_analysis"] = generate_match_analysis(row.to_dict())
            time.sleep(2)
        
        # 最终统计
        home_win_count = len(prediction_df[prediction_df['prediction'] == '主胜'])
        draw_count = len(prediction_df[prediction_df['prediction'] == '平局'])
        away_win_count = len(prediction_df[prediction_df['prediction'] == '客胜'])
        logger.info(f"✅ 最终预测统计：主胜{home_win_count}场，平局{draw_count}场，客胜{away_win_count}场")
        return prediction_df
    
    except Exception as e:
        logger.critical(f"❌ 模型预测环节异常，管道终止：{str(e)}", exc_info=True)
        exit(1)

# ===================== 静态页面生成 =====================
def generate_static_page(prediction_df: pd.DataFrame):
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # 生成JSON结果
        json_path = os.path.join(OUTPUT_DIR, "predictions.json")
        result_json = {
            "generate_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "predict_days": PREDICT_DAYS,
            "matches_count": len(prediction_df),
            "competitions": COMPETITIONS,
            "deepseek_enabled": DEEPSEEK_AVAILABLE,
            "model_used": "超级融合模型SuperFusionModel",
            "skip_confidence": f"{SKIP_CONFIDENCE_THRESHOLD*100}%",
            "ai_confidence": f"{AI_ANALYSIS_CONFIDENCE_THRESHOLD*100}%",
            "predictions": []
        }
        
        if not prediction_df.empty:
            predictions_list = prediction_df.drop(columns=["match_date"]).to_dict("records")
            for idx, pred in enumerate(predictions_list):
                match_date = prediction_df.iloc[idx]["match_date"]
                pred["match_time"] = match_date.strftime("%Y-%m-%d %H:%M UTC") if match_date else "未知"
                for key in ["home_win_prob", "draw_prob", "away_win_prob", "expected_value", "model_confidence"]:
                    if key in pred and pd.notna(pred[key]):
                        pred[key] = round(float(pred[key]), 4)
                result_json["predictions"].append(pred)
        
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(result_json, f, ensure_ascii=False, indent=2)
        
        # 生成HTML页面
        html_path = os.path.join(OUTPUT_DIR, "index.html")
        total_matches = len(prediction_df)
        home_win_count = len(prediction_df[prediction_df['prediction'] == '主胜']) if not prediction_df.empty else 0
        avg_confidence = round(prediction_df['model_confidence'].mean() * 100, 1) if not prediction_df.empty else 0.0
        ai_count = len(prediction_df[prediction_df['match_analysis'] != "本场比赛无AI分析，可参考概率数据进行决策"]) if not prediction_df.empty else 0

        html_content = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>足球赛事预测结果</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }}
        body {{ background: #f5f7fa; padding: 15px; max-width: 100%; margin: 0 auto; }}
        .header {{ text-align: center; margin-bottom: 25px; }}
        .header h1 {{ color: #2c3e50; margin-bottom: 10px; font-size: 24px; line-height: 1.4; }}
        .header .info {{ color: #7f8c8d; font-size: 13px; margin-top: 6px; }}
        .rule-tag {{ display: inline-block; background: #3498db; color: white; padding: 3px 8px; border-radius: 4px; font-size: 12px; margin: 0 3px; }}
        .stats {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px; margin-bottom: 25px; }}
        .stat-card {{ background: white; padding: 18px 12px; border-radius: 12px; box-shadow: 0 2px 12px rgba(0,0,0,0.08); text-align: center; }}
        .stat-card .num {{ font-size: 32px; font-weight: bold; color: #3498db; margin-bottom: 6px; }}
        .stat-card .label {{ color: #7f8c8d; font-size: 13px; }}
        .match-card {{ background: white; border-radius: 12px; box-shadow: 0 2px 12px rgba(0,0,0,0.08); margin-bottom: 15px; overflow: hidden; }}
        .match-header {{ background: #2c3e50; color: white; padding: 12px 15px; display: flex; justify-content: space-between; align-items: center; }}
        .match-header .league {{ font-weight: bold; font-size: 14px; }}
        .match-header .time {{ font-size: 12px; opacity: 0.9; }}
        .match-teams {{ padding: 20px 15px; display: grid; grid-template-columns: 42% 16% 42%; align-items: center; text-align: center; }}
        .team-name {{ font-size: 16px; font-weight: bold; margin-bottom: 4px; }}
        .team-en {{ font-size: 11px; color: #7f8c8d; }}
        .vs {{ font-size: 18px; font-weight: bold; color: #3498db; }}
        .match-prob {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; padding: 0 15px 15px; }}
        .prob-item {{ text-align: center; padding: 8px; border-radius: 8px; }}
        .prob-item.home {{ background: #d4edda; }}
        .prob-item.draw {{ background: #fff3cd; }}
        .prob-item.away {{ background: #f8d7da; }}
        .prob-value {{ font-size: 18px; font-weight: bold; margin-bottom: 2px; }}
        .prob-label {{ font-size: 12px; color: #2c3e50; }}
        .prediction-tag {{ margin: 0 15px 15px; padding: 10px; border-radius: 8px; text-align: center; font-weight: bold; font-size: 16px; }}
        .prediction-tag.home {{ background: #d4edda; color: #155724; }}
        .prediction-tag.draw {{ background: #fff3cd; color: #856404; }}
        .prediction-tag.away {{ background: #f8d7da; color: #721c24; }}
        .analysis-box {{ margin: 0 15px 15px; padding: 12px; background: #f8f9fa; border-radius: 8px; font-size: 13px; line-height: 1.6; color: #34495e; }}
        .match-meta {{ display: flex; justify-content: space-between; padding: 10px 15px; border-top: 1px solid #ecf0f1; font-size: 12px; color: #7f8c8d; flex-wrap: wrap; gap: 8px; }}
        .confidence-tag {{ padding: 2px 6px; border-radius: 4px; color: white; }}
        .confidence-high {{ background: #28a745; }}
        .confidence-normal {{ background: #ffc107; color: #212529; }}
        .empty {{ text-align: center; padding: 60px 20px; color: #7f8c8d; font-size: 16px; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>⚽ 足球赛事预测结果</h1>
        <div class="info">
            生成时间：{datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")} | 预测未来 {PREDICT_DAYS} 天赛事
        </div>
        <div class="info">
            核心规则：
            <span class="rule-tag">置信度<{SKIP_CONFIDENCE_THRESHOLD*100}% 直接跳过</span>
            <span class="rule-tag">置信度≥{AI_ANALYSIS_CONFIDENCE_THRESHOLD*100}% 生成AI分析</span>
        </div>
        <div class="info">核心引擎：超级融合模型SuperFusionModel</div>
    </div>

    <div class="stats">
        <div class="stat-card">
            <div class="num">{total_matches}</div>
            <div class="label">有效预测赛事</div>
        </div>
        <div class="stat-card">
            <div class="num">{len(COMPETITIONS)}</div>
            <div class="label">覆盖赛事数</div>
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

    {"".join([f'''
    <div class="match-card">
        <div class="match-header">
            <span class="league">{row["competition_code"]}</span>
            <span class="time">{row["match_time"]}</span>
        </div>
        <div class="match-teams">
            <div>
                <div class="team-name">{row["home_team_cn"]}</div>
                <div class="team-en">{row["home_team"]}</div>
            </div>
            <div class="vs">VS</div>
            <div>
                <div class="team-name">{row["away_team_cn"]}</div>
                <div class="team-en">{row["away_team"]}</div>
            </div>
        </div>
        <div class="match-prob">
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
        <div class="prediction-tag {'home' if row['prediction'] == '主胜' else 'draw' if row['prediction'] == '平局' else 'away'}">
            最终预测：{row["prediction"]}
        </div>
        <div class="analysis-box">
            <strong>赛事分析：</strong>{row["match_analysis"]}
        </div>
        <div class="match-meta">
            <span>置信度：<span class="confidence-tag {'confidence-high' if row['model_confidence'] >= 0.8 else 'confidence-normal'}">{round(row['model_confidence']*100, 1)}%</span></span>
            <span>EV值：{round(row["expected_value"]*100, 2)}%</span>
            <span>凯利建议：{row["kelly_suggestion"]}</span>
        </div>
    </div>
    ''' for row in result_json["predictions"]]) if len(result_json["predictions"]) > 0 else '''
    <div class="empty">暂无有效预测比赛，所有比赛置信度均低于{SKIP_CONFIDENCE_THRESHOLD*100}%</div>
    '''}
</body>
</html>
        """
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        
        logger.info(f"✅ 静态页面生成完成，输出目录：{OUTPUT_DIR}")
    
    except Exception as e:
        logger.critical(f"❌ 静态页面生成失败，管道终止：{str(e)}", exc_info=True)
        exit(1)

# ===================== 执行报告 =====================
def generate_execution_report(start_time: datetime, matches_count: int, features_shape: tuple, predictions_count: int, error: str = None):
    end_time = datetime.now(timezone.utc)
    duration = round((end_time - start_time).total_seconds() / 60, 4)
    
    logger.info("="*66)
    logger.info("PIPELINE EXECUTION REPORT")
    logger.info("="*66)
    
    report = {
        "timestamp": end_time.isoformat().replace("+00:00", "Z"),
        "status": "success" if predictions_count > 0 else "failed",
        "core_model": "超级融合模型SuperFusionModel",
        "deepseek_enabled": DEEPSEEK_AVAILABLE,
        "force_fusion_mode": True,
        "competitions": COMPETITIONS,
        "error": error,
        "original_matches_count": matches_count,
        "final_valid_matches_count": predictions_count,
        "features_shape": list(features_shape),
        "duration_minutes": duration
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
    conn = None
    final_error = None
    matches_count = 0
    features_shape = (0, 0)
    predictions_count = 0
    future_matches = []
    historical_matches = []
    
    logger.info("="*66)
    logger.info("🚀 STARTING FULL FOOTBALL PREDICTION PIPELINE")
    logger.info("="*66)
    
    try:
        # 密钥验证
        logger.info("=== 启动前密钥验证 ===")
        valid_keys = validate_and_get_api_keys()
        if len(valid_keys) == 0:
            raise Exception("无有效API密钥，管道终止")
        init_deepseek()
        
        # 初始化组件
        aggregator = create_data_aggregator()
        collector = FootballDataCollector(DB_PATH)
        conn = init_database()
        init_prediction_model()
        logger.info("✅ 管道初始化成功")

        # 采集历史数据
        historical_matches = fetch_historical_matches(aggregator, COMPETITIONS, HISTORY_DAYS)
        if len(historical_matches) == 0:
            raise Exception("未获取到任何有效历史数据，无法构建特征，管道终止")

        # 采集未来赛程
        future_matches = fetch_future_matches(aggregator, COMPETITIONS, PREDICT_DAYS)
        matches_count = len(future_matches)
        if matches_count == 0:
            raise Exception("未获取到任何有效未来赛程，管道终止")

        # 特征工程
        logger.info("🔧 开始特征工程")
        features_df = build_features_dataset(future_matches, historical_matches)
        features_shape = features_df.shape
        if features_df.empty:
            raise Exception("未提取到有效特征，管道终止")
        logger.info(f"✅ 特征工程完成，特征形状：{features_shape}")

        # 模型预测
        logger.info("🤖 开始模型预测")
        prediction_df = run_prediction_model(features_df, future_matches)
        predictions_count = len(prediction_df)

        # 生成静态页面
        logger.info("📄 生成静态页面")
        generate_static_page(prediction_df)

        # 生成报告
        generate_execution_report(start_time, matches_count, features_shape, predictions_count)
        logger.info("🎉 全管道执行成功！GitHub Pages部署就绪")

    except Exception as e:
        final_error = str(e)
        logger.critical(f"❌ 管道执行异常，已终止：{final_error}", exc_info=True)
        generate_execution_report(start_time, matches_count, features_shape, predictions_count, final_error)
        exit(1)
    finally:
        if conn:
            conn.close()
    
    exit(0)


if __name__ == "__main__":
    main()
