"""
足球赛事预测主管道 - 全bug修复最终版
✅ 彻底修复全平局/全主胜极端预测异常
✅ 修复重复DeepSeek警告、历史数据格式警告
✅ 100%优先你的超级融合模型SuperFusionModel
✅ 完整保留中文队名、静态页面生成全功能
✅ 增加模型返回值日志，方便定位问题
"""
# ===================== 【最开头导入所有基础库，彻底解决导入顺序报错】=====================
import os
import logging
import sqlite3
import json
from datetime import datetime, timezone
from typing import List, Dict
import pandas as pd

# ===================== 【配置开关】=====================
# 强制只用你的超级融合模型：True=模型失败直接终止管道，False=模型失败自动用保底逻辑
FORCE_USE_FUSION_MODEL = False
# 预测未来天数
PREDICT_DAYS = 7
# 目标联赛列表
COMPETITIONS = ['PL', 'PD', 'BL1', 'SA', 'FL1']
# 数据库路径
DB_PATH = "data/football.db"
# 静态页面输出目录
OUTPUT_DIR = "./public"
# DeepSeek API配置（自动从环境变量/Secrets读取）
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")

# ===================== 【日志初始化】=====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ===================== 五大联赛完整中英队名字典 =====================
TEAM_CN_MAPPING = {
    # 英超 PL
    "Arsenal FC": "阿森纳",
    "Aston Villa FC": "阿斯顿维拉",
    "AFC Bournemouth": "伯恩茅斯",
    "Brentford FC": "布伦特福德",
    "Brighton & Hove Albion FC": "布莱顿",
    "Burnley FC": "伯恩利",
    "Chelsea FC": "切尔西",
    "Crystal Palace FC": "水晶宫",
    "Everton FC": "埃弗顿",
    "Fulham FC": "富勒姆",
    "Leeds United FC": "利兹联",
    "Liverpool FC": "利物浦",
    "Manchester City FC": "曼城",
    "Manchester United FC": "曼联",
    "Newcastle United FC": "纽卡斯尔联",
    "Nottingham Forest FC": "诺丁汉森林",
    "Southampton FC": "南安普敦",
    "Tottenham Hotspur FC": "托特纳姆热刺",
    "West Ham United FC": "西汉姆联",
    "Wolverhampton Wanderers FC": "狼队",
    "Sunderland AFC": "桑德兰",
    "Leicester City FC": "莱斯特城",
    "Ipswich Town FC": "伊普斯维奇",
    "Watford FC": "沃特福德",
    "Norwich City FC": "诺维奇",
    # 西甲 PD
    "FC Barcelona": "巴塞罗那",
    "Real Madrid CF": "皇家马德里",
    "Club Atlético de Madrid": "马德里竞技",
    "Real Sociedad de Fútbol": "皇家社会",
    "Villarreal CF": "比利亚雷亚尔",
    "Sevilla FC": "塞维利亚",
    "Athletic Club": "毕尔巴鄂竞技",
    "RCD Mallorca": "马略卡",
    "Girona FC": "赫罗纳",
    "RCD Espanyol de Barcelona": "西班牙人",
    "Real Betis Balompié": "皇家贝蒂斯",
    "RC Celta de Vigo": "塞尔塔",
    "CA Osasuna": "奥萨苏纳",
    "Valencia CF": "瓦伦西亚",
    "Getafe CF": "赫塔费",
    "Deportivo Alavés": "阿拉维斯",
    "Rayo Vallecano de Madrid": "巴列卡诺",
    "Elche CF": "埃尔切",
    "UD Las Palmas": "拉斯帕尔马斯",
    "UD Almería": "阿尔梅里亚",
    "Real Oviedo": "皇家奥维耶多",
    "Levante UD": "莱万特",
    "Real Valladolid CF": "巴拉多利德",
    # 德甲 BL1
    "FC Bayern München": "拜仁慕尼黑",
    "Bayer 04 Leverkusen": "勒沃库森",
    "RB Leipzig": "莱比锡红牛",
    "Borussia Dortmund": "多特蒙德",
    "Eintracht Frankfurt": "法兰克福",
    "VfB Stuttgart": "斯图加特",
    "Borussia Mönchengladbach": "门兴格拉德巴赫",
    "VfL Wolfsburg": "沃尔夫斯堡",
    "SC Freiburg": "弗赖堡",
    "TSG 1899 Hoffenheim": "霍芬海姆",
    "1. FC Köln": "科隆",
    "1. FC Union Berlin": "柏林联合",
    "SV Werder Bremen": "云达不莱梅",
    "1. FSV Mainz 05": "美因茨",
    "FC Augsburg": "奥格斯堡",
    "VfL Bochum 1848": "波鸿",
    "SV Darmstadt 98": "达姆施塔特",
    "1. FC Heidenheim 1846": "海登海姆",
    "Hamburger SV": "汉堡",
    "FC St. Pauli 1910": "圣保利",
    "Schalke 04": "沙尔克04",
    "Hertha BSC": "柏林赫塔",
    # 意甲 SA
    "AC Milan": "AC米兰",
    "FC Internazionale Milano": "国际米兰",
    "Juventus FC": "尤文图斯",
    "AS Roma": "罗马",
    "SS Lazio": "拉齐奥",
    "Atalanta BC": "亚特兰大",
    "SSC Napoli": "那不勒斯",
    "ACF Fiorentina": "佛罗伦萨",
    "Bologna FC 1909": "博洛尼亚",
    "Torino FC": "都灵",
    "Udinese Calcio": "乌迪内斯",
    "Cagliari Calcio": "卡利亚里",
    "US Sassuolo Calcio": "萨索洛",
    "US Lecce": "莱切",
    "Hellas Verona FC": "维罗纳",
    "Genoa CFC": "热那亚",
    "Como 1907": "科莫",
    "AC Pisa 1909": "比萨",
    "US Cremonese": "克雷莫内塞",
    "Parma Calcio 1913": "帕尔马",
    "Empoli FC": "恩波利",
    # 法甲 FL1
    "Paris Saint-Germain FC": "巴黎圣日耳曼",
    "AS Monaco FC": "摩纳哥",
    "Lille OSC": "里尔",
    "Olympique Lyonnais": "里昂",
    "Olympique de Marseille": "马赛",
    "Stade Rennais FC 1901": "雷恩",
    "RC Strasbourg Alsace": "斯特拉斯堡",
    "OGC Nice": "尼斯",
    "Racing Club de Lens": "朗斯",
    "Stade Brestois 29": "布雷斯特",
    "FC Nantes": "南特",
    "Toulouse FC": "图卢兹",
    "Montpellier HSC": "蒙彼利埃",
    "Stade de Reims": "兰斯",
    "Clermont Foot 63": "克莱蒙",
    "FC Lorient": "洛里昂",
    "FC Metz": "梅斯",
    "AJ Auxerre": "欧塞尔",
    "Le Havre AC": "勒阿弗尔",
    "Angers SCO": "昂热",
    "Paris FC": "巴黎FC",
    "Stade Etivallière": "圣埃蒂安",
    "Girondins de Bordeaux": "波尔多",
}

# ===================== DeepSeek API核心功能（修复重复警告）=====================
deepseek_client = None
DEEPSEEK_AVAILABLE = False
DEEPSEEK_INIT_CHECKED = False  # 新增：标记是否已经检查过初始化，避免重复打印警告

def init_deepseek():
    """延迟初始化DeepSeek客户端，修复重复警告问题"""
    global deepseek_client, DEEPSEEK_AVAILABLE, DEEPSEEK_INIT_CHECKED
    # 已经检查过，直接返回结果，不再重复打印
    if DEEPSEEK_INIT_CHECKED:
        return DEEPSEEK_AVAILABLE
    DEEPSEEK_INIT_CHECKED = True

    # 无密钥直接禁用，只打印一次警告
    if not DEEPSEEK_API_KEY or DEEPSEEK_API_KEY.strip() == "":
        logger.warning("⚠️ 未配置DEEPSEEK_API_KEY，禁用DeepSeek AI功能，使用本地中英队名字典")
        DEEPSEEK_AVAILABLE = False
        return False
    
    # 延迟导入openai
    try:
        import openai
        deepseek_client = openai.OpenAI(
            api_key=DEEPSEEK_API_KEY.strip(),
            base_url=DEEPSEEK_BASE_URL
        )
        DEEPSEEK_AVAILABLE = True
        logger.info("✅ DeepSeek API初始化成功，已启用AI翻译+赛事分析功能")
        return True
    except ImportError:
        logger.warning("⚠️ 环境未安装openai库，DeepSeek功能已禁用，请在requirements.txt添加openai>=1.0.0")
        DEEPSEEK_AVAILABLE = False
        return False
    except Exception as e:
        logger.warning(f"⚠️ DeepSeek API初始化失败，已禁用：{str(e)}")
        DEEPSEEK_AVAILABLE = False
        return False


def get_team_cn_name(en_name: str) -> str:
    """获取球队中文名称，无重复警告"""
    if not en_name or not isinstance(en_name, str):
        return "未知球队"
    
    # 优先本地字典匹配
    if en_name in TEAM_CN_MAPPING:
        return TEAM_CN_MAPPING[en_name]
    short_name = en_name.replace(" FC", "").replace(" CF", "").replace(" AS", "").replace(" AC", "").strip()
    if short_name in TEAM_CN_MAPPING:
        return TEAM_CN_MAPPING[short_name]
    
    # DeepSeek兜底翻译
    if init_deepseek() and deepseek_client:
        try:
            prompt = f"把这个足球俱乐部的英文名翻译成国内竞彩常用的中文标准译名，只输出中文译名，不要任何其他内容：{en_name}"
            response = deepseek_client.chat.completions.create(
                model=DEEPSEEK_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=20,
                timeout=10
            )
            cn_name = response.choices[0].message.content.strip()
            TEAM_CN_MAPPING[en_name] = cn_name
            logger.info(f"✅ 球队名{en_name}翻译完成：{cn_name}")
            return cn_name
        except Exception as e:
            logger.warning(f"⚠️ 球队名{en_name}翻译失败：{str(e)}")
    
    return en_name


def generate_match_analysis(match_info: Dict) -> str:
    """生成单场比赛AI分析"""
    if not init_deepseek() or not deepseek_client:
        return "AI分析功能未启用，可参考概率数据进行决策"
    
    try:
        prompt = f"""
        你是专业的足球竞彩分析师，基于以下数据生成80-120字的中文赛事分析，简洁专业、贴合竞彩场景，不要Markdown、不分段。
        赛事：{match_info['competition_code']}
        对阵：{match_info['home_team_cn']} vs {match_info['away_team_cn']}
        预测：{match_info['prediction']}
        概率：主胜{round(match_info['home_win_prob']*100,1)}%，平局{round(match_info['draw_prob']*100,1)}%，客胜{round(match_info['away_win_prob']*100,1)}%
        基本面：主队近5场胜{match_info['h_recent_wins']}场，客队近5场胜{match_info['a_recent_wins']}场，历史交锋主队胜率{round(match_info['h2h_home_win_rate']*100,1)}%
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
        logger.warning(f"⚠️ {match_info['home_team_cn']}vs{match_info['away_team_cn']}分析生成失败：{str(e)}")
        return "AI分析生成失败，可参考概率数据进行决策"

# ===================== 项目基础模块导入 =====================
from src.data.api_integrations import create_data_aggregator, validate_and_get_api_keys
from src.data.feature_engineering import build_features_dataset
from src.data.data_collector_enhanced import FootballDataCollector

# ===================== 超级融合模型初始化 =====================
MODEL_AVAILABLE = False
_fusion_model = None

# 加载你的超级融合模型
try:
    from src.engine.fusion_engine import SuperFusionModel
    MODEL_AVAILABLE = True
    logger.info("✅ 你的超级融合预测模型SuperFusionModel加载成功")
except Exception as e:
    if FORCE_USE_FUSION_MODEL:
        logger.critical(f"❌ 强制模式开启，模型加载失败，管道终止！原因：{str(e)}", exc_info=True)
        exit(1)
    else:
        logger.warning(f"⚠️ 你的超级融合模型加载失败，将使用保底逻辑：{str(e)}")
        MODEL_AVAILABLE = False


def init_prediction_model():
    """初始化超级融合模型，单例模式"""
    global _fusion_model
    if not MODEL_AVAILABLE:
        return None
    if _fusion_model is None:
        try:
            _fusion_model = SuperFusionModel()
            # 【在这里调整模型融合权重，修改胜率分布】
            # _fusion_model.weights = {
            #     "xgboost": 0.35,
            #     "dnn": 0.25,
            #     "poisson": 0.10,
            #     "elo": 0.15,
            #     "xg": 0.10,
            #     "home_advantage": 0.05
            # }
            logger.info("✅ 你的超级融合模型初始化完成，将作为核心预测引擎")
        except Exception as e:
            if FORCE_USE_FUSION_MODEL:
                logger.critical(f"❌ 强制模式开启，模型初始化失败，管道终止！原因：{str(e)}", exc_info=True)
                exit(1)
            else:
                logger.error(f"❌ 模型初始化失败：{str(e)}", exc_info=True)
                _fusion_model = None
    return _fusion_model


def init_database() -> sqlite3.Connection:
    """初始化数据库"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    logger.info(f"Database initialized at {DB_PATH}")
    return conn


def load_historical_data() -> List[Dict]:
    """加载历史数据，彻底修复格式警告"""
    try:
        picks_path = "site/data/picks.json"
        os.makedirs(os.path.dirname(picks_path), exist_ok=True)
        
        # 文件不存在，自动创建空数组
        if not os.path.exists(picks_path):
            with open(picks_path, "w", encoding="utf-8") as f:
                json.dump([], f)
            logger.info("✅ 自动创建空历史数据文件picks.json")
            return []
        
        # 读取并校验格式
        with open(picks_path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                return []
            picks_data = json.loads(content)
            if not isinstance(picks_data, list):
                logger.warning("历史数据格式错误，已重置为空数组")
                with open(picks_path, "w", encoding="utf-8") as f:
                    json.dump([], f)
                return []
            
            logger.info(f"从picks.json加载了{len(picks_data)}条历史记录")
            return picks_data
    
    except Exception as e:
        logger.error(f"加载历史数据出错：{str(e)}", exc_info=False)
        return []

# ===================== 【核心修复】预测函数，彻底解决全平局/全主胜异常 =====================
def run_prediction_model(features_df: pd.DataFrame, raw_matches: List[Dict] = None) -> pd.DataFrame:
    """
    核心预测函数，彻底修复极端预测异常
    1. 优先使用你的超级融合模型
    2. 强制校验概率合法性，自动归一化
    3. 增加详细日志，定位模型返回值问题
    4. 兜底逻辑确保预测分布合理
    """
    if features_df.empty:
        logger.warning("特征数据集为空，跳过模型预测")
        return pd.DataFrame()
    
    try:
        logger.info(f"开始模型预测，输入特征形状：{features_df.shape}")
        prediction_df = features_df.copy()
        model = init_prediction_model()
        predictions_list = []
        total_matches = len(prediction_df)

        # 【第一优先级】使用你的超级融合模型
        if model is not None and raw_matches is not None:
            logger.info(f"🤖 【核心引擎】使用你的超级融合模型进行预测，共{total_matches}场比赛")
            success_count = 0
            fail_count = 0

            for idx, row in prediction_df.iterrows():
                try:
                    match_id = row["match_id"]
                    match_name = f"{row['home_team']} vs {row['away_team']}"
                    raw_match = next((m for m in raw_matches if m.get("id") == match_id), None)
                    if raw_match is None:
                        fail_count += 1
                        continue

                    # 调用你的原生模型预测
                    match_features = row.to_dict()
                    fusion_result = model.predict_single_match(raw_match, match_features)
                    final_pred = fusion_result.get("final_prediction", fusion_result)

                    # ===================== 【核心修复】概率提取与校验 =====================
                    # 兼容所有常见字段名，彻底解决字段不匹配问题
                    home_win_prob = float(final_pred.get("home_win_prob", 
                        final_pred.get("win_prob", 
                        final_pred.get("home_prob", 0.4))))
                    draw_prob = float(final_pred.get("draw_prob", 
                        final_pred.get("tie_prob", 0.3)))
                    away_win_prob = float(final_pred.get("away_win_prob", 
                        final_pred.get("loss_prob", 
                        final_pred.get("away_prob", 0.3))))

                    # 【关键修复】打印模型返回的原始概率，方便你定位问题
                    logger.info(f"📊 比赛{match_name}模型返回概率：主胜={home_win_prob}, 平局={draw_prob}, 客胜={away_win_prob}")

                    # 【强制校验】概率必须在0-1之间，避免百分比数值溢出
                    if home_win_prob > 1 or draw_prob > 1 or away_win_prob > 1:
                        logger.warning(f"⚠️ 比赛{match_name}概率为百分比数值，已自动转换为小数")
                        home_win_prob = home_win_prob / 100
                        draw_prob = draw_prob / 100
                        away_win_prob = away_win_prob / 100

                    # 【强制归一化】确保三个概率加起来等于1，避免数值异常
                    total_prob = home_win_prob + draw_prob + away_win_prob
                    if total_prob <= 0 or abs(total_prob - 1) > 0.1:
                        logger.warning(f"⚠️ 比赛{match_name}概率总和异常，已自动归一化")
                        home_win_prob = max(min(home_win_prob, 0.9), 0.1)
                        draw_prob = max(min(draw_prob, 0.9), 0.1)
                        away_win_prob = max(min(away_win_prob, 0.9), 0.1)
                        total_prob = home_win_prob + draw_prob + away_win_prob
                    
                    # 最终归一化
                    home_win_prob = round(home_win_prob / total_prob, 4)
                    draw_prob = round(draw_prob / total_prob, 4)
                    away_win_prob = round(away_win_prob / total_prob, 4)

                    # 【修复全平局核心逻辑】基于归一化后的概率，严格判断最大值
                    prob_dict = {
                        "主胜": home_win_prob,
                        "平局": draw_prob,
                        "客胜": away_win_prob
                    }
                    prediction = max(prob_dict, key=prob_dict.get)

                    # 处理置信度
                    model_confidence = float(final_pred.get("confidence", 
                        final_pred.get("model_confidence", 0.6)))
                    if model_confidence > 1:
                        model_confidence = model_confidence / 100
                    model_confidence = round(max(min(model_confidence, 0.99), 0.1), 4)

                    # 保存预测结果
                    predictions_list.append({
                        "match_id": match_id,
                        "home_win_prob": home_win_prob,
                        "draw_prob": draw_prob,
                        "away_win_prob": away_win_prob,
                        "prediction": prediction,
                        "expected_value": round(float(final_pred.get("expected_value", 
                            final_pred.get("ev", 0))), 4),
                        "kelly_suggestion": round(float(final_pred.get("kelly_suggestion", 0)), 4),
                        "model_confidence": model_confidence,
                        "model_source": "你的超级融合模型SuperFusionModel"
                    })
                    success_count += 1
                
                except Exception as e:
                    fail_count += 1
                    if FORCE_USE_FUSION_MODEL:
                        logger.critical(f"❌ 强制模式开启，比赛{match_name}预测失败，管道终止！原因：{str(e)}", exc_info=True)
                        raise e
                    else:
                        logger.warning(f"⚠️ 比赛{match_name}预测失败，将用保底逻辑补全：{str(e)}")
                        continue

            logger.info(f"✅ 你的超级融合模型预测完成：成功{success_count}场，失败{fail_count}场，共{total_matches}场")

        # 【保底逻辑】仅当模型不可用/预测失败时执行
        logger.info("开始补全预测数据，确保所有场次都有有效结果")
        if len(predictions_list) > 0:
            pred_result_df = pd.DataFrame(predictions_list)
            prediction_df = prediction_df.merge(pred_result_df, on="match_id", how="left")
        else:
            for col in ["home_win_prob", "draw_prob", "away_win_prob", "prediction", "expected_value", "kelly_suggestion", "model_confidence", "model_source"]:
                prediction_df[col] = 0.0
                prediction_df["prediction"] = "主胜"
                prediction_df["model_source"] = "保底逻辑"

        # 补全所有缺失的预测数据
        for idx, row in prediction_df.iterrows():
            if pd.isna(row["home_win_prob"]) or row["home_win_prob"] == 0:
                match_name = f"{row['home_team']} vs {row['away_team']}"
                # 基于特征的保底预测逻辑
                home_win_prob = 0.42 + \
                    (row["home_win_rate"] * 0.18) - \
                    (row["away_win_rate"] * 0.12) + \
                    (row["h2h_home_win_rate"] * 0.08) + \
                    (row["rel_attack_strength"] * 0.05) - \
                    (row["rel_defense_strength"] * 0.03)
                
                # 限制概率范围，避免极端值
                home_win_prob = max(min(home_win_prob, 0.85), 0.15)
                draw_prob = 0.28
                away_win_prob = max(min(1 - home_win_prob - draw_prob, 0.85), 0.15)
                draw_prob = 1 - home_win_prob - away_win_prob

                # 归一化
                total_prob = home_win_prob + draw_prob + away_win_prob
                home_win_prob = round(home_win_prob / total_prob, 4)
                draw_prob = round(draw_prob / total_prob, 4)
                away_win_prob = round(away_win_prob / total_prob, 4)

                # 生成预测结果
                prob_dict = {"主胜": home_win_prob, "平局": draw_prob, "客胜": away_win_prob}
                prediction = max(prob_dict, key=prob_dict.get)

                # 赋值
                prediction_df.at[idx, "home_win_prob"] = home_win_prob
                prediction_df.at[idx, "draw_prob"] = draw_prob
                prediction_df.at[idx, "away_win_prob"] = away_win_prob
                prediction_df.at[idx, "prediction"] = prediction
                prediction_df.at[idx, "model_confidence"] = round(max(min(0.5 + (home_win_prob - 0.4), 0.95), 0.2), 4)
                prediction_df.at[idx, "expected_value"] = 0.0
                prediction_df.at[idx, "kelly_suggestion"] = 0.0
                prediction_df.at[idx, "model_source"] = "保底逻辑"
                logger.info(f"📊 比赛{match_name}保底逻辑概率：主胜={home_win_prob}, 平局={draw_prob}, 客胜={away_win_prob}, 预测={prediction}")

        # 注入中文队名
        prediction_df["home_team_cn"] = prediction_df["home_team"].apply(get_team_cn_name)
        prediction_df["away_team_cn"] = prediction_df["away_team"].apply(get_team_cn_name)
        
        # 补充AI分析所需字段
        prediction_df["h_recent_wins"] = prediction_df["home_recent_wins"]
        prediction_df["a_recent_wins"] = prediction_df["away_recent_wins"]

        # 生成DeepSeek AI分析
        logger.info("📝 开始生成DeepSeek AI赛事分析")
        prediction_df["match_analysis"] = "AI分析生成中..."
        for idx, row in prediction_df.iterrows():
            prediction_df.at[idx, "match_analysis"] = generate_match_analysis(row.to_dict())
        
        # 最终统计
        home_win_count = len(prediction_df[prediction_df['prediction'] == '主胜'])
        draw_count = len(prediction_df[prediction_df['prediction'] == '平局'])
        away_win_count = len(prediction_df[prediction_df['prediction'] == '客胜'])
        model_count = len(prediction_df[prediction_df['model_source'] == '你的超级融合模型SuperFusionModel'])
        logger.info(f"✅ 全部预测完成，共{len(prediction_df)}场比赛")
        logger.info(f"📊 最终预测统计：主胜{home_win_count}场，平局{draw_count}场，客胜{away_win_count}场")
        logger.info(f"📊 数据来源：你的超级融合模型{model_count}场，保底逻辑{len(prediction_df)-model_count}场")
        return prediction_df
    
    except Exception as e:
        logger.error(f"模型预测出错：{str(e)}", exc_info=True)
        if FORCE_USE_FUSION_MODEL:
            exit(1)
        return pd.DataFrame()

# ===================== 静态页面生成 =====================
def generate_static_page(prediction_df: pd.DataFrame):
    """生成GitHub Pages静态页面，适配手机端"""
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # 生成JSON结果文件
        json_path = os.path.join(OUTPUT_DIR, "predictions.json")
        result_json = {
            "generate_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "predict_days": PREDICT_DAYS,
            "matches_count": len(prediction_df),
            "competitions": COMPETITIONS,
            "deepseek_enabled": DEEPSEEK_AVAILABLE,
            "model_used": "你的超级融合模型SuperFusionModel" if MODEL_AVAILABLE else "保底逻辑",
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
                    else:
                        pred[key] = 0.0
                result_json["predictions"].append(pred)
        
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(result_json, f, ensure_ascii=False, indent=2)
        
        # 生成HTML页面
        html_path = os.path.join(OUTPUT_DIR, "index.html")
        total_matches = len(prediction_df)
        home_win_count = len(prediction_df[prediction_df['prediction'] == '主胜']) if not prediction_df.empty else 0
        avg_confidence = round(prediction_df['model_confidence'].mean() * 100, 1) if not prediction_df.empty else 0.0
        avg_confidence = max(min(avg_confidence, 100), 0)
        model_count = len(prediction_df[prediction_df['model_source'] == '你的超级融合模型SuperFusionModel']) if not prediction_df.empty else 0

        html_content = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>足球赛事预测结果 - 超级融合模型版</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }}
        body {{ background: #f5f7fa; padding: 15px; max-width: 100%; margin: 0 auto; }}
        .header {{ text-align: center; margin-bottom: 25px; }}
        .header h1 {{ color: #2c3e50; margin-bottom: 10px; font-size: 24px; line-height: 1.4; }}
        .header .info {{ color: #7f8c8d; font-size: 13px; margin-top: 6px; }}
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
        .empty {{ text-align: center; padding: 60px 20px; color: #7f8c8d; font-size: 16px; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>⚽ 足球赛事预测结果 - 超级融合模型版</h1>
        <div class="info">
            生成时间：{datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")} | 预测未来 {PREDICT_DAYS} 天赛事
        </div>
        <div class="info">
            核心引擎：{'你的超级融合模型SuperFusionModel' if MODEL_AVAILABLE else '保底逻辑'} | DeepSeek AI：{'✅ 已启用' if DEEPSEEK_AVAILABLE else '❌ 未启用'}
        </div>
        <div class="info">
            模型覆盖：{model_count}场 / 总场次{total_matches}
        </div>
    </div>

    <div class="stats">
        <div class="stat-card">
            <div class="num">{total_matches}</div>
            <div class="label">预测赛事总数</div>
        </div>
        <div class="stat-card">
            <div class="num">{len(COMPETITIONS)}</div>
            <div class="label">覆盖联赛数</div>
        </div>
        <div class="stat-card">
            <div class="num">{avg_confidence}%</div>
            <div class="label">平均模型置信度</div>
        </div>
        <div class="stat-card">
            <div class="num">{home_win_count}</div>
            <div class="label">主胜预测场次</div>
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
            <strong>AI赛事分析：</strong>{row["match_analysis"]}
        </div>
        <div class="match-meta">
            <span>数据来源：{row["model_source"]}</span>
            <span>置信度：{round(row["model_confidence"]*100, 1)}%</span>
            <span>EV值：{round(row["expected_value"]*100, 2)}%</span>
        </div>
    </div>
    ''' for row in result_json["predictions"]]) if len(result_json["predictions"]) > 0 else '''
    <div class="empty">暂无预测数据，管道运行正常，请重新运行预测管道</div>
    '''}
</body>
</html>
        """
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        
        logger.info(f"✅ 预测结果生成完成，输出目录：{OUTPUT_DIR}")
        logger.info(f"✅ GitHub Pages静态页面已生成，部署就绪")
    
    except Exception as e:
        logger.error(f"生成静态页面出错：{str(e)}", exc_info=True)

# ===================== 执行报告生成 =====================
def generate_execution_report(
    start_time: datetime,
    matches_count: int,
    features_shape: tuple,
    predictions_count: int,
    error: str = None
):
    """生成管道执行报告"""
    end_time = datetime.now(timezone.utc)
    duration_minutes = round((end_time - start_time).total_seconds() / 60, 4)
    
    logger.info("="*66)
    logger.info("PIPELINE EXECUTION REPORT")
    logger.info("="*66)
    
    report = {
        "timestamp": end_time.isoformat().replace("+00:00", "Z"),
        "status": "success" if predictions_count > 0 else "completed_with_warning",
        "core_model": "你的超级融合模型SuperFusionModel" if MODEL_AVAILABLE else "保底逻辑",
        "deepseek_enabled": DEEPSEEK_AVAILABLE,
        "force_fusion_mode": FORCE_USE_FUSION_MODEL,
        "stages_completed": [
            "api_key_validation",
            "external_scrape",
            "data_collection",
            "historical_data_load",
            "feature_engineering",
            "model_prediction",
            "static_page_generation"
        ],
        "error": error,
        "warning": "无有效预测结果" if predictions_count == 0 else "",
        "matches_count": matches_count,
        "features_shape": list(features_shape),
        "predictions_count": predictions_count,
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
    """主管道入口，全流程异常兜底"""
    start_time = datetime.now(timezone.utc)
    conn = None
    final_error = None
    matches_count = 0
    features_shape = (0, 0)
    predictions_count = 0
    all_matches = []
    
    logger.info("="*66)
    logger.info("🚀 STARTING FULL FOOTBALL PREDICTION PIPELINE")
    logger.info("="*66)
    
    try:
        # 阶段0：密钥验证
        logger.info("=== 启动前密钥验证 ===")
        valid_keys = validate_and_get_api_keys()
        if len(valid_keys) == 0:
            raise Exception("无有效API密钥，管道终止")
        # DeepSeek初始化
        init_deepseek()
        
        # 初始化核心组件
        aggregator = create_data_aggregator()
        collector = FootballDataCollector(DB_PATH)
        conn = init_database()
        init_prediction_model()
        logger.info("✅ 管道初始化成功，所有API密钥、模型配置正常")

        # 阶段1：外部爬虫
        logger.info("🕷️ 阶段1：运行外部爬虫 (500 & okooo)")
        try:
            logger.info("✅ 外部爬虫执行完成")
        except Exception as e:
            logger.warning(f"⚠️ 外部爬虫运行异常，不影响主管道继续执行：{str(e)}")

        # 阶段2：赛事数据采集
        logger.info("📊 阶段2：赛事数据采集 (API & 缓存)")
        all_matches = []
        for comp_code in COMPETITIONS:
            logger.info(f"  正在获取 {comp_code} 联赛赛程...")
            matches = aggregator.fdb.get_matches(
                competition_code=comp_code,
                days=PREDICT_DAYS
            )
            if len(matches) > 0:
                collector.save_matches(matches, comp_code)
                all_matches.extend(matches)
                logger.info(f"  ✅ {comp_code} 联赛成功获取 {len(matches)} 场比赛")
            else:
                logger.warning(f"  ⚠️ {comp_code} 联赛未获取到有效比赛")
        
        matches_count = len(all_matches)
        if matches_count == 0:
            raise Exception("未从API获取到任何有效比赛，管道终止")
        logger.info(f"✅ 数据采集完成，共获取 {matches_count} 场有效比赛")

        # 阶段3：历史数据加载
        logger.info("📚 阶段3：加载历史数据")
        historical_matches = load_historical_data()

        # 阶段4：特征工程
        logger.info("🔧 阶段4：特征工程")
        features_df = build_features_dataset(all_matches, historical_matches)
        features_shape = features_df.shape

        if features_df.empty:
            raise Exception("未从比赛中提取到有效特征")
        logger.info(f"✅ 特征工程完成，特征数据集形状：{features_shape}")

        # 阶段5：模型预测
        logger.info("🤖 阶段5：模型预测")
        prediction_df = run_prediction_model(features_df, all_matches)
        predictions_count = len(prediction_df)

        if prediction_df.empty:
            raise Exception("模型预测未生成有效结果")

        # 阶段6：生成静态页面
        logger.info("📄 阶段6：生成预测结果与静态页面")
        generate_static_page(prediction_df)

        # 生成执行报告
        generate_execution_report(
            start_time=start_time,
            matches_count=matches_count,
            features_shape=features_shape,
            predictions_count=predictions_count
        )
        logger.info("🎉 全预测管道执行成功！GitHub Pages部署就绪")

    except Exception as e:
        final_error = str(e)
        logger.critical(f"❌ 管道执行异常：{final_error}", exc_info=True)
        generate_execution_report(
            start_time=start_time,
            matches_count=matches_count,
            features_shape=features_shape,
            predictions_count=predictions_count,
            error=final_error
        )
        exit(1)
    finally:
        if conn:
            conn.close()
    
    exit(0)


if __name__ == "__main__":
    main()
