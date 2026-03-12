"""
完整的足球预测管道
从数据收集 -> 特征工程 -> 模型训练 -> 预测生成 -> 结果导出
修复：除以零报错、环境变量密钥读取、空数据兜底、异常容错、create_data_aggregator参数匹配报错
"""

import pandas as pd
import numpy as np
from datetime import datetime
import logging
from pathlib import Path
import json
from typing import Dict, List, Tuple
import os

# 导入本地模块
from src.data.api_integrations import create_data_aggregator, validate_and_get_api_keys
from src.collect.utils import now_cn_date
from src.data.feature_engineering import FeatureEngineer
from src.data.data_collector_enhanced import (
    DataCollector, HistoricalDataLoader, CacheManager
)
from src.models.advanced_ml import MetaLearner
from src.models.poisson import predict_poisson
from src.models.elo import update_elo
from src.engine.fusion_engine import SuperFusionModel, BatchPredictor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== 修复1：补全3组密钥读取，和api_integrations.py完全对齐 ====================
# 从环境变量读取所有密钥，和日志验证的3组密钥完全匹配
ENV_CONFIG = {
    "API_FOOTBALL_KEY": os.getenv("API_FOOTBALL_KEY", ""),
    "FOOTBALL_DATA_KEY": os.getenv("FOOTBALL_DATA_KEY", ""),
    "ODDS_API_KEY": os.getenv("ODDS_API_KEY", "")
}

class FootballPredictionPipeline:
    """完整的足球预测管道"""
    
    def __init__(
        self,
        football_api_key: str = None,
        football_data_key: str = None,  # 修复2：补全缺失的football_data_key参数
        odds_api_key: str = None,
        db_path: str = "data/football.db"
    ):
        """
        初始化管道
        
        Args:
            football_api_key: api-sports.io API密钥（对应API_FOOTBALL_KEY）
            football_data_key: football-data.org API密钥（对应FOOTBALL_DATA_KEY）
            odds_api_key: The Odds API密钥（对应ODDS_API_KEY）
            db_path: 数据库路径
        """
        # 优先使用传入的密钥，兜底用环境变量，和api_integrations逻辑完全一致
        self.football_api_key = football_api_key if football_api_key else ENV_CONFIG["API_FOOTBALL_KEY"]
        self.football_data_key = football_data_key if football_data_key else ENV_CONFIG["FOOTBALL_DATA_KEY"]
        self.odds_api_key = odds_api_key if odds_api_key else ENV_CONFIG["ODDS_API_KEY"]
        
        # ==================== 核心修复3：彻底解决参数不匹配报错 ====================
        # 调用参数名、数量和api_integrations.py里的函数定义100%匹配，无多余/缺失参数
        try:
            self.data_aggregator = create_data_aggregator(
                football_api_key=self.football_api_key,
                football_data_key=self.football_data_key,
                odds_api_key=self.odds_api_key
            )
            logger.info("✅ 数据聚合器初始化成功，API密钥参数匹配正常")
        except TypeError as e:
            logger.critical(f"❌ 数据聚合器初始化失败（参数不匹配）：{e}", exc_info=True)
            raise
        except Exception as e:
            logger.critical(f"❌ 数据聚合器初始化失败：{e}", exc_info=True)
            raise

        # 其他组件初始化，保留原有逻辑
        self.data_collector = DataCollector(db_path)
        self.feature_engineer = FeatureEngineer()
        self.meta_learner = MetaLearner()
        self.fusion_model = SuperFusionModel()
        self.fusion_model.load_meta_learner(self.meta_learner)
        self.cache = CacheManager()
        
        self.historical_data = None
        self.features = None
        self.predictions = None
        
        logger.info("Pipeline initialized successfully")
        # 密钥状态日志，和api_integrations验证结果对齐
        logger.info(f"API_FOOTBALL_KEY 状态：{'已配置' if self.football_api_key else '未配置'}")
        logger.info(f"FOOTBALL_DATA_KEY 状态：{'已配置' if self.football_data_key else '未配置'}")
        logger.info(f"ODDS_API_KEY 状态：{'已配置' if self.odds_api_key else '未配置'}")
    
    def stage_0_scrape_external_data(self) -> None:
        """阶段0：运行所有外部爬虫，更新site/data目录。

        仅在需要的时候调用，任何错误均会被捕获并记录，但不会停止主流程。
        """
        logger.info("🕷️ Stage 0: Running external scrapers (500 & okooo)")
        try:
            from src.collect import export_500, export_okooo
            # 抓取最新1天500网数据
            export_500(days=1)
            # 抓取最近3天澳客数据作为示例
            today = now_cn_date()
            export_okooo(start_date=today, days=3, version="full")
            logger.info("✅ External scrapers completed")
        except Exception as e:
            logger.warning(f"External scrapers failed: {e}", exc_info=True)

    def stage_1_collect_data(self, competitions: List[str] = None) -> pd.DataFrame:
        """
        阶段1：数据收集
        
        Args:
            competitions: 联赛代码列表 (如['PL', 'SA', 'BL1'])
        
        Returns:
            比赛DataFrame
        """
        if competitions is None:
            competitions = ['PL', 'SA', 'BL1', 'FR1', 'IT1']
        
        logger.info("📊 Stage 1: Data Collection (API & cache)")
        all_matches = []
        
        for comp in competitions:
            logger.info(f"  Fetching matches for {comp}...")
            try:
                # 检查缓存
                cached = self.cache.get(f"matches_{comp}")
                if cached:
                    logger.info(f"  Using cached data for {comp}")
                    all_matches.extend(cached)
                    continue
                
                # API拉取数据，增加异常捕获，单联赛失败不影响整体
                matches = self.data_aggregator.fdb.get_matches(comp)
                if matches and len(matches) > 0:
                    self.cache.set(f"matches_{comp}", matches)
                    all_matches.extend(matches)
                    logger.info(f"  Successfully fetched {len(matches)} matches for {comp}")
                else:
                    logger.warning(f"  No matches found for {comp}")
            except Exception as e:
                logger.error(f"  Failed to fetch matches for {comp}: {e}", exc_info=True)
                continue
        
        # 空数据兜底，避免转换DataFrame时报错
        if not all_matches or len(all_matches) == 0:
            logger.warning("❌ No matches collected from all competitions")
            return pd.DataFrame()
        
        # 转换为DataFrame，增加字段兜底
        matches_df = pd.DataFrame([
            {
                'id': m.get('id', ''),
                'date': m.get('utcDate', m.get('date', datetime.now().isoformat())),
                'league': m.get('competition', {}).get('code', comp),
                'home_team': m.get('homeTeam', {}).get('name', '未知主队'),
                'away_team': m.get('awayTeam', {}).get('name', '未知客队'),
                'status': m.get('status', 'SCHEDULED'),
                'home_goals': m.get('score', {}).get('fullTime', {}).get('home', np.nan),
                'away_goals': m.get('score', {}).get('fullTime', {}).get('away', np.nan),
            }
            for m in all_matches
        ])
        
        logger.info(f"✅ Collected {len(matches_df)} valid matches")
        self.historical_data = matches_df
        
        # 保存到数据库，增加异常捕获
        try:
            for _, match in matches_df.iterrows():
                self.data_collector.save_match(match.to_dict())
        except Exception as e:
            logger.warning(f"Failed to save matches to database: {e}", exc_info=True)
        
        return matches_df
    
    def stage_2_load_historical_data(self, picks_path: str = "site/data/picks.json") -> pd.DataFrame:
        """
        阶段2：加载历史数据
        
        Returns:
            历史比赛DataFrame
        """
        logger.info("📚 Stage 2: Loading Historical Data")
        
        try:
            df = HistoricalDataLoader.create_dataframe_from_site_data(picks_path)
            
            if len(df) > 0:
                logger.info(f"✅ Loaded {len(df)} historical records")
                # 合并历史数据，避免覆盖新采集的比赛数据
                if self.historical_data is not None and not self.historical_data.empty:
                    self.historical_data = pd.concat([self.historical_data, df], ignore_index=True).drop_duplicates(subset=['id'])
                else:
                    self.historical_data = df
                return df
            else:
                logger.warning("⚠️ No historical data found in picks file")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"❌ Error loading historical data: {e}", exc_info=True)
            return pd.DataFrame()
    
    def stage_3_feature_engineering(self, matches_df: pd.DataFrame) -> pd.DataFrame:
        """
        阶段3：特征工程
        
        Args:
            matches_df: 比赛DataFrame
        
        Returns:
            特征DataFrame
        """
        logger.info("🔧 Stage 3: Feature Engineering")
        
        # 空数据兜底
        if matches_df.empty:
            logger.warning("❌ No matches available for feature engineering")
            return pd.DataFrame()
        
        # 历史数据兜底，避免传入None导致特征工程报错
        historical_df = self.historical_data if self.historical_data is not None and not self.historical_data.empty else matches_df
        
        features_list = []
        
        for idx, match in matches_df.iterrows():
            try:
                features = self.feature_engineer.build_match_features(
                    match.to_dict(),
                    historical_df
                )
                
                if features and len(features) > 0:
                    # 补充比赛ID，方便后续关联
                    features['match_id'] = match.get('id', f'unknown_{idx}')
                    features_list.append(features)
                    
            except Exception as e:
                logger.error(f"Error extracting features for match {idx} (id:{match.get('id')}): {e}", exc_info=True)
                continue
        
        # 空特征兜底
        if not features_list or len(features_list) == 0:
            logger.warning("❌ No features extracted from matches")
            return pd.DataFrame()
        
        features_df = pd.DataFrame(features_list)
        # 缺失值填充，避免模型预测报错
        features_df = features_df.replace([np.inf, -np.inf], np.nan).fillna(0)
        
        logger.info(f"✅ Extracted valid features for {len(features_df)} matches")
        logger.info(f"   Features shape: {features_df.shape}")
        
        self.features = features_df
        return features_df
    
    def stage_4_train_models(self, matches_df: pd.DataFrame, features_df: pd.DataFrame) -> MetaLearner:
        """
        阶段4：训练机器学习模型 (可选)
        
        仅当有足够历史数据时执行
        """
        logger.info("🧠 Stage 4: Training ML Models (Optional)")
        
        # 检查是否有足够的历史数据进行训练
        if len(features_df) < 100:
            logger.warning("⚠️ Insufficient data for model training (need >= 100 samples)")
            logger.info("   Skipping ML model training, using ensemble predictions only")
            return None
        
        try:
            # 标签生成：使用真实比赛结果，无结果时跳过训练
            if 'home_goals' not in matches_df.columns or 'away_goals' not in matches_df.columns:
                logger.warning("⚠️ No match result data available for training, skipping")
                return None
            
            # 生成真实标签，过滤无结果的比赛
            valid_matches = matches_df.dropna(subset=['home_goals', 'away_goals'])
            if len(valid_matches) < 100:
                logger.warning("⚠️ Insufficient valid match results for training, skipping")
                return None
            
            # 生成标签：主队胜=win，平=draw，客队胜=loss
            y = []
            for _, row in valid_matches.iterrows():
                if row['home_goals'] > row['away_goals']:
                    y.append('win')
                elif row['home_goals'] == row['away_goals']:
                    y.append('draw')
                else:
                    y.append('loss')
            y_series = pd.Series(y)
            
            # 对齐特征和标签
            train_features = features_df.iloc[valid_matches.index]
            
            logger.info(f"   Training XGBoost ensemble with {len(train_features)} valid samples...")
            self.meta_learner.train_all_models(train_features, y_series)
            
            logger.info("✅ All models trained successfully")
            self.fusion_model.load_meta_learner(self.meta_learner)
            
            return self.meta_learner
            
        except Exception as e:
            logger.error(f"❌ Error training models: {e}", exc_info=True)
            logger.info("   Continuing without ML models...")
            return None
    
    def stage_5_generate_predictions(
        self,
        matches_df: pd.DataFrame,
        features_df: pd.DataFrame
    ) -> List[Dict]:
        """
        阶段5：生成预测
        
        Args:
            matches_df: 比赛DataFrame
            features_df: 特征DataFrame
        
        Returns:
            预测结果列表
        """
        logger.info("🔮 Stage 5: Generating Predictions")
        
        # 空数据兜底
        if matches_df.empty or features_df.empty:
            logger.warning("❌ No matches or features available for prediction")
            return []
        
        predictions = []
        # 对齐比赛和特征，确保一一对应
        match_feature_map = {row['match_id']: row for _, row in features_df.iterrows()}
        
        for idx, (_, match) in enumerate(matches_df.iterrows()):
            try:
                match_id = match.get('id', f'unknown_{idx}')
                # 跳过无对应特征的比赛
                if match_id not in match_feature_map:
                    logger.warning(f"   No features found for match {match_id}, skipping")
                    continue
                
                features = match_feature_map[match_id]
                # 融合模型预测
                prediction = self.fusion_model.predict_single_match(
                    match.to_dict(),
                    features
                )
                # 补充比赛基础信息，方便后续使用
                prediction['match_id'] = match_id
                prediction['home_team'] = match.get('home_team', '未知主队')
                prediction['away_team'] = match.get('away_team', '未知客队')
                prediction['league'] = match.get('league', '未知联赛')
                prediction['match_date'] = match.get('date', datetime.now().isoformat())
                
                predictions.append(prediction)
                
                # 每10个打印进度
                if (idx + 1) % 10 == 0:
                    logger.info(f"   Processed {idx + 1}/{len(matches_df)} matches")
                    
            except Exception as e:
                logger.error(f"Error predicting match {idx} (id:{match.get('id')}): {e}", exc_info=True)
                continue
        
        logger.info(f"✅ Generated valid predictions for {len(predictions)} matches")
        self.predictions = predictions
        
        return predictions
    
    def stage_6_filter_top_picks(self, predictions: List[Dict], min_ev: float = 0.05) -> List[Dict]:
        """
        阶段6：筛选顶级推荐
        
        Args:
            predictions: 预测列表
            min_ev: 最小EV阈值（小数，0.05=5%）
        
        Returns:
            筛选后的推荐列表
        """
        logger.info("🏆 Stage 6: Filtering Top Picks")
        
        # 空数据兜底
        if not predictions or len(predictions) == 0:
            logger.warning("❌ No predictions available for filtering")
            return []
        
        top_picks = []
        min_ev_percent = min_ev * 100  # 转换为百分比，和预测结果中的EV单位对齐
        
        for pred in predictions:
            try:
                ev = pred.get('expected_value', 0)
                confidence = pred.get('confidence', 0)
                
                # 筛选条件：EV > 阈值 且置信度 > 50%，增加空值兜底
                if ev > min_ev_percent and confidence >= 50 and confidence <= 100:
                    top_picks.append(pred)
                    
            except Exception as e:
                logger.warning(f"Error filtering prediction: {e}", exc_info=True)
                continue
        
        # 按EV降序排序
        if len(top_picks) > 0:
            top_picks.sort(key=lambda x: x.get('expected_value', 0), reverse=True)
        
        logger.info(f"✅ Found {len(top_picks)} top picks (EV > {min_ev_percent}%)")
        
        return top_picks
    
    def stage_7_export_results(
        self,
        predictions: List[Dict],
        top_picks: List[Dict],
        output_dir: str = "site/data"
    ) -> Tuple[str, str]:
        """
        阶段7：导出结果
        
        Args:
            predictions: 所有预测列表
            top_picks: 顶级推荐列表
            output_dir: 输出目录
        
        Returns:
            (predictions_path, picks_path)
        """
        logger.info("💾 Stage 7: Exporting Results")
        
        # 创建输出目录，不存在则自动创建
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        try:
            # 导出完整预测
            predictions_path = f"{output_dir}/complete_predictions.json"
            with open(predictions_path, 'w', encoding='utf-8') as f:
                json.dump(predictions, f, ensure_ascii=False, indent=2, default=str)
            logger.info(f"   Exported {len(predictions)} predictions to {predictions_path}")
            
            # 导出顶级推荐
            picks_path = f"{output_dir}/picks_updated.json"
            with open(picks_path, 'w', encoding='utf-8') as f:
                json.dump(top_picks, f, ensure_ascii=False, indent=2, default=str)
            logger.info(f"   Exported {len(top_picks)} top picks to {picks_path}")
            
            # 生成统计报告
            stats = self._generate_stats_report(predictions, top_picks)
            stats_path = f"{output_dir}/analysis_stats.json"
            with open(stats_path, 'w', encoding='utf-8') as f:
                json.dump(stats, f, ensure_ascii=False, indent=2)
            logger.info(f"   Generated statistics report: {stats_path}")
            
            logger.info("✅ All results exported successfully")
            
            return predictions_path, picks_path
            
        except Exception as e:
            logger.error(f"❌ Error exporting results: {e}", exc_info=True)
            return None, None
    
    def _generate_stats_report(self, all_preds, top_picks) -> Dict:
        """生成统计报告，彻底修复除以零、空列表计算警告"""
        # 预计算所有指标的列表，增加空值兜底
        all_confidence = [p.get('confidence', 0) for p in all_preds if p.get('confidence') is not None]
        all_win_prob = [p.get('final_prediction', {}).get('win_prob', 0) for p in all_preds if p.get('final_prediction') is not None]
        top_ev = [p.get('expected_value', 0) for p in top_picks if p.get('expected_value') is not None]
        all_ev = [p.get('expected_value', 0) for p in all_preds if p.get('expected_value') is not None]
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'total_predictions': len(all_preds),
            'top_picks_count': len(top_picks),
            # 修复：空列表时返回0，避免np.mean空列表警告
            'avg_confidence': np.mean(all_confidence) if len(all_confidence) > 0 else 0,
            'avg_win_probability': np.mean(all_win_prob) if len(all_win_prob) > 0 else 0,
            'avg_expected_value': np.mean(top_ev) if len(top_ev) > 0 else 0,
            'max_expected_value': max(all_ev, default=0),
            'top_picks_ratio': round((len(top_picks)/len(all_preds))*100, 2) if len(all_preds) > 0 else 0,
            'model_weights': {
                'poisson': '20%',
                'xgboost': '25%',
                'dnn': '25%',
                'elo': '15%',
                'xg_model': '10%',
                'home_bias': '5%'
            },
            'data_sources': [
                'Football-data.org',
                'Understat (xG)',
                'Sofascore',
                'The Odds API',
                'Historical picks'
            ]
        }
        return report
    
    def run_full_pipeline(
        self,
        run_scrapers: bool = False,
        stage_load_historical: bool = True,
        stage_train_models: bool = False,
        competitions: List[str] = None,
        min_ev_threshold: float = 0.05
    ) -> Dict:
        """
        运行完整管道
        
        Args:
            run_scrapers: 是否先运行500网/澳客爬虫刷新数据
            stage_load_historical: 是否加载历史数据
            stage_train_models: 是否训练ML模型
            competitions: 联赛列表
            min_ev_threshold: 顶级推荐最小EV阈值（小数）
        
        Returns:
            包含所有结果的字典
        """
        logger.info("=" * 60)
        logger.info("🚀 STARTING FULL PREDICTION PIPELINE")
        logger.info("=" * 60)
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'status': 'running',
            'stages_completed': [],
            'error': None,
            'warning': None
        }
        
        try:
            # Stage 0: 运行爬虫（可选）
            if run_scrapers:
                self.stage_0_scrape_external_data()
                results['stages_completed'].append('external_scrape')

            # Stage 1: 收集数据
            matches_df = self.stage_1_collect_data(competitions)
            results['stages_completed'].append('data_collection')
            results['matches_count'] = len(matches_df)
            
            # 核心兜底：0场比赛时，提前终止，避免后续报错
            if matches_df.empty:
                error_msg = 'No matches collected from API, please check API key validity, network connection and request quota'
                logger.error(f"❌ Pipeline terminated: {error_msg}")
                results['status'] = 'completed_with_warning'
                results['warning'] = error_msg
                results['duration_minutes'] = round((datetime.now() - datetime.fromisoformat(results['timestamp'])).total_seconds() / 60, 2)
                return results
            
            # Stage 2: 加载历史数据 (可选)
            if stage_load_historical:
                self.stage_2_load_historical_data()
                results['stages_completed'].append('historical_data_loading')
            
            # Stage 3: 特征工程
            features_df = self.stage_3_feature_engineering(matches_df)
            results['stages_completed'].append('feature_engineering')
            results['features_shape'] = features_df.shape
            
            # 特征为空兜底
            if features_df.empty:
                error_msg = 'No valid features extracted from matches'
                logger.error(f"❌ Pipeline terminated: {error_msg}")
                results['status'] = 'completed_with_warning'
                results['warning'] = error_msg
                results['duration_minutes'] = round((datetime.now() - datetime.fromisoformat(results['timestamp'])).total_seconds() / 60, 2)
                return results
            
            # Stage 4: 训练模型 (可选)
            if stage_train_models:
                self.stage_4_train_models(matches_df, features_df)
                results['stages_completed'].append('model_training')
            
            # Stage 5: 生成预测
            all_predictions = self.stage_5_generate_predictions(matches_df, features_df)
            results['stages_completed'].append('prediction_generation')
            results['predictions_count'] = len(all_predictions)
            
            # 预测为空兜底
            if not all_predictions:
                error_msg = 'No valid predictions generated from matches'
                logger.error(f"❌ Pipeline terminated: {error_msg}")
                results['status'] = 'completed_with_warning'
                results['warning'] = error_msg
                results['duration_minutes'] = round((datetime.now() - datetime.fromisoformat(results['timestamp'])).total_seconds() / 60, 2)
                return results
            
            # Stage 6: 筛选顶级推荐
            top_picks = self.stage_6_filter_top_picks(all_predictions, min_ev=min_ev_threshold)
            results['stages_completed'].append('filtering_top_picks')
            results['top_picks_count'] = len(top_picks)
            
            # Stage 7: 导出结果
            pred_path, picks_path = self.stage_7_export_results(all_predictions, top_picks)
            results['stages_completed'].append('result_export')
            results['predictions_file_path'] = pred_path
            results['top_picks_file_path'] = picks_path
            
            # 最终状态更新
            results['status'] = 'completed'
            results['duration_minutes'] = round((datetime.now() - datetime.fromisoformat(results['timestamp'])).total_seconds() / 60, 2)
            
            logger.info("=" * 60)
            logger.info("✅ PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 60)
            logger.info(f"📊 Final Results Summary:")
            logger.info(f"   - Total matches processed: {results['matches_count']}")
            logger.info(f"   - Total valid predictions: {results['predictions_count']}")
            logger.info(f"   - Top value picks: {results['top_picks_count']}")
            # 修复：彻底解决除以零报错
            if results['predictions_count'] > 0:
                top_ratio = round((results['top_picks_count'] / results['predictions_count']) * 100, 1)
                logger.info(f"   - Top picks ratio: {top_ratio}%")
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ Pipeline failed: {error_msg}", exc_info=True)
            results['status'] = 'failed'
            results['error'] = error_msg
            results['duration_minutes'] = round((datetime.now() - datetime.fromisoformat(results['timestamp'])).total_seconds() / 60, 2)
        
        return results


# 主执行函数
def main():
    """主函数，启动完整预测管道"""
    
    # 初始化管道，自动读取环境变量密钥，无需手动填写
    # 启动前先执行密钥验证，和api_integrations.py对齐
    logger.info("=== 启动前密钥验证 ===")
    validate_and_get_api_keys()
    
    pipeline = FootballPredictionPipeline()
    
    # 运行完整管道，可根据需求修改参数
    results = pipeline.run_full_pipeline(
        run_scrapers=True,            # 先刷新500网/澳客爬虫数据
        stage_load_historical=True,   # 加载历史数据提升特征质量
        stage_train_models=False,     # 有≥100场历史结果数据时可设为True
        competitions=['PL', 'SA', 'BL1', 'FR1', 'IT1', 'JCZQ'],  # 目标联赛列表
        min_ev_threshold=0.05         # 仅保留EV≥5%的推荐
    )
    
    # 输出最终执行报告
    print("\n" + "=" * 60)
    print("PIPELINE FINAL EXECUTION REPORT")
    print("=" * 60)
    print(json.dumps(results, indent=2, ensure_ascii=False))
    print("=" * 60)


if __name__ == "__main__":
    main()
