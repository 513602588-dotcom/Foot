"""
顶级机器学习模型 - XGBoost + Deep Learning
用于高精度足球比赛预测
"""

import numpy as np
import pandas as pd
from typing import Tuple, Dict, List
import logging
from joblib import dump, load
import pickle

try:
    import xgboost as xgb
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import log_loss, accuracy_score, roc_auc_score
except ImportError:
    print("Warning: Some ML libraries not installed. Install with: pip install xgboost scikit-learn")

try:
    import tensorflow as tf
    from tensorflow import keras
    from tensorflow.keras import layers
except ImportError:
    print("Warning: TensorFlow not installed. Install with: pip install tensorflow")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class XGBoostEnsemble:
    """XGBoost集合模型 - 分别预测胜/平/负"""
    
    def __init__(self, depth: int = 6, lr: float = 0.05, rounds: int = 200):
        self.depth = depth
        self.lr = lr
        self.rounds = rounds
        self.models = {'win': None, 'draw': None, 'loss': None}
        self.scaler = StandardScaler()
        self.feature_names = []
    
    def _prepare_data(self, X: pd.DataFrame, y: pd.Series = None) -> Tuple[np.ndarray, np.ndarray, list]:
        """准备和标准化数据"""
        features = X.select_dtypes(include=[np.number]).fillna(0)
        self.feature_names = features.columns.tolist()
        X_scaled = self.scaler.fit_transform(features) if y is not None else self.scaler.transform(features)
        
        if y is not None:
            return X_scaled, y.values, self.feature_names
        return X_scaled, None, self.feature_names
    
    def train(self, X: pd.DataFrame, y: pd.Series):
        """训练三个二分类XGBoost模型"""
        X_scaled, y_vals, _ = self._prepare_data(X, y)
        
        for outcome, label in [('win', 1), ('draw', 2), ('loss', 3)]:
            logger.info(f"Training XGBoost for {outcome} outcome...")
            
            # 创建二分类标签
            y_binary = (y_vals == label).astype(int)
            
            # 分割数据
            X_train, X_test, y_train, y_test = train_test_split(
                X_scaled, y_binary, test_size=0.2, random_state=42
            )
            
            # 训练XGBoost
            model = xgb.XGBClassifier(
                max_depth=self.depth,
                learning_rate=self.lr,
                n_estimators=self.rounds,
                objective='binary:logistic',
                eval_metric='logloss',
                random_state=42,
                n_jobs=-1,
                use_label_encoder=False
            )
            
            model.fit(
                X_train, y_train,
                eval_set=[(X_test, y_test)],
                early_stopping_rounds=20,
                verbose=False
            )
            
            # 评估
            y_pred_proba = model.predict_proba(X_test)[:, 1]
            auc = roc_auc_score(y_test, y_pred_proba)
            logger.info(f"  {outcome.upper()} model AUC: {auc:.4f}")
            
            self.models[outcome] = model
    
    def predict_proba(self, X: pd.DataFrame) -> Dict[str, np.ndarray]:
        """预测概率"""
        X_scaled, _, _ = self._prepare_data(X)
        
        probs = {}
        for outcome in ['win', 'draw', 'loss']:
            if self.models[outcome]:
                probs[outcome] = self.models[outcome].predict_proba(X_scaled)[:, 1]
        
        # 正则化为概率和为1
        total = np.array([probs.get(k, 0) for k in ['win', 'draw', 'loss']]).sum(axis=0)
        for key in ['win', 'draw', 'loss']:
            if key in probs:
                probs[key] = probs[key] / np.maximum(total, 1e-6)
        
        return probs
    
    def save(self, path: str):
        """保存模型"""
        dump(self, path)
        logger.info(f"Model saved to {path}")
    
    @staticmethod
    def load(path: str):
        """加载模型"""
        return load(path)


class DeepNeuralNetwork:
    """深度神经网络模型"""
    
    def __init__(self, input_dim: int = None):
        self.model = None
        self.input_dim = input_dim
        self.scaler = StandardScaler()
    
    def build(self, input_dim: int, hidden_units: List[int] = None):
        """构建神经网络"""
        if hidden_units is None:
            hidden_units = [256, 128, 64]
        
        self.input_dim = input_dim
        
        model = keras.Sequential([
            layers.Input(shape=(input_dim,)),
            layers.Dense(hidden_units[0], activation='relu'),
            layers.BatchNormalization(),
            layers.Dropout(0.3),
            
            layers.Dense(hidden_units[1], activation='relu'),
            layers.BatchNormalization(),
            layers.Dropout(0.3),
            
            layers.Dense(hidden_units[2], activation='relu'),
            layers.BatchNormalization(),
            layers.Dropout(0.2),
            
            # 三个输出头：胜/平/负
            layers.Dense(3, activation='softmax')
        ])
        
        model.compile(
            optimizer=keras.optimizers.Adam(learning_rate=0.001),
            loss='sparse_categorical_crossentropy',
            metrics=['accuracy']
        )
        
        self.model = model
        logger.info(f"Built DNN with {input_dim} input features")
        return model
    
    def train(self, X: pd.DataFrame, y: pd.Series, epochs: int = 50, batch_size: int = 32):
        """训练模型"""
        X_scaled = self.scaler.fit_transform(X.select_dtypes(include=[np.number]).fillna(0))
        
        # 将结果转换为类别标签 (0=loss, 1=draw, 2=win)
        y_encoded = pd.Categorical(y, categories=['loss', 'draw', 'win']).codes
        
        if self.model is None:
            self.build(X_scaled.shape[1])
        
        history = self.model.fit(
            X_scaled, y_encoded,
            epochs=epochs,
            batch_size=batch_size,
            validation_split=0.2,
            verbose=1,
            callbacks=[
                keras.callbacks.EarlyStopping(monitor='val_loss', patience=10),
                keras.callbacks.ReduceLROnPlateau(monitor='val_loss', factor=0.5, patience=5)
            ]
        )
        
        return history
    
    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        """预测概率"""
        X_scaled = self.scaler.transform(X.select_dtypes(include=[np.number]).fillna(0))
        return self.model.predict(X_scaled, verbose=0)
    
    def save(self, path: str):
        """保存模型"""
        if self.model:
            self.model.save(path)
            logger.info(f"DNN model saved to {path}")
    
    @staticmethod
    def load(path: str):
        """加载模型"""
        model = keras.models.load_model(path)
        dnn = DeepNeuralNetwork()
        dnn.model = model
        return dnn


class SupportVectorModel:
    """支持向量机（用于平手预测）"""
    
    def __init__(self):
        try:
            from sklearn.svm import SVC
            self.SVC = SVC
            self.model = None
            self.scaler = StandardScaler()
        except ImportError:
            logger.error("scikit-learn not installed")
    
    def train(self, X: pd.DataFrame, y: pd.Series):
        """训练SVM"""
        X_scaled = self.scaler.fit_transform(X.select_dtypes(include=[np.number]).fillna(0))
        y_binary = (y == 'draw').astype(int)
        
        self.model = self.SVC(kernel='rbf', C=1.0, probability=True)
        self.model.fit(X_scaled, y_binary)
        logger.info("SVM trained for draw prediction")
    
    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        """预测平手概率"""
        X_scaled = self.scaler.transform(X.select_dtypes(include=[np.number]).fillna(0))
        return self.model.predict_proba(X_scaled)[:, 1]


class MetaLearner:
    """元学习器 - 融合多个基模型"""
    
    def __init__(self):
        self.xgb_model = None
        self.dnn_model = None
        self.svm_model = None
        self.weights = {'xgb': 0.4, 'dnn': 0.4, 'svm': 0.2}
    
    def train_all_models(self, X: pd.DataFrame, y: pd.Series):
        """训练所有基模型"""
        logger.info("Training XGBoost ensemble...")
        self.xgb_model = XGBoostEnsemble()
        self.xgb_model.train(X, y)
        
        logger.info("Training Deep Neural Network...")
        self.dnn_model = DeepNeuralNetwork()
        self.dnn_model.build(X.select_dtypes(include=[np.number]).shape[1])
        self.dnn_model.train(X, y)
        
        logger.info("Training SVM for draw prediction...")
        self.svm_model = SupportVectorModel()
        self.svm_model.train(X, y)
        
        logger.info("All base models trained successfully")
    
    def predict(self, X: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        融合多个模型的预测
        返回: (win_prob, draw_prob, loss_prob)
        """
        predictions = {}
        
        # XGBoost预测
        if self.xgb_model:
            xgb_probs = self.xgb_model.predict_proba(X)
            predictions['xgb_win'] = xgb_probs.get('win', np.zeros(len(X)))
            predictions['xgb_draw'] = xgb_probs.get('draw', np.zeros(len(X)))
            predictions['xgb_loss'] = xgb_probs.get('loss', np.zeros(len(X)))
        
        # DNN预测
        if self.dnn_model:
            dnn_probs = self.dnn_model.predict_proba(X)
            predictions['dnn_loss'] = dnn_probs[:, 0]
            predictions['dnn_draw'] = dnn_probs[:, 1]
            predictions['dnn_win'] = dnn_probs[:, 2]
        
        # SVM平手预测强化
        if self.svm_model:
            predictions['svm_draw'] = self.svm_model.predict_proba(X)
        
        # 融合
        n_samples = len(X)
        win_prob = np.zeros(n_samples)
        draw_prob = np.zeros(n_samples)
        loss_prob = np.zeros(n_samples)
        
        weight_sum = 0
        
        if self.xgb_model:
            win_prob += predictions.get('xgb_win', 0) * self.weights['xgb']
            draw_prob += predictions.get('xgb_draw', 0) * self.weights['xgb']
            loss_prob += predictions.get('xgb_loss', 0) * self.weights['xgb']
            weight_sum += self.weights['xgb']
        
        if self.dnn_model:
            win_prob += predictions.get('dnn_win', 0) * self.weights['dnn']
            draw_prob += predictions.get('dnn_draw', 0) * self.weights['dnn'] * 1.2  # 加权平手
            loss_prob += predictions.get('dnn_loss', 0) * self.weights['dnn']
            weight_sum += self.weights['dnn']
        
        if self.svm_model:
            draw_prob += predictions.get('svm_draw', 0) * self.weights['svm']
            weight_sum += self.weights['svm'] * 0.5
        
        # 正则化为概率
        win_prob = win_prob / weight_sum
        draw_prob = draw_prob / weight_sum
        loss_prob = loss_prob / weight_sum
        
        # 确保和为1
        total = win_prob + draw_prob + loss_prob
        win_prob /= total
        draw_prob /= total
        loss_prob /= total
        
        return win_prob, draw_prob, loss_prob
    
    def save(self, base_path: str):
        """保存所有模型"""
        if self.xgb_model:
            self.xgb_model.save(f"{base_path}_xgb.pkl")
        if self.dnn_model:
            self.dnn_model.save(f"{base_path}_dnn.h5")
        logger.info(f"Meta-learner models saved to {base_path}")
