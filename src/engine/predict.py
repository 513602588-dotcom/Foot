import pandas as pd
import json
from datetime import datetime
from .models.poisson import predict_poisson
from .models.ml_models import train_ml_models  # 或加载pkl
# ... 其他import

class PredictEngine:
    def __init__(self):
        self.elo_ratings = {}  # 持久化elo dict
        self.models = {}       # 加载ml模型

    def run_all(self, matches_df: pd.DataFrame, historical_df=None):
        results = []
        for _, match in matches_df.iterrows():
            # 1. Elo
            home_elo = self.elo_ratings.get(match['home'], 1500)
            away_elo = self.elo_ratings.get(match['away'], 1500)
            elo_win = 1 / (1 + 10**((away_elo - home_elo - 100)/400))
            
            # 2. Poisson + Dixon-Coles简单版
            p_win, p_draw, p_lose, scoreline, xG_h, xG_a = predict_poisson(1.4, 1.1, 1.2, 1.3)  # 动态参数可从历史算
            
            # 3. ML概率（简化：用Poisson作为特征输入）
            ml_prob = (p_win + elo_win) / 2  # 占位，真实用pkl predict
            
            # 4. Ensemble融合（Grok最强）
            fusion = 0.4*p_win + 0.3*elo_win + 0.2*ml_prob + 0.1*p_win  # 可调权重
            
            # 5. EV & Kelly（当有SP时）
            if 'SP_win' in match and match['SP_win']:
                implied = 1 / float(match['SP_win'])
                ev = fusion - implied
                kelly = max(0, (fusion * (float(match['SP_win'])-1) - (1-fusion)) / (float(match['SP_win'])-1))
            else:
                ev = kelly = 0
            
            row = {
                "日期": match['日期'],
                "联赛": match['联赛'],
                "主队": match['主队'],
                "客队": match['客队'],
                "xG主": round(xG_h,2),
                "xG客": round(xG_a,2),
                "概率融合": round(fusion*100,1),
                "模型概率": f"Poisson:{round(p_win*100,1)}% / Elo:{round(elo_win*100,1)}%",
                "最可能比分": f"{scoreline[0]}-{scoreline[1]}",
                "EV": round(ev*100,1),
                "Kelly": round(kelly*100,1),
                "推荐": "价值盘" if ev > 0.05 else "观望",
                "理由": f"融合胜率{round(fusion*100,1)}% + xG优势{round(xG_h-xG_a,1)}",
                "赔率胜平负": f"{match.get('SP胜','')} / {match.get('SP平','')} / {match.get('SP负','')}"
            }
            results.append(row)
            
            # 更新Elo（模拟）
            self.elo_ratings[match['主队']] = home_elo + 10
        
        # 保存JSON（前端直接用）
        with open("site/data/predictions.json", "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=4)
        
        # Top Picks：过滤EV>0.05
        picks = [r for r in results if r["EV"] > 5]
        with open("site/data/picks.json", "w", encoding="utf-8") as f:
            json.dump(picks, f, ensure_ascii=False, indent=4)
        
        print(f"✅ 分析完成！共 {len(results)} 场，Top Picks {len(picks)} 个")
        return results