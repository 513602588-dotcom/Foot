import sys
import os
# ==================== 关键修复：自动把项目根目录加入模块路径 ====================
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import pandas as pd
from datetime import datetime, timezone, timedelta
import warnings
warnings.filterwarnings("ignore")

# === 你的全部顶级模型（Poisson+Elo+RF+MLP+融合+EV/Kelly）===
from src.data.sources import LEAGUES, season_code_for, prev_season, fetch_league, split_played_future, fetch_fixtures_fallback, pick_1x2_odds
from src.models.poisson_elo import run_elo, fit_poisson, FitModels, predict as predict_pe
from src.models.ml_ensemble import train_models, compute_latest_team_form, predict_proba
from src.models.bookmaker import predict_from_odds
from src.models.upset import avoid_upset
from src.engine.value import implied_prob, remove_overround, calc, score, label
from src.backtest.backtest import backtest

FUTURE_WINDOW_DAYS = 7
EV_THRESHOLD_BT = 0.03
W_PE = 0.50
W_ML = 0.30
W_BM = 0.20

def fuse_probs(pe, ml=None, weights=None):
    if weights is None:
        w_pe, w_ml, w_bm = W_PE, W_ML, 0.0
    else:
        w_pe, w_ml, w_bm = weights
    ph = w_pe * pe[0] + (w_ml * ml[0] if ml else 0)
    pd_ = w_pe * pe[1] + (w_ml * ml[1] if ml else 0)
    pa = w_pe * pe[2] + (w_ml * ml[2] if ml else 0)
    s = ph + pd_ + pa
    return (ph/s, pd_/s, pa/s) if s > 0 else pe

def main():
    os.makedirs("site/data", exist_ok=True)
    with open("site/.nojekyll", "w", encoding="utf-8") as f: f.write("")

    now = datetime.now(timezone.utc)
    print(f"🚀 开始构建 - {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")

    # 强制优先用竞彩500数据
    fx = pd.DataFrame()
    try:
        j = json.loads(open("site/data/jczq.json", "r", encoding="utf-8").read())
        ms = j.get("matches") or []
        if ms:
            fx = pd.DataFrame(ms)
            # 修复日期解析（你的jczq.json有date和time列）
            fx["Date"] = pd.to_datetime(fx["date"], errors="coerce") + pd.to_timedelta(fx.get("time","" ).str.split().str[0], errors="coerce")
            fx = fx.rename(columns={"home":"HomeTeam", "away":"AwayTeam", "league":"League", "date":"orig_date"})
            print(f"✅ 竞彩数据加载成功，共 {len(fx)} 场")
    except Exception as e:
        print("WARN jczq加载失败:", e)

    # 过滤未来7天
    if not fx.empty:
        fx["Date"] = pd.to_datetime(fx["Date"], errors="coerce")
        fx = fx.dropna(subset=["Date"])
        fx = fx[(fx["Date"] >= now.date()) & (fx["Date"] <= now.date() + timedelta(days=FUTURE_WINDOW_DAYS))]
        fx = fx.sort_values(["Date", "League", "HomeTeam"])

    # 回测历史用澳客数据（你已有）
    played_df = pd.read_csv("data/history_okooo.csv") if os.path.exists("data/history_okooo.csv") else pd.DataFrame()

    # 运行所有顶级模型
    elo = run_elo(played_df[["Date","HomeTeam","AwayTeam","FTHG","FTAG"]].copy()) if not played_df.empty else None
    m_h, m_a = fit_poisson(played_df[["HomeTeam","AwayTeam","FTHG","FTAG"]].copy()) if not played_df.empty else (None,None)
    pe_models = FitModels(home=m_h, away=m_a, elo=elo) if elo else None

    ml_models = train_models(played_df) if not played_df.empty else None
    team_form = compute_latest_team_form(played_df) if ml_models else {}

    # 预测 + 融合 + EV/Kelly
    rows = []
    for _, r in fx.iterrows():
        home, away = str(r.get("HomeTeam","")), str(r.get("AwayTeam",""))
        if not home or not away: continue

        pe = predict_pe(pe_models, home, away) if pe_models else {"p_home":0.45,"p_draw":0.3,"p_away":0.25,"xg_home":1.4,"xg_away":1.1,"most_likely_score":"2-1"}
        pe_probs = (pe.get("p_home",0.45), pe.get("p_draw",0.3), pe.get("p_away",0.25))
        ml_probs = predict_proba(ml_models, team_form, home, away) if ml_models else None

        ph, pd_, pa = fuse_probs(pe_probs, ml_probs)

        oh, od, oa = r.get("odds_win"), r.get("odds_draw"), r.get("odds_lose")
        evv = kellyv = None
        pick = "模型"
        if oh and od and oa:
            q1,qx,q2 = implied_prob(oh), implied_prob(od), implied_prob(oa)
            f1,fx_,f2 = remove_overround(q1,qx,q2)
            best = max([calc(ph,oh,f1,"主胜"), calc(pd_,od,fx_,"平"), calc(pa,oa,f2,"客胜")], key=lambda x:x.ev)
            evv = round(best.ev,4)
            kellyv = round(min(best.kelly,0.08),4)
            pick = best.pick

        rows.append({
            "date": str(r["Date"].date()) if "Date" in r else r.get("orig_date",""),
            "league": r.get("League",""),
            "home": home,
            "away": away,
            "xg_home": round(pe.get("xg_home",1.4),2),
            "xg_away": round(pe.get("xg_away",1.1),2),
            "p_home": round(ph,4),
            "p_draw": round(pd_,4),
            "p_away": round(pa,4),
            "most_likely_score": pe.get("most_likely_score","2-1"),
            "odds_win": oh,
            "odds_draw": od,
            "odds_lose": oa,
            "ev": evv,
            "kelly": kellyv,
            "pick": pick,
            "why": f"融合胜率{round(ph*100,1)}% | xG优势{round(pe.get('xg_home',1.4)-pe.get('xg_away',1.1),1)}"
        })

    top = sorted([x for x in rows if x.get("ev") and x["ev"]>0.05], key=lambda z: z["ev"], reverse=True)[:30]

    payload = {
        "meta": {"generated_at_utc": now.strftime("%Y-%m-%d %H:%M:%S UTC"), "fusion": f"PE {W_PE} + ML {W_ML}"},
        "stats": {"fixtures": len(rows), "top": len(top)},
        "top_picks": top,
        "all": rows
    }

    with open("site/data/picks.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"🎉 构建完成！Top Picks: {len(top)} 个 | 全部赛程: {len(rows)} 场")
    print("✅ 现在刷新 https://bosun4.github.io/Foot/ （Ctrl+F5硬刷新）即可看到全部数据！")

if __name__ == "__main__":
    main()
