import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://trade.500.com/",
}

def _to_float(x: str) -> Optional[float]:
    try:
        x = (x or "").strip()
        if not x:
            return None
        v = float(x)
        return v if v > 1 else None
    except:
        return None

def fetch_one_day(date_str: str) -> List[Dict]:
    url = f"https://trade.500.com/jczq/?date={date_str}"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.encoding = "utf-8"
    soup = BeautifulSoup(r.text, "html.parser")

    matches = []
    rows = soup.find_all("tr")
    for row in rows:
        tds = row.find_all("td")
        # 页面结构可能变：这里用“至少 6 列”做兜底
        if len(tds) < 6:
            continue

        match_num = tds[0].get_text(strip=True)
        # match_num 通常长这样：周二002 / 001
        if not match_num or len(match_num) > 10:
            continue

        league_a = tds[1].find("a")
        league = league_a.get_text(strip=True) if league_a else tds[1].get_text(strip=True)

        kick_time = tds[2].get_text(strip=True)

        # 主客队通常在第 4 列（index 3）
        team_as = tds[3].find_all("a")
        home = team_as[0].get_text(strip=True) if len(team_as) >= 1 else tds[3].get_text(strip=True)
        away = team_as[-1].get_text(strip=True) if len(team_as) >= 2 else ""

        handicap = tds[4].get_text(strip=True)

        sp_td = tds[5]
        sp_spans = sp_td.find_all("span")
        sp_win = sp_spans[0].get_text(strip=True) if len(sp_spans) > 0 else ""
        sp_draw = sp_spans[1].get_text(strip=True) if len(sp_spans) > 1 else ""
        sp_lose = sp_spans[2].get_text(strip=True) if len(sp_spans) > 2 else ""

        # 统一成我们站用的字段
        matches.append({
            "date": date_str,
            "match": match_num,
            "league": league,
            "time": kick_time,
            "home": home,
            "away": away,
            "handicap": handicap,
            "odds_win": _to_float(sp_win),
            "odds_draw": _to_float(sp_draw),
            "odds_lose": _to_float(sp_lose),
            "source": "trade.500.com/jczq"
        })
    return matches

def export(days: int = 1) -> Dict:
    today = datetime.now(timezone.utc) + timedelta(hours=8)  # 按中国日期
    all_matches: List[Dict] = []
    for i in range(days):
        d = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        try:
            ms = fetch_one_day(d)
            all_matches.extend(ms)
        except Exception as e:
            # 单天失败不影响整体
            continue

    payload = {
        "meta": {
            "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "source": "trade.500.com/jczq",
            "days": days,
            "count": len(all_matches),
        },
        "matches": all_matches
    }
    return payload

def main():
    out_path = Path("site/data/jczq.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    old = None
    if out_path.exists():
        try:
            old = json.loads(out_path.read_text(encoding="utf-8"))
        except:
            old = None

    try:
        payload = export(days=1)
        # 如果抓到 0 场，保留旧数据，避免网站变空
        if payload["meta"]["count"] == 0 and old:
            payload = old
            payload.setdefault("meta", {})["note"] = "Fetch returned 0, kept previous data"
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print("OK jczq.json count=", payload.get("meta", {}).get("count"))
    except Exception as e:
        if old:
            old.setdefault("meta", {})["error"] = str(e)
            out_path.write_text(json.dumps(old, ensure_ascii=False, indent=2), encoding="utf-8")
            print("WARN kept old jczq.json, error=", e)
        else:
            payload = {"meta":{"generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                               "source":"trade.500.com/jczq","count":0,"error":str(e)},
                       "matches":[]}
            out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            print("ERROR wrote empty jczq.json, error=", e)

if __name__ == "__main__":
    main()
