import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .utils import HEADERS as BASE_HEADERS, decode_response, now_cn_date, safe_read_html, to_float

HEADERS = {**BASE_HEADERS, "Referer": "https://m.okooo.com/"}
URL = "https://m.okooo.com/jczq/"


def _guess_cols(df: pd.DataFrame) -> Dict[str, str]:
    cols = [str(c) for c in df.columns]
    low = [c.lower() for c in cols]

    def pick(*keys: str) -> str:
        for k in keys:
            for i, c in enumerate(cols):
                if k in low[i]:
                    return c
        return ""

    return {
        "league": pick("联赛", "赛事", "league"),
        "time": pick("时间", "开赛", "time"),
        "home": pick("主队", "home", "主"),
        "away": pick("客队", "away", "客"),
        "sp3": pick("sp胜", "主胜", "sp(胜)", "3"),
        "sp1": pick("sp平", "平局", "sp(平)", "1"),
        "sp0": pick("sp负", "客胜", "sp(负)", "0"),
    }


def _normalize_table(df: pd.DataFrame, date_str: str) -> pd.DataFrame:
    m = _guess_cols(df)
    out = pd.DataFrame()
    out["date"] = date_str
    out["source"] = "okooo_mobile_jczq"
    out["league"] = df[m["league"]] if m["league"] in df.columns else ""
    out["time"] = df[m["time"]] if m["time"] in df.columns else ""
    out["home"] = df[m["home"]] if m["home"] in df.columns else ""
    out["away"] = df[m["away"]] if m["away"] in df.columns else ""
    out["odds_win"] = df[m["sp3"]].map(to_float) if m["sp3"] in df.columns else None
    out["odds_draw"] = df[m["sp1"]].map(to_float) if m["sp1"] in df.columns else None
    out["odds_lose"] = df[m["sp0"]].map(to_float) if m["sp0"] in df.columns else None
    out = out[(out["home"].astype(str).str.len() > 0) & (out["away"].astype(str).str.len() > 0)]
    return out.reset_index(drop=True)


def _extract_from_rows(html: str, date_str: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "html.parser")
    rows: List[Dict[str, object]] = []

    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue

        text = " ".join(td.get_text(" ", strip=True) for td in tds)
        if not text:
            continue

        teams = re.findall(r"([\u4e00-\u9fffA-Za-z0-9·\-]{2,})", text)
        if len(teams) < 2:
            continue

        odds = [to_float(x) for x in re.findall(r"\b\d+\.\d{2}\b", text)]
        odds = [x for x in odds if x is not None]

        rows.append(
            {
                "date": date_str,
                "source": "okooo_mobile_jczq",
                "league": "竞彩",
                "time": "",
                "home": teams[0],
                "away": teams[1],
                "odds_win": odds[0] if len(odds) > 0 else None,
                "odds_draw": odds[1] if len(odds) > 1 else None,
                "odds_lose": odds[2] if len(odds) > 2 else None,
            }
        )

    if not rows:
        return pd.DataFrame(columns=["date", "source", "league", "time", "home", "away", "odds_win", "odds_draw", "odds_lose"])

    df = pd.DataFrame(rows)
    return df.drop_duplicates(subset=["date", "home", "away"]).reset_index(drop=True)


def fetch_today() -> pd.DataFrame:
    date_str = now_cn_date()
    resp = requests.get(URL, headers=HEADERS, timeout=20)
    html = decode_response(resp, default_encoding="utf-8")

    tables = safe_read_html(html)
    frames: List[pd.DataFrame] = []
    for tb in tables:
        try:
            n = _normalize_table(tb, date_str)
            if len(n) > 0:
                frames.append(n)
        except Exception:
            continue

    if frames:
        out = pd.concat(frames, ignore_index=True)
        return out.drop_duplicates(subset=["date", "home", "away"]).reset_index(drop=True)

    return _extract_from_rows(html, date_str)


def export_today() -> Dict[str, object]:
    df = fetch_today()
    return {
        "meta": {
            "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "source": URL,
            "count": int(len(df)),
        },
        "matches": df.to_dict("records"),
    }


def main() -> None:
    out = Path("site/data/jczq_okooo.json")
    out.parent.mkdir(parents=True, exist_ok=True)

    old = None
    if out.exists():
        try:
            old = json.loads(out.read_text(encoding="utf-8"))
        except Exception:
            old = None

    try:
        payload = export_today()
        if payload.get("meta", {}).get("count", 0) == 0 and old:
            old.setdefault("meta", {})["note"] = "Fetch returned 0, kept previous data"
            payload = old
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print("OK jczq_okooo.json count=", payload.get("meta", {}).get("count"))
    except Exception as exc:
        if old:
            old.setdefault("meta", {})["error"] = str(exc)
            out.write_text(json.dumps(old, ensure_ascii=False, indent=2), encoding="utf-8")
            print("WARN kept old jczq_okooo.json, error=", exc)
        else:
            payload = {
                "meta": {
                    "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "source": URL,
                    "count": 0,
                    "error": str(exc),
                },
                "matches": [],
            }
            out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            print("ERROR wrote empty jczq_okooo.json, error=", exc)


if __name__ == "__main__":
    main()
