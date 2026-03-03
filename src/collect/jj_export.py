import json
from datetime import datetime, timezone
from pathlib import Path
import requests

URL = "https://webapi.sporttery.cn/gateway/jc/football/getMatchListV1.qry"

def main():
    try:
        r = requests.get(URL, timeout=20)
        data = r.json()

        matches = []
        for day in data.get("value", {}).values():
            for m in day:
                matches.append({
                    "league": m.get("leagueName"),
                    "time": m.get("matchDate")+" "+m.get("matchTime"),
                    "home": m.get("homeTeamName"),
                    "away": m.get("awayTeamName"),
                    "odds_win": None,
                    "odds_draw": None,
                    "odds_lose": None,
                    "handicap": m.get("handicap")
                })

        payload = {
            "meta": {
                "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                "source": "sporttery.cn",
                "count": len(matches)
            },
            "matches": matches
        }

        Path("site/data").mkdir(parents=True, exist_ok=True)
        Path("site/data/jczq.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

        print("OK count=", len(matches))

    except Exception as e:
        print("ERROR", e)

if __name__ == "__main__":
    main()
