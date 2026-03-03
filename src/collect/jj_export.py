import json
from datetime import datetime, timezone
from pathlib import Path
from src.collect.jj_fetch import fetch_from_config

def main():
    payload = {
        "meta": {
            "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "source": "jj.shshier.com",
        },
        "matches": []
    }
    try:
        res = fetch_from_config()
        payload["matches"] = res["matches"]
        payload["meta"]["count"] = len(payload["matches"])
    except Exception as e:
        payload["meta"]["count"] = 0
        payload["meta"]["error"] = str(e)

    Path("site/data").mkdir(parents=True, exist_ok=True)
    Path("site/data/jczq.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print("OK: site/data/jczq.json count=", payload["meta"]["count"])

if __name__ == "__main__":
    main()
