import json, re, sys
from pathlib import Path
from urllib.parse import urlparse
import requests
from requests.exceptions import SSLError
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from playwright.sync_api import sync_playwright

KEYWORDS = ["match","odds","lottery","jczq","竞彩","sp","bonus","home","away","主队","客队","赔率"]

def safe_headers(h: dict) -> dict:
    # 默认去掉可能敏感的头（避免你 commit 出去）
    drop = {"cookie","authorization","x-token","token"}
    out = {}
    for k,v in h.items():
        lk = k.lower()
        if lk in drop: 
            continue
        if lk in {"user-agent","referer","accept","x-requested-with","sec-fetch-site","sec-fetch-mode","sec-fetch-dest"}:
            out[k] = v
    return out

def req_get(url: str, headers: dict, timeout=15):
    try:
        return requests.get(url, headers=headers, timeout=timeout)
    except SSLError:
        return requests.get(url, headers=headers, timeout=timeout, verify=False)

def looks_like_data(text: str) -> bool:
    t = text[:2500]
    return any(k.lower() in t.lower() for k in KEYWORDS)

def main():
    cfg_path = Path("data/jj_config.json")
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    home_url = cfg.get("home_url")
    if not home_url:
        print("home_url missing in data/jj_config.json"); sys.exit(1)

    candidates = []  # (url, headers)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        page = browser.new_page()

        def on_request(req):
            if req.resource_type in ("xhr","fetch"):
                u = req.url
                h = safe_headers(req.headers)
                candidates.append((u, h))

        page.on("request", on_request)
        page.goto(home_url, wait_until="domcontentloaded", timeout=120000)
        page.wait_for_timeout(12000)
        browser.close()

    # 去重（按 URL）
    uniq = {}
    for u,h in candidates:
        # 过滤静态资源
        if any(u.endswith(x) for x in (".js",".css",".png",".jpg",".svg",".woff",".woff2")):
            continue
        uniq[u] = h

    # 逐个试请求，找到第一个"像比赛/赔率 JSON"的
    best = None
    best_headers = None
    for u,h in list(uniq.items())[:200]:
        try:
            r = req_get(u, headers=h, timeout=12)
            ct = (r.headers.get("content-type","") or "").lower()
            txt = r.text
            if ("json" in ct or txt.strip().startswith("{") or txt.strip().startswith("[")) and looks_like_data(txt):
                best = u
                best_headers = h
                break
        except Exception:
            continue

    cfg["api_url"] = best or ""
    cfg["headers"] = best_headers or {}
    cfg_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")

    print("PROBE DONE.")
    print("api_url =", cfg["api_url"] or "(NOT FOUND)")
    print("headers =", cfg["headers"])

if __name__ == "__main__":
    main()
