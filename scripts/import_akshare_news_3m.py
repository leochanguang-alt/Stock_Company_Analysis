import os
import re
import json
import time
import requests
import datetime as dt
from dotenv import load_dotenv


def parse_args(argv):
    symbol = "000333"
    months = 3
    max_pages = 200
    page_size = 50
    for i, a in enumerate(argv):
        if a == "--symbol" and i + 1 < len(argv):
            symbol = argv[i + 1].strip()
        if a.startswith("--symbol="):
            symbol = a.split("=", 1)[1].strip()
        if a == "--months" and i + 1 < len(argv):
            months = int(argv[i + 1])
        if a.startswith("--months="):
            months = int(a.split("=", 1)[1])
        if a == "--max-pages" and i + 1 < len(argv):
            max_pages = int(argv[i + 1])
        if a.startswith("--max-pages="):
            max_pages = int(a.split("=", 1)[1])
        if a == "--page-size" and i + 1 < len(argv):
            page_size = int(argv[i + 1])
        if a.startswith("--page-size="):
            page_size = int(a.split("=", 1)[1])
    return symbol, months, max_pages, page_size


def to_iso_cn(date_str):
    if not date_str:
        return None
    if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", date_str):
        return date_str.replace(" ", "T") + "+08:00"
    return date_str


def fetch_news(symbol, months, max_pages=200, page_size=50):
    cutoff = dt.datetime.now() - dt.timedelta(days=months * 30)
    all_items = []

    for page in range(1, max_pages + 1):
        body = {
            "uid": "",
            "keyword": symbol,
            "type": ["cmsArticleWebOld"],
            "client": "web",
            "clientType": "web",
            "clientVersion": "curr",
            "param": {
                "cmsArticleWebOld": {
                    "searchScope": "default",
                    "sort": "default",
                    "pageIndex": page,
                    "pageSize": page_size,
                    "preTag": "<em>",
                    "postTag": "</em>",
                }
            },
        }
        cb = f"jQuery{str(time.time()).replace('.', '')}_{int(time.time() * 1000)}"
        url = "https://search-api-web.eastmoney.com/search/jsonp"

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": f"https://so.eastmoney.com/News/s?keyword={symbol}",
        }
        resp = requests.get(
            url,
            params={"cb": cb, "param": json.dumps(body, ensure_ascii=False)},
            headers=headers,
            timeout=30,
        )
        text = resp.text
        if resp.status_code != 200 or not text:
            continue

        m = re.search(r"jQuery\\d+_\\d+\\((.*)\\)\\s*$", text, re.S)
        payload = m.group(1) if m else None
        if not payload:
            left = text.find("(")
            right = text.rfind(")")
            if left != -1 and right != -1 and right > left:
                payload = text[left + 1 : right]
        if not payload:
            continue
        obj = json.loads(payload)
        items = obj.get("result", {}).get("cmsArticleWebOld", [])
        if not items:
            break

        all_items.extend(items)

        last_date = items[-1].get("date")
        if last_date:
            try:
                last_dt = dt.datetime.strptime(last_date, "%Y-%m-%d %H:%M:%S")
                if last_dt < cutoff:
                    break
            except Exception:
                pass

    recent = []
    for it in all_items:
        date_str = it.get("date")
        if not date_str:
            continue
        try:
            d = dt.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
        if d >= cutoff and d <= dt.datetime.now():
            recent.append(it)
    return recent, cutoff


def build_rows(items, symbol):
    rows = []
    for it in items:
        rows.append(
            {
                "symbol": symbol,
                "news_title": it.get("title") or None,
                "news_content": it.get("content") or None,
                "published_at": to_iso_cn(it.get("date")),
                "source": it.get("mediaName") or None,
                "news_url": it.get("url") or None,
                "grade": None,
                "reason": None,
                "mkt_cap_change_1_month": None,
            }
        )
    return [r for r in rows if r["news_title"] and r["news_url"] and r["published_at"]]


def insert_supabase(rows, supabase_url, supabase_key):
    if not rows:
        print("没有可插入的新闻记录")
        return

    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
    }

    url = f"{supabase_url}/rest/v1/cn_company_news"
    batch_size = 50
    inserted = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        resp = requests.post(url, headers=headers, json=batch, timeout=60)
        if resp.status_code not in (200, 201):
            print(f"批次 {i // batch_size + 1} 插入失败: {resp.status_code} {resp.text}")
            continue
        inserted += len(batch)
        print(f"已提交 {inserted}/{len(rows)}")

    print(f"完成：提交 {inserted} 条")


def main():
    load_dotenv(dotenv_path=".env")
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not supabase_key:
        print("缺少 SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY")
        return

    symbol, months, max_pages, page_size = parse_args(os.sys.argv[1:])
    items, cutoff = fetch_news(symbol, months, max_pages=max_pages, page_size=page_size)
    rows = build_rows(items, symbol)

    print(f"抓取到 {len(rows)} 条新闻（截止日期 >= {cutoff.strftime('%Y-%m-%d')}）")
    insert_supabase(rows, supabase_url, supabase_key)


if __name__ == "__main__":
    main()
