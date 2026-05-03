#!/usr/bin/env python3
"""
Fetch HKEX CCASS shareholding data for 00300.HK (Midea Group H-shares)
across key dates around IPO / lockup expiry, and output CSV + comparison.
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
from datetime import datetime

URL = "https://www3.hkexnews.hk/sdw/search/searchsdw.aspx"

def _opt(soup, name):
    el = soup.find("input", {"name": name})
    return el["value"] if el and el.has_attr("value") else ""
STOCK = "00300"

# Key dates (YYYY/MM/DD). CCASS only publishes for trading days.
DATES = [
    "2025/04/22",   # Earliest retrievable (~1-yr CCASS retention limit); 5 weeks post-lockup
    "2025/06/30",   # ~3.5 months post-lockup
    "2025/09/30",   # ~6.5 months post-lockup
    "2025/12/31",   # Year-end
    "2026/04/17",   # Latest
]

OUT_DIR = os.path.expanduser("~/Desktop/BUI_Investment_Management/000333.SZ/ccass/")
os.makedirs(OUT_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def fetch_one(session, date_str):
    """Return DataFrame of participant holdings for given date (YYYY/MM/DD)."""
    # Step 1: GET to retrieve ASP.NET viewstate tokens
    r = session.get(URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    vs  = _opt(soup, "__VIEWSTATE")
    vsg = _opt(soup, "__VIEWSTATEGENERATOR")
    ev  = _opt(soup, "__EVENTVALIDATION")

    # Step 2: POST with the date + stock code
    payload = {
        "__EVENTTARGET": "btnSearch",
        "__EVENTARGUMENT": "",
        "__VIEWSTATE": vs,
        "__VIEWSTATEGENERATOR": vsg,
        "__EVENTVALIDATION": ev,
        "today": datetime.now().strftime("%Y%m%d"),
        "sortBy": "shareholding",
        "sortDirection": "desc",
        "txtShareholdingDate": date_str,
        "txtStockCode": STOCK,
        "txtStockName": "",
        "txtParticipantID": "",
        "txtParticipantName": "",
        "txtSelPartID": "",
    }
    r2 = session.post(URL, headers={**HEADERS, "Referer": URL}, data=payload, timeout=60)
    r2.raise_for_status()

    soup2 = BeautifulSoup(r2.text, "html.parser")

    # Parse "Summary" block
    summary = {}
    for div in soup2.select("div.summary-content div.col-sm-12, div.ccass-search-datetime, div.summary-value"):
        pass

    # Results grid - target table with id ending in DataTable1 or class mobile-list-body
    rows_out = []
    # Detail table: <table class="table-mobile-list"> with <tr> per participant,
    # each <td class="col-...-..."> containing a <div class="mobile-list-body">value</div>
    def td_val(tr, cls):
        td = tr.find("td", class_=lambda c: c and cls in c)
        if td is None: return ""
        mb = td.find("div", class_="mobile-list-body")
        return (mb or td).get_text(strip=True)

    table = soup2.find("table", class_=lambda c: c and "table-mobile-list" in (c or []))
    if table:
        for tr in table.find_all("tr"):
            if not tr.find("td"):
                continue
            pid = td_val(tr, "col-participant-id")
            pname = td_val(tr, "col-participant-name")
            shares = td_val(tr, "col-shareholding")
            # Careful: col-shareholding also matches col-shareholding-percent — exclude pct
            pct_td = tr.find("td", class_=lambda c: c and "col-shareholding-percent" in c)
            pct = (pct_td.find("div", class_="mobile-list-body").get_text(strip=True)
                   if pct_td and pct_td.find("div", class_="mobile-list-body") else "")
            # Re-parse shares excluding the percent td
            sh_td = tr.find("td", class_=lambda c: c and "col-shareholding" in c and "percent" not in c)
            if sh_td:
                mb = sh_td.find("div", class_="mobile-list-body")
                shares = (mb or sh_td).get_text(strip=True)
            if pid:
                rows_out.append([pid, pname, shares, pct])

    df = pd.DataFrame(rows_out, columns=["Participant_ID", "Participant_Name",
                                          "Shareholding", "Pct_of_Issued"])
    # Clean numeric
    df["Shareholding_num"] = (
        df["Shareholding"].str.replace(",", "", regex=False)
                          .str.extract(r"(\d+)")[0]
                          .astype(float)
    )
    df["Pct_num"] = (
        df["Pct_of_Issued"].str.replace("%", "", regex=False)
                            .str.replace(",", "", regex=False)
                            .str.extract(r"([\d.]+)")[0]
                            .astype(float)
    )
    return df, r2.text


def main():
    session = requests.Session()
    all_snapshots = {}

    for d in DATES:
        print(f"\n=== Fetching {d} ===")
        try:
            df, raw = fetch_one(session, d)
            print(f"  Rows parsed: {len(df)}")
            if len(df) == 0:
                # Save raw HTML for debugging
                dbg = os.path.join(OUT_DIR, f"debug_{d.replace('/','')}.html")
                with open(dbg, "w") as fh:
                    fh.write(raw)
                print(f"  No rows parsed — raw HTML saved to {dbg}")
                continue
            fn = os.path.join(OUT_DIR, f"ccass_00300_{d.replace('/','')}.csv")
            df.to_csv(fn, index=False)
            print(f"  Saved: {fn}")
            print(f"  Top 5:")
            print(df.head(5).to_string(index=False))
            all_snapshots[d] = df
            time.sleep(2)
        except Exception as e:
            print(f"  ERR: {e}")

    # Build pivot comparison on top-20 participants by latest date
    if all_snapshots:
        latest = DATES[-1]
        if latest in all_snapshots:
            top = all_snapshots[latest].nlargest(25, "Shareholding_num")
            pivot_rows = []
            for _, r in top.iterrows():
                pid = r["Participant_ID"]
                row = {"Participant_ID": pid, "Participant_Name": r["Participant_Name"]}
                for d in DATES:
                    if d in all_snapshots:
                        m = all_snapshots[d][all_snapshots[d]["Participant_ID"] == pid]
                        row[f"Shares_{d}"] = float(m["Shareholding_num"].iloc[0]) if len(m) else None
                        row[f"Pct_{d}"]    = float(m["Pct_num"].iloc[0]) if len(m) else None
                pivot_rows.append(row)
            pv = pd.DataFrame(pivot_rows)
            pv_fn = os.path.join(OUT_DIR, "ccass_00300_comparison_top25.csv")
            pv.to_csv(pv_fn, index=False)
            print(f"\n=== Comparison saved: {pv_fn} ===")
            print(pv.to_string(index=False))


if __name__ == "__main__":
    main()
