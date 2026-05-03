#!/usr/bin/env python3
"""
Download TOTO LTD (5332.T / EDINET filer code E01172) annual 有価証券報告書 PDFs
for FY2021-FY2025 (5 years) from EDINET v2 API.

PREREQUISITE:
  1) Register for free EDINET API key at:
     https://api.edinet-fsa.go.jp/api/auth/index.html
     Click "新規利用者登録 / New User Registration" — takes 1 minute.
  2) After receiving the key by email, paste it into EDINET_KEY below,
     OR set environment variable EDINET_API_KEY before running.

Without a key, the API returns empty results.

Output: ~/Desktop/BUI_Investment_Management/5332.T/Annual.Reports/
        FY{YYYY}_TOTO_securities_report.pdf
"""
import os, requests, time
from datetime import date, timedelta

# === USER INPUT ===
EDINET_KEY = os.environ.get('EDINET_API_KEY', '')   # paste key here OR set env var

# === Config ===
TARGET_FILER_NAME_PREFIX = "TOTO"
TARGET_FILER_CODE = "E01172"        # TOTO LTD's EDINET filer code
DOC_TYPE_CODE = "120"               # 120 = 有価証券報告書 (Annual Securities Report)
YEARS = [2021, 2022, 2023, 2024, 2025]   # FY-end years (filed June of YYYY)

OUT_DIR = os.path.expanduser('~/Desktop/BUI_Investment_Management/5332.T/Annual.Reports')
os.makedirs(OUT_DIR, exist_ok=True)

API_BASE = 'https://api.edinet-fsa.go.jp/api/v2'
H = {'User-Agent': 'BUI Research bui@example.com'}

def search_doc_for_year(filing_year):
    """Search EDINET for TOTO's 有価証券報告書 filed in June of given year (FY-end YYYY-03-31)."""
    if not EDINET_KEY:
        print(f"  ⚠ No EDINET_KEY set. Trying anyway (will likely return empty).")
    # 有価証券報告書 typically filed mid-late June (within 3 months of FY-end 3/31)
    # Search a window: June 20 – June 30
    candidates = []
    for d in range(20, 31):
        date_str = f'{filing_year}-06-{d:02d}'
        params = {'date': date_str, 'type': '2'}
        if EDINET_KEY:
            params['Subscription-Key'] = EDINET_KEY
        try:
            r = requests.get(f'{API_BASE}/documents.json', params=params, headers=H, timeout=20)
            if r.status_code != 200:
                print(f"    {date_str}: HTTP {r.status_code}")
                continue
            j = r.json()
            results = j.get('results', [])
            for doc in results:
                if (doc.get('docTypeCode') == DOC_TYPE_CODE and
                    (doc.get('filerName','').upper().startswith(TARGET_FILER_NAME_PREFIX) or
                     doc.get('edinetCode') == TARGET_FILER_CODE)):
                    candidates.append({
                        'date': date_str,
                        'docID': doc['docID'],
                        'filer': doc.get('filerName',''),
                        'edinetCode': doc.get('edinetCode',''),
                        'periodStart': doc.get('periodStart'),
                        'periodEnd': doc.get('periodEnd'),
                        'docDescription': doc.get('docDescription',''),
                    })
        except Exception as e:
            print(f"    {date_str}: ERR {e}")
    return candidates

def download_pdf(doc_id, filing_year):
    """Download PDF for given docID."""
    out_path = os.path.join(OUT_DIR, f'FY{filing_year}_TOTO_securities_report.pdf')
    if os.path.exists(out_path) and os.path.getsize(out_path) > 100_000:
        print(f"    Already exists: {out_path}")
        return out_path
    params = {'type': '2'}   # type=2 = PDF (official format)
    if EDINET_KEY:
        params['Subscription-Key'] = EDINET_KEY
    url = f'{API_BASE}/documents/{doc_id}'
    r = requests.get(url, params=params, headers=H, timeout=60, stream=True)
    if r.status_code != 200:
        print(f"    Download failed: HTTP {r.status_code}")
        return None
    with open(out_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=64*1024):
            f.write(chunk)
    sz = os.path.getsize(out_path)
    print(f"    ✓ Downloaded: {out_path} ({sz/1024:.0f} KB)")
    return out_path

def main():
    print(f"\n{'='*60}\nEDINET TOTO Annual Report Downloader\n{'='*60}")
    print(f"Output dir: {OUT_DIR}")
    print(f"EDINET key: {'<SET>' if EDINET_KEY else '<NOT SET — register at https://api.edinet-fsa.go.jp/api/auth/index.html>'}")

    for yr in YEARS:
        print(f"\n--- FY{yr} (period ending {yr}-03-31, filed mid-2025/{yr}) ---")
        # FY ending 2021-03-31 was filed in mid-2021 (i.e., filing_year=2021)
        # Wait, for FY-end YYYY-03-31, the report is filed in June YYYY (3 months after)
        # So FY2021 (ending 2021-03-31) → filed June 2021
        candidates = search_doc_for_year(yr)
        if not candidates:
            print(f"  No 有価証券報告書 found for TOTO in June {yr}.")
            continue
        # Pick the one whose periodEnd is YYYY-03-31
        match = next((c for c in candidates if c.get('periodEnd') == f'{yr}-03-31'), candidates[0])
        print(f"  Found: docID={match['docID']}, periodEnd={match['periodEnd']}, filer={match['filer']}")
        download_pdf(match['docID'], yr)
        time.sleep(1)   # be polite

    print(f"\n{'='*60}\nDone.\n{'='*60}")
    print(f"\nIf nothing downloaded, register for the free EDINET API key:")
    print(f"  https://api.edinet-fsa.go.jp/api/auth/index.html")
    print(f"\nManual fallback — search and download from EDINET web UI:")
    print(f"  https://disclosure2.edinet-fsa.go.jp/WEEK0010.aspx")
    print(f"  Search for '{TARGET_FILER_NAME_PREFIX}' or filer code '{TARGET_FILER_CODE}'.")
    print(f"  Save PDFs to: {OUT_DIR}")

if __name__ == '__main__':
    main()
