"""
indexDataScrapper.py
────────────────────
Scrapes NEPSE index history (main index + sub-indices) from sharesansar
and saves them to data/index/<index_name>.csv

Usage:
    python indexDataScrapper.py                   # all indices
    python indexDataScrapper.py --index NEPSE      # specific index
    python indexDataScrapper.py --list             # show available indices
"""

import os
import csv
import argparse
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from utils import fetch_page, ensure_dir, clean_number, parse_date, setup_logger


logger = setup_logger("indexDataScrapper")

INDEX_URL = "https://www.sharesansar.com/index-history-data"
OUTPUT_DIR = "../data/index"

INDEX_CSV_COLUMNS = ["Date", "Index_Value", "Change", "Percent_Change",
                     "Turnover", "Traded_Shares", "Transactions"]

# Known NEPSE indices (as they appear in the sharesansar dropdown)
KNOWN_INDICES = {
    "NEPSE":       "NEPSE",
    "BANKING":     "Banking Sub-Index",
    "DEVBANK":     "Development Bank Index",
    "FINANCE":     "Finance Index",
    "HYDROPOWER":  "Hydropower Index",
    "INSURANCE":   "Insurance Sub-Index",
    "MICROFINANCE":"Microfinance Index",
    "MFUND":       "Mutual Fund",
    "MFGPROC":     "Manufacturing and Processing",
    "HOTELS":      "Hotels and Tourism",
    "OTHERS":      "Others Index",
    "TRADING":     "Trading",
    "INVESTMENT":  "Investment",
    "LIFEINSURANCE": "Life Insurance",
    "NONLIFEINSURANCE": "Non Life Insurance",
}


# ─── Parsing ───────────────────────────────────────────────────────────────────

def parse_index_table(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    rows = []

    table = soup.find("table")
    if not table:
        return rows

    headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
    col_map = {h: i for i, h in enumerate(headers)}

    def g(cells, *keys):
        for k in keys:
            idx = col_map.get(k)
            if idx is not None and idx < len(cells):
                v = cells[idx].get_text(strip=True)
                if v:
                    return v
        return ""

    tbody = table.find("tbody") or table
    for tr in tbody.find_all("tr"):
        cells = tr.find_all("td")
        if not cells or len(cells) < 3:
            continue

        row = {
            "Date":          parse_date(g(cells, "date", "published date")),
            "Index_Value":   clean_number(g(cells, "index value", "close", "index", "value")),
            "Change":        clean_number(g(cells, "change", "point change")),
            "Percent_Change":clean_number(g(cells, "% change", "percent change", "change%")),
            "Turnover":      clean_number(g(cells, "turnover", "total turnover", "amount")),
            "Traded_Shares": clean_number(g(cells, "traded shares", "volume", "qty")),
            "Transactions":  clean_number(g(cells, "transactions", "no. of transactions")),
        }
        if row["Date"] and row["Index_Value"]:
            rows.append(row)

    return rows


def get_total_pages(html: str) -> int:
    from allDataScrapper import get_total_pages as _gtp
    return _gtp(html)


# ─── Per-index scrape ──────────────────────────────────────────────────────────

def scrape_index(index_key: str) -> list[dict]:
    """
    Fetch all pages of history for a given index from sharesansar.
    sharesansar uses POST or query params for index selection.
    """
    all_rows = []

    # sharesansar index history uses: ?type=<index_key>&page=<N>
    base_url = f"{INDEX_URL}?type={index_key}"
    resp = fetch_page(base_url, logger)
    if not resp:
        logger.error(f"[{index_key}] Could not fetch page 1")
        return []

    html = resp.text
    rows = parse_index_table(html)
    all_rows.extend(rows)

    total_pages = get_total_pages(html)
    logger.info(f"[{index_key}] Total pages: {total_pages}, page 1 rows: {len(rows)}")

    for page in range(2, total_pages + 1):
        page_url = f"{base_url}&page={page}"
        resp = fetch_page(page_url, logger)
        if not resp:
            break
        page_rows = parse_index_table(resp.text)
        if not page_rows:
            break
        all_rows.extend(page_rows)
        logger.info(f"[{index_key}] Page {page}: {len(page_rows)} rows")

    # Sort ascending
    all_rows.sort(key=lambda r: r.get("Date", ""))
    return all_rows


# ─── CSV save ─────────────────────────────────────────────────────────────────

def save_csv(index_key: str, rows: list[dict]):
    ensure_dir(OUTPUT_DIR)
    filepath = os.path.join(OUTPUT_DIR, f"{index_key}.csv")

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=INDEX_CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f"[{index_key}] Saved {len(rows)} rows → {filepath}")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Scrape NEPSE index historical data")
    parser.add_argument("--index", help="Specific index key (e.g. NEPSE, BANKING)")
    parser.add_argument("--list", action="store_true", help="List available index keys")
    args = parser.parse_args()

    if args.list:
        print("\nAvailable index keys:")
        for k, v in KNOWN_INDICES.items():
            print(f"  {k:<20} {v}")
        return

    indices = [args.index.upper()] if args.index else list(KNOWN_INDICES.keys())

    for index_key in indices:
        logger.info(f"Scraping index: {index_key}")
        rows = scrape_index(index_key)
        if rows:
            save_csv(index_key, rows)
        else:
            logger.warning(f"[{index_key}] No data retrieved")


if __name__ == "__main__":
    main()
