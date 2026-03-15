"""
dailyDataScrapper.py
────────────────────
Fetches today's (or the most-recent trading day's) prices and appends
new rows to the existing per-company CSVs in OUTPUT_DIR.

Unlike the original repo version, this script:
  1. Reads the current last-date from each existing CSV
  2. Fetches fresh data from sharesansar.com today-share-price page
  3. Appends only new rows (no duplicates, no overwriting)
  4. Saves valid CSVs — no HTML artefacts

Usage:
    python dailyDataScrapper.py                 # update all known companies
    python dailyDataScrapper.py --symbols NABIL  # specific symbols
"""

import os
import csv
import sys
import argparse
import urllib3
from datetime import date, datetime
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from config import COMPANIES, OUTPUT_DIR, CSV_COLUMNS
from utils import fetch_page, ensure_dir, clean_number, parse_date, setup_logger


logger = setup_logger("dailyDataScrapper")

# sharesansar today-share-price returns a big table with all listed stocks
TODAY_URL = "https://www.sharesansar.com/today-share-price"
# Merolagani company page (as fallback, page 1 only → most-recent data)
MEROLAGANI_URL = "https://merolagani.com/CompanyDetail.aspx?symbol={symbol}"


# ─── Fetch today's prices from sharesansar ────────────────────────────────────

def fetch_today_sharesansar() -> dict[str, dict]:
    """
    Parse the full today-share-price table on sharesansar.
    Returns  {symbol -> {Date, Open, High, Low, Close, Change, Percent_Change, Volume, Turnover}}
    """
    resp = fetch_page(TODAY_URL, logger)
    if not resp:
        logger.error("Could not fetch today's data from sharesansar")
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", {"id": "headFixed"})
    if not table:
        table = soup.find("table", {"class": lambda c: c and "table" in c})
    if not table:
        logger.error("Price table not found on sharesansar today-share-price page")
        return {}

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

    today_str = date.today().strftime("%Y-%m-%d")
    data = {}

    tbody = table.find("tbody") or table
    for tr in tbody.find_all("tr"):
        cells = tr.find_all("td")
        if not cells or len(cells) < 5:
            continue

        symbol = g(cells, "symbol", "s.n.", "stock symbol")
        if not symbol or symbol.isdigit():
            # Some tables put S.N. in col 0 and symbol in col 1
            symbol = cells[1].get_text(strip=True) if len(cells) > 1 else ""

        if not symbol:
            continue

        row = {
            "Date":           parse_date(g(cells, "date", "published date") or today_str),
            "Open":           clean_number(g(cells, "open", "open price")),
            "High":           clean_number(g(cells, "high", "high price")),
            "Low":            clean_number(g(cells, "low", "low price")),
            "Close":          clean_number(g(cells, "close", "ltp", "last traded price", "close price")),
            "Change":         clean_number(g(cells, "change", "price change")),
            "Percent_Change": clean_number(g(cells, "% change", "%change", "percent change", "change%")),
            "Volume":         clean_number(g(cells, "volume", "qty", "total qty", "traded qty")),
            "Turnover":       clean_number(g(cells, "turnover", "total amount", "amount")),
        }

        if row["Close"]:
            data[symbol.upper()] = row

    logger.info(f"Fetched today's data for {len(data)} symbols from sharesansar")
    return data


# ─── Fetch latest row for a single company (fallback) ─────────────────────────

def fetch_latest_merolagani(symbol: str) -> list[dict]:
    """
    Fetch page 1 of merolagani company page and return the most-recent rows.
    Used as a fallback if sharesansar doesn't have the symbol.
    """
    from allDataScrapper import parse_table  # reuse the same parser
    url = MEROLAGANI_URL.format(symbol=symbol)
    resp = fetch_page(url, logger)
    if not resp:
        return []
    rows = parse_table(resp.text)
    return rows  # already sorted ascending on that page


# ─── CSV read / append helpers ────────────────────────────────────────────────

def read_existing_csv(filepath: str) -> tuple[list[dict], set[str]]:
    """Return (all_rows, set_of_dates_already_present)."""
    if not os.path.exists(filepath):
        return [], set()
    rows = []
    dates = set()
    with open(filepath, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
            dates.add(row.get("Date", ""))
    return rows, dates


def append_and_save(filepath: str, existing_rows: list[dict],
                    new_rows: list[dict], existing_dates: set[str]) -> int:
    """
    Append truly-new rows (by Date) to existing data, sort ascending, re-save.
    Returns number of rows actually added.
    """
    added = 0
    for row in new_rows:
        if row.get("Date") and row["Date"] not in existing_dates:
            existing_rows.append(row)
            existing_dates.add(row["Date"])
            added += 1

    if added == 0:
        return 0

    existing_rows.sort(key=lambda r: r.get("Date", ""))

    ensure_dir(os.path.dirname(filepath))
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(existing_rows)

    return added


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Daily update of NEPSE company CSVs")
    parser.add_argument("--symbols", nargs="+", help="Only update these symbols")
    parser.add_argument("--output", default=OUTPUT_DIR, help="Output directory")
    args = parser.parse_args()

    ensure_dir(args.output)
    symbols = [s.upper() for s in args.symbols] if args.symbols else list(COMPANIES.keys())

    # Step 1: grab full today-price table in one shot (efficient)
    logger.info("Fetching today's prices from sharesansar…")
    today_data = fetch_today_sharesansar()

    updated, skipped, failed = [], [], []

    for symbol in symbols:
        csv_path = os.path.join(args.output, f"{symbol}.csv")
        existing_rows, existing_dates = read_existing_csv(csv_path)

        # Prefer sharesansar bulk data; fall back to merolagani page
        if symbol in today_data:
            new_rows = [today_data[symbol]]
        else:
            logger.info(f"[{symbol}] Not in sharesansar bulk — falling back to merolagani")
            new_rows = fetch_latest_merolagani(symbol)

        if not new_rows:
            logger.warning(f"[{symbol}] No data found")
            failed.append(symbol)
            continue

        added = append_and_save(csv_path, existing_rows, new_rows, existing_dates)

        if added > 0:
            logger.info(f"[{symbol}] +{added} new row(s) saved → {csv_path}")
            updated.append(symbol)
        else:
            logger.info(f"[{symbol}] Already up-to-date, nothing to add")
            skipped.append(symbol)

    logger.info("=" * 60)
    logger.info(f"Done.  Updated: {len(updated)}  |  Already up-to-date: {len(skipped)}  |  Failed: {len(failed)}")
    if failed:
        logger.warning(f"Failed: {', '.join(failed)}")


if __name__ == "__main__":
    main()
