"""
dailyDataScrapper.py
────────────────────
Fetches the most-recent trading day's prices from merolagani.com
(same source and postback mechanism as allDataScrapper.py) and
appends only new rows to existing per-company CSVs.

How it works:
  1. Read last saved date from each existing CSV
  2. GET merolagani company page -> extract ASP.NET state tokens
  3. POST to activate Price History tab -> parse page 1 (most recent rows)
  4. Append only rows newer than the last saved date
  5. Save CSV — no duplicates, no overwriting

Usage:
    python dailyDataScrapper.py                  # update all companies
    python dailyDataScrapper.py --symbols NABIL   # specific symbols
"""

import os
import csv
import argparse
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from config import COMPANIES, OUTPUT_DIR, CSV_COLUMNS
from utils import fetch_page, ensure_dir, clean_number, parse_date, setup_logger, _SESSION, REQUEST_DELAY
from allDataScrapper import (
    extract_aspnet_state,
    post_history_tab,
    parse_price_table,
)

logger = setup_logger("dailyDataScrapper")

BASE_URL = "https://merolagani.com/CompanyDetail.aspx?symbol={symbol}"


# ─── Fetch latest rows for one company ────────────────────────────────────────

def fetch_latest(symbol: str) -> list[dict]:
    """
    Fire the merolagani postback for Price History tab and return
    only page 1 rows (most recent ~30 trading days).
    """
    url = BASE_URL.format(symbol=symbol)

    # Step 1: GET page to grab ASP.NET tokens
    resp = fetch_page(url, logger)
    if not resp:
        logger.error(f"[{symbol}] Could not fetch page")
        return []

    state = extract_aspnet_state(resp.text)
    if not state.get("__VIEWSTATE"):
        logger.error(f"[{symbol}] Could not extract __VIEWSTATE")
        return []

    # Step 2: POST to activate Price History tab
    html = post_history_tab(url, state, symbol)
    if not html:
        logger.error(f"[{symbol}] History tab postback failed")
        return []

    rows = parse_price_table(html)
    logger.info(f"[{symbol}] Fetched {len(rows)} recent rows from merolagani")
    return rows


# ─── CSV helpers ──────────────────────────────────────────────────────────────

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
    parser = argparse.ArgumentParser(description="Daily update of NEPSE company CSVs via merolagani")
    parser.add_argument("--symbols", nargs="+", help="Only update these symbols")
    parser.add_argument("--output", default=OUTPUT_DIR, help="Output directory")
    args = parser.parse_args()

    ensure_dir(args.output)
    symbols = [s.upper() for s in args.symbols] if args.symbols else list(COMPANIES.keys())

    logger.info(f"Starting daily update for {len(symbols)} companies")
    updated, skipped, failed = [], [], []

    for i, symbol in enumerate(symbols, 1):
        csv_path = os.path.join(args.output, f"{symbol}.csv")
        existing_rows, existing_dates = read_existing_csv(csv_path)

        logger.info(f"[{symbol}] ({i}/{len(symbols)}) Fetching latest data...")
        new_rows = fetch_latest(symbol)

        if not new_rows:
            logger.warning(f"[{symbol}] No data found")
            failed.append(symbol)
            continue

        added = append_and_save(csv_path, existing_rows, new_rows, existing_dates)

        if added > 0:
            logger.info(f"[{symbol}] +{added} new row(s) saved -> {csv_path}")
            updated.append(symbol)
        else:
            logger.info(f"[{symbol}] Already up-to-date, nothing to add")
            skipped.append(symbol)

    logger.info("=" * 60)
    logger.info(f"Done.  Updated: {len(updated)}  |  Up-to-date: {len(skipped)}  |  Failed: {len(failed)}")
    if failed:
        logger.warning(f"Failed: {', '.join(failed)}")


if __name__ == "__main__":
    main()