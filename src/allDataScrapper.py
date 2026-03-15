"""
allDataScrapper.py
──────────────────
Scrapes full historical OHLCV data for every symbol in config.COMPANIES
from merolagani.com using ASP.NET WebForms __doPostBack mechanics.

How it works:
  1. GET  /CompanyDetail.aspx?symbol=NABIL  → extract __VIEWSTATE, __EVENTVALIDATION
  2. POST same URL with __EVENTTARGET = btnHistoryTab → loads price history table
  3. POST again with PagerControl hdnCurrentPage to paginate through all pages

Usage:
    python allDataScrapper.py                        # all companies
    python allDataScrapper.py --symbols NABIL EBL    # specific symbols
    python allDataScrapper.py --resume               # skip existing CSVs
"""

import os
import re
import csv
import time
import argparse
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from config import COMPANIES, OUTPUT_DIR, CSV_COLUMNS
from utils import fetch_page, ensure_dir, clean_number, parse_date, setup_logger, _SESSION, REQUEST_DELAY

logger = setup_logger("allDataScrapper")

BASE_URL = "https://merolagani.com/CompanyDetail.aspx?symbol={symbol}"


# ─── ASP.NET state extraction ──────────────────────────────────────────────────

def extract_aspnet_state(html: str) -> dict:
    """Pull __VIEWSTATE, __VIEWSTATEGENERATOR, __EVENTVALIDATION from HTML."""
    soup = BeautifulSoup(html, "html.parser")
    state = {}
    for field in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"):
        el = soup.find("input", {"id": field})
        if el:
            state[field] = el.get("value", "")
    return state


# ─── Postback helpers ──────────────────────────────────────────────────────────

def post_history_tab(url: str, state: dict, symbol: str) -> str | None:
    """
    Fire the 'Price History' tab click postback.
    This replicates clicking: btnHistoryTab
    """
    payload = {
        "__EVENTTARGET":    "",
        "__EVENTARGUMENT":  "",
        "__VIEWSTATE":      state.get("__VIEWSTATE", ""),
        "__VIEWSTATEGENERATOR": state.get("__VIEWSTATEGENERATOR", ""),
        "__EVENTVALIDATION": state.get("__EVENTVALIDATION", ""),
        # The hidden submit button the JS calls when History tab is clicked
        "ctl00$ContentPlaceHolder1$CompanyDetail1$btnHistoryTab": "",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$hdnStockSymbol": symbol,
        "ctl00$ContentPlaceHolder1$CompanyDetail1$hdnActiveTabID": "navHistory",
    }
    time.sleep(REQUEST_DELAY)
    try:
        resp = _SESSION.post(url, data=payload, timeout=15)
        if resp.status_code == 200:
            return resp.text
    except Exception as e:
        logger.warning(f"[{symbol}] Tab postback failed: {e}")
    return None


def post_history_page(url: str, state: dict, symbol: str, page: int) -> str | None:
    """
    Paginate through price history by submitting the pager hidden button.
    merolagani uses PagerControlTransactionHistory1 with hdnCurrentPage (0-indexed).
    """
    payload = {
        "__EVENTTARGET":    "",
        "__EVENTARGUMENT":  "",
        "__VIEWSTATE":      state.get("__VIEWSTATE", ""),
        "__VIEWSTATEGENERATOR": state.get("__VIEWSTATEGENERATOR", ""),
        "__EVENTVALIDATION": state.get("__EVENTVALIDATION", ""),
        # Pager hidden fields (page is 0-indexed)
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlTransactionHistory1$hdnCurrentPage": str(page - 1),
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlTransactionHistory1$btnPaging": "",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlTransactionHistory1$hdnPCID": "PC1",
        "ctl00$ContentPlaceHolder1$CompanyDetail1$hdnStockSymbol": symbol,
        "ctl00$ContentPlaceHolder1$CompanyDetail1$hdnActiveTabID": "navHistory",
    }
    time.sleep(REQUEST_DELAY)
    try:
        resp = _SESSION.post(url, data=payload, timeout=15)
        if resp.status_code == 200:
            return resp.text
    except Exception as e:
        logger.warning(f"[{symbol}] Page {page} postback failed: {e}")
    return None


# ─── Table parsing ─────────────────────────────────────────────────────────────

def parse_price_table(html: str) -> list[dict]:
    """
    Extract rows from the price history table inside #divHistory / #ctl00_ContentPlaceHolder1_CompanyDetail1_divDataPrice.
    The table has class 'table table-bordered table-striped table-hover'.
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = []

    # Target specifically the price history div
    container = soup.find("div", {"id": "ctl00_ContentPlaceHolder1_CompanyDetail1_divDataPrice"})
    if not container:
        container = soup.find("div", {"id": "divHistory"})
    if not container:
        container = soup  # fallback to full page

    table = container.find("table", {"class": re.compile(r"table-bordered")}) if container else None
    if not table:
        # Try any table with OHLC headers
        for t in soup.find_all("table"):
            headers = [th.get_text(strip=True).lower() for th in t.find_all("th")]
            if "open" in headers and ("close" in headers or "ltp" in headers):
                table = t
                break

    if not table:
        return rows

    headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
    if not headers:
        return rows

    col_map = {h: i for i, h in enumerate(headers)}

    def g(cells, *keys):
        for k in keys:
            idx = col_map.get(k)
            if idx is not None and idx < len(cells):
                v = cells[idx].get_text(strip=True)
                if v and v not in ("-", "—"):
                    return v
        return ""

    tbody = table.find("tbody") or table
    for tr in tbody.find_all("tr"):
        cells = tr.find_all("td")
        if not cells or len(cells) < 4:
            continue

        date_raw = g(cells, "published date", "date", "market date")
        close    = g(cells, "close", "ltp", "last traded price")

        if not date_raw or not close:
            continue

        rows.append({
            "Date":           parse_date(date_raw),
            "Open":           clean_number(g(cells, "open", "open price")),
            "High":           clean_number(g(cells, "high", "high price")),
            "Low":            clean_number(g(cells, "low", "low price")),
            "Close":          clean_number(close),
            "Change":         clean_number(g(cells, "change", "price change", "difference")),
            "Percent_Change": clean_number(g(cells, "% change", "%change", "percent change")),
            "Volume":         clean_number(g(cells, "volume", "qty", "total qty", "traded qty")),
            "Turnover":       clean_number(g(cells, "turnover", "total amount", "amount")),
        })

    return rows


def get_total_pages(html: str) -> int:
    """
    Read the pager from the price history section.
    merolagani shows 'Records: X' or pagination links.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Look for the litRecords span inside the price history pager
    lit = soup.find("span", {"id": "ctl00_ContentPlaceHolder1_CompanyDetail1_PagerControlTransactionHistory1_litRecords"})
    if lit:
        text = lit.get_text(strip=True)
        # e.g. "Records: 1 - 30 of 1200" or "Showing 1 to 30 of 1200"
        m = re.search(r"of\s+([\d,]+)", text, re.IGNORECASE)
        if m:
            total = int(m.group(1).replace(",", ""))
            return max(1, (total + 29) // 30)  # merolagani default page size = 30

    # Fallback: count numbered pagination links
    container = soup.find("div", {"id": "ctl00_ContentPlaceHolder1_CompanyDetail1_divDataPrice"})
    if container:
        nums = []
        for a in container.find_all("a"):
            t = a.get_text(strip=True)
            if t.isdigit():
                nums.append(int(t))
        if nums:
            return max(nums)

    return 1


# ─── Per-company scrape ────────────────────────────────────────────────────────

def scrape_company(symbol: str) -> list[dict]:
    url = BASE_URL.format(symbol=symbol)

    # Step 1: GET the page to obtain ASP.NET state tokens
    resp = fetch_page(url, logger)
    if not resp:
        logger.error(f"[{symbol}] Could not fetch page")
        return []

    state = extract_aspnet_state(resp.text)
    if not state.get("__VIEWSTATE"):
        logger.error(f"[{symbol}] Could not extract __VIEWSTATE")
        return []

    # Step 2: POST to activate the Price History tab
    html = post_history_tab(url, state, symbol)
    if not html:
        logger.error(f"[{symbol}] History tab postback failed")
        return []

    # Update state from the postback response (ASP.NET rotates tokens)
    state = extract_aspnet_state(html)

    rows = parse_price_table(html)
    total_pages = get_total_pages(html)
    logger.info(f"[{symbol}] Total pages: {total_pages}, page 1 rows: {len(rows)}")

    all_rows = list(rows)

    # Step 3: paginate
    for page in range(2, total_pages + 1):
        html = post_history_page(url, state, symbol, page)
        if not html:
            logger.warning(f"[{symbol}] Page {page} failed, stopping")
            break

        state = extract_aspnet_state(html)   # rotate tokens for next page
        page_rows = parse_price_table(html)

        if not page_rows:
            logger.warning(f"[{symbol}] No rows on page {page}, stopping")
            break

        all_rows.extend(page_rows)
        logger.info(f"[{symbol}] Page {page} rows: {len(page_rows)}")

    # Deduplicate and sort ascending
    seen = set()
    unique = []
    for r in all_rows:
        key = (r["Date"], r["Close"])
        if key not in seen:
            seen.add(key)
            unique.append(r)

    unique.sort(key=lambda r: r.get("Date", ""))
    return unique


# ─── CSV save ─────────────────────────────────────────────────────────────────

def save_csv(symbol: str, rows: list[dict], output_dir: str) -> str:
    ensure_dir(output_dir)
    filepath = os.path.join(output_dir, f"{symbol}.csv")
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"[{symbol}] Saved {len(rows)} rows -> {filepath}")
    return filepath


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Scrape full historical NEPSE data")
    parser.add_argument("--symbols", nargs="+", help="Scrape only these symbols")
    parser.add_argument("--resume", action="store_true", help="Skip symbols whose CSV already exists")
    parser.add_argument("--output", default=OUTPUT_DIR, help=f"Output dir (default: {OUTPUT_DIR})")
    args = parser.parse_args()

    symbols = [s.upper() for s in args.symbols] if args.symbols else list(COMPANIES.keys())
    ensure_dir(args.output)

    logger.info(f"Starting full scrape for {len(symbols)} companies")
    success, failed = [], []

    for i, symbol in enumerate(symbols, 1):
        csv_path = os.path.join(args.output, f"{symbol}.csv")

        if args.resume and os.path.exists(csv_path):
            logger.info(f"[{symbol}] Skipping (CSV exists)")
            continue

        logger.info(f"[{symbol}] ({i}/{len(symbols)}) Scraping…")
        rows = scrape_company(symbol)

        if rows:
            save_csv(symbol, rows, args.output)
            success.append(symbol)
        else:
            logger.warning(f"[{symbol}] No data retrieved")
            failed.append(symbol)

    logger.info("=" * 60)
    logger.info(f"Done.  Success: {len(success)}  |  Failed: {len(failed)}")
    if failed:
        logger.warning(f"Failed symbols: {', '.join(failed)}")


if __name__ == "__main__":
    main()