"""
companyListScrapper.py
──────────────────────
Discovers ALL symbols currently listed on NEPSE by scraping
sharesansar.com's company listing page, then updates config.py
with the fresh list.

Usage:
    python companyListScrapper.py             # print discovered companies
    python companyListScrapper.py --save      # also write companies.json
    python companyListScrapper.py --update-config   # patch config.py COMPANIES dict
"""

import os
import json
import argparse
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from utils import fetch_page, ensure_dir, setup_logger

logger = setup_logger("companyListScrapper")

COMPANY_LIST_URL = "https://www.sharesansar.com/company"
OUTPUT_JSON = "../data/companies.json"


# ─── Parsing ───────────────────────────────────────────────────────────────────

def scrape_company_list() -> list[dict]:
    """
    Scrape the company directory on sharesansar.
    Returns a list of dicts: {symbol, name, sector, url}
    """
    companies = []
    page = 1

    while True:
        url = f"{COMPANY_LIST_URL}?page={page}" if page > 1 else COMPANY_LIST_URL
        logger.info(f"Fetching company list page {page}: {url}")
        resp = fetch_page(url, logger)
        if not resp:
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table")
        if not table:
            logger.warning(f"No table found on page {page}")
            break

        rows_found = 0
        tbody = table.find("tbody") or table
        for tr in tbody.find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) < 3:
                continue

            # Extract symbol from a link like /company/NABIL
            link = tr.find("a", href=lambda h: h and "/company/" in h)
            if link:
                symbol = link["href"].split("/company/")[-1].strip().upper()
                name = link.get_text(strip=True)
            else:
                symbol = cells[0].get_text(strip=True).upper()
                name = cells[1].get_text(strip=True) if len(cells) > 1 else ""

            # Sector column (varies by site layout)
            sector = ""
            for cell in cells:
                txt = cell.get_text(strip=True)
                if txt and txt not in (symbol, name) and not txt.isdigit():
                    sector = txt
                    break

            if symbol and len(symbol) <= 10:  # plausible ticker length
                companies.append({
                    "symbol": symbol,
                    "name": name,
                    "sector": sector,
                    "url": f"https://merolagani.com/CompanyDetail.aspx?symbol={symbol}",
                })
                rows_found += 1

        logger.info(f"Page {page}: found {rows_found} companies (total so far: {len(companies)})")

        # Check for a "Next" pagination link
        next_link = soup.find("a", string=lambda t: t and "next" in t.lower())
        if not next_link or rows_found == 0:
            break

        page += 1

    # Deduplicate by symbol
    seen = set()
    unique = []
    for c in companies:
        if c["symbol"] not in seen:
            seen.add(c["symbol"])
            unique.append(c)

    unique.sort(key=lambda c: c["symbol"])
    logger.info(f"Total unique companies discovered: {len(unique)}")
    return unique


# ─── Output helpers ────────────────────────────────────────────────────────────

def save_json(companies: list[dict], path: str):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(companies, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved companies list → {path}")


def update_config(companies: list[dict], config_path: str = "config.py"):
    """
    Rewrite the COMPANIES dict in config.py with the freshly discovered list.
    """
    dict_lines = ['COMPANIES = {\n']
    for c in companies:
        symbol = c["symbol"]
        name = c["name"].replace('"', '\\"')
        dict_lines.append(f'    "{symbol}": "{name}",\n')
    dict_lines.append('}\n')

    with open(config_path, "r", encoding="utf-8") as f:
        content = f.read()

    import re
    # Replace everything between COMPANIES = { ... } (non-greedy, multiline)
    pattern = r"COMPANIES\s*=\s*\{[^}]*\}"
    new_block = "".join(dict_lines)
    updated = re.sub(pattern, new_block, content, flags=re.DOTALL)

    with open(config_path, "w", encoding="utf-8") as f:
        f.write(updated)

    logger.info(f"config.py updated with {len(companies)} companies")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Discover all NEPSE-listed company symbols")
    parser.add_argument("--save", action="store_true", help="Save discovered companies to companies.json")
    parser.add_argument("--update-config", action="store_true", help="Patch COMPANIES dict in config.py")
    parser.add_argument("--output", default=OUTPUT_JSON, help="Path for companies.json")
    args = parser.parse_args()

    companies = scrape_company_list()

    # Always print a summary
    print(f"\n{'Symbol':<12} {'Name':<50} {'Sector'}")
    print("-" * 80)
    for c in companies:
        print(f"{c['symbol']:<12} {c['name']:<50} {c['sector']}")
    print(f"\nTotal: {len(companies)} companies\n")

    if args.save:
        save_json(companies, args.output)

    if args.update_config:
        update_config(companies)


if __name__ == "__main__":
    main()
