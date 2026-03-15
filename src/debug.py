"""
debug_html.py
─────────────
Run this to inspect what merolagani actually returns.
It saves the full HTML to debug_output/ so you can open it in a browser
and check the real table IDs/classes.

Usage:
    python debug_html.py
"""

import os
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from utils import fetch_page, setup_logger

logger = setup_logger("debug")

SYMBOL = "NABIL"
URL = f"https://merolagani.com/CompanyDetail.aspx?symbol={SYMBOL}"

os.makedirs("debug_output", exist_ok=True)

print(f"Fetching: {URL}")
resp = fetch_page(URL, logger)

if not resp:
    print("FAILED to fetch page")
    exit(1)

html = resp.text

# Save full HTML
with open("debug_output/merolagani_nabil.html", "w", encoding="utf-8") as f:
    f.write(html)
print(f"Full HTML saved → debug_output/merolagani_nabil.html  ({len(html):,} bytes)")

# Parse and report all tables found
soup = BeautifulSoup(html, "html.parser")
tables = soup.find_all("table")
print(f"\nFound {len(tables)} table(s):")
for i, t in enumerate(tables):
    headers = [th.get_text(strip=True) for th in t.find_all("th")]
    rows = t.find_all("tr")
    print(f"  Table {i+1}: id={t.get('id')!r}  class={t.get('class')!r}  "
          f"headers={headers[:6]}  rows={len(rows)}")

# Report all <div> IDs that might wrap the price history
print("\nDivs with IDs containing 'price', 'history', 'data', 'grid':")
for div in soup.find_all("div", id=True):
    did = div.get("id", "").lower()
    if any(k in did for k in ("price", "history", "data", "grid", "table", "stock")):
        print(f"  <div id={div.get('id')!r}>")

# Check if data loads via JavaScript (look for common JS data patterns)
print("\nJS data indicators:")
if "__VIEWSTATE" in html:
    print("  ✓ ASP.NET __VIEWSTATE found (form postback site)")
if "DataTable" in html or "datatables" in html.lower():
    print("  ✓ DataTables JS found")
if "ajax" in html.lower():
    print("  ✓ AJAX calls detected")
if "api/" in html.lower() or "/api?" in html.lower():
    print("  ✓ API endpoint references found")

# Look for any JSON data embedded in the page
import re
json_blocks = re.findall(r'var\s+\w+\s*=\s*(\[.*?\]|\{.*?\})\s*;', html[:50000], re.DOTALL)
if json_blocks:
    print(f"\n  Found {len(json_blocks)} embedded JS variable(s) with JSON data")
    for j in json_blocks[:3]:
        print(f"    {j[:120]}...")

print("\nDone. Open debug_output/merolagani_nabil.html in your browser to inspect.")