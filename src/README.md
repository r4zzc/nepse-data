# NEPSE Historical Data Scraper

A Python toolkit that scrapes historical and daily stock price data from the Nepal Stock Exchange (NEPSE).

---

## Project Structure

```
nepse_scraper/
├── src/
│   ├── config.py                 # Symbols list, URLs, paths
│   ├── utils.py                  # Shared helpers (HTTP, CSV, logging)
│   ├── allDataScrapper.py        # ★ Full historical scrape (all companies)
│   ├── dailyDataScrapper.py      # ★ Daily incremental update (appends new rows)
│   ├── companyListScrapper.py    # Auto-discover all listed symbols
│   ├── indexDataScrapper.py      # NEPSE index + sub-index history
│   └── requirements.txt
├── data/
│   ├── company/                  # One CSV per symbol: NABIL.csv, EBL.csv …
│   │   └── *.csv
│   └── index/                    # NEPSE.csv, BANKING.csv …
│       └── *.csv
└── logs/                         # Auto-created log files
```

---

## Setup

```bash
cd src
pip install -r requirements.txt
```

---

## Usage

### 1. Discover All Listed Companies
```bash
python companyListScrapper.py --save           # saves data/companies.json
python companyListScrapper.py --update-config  # patches COMPANIES in config.py
```

### 2. Scrape Full Historical Data (first-time)
```bash
# All companies defined in config.py
python allDataScrapper.py

# Specific symbols only
python allDataScrapper.py --symbols NABIL EBL NICA

# Resume an interrupted run (skip already-scraped symbols)
python allDataScrapper.py --resume

# Custom output directory
python allDataScrapper.py --output /path/to/data
```

### 3. Daily Update (append new rows only)
```bash
python dailyDataScrapper.py

# Specific symbols
python dailyDataScrapper.py --symbols NABIL NMB
```

### 4. Scrape NEPSE Index History
```bash
python indexDataScrapper.py               # all sub-indices
python indexDataScrapper.py --index NEPSE # main NEPSE index only
python indexDataScrapper.py --list        # list available index keys
```

---

## CSV Format

Each company CSV (`data/company/<SYMBOL>.csv`) contains:

| Column         | Description                     |
|----------------|---------------------------------|
| Date           | YYYY-MM-DD                      |
| Open           | Opening price (NPR)             |
| High           | Day's high price (NPR)          |
| Low            | Day's low price (NPR)           |
| Close          | Last traded / closing price     |
| Change         | Absolute price change           |
| Percent_Change | % change from previous close    |
| Volume         | Number of shares traded         |
| Turnover       | Total traded value (NPR)        |

Index CSVs (`data/index/<INDEX>.csv`) contain:
`Date, Index_Value, Change, Percent_Change, Turnover, Traded_Shares, Transactions`

---

## GitHub Actions (Automated Daily Update)

Create `.github/workflows/daily_update.yml`:

```yaml
name: Daily NEPSE Update
on:
  schedule:
    # Runs at 16:00 NPT (10:15 UTC) Sunday–Friday
    - cron: "15 10 * * 0-5"
  workflow_dispatch:

jobs:
  update:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with: { python-version: "3.11" }
      - run: pip install -r src/requirements.txt
      - run: python src/dailyDataScrapper.py
      - uses: stefanzweifel/git-auto-commit-action@v5
        with:
          commit_message: "data: daily price update"
```

---

## Notes / Troubleshooting

- **SSL errors** – NEPSE-related sites have incomplete certificate chains.  
  Both scrapers disable SSL verification (`verify=False`). Suppress the resulting  
  `InsecureRequestWarning` by running through a proxy or setting `PYTHONWARNINGS=ignore`.

- **Rate limiting** – A 1.5 s delay between requests is built in. Reduce  
  `REQUEST_DELAY` in `config.py` carefully; being too aggressive risks getting  
  temporarily blocked.

- **Empty CSVs / "HTML data, no CSV"** – The original repo's `dailyDataScrapper.py`  
  printed HTML to stdout without saving. This version always writes proper CSVs  
  by calling `append_and_save()` even when creating a new file.

- **Adding new symbols** – Edit `COMPANIES` in `config.py`, or run  
  `companyListScrapper.py --update-config` to auto-populate it.

---

## Data Sources

| Source | Used for |
|--------|----------|
| [merolagani.com](https://merolagani.com) | Historical OHLCV per company (paginated) |
| [sharesansar.com](https://www.sharesansar.com) | Today's live prices (bulk table) & index history |

Scraping is for personal/educational use. Please respect the websites' terms of service.
