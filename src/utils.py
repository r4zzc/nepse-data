"""
utils.py - Shared helpers for NEPSE scrapers
"""

import os
import time
import logging
import requests
from datetime import datetime
from typing import Optional

from config import LOGS_DIR, REQUEST_TIMEOUT, MAX_RETRIES, REQUEST_DELAY

# Re-export for other modules
__all__ = ["setup_logger", "fetch_page", "ensure_dir", "clean_number",
           "parse_date", "_SESSION", "REQUEST_DELAY"]


# ─── Logging setup ────────────────────────────────────────────────────────────

def setup_logger(name: str) -> logging.Logger:
    """Create a logger that writes to both console and a log file."""
    os.makedirs(LOGS_DIR, exist_ok=True)
    log_file = os.path.join(LOGS_DIR, f"{name}_{datetime.now().strftime('%Y%m%d')}.log")

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")

        fh = logging.FileHandler(log_file)
        fh.setFormatter(fmt)
        logger.addHandler(fh)

        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        # Force UTF-8 on Windows terminals (avoids CP1252 UnicodeEncodeError)
        import sys
        if hasattr(sys.stdout, 'reconfigure'):
            sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        logger.addHandler(ch)

    return logger


# ─── HTTP helpers ──────────────────────────────────────────────────────────────

def _make_session() -> requests.Session:
    """
    Build a persistent session that looks like a real Chrome browser.
    A session reuses the TCP connection and carries cookies automatically,
    which is what the sites expect.
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    })
    session.verify = False
    return session


# One shared session for the entire run — carries cookies across requests
_SESSION = _make_session()


def fetch_page(url: str, logger: Optional[logging.Logger] = None,
               params: dict = None, retries: int = MAX_RETRIES) -> Optional[requests.Response]:
    """
    Fetch a URL using the shared session with retries and polite delay.
    Returns a Response object or None on failure.
    """
    log = logger or logging.getLogger("utils")

    # Set Referer to the site's own homepage so it looks like in-site navigation
    from urllib.parse import urlparse
    parsed = urlparse(url)
    referer = f"{parsed.scheme}://{parsed.netloc}/"
    _SESSION.headers.update({"Referer": referer})

    for attempt in range(1, retries + 1):
        try:
            time.sleep(REQUEST_DELAY)
            resp = _SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as e:
            log.warning(f"HTTP {e.response.status_code} on attempt {attempt}/{retries}: {url}")
        except requests.exceptions.ConnectionError:
            log.warning(f"Connection error on attempt {attempt}/{retries}: {url}")
        except requests.exceptions.Timeout:
            log.warning(f"Timeout on attempt {attempt}/{retries}: {url}")
        except Exception as e:
            log.warning(f"Unexpected error on attempt {attempt}/{retries}: {e}")

        if attempt < retries:
            time.sleep(REQUEST_DELAY * attempt * 2)  # exponential back-off

    log.error(f"Failed after {retries} attempts: {url}")
    return None


# ─── CSV / filesystem helpers ──────────────────────────────────────────────────

def ensure_dir(path: str):
    """Create directory (and parents) if it doesn't exist."""
    os.makedirs(path, exist_ok=True)


def clean_number(value: str) -> str:
    """Strip commas and whitespace from a numeric string."""
    if value is None:
        return ""
    return value.replace(",", "").strip()


def parse_date(raw: str, fmt: str = "%Y-%m-%d") -> str:
    """
    Attempt to normalise a date string to YYYY-MM-DD.
    Returns the original string if parsing fails.
    """
    for src_fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw.strip(), src_fmt).strftime(fmt)
        except ValueError:
            continue
    return raw.strip()