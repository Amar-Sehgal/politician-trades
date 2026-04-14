"""Scrape politician trades from Capitol Trades."""

import re
import time
import hashlib
import logging
from datetime import datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup

from . import db

logger = logging.getLogger(__name__)

BASE_URL = "https://www.capitoltrades.com/trades?assetType=stock"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml",
}

AMOUNT_MAP = {
    "1K": 1_000,
    "15K": 15_000,
    "50K": 50_000,
    "100K": 100_000,
    "250K": 250_000,
    "500K": 500_000,
    "1M": 1_000_000,
    "5M": 5_000_000,
    "25M": 25_000_000,
    "50M": 50_000_000,
}

MONTH_MAP = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}


def parse_amount(amount_str: str) -> tuple[int, int]:
    """Parse '1K-15K' into (1000, 15000)."""
    # Normalize unicode dashes
    amount_str = amount_str.replace("\u2013", "-").replace("\u2014", "-")
    parts = amount_str.split("-")
    if len(parts) == 2:
        low = AMOUNT_MAP.get(parts[0].strip(), 0)
        high = AMOUNT_MAP.get(parts[1].strip(), 0)
        return low, high
    return 0, 0


def parse_date(day_month: str, year: str) -> Optional[str]:
    """Parse '20 Mar' + '2026' into '2026-03-20'."""
    try:
        parts = day_month.strip().split()
        if len(parts) == 2:
            day = parts[0].zfill(2)
            month = MONTH_MAP.get(parts[1], "01")
            return f"{year}-{month}-{day}"
    except (ValueError, IndexError):
        pass
    return None


def make_trade_id(politician: str, ticker: str, trade_type: str,
                  trade_date: str, amount_low: int) -> str:
    raw = f"{politician}|{ticker}|{trade_type}|{trade_date}|{amount_low}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def parse_page(html: str) -> list[dict]:
    """Parse a single page of trades from HTML."""
    soup = BeautifulSoup(html, "html.parser")
    trades = []

    rows = soup.select("tr.h-14")
    for row in rows:
        tds = row.find_all("td")
        if len(tds) < 8:
            continue

        try:
            # Politician (td 0)
            pol_name_el = tds[0].select_one(".politician-name a")
            politician = pol_name_el.get_text(strip=True) if pol_name_el else None
            if not politician:
                continue

            party_el = tds[0].select_one("[class*='party--']")
            party = party_el.get_text(strip=True) if party_el else None

            chamber_el = tds[0].select_one("[class*='chamber--']")
            chamber = chamber_el.get_text(strip=True) if chamber_el else None

            state_el = tds[0].select_one("[class*='us-state-compact--']")
            state = state_el.get_text(strip=True) if state_el else None

            # Asset (td 1)
            asset_el = tds[1].select_one(".issuer-name a")
            asset_name = asset_el.get_text(strip=True) if asset_el else None

            ticker_el = tds[1].select_one(".issuer-ticker")
            ticker_raw = ticker_el.get_text(strip=True) if ticker_el else None

            # Skip trades without tickers
            if not ticker_raw or ticker_raw == "N/A":
                continue

            # Strip :US suffix for cleaner ticker
            ticker = ticker_raw.replace(":US", "")

            # Published date (td 2) - we don't store this separately
            # Trade date (td 3)
            date_divs = tds[3].select("div.text-size-3, div.text-size-2")
            trade_date = None
            if len(date_divs) >= 2:
                day_month = date_divs[0].get_text(strip=True)
                year = date_divs[1].get_text(strip=True)
                trade_date = parse_date(day_month, year)

            # Reporting gap (td 4)
            gap_el = tds[4].select_one("[class*='reporting-gap-tier']")
            reporting_gap = int(gap_el.get_text(strip=True)) if gap_el else None

            # Owner (td 5)
            owner_text = tds[5].get_text(strip=True)
            owner = owner_text if owner_text else None

            # Trade type + amount (td 6)
            type_el = tds[6].select_one("[class*='tx-type--']")
            trade_type = None
            if type_el:
                classes = type_el.get("class", [])
                for c in classes:
                    if "tx-type--buy" in c:
                        trade_type = "buy"
                        break
                    elif "tx-type--sell" in c:
                        trade_type = "sell"
                        break
                if not trade_type:
                    trade_type = type_el.get_text(strip=True).lower()

            amount_el = tds[6].select_one("[class*='hover:text-foreground']")
            amount_low, amount_high = 0, 0
            if amount_el:
                amount_low, amount_high = parse_amount(amount_el.get_text(strip=True))

            # Price (td 7)
            price = None
            price_el = tds[7].select_one("span")
            if price_el:
                price_text = price_el.get_text(strip=True).replace("$", "").replace(",", "")
                try:
                    price = float(price_text)
                except ValueError:
                    pass

            # Filed date - derive from trade_date + reporting_gap
            filed_date = None
            if trade_date and reporting_gap:
                try:
                    td = datetime.strptime(trade_date, "%Y-%m-%d")
                    from datetime import timedelta
                    fd = td + timedelta(days=reporting_gap)
                    filed_date = fd.strftime("%Y-%m-%d")
                except ValueError:
                    pass

            trade_id = make_trade_id(politician, ticker, trade_type or "", trade_date or "", amount_low)

            trades.append({
                "trade_id": trade_id,
                "politician": politician,
                "party": party,
                "chamber": chamber,
                "state": state,
                "asset_name": asset_name,
                "ticker": ticker,
                "trade_type": trade_type,
                "trade_date": trade_date,
                "filed_date": filed_date,
                "amount_low": amount_low,
                "amount_high": amount_high,
                "price": price,
                "owner": owner,
                "reporting_gap_days": reporting_gap,
            })

        except Exception as e:
            logger.warning(f"Failed to parse trade row: {e}")
            continue

    return trades


def scrape_page(page: int, session: Optional[requests.Session] = None) -> tuple[list[dict], int]:
    """Scrape a single page. Returns (trades, total_pages)."""
    s = session or requests.Session()
    url = f"{BASE_URL}&page={page}"
    resp = s.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    html = resp.text
    trades = parse_page(html)

    # Extract total pages from pagination links (highest page= value)
    total_pages = 0
    page_nums = re.findall(r'page=(\d+)', html)
    if page_nums:
        total_pages = max(int(p) for p in page_nums)

    return trades, total_pages


def scrape_all(max_pages: int = 0, delay: float = 1.0,
               start_page: int = 1, db_path=None) -> int:
    """Scrape all pages and store in database.

    Args:
        max_pages: Stop after this many pages (0 = all).
        delay: Seconds between requests.
        start_page: Page to start from.
        db_path: Optional database path.

    Returns:
        Total number of new trades inserted.
    """
    db.init_db(db_path)
    conn = db.get_connection(db_path)
    session = requests.Session()

    total_new = 0
    page = start_page
    total_pages = None
    consecutive_empty = 0

    while True:
        try:
            trades, tp = scrape_page(page, session)
            if total_pages is None and tp > 0:
                total_pages = tp
                logger.info(f"Total pages: {total_pages}")

            new_count = 0
            for trade in trades:
                if db.insert_trade(conn, trade):
                    new_count += 1
            conn.commit()

            total_new += new_count
            logger.info(f"Page {page}: {len(trades)} trades parsed, {new_count} new")

            if len(trades) == 0:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    logger.info("3 consecutive empty pages, stopping")
                    break
            else:
                consecutive_empty = 0

            if total_pages and page >= total_pages:
                break
            if max_pages and (page - start_page + 1) >= max_pages:
                break

            page += 1
            time.sleep(delay)

        except requests.RequestException as e:
            logger.error(f"Request failed on page {page}: {e}")
            time.sleep(5)
            continue
        except KeyboardInterrupt:
            logger.info("Interrupted, saving progress")
            conn.commit()
            break

    conn.close()
    return total_new
