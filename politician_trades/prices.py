"""Fetch historical prices via yfinance."""

import logging
from datetime import datetime, timedelta

import yfinance as yf

from . import db

logger = logging.getLogger(__name__)


def fetch_prices_for_ticker(ticker: str, conn, start_date: str = "2020-01-01") -> int:
    """Fetch and store historical prices for a ticker. Returns rows inserted."""
    try:
        end_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        data = yf.download(ticker, start=start_date, end=end_date, progress=False, auto_adjust=True)

        if data.empty:
            logger.warning(f"No price data for {ticker}")
            return 0

        count = 0
        for date_idx, row in data.iterrows():
            date_str = date_idx.strftime("%Y-%m-%d")
            try:
                open_ = float(row["Open"].iloc[0]) if hasattr(row["Open"], "iloc") else float(row["Open"])
                high = float(row["High"].iloc[0]) if hasattr(row["High"], "iloc") else float(row["High"])
                low = float(row["Low"].iloc[0]) if hasattr(row["Low"], "iloc") else float(row["Low"])
                close = float(row["Close"].iloc[0]) if hasattr(row["Close"], "iloc") else float(row["Close"])
                volume = int(row["Volume"].iloc[0]) if hasattr(row["Volume"], "iloc") else int(row["Volume"])
            except (TypeError, IndexError):
                open_ = float(row["Open"])
                high = float(row["High"])
                low = float(row["Low"])
                close = float(row["Close"])
                volume = int(row["Volume"])

            db.insert_price(conn, ticker, date_str, open_, high, low, close, volume)
            count += 1

        conn.commit()
        logger.info(f"{ticker}: {count} price rows")
        return count

    except Exception as e:
        logger.error(f"Failed to fetch prices for {ticker}: {e}")
        return 0


def fetch_all_prices(db_path=None, start_date: str = "2020-01-01") -> None:
    """Fetch prices for all tickers in the database."""
    conn = db.get_connection(db_path)
    tickers = db.get_all_tickers(conn)
    logger.info(f"Fetching prices for {len(tickers)} tickers")

    for i, ticker in enumerate(tickers):
        logger.info(f"[{i+1}/{len(tickers)}] {ticker}")
        fetch_prices_for_ticker(ticker, conn, start_date)

    conn.close()
