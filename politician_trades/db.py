"""SQLite database layer for politician trades."""

import sqlite3
from pathlib import Path
from typing import Optional

DB_PATH = Path(__file__).parent.parent / "trades.db"


def get_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path or DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: Optional[Path] = None) -> None:
    conn = get_connection(db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id TEXT UNIQUE,
            politician TEXT NOT NULL,
            party TEXT,
            chamber TEXT,
            state TEXT,
            asset_name TEXT,
            ticker TEXT,
            trade_type TEXT NOT NULL,
            trade_date TEXT,
            filed_date TEXT,
            amount_low INTEGER,
            amount_high INTEGER,
            price REAL,
            owner TEXT,
            reporting_gap_days INTEGER,
            scraped_at TEXT DEFAULT (datetime('now')),

            UNIQUE(politician, ticker, trade_type, trade_date, amount_low)
        );

        CREATE INDEX IF NOT EXISTS idx_trades_politician ON trades(politician);
        CREATE INDEX IF NOT EXISTS idx_trades_ticker ON trades(ticker);
        CREATE INDEX IF NOT EXISTS idx_trades_date ON trades(trade_date);
        CREATE INDEX IF NOT EXISTS idx_trades_type ON trades(trade_type);

        CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            UNIQUE(ticker, date)
        );

        CREATE INDEX IF NOT EXISTS idx_prices_ticker ON prices(ticker);
        CREATE INDEX IF NOT EXISTS idx_prices_date ON prices(date);
    """)
    conn.commit()
    conn.close()


def insert_trade(conn: sqlite3.Connection, trade: dict) -> bool:
    """Insert a trade, returning True if new, False if duplicate."""
    try:
        conn.execute("""
            INSERT OR IGNORE INTO trades
                (trade_id, politician, party, chamber, state, asset_name, ticker,
                 trade_type, trade_date, filed_date, amount_low, amount_high,
                 price, owner, reporting_gap_days)
            VALUES
                (:trade_id, :politician, :party, :chamber, :state, :asset_name, :ticker,
                 :trade_type, :trade_date, :filed_date, :amount_low, :amount_high,
                 :price, :owner, :reporting_gap_days)
        """, trade)
        return conn.total_changes > 0
    except sqlite3.IntegrityError:
        return False


def insert_price(conn: sqlite3.Connection, ticker: str, date: str,
                 open_: float, high: float, low: float, close: float, volume: int) -> None:
    conn.execute("""
        INSERT OR IGNORE INTO prices (ticker, date, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (ticker, date, open_, high, low, close, volume))


def get_all_tickers(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT DISTINCT ticker FROM trades WHERE ticker IS NOT NULL AND ticker != 'N/A'"
    ).fetchall()
    return [r["ticker"] for r in rows]


def get_trades_by_politician(conn: sqlite3.Connection, politician: str) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM trades WHERE politician = ? AND ticker IS NOT NULL AND ticker != 'N/A' ORDER BY trade_date",
        (politician,)
    ).fetchall()
    return [dict(r) for r in rows]


def get_all_politicians(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT DISTINCT politician FROM trades WHERE ticker IS NOT NULL AND ticker != 'N/A' ORDER BY politician"
    ).fetchall()
    return [r["politician"] for r in rows]


def get_price_on_date(conn: sqlite3.Connection, ticker: str, date: str) -> Optional[float]:
    """Get closing price on or nearest prior date."""
    row = conn.execute(
        "SELECT close FROM prices WHERE ticker = ? AND date <= ? ORDER BY date DESC LIMIT 1",
        (ticker, date)
    ).fetchone()
    return row["close"] if row else None


def get_latest_price(conn: sqlite3.Connection, ticker: str) -> Optional[float]:
    row = conn.execute(
        "SELECT close FROM prices WHERE ticker = ? ORDER BY date DESC LIMIT 1",
        (ticker,)
    ).fetchone()
    return row["close"] if row else None


def get_trade_count(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) as cnt FROM trades WHERE ticker IS NOT NULL AND ticker != 'N/A'").fetchone()
    return row["cnt"]


def get_price_count(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(DISTINCT ticker) as cnt FROM prices").fetchone()
    return row["cnt"]
