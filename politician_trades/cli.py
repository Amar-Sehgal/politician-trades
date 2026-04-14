"""CLI entry point for politician trade tracker."""

import argparse
import logging
import sys

from . import db
from .scraper import scrape_all
from .prices import fetch_all_prices
from .performance import leaderboard, politician_detail
from .holdtime import analyze_hold_times

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def cmd_scrape(args):
    db.init_db()
    new = scrape_all(
        max_pages=args.pages,
        delay=args.delay,
        start_page=args.start,
    )
    total = db.get_trade_count()
    print(f"\nDone. {new} new trades scraped. Total in DB: {total}")


def cmd_prices(args):
    db.init_db()
    fetch_all_prices(start_date=args.start_date)
    conn = db.get_connection()
    print(f"\nPrices fetched for {db.get_price_count(conn)} tickers")
    conn.close()


def cmd_leaderboard(args):
    db.init_db()
    print(leaderboard(top_n=args.top))


def cmd_detail(args):
    db.init_db()
    print(politician_detail(args.name))


def cmd_holdtime(args):
    db.init_db()
    print(analyze_hold_times())


def cmd_status(args):
    db.init_db()
    conn = db.get_connection()
    trade_count = db.get_trade_count()
    price_tickers = db.get_price_count(conn)
    politicians = db.get_all_politicians(conn)

    # Recent trades
    recent = conn.execute(
        "SELECT politician, ticker, trade_type, trade_date FROM trades "
        "WHERE ticker IS NOT NULL AND ticker != 'N/A' "
        "ORDER BY trade_date DESC LIMIT 10"
    ).fetchall()

    print(f"\nDatabase Status:")
    print(f"  Trades (with tickers): {trade_count}")
    print(f"  Politicians: {len(politicians)}")
    print(f"  Tickers with prices: {price_tickers}")
    print(f"\nMost Recent Trades:")
    for r in recent:
        print(f"  {r['trade_date']}  {r['politician']:25s}  {r['trade_type']:4s}  {r['ticker']}")

    conn.close()


def cmd_search(args):
    """Search trades by politician name or ticker."""
    db.init_db()
    conn = db.get_connection()

    query = args.query.upper()

    # Search by ticker
    ticker_trades = conn.execute(
        "SELECT * FROM trades WHERE UPPER(ticker) = ? ORDER BY trade_date DESC",
        (query,)
    ).fetchall()

    # Search by politician name (partial match)
    name_trades = conn.execute(
        "SELECT * FROM trades WHERE UPPER(politician) LIKE ? AND ticker IS NOT NULL AND ticker != 'N/A' ORDER BY trade_date DESC",
        (f"%{query}%",)
    ).fetchall()

    trades = ticker_trades or name_trades
    if not trades:
        print(f"No trades found for '{args.query}'")
        conn.close()
        return

    from tabulate import tabulate
    rows = []
    for t in trades[:50]:
        rows.append([
            t["trade_date"],
            t["politician"],
            t["party"][:3] if t["party"] else "",
            t["ticker"],
            t["trade_type"].upper() if t["trade_type"] else "",
            f"${t['amount_low']:,}-${t['amount_high']:,}" if t["amount_low"] else "",
            f"${t['price']:.2f}" if t["price"] else "",
        ])

    print(tabulate(rows, headers=["Date", "Politician", "Pty", "Ticker", "Type", "Amount", "Price"],
                   tablefmt="simple"))
    if len(trades) > 50:
        print(f"\n... {len(trades) - 50} more results")

    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Politician Trade Tracker")
    sub = parser.add_subparsers(dest="command")

    # scrape
    p = sub.add_parser("scrape", help="Scrape trades from Capitol Trades")
    p.add_argument("--pages", type=int, default=50, help="Max pages to scrape (0=all)")
    p.add_argument("--delay", type=float, default=1.0, help="Delay between requests (seconds)")
    p.add_argument("--start", type=int, default=1, help="Start page number")
    p.set_defaults(func=cmd_scrape)

    # prices
    p = sub.add_parser("prices", help="Fetch price data for all tickers")
    p.add_argument("--start-date", default="2020-01-01", help="Start date for price history")
    p.set_defaults(func=cmd_prices)

    # leaderboard
    p = sub.add_parser("leaderboard", help="Show politician performance leaderboard")
    p.add_argument("--top", type=int, default=25, help="Number of politicians to show")
    p.set_defaults(func=cmd_leaderboard)

    # detail
    p = sub.add_parser("detail", help="Show detailed trades for a politician")
    p.add_argument("name", help="Politician name (exact match)")
    p.set_defaults(func=cmd_detail)

    # holdtime
    p = sub.add_parser("holdtime", help="Analyze position hold times")
    p.set_defaults(func=cmd_holdtime)

    # status
    p = sub.add_parser("status", help="Show database status")
    p.set_defaults(func=cmd_status)

    # search
    p = sub.add_parser("search", help="Search trades by politician name or ticker")
    p.add_argument("query", help="Politician name or ticker to search")
    p.set_defaults(func=cmd_search)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
