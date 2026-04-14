"""Track historical trading performance per politician."""

import logging
from dataclasses import dataclass, field
from datetime import datetime

from tabulate import tabulate

from . import db

logger = logging.getLogger(__name__)


@dataclass
class TradeResult:
    politician: str
    ticker: str
    trade_type: str
    trade_date: str
    amount_mid: float
    entry_price: float
    current_price: float
    pct_return: float  # positive = good for the direction traded
    dollar_return: float


@dataclass
class PoliticianStats:
    name: str
    party: str
    chamber: str
    total_trades: int = 0
    winning_trades: int = 0
    total_return_pct: float = 0.0
    avg_return_pct: float = 0.0
    estimated_pnl: float = 0.0
    best_trade: str = ""
    best_return: float = 0.0
    worst_trade: str = ""
    worst_return: float = 0.0
    buy_count: int = 0
    sell_count: int = 0
    results: list = field(default_factory=list)


def amount_midpoint(low: int, high: int) -> float:
    if low and high:
        return (low + high) / 2
    return low or high or 5000


def compute_trade_return(trade: dict, conn) -> TradeResult | None:
    """Compute return for a single trade using price data."""
    ticker = trade["ticker"]
    trade_date = trade["trade_date"]
    trade_type = trade["trade_type"]

    if not ticker or not trade_date or not trade_type:
        return None

    entry_price = trade.get("price")
    if not entry_price:
        entry_price = db.get_price_on_date(conn, ticker, trade_date)
    if not entry_price:
        return None

    current_price = db.get_latest_price(conn, ticker)
    if not current_price:
        return None

    # For buys: return = (current - entry) / entry
    # For sells: return = (entry - current) / entry (positive if sold before drop)
    if trade_type == "buy":
        pct_return = (current_price - entry_price) / entry_price * 100
    else:
        pct_return = (entry_price - current_price) / entry_price * 100

    amount_mid = amount_midpoint(trade.get("amount_low", 0), trade.get("amount_high", 0))
    dollar_return = amount_mid * (pct_return / 100)

    return TradeResult(
        politician=trade["politician"],
        ticker=ticker,
        trade_type=trade_type,
        trade_date=trade_date,
        amount_mid=amount_mid,
        entry_price=entry_price,
        current_price=current_price,
        pct_return=pct_return,
        dollar_return=dollar_return,
    )


def analyze_politician(politician: str, conn) -> PoliticianStats | None:
    """Analyze all trades for a single politician."""
    trades = db.get_trades_by_politician(conn, politician)
    if not trades:
        return None

    party = trades[0].get("party", "")
    chamber = trades[0].get("chamber", "")
    stats = PoliticianStats(name=politician, party=party, chamber=chamber)

    for trade in trades:
        result = compute_trade_return(trade, conn)
        if not result:
            continue

        stats.total_trades += 1
        stats.results.append(result)

        if result.trade_type == "buy":
            stats.buy_count += 1
        else:
            stats.sell_count += 1

        if result.pct_return > 0:
            stats.winning_trades += 1

        stats.total_return_pct += result.pct_return
        stats.estimated_pnl += result.dollar_return

        if result.pct_return > stats.best_return:
            stats.best_return = result.pct_return
            stats.best_trade = f"{result.ticker} ({result.trade_type})"

        if result.pct_return < stats.worst_return:
            stats.worst_return = result.pct_return
            stats.worst_trade = f"{result.ticker} ({result.trade_type})"

    if stats.total_trades > 0:
        stats.avg_return_pct = stats.total_return_pct / stats.total_trades

    return stats


def leaderboard(db_path=None, top_n: int = 25) -> str:
    """Generate a politician performance leaderboard."""
    conn = db.get_connection(db_path)
    politicians = db.get_all_politicians(conn)

    all_stats = []
    for pol in politicians:
        stats = analyze_politician(pol, conn)
        if stats and stats.total_trades >= 3:
            all_stats.append(stats)

    # Sort by average return
    all_stats.sort(key=lambda s: s.avg_return_pct, reverse=True)

    rows = []
    for i, s in enumerate(all_stats[:top_n], 1):
        win_rate = (s.winning_trades / s.total_trades * 100) if s.total_trades > 0 else 0
        rows.append([
            i,
            s.name,
            s.party[:3] if s.party else "",
            s.chamber[:3] if s.chamber else "",
            s.total_trades,
            f"{s.buy_count}B/{s.sell_count}S",
            f"{win_rate:.0f}%",
            f"{s.avg_return_pct:+.1f}%",
            f"${s.estimated_pnl:+,.0f}",
            s.best_trade,
            f"{s.best_return:+.1f}%",
            s.worst_trade,
            f"{s.worst_return:+.1f}%",
        ])

    conn.close()

    headers = ["#", "Politician", "Pty", "Chm", "Trades", "B/S", "Win%",
               "Avg Ret", "Est PnL", "Best", "Best%", "Worst", "Worst%"]
    return tabulate(rows, headers=headers, tablefmt="simple")


def politician_detail(politician: str, db_path=None) -> str:
    """Detailed trade-by-trade breakdown for one politician."""
    conn = db.get_connection(db_path)
    stats = analyze_politician(politician, conn)
    if not stats:
        conn.close()
        return f"No trades with price data found for {politician}"

    lines = [
        f"\n{'='*80}",
        f"  {stats.name} ({stats.party}, {stats.chamber})",
        f"{'='*80}",
        f"  Trades: {stats.total_trades} ({stats.buy_count} buys, {stats.sell_count} sells)",
        f"  Win Rate: {stats.winning_trades}/{stats.total_trades} ({stats.winning_trades/max(stats.total_trades,1)*100:.0f}%)",
        f"  Avg Return: {stats.avg_return_pct:+.1f}%",
        f"  Est Total PnL: ${stats.estimated_pnl:+,.0f}",
        f"  Best: {stats.best_trade} ({stats.best_return:+.1f}%)",
        f"  Worst: {stats.worst_trade} ({stats.worst_return:+.1f}%)",
        "",
    ]

    # Trade-by-trade
    rows = []
    for r in sorted(stats.results, key=lambda x: x.trade_date):
        rows.append([
            r.trade_date,
            r.ticker,
            r.trade_type.upper(),
            f"${r.entry_price:.2f}",
            f"${r.current_price:.2f}",
            f"{r.pct_return:+.1f}%",
            f"${r.dollar_return:+,.0f}",
        ])

    headers = ["Date", "Ticker", "Type", "Entry", "Current", "Return", "Est PnL"]
    lines.append(tabulate(rows, headers=headers, tablefmt="simple"))

    conn.close()
    return "\n".join(lines)
