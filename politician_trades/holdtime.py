"""Analyze position hold times by matching buys to sells (FIFO)."""

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

from tabulate import tabulate

from . import db

logger = logging.getLogger(__name__)


@dataclass
class HoldPeriod:
    politician: str
    ticker: str
    buy_date: str
    sell_date: str
    days_held: int
    buy_amount_mid: float
    sell_amount_mid: float


def amount_midpoint(low: int, high: int) -> float:
    if low and high:
        return (low + high) / 2
    return low or high or 5000


def match_trades(trades: list[dict]) -> list[HoldPeriod]:
    """Match buys to sells for the same politician+ticker using FIFO."""
    # Group by (politician, ticker)
    grouped: dict[tuple[str, str], dict[str, list]] = defaultdict(lambda: {"buys": [], "sells": []})

    for t in trades:
        ticker = t.get("ticker")
        politician = t.get("politician")
        trade_date = t.get("trade_date")
        if not ticker or not politician or not trade_date:
            continue

        key = (politician, ticker)
        entry = {
            "date": trade_date,
            "amount_mid": amount_midpoint(t.get("amount_low", 0), t.get("amount_high", 0)),
        }

        if t["trade_type"] == "buy":
            grouped[key]["buys"].append(entry)
        elif t["trade_type"] == "sell":
            grouped[key]["sells"].append(entry)

    # FIFO matching: for each sell, match to earliest unmatched buy
    holds = []
    for (politician, ticker), positions in grouped.items():
        buys = sorted(positions["buys"], key=lambda x: x["date"])
        sells = sorted(positions["sells"], key=lambda x: x["date"])

        buy_idx = 0
        for sell in sells:
            # Find next buy that happened before this sell
            while buy_idx < len(buys) and buys[buy_idx]["date"] > sell["date"]:
                buy_idx += 1

            if buy_idx < len(buys) and buys[buy_idx]["date"] <= sell["date"]:
                buy = buys[buy_idx]
                buy_dt = datetime.strptime(buy["date"], "%Y-%m-%d")
                sell_dt = datetime.strptime(sell["date"], "%Y-%m-%d")
                days_held = (sell_dt - buy_dt).days

                if days_held >= 0:
                    holds.append(HoldPeriod(
                        politician=politician,
                        ticker=ticker,
                        buy_date=buy["date"],
                        sell_date=sell["date"],
                        days_held=days_held,
                        buy_amount_mid=buy["amount_mid"],
                        sell_amount_mid=sell["amount_mid"],
                    ))
                    buy_idx += 1

    return holds


def analyze_hold_times(db_path=None) -> str:
    """Analyze hold times across all politicians."""
    conn = db.get_connection(db_path)

    # Get all trades
    rows = conn.execute(
        "SELECT * FROM trades WHERE ticker IS NOT NULL AND ticker != 'N/A' ORDER BY trade_date"
    ).fetchall()
    trades = [dict(r) for r in rows]
    conn.close()

    holds = match_trades(trades)

    if not holds:
        return "No matched buy->sell pairs found. Need more historical data."

    # Per-politician stats
    pol_holds: dict[str, list[HoldPeriod]] = defaultdict(list)
    for h in holds:
        pol_holds[h.politician].append(h)

    # Summary table
    summary_rows = []
    for pol, hs in sorted(pol_holds.items()):
        days = [h.days_held for h in hs]
        avg_days = sum(days) / len(days)
        min_days = min(days)
        max_days = max(days)
        tickers = set(h.ticker for h in hs)
        summary_rows.append([
            pol,
            len(hs),
            f"{avg_days:.0f}",
            min_days,
            max_days,
            len(tickers),
            ", ".join(sorted(tickers)[:5]) + ("..." if len(tickers) > 5 else ""),
        ])

    summary_rows.sort(key=lambda r: float(r[2]))  # sort by avg hold time

    lines = [
        f"\nMatched {len(holds)} buy->sell pairs across {len(pol_holds)} politicians\n",
        tabulate(
            summary_rows,
            headers=["Politician", "Pairs", "Avg Days", "Min", "Max", "Tickers", "Top Tickers"],
            tablefmt="simple",
        ),
    ]

    # Overall stats
    all_days = [h.days_held for h in holds]
    lines.extend([
        f"\nOverall: avg={sum(all_days)/len(all_days):.0f} days, "
        f"median={sorted(all_days)[len(all_days)//2]} days, "
        f"min={min(all_days)}, max={max(all_days)}",
    ])

    # Detailed list of matched pairs
    lines.append("\n\nDetailed Matched Pairs (sorted by hold time):")
    detail_rows = []
    for h in sorted(holds, key=lambda x: x.days_held):
        detail_rows.append([
            h.politician,
            h.ticker,
            h.buy_date,
            h.sell_date,
            h.days_held,
            f"${h.buy_amount_mid:,.0f}",
            f"${h.sell_amount_mid:,.0f}",
        ])

    lines.append(tabulate(
        detail_rows[:50],  # limit output
        headers=["Politician", "Ticker", "Buy Date", "Sell Date", "Days", "Buy Amt", "Sell Amt"],
        tablefmt="simple",
    ))

    if len(detail_rows) > 50:
        lines.append(f"\n... and {len(detail_rows) - 50} more pairs")

    return "\n".join(lines)
