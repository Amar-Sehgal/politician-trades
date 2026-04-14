# Politician Trade Tracker

Track and analyze US politicians' stock trades from [Capitol Trades](https://www.capitoltrades.com/trades).

## Features

- **Scraper**: Pulls trade disclosures (politician, ticker, buy/sell, date, amount range, price)
- **Performance Tracker**: Computes per-politician returns, win rates, estimated PnL
- **Hold Time Analysis**: Matches buys to sells (FIFO) to estimate average position durations
- **Leaderboard**: Ranks politicians by trading performance
- **Search**: Look up trades by politician name or ticker

## Setup

```bash
pip3 install -r requirements.txt
```

## Usage

```bash
# Scrape trades (default: 50 pages, ~600 trades)
python3 -m politician_trades scrape --pages 50

# Scrape everything (~2966 pages, ~35K trades) - takes a while
python3 -m politician_trades scrape --pages 0

# Fetch price history for all scraped tickers
python3 -m politician_trades prices

# View the leaderboard
python3 -m politician_trades leaderboard

# Detailed view for one politician
python3 -m politician_trades detail "Michael McCaul"

# Analyze hold times
python3 -m politician_trades holdtime

# Search by ticker or politician
python3 -m politician_trades search NVDA
python3 -m politician_trades search "Pelosi"

# Check database status
python3 -m politician_trades status
```

## Data Notes

- Trades are reported with a 15-45 day filing delay (STOCK Act)
- Dollar amounts are ranges, not exact (e.g., $15K-$50K)
- Only trades with stock tickers are tracked; municipal bonds and private LLCs are skipped
- Hold times are estimated via FIFO matching of buys to sells per politician+ticker
- Performance is calculated from trade date to latest available price

## Data Source

All trade data is sourced from [Capitol Trades](https://www.capitoltrades.com), which aggregates STOCK Act filings.
