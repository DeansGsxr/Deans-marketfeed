import argparse
import datetime as dt
from typing import Optional


def run_strategy(strategy: str) -> None:
    """Dispatch function for bot strategies."""
    if strategy == "bm":
        print("Running baseline model strategy (placeholder)")
    elif strategy == "turtle":
        print("Running turtle strategy (placeholder)")
    elif strategy == "mm":
        print("Running market maker strategy (placeholder)")
    else:
        raise ValueError(f"Unsupported strategy: {strategy}")


def backtest(strategy: str, symbol: str, start: str, end: Optional[str]) -> None:
    """Simple backtest routine.

    Currently only implements a buy-and-hold baseline model. Other strategies
    print a placeholder message.
    """
    import yfinance as yf

    end = end or dt.date.today().isoformat()
    data = yf.download(symbol, start=start, end=end, progress=False)
    if data.empty:
        raise ValueError("No data returned from yfinance")
    if strategy == "bm":
        ret = data["Close"].pct_change().dropna()
        total = (1 + ret).prod() - 1
        print(
            f"Buy-and-hold return for {symbol} from {start} to {end}: {total:.2%}"
        )
    else:
        print(f"Backtest not implemented for strategy: {strategy}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Educational SMC bot")
    parser.add_argument("--strategy", "-s", choices=["bm", "turtle", "mm"], default="bm")
    parser.add_argument("--symbol", default="SPY", help="Ticker symbol")
    parser.add_argument("--start", default="2020-01-01", help="Backtest start date (YYYY-MM-DD)")
    parser.add_argument("--end", default=None, help="Backtest end date (YYYY-MM-DD)")
    parser.add_argument("--backtest", action="store_true", help="Run a simple backtest")
    args = parser.parse_args()
    if args.backtest:
        backtest(args.strategy, args.symbol, args.start, args.end)
    else:
        run_strategy(args.strategy)


if __name__ == "__main__":
    main()
