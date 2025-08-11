"""Fetch market data, save CSVs, and summarize with ChatGPT."""

import os
from datetime import datetime, timedelta, timezone

try:
    import pandas as pd
    import requests
except ModuleNotFoundError as exc:
    raise SystemExit("The 'pandas' and 'requests' packages are required to run fetch.py") from exc

try:
    from openai import OpenAI
except ModuleNotFoundError as exc:
    raise SystemExit("The 'openai' package is required to run fetch.py") from exc

API_KEY = os.environ["POLYGON_API_KEY"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
CLIENT = OpenAI(api_key=OPENAI_API_KEY)
BASE = "https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/minute/{start}/{end}"
PARAMS = {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": API_KEY}

EQUITIES = ["SPY", "QQQ"]

ROLLING_MINUTES = 24 * 60  # last 24h of 1m, then we resample


def fetch_polygon_1m(sym: str, start_iso: str, end_iso: str) -> pd.DataFrame:
    """Fetch 1-minute bars from Polygon."""
    url = BASE.format(sym=sym, start=start_iso, end=end_iso)
    r = requests.get(url, params=PARAMS, timeout=60)
    r.raise_for_status()
    rows = r.json().get("results", [])
    if not rows:
        return pd.DataFrame(columns=["Datetime", "Open", "High", "Low", "Close", "Volume"])
    df = pd.DataFrame(rows)
    # Convert epoch milliseconds to New York time, rename OHLCV columns, sort, and deduplicate
    df["Datetime"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_convert("America/New_York")
    df.rename(columns={"o": "Open", "h": "High", "l": "Low", "c": "Close", "v": "Volume"}, inplace=True)
    df = (
        df[["Datetime", "Open", "High", "Low", "Close", "Volume"]]
        .sort_values("Datetime")
        .drop_duplicates("Datetime")
    )
    return df


def recent_window(minutes: int = ROLLING_MINUTES) -> tuple[datetime, datetime]:
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=minutes)
    return start, end


def resample(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    o = (
        df.set_index("Datetime")
        .resample(rule, label="right", closed="right")
        .agg({"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"})
        .dropna()
    )
    return o.reset_index()


def save_all_timeframes(sym: str, df: pd.DataFrame) -> None:
    df.to_csv(f"{sym}_1m.csv", index=False)
    frames = {
        "3m": "3T",
        "5m": "5T",
        "15m": "15T",
        "30m": "30T",
        "45m": "45T",
        "1h": "60T",
        "4h": "240T",
        "1d": "1D",
    }
    for name, rule in frames.items():
        resample(df, rule).to_csv(f"{sym}_{name}.csv", index=False)


def analyze_with_chatgpt(sym: str, df: pd.DataFrame) -> None:
    """Send a brief summary of the latest bars to ChatGPT."""
    if df.empty:
        return
    sample = df.tail(5).to_dict(orient="records")
    prompt = (
        "You are a market analyst. Using only the OHLCV data provided below, "
        f"summarize the recent price action for {sym} in one or two sentences. "
        "Do not request additional data.\n"
        f"{sample}"
    )
    try:
        resp = CLIENT.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
        )
        msg = resp.choices[0].message["content"].strip()
        print(f"ChatGPT analysis for {sym}: {msg}")
    except Exception as exc:
        print(f"ChatGPT analysis failed for {sym}: {exc}")


def main() -> None:
    start, end = recent_window()  # rolling window bounds (UTC)
    for sym in EQUITIES:
        # pull 1m bars for the date range, then trim to the exact window
        df = fetch_polygon_1m(sym, start.date().isoformat(), end.date().isoformat())
        if df.empty:
            print(f"No data returned for {sym}; skipping.")
            continue

        # keep strict window using the DataFrame's timezone (New York)
        start_ny = start.astimezone(df["Datetime"].iloc[0].tz)
        df = df[df["Datetime"] >= start_ny]
        if df.empty:
            print(f"No rows remain after window trim for {sym}; skipping.")
            continue

        # write all requested timeframes
        save_all_timeframes(sym, df)

        # analyze latest data with ChatGPT
        analyze_with_chatgpt(sym, df)

    print("Done.")


if __name__ == "__main__":
    main()
