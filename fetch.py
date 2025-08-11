# fetch.py — pulls SPY/QQQ 1m from Polygon, resamples to all TFs, and saves crypto prices
# Needs repo secret: POLYGON_API_KEY

import os
from datetime import datetime, timedelta, timezone

try:
    import pandas as pd
except ModuleNotFoundError as exc:
    raise SystemExit("The 'pandas' package is required to run fetch.py") from exc

try:
    import requests
except ModuleNotFoundError as exc:
    raise SystemExit("The 'requests' package is required to run fetch.py") from exc

API_KEY = os.environ["POLYGON_API_KEY"]
BASE = "https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/minute/{start}/{end}"
PARAMS = {"adjusted":"true","sort":"asc","limit":50000,"apiKey":API_KEY}

EQUITIES = ["SPY","QQQ"]  # indexes you trade
# coins for price-only (edit if you want)
COINS = {
  "bitcoin":"BTC",
  "ethereum":"ETH",
  "solana":"SOL",
  "avalanche-2":"AVAX",
  "cardano":"ADA",
  "aioz-network":"AIOZ",
  "origintrail":"TRAC",
  "superverse":"SUPER",
  "snek":"SNEK",
  "bertram":"BERT",
  "pengu":"PENGU"
}

ROLLING_MINUTES = 24*60  # last 24h of 1m, then we resample

# ---------- helpers ----------
def fetch_polygon_1m(sym, start_iso, end_iso):
    url = BASE.format(sym=sym, start=start_iso, end=end_iso)
    r = requests.get(url, params=PARAMS, timeout=60)
    r.raise_for_status()
    rows = r.json().get("results", [])
    if not rows:
        return pd.DataFrame(columns=["Datetime","Open","High","Low","Close","Volume"])
    df = pd.DataFrame(rows)
    df["Datetime"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_convert("America/New_York")
    df.rename(columns={"o":"Open","h":"High","l":"Low","c":"Close","v":"Volume"}, inplace=True)
    df = df[["Datetime","Open","High","Low","Close","Volume"]].sort_values("Datetime").drop_duplicates("Datetime")
    return df

def recent_window(minutes=ROLLING_MINUTES):
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=minutes)
    return start, end

def resample(df, rule):
    o = df.set_index("Datetime").resample(rule, label="right", closed="right").agg(
        {"Open":"first","High":"max","Low":"min","Close":"last","Volume":"sum"}
    ).dropna()
    o = o.reset_index()
    return o

def save_all_timeframes(sym, df):
    # 1m raw
    df.to_csv(f"{sym}_1m.csv", index=False)
    # 3m/5m/15m/30m/45m/1h/4h/1d
    frames = {
        "3m":"3T", "5m":"5T", "15m":"15T", "30m":"30T", "45m":"45T",
        "1h":"60T", "4h":"240T", "1d":"1D"
    }
    for name, rule in frames.items():
        out = resample(df, rule)
        out.to_csv(f"{sym}_{name}.csv", index=False)

def fetch_crypto_prices():
    # CoinGecko free endpoint (no key). Returns price + 24h change.
    ids = ",".join(COINS.keys())
    url = f"https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": ids, "vs_currencies": "usd", "include_24hr_change": "true"}
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    rows = []
    for cg_id, sym in COINS.items():
        if cg_id in data:
            d = data[cg_id]
            rows.append({"Symbol": sym, "PriceUSD": d.get("usd"), "Change24hPct": d.get("usd_24h_change")})
    pd.DataFrame(rows).to_csv("CRYPTO_PRICES.csv", index=False)

if __name__ == "__main__":
    start, end = recent_window()
    for sym in EQUITIES:
        df = fetch_polygon_1m(sym, start.date().isoformat(), end.date().isoformat())
        if df.empty:
            continue
        # keep strict window
        start_ny = start.astimezone(df["Datetime"].iloc[0].tz)
        df = df[df["Datetime"] >= start_ny]
        save_all_timeframes(sym, df)
    fetch_crypto_prices()
    print("Done.")
