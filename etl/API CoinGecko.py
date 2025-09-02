import os
import time
import math
import json
import logging
from typing import List, Dict
import requests
import pandas as pd

COINGECKO_BASE = "https://api.coingecko.com/api/v3"
DEFAULT_COINS = ["bitcoin", "ethereum", "tether", "binancecoin", "solana"]
VS_CURRENCY = "usd"
DAYS = 365  

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
PRICES_CSV = os.path.join(OUTPUT_DIR, "crypto_prices.csv")
META_CSV = os.path.join(OUTPUT_DIR, "coin_metadata.csv")

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO
)

import random

def safe_request(url: str, params: Dict = None, max_retries: int = 8, backoff: float = 2.0):
    headers = {"User-Agent": "vinic-crypto-etl/1.0 (+https://github.com/)"}
    for i in range(max_retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=30)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                base = (backoff ** i)
                jitter = random.uniform(0.5, 1.5)
                wait = max(1.0, base * jitter)
                logging.warning(f"Status {r.status_code} em {url}. Tentando novamente em {wait:.1f}s.")
                time.sleep(wait)
                continue
            r.raise_for_status()
        except requests.RequestException as e:
            base = (backoff ** i)
            jitter = random.uniform(0.5, 1.5)
            wait = max(1.0, base * jitter)
            logging.warning(f"Erro de rede: {e}. Retry em {wait:.1f}s.")
            time.sleep(wait)
    raise RuntimeError(f"Falha ao requisitar {url} após {max_retries} tentativas.")


def _fallback_meta_from_ids(coin_ids: List[str]) -> pd.DataFrame:
    rows = []
    for cid in coin_ids:
        rows.append({
            "coin_id": cid,
            "symbol": cid[:4].upper(),
            "name": cid.replace("-", " ").title(),
            "market_cap_rank": None
        })
    return pd.DataFrame(rows)

def fetch_coin_metadata(coin_ids: List[str]) -> pd.DataFrame:
    """Busca nome, símbolo e rank; usa cache em META_CSV e tem fallback sem API."""
    if os.path.exists(META_CSV):
        try:
            meta_old = pd.read_csv(META_CSV)
            have = set(str(x) for x in meta_old["coin_id"].tolist())
            missing = [c for c in coin_ids if c not in have]
            if not missing:
                return meta_old
        except Exception:
            pass



    try:
        url = f"{COINGECKO_BASE}/coins/markets"
        params = {"vs_currency": VS_CURRENCY, "ids": ",".join(coin_ids)}
        time.sleep(6)
        data = safe_request(url, params=params)
        meta_rows = []
        for row in data:
            meta_rows.append({
                "coin_id": row.get("id"),
                "symbol": row.get("symbol"),
                "name": row.get("name"),
                "market_cap_rank": row.get("market_cap_rank")
            })
        df_meta = pd.DataFrame(meta_rows)

        if os.path.exists(META_CSV):
            try:
                old = pd.read_csv(META_CSV)
                df_meta = pd.concat([old, df_meta], ignore_index=True)\
                           .drop_duplicates(subset=["coin_id"], keep="last")
            except Exception:
                pass
        return df_meta
    except Exception as e:
        logging.warning(f"Falha ao buscar metadados via API ({e}). Usando fallback minimalista.")
        df_meta = _fallback_meta_from_ids(coin_ids)
        if os.path.exists(META_CSV):
            try:
                old = pd.read_csv(META_CSV)
                df_meta = pd.concat([old, df_meta], ignore_index=True)\
                           .drop_duplicates(subset=["coin_id"], keep="first")
            except Exception:
                pass
        return df_meta


def fetch_market_chart(coin_id: str, vs_currency: str = VS_CURRENCY, days: int | str = DAYS) -> pd.DataFrame:
    """Retorna preços históricos (timestamp, price)."""
    url = f"{COINGECKO_BASE}/coins/{coin_id}/market_chart"
    params = {"vs_currency": vs_currency, "days": days, "interval": "daily"}
    data = safe_request(url, params=params)


    prices = data.get("prices", [])
    if not prices:
        return pd.DataFrame()

    df = pd.DataFrame(prices, columns=["timestamp_ms", "price"])
    df["date"] = pd.to_datetime(df["timestamp_ms"], unit="ms").dt.date
    df["coin_id"] = coin_id
    df["vs_currency"] = vs_currency

    df = df.sort_values(["coin_id", "date"]).drop_duplicates(["coin_id", "date"])
    return df[["date", "coin_id", "vs_currency", "price"]]

def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula retorno diário, variação %, médias móveis (7d, 30d), drawdown e retorno acumulado."""
    df = df.copy()
    df["price_lag"] = df.groupby("coin_id")["price"].shift(1)
    df["daily_return"] = (df["price"] / df["price_lag"]) - 1
    df["pct_change"] = df["daily_return"] * 100.0

    df["ma_7"]  = df.groupby("coin_id")["price"].transform(lambda s: s.rolling(7, min_periods=1).mean())
    df["ma_30"] = df.groupby("coin_id")["price"].transform(lambda s: s.rolling(30, min_periods=1).mean())

    df["cum_return"] = df.groupby("coin_id")["daily_return"].transform(lambda s: (1 + s).cumprod() - 1)

    def drawdown(series: pd.Series) -> pd.Series:
        cummax = series.cummax()
        return (series - cummax) / cummax
    df["rolling_max_price"] = df.groupby("coin_id")["price"].transform(lambda s: s.cummax())
    df["drawdown"] = drawdown(df["price"])

    df["daily_return"] = df["daily_return"].fillna(0.0)
    df["pct_change"] = df["pct_change"].fillna(0.0)
    return df.drop(columns=["price_lag"])

def get_top_coins_by_mcap(n: int = 5) -> List[str]:

    url = f"{COINGECKO_BASE}/coins/markets"
    params = {"vs_currency": VS_CURRENCY, "order": "market_cap_desc", "per_page": n, "page": 1}
    data = safe_request(url, params=params)
    return [row["id"] for row in data]

def run_etl(coin_ids: List[str] | None = None, days: int | str = DAYS):
    if coin_ids is None:
        coin_ids = DEFAULT_COINS

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    logging.info(f"Moedas: {coin_ids} | Período: {days} dias")
    frames = []
    for cid in coin_ids:
        logging.info(f"Baixando {cid}…")
        df_c = fetch_market_chart(cid, days=days)
        if df_c.empty:
            logging.warning(f"Sem dados para {cid}.")
            continue
        frames.append(df_c)
        time.sleep(1.1)

    if not frames:
        raise RuntimeError("Nenhum dado retornado.")

    df_all = pd.concat(frames, ignore_index=True)
    df_all = compute_metrics(df_all)

    df_all.to_csv(PRICES_CSV, index=False)
    logging.info(f"Salvo: {PRICES_CSV} ({len(df_all):,} linhas)")

    time.sleep(4)

    meta = fetch_coin_metadata(sorted(df_all['coin_id'].unique().tolist()))
    meta.to_csv(META_CSV, index=False)
    logging.info(f"Salvo: {META_CSV}")

if __name__ == "__main__":
    coins = DEFAULT_COINS
    run_etl(coin_ids=coins, days=DAYS)
