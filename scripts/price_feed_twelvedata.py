#!/usr/bin/env python3
"""
Price Feed - Twelve Data
Remplace Yahoo Finance pour EUR/USD
Timeframes : 5M, 1H, 4H
Cron : */5 * * * * pour le 5M (1H et 4H insérés au passage)
"""

import requests
import psycopg2
from datetime import datetime, timezone
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

API_KEY = "f77115fae5cd47baa4a5f48f951cd066"
BASE_URL = "https://api.twelvedata.com/time_series"

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME", "trading"),
    "user": os.getenv("DB_USER", "trading"),
    "password": os.getenv("DB_PASS", "Trading2026"),
}

TIMEFRAMES = {
    "5M":  {"interval": "5min",  "outputsize": 12},  # 1h de données
    "1H":  {"interval": "1h",    "outputsize": 6},   # 6h de données
    "4H":  {"interval": "4h",    "outputsize": 3},   # 12h de données
    "1D":  {"interval": "1day",  "outputsize": 5},   # 5 jours
    "1W":  {"interval": "1week", "outputsize": 3},   # 3 semaines
}

def fetch_candles(interval: str, outputsize: int) -> list:
    params = {
        "symbol": "EUR/USD",
        "interval": interval,
        "outputsize": outputsize,
        "apikey": API_KEY,
        "timezone": "UTC",
        "format": "JSON"
    }
    r = requests.get(BASE_URL, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()

    if "values" not in data:
        logging.error(f"Pas de données pour {interval}: {data.get('message', data.get('status'))}")
        return []

    candles = []
    for v in data["values"]:
        candles.append({
            "candle_time": v["datetime"],
            "open":  float(v["open"]),
            "high":  float(v["high"]),
            "low":   float(v["low"]),
            "close": float(v["close"]),
            "volume": 0
        })
    return candles

def insert_candles(conn, tf: str, candles: list) -> int:
    if not candles:
        return 0
    cur = conn.cursor()
    inserted = 0
    for c in candles:
        cur.execute("""
            INSERT INTO prices_smc (timeframe, candle_time, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (timeframe, candle_time) DO UPDATE SET
                open   = EXCLUDED.open,
                high   = EXCLUDED.high,
                low    = EXCLUDED.low,
                close  = EXCLUDED.close,
                volume = EXCLUDED.volume
        """, (tf, c["candle_time"], c["open"], c["high"], c["low"], c["close"], c["volume"]))
        if cur.rowcount > 0:
            inserted += 1
    conn.commit()
    cur.close()
    return inserted

def should_fetch(tf: str, now: datetime) -> bool:
    """Éviter les appels inutiles selon le timeframe"""
    minute = now.minute
    if tf == "5M":
        return True  # toujours (cron toutes les 5min)
    if tf == "1H":
        return minute < 6  # seulement en début d'heure
    if tf == "4H":
        return minute < 6 and now.hour % 4 == 0  # toutes les 4h
    if tf == "1D":
        return now.hour == 0 and now.minute < 6
    if tf == "1W":
        return now.weekday() == 0 and now.hour == 0 and now.minute < 6
    return True

def main():
    now = datetime.now(timezone.utc)
    logging.info(f"Price Feed Twelve Data — {now.strftime('%Y-%m-%d %H:%M UTC')}")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        logging.error(f"Connexion DB impossible: {e}")
        return

    total = 0
    for tf, cfg in TIMEFRAMES.items():
        if not should_fetch(tf, now):
            logging.info(f"  {tf} — skipped (hors cycle)")
            continue
        try:
            candles = fetch_candles(cfg["interval"], cfg["outputsize"])
            n = insert_candles(conn, tf, candles)
            logging.info(f"  {tf} — {len(candles)} récupérées, {n} insérées")
            total += n
        except Exception as e:
            logging.error(f"  {tf} — erreur: {e}")

    conn.close()
    logging.info(f"Total inséré: {total} bougies")

if __name__ == "__main__":
    main()
