#!/usr/bin/env python3
"""
backtest_h4_lag.py
Quantifie le delai entre le moment ou H1 bascule vers un biais tranche
(BULLISH ou BEARISH) et le moment ou H4 confirme la meme direction
(etablissant la confluence), sur toute la periode disponible.

Origine: post-mortem du 14/08/2026, ou H1 est reste BULLISH ~5h avant que
H4 ne confirme, laissant filer une partie du mouvement. Objectif : savoir
si ce delai de 5h est exceptionnel ou typique.

Lecture seule sur la DB (aucun INSERT/UPDATE) - pur backtest offline.
"""

import psycopg2

DB_CONFIG = {
    'host': 'localhost', 'port': 5432,
    'dbname': 'trading', 'user': 'trading', 'password': 'Trading2026'
}

BUFFER_NEUTRAL_PIPS = 0.0002
MIN_SIGNIFICANT_PIPS = 0.0010
RECENT_LOOKBACK = 10


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def analyze_structure(candles, window_size=3):
    if len(candles) < window_size * 2 + 1:
        return {'bias': 'NEUTRAL'}
    candles = sorted(candles, key=lambda c: c['candle_time'])
    swing_highs, swing_lows = [], []
    for i in range(window_size, len(candles) - window_size):
        c = candles[i]
        is_high = all(candles[i - j]['high'] < c['high'] and candles[i + j]['high'] < c['high']
                      for j in range(1, window_size + 1))
        is_low = all(candles[i - j]['low'] > c['low'] and candles[i + j]['low'] > c['low']
                     for j in range(1, window_size + 1))
        if is_high:
            swing_highs.append({'price': c['high'], 'time': c['candle_time']})
        if is_low:
            swing_lows.append({'price': c['low'], 'time': c['candle_time']})

    def filter_sig(swings, is_high):
        out = []
        for s in swings:
            if not out:
                out.append(s); continue
            last = out[-1]
            if abs(s['price'] - last['price']) >= 0.0010:
                out.append(s)
            elif is_high and s['price'] > last['price']:
                out[-1] = s
            elif not is_high and s['price'] < last['price']:
                out[-1] = s
        return out

    swing_highs = filter_sig(swing_highs, True)
    swing_lows = filter_sig(swing_lows, False)
    current_close = candles[-1]['close']

    if len(swing_highs) < 2 or len(swing_lows) < 2:
        recent = candles[-48:]
        fH = max(c['high'] for c in recent)
        fL = min(c['low'] for c in recent)
        rng = fH - fL
        eq50 = fL + rng * 0.5
        bias = 'NEUTRAL' if abs(current_close - eq50) < BUFFER_NEUTRAL_PIPS \
            else ('BEARISH' if current_close < eq50 else 'BULLISH')
        return {'bias': bias}

    last_hh, prev_hh = swing_highs[-1], swing_highs[-2]
    last_ll, prev_ll = swing_lows[-1], swing_lows[-2]
    recent = candles[-RECENT_LOOKBACK:]
    recent_low_val = min(c['low'] for c in recent)
    recent_high_val = max(c['high'] for c in recent)
    if recent_low_val < last_ll['price'] - MIN_SIGNIFICANT_PIPS:
        idx_low = min(range(len(recent)), key=lambda i: recent[i]['low'])
        last_ll = {'price': recent_low_val, 'time': recent[idx_low]['candle_time']}
    if recent_high_val > last_hh['price'] + MIN_SIGNIFICANT_PIPS:
        idx_high = max(range(len(recent)), key=lambda i: recent[i]['high'])
        last_hh = {'price': recent_high_val, 'time': recent[idx_high]['candle_time']}

    range_mid = (last_hh['price'] + last_ll['price']) / 2
    bias = 'NEUTRAL' if abs(current_close - range_mid) < BUFFER_NEUTRAL_PIPS \
        else ('BULLISH' if current_close > range_mid else 'BEARISH')
    return {'bias': bias}


def load_candles(conn, timeframe, start_date):
    cur = conn.cursor()
    cur.execute("""
        SELECT candle_time, open, high, low, close FROM prices_smc
        WHERE timeframe = %s AND candle_time >= %s ORDER BY candle_time ASC
    """, (timeframe, start_date))
    rows = cur.fetchall()
    cur.close()
    return [{'candle_time': r[0], 'open': float(r[1]), 'high': float(r[2]),
             'low': float(r[3]), 'close': float(r[4])} for r in rows]


def compute_bias_series(candles, lookback_n):
    series = []
    for i in range(lookback_n, len(candles)):
        window = candles[max(0, i - lookback_n):i + 1]
        result = analyze_structure(window, window_size=3)
        series.append((candles[i]['candle_time'], result['bias']))
    return series


def find_h4_confirmation_delays(series_h1, series_h4):
    delays = []
    details = []
    prev_h1_bias = None
    h4_idx = 0
    n_h4 = len(series_h4)

    for t1, bias1 in series_h1:
        if bias1 == prev_h1_bias or bias1 == 'NEUTRAL':
            prev_h1_bias = bias1
            continue
        prev_h1_bias = bias1

        while h4_idx + 1 < n_h4 and series_h4[h4_idx + 1][0] <= t1:
            h4_idx += 1
        if series_h4[h4_idx][0] > t1:
            continue

        if series_h4[h4_idx][1] == bias1:
            delays.append(0)
            details.append({'t1': t1, 'bias': bias1, 'delay_min': 0, 'confirmed_at': t1})
            continue

        confirmed_at = None
        idx_search = h4_idx
        for t1_next, bias1_next in series_h1:
            if t1_next <= t1:
                continue
            if bias1_next != bias1 and bias1_next != 'NEUTRAL':
                break
            while idx_search + 1 < n_h4 and series_h4[idx_search + 1][0] <= t1_next:
                idx_search += 1
            if series_h4[idx_search][1] == bias1:
                confirmed_at = t1_next
                break

        if confirmed_at:
            delay_min = (confirmed_at - t1).total_seconds() / 60
            delays.append(delay_min)
            details.append({'t1': t1, 'bias': bias1, 'delay_min': delay_min, 'confirmed_at': confirmed_at})
        else:
            delays.append(None)
            details.append({'t1': t1, 'bias': bias1, 'delay_min': None, 'confirmed_at': None})

    return delays, details


if __name__ == '__main__':
    conn = get_conn()
    START = '2026-06-29'

    print(f"Chargement des donnees depuis {START}...")
    h1 = load_candles(conn, 'H1', START)
    h4 = load_candles(conn, 'H4', START)
    print(f"H1={len(h1)} H4={len(h4)}\n")

    LOOKBACK_H1 = 20 * 24
    LOOKBACK_H4 = 30 * 6

    print("Calcul des series de biais...")
    series_h1 = compute_bias_series(h1, LOOKBACK_H1)
    series_h4 = compute_bias_series(h4, LOOKBACK_H4)
    print(f"Points: H1={len(series_h1)} H4={len(series_h4)}\n")

    delays, details = find_h4_confirmation_delays(series_h1, series_h4)

    confirmed = [d for d in delays if d is not None]
    never = [d for d in delays if d is None]

    print(f"=== RESULTATS ===")
    print(f"Transitions H1 vers biais tranche : {len(delays)}")
    print(f"  Confirmees par H4 avant que H1 ne reparte : {len(confirmed)}")
    print(f"  Jamais confirmees : {len(never)}")

    if confirmed:
        confirmed_sorted = sorted(confirmed)
        avg_delay = sum(confirmed) / len(confirmed)
        median_delay = confirmed_sorted[len(confirmed_sorted) // 2]
        max_delay = max(confirmed)
        print(f"\n  Delai moyen (parmi confirmees) : {avg_delay:.0f} min ({avg_delay/60:.1f}h)")
        print(f"  Delai median : {median_delay:.0f} min ({median_delay/60:.1f}h)")
        print(f"  Delai max : {max_delay:.0f} min ({max_delay/60:.1f}h)")
        print(f"  Delais individuels (min) : {[round(d) for d in confirmed_sorted]}")

    print(f"\n=== DETAIL DE TOUS LES CAS (tries par delai decroissant) ===")
    details_sorted = sorted(details, key=lambda d: (d['delay_min'] is None, -(d['delay_min'] or 0)))
    for d in details_sorted:
        delay_str = f"{d['delay_min']:.0f} min ({d['delay_min']/60:.1f}h)" if d['delay_min'] is not None else "JAMAIS CONFIRME"
        print(f"  H1 bascule {d['bias']} a {d['t1']} -> confirme H4 a {d['confirmed_at']} | delai = {delay_str}")

    conn.close()
