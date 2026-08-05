#!/usr/bin/env python3
"""
backtest_confluence_timeframes.py
Quantifie le taux de confluence (bias identique et non-NEUTRAL sur les deux
timeframes d'une paire) pour plusieurs couples candidats, afin de determiner
si H1/H4 (couple actuel de structure_analyzer.py) est optimal pour EUR/USD
en ICT, ou si un autre couple (M15/H1, H4/D1) offrirait plus d'opportunites
de confluence sans sacrifier la pertinence structurelle.

Portage fidele de analyze_structure() (structure_analyzer.py), identique a
celui deja valide dans backtest_buy_filter_strict.py.

Lecture seule sur la DB (aucun INSERT/UPDATE) - pur backtest offline.
"""

import psycopg2
from datetime import datetime, timedelta

DB_CONFIG = {
    'host': 'localhost', 'port': 5432,
    'dbname': 'trading', 'user': 'trading', 'password': 'Trading2026'
}

BUFFER_NEUTRAL_PIPS = 0.0005
MIN_SIGNIFICANT_PIPS = 0.0010
RECENT_LOOKBACK = 10


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


# ---------------------------------------------------------------------------
# Portage fidele de structure_analyzer.analyze_structure() (aucune modif de
# logique, identique a la version deja validee dans backtest_buy_filter_strict.py)
# ---------------------------------------------------------------------------
def analyze_structure(candles, window_size=3):
    if len(candles) < window_size * 2 + 1:
        return {'error': f'Pas assez de bougies: {len(candles)}'}

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
        SELECT candle_time, open, high, low, close
        FROM prices_smc
        WHERE timeframe = %s AND candle_time >= %s
        ORDER BY candle_time ASC
    """, (timeframe, start_date))
    rows = cur.fetchall()
    cur.close()
    return [{'candle_time': r[0], 'open': float(r[1]), 'high': float(r[2]),
             'low': float(r[3]), 'close': float(r[4])} for r in rows]


def compute_bias_series(candles, lookback_n):
    """
    Calcule le biais a CHAQUE bougie de ce timeframe (mimant la mise a jour
    native de structure_analyzer.py a chaque cloture), en utilisant une
    fenetre glissante des lookback_n dernieres bougies disponibles.
    Retourne une liste triee de (candle_time, bias).
    """
    series = []
    for i in range(lookback_n, len(candles)):
        window = candles[max(0, i - lookback_n):i + 1]
        result = analyze_structure(window, window_size=3)
        bias = result.get('bias', 'NEUTRAL')
        series.append((candles[i]['candle_time'], bias))
    return series


def compute_confluence_rate(series_fine, series_coarse, pair_label):
    """
    Pour chaque point de la serie fine, prend le dernier biais connu de la
    serie coarse a ou avant ce timestamp (mimant la lecture DB : la derniere
    ligne structure_smc disponible au moment T). Calcule le % de temps en
    confluence (bias identiques et non-NEUTRAL).
    """
    if not series_fine or not series_coarse:
        return None

    coarse_idx = 0
    n_coarse = len(series_coarse)
    n_total, n_confluence = 0, 0
    n_neutral_fine, n_neutral_coarse, n_disagree = 0, 0, 0

    for t_fine, bias_fine in series_fine:
        while coarse_idx + 1 < n_coarse and series_coarse[coarse_idx + 1][0] <= t_fine:
            coarse_idx += 1
        t_coarse, bias_coarse = series_coarse[coarse_idx]
        if t_coarse > t_fine:
            continue  # pas encore de donnee coarse disponible a cet instant

        n_total += 1
        if bias_fine == 'NEUTRAL':
            n_neutral_fine += 1
        elif bias_coarse == 'NEUTRAL':
            n_neutral_coarse += 1
        elif bias_fine == bias_coarse:
            n_confluence += 1
        else:
            n_disagree += 1

    if n_total == 0:
        return None

    return {
        'pair': pair_label,
        'n_points': n_total,
        'confluence_pct': round(100 * n_confluence / n_total, 1),
        'neutral_fine_pct': round(100 * n_neutral_fine / n_total, 1),
        'neutral_coarse_pct': round(100 * n_neutral_coarse / n_total, 1),
        'disagree_pct': round(100 * n_disagree / n_total, 1),
    }


if __name__ == '__main__':
    conn = get_conn()

    # Date d'evaluation commune (fenetre de reporting de la confluence)
    EVAL_START = datetime(2026, 5, 25)

    # Chargement depuis la date de demarrage NATIVE de chaque flux (pas EVAL_START),
    # pour que le lookback ait suffisamment d'historique reel disponible. Bug
    # corrige le 04/08/2026 : charger depuis EVAL_START directement privait le
    # lookback de toute marge, donnant 0 points calcules pour H4/D1.
    print("Chargement des donnees (depuis la date native de chaque flux)...")
    m15 = load_candles(conn, 'M15', '2026-03-13')
    h1 = load_candles(conn, 'H1', '2026-04-27')
    h4 = load_candles(conn, 'H4', '2026-05-07')
    d1 = load_candles(conn, 'D1', '2024-01-01')
    print(f"M15={len(m15)} H1={len(h1)} H4={len(h4)} D1={len(d1)}")

    # Lookback en nombre de bougies. H4 n'a que ~350 bougies au total depuis
    # son demarrage natif (07/05/2026) - lookback reduit en consequence (30
    # jours plutot que 60) pour laisser une fenetre d'evaluation exploitable.
    LOOKBACK_M15 = 5 * 24 * 4    # 5 jours de bougies M15 (96/jour)
    LOOKBACK_H1 = 20 * 24        # 20 jours de bougies H1 (comme structure_analyzer.py)
    LOOKBACK_H4 = 30 * 6         # 30 jours de bougies H4 (reduit vs prod, contrainte de donnees)
    LOOKBACK_D1 = 90             # 90 jours de bougies D1

    print("Calcul des series de biais (peut prendre un moment)...")
    series_m15_full = compute_bias_series(m15, LOOKBACK_M15)
    series_h1_full = compute_bias_series(h1, LOOKBACK_H1)
    series_h4_full = compute_bias_series(h4, LOOKBACK_H4)
    series_d1_full = compute_bias_series(d1, LOOKBACK_D1)

    # Trim : on ne garde que les points a partir de EVAL_START pour la
    # comparaison de confluence, meme si le calcul du bias a pu commencer
    # plus tot (grace au lookback charge depuis la date native).
    series_m15 = [(t, b) for t, b in series_m15_full if t >= EVAL_START]
    series_h1 = [(t, b) for t, b in series_h1_full if t >= EVAL_START]
    series_h4 = [(t, b) for t, b in series_h4_full if t >= EVAL_START]
    series_d1 = [(t, b) for t, b in series_d1_full if t >= EVAL_START]
    print(f"Points evalues (>= {EVAL_START.date()}): M15={len(series_m15)} H1={len(series_h1)} H4={len(series_h4)} D1={len(series_d1)}")

    pairs = [
        ('H1/H4 (ACTUEL EN PROD)', series_h1, series_h4),
        ('M15/H1', series_m15, series_h1),
        ('H4/D1', series_h4, series_d1),
    ]

    print("\n=== TAUX DE CONFLUENCE PAR PAIRE ===\n")
    for label, fine, coarse in pairs:
        result = compute_confluence_rate(fine, coarse, label)
        if result:
            print(f"--- {label} ---")
            for k, v in result.items():
                if k != 'pair':
                    print(f"  {k}: {v}")
            print()
        else:
            print(f"--- {label} --- PAS ASSEZ DE DONNEES\n")

    conn.close()
