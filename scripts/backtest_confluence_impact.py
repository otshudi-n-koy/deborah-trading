#!/usr/bin/env python3
"""
backtest_confluence_impact.py
Isole l'impact du filtre de confluence H1/H4 obligatoire (signal_generator.py,
etape 4) sur le volume de signaux generes, en simulant le pipeline complet
DEUX FOIS sur la meme periode :
  - SCENARIO A (prod actuelle) : confluence H1/H4 obligatoire
  - SCENARIO B (contrefactuel) : bias H1 seul suffit (H4 ignore)

Objectif : verifier l'hypothese que la rarete des signaux, observee en
cassure nette apres la semaine du 29/06/2026 (19 signaux -> 7 -> 2 -> 1),
est attribuable au filtre de confluence plutot qu'a d'autres facteurs
(SELL-only depuis le 16/07, buffer PD array, RR, etc.)

Portage fidele de analyze_structure() (structure_analyzer.py) et de la
logique entry/SL/TP (signal_generator.py etapes 7-12), coherent avec
backtest_buy_filter_strict.py et backtest_confluence_timeframes.py.

Lecture seule sur la DB (aucun INSERT/UPDATE) - pur backtest offline.
"""

import psycopg2
from datetime import datetime

DB_CONFIG = {
    'host': 'localhost', 'port': 5432,
    'dbname': 'trading', 'user': 'trading', 'password': 'Trading2026'
}

BUFFER_NEUTRAL_PIPS = 0.0005
MIN_SIGNIFICANT_PIPS = 0.0010
RECENT_LOOKBACK = 10
ARRAY_BUFFER = 0.00050   # 5 pips, valeur actuelle en prod depuis le 04/08/2026
MIN_SL = 0.00100
SL_BUFFER = 0.00010
MIN_RR = 1.5


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


# ---------------------------------------------------------------------------
# Portage fidele de structure_analyzer.analyze_structure() (identique aux
# versions deja validees dans les backtests precedents)
# ---------------------------------------------------------------------------
def analyze_structure(candles, window_size=3):
    if len(candles) < window_size * 2 + 1:
        return {'bias': 'NEUTRAL', 'swing_high': None, 'swing_low': None, 'liquidity_target': None}

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
        return {'bias': bias, 'swing_high': fH, 'swing_low': fL,
                'liquidity_target': fL if bias == 'BEARISH' else fH}

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

    lower_lows = [s for s in swing_lows if s['price'] < current_close]
    higher_highs = [s for s in swing_highs if s['price'] > current_close]
    if bias == 'BULLISH':
        liq = min(higher_highs, key=lambda s: s['price'])['price'] if higher_highs else round(current_close + 0.005, 5)
    else:
        liq = max(lower_lows, key=lambda s: s['price'])['price'] if lower_lows else round(current_close - 0.005, 5)

    return {'bias': bias, 'swing_high': last_hh['price'], 'swing_low': last_ll['price'],
            'liquidity_target': liq}


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


def load_pd_arrays(conn, start_date):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, type, direction, price_high, price_low, price_eq,
               candle_time, invalidated_at
        FROM pd_arrays_smc
        WHERE timeframe = 'M5' AND candle_time >= %s
        ORDER BY candle_time ASC
    """, (start_date,))
    rows = cur.fetchall()
    cur.close()
    return [{'id': r[0], 'type': r[1], 'direction': r[2], 'price_high': float(r[3]),
             'price_low': float(r[4]), 'price_eq': float(r[5]),
             'candle_time': r[6], 'invalidated_at': r[7]} for r in rows]


def try_build_signal(bias, current_price, active_arrays, struct):
    """
    Reproduit fidelement signal_generator.py etapes 7-12 (selection PD array
    + construction entry/SL/TP/RR), independant de la source du DB en live -
    ici tous les parametres sont deja fournis par la simulation.
    Retourne (signal_dict_ou_None, reason).
    """
    direction = 'bullish' if bias == 'BULLISH' else 'bearish'
    candidates = [a for a in active_arrays if a['direction'] == direction]
    if not candidates:
        return None, 'aucun_pd_array_direction'

    best_array = None
    for arr in candidates:
        if (current_price <= arr['price_high'] + ARRAY_BUFFER and
                current_price >= arr['price_low'] - ARRAY_BUFFER):
            best_array = arr
            break
    if not best_array:
        return None, 'prix_hors_pd_array'

    swing_high, swing_low, liq_target = struct['swing_high'], struct['swing_low'], struct['liquidity_target']
    if swing_high is None or swing_low is None or liq_target is None:
        return None, 'structure_incomplete'

    if bias == 'BULLISH':
        entry = best_array['price_eq']
        sl = round(best_array['price_low'] - SL_BUFFER, 5)
        if entry - sl < MIN_SL:
            sl = round(entry - MIN_SL, 5)
        tp = swing_high if liq_target < current_price + 0.0015 else liq_target
        if tp <= current_price:
            tp = liq_target
        if tp <= entry:
            return None, 'tp_invalide'
    else:
        entry = best_array['price_eq']
        sl = round(best_array['price_high'] + SL_BUFFER, 5)
        if sl - entry < MIN_SL:
            sl = round(entry + MIN_SL, 5)
        tp = swing_low if liq_target > current_price - 0.0015 else liq_target
        if tp >= current_price:
            tp = liq_target
        if tp >= entry:
            return None, 'tp_invalide'

    risk = abs(entry - sl)
    reward = abs(tp - entry)
    rr = round(reward / risk, 2) if risk > 0 else 0
    if rr < MIN_RR:
        return None, f'rr_insuffisant_{rr}'

    return {'signal_type': 'BUY_LIMIT' if bias == 'BULLISH' else 'SELL_LIMIT',
            'entry': entry, 'sl': sl, 'tp': tp, 'rr': rr}, None


def run_backtest(conn, start_date):
    m5 = load_candles(conn, 'M5', start_date)
    h1 = load_candles(conn, 'H1', start_date)
    h4 = load_candles(conn, 'H4', start_date)
    pd_arrays = load_pd_arrays(conn, start_date)

    print(f"Bougies chargees: M5={len(m5)} H1={len(h1)} H4={len(h4)} | PD arrays={len(pd_arrays)}")

    results_a = []  # avec confluence obligatoire (comportement prod actuel)
    results_b = []  # sans confluence obligatoire (H1 seul suffit)
    reasons_a, reasons_b = {}, {}

    # Structure recalculee une fois par heure (mimant structure_analyzer.py),
    # on avance donc sur les points H1 plutot que M5 pour limiter le cout de
    # calcul (analyze_structure recalcule sur ~480 bougies a chaque appel).
    LOOKBACK_H1 = 20 * 24
    LOOKBACK_H4 = 30 * 6  # reduit vs prod (60j) faute d'historique H4 suffisant, cf backtest_confluence_timeframes.py

    for i in range(LOOKBACK_H1, len(h1)):
        h1_window = h1[max(0, i - LOOKBACK_H1):i + 1]
        t_now = h1_window[-1]['candle_time']
        struct_h1 = analyze_structure(h1_window, window_size=3)
        bias = struct_h1['bias']

        h4_window = [c for c in h4 if c['candle_time'] <= t_now][-LOOKBACK_H4:]
        if len(h4_window) < 7:
            continue
        struct_h4 = analyze_structure(h4_window, window_size=3)
        h4_bias = struct_h4['bias']

        confluence = (bias == h4_bias and bias != 'NEUTRAL')

        if not bias or bias == 'NEUTRAL':
            continue  # ce check existe dans les deux scenarios (independant de la confluence)

        current_price = h1_window[-1]['close']
        active_arrays = [
            a for a in pd_arrays
            if a['candle_time'] <= t_now
            and (a['invalidated_at'] is None or a['invalidated_at'] > t_now)
        ]

        # SCENARIO A : confluence obligatoire (comportement prod actuel)
        if confluence:
            sig, reason = try_build_signal(bias, current_price, active_arrays, struct_h1)
            if sig:
                results_a.append({**sig, 'time': t_now})
            else:
                reasons_a[reason] = reasons_a.get(reason, 0) + 1
        else:
            reasons_a['pas_de_confluence'] = reasons_a.get('pas_de_confluence', 0) + 1

        # SCENARIO B : H1 seul suffit (H4 ignore)
        sig, reason = try_build_signal(bias, current_price, active_arrays, struct_h1)
        if sig:
            results_b.append({**sig, 'time': t_now})
        else:
            reasons_b[reason] = reasons_b.get(reason, 0) + 1

    return results_a, reasons_a, results_b, reasons_b


if __name__ == '__main__':
    conn = get_conn()

    # Periode post-cassure identifiee (semaine du 29/06 = pic, puis chute nette)
    START = '2026-06-29'

    print(f"Backtest impact confluence H1/H4, depuis {START}\n")
    results_a, reasons_a, results_b, reasons_b = run_backtest(conn, START)

    print(f"\n=== SCENARIO A (confluence obligatoire, PROD ACTUELLE) ===")
    print(f"Signaux valides generes: {len(results_a)}")
    print("Raisons de rejet:")
    for k, v in sorted(reasons_a.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")

    print(f"\n=== SCENARIO B (H1 seul, H4 ignore) ===")
    print(f"Signaux valides generes: {len(results_b)}")
    print("Raisons de rejet:")
    for k, v in sorted(reasons_b.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")

    if results_a:
        n_buy_a = len([r for r in results_a if r['signal_type'] == 'BUY_LIMIT'])
        n_sell_a = len([r for r in results_a if r['signal_type'] == 'SELL_LIMIT'])
        print(f"\nScenario A - repartition: BUY={n_buy_a} SELL={n_sell_a}")
    if results_b:
        n_buy_b = len([r for r in results_b if r['signal_type'] == 'BUY_LIMIT'])
        n_sell_b = len([r for r in results_b if r['signal_type'] == 'SELL_LIMIT'])
        print(f"Scenario B - repartition: BUY={n_buy_b} SELL={n_sell_b}")

    print(f"\n=== IMPACT ISOLE DE LA CONFLUENCE ===")
    print(f"Signaux supplementaires generes SANS l'obligation de confluence: {len(results_b) - len(results_a)}")
    if len(results_b) > 0:
        ratio = round(len(results_a) / len(results_b) * 100, 1)
        print(f"Le filtre de confluence ne laisse passer que {ratio}% des signaux qui auraient ete generes sans lui")

    conn.close()
