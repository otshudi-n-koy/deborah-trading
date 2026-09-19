#!/usr/bin/env python3
"""
backtest_big_moves_opportunity_cost.py
ETAPE 1 du chantier #62 (strategie de continuation/breakout).

Detecte objectivement les "gros mouvements" directionnels (fenetre
glissante, deplacement net du prix au-dela d'un seuil sur une duree
donnee) sur toute la periode disponible, puis verifie si le pipeline
actuel (confluence H1/H4, buffer PD array, RR par direction, ATR>=3p,
buffer neutre 2p - meme logique que tous les backtests de la semaine)
genere ne serait-ce qu'UN SEUL signal valide (BUY ou SELL selon la
direction du mouvement) a un moment quelconque pendant la fenetre.

Objectif : quantifier le cout d'opportunite reel (nombre de mouvements
manques, pips cumules manques vs captes) plutot que de se fier a une
impression subjective.

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
ARRAY_BUFFER = 0.00050
SL_BUFFER = 0.00010
MIN_SL = 0.00100
MIN_RR_SELL = 1.2
MIN_RR_BUY = 1.0
ATR_MIN_PIPS = 3.0


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


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
        SELECT candle_time, open, high, low, close FROM prices_smc
        WHERE timeframe = %s AND candle_time >= %s ORDER BY candle_time ASC
    """, (timeframe, start_date))
    rows = cur.fetchall()
    cur.close()
    return [{'candle_time': r[0], 'open': float(r[1]), 'high': float(r[2]),
             'low': float(r[3]), 'close': float(r[4])} for r in rows]


def load_pd_arrays(conn, start_date):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, type, direction, price_high, price_low, price_eq, candle_time, invalidated_at
        FROM pd_arrays_smc WHERE timeframe = 'M5' AND candle_time >= %s ORDER BY candle_time ASC
    """, (start_date,))
    rows = cur.fetchall()
    cur.close()
    return [{'id': r[0], 'type': r[1], 'direction': r[2], 'price_high': float(r[3]),
             'price_low': float(r[4]), 'price_eq': float(r[5]),
             'candle_time': r[6], 'invalidated_at': r[7]} for r in rows]


def compute_atr_pips(candles_before, n=14):
    if len(candles_before) < n:
        return 0
    recent = candles_before[-n:]
    avg_range = sum(c['high'] - c['low'] for c in recent) / n
    return round(avg_range * 10000, 1)


def detect_big_moves(m5, window_hours, threshold_pips):
    window_bars = window_hours * 12
    moves = []
    i = 0
    n = len(m5)
    while i < n - window_bars:
        start = m5[i]
        end = m5[i + window_bars]
        delta_pips = round((end['close'] - start['close']) * 10000, 1)
        if abs(delta_pips) >= threshold_pips:
            direction = 'BULLISH' if delta_pips > 0 else 'BEARISH'
            moves.append({
                'start_time': start['candle_time'], 'end_time': end['candle_time'],
                'start_price': start['close'], 'end_price': end['close'],
                'delta_pips': delta_pips, 'direction': direction,
            })
            i += window_bars
        else:
            i += 1
    return moves


def try_build_signal(bias, current_price, active_arrays, struct):
    direction = 'bullish' if bias == 'BULLISH' else 'bearish'
    candidates = [a for a in active_arrays if a['direction'] == direction]
    if not candidates:
        return None
    best_array = None
    for arr in candidates:
        if (current_price <= arr['price_high'] + ARRAY_BUFFER and
                current_price >= arr['price_low'] - ARRAY_BUFFER):
            best_array = arr
            break
    if not best_array:
        return None
    swing_high, swing_low, liq_target = struct['swing_high'], struct['swing_low'], struct['liquidity_target']
    if swing_high is None or swing_low is None or liq_target is None:
        return None
    if bias == 'BULLISH':
        entry = best_array['price_eq']
        sl = round(best_array['price_low'] - SL_BUFFER, 5)
        if entry - sl < MIN_SL:
            sl = round(entry - MIN_SL, 5)
        tp = swing_high if liq_target < current_price + 0.0015 else liq_target
        if tp <= current_price:
            tp = liq_target
        if tp <= entry:
            return None
        min_rr = MIN_RR_BUY
    else:
        entry = best_array['price_eq']
        sl = round(best_array['price_high'] + SL_BUFFER, 5)
        if sl - entry < MIN_SL:
            sl = round(entry + MIN_SL, 5)
        tp = swing_low if liq_target > current_price - 0.0015 else liq_target
        if tp >= current_price:
            tp = liq_target
        if tp >= entry:
            return None
        min_rr = MIN_RR_SELL
    risk = abs(entry - sl)
    reward = abs(tp - entry)
    rr = round(reward / risk, 2) if risk > 0 else 0
    if rr < min_rr:
        return None
    return {'entry': entry, 'sl': sl, 'tp': tp, 'rr': rr}


def check_signal_during_window(m5, h1, h4, pd_arrays, move):
    LOOKBACK_H1 = 20 * 24
    LOOKBACK_H4 = 30 * 6

    window_m5 = [c for c in m5 if move['start_time'] <= c['candle_time'] <= move['end_time']]
    n_signals = 0
    for c in window_m5:
        t_now = c['candle_time']
        h1_window = [x for x in h1 if x['candle_time'] <= t_now][-LOOKBACK_H1:]
        if len(h1_window) < 10:
            continue
        struct_h1 = analyze_structure(h1_window, window_size=3)
        bias = struct_h1['bias']
        if bias != move['direction']:
            continue
        h4_window = [x for x in h4 if x['candle_time'] <= t_now][-LOOKBACK_H4:]
        if len(h4_window) < 7:
            continue
        struct_h4 = analyze_structure(h4_window, window_size=3)
        if struct_h4['bias'] != move['direction']:
            continue
        m5_before = [x for x in m5 if x['candle_time'] <= t_now]
        atr_pips = compute_atr_pips(m5_before, 14)
        if atr_pips < ATR_MIN_PIPS:
            continue
        active_arrays = [
            a for a in pd_arrays
            if a['candle_time'] <= t_now
            and (a['invalidated_at'] is None or a['invalidated_at'] > t_now)
        ]
        sig = try_build_signal(bias, c['close'], active_arrays, struct_h1)
        if sig:
            n_signals += 1
    return n_signals


if __name__ == '__main__':
    conn = get_conn()
    START = '2026-06-29'

    print(f"Chargement des donnees depuis {START}...")
    m5 = load_candles(conn, 'M5', START)
    h1 = load_candles(conn, 'H1', START)
    h4 = load_candles(conn, 'H4', START)
    pd_arrays = load_pd_arrays(conn, START)
    print(f"M5={len(m5)} H1={len(h1)} H4={len(h4)} PD_arrays={len(pd_arrays)}\n")

    for window_hours, threshold_pips in [(6, 40), (5, 45), (4, 35)]:
        print(f"=== Detection : mouvements >= {threshold_pips}p sur {window_hours}h ===")
        moves = detect_big_moves(m5, window_hours, threshold_pips)
        print(f"  Mouvements detectes: {len(moves)}")
        total_pips = sum(abs(m['delta_pips']) for m in moves)
        print(f"  Pips cumules (valeur absolue): {total_pips:.1f}p")

        n_captured, n_missed = 0, 0
        captured_pips, missed_pips = 0, 0
        for move in moves:
            n_sig = check_signal_during_window(m5, h1, h4, pd_arrays, move)
            if n_sig > 0:
                n_captured += 1
                captured_pips += abs(move['delta_pips'])
            else:
                n_missed += 1
                missed_pips += abs(move['delta_pips'])
            print(f"    {move['start_time']} -> {move['end_time']} | {move['direction']} "
                  f"| {move['delta_pips']:+.1f}p | signaux generes pendant fenetre: {n_sig}")

        print(f"\n  RESUME: {n_captured} mouvements avec >=1 signal, {n_missed} totalement manques")
        print(f"  Pips captes (potentiellement): {captured_pips:.1f}p | Pips manques: {missed_pips:.1f}p")
        print()

    conn.close()
