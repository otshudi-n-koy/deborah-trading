#!/usr/bin/env python3
"""
backtest_blocked_zone_opportunities.py
Reconstruit precisement le setup (entry/sl/tp) a chacun des 39 timestamps
ou une regle de zone (BUY!=PREMIUM ou SELL!=DISCOUNT) a bloque un signal
reel, en ignorant volontairement la regle de zone pour voir ce que le
setup aurait donne, puis simule contre les vraies bougies M5.

Meme moteur (analyze_structure, matching PD array) que tous les backtests
de la semaine.

Lecture seule sur la DB (aucun INSERT/UPDATE) - pur backtest offline.
"""

import psycopg2
from datetime import datetime

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
MIN_RR_BUY = 1.2
MIN_RR_SELL = 1.2
ATR_MIN_PIPS = 3.0

EVENTS_RAW = """2026-09-10 12:15:00,SELL
2026-09-10 12:46:00,SELL
2026-09-10 13:15:00,SELL
2026-09-10 13:35:00,SELL
2026-09-10 14:00:00,SELL
2026-09-11 12:45:00,SELL
2026-09-11 14:31:00,BUY
2026-09-11 15:16:00,BUY
2026-09-14 06:30:00,SELL
2026-09-14 06:55:00,SELL
2026-09-14 07:00:00,SELL
2026-09-14 07:55:00,SELL
2026-09-14 08:06:00,SELL
2026-09-14 08:25:00,SELL
2026-09-14 08:51:00,SELL
2026-09-14 09:11:00,SELL
2026-09-14 09:31:00,SELL
2026-09-14 12:46:00,SELL
2026-09-14 13:00:00,SELL
2026-09-14 13:45:00,SELL
2026-09-14 14:05:00,SELL
2026-09-14 15:11:00,SELL
2026-09-14 15:36:00,SELL
2026-09-14 16:41:00,SELL
2026-09-16 13:16:00,SELL
2026-09-16 13:26:00,SELL
2026-09-16 14:16:00,SELL
2026-09-16 14:26:00,SELL
2026-09-16 18:25:00,SELL
2026-09-16 18:45:00,SELL
2026-09-16 19:06:00,SELL
2026-09-16 20:20:00,SELL
2026-09-16 20:30:00,SELL
2026-09-16 21:20:00,SELL
2026-09-18 11:15:00,SELL
2026-09-18 11:30:00,SELL
2026-09-18 12:55:00,SELL
2026-09-18 15:35:00,SELL
2026-09-18 16:11:00,SELL"""


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


def load_h1_before(conn, t_now, lookback=480):
    cur = conn.cursor()
    cur.execute("""
        SELECT candle_time, open, high, low, close FROM prices_smc
        WHERE timeframe='H1' AND candle_time <= %s ORDER BY candle_time DESC LIMIT %s
    """, (t_now, lookback))
    rows = cur.fetchall()
    cur.close()
    rows.reverse()
    return [{'candle_time': r[0], 'open': float(r[1]), 'high': float(r[2]),
             'low': float(r[3]), 'close': float(r[4])} for r in rows]


def load_pd_arrays_active(conn, t_now):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, direction, price_high, price_low, price_eq, candle_time
        FROM pd_arrays_smc
        WHERE timeframe='M5' AND candle_time <= %s
        AND (invalidated_at IS NULL OR invalidated_at > %s)
        ORDER BY candle_time ASC
    """, (t_now, t_now))
    rows = cur.fetchall()
    cur.close()
    return [{'id': r[0], 'direction': r[1], 'price_high': float(r[2]),
             'price_low': float(r[3]), 'price_eq': float(r[4]), 'candle_time': r[5]} for r in rows]


def load_current_price(conn, t_now):
    cur = conn.cursor()
    cur.execute("""
        SELECT close FROM prices_smc WHERE timeframe='M5' AND candle_time <= %s
        ORDER BY candle_time DESC LIMIT 1
    """, (t_now,))
    row = cur.fetchone()
    cur.close()
    return float(row[0]) if row else None


def load_m5_after(conn, t_now, limit=2000):
    cur = conn.cursor()
    cur.execute("""
        SELECT candle_time, high, low FROM prices_smc
        WHERE timeframe='M5' AND candle_time > %s ORDER BY candle_time ASC LIMIT %s
    """, (t_now, limit))
    rows = cur.fetchall()
    cur.close()
    return [{'candle_time': r[0], 'high': float(r[1]), 'low': float(r[2])} for r in rows]


def try_build_signal(direction, current_price, active_arrays, struct):
    pd_direction = 'bullish' if direction == 'BUY' else 'bearish'
    candidates = [a for a in active_arrays if a['direction'] == pd_direction]
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
    entry = best_array['price_eq']
    if direction == 'BUY':
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


def simulate(direction, entry, sl, tp, m5_after):
    for c in m5_after:
        if direction == 'BUY':
            if c['low'] <= sl:
                return 'LOSS', -1.0
            if c['high'] >= tp:
                return 'WIN', (tp - entry) / (entry - sl) if entry > sl else 0
        else:
            if c['high'] >= sl:
                return 'LOSS', -1.0
            if c['low'] <= tp:
                return 'WIN', (entry - tp) / (sl - entry) if sl > entry else 0
    return None, None


if __name__ == '__main__':
    conn = get_conn()
    events = []
    for line in EVENTS_RAW.strip().split('\n'):
        ts, direction = line.split(',')
        events.append((datetime.strptime(ts, '%Y-%m-%d %H:%M:%S'), direction))

    print(f"{len(events)} clusters a reconstruire\n")

    trades = []
    n_reconstruction_failed = 0

    for t_now, direction in events:
        h1_window = load_h1_before(conn, t_now)
        if len(h1_window) < 10:
            n_reconstruction_failed += 1
            continue
        struct_h1 = analyze_structure(h1_window, window_size=3)

        current_price = load_current_price(conn, t_now)
        if current_price is None:
            n_reconstruction_failed += 1
            continue

        active_arrays = load_pd_arrays_active(conn, t_now)
        sig = try_build_signal(direction, current_price, active_arrays, struct_h1)
        if not sig:
            n_reconstruction_failed += 1
            print(f"  {t_now} {direction} | RECONSTRUCTION ECHOUEE (zone plus matchee au meme instant)")
            continue

        m5_after = load_m5_after(conn, t_now)
        result, rr_real = simulate(direction, sig['entry'], sig['sl'], sig['tp'], m5_after)
        if result is None:
            n_reconstruction_failed += 1
            print(f"  {t_now} {direction} | INCONCLUSIF (pas assez de donnees futures)")
            continue

        trades.append({'time': t_now, 'direction': direction, 'result': result, 'rr_real': rr_real})
        print(f"  {t_now} {direction} | entry={sig['entry']:.5f} sl={sig['sl']:.5f} tp={sig['tp']:.5f} rr={sig['rr']} | {result}")

    n = len(trades)
    print(f"\n=== RESUME ===")
    print(f"Reconstructions reussies: {n}/{len(events)} ({n_reconstruction_failed} echouees/inconclusives)")
    if n > 0:
        wins = [t for t in trades if t['result'] == 'WIN']
        wr = len(wins) / n
        avg_rr = sum(t['rr_real'] for t in wins) / len(wins) if wins else 0
        kelly = wr * avg_rr - (1 - wr)
        print(f"WIN: {len(wins)} | LOSS: {n - len(wins)}")
        print(f"WR: {round(wr*100,1)}% | RR moyen (gagnants): {round(avg_rr,2)} | Kelly: {round(kelly,3)}")

    conn.close()
