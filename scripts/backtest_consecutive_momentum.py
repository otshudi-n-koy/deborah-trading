#!/usr/bin/env python3
"""
backtest_consecutive_momentum.py
Troisieme piste pour capter le cas du 18/08 soir (apres echec de
deplacement net cumule, ticket #58, et TP etendu, ticket suivant) : un
filtre de momentum base sur le nombre de bougies M5 consecutives dans la
meme direction (close < open pour bearish), plutot que sur l'amplitude ou
la distance. Une sequence de N bougies rouges consecutives, meme petites,
indique une pression directionnelle soutenue - independant de l'ATR.

Condition testee : signal accepte si ATR>=3p (comme actuellement) OU au
moins N bougies M5 consecutives dans la direction du biais. Meme pipeline
complet sinon (confluence H1/H4, buffer 5 pips, MIN_RR_SELL=1.2, buffer
neutre 2 pips).

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


def count_consecutive_bearish(candles_before, max_check=20):
    """Compte le nombre de bougies M5 consecutives (en partant de la plus
    recente) ou close < open (bougie rouge), tolerance : une bougie
    'neutre'/verte isolee ne casse pas la sequence si elle est suivie par
    un retour rouge immediat n'est PAS tolere ici (strict, sequence pure)."""
    count = 0
    for c in reversed(candles_before[-max_check:]):
        if c['close'] < c['open']:
            count += 1
        else:
            break
    return count


def try_build_sell_signal(current_price, active_arrays, struct):
    candidates = [a for a in active_arrays if a['direction'] == 'bearish']
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
    swing_low, liq_target = struct['swing_low'], struct['liquidity_target']
    if swing_low is None or liq_target is None:
        return None
    entry = best_array['price_eq']
    sl = round(best_array['price_high'] + SL_BUFFER, 5)
    if sl - entry < MIN_SL:
        sl = round(entry + MIN_SL, 5)
    tp = swing_low if liq_target > current_price - 0.0015 else liq_target
    if tp >= current_price:
        tp = liq_target
    if tp >= entry:
        return None
    risk = abs(entry - sl)
    reward = abs(tp - entry)
    rr = round(reward / risk, 2) if risk > 0 else 0
    if rr < MIN_RR_SELL:
        return None
    return {'entry': entry, 'sl': sl, 'tp': tp, 'rr': rr}


def simulate_trade(entry, sl, tp, m5_after):
    for c in m5_after:
        if c['high'] >= sl:
            return 'LOSS', -1.0
        if c['low'] <= tp:
            rr_real = (entry - tp) / (sl - entry) if sl > entry else 0
            return 'WIN', rr_real
    return None, None


def run_backtest(m5, h1, h4, pd_arrays, use_momentum, min_consecutive=5):
    LOOKBACK_H1 = 20 * 24
    LOOKBACK_H4 = 30 * 6
    trades = []
    n_added_by_momentum = 0

    for i in range(LOOKBACK_H1, len(h1)):
        h1_window = h1[max(0, i - LOOKBACK_H1):i + 1]
        t_now = h1_window[-1]['candle_time']
        struct_h1 = analyze_structure(h1_window, window_size=3)
        bias = struct_h1['bias']
        if bias != 'BEARISH':
            continue
        h4_window = [c for c in h4 if c['candle_time'] <= t_now][-LOOKBACK_H4:]
        if len(h4_window) < 7:
            continue
        struct_h4 = analyze_structure(h4_window, window_size=3)
        if struct_h4['bias'] != 'BEARISH':
            continue

        m5_before = [c for c in m5 if c['candle_time'] <= t_now]
        atr_pips = compute_atr_pips(m5_before, 14)
        atr_ok = atr_pips >= ATR_MIN_PIPS

        momentum_ok = False
        if use_momentum and not atr_ok:
            n_consec = count_consecutive_bearish(m5_before)
            momentum_ok = n_consec >= min_consecutive

        if not (atr_ok or momentum_ok):
            continue

        current_price = h1_window[-1]['close']
        active_arrays = [
            a for a in pd_arrays
            if a['candle_time'] <= t_now
            and (a['invalidated_at'] is None or a['invalidated_at'] > t_now)
        ]
        sig = try_build_sell_signal(current_price, active_arrays, struct_h1)
        if not sig:
            continue
        m5_after = [c for c in m5 if c['candle_time'] > t_now][:2000]
        result, rr_real = simulate_trade(sig['entry'], sig['sl'], sig['tp'], m5_after)
        if result is None:
            continue

        if momentum_ok and not atr_ok:
            n_added_by_momentum += 1

        trades.append({'result': result, 'rr_real': rr_real})

    return trades, n_added_by_momentum


def compute_metrics(trades):
    n = len(trades)
    if n == 0:
        return {'n_trades': 0}
    wins = [t for t in trades if t['result'] == 'WIN']
    wr = len(wins) / n
    avg_rr = sum(t['rr_real'] for t in wins) / len(wins) if wins else 0
    expectancy_r = wr * avg_rr - (1 - wr)
    return {'n_trades': n, 'win_rate_pct': round(wr * 100, 1),
            'avg_rr_realized': round(avg_rr, 2), 'expectancy_r': round(expectancy_r, 3)}


if __name__ == '__main__':
    conn = get_conn()
    START = '2026-06-29'
    print(f"Chargement des donnees depuis {START}...")
    m5 = load_candles(conn, 'M5', START)
    h1 = load_candles(conn, 'H1', START)
    h4 = load_candles(conn, 'H4', START)
    pd_arrays = load_pd_arrays(conn, START)
    print(f"M5={len(m5)} H1={len(h1)} H4={len(h4)} PD_arrays={len(pd_arrays)}\n")

    print("=== BASELINE (ATR seul, actuel) ===")
    trades_a, _ = run_backtest(m5, h1, h4, pd_arrays, use_momentum=False)
    for k, v in compute_metrics(trades_a).items():
        print(f"  {k}: {v}")

    for min_c in [3, 5, 7, 10]:
        print(f"\n=== ATR OU >= {min_c} bougies M5 consecutives ===")
        trades, added = run_backtest(m5, h1, h4, pd_arrays, use_momentum=True, min_consecutive=min_c)
        for k, v in compute_metrics(trades).items():
            print(f"  {k}: {v}")
        print(f"  dont setups ajoutes uniquement par le momentum : {added}")

    conn.close()
