#!/usr/bin/env python3
"""
backtest_killzone_impact.py
Isole l'impact de la restriction aux killzones (London/NY/London Close,
check_killzone() dans signal_generator.py) sur le volume ET la qualite des
signaux, en simulant le pipeline complet DEUX FOIS sur la meme periode :
  - SCENARIO A (prod actuelle) : signal genere seulement en killzone
  - SCENARIO B (contrefactuel) : signal genere a toute heure, 24h/24

Objectif : verifier si la restriction killzone (pratique ICT standard,
jamais backtestee en A/B sur ces donnees - seuls des backtests unilateraux
"avec filtre session" existaient, cf. /root/backtest_with_session.py) ajoute
reellement de la valeur ou coupe des opportunites valides.

Reprend le pipeline complet valide dans backtest_rr_sensitivity.py
(confluence H1/H4 obligatoire, buffer 5 pips, MIN_RR decouple par direction
comme en prod actuelle : SELL=1.2, BUY=1.0), seule la restriction horaire
change entre les deux scenarios.

Lecture seule sur la DB (aucun INSERT/UPDATE) - pur backtest offline.
"""

import psycopg2
from collections import Counter

DB_CONFIG = {
    'host': 'localhost', 'port': 5432,
    'dbname': 'trading', 'user': 'trading', 'password': 'Trading2026'
}

BUFFER_NEUTRAL_PIPS = 0.0005
MIN_SIGNIFICANT_PIPS = 0.0010
RECENT_LOOKBACK = 10
ARRAY_BUFFER = 0.00050
MIN_SL = 0.00100
SL_BUFFER = 0.00010
MIN_RR_SELL = 1.2
MIN_RR_BUY = 1.0


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


def in_killzone(dt):
    """
    Reproduit check_killzone() de signal_generator.py (heure Paris, approxime
    en UTC+2 comme deja fait dans backtest_with_session.py - a affiner par
    saison si besoin).
    """
    h_paris = (dt.hour + 2) % 24
    if dt.weekday() >= 5:
        return False
    if 8 <= h_paris < 11:
        return True
    if 13 <= h_paris < 16:
        return True
    if 16 <= h_paris < 18:
        return True
    return False


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

    return {'signal_type': 'BUY_LIMIT' if bias == 'BULLISH' else 'SELL_LIMIT',
            'entry': entry, 'sl': sl, 'tp': tp, 'rr_theoretical': rr}


def simulate_trade(entry, sl, tp, bias, m5_after):
    for c in m5_after:
        if bias == 'BULLISH':
            if c['low'] <= sl:
                return 'LOSS', -1.0
            if c['high'] >= tp:
                rr_real = (tp - entry) / (entry - sl) if entry > sl else 0
                return 'WIN', rr_real
        else:
            if c['high'] >= sl:
                return 'LOSS', -1.0
            if c['low'] <= tp:
                rr_real = (entry - tp) / (sl - entry) if sl > entry else 0
                return 'WIN', rr_real
    return None, None


def run_backtest(m5, h1, h4, pd_arrays, require_killzone):
    LOOKBACK_H1 = 20 * 24
    LOOKBACK_H4 = 30 * 6

    trades = []

    for i in range(LOOKBACK_H1, len(h1)):
        h1_window = h1[max(0, i - LOOKBACK_H1):i + 1]
        t_now = h1_window[-1]['candle_time']

        if require_killzone and not in_killzone(t_now):
            continue

        struct_h1 = analyze_structure(h1_window, window_size=3)
        bias = struct_h1['bias']
        if not bias or bias == 'NEUTRAL':
            continue

        h4_window = [c for c in h4 if c['candle_time'] <= t_now][-LOOKBACK_H4:]
        if len(h4_window) < 7:
            continue
        struct_h4 = analyze_structure(h4_window, window_size=3)
        confluence = (bias == struct_h4['bias'] and bias != 'NEUTRAL')
        if not confluence:
            continue

        current_price = h1_window[-1]['close']
        active_arrays = [
            a for a in pd_arrays
            if a['candle_time'] <= t_now
            and (a['invalidated_at'] is None or a['invalidated_at'] > t_now)
        ]

        sig = try_build_signal(bias, current_price, active_arrays, struct_h1)
        if not sig:
            continue

        m5_after = [c for c in m5 if c['candle_time'] > t_now][:2000]
        result, rr_real = simulate_trade(sig['entry'], sig['sl'], sig['tp'], bias, m5_after)
        if result is None:
            continue

        trades.append({'result': result, 'rr_real': rr_real, 'signal_type': sig['signal_type'],
                        'time': t_now, 'in_killzone': in_killzone(t_now)})

    return trades


def compute_metrics(trades):
    n = len(trades)
    if n == 0:
        return {'n_trades': 0}
    wins = [t for t in trades if t['result'] == 'WIN']
    wr = len(wins) / n
    avg_rr = sum(t['rr_real'] for t in wins) / len(wins) if wins else 0
    kelly = wr * avg_rr - (1 - wr)
    return {
        'n_trades': n,
        'win_rate_pct': round(wr * 100, 1),
        'avg_rr_realized': round(avg_rr, 2),
        'expectancy_r': round(kelly, 3),
        'buy_sell': dict(Counter(t['signal_type'] for t in trades)),
    }


if __name__ == '__main__':
    conn = get_conn()
    START = '2026-06-29'

    print(f"Chargement des donnees depuis {START}...")
    m5 = load_candles(conn, 'M5', START)
    h1 = load_candles(conn, 'H1', START)
    h4 = load_candles(conn, 'H4', START)
    pd_arrays = load_pd_arrays(conn, START)
    print(f"M5={len(m5)} H1={len(h1)} H4={len(h4)} PD_arrays={len(pd_arrays)}\n")

    print("=== SCENARIO A (killzone obligatoire, PROD ACTUELLE) ===")
    trades_a = run_backtest(m5, h1, h4, pd_arrays, require_killzone=True)
    metrics_a = compute_metrics(trades_a)
    for k, v in metrics_a.items():
        print(f"  {k}: {v}")

    print("\n=== SCENARIO B (24h/24, sans restriction killzone) ===")
    trades_b = run_backtest(m5, h1, h4, pd_arrays, require_killzone=False)
    metrics_b = compute_metrics(trades_b)
    for k, v in metrics_b.items():
        print(f"  {k}: {v}")

    n_b_in_kz = len([t for t in trades_b if t['in_killzone']])
    n_b_out_kz = len([t for t in trades_b if not t['in_killzone']])
    print(f"\n=== DETAIL SCENARIO B ===")
    print(f"  Trades qui seraient tombes EN killzone: {n_b_in_kz}")
    print(f"  Trades qui seraient tombes HORS killzone: {n_b_out_kz}")

    trades_b_out = [t for t in trades_b if not t['in_killzone']]
    metrics_b_out = compute_metrics(trades_b_out)
    print(f"\n=== METRIQUES DES TRADES HORS-KILLZONE UNIQUEMENT (scenario B) ===")
    for k, v in metrics_b_out.items():
        print(f"  {k}: {v}")

    print(f"\n=== IMPACT ISOLE DE LA RESTRICTION KILLZONE ===")
    print(f"Trades supplementaires generes SANS restriction killzone: {len(trades_b) - len(trades_a)}")

    conn.close()
