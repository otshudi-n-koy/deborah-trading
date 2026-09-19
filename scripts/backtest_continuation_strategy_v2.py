#!/usr/bin/env python3
"""
backtest_continuation_strategy_v2.py
Chantier #62, ETAPE 2 v2 : decouple le declencheur (CHoCH confirme a un
instant T) du matching de zone (recherche dans une fenetre [T, T+W]
apres l'evenement, au lieu d'exiger simultaneite stricte a la bougie
M5 pres - diagnostic du 19/08 soir sur v1, qui donnait le meme trade
unique (n=1) quels que soient les seuils secondaires assouplis).

Detection d'evenement CHoCH : transition "non confirme -> confirme" sur
M5 (edge detection), dans le sens du biais H1/H4 confluent a cet instant.
Zone d'entree : formee A OU APRES l'evenement CHoCH (liee structurellement
au mouvement, pas juste "recente" independamment), matchee a un moment
quelconque dans la fenetre de tolerance W apres l'evenement.

SL sur le swing d'origine (au moment de l'evenement CHoCH), TP au
liquidity_target (au moment de l'entree effective), RR variable teste.

Lecture seule sur la DB (aucun INSERT/UPDATE) - pur backtest offline.
"""

import psycopg2
import sys
sys.path.insert(0, '/opt/deborah-trading/scripts')
import smc_choch_bos
from datetime import timedelta

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


def detect_choch_events(m5, h1, h4):
    LOOKBACK_H1 = 20 * 24
    LOOKBACK_H4 = 30 * 6
    events = []
    was_confirmed = {'BULLISH': False, 'BEARISH': False}

    for i, c in enumerate(m5):
        t_now = c['candle_time']
        if i < 90:
            continue
        h1_window = [x for x in h1 if x['candle_time'] <= t_now][-LOOKBACK_H1:]
        if len(h1_window) < 10:
            continue
        struct_h1 = analyze_structure(h1_window, window_size=3)
        bias = struct_h1['bias']
        if bias == 'NEUTRAL':
            was_confirmed['BULLISH'] = False
            was_confirmed['BEARISH'] = False
            continue
        h4_window = [x for x in h4 if x['candle_time'] <= t_now][-LOOKBACK_H4:]
        if len(h4_window) < 7:
            continue
        struct_h4 = analyze_structure(h4_window, window_size=3)
        if struct_h4['bias'] != bias:
            continue

        m5_window_choch = m5[max(0, i - 89):i + 1]
        facts = smc_choch_bos.get_choch_bos_sweep_facts(m5_window_choch, bias)
        is_confirmed_now = facts['choch_detected'] and facts['bos_confirmed']

        if is_confirmed_now and not was_confirmed[bias]:
            events.append({
                'time': t_now, 'direction': bias,
                'swing_high': struct_h1['swing_high'], 'swing_low': struct_h1['swing_low'],
            })
        was_confirmed[bias] = is_confirmed_now
        other = 'BEARISH' if bias == 'BULLISH' else 'BULLISH'
        was_confirmed[other] = False

    return events


def try_match_zone(bias, current_price, active_arrays, event_time):
    direction = 'bullish' if bias == 'BULLISH' else 'bearish'
    candidates = [a for a in active_arrays
                  if a['direction'] == direction and a['candle_time'] >= event_time]
    if not candidates:
        return None
    candidates_sorted = sorted(candidates, key=lambda a: a['candle_time'])
    best_array = None
    for arr in candidates_sorted:
        if (current_price <= arr['price_high'] + ARRAY_BUFFER and
                current_price >= arr['price_low'] - ARRAY_BUFFER):
            best_array = arr
            break
    if not best_array:
        return None
    return {'entry': best_array['price_eq'], 'zone_id': best_array['id']}


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


def compute_intermediate_swing(m5, event_time, direction, lookback_bars):
    """SL alternatif : swing LOCAL/intermediaire sur les lookback_bars
    dernieres bougies M5 avant l'evenement, au lieu du swing structurel H1
    (qui peut remonter a plusieurs heures/jours avant le vrai depart du
    mouvement de continuation). Plus proche du prix, risque plus contenu."""
    window = [c for c in m5 if c['candle_time'] <= event_time][-lookback_bars:]
    if not window:
        return None
    if direction == 'BULLISH':
        return min(c['low'] for c in window)
    else:
        return max(c['high'] for c in window)


def run_backtest_v3(m5, h1, h4, pd_arrays, events, window_min, min_rr, sl_mode, sl_param):
    """sl_mode: 'origin' (comme v2, swing H1 complet), 'intermediate'
    (swing local sur sl_param bougies M5), 'atr' (multiple ATR, sl_param
    = multiplicateur)."""
    LOOKBACK_H1 = 20 * 24
    trades = []
    all_rr = []

    for ev in events:
        window_end = ev['time'] + timedelta(minutes=window_min)
        window_candles = [c for c in m5 if ev['time'] < c['candle_time'] <= window_end]
        trade_found = False

        for c in window_candles:
            if trade_found:
                break
            t_now = c['candle_time']
            h1_window = [x for x in h1 if x['candle_time'] <= t_now][-LOOKBACK_H1:]
            if len(h1_window) < 10:
                continue
            struct_h1 = analyze_structure(h1_window, window_size=3)
            if struct_h1['bias'] != ev['direction']:
                break

            m5_before = [x for x in m5 if x['candle_time'] <= t_now]
            atr_pips = compute_atr_pips(m5_before, 14)
            if atr_pips < ATR_MIN_PIPS:
                continue

            active_arrays = [
                a for a in pd_arrays
                if a['candle_time'] <= t_now
                and (a['invalidated_at'] is None or a['invalidated_at'] > t_now)
            ]
            match = try_match_zone(ev['direction'], c['close'], active_arrays, ev['time'])
            if not match:
                continue

            entry = match['entry']
            liq_target = struct_h1['liquidity_target']

            if sl_mode == 'origin':
                raw_sl = ev['swing_low'] if ev['direction'] == 'BULLISH' else ev['swing_high']
            elif sl_mode == 'intermediate':
                raw_sl = compute_intermediate_swing(m5, ev['time'], ev['direction'], sl_param)
                if raw_sl is None:
                    continue
            elif sl_mode == 'atr':
                atr_price = atr_pips * 0.0001
                raw_sl = entry - atr_price * sl_param if ev['direction'] == 'BULLISH' else entry + atr_price * sl_param
            else:
                continue

            if ev['direction'] == 'BULLISH':
                sl = round(raw_sl - SL_BUFFER, 5)
                if entry - sl < MIN_SL:
                    sl = round(entry - MIN_SL, 5)
                tp = liq_target
                if tp <= entry:
                    continue
            else:
                sl = round(raw_sl + SL_BUFFER, 5)
                if sl - entry < MIN_SL:
                    sl = round(entry + MIN_SL, 5)
                tp = liq_target
                if tp >= entry:
                    continue

            risk = abs(entry - sl)
            reward = abs(tp - entry)
            rr = round(reward / risk, 2) if risk > 0 else 0
            all_rr.append(rr)
            if rr < min_rr:
                continue

            m5_after = [x for x in m5 if x['candle_time'] > t_now][:2000]
            result, rr_real = simulate_trade(entry, sl, tp, ev['direction'], m5_after)
            if result is None:
                continue

            trades.append({'result': result, 'rr_real': rr_real, 'bias': ev['direction'],
                            'event_time': ev['time'], 'entry_time': t_now, 'rr_theoretical': rr})
            trade_found = True

    return trades, all_rr


def compute_metrics(trades):
    n = len(trades)
    if n == 0:
        return {'n_trades': 0}
    wins = [t for t in trades if t['result'] == 'WIN']
    wr = len(wins) / n
    avg_rr = sum(t['rr_real'] for t in wins) / len(wins) if wins else 0
    kelly = wr * avg_rr - (1 - wr)
    return {'n_trades': n, 'win_rate_pct': round(wr * 100, 1),
            'avg_rr_realized': round(avg_rr, 2), 'expectancy_r': round(kelly, 3)}


if __name__ == '__main__':
    conn = get_conn()
    START = '2026-05-22'  # extension 19/08 : M5 (convention "live", pas l'ancien
    # '5M' archive) demarre reellement a cette date - passe de 8 a 13 semaines
    # de donnees (+60%) pour un echantillon plus robuste sur les evenements
    # CHoCH+BOS, plutot que re-tester indefiniment sur le meme petit lot de
    # 29 evenements deja epuise sur la fenetre 29/06-19/08.

    print(f"Chargement des donnees depuis {START}...")
    m5 = load_candles(conn, 'M5', START)
    h1 = load_candles(conn, 'H1', START)
    h4 = load_candles(conn, 'H4', START)
    pd_arrays = load_pd_arrays(conn, START)
    print(f"M5={len(m5)} H1={len(h1)} H4={len(h4)} PD_arrays={len(pd_arrays)}\n")

    print("Detection des evenements CHoCH+BOS confirmes (edge detection)...")
    events = detect_choch_events(m5, h1, h4)
    print(f"  Evenements detectes: {len(events)}")
    for ev in events:
        print(f"    {ev['time']} | {ev['direction']}")
    print()

    for window_min, min_rr in [(60, 1.2)]:
        print(f"=== Fenetre tolerance = {window_min}min, RR>={min_rr} (SL origin, baseline) ===")
        trades, _ = run_backtest_v3(m5, h1, h4, pd_arrays, events, window_min, min_rr, 'origin', None)
        for k, v in compute_metrics(trades).items():
            print(f"  {k}: {v}")
        print()

    print("=== ETAPE 2 v3 (fenetre elargie) : SL ATR, seule famille positive precedemment ===\n")

    for mult, label in [(1.5, "1.5x ATR"), (2.0, "2x ATR")]:
        print(f"--- SL = {label} depuis l'entree (RR>=1.2) ---")
        trades, all_rr = run_backtest_v3(m5, h1, h4, pd_arrays, events, 60, 1.2, 'atr', mult)
        for k, v in compute_metrics(trades).items():
            print(f"  {k}: {v}")
        for t in trades:
            print(f"    {t['event_time']} | {t['bias']} | RR={t['rr_theoretical']} | {t['result']}")
        print()

    conn.close()
