#!/usr/bin/env python3
"""
backtest_invalidation_threshold.py
Teste un seuil d'invalidation de zone PD array assoupli. Actuellement
(pd_array_detector.py), une zone bearish est invalidee des le premier
contact du prix (price <= price_high + 2 pips), meme fenetre de temps que
le buffer de matching de signal_generator.py (5 pips) - les deux se
chevauchent presque totalement, le bot doit "attraper" le prix pile
pendant l'instant ou il traverse la zone.

Diagnostic du 19/08 : 91.7% des zones formees sur 48h sont invalidees,
touched=0 sur 100% d'entre elles (pas d'etat intermediaire "touche mais
exploitable" - passage direct active->invalidated des le premier contact).

Hypothese testee : retarder l'invalidation (exiger un franchissement plus
large, pas juste un contact) donnerait plus de temps a signal_generator.py
pour matcher la zone avant qu'elle ne disparaisse.

Meme pipeline complet (confluence H1/H4, buffer PD array 5 pips,
MIN_RR_SELL=1.2, ATR>=3p, buffer neutre 2 pips), seul le moment
d'invalidation (recalcule directement depuis les bougies M5 reelles,
pas depuis invalidated_at stocke en base) varie.

Lecture seule sur la DB (aucun INSERT/UPDATE) - pur backtest offline.
"""

import psycopg2
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


def load_pd_arrays_raw(conn, start_date):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, type, direction, price_high, price_low, price_eq, candle_time
        FROM pd_arrays_smc WHERE timeframe = 'M5' AND candle_time >= %s ORDER BY candle_time ASC
    """, (start_date,))
    rows = cur.fetchall()
    cur.close()
    return [{'id': r[0], 'type': r[1], 'direction': r[2], 'price_high': float(r[3]),
             'price_low': float(r[4]), 'price_eq': float(r[5]),
             'candle_time': r[6]} for r in rows]


def load_pd_arrays_with_stored_invalidation(conn, start_date):
    """Baseline : utilise invalidated_at reellement stocke en base (deja
    coherent avec tous les backtests precedents de la semaine)."""
    cur = conn.cursor()
    cur.execute("""
        SELECT id, type, direction, price_high, price_low, price_eq, candle_time, invalidated_at
        FROM pd_arrays_smc WHERE timeframe = 'M5' AND direction='bearish' AND candle_time >= %s ORDER BY candle_time ASC
    """, (start_date,))
    rows = cur.fetchall()
    cur.close()
    return [{'id': r[0], 'type': r[1], 'direction': r[2], 'price_high': float(r[3]),
             'price_low': float(r[4]), 'price_eq': float(r[5]),
             'candle_time': r[6], 'invalidated_at': r[7]} for r in rows]


def recompute_invalidation_delayed(pd_arrays, m5, extra_margin_pips):
    """
    Invalidation RETARDEE : au lieu du premier contact (price atteint
    price_low pour bearish), exige que le prix depasse COMPLETEMENT la
    zone, au-dela de price_high + extra_margin_pips - une vraie extension
    de la fenetre de vie, pas juste une tolerance de depassement comme
    dans le modele actuel de prod.
    """
    margin = extra_margin_pips * 0.0001
    m5_sorted = sorted(m5, key=lambda c: c['candle_time'])
    result = []
    for arr in pd_arrays:
        invalidated_at = None
        for c in m5_sorted:
            if c['candle_time'] <= arr['candle_time']:
                continue
            if c['close'] >= arr['price_high'] + margin:
                invalidated_at = c['candle_time']
                break
        result.append({**arr, 'invalidated_at': invalidated_at})
    return result


def compute_atr_pips(candles_before, n=14):
    if len(candles_before) < n:
        return 0
    recent = candles_before[-n:]
    avg_range = sum(c['high'] - c['low'] for c in recent) / n
    return round(avg_range * 10000, 1)


def try_build_sell_signal(current_price, active_arrays, struct, verbose=False):
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
    result = {'entry': entry, 'sl': sl, 'tp': tp, 'rr': rr}
    if verbose:
        result['zone_id'] = best_array['id']
        result['zone_formed_at'] = best_array['candle_time']
        result['zone_price_high'] = best_array['price_high']
        result['zone_price_low'] = best_array['price_low']
    return result


def simulate_trade(entry, sl, tp, m5_after):
    for c in m5_after:
        if c['high'] >= sl:
            return 'LOSS', -1.0
        if c['low'] <= tp:
            rr_real = (entry - tp) / (sl - entry) if sl > entry else 0
            return 'WIN', rr_real
    return None, None


def run_backtest(m5, h1, h4, pd_arrays, verbose=False):
    LOOKBACK_H1 = 20 * 24
    LOOKBACK_H4 = 30 * 6
    trades = []
    candidates_bearish = [a for a in pd_arrays if a['direction'] == 'bearish']
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
        if atr_pips < ATR_MIN_PIPS:
            continue
        current_price = h1_window[-1]['close']
        active_arrays = [
            a for a in candidates_bearish
            if a['candle_time'] <= t_now
            and (a['invalidated_at'] is None or a['invalidated_at'] > t_now)
        ]
        sig = try_build_sell_signal(current_price, active_arrays, struct_h1, verbose=verbose)
        if not sig:
            continue
        m5_after = [c for c in m5 if c['candle_time'] > t_now][:2000]
        result, rr_real = simulate_trade(sig['entry'], sig['sl'], sig['tp'], m5_after)
        if result is None:
            continue
        trade_record = {'result': result, 'rr_real': rr_real}
        if verbose:
            trade_record.update({
                'signal_time': t_now, 'zone_id': sig['zone_id'],
                'zone_formed_at': sig['zone_formed_at'],
                'zone_price_high': sig['zone_price_high'],
                'zone_price_low': sig['zone_price_low'],
                'entry': sig['entry'],
            })
        trades.append(trade_record)
    return trades


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


def recompute_invalidation_delayed_with_ttl(pd_arrays, m5, extra_margin_pips, ttl_days):
    """
    Combine invalidation retardee (depassement complet requis) ET un TTL
    maximum (age au-dela duquel la zone est invalidee meme sans contact
    prix) - evite le risque de matcher sur des niveaux vraiment obsoletes
    identifie comme reserve du test "invalidation totalement desactivee".
    """
    margin = extra_margin_pips * 0.0001
    ttl = timedelta(days=ttl_days)
    m5_sorted = sorted(m5, key=lambda c: c['candle_time'])
    result = []
    for arr in pd_arrays:
        invalidated_at = None
        ttl_limit = arr['candle_time'] + ttl
        for c in m5_sorted:
            if c['candle_time'] <= arr['candle_time']:
                continue
            if c['candle_time'] >= ttl_limit:
                invalidated_at = ttl_limit
                break
            if c['close'] >= arr['price_high'] + margin:
                invalidated_at = c['candle_time']
                break
        result.append({**arr, 'invalidated_at': invalidated_at})
    return result


def recompute_invalidation_touch_tolerant(pd_arrays, m5, max_touches, break_margin_pips, ttl_days):
    """
    Mecanisme combine (ticket #61, conception finale) :
    - Contact tolere : prix entre dans [price_low, price_high] puis en
      ressort sans depasser price_high+margin - ne compte PAS comme
      invalidation, incremente juste un compteur de contacts.
    - Cassure reelle : une bougie M5 cloture au-dela de price_high+margin
      -> invalidation immediate, quel que soit le nombre de contacts.
    - Tolerance epuisee : si le nombre de contacts depasse max_touches
      sans jamais casser -> invalidation (evite qu'une zone trop testee
      reste active indefiniment sans jamais montrer de vraie cassure).
    - TTL : filet de securite, invalide apres ttl_days meme sans cassure
      ni tolerance epuisee (deja valide neutre sur 30-90j au ticket #61).
    max_touches=None -> tolerance illimitee (seul TTL/cassure invalident).
    """
    margin = break_margin_pips * 0.0001
    ttl = timedelta(days=ttl_days) if ttl_days else None
    m5_sorted = sorted(m5, key=lambda c: c['candle_time'])
    result = []
    for arr in pd_arrays:
        invalidated_at = None
        touches = 0
        was_inside = False
        ttl_limit = arr['candle_time'] + ttl if ttl else None
        for c in m5_sorted:
            if c['candle_time'] <= arr['candle_time']:
                continue
            if ttl_limit and c['candle_time'] >= ttl_limit:
                invalidated_at = ttl_limit
                break
            price = c['close']
            if price >= arr['price_high'] + margin:
                invalidated_at = c['candle_time']
                break
            inside_now = arr['price_low'] <= price <= arr['price_high']
            if inside_now and not was_inside:
                touches += 1
                if max_touches is not None and touches > max_touches:
                    invalidated_at = c['candle_time']
                    break
            was_inside = inside_now
        result.append({**arr, 'invalidated_at': invalidated_at})
    return result


if __name__ == '__main__':
    conn = get_conn()
    START = '2026-06-29'

    print(f"Chargement des donnees depuis {START}...")
    m5 = load_candles(conn, 'M5', START)
    h1 = load_candles(conn, 'H1', START)
    h4 = load_candles(conn, 'H4', START)
    print(f"M5={len(m5)} H1={len(h1)} H4={len(h4)}\n")

    print("=== BASELINE (invalidated_at reel stocke en base, comportement actuel) ===")
    pd_arrays_baseline = load_pd_arrays_with_stored_invalidation(conn, START)
    print(f"  PD_arrays bearish: {len(pd_arrays_baseline)}")
    trades = run_backtest(m5, h1, h4, pd_arrays_baseline)
    for k, v in compute_metrics(trades).items():
        print(f"  {k}: {v}")

    pd_arrays_raw = load_pd_arrays_raw(conn, START)
    pd_arrays_raw_bearish = [a for a in pd_arrays_raw if a['direction'] == 'bearish']

    for extra_margin, label in [(0, "0p (prix doit juste depasser price_high)"),
                                  (5, "5p au-dela de price_high"),
                                  (10, "10p au-dela de price_high")]:
        print(f"\n=== Invalidation RETARDEE : {label} ===")
        pd_arrays = recompute_invalidation_delayed(pd_arrays_raw_bearish, m5, extra_margin)
        n_still_active = sum(1 for a in pd_arrays if a['invalidated_at'] is None)
        print(f"  Zones jamais invalidees sur la fenetre: {n_still_active}/{len(pd_arrays)}")
        trades = run_backtest(m5, h1, h4, pd_arrays)
        for k, v in compute_metrics(trades).items():
            print(f"  {k}: {v}")

    print(f"\n=== INVALIDATION DESACTIVEE (zones actives indefiniment) ===")
    pd_arrays_never_invalidated = [{**a, 'invalidated_at': None} for a in pd_arrays_raw_bearish]
    trades_never = run_backtest(m5, h1, h4, pd_arrays_never_invalidated, verbose=True)
    for k, v in compute_metrics(trades_never).items():
        print(f"  {k}: {v}")

    print(f"\n=== DETAIL DES 5 TRADES (invalidation desactivee) - identification du cas supplementaire ===")
    for t in trades_never:
        age_days = (t['signal_time'] - t['zone_formed_at']).days
        print(f"  Signal a {t['signal_time']} | entry={t['entry']:.5f} | resultat={t['result']}")
        print(f"    Zone id={t['zone_id']} formee le {t['zone_formed_at']} (age au signal: {age_days}j)")
        print(f"    Zone price_high={t['zone_price_high']:.5f} price_low={t['zone_price_low']:.5f}")

    for ttl_days in [30, 60, 90]:
        print(f"\n=== Invalidation retardee (depassement complet) + TTL {ttl_days}j ===")
        pd_arrays = recompute_invalidation_delayed_with_ttl(pd_arrays_raw_bearish, m5, 0, ttl_days)
        n_still_active = sum(1 for a in pd_arrays if a['invalidated_at'] is None)
        print(f"  Zones jamais invalidees sur la fenetre: {n_still_active}/{len(pd_arrays)}")
        trades = run_backtest(m5, h1, h4, pd_arrays)
        for k, v in compute_metrics(trades).items():
            print(f"  {k}: {v}")

    print(f"\n=== MECANISME COMBINE : contacts toleres + cassure reelle + TTL 60j ===")
    for max_touches, label in [(1, "max 1 contact"), (2, "max 2 contacts"),
                                 (3, "max 3 contacts"), (None, "contacts illimites")]:
        print(f"\n--- {label} (cassure=0p, TTL=60j) ---")
        pd_arrays = recompute_invalidation_touch_tolerant(pd_arrays_raw_bearish, m5, max_touches, 0, 60)
        n_still_active = sum(1 for a in pd_arrays if a['invalidated_at'] is None)
        print(f"  Zones jamais invalidees sur la fenetre: {n_still_active}/{len(pd_arrays)}")
        trades = run_backtest(m5, h1, h4, pd_arrays)
        for k, v in compute_metrics(trades).items():
            print(f"  {k}: {v}")

    conn.close()
