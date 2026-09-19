#!/usr/bin/env python3
"""
backtest_counterfactual_rules.py
Teste deux regles alternatives sur les 9 derniers VRAIS trades du compte
E8 5K (id 113-121, tous SL_HIT_LATE), en utilisant les vraies bougies M5
apres l'entree reelle - pas une simulation abstraite, un contrefactuel
direct sur l'historique reel.

Regle A : SL fixe a 20 pips (au lieu du SL structurel ICT actuel,
generalement ~10p via MIN_SL) - le trade aurait-il survecu jusqu'au TP,
ou juste retarde/aggrave la perte ?

Regle B (niveau 2, proposee) : ne jamais SELL si le prix d'entree est
deja en zone DISCOUNT (sous l'equilibre H1, range_mid = (swing_high+
swing_low)/2) - le trade aurait-il ete bloque avant meme d'etre pris ?

Lecture seule sur la DB (aucun INSERT/UPDATE) - pur backtest offline.
"""

import psycopg2

DB_CONFIG = {
    'host': 'localhost', 'port': 5432,
    'dbname': 'trading', 'user': 'trading', 'password': 'Trading2026'
}

RECENT_LOOKBACK = 10
MIN_SIGNIFICANT_PIPS = 0.0010


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def analyze_structure(candles, window_size=3):
    if len(candles) < window_size * 2 + 1:
        return {'swing_high': None, 'swing_low': None, 'range_mid': None}
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

    if len(swing_highs) < 2 or len(swing_lows) < 2:
        recent = candles[-48:]
        fH = max(c['high'] for c in recent)
        fL = min(c['low'] for c in recent)
        return {'swing_high': fH, 'swing_low': fL, 'range_mid': fL + (fH - fL) * 0.5}

    last_hh, last_ll = swing_highs[-1], swing_lows[-1]
    recent = candles[-RECENT_LOOKBACK:]
    recent_low_val = min(c['low'] for c in recent)
    recent_high_val = max(c['high'] for c in recent)
    if recent_low_val < last_ll['price'] - MIN_SIGNIFICANT_PIPS:
        last_ll = {'price': recent_low_val}
    if recent_high_val > last_hh['price'] + MIN_SIGNIFICANT_PIPS:
        last_hh = {'price': recent_high_val}

    range_mid = (last_hh['price'] + last_ll['price']) / 2
    return {'swing_high': last_hh['price'], 'swing_low': last_ll['price'], 'range_mid': range_mid}


def load_real_trades(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT t.id, t.signal_id, s.entry_price, s.sl_price, s.tp_price,
               t.pnl_eur, t.exit_reason, s.filled_at
        FROM trades_smc t JOIN signals_smc s ON t.signal_id = s.id
        WHERE t.id IN (113,114,115,116,117,118,119,120,121)
        ORDER BY t.close_at ASC
    """)
    rows = cur.fetchall()
    cur.close()
    return [{'trade_id': r[0], 'signal_id': r[1], 'entry': float(r[2]),
             'sl_real': float(r[3]), 'tp': float(r[4]), 'pnl_real': float(r[5]),
             'exit_reason': r[6], 'filled_at': r[7]} for r in rows]


def load_m5_after(conn, filled_at, limit=2000):
    cur = conn.cursor()
    cur.execute("""
        SELECT candle_time, high, low, close FROM prices_smc
        WHERE timeframe='M5' AND candle_time > %s ORDER BY candle_time ASC LIMIT %s
    """, (filled_at, limit))
    rows = cur.fetchall()
    cur.close()
    return [{'candle_time': r[0], 'high': float(r[1]), 'low': float(r[2]), 'close': float(r[3])} for r in rows]


def load_h1_before(conn, filled_at, lookback=480):
    cur = conn.cursor()
    cur.execute("""
        SELECT candle_time, high, low, close FROM prices_smc
        WHERE timeframe='H1' AND candle_time <= %s ORDER BY candle_time DESC LIMIT %s
    """, (filled_at, lookback))
    rows = cur.fetchall()
    cur.close()
    rows.reverse()
    return [{'candle_time': r[0], 'high': float(r[1]), 'low': float(r[2]), 'close': float(r[3])} for r in rows]


def simulate_sl20(entry, tp, m5_after):
    sl20 = round(entry + 0.0020, 5)
    for c in m5_after:
        if c['high'] >= sl20:
            return 'LOSS', sl20
        if c['low'] <= tp:
            return 'WIN', sl20
    return 'INCONCLUSIF (2000 bougies)', sl20


if __name__ == '__main__':
    conn = get_conn()
    trades = load_real_trades(conn)

    print(f"=== REGLE A : SL fixe 20 pips (au lieu du SL structurel ~10p actuel) ===\n")
    total_pnl_real = 0
    n_would_flip_to_win = 0
    for t in trades:
        m5_after = load_m5_after(conn, t['filled_at'])
        result_20, sl20 = simulate_sl20(t['entry'], t['tp'], m5_after)
        total_pnl_real += t['pnl_real']
        flip = " <-- AURAIT CHANGE (perte -> gain potentiel)" if result_20 == 'WIN' and t['pnl_real'] < 0 else ""
        if flip:
            n_would_flip_to_win += 1
        print(f"  Trade #{t['trade_id']} | entry={t['entry']:.5f} SL_reel={t['sl_real']:.5f} "
              f"SL_20p={sl20:.5f} TP={t['tp']:.5f}")
        print(f"    Reel: {t['exit_reason']} ({t['pnl_real']:+.2f}EUR) | "
              f"Avec SL=20p: {result_20}{flip}")
    print(f"\n  RESUME REGLE A: {n_would_flip_to_win}/{len(trades)} trades auraient bascule en WIN avec SL=20p")
    print(f"  PnL reel cumule sur ces 9 trades: {total_pnl_real:+.2f}EUR")

    print(f"\n=== REGLE B : SELL interdit si entry en zone DISCOUNT (sous l'equilibre H1) ===\n")
    n_blocked = 0
    for t in trades:
        h1_before = load_h1_before(conn, t['filled_at'])
        struct = analyze_structure(h1_before, window_size=3)
        range_mid = struct.get('range_mid')
        if range_mid is None:
            print(f"  Trade #{t['trade_id']} | pas assez de donnees H1 pour statuer")
            continue
        zone = 'DISCOUNT' if t['entry'] < range_mid else 'PREMIUM'
        blocked = zone == 'DISCOUNT'
        if blocked:
            n_blocked += 1
        print(f"  Trade #{t['trade_id']} | entry={t['entry']:.5f} eq50(H1)={range_mid:.5f} "
              f"zone={zone} | {'BLOQUE par regle B' if blocked else 'autorise'} "
              f"| reel: {t['pnl_real']:+.2f}EUR")

    print(f"\n  RESUME REGLE B: {n_blocked}/{len(trades)} trades auraient ete BLOQUES avant meme d'etre pris")

    conn.close()
