#!/usr/bin/env python3
"""
backtest_choch_bos_filter_value.py
Question du 07/09 : le filtre CHoCH+BOS (ticket #38) ne laisse passer que
1/64 des setups BUY shadow - est-il trop selectif ? Ce script simule
CHAQUE ligne de signals_shadow (entry/sl/tp reels, deja calcules par
signal_generator.py au moment de la generation) contre les vraies bougies
M5 qui ont suivi, pour comparer objectivement :
- Performance de TOUS les setups (comme si le filtre n'existait pas,
  seul RR>=1.0 deja applique en amont)
- Performance du sous-ensemble ayant passe le filtre CHoCH+BOS

Lecture seule sur la DB (aucun INSERT/UPDATE) - pur backtest offline,
base sur les vrais setups deja generes, pas une reconstruction.
"""

import psycopg2

DB_CONFIG = {
    'host': 'localhost', 'port': 5432,
    'dbname': 'trading', 'user': 'trading', 'password': 'Trading2026'
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def load_shadow_signals(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, entry_price, sl_price, tp_price, rr_ratio,
               passed_choch_bos_filter, created_at
        FROM signals_shadow ORDER BY created_at ASC
    """)
    rows = cur.fetchall()
    cur.close()
    return [{'id': r[0], 'entry': float(r[1]), 'sl': float(r[2]), 'tp': float(r[3]),
             'rr': float(r[4]), 'passed_filter': r[5], 'created_at': r[6]} for r in rows]


def load_m5_after(conn, created_at, limit=2000):
    cur = conn.cursor()
    cur.execute("""
        SELECT candle_time, high, low FROM prices_smc
        WHERE timeframe='M5' AND candle_time > %s ORDER BY candle_time ASC LIMIT %s
    """, (created_at, limit))
    rows = cur.fetchall()
    cur.close()
    return [{'candle_time': r[0], 'high': float(r[1]), 'low': float(r[2])} for r in rows]


def simulate_bullish(entry, sl, tp, m5_after):
    for c in m5_after:
        if c['low'] <= sl:
            return 'LOSS', -1.0
        if c['high'] >= tp:
            rr_real = (tp - entry) / (entry - sl) if entry > sl else 0
            return 'WIN', rr_real
    return None, None


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
    signals = load_shadow_signals(conn)
    print(f"Total signaux shadow BUY en base: {len(signals)}\n")

    all_trades = []
    filtered_trades = []
    n_inconclusive = 0

    for s in signals:
        m5_after = load_m5_after(conn, s['created_at'])
        result, rr_real = simulate_bullish(s['entry'], s['sl'], s['tp'], m5_after)
        if result is None:
            n_inconclusive += 1
            continue
        trade = {'id': s['id'], 'result': result, 'rr_real': rr_real,
                  'passed_filter': s['passed_filter'], 'created_at': s['created_at']}
        all_trades.append(trade)
        if s['passed_filter']:
            filtered_trades.append(trade)

    print(f"Signaux inconclusifs (pas assez de donnees M5 futures): {n_inconclusive}\n")

    print("=== SANS le filtre CHoCH+BOS (tous les setups RR>=1.0) ===")
    for k, v in compute_metrics(all_trades).items():
        print(f"  {k}: {v}")

    print("\n=== AVEC le filtre CHoCH+BOS strict (comportement actuel) ===")
    for k, v in compute_metrics(filtered_trades).items():
        print(f"  {k}: {v}")

    print("\n=== DETAIL COMPLET (tous les setups resolus) ===")
    n_win_all = sum(1 for t in all_trades if t['result'] == 'WIN')
    n_loss_all = sum(1 for t in all_trades if t['result'] == 'LOSS')
    print(f"  Total resolus: {len(all_trades)} | WIN: {n_win_all} | LOSS: {n_loss_all}")
    for t in all_trades:
        marker = " [PASSE FILTRE]" if t['passed_filter'] else ""
        print(f"    id={t['id']} {t['created_at']} | {t['result']}{marker}")

    conn.close()
