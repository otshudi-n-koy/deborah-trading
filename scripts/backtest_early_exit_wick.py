#!/usr/bin/env python3
"""
backtest_early_exit_wick.py
Teste une sortie anticipee sur perte qui se dessine, inspiree de la video
partagee le 09/09 (sortie anticipee sur GAIN qui se degrade -> ici
symetrique, sur PERTE) : si une bougie M5 montre un petit corps + une
longue meche dans le sens DEFAVORABLE a la position (rejet net), sortir
immediatement plutot que d'attendre le SL complet.

Definition operationnelle testee :
- Position BUY : bougie M5 avec meche HAUTE >= 2x le corps ET corps <
  30% du range total de la bougie -> signe de rejet vers le bas ->
  sortie anticipee au close de cette bougie
- Position SELL : symetrique, meche BASSE >= 2x le corps

Applique aux VRAIS trades perdants recents (donnees reelles M5 pendant
la duree de vie de chaque position), pour mesurer objectivement le gain
potentiel avant toute implementation.

Lecture seule sur la DB (aucun INSERT/UPDATE) - pur backtest offline.
"""

import psycopg2

DB_CONFIG = {
    'host': 'localhost', 'port': 5432,
    'dbname': 'trading', 'user': 'trading', 'password': 'Trading2026'
}

WICK_RATIO_MIN = 2.0
BODY_MAX_PCT = 0.30


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def load_real_winning_trades(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT t.id, s.type, s.entry_price, s.sl_price, s.tp_price,
               t.pnl_eur, s.filled_at, t.close_at
        FROM trades_smc t JOIN signals_smc s ON t.signal_id = s.id
        WHERE t.result = 'WIN' AND s.filled_at IS NOT NULL
        ORDER BY t.close_at DESC LIMIT 12
    """)
    rows = cur.fetchall()
    cur.close()
    return [{'trade_id': r[0], 'type': r[1], 'entry': float(r[2]), 'sl': float(r[3]),
             'tp': float(r[4]), 'pnl_real': float(r[5]), 'filled_at': r[6], 'close_at': r[7]} for r in rows]


def load_real_losing_trades(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT t.id, s.type, s.entry_price, s.sl_price, s.tp_price,
               t.pnl_eur, s.filled_at, t.close_at
        FROM trades_smc t JOIN signals_smc s ON t.signal_id = s.id
        WHERE t.result = 'LOSS' AND s.filled_at IS NOT NULL
        ORDER BY t.close_at DESC LIMIT 12
    """)
    rows = cur.fetchall()
    cur.close()
    return [{'trade_id': r[0], 'type': r[1], 'entry': float(r[2]), 'sl': float(r[3]),
             'tp': float(r[4]), 'pnl_real': float(r[5]), 'filled_at': r[6], 'close_at': r[7]} for r in rows]


def load_m5_window(conn, filled_at, close_at):
    cur = conn.cursor()
    cur.execute("""
        SELECT candle_time, open, high, low, close FROM prices_smc
        WHERE timeframe='M5' AND candle_time > %s AND candle_time <= %s
        ORDER BY candle_time ASC
    """, (filled_at, close_at))
    rows = cur.fetchall()
    cur.close()
    return [{'candle_time': r[0], 'open': float(r[1]), 'high': float(r[2]),
             'low': float(r[3]), 'close': float(r[4])} for r in rows]


def check_adverse_wick(candle, is_buy):
    body = abs(candle['close'] - candle['open'])
    full_range = candle['high'] - candle['low']
    if full_range == 0:
        return False
    if is_buy:
        wick = candle['high'] - max(candle['open'], candle['close'])
    else:
        wick = min(candle['open'], candle['close']) - candle['low']
    if body == 0:
        return wick > 0
    return (wick / body >= WICK_RATIO_MIN) and (body / full_range <= BODY_MAX_PCT)


def simulate_early_exit(trade, m5_window, min_progress_pct=0.0):
    is_buy = trade['type'] == 'BUY_LIMIT'
    entry = trade['entry']
    tp = trade['tp']
    total_distance = abs(tp - entry)
    for c in m5_window:
        if is_buy:
            if c['low'] <= trade['sl']:
                return None
            if c['high'] >= trade['tp']:
                return None
        else:
            if c['high'] >= trade['sl']:
                return None
            if c['low'] <= trade['tp']:
                return None
        if check_adverse_wick(c, is_buy):
            # RAFFINEMENT 10/09/2026 : n'accepter le signal que si une part
            # significative du trajet vers le TP est deja parcourue - le
            # mecanisme decrit (transcription video) se declenche "arrive
            # aux anciens points hauts", pas en debut de mouvement. Sans ce
            # filtre, 2 faux positifs sur gagnants complets (#107: progres
            # ~7% seulement, #109: ~59%) etaient acceptes a tort.
            progress = abs(c['close'] - entry) / total_distance if total_distance > 0 else 0
            if progress < min_progress_pct:
                continue
            exit_price = c['close']
            if is_buy:
                pnl_pips = (exit_price - entry) * 10000
            else:
                pnl_pips = (entry - exit_price) * 10000
            return {'exit_time': c['candle_time'], 'exit_price': exit_price, 'pnl_pips': pnl_pips, 'progress': progress}
    return None


if __name__ == '__main__':
    conn = get_conn()

    print("=== TEST SUR TRADES PERDANTS (deja fait, rappel) ===\n")
    trades_loss = load_real_losing_trades(conn)
    n_early_loss = 0
    total_real_loss = 0
    total_hypo_loss = 0
    for t in trades_loss:
        m5_window = load_m5_window(conn, t['filled_at'], t['close_at'])
        early = simulate_early_exit(t, m5_window, min_progress_pct=0.5)
        total_real_loss += t['pnl_real']
        sl_pips = abs(t['entry'] - t['sl']) * 10000
        lot_value_per_pip = abs(t['pnl_real']) / sl_pips if sl_pips > 0 else 0
        if early:
            n_early_loss += 1
            total_hypo_loss += early['pnl_pips'] * lot_value_per_pip
        else:
            total_hypo_loss += t['pnl_real']
    print(f"  {n_early_loss}/{len(trades_loss)} sorties anticipees | reel={total_real_loss:+.2f}EUR | hypothetique={total_hypo_loss:+.2f}EUR\n")

    print("=== TEST SUR TRADES GAGNANTS (cout potentiel en faux positifs) ===\n")
    trades_win = load_real_winning_trades(conn)
    print(f"Trades gagnants reels analyses: {len(trades_win)}\n")

    n_would_cut_early = 0
    total_pnl_real_win = 0
    total_pnl_hypothetique_win = 0

    for t in trades_win:
        m5_window = load_m5_window(conn, t['filled_at'], t['close_at'])
        early = simulate_early_exit(t, m5_window, min_progress_pct=0.5)
        total_pnl_real_win += t['pnl_real']

        sl_pips = abs(t['entry'] - t['sl']) * 10000
        tp_pips = abs(t['tp'] - t['entry']) * 10000
        lot_value_per_pip = abs(t['pnl_real']) / tp_pips if tp_pips > 0 else 0

        if early:
            n_would_cut_early += 1
            pnl_hypo = early['pnl_pips'] * lot_value_per_pip
            total_pnl_hypothetique_win += pnl_hypo
            manque_a_gagner = t['pnl_real'] - pnl_hypo
            print(f"  Trade #{t['trade_id']} ({t['type']}) | reel={t['pnl_real']:+.2f}EUR (WIN complet) | "
                  f"AURAIT ETE COUPE a {early['exit_time']} | pnl hypothetique={pnl_hypo:+.2f}EUR | "
                  f"manque a gagner={manque_a_gagner:+.2f}EUR")
        else:
            total_pnl_hypothetique_win += t['pnl_real']
            print(f"  Trade #{t['trade_id']} ({t['type']}) | reel={t['pnl_real']:+.2f}EUR | "
                  f"AUCUN signal de rejet avant TP - gain preserve integralement")

    print(f"\n=== RESUME GAGNANTS ===")
    print(f"Trades ou le gain aurait ete COUPE prematurement: {n_would_cut_early}/{len(trades_win)}")
    print(f"PnL reel cumule (ces {len(trades_win)} trades gagnants): {total_pnl_real_win:+.2f}EUR")
    print(f"PnL hypothetique cumule (avec regle de sortie anticipee active): {total_pnl_hypothetique_win:+.2f}EUR")
    print(f"Cout net de la regle sur les gagnants: {total_pnl_hypothetique_win - total_pnl_real_win:+.2f}EUR")

    print(f"\n=== BILAN GLOBAL (perdants + gagnants) ===")
    gain_sur_perdants = total_hypo_loss - total_real_loss
    cout_sur_gagnants = total_pnl_hypothetique_win - total_pnl_real_win
    print(f"Gain recupere sur les perdants: {gain_sur_perdants:+.2f}EUR")
    print(f"Cout sur les gagnants (manque a gagner): {cout_sur_gagnants:+.2f}EUR")
    print(f"BILAN NET DE LA REGLE: {gain_sur_perdants + cout_sur_gagnants:+.2f}EUR")

    conn.close()
