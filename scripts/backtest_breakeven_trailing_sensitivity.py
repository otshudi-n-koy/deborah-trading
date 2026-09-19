#!/usr/bin/env python3
"""
backtest_breakeven_trailing_sensitivity.py

Teste si le breakeven auto (simple + CHoCH) et le trailing stop de
position_monitor.py améliorent réellement le Kelly, en réutilisant
EXACTEMENT la boucle de génération de signal de backtest_ci.py
(load_candles, analyze_structure, try_build_signal) et en substituant
uniquement simulate_trade_outcome() par une variante bar-par-bar qui
reproduit fidèlement breakeven/trailing (mêmes seuils que
position_monitor.py, lignes ~760-850).

Contrairement au buffer/RR/ATR (backtestés avec preuve à l'appui) et aux
TP_PALIERS (désactivés sur preuve empirique documentée dans le code), le
breakeven et le trailing n'ont jamais eu ce traitement — ce script comble
ce trou.

Simplification assumée : trail_mult/trail_dist_mult dépendent du
session_bias réel au moment du trade dans position_monitor.py (2.5/1.5 si
BULLISH/BEARISH connu, sinon 1.0/1.0) — ce script teste les deux régimes
séparément (comportement "neutre" 1.0/1.0 et comportement "biaisé"
2.5/1.5) plutôt que de recalculer le session_bias dynamiquement, pour
encadrer l'écart réel sans le deviner.

Lecture seule sur la DB (aucun INSERT/UPDATE) - pur backtest offline.
"""
import sys
sys.path.insert(0, ".")

import psycopg2
from backtest_ci import (
    load_candles, load_pd_arrays, analyze_structure, try_build_signal,
    calc_kelly, LOOKBACK_H1, LOOKBACK_H4, get_connection,
)

PIP = 0.0001
BE_MIN_PIPS = 3
BE_BUFFER_PIPS = 2
CHOCH_MAX_DIST_PIPS = 15
CHOCH_WINDOW = 8


def detect_choch_m5(candles, direction, window):
    """Portage minimal depuis position_monitor.py pour ce backtest."""
    if len(candles) < window + 2:
        return None, False
    recent = candles[-window:]
    if direction == 'short':
        swing_low = min(c['low'] for c in recent[:-1])
        if recent[-1]['close'] > swing_low and recent[-1]['high'] > max(c['high'] for c in recent[:-1]):
            return swing_low, True
    else:
        swing_high = max(c['high'] for c in recent[:-1])
        if recent[-1]['close'] < swing_high and recent[-1]['low'] < min(c['low'] for c in recent[:-1]):
            return swing_high, True
    return None, False


def simulate_with_breakeven_trailing(signal, m5_after, enable_be=True, enable_trailing=True,
                                       trail_mult=1.0, trail_dist_mult=1.0, max_bars=500):
    # Retourne (result, r_multiple, exit_time) -- exit_time=None si NO_FILL (jamais ouvert)
    entry, sl_initial, tp = signal['entry'], signal['sl'], signal['tp']
    is_buy = signal['signal_type'] == 'BUY_LIMIT'
    sl_dist = abs(entry - sl_initial)
    if sl_dist == 0:
        return 'LOSS', -1.0

    current_sl = sl_initial
    filled = False
    breakeven_done = False
    choch_direction = 'long' if is_buy else 'short'

    for i, c in enumerate(m5_after[:max_bars]):
        if not filled:
            if is_buy and c['low'] <= entry:
                filled = True
            elif not is_buy and c['high'] >= entry:
                filled = True
            if not filled:
                continue

        price_low, price_high, price_close = c['low'], c['high'], c['close']
        profit_pips = 0
        if is_buy and price_close > entry:
            profit_pips = (price_close - entry) / PIP
        elif not is_buy and price_close < entry:
            profit_pips = (entry - price_close) / PIP

        if enable_be and not breakeven_done:
            if is_buy and price_low > entry + BE_MIN_PIPS * PIP and current_sl < entry:
                current_sl = round(entry - BE_BUFFER_PIPS * PIP, 5)
                breakeven_done = True
            elif not is_buy and price_high < entry - BE_MIN_PIPS * PIP and current_sl > entry:
                current_sl = round(entry + BE_BUFFER_PIPS * PIP, 5)
                breakeven_done = True

        if enable_be and not breakeven_done and profit_pips >= 15:
            window = m5_after[max(0, i - CHOCH_WINDOW):i + 1]
            _, choch_found = detect_choch_m5(window, choch_direction, CHOCH_WINDOW)
            if choch_found:
                if is_buy and current_sl < entry:
                    current_sl = round(entry - BE_BUFFER_PIPS * PIP, 5)
                    breakeven_done = True
                elif not is_buy and current_sl > entry:
                    current_sl = round(entry + BE_BUFFER_PIPS * PIP, 5)
                    breakeven_done = True

        if enable_trailing:
            if is_buy:
                profit_dist = price_close - entry
                if profit_dist >= sl_dist * trail_mult:
                    trail_sl = round(price_close - sl_dist * trail_dist_mult, 5)
                    if trail_sl > current_sl:
                        current_sl = trail_sl
            else:
                profit_dist = entry - price_close
                if profit_dist >= sl_dist * trail_mult:
                    trail_sl = round(price_close + sl_dist * trail_dist_mult, 5)
                    if trail_sl < current_sl:
                        current_sl = trail_sl

        if is_buy:
            hit_sl = price_low <= current_sl
            hit_tp = price_high >= tp
        else:
            hit_sl = price_high >= current_sl
            hit_tp = price_low <= tp

        if hit_sl or hit_tp:
            if hit_tp and not hit_sl:
                return 'WIN', signal['rr'], c['candle_time']
            r = (current_sl - entry) / sl_dist if is_buy else (entry - current_sl) / sl_dist
            was_breakeven = breakeven_done and abs(current_sl - entry) <= BE_BUFFER_PIPS * PIP * 1.5
            return ('BREAKEVEN' if was_breakeven else 'LOSS'), r, c['candle_time']

    if not filled:
        return 'NO_FILL', 0.0, None
    return 'TIMEOUT', 0.0, m5_after[min(max_bars, len(m5_after)) - 1]['candle_time'] if m5_after else None


def run_variant(conn, start_date, end_date, enable_be, enable_trailing, trail_mult, trail_dist_mult):
    m5 = load_candles(conn, 'M5', start_date, end_date)
    h1 = load_candles(conn, 'H1', start_date, end_date)
    h4 = load_candles(conn, 'H4', start_date, end_date)
    pd_arrays = load_pd_arrays(conn, start_date, end_date)

    trades = []
    next_available_time = None

    for i in range(LOOKBACK_H1, len(h1)):
        h1_window = h1[max(0, i - LOOKBACK_H1):i + 1]
        t_now = h1_window[-1]['candle_time']
        if next_available_time is not None and t_now < next_available_time:
            continue
        struct_h1 = analyze_structure(h1_window, window_size=3)
        bias = struct_h1['bias']
        if not bias or bias == 'NEUTRAL':
            continue

        h4_window = [c for c in h4 if c['candle_time'] <= t_now][-LOOKBACK_H4:]
        if len(h4_window) < 7:
            continue
        struct_h4 = analyze_structure(h4_window, window_size=3)
        if struct_h4['bias'] != bias:
            continue

        current_price = h1_window[-1]['close']
        active_arrays = [
            a for a in pd_arrays
            if a['candle_time'] <= t_now
            and (a['invalidated_at'] is None or a['invalidated_at'] > t_now)
        ]
        sig, reason = try_build_signal(bias, current_price, active_arrays, struct_h1)
        if not sig:
            continue

        m5_after = [c for c in m5 if c['candle_time'] > t_now]
        result, r_multiple, exit_time = simulate_with_breakeven_trailing(
            sig, m5_after, enable_be=enable_be, enable_trailing=enable_trailing,
            trail_mult=trail_mult, trail_dist_mult=trail_dist_mult,
        )
        if result in ('WIN', 'LOSS', 'BREAKEVEN'):
            trades.append({**sig, 'time': t_now, 'result': result, 'r_multiple': r_multiple})
            next_available_time = exit_time
        elif result == 'TIMEOUT':
            next_available_time = exit_time

    return trades


def compute_metrics(trades):
    wins = [t for t in trades if t['result'] == 'WIN']
    losses = [t for t in trades if t['result'] == 'LOSS']
    breakevens = [t for t in trades if t['result'] == 'BREAKEVEN']
    total = len(wins) + len(losses) + len(breakevens)
    if total == 0:
        return {'nb_trades': 0, 'wr': 0.0, 'avg_rr': 0.0, 'kelly': 0.0, 'breakeven_count': 0}

    wr = len(wins) / total
    avg_win_r = sum(t['r_multiple'] for t in wins) / len(wins) if wins else 0.0
    avg_loss_r = sum(t['r_multiple'] for t in losses) / len(losses) if losses else -1.0
    rr = abs(avg_win_r / avg_loss_r) if avg_loss_r != 0 else 0.0
    kelly = calc_kelly(wr, rr)

    return {
        'nb_trades': total, 'wr': round(wr * 100, 2), 'avg_rr': round(rr, 2),
        'kelly': round(kelly, 4), 'breakeven_count': len(breakevens),
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    conn = get_connection()

    variants = [
        ("baseline (ni BE ni trailing)", False, False, 1.0, 1.0),
        ("BE seul", True, False, 1.0, 1.0),
        ("trailing seul (neutre 1.0/1.0)", False, True, 1.0, 1.0),
        ("trailing seul (biaise 2.5/1.5)", False, True, 2.5, 1.5),
        ("BE + trailing (neutre)", True, True, 1.0, 1.0),
        ("BE + trailing (biaise, = prod actuelle)", True, True, 2.5, 1.5),
    ]

    for label, be, trail, tm, tdm in variants:
        trades = run_variant(conn, args.start, args.end, be, trail, tm, tdm)
        m = compute_metrics(trades)
        print(f"=== {label} ===")
        for k, v in m.items():
            print(f"  {k}: {v}")
        print()

    conn.close()
