#!/usr/bin/env python3
"""
backtest_buy_filter_strict.py
Backtest du filtre BUY strict SMC/ICT, ISO-STRATEGIE avec le pipeline SELL en
production (structure_analyzer.py + signal_generator.py), + couche de filtre
supplementaire (CHoCH/BOS/OB propre/FVG partiel/liquidity sweep) avant
d'accepter un signal BUY.

A deposer sur le VPS dans /opt/deborah-trading/scripts/ ou /root/.
Lecture seule sur la DB (aucun INSERT/UPDATE) - pur backtest offline.
"""

import psycopg2
import logging
from datetime import datetime, timedelta, timezone

DB_CONFIG = {
    'host': 'localhost', 'port': 5432,
    'dbname': 'trading', 'user': 'trading', 'password': 'Trading2026'
}

PIP = 0.0001
MIN_SL = 0.00100       # identique a signal_generator.py
SL_BUFFER = 0.00010    # identique a signal_generator.py
BUFFER_NEUTRAL_PIPS = 0.0005   # identique a structure_analyzer.py
MIN_SIGNIFICANT_PIPS = 0.0010  # identique a structure_analyzer.py
RECENT_LOOKBACK = 10           # identique a structure_analyzer.py
ARRAY_BUFFER = 0.00030         # identique a signal_generator.py (buffer PD array)
MIN_RR = 1.5                   # identique a signal_generator.py

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


# ---------------------------------------------------------------------------
# 1. PORTAGE EXACT DE structure_analyzer.analyze_structure()
#    (copie fidele, aucune modification de logique)
# ---------------------------------------------------------------------------
def analyze_structure(candles, window_size=3):
    if len(candles) < window_size * 2 + 1:
        return {'error': f'Pas assez de bougies: {len(candles)}'}

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
        return {
            'bias': bias, 'swing_high': fH, 'swing_low': fL,
            'zone_type': 'PREMIUM' if current_close > eq50 else 'DISCOUNT',
            'eq50': round(eq50, 5),
            'liquidity_target': round(fL if bias == 'BEARISH' else fH, 5),
            'current_close': current_close, 'method': 'FALLBACK_EXTREMES'
        }

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

    rng = last_hh['price'] - last_ll['price']
    eq50 = last_ll['price'] + rng * 0.5
    ote_h = last_ll['price'] + rng * 0.786
    ote_l = last_ll['price'] + rng * 0.618

    if ote_l <= current_close <= ote_h:
        zone_type = 'OTE'
    elif current_close > eq50:
        zone_type = 'PREMIUM'
    else:
        zone_type = 'DISCOUNT'

    lower_lows = [s for s in swing_lows if s['price'] < current_close]
    higher_highs = [s for s in swing_highs if s['price'] > current_close]
    if bias == 'BULLISH':
        liq = min(higher_highs, key=lambda s: s['price'])['price'] if higher_highs else round(current_close + 0.005, 5)
    else:
        liq = max(lower_lows, key=lambda s: s['price'])['price'] if lower_lows else round(current_close - 0.005, 5)

    return {
        'bias': bias, 'swing_high': last_hh['price'], 'swing_low': last_ll['price'],
        'zone_type': zone_type, 'eq50': round(eq50, 5),
        'liquidity_target': round(liq, 5), 'current_close': current_close,
        'method': 'SWING_ANALYSIS'
    }


# ---------------------------------------------------------------------------
# 2. CHARGEMENT DES DONNEES
# ---------------------------------------------------------------------------
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


def load_pd_arrays_bullish(conn, start_date):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, type, direction, price_high, price_low, price_eq,
               strength, status, candle_time, invalidated_at
        FROM pd_arrays_smc
        WHERE timeframe = 'M5' AND direction = 'bullish' AND candle_time >= %s
        ORDER BY candle_time ASC
    """, (start_date,))
    rows = cur.fetchall()
    cur.close()
    return [{'id': r[0], 'type': r[1], 'direction': r[2], 'price_high': float(r[3]),
             'price_low': float(r[4]), 'price_eq': float(r[5]), 'strength': r[6],
             'status': r[7], 'candle_time': r[8], 'invalidated_at': r[9]} for r in rows]


# ---------------------------------------------------------------------------
# 3. COUCHE FILTRE STRICT SUPPLEMENTAIRE (au-dela du pipeline SELL standard)
#    Detection CHoCH/BOS basee sur une VRAIE structure de swings (HH/HL/LH/LL),
#    reutilisant la meme mecanique de swing detection que analyze_structure()
#    (structure_analyzer.py), appliquee ici sur M5 au lieu de H1/H4. C'est le
#    choix retenu plutot qu'une heuristique de proxy, pour rester iso-ICT avec
#    le reste du systeme (cf. discussion du 03/08/2026).
# ---------------------------------------------------------------------------
def detect_swings_m5(m5_window, window_size=3, min_sig_pips=0.0006):
    """
    Detection de swing highs/lows sur M5, meme mecanique que
    structure_analyzer.analyze_structure() (fenetre glissante + filtrage par
    ecart minimum significatif), mais seuil reduit (6 pips au lieu de 10)
    car M5 est une granularite plus fine que H1 - a calibrer/backtester.
    Retourne une liste chronologique de swings classes HH/HL/LH/LL.
    """
    if len(m5_window) < window_size * 2 + 1:
        return []

    swing_highs, swing_lows = [], []
    for i in range(window_size, len(m5_window) - window_size):
        c = m5_window[i]
        is_high = all(m5_window[i - j]['high'] < c['high'] and m5_window[i + j]['high'] < c['high']
                      for j in range(1, window_size + 1))
        is_low = all(m5_window[i - j]['low'] > c['low'] and m5_window[i + j]['low'] > c['low']
                     for j in range(1, window_size + 1))
        if is_high:
            swing_highs.append({'index': i, 'price': c['high'], 'time': c['candle_time'], 'kind_raw': 'high'})
        if is_low:
            swing_lows.append({'index': i, 'price': c['low'], 'time': c['candle_time'], 'kind_raw': 'low'})

    def filter_sig(swings, is_high):
        out = []
        for s in swings:
            if not out:
                out.append(s); continue
            last = out[-1]
            if abs(s['price'] - last['price']) >= min_sig_pips:
                out.append(s)
            elif is_high and s['price'] > last['price']:
                out[-1] = s
            elif not is_high and s['price'] < last['price']:
                out[-1] = s
        return out

    swing_highs = filter_sig(swing_highs, True)
    swing_lows = filter_sig(swing_lows, False)

    # Classification HH/LH et LL/HL par comparaison au swing precedent du meme type
    for i, s in enumerate(swing_highs):
        prev = swing_highs[i - 1] if i > 0 else None
        s['kind'] = 'HH' if (prev is None or s['price'] > prev['price']) else 'LH'
    for i, s in enumerate(swing_lows):
        prev = swing_lows[i - 1] if i > 0 else None
        s['kind'] = 'LL' if (prev is None or s['price'] < prev['price']) else 'HL'

    all_swings = sorted(swing_highs + swing_lows, key=lambda s: s['index'])
    return all_swings


def detect_recent_choch_bullish(m5_window, lookback=80):
    """
    CHoCH haussier ICT : la structure de swings recente etait baissiere
    (sequence dominee par LH/LL) et le prix vient de cloturer au-dessus du
    dernier swing high (LH) significatif - premier signe de changement de
    caractere, PAS une simple continuation (qui serait un BOS).
    Retourne (bool, choch_level) : niveau du swing casse si CHoCH confirme.
    """
    if len(m5_window) < lookback:
        return False, None
    window = m5_window[-lookback:]
    swings = detect_swings_m5(window)
    if len(swings) < 4:
        return False, None

    # Tendance recente = kind majoritaire des 4 derniers swings avant les 5
    # dernieres bougies (evite d'inclure le mouvement de cassure lui-meme)
    pre_swings = [s for s in swings if s['index'] < len(window) - 5]
    if len(pre_swings) < 3:
        return False, None
    recent_kinds = [s['kind'] for s in pre_swings[-4:]]
    was_bearish_structure = recent_kinds.count('LH') + recent_kinds.count('LL') >= 3

    # Dernier swing high (LH si bearish structure) = niveau a casser pour CHoCH
    last_swing_high = next((s for s in reversed(pre_swings) if s['kind'] in ('HH', 'LH')), None)
    if not last_swing_high:
        return False, None

    last_close = window[-1]['close']
    choch_confirmed = was_bearish_structure and last_close > last_swing_high['price']
    return choch_confirmed, (last_swing_high['price'] if choch_confirmed else None)


def detect_bos_confirmation_bullish(m5_window, choch_level, max_bars=15):
    """
    BOS haussier = continuation confirmee post-CHoCH : une nouvelle cassure
    haussiere au-dela du niveau CHoCH (pas juste un retest), dans une fenetre
    de suivi limitee (evite de valider un BOS trop eloigne dans le temps).
    """
    if choch_level is None:
        return False
    recent = m5_window[-max_bars:]
    tolerance = choch_level * 1.0002  # ~2 pips de marge pour eviter le bruit
    return any(c['high'] > tolerance for c in recent)


def liquidity_sweep_before(m5_window, lookback=15, recent_bars=10):
    """
    Un swing low a ete balaye (meche sous un plus bas anterieur) dans l'une
    des `recent_bars` dernieres bougies avant l'instant present, comparee au
    plus bas des `lookback` bougies qui la precedent. Fenetre de tolerance
    (plutot qu'une position fixe) pour rester realiste sur M5, ou le sweep
    et la confirmation ne tombent pas systematiquement sur la meme bougie.
    """
    total_needed = lookback + recent_bars + 1
    if len(m5_window) < total_needed:
        return False
    for offset in range(1, recent_bars + 1):
        candidate_idx = len(m5_window) - 1 - offset
        prior_window = m5_window[max(0, candidate_idx - lookback):candidate_idx]
        if not prior_window:
            continue
        prior_low = min(c['low'] for c in prior_window)
        if m5_window[candidate_idx]['low'] < prior_low:
            return True
    return False


def fvg_partial_fill_ok(pd_array, current_price, min_fill=0.3, max_fill=0.7):
    """FVG partiellement comble : le prix retrace entre min_fill et max_fill du gap."""
    top, bottom = pd_array['price_high'], pd_array['price_low']
    if top <= bottom:
        return False
    fill_ratio = (top - current_price) / (top - bottom)
    return min_fill <= fill_ratio <= max_fill


def ob_is_clean(pd_array, m5_history_before):
    """OB propre = pas retestee avant la formation (touched=0 deja filtre en amont)."""
    top, bottom = pd_array['price_high'], pd_array['price_low']
    for c in m5_history_before:
        if bottom <= c['low'] <= top or bottom <= c['high'] <= top:
            return False
    return True


# ---------------------------------------------------------------------------
# 4. SIMULATION D'UN TRADE (meme mecanique que position_monitor.py, simplifiee
#    - sans BE/trailing pour une premiere passe ; a enrichir si besoin)
# ---------------------------------------------------------------------------
def simulate_trade(entry, sl, tp, m5_after_entry):
    for c in m5_after_entry:
        if c['low'] <= sl:
            return {'result': 'LOSS', 'exit_reason': 'HIT_SL', 'rr_realized': -1.0}
        if c['high'] >= tp:
            rr = (tp - entry) / (entry - sl) if entry > sl else 0
            return {'result': 'WIN', 'exit_reason': 'HIT_TP', 'rr_realized': rr}
    return {'result': 'OPEN', 'exit_reason': 'NO_EXIT', 'rr_realized': None}


# ---------------------------------------------------------------------------
# 5. BOUCLE PRINCIPALE DE BACKTEST
# ---------------------------------------------------------------------------
def run_backtest(conn, start_date='2026-05-22', strict_filter=True,
                  session_filter=None, require_h4_bias=False, debug_counters=None,
                  use_choch=True, use_bos=True, use_sweep=True, use_fvg=True,
                  use_ob_clean=True, use_zone=True, array_buffer=ARRAY_BUFFER):
    """
    Reproduit signal_generator.py bar par bar sur M5, en ne gardant que les
    signaux BUY_LIMIT (bias BULLISH), avec la couche de filtre stricte
    optionnelle en plus.

    NB: le calcul H1 de structure_analyzer.py tourne HORAIRE en prod (bougies
    H1, 20 jours de lookback). Ici on le recalcule a chaque bougie M5 en
    utilisant les bougies H1 disponibles jusqu'a cet instant - c'est le
    portage fidele du comportement "structure mise a jour toutes les heures".

    debug_counters: dict optionnel, incremente a chaque etage de filtre pour
    diagnostiquer ou les candidats sont elimines.
    """
    if debug_counters is None:
        debug_counters = {}

    m5 = load_candles(conn, 'M5', start_date)
    h1 = load_candles(conn, 'H1', start_date)
    pd_arrays = load_pd_arrays_bullish(conn, start_date)

    results = []

    for idx in range(100, len(m5) - 1):  # marge de securite au debut/fin
        now = m5[idx]
        now_time = now['candle_time']

        # --- reconstruire l'etat structure_smc "tel qu'il aurait ete" a cet instant ---
        h1_available = [c for c in h1 if c['candle_time'] <= now_time]
        if len(h1_available) < 10:
            continue
        struct = analyze_structure(h1_available[-480:], window_size=3)
        if 'error' in struct or struct['bias'] != 'BULLISH':
            continue
        debug_counters['1_bias_bullish'] = debug_counters.get('1_bias_bullish', 0) + 1

        # --- session filter optionnel ---
        if session_filter:
            hour_paris = (now_time.hour + 2) % 24  # approx UTC->Paris, a affiner DST
            sessions = {
                'LONDON': 8 <= hour_paris < 11,
                'NEW_YORK': 13 <= hour_paris < 16,
                'LONDON_CLOSE': 16 <= hour_paris < 18,
            }
            if not sessions.get(session_filter, False):
                continue

        # --- PD array bullish actif a cet instant, prix dedans (iso signal_generator.py) ---
        active_arrays = [
            a for a in pd_arrays
            if a['candle_time'] <= now_time
            and (a['invalidated_at'] is None or a['invalidated_at'] > now_time)
        ]
        if not active_arrays:
            continue

        current_price = now['close']
        best_array = None
        for arr in active_arrays:
            if (current_price <= arr['price_high'] + array_buffer and
                    current_price >= arr['price_low'] - array_buffer):
                best_array = arr
                break
        if not best_array:
            continue
        debug_counters['2_pd_array_match'] = debug_counters.get('2_pd_array_match', 0) + 1

        # --- couche filtre stricte optionnelle ---
        if strict_filter:
            m5_window = m5[max(0, idx - 90):idx + 1]

            if use_choch:
                choch_ok, choch_level = detect_recent_choch_bullish(m5_window)
                if not choch_ok:
                    continue
                debug_counters['3_choch_ok'] = debug_counters.get('3_choch_ok', 0) + 1
            else:
                choch_level = struct['swing_high']

            if use_bos:
                if not detect_bos_confirmation_bullish(m5_window, choch_level):
                    continue
                debug_counters['4_bos_ok'] = debug_counters.get('4_bos_ok', 0) + 1

            if use_sweep:
                if not liquidity_sweep_before(m5_window):
                    continue
                debug_counters['5_sweep_ok'] = debug_counters.get('5_sweep_ok', 0) + 1

            if use_fvg:
                if best_array['type'] == 'FVG' and not fvg_partial_fill_ok(best_array, current_price):
                    debug_counters['FVG_REJECTED_type'] = debug_counters.get('FVG_REJECTED_type', 0) + 1
                    continue
                debug_counters['6_fvg_ok'] = debug_counters.get('6_fvg_ok', 0) + 1
                debug_counters[f"6_fvg_ok_array_type_{best_array['type']}"] = debug_counters.get(f"6_fvg_ok_array_type_{best_array['type']}", 0) + 1

            if use_ob_clean:
                m5_before_ob = [c for c in m5 if c['candle_time'] < best_array['candle_time']][-30:]
                if not ob_is_clean(best_array, m5_before_ob):
                    continue
                debug_counters['7_ob_clean_ok'] = debug_counters.get('7_ob_clean_ok', 0) + 1

            if use_zone:
                if struct['zone_type'] not in ('DISCOUNT', 'OTE'):
                    continue
                debug_counters['8_zone_ok'] = debug_counters.get('8_zone_ok', 0) + 1

        # --- construction entry/SL/TP, iso signal_generator.py ---
        entry = best_array['price_eq']
        sl = round(best_array['price_low'] - SL_BUFFER, 5)
        if entry - sl < MIN_SL:
            sl = round(entry - MIN_SL, 5)
        tp = struct['swing_high'] if struct['liquidity_target'] < current_price + 0.0015 else struct['liquidity_target']
        if tp <= current_price:
            tp = struct['liquidity_target']
        if tp <= entry:
            continue

        risk = abs(entry - sl)
        reward = abs(tp - entry)
        rr = round(reward / risk, 2) if risk > 0 else 0
        if rr < MIN_RR:
            continue
        debug_counters['9_rr_ok'] = debug_counters.get('9_rr_ok', 0) + 1

        # --- simulation ---
        m5_after = m5[idx + 1: idx + 1 + 2000]  # fenetre de simulation large
        sim = simulate_trade(entry, sl, tp, m5_after)
        if sim['result'] == 'OPEN':
            continue

        results.append({
            'entry_time': now_time, 'entry': entry, 'sl': sl, 'tp': tp,
            'rr_theoretical': rr, 'result': sim['result'],
            'exit_reason': sim['exit_reason'], 'rr_realized': sim['rr_realized'],
            'pd_array_id': best_array['id'], 'pd_array_type': best_array['type'],
            'zone_type': struct['zone_type'],
        })

    return results


def compute_metrics(results):
    n = len(results)
    if n == 0:
        return {'n_trades': 0}
    wins = [r for r in results if r['result'] == 'WIN']
    wr = len(wins) / n
    avg_rr = sum(r['rr_realized'] for r in wins) / len(wins) if wins else 0
    kelly = wr * avg_rr - (1 - wr)
    from collections import Counter
    return {
        'n_trades': n,
        'win_rate_pct': round(wr * 100, 2),
        'avg_rr_realized': round(avg_rr, 2),
        'expectancy_r': round(kelly, 3),
        'exit_reason_distribution': dict(Counter(r['exit_reason'] for r in results)),
        'zone_type_distribution': dict(Counter(r['zone_type'] for r in results)),
    }


if __name__ == '__main__':
    conn = get_conn()

    configs = [
        # Test de sensibilite au buffer PD array, filtre strict desactive
        # pour isoler l'effet du buffer seul sur le taux de match (etage 2)
        {'strict_filter': False, 'session_filter': None, 'array_buffer': 0.00030},  # 3 pips (actuel prod)
        {'strict_filter': False, 'session_filter': None, 'array_buffer': 0.00050},  # 5 pips
        {'strict_filter': False, 'session_filter': None, 'array_buffer': 0.00080},  # 8 pips
        # Puis avec CHoCH+BOS (config retenue le 03/08) sur chaque buffer,
        # pour voir si un buffer plus large ameliore aussi l'echantillon final
        {'strict_filter': True, 'use_choch': True, 'use_bos': True, 'use_sweep': False,
         'use_fvg': False, 'use_ob_clean': False, 'use_zone': False, 'array_buffer': 0.00030},
        {'strict_filter': True, 'use_choch': True, 'use_bos': True, 'use_sweep': False,
         'use_fvg': False, 'use_ob_clean': False, 'use_zone': False, 'array_buffer': 0.00050},
        {'strict_filter': True, 'use_choch': True, 'use_bos': True, 'use_sweep': False,
         'use_fvg': False, 'use_ob_clean': False, 'use_zone': False, 'array_buffer': 0.00080},
    ]

    for cfg in configs:
        print(f"\n=== Config: {cfg} ===")
        debug_counters = {}
        res = run_backtest(conn, debug_counters=debug_counters, **cfg)
        metrics = compute_metrics(res)
        for k, v in metrics.items():
            print(f"  {k}: {v}")
        if cfg.get('strict_filter'):
            print("  --- entonnoir de filtrage ---")
            for k in sorted(debug_counters.keys()):
                print(f"  {k}: {debug_counters[k]}")

    conn.close()
