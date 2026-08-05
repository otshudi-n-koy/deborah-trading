#!/usr/bin/env python3
"""
smc_choch_bos.py
Module partage de detection CHoCH/BOS haussier base sur une vraie structure
de swings (HH/HL/LH/LL), meme mecanique que structure_analyzer.py mais
appliquee sur M5.

Extrait fidele de backtest_buy_filter_strict.py (03/08/2026), valide en
ablation (CHoCH+BOS: WR 66.7%, Kelly +1.38 sur echantillon n=6 - non
concluant statistiquement mais tres encourageant directionnellement).

Utilise par:
- backtest_buy_filter_strict.py (backtest offline)
- signal_generator.py (mode shadow, flag passed_choch_bos_filter)
"""

MIN_SIG_PIPS_DEFAULT = 0.0006  # seuil swing significatif sur M5 (vs 0.0010 sur H1/H4)


def detect_swings_m5(m5_window, window_size=3, min_sig_pips=MIN_SIG_PIPS_DEFAULT):
    """
    Detection de swing highs/lows sur M5, meme mecanique que
    structure_analyzer.analyze_structure() (fenetre glissante + filtrage par
    ecart minimum significatif). Retourne une liste chronologique de swings
    classes HH/HL/LH/LL.
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
    dernier swing high (LH) significatif.
    Retourne (bool, choch_level).
    """
    if len(m5_window) < lookback:
        return False, None
    window = m5_window[-lookback:]
    swings = detect_swings_m5(window)
    if len(swings) < 4:
        return False, None

    pre_swings = [s for s in swings if s['index'] < len(window) - 5]
    if len(pre_swings) < 3:
        return False, None
    recent_kinds = [s['kind'] for s in pre_swings[-4:]]
    was_bearish_structure = recent_kinds.count('LH') + recent_kinds.count('LL') >= 3

    last_swing_high = next((s for s in reversed(pre_swings) if s['kind'] in ('HH', 'LH')), None)
    if not last_swing_high:
        return False, None

    last_close = window[-1]['close']
    choch_confirmed = was_bearish_structure and last_close > last_swing_high['price']
    return choch_confirmed, (last_swing_high['price'] if choch_confirmed else None)


def detect_bos_confirmation_bullish(m5_window, choch_level, max_bars=15):
    """
    BOS haussier = continuation confirmee post-CHoCH : nouvelle cassure
    haussiere au-dela du niveau CHoCH, dans une fenetre de suivi limitee.
    """
    if choch_level is None:
        return False
    recent = m5_window[-max_bars:]
    tolerance = choch_level * 1.0002  # ~2 pips de marge
    return any(c['high'] > tolerance for c in recent)


def detect_recent_choch_bearish(m5_window, lookback=80):
    """
    CHoCH baissier ICT : symetrique de detect_recent_choch_bullish. Structure
    de swings recente haussiere (HH/HL) cassee par une cloture sous le
    dernier swing low (HL) significatif.
    Retourne (bool, choch_level).
    """
    if len(m5_window) < lookback:
        return False, None
    window = m5_window[-lookback:]
    swings = detect_swings_m5(window)
    if len(swings) < 4:
        return False, None

    pre_swings = [s for s in swings if s['index'] < len(window) - 5]
    if len(pre_swings) < 3:
        return False, None
    recent_kinds = [s['kind'] for s in pre_swings[-4:]]
    was_bullish_structure = recent_kinds.count('HL') + recent_kinds.count('HH') >= 3

    last_swing_low = next((s for s in reversed(pre_swings) if s['kind'] in ('LL', 'HL')), None)
    if not last_swing_low:
        return False, None

    last_close = window[-1]['close']
    choch_confirmed = was_bullish_structure and last_close < last_swing_low['price']
    return choch_confirmed, (last_swing_low['price'] if choch_confirmed else None)


def detect_bos_confirmation_bearish(m5_window, choch_level, max_bars=15):
    """BOS baissier = symetrique de detect_bos_confirmation_bullish."""
    if choch_level is None:
        return False
    recent = m5_window[-max_bars:]
    tolerance = choch_level * 0.9998  # ~2 pips de marge
    return any(c['low'] < tolerance for c in recent)


def liquidity_sweep_before(m5_window, direction, lookback=15, recent_bars=10):
    """
    Detection generique de liquidity sweep, dans les deux directions.
    direction='bullish' -> cherche un sweep d'un plus bas anterieur (mise en
    place typique avant un mouvement haussier). direction='bearish' -> sweep
    d'un plus haut anterieur.
    Fenetre de tolerance (recent_bars) plutot qu'une position fixe, cf.
    ajustement du 03/08/2026 sur backtest_buy_filter_strict.py.
    """
    total_needed = lookback + recent_bars + 1
    if len(m5_window) < total_needed:
        return False
    for offset in range(1, recent_bars + 1):
        candidate_idx = len(m5_window) - 1 - offset
        prior_window = m5_window[max(0, candidate_idx - lookback):candidate_idx]
        if not prior_window:
            continue
        if direction == 'bullish':
            prior_low = min(c['low'] for c in prior_window)
            if m5_window[candidate_idx]['low'] < prior_low:
                return True
        else:
            prior_high = max(c['high'] for c in prior_window)
            if m5_window[candidate_idx]['high'] > prior_high:
                return True
    return False


def get_choch_bos_sweep_facts(m5_window, bias):
    """
    Dispatcher generique pour l'agent pre-killzone (et tout futur consommateur
    bidirectionnel) : calcule CHoCH/BOS/sweep dans la direction du bias fourni
    ('BULLISH' ou 'BEARISH'), retourne un dict de faits bruts, deterministes,
    prets a etre injectes tels quels dans un prompt - aucune interpretation
    laissee au LLM.
    """
    if bias not in ('BULLISH', 'BEARISH'):
        return {
            'choch_detected': False, 'choch_level': None,
            'bos_confirmed': False, 'liquidity_sweep_detected': False,
        }

    if bias == 'BULLISH':
        choch_ok, choch_level = detect_recent_choch_bullish(m5_window)
        bos_ok = detect_bos_confirmation_bullish(m5_window, choch_level) if choch_ok else False
        sweep_ok = liquidity_sweep_before(m5_window, 'bullish')
    else:
        choch_ok, choch_level = detect_recent_choch_bearish(m5_window)
        bos_ok = detect_bos_confirmation_bearish(m5_window, choch_level) if choch_ok else False
        sweep_ok = liquidity_sweep_before(m5_window, 'bearish')

    return {
        'choch_detected': choch_ok, 'choch_level': choch_level,
        'bos_confirmed': bos_ok, 'liquidity_sweep_detected': sweep_ok,
    }


def passed_choch_bos_filter(m5_window):
    """
    Fonction combinee pratique pour signal_generator.py (mode shadow BUY) :
    retourne (bool_passed, choch_level) en une seule verification CHoCH puis
    BOS. Conservee inchangee (bullish uniquement) pour ne pas affecter le
    deploiement shadow deja en place.
    """
    choch_ok, choch_level = detect_recent_choch_bullish(m5_window)
    if not choch_ok:
        return False, None
    bos_ok = detect_bos_confirmation_bullish(m5_window, choch_level)
    return bos_ok, choch_level
