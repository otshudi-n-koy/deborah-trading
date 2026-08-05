#!/usr/bin/env python3
"""
signal_projection.py
Factorise la logique de selection PD-array + construction entry/SL/TP/RR de
signal_generator.py (etapes 7-12), en une fonction pure de LECTURE SEULE,
reutilisable par tout consommateur qui veut savoir "que produirait
signal_generator.py maintenant" sans dupliquer le code ni ecrire en DB.

Utilise par:
- agent_pre_killzone.py (projection affichee au briefing, explicitement
  labellisee comme non confirmee)

IMPORTANT : ce module ne fait AUCUNE ecriture DB. Il ne fait que lire et
calculer, exactement comme signal_generator.py le ferait avant son propre
INSERT. Toute divergence de logique avec signal_generator.py doit etre
corrigee ICI en priorite pour rester iso-strategie (le porteur de verite
reste signal_generator.py ; ce module en est un miroir de lecture).
"""

MIN_SL = 0.00100        # identique a signal_generator.py
SL_BUFFER = 0.00010      # identique a signal_generator.py
ARRAY_BUFFER = 0.00050   # identique a signal_generator.py (5 pips depuis le 04/08/2026)
MIN_RR = 1.5             # identique a signal_generator.py


def compute_signal_projection(cur, bias, current_price):
    """
    Rejoue la logique de signal_generator.py (etapes 7-12) a l'instant present,
    sans rien ecrire en DB. Retourne soit un dict de projection, soit None
    avec une raison de rejet (meme esprit que les logs "RR insuffisant",
    "Prix hors PD Array", etc. de signal_generator.py).

    Retourne: (projection_dict_or_None, reason_str)
    """
    if bias not in ('BULLISH', 'BEARISH'):
        return None, 'biais neutre ou absent'

    direction = 'bullish' if bias == 'BULLISH' else 'bearish'

    cur.execute("""
        SELECT id, type, direction, price_high, price_low, price_eq, strength, combo
        FROM pd_arrays_smc
        WHERE status = 'active' AND touched = 0
        AND direction = %s
        AND price_high >= 0
        AND timeframe = 'M5'
        ORDER BY strength DESC, created_at DESC
        LIMIT 10
    """, (direction,))
    pd_arrays = [
        {'id': r[0], 'type': r[1], 'direction': r[2],
         'price_high': float(r[3]), 'price_low': float(r[4]),
         'price_eq': float(r[5]), 'strength': r[6], 'combo': r[7]}
        for r in cur.fetchall()
    ]

    if not pd_arrays:
        return None, f'aucun PD array actif ({direction})'

    best_array = None
    for arr in pd_arrays:
        if (current_price <= arr['price_high'] + ARRAY_BUFFER and
                current_price >= arr['price_low'] - ARRAY_BUFFER):
            best_array = arr
            break

    if not best_array:
        return None, 'prix hors de tous les PD arrays actifs'

    cur.execute("""
        SELECT swing_high, swing_low, liquidity_target
        FROM structure_smc ORDER BY updated_at DESC LIMIT 1
    """)
    struct_row = cur.fetchone()
    if not struct_row:
        return None, 'pas de structure_smc disponible'
    swing_high, swing_low, liq_target = (float(x) for x in struct_row)

    if bias == 'BULLISH':
        entry = best_array['price_eq']
        sl = round(best_array['price_low'] - SL_BUFFER, 5)
        if entry - sl < MIN_SL:
            sl = round(entry - MIN_SL, 5)
        tp = swing_high if liq_target < current_price + 0.0015 else liq_target
        if tp <= current_price:
            tp = liq_target
    else:
        entry = best_array['price_eq']
        sl = round(best_array['price_high'] + SL_BUFFER, 5)
        if sl - entry < MIN_SL:
            sl = round(entry + MIN_SL, 5)
        tp = swing_low if liq_target > current_price - 0.0015 else liq_target
        if tp >= current_price:
            tp = liq_target

    if bias == 'BULLISH' and tp <= entry:
        return None, f'TP invalide pour BUY (tp={tp} entry={entry})'
    if bias == 'BEARISH' and tp >= entry:
        return None, f'TP invalide pour SELL (tp={tp} entry={entry})'

    risk = abs(entry - sl)
    reward = abs(tp - entry)
    rr = round(reward / risk, 2) if risk > 0 else 0
    if rr < MIN_RR:
        return None, f'RR insuffisant: {rr} (min {MIN_RR})'

    sl_pips = round(risk * 10000, 1)
    tp_pips = round(reward * 10000, 1)

    return {
        'signal_type': 'BUY_LIMIT' if bias == 'BULLISH' else 'SELL_LIMIT',
        'entry': entry, 'sl': sl, 'tp': tp,
        'sl_pips': sl_pips, 'tp_pips': tp_pips, 'rr': rr,
        'pd_array_id': best_array['id'], 'pd_array_type': best_array['type'],
    }, None
