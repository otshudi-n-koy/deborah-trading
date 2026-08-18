#!/usr/bin/env python3
"""
SMC Structure Analyzer
Remplace le workflow n8n "SMC - Structure Analysis v2"
Tourne toutes les heures via cron système
"""

import psycopg2
import logging
from datetime import datetime, timezone

# Config
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'trading',
    'user': 'trading',
    'password': 'trading'
}

LOG_FILE = '/opt/deborah-trading/scripts/structure_analyzer.log'
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

# BUFFER_NEUTRAL_PIPS abaisse de 5 a 2 pips le 17/08/2026 (ticket #54).
# Backtest de sensibilite (backtest_neutral_buffer_sensitivity.py) : le
# biais H1 neutre dominait 26-57% du temps sur les jours de semaine recents
# avec le seuil 5p, plus que l'ATR faible deja optimise (ticket #46).
# Pattern monotone net : 0p -> 0% temps neutre/n=6/Kelly=+1.16 (meilleur),
# 2p -> 13.3%/n=4/Kelly=+0.855, 5p (ancien) -> 27.1%/n=4/Kelly=+0.855 (meme
# echantillon que 2p, pas de gain a garder 5p), 8p+ -> volume qui s'effondre
# sans gain de qualite stable. Approche progressive et prudente retenue
# (2p plutot que 0p directement) - le concept de zone neutre existe
# probablement pour eviter le bruit de marche pres du point d'equilibre,
# a valider en conditions reelles avant d'envisager une nouvelle baisse.
BUFFER_NEUTRAL_PIPS = 0.0002  # 2 pips : zone tampon autour du point median
                               # avant de trancher un biais BULLISH/BEARISH

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def analyze_structure(candles, window_size=3):
    """Détecte swing highs/lows et calcule le biais SMC"""
    if len(candles) < window_size * 2 + 1:
        return {'error': f'Pas assez de bougies: {len(candles)}'}

    candles = sorted(candles, key=lambda c: c['candle_time'])

    swing_highs = []
    swing_lows = []

    for i in range(window_size, len(candles) - window_size):
        c = candles[i]
        is_high = all(float(candles[i-j]['high']) < float(c['high']) and
                     float(candles[i+j]['high']) < float(c['high'])
                     for j in range(1, window_size + 1))
        is_low = all(float(candles[i-j]['low']) > float(c['low']) and
                    float(candles[i+j]['low']) > float(c['low'])
                    for j in range(1, window_size + 1))

        if is_high:
            swing_highs.append({'price': float(c['high']), 'time': c['candle_time']})
        if is_low:
            swing_lows.append({'price': float(c['low']), 'time': c['candle_time']})

    # Filtrer swings significatifs (écart min 10 pips)
    def filter_sig(swings, is_high):
        out = []
        for s in swings:
            if not out:
                out.append(s)
                continue
            last = out[-1]
            if abs(s['price'] - last['price']) >= 0.0010:
                out.append(s)
            elif is_high and s['price'] > last['price']:
                out[-1] = s
            elif not is_high and s['price'] < last['price']:
                out[-1] = s
        return out

    swing_highs = filter_sig(swing_highs, True)
    swing_lows  = filter_sig(swing_lows, False)

    current_close = float(candles[-1]['close'])

    # Fallback si pas assez de swings
    if len(swing_highs) < 2 or len(swing_lows) < 2:
        recent = candles[-48:]
        fH = max(float(c['high']) for c in recent)
        fL = min(float(c['low'])  for c in recent)
        rng = fH - fL
        eq50 = fL + rng * 0.5
        if abs(current_close - eq50) < BUFFER_NEUTRAL_PIPS:
            bias = 'NEUTRAL'
        else:
            bias = 'BEARISH' if current_close < eq50 else 'BULLISH'
        return {
            'bias': bias,
            'swing_high': fH, 'swing_low': fL,
            'last_bos_price': None, 'last_mss_price': None,
            'zone_type': 'PREMIUM' if current_close > eq50 else 'DISCOUNT',
            'ote_high': round(fL + rng * 0.786, 5),
            'ote_low':  round(fL + rng * 0.618, 5),
            'eq50':     round(eq50, 5),
            'liquidity_target': round(fL if bias == 'BEARISH' else fH, 5),
            'current_close': current_close,
            'swing_highs_found': len(swing_highs),
            'swing_lows_found':  len(swing_lows),
            'method': 'FALLBACK_EXTREMES'
        }

    last_hh = swing_highs[-1]
    prev_hh = swing_highs[-2]
    last_ll = swing_lows[-1]
    prev_ll = swing_lows[-2]

    # GARDE-FOU REACTIVITE (28/07/2026) : un swing confirme (window_size=3)
    # necessite 3 bougies APRES l'extreme pour etre reconnu, creant un delai
    # structurel pendant lequel swing_low/swing_high reste obsolete si le
    # prix a deja franchi ce niveau de facon decisive. Cause racine du bug
    # TP corrige en urgence le 24/07/2026 (rustine cote signal_generator.py,
    # celle-ci corrige la source). On complete last_hh/last_ll avec l'extreme
    # le plus recent des dernieres bougies si celui-ci depasse le swing confirme,
    # evitant que le calcul de biais/liquidite se base sur un niveau perime.
    RECENT_LOOKBACK = 10
    MIN_SIGNIFICANT_PIPS = 0.0010  # meme seuil que filter_sig, pour eviter qu'une
                                    # simple meche non significative ne remplace un
                                    # swing confirme comme reference structurelle
    recent = candles[-RECENT_LOOKBACK:]
    recent_low_val = min(float(c['low']) for c in recent)
    recent_high_val = max(float(c['high']) for c in recent)
    if recent_low_val < last_ll['price'] - MIN_SIGNIFICANT_PIPS:
        idx_low = min(range(len(recent)), key=lambda i: float(recent[i]['low']))
        last_ll = {'price': recent_low_val, 'time': recent[idx_low]['candle_time']}
    if recent_high_val > last_hh['price'] + MIN_SIGNIFICANT_PIPS:
        idx_high = max(range(len(recent)), key=lambda i: float(recent[i]['high']))
        last_hh = {'price': recent_high_val, 'time': recent[idx_high]['candle_time']}

    # Biais basé sur position dans le range macro (ICT Premium/Discount)
    range_mid = (last_hh['price'] + last_ll['price']) / 2
    if abs(current_close - range_mid) < BUFFER_NEUTRAL_PIPS:
        bias = 'NEUTRAL'
    else:
        bias = 'BULLISH' if current_close > range_mid else 'BEARISH'
    bos_price = mss_price = None

    if   current_close > last_hh['price'] and last_hh['price'] > prev_hh['price']:
        bos_price = last_hh['price']
    elif current_close < last_ll['price'] and last_ll['price'] < prev_ll['price']:
        bos_price = last_ll['price']
    elif current_close > last_hh['price'] and last_hh['price'] < prev_hh['price']:
        mss_price = last_hh['price']
    elif current_close < last_ll['price'] and last_ll['price'] > prev_ll['price']:
        mss_price = last_ll['price']

    rng   = last_hh['price'] - last_ll['price']
    eq50  = last_ll['price'] + rng * 0.5
    ote_h = last_ll['price'] + rng * 0.786
    ote_l = last_ll['price'] + rng * 0.618

    if   current_close >= ote_l and current_close <= ote_h: zone_type = 'OTE'
    elif current_close > eq50:                               zone_type = 'PREMIUM'
    else:                                                    zone_type = 'DISCOUNT'

    lower_lows   = [s for s in swing_lows  if s['price'] < current_close]
    higher_highs = [s for s in swing_highs if s['price'] > current_close]

    if bias == 'BULLISH':
        liq = min(higher_highs, key=lambda s: s['price'])['price'] if higher_highs else round(current_close + 0.005, 5)
    else:
        liq = max(lower_lows,  key=lambda s: s['price'])['price'] if lower_lows  else round(current_close - 0.005, 5)

    return {
        'bias': bias,
        'swing_high': last_hh['price'], 'swing_low': last_ll['price'],
        'last_bos_price': bos_price, 'last_mss_price': mss_price,
        'zone_type': zone_type,
        'ote_high': round(ote_h, 5), 'ote_low': round(ote_l, 5),
        'eq50': round(eq50, 5),
        'liquidity_target': round(liq, 5),
        'current_close': current_close,
        'swing_highs_found': len(swing_highs),
        'swing_lows_found':  len(swing_lows),
        'method': 'SWING_ANALYSIS'
    }

def run():
    try:
        conn = get_conn()
        cur  = conn.cursor()

        # Récupérer bougies 1H (20 jours)
        cur.execute("""
            SELECT candle_time, open, high, low, close
            FROM prices_smc
            WHERE timeframe = 'H1'
            AND candle_time >= NOW() - INTERVAL '20 days'
            ORDER BY candle_time ASC
            LIMIT 480
        """)
        rows_1h = [{'candle_time': r[0], 'open': r[1], 'high': r[2], 'low': r[3], 'close': r[4]} for r in cur.fetchall()]

        # Récupérer bougies 4H (60 jours)
        cur.execute("""
            SELECT candle_time, open, high, low, close
            FROM prices_smc
            WHERE timeframe = 'H4'
            AND candle_time >= NOW() - INTERVAL '60 days'
            ORDER BY candle_time ASC
            LIMIT 360
        """)
        rows_4h = [{'candle_time': r[0], 'open': r[1], 'high': r[2], 'low': r[3], 'close': r[4]} for r in cur.fetchall()]

        # FIX 18/08/2026 (ticket #56) : l'API cTrader (ProtoOAGetTrendbarsReq)
        # ne renvoie pas de mise a jour live pour la bougie H1/H4 en cours de
        # formation - confirme sur 3 cycles consecutifs (close identique
        # pendant 15 min malgre un vrai mouvement de marche visible sur M1).
        # Consequence : current_close (donc swing_high/liquidity_target/TP)
        # etait calcule sur un prix obsolete pendant tout le cycle en cours,
        # jusqu'a 59 min de retard. On reconstruit ici la bougie en cours a
        # partir des bougies M1 (flux actif depuis le 07/08, ticket #48),
        # qui elles se mettent bien a jour en temps reel.
        def _build_live_candle(rows_htf):
            """Remplace la derniere bougie de rows_htf par une version
            reconstruite depuis les M1 disponibles pour la periode en cours,
            si des M1 plus recentes que cette derniere bougie existent."""
            if not rows_htf:
                return rows_htf
            last_candle_time = rows_htf[-1]['candle_time']
            cur.execute("""
                SELECT candle_time, open, high, low, close
                FROM prices_smc
                WHERE timeframe = 'M1' AND candle_time >= %s
                ORDER BY candle_time ASC
            """, (last_candle_time,))
            m1_rows = cur.fetchall()
            if not m1_rows:
                return rows_htf
            live_open = float(m1_rows[0][1])
            live_high = max(float(r[2]) for r in m1_rows)
            live_low = min(float(r[3]) for r in m1_rows)
            live_close = float(m1_rows[-1][4])
            rows_htf = rows_htf[:-1] + [{
                'candle_time': last_candle_time,
                'open': live_open, 'high': live_high,
                'low': live_low, 'close': live_close,
            }]
            return rows_htf

        try:
            rows_1h = _build_live_candle(rows_1h)
            rows_4h = _build_live_candle(rows_4h)
        except Exception as _le:
            logging.error(f'Erreur construction bougie live M1: {_le}')

        if len(rows_1h) < 10:
            logging.warning(f'Pas assez de bougies 1H: {len(rows_1h)}')
            return
        if len(rows_4h) < 10:
            logging.warning(f'Pas assez de bougies 4H: {len(rows_4h)}')
            return

        s1h = analyze_structure(rows_1h, 3)
        s4h = analyze_structure(rows_4h, 3)

        if 'error' in s1h or 'error' in s4h:
            logging.warning(f'Erreur analyse: 1H={s1h} 4H={s4h}')
            return

        confluence = s1h['bias'] == s4h['bias'] and s1h['bias'] != 'NEUTRAL'

        if not confluence:
            logging.info(f'Pas de confluence — 1H: {s1h["bias"]} / 4H: {s4h["bias"]}')
            # IMPORTANT : ne PAS return ici. Il faut ecrire confluence=false en DB
            # pour que signal_generator.py voie l'etat reel a jour, sinon la table
            # garde silencieusement la derniere confluence=true figee (bug decouvert
            # le 06/07/2026 : signal #78 genere sur une confluence perimee de 3 jours).

        # Upsert dans structure_smc
        cur.execute("""
            INSERT INTO structure_smc
            (bias, h4_bias, confluence, swing_high, swing_low,
             last_bos_price, last_mss_price, zone_type,
             ote_high, ote_low, eq50, liquidity_target, timeframe, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'1H',NOW())
            ON CONFLICT (timeframe) DO UPDATE SET
              bias             = EXCLUDED.bias,
              h4_bias          = EXCLUDED.h4_bias,
              confluence       = EXCLUDED.confluence,
              swing_high       = EXCLUDED.swing_high,
              swing_low        = EXCLUDED.swing_low,
              last_bos_price   = EXCLUDED.last_bos_price,
              last_mss_price   = EXCLUDED.last_mss_price,
              zone_type        = EXCLUDED.zone_type,
              ote_high         = EXCLUDED.ote_high,
              ote_low          = EXCLUDED.ote_low,
              eq50             = EXCLUDED.eq50,
              liquidity_target = EXCLUDED.liquidity_target,
              updated_at       = NOW()
        """, (
            s1h['bias'], s4h['bias'], confluence,
            s1h['swing_high'], s1h['swing_low'],
            s1h['last_bos_price'], s1h['last_mss_price'],
            s1h['zone_type'],
            s1h['ote_high'], s1h['ote_low'], s1h['eq50'],
            s1h['liquidity_target']
        ))

        try:
            cur.execute("""
                INSERT INTO structure_smc_history
                (bias, h4_bias, confluence, zone_type, swing_high, swing_low,
                 last_bos_price, last_mss_price, liquidity_target, eq50, recorded_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            """, (
                s1h['bias'], s4h['bias'], confluence, s1h['zone_type'],
                s1h['swing_high'], s1h['swing_low'],
                s1h['last_bos_price'], s1h['last_mss_price'],
                s1h['liquidity_target'], s1h['eq50']
            ))
        except Exception as _he:
            logging.error(f'Erreur log structure_smc_history: {_he}')

        conn.commit()
        logging.info(
            f"Structure OK — bias={s1h['bias']} h4={s4h['bias']} "
            f"confluence={confluence} zone={s1h['zone_type']} "
            f"eq50={s1h['eq50']} 1H={len(rows_1h)} 4H={len(rows_4h)}"
        )

    except Exception as e:
        logging.error(f'Erreur: {e}', exc_info=True)
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

if __name__ == '__main__':
    run()
