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

    # Biais basé sur position dans le range macro (ICT Premium/Discount)
    range_mid = (last_hh['price'] + last_ll['price']) / 2
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
            WHERE timeframe = '1H'
            AND candle_time >= NOW() - INTERVAL '20 days'
            ORDER BY candle_time ASC
            LIMIT 480
        """)
        rows_1h = [{'candle_time': r[0], 'open': r[1], 'high': r[2], 'low': r[3], 'close': r[4]} for r in cur.fetchall()]

        # Récupérer bougies 4H (60 jours)
        cur.execute("""
            SELECT candle_time, open, high, low, close
            FROM prices_smc
            WHERE timeframe = '4H'
            AND candle_time >= NOW() - INTERVAL '60 days'
            ORDER BY candle_time ASC
            LIMIT 360
        """)
        rows_4h = [{'candle_time': r[0], 'open': r[1], 'high': r[2], 'low': r[3], 'close': r[4]} for r in cur.fetchall()]

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
            return

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
            s4h['bias'], s4h['bias'], confluence,
            s1h['swing_high'], s1h['swing_low'],
            s1h['last_bos_price'], s1h['last_mss_price'],
            s1h['zone_type'],
            s1h['ote_high'], s1h['ote_low'], s1h['eq50'],
            s1h['liquidity_target']
        ))

        conn.commit()
        logging.info(
            f"Structure OK — bias={s4h['bias']} h4={s4h['bias']} "
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
