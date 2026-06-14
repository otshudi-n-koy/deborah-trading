#!/usr/bin/env python3
"""
SMC PD Array Detector
Remplace le workflow n8n "SMC - PD Array Detection"
Tourne toutes les 5 minutes via cron système
Détecte Order Blocks (OB) et Fair Value Gaps (FVG) sur données 5M
"""

import psycopg2
import logging
from datetime import datetime

LOG_FILE = '/opt/deborah-trading/scripts/pd_array_detector.log'
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

DB_CONFIG = {
    'host': 'localhost', 'port': 5432,
    'dbname': 'trading', 'user': 'trading', 'password': 'trading'
}

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def detect_ob(candles):
    """Détecte les Order Blocks (dernière bougie opposée avant un move fort)"""
    obs = []
    for i in range(2, len(candles) - 1):
        c     = candles[i]
        prev  = candles[i-1]
        next1 = candles[i+1]

        c_open  = float(c['open'])
        c_close = float(c['close'])
        c_high  = float(c['high'])
        c_low   = float(c['low'])

        n_open  = float(next1['open'])
        n_close = float(next1['close'])

        c_body  = abs(c_close - c_open)
        n_body  = abs(n_close - n_open)

        if c_body < 0.00005:  # Ignorer dojis
            continue

        # OB Bearish : bougie haussière suivie d'une bougie baissière forte
        if (c_close > c_open and          # bougie verte
            n_close < n_open and          # suivie rouge
            n_body > c_body * 1.5 and     # body suivant > 1.5x
            n_close < c_open):            # close sous l'open de l'OB
            obs.append({
                'type': 'OB',
                'direction': 'bearish',
                'price_high': c_high,
                'price_low':  c_open,
                'price_eq':   round((c_high + c_open) / 2, 5),
                'candle_time': c['candle_time'],
                'strength': 2
            })

        # OB Bullish : bougie baissière suivie d'une bougie haussière forte
        elif (c_close < c_open and        # bougie rouge
              n_close > n_open and        # suivie verte
              n_body > c_body * 1.5 and   # body suivant > 1.5x
              n_close > c_open):          # close au-dessus de l'open de l'OB
            obs.append({
                'type': 'OB',
                'direction': 'bullish',
                'price_high': c_open,
                'price_low':  c_low,
                'price_eq':   round((c_open + c_low) / 2, 5),
                'candle_time': c['candle_time'],
                'strength': 2
            })

    return obs

def detect_fvg(candles):
    """Détecte les Fair Value Gaps (imbalances entre 3 bougies)"""
    fvgs = []
    for i in range(1, len(candles) - 1):
        prev = candles[i-1]
        curr = candles[i]
        next1 = candles[i+1]

        prev_high = float(prev['high'])
        prev_low  = float(prev['low'])
        next_high = float(next1['high'])
        next_low  = float(next1['low'])
        curr_close = float(curr['close'])
        curr_open  = float(curr['open'])

        # FVG Bearish : gap entre low de prev et high de next (move baissier)
        if (prev_low > next_high and
            curr_close < curr_open and   # bougie baissière au centre
            prev_low - next_high >= 0.00010):  # gap min 1 pip
            fvgs.append({
                'type': 'FVG',
                'direction': 'bearish',
                'price_high': prev_low,
                'price_low':  next_high,
                'price_eq':   round((prev_low + next_high) / 2, 5),
                'candle_time': curr['candle_time'],
                'strength': 2
            })

        # FVG Bullish : gap entre high de prev et low de next (move haussier)
        elif (next_low > prev_high and
              curr_close > curr_open and  # bougie haussière au centre
              next_low - prev_high >= 0.00010):  # gap min 1 pip
            fvgs.append({
                'type': 'FVG',
                'direction': 'bullish',
                'price_high': next_low,
                'price_low':  prev_high,
                'price_eq':   round((next_low + prev_high) / 2, 5),
                'candle_time': curr['candle_time'],
                'strength': 2
            })

    return fvgs

def run():
    try:
        conn = get_conn()
        cur  = conn.cursor()

        # Récupérer bougies 5M des 12 dernières heures
        cur.execute("""
            SELECT candle_time, open, high, low, close
            FROM prices_smc
            WHERE timeframe = 'M5'
            AND candle_time >= NOW() - INTERVAL '12 hours'
            ORDER BY candle_time ASC
        """)
        rows = [{'candle_time': r[0], 'open': r[1], 'high': r[2],
                 'low': r[3], 'close': r[4]} for r in cur.fetchall()]

        if len(rows) < 10:
            logging.warning(f'Pas assez de bougies 5M: {len(rows)}')
            return

        # Détecter OB et FVG
        obs  = detect_ob(rows)
        fvgs = detect_fvg(rows)
        arrays = obs + fvgs

        # Prix actuel
        current_price = float(rows[-1]['close'])

        # Invalider les PD Arrays touchés par le prix
        cur.execute("""
            UPDATE pd_arrays_smc
            SET status = 'invalidated', invalidated_at = NOW()
            WHERE status = 'active'
            AND (
                (direction = 'bearish' AND %s >= price_low AND %s <= price_high + 0.00020)
                OR
                (direction = 'bullish' AND %s <= price_high AND %s >= price_low - 0.00020)
            )
        """, (current_price, current_price, current_price, current_price))

        inserted = 0
        for arr in arrays:
            # Vérifier qu'il n'existe pas déjà un array similaire
            cur.execute("""
                SELECT COUNT(*) FROM pd_arrays_smc
                WHERE type = %s AND direction = %s
                AND ABS(price_eq - %s) < 0.00010
                AND status = 'active'
            """, (arr['type'], arr['direction'], arr['price_eq']))

            if cur.fetchone()[0] > 0:
                continue

            cur.execute("""
                INSERT INTO pd_arrays_smc
                (type, direction, price_high, price_low, price_eq,
                 strength, touched, status, combo, timeframe, candle_time, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, 0, 'active', false, '5M', %s, NOW())
            ON CONFLICT ON CONSTRAINT pd_arrays_unique DO NOTHING
            """, (
                arr['type'], arr['direction'],
                arr['price_high'], arr['price_low'], arr['price_eq'],
                arr['strength'], arr['candle_time']
            ))
            inserted += 1

        conn.commit()

        # Stats
        cur.execute("SELECT COUNT(*) FROM pd_arrays_smc WHERE status='active' AND touched=0")
        active = cur.fetchone()[0]

        logging.info(
            f"PD Arrays — {inserted} insérés, {active} actifs, "
            f"prix={current_price}, bougies={len(rows)}, "
            f"OB={len(obs)} FVG={len(fvgs)}"
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
