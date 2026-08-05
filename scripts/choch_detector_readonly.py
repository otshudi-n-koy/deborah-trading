#!/usr/bin/env python3
"""
CHoCH/BOS Detector - Mode lecture seule (Etape 1 du chantier multi-timeframe, valide le 18/07/2026)
Utilise smc_engine_final.py (deja existant, jamais branche sur le pipeline live) pour
detecter CHoCH/BOS sur M5, et logue les evenements sans AUCUN impact sur signal_generator.py.
Objectif : accumuler des donnees CHoCH/BOS reelles avant de decider si/comment les integrer
au pipeline de decision (etapes 2-4 du chantier, plus tard).
"""
import psycopg2
import logging
import sys
sys.path.insert(0, '/root')
from smc_engine_final import Candle, SMCEngine

DB_CONFIG = {'host': 'localhost', 'port': 5432, 'dbname': 'trading', 'user': 'trading', 'password': 'trading'}
LOG_FILE = '/opt/deborah-trading/scripts/choch_detector_readonly.log'
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def run():
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT candle_time, open, high, low, close
            FROM prices_smc WHERE timeframe='M5'
            ORDER BY candle_time DESC LIMIT 300
        """)
        rows = cur.fetchall()[::-1]
        if len(rows) < 60:
            logging.warning(f'Pas assez de bougies M5: {len(rows)}')
            return

        candles = [Candle(str(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4])) for r in rows]

        eng = SMCEngine(candles, swing_lookback=5)
        eng.analyze()

        # Ne logger que les evenements de structure recents (derniere heure de donnees, ~12 bougies M5)
        recent_cutoff_idx = len(candles) - 12
        recent_events = [s for s in eng.structure if s.index >= recent_cutoff_idx]

        inserted = 0
        for ev in recent_events:
            cur.execute("""
                SELECT COUNT(*) FROM choch_log_smc
                WHERE candle_time = %s AND kind = %s AND direction = %s
            """, (ev.time, ev.kind, ev.direction))
            if cur.fetchone()[0] > 0:
                continue
            cur.execute("""
                INSERT INTO choch_log_smc (candle_time, kind, direction, level, created_at)
                VALUES (%s, %s, %s, %s, NOW())
            """, (ev.time, ev.kind, ev.direction, ev.level))
            inserted += 1

        conn.commit()
        logging.info(f'CHoCH/BOS scan - {len(recent_events)} evenements recents, {inserted} nouveaux inseres')

    except Exception as e:
        logging.error(f'Erreur: {e}', exc_info=True)
    finally:
        try:
            cur.close(); conn.close()
        except:
            pass

if __name__ == '__main__':
    run()
