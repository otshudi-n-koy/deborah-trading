#!/usr/bin/env python3
"""
Alert Monitor - SMC OB/FVG/CHoCH
Remplace le workflow n8n SMC Trading Alerts - OB/FVG
Tourne toutes les 5 minutes via cron
Déduplication via last_alert_at dans pd_arrays_smc
"""
import psycopg2, requests, logging, os, sys
from datetime import datetime, timezone

LOG_FILE = '/opt/deborah-trading/scripts/alert_monitor.log'
logger = logging.getLogger('alert')
logger.setLevel(logging.INFO)
if not logger.handlers:
    fh = logging.FileHandler(LOG_FILE)
    fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logger.addHandler(fh)

DB_CONFIG = {
    "host": "localhost", "port": 5432,
    "dbname": "trading", "user": "trading", "password": "Trading2026"
}

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
TTL_HOURS = 4

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def send_telegram(message: str):
    try:
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            # Récupérer depuis n8n credentials ou config
            conn = get_conn()
            cur = conn.cursor()
            # Utiliser le webhook n8n comme fallback
            requests.post(
                'http://localhost:5678/webhook/smc-signal-alert',
                json={'message': message}, timeout=5
            )
            cur.close(); conn.close()
        else:
            requests.post(
                f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage',
                json={'chat_id': TELEGRAM_CHAT_ID, 'text': message},
                timeout=5
            )
    except Exception as e:
        logger.warning(f'Telegram: {e}')

def run():
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()

        # Prix actuel
        cur.execute("""
            SELECT close FROM prices_smc
            WHERE timeframe='5M' ORDER BY candle_time DESC LIMIT 1
        """)
        row = cur.fetchone()
        if not row:
            return
        price = float(row[0])

        # OB/FVG actifs touchés par le prix ET pas alertés depuis TTL_HOURS
        cur.execute("""
            SELECT id, type, direction, price_high, price_low, candle_time
            FROM pd_arrays_smc
            WHERE status = 'active'
            AND price_low <= %s AND price_high >= %s
            AND (last_alert_at IS NULL 
                 OR last_alert_at < NOW() - INTERVAL '%s hours')
            ORDER BY created_at DESC
            LIMIT 10
        """, (price, price, TTL_HOURS))
        
        arrays = cur.fetchall()

        for arr in arrays:
            arr_id, arr_type, direction, high, low, candle_time = arr
            high = float(high); low = float(low)

            # Marquer comme alerté
            cur.execute(
                "UPDATE pd_arrays_smc SET last_alert_at=NOW() WHERE id=%s",
                (arr_id,)
            )
            conn.commit()

            # Emoji
            emoji = '🔶' if arr_type == 'OB' else '🟦'
            dir_upper = direction.upper()

            msg = (
                f"{emoji} {arr_type} {dir_upper} TOUCHÉ\n"
                f"Prix: {price}\n"
                f"Zone: {low} — {high}\n"
                f"{arr_type} formé: {candle_time}\n"
                f"#{arr_type} #{direction} #EURUSD"
            )
            send_telegram(msg)
            logger.info(f'{arr_type} {direction} touché — prix={price} zone={low}-{high}')

        # CHoCH : détection via structure bias change
        cur.execute("""
            SELECT timeframe, bias, last_bos_price, last_mss_price, updated_at
            FROM structure_smc
            WHERE updated_at > NOW() - INTERVAL '10 minutes'
            ORDER BY updated_at DESC LIMIT 3
        """)
        for row in cur.fetchall():
            tf, bias, bos, mss, updated = row
            if mss:
                minutes_ago = int((datetime.now() - updated.replace(tzinfo=None)).total_seconds() / 60)
                msg = (
                    f"⚡ CHoCH DÉTECTÉ - {bias.upper() if bias else 'N/A'}\n"
                    f"Niveau: {mss}\n"
                    f"Il y a {minutes_ago} min\n"
                    f"#SMC #CHoCH #{bias} #EURUSD"
                )
                send_telegram(msg)
                logger.info(f'CHoCH {bias} — niveau={mss}')

    except Exception as e:
        logger.error(f'Erreur: {e}', exc_info=True)
    finally:
        try: cur.close(); conn.close()
        except: pass

if __name__ == '__main__':
    run()
