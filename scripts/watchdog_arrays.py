#!/usr/bin/env python3
"""
Watchdog PD Arrays — alerte Telegram si aucun array valide près du prix actuel
"""
import psycopg2
import requests
import os

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', '8400529290:AAEyRzGa0JNCsuecpNJ6gXrqQQM8hnlt-ao')
TELEGRAM_CHAT  = os.getenv('TELEGRAM_CHAT', '1664221853')
PROXIMITY_PIPS = 0.0050  # 50 pips

def get_db():
    return psycopg2.connect(host='localhost', dbname='trading', user='trading', password='trading', port=5432)

def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        print("Telegram non configuré")
        return
    requests.post(f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage',
        json={'chat_id': TELEGRAM_CHAT, 'text': msg, 'parse_mode': 'HTML'})

def main():
    conn = get_db()
    cur = conn.cursor()
    
    # Prix actuel
    cur.execute("SELECT close FROM prices_smc WHERE timeframe='M5' ORDER BY candle_time DESC LIMIT 1")
    row = cur.fetchone()
    if not row:
        print("Pas de prix")
        return
    price = float(row[0])
    
    # Arrays valides près du prix
    cur.execute("""
        SELECT COUNT(*) FROM pd_arrays_smc 
        WHERE status='active' AND touched=0
        AND price_high >= %s - %s
        AND price_low <= %s + %s
    """, (price, PROXIMITY_PIPS, price, PROXIMITY_PIPS))
    count = cur.fetchone()[0]
    
    # Bot status
    cur.execute("SELECT bot_status FROM capital_smc WHERE id=1")
    bot_status = cur.fetchone()[0]
    
    conn.close()
    
    print(f"Prix: {price:.5f} | Arrays proches: {count} | Bot: {bot_status}")
    
    if count == 0 and bot_status == 'ACTIVE':
        msg = (f"⚠️ <b>SMC — Aucun PD Array valide</b>\n"
               f"Prix: {price:.5f}\n"
               f"Rayon: ±50 pips\n"
               f"Action requise: vérifier pd_array_detector")
        send_telegram(msg)
        print("Alerte Telegram envoyée")

if __name__ == '__main__':
    main()
