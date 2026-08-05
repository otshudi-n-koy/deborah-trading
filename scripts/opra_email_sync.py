#!/usr/bin/env python3
import imaplib, email, re, psycopg2, requests
from email.header import decode_header

IMAP_HOST = 'imap.gmail.com'
IMAP_USER = 'nkoy.otshudi@gmail.com'
IMAP_PASS = 'lcnzrxtgtdljbsqc'
TELEGRAM_TOKEN = '8400529290:AAEyRzGa0JNCsuecpNJ6gXrqQQM8hnlt-ao'
TELEGRAM_CHAT = '1664221853'

def get_conn():
    return psycopg2.connect(host='localhost', port=5432, dbname='trading', user='trading', password='Trading2026')

def send_telegram(msg):
    requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                  json={'chat_id': TELEGRAM_CHAT, 'text': msg, 'parse_mode': 'Markdown'})

def extract(text, pattern):
    m = re.search(pattern, text, re.IGNORECASE)
    return m.group(1).strip() if m else None

def process_email(msg_id, body, subject):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT 1 FROM processed_emails_smc WHERE message_id=%s", (msg_id,))
    if cur.fetchone():
        cur.close(); conn.close(); return

    balance = extract(body, r'Current balance\s*\n?\s*USD\s*([\d.]+)')
    current_balance = float(balance) if balance else None

    if re.search(r'pending order filled', subject, re.I):
        cur.execute("INSERT INTO processed_emails_smc (message_id, subject) VALUES (%s,%s) ON CONFLICT DO NOTHING", (msg_id, subject))
        conn.commit(); cur.close(); conn.close(); return

    pos_id_raw = extract(body, r'Position ID\s*\n?\s*PID(\d+)')
    net_pnl_raw = extract(body, r'Net P&L\s*\n?\s*USD\s*(-?[\d.]+)')
    if not pos_id_raw or not net_pnl_raw:
        cur.close(); conn.close(); return

    position_id = int(pos_id_raw)
    net_pnl = float(net_pnl_raw)

    cur.execute("SELECT id, pnl_eur FROM trades_smc WHERE mt5_ticket=%s ORDER BY close_at DESC LIMIT 1", (position_id,))
    row = cur.fetchone()

    if row:
        trade_id, db_pnl = row
        db_pnl = float(db_pnl or 0)
        if abs(db_pnl - net_pnl) > 0.5:
            cur.execute("UPDATE trades_smc SET pnl_eur=%s WHERE id=%s", (net_pnl, trade_id))
            send_telegram(f"⚠️ *Écart PnL corrigé*\nPosition: {position_id}\nDB: {db_pnl}€ → Email: {net_pnl}€")

    if current_balance:
        cur.execute("""UPDATE capital_smc SET capital_actuel=%s,
            variation_pct=ROUND(((%s - capital_initial)/capital_initial*100)::numeric,3),
            updated_at=NOW() WHERE id=1""", (current_balance, current_balance))

    cur.execute("INSERT INTO processed_emails_smc (message_id, subject) VALUES (%s,%s) ON CONFLICT DO NOTHING", (msg_id, subject))
    conn.commit(); cur.close(); conn.close()

def main():
    imap = imaplib.IMAP4_SSL(IMAP_HOST)
    imap.login(IMAP_USER, IMAP_PASS)
    imap.select('INBOX')
    status, data = imap.search(None, 'FROM', '"alerts.ctrader.com"', 'UNSEEN')
    for num in data[0].split():
        status, msg_data = imap.fetch(num, '(RFC822)')
        msg = email.message_from_bytes(msg_data[0][1])
        msg_id = msg.get('Message-ID', '')
        subject = str(decode_header(msg['Subject'])[0][0])
        body = ''
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == 'text/plain':
                    body = part.get_payload(decode=True).decode(errors='ignore')
                    break
        else:
            body = msg.get_payload(decode=True).decode(errors='ignore')
        try:
            process_email(msg_id, body, subject)
            imap.store(num, '+FLAGS', '\\Seen')
        except Exception as e:
            print(f"Erreur email {msg_id}: {e}")
    imap.logout()

if __name__ == '__main__':
    main()
