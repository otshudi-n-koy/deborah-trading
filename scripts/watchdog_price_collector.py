import psycopg2, requests, os, json
from datetime import datetime, timezone

DB = dict(host="localhost", dbname="trading", user="trading", password=os.environ.get("PGPASSWORD", ""))
BOT_TOKEN = "8400529290:AAEyRzGa0JNCsuecpNJ6gXrqQQM8hnlt-ao"
CHAT_ID = "1664221853"
STATE_FILE = "/opt/deborah-trading/scripts/.watchdog_price_state.json"
THRESHOLD_MIN = 15
COOLDOWN_MIN = 60

def send_telegram(msg):
    requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                   json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"}, timeout=10)

def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {"last_alert": None}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

conn = psycopg2.connect(**DB)
cur = conn.cursor()
cur.execute("SELECT MAX(candle_time) FROM prices_smc WHERE timeframe = 'M5'")
last_candle = cur.fetchone()[0]
cur.close()
conn.close()

now = datetime.now(timezone.utc)

# Marche forex ferme le week-end : vendredi ~21h UTC -> dimanche ~21h UTC.
# On ignore silencieusement le watchdog dans cette fenetre pour eviter
# des fausses alertes chaque week-end (pas de nouvelle bougie = normal).
weekday = now.weekday()  # 0=lundi ... 4=vendredi, 5=samedi, 6=dimanche
is_weekend_closed = (
    weekday == 5  # samedi : toute la journee fermee
    or (weekday == 4 and now.hour >= 21)  # vendredi apres 21h UTC
    or (weekday == 6 and now.hour < 21)   # dimanche avant 21h UTC
)
if is_weekend_closed:
    print(f"{now.isoformat()} SKIP - marche ferme (week-end)")
    exit()

if last_candle is None:
    gap_min = 999999
else:
    if last_candle.tzinfo is None:
        last_candle = last_candle.replace(tzinfo=timezone.utc)
    gap_min = (now - last_candle).total_seconds() / 60

state = load_state()

if gap_min > THRESHOLD_MIN:
    last_alert = state.get("last_alert")
    can_alert = True
    if last_alert:
        last_alert_dt = datetime.fromisoformat(last_alert)
        can_alert = (now - last_alert_dt).total_seconds() / 60 >= COOLDOWN_MIN
    if can_alert:
        send_telegram(
            f"WATCHDOG - Collecteur de prix en panne\n\n"
            f"Derniere bougie M5 recue : {last_candle}\n"
            f"Retard : {gap_min:.0f} min (seuil {THRESHOLD_MIN} min)\n\n"
            f"Verifier ctrader_collector.py et le token OAuth."
        )
        state["last_alert"] = now.isoformat()
        save_state(state)
        print(f"{datetime.now(timezone.utc).isoformat()} ALERT SENT - gap {gap_min:.0f} min")
    else:
        print(f"{datetime.now(timezone.utc).isoformat()} Gap {gap_min:.0f} min but cooldown active, no alert")
else:
    if state.get("last_alert"):
        send_telegram(f"OK - Collecteur de prix retabli (derniere bougie il y a {gap_min:.0f} min)")
        state["last_alert"] = None
        save_state(state)
    print(f"{datetime.now(timezone.utc).isoformat()} OK - gap {gap_min:.0f} min")
