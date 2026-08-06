#!/usr/bin/env python3
"""
agent_event_watcher.py — Agent événementiel temps réel (05/08/2026)

Complète agent_pre_killzone.py (briefings périodiques à heures fixes) en
détectant et notifiant les changements significatifs ENTRE deux briefings,
au plus proche de l'événement (cron 1 min).

Principe : AUCUN LLM ici. Chaque événement est une comparaison déterministe
entre l'état actuel (lu en DB) et le dernier état connu (stocké dans
agent_event_state), avec un message Telegram templaté fixe. Zéro risque
d'hallucination sur des faits binaires (confluence vraie/fausse, statut
bot, etc.) — cohérent avec le principe retenu pour agent_pre_killzone.py.

Événements couverts :
1. Confluence H1/H4 qui bascule (True<->False)
2. Statut bot qui change (ACTIVE<->PAUSE, avec raison si disponible)
3. Streak de pertes : notifie chaque nouvelle perte a partir de 3
   consecutives, et systematiquement quand la serie se casse
4. CHoCH+BOS M5 nouvellement confirme (reutilise smc_choch_bos.py)
5. Nouvelle zone PD array proche du prix (meme direction que le biais)
6. Setup shadow BUY qualifie (passed_choch_bos_filter=True)
7. Fill confirme — filet de securite pour le cas ou position_monitor.py
   n'a pas notifie au premier essai (sync_position_id rate initialement,
   cf. investigation du 05/08/2026)
"""
import psycopg2
import requests
import sys
from datetime import datetime, timezone

sys.path.insert(0, '/opt/deborah-trading/scripts')
import smc_choch_bos

DB = dict(host='localhost', dbname='trading', user='trading', password='Trading2026')
TELEGRAM_TOKEN = "8400529290:AAEyRzGa0JNCsuecpNJ6gXrqQQM8hnlt-ao"
TELEGRAM_CHAT = "1664221853"

NEAR_PRICE_MARGIN = 0.00100  # 10 pips, pour detecter une "nouvelle zone proche"
STREAK_LOSS_ALERT_MIN = 3    # notifie a partir de 3 pertes consecutives


def send_telegram(msg):
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT, "text": msg, "parse_mode": "Markdown"}, timeout=10)
        if r.status_code != 200:
            print(f"ERREUR Telegram: HTTP {r.status_code} — {r.text[:200]}")
            return False
        return True
    except Exception as e:
        print(f"ERREUR Telegram (exception): {e}")
        return False


def get_state(cur, key):
    cur.execute("SELECT value FROM agent_event_state WHERE key=%s", (key,))
    row = cur.fetchone()
    return row[0] if row else None


def set_state(cur, key, value):
    cur.execute("""
        INSERT INTO agent_event_state (key, value, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
    """, (key, str(value)))


def check_confluence(cur):
    cur.execute("""
        SELECT bias, h4_bias, confluence, zone_type
        FROM structure_smc ORDER BY updated_at DESC LIMIT 1
    """)
    row = cur.fetchone()
    if not row:
        return
    bias, h4_bias, confluence, zone_type = row
    current = str(confluence)
    previous = get_state(cur, 'confluence')

    if previous is not None and previous != current:
        if confluence:
            send_telegram(
                f"🟢 *CONFLUENCE ETABLIE* — {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"
                f"H1={bias} | H4={h4_bias} | Zone={zone_type}\n"
                f"Le pipeline peut desormais evaluer des setups dans cette direction."
            )
        else:
            send_telegram(
                f"🔴 *CONFLUENCE ROMPUE* — {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"
                f"H1={bias} | H4={h4_bias}\n"
                f"Generation de signal suspendue jusqu'a realignement."
            )
    set_state(cur, 'confluence', current)


def check_bot_status(cur):
    cur.execute("SELECT bot_status, pause_reason FROM capital_smc WHERE id=1")
    row = cur.fetchone()
    if not row:
        return
    bot_status, pause_reason = row
    current = f"{bot_status}|{pause_reason or ''}"
    previous = get_state(cur, 'bot_status')

    if previous is not None and previous != current:
        if bot_status == 'PAUSE':
            reason_txt = f"\nRaison: {pause_reason}" if pause_reason else "\n(raison non precisee — hors killzone probable)"
            send_telegram(
                f"⏸️ *BOT EN PAUSE* — {datetime.now(timezone.utc).strftime('%H:%M')} UTC{reason_txt}"
            )
        else:
            send_telegram(
                f"▶️ *BOT ACTIF* — {datetime.now(timezone.utc).strftime('%H:%M')} UTC"
            )
    set_state(cur, 'bot_status', current)


def check_streak(cur):
    cur.execute("SELECT updated_at FROM config_smc WHERE name='ctrader_account_mode'")
    row_switch = cur.fetchone()
    account_switch_at = row_switch[0] if row_switch else None

    cur.execute("""
        SELECT result FROM trades_smc
        WHERE result IN ('WIN','LOSS') AND close_at > %s AND is_phantom IS NOT TRUE
        ORDER BY close_at DESC LIMIT 20
    """, (account_switch_at,))
    streak_rows = [r[0] for r in cur.fetchall()]
    if not streak_rows:
        return

    streak_type = streak_rows[0]
    streak_count = 0
    for r in streak_rows:
        if r == streak_type:
            streak_count += 1
        else:
            break

    current = f"{streak_type}|{streak_count}"
    previous = get_state(cur, 'streak')

    if previous is not None and previous != current:
        prev_type, prev_count = (previous.split('|') + ['', '0'])[:2]
        prev_count = int(prev_count) if prev_count.isdigit() else 0

        broke = (prev_type != streak_type) and prev_type != ''
        if broke and prev_type == 'LOSS' and int(prev_count) >= STREAK_LOSS_ALERT_MIN:
            send_telegram(
                f"✅ *SERIE DE PERTES CASSEE* — {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"
                f"Serie precedente: {prev_count} pertes consecutives. Nouveau resultat: WIN."
            )
        elif streak_type == 'LOSS' and streak_count >= STREAK_LOSS_ALERT_MIN and streak_count != prev_count:
            send_telegram(
                f"⚠️ *{streak_count} PERTES CONSECUTIVES* — {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"
                f"Vigilance recommandee."
            )
    set_state(cur, 'streak', current)


def check_choch_bos(cur):
    cur.execute("SELECT bias FROM structure_smc ORDER BY updated_at DESC LIMIT 1")
    row = cur.fetchone()
    bias = row[0] if row else None
    if bias not in ('BULLISH', 'BEARISH'):
        return

    cur.execute("""
        SELECT candle_time, open, high, low, close
        FROM prices_smc WHERE timeframe='M5'
        ORDER BY candle_time DESC LIMIT 100
    """)
    rows = cur.fetchall()
    m5_window = list(reversed([
        {'candle_time': r[0], 'open': float(r[1]), 'high': float(r[2]),
         'low': float(r[3]), 'close': float(r[4])} for r in rows
    ]))
    if len(m5_window) < 90:
        return

    facts = smc_choch_bos.get_choch_bos_sweep_facts(m5_window, bias)
    current = f"{facts['choch_detected']}|{facts['bos_confirmed']}|{facts.get('choch_level')}"
    previous = get_state(cur, 'choch_bos_m5')

    if previous is not None and previous != current and facts['choch_detected'] and facts['bos_confirmed']:
        prev_choch, prev_bos = (previous.split('|') + ['False', 'False'])[:2]
        if not (prev_choch == 'True' and prev_bos == 'True'):
            level_txt = f" (niveau {facts['choch_level']:.5f})" if facts['choch_level'] else ""
            sweep_txt = " + liquidity sweep" if facts['liquidity_sweep_detected'] else ""
            send_telegram(
                f"🔎 *CHoCH+BOS CONFIRMES M5* — {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"
                f"Direction: {bias}{level_txt}{sweep_txt}\n"
                f"Structure de retournement M5 confirmee — a surveiller."
            )
    set_state(cur, 'choch_bos_m5', current)


def check_new_pd_array_near_price(cur):
    cur.execute("SELECT bias FROM structure_smc ORDER BY updated_at DESC LIMIT 1")
    row = cur.fetchone()
    bias = row[0] if row else None
    if bias not in ('BULLISH', 'BEARISH'):
        return
    direction = 'bullish' if bias == 'BULLISH' else 'bearish'

    cur.execute("SELECT close FROM prices_smc WHERE timeframe='M5' ORDER BY candle_time DESC LIMIT 1")
    price_row = cur.fetchone()
    if not price_row:
        return
    price = float(price_row[0])

    cur.execute("""
        SELECT id, type, price_high, price_low, price_eq
        FROM pd_arrays_smc
        WHERE status='active' AND touched=0 AND direction=%s
        AND timeframe='M5'
        AND price_eq BETWEEN %s AND %s
        ORDER BY id DESC LIMIT 1
    """, (direction, price - NEAR_PRICE_MARGIN, price + NEAR_PRICE_MARGIN))
    row = cur.fetchone()
    if not row:
        return
    arr_id, arr_type, arr_high, arr_low, arr_eq = row
    arr_high, arr_low, arr_eq = float(arr_high), float(arr_low), float(arr_eq)

    last_notified_id = get_state(cur, 'last_pd_array_near_id')
    if last_notified_id is None or int(last_notified_id) < arr_id:
        dist_pips = round(abs(arr_eq - price) * 10000, 1)
        cur.execute("""
            INSERT INTO agent_watched_zones (pd_array_id, notified_at, dist_pips_at_notification)
            VALUES (%s, NOW(), %s)
            ON CONFLICT (pd_array_id) DO NOTHING
        """, (arr_id, dist_pips))
        send_telegram(
            f"📍 *NOUVELLE ZONE PROCHE* — {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"
            f"{arr_type} {direction} — HIGH={arr_high:.5f} LOW={arr_low:.5f}\n"
            f"~{dist_pips}p du prix actuel ({price:.5f})"
        )
        set_state(cur, 'last_pd_array_near_id', arr_id)


def check_shadow_buy_qualified(cur):
    last_id = get_state(cur, 'last_shadow_qualified_id')
    last_id_val = int(last_id) if last_id else 0

    cur.execute("""
        SELECT id, entry_price, sl_price, tp_price, rr_ratio, choch_level, killzone
        FROM signals_shadow
        WHERE passed_choch_bos_filter = true AND id > %s
        ORDER BY id ASC
    """, (last_id_val,))
    rows = cur.fetchall()
    if not rows:
        return

    for r in rows:
        sig_id, entry, sl, tp, rr, choch_level, killzone = r
        send_telegram(
            f"🟡 *SETUP SHADOW BUY QUALIFIE* — {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"
            f"CHoCH+BOS confirmes — Entry={float(entry):.5f} SL={float(sl):.5f} TP={float(tp):.5f}\n"
            f"RR={rr} | Session={killzone}\n"
            f"(observation seule, BUY reste suspendu en execution)"
        )
        set_state(cur, 'last_shadow_qualified_id', sig_id)


def check_delayed_fills(cur):
    """
    Filet de securite : signaux passes en 'filled' avec mt5_ticket confirme,
    mais dont sync_attempts > 0 suggere que la notification initiale dans
    position_monitor.py a pu etre ratee (sync_position_id echouee au 1er
    essai). Notifie une fois par signal_id, meme si redondant avec un envoi
    initial reussi — la redondance est preferable a un silence sur un fill reel.
    """
    last_id = get_state(cur, 'last_fill_notified_id')
    last_id_val = int(last_id) if last_id else 0

    cur.execute("""
        SELECT id, type, entry_price, sl_price, tp_price, lot_size, sync_attempts
        FROM signals_smc
        WHERE status='filled' AND mt5_ticket IS NOT NULL
        AND sync_attempts > 0 AND id > %s
        ORDER BY id ASC
    """, (last_id_val,))
    rows = cur.fetchall()
    if not rows:
        return

    for r in rows:
        sig_id, sig_type, entry, sl, tp, lots, sync_attempts = r
        send_telegram(
            f"🔔 *FILL CONFIRME (rattrapage)* — {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"
            f"Signal #{sig_id} — {sig_type} EUR/USD\n"
            f"Entry={float(entry):.5f} SL={float(sl):.5f} TP={float(tp):.5f} Lots={lots}\n"
            f"Sync reussie apres {sync_attempts} tentative(s) — notification initiale possiblement manquee."
        )
        set_state(cur, 'last_fill_notified_id', sig_id)


def check_zone_invalidations(cur):
    """
    Suivi des zones precedemment notifiees (agent_watched_zones) : notifie
    quand l'une d'elles passe a 'invalidated', avec la duree de vie observee
    depuis la notification et la raison si deductible (prix qui l'a
    traversee = mitigation reelle, vs simplement expiree/remplacee sans
    jamais avoir ete testee par le prix).
    Nettoie l'entree une fois traitee (qu'elle soit invalidee ou toujours
    active, pour eviter que la table grossisse indefiniment - watch limite
    a 48h par zone, au-dela on arrete de suivre silencieusement).
    """
    cur.execute("""
        SELECT w.pd_array_id, w.notified_at, w.dist_pips_at_notification,
               p.status, p.type, p.direction, p.invalidated_at,
               p.price_high, p.price_low
        FROM agent_watched_zones w
        JOIN pd_arrays_smc p ON p.id = w.pd_array_id
    """)
    rows = cur.fetchall()
    if not rows:
        return

    cur.execute("SELECT close FROM prices_smc WHERE timeframe='M5' ORDER BY candle_time DESC LIMIT 1")
    price_row = cur.fetchone()
    current_price = float(price_row[0]) if price_row else None

    for r in rows:
        (arr_id, notified_at, dist_at_notif, status, arr_type, direction,
         invalidated_at, price_high, price_low) = r

        age_min = None
        if notified_at:
            age_min = round((datetime.now(timezone.utc).replace(tzinfo=None) - notified_at).total_seconds() / 60)

        if status == 'invalidated':
            price_high, price_low = float(price_high), float(price_low)
            # Deduction simple : si le prix actuel est DANS la zone (ou tres
            # proche), l'invalidation vient probablement d'une mitigation
            # reelle (le prix l'a traversee) ; sinon, plus probablement
            # remplacee/expiree sans avoir ete testee.
            reason = "raison inconnue"
            if current_price is not None:
                if price_low - 0.0003 <= current_price <= price_high + 0.0003:
                    reason = "mitigee (prix a traverse la zone)"
                else:
                    reason = "expiree sans etre testee par le prix"

            send_telegram(
                f"⚫ *ZONE INVALIDEE* — {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"
                f"{arr_type} {direction} — etait a {dist_at_notif}p a sa detection\n"
                f"Duree de vie: {age_min} min | {reason}"
            )
            cur.execute("DELETE FROM agent_watched_zones WHERE pd_array_id=%s", (arr_id,))
        elif age_min is not None and age_min > 48 * 60:
            # Watch expire (48h), on arrete silencieusement le suivi
            cur.execute("DELETE FROM agent_watched_zones WHERE pd_array_id=%s", (arr_id,))


def run():
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    try:
        check_confluence(cur)
        check_bot_status(cur)
        check_streak(cur)
        check_choch_bos(cur)
        check_new_pd_array_near_price(cur)
        check_zone_invalidations(cur)
        check_shadow_buy_qualified(cur)
        check_delayed_fills(cur)
        conn.commit()
        print("Done")
    except Exception as e:
        conn.rollback()
        print(f"Erreur: {e}")
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    run()
