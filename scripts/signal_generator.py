#!/usr/bin/env python3
"""
SMC Signal Generator
Remplace le workflow n8n "SMC - Signal Generator"
Tourne toutes les 5 minutes via cron système
"""

import psycopg2
from datetime import datetime, timedelta, timezone
import logging
import requests
from datetime import datetime, timezone

LOG_FILE = '/opt/deborah-trading/scripts/signal_generator.log'
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

DB_CONFIG = {
    'host': 'localhost', 'port': 5432,
    'dbname': 'trading', 'user': 'trading', 'password': 'Trading2026'
}

# Telegram — récupérer depuis la DB ou config
TELEGRAM_BOT_TOKEN = None  # sera lu depuis n8n credentials si besoin
TELEGRAM_CHAT_ID   = None

def get_first_friday(year, month):
    first_day = datetime(year, month, 1)
    days_until_friday = (4 - first_day.weekday()) % 7
    return first_day + timedelta(days=days_until_friday)

def is_nfp_window():
    """Fenetre NFP : 11h00-13h00 UTC le premier vendredi du mois"""
    now = datetime.now(timezone.utc)
    first_friday = get_first_friday(now.year, now.month)
    if now.date() != first_friday.date():
        return False
    return 11 <= now.hour < 13


def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def check_killzone():
    """Vérifie si on est dans une killzone London ou NY (heure Paris)"""
    import pytz
    paris = pytz.timezone('Europe/Paris')
    now_paris = datetime.now(paris)

    if now_paris.weekday() >= 5:  # weekend
        return {'in_killzone': False, 'session': 'WEEKEND'}

    h = now_paris.hour
    if 8 <= h < 11:
        return {'in_killzone': True,  'session': 'LONDON'}
    elif 13 <= h < 16:
        return {'in_killzone': True,  'session': 'NEW_YORK'}
    elif 16 <= h < 18:
        return {'in_killzone': True,  'session': 'LONDON_CLOSE'}
    else:
        return {'in_killzone': False, 'session': 'NONE'}

def send_telegram(message):
    """Envoie une alerte Telegram via API directe"""
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT value FROM config_smc WHERE name='telegram_bot_token' LIMIT 1")
        row = cur.fetchone()
        if not row:
            cur.close(); conn.close(); return
        token = row[0]
        cur.execute("SELECT value FROM config_smc WHERE name='telegram_chat_id' LIMIT 1")
        row2 = cur.fetchone()
        cur.close(); conn.close()
        if not row2: return
        requests.post(
            f'https://api.telegram.org/bot{token}/sendMessage',
            json={'chat_id': row2[0], 'text': message, 'parse_mode': 'Markdown'},
            timeout=5
        )
        import logging as _log
        _log.getLogger(__name__).info('Telegram envoye')
    except Exception as e:
        import logging as _log
        _log.getLogger(__name__).warning(f'Telegram erreur: {e}')

def run():
    try:
        conn = get_conn()
        cur  = conn.cursor()

        # 1. Vérifier killzone
        kz = check_killzone()
        if not kz['in_killzone']:
            logging.debug(f'Hors killzone: {kz["session"]}')
            return

        # 1b. Filtre NFP
        if is_nfp_window():
            logging.info('NFP window active (11h-13h UTC) — signal bloque')
            return

        # 2. Vérifier bot status
        cur.execute("SELECT bot_status, capital_actuel, risk_pct_current, daily_pnl_pct, consecutive_losses, session_bias FROM capital_smc WHERE id=1")
        cap = cur.fetchone()
        if not cap:
            logging.warning('Pas de données capital_smc')
            return

        bot_status, capital_actuel, risk_pct, daily_pnl, consec_losses, session_bias = cap
        if bot_status != 'ACTIVE':
            logging.info(f'Bot en pause: {bot_status}')
            return

        # 3. Circuit breaker
        if float(daily_pnl or 0) <= -3.5:
            logging.info(f'Circuit breaker: daily_pnl={daily_pnl}%')
            return
        if int(consec_losses or 0) >= 3:
            logging.info(f'Circuit breaker: {consec_losses} pertes consecutives — pause session')
            return
        # Filtre biais session
        bias_db = session_bias or "NEUTRE"
        if bias_db == "BEARISH":
            signal_type_allowed = "SELL_LIMIT"
        elif bias_db == "BULLISH":
            signal_type_allowed = "BUY_LIMIT"
        else:
            signal_type_allowed = None  # NEUTRE = les deux autorises


        # 4. Vérifier structure (confluence)
        cur.execute("""
            SELECT bias, h4_bias, confluence, zone_type, ote_low, ote_high,
                   eq50, liquidity_target, swing_high, swing_low
            FROM structure_smc
            ORDER BY updated_at DESC LIMIT 1
        """)
        struct = cur.fetchone()
        if not struct:
            logging.warning('Pas de structure SMC')
            return

        bias, h4_bias, confluence, zone_type, ote_low, ote_high, eq50, liq_target, swing_high, swing_low = struct

        if not bias or bias == 'NEUTRAL':
            logging.info('Biais neutre ou absent')
            return

        if not confluence:
            logging.info(f'Pas de confluence — H4: {h4_bias} / 1H: {bias}')
            return

        # 5. Vérifier pas de position ouverte
        cur.execute("SELECT COUNT(*) FROM signals_smc WHERE status IN ('pending','filled')")
        open_pos = cur.fetchone()[0]
        if open_pos > 0:
            logging.debug(f'Position déjà ouverte: {open_pos}')
            return

        # 6. Prix actuel
        cur.execute("SELECT close FROM prices_smc WHERE timeframe='M5' ORDER BY candle_time DESC LIMIT 1")
        price_row = cur.fetchone()
        if not price_row:
            logging.warning('Pas de prix 5M')
            return
        current_price = float(price_row[0])

        # 7. Récupérer PD Arrays valides
        direction = 'bullish' if bias == 'BULLISH' else 'bearish'
        cur.execute("""
            SELECT id, type, direction, price_high, price_low, price_eq, strength, combo
            FROM pd_arrays_smc
            WHERE status = 'active' AND touched = 0
            AND direction = %s
            AND price_high >= 0
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
            logging.info(f'Aucun PD Array valide ({direction})')
            return

        # 8. Trouver le meilleur PD Array où le prix est dedans
        buffer = 0.00030
        best_array = None
        for arr in pd_arrays:
            in_array = (
                current_price <= arr['price_high'] + buffer and
                current_price >= arr['price_low']  - buffer
            )
            if in_array:
                best_array = arr
                break

        if not best_array:
            logging.info(
                f'Prix hors PD Array — prix={current_price} '
                f'arrays={[(a["price_low"],a["price_high"]) for a in pd_arrays[:3]]}'
            )
            return

        # 10. Vérifier FVG première visite (IFVG)
        # 11. Calculer entry / SL / TP
        MIN_SL = 0.00100  # 10 pips minimum (protection spread + volatilite)
        sl_buffer = 0.00010

        if bias == 'BULLISH':
            entry = best_array['price_eq']
            sl    = round(best_array['price_low'] - sl_buffer, 5)
            if entry - sl < MIN_SL:
                sl = round(entry - MIN_SL, 5)
            tp = float(swing_high) if float(liq_target) < float(current_price) + 0.0015 else float(liq_target)
        else:
            entry = best_array['price_eq']
            sl    = round(best_array['price_high'] + sl_buffer, 5)
            if sl - entry < MIN_SL:
                sl = round(entry + MIN_SL, 5)
            tp = float(swing_high) if float(liq_target) < float(current_price) + 0.0015 else float(liq_target)

        # Validation TP
        if bias == 'BULLISH' and tp <= entry:
            logging.info(f'TP invalide pour BUY: tp={tp} entry={entry}')
            return
        if bias == 'BEARISH' and tp >= entry:
            logging.info(f'TP invalide pour SELL: tp={tp} entry={entry}')
            return

        # 11. Calculer RR
        risk   = abs(entry - sl)
        reward = abs(tp - entry)
        rr     = round(reward / risk, 2)

        if rr < 1.5:
            logging.info(f'RR insuffisant: {rr} (min 1.5)')
            return

        # 12. Calculer lot size
        capital     = float(capital_actuel)
        risk_pct_f  = float(risk_pct)
        risk_eur    = capital * risk_pct_f
        sl_pips     = round(risk * 10000, 1)
        pip_value   = 10
        lot_size    = round(risk_eur / (sl_pips * pip_value), 2)
        tp_pips     = round(reward * 10000, 1)

        signal_type = 'BUY_LIMIT' if bias == 'BULLISH' else 'SELL_LIMIT'
        # Bloquer si biais contraire
        if signal_type_allowed and signal_type != signal_type_allowed:
            logging.info(f'Signal {signal_type} bloque — biais {bias_db} autorise seulement {signal_type_allowed}')
            return

        # 13. Insérer le signal
        cur.execute("""
            INSERT INTO signals_smc
            (type, entry_price, sl_price, tp_price, lot_size, risk_pct,
             sl_pips, tp_pips, rr_ratio, pd_array_id, killzone, status, created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'pending',NOW())
            RETURNING id
        """, (
            signal_type, entry, sl, tp, lot_size, risk_pct_f,
            sl_pips, tp_pips, rr, best_array['id'], kz['session']
        ))
        signal_id = cur.fetchone()[0]

        # 14. Marquer le PD Array comme touché
        cur.execute(
            "UPDATE pd_arrays_smc SET touched = touched + 1 WHERE id = %s",
            (best_array['id'],)
        )

        conn.commit()

        msg = (
            f"🔴 SIGNAL SMC EUR/USD\n"
            f"Type: {signal_type}\n"
            f"Entry: {entry}\n"
            f"SL: {sl} ({sl_pips} pips)\n"
            f"TP: {tp} ({tp_pips} pips)\n"
            f"RR: 1:{rr}\n"
            f"Lots: {lot_size}\n"
            f"PD: {best_array['type']}\n"
            f"Session: {kz['session']}\n"
            f"Signal ID: {signal_id}"
        )

        logging.info(
            f"SIGNAL {signal_type} — entry={entry} sl={sl} tp={tp} "
            f"rr={rr} lots={lot_size} session={kz['session']} id={signal_id}"
        )

        # Envoyer Telegram direct
        send_telegram(msg)

    except Exception as e:
        logging.error(f'Erreur: {e}', exc_info=True)
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

# ==============================================================================
# ENGULFANT H4 — Détection et génération de signal MARKET
# Hugo pattern : sweep + clôture opposée + confirmation M5
# ==============================================================================

def get_candles_db(cur, timeframe: str, limit: int = 20) -> list:
    cur.execute("""
        SELECT candle_time, open, high, low, close
        FROM prices_smc
        WHERE timeframe = %s
        ORDER BY candle_time DESC
        LIMIT %s
    """, (timeframe, limit))
    rows = cur.fetchall()
    return list(reversed([{
        'time': r[0], 'open': float(r[1]), 'high': float(r[2]),
        'low': float(r[3]), 'close': float(r[4])
    } for r in rows]))


def detect_engulfing_h4(candles: list) -> dict | None:
    """
    Détecte un engulfant H4 optimisé récent (dernières 3 bougies).
    Bearish : sweep high + clôture sous low précédent
    Bullish : sweep low + clôture au-dessus du high précédent
    Retourne le setup ou None.
    """
    if len(candles) < 2:
        return None

    MIN_OB_SIZE = 0.0003  # 3 pips minimum

    # Cherche dans les 3 dernières bougies
    for i in range(max(1, len(candles) - 3), len(candles)):
        prev = candles[i - 1]
        curr = candles[i]

        prev_body_top = max(prev['open'], prev['close'])
        prev_body_bot = min(prev['open'], prev['close'])
        ob_size = prev_body_top - prev_body_bot

        if ob_size < MIN_OB_SIZE:
            continue

        prev_bull = prev['close'] > prev['open']
        curr_bear = curr['close'] < curr['open']
        prev_bear = prev['close'] < prev['open']
        curr_bull = curr['close'] > curr['open']

        # BEARISH engulfant optimisé
        if (prev_bull and curr_bear and
                curr['high'] > prev['high'] and
                curr['close'] < prev['low']):
            return {
                'direction': 'bearish',
                'time_4h': curr['time'],
                'ob_top': prev_body_top,
                'ob_bot': prev_body_bot,
                'tp_level': curr['low'],
                'engulf_high': curr['high'],
                'engulf_low': curr['low'],
            }

        # BULLISH engulfant optimisé
        elif (prev_bear and curr_bull and
              curr['low'] < prev['low'] and
              curr['close'] > prev['high']):
            return {
                'direction': 'bullish',
                'time_4h': curr['time'],
                'ob_top': prev_body_top,
                'ob_bot': prev_body_bot,
                'tp_level': curr['high'],
                'engulf_high': curr['high'],
                'engulf_low': curr['low'],
            }

    return None


def check_m5_confirmation(cur, setup: dict) -> dict | None:
    """
    Vérifie si le prix est dans l'OB H4 sur M5
    et si la dernière bougie M5 confirme la direction.
    Retourne les paramètres d'entrée ou None.
    """
    cur.execute("""
        SELECT candle_time, open, high, low, close
        FROM prices_smc
        WHERE timeframe = 'M5'
        AND candle_time > %s
        ORDER BY candle_time DESC
        LIMIT 5
    """, (setup['time_4h'],))
    rows = cur.fetchall()
    if not rows:
        return None

    candles_m5 = list(reversed([{
        'time': r[0], 'open': float(r[1]), 'high': float(r[2]),
        'low': float(r[3]), 'close': float(r[4])
    } for r in rows]))

    ob_top = setup['ob_top']
    ob_bot = setup['ob_bot']

    for c in candles_m5:
        in_ob = c['low'] <= ob_top and c['high'] >= ob_bot

        if not in_ob:
            continue

        if setup['direction'] == 'bearish' and c['close'] < c['open']:
            # Confirmation bearish
            entry = c['close']
            sl = round(c['high'] + 0.00005, 5)
            tp = round(setup['tp_level'], 5)
            sl_pips = round((sl - entry) * 10000, 1)
            tp_pips = round((entry - tp) * 10000, 1)
            if sl_pips <= 0 or tp_pips <= 0:
                continue
            rr = round(tp_pips / sl_pips, 2)
            if rr < 1.5:
                continue
            return {
                'signal_type': 'SELL',
                'entry': entry,
                'sl': sl,
                'tp': tp,
                'sl_pips': sl_pips,
                'tp_pips': tp_pips,
                'rr': rr,
                'confirm_time': c['time'],
            }

        elif setup['direction'] == 'bullish' and c['close'] > c['open']:
            # Confirmation bullish
            entry = c['close']
            sl = round(c['low'] - 0.00005, 5)
            tp = round(setup['tp_level'], 5)
            sl_pips = round((entry - sl) * 10000, 1)
            tp_pips = round((tp - entry) * 10000, 1)
            if sl_pips <= 0 or tp_pips <= 0:
                continue
            rr = round(tp_pips / sl_pips, 2)
            if rr < 1.5:
                continue
            return {
                'signal_type': 'BUY',
                'entry': entry,
                'sl': sl,
                'tp': tp,
                'sl_pips': sl_pips,
                'tp_pips': tp_pips,
                'rr': rr,
                'confirm_time': c['time'],
            }

    return None


def run_engulfing():
    """
    Détecte les engulfants H4 et génère des signaux MARKET (BUY/SELL).
    Tourne en parallèle du signal SMC classique.
    """
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()

        # 1. Vérifier killzone
        kz = check_killzone()
        if not kz['in_killzone']:
            logging.debug(f'Engulfing — hors killzone: {kz["session"]}')
            return

        # 2. Vérifier pas de position ouverte
        cur.execute("SELECT COUNT(*) FROM signals_smc WHERE status IN ('pending','filled')")
        if cur.fetchone()[0] > 0:
            logging.debug('Engulfing — position déjà ouverte')
            return

        # 3. Récupérer capital et risque
        cur.execute("""
            SELECT capital_actuel, risk_pct_current, bot_status
            FROM capital_smc WHERE id=1
        """)
        row = cur.fetchone()
        if not row or row[2] != 'ACTIVE':
            logging.debug(f'Engulfing — bot non actif: {row[2] if row else "N/A"}')
            return

        capital = float(row[0])
        risk_pct = float(row[1])
        risk_usd = capital * risk_pct

        # 4. Détecter engulfant H4
        candles_4h = get_candles_db(cur, '4H', 10)
        setup = detect_engulfing_h4(candles_4h)

        if not setup:
            logging.debug('Engulfing — aucun engulfant H4 détecté')
            return

        from datetime import datetime, timezone, timedelta
        max_age = datetime.now(timezone.utc) - timedelta(days=2)
        setup_time = setup["time_4h"].replace(tzinfo=timezone.utc) if setup["time_4h"].tzinfo is None else setup["time_4h"]
        if setup_time < max_age:
            logging.info(f'Engulfing — setup trop ancien ({setup["time_4h"]}), ignore')
            return
        logging.info(f'Engulfing — setup {setup["direction"]} détecté à {setup["time_4h"]}')

        # 5. Confirmation M5
        params = check_m5_confirmation(cur, setup)
        if not params:
            logging.debug('Engulfing — pas de confirmation M5')
            return

        # 6. Calculer lots
        sl_pips = params['sl_pips']
        pip_value = 10  # EUR/USD standard lot
        lot_size = round(risk_usd / (sl_pips * pip_value), 2)
        lot_size = max(0.01, min(lot_size, 10.0))

        entry = params['entry']
        sl = params['sl']
        tp = params['tp']
        rr = params['rr']
        signal_type = params['signal_type']

        # 7. Insérer le signal
        cur.execute("""
            INSERT INTO signals_smc
            (type, entry_price, sl_price, tp_price, lot_size, risk_pct,
             sl_pips, tp_pips, rr_ratio, killzone, status, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', NOW())
            RETURNING id
        """, (signal_type, entry, sl, tp, lot_size, risk_pct,
              sl_pips, params['tp_pips'], rr, kz['session'].upper()))

        signal_id = cur.fetchone()[0]
        conn.commit()

        # 8. Message Telegram
        direction_emoji = '🔴' if signal_type == 'SELL' else '🟢'
        msg = (
            f"{direction_emoji} *ENGULFANT H4 — {signal_type} EUR/USD*\n"
            f"Session: {kz['session']}\n"
            f"Entry: {entry}\n"
            f"SL: {sl} ({sl_pips} pips)\n"
            f"TP: {tp} ({params['tp_pips']} pips)\n"
            f"RR: {rr}\n"
            f"Lots: {lot_size}\n"
            f"Signal ID: {signal_id}"
        )
        send_telegram(msg)
        logging.info(
            f'ENGULFANT {signal_type} — entry={entry} sl={sl} tp={tp} '
            f'rr={rr} lots={lot_size} session={kz["session"]} id={signal_id}'
        )

    except Exception as e:
        logging.error(f'Erreur engulfing: {e}', exc_info=True)
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass


if __name__ == '__main__':
    run()
#    run_engulfing()  # Desactive — WR 20.9% backtest 3 ans
