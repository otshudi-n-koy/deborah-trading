#!/usr/bin/env python3
"""
Position Monitor - SMC
Tourne toutes les minutes via cron
TP partiels : 6 paliers en pips fixes (10/20/35/55/80/TP final)
Lots : 20/20/20/20/15/5%
exit_reason tracé dans trades_smc
"""

import psycopg2
import requests
import logging
# cTrader executor — integration automatique
try:
    import sys as _sys
    _sys.path.insert(0, '/opt/deborah-trading/scripts')
    import ctrader_executor_v2 as ctrader_ex
    CTRADER_ENABLED = True
except Exception as _e:
    CTRADER_ENABLED = False
    logging.getLogger('pm').warning(f'cTrader executor non disponible: {_e}')
import os
from datetime import datetime, timezone

LOG_FILE = '/opt/deborah-trading/scripts/position_monitor.log'

logger = logging.getLogger('pm')
logger.setLevel(logging.INFO)
if not logger.handlers:
    fh = logging.FileHandler(LOG_FILE)
    fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logger.addHandler(fh)

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME", "trading"),
    "user": os.getenv("DB_USER", "trading"),
    "password": os.getenv("DB_PASS", "Trading2026"),
}

TIMEOUT_HOURS = 8

# Parametres CHoCH breakeven
CHOCH_MAX_DIST_PIPS = 15
CHOCH_MIN_DELAY_CANDLES = 1
CHOCH_WINDOW = 8

# TP partiels en pips fixes depuis l'entry
# Bases sur l'analyse historique des trades (max pips observes : 36p courant, 76p exceptionnel)
# Revision prevue : apres 50 trades (Phase 3)
# Palier : (pips, % lots a fermer)
# TP_PALIERS desactives sur le compte experimental (05/07/2026) pour tester
# la strategie "pure" sans securisation psychologique de gains partiels.
# Backtest historique montrait deja que le baseline (TP unique) surperforme
# statistiquement les configurations avec partiels. Liste vide = 100% des
# lots visent directement le TP final unique.
TP_PALIERS = [
    # Reactiver les paliers ci-dessous si retour au vrai challenge E8 :
    # (8,   0.30),  # TP1 : +8p  — couvre le SL, securisation immediate (30%)
    # (15,  0.25),  # TP2 : +15p — zone la plus frequente (25%)
    # (25,  0.20),  # TP3 : +25p — max observe sur plupart des trades (20%)
    # (40,  0.15),  # TP4 : +40p — rare mais atteint (15%)
    # (70,  0.08),  # TP5 : +70p — tres rare, trades exceptionnels (8%)
]
PIP = 0.0001  # 1 pip EURUSD


def detect_choch_m5(candles, direction, window=8):
    if len(candles) < 3:
        return None, False
    search = candles[-window:] if len(candles) >= window else candles
    for i in range(1, len(search) - 1):
        prev = search[i - 1]
        curr = search[i]
        nxt  = search[i + 1]
        if direction == 'short':
            if curr['low'] < prev['low'] and nxt['close'] > prev['high']:
                return curr['low'], True
        else:
            if curr['high'] > prev['high'] and nxt['close'] < prev['low']:
                return curr['high'], True
    return None, False


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def send_telegram(message):
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
        resp = requests.post(
            f'https://api.telegram.org/bot{token}/sendMessage',
            json={'chat_id': row2[0], 'text': message},
            timeout=5
        )
        if resp.status_code != 200:
            logger.error(f'Telegram echec envoi (status={resp.status_code}): {resp.text}')
    except Exception as e:
        logger.warning(f'Telegram non disponible: {e}')


def close_position(cur, conn, sig_id, entry, exit_price, lots, pnl,
                   sig_type, killzone, result_label, exit_reason='', breakeven_triggered=None):
    # Garde-fou idempotence : empeche un double traitement si une autre branche
    # (ex: detection fermeture manuelle) a deja cloture ce signal DANS LE MEME
    # cycle cron, avant que la boucle principale HIT_TP/HIT_SL ne le traite aussi
    # sur la base de sa liste de signaux chargee en memoire en debut de script.
    # Bug decouvert le 06/07/2026 : signal #78 double-insere (trades_smc id 104+105),
    # capital_actuel double-compte (+2.64 en trop).
    cur.execute("SELECT status FROM signals_smc WHERE id=%s", (sig_id,))
    _current_status = cur.fetchone()
    if _current_status and _current_status[0] == 'closed':
        logger.warning(f"Signal {sig_id} deja ferme (statut='closed') — close_position() ignoree pour eviter un doublon")
        return
    pnl_pct = round(pnl / 10000 * 100, 3)

    # === Etape 1 : tentative de fermeture reelle sur cTrader AVANT tout commit DB ===
    real_close_confirmed = True
    pos_id_to_check = None
    if CTRADER_ENABLED:
        real_close_confirmed = False
        try:
            import psycopg2 as _pg
            _c=_pg.connect(host="localhost",port=5432,dbname="trading",user="trading",password="Trading2026")
            _cu=_c.cursor()
            _cu.execute("SELECT mt5_ticket, lot_size FROM signals_smc WHERE id=%s",(sig_id,))
            _r=_cu.fetchone(); _cu.close(); _c.close()
            if _r and _r[0]:
                pos_id_to_check = int(_r[0])
                import subprocess as _sp2, json as _json2
                _close_proc = _sp2.run(
                    ['python3', '/opt/deborah-trading/scripts/close_position_standalone.py',
                     str(pos_id_to_check), str(float(_r[1]))],
                    capture_output=True, text=True, timeout=35)
                _close_lines = [l for l in _close_proc.stdout.strip().split('\n') if l.strip()]
                try:
                    _close_result = _json2.loads(_close_lines[-1]) if _close_lines else {'success': False, 'error': 'no output'}
                except Exception:
                    _close_result = {'success': False, 'error': f'unparsable output: {_close_proc.stdout[-200:]}'}
                logger.info(f"Signal {sig_id} demande de fermeture envoyee cTrader pos={pos_id_to_check} - resultat: {_close_result}")
                import time as _time
                _time.sleep(3)
                try:
                    import subprocess as _sp
                    _check = _sp.run(
                        ['python3', '/opt/deborah-trading/scripts/check_close_confirmed.py', str(pos_id_to_check)],
                        capture_output=True, timeout=30
                    )
                    if _check.returncode == 0:
                        real_close_confirmed = True
                        logger.info(f"Signal {sig_id} fermeture confirmee (pos absente des positions ouvertes)")
                    elif _check.returncode == 1:
                        logger.error(f"Signal {sig_id} fermeture NON confirmee - position {pos_id_to_check} toujours ouverte")
                    else:
                        logger.error(f"Signal {sig_id} verification fermeture erreur (subprocess code={_check.returncode}): {_check.stderr.decode(errors='replace')[:200]}")
                except Exception as _ve:
                    logger.error(f"Signal {sig_id} verification fermeture erreur: {_ve}")
            else:
                real_close_confirmed = True
        except Exception as _ce:
            logger.error(f"Signal {sig_id} close cTrader erreur: {_ce}")

    if not real_close_confirmed:
        cur.execute(
            "UPDATE signals_smc SET status='close_failed' WHERE id=%s",
            (sig_id,)
        )
        conn.commit()
        logger.error(f"Signal {sig_id} marque close_failed - monitoring continue, intervention manuelle possible")
        send_telegram(
            f"ECHEC FERMETURE - Signal #{sig_id}\nLa fermeture cTrader n'a pas pu etre confirmee.\nPosition potentiellement toujours ouverte sur le broker.\nVerification manuelle recommandee."
        )
        return None

    cur.execute(
        "UPDATE signals_smc SET status='closed', closed_at=NOW() WHERE id=%s",
        (sig_id,)
    )
    cur.execute("""
        INSERT INTO trades_smc
        (signal_id, open_price, close_price, lot_size, pnl_eur, pnl_pct,
         result, killzone, open_at, close_at, exit_reason, breakeven_triggered)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,
            (SELECT filled_at FROM signals_smc WHERE id=%s), NOW(), %s, %s)
        RETURNING id
    """, (sig_id, entry, exit_price, lots, round(pnl, 2), pnl_pct,
          result_label, killzone, sig_id, exit_reason, breakeven_triggered))
    trade_id = cur.fetchone()[0]
    cur.execute("""
        UPDATE capital_smc SET
            capital_actuel = capital_actuel + %s,
            daily_pnl_pct  = daily_pnl_pct + %s,
            consecutive_losses = CASE WHEN %s < 0
                THEN consecutive_losses + 1 ELSE 0 END,
            updated_at = NOW()
        WHERE id = 1
    """, (round(pnl, 2), pnl_pct, pnl))
    conn.commit()
    import subprocess
    subprocess.Popen(['python3', '/root/agent_post_trade.py', str(trade_id)])

    if CTRADER_ENABLED and pos_id_to_check:
        try:
            _pnl_sync = ctrader_ex.sync_real_pnl(sig_id, trade_id)
            if _pnl_sync:
                logger.info(f"Signal {sig_id} PnL reel synchronise: {_pnl_sync['pnl_net']}EUR")
            else:
                logger.warning(f"Signal {sig_id} sync_real_pnl: pas de closePositionDetail (deal pas encore visible?) — pnl_reconciler.py retentera")
        except Exception as _pe:
            logger.error(f"Signal {sig_id} sync_real_pnl erreur: {_pe}")

    return pnl_pct
def close_partial(cur, conn, sig_id, entry, exit_price, lots_closed, pnl,
                  sig_type, killzone, palier_label, lots_remaining, breakeven_triggered=None):
    pnl_pct = round(pnl / 10000 * 100, 3)
    cur.execute("""
        INSERT INTO trades_smc
        (signal_id, open_price, close_price, lot_size, pnl_eur, pnl_pct,
         result, killzone, open_at, close_at, exit_reason, breakeven_triggered)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,
            (SELECT filled_at FROM signals_smc WHERE id=%s), NOW(), %s, %s)
        RETURNING id
    """, (sig_id, entry, exit_price, round(lots_closed, 2), round(pnl, 2),
          pnl_pct, palier_label, killzone, sig_id, palier_label, breakeven_triggered))
    trade_id = cur.fetchone()[0]
    cur.execute("UPDATE signals_smc SET lot_size=%s WHERE id=%s",
                (round(lots_remaining, 2), sig_id))
    cur.execute("""
        UPDATE capital_smc SET
            capital_actuel = capital_actuel + %s,
            daily_pnl_pct  = daily_pnl_pct + %s,
            updated_at = NOW()
        WHERE id = 1
    """, (round(pnl, 2), pnl_pct))
    conn.commit()
    logger.info(f'Signal {sig_id} {palier_label} — exit={exit_price} '
                f'lots_closed={lots_closed:.2f} lots_rem={lots_remaining:.2f} '
                f'pnl={pnl:.2f}EUR')
    return pnl_pct, trade_id


def get_tp_palier(cur, sig_id):
    cur.execute("SELECT tp_palier FROM signals_smc WHERE id=%s", (sig_id,))
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def set_tp_palier(cur, conn, sig_id, palier):
    cur.execute("UPDATE signals_smc SET tp_palier=%s WHERE id=%s", (palier, sig_id))
    conn.commit()


def get_breakeven_fired(cur, sig_id):
    """
    Lit le flag breakeven_fired pose a l'evenement (voir bloc TRAILING/BREAKEVEN
    dans run()), plutot que de le rededuire a la cloture depuis la position du SL
    (proxy heuristique casse par le trailing qui deplace le SL apres le breakeven —
    cf. diagnostic du 15/08/2026, breakeven_triggered toujours false en base malgre
    des breakevens confirmes dans les logs).
    """
    cur.execute("SELECT breakeven_fired FROM signals_smc WHERE id=%s", (sig_id,))
    row = cur.fetchone()
    return bool(row[0]) if row and row[0] is not None else False


def run():
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        logger.info("heartbeat")

        # 0. Fermeture vendredi
        now_utc = datetime.now(timezone.utc)
        if now_utc.weekday() == 4 and now_utc.hour >= 21:
            cur.execute("SELECT id, type, entry_price, sl_price, tp_price, lot_size, killzone, sl_pips FROM signals_smc WHERE status='filled'")
            for sig in cur.fetchall():
                sig_id, sig_type, entry, sl, tp, lots, killzone, sl_pips_original = sig
                entry = float(entry); lots = float(lots)
                cur.execute("SELECT close FROM prices_smc WHERE timeframe='M5' ORDER BY candle_time DESC LIMIT 1")
                price_row = cur.fetchone()
                if not price_row: continue
                exit_price = float(price_row[0])
                pnl = (float(entry) - exit_price) * lots * 100000 if sig_type in ('SELL_LIMIT','SELL') \
                      else (exit_price - float(entry)) * lots * 100000
                pnl_pct = round(pnl / 10000 * 100, 3)
                _be_triggered_friday = get_breakeven_fired(cur, sig_id)
                cur.execute("UPDATE signals_smc SET status='closed', closed_at=NOW() WHERE id=%s", (sig_id,))
                cur.execute("""
                    INSERT INTO trades_smc (signal_id, open_price, close_price, lot_size, pnl_eur, pnl_pct,
                    result, killzone, open_at, close_at, exit_reason, breakeven_triggered)
                    VALUES (%s,%s,%s,%s,%s,%s,'FRIDAY_CLOSE',%s,
                    (SELECT filled_at FROM signals_smc WHERE id=%s),NOW(),'FRIDAY_CLOSE',%s)
                """, (sig_id, entry, exit_price, lots, round(pnl,2), pnl_pct, killzone, sig_id, _be_triggered_friday))
                cur.execute("UPDATE capital_smc SET capital_actuel=capital_actuel+%s, daily_pnl_pct=daily_pnl_pct+%s, updated_at=NOW() WHERE id=1",
                            (round(pnl,2), pnl_pct))
                conn.commit()
                sign = '+' if pnl >= 0 else ''
                logger.info(f'Signal {sig_id} FRIDAY_CLOSE — exit={exit_price} pnl={pnl:.2f}EUR')
                send_telegram(f"VENDREDI CLOSE — {sig_type} EUR/USD\nEntry: {entry} Exit: {exit_price}\nPnL: {sign}{pnl:.2f}EUR\nID: {sig_id}")
            return

        # 1. Expirer signaux PENDING (annuler cote cTrader avant de committer)
        cur.execute("""
            SELECT id, killzone, ctrader_order_id FROM signals_smc
            WHERE status='pending' AND (
                -- Fenetres elargies le 17/07/2026 : le patch d'invalidation OB (position_monitor.py,
                -- bloc "1bis") protege deja contre les zones devenues caduques pendant l'attente,
                -- donc on peut laisser plus de temps au marche de revenir sur le niveau
                -- sans risquer un fill sur premisse obsolete.
                (killzone='LONDON'   AND created_at < NOW() - INTERVAL '48 hours')
                OR (killzone='NEW_YORK' AND created_at < NOW() - INTERVAL '48 hours')
                OR (created_at < NOW() - INTERVAL '48 hours')
            )
        """)
        to_expire = cur.fetchall()
        for exp_id, exp_killzone, exp_order_id in to_expire:
            if exp_order_id and CTRADER_ENABLED:
                import subprocess, json as _json
                try:
                    _proc = subprocess.run(
                        ['python3', '/opt/deborah-trading/scripts/cancel_order_standalone.py', str(exp_order_id)],
                        capture_output=True, text=True, timeout=35)
                    _lines = [l for l in _proc.stdout.strip().split('\n') if l.strip()]
                    _res = _json.loads(_lines[-1]) if _lines else {'success': False, 'error': 'no output'}
                except Exception as _ce:
                    _res = {'success': False, 'error': str(_ce)}
                if _res.get('success'):
                    logger.info(f'Signal {exp_id} ordre {exp_order_id} annule cote cTrader')
                else:
                    logger.warning(f'Signal {exp_id} echec annulation ordre {exp_order_id}: {_res.get("error")}')
            cur.execute("UPDATE signals_smc SET status='expired', closed_at=NOW() WHERE id=%s", (exp_id,))
            logger.info(f'Signal {exp_id} expire ({exp_killzone})')
        conn.commit()

        # 1bis. Annuler signaux PENDING dont la zone pd_array sous-jacente est invalidee
        # (race condition : la zone etait active a la creation du signal mais s'est
        # invalidee pendant que l'ordre LIMIT restait en attente de fill)
        cur.execute("""
            SELECT s.id, s.ctrader_order_id, p.id, p.status, s.entry_price
            FROM signals_smc s
            JOIN pd_arrays_smc p ON s.pd_array_id = p.id
            WHERE s.status='pending' AND p.status != 'active'
        """)
        to_cancel_ob = cur.fetchall()
        for cxl_id, cxl_order_id, cxl_pd_id, cxl_pd_status, cxl_entry_price in to_cancel_ob:
            if cxl_order_id and CTRADER_ENABLED:
                import subprocess as _sp4, json as _json4
                try:
                    _proc4 = _sp4.run(
                        ['python3', '/opt/deborah-trading/scripts/cancel_order_standalone.py', str(cxl_order_id)],
                        capture_output=True, text=True, timeout=35)
                    _lines4 = [l for l in _proc4.stdout.strip().split('\n') if l.strip()]
                    _res4 = _json4.loads(_lines4[-1]) if _lines4 else {'success': False, 'error': 'no output'}
                except Exception as _ce4:
                    _res4 = {'success': False, 'error': str(_ce4)}
                if _res4.get('success'):
                    logger.info(f'Signal {cxl_id} ordre {cxl_order_id} annule cote cTrader (pd_array {cxl_pd_id} status={cxl_pd_status})')
                    cur.execute("UPDATE signals_smc SET status='expired', closed_at=NOW() WHERE id=%s", (cxl_id,))
                    logger.info(f'Signal {cxl_id} annule - pd_array {cxl_pd_id} devenu {cxl_pd_status} pendant attente fill')
                    send_telegram(f"ORDRE ANNULE - Signal #{cxl_id}\nZone pd_array sous-jacente invalidee pendant l'attente (status={cxl_pd_status}).\nOrdre LIMIT annule pour eviter une entree sur premisse caduque.")
                else:
                    # RACE CONDITION decouverte le 23/07/2026 (signal #91) : l'annulation
                    # peut echouer ("Order not found") parce que l'ordre s'est REMPLI
                    # entre-temps (devenu une position, plus un ordre pending). Avant de
                    # marquer expired a tort, verifier via sync_position_id si une vraie
                    # position existe - sinon elle reste invisible du monitoring.
                    _real_pos_id = None
                    try:
                        _real_pos_id = ctrader_ex.sync_position_id(cxl_id)
                    except Exception as _se:
                        logger.error(f'Signal {cxl_id} verification fill (sync_position_id) erreur: {_se}')
                    if _real_pos_id:
                        # fill_price approxime avec entry_price (ordre LIMIT : ecart typiquement
                        # <1 pip constate sur signal #91). Necessaire pour que breakeven_triggered
                        # soit calculable a la cloture (bug decouvert le 24/07/2026 sur signal #92 :
                        # UPDATE precedent ne renseignait pas fill_price, cassant silencieusement
                        # le calcul cote reconciliation de cloture).
                        cur.execute("UPDATE signals_smc SET status='filled', filled_at=NOW(), fill_price=%s, mt5_ticket=%s WHERE id=%s", (cxl_entry_price, _real_pos_id, cxl_id))
                        logger.warning(f'Signal {cxl_id} en fait REMPLI (position {_real_pos_id}) malgre pd_array invalide - reconcilie comme filled, pas expire')
                        send_telegram(f"ATTENTION - Signal #{cxl_id}\nOrdre rempli juste avant tentative d'annulation (position {_real_pos_id}).\npd_array {cxl_pd_id} invalidee mais position reelle ouverte - reconciliee automatiquement, surveillance active.")
                    else:
                        logger.warning(f'Signal {cxl_id} echec annulation ordre {cxl_order_id} (pd_array invalide), aucune position trouvee: {_res4.get("error")}')
                        cur.execute("UPDATE signals_smc SET status='expired', closed_at=NOW() WHERE id=%s", (cxl_id,))
                        logger.info(f'Signal {cxl_id} annule - pd_array {cxl_pd_id} devenu {cxl_pd_status} pendant attente fill')
                        send_telegram(f"ORDRE ANNULE - Signal #{cxl_id}\nZone pd_array sous-jacente invalidee pendant l'attente (status={cxl_pd_status}).\nOrdre LIMIT annule pour eviter une entree sur premisse caduque.")
        conn.commit()

        # 2. Prix actuel
        cur.execute("SELECT close, high, low FROM prices_smc WHERE timeframe='M5' ORDER BY candle_time DESC LIMIT 1")
        row = cur.fetchone()
        if not row:
            logger.warning('Pas de prix disponible')
            return
        price = float(row[0])
        price_high = float(row[1])
        price_low = float(row[2])

        # 2b. Circuit breaker
        cur.execute("SELECT daily_pnl_pct, capital_actuel, capital_initial FROM capital_smc WHERE id=1")
        cap_row = cur.fetchone()
        if cap_row:
            daily_pnl_pct = float(cap_row[0] or 0)
            capital_actuel = float(cap_row[1] or 10000)
            capital_initial = float(cap_row[2] or 10000)
            # DECOUPLAGE 04/08/2026 : bascule de capital_smc.session_bias (LLM,
            # asynchrone) vers structure_smc.bias (deterministe, structure_analyzer.py)
            cur.execute("SELECT bias FROM structure_smc ORDER BY updated_at DESC LIMIT 1")
            _struct_bias_row = cur.fetchone()
            session_bias_val = (_struct_bias_row[0] if _struct_bias_row else None) or "NEUTRAL"
            dd_total_pct = (capital_initial - capital_actuel) / capital_initial * 100 \
                           if capital_actuel < capital_initial else 0
            if daily_pnl_pct <= -4.0:
                logger.warning(f'CIRCUIT BREAKER DD JOUR: {daily_pnl_pct}%')
                send_telegram(f"CIRCUIT BREAKER DD JOURNALIER\nDD jour: {daily_pnl_pct}%\nToutes positions fermees.")
                cur.execute("SELECT id, type, entry_price, lot_size, killzone, sl_price, sl_pips FROM signals_smc WHERE status='filled'")
                for sig in cur.fetchall():
                    sig_id2, sig_type2, s_entry, s_lots, s_kz, s_sl, s_sl_pips = sig
                    s_entry = float(s_entry); s_lots = float(s_lots)
                    pnl_cb = (price - s_entry)*s_lots*100000 if sig_type2 in ('BUY_LIMIT','BUY') \
                             else (s_entry - price)*s_lots*100000
                    _be_triggered_cb = get_breakeven_fired(cur, sig_id2)
                    close_position(cur, conn, sig_id2, s_entry, price, s_lots, pnl_cb, sig_type2, s_kz,
                                   'WIN' if pnl_cb > 0 else 'LOSS', 'CIRCUIT_BREAKER', _be_triggered_cb)
                cur.execute("UPDATE capital_smc SET bot_status='PAUSE', pause_reason='CIRCUIT_BREAKER', updated_at=NOW() WHERE id=1")
                conn.commit()
                import subprocess
                subprocess.Popen(['python3', '/root/agent_circuit_breaker.py', 'DD_JOUR', str(round(daily_pnl_pct,2))])
                return
            if dd_total_pct >= 6.0:
                logger.warning(f'CIRCUIT BREAKER DD TOTAL: {dd_total_pct:.2f}%')
                send_telegram(f"CIRCUIT BREAKER DD TOTAL\nDD total: {dd_total_pct:.2f}%\nBot mis en pause.")
                cur.execute("UPDATE capital_smc SET bot_status='PAUSE', pause_reason='CIRCUIT_BREAKER', updated_at=NOW() WHERE id=1")
                conn.commit()
                import subprocess
                subprocess.Popen(['python3', '/root/agent_circuit_breaker.py', 'DD_TOTAL', str(round(dd_total_pct,2))])
                return

        # 3. Signaux actifs
        cur.execute("""
            SELECT id, type, entry_price, sl_price, tp_price,
                   lot_size, status, killzone, filled_at, fill_price, sl_pips,
                   ctrader_order_id
            FROM signals_smc
            WHERE status IN ('pending','filled')
            ORDER BY created_at DESC
        """)
        signals = cur.fetchall()

        if not signals:
            logger.info(f'Aucune position ouverte — prix={price}')
            return

        now_utc = datetime.now(timezone.utc)

        # === RETRY ASYNC : signaux filled sans mt5_ticket confirme ===
        MAX_SYNC_ATTEMPTS = 60  # ~1h, augmente le 08/07/2026 suite ecart observe entre detection locale wick et fill reel broker (40min)
        cur.execute("SELECT id, sync_attempts FROM signals_smc WHERE status='filled' AND mt5_ticket IS NULL")
        unsynced = cur.fetchall()
        for unsynced_id, attempts in unsynced:
            attempts = attempts or 0
            if attempts >= MAX_SYNC_ATTEMPTS:
                cur.execute("UPDATE signals_smc SET status='sync_failed' WHERE id=%s", (unsynced_id,))
                conn.commit()
                logger.error(f'Signal {unsynced_id} SYNC_FAILED apres {MAX_SYNC_ATTEMPTS} tentatives - monitoring arrete')
                send_telegram(f"SYNC_FAILED - Signal #{unsynced_id}\nAucune position trouvee sur cTrader apres {MAX_SYNC_ATTEMPTS} tentatives.\nLe vrai fill broker peut survenir plus tard que la detection locale (ecart de flux de prix observe).\nOrdre potentiellement toujours pending, ou annule/rejete.\nVerification manuelle recommandee.")
                continue
            if not CTRADER_ENABLED:
                continue
            try:
                retry_pos_id = ctrader_ex.sync_position_id(unsynced_id)
            except Exception as _e:
                retry_pos_id = None
                logger.error(f'Signal {unsynced_id} retry sync erreur: {_e}')
            if retry_pos_id:
                logger.info(f'Signal {unsynced_id} position_id sync (retry): {retry_pos_id}')
            else:
                cur.execute("UPDATE signals_smc SET sync_attempts = sync_attempts + 1 WHERE id=%s", (unsynced_id,))
                conn.commit()
                logger.warning(f'Signal {unsynced_id} retry sync echoue (tentative {attempts + 1}/{MAX_SYNC_ATTEMPTS})')
        # === DETECTION FERMETURE MANUELLE ===
        cur.execute("SELECT id, mt5_ticket, fill_price, filled_at, killzone, sl_price, tp_price, lot_size, sl_pips FROM signals_smc WHERE status='filled' AND mt5_ticket IS NOT NULL")
        actively_monitored = cur.fetchall()
        if actively_monitored and CTRADER_ENABLED:
            try:
                open_pos_ids = ctrader_ex.get_open_position_ids()
            except Exception as _ge:
                open_pos_ids = None
                logger.error(f"get_open_position_ids erreur: {_ge}")
            if open_pos_ids is not None:
                for mon_id, mon_ticket, mon_fill_price, mon_filled_at, mon_killzone, mon_sl_price, mon_tp_price, mon_lot_size, mon_sl_pips in actively_monitored:
                    if int(mon_ticket) not in open_pos_ids:
                        logger.warning(f"Signal {mon_id} pos={mon_ticket} absent des positions ouvertes - fermeture manuelle suspectee")
                        try:
                            import subprocess as _sp2
                            import json as _json2
                            _sync_proc = _sp2.run(
                                ['python3', '/opt/deborah-trading/scripts/sync_real_pnl_standalone.py', str(mon_id)],
                                capture_output=True, timeout=30
                            )
                            if _sync_proc.returncode == 0:
                                _stdout_lines = [l for l in _sync_proc.stdout.decode().strip().split('\n') if l.strip()]
                                _pnl_manual = _json2.loads(_stdout_lines[-1]) if _stdout_lines else None
                            else:
                                _pnl_manual = None
                                logger.error(f"Signal {mon_id} sync_real_pnl (manual close) subprocess erreur (code={_sync_proc.returncode}): {_sync_proc.stderr.decode(errors='replace')[:200]}")
                        except Exception as _me:
                            _pnl_manual = None
                            logger.error(f"Signal {mon_id} sync_real_pnl (manual close) erreur: {_me}")
                        if _pnl_manual:
                            _close_p = float(_pnl_manual['close_price'])
                            _pip = 0.0001
                            _near_sl = mon_sl_price is not None and abs(_close_p - float(mon_sl_price)) <= 3 * _pip
                            _near_tp = mon_tp_price is not None and abs(_close_p - float(mon_tp_price)) <= 3 * _pip
                            if _near_sl:
                                _exit_reason = 'SL_HIT_LATE'
                            elif _near_tp:
                                _exit_reason = 'TP_HIT_LATE'
                            else:
                                _exit_reason = 'MANUAL_CLOSE'
                            _be_triggered_manual = get_breakeven_fired(cur, mon_id)
                            cur.execute(
                                "INSERT INTO trades_smc (signal_id, mt5_ticket, open_price, close_price, lot_size, pnl_eur, result, exit_reason, killzone, open_at, close_at, breakeven_triggered) "
                                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s) RETURNING id",
                                (mon_id, mon_ticket, mon_fill_price, _pnl_manual['close_price'], mon_lot_size, _pnl_manual['pnl_net'],
                                 'WIN' if _pnl_manual['pnl_net'] > 0 else 'LOSS', _exit_reason, mon_killzone, mon_filled_at, _be_triggered_manual)
                            )
                            _manual_trade_id = cur.fetchone()[0]
                            cur.execute("UPDATE signals_smc SET status='closed', closed_at=NOW() WHERE id=%s", (mon_id,))
                            # FIX 24/08/2026 : cette branche de reconciliation (fermeture
                            # broker detectee, ex: trade #120/signal #97) faisait son propre
                            # UPDATE capital_smc SANS jamais toucher consecutive_losses,
                            # contrairement au chemin normal close_position() (ligne ~205).
                            # Meme classe de bug que breakeven_triggered corrige le 03/08 -
                            # deux chemins de cloture paralleles, un seul synchronise. Decouvert
                            # en verifiant pourquoi consecutive_losses restait a 0 en base
                            # malgre une vraie serie de 8 pertes consecutives (confirmee via
                            # export CSV du broker). Consequence potentielle : circuit breaker
                            # base sur ce compteur jamais declenche sur cette serie.
                            cur.execute(
                                "UPDATE capital_smc SET capital_actuel=capital_actuel+%s, "
                                "consecutive_losses = CASE WHEN %s < 0 THEN consecutive_losses + 1 ELSE 0 END, "
                                "updated_at=NOW() WHERE id=1",
                                (_pnl_manual['pnl_net'], _pnl_manual['pnl_net']))
                            conn.commit()
                            logger.info(f"Signal {mon_id} cloture manuellement detectee et enregistree: pnl={_pnl_manual['pnl_net']}EUR (capital mis a jour)")
                            send_telegram(f"INFO - Signal #{mon_id} cloture broker detectee par reconciliation ({_exit_reason})\nPnL reel: {_pnl_manual['pnl_net']}EUR")
                            import subprocess as _sp3
                            _sp3.Popen(['python3', '/root/agent_post_trade.py', str(_manual_trade_id)])
                        else:
                            logger.error(f"Signal {mon_id} absent des positions mais sync_real_pnl a echoue - verification manuelle requise")

        for sig in signals:
            sig_id, sig_type, entry, sl, tp, lots, status, killzone, filled_at, fill_price_db, sl_pips_original, ctrader_order_id_db = sig
            entry = float(entry); sl = float(sl)
            tp = float(tp);       lots = float(lots)

            # === PENDING -> FILLED ===
            if status == 'pending':
                # RACE CONDITION decouverte le 23/07/2026 (signal #92) : la detection
                # de fill locale (M5 high/low) ne verifiait pas si l'ordre avait ete
                # reellement soumis au broker (ctrader_order_id). Un signal pouvait
                # etre marque 'filled' localement AVANT que ctrader_executor_v2.py
                # n'ait eu l'occasion de soumettre le vrai ordre LIMIT - qui ne le
                # soumet alors plus jamais (il ne cherche que status='pending'),
                # laissant le signal bloque en 'filled' sans mt5_ticket a vie.
                if not ctrader_order_id_db:
                    logger.warning(f'Signal {sig_id} pending sans ctrader_order_id - ordre pas encore soumis au broker, en attente de ctrader_executor_v2')
                    continue
                # Detection remplissage basee sur high/low M5 (pas juste close),
                # pour capturer les wicks qui touchent l'entree sans que la bougie
                # cloture au-dela - meme principe que HIT_TP/HIT_SL. Bug decouvert
                # le 08/07/2026 : signal #80 reste pending malgre remplissage reel
                # confirme cote broker (wick M5 07:30 high=1.14315 > entry=1.14309,
                # close=1.14289 jamais vu par l'ancienne detection close-only).
                filled = (
                    (sig_type in ('SELL_LIMIT', 'SELL') and price_high >= entry) or
                    (sig_type in ('BUY_LIMIT',  'BUY')  and price_low <= entry)
                )
                if filled:
                    cur.execute("SELECT confirmation_mode FROM capital_smc WHERE id=1")
                    conf_row = cur.fetchone()
                    conf_mode = conf_row[0] if conf_row else 'IFVG_CONFIRM'
                    if conf_mode in ('OB_CONFIRM', 'IFVG_CONFIRM'):
                        cur.execute("SELECT open, high, low, close FROM prices_smc WHERE timeframe='M5' ORDER BY candle_time DESC LIMIT 1")
                        candle = cur.fetchone()
                        if candle:
                            o, h, l, c2 = float(candle[0]), float(candle[1]), float(candle[2]), float(candle[3])
                            rng = h - l
                            if rng > 0:
                                body = (c2 - o) if sig_type in ('BUY_LIMIT','BUY') else (o - c2)
                                confirmed = abs(body)/rng >= 0.40
                                if not confirmed:
                                    logger.info(f'Signal {sig_id} — confirmation bougie attente body={round(body*10000,1)}p rng={round(rng*10000,1)}p')
                                    continue

                    # Calcul niveaux TP partiels pour Telegram
                    tp_levels = []
                    for pips, pct in TP_PALIERS:
                        if sig_type in ('BUY_LIMIT','BUY'):
                            tp_levels.append(round(entry + pips * PIP, 5))
                        else:
                            tp_levels.append(round(entry - pips * PIP, 5))

                    cur.execute(
                        "UPDATE signals_smc SET status='filled', filled_at=NOW(), fill_price=%s, tp_palier=0 WHERE id=%s",
                        (entry, sig_id)
                    )
                    conn.commit()
                    logger.info(f'Signal {sig_id} FILLED — prix={price} entry={entry} '
                                f'TP1={tp_levels[0]} TP2={tp_levels[1]} TP3={tp_levels[2]} TP={tp}')
                    # Sync position_id reel depuis cTrader (tentative unique)
                    pos_id = None
                    if CTRADER_ENABLED:
                        try:
                            pos_id = ctrader_ex.sync_position_id(sig_id)
                        except Exception as _e:
                            logger.error(f'Signal {sig_id} sync_position_id erreur: {_e}')
                    if pos_id:
                        logger.info(f'Signal {sig_id} position_id sync: {pos_id}')
                    else:
                        cur.execute("UPDATE signals_smc SET sync_attempts = sync_attempts + 1 WHERE id=%s", (sig_id,))
                        conn.commit()
                        logger.warning(f'Signal {sig_id} position_id non trouve sur cTrader (1ere tentative)')
                    if pos_id:
                        send_telegram(
                        f"POSITION OUVERTE — {sig_type} EUR/USD\n"
                        f"Entry: {entry}  SL: {sl}\n"
                        f"TP1: {tp_levels[0]} (+10p, 20%)\n"
                        f"TP2: {tp_levels[1]} (+20p, 20%)\n"
                        f"TP3: {tp_levels[2]} (+35p, 20%)\n"
                        f"TP4: {tp_levels[3]} (+55p, 20%)\n"
                        f"TP5: {tp_levels[4]} (+80p, 15%)\n"
                        f"TP6: {tp} (final, 5%)\n"
                        f"Lots: {lots}  ID: {sig_id}"
                    )
                continue

            # === FILLED : action ===
            # Garde-fou : ne jamais monitorer activement un signal filled
            # dont la position cTrader n'est pas confirmee (cf incident #44/#48)
            if status == 'filled':
                cur.execute("SELECT mt5_ticket FROM signals_smc WHERE id=%s", (sig_id,))
                _mt5_row = cur.fetchone()
                if not _mt5_row or not _mt5_row[0]:
                    continue
            action = 'HOLD'
            pnl = 0.0
            entry_exec = float(fill_price_db) if fill_price_db else entry

            if sig_type in ('BUY_LIMIT', 'BUY'):
                float_pnl = (price - entry_exec) * lots * 100000
                if price_high >= tp:
                    action = 'HIT_TP'; pnl = (tp - entry_exec) * lots * 100000
                elif price_low <= sl:
                    action = 'HIT_SL'; pnl = (sl - entry_exec) * lots * 100000
            else:
                float_pnl = (entry_exec - price) * lots * 100000
                if price_low <= tp:
                    action = 'HIT_TP'; pnl = (entry_exec - tp) * lots * 100000
                elif price_high >= sl:
                    action = 'HIT_SL'; pnl = (entry_exec - sl) * lots * 100000

            # === TP PARTIELS EN PIPS FIXES ===
            if action == 'HOLD':
                palier_actuel = get_tp_palier(cur, sig_id)
                lots_initial_ref = lots  # pour calcul lots a fermer

                for idx, (pips, lots_pct) in enumerate(TP_PALIERS):
                    palier_num = idx + 1
                    if palier_actuel >= palier_num:
                        continue  # deja atteint

                    if sig_type in ('BUY_LIMIT','BUY'):
                        tp_level = round(entry + pips * PIP, 5)
                        hit = price >= tp_level
                    else:
                        tp_level = round(entry - pips * PIP, 5)
                        hit = price <= tp_level

                    if not hit:
                        break  # paliers sequentiels — pas la peine de tester les suivants

                    # Fermeture partielle
                    lots_to_close = round(lots * lots_pct / (1 - sum(p for _, p in TP_PALIERS[:idx])), 2)
                    # Plus simple : % des lots RESTANTS
                    lots_to_close = round(lots * lots_pct, 2)
                    if lots_to_close > lots:
                        lots_to_close = lots
                    lots_remaining = round(lots - lots_to_close, 2)
                    if lots_remaining < 0.01:
                        lots_remaining = 0.01

                    if sig_type in ('BUY_LIMIT','BUY'):
                        pnl_partial = (tp_level - entry_exec) * lots_to_close * 100000
                    else:
                        pnl_partial = (entry_exec - tp_level) * lots_to_close * 100000

                    palier_label = f'TP{palier_num}_PARTIAL'
                    _be_triggered_partial = get_breakeven_fired(cur, sig_id)
                    pnl_pct_p, _ = close_partial(cur, conn, sig_id, entry, tp_level,
                                                  lots_to_close, pnl_partial, sig_type,
                                                  killzone, palier_label, lots_remaining,
                                                  _be_triggered_partial)

                    # SL monte au niveau du palier atteint
                    # SL garanti 2 pips sous TP1 pour eviter HIT_SL immediat
                    sl_buffer_pips = 3 * PIP  # Buffer 3p pour absorber oscillations
                    new_sl_level = round(tp_level - sl_buffer_pips, 5) if sig_type in ('BUY_LIMIT','BUY') else round(tp_level + sl_buffer_pips, 5)
                    cur.execute("UPDATE signals_smc SET sl_price=%s WHERE id=%s", (new_sl_level, sig_id))
                    set_tp_palier(cur, conn, sig_id, palier_num)
                    sl = new_sl_level
                    lots = lots_remaining

                    sign = '+' if pnl_partial >= 0 else ''
                    send_telegram(
                        f"TP{palier_num} PARTIEL (+{pips}p) — {sig_type} EUR/USD\n"
                        f"{int(lots_pct*100)}% ferme a {tp_level}\n"
                        f"PnL partiel: {sign}{pnl_partial:.2f}EUR ({pnl_pct_p}%)\n"
                        f"SL garanti a {tp_level}\n"
                        f"Lots restants: {lots_remaining}\nID: {sig_id}"
                    )
                    # Synchronisation cTrader — fermeture partielle + amendment SL
                    if CTRADER_ENABLED:
                        try:
                            import subprocess as _sp3
                            _tp_proc = _sp3.run(
                                ['python3', '/opt/deborah-trading/scripts/sync_tp_palier_standalone.py', str(sig_id), str(palier_num), str(lots_to_close), str(tp_level)],
                                capture_output=True, timeout=30
                            )
                            if _tp_proc.returncode == 0:
                                logger.info(f'Signal {sig_id} TP{palier_num} sync cTrader OK')
                            elif _tp_proc.returncode == 1:
                                logger.warning(f'Signal {sig_id} TP{palier_num} sync cTrader SKIP (pas de mt5_ticket)')
                            else:
                                logger.error(f'Signal {sig_id} TP{palier_num} cTrader erreur (subprocess code={_tp_proc.returncode}): {_tp_proc.stderr.decode(errors="replace")[:200]}')
                        except Exception as _ce:
                            logger.error(f'Signal {sig_id} TP{palier_num} cTrader erreur: {_ce}')

                    float_pnl = (price - entry_exec) * lots * 100000 \
                                if sig_type in ('BUY_LIMIT','BUY') \
                                else (entry_exec - price) * lots * 100000
                    palier_actuel = palier_num

            # === TRAILING STOP / BREAKEVEN CHoCH ===
            if action == 'HOLD' and float_pnl is not None:
                profit_pips = abs(entry - price) * 10000 if (
                    (sig_type in ('SELL_LIMIT','SELL') and price < entry) or
                    (sig_type in ('BUY_LIMIT','BUY')  and price > entry)
                ) else 0
                new_sl = None
                is_breakeven_event = False  # True seulement si new_sl vient d'un vrai
                                             # breakeven (simple ou CHoCH), jamais du trailing
                # BREAKEVEN AUTO — Option A+C
                # Condition : bougie M5 entiere au-dessus entry + 3 pips (price_low confirme)
                BE_MIN_PIPS = 3
                BE_BUFFER_PIPS = 2  # marge pour absorber micro stop-hunts post-breakeven
                if new_sl is None:
                    if sig_type in ("BUY_LIMIT","BUY") and price_low > entry + BE_MIN_PIPS * PIP and sl < entry:
                        new_sl = round(entry - BE_BUFFER_PIPS * PIP, 5)
                        is_breakeven_event = True
                        logger.info(f"Signal {sig_id} BREAKEVEN CONFIRME (low={price_low:.5f} > entry+{BE_MIN_PIPS}p) SL -> {new_sl} (buffer {BE_BUFFER_PIPS}p)")
                    elif sig_type in ("SELL_LIMIT","SELL") and price_high < entry - BE_MIN_PIPS * PIP and sl > entry:
                        new_sl = round(entry + BE_BUFFER_PIPS * PIP, 5)
                        is_breakeven_event = True
                        logger.info(f"Signal {sig_id} BREAKEVEN CONFIRME (high={price_high:.5f} < entry-{BE_MIN_PIPS}p) SL -> {new_sl} (buffer {BE_BUFFER_PIPS}p)")
                palier_actuel = get_tp_palier(cur, sig_id)

                if sig_type in ('SELL_LIMIT', 'SELL'):
                    if profit_pips >= 15 and sl > entry:
                        cur.execute("""
                            SELECT candle_time, open, high, low, close FROM prices_smc
                            WHERE timeframe='M5' AND candle_time > %s
                            ORDER BY candle_time ASC LIMIT %s
                        """, (filled_at, CHOCH_WINDOW + 2))
                        m5_rows = cur.fetchall()
                        if len(m5_rows) >= CHOCH_MIN_DELAY_CANDLES + 2:
                            m5_candles = [{'time': r[0], 'open': float(r[1]),
                                          'high': float(r[2]), 'low': float(r[3]),
                                          'close': float(r[4])} for r in m5_rows]
                            choch_level, choch_found = detect_choch_m5(m5_candles, 'short', CHOCH_WINDOW)
                            if choch_found and choch_level:
                                dist_pips = abs(price - choch_level) * 10000
                                if dist_pips <= CHOCH_MAX_DIST_PIPS:
                                    new_sl = round(entry + BE_BUFFER_PIPS * PIP, 5)
                                    is_breakeven_event = True
                                    logger.info(f'Signal {sig_id} BREAKEVEN CHOCH SL -> {new_sl} (buffer {BE_BUFFER_PIPS}p)')
                    if new_sl is None:
                        # sl_dist FIXE base sur sl_pips d'origine (immuable), PAS sur le
                        # sl courant qui devient egal a entry apres breakeven (sl_dist=0
                        # sinon, ce qui declenchait un trailing instantane colle au prix
                        # courant - bug decouvert le 06/07/2026 sur signal #78).
                        sl_dist = float(sl_pips_original) * PIP if sl_pips_original else abs(entry - sl)
                        profit_dist = entry - price
                        trail_mult = 2.5 if session_bias_val in ("BEARISH","BULLISH") else 1.0
                        trail_dist_mult = 1.5 if session_bias_val in ("BEARISH","BULLISH") else 1.0
                        if profit_dist >= sl_dist * trail_mult:
                            trail_sl = round(price + sl_dist * trail_dist_mult, 5)
                            if trail_sl < sl:
                                new_sl = trail_sl
                                logger.info(f'Signal {sig_id} TRAILING SL -> {new_sl}')
                else:
                    if profit_pips >= 15 and sl < entry:
                        cur.execute("""
                            SELECT candle_time, open, high, low, close FROM prices_smc
                            WHERE timeframe='M5' AND candle_time > %s
                            ORDER BY candle_time ASC LIMIT %s
                        """, (filled_at, CHOCH_WINDOW + 2))
                        m5_rows = cur.fetchall()
                        if len(m5_rows) >= CHOCH_MIN_DELAY_CANDLES + 2:
                            m5_candles = [{'time': r[0], 'open': float(r[1]),
                                          'high': float(r[2]), 'low': float(r[3]),
                                          'close': float(r[4])} for r in m5_rows]
                            choch_level, choch_found = detect_choch_m5(m5_candles, 'long', CHOCH_WINDOW)
                            if choch_found and choch_level:
                                dist_pips = abs(price - choch_level) * 10000
                                if dist_pips <= CHOCH_MAX_DIST_PIPS:
                                    new_sl = round(entry - BE_BUFFER_PIPS * PIP, 5)
                                    is_breakeven_event = True
                                    logger.info(f'Signal {sig_id} BREAKEVEN CHOCH SL -> {new_sl} (buffer {BE_BUFFER_PIPS}p)')
                    if new_sl is None:
                        # sl_dist FIXE base sur sl_pips d'origine (voir commentaire bloc SELL)
                        sl_dist = float(sl_pips_original) * PIP if sl_pips_original else abs(entry - sl)
                        profit_dist = price - entry
                        trail_mult = 2.5 if session_bias_val in ("BEARISH","BULLISH") else 1.0
                        trail_dist_mult = 1.5 if session_bias_val in ("BEARISH","BULLISH") else 1.0
                        if profit_dist >= sl_dist * trail_mult:
                            trail_sl = round(price - sl_dist * trail_dist_mult, 5)
                            if trail_sl > sl:
                                new_sl = trail_sl
                                logger.info(f'Signal {sig_id} TRAILING SL -> {new_sl}')

                if new_sl is not None:
                    # Ne jamais reculer sous le dernier palier garanti
                    if palier_actuel >= 1:
                        pips_palier = TP_PALIERS[palier_actuel - 1][0]
                        floor = round(entry + pips_palier * PIP, 5) if sig_type in ('BUY_LIMIT','BUY') \
                                else round(entry - pips_palier * PIP, 5)
                        if sig_type in ('BUY_LIMIT','BUY'):
                            new_sl = max(new_sl, floor)
                        else:
                            new_sl = min(new_sl, floor)
                    cur.execute(
                        "UPDATE signals_smc SET sl_price=%s, breakeven_fired = breakeven_fired OR %s WHERE id=%s",
                        (new_sl, is_breakeven_event, sig_id)
                    )
                    conn.commit()
                    sl = new_sl
                    # Synchronisation cTrader — amendment SL
                    if CTRADER_ENABLED:
                        try:
                            from psycopg2 import sql as _sql
                            _conn2 = __import__('psycopg2').connect(**{
                                'host':'localhost','port':5432,'dbname':'trading',
                                'user':'trading','password':'Trading2026'})
                            _cur2 = _conn2.cursor()
                            _cur2.execute("SELECT mt5_ticket FROM signals_smc WHERE id=%s", (sig_id,))
                            _row = _cur2.fetchone()
                            _cur2.close(); _conn2.close()
                            if _row and _row[0]:
                                import subprocess, json as _json
                                _proc = subprocess.run(
                                    ['python3', '/opt/deborah-trading/scripts/amend_sl_standalone.py',
                                     str(int(_row[0])), str(new_sl)],
                                    capture_output=True, text=True, timeout=35)
                                _lines = [l for l in _proc.stdout.strip().split('\n') if l.strip()]
                                try:
                                    _res = _json.loads(_lines[-1]) if _lines else {'success': False, 'error': 'no output'}
                                except Exception:
                                    _res = {'success': False, 'error': f'unparsable output: {_proc.stdout[-200:]}'}
                                if _res.get('success'):
                                    logger.info(f'Signal {sig_id} SL amende cTrader -> {new_sl}')
                                else:
                                    logger.error(f'Signal {sig_id} amend SL cTrader echec: {_res.get("error")}')
                        except Exception as _ce:
                            logger.error(f'Signal {sig_id} amend SL cTrader erreur: {_ce}')

            # === TIMEOUT ===
            if action == 'HOLD' and filled_at:
                fa = filled_at.replace(tzinfo=timezone.utc) if filled_at.tzinfo is None else filled_at
                hours_open = (now_utc - fa).total_seconds() / 3600
                timeout = TIMEOUT_HOURS if float_pnl <= 0 else TIMEOUT_HOURS * 2
                if hours_open >= timeout:
                    action = 'TIMEOUT'
                    pnl = float_pnl
                    _be_triggered_timeout = get_breakeven_fired(cur, sig_id)
                    pnl_pct = close_position(cur, conn, sig_id, entry, price, lots, pnl,
                                             sig_type, killzone, 'WIN' if pnl > 0 else 'TIMEOUT',
                                             'TIMEOUT', _be_triggered_timeout)
                    sign = '+' if pnl >= 0 else ''
                    logger.info(f'Signal {sig_id} TIMEOUT {hours_open:.1f}h — prix={price} pnl={pnl:.2f}EUR ({pnl_pct}%)')
                    send_telegram(
                        f"TIMEOUT {TIMEOUT_HOURS}H — {sig_type} EUR/USD\n"
                        f"Entry: {entry} Exit: {price}\n"
                        f"PnL: {sign}{pnl:.2f}EUR ({pnl_pct}%)\nID: {sig_id}"
                    )
                    continue
                else:
                    logger.info(f'Signal {sig_id} HOLD — prix={price} PnL flottant={float_pnl:.2f}EUR ({hours_open:.1f}h/{timeout}h)')
                    continue

            if action == 'HOLD':
                logger.info(f'Signal {sig_id} HOLD — prix={price} PnL flottant={float_pnl:.2f}EUR')
                continue

            # === HIT_TP ou HIT_SL ===
            exit_price = tp if action == 'HIT_TP' else sl
            palier_actuel = get_tp_palier(cur, sig_id)
            if action == 'HIT_TP':
                result_label = f'TP{palier_actuel+1}_FINAL' if palier_actuel >= 1 else 'WIN'
                exit_reason = 'HIT_TP'
            else:
                result_label = 'WIN' if pnl > 0 else 'LOSS'
                exit_reason = 'TRAILING' if sl > entry else 'HIT_SL'

            _be_triggered = get_breakeven_fired(cur, sig_id)
            pnl_pct = close_position(cur, conn, sig_id, entry, exit_price, lots, pnl,
                                     sig_type, killzone, result_label, exit_reason, _be_triggered)
            sign = '+' if pnl >= 0 else ''
            logger.info(f'Signal {sig_id} {action} — prix={price} pnl={pnl:.2f}EUR ({pnl_pct}%) reason={exit_reason}')
            send_telegram(
                f"{action} — {sig_type} EUR/USD\n"
                f"Entry: {entry} Exit: {exit_price}\n"
                f"PnL: {sign}{pnl:.2f}EUR ({pnl_pct}%)\n"
                f"Raison: {exit_reason}\nID: {sig_id}"
            )

    except Exception as e:
        logger.error(f'Erreur: {e}', exc_info=True)
    finally:
        try:
            cur.close(); conn.close()
        except:
            pass


if __name__ == '__main__':
    run()
