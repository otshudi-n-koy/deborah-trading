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
TP_PALIERS = [
    (8,   0.30),  # TP1 : +8p  — couvre le SL, securisation immediate (30%)
    (15,  0.25),  # TP2 : +15p — zone la plus frequente (25%)
    (25,  0.20),  # TP3 : +25p — max observe sur plupart des trades (20%)
    (40,  0.15),  # TP4 : +40p — rare mais atteint (15%)
    (70,  0.08),  # TP5 : +70p — tres rare, trades exceptionnels (8%)
    # TP6 = TP final (lots restants ~2%)
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
        requests.post(
            f'https://api.telegram.org/bot{token}/sendMessage',
            json={'chat_id': row2[0], 'text': message, 'parse_mode': 'Markdown'},
            timeout=5
        )
    except Exception as e:
        logger.warning(f'Telegram non disponible: {e}')


def close_position(cur, conn, sig_id, entry, exit_price, lots, pnl,
                   sig_type, killzone, result_label, exit_reason=''):
    pnl_pct = round(pnl / 10000 * 100, 3)
    cur.execute(
        "UPDATE signals_smc SET status='closed', closed_at=NOW() WHERE id=%s",
        (sig_id,)
    )
    cur.execute("""
        INSERT INTO trades_smc
        (signal_id, open_price, close_price, lot_size, pnl_eur, pnl_pct,
         result, killzone, open_at, close_at, exit_reason)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,
            (SELECT filled_at FROM signals_smc WHERE id=%s), NOW(), %s)
        RETURNING id
    """, (sig_id, entry, exit_price, lots, round(pnl, 2), pnl_pct,
          result_label, killzone, sig_id, exit_reason))
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
    return pnl_pct


def close_partial(cur, conn, sig_id, entry, exit_price, lots_closed, pnl,
                  sig_type, killzone, palier_label, lots_remaining):
    pnl_pct = round(pnl / 10000 * 100, 3)
    cur.execute("""
        INSERT INTO trades_smc
        (signal_id, open_price, close_price, lot_size, pnl_eur, pnl_pct,
         result, killzone, open_at, close_at, exit_reason)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,
            (SELECT filled_at FROM signals_smc WHERE id=%s), NOW(), %s)
        RETURNING id
    """, (sig_id, entry, exit_price, round(lots_closed, 2), round(pnl, 2),
          pnl_pct, palier_label, killzone, sig_id, palier_label))
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
            cur.execute("SELECT id, type, entry_price, sl_price, tp_price, lot_size, killzone FROM signals_smc WHERE status='filled'")
            for sig in cur.fetchall():
                sig_id, sig_type, entry, sl, tp, lots, killzone = sig
                entry = float(entry); lots = float(lots)
                cur.execute("SELECT close FROM prices_smc WHERE timeframe='M5' ORDER BY candle_time DESC LIMIT 1")
                price_row = cur.fetchone()
                if not price_row: continue
                exit_price = float(price_row[0])
                pnl = (float(entry) - exit_price) * lots * 100000 / 10 if sig_type in ('SELL_LIMIT','SELL') \
                      else (exit_price - float(entry)) * lots * 100000 / 10
                pnl_pct = round(pnl / 10000 * 100, 3)
                cur.execute("UPDATE signals_smc SET status='closed', closed_at=NOW() WHERE id=%s", (sig_id,))
                cur.execute("""
                    INSERT INTO trades_smc (signal_id, open_price, close_price, lot_size, pnl_eur, pnl_pct,
                    result, killzone, open_at, close_at, exit_reason)
                    VALUES (%s,%s,%s,%s,%s,%s,'FRIDAY_CLOSE',%s,
                    (SELECT filled_at FROM signals_smc WHERE id=%s),NOW(),'FRIDAY_CLOSE')
                """, (sig_id, entry, exit_price, lots, round(pnl,2), pnl_pct, killzone, sig_id))
                cur.execute("UPDATE capital_smc SET capital_actuel=capital_actuel+%s, daily_pnl_pct=daily_pnl_pct+%s, updated_at=NOW() WHERE id=1",
                            (round(pnl,2), pnl_pct))
                conn.commit()
                sign = '+' if pnl >= 0 else ''
                logger.info(f'Signal {sig_id} FRIDAY_CLOSE — exit={exit_price} pnl={pnl:.2f}EUR')
                send_telegram(f"VENDREDI CLOSE — {sig_type} EUR/USD\nEntry: {entry} Exit: {exit_price}\nPnL: {sign}{pnl:.2f}EUR\nID: {sig_id}")
            return

        # 1. Expirer signaux PENDING
        cur.execute("""
            UPDATE signals_smc SET status='expired', closed_at=NOW()
            WHERE status='pending' AND (
                (killzone='LONDON'   AND NOW() AT TIME ZONE 'UTC' > CURRENT_DATE + INTERVAL '16 hours')
                OR (killzone='NEW_YORK' AND NOW() AT TIME ZONE 'UTC' > CURRENT_DATE + INTERVAL '21 hours')
                OR (created_at < NOW() - INTERVAL '12 hours')
            ) RETURNING id, killzone
        """)
        for row in cur.fetchall():
            logger.info(f'Signal {row[0]} expire ({row[1]})')
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
        cur.execute("SELECT daily_pnl_pct, capital_actuel, capital_initial, session_bias FROM capital_smc WHERE id=1")
        cap_row = cur.fetchone()
        if cap_row:
            daily_pnl_pct = float(cap_row[0] or 0)
            capital_actuel = float(cap_row[1] or 10000)
            capital_initial = float(cap_row[2] or 10000)
            session_bias_val = cap_row[3] or "NEUTRE"
            dd_total_pct = (capital_initial - capital_actuel) / capital_initial * 100 \
                           if capital_actuel < capital_initial else 0
            if daily_pnl_pct <= -4.0:
                logger.warning(f'CIRCUIT BREAKER DD JOUR: {daily_pnl_pct}%')
                send_telegram(f"CIRCUIT BREAKER DD JOURNALIER\nDD jour: {daily_pnl_pct}%\nToutes positions fermees.")
                cur.execute("SELECT id, type, entry_price, lot_size, killzone FROM signals_smc WHERE status='filled'")
                for sig in cur.fetchall():
                    sig_id2, sig_type2, s_entry, s_lots, s_kz = sig
                    s_entry = float(s_entry); s_lots = float(s_lots)
                    pnl_cb = (price - s_entry)*s_lots*100000/10 if sig_type2 in ('BUY_LIMIT','BUY') \
                             else (s_entry - price)*s_lots*100000/10
                    close_position(cur, conn, sig_id2, s_entry, price, s_lots, pnl_cb, sig_type2, s_kz,
                                   'WIN' if pnl_cb > 0 else 'LOSS', 'CIRCUIT_BREAKER')
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
                   lot_size, status, killzone, filled_at, fill_price
            FROM signals_smc
            WHERE status IN ('pending','filled')
            ORDER BY created_at DESC
        """)
        signals = cur.fetchall()

        if not signals:
            logger.info(f'Aucune position ouverte — prix={price}')
            return

        now_utc = datetime.now(timezone.utc)

        for sig in signals:
            sig_id, sig_type, entry, sl, tp, lots, status, killzone, filled_at, fill_price_db = sig
            entry = float(entry); sl = float(sl)
            tp = float(tp);       lots = float(lots)

            # === PENDING -> FILLED ===
            if status == 'pending':
                filled = (
                    (sig_type in ('SELL_LIMIT', 'SELL') and price >= entry) or
                    (sig_type in ('BUY_LIMIT',  'BUY')  and price <= entry)
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
            action = 'HOLD'
            pnl = 0.0
            entry_exec = float(fill_price_db) if fill_price_db else entry

            if sig_type in ('BUY_LIMIT', 'BUY'):
                float_pnl = (price - entry_exec) * lots * 100000 / 10
                if price_high >= tp:
                    action = 'HIT_TP'; pnl = (tp - entry_exec) * lots * 100000 / 10
                elif price_low <= sl:
                    action = 'HIT_SL'; pnl = (sl - entry_exec) * lots * 100000 / 10
            else:
                float_pnl = (entry_exec - price) * lots * 100000 / 10
                if price_low <= tp:
                    action = 'HIT_TP'; pnl = (entry_exec - tp) * lots * 100000 / 10
                elif price_high >= sl:
                    action = 'HIT_SL'; pnl = (entry_exec - sl) * lots * 100000 / 10

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
                        pnl_partial = (tp_level - entry_exec) * lots_to_close * 100000 / 10
                    else:
                        pnl_partial = (entry_exec - tp_level) * lots_to_close * 100000 / 10

                    palier_label = f'TP{palier_num}_PARTIAL'
                    pnl_pct_p, _ = close_partial(cur, conn, sig_id, entry, tp_level,
                                                  lots_to_close, pnl_partial, sig_type,
                                                  killzone, palier_label, lots_remaining)

                    # SL monte au niveau du palier atteint
                    cur.execute("UPDATE signals_smc SET sl_price=%s WHERE id=%s", (tp_level, sig_id))
                    set_tp_palier(cur, conn, sig_id, palier_num)
                    sl = tp_level
                    lots = lots_remaining

                    sign = '+' if pnl_partial >= 0 else ''
                    send_telegram(
                        f"TP{palier_num} PARTIEL (+{pips}p) — {sig_type} EUR/USD\n"
                        f"{int(lots_pct*100)}% ferme a {tp_level}\n"
                        f"PnL partiel: {sign}{pnl_partial:.2f}EUR ({pnl_pct_p}%)\n"
                        f"SL garanti a {tp_level}\n"
                        f"Lots restants: {lots_remaining}\nID: {sig_id}"
                    )

                    float_pnl = (price - entry_exec) * lots * 100000 / 10 \
                                if sig_type in ('BUY_LIMIT','BUY') \
                                else (entry_exec - price) * lots * 100000 / 10
                    palier_actuel = palier_num

            # === TRAILING STOP / BREAKEVEN CHoCH ===
            if action == 'HOLD' and float_pnl is not None:
                profit_pips = abs(entry - price) * 10000 if (
                    (sig_type in ('SELL_LIMIT','SELL') and price < entry) or
                    (sig_type in ('BUY_LIMIT','BUY')  and price > entry)
                ) else 0
                new_sl = None
                # BREAKEVEN AUTO
                if new_sl is None:
                    if sig_type in ("BUY_LIMIT","BUY") and price > entry and sl < entry:
                        new_sl = entry
                        logger.info(f"Signal {sig_id} BREAKEVEN AUTO SL -> {entry}")
                    elif sig_type in ("SELL_LIMIT","SELL") and price < entry and sl > entry:
                        new_sl = entry
                        logger.info(f"Signal {sig_id} BREAKEVEN AUTO SL -> {entry}")
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
                                    new_sl = entry
                                    logger.info(f'Signal {sig_id} BREAKEVEN CHOCH SL -> {entry}')
                    if new_sl is None:
                        sl_dist = abs(entry - sl)
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
                                    new_sl = entry
                                    logger.info(f'Signal {sig_id} BREAKEVEN CHOCH SL -> {entry}')
                    if new_sl is None:
                        sl_dist = abs(entry - sl)
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
                    cur.execute("UPDATE signals_smc SET sl_price=%s WHERE id=%s", (new_sl, sig_id))
                    conn.commit()
                    sl = new_sl

            # === TIMEOUT ===
            if action == 'HOLD' and filled_at:
                fa = filled_at.replace(tzinfo=timezone.utc) if filled_at.tzinfo is None else filled_at
                hours_open = (now_utc - fa).total_seconds() / 3600
                timeout = TIMEOUT_HOURS if float_pnl <= 0 else TIMEOUT_HOURS * 2
                if hours_open >= timeout:
                    action = 'TIMEOUT'
                    pnl = float_pnl
                    pnl_pct = close_position(cur, conn, sig_id, entry, price, lots, pnl,
                                             sig_type, killzone, 'WIN' if pnl > 0 else 'TIMEOUT',
                                             'TIMEOUT')
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

            pnl_pct = close_position(cur, conn, sig_id, entry, exit_price, lots, pnl,
                                     sig_type, killzone, result_label, exit_reason)
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
