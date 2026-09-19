#!/usr/bin/env python3
"""
backtest_ci.py — Backtest CI quotidien contre le baseline actif.

Pipeline : analyze_structure (H1/H4) -> try_build_signal (PD array + entry/SL/TP)
-> simulate_trade_outcome (walk-forward M5 : premier de SL/TP touché) -> WR/RR/Kelly
-> comparaison vs test_baselines (is_active=true) -> écriture test_reports.

Logique de génération de signal portée fidèlement de backtest_confluence_impact.py
(ticket #41, confluence H1/H4 validée), avec un correctif : MIN_RR décorrélé par
direction (SELL=1.2, BUY=1.0, ticket #43) au lieu du MIN_RR=1.5 unique du script
d'origine — pour rester fidèle aux paramètres réellement en prod aujourd'hui.

Calcul WR/RR/Kelly répliqué depuis /root/risk_manager.py (calc_kelly), en
R-multiple plutôt qu'en pnl_eur : évite de dépendre du position sizing exact,
suffisant pour comparer WR/RR/Kelly entre baseline et run courant.

Lecture seule sur prices_smc / pd_arrays_smc. Écrit uniquement dans test_reports
(jamais dans trades_smc ou toute table de prod).

Usage :
    python3 backtest_ci.py                      # utilise la fenêtre du baseline actif
    python3 backtest_ci.py --dataset-start 2026-07-01 --dataset-end 2026-08-14
    python3 backtest_ci.py --dry-run             # n'écrit rien, n'notifie rien
    python3 backtest_ci.py --no-notify           # écrit le rapport mais ne notifie pas

Cron suggéré (quotidien, hors killzones) :
    0 4 * * * cd /opt/deborah-trading/scripts && python3 backtest_ci.py >> /var/log/backtest_ci.log 2>&1
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERREUR: psycopg2 introuvable. pip install psycopg2-binary --break-system-packages", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Paramètres alignés sur la prod actuelle (voir userMemories / risk_manager.py)
# ---------------------------------------------------------------------------
ARRAY_BUFFER = 0.00050      # 5 pips, buffer PD array (ticket #39)
MIN_SL = 0.00100
SL_BUFFER = 0.00010
MIN_RR_SELL = 1.2           # ticket #43 : MIN_RR decouple par direction
MIN_RR_BUY = 1.0
BUFFER_NEUTRAL_PIPS = 0.0005
MIN_SIGNIFICANT_PIPS = 0.0010
RECENT_LOOKBACK = 10
LOOKBACK_H1 = 20 * 24
LOOKBACK_H4 = 30 * 6

MIN_TRADES_KELLY = 15
MAX_KELLY_FRACTION = 1 / 4

# Seuil de dérive Kelly déclenchant une alerte (ticket + Telegram).
# NON CALIBRÉ : valeur provisoire, à recalculer une fois qu'on a un historique
# réel de runs backtest_ci (cf. chantier #4 du plan — même logique que le
# seuil WR 48% de risk_manager.py, qui doit lui aussi être recalibré).
KELLY_DRIFT_WARN_PCT = -20.0
KELLY_DRIFT_FAIL_PCT = -40.0
KELLY_DRIFT_ABS_WARN = -0.02
KELLY_DRIFT_ABS_FAIL = -0.05

TELEGRAM_TOKEN = "8400529290:AAEyRzGa0JNCsuecpNJ6gXrqQQM8hnlt-ao"
TELEGRAM_CHAT = "1664221853"
KANBOARD_URL = "http://localhost:8090/jsonrpc.php"
KANBOARD_TOKEN = "3e386b6b099429ca66faa2a12766142e5ba2e6493965c84223fc9739df83"
KANBOARD_PROJECT_ID = 1
KANBOARD_COLUMN_OUVERT = 1


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE", "trading"),
        user=os.environ.get("PGUSER", "trading"),
        password=os.environ.get("PGPASSWORD", "Trading2026!"),
    )


# ---------------------------------------------------------------------------
# Structure H1/H4 — portage fidèle de structure_analyzer.analyze_structure()
# (identique à backtest_confluence_impact.py, non modifié)
# ---------------------------------------------------------------------------
def analyze_structure(candles, window_size=3):
    if len(candles) < window_size * 2 + 1:
        return {'bias': 'NEUTRAL', 'swing_high': None, 'swing_low': None, 'liquidity_target': None}

    candles = sorted(candles, key=lambda c: c['candle_time'])
    swing_highs, swing_lows = [], []

    for i in range(window_size, len(candles) - window_size):
        c = candles[i]
        is_high = all(candles[i - j]['high'] < c['high'] and candles[i + j]['high'] < c['high']
                      for j in range(1, window_size + 1))
        is_low = all(candles[i - j]['low'] > c['low'] and candles[i + j]['low'] > c['low']
                     for j in range(1, window_size + 1))
        if is_high:
            swing_highs.append({'price': c['high'], 'time': c['candle_time']})
        if is_low:
            swing_lows.append({'price': c['low'], 'time': c['candle_time']})

    def filter_sig(swings, is_high):
        out = []
        for s in swings:
            if not out:
                out.append(s); continue
            last = out[-1]
            if abs(s['price'] - last['price']) >= 0.0010:
                out.append(s)
            elif is_high and s['price'] > last['price']:
                out[-1] = s
            elif not is_high and s['price'] < last['price']:
                out[-1] = s
        return out

    swing_highs = filter_sig(swing_highs, True)
    swing_lows = filter_sig(swing_lows, False)
    current_close = candles[-1]['close']

    if len(swing_highs) < 2 or len(swing_lows) < 2:
        recent = candles[-48:]
        fH = max(c['high'] for c in recent)
        fL = min(c['low'] for c in recent)
        rng = fH - fL
        eq50 = fL + rng * 0.5
        bias = 'NEUTRAL' if abs(current_close - eq50) < BUFFER_NEUTRAL_PIPS \
            else ('BEARISH' if current_close < eq50 else 'BULLISH')
        return {'bias': bias, 'swing_high': fH, 'swing_low': fL,
                'liquidity_target': fL if bias == 'BEARISH' else fH}

    last_hh, prev_hh = swing_highs[-1], swing_highs[-2]
    last_ll, prev_ll = swing_lows[-1], swing_lows[-2]

    recent = candles[-RECENT_LOOKBACK:]
    recent_low_val = min(c['low'] for c in recent)
    recent_high_val = max(c['high'] for c in recent)
    if recent_low_val < last_ll['price'] - MIN_SIGNIFICANT_PIPS:
        idx_low = min(range(len(recent)), key=lambda i: recent[i]['low'])
        last_ll = {'price': recent_low_val, 'time': recent[idx_low]['candle_time']}
    if recent_high_val > last_hh['price'] + MIN_SIGNIFICANT_PIPS:
        idx_high = max(range(len(recent)), key=lambda i: recent[i]['high'])
        last_hh = {'price': recent_high_val, 'time': recent[idx_high]['candle_time']}

    range_mid = (last_hh['price'] + last_ll['price']) / 2
    bias = 'NEUTRAL' if abs(current_close - range_mid) < BUFFER_NEUTRAL_PIPS \
        else ('BULLISH' if current_close > range_mid else 'BEARISH')

    lower_lows = [s for s in swing_lows if s['price'] < current_close]
    higher_highs = [s for s in swing_highs if s['price'] > current_close]
    if bias == 'BULLISH':
        liq = min(higher_highs, key=lambda s: s['price'])['price'] if higher_highs else round(current_close + 0.005, 5)
    else:
        liq = max(lower_lows, key=lambda s: s['price'])['price'] if lower_lows else round(current_close - 0.005, 5)

    return {'bias': bias, 'swing_high': last_hh['price'], 'swing_low': last_ll['price'],
            'liquidity_target': liq}


def load_candles(conn, timeframe, start_date, end_date):
    cur = conn.cursor()
    cur.execute("""
        SELECT candle_time, open, high, low, close
        FROM prices_smc
        WHERE timeframe = %s AND candle_time >= %s AND candle_time <= %s
        ORDER BY candle_time ASC
    """, (timeframe, start_date, end_date))
    rows = cur.fetchall()
    cur.close()
    return [{'candle_time': r[0], 'open': float(r[1]), 'high': float(r[2]),
             'low': float(r[3]), 'close': float(r[4])} for r in rows]


def load_pd_arrays(conn, start_date, end_date):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, type, direction, price_high, price_low, price_eq,
               candle_time, invalidated_at
        FROM pd_arrays_smc
        WHERE timeframe = 'M5' AND candle_time >= %s AND candle_time <= %s
        ORDER BY candle_time ASC
    """, (start_date, end_date))
    rows = cur.fetchall()
    cur.close()
    return [{'id': r[0], 'type': r[1], 'direction': r[2], 'price_high': float(r[3]),
             'price_low': float(r[4]), 'price_eq': float(r[5]),
             'candle_time': r[6], 'invalidated_at': r[7]} for r in rows]


def try_build_signal(bias, current_price, active_arrays, struct):
    """
    Portage de signal_generator.py etapes 7-12, avec MIN_RR decouple par direction
    (ticket #43) au lieu du MIN_RR=1.5 unique de backtest_confluence_impact.py.
    """
    direction = 'bullish' if bias == 'BULLISH' else 'bearish'
    candidates = [a for a in active_arrays if a['direction'] == direction]
    if not candidates:
        return None, 'aucun_pd_array_direction'

    best_array = None
    for arr in candidates:
        if (current_price <= arr['price_high'] + ARRAY_BUFFER and
                current_price >= arr['price_low'] - ARRAY_BUFFER):
            best_array = arr
            break
    if not best_array:
        return None, 'prix_hors_pd_array'

    swing_high, swing_low, liq_target = struct['swing_high'], struct['swing_low'], struct['liquidity_target']
    if swing_high is None or swing_low is None or liq_target is None:
        return None, 'structure_incomplete'

    if bias == 'BULLISH':
        entry = best_array['price_eq']
        sl = round(best_array['price_low'] - SL_BUFFER, 5)
        if entry - sl < MIN_SL:
            sl = round(entry - MIN_SL, 5)
        tp = swing_high if liq_target < current_price + 0.0015 else liq_target
        if tp <= current_price:
            tp = liq_target
        if tp <= entry:
            return None, 'tp_invalide'
        min_rr = MIN_RR_BUY
    else:
        entry = best_array['price_eq']
        sl = round(best_array['price_high'] + SL_BUFFER, 5)
        if sl - entry < MIN_SL:
            sl = round(entry + MIN_SL, 5)
        tp = swing_low if liq_target > current_price - 0.0015 else liq_target
        if tp >= current_price:
            tp = liq_target
        if tp >= entry:
            return None, 'tp_invalide'
        min_rr = MIN_RR_SELL

    risk = abs(entry - sl)
    reward = abs(tp - entry)
    rr = round(reward / risk, 2) if risk > 0 else 0
    if rr < min_rr:
        return None, f'rr_insuffisant_{rr}'

    return {'signal_type': 'BUY_LIMIT' if bias == 'BULLISH' else 'SELL_LIMIT',
            'entry': entry, 'sl': sl, 'tp': tp, 'rr': rr}, None


def simulate_trade_outcome(signal, m5_after, max_bars=500):
    """
    Walk-forward sur M5 après le signal : avance bougie par bougie jusqu'à
    ce que SL ou TP soit touché.

    Hypothèse conservatrice si SL et TP sont touchés dans la MÊME bougie M5 :
    on considère le SL touché en premier (pire cas, évite un biais optimiste
    côté backtest — cohérent avec la prudence habituelle du projet).

    Retourne (result, r_multiple) où result in {'WIN','LOSS','NO_FILL','TIMEOUT'}.
    NO_FILL : le prix n'a jamais atteint le niveau d'entrée (ordre LIMIT jamais rempli).
    TIMEOUT : ni SL ni TP touché dans max_bars bougies (fenêtre 48h ~ 576 bougies M5,
    cohérent avec le pending_order_expiry_h=48 en prod ; max_bars=500 par défaut
    laisse une marge de sécurité en dessous).
    """
    entry, sl, tp = signal['entry'], signal['sl'], signal['tp']
    is_buy = signal['signal_type'] == 'BUY_LIMIT'

    filled = False
    for i, c in enumerate(m5_after[:max_bars]):
        if not filled:
            # Ordre LIMIT : rempli si le prix revient au niveau d'entrée
            if is_buy and c['low'] <= entry:
                filled = True
            elif not is_buy and c['high'] >= entry:
                filled = True
            if not filled:
                continue

        if is_buy:
            hit_sl = c['low'] <= sl
            hit_tp = c['high'] >= tp
        else:
            hit_sl = c['high'] >= sl
            hit_tp = c['low'] <= tp

        if hit_sl and hit_tp:
            return 'LOSS', -1.0  # hypothèse conservatrice : SL en premier
        elif hit_sl:
            return 'LOSS', -1.0
        elif hit_tp:
            return 'WIN', signal['rr']

    if not filled:
        return 'NO_FILL', 0.0
    return 'TIMEOUT', 0.0


def calc_kelly(wr, rr):
    if rr <= 0:
        return 0
    return max(0, (wr * rr - (1 - wr)) / rr)


def run_backtest(conn, start_date, end_date):
    m5 = load_candles(conn, 'M5', start_date, end_date)
    h1 = load_candles(conn, 'H1', start_date, end_date)
    h4 = load_candles(conn, 'H4', start_date, end_date)
    pd_arrays = load_pd_arrays(conn, start_date, end_date)

    m5_by_time = m5  # deja trie par candle_time ASC

    trades = []
    reasons = {}

    for i in range(LOOKBACK_H1, len(h1)):
        h1_window = h1[max(0, i - LOOKBACK_H1):i + 1]
        t_now = h1_window[-1]['candle_time']
        struct_h1 = analyze_structure(h1_window, window_size=3)
        bias = struct_h1['bias']

        if not bias or bias == 'NEUTRAL':
            continue

        h4_window = [c for c in h4 if c['candle_time'] <= t_now][-LOOKBACK_H4:]
        if len(h4_window) < 7:
            continue
        struct_h4 = analyze_structure(h4_window, window_size=3)
        if struct_h4['bias'] != bias:
            reasons['pas_de_confluence'] = reasons.get('pas_de_confluence', 0) + 1
            continue

        current_price = h1_window[-1]['close']
        active_arrays = [
            a for a in pd_arrays
            if a['candle_time'] <= t_now
            and (a['invalidated_at'] is None or a['invalidated_at'] > t_now)
        ]

        sig, reason = try_build_signal(bias, current_price, active_arrays, struct_h1)
        if not sig:
            reasons[reason] = reasons.get(reason, 0) + 1
            continue

        m5_after = [c for c in m5_by_time if c['candle_time'] > t_now]
        result, r_multiple = simulate_trade_outcome(sig, m5_after)
        if result in ('WIN', 'LOSS'):
            trades.append({**sig, 'time': t_now, 'result': result, 'r_multiple': r_multiple})
        else:
            reasons[result.lower()] = reasons.get(result.lower(), 0) + 1

    return trades, reasons


def compute_metrics(trades):
    wins = [t for t in trades if t['result'] == 'WIN']
    losses = [t for t in trades if t['result'] == 'LOSS']
    total = len(wins) + len(losses)
    if total == 0:
        return {'nb_trades': 0, 'wr': 0.0, 'avg_rr': 0.0, 'kelly': 0.0}

    wr = len(wins) / total
    avg_win_r = sum(t['r_multiple'] for t in wins) / len(wins) if wins else 0.0
    avg_loss_r = sum(t['r_multiple'] for t in losses) / len(losses) if losses else -1.0
    rr = abs(avg_win_r / avg_loss_r) if avg_loss_r != 0 else 0.0
    kelly = calc_kelly(wr, rr)

    return {'nb_trades': total, 'wr': round(wr * 100, 2), 'avg_rr': round(rr, 2), 'kelly': round(kelly, 4)}


def get_active_baseline(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM test_baselines WHERE is_active = true")
        return cur.fetchone()


def compute_diff(metrics, baseline):
    diff = {}
    for key, baseline_key in (('wr', 'wr'), ('avg_rr', 'avg_rr'), ('kelly', 'kelly')):
        current = float(metrics[key])
        ref = float(baseline[baseline_key]) if baseline[baseline_key] is not None else 0.0
        delta = current - ref
        delta_pct = (delta / ref * 100) if ref != 0 else None
        diff[f'{key}_delta'] = round(delta, 4)
        diff[f'{key}_delta_pct'] = round(delta_pct, 2) if delta_pct is not None else None
    return diff


def determine_status(diff):
    kelly_delta_pct = diff.get('kelly_delta_pct')
    kelly_delta_abs = diff.get('kelly_delta')

    if kelly_delta_pct is not None:
        if kelly_delta_pct <= KELLY_DRIFT_FAIL_PCT:
            return 'fail'
        if kelly_delta_pct <= KELLY_DRIFT_WARN_PCT:
            return 'warn'
        return 'pass'

    # Baseline a Kelly=0 (ou reference nulle) -> pct indefini, repli sur le delta absolu.
    if kelly_delta_abs is not None:
        if kelly_delta_abs <= KELLY_DRIFT_ABS_FAIL:
            return 'fail'
        if kelly_delta_abs <= KELLY_DRIFT_ABS_WARN:
            return 'warn'
        return 'pass'

    return 'warn'


def get_git_commit():
    try:
        out = subprocess.run(
            ['git', 'rev-parse', 'HEAD'],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            capture_output=True, text=True, timeout=5,
        )
        return out.stdout.strip()[:40] if out.returncode == 0 else None
    except Exception:
        return None


def create_kanboard_ticket(status, metrics, diff, baseline_id):
    import urllib.request
    payload = {
        "jsonrpc": "2.0",
        "method": "createTask",
        "id": 1,
        "params": {
            "title": f"[backtest_ci] Derive detectee ({status}) vs baseline #{baseline_id}",
            "project_id": KANBOARD_PROJECT_ID,
            "column_id": KANBOARD_COLUMN_OUVERT,
            "description": (
                f"Run backtest_ci : WR={metrics['wr']}% RR={metrics['avg_rr']} Kelly={metrics['kelly']} "
                f"(nb_trades={metrics['nb_trades']})\n"
                f"Diff vs baseline #{baseline_id} : {json.dumps(diff, ensure_ascii=False)}\n"
                f"Seuils (non calibres, provisoires) : warn={KELLY_DRIFT_WARN_PCT}% fail={KELLY_DRIFT_FAIL_PCT}%"
            ),
        },
    }
    req = urllib.request.Request(
        KANBOARD_URL, data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"}, method="POST",
    )
    import base64
    auth = base64.b64encode(f"jsonrpc:{KANBOARD_TOKEN}".encode()).decode()
    req.add_header("Authorization", f"Basic {auth}")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            return result.get("result")
    except Exception as e:
        print(f"WARN: creation ticket Kanboard echouee: {e}", file=sys.stderr)
        return None


def send_telegram(msg):
    import urllib.request
    import urllib.parse
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = urllib.parse.urlencode({"chat_id": TELEGRAM_CHAT, "text": msg}).encode()
    try:
        urllib.request.urlopen(url, data=data, timeout=10)
        return True
    except Exception as e:
        print(f"WARN: notification Telegram echouee: {e}", file=sys.stderr)
        return False


def write_report(conn, test_type, baseline_id, status, metrics, diff, kanboard_ticket_id, notified):
    with conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO test_reports
                (test_type, baseline_id, git_commit_hash, status, nb_trades, wr, avg_rr, kelly,
                 diff_vs_baseline, kanboard_ticket_id, notified_telegram)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING report_id
            """,
            (
                test_type, baseline_id, get_git_commit(), status,
                metrics['nb_trades'], metrics['wr'], metrics['avg_rr'], metrics['kelly'],
                json.dumps(diff) if diff else None, kanboard_ticket_id, notified,
            ),
        )
        return cur.fetchone()[0]


def already_notified_today(conn, baseline_id, status):
    """
    Idempotence : si un report du jour avec le meme baseline et un statut
    warn/fail a deja notifie (ticket cree ou telegram envoye), on ne
    renotifie pas. Le cron peut etre relance sans spammer Kanboard/Telegram.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM test_reports
            WHERE test_type = 'backtest_ci' AND baseline_id = %s
              AND status IN ('warn', 'fail')
              AND run_at::date = CURRENT_DATE
              AND (kanboard_ticket_id IS NOT NULL OR notified_telegram = true)
            LIMIT 1
            """,
            (baseline_id,),
        )
        return cur.fetchone() is not None


def main():
    parser = argparse.ArgumentParser(description="Backtest CI quotidien vs baseline actif")
    parser.add_argument("--dataset-start", type=str, default=None, help="YYYY-MM-DD (defaut: fenetre du baseline actif)")
    parser.add_argument("--dataset-end", type=str, default=None, help="YYYY-MM-DD (defaut: fenetre du baseline actif)")
    parser.add_argument("--dry-run", action="store_true", help="N'ecrit rien en DB, ne notifie rien")
    parser.add_argument("--no-notify", action="store_true", help="Ecrit le rapport mais ne notifie pas")
    args = parser.parse_args()

    conn = get_connection()
    try:
        baseline = get_active_baseline(conn)
        if baseline is None:
            print("ERREUR: aucun baseline actif. Lancer baseline_manager.py --activate <id> d'abord.", file=sys.stderr)
            sys.exit(1)

        start_date = args.dataset_start or baseline['dataset_start'].strftime('%Y-%m-%d')
        end_date = args.dataset_end or baseline['dataset_end'].strftime('%Y-%m-%d')

        print(f"Backtest CI | fenetre {start_date} -> {end_date} | baseline actif #{baseline['baseline_id']}")

        trades, reasons = run_backtest(conn, start_date, end_date)
        metrics = compute_metrics(trades)
        diff = compute_diff(metrics, baseline)
        status = determine_status(diff)

        print(f"Trades: {metrics['nb_trades']} | WR: {metrics['wr']}% | RR: {metrics['avg_rr']} | Kelly: {metrics['kelly']}")
        print(f"Diff vs baseline #{baseline['baseline_id']}: {json.dumps(diff, ensure_ascii=False)}")
        print(f"Statut: {status}")
        if reasons:
            print("Raisons de rejet (top 5):")
            for k, v in sorted(reasons.items(), key=lambda x: -x[1])[:5]:
                print(f"  {k}: {v}")

        if args.dry_run:
            print("\n--dry-run: aucune ecriture, aucune notification.")
            return

        kanboard_ticket_id = None
        notified = False
        if status in ('warn', 'fail') and not args.no_notify:
            if already_notified_today(conn, baseline['baseline_id'], status):
                print(f"\nDerive deja notifiee aujourd'hui pour ce baseline — pas de nouvelle alerte (idempotence).")
            else:
                kanboard_ticket_id = create_kanboard_ticket(status, metrics, diff, baseline['baseline_id'])
                telegram_ok = send_telegram(
                    f"[backtest_ci] Derive {status.upper()} vs baseline #{baseline['baseline_id']}\n"
                    f"WR={metrics['wr']}% RR={metrics['avg_rr']} Kelly={metrics['kelly']}\n"
                    f"Diff: {json.dumps(diff, ensure_ascii=False)}"
                )
                # notified_telegram reflete le succes reel de l'envoi, pas seulement
                # la tentative — sinon un report marque "notifie" alors que le
                # message n'est jamais parti (ex. panne reseau) induirait N'Koy en erreur.
                notified = telegram_ok

        report_id = write_report(conn, 'backtest_ci', baseline['baseline_id'], status, metrics, diff,
                                  kanboard_ticket_id, notified)
        print(f"\nRapport #{report_id} ecrit dans test_reports.")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
