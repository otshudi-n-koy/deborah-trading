#!/usr/bin/env python3
"""
cTrader Executor v2 - SMC
Exécute les ordres sur cTrader via Protobuf SDK
Demo account : 4253346 (Pepperstone Europe)
"""

import psycopg2
import logging
import os
import sys
import time
from datetime import datetime

LOG_FILE = '/opt/deborah-trading/scripts/ctrader_executor.log'
logger = logging.getLogger('ctrader')
logger.setLevel(logging.INFO)
if not logger.handlers:
    fh = logging.FileHandler(LOG_FILE)
    fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logger.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logger.addHandler(sh)

DB_CONFIG = {
    "host": "localhost", "port": 5432,
    "dbname": "trading", "user": "trading", "password": "Trading2026"
}

CLIENT_ID     = "28221_xsMIPF9Cc99ufPI1rrsEeiF3J3NeI6pKw4f9ZQrRamNPDGlvPw"
CLIENT_SECRET = "tq2aoMRq7MMEMHuJ1H2NstGvkPD4RZ2pgzENpCAMWIkNIMrYzk"
ACCOUNT_ID    = 47331566

# Résultat global pour la communication entre callbacks et main
_result = {}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def get_config() -> dict:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT name, value FROM config_smc")
    config = {row[0]: row[1] for row in cur.fetchall()}
    cur.close(); conn.close()
    return config


def place_order_sync(signal: dict) -> dict:
    """
    Place un ordre sur cTrader de manière synchrone.
    Utilise Twisted reactor avec timeout.
    """
    from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
    from ctrader_open_api.messages.OpenApiMessages_pb2 import (
        ProtoOAApplicationAuthReq, ProtoOAApplicationAuthRes,
        ProtoOAAccountAuthReq, ProtoOAAccountAuthRes,
        ProtoOANewOrderReq, ProtoOAExecutionEvent,
        ProtoOAErrorRes, ProtoOAOrderErrorEvent
    )
    from twisted.internet import reactor

    config  = get_config()
    token   = config.get('ctrader_access_token')
    result  = {'done': False, 'success': False, 'data': {}}

    # Direction
    direction = 'BUY' if signal['type'] in ('BUY', 'BUY_LIMIT') else 'SELL'

    # Volume : 1 lot = 100,000 unités → en centièmes = * 100
    lots   = float(signal['lot_size'])
    volume = int(lots * 100000 * 100)

    entry = float(signal['entry_price'])
    sl    = float(signal['sl_price'])
    tp    = float(signal['tp_price'])

    # SL/TP en points relatifs (1 pip EUR/USD = 10 points)
    if direction == 'SELL':
        sl_pips = round((sl - entry) * 10000, 1)
        tp_pips = round((entry - tp) * 10000, 1)
    else:
        sl_pips = round((entry - sl) * 10000, 1)
        tp_pips = round((tp - entry) * 10000, 1)

    steps = [0]  # 0=init, 1=app_auth, 2=acc_auth, 3=order_sent

    def on_message(client, message):
        msg_type = message.payloadType

        if msg_type == ProtoOAApplicationAuthRes().payloadType:
            steps[0] = 1
            auth = ProtoOAAccountAuthReq()
            auth.ctidTraderAccountId = ACCOUNT_ID
            auth.accessToken = token
            client.send(auth)

        elif msg_type == ProtoOAAccountAuthRes().payloadType:
            steps[0] = 2
            logger.info(f"Compte {ACCOUNT_ID} authentifié — envoi ordre {direction} {lots}L EURUSD")

            order = ProtoOANewOrderReq()
            order.ctidTraderAccountId = ACCOUNT_ID
            order.symbolName          = "EURUSD"
            order.orderType           = 1  # MARKET
            order.tradeSide           = 2 if direction == 'SELL' else 1  # 1=BUY 2=SELL
            order.volume              = volume
            order.stopLoss            = round(sl, 5)
            order.takeProfit          = round(tp, 5)
            order.comment             = f"SMC_{signal['id']}"
            order.label               = f"SMC_{signal['id']}"
            client.send(order)
            steps[0] = 3

        elif msg_type == ProtoOAExecutionEvent().payloadType:
            r = Protobuf.extract(message)
            result['success'] = True
            result['data'] = {
                'order_id':    r.order.orderId if r.HasField('order') else None,
                'position_id': r.position.positionId if r.HasField('position') else None,
                'exec_type':   r.executionType,
                'volume':      volume,
                'direction':   direction,
            }
            logger.info(f"Ordre exécuté: {result['data']}")
            result['done'] = True
            if reactor.running:
                reactor.callFromThread(reactor.stop)

        elif msg_type == ProtoOAErrorRes().payloadType:
            r = Protobuf.extract(message)
            result['error'] = f"{r.errorCode}: {r.description}"
            logger.error(f"Erreur ordre: {result['error']}")
            result['done'] = True
            if reactor.running:
                reactor.callFromThread(reactor.stop)

        elif msg_type == ProtoOAOrderErrorEvent().payloadType:
            r = Protobuf.extract(message)
            result['error'] = f"OrderError: {r.errorCode}"
            logger.error(f"Erreur ordre: {result['error']}")
            result['done'] = True
            if reactor.running:
                reactor.callFromThread(reactor.stop)

    def on_connected(client):
        req = ProtoOAApplicationAuthReq()
        req.clientId     = CLIENT_ID
        req.clientSecret = CLIENT_SECRET
        client.send(req)

    def on_disconnected(client, reason):
        if not result['done']:
            result['error'] = f"Déconnecté: {reason}"
            result['done']  = True
        if reactor.running:
            reactor.callFromThread(reactor.stop)

    # Timeout 30s
    def timeout():
        if not result['done']:
            result['error'] = 'Timeout 30s'
            result['done']  = True
        if reactor.running:
            reactor.stop()

    client = Client(EndPoints.PROTOBUF_DEMO_HOST, EndPoints.PROTOBUF_PORT, TcpProtocol)
    client.setConnectedCallback(on_connected)
    client.setDisconnectedCallback(on_disconnected)
    client.setMessageReceivedCallback(on_message)
    client.startService()

    reactor.callLater(30, timeout)
    reactor.run()

    return result


def execute_pending_signals():
    """Exécute les signaux filled non encore envoyés à cTrader"""
    conn = get_conn()
    cur  = conn.cursor()

    cur.execute("""
        SELECT id, type, entry_price, sl_price, tp_price,
               lot_size, rr_ratio, killzone
        FROM signals_smc
        WHERE status = 'filled'
        AND mt5_ticket IS NULL
        ORDER BY created_at DESC LIMIT 1
    """)
    signals = cur.fetchall()

    if not signals:
        logger.info("Aucun signal à exécuter sur cTrader")
        cur.close(); conn.close()
        return

    for sig in signals:
        signal = {
            'id': sig[0], 'type': sig[1], 'entry_price': sig[2],
            'sl_price': sig[3], 'tp_price': sig[4],
            'lot_size': sig[5], 'rr_ratio': sig[6], 'killzone': sig[7]
        }
        logger.info(f"Exécution signal {signal['id']} sur cTrader...")
        result = place_order_sync(signal)

        if result.get('success'):
            ticket = str(result['data'].get('position_id') or result['data'].get('order_id'))
            cur.execute(
                "UPDATE signals_smc SET mt5_ticket=%s WHERE id=%s",
                (ticket, signal['id'])
            )
            conn.commit()
            logger.info(f"Signal {signal['id']} exécuté — ticket={ticket}")
        else:
            logger.error(f"Échec signal {signal['id']}: {result.get('error')}")

    cur.close(); conn.close()


def test_place_order():
    """Test : place un ordre SELL EURUSD 0.01L sur le compte demo"""
    logger.info("=== TEST place_order ===")
    signal = {
        'id': 999,
        'type': 'SELL',
        'entry_price': 1.16000,
        'sl_price':    1.16050,
        'tp_price':    1.15800,
        'lot_size':    0.01,
        'rr_ratio':    4.0,
        'killzone':    'TEST'
    }
    result = place_order_sync(signal)
    if result.get('success'):
        logger.info(f"✅ TEST OK — order_id={result['data']}")
    else:
        logger.error(f"❌ TEST FAILED — {result.get('error')}")
    return result


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        test_place_order()
    else:
        execute_pending_signals()
