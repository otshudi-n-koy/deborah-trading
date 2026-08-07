#!/usr/bin/env python3
"""
cTrader Price Collector
Récupère les bougies M5 et H1 EURUSD depuis cTrader Open API
et les insère dans prices_smc (PostgreSQL)
Remplace TwelveData comme source de prix.
"""

import os
import sys
import time
import logging
import psycopg2
from datetime import datetime, timezone
from twisted.internet import reactor, defer
from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import *
from ctrader_open_api.messages.OpenApiMessages_pb2 import *
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import *
from dotenv import load_dotenv

load_dotenv('/root/ctrader-mcp-server/.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler('/var/log/ctrader_collector.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# Config
CLIENT_ID     = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
ACCESS_TOKEN  = os.getenv('ACCESS_TOKEN')
ACCOUNT_ID    = int(os.getenv('ACCOUNT_ID', '0'))
HOST_TYPE     = os.getenv('HOST', 'demo').lower()

DB_HOST = 'localhost'
DB_NAME = 'trading'
DB_USER = 'trading'
DB_PASS = 'trading'
DB_PORT = 5432

SYMBOL_NAME = 'EURUSD'
TIMEFRAMES  = ['M1', 'M5', 'M15', 'H1', 'H4', 'D1']
# M1 ajoute le 07/08/2026 : le mapping TIMEFRAME_MAP/PERIOD_MS existait deja
# mais M1 n'etait jamais collecte. Objectif : accumuler un historique M1
# exploitable pour backtester un cron signal_generator.py a 1 minute
# (actuellement 5 min, hypothese que des zones PD array de duree de vie
# courte - 5-10 min observees le 07/08 - echappent au cron actuel).
CANDLES_COUNT = 100  # bougies à récupérer

TIMEFRAME_MAP = {
    'M1':  ProtoOATrendbarPeriod.M1,
    'M5':  ProtoOATrendbarPeriod.M5,
    'M15': ProtoOATrendbarPeriod.M15,
    'M30': ProtoOATrendbarPeriod.M30,
    'H1':  ProtoOATrendbarPeriod.H1,
    'H4':  ProtoOATrendbarPeriod.H4,
    'D1':  ProtoOATrendbarPeriod.D1,
    'W1':  ProtoOATrendbarPeriod.W1,
    'MN1': ProtoOATrendbarPeriod.MN1,
}

PERIOD_MS = {
    'M1': 60_000, 'M5': 300_000, 'M15': 900_000, 'M30': 1_800_000,
    'H1': 3_600_000, 'H4': 14_400_000, 'D1': 86_400_000, 'W1': 604_800_000, 'MN1': 2_592_000_000,
}


def get_db():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASS, port=DB_PORT
    )


def insert_candles(candles, timeframe):
    """Insère les bougies dans prices_smc avec ON CONFLICT DO NOTHING"""
    if not candles:
        return 0
    conn = get_db()
    cur = conn.cursor()
    inserted = 0
    for c in candles:
        try:
            cur.execute("""
                INSERT INTO prices_smc (timeframe, candle_time, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (timeframe, candle_time) DO NOTHING
            """, (
                timeframe,
                c['timestamp'],
                round(c['open'], 5),
                round(c['high'], 5),
                round(c['low'], 5),
                round(c['close'], 5),
                int(c.get('volume', 0))
            ))
            if cur.rowcount > 0:
                inserted += 1
        except Exception as e:
            log.error(f"Insert error: {e}")
    conn.commit()
    cur.close()
    conn.close()
    return inserted


class CTraderCollector:

    def __init__(self):
        self.client = None
        self.symbol_id = None
        self.symbols = {}
        self.pending_requests = {}
        self.results = {}
        self.authenticated = False

    def run(self):
        host = EndPoints.PROTOBUF_LIVE_HOST if HOST_TYPE == 'live' else EndPoints.PROTOBUF_DEMO_HOST
        self.client = Client(host, EndPoints.PROTOBUF_PORT, TcpProtocol)
        self.client.setConnectedCallback(self._on_connected)
        self.client.setDisconnectedCallback(self._on_disconnected)
        self.client.setMessageReceivedCallback(self._on_message)
        self.client.startService()
        reactor.run()

    def _on_connected(self, client):
        log.info("Connecté à cTrader")
        req = ProtoOAApplicationAuthReq()
        req.clientId = CLIENT_ID
        req.clientSecret = CLIENT_SECRET
        client.send(req)

    def _on_disconnected(self, client, reason):
        log.warning(f"Déconnecté: {reason}")
        if reactor.running:
            reactor.stop()

    def _on_message(self, client, message):
        try:
            pt = message.payloadType

            if pt == ProtoOAApplicationAuthRes().payloadType:
                log.info("App authentifiée")
                req = ProtoOAAccountAuthReq()
                req.ctidTraderAccountId = ACCOUNT_ID
                req.accessToken = ACCESS_TOKEN
                client.send(req)

            elif pt == ProtoOAAccountAuthRes().payloadType:
                log.info("Compte authentifié")
                self.authenticated = True
                self._request_symbols()

            elif pt == ProtoOASymbolsListRes().payloadType:
                resp = Protobuf.extract(message)
                for s in resp.symbol:
                    self.symbols[s.symbolName] = {
                        'id': s.symbolId,
                        'digits': getattr(s, 'digits', 5)
                    }
                log.info(f"{len(self.symbols)} symboles chargés")
                sym = self.symbols.get(SYMBOL_NAME)
                if not sym:
                    log.error(f"{SYMBOL_NAME} non trouvé")
                    reactor.stop()
                    return
                self.symbol_id = sym['id']
                self.digits = sym.get('digits', 5)
                self._request_all_candles()

            elif pt == ProtoOAGetTrendbarsRes().payloadType:
                resp = Protobuf.extract(message)
                tf = self.pending_requests.get(resp.symbolId)
                if tf is None:
                    # Chercher par période
                    for k, v in self.pending_requests.items():
                        tf = v
                        break
                self._handle_trendbars(resp, tf)

        except Exception as e:
            log.error(f"Erreur message: {e}", exc_info=True)

    def _request_symbols(self):
        req = ProtoOASymbolsListReq()
        req.ctidTraderAccountId = ACCOUNT_ID
        self.client.send(req)

    def _request_all_candles(self):
        """Lance les requêtes pour M5 et H1"""
        self._tf_queue = list(TIMEFRAMES)
        self._request_next_tf()

    def _request_next_tf(self):
        if not self._tf_queue:
            self._finish()
            return
        tf = self._tf_queue.pop(0)
        self._current_tf = tf
        log.info(f"Requête {SYMBOL_NAME} {tf}...")
        now_ms = int(time.time() * 1000)
        req = ProtoOAGetTrendbarsReq()
        req.ctidTraderAccountId = ACCOUNT_ID
        req.symbolId = self.symbol_id
        req.period = TIMEFRAME_MAP[tf]
        req.fromTimestamp = now_ms - (CANDLES_COUNT * PERIOD_MS[tf])
        req.toTimestamp = now_ms
        req.count = CANDLES_COUNT
        self.client.send(req)

    def _handle_trendbars(self, resp, tf=None):
        if tf is None:
            tf = self._current_tf
        divisor = 10 ** self.digits
        candles = []
        for bar in resp.trendbar:
            try:
                low = bar.low / divisor
                ts = datetime.fromtimestamp(
                    bar.utcTimestampInMinutes * 60,
                    tz=timezone.utc
                ).replace(tzinfo=None)
                candles.append({
                    'timestamp': ts,
                    'open':  low + (getattr(bar, 'deltaOpen',  0) / divisor),
                    'high':  low + (getattr(bar, 'deltaHigh',  0) / divisor),
                    'low':   low,
                    'close': low + (getattr(bar, 'deltaClose', 0) / divisor),
                    'volume': getattr(bar, 'volume', 0)
                })
            except Exception as e:
                log.warning(f"Bar skip: {e}")

        inserted = insert_candles(candles, tf)
        log.info(f"{tf}: {len(candles)} bougies récupérées, {inserted} nouvelles insérées")
        self.results[tf] = {'total': len(candles), 'inserted': inserted}

        # Bougie la plus récente pour log
        if candles:
            last = sorted(candles, key=lambda x: x['timestamp'])[-1]
            log.info(f"{tf} dernière bougie: {last['timestamp']} O={last['open']:.5f} H={last['high']:.5f} L={last['low']:.5f} C={last['close']:.5f}")

        # Prochaine requête
        reactor.callLater(1, self._request_next_tf)

    def _finish(self):
        log.info("=== Collecte terminée ===")
        for tf, r in self.results.items():
            log.info(f"  {tf}: {r['total']} bougies, {r['inserted']} nouvelles")
        reactor.callLater(0.5, reactor.stop)


def main():
    if not all([CLIENT_ID, CLIENT_SECRET, ACCESS_TOKEN, ACCOUNT_ID]):
        log.error("Credentials manquants dans .env")
        sys.exit(1)

    log.info(f"=== cTrader Collector — {SYMBOL_NAME} ===")
    log.info(f"Host: {HOST_TYPE} | Account: {ACCOUNT_ID}")

    collector = CTraderCollector()
    collector.run()


if __name__ == '__main__':
    main()
# DEBUG
