#!/usr/bin/env python3
"""cTrader Executor v2 — SMC/ICT — Ouverture LIMIT, fermeture partielle, amendment SL"""
import psycopg2, logging, sys, time, requests

LOG_FILE = '/opt/deborah-trading/scripts/ctrader_executor.log'
logger = logging.getLogger('ctrader_v2')
logger.setLevel(logging.INFO)
if not logger.handlers:
    fh = logging.FileHandler(LOG_FILE)
    fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logger.addHandler(fh)
    # StreamHandler retire (31/07/2026) : redondant avec la redirection
    # cron ">> ctrader_executor.log", causait un doublon de chaque ligne

DB_CONFIG = {"host":"localhost","port":5432,"dbname":"trading","user":"trading","password":"Trading2026"}
CLIENT_ID = "28221_xsMIPF9Cc99ufPI1rrsEeiF3J3NeI6pKw4f9ZQrRamNPDGlvPw"
CLIENT_SECRET = "tq2aoMRq7MMEMHuJ1H2NstGvkPD4RZ2pgzENpCAMWIkNIMrYzk"
ACCOUNTS = {'demo': 47331566, 'e8': 47580350, 'experimental': 47806214}
TELEGRAM_TOKEN = "8400529290:AAEyRzGa0JNCsuecpNJ6gQM8hnlt-ao"
TELEGRAM_CHAT = "1664221853"
# symbolId EURUSD selon le compte
# Pepperstone Demo 47331566 : symbolId=1 (EURUSD)
# E8 Opra Markets 47580350  : symbolId=184 (EURUSD+)
SYMBOL_IDS = {47331566: 1, 47580350: 184, 47806214: 184}  # 47806214 = E8 5K experimental (Opra Markets, meme symbolId que E8 25K)
EURUSD_SYMBOL_ID = 1  # sera overridé dans place_limit_order

def send_telegram(msg):
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id":TELEGRAM_CHAT,"text":msg,"parse_mode":"Markdown"},timeout=10)
    except Exception as e:
        logger.warning(f"Telegram: {e}")

def get_conn(): return psycopg2.connect(**DB_CONFIG)

def get_access_token():
    conn=get_conn(); cur=conn.cursor()
    cur.execute("SELECT value FROM config_smc WHERE name='ctrader_access_token'")
    row=cur.fetchone(); cur.close(); conn.close()
    return row[0] if row else None

def get_active_account_id():
    try:
        conn=get_conn(); cur=conn.cursor()
        cur.execute("SELECT value FROM config_smc WHERE name='ctrader_account_mode'")
        row=cur.fetchone(); cur.close(); conn.close()
        return ACCOUNTS.get(row[0] if row else 'demo', ACCOUNTS['demo'])
    except Exception:
        return ACCOUNTS['demo']

def run_ctrader_action(action_fn, timeout_sec=30):
    from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
    from ctrader_open_api.messages.OpenApiMessages_pb2 import (
        ProtoOAApplicationAuthReq, ProtoOAApplicationAuthRes,
        ProtoOAAccountAuthReq, ProtoOAAccountAuthRes,
        ProtoOAExecutionEvent, ProtoOAErrorRes, ProtoOAOrderErrorEvent)
    from twisted.internet import reactor
    token=get_access_token(); account_id=get_active_account_id()
    result={'done':False,'success':False,'data':{},'error':None}
    def on_message(client, message):
        mt=message.payloadType
        if mt==ProtoOAApplicationAuthRes().payloadType:
            a=ProtoOAAccountAuthReq(); a.ctidTraderAccountId=account_id; a.accessToken=token; client.send(a)
        elif mt==ProtoOAAccountAuthRes().payloadType:
            logger.info(f"Compte {account_id} authentifie"); action_fn(client, result)
        elif mt==ProtoOAExecutionEvent().payloadType:
            r=Protobuf.extract(message); result['success']=True
            result['data']={'order_id':r.order.orderId if r.HasField('order') else None,
                'position_id':r.position.positionId if r.HasField('position') else None}
            logger.info(f"Execution: {result['data']}"); result['done']=True
            if reactor.running: reactor.callFromThread(reactor.stop)
        elif mt in (ProtoOAErrorRes().payloadType, ProtoOAOrderErrorEvent().payloadType):
            r=Protobuf.extract(message)
            result['error']=getattr(r,'description',str(r.errorCode))
            logger.error(f"Erreur: {result['error']}"); result['done']=True
            if reactor.running: reactor.callFromThread(reactor.stop)
    def on_connected(client):
        req=ProtoOAApplicationAuthReq(); req.clientId=CLIENT_ID; req.clientSecret=CLIENT_SECRET; client.send(req)
    def on_disconnected(client, reason):
        if not result['done']: result['error']=f"Deconnecte: {reason}"; result['done']=True
        if reactor.running: reactor.callFromThread(reactor.stop)
    def on_timeout():
        if not result['done']: result['error']='Timeout'; result['done']=True
        if reactor.running: reactor.stop()
    client=Client(EndPoints.PROTOBUF_DEMO_HOST, EndPoints.PROTOBUF_PORT, TcpProtocol)
    client.setConnectedCallback(on_connected); client.setDisconnectedCallback(on_disconnected)
    client.setMessageReceivedCallback(on_message); client.startService()
    reactor.callLater(timeout_sec, on_timeout); reactor.run()
    return result

def place_limit_order(signal):
    from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOANewOrderReq
    account_id=get_active_account_id()
    direction='BUY' if signal['type'] in ('BUY','BUY_LIMIT') else 'SELL'
    volume=int(float(signal['lot_size'])*100000*100)
    entry=float(signal['entry_price']); sl=float(signal['sl_price']); tp=float(signal['tp_price'])
    def action(client, result):
        o=ProtoOANewOrderReq(); o.ctidTraderAccountId=account_id
        o.symbolId=SYMBOL_IDS.get(account_id, 1); o.orderType=2
        o.tradeSide=2 if direction=='SELL' else 1; o.volume=volume
        o.limitPrice=round(entry,5); o.stopLoss=round(sl,5); o.takeProfit=round(tp,5)
        o.comment=f"SMC_{signal['id']}"; o.label=f"SMC_{signal['id']}"
        client.send(o); logger.info(f"LIMIT {direction} {signal['lot_size']}L @ {entry}")
    return run_ctrader_action(action)

def close_position_partial(position_id, lots_to_close):
    from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOAClosePositionReq
    account_id=get_active_account_id(); volume=int(lots_to_close*100000*100)
    def action(client, result):
        req=ProtoOAClosePositionReq(); req.ctidTraderAccountId=account_id
        req.positionId=position_id; req.volume=volume; client.send(req)
        logger.info(f"Close partiel: pos={position_id} {lots_to_close}L")
    return run_ctrader_action(action)

def close_position_full(position_id, lots_total):
    return close_position_partial(position_id, lots_total)

def amend_sl(position_id, new_sl, new_tp=None):
    from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOAAmendPositionSLTPReq
    account_id=get_active_account_id()
    def action(client, result):
        req=ProtoOAAmendPositionSLTPReq(); req.ctidTraderAccountId=account_id
        req.positionId=position_id; req.stopLoss=round(new_sl,5)
        if new_tp: req.takeProfit=round(new_tp,5)
        client.send(req); logger.info(f"SL amende: pos={position_id} SL={new_sl}")
    return run_ctrader_action(action)

def cancel_order(order_id):
    from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOACancelOrderReq
    account_id=get_active_account_id()
    def action(client, result):
        req=ProtoOACancelOrderReq(); req.ctidTraderAccountId=account_id
        req.orderId=order_id; client.send(req)
        logger.info(f"Ordre annule: {order_id}")
    return run_ctrader_action(action)

def execute_pending_signals():
    conn=get_conn(); cur=conn.cursor()
    # Verifier bot_status avant execution
    cur.execute("SELECT bot_status FROM capital_smc WHERE id=1")
    bot_status = cur.fetchone()[0]
    if bot_status != 'ACTIVE':
        logger.info(f"Bot en {bot_status} — execution annulee")
        cur.close(); conn.close(); return
    cur.execute("""SELECT s.id,s.type,s.entry_price,s.sl_price,s.tp_price,s.lot_size,
        s.rr_ratio,s.killzone,p.status
        FROM signals_smc s
        LEFT JOIN pd_arrays_smc p ON s.pd_array_id = p.id
        WHERE s.status='pending' AND s.ctrader_order_id IS NULL
        ORDER BY s.created_at DESC LIMIT 1""")
    sigs=cur.fetchall()
    if not sigs: logger.info("Aucun signal pending"); cur.close(); conn.close(); return
    for sig in sigs:
        pd_status = sig[8]
        # Revalidation synchrone (bug #87) : la zone pd_array peut avoir ete
        # invalidee par pd_array_detector.py (cron 5min) pendant la fenetre
        # jusqu'a 60s separant l'insertion du signal et ce cron d'execution.
        if pd_status is not None and pd_status != 'active':
            cur.execute("UPDATE signals_smc SET status='expired', closed_at=NOW() WHERE id=%s", (sig[0],))
            conn.commit()
            logger.warning(f"Signal #{sig[0]} annule avant soumission - pd_array devenue {pd_status} entre insertion et execution")
            continue
        signal={'id':sig[0],'type':sig[1],'entry_price':sig[2],'sl_price':sig[3],
            'tp_price':sig[4],'lot_size':sig[5],'rr_ratio':sig[6],'killzone':sig[7]}
        result=place_limit_order(signal)
        if result.get('success'):
            order_id = str(result['data'].get('order_id') or '')
            # mt5_ticket n'est PAS renseigne ici : le position_id retourne a la pose
            # d'un ordre pending n'est qu'un pre-attribue par cTrader, pas une position
            # reellement confirmee (meme classe de bug que le ticket #3). Seul
            # sync_position_id() (appele au moment du FILL reel) doit ecrire mt5_ticket.
            cur.execute("UPDATE signals_smc SET ctrader_order_id=%s WHERE id=%s",
                (order_id, signal['id']))
            conn.commit()
            logger.info(f"Signal #{signal['id']} order_id={order_id} (pose, en attente de fill)")
            send_telegram(f"✅ *Ordre cTrader*\n#{signal['id']} {signal['type']}\nEntry:`{float(signal['entry_price']):.5f}` OrderID:`{order_id}`")
        else:
            logger.error(f"Echec #{signal['id']}: {result.get('error')}")
    cur.close(); conn.close()

def get_open_position_ids():
    """Retourne l'ensemble des positionId actuellement ouverts sur le compte actif."""
    from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
    from ctrader_open_api.messages.OpenApiMessages_pb2 import (
        ProtoOAApplicationAuthReq, ProtoOAApplicationAuthRes,
        ProtoOAAccountAuthReq, ProtoOAAccountAuthRes,
        ProtoOAReconcileReq, ProtoOAReconcileRes)
    from twisted.internet import reactor
    token = get_access_token()
    account_id = get_active_account_id()
    result = {'done': False, 'position_ids': set()}

    def on_message(client, message):
        if message.payloadType == ProtoOAApplicationAuthRes().payloadType:
            a = ProtoOAAccountAuthReq()
            a.ctidTraderAccountId = account_id
            a.accessToken = token
            client.send(a)
        elif message.payloadType == ProtoOAAccountAuthRes().payloadType:
            req = ProtoOAReconcileReq()
            req.ctidTraderAccountId = account_id
            client.send(req)
        elif message.payloadType == ProtoOAReconcileRes().payloadType:
            r = Protobuf.extract(message)
            for p in r.position:
                result['position_ids'].add(p.positionId)
            result['done'] = True
            if reactor.running:
                reactor.callFromThread(reactor.stop)

    def on_connected(c):
        req = ProtoOAApplicationAuthReq()
        req.clientId = CLIENT_ID
        req.clientSecret = CLIENT_SECRET
        c.send(req)

    def on_disconnected(c, r):
        result['done'] = True
        if reactor.running:
            reactor.callFromThread(reactor.stop)

    reactor.callLater(15, reactor.stop)
    client = Client(EndPoints.PROTOBUF_DEMO_HOST, EndPoints.PROTOBUF_PORT, TcpProtocol)
    client.setConnectedCallback(on_connected)
    client.setDisconnectedCallback(on_disconnected)
    client.setMessageReceivedCallback(on_message)
    client.startService()
    reactor.run()

    return result['position_ids']

def sync_position_id(signal_id):
    """Recupere le vrai position_id depuis cTrader apres fill"""
    from ctrader_open_api import Client,Protobuf,TcpProtocol,EndPoints
    from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOAApplicationAuthReq,ProtoOAApplicationAuthRes,ProtoOAAccountAuthReq,ProtoOAAccountAuthRes,ProtoOAReconcileReq,ProtoOAReconcileRes
    from twisted.internet import reactor
    token=get_access_token(); account_id=get_active_account_id()
    result={'done':False,'position_id':None}
    def on_message(client,message):
        if message.payloadType==ProtoOAApplicationAuthRes().payloadType:
            a=ProtoOAAccountAuthReq();a.ctidTraderAccountId=account_id;a.accessToken=token;client.send(a)
        elif message.payloadType==ProtoOAAccountAuthRes().payloadType:
            req=ProtoOAReconcileReq();req.ctidTraderAccountId=account_id;client.send(req)
        elif message.payloadType==ProtoOAReconcileRes().payloadType:
            r=Protobuf.extract(message)
            # Chercher dans positions ouvertes
            for p in r.position:
                if f"SMC_{signal_id}" in p.tradeData.label:
                    result['position_id']=p.positionId
                    logger.info(f"Signal #{signal_id} position_id={p.positionId} (position)")
                    break
            # Si pas trouve dans les positions reelles, l'ordre est encore
            # pending (non rempli) : ne PAS retourner un faux positif.
            if not result['position_id']:
                for o in r.order:
                    if f"SMC_{signal_id}" in o.tradeData.label:
                        logger.info(f"Signal #{signal_id} ordre encore pending (order_id={o.orderId}), pas de position confirmee")
                        break
            result['done']=True
            if reactor.running: reactor.callFromThread(reactor.stop)
    def on_connected(c):
        req=ProtoOAApplicationAuthReq();req.clientId=CLIENT_ID;req.clientSecret=CLIENT_SECRET;c.send(req)
    def on_disconnected(c,r):
        result['done']=True
        if reactor.running: reactor.callFromThread(reactor.stop)
    reactor.callLater(15,reactor.stop)
    client=Client(EndPoints.PROTOBUF_DEMO_HOST,EndPoints.PROTOBUF_PORT,TcpProtocol)
    client.setConnectedCallback(on_connected);client.setDisconnectedCallback(on_disconnected)
    client.setMessageReceivedCallback(on_message);client.startService();reactor.run()
    if result['position_id']:
        conn=get_conn();cur=conn.cursor()
        cur.execute("UPDATE signals_smc SET mt5_ticket=%s WHERE id=%s",(str(result['position_id']),signal_id))
        conn.commit();cur.close();conn.close()
        logger.info(f"Signal #{signal_id} mt5_ticket={result['position_id']}")
        return result['position_id']
    return None

def sync_real_pnl(signal_id):
    """Recupere le vrai PnL/close_price depuis cTrader et synchronise trades_smc."""
    from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
    from ctrader_open_api.messages.OpenApiMessages_pb2 import (
        ProtoOAApplicationAuthReq, ProtoOAApplicationAuthRes,
        ProtoOAAccountAuthReq, ProtoOAAccountAuthRes,
        ProtoOADealListByPositionIdReq, ProtoOADealListByPositionIdRes)
    from twisted.internet import reactor
    import time

    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT mt5_ticket FROM signals_smc WHERE id=%s", (signal_id,))
    row = cur.fetchone()
    cur.close(); conn.close()
    if not row or not row[0]:
        logger.warning(f"sync_real_pnl: signal #{signal_id} sans mt5_ticket, skip")
        return None
    position_id = int(row[0])
    token = get_access_token()
    account_id = get_active_account_id()
    result = {'done': False, 'close_deal': None}
    now_ms = int(time.time() * 1000)
    from_ts = now_ms - (30 * 24 * 60 * 60 * 1000)

    def on_message(client, message):
        if message.payloadType == ProtoOAApplicationAuthRes().payloadType:
            a = ProtoOAAccountAuthReq()
            a.ctidTraderAccountId = account_id
            a.accessToken = token
            client.send(a)
        elif message.payloadType == ProtoOAAccountAuthRes().payloadType:
            req = ProtoOADealListByPositionIdReq()
            req.ctidTraderAccountId = account_id
            req.positionId = position_id
            req.fromTimestamp = from_ts
            req.toTimestamp = now_ms
            client.send(req)
        elif message.payloadType == ProtoOADealListByPositionIdRes().payloadType:
            r = Protobuf.extract(message)
            for d in r.deal:
                if d.HasField('closePositionDetail'):
                    cpd = d.closePositionDetail
                    digits = cpd.moneyDigits if cpd.moneyDigits else 2
                    scale = 10 ** digits
                    pnl_net = (cpd.grossProfit - cpd.commission - cpd.swap) / scale
                    result['close_deal'] = {
                        'close_price': d.executionPrice,
                        'pnl_net': round(pnl_net, 2),
                        'balance_after': cpd.balance / scale,
                    }
                    logger.info(f"Signal #{signal_id} pos={position_id} deal_close: close_price={d.executionPrice} pnl_net={pnl_net:.2f}")
            result['done'] = True
            if reactor.running:
                reactor.callFromThread(reactor.stop)

    def on_connected(c):
        req = ProtoOAApplicationAuthReq()
        req.clientId = CLIENT_ID
        req.clientSecret = CLIENT_SECRET
        c.send(req)

    def on_disconnected(c, r):
        result['done'] = True
        if reactor.running:
            reactor.callFromThread(reactor.stop)

    reactor.callLater(20, reactor.stop)
    client = Client(EndPoints.PROTOBUF_DEMO_HOST, EndPoints.PROTOBUF_PORT, TcpProtocol)
    client.setConnectedCallback(on_connected)
    client.setDisconnectedCallback(on_disconnected)
    client.setMessageReceivedCallback(on_message)
    client.startService()
    reactor.run()

    if not result['close_deal']:
        logger.warning(f"sync_real_pnl: aucun closePositionDetail pour signal #{signal_id} (pos={position_id})")
        return None

    cd = result['close_deal']
    conn = get_conn(); cur = conn.cursor()
    cur.execute("UPDATE trades_smc SET pnl_eur=%s, close_price=%s WHERE signal_id=%s",
                (cd['pnl_net'], cd['close_price'], signal_id))
    rows_updated = cur.rowcount
    conn.commit(); cur.close(); conn.close()
    logger.info(f"Signal #{signal_id}: trades_smc maj (rows={rows_updated}) pnl_eur={cd['pnl_net']} close_price={cd['close_price']}")
    return cd

def sync_tp_palier(signal_id, palier, lots_closed, new_sl):
    conn=get_conn(); cur=conn.cursor()
    cur.execute("SELECT mt5_ticket FROM signals_smc WHERE id=%s",(signal_id,))
    row=cur.fetchone(); cur.close(); conn.close()
    if not row or not row[0]:
        logger.warning(f"#{signal_id}: pas de mt5_ticket"); return False
    pos_id=int(row[0])
    r1=close_position_partial(pos_id, lots_closed)
    if not r1.get('success'): logger.error(f"Close partiel echoue: {r1.get('error')}"); return False
    time.sleep(1)
    r2=amend_sl(pos_id, new_sl)
    if not r2.get('success'): logger.warning(f"Amend SL echoue: {r2.get('error')}")
    return True

if __name__=='__main__':
    cmd=sys.argv[1] if len(sys.argv)>1 else 'execute'
    if cmd=='execute': execute_pending_signals()
    elif cmd=='close' and len(sys.argv)>=4: print(close_position_full(int(sys.argv[2]),float(sys.argv[3])))
    elif cmd=='amend_sl' and len(sys.argv)>=4: print(amend_sl(int(sys.argv[2]),float(sys.argv[3])))
    elif cmd=='cancel' and len(sys.argv)>=3: print(cancel_order(int(sys.argv[2])))
    elif cmd=='test':
        logger.info("=== TEST connexion ===")
        def test_action(client, result):
            result['success']=True; result['done']=True
            logger.info("Connexion OK !")
            from twisted.internet import reactor
            if reactor.running: reactor.callFromThread(reactor.stop)
        print(run_ctrader_action(test_action))
    else: print("Usage: execute|close <pos_id> <lots>|amend_sl <pos_id> <sl>|cancel <order_id>|test")
