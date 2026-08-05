#!/usr/bin/env python3
"""Recupere la liste des symboles disponibles pour un compte cTrader donne,
et cherche EURUSD dedans. Utile pour verifier le symbolId d'un nouveau compte."""
import sys, json
sys.path.insert(0, '/opt/deborah-trading/scripts')

def get_symbols(account_id):
    from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
    from ctrader_open_api.messages.OpenApiMessages_pb2 import (
        ProtoOAApplicationAuthReq, ProtoOAApplicationAuthRes,
        ProtoOAAccountAuthReq, ProtoOAAccountAuthRes,
        ProtoOASymbolsListReq, ProtoOASymbolsListRes)
    from twisted.internet import reactor
    from ctrader_executor_v2 import get_access_token, CLIENT_ID, CLIENT_SECRET

    token = get_access_token()
    result = {'done': False, 'symbols': []}

    def on_message(client, message):
        if message.payloadType == ProtoOAApplicationAuthRes().payloadType:
            a = ProtoOAAccountAuthReq(); a.ctidTraderAccountId = account_id; a.accessToken = token
            client.send(a)
        elif message.payloadType == ProtoOAAccountAuthRes().payloadType:
            req = ProtoOASymbolsListReq(); req.ctidTraderAccountId = account_id
            client.send(req)
        elif message.payloadType == ProtoOASymbolsListRes().payloadType:
            r = Protobuf.extract(message)
            for s in r.symbol:
                if 'EURUSD' in s.symbolName:
                    result['symbols'].append({'symbolId': s.symbolId, 'symbolName': s.symbolName})
            result['done'] = True
            if reactor.running: reactor.callFromThread(reactor.stop)

    def on_connected(c):
        req = ProtoOAApplicationAuthReq(); req.clientId = CLIENT_ID; req.clientSecret = CLIENT_SECRET
        c.send(req)

    def on_disconnected(c, r):
        result['done'] = True
        if reactor.running: reactor.callFromThread(reactor.stop)

    reactor.callLater(15, reactor.stop)
    client = Client(EndPoints.PROTOBUF_DEMO_HOST, EndPoints.PROTOBUF_PORT, TcpProtocol)
    client.setConnectedCallback(on_connected)
    client.setDisconnectedCallback(on_disconnected)
    client.setMessageReceivedCallback(on_message)
    client.startService()
    reactor.run()
    return result['symbols']

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("usage: get_symbol_id.py <account_id>")
        sys.exit(1)
    symbols = get_symbols(int(sys.argv[1]))
    print(json.dumps(symbols, indent=2))
