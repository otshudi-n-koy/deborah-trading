#!/usr/bin/env python3
import os, logging
from flask import Flask, request, jsonify
from dotenv import load_dotenv

load_dotenv('/root/ctrader-mcp-server/.env')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger(__name__)

SERVICE_PORT = int(os.getenv('SERVICE_PORT', '8003'))
API_TOKEN    = os.getenv('API_TOKEN', 'Trading2026')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN', '')
ACCOUNT_ID   = int(os.getenv('ACCOUNT_ID', '0'))
HOST_TYPE    = os.getenv('HOST', 'demo')

app = Flask(__name__)

@app.route('/health')
def health():
    return jsonify({
        'status': 'waiting_token' if not ACCESS_TOKEN else 'ready',
        'token': bool(ACCESS_TOKEN),
        'account': ACCOUNT_ID,
        'host': HOST_TYPE
    })

@app.route('/order', methods=['POST'])
def order():
    if request.headers.get('X-Token') != API_TOKEN:
        return jsonify({'error': 'unauthorized'}), 401
    if not ACCESS_TOKEN:
        return jsonify({'error': 'ACCESS_TOKEN manquant - approbation cTrader en attente'}), 503
    return jsonify({'status': 'not_implemented_yet'}), 501

if __name__ == '__main__':
    log.info(f"cTrader Service - port {SERVICE_PORT} - token: {'OK' if ACCESS_TOKEN else 'MANQUANT'}")
    app.run(host='0.0.0.0', port=SERVICE_PORT, debug=False)
