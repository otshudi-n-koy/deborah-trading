#!/usr/bin/env python3
"""Annule un ordre pending sur cTrader. Appele en subprocess pour eviter
le conflit ReactorNotRestartable avec d'autres appels Twisted (meme pattern
que check_close_confirmed.py, amend_sl_standalone.py)."""
import sys, json
sys.path.insert(0, '/opt/deborah-trading/scripts')
from ctrader_executor_v2 import cancel_order

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(json.dumps({'success': False, 'error': 'usage: cancel_order_standalone.py <order_id>'}))
        sys.exit(2)
    try:
        order_id = int(sys.argv[1])
        result = cancel_order(order_id)
        print(json.dumps({'success': bool(result.get('success')), 'error': result.get('error')}))
        sys.exit(0 if result.get('success') else 1)
    except Exception as e:
        print(json.dumps({'success': False, 'error': str(e)}))
        sys.exit(2)
