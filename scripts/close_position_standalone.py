#!/usr/bin/env python3
"""Ferme une position cTrader. Appele en subprocess pour eviter le conflit
ReactorNotRestartable avec d'autres appels Twisted dans le meme processus."""
import sys, json
sys.path.insert(0, '/opt/deborah-trading/scripts')
from ctrader_executor_v2 import close_position_full

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print(json.dumps({'success': False, 'error': 'usage: close_position_standalone.py <position_id> <lots>'}))
        sys.exit(2)
    try:
        position_id = int(sys.argv[1])
        lots = float(sys.argv[2])
        result = close_position_full(position_id, lots)
        print(json.dumps({'success': bool(result.get('success')), 'error': result.get('error')}))
        sys.exit(0 if result.get('success') else 1)
    except Exception as e:
        print(json.dumps({'success': False, 'error': str(e)}))
        sys.exit(2)
