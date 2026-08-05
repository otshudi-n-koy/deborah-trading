#!/usr/bin/env python3
"""Script standalone pour amender le SL (et TP optionnel) d'une position cTrader.
Appele en subprocess pour eviter le conflit ReactorNotRestartable avec d'autres
appels Twisted dans le meme processus (meme pattern que check_close_confirmed.py)."""
import sys, json
sys.path.insert(0, '/opt/deborah-trading/scripts')
from ctrader_executor_v2 import amend_sl

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print(json.dumps({'success': False, 'error': 'usage: amend_sl_standalone.py <position_id> <new_sl> [new_tp]'}))
        sys.exit(2)
    try:
        position_id = int(sys.argv[1])
        new_sl = float(sys.argv[2])
        new_tp = float(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3] != 'None' else None
        result = amend_sl(position_id, new_sl, new_tp)
        print(json.dumps({'success': bool(result.get('success')), 'error': result.get('error')}))
        sys.exit(0 if result.get('success') else 1)
    except Exception as e:
        print(json.dumps({'success': False, 'error': str(e)}))
        sys.exit(2)
