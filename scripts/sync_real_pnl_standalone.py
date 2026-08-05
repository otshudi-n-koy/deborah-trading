#!/usr/bin/env python3
"""Script standalone pour sync_real_pnl, appele en subprocess pour eviter ReactorNotRestartable."""
import sys
import json
sys.path.insert(0, '/opt/deborah-trading/scripts')
from ctrader_executor_v2 import sync_real_pnl

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(json.dumps(None))
        sys.exit(1)
    try:
        signal_id = int(sys.argv[1])
        result = sync_real_pnl(signal_id)
        print(json.dumps(result))
        sys.exit(0)
    except Exception as e:
        print(json.dumps({'error': str(e)}))
        sys.exit(2)
