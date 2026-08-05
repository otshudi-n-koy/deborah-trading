#!/usr/bin/env python3
import sys
sys.path.insert(0, "/opt/deborah-trading/scripts")
from ctrader_executor_v2 import sync_tp_palier

if __name__ == "__main__":
    if len(sys.argv) < 5:
        sys.exit(2)
    try:
        signal_id = int(sys.argv[1])
        palier = int(sys.argv[2])
        lots_closed = float(sys.argv[3])
        new_sl = float(sys.argv[4])
        result = sync_tp_palier(signal_id, palier, lots_closed, new_sl)
        sys.exit(0 if result else 1)
    except Exception as e:
        print(str(e), file=sys.stderr)
        sys.exit(2)
