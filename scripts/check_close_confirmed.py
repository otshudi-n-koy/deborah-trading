#!/usr/bin/env python3
"""Script standalone pour verifier si une position est fermee, appele en subprocess
pour eviter le conflit ReactorNotRestartable avec d'autres appels Twisted dans le meme processus."""
import sys
sys.path.insert(0, '/opt/deborah-trading/scripts')
from ctrader_executor_v2 import get_open_position_ids

if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit(2)
    try:
        position_id = int(sys.argv[1])
        open_ids = get_open_position_ids()
        if position_id not in open_ids:
            sys.exit(0)  # confirme ferme
        else:
            sys.exit(1)  # toujours ouvert
    except Exception:
        sys.exit(2)  # erreur
