#!/usr/bin/env python3
"""Crée ou met à jour une tâche Kanboard via l'API JSON-RPC (local)."""
import requests, json
import urllib3
urllib3.disable_warnings()

KANBOARD_URL = "https://localhost/jsonrpc.php"
API_TOKEN = "3e386b6b099429ca66faa2a12766142e5ba2e6493965c84223fc9739df83"
PROJECT_ID = 1

def call(method, params):
    payload = {"jsonrpc": "2.0", "method": method, "id": 1, "params": params}
    resp = requests.post(KANBOARD_URL, auth=("jsonrpc", API_TOKEN), json=payload, verify=False,
                          headers={"Content-Type": "application/json"})
    print(resp.status_code, resp.text)
    return resp.json()

def create_task(title, description, column_id=1):
    return call("createTask", {
        "title": title,
        "project_id": PROJECT_ID,
        "column_id": column_id,
        "description": description
    })

if __name__ == '__main__':
    title = "Point exploration SMC — 17/20-30 trades post-fix (02/07/2026)"
    description = """STATUT EXPLORATION (0.02% risque) — post ReactorNotRestartable fix (23/06)

Global : 17 trades | 9 wins / 8 losses | WR 52.9% | PnL cumulé +5.75€

Répartition directionnelle :
- SELL_LIMIT : 11 trades, 7 wins, WR 63.6%, +11.55€
- BUY_LIMIT  : 6 trades, 2 wins, WR 33.3%, -5.80€

OBSERVATION : le déséquilibre BUY vs SELL déjà identifié dans l'historique complet (44 trades : SELL +33.45€/69%WR vs BUY -178.11€/32%WR) se confirme sur l'échantillon post-fix, malgré la petite taille.

DÉCISION : poursuivre exploration jusqu'à 20-30 trades avant transition challenge expérimental. Envisager backtest du filtre trend-alignment (SELL only en H4/D1 bearish) avant d'augmenter le risque.

Prochain check : à 20+ trades post-fix."""
    create_task(title, description)
