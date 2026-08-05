#!/usr/bin/env python3
"""Déplace une tâche Kanboard et ajoute un commentaire via l'API JSON-RPC."""
import requests, json
import urllib3
urllib3.disable_warnings()

KANBOARD_URL = "https://localhost/jsonrpc.php"
API_TOKEN = "3e386b6b099429ca66faa2a12766142e5ba2e6493965c84223fc9739df83"
PROJECT_ID = 1

def call(method, params):
    payload = {"jsonrpc": "2.0", "method": method, "id": 1, "params": params}
    resp = requests.post(KANBOARD_URL, auth=("jsonrpc", API_TOKEN), json=payload,
                          verify=False, headers={"Content-Type": "application/json"})
    return resp.json()

def get_columns():
    result = call("getColumns", {"project_id": PROJECT_ID})
    return result.get('result', [])

def move_task_to_column(task_id, column_id):
    return call("moveTaskPosition", {
        "project_id": PROJECT_ID,
        "task_id": task_id,
        "column_id": column_id,
        "position": 1,
        "swimlane_id": 1
    })

def add_comment(task_id, content):
    return call("createComment", {
        "task_id": task_id,
        "user_id": 1,
        "content": content
    })

if __name__ == '__main__':
    cols = get_columns()
    print("Colonnes disponibles :")
    for c in cols:
        print(f"  id={c['id']}  title={c['title']}")

    target_col = next((c for c in cols if 'patch' in c['title'].lower()), None)
    if not target_col:
        print("Colonne 'Patch deploye' introuvable, arrêt.")
        exit(1)

    print(f"\nDéplacement du ticket #3 vers colonne '{target_col['title']}' (id={target_col['id']})")
    move_result = move_task_to_column(3, target_col['id'])
    print("Move result:", move_result)

    comment = """PATCH DÉPLOYÉ (02/07/2026)

Root cause identifiée et corrigée : le fallback dans sync_position_id() (lignes 239-245) retournait le positionId pré-attribué d'un ordre encore pending comme si c'était une position confirmée, causant le faux positif documenté sur signal #56.

Correctif appliqué : suppression du fallback trompeur. Si aucune position réelle n'est trouvée dans r.position, la fonction retourne désormais None proprement (comportement déjà géré correctement en aval par position_monitor.py via le mécanisme sync_attempts/MAX_SYNC_ATTEMPTS).

Aucune modification nécessaire côté appelants — le retry borné + alerte Telegram en cas d'échec répété existaient déjà et gèrent bien le cas None.

Pas de ReactorNotRestartable ici contrairement à la piste initialement envisagée : sync_position_id() faisait déjà tout en un seul appel réseau (ProtoOAReconcileReq), le bug était purement logique, pas architectural.

Validation : syntaxe vérifiée (py_compile OK), __pycache__ nettoyé. Test en conditions réelles en attente du prochain ordre pending non atteint par le prix."""

    comment_result = add_comment(3, comment)
    print("Comment result:", comment_result)
