#!/usr/bin/env python3
"""Rafraîchit le token OAuth cTrader avant expiration. Cron mensuel."""
import os
import re
import logging
import requests
import psycopg2

DB_CONFIG = {"host": "localhost", "port": 5432, "dbname": "trading",
             "user": "trading", "password": "Trading2026"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [oauth_refresh] %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ENV_PATH = "/root/ctrader-mcp-server/.env"


def load_env():
    env = {}
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k] = v
    return env


def update_env(access_token, refresh_token):
    with open(ENV_PATH) as f:
        content = f.read()
    content = re.sub(r"^ACCESS_TOKEN=.*$", f"ACCESS_TOKEN={access_token}", content, flags=re.MULTILINE)
    content = re.sub(r"^REFRESH_TOKEN=.*$", f"REFRESH_TOKEN={refresh_token}", content, flags=re.MULTILINE)
    with open(ENV_PATH, "w") as f:
        f.write(content)


def update_config_smc(access_token, refresh_token):
    """
    FIX 11/08/2026 : ce script rafraichissait le token uniquement dans le
    fichier .env, jamais dans config_smc - la vraie source lue en
    production par ctrader_executor_v2.get_access_token(). Consequence :
    le token de production n'avait pas ete rafraichi depuis le 02/07/2026
    (~6 semaines), potentiellement bloquant toute execution reelle d'ordre
    silencieusement. Ecrit desormais aussi dans config_smc, en plus du .env
    existant (aucun comportement retire, uniquement ajoute).
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO config_smc (name, value, updated_at)
            VALUES ('ctrader_access_token', %s, NOW())
            ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        """, (access_token,))
        cur.execute("""
            INSERT INTO config_smc (name, value, updated_at)
            VALUES ('ctrader_refresh_token', %s, NOW())
            ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        """, (refresh_token,))
        conn.commit()
        cur.close()
        conn.close()
        log.info("config_smc mis a jour (ctrader_access_token, ctrader_refresh_token)")
    except Exception as e:
        log.error(f"Erreur mise a jour config_smc: {e}")


def run():
    env = load_env()
    resp = requests.post(
        "https://connect.spotware.com/apps/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": env["REFRESH_TOKEN"],
            "client_id": env["CLIENT_ID"],
            "client_secret": env["CLIENT_SECRET"],
        },
        timeout=15,
    )
    data = resp.json()
    if "access_token" not in data:
        log.error(f"Refresh échoué: {data}")
        return

    update_env(data["access_token"], data["refresh_token"])
    update_config_smc(data["access_token"], data["refresh_token"])
    log.info(f"Token rafraîchi, expire dans {data.get('expires_in')}s (~{data.get('expires_in', 0)//86400}j)")


if __name__ == "__main__":
    run()
