#!/usr/bin/env python3
"""Rafraîchit le token OAuth cTrader avant expiration. Cron mensuel."""
import os
import re
import logging
import requests

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
    log.info(f"Token rafraîchi, expire dans {data.get('expires_in')}s (~{data.get('expires_in', 0)//86400}j)")


if __name__ == "__main__":
    run()
