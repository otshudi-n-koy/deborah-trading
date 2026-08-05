#!/usr/bin/env python3
"""Echange un code d'autorisation OAuth cTrader contre un access_token.
Usage: python3 exchange_oauth_code.py <code>"""
import sys, requests, json

CLIENT_ID = "28221_xsMIPF9Cc99ufPI1rrsEeiF3J3NeI6pKw4f9ZQrRamNPDGlvPw"
CLIENT_SECRET = "tq2aoMRq7MMEMHuJ1H2NstGvkPD4RZ2pgzENpCAMWIkNIMrYzk"
REDIRECT_URI = "https://deborah-trading.duckdns.org/ctrader-callback"

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("usage: exchange_oauth_code.py <code>")
        sys.exit(1)
    code = sys.argv[1]
    resp = requests.post("https://openapi.ctrader.com/apps/token", params={
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    })
    print(resp.status_code, resp.text)
