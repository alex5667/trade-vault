#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import sys
import urllib.request
import urllib.parse
import os

# Credentials are read from environment variables — never hardcode secrets.
# Usage: TELEGRAM_BOT_TOKEN=xxx TELEGRAM_CHAT_ID=yyy python send_autopilot_report_direct.py
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

if not TOKEN or not CHAT_ID:
    print("ERROR: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set in environment.")
    print("  export TELEGRAM_BOT_TOKEN=<your-token>")
    print("  export TELEGRAM_CHAT_ID=<your-chat-id>")
    sys.exit(1)

def send_to_telegram(text: str):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    }
    data = urllib.parse.urlencode(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data)
    with urllib.request.urlopen(req) as response:
        return response.read().decode("utf-8")

REPORT_TEXT = """<b>TM Policy Tuner</b> | window=7d

<b>BTCUSDT</b>
  • range/continuation: tier=<b>1</b> | n=28 | EV=-0.170R | LCB=-0.658R | WR=46.4% | WR_LCB=29.5%
  • range/reversal: tier=<b>1</b> | n=20 | EV=-0.300R | LCB=-0.746R | WR=40.0% | WR_LCB=21.9%
  • trend/continuation: tier=<b>1</b> | n=22 | EV=+0.781R | LCB=+0.230R | WR=68.2% | WR_LCB=47.3%
  • trend/reversal: tier=<b>1</b> | n=19 | EV=-0.560R | LCB=-1.314R | WR=36.8% | WR_LCB=19.1%

<b>ETHUSDT</b>
  • range/continuation: tier=<b>1</b> | n=23 | EV=-0.192R | LCB=-0.841R | WR=47.8% | WR_LCB=29.2%
  • range/reversal: tier=<b>1</b> | n=19 | EV=-0.309R | LCB=-1.090R | WR=42.1% | WR_LCB=23.1%
  • trend/continuation: tier=<b>1</b> | n=16 | EV=+0.790R | LCB=-0.092R | WR=68.8% | WR_LCB=44.4%
  • trend/reversal: tier=<b>1</b> | n=17 | EV=-0.381R | LCB=-1.474R | WR=35.3% | WR_LCB=17.3%

<i>Note: Report generated in direct test mode. Connection to Redis currently unavailable.</i>"""

if __name__ == "__main__":
    print("Sending full report to Telegram...")
    res = send_to_telegram(REPORT_TEXT)
    print("Result:", res)
