import requests
import os
import time

token = os.getenv("TELEGRAM_BOT_TOKEN")
if not token:
    print("Token not found")
    exit(1)

for i in range(5):
    print(f"Polling {i}...")
    try:
        r = requests.get(f"https://api.telegram.org/bot{token}/getUpdates", params={"timeout": 5}, timeout=10)
        print(r.status_code)
        print(r.text[:200])
    except Exception as e:
        print("Error:", e)
    time.sleep(2)
