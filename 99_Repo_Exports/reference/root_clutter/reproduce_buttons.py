import asyncio
import os
import sys
import json
from dotenv import load_dotenv

load_dotenv("telegram-worker/.env")

# Add the telegram-worker directory to sys.path
sys.path.append(os.path.join(os.getcwd(), "telegram-worker"))

from notifier import send_html_to_telegram

async def test_buttons():
    
    text = "🧪 <b>Reproduction Test</b>\n\nThis message should have buttons below."
    buttons = [[
        {"text": "👀 Preview diff", "callback": "recs:preview2:test_bundle:sig123"},
        {"text": "✅ Approve challenger", "callback": "recs:confirm:test_bundle:sig123"},
        {"text": "❌ Reject", "callback": "recs:reject:test_bundle:sig123"},
    ]]
    
    print(f"Sending test message with buttons...")
    success = await send_html_to_telegram(text, buttons=buttons)
    if success:
        print("✅ Message sent successfully!")
    else:
        print("❌ Failed to send message.")

if __name__ == "__main__":
    asyncio.run(test_buttons())
