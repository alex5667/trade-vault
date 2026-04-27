import sys

path = "telegram-worker/notify_worker.py"
with open(path, "r") as f:
    text = f.read()

# Add a try-except to start() and logging to handle_update
new_start = """
    async def start(self):
        if not self.token:
            print("⚠️ BotCallbackPoller disabled: no token")
            return
        
        print("🚀 BotCallbackPoller started")
        self.running = True
        try:
            import httpx
            
            # Determine approval prefix
            self.approvals_prefix = os.getenv("ENTRY_POLICY_APPROVALS_PREFIX", "cfg:suggestions:entry_policy:approvals")
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                while self.running:
                    try:
                        url = f"https://api.telegram.org/bot{self.token}/getUpdates"
                        params = {
                            "offset": self.offset,
                            "timeout": 15,
                            "allowed_updates": ["callback_query"]
                        }
                        resp = await client.get(url, params=params)
                        if resp.status_code != 200:
                            print(f"⚠️ BotCallbackPoller getUpdates HTTP {resp.status_code}")
                            await asyncio.sleep(5)
                            continue
                            
                        data = resp.json()
                        if not data.get("ok"):
                            print(f"⚠️ BotCallbackPoller getUpdates not ok: {data}")
                            await asyncio.sleep(5)
                            continue
                            
                        updates = data.get("result", [])
                        if updates:
                            print(f"🔧 BotCallbackPoller received {len(updates)} updates")
                        for update in updates:
                            self.offset = update["update_id"] + 1
                            await self.handle_update(client, update)
                    except Exception as e:
                        print(f"❌ BotCallbackPoller loop error: {e}")
                        await asyncio.sleep(5)
        except Exception as e:
            print(f"❌ BotCallbackPoller fatal crash: {e}")
"""

import re
text = re.sub(r'async def start\(self\):.*?async def handle_update', new_start.strip() + '\n\n    async def handle_update', text, flags=re.DOTALL)

with open(path, "w") as f:
    f.write(text)

print("Patched notify_worker.py")
