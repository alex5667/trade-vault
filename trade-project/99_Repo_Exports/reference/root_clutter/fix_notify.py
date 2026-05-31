import os
import glob

# Files to patch
target_files = glob.glob("/home/alex/front/trade/scanner_infra/**/notify_worker.py", recursive=True)

approve_target = """                pending = json.loads(raw_pending)
                candidate_path = pending.get("candidate_path", "")
                production_path = pending.get("production_path", "")
                metrics = pending.get("metrics", {})"""

approve_replacement = """                pending = json.loads(raw_pending)
                
                if pending.get("status") != "PENDING":
                    await client.post(
                        f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                        json={"callback_query_id": cb_id, "text": f"⚠️ Already {pending.get('status', 'processed').lower()}"}
                    )
                    await self._remove_buttons(client, chat_id, message_id)
                    return

                candidate_path = pending.get("candidate_path", "")
                production_path = pending.get("production_path", "")
                metrics = pending.get("metrics", {})"""

reject_target = """                raw_pending = self.r.get(pending_key)
                candidate_path = ""
                if raw_pending:
                    pending = json.loads(raw_pending)
                    pending["status"] = "REJECTED"
                    pending["rejected_by"] = username
                    pending["rejected_at_ms"] = int(time.time() * 1000)
                    self.r.set(pending_key, json.dumps(pending, ensure_ascii=False), keepttl=True)
                    candidate_path = pending.get("candidate_path", "")"""

reject_replacement = """                raw_pending = self.r.get(pending_key)
                candidate_path = ""
                if raw_pending:
                    pending = json.loads(raw_pending)
                    
                    if pending.get("status") != "PENDING":
                        await client.post(
                            f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                            json={"callback_query_id": cb_id, "text": f"⚠️ Already {pending.get('status', 'processed').lower()}"}
                        )
                        await self._remove_buttons(client, chat_id, message_id)
                        return

                    pending["status"] = "REJECTED"
                    pending["rejected_by"] = username
                    pending["rejected_at_ms"] = int(time.time() * 1000)
                    self.r.set(pending_key, json.dumps(pending, ensure_ascii=False), keepttl=True)
                    candidate_path = pending.get("candidate_path", "")"""

for filepath in target_files:
    if not os.path.isfile(filepath): continue
    with open(filepath, 'r') as f:
        content = f.read()
    
    modified = False
    if approve_target in content:
        content = content.replace(approve_target, approve_replacement)
        print(f"Patched approve in {filepath}")
        modified = True
    else:
        print(f"Approve target not found in {filepath} (Already patched?)")
        
    if reject_target in content:
        content = content.replace(reject_target, reject_replacement)
        print(f"Patched reject in {filepath}")
        modified = True
    else:
        print(f"Reject target not found in {filepath} (Already patched?)")
        
    if modified:
        with open(filepath, 'w') as f:
            f.write(content)

