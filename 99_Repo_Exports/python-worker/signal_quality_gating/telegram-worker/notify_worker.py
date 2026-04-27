from utils.time_utils import get_ny_time_millis
#!/usr/bin/env python3
"""
Notify‑worker: отдельный процесс, читающий задачи из Redis Stream (notify:telegram)
и рассылающий уведомления в Telegram через бота. Не блокирует парсинг.

Особенности:
- Чтение через consumer group 'notify-group'
- Повторная отправка при ошибках с экспоненциальным бэк‑оффом
- Подтверждение (XACK) после успешной отправки
- ВРЕМЕННО: сообщения НЕ удаляются из потока после обработки (для отладки)
"""
import asyncio
from utils.task_manager import safe_create_task

import json
import os
import sys
import time
import re
from typing import Any, Dict, Optional, List

import redis
from redis.exceptions import ResponseError
from dotenv import load_dotenv

# Enable console output for debugging
import builtins
_original_print = builtins.print

# Подключаем модули, предполагая, что они доступны в python path
try:
    from app.config import load_settings
    from notifier import notify_parsed_signal, delete_message_from_stream, ENABLED as NOTIFY_ENABLED, send_html_to_telegram
except ImportError:
    # Fallback для локального запуска или если пути отличаются
    sys.path.append(os.getcwd())
    try:
        from app.config import load_settings
        from notifier import notify_parsed_signal, delete_message_from_stream, ENABLED as NOTIFY_ENABLED, send_html_to_telegram
    except ImportError:
        print("❌ CRITICAL: Could not import app modules. Check PYTHONPATH.")
        sys.exit(1)


# Загружаем переменные окружения из .env файла
load_dotenv()

# =============================================================================
# Helper Utilities & Normalization
# =============================================================================

JSON_FIELD_KEYS = {
    # outbox v2
    "signal_payload",
    "signal_settings",
    # common nested blobs
    "risk",
    "metadata",
    "indicators",
    "confirmations",
    # UI controls
    "buttons" 
}

def _b2s(x: Any) -> Any:
    """Safely convert bytes to string."""
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", errors="ignore")
    return x if x is not None else ""

def _looks_like_json(s: str) -> bool:
    """Check if string looks like JSON object or array."""
    s = s.strip()
    return (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]"))

def _maybe_json_load(v: Any) -> Any:
    """Safely decode JSON strings or bytes."""
    # Idempotent: if already dict/list -> return as is
    if isinstance(v, (dict, list)):
        return v
    
    # Bytes -> decode to string
    if isinstance(v, (bytes, bytearray)):
        v = v.decode("utf-8", errors="ignore")
        
    if not isinstance(v, str):
        return v
        
    if not _looks_like_json(v):
        return v
        
    try:
        return json.loads(v)
    except Exception:
        return v

def normalize_entry(entry: Any) -> Dict[str, Any]:
    """
    Robust normalization for Redis Stream entries:
      1. Convert bytes keys/values to strings.
      2. Handle list-pairs (redis-py raw) -> dict.
      3. Merge legacy 'data'/'payload' JSON usage.
      4. Decode known JSON fields (signal_payload, buttons, etc).
    """
    if not entry:
        return {}

    # 1 & 2: Base dict conversion
    out: Dict[str, Any] = {}
    if isinstance(entry, dict):
        out = {_b2s(k): _b2s(v) for k, v in entry.items() if k is not None}
    elif isinstance(entry, (list, tuple)):
        # Handle raw list [k, v, k, v...]
        try:
            d = dict(zip(entry[::2], entry[1::2]))
            out = {_b2s(k): _b2s(v) for k, v in d.items() if k is not None}
        except Exception:
            return {}

    # 3: Merge legacy 'data' or 'payload' if they contain JSON strings/dicts of top-level fields
    for carrier in ("data", "payload"):
        if carrier in out:
            val = out.get(carrier)
            obj = _maybe_json_load(val)
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if k not in out:
                        out[k] = v

    # 4: Decode specific known JSON fields
    for k in list(out.keys()):
        if k in JSON_FIELD_KEYS:
            out[k] = _maybe_json_load(out.get(k))

    return out


# =============================================================================
# Outbox Meta Sidecar
# =============================================================================

def _outbox_meta_prefix() -> str:
    return os.getenv("OUTBOX_META_PREFIX", "signal:meta:")

def _outbox_meta_key(signal_id: str) -> str:
    return f"{_outbox_meta_prefix()}{signal_id}"

def _fetch_outbox_meta(redis_client: Any, signal_id: str) -> dict:
    if redis_client is None or not signal_id:
        return {}
    try:
        raw = redis_client.get(_outbox_meta_key(signal_id))
        return _maybe_json_load(raw) if raw else {}
    except Exception:
        return {}

def _compact_config_params(cfg: Any) -> Any:
    if not isinstance(cfg, dict):
        return cfg
    max_keys = int(os.getenv("TG_CONFIG_PARAMS_MAX_KEYS", "0") or 0)
    if max_keys > 0 and len(cfg) > max_keys:
        keys = sorted(cfg.keys())[:max_keys]
        return {k: cfg.get(k) for k in keys}
    return cfg

def _attach_outbox_meta(redis_client: Any, *, entry: dict, parsed: dict, raw: dict) -> None:
    if (os.getenv("TG_INCLUDE_CONFIG_PARAMS", "1").lower() in {"0", "false", "no"}):
        return

    signal_id = (
        str(parsed.get("signal_id") or "")
        or str(entry.get("signal_id") or "")
        or str(parsed.get("id") or "")
        or str(entry.get("id") or "")
    )
    if not signal_id:
        return

    meta = _fetch_outbox_meta(redis_client, signal_id)
    cfg = meta.get("config_params")
    if not cfg:
        return

    cfg = _compact_config_params(cfg)
    parsed["config_params"] = cfg
    
    ss = parsed.get("signal_settings")
    if isinstance(ss, dict):
        ss["config_params"] = cfg
    
    raw["config_params"] = cfg
    raw["signal_id"] = signal_id


# =============================================================================
# Bot Callback Poller
# =============================================================================

class BotCallbackPoller:
    """
    Simple poller for bot updates (callbacks) using httpx.
    Runs in background task within notify_worker.

    Also runs a reminder loop for SG Calibrator pending approvals:
    if the user doesn't respond within 30 min, the Telegram message
    with buttons is resent every SG_CALIB_REMINDER_SEC (default 1800s).
    """
    def __init__(self, redis_client):
        self.r = redis_client
        self.token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.running = False
        self.offset = 0
        
    async def start(self):
        if not self.token:
            print("⚠️ BotCallbackPoller disabled: no token")
            return
        
        print("🚀 BotCallbackPoller started")
        self.running = True

        # Launch SG Calibrator reminder loop in background
        safe_create_task(self._sg_calib_reminder_loop())

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

    async def handle_update(self, client, update):
        cb = update.get("callback_query")
        if not cb:
            return
            
        cb_id = cb.get("id")
        data = cb.get("data", "")
        from_user = cb.get("from", {})
        username = from_user.get("username") or str(from_user.get("id"))
        message = cb.get("message", {})
        chat_id = message.get("chat", {}).get("id")
        message_id = message.get("message_id")
        
        meta_prefix = os.getenv("ENTRY_POLICY_META_PREFIX", "cfg:suggestions:entry_policy:meta")
        proposal_ttl = int(os.getenv("TM_PROPOSAL_TTL_SEC", "1209600"))  # 14d
        notify_stream = os.getenv("TM_TELEGRAM_STREAM", os.getenv("NOTIFY_STREAM", "notify:telegram"))

        if data.startswith("approve:"):
            # Format: approve:<sid>
            try:
                sid = data.split(":", 1)[1]
                # Store approval
                key = f"{self.approvals_prefix}:{sid}"
                self.r.sadd(key, username)
                self.r.expire(key, proposal_ttl)
                count = self.r.scard(key)
                
                # Mark as applied
                applied_key = f"cfg:suggestions:entry_policy:applied:{sid}"
                self.r.set(applied_key, str(get_ny_time_millis()), ex=proposal_ttl)
                
                # Answer callback immediately (stop loading spinner)
                await client.post(
                    f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                    json={"callback_query_id": cb_id, "text": f"✅ Approved! (Total: {count})"}
                )
                
                # Send confirmation notification to Telegram
                confirm_text = f"✅ <b>Proposal {sid[:8]}… APPROVED</b>\nby @{username} (approvals: {count})\n\n<i>Changes applied to cfg:suggestions</i>"
                self.r.xadd(notify_stream, {"type": "report", "text": confirm_text}, maxlen=20000, approximate=True)
                
                # Remove buttons from original message
                await self._remove_buttons(client, chat_id, message_id)
                
                print(f"✅ Callback approval: {username} -> {sid} (Total: {count})")
            except Exception as e:
                print(f"❌ Callback approve error: {e}")
                
        elif data.startswith("reject:"):
            # Format: reject:<sid>
            try:
                sid = data.split(":", 1)[1]
                
                # Mark as rejected
                rejected_key = f"cfg:suggestions:entry_policy:rejected:{sid}"
                self.r.set(rejected_key, json.dumps({
                    "by": username,
                    "ts_ms": get_ny_time_millis(),
                }), ex=proposal_ttl)
                
                # Delete the proposal meta key (discard)
                meta_key = f"{meta_prefix}:{sid}"
                self.r.delete(meta_key)
                
                # Answer callback
                await client.post(
                    f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                    json={"callback_query_id": cb_id, "text": "❌ Rejected!"}
                )
                
                # Send rejection notification to Telegram
                confirm_text = f"❌ <b>Proposal {sid[:8]}… REJECTED</b>\nby @{username}\n\n<i>Proposal discarded from cfg:suggestions</i>"
                self.r.xadd(notify_stream, {"type": "report", "text": confirm_text}, maxlen=20000, approximate=True)
                
                # Remove buttons from original message
                await self._remove_buttons(client, chat_id, message_id)
                
                print(f"❌ Callback rejection: {username} -> {sid}")
            except Exception as e:
                print(f"❌ Callback reject error: {e}")
        elif data.startswith("trail_approve:"):
            # Format: trail_approve:<run_id>
            try:
                run_id = data.split(":", 1)[1]
                pending_key = f"trail:calib:pending:{run_id}"
                
                # Read pending data for rich confirmation
                raw_pending = self.r.get(pending_key)
                pending = {}
                if raw_pending:
                    pending = json.loads(raw_pending)
                    pending["status"] = "APPROVED"
                    pending["approved_by"] = username
                    pending["approved_at_ms"] = get_ny_time_millis()
                    self.r.set(pending_key, json.dumps(pending, ensure_ascii=False), keepttl=True)
                
                # Switch all trail:calib:* keys to enforce mode
                switched = 0
                calib_prefix = os.getenv("TRAIL_CALIB_KEY_PREFIX", "trail:calib") or "trail:calib"
                cursor = 0
                while True:
                    cursor, keys = self.r.scan(cursor=cursor, match=f"{calib_prefix}:*", count=10000)
                    for k in keys:
                        # Skip pending keys
                        if ":pending:" in k:
                            continue
                        self.r.hset(k, "mode", "enforce")
                        switched += 1
                    if cursor == 0:
                        break
                
                # Answer callback
                await client.post(
                    f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                    json={"callback_query_id": cb_id, "text": f"✅ Trail calibration approved! ({switched} keys)"}
                )
                
                # Build rich confirmation with param details
                param_details = pending.get("param_details", [])
                shadow_data = pending.get("shadow_summary", {})
                stability_data = pending.get("stability_summary", {})

                # Per-symbol params table
                params_lines = []
                for pd in sorted(param_details, key=lambda x: x.get("symbol", "")):
                    params_lines.append(
                        f"  <code>{pd['symbol']}</code>: "
                        f"cb={pd['callback_atr_mult']:.3f}×ATR "
                        f"off={pd['activate_offset_bps']:.1f}bps "
                        f"lock={pd['min_profit_lock_r']:.3f}R "
                        f"(conf={pd['confidence']:.2f}, n={pd['n_total']})"
                    )
                params_block = "\n".join(params_lines) if params_lines else "  (no details)"

                # Shadow summary line
                shadow_line = ""
                if shadow_data:
                    shadow_line = (
                        f"\n📊 <b>Shadow P&L:</b> "
                        f"{shadow_data.get('n_better', 0)}✅ better, "
                        f"{shadow_data.get('n_neutral', 0)}🔄 neutral, "
                        f"{shadow_data.get('n_worse', 0)}⚠️ worse | "
                        f"avg Δ={shadow_data.get('avg_delta_r', 0):+.3f}R\n"
                    )

                # Stability summary line
                stability_line = ""
                if stability_data:
                    stability_line = (
                        f"📏 <b>Stability:</b> "
                        f"{stability_data.get('n_stable', 0)}/{stability_data.get('n_total', 0)} stable\n"
                    )

                confirm_text = (
                    f"✅ <b>Trail Calibration APPROVED</b>\n"
                    f"by @{username}\n\n"
                    f"<b>{switched}</b> trail:calib keys switched to <code>mode=enforce</code>\n\n"
                    f"<b>Applied Parameters:</b>\n"
                    f"{params_block}\n"
                    f"{shadow_line}"
                    f"{stability_line}\n"
                    f"<i>Executor will now use calibrated trailing params.</i>\n\n"
                    f"Run ID: <code>{run_id}</code>"
                )
                self.r.xadd(notify_stream, {"type": "report", "text": confirm_text}, maxlen=20000, approximate=True)
                
                # Remove buttons
                await self._remove_buttons(client, chat_id, message_id)
                
                print(f"✅ Trail calibration approved: {username} -> {run_id} ({switched} keys enforced)")
            except Exception as e:
                print(f"❌ Trail approve error: {e}")
                
        elif data.startswith("trail_reject:"):
            # Format: trail_reject:<run_id>
            try:
                run_id = data.split(":", 1)[1]
                pending_key = f"trail:calib:pending:{run_id}"
                
                # Read pending data for context
                raw_pending = self.r.get(pending_key)
                pending = {}
                if raw_pending:
                    pending = json.loads(raw_pending)
                    pending["status"] = "REJECTED"
                    pending["rejected_by"] = username
                    pending["rejected_at_ms"] = get_ny_time_millis()
                    self.r.set(pending_key, json.dumps(pending, ensure_ascii=False), keepttl=True)
                
                # Answer callback
                await client.post(
                    f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                    json={"callback_query_id": cb_id, "text": "❌ Trail calibration rejected"}
                )
                
                # Build rejection with shadow context
                shadow_data = pending.get("shadow_summary", {})
                stability_data = pending.get("stability_summary", {})
                n_params = pending.get("n_params", 0)
                symbols = pending.get("symbols") or []

                context_lines = []
                if shadow_data:
                    context_lines.append(
                        f"Shadow: {shadow_data.get('n_better', 0)}✅ "
                        f"{shadow_data.get('n_neutral', 0)}🔄 "
                        f"{shadow_data.get('n_worse', 0)}⚠️ "
                        f"avg_Δ={shadow_data.get('avg_delta_r', 0):+.3f}R"
                    )
                if stability_data:
                    context_lines.append(
                        f"Stability: {stability_data.get('n_stable', 0)}/"
                        f"{stability_data.get('n_total', 0)} stable"
                    )
                context_block = "\n".join(f"  {l}" for l in context_lines) if context_lines else ""

                context_part = f"{context_block}\n\n" if context_block else ""
                reject_text = (
                    f"❌ <b>Trail Calibration REJECTED</b>\n"
                    f"by @{username}\n\n"
                    f"{n_params} params for {', '.join(symbols[:5])} remain in <code>mode=shadow</code>\n"
                    f"{context_part}"
                    f"<i>Executor uses static defaults. Next calibration in ~6h.</i>\n\n"
                    f"Run ID: <code>{run_id}</code>"
                )
                self.r.xadd(notify_stream, {"type": "report", "text": reject_text}, maxlen=20000, approximate=True)
                
                # Remove buttons
                await self._remove_buttons(client, chat_id, message_id)
                
                print(f"❌ Trail calibration rejected: {username} -> {run_id}")
            except Exception as e:
                print(f"❌ Trail reject error: {e}")

        elif data.startswith("ml_scorer_approve:"):
            # Format: ml_scorer_approve:<run_id>
            try:
                run_id = data.split(":", 1)[1]
                pending_key = f"ml_scorer:pending:{run_id}"
                
                raw_pending = self.r.get(pending_key)
                if not raw_pending:
                    await client.post(
                        f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                        json={"callback_query_id": cb_id, "text": "⚠️ Pending record expired"}
                    )
                    return
                
                pending = json.loads(raw_pending)
                
                if pending.get("status") != "PENDING":
                    await client.post(
                        f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                        json={"callback_query_id": cb_id, "text": f"⚠️ Already {pending.get('status', 'processed').lower()}"}
                    )
                    await self._remove_buttons(client, chat_id, message_id)
                    return

                candidate_path = pending.get("candidate_path", "")
                production_path = pending.get("production_path", "")
                metrics = pending.get("metrics", {})
                
                # Promote: copy candidate → production
                promoted = False
                if candidate_path and production_path:
                    try:
                        import shutil
                        shutil.copy2(candidate_path, production_path)
                        promoted = True
                        print(f"✅ ML Scorer promoted: {candidate_path} → {production_path}")
                    except Exception as e:
                        print(f"❌ ML Scorer promote failed: {e}")
                
                # Update pending status
                pending["status"] = "APPROVED"
                pending["approved_by"] = username
                pending["approved_at_ms"] = get_ny_time_millis()
                pending["promoted"] = promoted
                self.r.set(pending_key, json.dumps(pending, ensure_ascii=False), keepttl=True)
                
                # Answer callback
                status_text = "✅ ML Scorer promoted!" if promoted else "⚠️ Approved but promote failed"
                await client.post(
                    f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                    json={"callback_query_id": cb_id, "text": status_text}
                )
                
                # Confirmation to Telegram with metrics
                mae = metrics.get("mae_oof", -1)
                r2 = metrics.get("r2_oof", -1)
                spearman = metrics.get("spearman_oof", -1)
                n_samples = pending.get("n_samples", 0)
                
                confirm_text = (
                    f"✅ <b>ML Scorer V2 APPROVED</b>\n"
                    f"by @{username}\n\n"
                    f"{'✅ Model promoted to production' if promoted else '⚠️ Approve recorded but file copy failed'}\n\n"
                    f"📊 <b>Model Metrics</b>\n"
                    f"  • MAE:      <code>{mae:.4f}</code>\n"
                    f"  • R²:       <code>{r2:.4f}</code>\n"
                    f"  • Spearman: <code>{spearman:.4f}</code>\n"
                    f"  • Samples:  <code>{n_samples}</code>\n\n"
                    f"<i>MLScoringGate will hot-reload the new model within 60s.</i>\n\n"
                    f"Run ID: <code>{run_id}</code>"
                )
                self.r.xadd(notify_stream, {"type": "report", "text": confirm_text}, maxlen=20000, approximate=True)
                
                # Remove buttons
                await self._remove_buttons(client, chat_id, message_id)
                
                print(f"✅ ML Scorer approved: {username} -> {run_id} (promoted={promoted})")
            except Exception as e:
                print(f"❌ ML Scorer approve error: {e}")
                
        elif data.startswith("ml_scorer_reject:"):
            # Format: ml_scorer_reject:<run_id>
            try:
                run_id = data.split(":", 1)[1]
                pending_key = f"ml_scorer:pending:{run_id}"
                
                raw_pending = self.r.get(pending_key)
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
                    pending["rejected_at_ms"] = get_ny_time_millis()
                    self.r.set(pending_key, json.dumps(pending, ensure_ascii=False), keepttl=True)
                    candidate_path = pending.get("candidate_path", "")
                
                # Delete candidate file
                deleted = False
                if candidate_path:
                    try:
                        import os as _os
                        if _os.path.isfile(candidate_path):
                            _os.remove(candidate_path)
                            deleted = True
                    except Exception:
                        pass
                
                # Answer callback
                await client.post(
                    f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                    json={"callback_query_id": cb_id, "text": "❌ ML Scorer rejected"}
                )
                
                # Rejection to Telegram
                reject_text = (
                    f"❌ <b>ML Scorer V2 REJECTED</b>\n"
                    f"by @{username}\n\n"
                    f"Candidate model discarded{' (file deleted)' if deleted else ''}.\n"
                    f"Current production model (if any) remains active.\n\n"
                    f"Run ID: <code>{run_id}</code>"
                )
                self.r.xadd(notify_stream, {"type": "report", "text": reject_text}, maxlen=20000, approximate=True)
                
                # Remove buttons
                await self._remove_buttons(client, chat_id, message_id)
                
                print(f"❌ ML Scorer rejected: {username} -> {run_id}")
            except Exception as e:
                print(f"❌ ML Scorer reject error: {e}")

        elif data.startswith("sg_calib_approve:"):
            # Format: sg_calib_approve:<run_id>
            # → Promote Strong Gate from shadow_enforce → full_enforce
            try:
                run_id = data.split(":", 1)[1]
                pending_key = f"sg_calib:pending:{run_id}"

                # Read pending data
                raw_pending = self.r.get(pending_key)
                pending = {}
                if raw_pending:
                    pending = json.loads(raw_pending)
                    pending["status"] = "APPROVED"
                    pending["approved_by"] = username
                    pending["approved_at_ms"] = get_ny_time_millis()
                    self.r.set(pending_key, json.dumps(pending, ensure_ascii=False), keepttl=True)

                if not raw_pending:
                    await client.post(
                        f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                        json={"callback_query_id": cb_id, "text": "⚠️ Pending record expired or not found"}
                    )
                    return

                # Switch all config:orderflow:* to full_enforce
                switched = 0
                cursor = 0
                while True:
                    cursor, keys = self.r.scan(cursor=cursor, match="config:orderflow:*", count=10000)
                    for k in keys:
                        self.r.hset(k, "sg_calib_mode", "full_enforce")
                    switched += len(keys)
                    if cursor == 0:
                        break

                # Also set the state key to full_enforce
                state_key = "cfg:sg_calib:state"
                try:
                    state_raw = self.r.get(state_key)
                    if state_raw:
                        state = json.loads(state_raw)
                        state["mode"] = "full_enforce"
                        state["approved_by"] = username
                        state["approved_at_ms"] = get_ny_time_millis()
                        self.r.set(state_key, json.dumps(state, ensure_ascii=False), keepttl=True)
                except Exception:
                    pass

                # Answer callback
                await client.post(
                    f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                    json={"callback_query_id": cb_id, "text": f"✅ Strong Gate → FULL ENFORCE ({switched} symbols)"}
                )

                # Build rich confirmation
                precision = pending.get("veto_precision", 0)
                lift = pending.get("veto_lift", 0)
                streak = pending.get("proof_streak", 0)

                confirm_text = (
                    f"🔴 <b>G5 Strong Gate → FULL ENFORCE</b>\n"
                    f"by @{username}\n\n"
                    f"<b>{switched}</b> symbols switched to <code>mode=full_enforce</code>\n\n"
                    f"📊 <b>Evidence at promotion:</b>\n"
                    f"  • Veto Precision: <code>{precision:.1%}</code>\n"
                    f"  • Veto Lift: <code>{lift:+.1%}</code>\n"
                    f"  • Proof Streak: <code>{streak}</code>\n\n"
                    f"⚠️ <b>Strong Gate vetoes are now REAL (non-shadow).</b>\n"
                    f"Signals failing the gate will be rejected.\n\n"
                    f"<i>To revert: set <code>sg_calib_mode=shadow</code> in dynamic_cfg</i>\n\n"
                    f"Run ID: <code>{run_id}</code>"
                )
                self.r.xadd(notify_stream, {"type": "report", "text": confirm_text}, maxlen=20000, approximate=True)

                # Remove buttons
                await self._remove_buttons(client, chat_id, message_id)

                print(f"✅ SG Calib approved (full_enforce): {username} -> {run_id} ({switched} symbols)")
            except Exception as e:
                print(f"❌ SG Calib approve error: {e}")

        elif data.startswith("sg_calib_reject:"):
            # Format: sg_calib_reject:<run_id>
            # → Revert Strong Gate back to shadow
            try:
                run_id = data.split(":", 1)[1]
                pending_key = f"sg_calib:pending:{run_id}"

                # Read pending data
                raw_pending = self.r.get(pending_key)
                pending = {}
                if raw_pending:
                    pending = json.loads(raw_pending)
                    pending["status"] = "REJECTED"
                    pending["rejected_by"] = username
                    pending["rejected_at_ms"] = get_ny_time_millis()
                    self.r.set(pending_key, json.dumps(pending, ensure_ascii=False), keepttl=True)

                # Switch all config:orderflow:* back to shadow
                switched = 0
                cursor = 0
                while True:
                    cursor, keys = self.r.scan(cursor=cursor, match="config:orderflow:*", count=10000)
                    for k in keys:
                        self.r.hset(k, "sg_calib_mode", "shadow")
                    switched += len(keys)
                    if cursor == 0:
                        break

                # Also set the state key to shadow
                state_key = "cfg:sg_calib:state"
                try:
                    state_raw = self.r.get(state_key)
                    if state_raw:
                        state = json.loads(state_raw)
                        state["mode"] = "shadow"
                        state["proof_streak"] = 0
                        state["rejected_by"] = username
                        state["rejected_at_ms"] = get_ny_time_millis()
                        self.r.set(state_key, json.dumps(state, ensure_ascii=False), keepttl=True)
                except Exception:
                    pass

                # Answer callback
                await client.post(
                    f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                    json={"callback_query_id": cb_id, "text": "⬇️ Strong Gate reverted to SHADOW"}
                )

                # Build rejection notification
                precision = pending.get("veto_precision", 0)
                lift = pending.get("veto_lift", 0)

                reject_text = (
                    f"🟡 <b>G5 Strong Gate → SHADOW (reverted)</b>\n"
                    f"by @{username}\n\n"
                    f"<b>{switched}</b> symbols reverted to <code>mode=shadow</code>\n\n"
                    f"📊 <b>Stats at revert:</b>\n"
                    f"  • Veto Precision: <code>{precision:.1%}</code>\n"
                    f"  • Veto Lift: <code>{lift:+.1%}</code>\n\n"
                    f"<i>Proof streak reset to 0. Calibrator will re-evaluate in next cycle.</i>\n\n"
                    f"Run ID: <code>{run_id}</code>"
                )
                self.r.xadd(notify_stream, {"type": "report", "text": reject_text}, maxlen=20000, approximate=True)

                # Remove buttons
                await self._remove_buttons(client, chat_id, message_id)

                print(f"⬇️ SG Calib rejected (shadow): {username} -> {run_id} ({switched} symbols)")
            except Exception as e:
                print(f"❌ SG Calib reject error: {e}")

        else:
            # Unknown callback — acknowledge to stop the loading animation
            await client.post(
                f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                json={"callback_query_id": cb_id, "text": "Action recorded"}
            )

    async def _remove_buttons(self, client, chat_id, message_id):
        """Remove inline keyboard from the original message (best-effort)."""
        if not chat_id or not message_id:
            return
        try:
            await client.post(
                f"https://api.telegram.org/bot{self.token}/editMessageReplyMarkup",
                json={
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "reply_markup": {"inline_keyboard": []},
                }
            )
        except Exception as e:
            print(f"⚠️ Failed to remove buttons: {e}")
    
    def stop(self):
        self.running = False

    # -----------------------------------------------------------------
    # SG Calibrator reminder loop
    # -----------------------------------------------------------------

    async def _sg_calib_reminder_loop(self) -> None:
        """Resend SG Calibrator Telegram approval message every REMINDER_SEC
        if user hasn't responded (no approve/reject button press).

        Scans Redis for sg_calib:pending:* keys with status=PENDING.
        If last_reminder_ms older than REMINDER_SEC → resend with buttons.
        Stops after REMINDER_MAX reminders or when status != PENDING.

        ENV:
          SG_CALIB_REMINDER_SEC  — interval between reminders (default: 1800 = 30 min)
          SG_CALIB_REMINDER_MAX  — max reminder count before giving up (default: 48 ≈ 24h)
        """
        reminder_sec = int(os.getenv("SG_CALIB_REMINDER_SEC", "1800"))
        reminder_max = int(os.getenv("SG_CALIB_REMINDER_MAX", "48"))
        check_interval = min(60, reminder_sec // 2)  # poll every 60s or half-interval
        notify_stream = os.getenv("NOTIFY_STREAM", "notify:telegram")

        print(
            f"🔔 SG Calib reminder loop started: interval={reminder_sec}s, "
            f"max={reminder_max}, check_every={check_interval}s"
        )

        while self.running:
            try:
                await asyncio.sleep(check_interval)

                # Scan for pending calibration approvals
                cursor = 0
                now_ms = get_ny_time_millis()
                pending_found = 0
                reminders_sent = 0

                while True:
                    cursor, keys = await asyncio.to_thread(
                        lambda c=cursor: self.r.scan(cursor=c, match="sg_calib:pending:*", count=10000)
                    )
                    for key in keys:
                        try:
                            raw = await asyncio.to_thread(self.r.get, key)
                            if not raw:
                                continue
                            pending = json.loads(raw)

                            # Only process PENDING records
                            if pending.get("status") != "PENDING":
                                continue

                            pending_found += 1
                            run_id = pending.get("run_id", "")
                            last_reminder = int(pending.get("last_reminder_ms") or pending.get("created_at_ms", 0))
                            reminder_count = int(pending.get("reminder_count", 0))

                            # Check max reminders
                            if reminder_count >= reminder_max:
                                print(
                                    f"⚠️ SG Calib reminder: run_id={run_id} hit max "
                                    f"({reminder_count}/{reminder_max}), marking EXPIRED"
                                )
                                pending["status"] = "EXPIRED"
                                pending["expired_at_ms"] = now_ms
                                await asyncio.to_thread(
                                    self.r.set, key,
                                    json.dumps(pending, ensure_ascii=False),
                                )
                                # Keep TTL from original set
                                continue

                            # Check if enough time has passed since last reminder
                            elapsed_ms = now_ms - last_reminder
                            if elapsed_ms < reminder_sec * 1000:
                                continue

                            # Build and send reminder
                            reminder_count += 1
                            precision = pending.get("veto_precision", 0)
                            lift = pending.get("veto_lift", 0)
                            streak = pending.get("proof_streak", 0)
                            mode = pending.get("effective_mode", "shadow")
                            created_min_ago = int((now_ms - int(pending.get("created_at_ms", now_ms))) / 60000)

                            reminder_text = (
                                f"🔔 <b>НАПОМИНАНИЕ: G5 Strong Gate ожидает решение!</b>\n"
                                f"\n"
                                f"⏱️ Ожидание: <code>{created_min_ago} мин</code> "
                                f"(напоминание #{reminder_count})\n"
                                f"\n"
                                f"📊 <b>Mode:</b> <code>{mode}</code>\n"
                                f"📈 Precision: <code>{precision:.1%}</code>\n"
                                f"🔺 Lift: <code>{lift:+.1%}</code>\n"
                                f"📊 Streak: <code>{streak}</code>\n"
                                f"\n"
                                f"<b>Нажмите кнопку для подтверждения или отката:</b>\n"
                                f"Run ID: <code>{run_id}</code>"
                            )

                            buttons = [[
                                {"text": "✅ Full Enforce", "callback_data": f"sg_calib_approve:{run_id}"},
                                {"text": "⬇️ Revert Shadow", "callback_data": f"sg_calib_reject:{run_id}"},
                            ]]
                            buttons_json = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))

                            # Send via notify:telegram stream
                            fields = {
                                "type": "report",
                                "text": reminder_text,
                                "buttons": buttons_json,
                                "ts": str(now_ms),
                            }
                            await asyncio.to_thread(
                                lambda: self.r.xadd(
                                    notify_stream, fields,
                                    maxlen=200000, approximate=True,
                                )
                            )

                            # Update pending record
                            pending["last_reminder_ms"] = now_ms
                            pending["reminder_count"] = reminder_count
                            await asyncio.to_thread(
                                lambda k=key, v=json.dumps(pending, ensure_ascii=False): self.r.set(k, v)
                            )
                            # Preserve TTL — we don't set ex= so it keeps original TTL

                            reminders_sent += 1
                            print(
                                f"🔔 SG Calib reminder #{reminder_count} sent for "
                                f"run_id={run_id} (waiting {created_min_ago}m)"
                            )

                        except Exception as e:
                            print(f"⚠️ SG Calib reminder key error ({key}): {e}")
                            continue

                    if cursor == 0:
                        break

                if reminders_sent > 0:
                    print(
                        f"🔔 SG Calib reminder cycle: {reminders_sent} sent, "
                        f"{pending_found} pending total"
                    )

            except Exception as e:
                print(f"❌ SG Calib reminder loop error: {e}")
                await asyncio.sleep(30)


# =============================================================================
# Main Logic
# =============================================================================

GROUP = os.getenv("NOTIFY_GROUP", "notify-group")
CONSUMER = os.getenv("NOTIFY_CONSUMER", f"notify-consumer-{os.getpid()}")
MAX_RETRIES = int(os.getenv("NOTIFY_MAX_RETRIES", "5"))
message_log_counter = 0
MESSAGE_LOG_INTERVAL = 1

def get_redis(url: str) -> redis.Redis:
    return redis.Redis.from_url(
        url,
        decode_responses=True,
        socket_timeout=10,
        socket_connect_timeout=5,
        max_connections=10,
        health_check_interval=30
    )

def is_nogroup_error(error: Exception) -> bool:
    message = str(error)
    return "NOGROUP" in message or "No such key" in message or "no such key" in message

def ensure_consumer_group(client: redis.Redis, stream_name: str, group_name: str) -> None:
    max_retries = 30
    retry_count = 0
    while retry_count < max_retries:
        try:
            client.xgroup_create(name=stream_name, groupname=group_name, id="$", mkstream=True)
            print(f"✅ consumer group created: {group_name}")
            return
        except ResponseError as exc:
            error_msg = str(exc)
            if "BUSYGROUP" in error_msg:
                print(f"ℹ️ consumer group already exists: {group_name}")
                return
            if "Redis is loading the dataset in memory" in error_msg:
                retry_count += 1
                wait_time = min(5 * retry_count, 30)
                print(f"⚠️ Redis loading data ({retry_count}/{max_retries}), waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            if is_nogroup_error(exc):
                print(f"⚠️ Stream {stream_name} missing, creating...")
                client.xadd(stream_name, {"bootstrap": "1"}, maxlen=200000)
                client.xgroup_create(name=stream_name, groupname=group_name, id="$", mkstream=True)
                print(f"✅ consumer group recreated: {group_name}")
                return
            raise
    raise RuntimeError(f"Redis is still loading dataset after {max_retries} attempts")


async def handle_message(entry: Dict[str, Any], stream_name: str = None, message_id: str = None, redis: Any = None) -> bool:
    global message_log_counter

    # IMPORTANT: entry is already normalized by the main loop before calling handle_message
    
    if not NOTIFY_ENABLED:
        print(f"⚠️ notify_worker: notifications disabled")
        return True
        
    try:
        message_log_counter += 1
        msg_type = entry.get("type")
        
        # ✅ PRIORITY 1: Handle reports and alerts
        if msg_type in ("report", "alert"):
            text = entry.get("text", "")
            if not text:
                print(f"⚠️ notify_worker: skipping empty {msg_type}")
                return True
            
            # Buttons should be a list or dict if normalized correctly, 
            # OR a JSON string if simple normalization happened.
            # Since we added "buttons" to JSON_FIELD_KEYS, normalize_entry MUST maintain it as list/dict.
            buttons = entry.get("buttons")
            
            # Debugging for safety
            if buttons:
                print(f"🔧 DEBUG: Report buttons type={type(buttons)}")
                
            success = await send_html_to_telegram(text, buttons=buttons)
            
            if message_log_counter % MESSAGE_LOG_INTERVAL == 0:
                status = "✅" if success else "❌"
                label = "sent" if success else "delivery failed"
                print(f"{status} Report #{message_log_counter} {label} ({len(text)} chars)")
            return success
        
        # ✅ PRIORITY 2: Handle Signals (Outbox or Legacy)
        signal_payload = entry.get("signal_payload")
        signal_settings = entry.get("signal_settings")

        if signal_payload and isinstance(signal_payload, dict):
            # Outbox Signal
            parsed = dict(signal_payload)
            if signal_settings and isinstance(signal_settings, dict):
                parsed["signal_settings"] = signal_settings

            source = parsed.get("source", "OrderFlow")
            raw = {
                "source": source,
                "signal_settings": signal_settings,
                "envelope_type": "outbox_signal",
            }

            _attach_outbox_meta(redis_client=redis, entry=entry, parsed=parsed, raw=raw)

        elif "text" in entry and "side" in entry and "price" in entry:
            # XAUUSD Format
            text = entry.get("text", "")
            side = entry.get("side", "")
            price = entry.get("price", "")
            
            symbol_match = re.search(r'(XAU\w*|BTC\w*|ETH\w*|[A-Z]{3,})', text)
            symbol = symbol_match.group(1) if symbol_match else "XAUUSD"
            
            risk_json = entry.get("risk")
            stop = None
            tp_list = []
            if isinstance(risk_json, dict):
                stop = risk_json.get("sl")
                tp_list = risk_json.get("tp_levels", [])
            
            parsed = {
                "symbol": symbol,
                "direction": side,
                "entry": price,
                "stop": stop or "",
                "tp": tp_list or [],
                "leverage": entry.get("lot", "1.0"),
                "confidence": None,
                "timeframe": None,
                "exchange": "MT5",
                "source": "XAUUSD OrderFlow",
                "orderType": entry.get("note", "Market"),
                "profitPct": None,
                "raw_text": text,
                "is_xauusd": True,
            }
            # Special raw construction for XAUUSD to bypass formatting
            raw = {
                "chat_title": "XAUUSD OrderFlow Analysis",
                "username": "scanner-python-worker",
                "text": text,
                "is_xauusd": True
            }
            # send logic below handles this
            await notify_parsed_signal(parsed, raw, stream_name=stream_name, message_id=message_id)
            return True

        else:
            # Legacy/Standard Signal
            existing_text = entry.get("text")
            symbol = entry.get("symbol")
            if not existing_text or not symbol:
                # Logging disabled: skipping invalid signal output
                # print(f"⚠️ notify_worker: skipping invalid signal (missing text or symbol). Entry: {json.dumps(entry, ensure_ascii=False)}")
                return True

            parsed = {
                "symbol": entry.get("symbol") or "",
                "direction": entry.get("direction") or "",
                "entry": entry.get("entry") or "",
                "stop": entry.get("stop") or "",
                "tp": [],
                "leverage": entry.get("leverage") or "",
                "confidence": entry.get("confidence") or "",
                "timeframe": entry.get("timeframe") or "",
                "exchange": entry.get("exchange") or "",
                "source": entry.get("source") or entry.get("username") or entry.get("chat_title") or "Unknown Channel",
                "orderType": entry.get("orderType") or "",
                "profitPct": entry.get("profitPct") or "",
                "raw_text": existing_text
            }
            
            tp_raw = entry.get("tp")
            if tp_raw:
                if isinstance(tp_raw, list):
                    parsed["tp"] = tp_raw
                elif isinstance(tp_raw, str):
                     try:
                        if tp_raw.startswith('[') and tp_raw.endswith(']'):
                            parsed["tp"] = json.loads(tp_raw)
                        else:
                            parsed["tp"] = [float(x.strip()) for x in tp_raw.split(",") if x.strip()]
                     except Exception:
                        parsed["tp"] = []

            raw = {
                "chat_title": entry.get("chat_title"),
                "username": entry.get("username"),
                "text": existing_text
            }

        await notify_parsed_signal(parsed, raw, stream_name=stream_name, message_id=message_id)
        return True

    except Exception as e:
        print(f"❌ notify_worker: send failed: {e}")
        return False


async def main():
    notify_stream = os.getenv("NOTIFY_STREAM", "notify:telegram")
    print(f"\n🚀 NOTIFY-WORKER: Started. GROUP={GROUP}, CONSUMER={CONSUMER}")
    
    settings = load_settings()
    # DEBUG: Mask password but print host
    safe_url = settings.redis_url
    if "@" in safe_url:
        safe_url = safe_url.split("://")[0] + "://***:***@" + safe_url.split("@")[1]
    print(f"Connecting to REDIS_URL: {safe_url}")
    
    r = get_redis(settings.redis_url)
    
    # Start poller
    poller = BotCallbackPoller(r)
    safe_create_task(poller.start())
    
    # Ensure group
    try:
        ensure_consumer_group(r, notify_stream, GROUP)
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ Init failed: {e}")
        return

    # Backfill loop
    print(f"🔄 Starting backfill...")
    backfill_total = 0
    while True:
        try:
            msgs = await asyncio.to_thread(
                r.xreadgroup, GROUP, CONSUMER, {notify_stream: "0"}, count=50, block=1000
            )
            if not msgs or not msgs[0][1]:
                print(f"✅ Backfill complete.")
                break
            
            batch = msgs[0][1]
            backfill_total += len(batch)
            print(f"📥 Backfill batch: {len(batch)} msgs")
            
            for stream_name, entries in msgs:
                for msg_id, fields in entries:
                    entry = normalize_entry(fields)
                    if await handle_message(entry, stream_name, msg_id, r):
                        r.xack(stream_name, GROUP, msg_id)
                        
        except Exception as e:
            print(f"❌ Backfill error: {e}")
            await asyncio.sleep(1)

    # Main loop
    print(f"👂 Listening for new messages on {notify_stream}...")
    backoff = 1
    processed = 0
    dlq_retry_interval = int(os.getenv("NOTIFY_DLQ_RETRY_INTERVAL", "300"))  # 5 минут
    last_dlq_retry = time.time()
    
    while True:
        try:
            msgs = await asyncio.to_thread(
                r.xreadgroup, GROUP, CONSUMER, {notify_stream: ">"}, count=10, block=5000
            )
            
            if not msgs or not msgs[0][1]:
                backoff = 1
                # DLQ retry при простое
                now = time.time()
                if now - last_dlq_retry >= dlq_retry_interval:
                    last_dlq_retry = now
                    try:
                        from notifier import _get_notifier
                        notifier = _get_notifier()
                        await notifier.retry_dlq(max_items=10)
                    except Exception as e:
                        print(f"⚠️ DLQ retry error: {e}")
                await asyncio.sleep(0)  # Yield to loop
                continue
                
            for stream_name, entries in msgs:
                for msg_id, fields in entries:
                    processed += 1
                    
                    # 1. Normalize ONCE
                    entry = normalize_entry(fields)
                    
                    # 2. Handle
                    if await handle_message(entry, stream_name, msg_id, r):
                        r.xack(stream_name, GROUP, msg_id)
            
        except Exception as e:
            if is_nogroup_error(e):
                print("⚠️ Group missing, recreating...")
                ensure_consumer_group(r, notify_stream, GROUP)
                continue
            
            print(f"❌ Read loop error: {e}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user")
    except Exception as e:
        print(f"💥 Fatal error: {e}")
