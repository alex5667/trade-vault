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
        # Launch ADV (G10) Calibrator reminder loop in background
        safe_create_task(self._adv_calib_reminder_loop())
        # Launch RG (G14) Research Guard Calibrator reminder loop in background
        safe_create_task(self._rg_calib_reminder_loop())
        # Launch Cont Ctx Window Calibrator reminder loop in background
        safe_create_task(self._cont_ctx_calib_reminder_loop())
        # Launch OF Gate Report autoreject loop in background
        safe_create_task(self._of_gate_recs_autoreject_loop())

        try:
            import httpx
            
            # Determine approval prefix
            self.approvals_prefix = os.getenv("ENTRY_POLICY_APPROVALS_PREFIX", "cfg:suggestions:entry_policy:approvals")
            
            # read=40s covers long-poll timeout=15 + network buffer.
            # httpx.ReadTimeout.str() is empty — always log type+repr.
            _tg_timeout = httpx.Timeout(connect=10.0, read=40.0, write=10.0, pool=5.0)
            async with httpx.AsyncClient(timeout=_tg_timeout) as client:
                pubsub = self.r.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe("telegram_callbacks")
                while self.running:
                    try:
                        msg = pubsub.get_message(ignore_subscribe_messages=True)
                        if msg and msg["type"] == "message":
                            try:
                                cb_data = json.loads(msg["data"])
                                update = {"callback_query": cb_data}
                                print(f"🔧 BotCallbackPoller received update via PubSub")
                                await self.handle_update(client, update)
                            except Exception as parse_e:
                                print(f"⚠️ Invalid callback JSON: {parse_e}")
                        else:
                            await asyncio.sleep(0.5)
                    except Exception as e:
                        print(f"❌ BotCallbackPoller loop error: {type(e).__name__}: {e!r}")
                        await asyncio.sleep(5)
        except Exception as e:
            print(f"❌ BotCallbackPoller fatal crash: {type(e).__name__}: {e!r}")

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
            # Selective enforcement: only enforce symbols where shadow A/B
            # shows BETTER or NEUTRAL. Skip WORSE symbols (keep shadow).
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
                
                # Per-symbol shadow results for selective enforcement
                shadow_per_symbol = pending.get("shadow_per_symbol", {})
                
                # Scan trail:calib:* keys and selectively enforce
                enforced_keys = []
                skipped_keys = []
                calib_prefix = os.getenv("TRAIL_CALIB_KEY_PREFIX", "trail:calib") or "trail:calib"
                cursor = 0
                while True:
                    cursor, keys = self.r.scan(cursor=cursor, match=f"{calib_prefix}:*", count=10000)
                    for k in keys:
                        # Skip pending/stability/shadow auxiliary keys
                        if ":pending:" in k or ":stability:" in k or ":shadow:" in k:
                            continue
                        
                        # Extract symbol:regime from key (trail:calib:BTCUSDT:na -> BTCUSDT:na)
                        suffix = k.replace(f"{calib_prefix}:", "", 1)
                        
                        # Check shadow recommendation for this symbol
                        shadow_info = shadow_per_symbol.get(suffix, {})
                        recommendation = shadow_info.get("recommendation", "")
                        delta_r = shadow_info.get("delta_pnl_r", 0.0)
                        
                        if shadow_info and delta_r < 0:
                            # Keep in shadow — calibration would hurt this symbol (any negative Δ)
                            skipped_keys.append({
                                "key": suffix,
                                "delta_r": delta_r,
                                "reason": recommendation or "NEGATIVE_DELTA",
                            })
                        else:
                            # Δ >= 0, or no shadow data (fail-open) → enforce
                            self.r.hset(k, "mode", "enforce")
                            enforced_keys.append({
                                "key": suffix,
                                "delta_r": delta_r,
                                "recommendation": recommendation or "NO_DATA",
                            })
                    if cursor == 0:
                        break
                
                n_enforced = len(enforced_keys)
                n_skipped = len(skipped_keys)
                
                # Answer callback
                await client.post(
                    f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                    json={"callback_query_id": cb_id, "text": f"✅ Trail approved! ({n_enforced} enforced, {n_skipped} skipped)"}
                )
                
                # Build rich confirmation with param details
                param_details = pending.get("param_details", [])
                shadow_data = pending.get("shadow_summary", {})
                stability_data = pending.get("stability_summary", {})

                # Build enforced params table (only enforced symbols)
                enforced_syms = {ek["key"].split(":")[0] for ek in enforced_keys}
                enforced_lines = []
                for pd in sorted(param_details, key=lambda x: x.get("symbol", "")):
                    sym_regime = f"{pd['symbol']}:{pd.get('regime', 'na')}"
                    if sym_regime in {ek["key"] for ek in enforced_keys}:
                        sr_info = shadow_per_symbol.get(sym_regime, {})
                        delta_str = f" Δ={sr_info.get('delta_pnl_r', 0):+.3f}R" if sr_info else ""
                        enforced_lines.append(
                            f"  <code>{pd['symbol']}</code>: "
                            f"cb={pd['callback_atr_mult']:.3f}×ATR "
                            f"off={pd['activate_offset_bps']:.1f}bps "
                            f"lock={pd['min_profit_lock_r']:.3f}R"
                            f"{delta_str}"
                        )
                enforced_block = "\n".join(enforced_lines) if enforced_lines else "  (none)"

                # Build skipped symbols table
                skipped_lines = []
                for sk in sorted(skipped_keys, key=lambda x: x.get("key", "")):
                    skipped_lines.append(
                        f"  <code>{sk['key']}</code>: Δ={sk['delta_r']:+.3f}R ({sk['reason']})"
                    )
                skipped_block = "\n".join(skipped_lines) if skipped_lines else "  (none)"

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
                    f"✅ <b>Trail Calibration APPROVED (selective)</b>\n"
                    f"by @{username}\n\n"
                    f"<b>✅ Enforced ({n_enforced}):</b>\n"
                    f"{enforced_block}\n\n"
                    f"<b>⏭️ Skipped ({n_skipped}, kept shadow):</b>\n"
                    f"{skipped_block}\n\n"
                    f"{shadow_line}"
                    f"{stability_line}\n"
                    f"<i>Executor uses calibrated params for enforced symbols only.</i>\n\n"
                    f"Run ID: <code>{run_id}</code>"
                )
                self.r.xadd(notify_stream, {"type": "report", "text": confirm_text}, maxlen=20000, approximate=True)
                
                # Update pending with enforcement details
                if pending:
                    pending["enforced_keys"] = [ek["key"] for ek in enforced_keys]
                    pending["skipped_keys"] = [sk["key"] for sk in skipped_keys]
                    self.r.set(pending_key, json.dumps(pending, ensure_ascii=False), keepttl=True)
                
                # Remove buttons
                await self._remove_buttons(client, chat_id, message_id)
                
                print(f"✅ Trail calibration approved: {username} -> {run_id} ({n_enforced} enforced, {n_skipped} skipped)")
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

        elif data.startswith("adv_calib_approve:"):
            # Format: adv_calib_approve:<run_id>
            # → Promote G10 adverse gate symbols from shadow → enforce
            try:
                run_id = data.split(":", 1)[1]
                pending_key = f"adv_calib:pending:{run_id}"

                raw_pending = self.r.get(pending_key)
                if not raw_pending:
                    await client.post(
                        f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                        json={"callback_query_id": cb_id, "text": "⚠️ Pending record expired or not found"}
                    )
                    return

                pending = json.loads(raw_pending)
                symbols = pending.get("symbols", [])
                pending["status"] = "APPROVED"
                pending["approved_by"] = username
                pending["approved_at_ms"] = get_ny_time_millis()
                self.r.set(pending_key, json.dumps(pending, ensure_ascii=False), keepttl=True)

                # Switch each symbol to enforce
                switched = 0
                for sym in symbols:
                    dyn_key = f"config:orderflow:{sym}"
                    try:
                        self.r.hset(dyn_key, "adv_calib_mode", "enforce")
                        self.r.hset(dyn_key, "adverse_check_enable", "1")
                        switched += 1
                    except Exception:
                        pass

                    # Update per-symbol state
                    state_key = f"cfg:adv_calib:state:{sym}"
                    try:
                        state_raw = self.r.get(state_key)
                        if state_raw:
                            state = json.loads(state_raw)
                            state["mode"] = "enforce"
                            state["approved_by"] = username
                            self.r.set(state_key, json.dumps(state, ensure_ascii=False), keepttl=True)
                    except Exception:
                        pass

                await client.post(
                    f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                    json={"callback_query_id": cb_id, "text": f"🟢 G10 Adverse → ENFORCE ({switched} symbols)"}
                )

                sym_list = ", ".join(symbols[:10])
                sym_data = pending.get("symbol_data", {})

                confirm_text = (
                    f"🟢 <b>G10 Adverse Gate → ENFORCE</b>\n"
                    f"by @{username}\n\n"
                    f"<b>{switched}</b> symbols switched to <code>mode=enforce</code>\n"
                    f"Symbols: <code>{sym_list}</code>\n\n"
                    f"⚠️ <b>Adverse gate vetoes are now REAL.</b>\n"
                    f"Reversals without evidence will be blocked.\n\n"
                    f"<i>To revert: set <code>adv_calib_mode=disabled</code> per symbol</i>\n\n"
                    f"Run ID: <code>{run_id}</code>"
                )
                self.r.xadd(notify_stream, {"type": "report", "text": confirm_text}, maxlen=20000, approximate=True)
                await self._remove_buttons(client, chat_id, message_id)

                print(f"🟢 ADV Calib approved (enforce): {username} -> {run_id} ({switched} symbols)")
            except Exception as e:
                print(f"❌ ADV Calib approve error: {e}")

        elif data.startswith("adv_calib_reject:"):
            # Format: adv_calib_reject:<run_id>
            # → Disable G10 adverse gate for listed symbols
            try:
                run_id = data.split(":", 1)[1]
                pending_key = f"adv_calib:pending:{run_id}"

                raw_pending = self.r.get(pending_key)
                pending = {}
                if raw_pending:
                    pending = json.loads(raw_pending)
                    pending["status"] = "REJECTED"
                    pending["rejected_by"] = username
                    pending["rejected_at_ms"] = get_ny_time_millis()
                    self.r.set(pending_key, json.dumps(pending, ensure_ascii=False), keepttl=True)

                symbols = pending.get("symbols", [])

                switched = 0
                for sym in symbols:
                    dyn_key = f"config:orderflow:{sym}"
                    try:
                        self.r.hset(dyn_key, "adv_calib_mode", "disabled")
                        self.r.hset(dyn_key, "adverse_check_enable", "0")
                        switched += 1
                    except Exception:
                        pass

                    state_key = f"cfg:adv_calib:state:{sym}"
                    try:
                        state_raw = self.r.get(state_key)
                        if state_raw:
                            state = json.loads(state_raw)
                            state["mode"] = "disabled"
                            state["proof_streak"] = 0
                            state["rejected_by"] = username
                            self.r.set(state_key, json.dumps(state, ensure_ascii=False), keepttl=True)
                    except Exception:
                        pass

                await client.post(
                    f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                    json={"callback_query_id": cb_id, "text": f"⬇️ G10 Adverse disabled ({switched} symbols)"}
                )

                sym_list = ", ".join(symbols[:10])
                reject_text = (
                    f"⚪ <b>G10 Adverse Gate → DISABLED</b>\n"
                    f"by @{username}\n\n"
                    f"<b>{switched}</b> symbols disabled: <code>{sym_list}</code>\n\n"
                    f"<i>Proof streaks reset. Calibrator will re-evaluate next cycle.</i>\n\n"
                    f"Run ID: <code>{run_id}</code>"
                )
                self.r.xadd(notify_stream, {"type": "report", "text": reject_text}, maxlen=20000, approximate=True)
                await self._remove_buttons(client, chat_id, message_id)

                print(f"⬇️ ADV Calib rejected (disabled): {username} -> {run_id} ({switched} symbols)")
            except Exception as e:
                print(f"❌ ADV Calib reject error: {e}")

        elif data.startswith("rg_calib_approve:"):
            # Format: rg_calib_approve:<run_id>
            # → Promote G14 Research Guard from REPORT-ONLY → ENFORCE
            try:
                run_id = data.split(":", 1)[1]
                pending_key = f"rg_calib:pending:{run_id}"

                raw_pending = self.r.get(pending_key)
                if not raw_pending:
                    await client.post(
                        f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                        json={"callback_query_id": cb_id, "text": "⚠️ Pending record expired or not found"}
                    )
                    return

                pending = json.loads(raw_pending)
                pending["status"] = "APPROVED"
                pending["approved_by"] = username
                pending["approved_at_ms"] = get_ny_time_millis()
                self.r.set(pending_key, json.dumps(pending, ensure_ascii=False), keepttl=True)

                # Switch blocker to ENFORCE (report_only=0)
                blocker_key = os.getenv("STRATEGY_RESEARCH_GUARD_BLOCKER_KEY", "cfg:research_guard:blocker:v1")
                try:
                    blocker_raw = self.r.get(blocker_key)
                    blocker = json.loads(blocker_raw) if blocker_raw else {}
                    blocker["report_only"] = 0
                    blocker["rg_calib_mode"] = "enforce"
                    blocker["rg_calib_approved_by"] = username
                    blocker["rg_calib_approved_at_ms"] = get_ny_time_millis()
                    self.r.set(blocker_key, json.dumps(blocker, ensure_ascii=False))
                except Exception as be:
                    print(f"⚠️ RG Calib: failed to update blocker key: {be}")

                # Update calibrator state
                state_key = "cfg:rg_calib:state"
                try:
                    state_raw = self.r.get(state_key)
                    if state_raw:
                        state = json.loads(state_raw)
                        state["mode"] = "enforce"
                        state["approved_by"] = username
                        self.r.set(state_key, json.dumps(state, ensure_ascii=False), keepttl=True)
                except Exception:
                    pass

                await client.post(
                    f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                    json={"callback_query_id": cb_id, "text": "🔴 G14 Research Guard → ENFORCE"}
                )

                psr = pending.get("latest_psr", 0)
                dsr = pending.get("latest_dsr", 0)
                pbo = pending.get("latest_pbo", 0)

                confirm_text = (
                    f"🔴 <b>G14 Research Guard → ENFORCE</b>\n"
                    f"by @{username}\n\n"
                    f"<code>STRATEGY_RESEARCH_GUARD_REPORT_ONLY=0</code>\n\n"
                    f"📈 PSR: <code>{psr}</code>\n"
                    f"📈 DSR: <code>{dsr}</code>\n"
                    f"📉 PBO: <code>{pbo}</code>\n\n"
                    f"⚠️ <b>Blocker now enforces.</b>\n"
                    f"Rollout-sensitive jobs (promote/apply) will be blocked\n"
                    f"if nightly metrics degrade below thresholds.\n\n"
                    f"<i>To revert: approve rollback or set report_only=1</i>\n\n"
                    f"Run ID: <code>{run_id}</code>"
                )
                self.r.xadd(notify_stream, {"type": "report", "text": confirm_text}, maxlen=20000, approximate=True)
                await self._remove_buttons(client, chat_id, message_id)

                print(f"🔴 RG Calib approved (enforce): {username} -> {run_id}")
            except Exception as e:
                print(f"❌ RG Calib approve error: {e}")

        elif data.startswith("rg_calib_reject:"):
            # Format: rg_calib_reject:<run_id>
            # → Keep G14 in REPORT-ONLY mode, reset proof streak
            try:
                run_id = data.split(":", 1)[1]
                pending_key = f"rg_calib:pending:{run_id}"

                raw_pending = self.r.get(pending_key)
                pending = {}
                if raw_pending:
                    pending = json.loads(raw_pending)
                    pending["status"] = "REJECTED"
                    pending["rejected_by"] = username
                    pending["rejected_at_ms"] = get_ny_time_millis()
                    self.r.set(pending_key, json.dumps(pending, ensure_ascii=False), keepttl=True)

                # Ensure blocker stays in REPORT-ONLY
                blocker_key = os.getenv("STRATEGY_RESEARCH_GUARD_BLOCKER_KEY", "cfg:research_guard:blocker:v1")
                try:
                    blocker_raw = self.r.get(blocker_key)
                    blocker = json.loads(blocker_raw) if blocker_raw else {}
                    blocker["report_only"] = 1
                    blocker["rg_calib_mode"] = "report"
                    self.r.set(blocker_key, json.dumps(blocker, ensure_ascii=False))
                except Exception:
                    pass

                # Reset calibrator state streak
                state_key = "cfg:rg_calib:state"
                try:
                    state_raw = self.r.get(state_key)
                    if state_raw:
                        state = json.loads(state_raw)
                        state["mode"] = "report"
                        state["proof_streak"] = 0
                        state["rejected_by"] = username
                        self.r.set(state_key, json.dumps(state, ensure_ascii=False), keepttl=True)
                except Exception:
                    pass

                await client.post(
                    f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                    json={"callback_query_id": cb_id, "text": "🟢 G14 → kept REPORT-ONLY"}
                )

                reject_text = (
                    f"🟢 <b>G14 Research Guard → REPORT-ONLY</b>\n"
                    f"by @{username}\n\n"
                    f"Proof streak reset. Calibrator will re-evaluate next cycle.\n\n"
                    f"Run ID: <code>{run_id}</code>"
                )
                self.r.xadd(notify_stream, {"type": "report", "text": reject_text}, maxlen=20000, approximate=True)
                await self._remove_buttons(client, chat_id, message_id)

                print(f"🟢 RG Calib rejected (report-only): {username} -> {run_id}")
            except Exception as e:
                print(f"❌ RG Calib reject error: {e}")

        elif data.startswith("cont_ctx_approve:"):
            # Format: cont_ctx_approve:<run_id>
            # → Apply recommended cont_ctx_valid_ms per symbol
            try:
                run_id = data.split(":", 1)[1]
                pending_key = f"cont_ctx_calib:pending:{run_id}"

                raw_pending = self.r.get(pending_key)
                if not raw_pending:
                    await client.post(
                        f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                        json={"callback_query_id": cb_id, "text": "⚠️ Pending record expired or not found"}
                    )
                    return

                pending = json.loads(raw_pending)
                symbols = pending.get("symbols", [])
                recommendations = pending.get("recommendations", {})
                pending["status"] = "APPROVED"
                pending["approved_by"] = username
                pending["approved_at_ms"] = get_ny_time_millis()
                self.r.set(pending_key, json.dumps(pending, ensure_ascii=False), keepttl=True)

                # Apply recommended windows with bounded step
                applied = []
                max_step_ms = 30000
                min_ms = 90000
                max_ms = 240000
                for sym in symbols:
                    rec = recommendations.get(sym, {})
                    recommended_ms = int(rec.get("recommended_ms", 0))
                    if recommended_ms <= 0:
                        continue
                    dyn_key = f"config:orderflow:{sym}"
                    try:
                        cur_raw = self.r.hget(dyn_key, "cont_ctx_valid_ms")
                        cur = int(cur_raw) if cur_raw else 120000
                        bounded = max(min_ms, min(max_ms, recommended_ms))
                        if abs(bounded - cur) > max_step_ms:
                            bounded = cur + max_step_ms if bounded > cur else cur - max_step_ms
                        bounded = max(min_ms, min(max_ms, bounded))
                        self.r.hset(dyn_key, "cont_ctx_valid_ms", str(bounded))
                        applied.append(f"{sym}: {cur}→{bounded}ms")
                    except Exception:
                        pass

                await client.post(
                    f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                    json={"callback_query_id": cb_id, "text": f"✅ Cont Ctx Window applied ({len(applied)} sym)"}
                )

                applied_block = "\n".join(f"  • <code>{a}</code>" for a in applied) if applied else "  (none)"
                confirm_text = (
                    f"✅ <b>Cont Ctx Window — APPLIED</b>\n"
                    f"by @{username}\n\n"
                    f"<b>{len(applied)}</b> symbols updated:\n"
                    f"{applied_block}\n\n"
                    f"<i>Engine использует новые окна после перезагрузки config.</i>\n\n"
                    f"Run ID: <code>{run_id}</code>"
                )
                self.r.xadd(notify_stream, {"type": "report", "text": confirm_text}, maxlen=20000, approximate=True)
                await self._remove_buttons(client, chat_id, message_id)

                print(f"✅ Cont Ctx Calib approved: {username} -> {run_id} ({len(applied)} sym)")
            except Exception as e:
                print(f"❌ Cont Ctx Calib approve error: {e}")

        elif data.startswith("cont_ctx_reject:"):
            # Format: cont_ctx_reject:<run_id>
            # → Reject cont_ctx window recommendation, keep current values
            try:
                run_id = data.split(":", 1)[1]
                pending_key = f"cont_ctx_calib:pending:{run_id}"

                raw_pending = self.r.get(pending_key)
                pending = {}
                if raw_pending:
                    pending = json.loads(raw_pending)
                    pending["status"] = "REJECTED"
                    pending["rejected_by"] = username
                    pending["rejected_at_ms"] = get_ny_time_millis()
                    self.r.set(pending_key, json.dumps(pending, ensure_ascii=False), keepttl=True)

                symbols = pending.get("symbols", [])
                sym_list = ", ".join(symbols[:10])

                await client.post(
                    f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                    json={"callback_query_id": cb_id, "text": "❌ Cont Ctx Window — rejected"}
                )

                reject_text = (
                    f"❌ <b>Cont Ctx Window — REJECTED</b>\n"
                    f"by @{username}\n\n"
                    f"Символы: <code>{sym_list}</code>\n\n"
                    f"<i>Текущие окна сохранены. Калибратор переоценит в следующем цикле.</i>\n\n"
                    f"Run ID: <code>{run_id}</code>"
                )
                self.r.xadd(notify_stream, {"type": "report", "text": reject_text}, maxlen=20000, approximate=True)
                await self._remove_buttons(client, chat_id, message_id)

                print(f"❌ Cont Ctx Calib rejected: {username} -> {run_id}")
            except Exception as e:
                print(f"❌ Cont Ctx Calib reject error: {e}")

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

    # -----------------------------------------------------------------
    # ADV (G10) Calibrator reminder loop
    # -----------------------------------------------------------------

    async def _adv_calib_reminder_loop(self) -> None:
        """Resend G10 Adverse Calibrator Telegram approval message every REMINDER_SEC
        if user hasn't responded (no approve/reject button press).

        Scans Redis for adv_calib:pending:* keys with status=PENDING.
        If last_reminder_ms older than REMINDER_SEC → resend with buttons.
        Stops after REMINDER_MAX reminders or when status != PENDING.

        ENV:
          ADV_CALIB_REMINDER_SEC  — interval between reminders (default: 1800 = 30 min)
          ADV_CALIB_REMINDER_MAX  — max reminder count before giving up (default: 48 ≈ 24h)
        """
        reminder_sec = int(os.getenv("ADV_CALIB_REMINDER_SEC", "1800"))
        reminder_max = int(os.getenv("ADV_CALIB_REMINDER_MAX", "48"))
        check_interval = min(60, reminder_sec // 2)
        notify_stream = os.getenv("NOTIFY_STREAM", "notify:telegram")

        print(
            f"🔔 ADV Calib reminder loop started: interval={reminder_sec}s, "
            f"max={reminder_max}, check_every={check_interval}s"
        )

        while self.running:
            try:
                await asyncio.sleep(check_interval)

                cursor = 0
                now_ms = get_ny_time_millis()
                pending_found = 0
                reminders_sent = 0

                while True:
                    cursor, keys = await asyncio.to_thread(
                        lambda c=cursor: self.r.scan(cursor=c, match="adv_calib:pending:*", count=10000)
                    )
                    for key in keys:
                        try:
                            raw = await asyncio.to_thread(self.r.get, key)
                            if not raw:
                                continue
                            pending = json.loads(raw)

                            if pending.get("status") != "PENDING":
                                continue

                            pending_found += 1
                            run_id = pending.get("run_id", "")
                            last_reminder = int(pending.get("last_reminder_ms") or pending.get("created_at_ms", 0))
                            reminder_count = int(pending.get("reminder_count", 0))
                            symbols = pending.get("symbols", [])

                            # Max reminders → expire
                            if reminder_count >= reminder_max:
                                print(
                                    f"⚠️ ADV Calib reminder: run_id={run_id} hit max "
                                    f"({reminder_count}/{reminder_max}), marking EXPIRED"
                                )
                                pending["status"] = "EXPIRED"
                                pending["expired_at_ms"] = now_ms
                                await asyncio.to_thread(
                                    self.r.set, key,
                                    json.dumps(pending, ensure_ascii=False),
                                )
                                continue

                            # Not enough time passed?
                            elapsed_ms = now_ms - last_reminder
                            if elapsed_ms < reminder_sec * 1000:
                                continue

                            # Build reminder
                            reminder_count += 1
                            sym_list = ", ".join(symbols[:10])
                            sym_data = pending.get("symbol_data", {})
                            created_min_ago = int((now_ms - int(pending.get("created_at_ms", now_ms))) / 60000)

                            # Precision summary from symbol_data
                            prec_summary = ""
                            for sym in symbols[:5]:
                                sd = sym_data.get(sym, {})
                                p = sd.get("reversal_veto_precision", 0)
                                l = sd.get("reversal_veto_lift", 0)
                                prec_summary += f"  • <code>{sym}</code>: prec=<code>{p:.1%}</code> lift=<code>{l:+.1%}</code>\n"

                            reminder_text = (
                                f"🔔 <b>НАПОМИНАНИЕ: G10 Adverse Gate ожидает решение!</b>\n"
                                f"\n"
                                f"⏱️ Ожидание: <code>{created_min_ago} мин</code> "
                                f"(напоминание #{reminder_count})\n"
                                f"\n"
                                f"🛡️ <b>Символы в shadow ({len(symbols)}):</b>\n"
                                f"<code>{sym_list}</code>\n"
                                f"\n"
                                f"{prec_summary}"
                                f"\n"
                                f"<b>Нажмите кнопку для подтверждения или отката:</b>\n"
                                f"Run ID: <code>{run_id}</code>"
                            )

                            buttons = [[
                                {"text": f"🟢 Enforce ({len(symbols)} sym)", "callback_data": f"adv_calib_approve:{run_id}"},
                                {"text": "⬇️ Disable All", "callback_data": f"adv_calib_reject:{run_id}"},
                            ]]
                            buttons_json = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))

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

                            # Update pending
                            pending["last_reminder_ms"] = now_ms
                            pending["reminder_count"] = reminder_count
                            await asyncio.to_thread(
                                lambda k=key, v=json.dumps(pending, ensure_ascii=False): self.r.set(k, v)
                            )

                            reminders_sent += 1
                            print(
                                f"🔔 ADV Calib reminder #{reminder_count} sent for "
                                f"run_id={run_id} (waiting {created_min_ago}m, {len(symbols)} sym)"
                            )

                        except Exception as e:
                            print(f"⚠️ ADV Calib reminder key error ({key}): {e}")
                            continue

                    if cursor == 0:
                        break

                if reminders_sent > 0:
                    print(
                        f"🔔 ADV Calib reminder cycle: {reminders_sent} sent, "
                        f"{pending_found} pending total"
                    )

            except Exception as e:
                print(f"❌ ADV Calib reminder loop error: {e}")
                await asyncio.sleep(30)

    async def _rg_calib_reminder_loop(self) -> None:
        """Resend G14 Research Guard Calibrator approval message every REMINDER_SEC
        if user hasn't responded (no approve/reject button press).

        Scans Redis for rg_calib:pending:* keys with status=PENDING.
        If last_reminder_ms older than REMINDER_SEC → resend with buttons.
        Stops after REMINDER_MAX reminders or when status != PENDING.

        ENV:
          RG_CALIB_REMINDER_SEC   — interval between reminders (default: 1800 = 30 min)
          RG_CALIB_REMINDER_MAX   — max reminder count before giving up (default: 48 ≈ 24h)
        """
        reminder_sec = int(os.getenv("RG_CALIB_REMINDER_SEC", "1800"))
        reminder_max = int(os.getenv("RG_CALIB_REMINDER_MAX", "48"))
        check_interval = min(60, reminder_sec // 2)
        notify_stream = os.getenv("NOTIFY_STREAM", "notify:telegram")

        print(
            f"🔔 RG Calib reminder loop started: interval={reminder_sec}s, "
            f"max={reminder_max}, check_every={check_interval}s"
        )

        while self.running:
            try:
                await asyncio.sleep(check_interval)

                cursor = 0
                now_ms = get_ny_time_millis()
                pending_found = 0
                reminders_sent = 0

                while True:
                    cursor, keys = await asyncio.to_thread(
                        lambda c=cursor: self.r.scan(cursor=c, match="rg_calib:pending:*", count=10000)
                    )
                    for key in keys:
                        try:
                            raw = await asyncio.to_thread(self.r.get, key)
                            if not raw:
                                continue
                            pending = json.loads(raw)

                            if pending.get("status") != "PENDING":
                                continue

                            pending_found += 1
                            run_id = pending.get("run_id", "")
                            last_reminder = int(pending.get("last_reminder_ms") or pending.get("created_at_ms", 0))
                            reminder_count = int(pending.get("reminder_count", 0))

                            # Max reminders → expire
                            if reminder_count >= reminder_max:
                                print(
                                    f"⚠️ RG Calib reminder: run_id={run_id} hit max "
                                    f"({reminder_count}/{reminder_max}), marking EXPIRED"
                                )
                                pending["status"] = "EXPIRED"
                                pending["expired_at_ms"] = now_ms
                                await asyncio.to_thread(
                                    self.r.set, key,
                                    json.dumps(pending, ensure_ascii=False),
                                )
                                continue

                            # Not enough time passed?
                            elapsed_ms = now_ms - last_reminder
                            if elapsed_ms < reminder_sec * 1000:
                                continue

                            # Build reminder
                            reminder_count += 1
                            created_min_ago = int((now_ms - int(pending.get("created_at_ms", now_ms))) / 60000)

                            psr = pending.get("latest_psr", 0)
                            dsr = pending.get("latest_dsr", 0)
                            pbo = pending.get("latest_pbo", 0)
                            streak = pending.get("proof_streak", 0)

                            reminder_text = (
                                f"🔔 <b>НАПОМИНАНИЕ: G14 Research Guard ожидает решение!</b>\n"
                                f"\n"
                                f"⏱️ Ожидание: <code>{created_min_ago} мин</code> "
                                f"(напоминание #{reminder_count})\n"
                                f"\n"
                                f"📊 <b>Nightly Metrics:</b>\n"
                                f"  PSR: <code>{psr}</code>\n"
                                f"  DSR: <code>{dsr}</code>\n"
                                f"  PBO: <code>{pbo}</code>\n"
                                f"  Streak: <code>{streak}</code>\n"
                                f"\n"
                                f"<b>Нажмите кнопку для подтверждения:</b>\n"
                                f"Run ID: <code>{run_id}</code>"
                            )

                            buttons = [[
                                {"text": "🔴 Enforce (Block Deploys)", "callback_data": f"rg_calib_approve:{run_id}"},
                                {"text": "🟢 Keep Report-Only", "callback_data": f"rg_calib_reject:{run_id}"},
                            ]]
                            buttons_json = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))

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

                            # Update pending
                            pending["last_reminder_ms"] = now_ms
                            pending["reminder_count"] = reminder_count
                            await asyncio.to_thread(
                                lambda k=key, v=json.dumps(pending, ensure_ascii=False): self.r.set(k, v)
                            )

                            reminders_sent += 1
                            print(
                                f"🔔 RG Calib reminder #{reminder_count} sent for "
                                f"run_id={run_id} (waiting {created_min_ago}m)"
                            )

                        except Exception as e:
                            print(f"⚠️ RG Calib reminder key error ({key}): {e}")
                            continue

                    if cursor == 0:
                        break

                if reminders_sent > 0:
                    print(
                        f"🔔 RG Calib reminder cycle: {reminders_sent} sent, "
                        f"{pending_found} pending total"
                    )

            except Exception as e:
                print(f"❌ RG Calib reminder loop error: {e}")
                await asyncio.sleep(30)

    # -----------------------------------------------------------------
    # Cont Ctx Window Calibrator reminder loop
    # -----------------------------------------------------------------

    async def _cont_ctx_calib_reminder_loop(self) -> None:
        """Resend Cont Ctx Window Calibrator Telegram approval message every REMINDER_SEC
        if user hasn't responded (no approve/reject button press).

        Scans Redis for cont_ctx_calib:pending:* keys with status=PENDING.
        If last_reminder_ms older than REMINDER_SEC → resend with buttons.
        Stops after REMINDER_MAX reminders or when status != PENDING.

        ENV:
          CONT_CTX_CALIB_REMINDER_SEC  — interval between reminders (default: 1800 = 30 min)
          CONT_CTX_CALIB_REMINDER_MAX  — max reminder count before giving up (default: 48 ≈ 24h)
        """
        reminder_sec = int(os.getenv("CONT_CTX_CALIB_REMINDER_SEC", "1800"))
        reminder_max = int(os.getenv("CONT_CTX_CALIB_REMINDER_MAX", "48"))
        check_interval = min(60, reminder_sec // 2)
        notify_stream = os.getenv("NOTIFY_STREAM", "notify:telegram")

        print(
            f"🔔 Cont Ctx Calib reminder loop started: interval={reminder_sec}s, "
            f"max={reminder_max}, check_every={check_interval}s"
        )

        while self.running:
            try:
                await asyncio.sleep(check_interval)

                cursor = 0
                now_ms = get_ny_time_millis()
                pending_found = 0
                reminders_sent = 0

                while True:
                    cursor, keys = await asyncio.to_thread(
                        lambda c=cursor: self.r.scan(cursor=c, match="cont_ctx_calib:pending:*", count=10000)
                    )
                    for key in keys:
                        try:
                            raw = await asyncio.to_thread(self.r.get, key)
                            if not raw:
                                continue
                            pending = json.loads(raw)

                            if pending.get("status") != "PENDING":
                                continue

                            pending_found += 1
                            run_id = pending.get("run_id", "")
                            last_reminder = int(pending.get("last_reminder_ms") or pending.get("created_at_ms", 0))
                            reminder_count = int(pending.get("reminder_count", 0))
                            symbols = pending.get("symbols", [])
                            recommendations = pending.get("recommendations", {})

                            # Max reminders → expire
                            if reminder_count >= reminder_max:
                                print(
                                    f"⚠️ Cont Ctx Calib reminder: run_id={run_id} hit max "
                                    f"({reminder_count}/{reminder_max}), marking EXPIRED"
                                )
                                pending["status"] = "EXPIRED"
                                pending["expired_at_ms"] = now_ms
                                await asyncio.to_thread(
                                    self.r.set, key,
                                    json.dumps(pending, ensure_ascii=False),
                                )
                                continue

                            # Not enough time passed?
                            elapsed_ms = now_ms - last_reminder
                            if elapsed_ms < reminder_sec * 1000:
                                continue

                            # Build reminder
                            reminder_count += 1
                            created_min_ago = int((now_ms - int(pending.get("created_at_ms", now_ms))) / 60000)

                            # Per-symbol summary from recommendations with verdicts
                            sym_summary = ""
                            overall_has_cons = False
                            overall_has_warnings = False
                            for sym in symbols[:5]:
                                rec = recommendations.get(sym, {})
                                base = rec.get("baseline_ms", 0)
                                recom = rec.get("recommended_ms", 0)
                                exp_r = float(rec.get("expectancy_r", 0))
                                fb = float(rec.get("false_breakout_rate", 0))
                                n = int(rec.get("sample_n", 0))
                                delta_s = (int(recom) - int(base)) // 1000

                                # Compact verdict per symbol
                                verdict_icon = "🟢"
                                verdict_hint = "ОК"
                                if fb > 0.18 or n < 50 or delta_s > 60:
                                    verdict_icon = "🔴"
                                    verdict_hint = "есть риски"
                                    overall_has_cons = True
                                elif fb > 0.15 or exp_r < 0.05 or delta_s > 30:
                                    verdict_icon = "🟡"
                                    verdict_hint = "нюансы"
                                    overall_has_warnings = True

                                sym_summary += (
                                    f"  • <code>{sym}</code>: "
                                    f"<code>{base}ms</code> → <code>{recom}ms</code> "
                                    f"E[R]=<code>{exp_r:+.4f}</code> "
                                    f"FB=<code>{fb:.1%}</code> n=<code>{n}</code>\n"
                                    f"    {verdict_icon} {verdict_hint}\n"
                                )

                            # Overall advice
                            if overall_has_cons:
                                advice = "🔴 Рекомендация: есть символы с рисками — рассмотрите Reject"
                            elif overall_has_warnings:
                                advice = "🟡 Рекомендация: допустимо, но проверьте нюансы"
                            else:
                                advice = "🟢 Рекомендация: метрики стабильны — рассмотрите Apply"

                            reminder_text = (
                                f"🔔 <b>НАПОМИНАНИЕ: Cont Ctx Window ожидает решение!</b>\n"
                                f"\n"
                                f"⏱️ Ожидание: <code>{created_min_ago} мин</code> "
                                f"(напоминание #{reminder_count})\n"
                                f"\n"
                                f"🔧 <b>Символы ({len(symbols)}):</b>\n"
                                f"{sym_summary}"
                                f"\n"
                                f"💡 {advice}\n"
                                f"\n"
                                f"<b>Apply</b> = расширить окно (больше сигналов, выше stale risk)\n"
                                f"<b>Reject</b> = оставить текущее (консервативно)\n"
                                f"\n"
                                f"<b>Нажмите кнопку:</b>\n"
                                f"Run ID: <code>{run_id}</code>"
                            )

                            buttons = [[
                                {"text": f"✅ Apply ({len(symbols)} sym)", "callback_data": f"cont_ctx_approve:{run_id}"},
                                {"text": "❌ Reject", "callback_data": f"cont_ctx_reject:{run_id}"},
                            ]]
                            buttons_json = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))

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

                            # Update pending
                            pending["last_reminder_ms"] = now_ms
                            pending["reminder_count"] = reminder_count
                            await asyncio.to_thread(
                                lambda k=key, v=json.dumps(pending, ensure_ascii=False): self.r.set(k, v)
                            )

                            reminders_sent += 1
                            print(
                                f"🔔 Cont Ctx Calib reminder #{reminder_count} sent for "
                                f"run_id={run_id} (waiting {created_min_ago}m, {len(symbols)} sym)"
                            )

                        except Exception as e:
                            print(f"⚠️ Cont Ctx Calib reminder key error ({key}): {e}")
                            continue

                    if cursor == 0:
                        break

                if reminders_sent > 0:
                    print(
                        f"🔔 Cont Ctx Calib reminder cycle: {reminders_sent} sent, "
                        f"{pending_found} pending total"
                    )

            except Exception as e:
                print(f"❌ Cont Ctx Calib reminder loop error: {e}")
                await asyncio.sleep(30)

    # -----------------------------------------------------------------
    # OF Gate Report Autoreject loop
    # -----------------------------------------------------------------

    async def _of_gate_recs_autoreject_loop(self) -> None:
        """Resend or auto-reject OF Gate Report recs if no action in 15 mins for mode=monitor.

        Scans Redis for recs:status:* keys with status=PENDING.
        Checks corresponding recs:bundle:* for meta.mode == monitor and meta.kind == of_gate_recs.
        If older than OF_RECS_AUTOREJECT_SEC (default 900s), sets status to REJECTED.

        ENV:
          OF_RECS_AUTOREJECT_SEC  — timeout for auto-reject (default: 900)
        """
        autoreject_sec = int(os.getenv("OF_RECS_AUTOREJECT_SEC", "900"))
        check_interval = min(60, autoreject_sec // 2)
        notify_stream = os.getenv("NOTIFY_STREAM", "notify:telegram")

        print(
            f"🔔 OF Gate Recs autoreject loop started: timeout={autoreject_sec}s, "
            f"check_every={check_interval}s"
        )

        while self.running:
            try:
                await asyncio.sleep(check_interval)

                cursor = 0
                now_ms = get_ny_time_millis()
                rejected_sent = 0

                while True:
                    cursor, keys = await asyncio.to_thread(
                        lambda c=cursor: self.r.scan(cursor=c, match="recs:status:*", count=10000)
                    )
                    for key in keys:
                        try:
                            status = await asyncio.to_thread(self.r.get, key)
                            if status != "PENDING":
                                continue

                            bundle_id = key.split("recs:status:")[1]
                            bundle_key = f"recs:bundle:{bundle_id}"
                            raw_bundle = await asyncio.to_thread(self.r.get, bundle_key)
                            if not raw_bundle:
                                continue

                            bundle = json.loads(raw_bundle)
                            meta = bundle.get("meta", {})
                            if meta.get("kind") != "of_gate_recs" or meta.get("mode") != "monitor":
                                continue

                            created_ms = int(bundle.get("created_ms", 0))
                            elapsed_ms = now_ms - created_ms
                            if elapsed_ms < autoreject_sec * 1000:
                                continue

                            # Auto Reject!
                            await asyncio.to_thread(self.r.set, key, "REJECTED", keepttl=True)
                            
                            ts = meta.get("ts", "unknown")

                            # Send to telegram
                            reject_text = (
                                f"⏳ <b>OF Gate Report AUTO-REJECTED</b>\n"
                                f"mode=<code>monitor</code> ts=<code>{ts}</code>\n"
                                f"Reason: No action within {autoreject_sec // 60} mins.\n\n"
                                f"Run ID: <code>{bundle_id}</code>"
                            )
                            fields = {
                                "type": "report",
                                "text": reject_text,
                                "ts": str(now_ms),
                            }
                            await asyncio.to_thread(
                                lambda: self.r.xadd(
                                    notify_stream, fields,
                                    maxlen=200000, approximate=True,
                                )
                            )

                            rejected_sent += 1
                            print(f"⏳ OF Gate Report auto-rejected: {bundle_id} (timeout {autoreject_sec}s)")

                        except Exception as e:
                            print(f"⚠️ OF Gate Recs autoreject key error ({key}): {e}")
                            continue

                    if cursor == 0:
                        break

            except Exception as e:
                print(f"❌ OF Gate Recs autoreject loop error: {e}")
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
            #  Format
            text = entry.get("text", "")
            side = entry.get("side", "")
            price = entry.get("price", "")
            
            symbol_match = re.search(r'(XAU\w*|BTC\w*|ETH\w*|[A-Z]{3,})', text)
            symbol = symbol_match.group(1) if symbol_match else ""
            
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
                "source": " OrderFlow",
                "orderType": entry.get("note", "Market"),
                "profitPct": None,
                "raw_text": text,
                "is_xauusd": True
            }
            # Special raw construction for  to bypass formatting
            raw = {
                "chat_title": " OrderFlow Analysis",
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
