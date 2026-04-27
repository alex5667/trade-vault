from __future__ import annotations

import asyncio
import json
import os
import time
import html
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import redis.asyncio as aioredis
from core.redis_keys import RedisStreams as RS

from services.entry_policy_core import EntryPolicyCfg, evaluate_entry_policy


def _now_ms() -> int:
    return int(time.time() * 1000)


def _day_utc() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime())


def _sha1(s: str) -> str:
    import hashlib
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


@dataclass
class AlertCfg:
    # capture
    duration_sec: int = 900             # 15 min default
    stream: str = "stream:trade:entry_candidate"
    snap_prefix: str = "smt:snap:"
    bundle_prefix: str = "smt:bundle:v1:"
    start_id: str = "$"
    block_ms: int = 1000
    count: int = 200

    # report
    out_dir: str = "/var/log/trade"
    keep_days: int = 14

    # alert thresholds
    min_total: int = 5
    min_allow_rate: float = 0.3        # %
    max_allow_rate: float = 50.0       # %
    max_delta_allow_pp: float = 7.0    # percentage points vs yesterday

    # publish alerts
    alerts_stream: str = "stream:trade:alerts"

    # scheduling
    run_at_hour: int = 3
    run_at_minute: int = 10
    enable_direct_notify: bool = True

    @staticmethod
    def from_env() -> "AlertCfg":
        return AlertCfg(
            duration_sec=int(os.getenv("EP_CAPTURE_DURATION_SEC", "900")),
            stream=os.getenv("SMT_ENTRY_STREAM", "stream:trade:entry_candidate"),
            snap_prefix=os.getenv("SMT_SNAP_PREFIX", "smt:snap:"),
            bundle_prefix=os.getenv("SMT_BUNDLE_PREFIX", "smt:bundle:v1:"),
            start_id=os.getenv("EP_CAPTURE_START_ID", "$"),
            block_ms=int(os.getenv("EP_CAPTURE_BLOCK_MS", "1000")),
            count=int(os.getenv("EP_CAPTURE_COUNT", "200")),
            out_dir=os.getenv("EP_REPORT_DIR", "/var/log/trade"),
            keep_days=int(os.getenv("EP_REPORT_KEEP_DAYS", "14")),
            min_total=int(os.getenv("EP_ALERT_MIN_TOTAL", "5")),
            min_allow_rate=float(os.getenv("EP_ALERT_MIN_ALLOW_RATE", "0.3")),
            max_allow_rate=float(os.getenv("EP_ALERT_MAX_ALLOW_RATE", "50.0")),
            max_delta_allow_pp=float(os.getenv("EP_ALERT_MAX_DELTA_ALLOW_PP", "7.0")),
            alerts_stream=os.getenv("EP_ALERTS_STREAM", "stream:trade:alerts"),
            run_at_hour=int(os.getenv("EP_RUN_AT_HOUR", "3")),
            run_at_minute=int(os.getenv("EP_RUN_AT_MINUTE", "10")),
            enable_direct_notify=bool(int(os.getenv("EP_ENABLE_DIRECT_NOTIFY", "1"))),
        )


def _core_cfg_from_env() -> EntryPolicyCfg:
    return EntryPolicyCfg(
        coh_thr=float(os.getenv("SMT_COH_THRESHOLD", "0.65")),
        leader_conf_min=float(os.getenv("SMT_LEADER_CONF_MIN_SCORE", "0.65")),
        min_of_score=float(os.getenv("SMT_ENTRY_MIN_OF_SCORE", "1.0")),
        max_zone_bp=float(os.getenv("SMT_ENTRY_MAX_ZONE_BP", "15")),
        max_zone_bp_thin=float(os.getenv("SMT_ENTRY_MAX_ZONE_BP_THIN", "10")),
        obi_min_sec=float(os.getenv("SMT_ENTRY_OBI_MIN_SEC", "1.5")),
        dedup_ms=int(os.getenv("SMT_ENTRY_DEDUP_MS", "60000")),
        allow_zone_id_change_if_near=bool(int(os.getenv("ENTRY_POLICY_ALLOW_ZONE_CHANGE_IF_NEAR", "0"))),
    )


def compute_summary(recs: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(recs)
    allow = sum(1 for r in recs if int(r.get("ok", 0)) == 1)
    deny = total - allow
    allow_rate = (allow / max(total, 1)) * 100.0

    by_reason = Counter()
    by_regime = Counter()
    by_symbol = Counter()
    by_zone_src = Counter()

    for r in recs:
        by_reason[str(r.get("reason_code", "na"))] += 1
        by_regime[str(r.get("regime", "na"))] += 1
        by_symbol[str(r.get("symbol", ""))] += 1
        by_zone_src[str(r.get("zone_src", "na"))] += 1

    top_reason = by_reason.most_common(10)
    top_regime = by_regime.most_common(10)
    top_symbol = by_symbol.most_common(10)
    top_zone_src = by_zone_src.most_common(10)

    return {
        "total": total,
        "allow": allow,
        "deny": deny,
        "allow_rate": allow_rate,
        "top_reason": top_reason,
        "top_regime": top_regime,
        "top_symbol": top_symbol,
        "top_zone_src": top_zone_src,
    }


def should_alert(today: Dict[str, Any], prev: Dict[str, Any], cfg: AlertCfg) -> Tuple[bool, str]:
    t = int(today.get("total", 0) or 0)
    if t < int(cfg.min_total):
        return True, f"NO_DATA total={t} (min={cfg.min_total})"

    ar = float(today.get("allow_rate", 0.0) or 0.0)
    if ar < float(cfg.min_allow_rate):
        return True, f"ALLOW_RATE_LOW {ar:.2f}% (min {cfg.min_allow_rate:.2f}%)"
    if ar > float(cfg.max_allow_rate):
        return True, f"ALLOW_RATE_HIGH {ar:.2f}% (max {cfg.max_allow_rate:.2f}%)"

    if prev and int(prev.get("total", 0) or 0) >= int(cfg.min_total):
        prev_ar = float(prev.get("allow_rate", 0.0) or 0.0)
        dpp = abs(ar - prev_ar)
        if dpp > float(cfg.max_delta_allow_pp):
            return True, f"ALLOW_RATE_DRIFT {ar:.2f}% vs {prev_ar:.2f}% (diff={dpp:.2f}pp) > {cfg.max_delta_allow_pp:.2f}pp"

    return False, "OK"


def render_markdown(day: str, summary: Dict[str, Any], prev: Dict[str, Any], alert: Tuple[bool, str]) -> str:
    total = int(summary.get("total", 0) or 0)
    allow = int(summary.get("allow", 0) or 0)
    deny = int(summary.get("deny", 0) or 0)
    ar = float(summary.get("allow_rate", 0.0) or 0.0)
    prev_ar = float(prev.get("allow_rate", 0.0) or 0.0) if prev else None

    lines = []
    lines.append(f"# Entry Policy Daily Report — {day}")
    lines.append("")
    lines.append(f"- total: **{total}**")
    lines.append(f"- allow: **{allow}**")
    lines.append(f"- deny: **{deny}**")
    if prev_ar is not None:
        lines.append(f"- allow_rate: **{ar:.2f}%** (yesterday {prev_ar:.2f}%)")
    else:
        lines.append(f"- allow_rate: **{ar:.2f}%**")
    lines.append(f"- alert: **{int(alert[0])}** — {alert[1]}")
    lines.append("")

    def _tbl(title: str, items: List[Tuple[str, int]]) -> None:
        lines.append(f"## {title}")
        lines.append("| key | count |")
        lines.append("|---|---:|")
        for k, c in items:
            lines.append(f"| {k} | {c} |")
        lines.append("")

    _tbl("Top reason_code", list(summary.get("top_reason", [])))
    _tbl("Top regimes", list(summary.get("top_regime", [])))
    _tbl("Top symbols", list(summary.get("top_symbol", [])))
    _tbl("Top zone_src", list(summary.get("top_zone_src", [])))

    return "\n".join(lines)


async def capture_and_replay(*, r: aioredis.Redis, cfg: AlertCfg, core_cfg: EntryPolicyCfg) -> List[Dict[str, Any]]:
    """
    Capture for duration_sec and run deterministic policy evaluation on captured snapshots.
    """
    t_end = _now_ms() + int(cfg.duration_sec) * 1000
    cur = str(cfg.start_id)
    dedup_state: Dict[str, int] = {}
    out: List[Dict[str, Any]] = []

    print(f"[{_now_ms()}] Starting capture for {cfg.duration_sec}s...")

    while _now_ms() < t_end:
        try:
            msgs = await r.xread({cfg.stream: cur}, count=int(cfg.count), block=int(cfg.block_ms))
        except Exception:
            await asyncio.sleep(0.2)
            continue
        if not msgs:
            continue
        for _stream, entries in msgs:
            for msg_id, fields in entries:
                cur = msg_id
                try:
                    if str(fields.get("type", "")) != "entry_candidate":
                        continue
                    sym = str(fields.get("symbol", "") or "").upper()
                    bundle_id = str(fields.get("bundle", "") or "")
                    if not sym or not bundle_id:
                        continue

                    snap_raw = await r.get(f"{cfg.snap_prefix}{sym}")
                    snap = json.loads(snap_raw) if snap_raw else {}
                    bundle = await r.hgetall(f"{cfg.bundle_prefix}{bundle_id}")

                    now_ms = int(fields.get("ts_ms") or 0)
                    if now_ms <= 0:
                        continue

                    dec = evaluate_entry_policy(
                        now_ms=now_ms,
                        cand=fields,
                        snap=snap,
                        bundle=bundle,
                        cfg=core_cfg,
                        dedup_state=dedup_state,
                    )

                    out.append(
                        {
                            "msg_id": msg_id,
                            "ts_ms": now_ms,
                            "symbol": sym,
                            "bundle": bundle_id,
                            "ok": 1 if dec.ok else 0,
                            "reason_code": dec.reason_code,
                            "notes": dec.notes,
                            "regime": str(snap.get("regime", "na") or "na"),
                            "zone_id": str(snap.get("zone_id", "") or ""),
                            "zone_src": str(snap.get("zone_src", "na") or "na"),
                            "zone_side": str(snap.get("zone_side", "NA") or "NA"),
                            "zone_dist_bp": float(snap.get("zone_dist_bp", 0.0) or 0.0),
                            "obi_stable_sec": float(snap.get("obi_stable_sec", 0.0) or 0.0),
                            "iceberg_strict": int(snap.get("iceberg_strict", 0) or 0),
                            "of_confirm_score": float(snap.get("of_confirm_score", 0.0) or 0.0),
                            "coh": float(bundle.get("coh", 0.0) or 0.0),
                            "leader_conf_score": float(bundle.get("leader_conf_score", 0.0) or 0.0),
                            "decision": str(bundle.get("decision", "") or ""),
                            "pick": str(bundle.get("pick", "") or ""),
                        }
                    )
                except Exception:
                    continue

    print(f"[{_now_ms()}] Capture complete. {len(out)} records processed.")
    return out


def _load_prev_summary(path: Path) -> Dict[str, Any]:
    try:
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _prune_reports(dirp: Path, keep_days: int) -> None:
    try:
        files = sorted([p for p in dirp.glob("entry_policy_summary_*.json") if p.is_file()])
        if len(files) <= keep_days:
            return
        for p in files[: max(0, len(files) - keep_days)]:
            try:
                p.unlink()
            except Exception:
                pass
    except Exception:
        return


def render_telegram_html(day: str, summary: Dict[str, Any], alert: Tuple[bool, str]) -> str:
    total = int(summary.get("total", 0) or 0)
    allow = int(summary.get("allow", 0) or 0)
    deny = int(summary.get("deny", 0) or 0)
    ar = float(summary.get("allow_rate", 0.0) or 0.0)
    
    emoji = "⚠️" if alert[0] else "✅"
    lines = [
        f"{emoji} <b>Entry Policy Regression Check</b>",
        f"📅 {html.escape(str(day))}",
        "",
        f"<b>Status:</b> {html.escape(str(alert[1]))}",
        "",
        f"<b>Stats:</b>",
        f"• Total: {total}",
        f"• Allow: {allow} ({ar:.2f}%)",
        f"• Deny: {deny}",
        "",
        "<b>Top Reasons:</b>"
    ]
    
    for r, c in summary.get("top_reason", [])[:3]:
        lines.append(f"• <code>{html.escape(str(r))}</code>: {c}")
        
    return "\n".join(lines)


async def publish_alert(*, r: aioredis.Redis, cfg: AlertCfg, day: str, summary: Dict[str, Any], reason: str) -> None:
    # 1. Publish to internal alert stream (structured)
    payload = {
        "day": day,
        "reason": reason,
        "summary": summary,
    }
    msg = {
        "type": "entry_policy_regression",
        "ts_ms": str(_now_ms()),
        "day": str(day),
        "reason": str(reason),
        "payload": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        "hash": _sha1(json.dumps({"day": day, "reason": reason}, separators=(",", ":"))),
    }
    try:
        await r.xadd(cfg.alerts_stream, msg, maxlen=20000, approximate=True)
    except Exception:
        pass

    # 2. Publish to Telegram notify stream (human-readable)
    # Only if there IS an alert (regression detected) AND direct notify is enabled
    if cfg.enable_direct_notify and should_alert(summary, {}, cfg)[0]:
        tg_text = render_telegram_html(day, summary, (True, reason))
        notify_msg = {
            "type": "report",
            "text": tg_text,
            "parse_mode": "HTML",
            "source": "EntryPolicyRegressionService",
            "severity": "warn",
            "timestamp": str(_now_ms())
        }
        try:
            # notify:telegram is standard, but check env or cfg or hardcode?
            # ReportingService uses "notify:telegram" from env NOTIFY_STREAM
            stream = os.getenv("NOTIFY_STREAM", RS.NOTIFY_TELEGRAM)
            await r.xadd(stream, notify_msg, maxlen=2000)
        except Exception as e:
            print(f"Error publishing to telegram: {e}")



class EntryPolicyRegressionService:
    def __init__(self):
        self.r: Optional[aioredis.Redis] = None
        self.acfg = AlertCfg.from_env()
        self.core_cfg = _core_cfg_from_env()

    async def run_once(self) -> None:
        if not self.r:
            redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
            self.r = aioredis.from_url(redis_url, decode_responses=True)

        day = _day_utc()
        out_dir = Path(self.acfg.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        prev_path = out_dir / "entry_policy_summary_prev.json"
        prev = _load_prev_summary(prev_path)

        recs = await capture_and_replay(r=self.r, cfg=self.acfg, core_cfg=self.core_cfg)
        summary = compute_summary(recs)

        alert = should_alert(summary, prev, self.acfg)

        # write artifacts
        summary_path = out_dir / "entry_policy_summary.json"
        hist_path = out_dir / f"entry_policy_summary_{day}.json"
        md_path = out_dir / f"entry_policy_report_{day}.md"
        nd_path = out_dir / f"entry_policy_replay_{day}.ndjson"

        try:
            # compact decisions ndjson
            with open(nd_path, "w", encoding="utf-8") as f:
                for r0 in recs:
                    f.write(json.dumps(r0, ensure_ascii=False, separators=(",", ":")) + "\n")
        except Exception:
            pass

        try:
            summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
            hist_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
            prev_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
            md_path.write_text(render_markdown(day, summary, prev, alert), encoding="utf-8")
        except Exception:
            pass

        _prune_reports(out_dir, int(self.acfg.keep_days))

        # --- Tuner suggestions (non-invasive) ---
        try:
            from tools.entry_policy_tuner_suggest import suggest_from_records, TunerCfg
            tcfg = TunerCfg.from_env()
            
            # feed current effective config to tuner, so "from/to" are correct even with overrides
            cur_env = {
                "SMT_COH_THRESHOLD": float(self.core_cfg.coh_thr),
                "SMT_LEADER_CONF_MIN_SCORE": float(self.core_cfg.leader_conf_min),
                "SMT_ENTRY_MAX_ZONE_BP": float(self.core_cfg.max_zone_bp),
                "SMT_ENTRY_MAX_ZONE_BP_THIN": float(self.core_cfg.max_zone_bp_thin),
                "SMT_ENTRY_OBI_MIN_SEC": float(self.core_cfg.obi_min_sec),
            }
            sugg = suggest_from_records(records=recs, tuner=tcfg, current_env=cur_env)
            
            sugg_path = out_dir / f"entry_policy_suggestions_{day}.json"
            sugg_latest = out_dir / "entry_policy_suggestions.json"
            
            sugg_path.write_text(json.dumps(sugg, ensure_ascii=False, indent=2), encoding="utf-8")
            sugg_latest.write_text(json.dumps(sugg, ensure_ascii=False, indent=2), encoding="utf-8")
            
            # Publish suggestions as info alert (optional)
            if int(sugg.get("safe_to_apply", 0) or 0) == 1 and bool(int(os.getenv("EP_TUNER_PUBLISH", "1"))):
                payload = {"day": day, "suggestions": sugg}
                msg = {
                    "type": "entry_policy_suggestions",
                    "ts_ms": str(_now_ms()),
                    "day": str(day),
                    "payload": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                    "hash": _sha1(json.dumps({"day": day, "kind": "suggestions"}, separators=(",", ":"))),
                }
                try:
                    await self.r.xadd(self.acfg.alerts_stream, msg, maxlen=20000, approximate=True)
                except Exception:
                    pass
                    
            # Store latest suggestions into Redis for approval workflow
            if bool(int(os.getenv("EP_SUGGESTIONS_STORE_REDIS", "1"))):
                key = os.getenv("EP_SUGGESTIONS_REDIS_KEY", "cfg:suggestions:entry_policy:latest")
                ttl_sec = int(os.getenv("EP_SUGGESTIONS_TTL_SEC", "604800"))  # 7 days
                await self.r.set(key, json.dumps(sugg, ensure_ascii=False, separators=(",", ":")), ex=ttl_sec)
        except Exception as e:
            print(f"Error in Tuner logic: {e}")

        if alert[0]:
            print(f"ALERT: {alert[1]}")
            await publish_alert(r=self.r, cfg=self.acfg, day=day, summary=summary, reason=alert[1])
        else:
            print(f"Regression Check OK: {summary['allow_rate']:.2f}% allow rate")

    def _seconds_until_run(self) -> float:
        now = datetime.now(timezone.utc)
        target = now.replace(hour=self.acfg.run_at_hour, minute=self.acfg.run_at_minute, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        return (target - now).total_seconds()

    async def run_forever(self) -> None:
        print(f"EntryPolicyRegressionService started. Schedule: {self.acfg.run_at_hour:02d}:{self.acfg.run_at_minute:02d} UTC")
        while True:
            wait_sec = self._seconds_until_run()
            print(f"Sleeping for {wait_sec:.1f}s until next run...")
            await asyncio.sleep(wait_sec)
            try:
                await self.run_once()
            except Exception as e:
                print(f"Error in regression check: {e}")
                await asyncio.sleep(60)  # retry delay or just continue


async def _amain() -> None:
    svc = EntryPolicyRegressionService()
    await svc.run_forever()


if __name__ == "__main__":
    asyncio.run(_amain())
