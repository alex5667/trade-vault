#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
from utils.time_utils import get_ny_time_millis
"""
TM Autopilot Service:
...
"""

import hashlib
import json
import os
import time
from typing import Any, Optional

import redis


# Tools are imported locally in run_once to avoid circular dependencies or early load errors


def _now_ms() -> int:
    return get_ny_time_millis()


def _b(x: Any, default: bool = False) -> bool:
    if isinstance(x, bool):
        return x
    if x is None:
        return default
    return str(x).strip().lower() in ("1", "true", "yes", "on")


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _html_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _format_report(tuner_out: dict[str, Any]) -> str:
    """
    Compact Telegram-friendly HTML report.
    """
    rows = int(tuner_out.get("rows_seen", 0) or 0)
    win = float(tuner_out.get("window_days", 0.0) or 0.0)
    min_n = int(tuner_out.get("min_n", 0) or 0)
    rg = tuner_out.get("rec_global", {}) or {}

    summary = tuner_out.get("summary", {})
    total = summary.get("total_rows", rows)
    skip_sym = summary.get("skipped_no_symbol", 0)
    skip_scn = summary.get("skipped_no_scenario", 0)

    lines = []
    lines.append(f"<b>TM Autopilot</b> | window={win:.1f}d | total_raw={total}")
    if skip_sym or skip_scn:
         lines.append(f"⚠️ <i>Skipped: no_sym={skip_sym}, no_scn={skip_scn}</i>")
    lines.append(f"<b>Active winners</b> (n>={min_n}): {len(rg)}")
    lines.append("")
    lines.append("<b>Global Recommendations (regime|scenario)</b>")

    # show top few
    keys = sorted(rg.keys())
    for k in keys[:30]:
        v = rg.get(k) or {}
        tier = v.get("tier")
        n = v.get("n")
        mean_r = float(v.get("mean_r", 0.0) or 0.0)
        lcb_r = float(v.get("lcb_r", -999.0) or -999.0)
        wr = float(v.get("winrate", 0.0) or 0.0)
        conf = float(v.get("conf", 0.0) or 0.0)
        lines.append(f"• <code>{_html_escape(k)}</code> → tier=<b>{tier}</b> n={n} meanR={mean_r:.2f} LCB={lcb_r:.2f} WR={wr:.2%} (conf={conf:.2f})")

    return "<br/>".join(lines)


def _write_proposal(r: redis.Redis, proposal: dict[str, Any]) -> str:
    """
    Writes:
      cfg:suggestions:entry_policy:meta:{sid}
      cfg:suggestions:entry_policy:latest:autopilot:{group} -> sid
      cfg:suggestions:entry_policy:approvals:{sid} (empty placeholder; approvals handled by your existing workflow)
    """
    group = str(proposal.get("group", "default") or "default").lower()
    sid = _sha1(json.dumps({"kind": "tm_autopilot", "group": group, "ts": int(proposal.get("updated_ts_ms", _now_ms()))}, separators=(",", ":")))
    meta_key = f"cfg:suggestions:entry_policy:meta:{sid}"
    latest_key = f"cfg:suggestions:entry_policy:latest:autopilot:{group}"
    appr_key = f"cfg:suggestions:entry_policy:approvals:{sid}"

    pipe = r.pipeline()
    pipe.set(meta_key, json.dumps(proposal, ensure_ascii=False, separators=(",", ":")))
    pipe.set(latest_key, sid)
    # Create approvals placeholder set (optional)
    pipe.delete(appr_key)
    # Keep TTL for hygiene (align with your usual suggestions TTL if you have one)
    ttl = int(os.getenv("TM_AUTOPILOT_SUGGESTION_TTL_SEC", str(14 * 86400)))
    pipe.expire(meta_key, ttl)
    pipe.expire(latest_key, ttl)
    pipe.expire(appr_key, ttl)
    pipe.execute()
    return sid


def _send_telegram_report(r: redis.Redis, html: str, buttons: Optional[list[list[dict[str, str]]]] = None) -> None:
    stream = os.getenv("TELEGRAM_NOTIFY_STREAM", "notify:telegram")
    fields = {"type": "report", "text": html}
    if buttons:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    r.xadd(stream, fields, maxlen=20000, approximate=True)


def run_once(r: redis.Redis) -> dict[str, Any]:
    now = _now_ms()
    window_days = float(os.getenv("TM_AUTOPILOT_WINDOW_DAYS", "7"))
    since_hours = float(os.getenv("TM_AUTOPILOT_SINCE_HOURS", str(window_days * 24.0)))
    min_n = int(os.getenv("TM_AUTOPILOT_MIN_N", "50"))
    min_edge_r = float(os.getenv("LCB_MIN_EDGE_R", "0.05"))

    trade_stream = os.getenv("TRADE_EVENTS_STREAM", "events:trades")
    tmp_path = os.getenv("TM_AUTOPILOT_TMP_NDJSON", "/tmp/closed_trades.ndjson")

    # 1) Export
    from tools.export_trade_closed_ndjson import export_stream
    exp_n, scanned = export_stream(
        r=r,
        stream=trade_stream,
        since_ms=now - int(since_hours * 3600 * 1000),
        out_path=tmp_path,
    )

    # 2) Tune
    from tools.tm_policy_tuner import load_rows, group_rows_by_context, pick_winners, build_overrides_v1_proposal, write_proposals_overrides_v1, render_report_md
    rows = load_rows(tmp_path)
    grouped = group_rows_by_context(rows, window_days=window_days)
    
    min_samples_by_regime = {
        "thin": int(os.getenv("LCB_MIN_SAMPLES_THIN", str(min_n))),
        "news": int(os.getenv("LCB_MIN_SAMPLES_THIN", str(min_n))),
        "illiquid": int(os.getenv("LCB_MIN_SAMPLES_THIN", str(min_n))),
    }
    winners = pick_winners(
        grouped,
        min_samples_default=min_n,
        min_edge_r=min_edge_r,
        min_samples_by_regime=min_samples_by_regime,
    )
    
    # 3) Optional proposal (with 24h guard)
    proposals_list = []
    can_propose = _b(os.getenv("TM_AUTOPILOT_ENABLE_PROPOSAL", "1"), True)
    if can_propose:
        last_prop_ts = 0
        try:
            val = r.get("state:tm_autopilot:last_proposal_ts_ms")
            if val: last_prop_ts = int(val)
        except Exception: pass
        
        prop_every_h = float(os.getenv("TM_AUTOPILOT_PROPOSAL_EVERY_HOURS", "24"))
        if (now - last_prop_ts) < (prop_every_h * 3600 * 1000):
            can_propose = False
            print(f"Proposal skipped: last was {last_prop_ts}, need {prop_every_h}h gap")

    if can_propose and winners:
        # Build proposals structure
        tuner_out = {"winners": winners}
        proposals_result = build_overrides_v1_proposal(tuner_out)
        proposals_list = proposals_result.get("proposals", [])
        
        # Write to Redis
        n_written = write_proposals_overrides_v1(r=r, winners=winners)
        
        if n_written > 0:
            try:
                r.set("state:tm_autopilot:last_proposal_ts_ms", str(now))
                # Save to DB - combine winner data with proposal data
                from services.analytics_db import save_autopilot_proposal
                
                # Create a map of winner data by (symbol, regime, scenario, group)
                winners_map = {}
                for w in winners:
                    key = (w["symbol"], w["regime"], w["scenario"], w.get("group", "default"))
                    winners_map[key] = w
                
                for p in proposals_list:
                    # Extract symbol, regime, scenario, group from latest_key
                    # Format: cfg:suggestions:entry_policy:latest:overrides_v1:{sym}:{rg}:{grp}:{scn}
                    latest_key = p.get("latest_key", "")
                    parts = latest_key.split(":")
                    if len(parts) >= 9:
                        sym = parts[5].upper()
                        rg = parts[6].lower()
                        grp = parts[7].lower()
                        scn = parts[8].lower()
                        
                        winner = winners_map.get((sym, rg, scn, grp))
                        if winner:
                            save_autopilot_proposal(
                                sid=p["sid"],
                                group=grp,
                                symbol=sym,
                                regime=rg,
                                scenario=scn,
                                winner_arm=winner["winner_arm"],
                                edge_lcb_r=float(winner.get("edge_lcb_r") or 0.0),
                                proposal_json=p["overrides_v1_json"]
                            )
            except Exception as e:
                print(f"Failed to persist proposals: {e}")

    # 4) Telegram report + Buttons
    md = render_report_md(winners, window_days=window_days)
    # Convert MD to basic HTML wrapper if notify_worker expects HTML
    # existing format used bold/italic tags
    html_report = f"<b>TM Autopilot Report</b>\n<pre>{_html_escape(md)}</pre>"
    
    buttons = []
    for p in proposals_list:
        label = f"Apply {p['symbol']}:{p['regime']} ({p['winner_arm']})"
        buttons.append([{"text": f"✅ {label}", "callback_data": f"approve:{p['sid']}"}])

    _send_telegram_report(r, html_report, buttons)

    return {"ts_ms": now, "rows_seen": len(rows), "winners_count": len(winners), "proposals_count": len(proposals_list)}


def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.from_url(redis_url, decode_responses=True)

    interval = int(os.getenv("TM_AUTOPILOT_INTERVAL_SEC", "3600"))
    if interval < 60:
        interval = 60

    lock_key = os.getenv("TM_AUTOPILOT_LOCK_KEY", "lock:tm_autopilot:v1")
    lock_ttl = int(os.getenv("TM_AUTOPILOT_LOCK_TTL_SEC", str(max(300, interval - 10))))

    while True:
        # safe-lock: prevent duplicates (e.g. multiple replicas)
        got = False
        try:
            got = bool(r.set(lock_key, str(_now_ms()), nx=True, ex=lock_ttl))
        except Exception:
            got = False

        if got:
            try:
                out = run_once(r)
                # small structured log
                print(json.dumps({"ok": True, "ts_ms": out.get("ts_ms"), "rows": out.get("rows_seen"), "proposal": out.get("proposal_sid", "")}, ensure_ascii=False))
            except Exception as e:
                print(json.dumps({"ok": False, "err": str(e)}, ensure_ascii=False))

        time.sleep(interval)


if __name__ == "__main__":
    main()
