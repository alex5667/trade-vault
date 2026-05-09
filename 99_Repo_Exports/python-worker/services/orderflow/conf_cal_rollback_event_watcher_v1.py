#!/usr/bin/env python3
from __future__ import annotations

"""
conf_cal_rollback_event_watcher_v1.py

ROI step: make rollbacks non-silent by emitting:
- Redis event (Stream + PubSub) when a rollback happens
- Optional Grafana annotation via HTTP API

This watcher is intentionally decoupled from the live health loop:
it only needs to read the live_status.json written by the health loop/exporter.

Env / Args
- CONF_CAL_LIVE_STATUS_PATH (default: /tmp/conf_cal_live_status.json)
- CONF_CAL_LIVE_STATUS_URL  (optional public URL for UI/Grafana link)
- REDIS_URL, EVENTS_STREAM_KEY, EVENTS_PUBSUB_CH
- GRAFANA_URL, GRAFANA_API_TOKEN (optional)
- GRAFANA_ANNOTATION_TAGS (comma sep, default: conf_cal,rollback)
- GRAFANA_DASHBOARD_UID / GRAFANA_PANEL_ID (optional, for scoping annotations)
"""

import os
import sys

# Prevent local utils.py from shadowing the global utils package
_script_dir = os.path.dirname(os.path.abspath(__file__))
if sys.path and os.path.abspath(sys.path[0]) == _script_dir:
    sys.path.pop(0)

_root_dir = os.path.abspath(os.path.join(_script_dir, "../.."))
if _root_dir not in sys.path:
    sys.path.insert(0, _root_dir)

import argparse
import json
import os
import time
from typing import Any

from utils.time_utils import get_ny_time_millis
import contextlib

try:
    import redis
except ImportError:  # pragma: no cover
    redis = None

try:
    from services.orderflow.conf_cal_ops_eventlog_v1 import publish_event, write_stream_event  # type: ignore
except Exception:  # pragma: no cover
    try:
        from orderflow_services.conf_cal_ops_eventlog_v1 import publish_event, write_stream_event  # type: ignore
    except Exception:  # pragma: no cover
        write_stream_event = None  # type: ignore
        publish_event = None  # type: ignore


def now_ms() -> int:
    return get_ny_time_millis()


def _read_json(path: str) -> dict[str, Any] | None:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _load_state(path: str) -> dict[str, Any]:
    st = _read_json(path)
    if isinstance(st, dict):
        return st
    return {}


def _save_state(path: str, st: dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(st, f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp, path)
    except Exception:
        pass


def _http_post_json(url: str, payload: dict[str, Any], token: str) -> bool:
    try:
        import urllib.request
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        with urllib.request.urlopen(req, timeout=10) as resp:
            return 200 <= int(resp.status) < 300
    except Exception:
        return False


def _grafana_annotate(status: dict[str, Any], *, url: str, token: str, tags: list[str], dashboard_uid: str, panel_id: int) -> bool:
    if not url or not token:
        return False

    ts = int(status.get("last_rollback_ts_ms") or status.get("ts_ms") or now_ms())
    reasons = status.get("rollback_reasons") or status.get("rollback_reason") or status.get("rollback") or {}
    reasons_s = json.dumps(reasons, ensure_ascii=False) if isinstance(reasons, (dict, list)) else str(reasons)

    st_url = os.getenv("CONF_CAL_LIVE_STATUS_URL", "").strip()
    text = f"conf_cal rollback\nreasons={reasons_s}"
    if st_url:
        text += f"\nstatus={st_url}"

    payload: dict[str, Any] = {"time": ts, "tags": tags, "text": text}
    if dashboard_uid:
        payload["dashboardUID"] = dashboard_uid
    if panel_id >= 0:
        payload["panelId"] = panel_id

    api = url.rstrip("/") + "/api/annotations"
    return _http_post_json(api, payload, token)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--status-path", default=os.getenv("CONF_CAL_LIVE_STATUS_PATH", "/tmp/conf_cal_live_status.json"))
    ap.add_argument("--state-path", default=os.getenv("CONF_CAL_ROLLBACK_WATCHER_STATE", "/tmp/conf_cal_rollback_watcher_state.json"))
    ap.add_argument("--poll-sec", type=float, default=float(os.getenv("CONF_CAL_ROLLBACK_POLL_SEC", "2.0")))
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    ap.add_argument("--stream-key", default=os.getenv("EVENTS_STREAM_KEY", "events:conf_cal"))
    ap.add_argument("--pubsub-ch", default=os.getenv("EVENTS_PUBSUB_CH", "events:conf_cal"))
    ap.add_argument("--stream-maxlen", type=int, default=int(os.getenv("EVENTS_STREAM_MAXLEN", "20000")))
    ap.add_argument("--run-id", default=os.getenv("CONF_CAL_ROLLBACK_RUN_ID", "rollback_watcher_v1"))
    # Grafana optional
    ap.add_argument("--grafana-url", default=os.getenv("GRAFANA_URL", "").strip())
    ap.add_argument("--grafana-token", default=os.getenv("GRAFANA_API_TOKEN", "").strip())
    ap.add_argument("--grafana-tags", default=os.getenv("GRAFANA_ANNOTATION_TAGS", "conf_cal,rollback"))
    ap.add_argument("--grafana-dashboard-uid", default=os.getenv("GRAFANA_DASHBOARD_UID", "").strip())
    ap.add_argument("--grafana-panel-id", type=int, default=int(os.getenv("GRAFANA_PANEL_ID", "-1")))
    args = ap.parse_args()

    r = None
    if redis is not None:
        try:
            r = redis.Redis.from_url(args.redis_url, decode_responses=False)
            r.ping()
        except Exception:
            r = None

    st = _load_state(args.state_path)
    last_total = int(st.get("last_rollback_total") or 0)
    last_ts = int(st.get("last_rollback_ts_ms") or 0)

    while True:
        status = _read_json(args.status_path) or {}
        rb_total = int(status.get("rollback_total") or 0)
        ts_ms = int(status.get("last_rollback_ts_ms") or 0)

        # Detect new rollback
        if rb_total > last_total or (ts_ms > last_ts and ts_ms > 0):
            print(f"[{now_ms()}] DETECTED ROLLBACK: total={rb_total} (was {last_total}), ts={ts_ms}")

            # Emit Redis Event
            if r and write_stream_event:
                payload = dict(status)
                payload["event_cause"] = "rollback_detected"
                write_stream_event(r, stream_key=args.stream_key, event_type="conf_cal_rollback", payload=payload, run_id=args.run_id, maxlen=args.stream_maxlen)
                if publish_event:
                    publish_event(r, channel=args.pubsub_ch, event_type="conf_cal_rollback", payload=payload, run_id=args.run_id)

            # Grafana Annotation
            if args.grafana_url and args.grafana_token:
                tags = [t.strip() for t in args.grafana_tags.split(",") if t.strip()]
                ok = _grafana_annotate(status, url=args.grafana_url, token=args.grafana_token, tags=tags, dashboard_uid=args.grafana_dashboard_uid, panel_id=args.grafana_panel_id)
                if ok:
                    print("Grafana annotation sent OK")
                else:
                    print("Grafana annotation failed")

            last_total = rb_total
            last_ts = ts_ms

            st["last_rollback_total"] = last_total
            st["last_rollback_ts_ms"] = last_ts
            _save_state(args.state_path, st)

        time.sleep(args.poll_sec)

if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        main()
