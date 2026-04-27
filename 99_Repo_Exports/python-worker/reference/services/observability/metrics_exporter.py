from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Dict, List, Tuple

import redis


def _now_ms() -> int:
    return get_ny_time_millis()


def _decode(x) -> str:
    if x is None:
        return ""
    if isinstance(x, bytes):
        return x.decode("utf-8", "ignore")
    return str(x)


def _parse_tfs_map(s: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for part in (s or "").split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            continue
        k, v = part.split(":", 1)
        try:
            out[k.strip()] = int(v.strip())
        except Exception:
            continue
    return out


def _sscan_all(r: redis.Redis, key: str, limit: int = 2000) -> List[str]:
    out: List[str] = []
    cur = 0
    while True:
        cur, batch = r.sscan(key, cursor=cur, count=10000)
        for b in batch or []:
            s = _decode(b)
            if s:
                out.append(s)
                if len(out) >= limit:
                    return sorted(set(out))
        if int(cur) == 0:
            break
    return sorted(set(out))


def _emit(lines: List[str], name: str, labels: Dict[str, str], value) -> None:
    if labels:
        lab = ",".join([f'{k}="{str(v)}"' for k, v in labels.items()])
        lines.append(f"{name}{{{lab}}} {value}")
    else:
        lines.append(f"{name} {value}")


def collect_metrics(r: redis.Redis) -> str:
    lines: List[str] = []
    symbols_set = os.getenv("MICROBAR_SYMBOLS_SET", "events:microbar_closed:symbols")
    tpl = os.getenv("MICROBAR_PER_SYMBOL_STREAM_TEMPLATE", "events:microbar_closed:{sym}")
    legacy_key = os.getenv("MICROBAR_LEGACY_STREAM", "events:microbar_closed")
    majors_key = os.getenv("MICROBAR_MAJORS_STREAM", "events:microbar_closed:majors")
    max_syms = int(os.getenv("METRICS_MAX_SYMBOLS", "200"))
    tfs_map = _parse_tfs_map(os.getenv("METRICS_TFS_MAP", "1m:1,5m:2,15m:3,1h:4"))

    syms = _sscan_all(r, symbols_set, limit=max_syms)
    _emit(lines, "microbar_symbols_active", {}, len(syms))

    # Streams lengths
    try:
        _emit(lines, "xlen_microbar_closed_total", {"stream": "legacy"}, int(r.xlen(legacy_key)))
    except Exception:
        pass
    try:
        _emit(lines, "xlen_microbar_closed_total", {"stream": "majors"}, int(r.xlen(majors_key)))
    except Exception:
        pass

    # Per-symbol xlen (limited)
    if "{sym}" in tpl and syms:
        pipe = r.pipeline()
        keys: List[Tuple[str, str]] = []
        for s in syms:
            k = tpl.format(sym=s)
            keys.append((s, k))
            pipe.xlen(k)
        try:
            lens = pipe.execute()
            for (s, _k), ln in zip(keys, lens):
                try:
                    _emit(lines, "xlen_microbar_closed_symbol", {"symbol": s}, int(ln or 0))
                except Exception:
                    pass
        except Exception:
            pass

    # ATR selected TF + bad/switch/jump windows
    if syms:
        pipe = r.pipeline()
        for s in syms:
            pipe.get(f"cfg:atr_tf:{s}")
            pipe.get(f"cfg:atr_sel_meta:{s}")
            pipe.get(f"cfg:atr_bad:{s}")
            pipe.get(f"cfg:atr_switch_count:{s}")
            pipe.get(f"cfg:atr_jump_count:{s}")
        vals = pipe.execute()

        for i, s in enumerate(syms):
            tf = _decode(vals[i * 5 + 0]) or ""
            meta_raw = vals[i * 5 + 1]
            try:
                bad = int(_decode(vals[i * 5 + 2]) or "0")
            except Exception:
                bad = 0
            
            try:
                sw = int(_decode(vals[i * 5 + 3]) or "0")
            except Exception:
                sw = 0
            
            try:
                jc = int(_decode(vals[i * 5 + 4]) or "0")
            except Exception:
                jc = 0

            if tf:
                _emit(lines, "atr_selected_tf", {"symbol": s, "tf": tf}, 1)
                _emit(lines, "atr_selected_tf_value", {"symbol": s}, tfs_map.get(tf, 0))
            _emit(lines, "atr_bad_active", {"symbol": s}, bad)
            _emit(lines, "atr_switch_count_window", {"symbol": s}, sw)
            _emit(lines, "atr_jump_count_window", {"symbol": s}, jc)

            # age_ms from meta
            if meta_raw:
                try:
                    d = json.loads(meta_raw) if isinstance(meta_raw, str) else json.loads(meta_raw.decode("utf-8", "ignore"))
                    age_ms = int(d.get("age_ms", 0) or 0)
                    _emit(lines, "atr_age_ms", {"symbol": s}, age_ms)
                except Exception:
                    pass

    # CVD quarantine active (gauge) + best-effort jump totals if present
    q_syms = _sscan_all(r, "cfg:cvd_quarantine:symbols", limit=max_syms)
    q_set = set(q_syms)
    for s in syms:
        _emit(lines, "cvd_quarantine_active", {"symbol": s}, 1 if s in q_set else 0)

    if syms:
        pipe = r.pipeline()
        for s in syms:
            pipe.get(f"metrics:cvd_jump_total:{s}")
        totals = pipe.execute()
        for s, v in zip(syms, totals):
            try:
                _emit(lines, "cvd_jump_total", {"symbol": s}, int(_decode(v) or "0"))
            except Exception:
                pass

    # LCB metrics (winner changes + margin)
    _append_lcb_metrics(lines, r)

    return "\n".join(lines) + "\n"


def _append_lcb_metrics(lines: List[str], r: redis.Redis) -> None:
    # lcb_winner_changes_total{symbol,regime,scenario}
    # lcb_margin{symbol,regime,scenario}
    try:
        keys = _sscan_all(r, "metrics:lcb:keys", limit=2000)
    except Exception:
        keys = []
    if not keys:
        return
    pipe = r.pipeline()
    for k in keys:
        pipe.get(f"metrics:lcb_winner_changes_total:{k}")
        pipe.get(f"metrics:lcb_margin:{k}")
    vals = pipe.execute()
    for i, k in enumerate(keys):
        try:
            symbol, regime, scenario = k.split("|", 2)
        except Exception:
            continue
        changes = _decode(vals[i * 2 + 0]) or "0"
        margin = _decode(vals[i * 2 + 1]) or "0"
        try:
            _emit(lines, "lcb_winner_changes_total", {"symbol": symbol, "regime": regime, "scenario": scenario}, int(changes))
        except Exception:
            pass
        try:
            _emit(lines, "lcb_margin", {"symbol": symbol, "regime": regime, "scenario": scenario}, float(margin))
        except Exception:
            pass


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ("/metrics", "/metrics/"):
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"not found")
            return
        try:
            body = collect_metrics(self.server.redis_client).encode("utf-8")
        except Exception as e:
            body = f"error {e}".encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return


def main():
    if os.getenv("METRICS_ENABLE", "0") not in {"1", "true", "yes"}:
        raise SystemExit("METRICS_ENABLE=0")
    redis_url = os.getenv("METRICS_REDIS_URL") or os.getenv("REPORTS_REDIS_URL") or os.getenv("REDIS_URL") or "redis://localhost:6379/0"
    max_connections = int(os.getenv("METRICS_REDIS_MAX_CONNECTIONS", "10"))
    r = redis.Redis.from_url(
        redis_url,
        decode_responses=False,
        max_connections=max_connections,
        socket_connect_timeout=5,
        socket_timeout=15,
        socket_keepalive=True,
        health_check_interval=30,
    )
    bind = os.getenv("METRICS_BIND", "0.0.0.0")
    port = int(os.getenv("METRICS_PORT", "9109"))
    httpd = HTTPServer((bind, port), _Handler)
    httpd.redis_client = r
    httpd.serve_forever()


if __name__ == "__main__":
    main()

