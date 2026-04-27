from __future__ import annotations

import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import List, Tuple

import redis


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


REDIS_URL = _env("REDIS_URL", "redis://redis-worker-1:6379/0")
PORT = int(_env("CHATOPS_METRICS_EXPORTER_PORT", "9816"))


def _r() -> redis.Redis:
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)


def _get_int(r: redis.Redis, key: str) -> int:
    try:
        v = r.get(key)
        return int(float(v)) if v else 0
    except Exception:
        return 0


def _scan_cmd_keys(r: redis.Redis) -> List[Tuple[str, int]]:
    out: List[Tuple[str, int]] = []
    cur = 0
    pat = "metrics:chatops:cmd_total:*"
    try:
        while True:
            cur, keys = r.scan(cur, match=pat, count=100)
            for k in keys:
                cmd = k.split(":")[-1]
                out.append((cmd, _get_int(r, k)))
            if cur == 0:
                break
    except Exception:
        return []
    return sorted(out, key=lambda x: x[0])


def render_metrics() -> str:
    r = _r()
    unauth = _get_int(r, "metrics:chatops:unauthorized_total")
    rl = _get_int(r, "metrics:chatops:rate_limited_total")
    pending = _get_int(r, "metrics:chatops:clear_pending_started_total")
    last_unauth = _get_int(r, "metrics:chatops:last_unauthorized_ts_ms")
    last_rl = _get_int(r, "metrics:chatops:last_rate_limited_ts_ms")
    cmd_counts = _scan_cmd_keys(r)

    lines = []
    lines.append("# HELP chatops_unauthorized_total Unauthorized attempts in allowed chat")
    lines.append("# TYPE chatops_unauthorized_total counter")
    lines.append(f"chatops_unauthorized_total {unauth}")

    lines.append("# HELP chatops_rate_limited_total Rate-limited admin commands")
    lines.append("# TYPE chatops_rate_limited_total counter")
    lines.append(f"chatops_rate_limited_total {rl}")

    lines.append("# HELP chatops_clear_pending_started_total Two-person clear pending started total")
    lines.append("# TYPE chatops_clear_pending_started_total counter")
    lines.append(f"chatops_clear_pending_started_total {pending}")

    lines.append("# HELP chatops_last_unauthorized_ts_ms Last unauthorized attempt timestamp (ms)")
    lines.append("# TYPE chatops_last_unauthorized_ts_ms gauge")
    lines.append(f"chatops_last_unauthorized_ts_ms {last_unauth}")

    lines.append("# HELP chatops_last_rate_limited_ts_ms Last rate-limited timestamp (ms)")
    lines.append("# TYPE chatops_last_rate_limited_ts_ms gauge")
    lines.append(f"chatops_last_rate_limited_ts_ms {last_rl}")

    lines.append("# HELP chatops_cmd_total Total chatops commands by cmd")
    lines.append("# TYPE chatops_cmd_total counter")
    for cmd, v in cmd_counts:
        cmd2 = cmd.replace('"', "").replace("\\", "")
        lines.append(f'chatops_cmd_total{{cmd="{cmd2}"}} {v}')

    return "\n".join(lines) + "\n"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path in ("/healthz", "/health"):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok\n")
            return
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        body = render_metrics().encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            # Client (Prometheus scraper) closed the connection before we
            # finished writing — harmless, suppress the noisy traceback.
            pass

    def log_message(self, format: str, *args) -> None:
        return


class _QuietHTTPServer(HTTPServer):
    """Suppress BrokenPipeError / ConnectionResetError tracebacks that
    socketserver prints to stderr on every scrape disconnect."""

    def handle_error(self, request: object, client_address: tuple) -> None:  # type: ignore[override]
        import sys
        exc = sys.exc_info()[1]
        if isinstance(exc, (BrokenPipeError, ConnectionResetError)):
            return
        super().handle_error(request, client_address)  # type: ignore[arg-type]


def main() -> None:
    srv = _QuietHTTPServer(("0.0.0.0", PORT), Handler)
    srv.serve_forever()


if __name__ == "__main__":
    main()
