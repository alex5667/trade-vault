from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Dict, Any

from tick_flow_full.core.promote_freeze import read_freeze, set_freeze, clear_freeze


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _write_ops_event(redis_url: str, event: Dict[str, Any]) -> None:
    stream = _env("OPS_EVENT_STREAM", "ops:eventlog")
    try:
        import redis  # type: ignore

        r = redis.Redis.from_url(redis_url, decode_responses=True)
        r.xadd(stream, {"ts_ms": str(_now_ms()), "event": json.dumps(event, ensure_ascii=False)[:4000]}, maxlen=5000, approximate=True)
    except Exception:
        # best-effort
        pass


def cmd_status(args: argparse.Namespace) -> int:
    st = read_freeze(args.redis_url)
    out = {"active": st.active, "until_ts_ms": st.until_ts_ms, "reason": st.reason, "source": st.source}
    print(json.dumps(out, ensure_ascii=False))
    return 0


def cmd_set(args: argparse.Namespace) -> int:
    ok = set_freeze(args.redis_url, duration_s=args.duration_s, reason=args.reason, source=args.source, extra={"actor": args.actor})
    _write_ops_event(args.redis_url, {"type": "promote_freeze_set", "ok": ok, "duration_s": args.duration_s, "reason": args.reason, "source": args.source, "actor": args.actor})
    print(json.dumps({"ok": ok}, ensure_ascii=False))
    return 0 if ok else 2


def cmd_clear(args: argparse.Namespace) -> int:
    ok = clear_freeze(args.redis_url)
    _write_ops_event(args.redis_url, {"type": "promote_freeze_clear", "ok": ok, "actor": args.actor})
    print(json.dumps({"ok": ok}, ensure_ascii=False))
    return 0 if ok else 2


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Edge Stack promote-freeze control (set/clear/status).")
    p.add_argument("--redis_url", default=_env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    p.add_argument("--actor", default=_env("OPS_ACTOR", "manual"))
    sub = p.add_subparsers(dest="cmd", required=True)

    s1 = sub.add_parser("status", help="Print current freeze state.")
    s1.set_defaults(func=cmd_status)

    s2 = sub.add_parser("set", help="Set freeze for a duration.")
    s2.add_argument("--duration_s", type=int, default=int(_env("EDGE_STACK_PROMOTE_FREEZE_DURATION_S", "86400")))
    s2.add_argument("--reason", required=True)
    s2.add_argument("--source", default="manual")
    s2.set_defaults(func=cmd_set)

    s3 = sub.add_parser("clear", help="Clear freeze.")
    s3.set_defaults(func=cmd_clear)

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))  # type: ignore


if __name__ == "__main__":
    raise SystemExit(main())
