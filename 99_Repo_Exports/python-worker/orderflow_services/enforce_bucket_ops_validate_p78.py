#!/usr/bin/env python3
from __future__ import annotations
"""enforce_bucket_ops_validate_p78.py

P78: Preflight checks for Enforce-Bucket system (slippage QA + bucket-aware enforcement).

Exit codes:
  0: OK - proceed
  1: Hard fail - infrastructure/config missing
  2: Soft block - insufficient data (skip promoter/apply)

Checks:
  - Redis reachable and key(s) exist
  - metrics:of_gate has enough events and (optionally) required fields
  - Timescale view v_exec_slippage_eval exists and has enough rows in lookback window

ENV:
  REDIS_URL (default redis://localhost:6379/0)
  OF_GATE_STREAM (default metrics:of_gate)
  ENFORCE_PREFLIGHT_MIN_OF_GATE (default 200)
  ENFORCE_PREFLIGHT_REQUIRE_FIELDS (default exec_regime_bucket,reason_code_top1)
  ANALYTICS_DB_DSN or DATABASE_URL (required for DB checks)
  ENFORCE_PREFLIGHT_VIEW (default v_exec_slippage_eval)
  ENFORCE_PREFLIGHT_LOOKBACK_H (default 24)
  ENFORCE_PREFLIGHT_MIN_DB_SAMPLES (default 100)

Notes:
  - Designed to be fast and fail-open for non-critical optional fields.
""",
from utils.time_utils import get_ny_time_millis

import json
import os
import sys
import time
import logging
from typing import Any, Dict, List, Tuple

try:
    import redis
except Exception:
    redis = None

try:
    import psycopg2  # type: ignore
except Exception:
    psycopg2 = None


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("EnforceBucketPreflight")


def _now_ms() -> int:
    return get_ny_time_millis()


def _env_int(name: str, default: str) -> int:
    try:
        return int(str(os.getenv(name, default)).strip())
    except Exception:
        return int(default)


def _env_list(name: str, default: str) -> List[str]:
    raw = str(os.getenv(name, default) or "").strip()
    if not raw:
        return []
    parts = []
    for x in raw.replace(";", ",").split(","):
        s = x.strip()
        if s:
            parts.append(s)
    return parts


def _b2s(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", "replace")
    return str(x)


def _parse_last_of_gate(fields: Dict[Any, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in fields.items():
        ks = _b2s(k)
        vs = v
        if isinstance(vs, (bytes, bytearray)):
            try:
                vs = vs.decode("utf-8", "replace")
            except Exception:
                pass
        # best-effort json decode
        if isinstance(vs, str):
            s = vs.strip()
            if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
                try:
                    out[ks] = json.loads(s)
                    continue
                except Exception:
                    pass
        out[ks] = vs

    # Flatten nested payload/json if present
    payload = None
    if isinstance(out.get("payload"), dict):
        payload = out.get("payload")
    elif isinstance(out.get("json"), dict):
        payload = out.get("json")
    if isinstance(payload, dict):
        merged = dict(out)
        for k, v in payload.items():
            merged[_b2s(k)] = v
        return merged
    return out


def _redis_client(url: str):
    if redis is None:
        raise RuntimeError("redis package not installed")
    return redis.Redis.from_url(url, decode_responses=False)


def _check_redis(redis_url: str, stream: str, min_events: int, require_fields: List[str]) -> Tuple[bool, str]:
    r = _redis_client(redis_url)
    try:
        # Ping
        r.ping()
    except Exception as e:
        return False, f"redis_ping_failed:{e}"

    try:
        n = int(r.xlen(stream) or 0)
    except Exception as e:
        return False, f"redis_xlen_failed:{e}"

    if n < min_events:
        return False, f"soft:insufficient_of_gate_events:n={n}<min={min_events}"

    # Validate last entry fields (soft)
    try:
        xs = r.xrevrange(stream, count=1)
        if xs:
            _id, fields = xs[0]
            parsed = _parse_last_of_gate(fields)
            missing = [f for f in require_fields if f and (f not in parsed or parsed.get(f) in (None, ""))]
            if missing:
                return False, f"soft:missing_fields:{','.join(missing)}"
    except Exception as e:
        # If parsing fails, treat as soft block
        return False, f"soft:last_entry_parse_failed:{e}"

    return True, "ok"


def _check_db(dsn: str, view: str, lookback_h: int, min_rows: int) -> Tuple[bool, str]:
    if psycopg2 is None:
        return False, "psycopg2_not_installed"

    q = f""",
    SELECT count(*)
    FROM {view}
    WHERE ts >= now() - interval %s,
    """,
    try:
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
    except Exception as e:
        return False, f"db_connect_failed:{e}"

    try:
        with conn.cursor() as cur:
            cur.execute(q, (f"{int(lookback_h)} hours",))
            row = cur.fetchone()
            n = int(row[0] or 0) if row else 0
    except Exception as e:
        try:
            conn.close()
        except Exception:
            pass
        return False, f"db_query_failed:{e}"

    try:
        conn.close()
    except Exception:
        pass

    if n < min_rows:
        return False, f"soft:insufficient_db_rows:n={n}<min={min_rows}"
    return True, "ok"


def main() -> int:
    redis_url = os.getenv("REDIS_URL") or os.getenv("CRYPTO_NOTIFY_REDIS_URL") or "redis://localhost:6379/0"
    stream = os.getenv("OF_GATE_STREAM", "metrics:of_gate")
    min_events = _env_int("ENFORCE_PREFLIGHT_MIN_OF_GATE", "200")
    require_fields = _env_list("ENFORCE_PREFLIGHT_REQUIRE_FIELDS", "exec_regime_bucket,reason_code_top1")

    dsn = os.getenv("ANALYTICS_DB_DSN") or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL")) or ""
    view = os.getenv("ENFORCE_PREFLIGHT_VIEW", "v_exec_slippage_eval")
    lookback_h = _env_int("ENFORCE_PREFLIGHT_LOOKBACK_H", "24")
    min_rows = _env_int("ENFORCE_PREFLIGHT_MIN_DB_SAMPLES", "100")

    out: Dict[str, Any] = {
        "ts_ms": _now_ms(),
        "ok": False,
        "redis": {"ok": False},
        "db": {"ok": False},
    }

    # Redis check
    ok_r, msg_r = _check_redis(redis_url, stream, min_events, require_fields)
    out["redis"] = {"ok": bool(ok_r), "msg": msg_r, "stream": stream, "min_events": min_events, "require_fields": require_fields}

    # DB check (hard fail if missing dsn)
    if not dsn:
        out["db"] = {"ok": False, "msg": "missing_analytics_dsn", "view": view}
        print(json.dumps(out, ensure_ascii=False))
        return 1

    ok_db, msg_db = _check_db(dsn, view, lookback_h, min_rows)
    out["db"] = {"ok": bool(ok_db), "msg": msg_db, "view": view, "lookback_h": lookback_h, "min_rows": min_rows}

    # Decide
    # Any infra error -> 1
    # Any soft:... -> 2
    # Both ok -> 0
    infra_fail = []
    soft_fail = []

    for comp in ("redis", "db"):
        msg = str(out.get(comp, {}).get("msg") or "")
        ok = bool(out.get(comp, {}).get("ok"))
        if ok:
            continue
        if msg.startswith("soft:"):
            soft_fail.append(f"{comp}:{msg}")
        else:
            infra_fail.append(f"{comp}:{msg}")

    if infra_fail:
        out["ok"] = False
        out["decision"] = "hard_fail"
        out["errors"] = infra_fail
        print(json.dumps(out, ensure_ascii=False))
        return 1

    if soft_fail:
        out["ok"] = False
        out["decision"] = "soft_block"
        out["errors"] = soft_fail
        print(json.dumps(out, ensure_ascii=False))
        return 2

    out["ok"] = True
    out["decision"] = "ok"
    print(json.dumps(out, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
