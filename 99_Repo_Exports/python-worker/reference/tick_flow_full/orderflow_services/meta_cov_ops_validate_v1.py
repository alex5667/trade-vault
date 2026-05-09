#!/usr/bin/env python3
"""meta_cov_ops_validate_v1.py

P36: Preflight check for meta coverage operations bundle.
Ensures critical data streams and config are available before running the bundle.

Exit codes:
  0: OK - Proceed
  1: Hard Fail - Infrastructure issue (Redis/Config unavailable)
  2: Soft Block - Insufficient data/metrics (Bundle should Soft Block/Dry-Run)

Env:
  META_COV_SOURCE_STREAM: default metrics:of_gate
  TRADE_EVENTS_STREAM: default events:trades
  DYN_CFG_KEY: default settings:dynamic_cfg
  META_COV_PREFLIGHT_MIN_OF_GATE: default 200
  META_COV_PREFLIGHT_MIN_TRADES: default 50
  META_COV_PREFLIGHT_TIMEOUT_SEC: default 8
  REDIS_URL: default redis://localhost:6379/0
"""

import json
import logging
import os
import sys

try:
    import redis
except ImportError:
    print("redis package not installed", file=sys.stderr)
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MetaCovPreflight")


class PreflightConfig:
    def __init__(self):
        self.redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")

        self.source_stream = os.getenv("META_COV_SOURCE_STREAM", "metrics:of_gate")
        self.trade_stream = os.getenv("TRADE_EVENTS_STREAM", "events:trades")
        self.dyn_cfg_key = os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg")

        self.min_of_gate = int(os.getenv("META_COV_PREFLIGHT_MIN_OF_GATE", "200"))
        self.min_trades = int(os.getenv("META_COV_PREFLIGHT_MIN_TRADES", "50"))
        # Timeout isn't used for connection (redis handles that), but conceptually relevant
        self.timeout_sec = int(os.getenv("META_COV_PREFLIGHT_TIMEOUT_SEC", "8"))

        # P41 requirements
        self.require_trade_meta = bool(int(os.environ.get("META_COV_PREFLIGHT_REQUIRE_TRADE_META", "0") or 0))


def check_dynamic_cfg(r: redis.Redis, key: str) -> bool:
    try:
        # Just check if key exists or we can access it
        # P36 requirement: settings:dynamic_cfg availability
        # We can check TTL or just existence.
        # existence is safer.
        if not r.exists(key):
            logger.error(f"Config Key {key} NOT FOUND.")
            return False
        return True
    except Exception as e:
        logger.error(f"Redis error checking config {key}: {e}")
        raise  # Re-raise to trigger hard fail


def _b2s(x):
    if x is None:
        return ""
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", "replace")
    return str(x)


def _loads_maybe_json(v):
    if v is None:
        return None

    work_v = v
    if isinstance(work_v, (bytes, bytearray)):
        try:
            work_v = work_v.decode("utf-8", "replace")
        except Exception:
            return v

    if isinstance(work_v, str):
        s = work_v.strip()
        if not s:
            return work_v
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")) or (s.startswith('"') and s.endswith('"')):
            try:
                parsed = json.loads(s)
                # If parsed is still a string and looks like JSON, try one more time
                if isinstance(parsed, str):
                    s2 = parsed.strip()
                    if (s2.startswith("{") and s2.endswith("}")) or (s2.startswith("[") and s2.endswith("]")):
                         try:
                             return json.loads(s2)
                         except Exception:
                             return parsed
                return parsed
            except Exception:
                return work_v
        return work_v
    return work_v


def _parse_entry(fields: dict) -> dict:
    out = {}
    payload_obj = None
    for k, v in fields.items():
        ks = _b2s(k)
        out[ks] = _loads_maybe_json(v)

    # Detect nested payload/json and merge
    if isinstance(out.get("payload"), dict):
        payload_obj = out.get("payload")
    elif isinstance(out.get("json"), dict):
        payload_obj = out.get("json")

    if payload_obj:
        merged = dict(out)
        for k, v in payload_obj.items():
            ks = _b2s(k)
            merged[ks] = v
        return merged
    return out


def _is_position_closed(d: dict) -> bool:
    ev = str(d.get("event") or d.get("type") or "").upper()
    if ev == "POSITION_CLOSED":
        return True
    st = (d.get("status") or "").upper()
    return st == "POSITION_CLOSED"


def check_stream_data(
    r: redis.Redis,
    stream_key: str,
    min_count: int,
    required_fields: list[str]
) -> bool:
    """
    Checks if stream has at least min_count entries and the LAST entry has required fields.
    Returns True if OK, False if insufficient/missing fields.
    """
    try:
        # Check length
        length = r.xlen(stream_key)
        if length < min_count:
            logger.warning(f"Stream {stream_key} has {length} entries (required {min_count}). Insufficient data.")
            return False

        # Check last entry for fields
        entries = r.xrevrange(stream_key, count=1)
        if not entries:
            logger.warning(f"Stream {stream_key} is empty (but xlen said {length}?).")
            return False

        _, raw_fields = entries[0]
        fields = _parse_entry(raw_fields)

        missing = [f for f in required_fields if f not in fields]
        if missing:
            logger.warning(f"Stream {stream_key} missing required fields: {missing}. Available: {list(fields.keys())}")
            return False

        logger.info(f"Stream {stream_key} OK: len={length}, fields verified.")
        return True

    except Exception as e:
        logger.error(f"Error checking stream {stream_key}: {e}")
        raise


def main() -> int:
    cfg = PreflightConfig()

    logger.info("Starting Meta Coverage Preflight Check...")
    logger.info(f"Config: source={cfg.source_stream}, trades={cfg.trade_stream}, cfg_key={cfg.dyn_cfg_key}")

    try:
        r = redis.from_url(cfg.redis_url, decode_responses=False, socket_timeout=5.0)
        r.ping()
    except Exception as e:
        logger.error(f"CRITICAL: Cannot connect to Redis at {cfg.redis_url}: {e}")
        return 1

    # 1. Check Config
    try:
        if not check_dynamic_cfg(r, cfg.dyn_cfg_key):
             logger.error("Dynamic Config check failed (Key not found).")
             return 1
    except Exception:
        return 1

    # 2. Check Data Streams
    soft_block = False

    # Source Stream
    try:
        if not check_stream_data(r, cfg.source_stream, cfg.min_of_gate, ["meta_feature_coverage", "meta_enforce_cov_bucket"]):
            logger.warning(f"Source stream {cfg.source_stream} check FAILED.")
            soft_block = True
    except Exception:
        return 1

    # Trade Stream (P41 improvements)
    try:
        length = r.xlen(cfg.trade_stream)
        if length < cfg.min_trades:
            logger.warning(f"Trade stream {cfg.trade_stream} has {length} < {cfg.min_trades} entries.")
            soft_block = True
        else:
            # Inspect last bunch to find a POSITION_CLOSED if possible
            entries = r.xrevrange(cfg.trade_stream, count=50)
            if not entries:
                 soft_block = True
            else:
                found_closed = False
                for _, raw_fields in entries:
                    fields = _parse_entry(raw_fields)
                    if not _is_position_closed(fields):
                        continue

                    found_closed = True
                    # Fields to check: r_mult (or r), meta_enforce_cov_bucket (or meta_cov_bucket), meta_enforce_applied (or meta_applied)
                    has_r = "r_mult" in fields or "r" in fields
                    has_bucket = "meta_enforce_cov_bucket" in fields or "meta_cov_bucket" in fields
                    has_applied = "meta_enforce_applied" in fields or "meta_applied" in fields

                    if not has_r:
                        logger.warning(f"Trade event missing 'r_mult'/'r'. Fields: {list(fields.keys())}")
                        soft_block = True
                        break

                    if not (has_bucket and has_applied):
                        msg = f"Trade event missing P41 meta fields (bucket/applied). Fields: {list(fields.keys())}"
                        if cfg.require_trade_meta:
                            logger.error(f"HARD BLOCK: {msg}")
                            return 1
                        else:
                            logger.warning(f"WARNING: {msg}")
                    break

                if not found_closed:
                    logger.warning(f"No POSITION_CLOSED found in last 50 entries of {cfg.trade_stream}.")
                    soft_block = True

    except Exception as e:
        logger.error(f"Error checking trade stream: {e}")
        return 1

    if soft_block:
        logger.warning("Preflight checks failed on DATA availability/sufficiency. Returning SOFT-BLOCK (rc=2).")
        print('{"ok": 0, "status": "soft-block", "reason": "insufficient_data"}')
        return 2

    logger.info("Preflight checks PASSED.")
    print('{"ok": 1, "status": "ok"}')
    return 0


if __name__ == "__main__":
    sys.exit(main())
