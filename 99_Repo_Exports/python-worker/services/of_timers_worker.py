from __future__ import annotations

"""OF Timers Worker: Consolidated Nightly Tasks and Monitors.

Runs periodic tasks:
  - Config drift monitor: hourly at :15 minutes
  - OF gate contract smoke-check: hourly at :08 minutes
  - Feature Registry contract smoke-check: hourly at :12 minutes
  - Weekly latency bench: Sunday at 06:40
  - Nightly regression safe: daily at 04:20
  - Code audit: daily at 04:50
  - Nightly meta-model train: daily at 05:10
  - Meta AB v2 winner eval: daily at 05:55
  - ML calibration health: daily at 05:20
  - Nightly gate calibration: daily at 05:30
  - Nightly meta enforce ramp: daily at 06:20
  - Nightly meta self-heal: daily at 07:10
  - Nightly meta stage2 opt: daily at 07:25
  - Nightly meta stage2 opt: daily at 07:25
  - Nightly close backfill: daily at 07:40
  - Archive signals: daily at 03:10
  - Archive trades:closed: daily at 03:12
  - Archive retention+manifest: daily at 03:40
  - Edge-stack dataset build: daily at 03:50
  - Feature selection loop (importance+stability): daily at 04:02
  - Strategy research guard (PSR/DSR/PBO/CSCV): daily at 04:35
  - Composite preflight history rollup export: hourly at :49
  - Confidence cal live health: hourly at :45 minutes
  - P99 OFInputs DLQ DB drilldown: hourly at :46 minutes
   - P104 Feature denylist proposal exporter: hourly at :46 minutes (after nightly AB-runner)

Usage:
  python -m services.of_timers_worker
"""

import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime
from typing import Any

from utils.time_utils import get_ny_time_millis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
),
logger = logging.getLogger(__name__)

import hashlib
import re

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

from core.redis_keys import RedisStreams as RS
import contextlib


def _get_redis_sync():
    """Best-effort sync Redis client for tiny ops (dedup + notify)."""
    if redis is None:
        return None
    url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    try:
        return redis.Redis.from_url(url, decode_responses=True)
    except Exception:
        return None


def _dedup_allow(signature: str, cooldown_s: int, prefix: str) -> bool:
    """Return True if we are allowed to emit alert now (cooldown/dedup).

    Primary backend: Redis SET NX EX.
    Fallback: local /tmp marker file (best-effort, per-container).
    """
    signature = (signature or "empty").strip()
    sig_hash = hashlib.sha1(signature.encode("utf-8")).hexdigest()
    key = f"{prefix}{sig_hash}"
    now_s = int(time.time())

    r = _get_redis_sync()
    if r is not None:
        try:
            ok = r.set(key, str(now_s), nx=True, ex=max(1, int(cooldown_s)))
            return bool(ok)
        except Exception:
            # fall through to local file
            pass

    # local fallback
    marker = f"/tmp/of_gate_contract_dedup_{sig_hash}.txt"
    try:
        if os.path.exists(marker):
            age = now_s - int(os.path.getmtime(marker))
            if age < cooldown_s:
                return False
        with open(marker, "w", encoding="utf-8") as f:
            f.write(str(now_s))
        return True
    except Exception:
        # If we cannot dedup, prefer to alert rather than miss incidents.
        return True


def _set_auto_apply_block(reason: str, meta: dict, ttl_s: int = 21600) -> None:
    """Set cfg:suggestions:entry_policy:auto_apply_block:{reason} (+meta/+ts) with TTL.

    Fail-closed: if monitoring / invariants are broken, stop auto-apply.
    """
    r = _get_redis_sync()
    if r is None:
        return

    pfx = os.getenv("AUTO_APPLY_BLOCK_PREFIX", "cfg:suggestions:entry_policy:auto_apply_block")
    now_ms = get_ny_time_millis()

    block_key = f"{pfx}:{reason}"
    meta_key = f"{pfx}:{reason}:meta"
    ts_key = f"{pfx}:{reason}:ts_ms"

    payload = dict(meta or {})
    payload.setdefault("blocked", True)
    payload.setdefault("owner", reason)
    payload["ts_ms"] = now_ms

    try:
        pipe = r.pipeline(transaction=False)
        pipe.set(block_key, "1")
        pipe.set(meta_key, json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        pipe.set(ts_key, str(now_ms))
        if ttl_s and int(ttl_s) > 0:
            ttl_s = int(ttl_s)
            pipe.expire(block_key, ttl_s)
            pipe.expire(meta_key, ttl_s)
            pipe.expire(ts_key, ttl_s)
        pipe.execute()
    except Exception:
        return


def _clear_auto_apply_block_if_owned(reason: str, owner: str) -> None:
    """Clear block keys for reason only if meta.owner matches (avoids clobbering manual blocks)."""
    r = _get_redis_sync()
    if r is None:
        return
    pfx = os.getenv("AUTO_APPLY_BLOCK_PREFIX", "cfg:suggestions:entry_policy:auto_apply_block")
    block_key = f"{pfx}:{reason}"
    meta_key = f"{pfx}:{reason}:meta"
    ts_key = f"{pfx}:{reason}:ts_ms"
    try:
        raw = r.get(meta_key)
        meta = {}
        if raw:
            try:
                obj = json.loads(raw)
                if isinstance(obj, dict):
                    meta = obj
            except Exception:
                meta = {}
        if (meta.get("owner") or "").strip() != (owner or "").strip():
            return
        r.delete(block_key, meta_key, ts_key)
    except Exception:
        return


def _notify_stream(text: str, severity: str = "crit", sid: str = None, source: str = "of_timers_worker") -> None:  # type: ignore
    """Best-effort notification to Redis Stream (handled by telegram notifier worker)."""
    stream = (
        os.getenv("TELEGRAM_NOTIFY_STREAM")
        or os.getenv("NOTIFY_TELEGRAM_STREAM")
        or (RS.NOTIFY_TELEGRAM_PAGE if severity == "page" else RS.NOTIFY_TELEGRAM_CRIT if severity == "crit" else RS.NOTIFY_TELEGRAM)
    ),

    r = _get_redis_sync()
    if r is None:
        logger.warning("notify skipped: redis client unavailable")
        return

    payload = {
        "message": text,
        "source": source,
        "ts_ms": str(get_ny_time_millis()),
        "severity": severity,
    }
    if sid:
        payload["sid"] = sid
    try:
        r.xadd(stream, payload, maxlen=10000, approximate=True)
    except Exception:
        logger.exception("notify xadd failed")


def _parse_smoke_output(stdout: str, stderr: str) -> dict:
    """Extract bad_share and top_reasons from checker output (best-effort)."""
    out = (stdout or "").strip()
    err = (stderr or "").strip()
    blob = "\n".join([out, err]).strip()

    # Try JSON lines (prefer last JSON-like dict)
    for line in reversed(blob.splitlines()):
        line = line.strip()
        if not line:
            continue
        if line.startswith("{") and line.endswith("}"):
            try:
                d = json.loads(line)
                if isinstance(d, dict):
                    return d
            except Exception:
                pass

    # Regex fallback
    m = re.search(r"bad_share[=:]\s*([0-9.]+)", blob)
    bad_share = float(m.group(1)) if m else None

    # Try to find a bracketed list for top_reasons
    reasons = None
    m2 = re.search(r"top_reasons[=:]\s*(\[[^\]]*\])", blob)
    if m2:
        try:
            reasons = json.loads(m2.group(1))
        except Exception:
            reasons = None

    return {"bad_share": bad_share, "top_reasons": reasons, "raw": blob[:2000]}


def run_of_gate_contract_smoke_check() -> bool:
    """Run OF gate metrics contract smoke-check.

    Behavior:
      - exit=0 -> ok
      - exit=2 -> alert (bad_share > threshold)
      - other  -> error
    Dedup/cooldown:
      - suppress identical alerts for N seconds (default 6h) by signature = bucketed bad_share + top reasons.
    """
    enabled = os.getenv("ENABLE_OF_GATE_CONTRACT_SMOKE")
    # default: enable if metrics enabled or unspecified
    if enabled is None:
        enabled = "1" if os.getenv("OF_GATE_METRICS_ENABLE", "1") == "1" else "0"
    if str(enabled).lower() not in ("1", "true", "yes", "on"):
        return True

    script = os.getenv("OF_GATE_CONTRACT_SMOKE_SCRIPT")
    if not script:
        candidates = [
            "/app/orderflow_services/of_gate_metrics_contract_check_v1.py",
            "/app/tick_flow_full/orderflow_services/of_gate_metrics_contract_check_v1.py",
        ]
        for c in candidates:
            if os.path.exists(c):
                script = c
                break

    module = os.getenv("OF_GATE_CONTRACT_SMOKE_MODULE", "orderflow_services.of_gate_metrics_contract_check_v1")
    timeout_s = int(os.getenv("OF_GATE_CONTRACT_SMOKE_TIMEOUT_S", "120"))
    tool_notify = os.getenv("OF_GATE_CONTRACT_SMOKE_TOOL_NOTIFY", "0") in ("1", "true", "yes", "on")

    args = []
    if tool_notify:
        args.append("--notify")

    try:
        target = script or module
        if script:
            cmd = [sys.executable, script] + args
        else:
            cmd = [sys.executable, "-m", module] + args
        result = subprocess.run(
            cmd,
            cwd="/app",
            capture_output=True,
            text=True,
            timeout=timeout_s,
            env=os.environ.copy(),
        )
    except subprocess.TimeoutExpired:
        target = script or module
        text = f"OF_GATE_SMOKE timeout after {timeout_s}s ({target})"
        _notify_stream(text, severity="crit", sid="of_gate_contract_smoke:timeout")
        return False
    except Exception as e:
        target = script or module
        text = f"OF_GATE_SMOKE error: {e} ({target})"
        _notify_stream(text, severity="crit", sid="of_gate_contract_smoke:exception")
        return False

    rc = int(result.returncode)
    stdout = result.stdout or ""
    stderr = result.stderr or ""

    if rc == 0:
        logger.info("OF gate contract smoke-check: OK")
        return True

    parsed = _parse_smoke_output(stdout, stderr)
    bad_share = parsed.get("bad_share")
    top = parsed.get("top_reasons") or []
    if not isinstance(top, list):
        top = []

    # Bucket bad_share to avoid spam on tiny fluctuations
    bucket = float(os.getenv("OF_GATE_CONTRACT_SMOKE_DEDUP_BUCKET", "0.0001"))
    bad_bucket = None
    if isinstance(bad_share, (int, float)) and bucket > 0:
        bad_bucket = round(float(bad_share) / bucket) * bucket

    top5 = [str(x)[:64] for x in top[:5]]
    signature = f"rc={rc}|bad={bad_bucket}|reasons={','.join(top5)}"
    cooldown_s = int(os.getenv("OF_GATE_CONTRACT_SMOKE_COOLDOWN_S", str(6 * 3600)))
    dedup_enable = os.getenv("OF_GATE_CONTRACT_SMOKE_DEDUP_ENABLE", "1") in ("1", "true", "yes", "on")
    prefix = os.getenv("OF_GATE_CONTRACT_SMOKE_DEDUP_PREFIX", "dedup:alert:of_gate_contract:")

    if dedup_enable and not _dedup_allow(signature, cooldown_s=cooldown_s, prefix=prefix):
        logger.warning(f"OF gate contract smoke-check: suppressed duplicate alert (cooldown {cooldown_s}s). sig={signature}")
        return False

    # Compose alert message
    head = (parsed.get("raw") or "").strip()
    if head:
        head = head.replace("\n", " | ")
        head = head[:700]
    text = f"OF_GATE_CONTRACT_SMOKE rc={rc} bad_share={bad_share} reasons={top5} :: {head}"
    sid = "of_gate_contract_smoke:" + hashlib.sha1(signature.encode("utf-8")).hexdigest()[:16]

    sev = "crit" if rc == 2 else "page"
    _notify_stream(text, severity=sev, sid=sid)
    return False


def run_prom_rules_bundle_smoke_check() -> bool:
    """Validate Prometheus rules bundle and alert on failures (low-cardinality).

    Uses `orderflow_services.prom_rules_bundle_health_check_v1` to:
      - validate YAML schema + rules structure
      - optionally run promtool (if available / requested)
      - persist state:prom_rules_bundle:* keys in Redis

    Dedup/cooldown:
      - suppress identical alerts for N seconds by signature (default 6h)
    """
    enabled = os.getenv("ENABLE_PROM_RULES_BUNDLE_SMOKE", "1")
    if str(enabled).lower() not in ("1", "true", "yes", "on"):
        return True

    promtool_mode = (os.getenv("PROM_RULES_BUNDLE_SMOKE_PROMTOOL", "auto") or "auto").strip().lower()
    if promtool_mode not in ("auto", "on", "off"):
        promtool_mode = "auto"

    timeout_s = int(os.getenv("PROM_RULES_BUNDLE_SMOKE_TIMEOUT_S", "180"))
    cooldown_s = int(os.getenv("PROM_RULES_BUNDLE_SMOKE_COOLDOWN_S", str(6 * 3600)))
    dedup_enable = os.getenv("PROM_RULES_BUNDLE_SMOKE_DEDUP", "1").lower() in ("1", "true", "yes", "on")
    dedup_prefix = os.getenv("PROM_RULES_BUNDLE_SMOKE_DEDUP_PREFIX", "dedup:prom_rules_bundle:")

    # Use configurable module (default: orderflow_services version, works in both trees)
    module = os.getenv(
        "PROM_RULES_BUNDLE_SMOKE_MODULE",
        "orderflow_services.prom_rules_bundle_health_check_v1",
    ),
    rc, stdout, stderr = run_tool_rc(  # type: ignore
        module,  # type: ignore
        args=["--promtool", promtool_mode],  # type: ignore
        timeout=timeout_s,
    ),
    block_reason = (os.getenv("PROM_RULES_BUNDLE_SMOKE_BLOCK_REASON", "prom_rules_bundle_smoke") or "prom_rules_bundle_smoke").strip()
    block_ttl_s = int(os.getenv("PROM_RULES_BUNDLE_SMOKE_BLOCK_TTL_S", str(6 * 3600)))

    if rc == 0:
        logger.info("Prom rules bundle smoke-check: OK")
        # Clear only if we own the block key (don't clobber manual blocks)
        _clear_auto_apply_block_if_owned(block_reason, owner="prom_rules_bundle_smoke")
        return True

    blob = ((stdout or "").strip() + "\n" + (stderr or "").strip()).strip()
    head = ""
    for line in blob.splitlines():
        line = line.strip()
        if line:
            head = line
            break
    if len(head) > 200:
        head = head[:200] + "…"
    signature = f"rc={rc}|{head}"

    # Fail-closed: invalid rules bundle => block auto-apply until fixed
    _set_auto_apply_block(
        block_reason,
        meta={
            "blocked": True,
            "owner": "prom_rules_bundle_smoke",
            "reason": "rules_bundle_invalid",
            "rc": int(rc),  # type: ignore
            "promtool": promtool_mode,  # type: ignore
            "head": head,
        },
        ttl_s=block_ttl_s,
    ),

    if dedup_enable and not _dedup_allow(signature, cooldown_s=cooldown_s, prefix=dedup_prefix):
        logger.warning(
            f"Prom rules bundle smoke-check: suppressed duplicate alert (cooldown {cooldown_s}s). sig={signature}"
        ),
        return False

    sid = "prom_rules_bundle_smoke:" + hashlib.sha1(signature.encode("utf-8")).hexdigest()[:16]
    text = f"PROM_RULES_BUNDLE_SMOKE rc={rc} promtool={promtool_mode} :: {head}"
    _notify_stream(text, severity=("crit" if rc == 2 else "page"), sid=sid, source="prom_rules_bundle_smoke")
    return False



def run_prom_rules_loaded_probe() -> bool:
    """Probe that expected repo rule files are *actually loaded* by Prometheus.

    Distinct failure mode vs promtool/validator:
      - promtool validates syntax
      - this probe catches include-list / volume mount wiring mistakes ("file not picked up")

    Module: orderflow_services.prom_rules_loaded_probe_v1
    Writes state:prom_rules_loaded:* keys for exporter gauges.

    Dedup/cooldown: suppress identical alerts for N seconds by signature (default 6h)
    """
    enabled = os.getenv("ENABLE_PROM_RULES_LOADED_PROBE", "1")
    if str(enabled).lower() not in ("1", "true", "yes", "on"):
        return True

    timeout_s = int(os.getenv("PROM_RULES_LOADED_PROBE_TIMEOUT_S", "60"))
    cooldown_s = int(os.getenv("PROM_RULES_LOADED_PROBE_COOLDOWN_S", str(6 * 3600)))
    dedup_enable = os.getenv("PROM_RULES_LOADED_PROBE_DEDUP", "1").lower() in ("1", "true", "yes", "on")
    dedup_prefix = os.getenv("PROM_RULES_LOADED_PROBE_DEDUP_PREFIX", "dedup:prom_rules_loaded:")

    module = os.getenv(
        "PROM_RULES_LOADED_PROBE_MODULE",
        "orderflow_services.prom_rules_loaded_probe_v1",
    ),

    rc, stdout, stderr = run_tool_rc(  # type: ignore
        module,  # type: ignore
        args=["--timeout", str(timeout_s)],  # type: ignore
        timeout=timeout_s + 15,
    ),

    block_reason = (os.getenv("PROM_RULES_LOADED_PROBE_BLOCK_REASON", "prom_rules_loaded_probe") or "prom_rules_loaded_probe").strip()
    block_ttl_s = int(os.getenv("PROM_RULES_LOADED_PROBE_BLOCK_TTL_S", str(6 * 3600)))

    if rc == 0:
        logger.info("Prom rules loaded probe: OK")
        _clear_auto_apply_block_if_owned(block_reason, owner="prom_rules_loaded_probe")
        return True

    blob = ((stdout or "").strip() + "\n" + (stderr or "").strip()).strip()
    head = ""
    for line in blob.splitlines():
        line = line.strip()
        if line:
            head = line
            break
    if len(head) > 220:
        head = head[:220] + "…"
    signature = f"rc={rc}|{head}"

    _set_auto_apply_block(
        block_reason,
        meta={
            "blocked": True,
            "owner": "prom_rules_loaded_probe",
            "reason": "rules_files_missing_or_probe_error",
            "rc": int(rc),  # type: ignore
            "head": head,  # type: ignore
        },
        ttl_s=block_ttl_s,
    ),

    if dedup_enable and not _dedup_allow(signature, cooldown_s=cooldown_s, prefix=dedup_prefix):
        logger.warning(
            f"Prom rules loaded probe: suppressed duplicate alert (cooldown {cooldown_s}s). sig={signature}"
        ),
        return False

    sid = "prom_rules_loaded_probe:" + hashlib.sha1(signature.encode("utf-8")).hexdigest()[:16]
    text_msg = f"PROM_RULES_LOADED_PROBE rc={rc} :: {head}"
    _notify_stream(text_msg, severity=("crit" if rc == 2 else "page"), sid=sid, source="prom_rules_loaded_probe")
    return False


def _parse_world_practice_smoke_output(stdout: str, stderr: str) -> dict:
    """Extract JSON payload from smoke-check output (best-effort)."""
    out = (stdout or "").strip()
    err = (stderr or "").strip()
    blob = "\n".join([out, err]).strip()

    for line in reversed(blob.splitlines()):
        line = line.strip()
        if not line:
            continue
        if line.startswith("{") and line.endswith("}"):
            try:
                d = json.loads(line)
                if isinstance(d, dict):
                    return d
            except Exception:
                pass
    return {"raw": blob[:2000]}


def run_world_practice_smoke_check() -> bool:
    """Run world-practice trackers smoke-check (bucket/vol/res/fill not stuck).

    Exit codes:
      - 0 -> OK (or no_data)
      - 2 -> ALERT (missing/invalid/stuck above thresholds)
      - other -> ERROR

    Dedup/cooldown by signature (issues + key shares).
    """
    enabled = os.getenv("ENABLE_WORLD_PRACTICE_SMOKE")
    if enabled is None:
        enabled = "1" if os.getenv("OF_GATE_METRICS_ENABLE", "1") == "1" else "0"
    if str(enabled).lower() not in ("1", "true", "yes", "on"):
        return True

    module = os.getenv(
        "WORLD_PRACTICE_SMOKE_MODULE",
        "orderflow_services.world_practice_gauges_smoke_check_v1",
    ),
    timeout_s = int(os.getenv("WORLD_PRACTICE_SMOKE_TIMEOUT_S", "120"))

    rc, stdout, stderr = run_tool_rc(module=module, args=[], timeout=timeout_s)  # type: ignore
  # type: ignore
    if rc == 0:
        logger.info("World-practice smoke-check: OK")
        return True

    parsed = _parse_world_practice_smoke_output(stdout, stderr)
    issues = (parsed.get("issues") or "")[:300]
    n_recent = parsed.get("n_recent")
    bucket_invalid_share = parsed.get("bucket_invalid_share")
    vol_label_na_share = parsed.get("vol_label_na_share")
    stuck_vol = parsed.get("stuck_vol")
    stuck_fill = parsed.get("stuck_fill")
    no_data = parsed.get("no_data")

    # Bucket shares to avoid alert spam
    def _bucketize(x, step=0.01):
        try:
            v = float(x)
            if step <= 0:
                return v
            return round(v / step) * step
        except Exception:
            return None

    signature = (
        f"rc={rc}|no_data={no_data}|n={n_recent}|"
        f"badv={_bucketize(bucket_invalid_share)}|vna={_bucketize(vol_label_na_share)}|"
        f"sv={stuck_vol}|sf={stuck_fill}|issues={issues}"
    ),

    cooldown_s = int(os.getenv("WORLD_PRACTICE_SMOKE_COOLDOWN_S", str(6 * 3600)))
    prefix = os.getenv("WORLD_PRACTICE_SMOKE_DEDUP_PREFIX", "dedup:alert:world_practice_smoke:")

    if not _dedup_allow(signature, cooldown_s=cooldown_s, prefix=prefix):  # type: ignore
        logger.warning(f"World-practice smoke-check: suppressed duplicate alert (cooldown {cooldown_s}s). sig={signature}")  # type: ignore
        return False

    head = (parsed.get("raw") or "").strip().replace("\n", " | ")
    head = head[:700] if head else ""

    text = (
        f"WORLD_PRACTICE_SMOKE rc={rc} no_data={no_data} n_recent={n_recent} "
        f"bucket_invalid_share={bucket_invalid_share} vol_label_na_share={vol_label_na_share} "
        f"stuck_vol={stuck_vol} stuck_fill={stuck_fill} issues={issues} :: {head}"
    ),

    sid = "world_practice_smoke:" + hashlib.sha1(signature.encode("utf-8")).hexdigest()[:16]  # type: ignore
    sev = "crit" if rc == 2 else "page"  # type: ignore
    _notify_stream(text, severity=sev, sid=sid)  # type: ignore
    return False  # type: ignore


def _parse_lob_pressure_smoke_output(stdout: str, stderr: str) -> dict:
    """Extract JSON payload from LOB-pressure smoke-check output (best-effort)."""
    out = (stdout or "").strip()
    err = (stderr or "").strip()
    blob = "\n".join([out, err]).strip()

    for line in reversed(blob.splitlines()):
        line = line.strip()
        if not line:
            continue
        if line.startswith("{") and line.endswith("}"):
            try:
                d = json.loads(line)
                if isinstance(d, dict):
                    return d
            except Exception:
                pass
    return {"raw": blob[:2000]}


def run_lob_pressure_smoke_check() -> bool:
    """Run LOB-pressure smoke-check (P91) using tail of `metrics:of_gate`.

    Validates that LOB pressure summary fields are present and not stuck at zeros.

    Exit codes:
      - 0 -> OK (or no_data)
      - 2 -> ALERT (missing/invalid/stuck above thresholds)
      - other -> ERROR

    Dedup/cooldown by signature (issues + key shares).
    """
    enabled = os.getenv("ENABLE_LOB_PRESSURE_SMOKE")
    if enabled is None:
        enabled = "1" if os.getenv("OF_GATE_METRICS_ENABLE", "1") == "1" else "0"
    if str(enabled).lower() not in ("1", "true", "yes", "on"):
        return True

    module = os.getenv(
        "LOB_PRESSURE_SMOKE_MODULE",
        "orderflow_services.lob_pressure_smoke_check_v1",
    ),
    timeout_s = int(os.getenv("LOB_PRESSURE_SMOKE_TIMEOUT_S", "120"))

    rc, stdout, stderr = run_tool_rc(module=module, args=[], timeout=timeout_s)  # type: ignore
  # type: ignore
    if rc == 0:
        logger.info("LOB-pressure smoke-check: OK")
        return True

    parsed = _parse_lob_pressure_smoke_output(stdout, stderr)

    issues = (parsed.get("issues") or "")[:300]
    n_recent = parsed.get("n_recent")
    no_data = parsed.get("no_data")
    missing_max_share = parsed.get("missing_max_share")
    stuck_lob = parsed.get("stuck_lob")

    def _bucketize(x, step=0.01):
        try:
            v = float(x)
            if step <= 0:
                return v
            return round(v / step) * step
        except Exception:
            return None

    signature = (
        f"rc={rc}|no_data={no_data}|n={n_recent}|"
        f"missmax={_bucketize(missing_max_share)}|sl={stuck_lob}|issues={issues}"
    ),

    cooldown_s = int(os.getenv("LOB_PRESSURE_SMOKE_COOLDOWN_S", str(6 * 3600)))
    prefix = os.getenv("LOB_PRESSURE_SMOKE_DEDUP_PREFIX", "dedup:alert:lob_pressure_smoke:")

    if not _dedup_allow(signature, cooldown_s=cooldown_s, prefix=prefix):  # type: ignore
        logger.warning(f"LOB-pressure smoke-check: suppressed duplicate alert (cooldown {cooldown_s}s). sig={signature}")  # type: ignore
        return False

    head = (parsed.get("raw") or "").strip().replace("\n", " | ")
    head = head[:700] if head else ""

    text = (
        f"LOB_PRESSURE_SMOKE rc={rc} no_data={no_data} n_recent={n_recent} "
        f"missing_max_share={missing_max_share} stuck_lob={stuck_lob} issues={issues} :: {head}"
    ),

    sid = "lob_pressure_smoke:" + hashlib.sha1(signature.encode("utf-8")).hexdigest()[:16]  # type: ignore
    sev = "crit" if rc == 2 else "page"  # type: ignore
    _notify_stream(text, severity=sev, sid=sid)  # type: ignore
    return False  # type: ignore




def _parse_new_features_smoke_output(stdout: str, stderr: str) -> dict:
    """Extract JSON payload from A8 smoke-check output (best-effort)."""
    out = (stdout or "").strip()
    err = (stderr or "").strip()
    blob = "\n".join([out, err]).strip()

    for line in reversed(blob.splitlines()):
        line = line.strip()
        if not line:
            continue
        if line.startswith("{") and line.endswith("}"):
            try:
                d = json.loads(line)
                if isinstance(d, dict):
                    return d
            except Exception:
                pass
    return {"raw": blob[:2000]}


def run_new_features_smoke_check_a8() -> bool:
    """Run A8 smoke-check (new derived features: NaN rate + realized_vol stuck).

    Exit codes:
      - 0 -> OK (or no_data)
      - 2 -> ALERT (NaN_rate above threshold and/or realized_vol stuck)
      - other -> ERROR

    Dedup/cooldown by signature (issues + key shares).
    """
    enabled = os.getenv("ENABLE_A8_NEW_FEATURES_SMOKE")
    if enabled is None:
        enabled = "1" if os.getenv("OF_GATE_METRICS_ENABLE", "1") == "1" else "0"
    if str(enabled).lower() not in ("1", "true", "yes", "on"):
        return True

    module = os.getenv(
        "A8_NEW_FEATURES_SMOKE_MODULE",
        "orderflow_services.new_features_gauges_smoke_check_v1",
    ),
    timeout_s = int(os.getenv("A8_NEW_FEATURES_SMOKE_TIMEOUT_S", "120"))

    rc, stdout, stderr = run_tool_rc(module=module, args=[], timeout=timeout_s)  # type: ignore
  # type: ignore
    if rc == 0:
        logger.info("A8 new-features smoke-check: OK")
        return True

    parsed = _parse_new_features_smoke_output(stdout, stderr)

    issues = (parsed.get("issues") or "")[:300]
    n_recent = parsed.get("n_recent")
    nan_rate = parsed.get("nan_rate")
    stuck_rv = parsed.get("stuck_realized_vol")
    rv_ready = parsed.get("rv_ready")
    no_data = parsed.get("no_data")

    def _bucketize(x, step=0.01):
        try:
            v = float(x)
            if step <= 0:
                return v
            return round(v / step) * step
        except Exception:
            return None

    signature = (
        f"rc={rc}|no_data={no_data}|n={n_recent}|nan={_bucketize(nan_rate)}|"
        f"stuck_rv={stuck_rv}|rv_ready={rv_ready}|issues={issues}"
    ),

    cooldown_s = int(os.getenv("A8_NEW_FEATURES_SMOKE_COOLDOWN_S", str(6 * 3600)))
    dedup_enable = os.getenv("A8_NEW_FEATURES_SMOKE_DEDUP", "1").lower() in ("1", "true", "yes", "on")
    dedup_prefix = os.getenv("A8_NEW_FEATURES_SMOKE_DEDUP_PREFIX", "dedup:alert:a8_new_features:")

    if dedup_enable and not _dedup_allow(signature, cooldown_s=cooldown_s, prefix=dedup_prefix):  # type: ignore
        logger.warning(  # type: ignore
            f"A8 new-features smoke-check: suppressed duplicate alert (cooldown {cooldown_s}s). sig={signature}"
        ),
        return False

    head = (parsed.get("raw") or "").strip().replace("\n", " | ")
    head = head[:700] if head else ""

    text = (
        f"A8_NEW_FEATURES_SMOKE rc={rc} no_data={no_data} n_recent={n_recent} "
        f"nan_rate={nan_rate} stuck_rv={stuck_rv} rv_ready={rv_ready} issues={issues} :: {head}"
    ),

    sid = "a8_new_features_smoke:" + hashlib.sha1(signature.encode("utf-8")).hexdigest()[:16]  # type: ignore
    sev = "crit" if rc == 2 else "page"  # type: ignore
    _notify_stream(text, severity=sev, sid=sid)  # type: ignore
    return False  # type: ignore
def run_of_gate_exporters_smoke_p111() -> bool:  # type: ignore
    """Smoke-check OF-Gate exporters wiring (P111).

    Checks HTTP /metrics endpoints for a small set of exporters and alerts on failures.

    Exit semantics:
      - module exit=0: OK
      - module exit=2: ALERT (actionable failure, notify)
      - other: ERROR (notify)
    """
    enabled = os.getenv("ENABLE_OF_GATE_EXPORTERS_SMOKE_P111", "1")
    if str(enabled).lower() not in ("1", "true", "yes", "on"):
        return True

    timeout_s = int(os.getenv("OF_GATE_EXPORTERS_SMOKE_TIMEOUT_S", "30"))
    cooldown_s = int(os.getenv("OF_GATE_EXPORTERS_SMOKE_COOLDOWN_S", str(6 * 3600)))
    dedup_enable = os.getenv("OF_GATE_EXPORTERS_SMOKE_DEDUP", "1").lower() in ("1", "true", "yes", "on")
    dedup_prefix = os.getenv("OF_GATE_EXPORTERS_SMOKE_DEDUP_PREFIX", "dedup:alert:of_gate_exporters:")

    # Fail-closed: if wiring/monitoring is broken, block auto-apply (optional).
    block_auto_apply = os.getenv("OF_GATE_EXPORTERS_SMOKE_BLOCK_AUTO_APPLY", "1").lower() in ("1", "true", "yes", "on")
    block_reason = os.getenv("OF_GATE_EXPORTERS_SMOKE_BLOCK_REASON", "of_gate_exporters_smoke")
    # Default: keep the block at least as long as dedup cooldown.
    block_ttl_s = int(os.getenv("OF_GATE_EXPORTERS_SMOKE_BLOCK_TTL_S", str(max(3600, cooldown_s))))

def run_atr_policy_bootstrap_audit() -> bool:
    """Audit-only consistency bootstrap from SQL snapshots."""
    env = os.environ.copy()
    env["ATR_POLICY_BOOTSTRAP_MODE"] = "audit_only"
    # Execute the python script in a subprocess
    timeout_s = 300
    try:
        subprocess.run(
            [sys.executable, "-m", "services.atr_policy_bootstrap_service"],
            cwd="/app",
            capture_output=True,
            text=True,
            timeout=timeout_s,
            env=env,
        )
    except subprocess.TimeoutExpired:
        logger.error(f"atr_policy_bootstrap_service audit timeout after {timeout_s}s")
        return False
    except Exception as e:
        logger.error(f"atr_policy_bootstrap_service audit error: {e}")
        return False
    return True

def run_atr_policy_state_drift_check() -> bool:
    """Check Redis/SQL drift and optionally repair Redis from SQL."""
    try:
        subprocess.run(
            [sys.executable, "-m", "services.atr_policy_state_consistency_checker"],
            cwd="/app",
            capture_output=True,
            text=True,
            timeout=300,
            env=os.environ.copy(),
        )
    except subprocess.TimeoutExpired:
        logger.error("atr_policy_state_consistency_checker timeout after 300s")
        return False
    except Exception as e:
        logger.error(f"atr_policy_state_consistency_checker error: {e}")
        return False
    return True


def run_atr_policy_full_recovery_audit() -> bool:
    """Dry-run / audit full PostgreSQL-backed policy recovery."""
    env = os.environ.copy()
    env["ATR_POLICY_FULL_RECOVERY_MODE"] = "audit_only"
    try:
        subprocess.run(
            [sys.executable, "-m", "services.atr_policy_full_recovery_service"],
            cwd="/app",
            capture_output=True,
            text=True,
            timeout=300,
            env=env,
        )
    except subprocess.TimeoutExpired:
        logger.error("atr_policy_full_recovery_service audit timeout after 300s")
        return False
    except Exception as e:
        logger.error(f"atr_policy_full_recovery_service audit error: {e}")
        return False
    return True

def run_atr_policy_restore_cert_audit() -> bool:
    """Continuous restore certification in audit-only mode."""
    env = os.environ.copy()
    env["ATR_POLICY_DRILL_MODE"] = "audit_only"
    try:
        subprocess.run(
            [sys.executable, "-m", "services.atr_policy_recovery_drill_runner"],
            cwd="/app",
            capture_output=True,
            text=True,
            timeout=300,
            env=env,
        )
    except subprocess.TimeoutExpired:
        logger.error("atr_policy_recovery_drill_runner audit timeout after 300s")
        return False
    except Exception as e:
        logger.error(f"atr_policy_recovery_drill_runner audit error: {e}")
        return False
    return True

def run_atr_policy_analytics_daily() -> bool:
    return run_tool(module="services.atr_policy_analytics_daily_service", timeout=300)

def run_atr_policy_analytics_tg_digest() -> bool:
    return run_tool(module="services.atr_policy_analytics_telegram_digest", timeout=300)


def run_atr_policy_restore_cert_execute() -> bool:
    """Bounded execute restore certification on synthetic cohort."""
    env = os.environ.copy()
    env["ATR_POLICY_DRILL_MODE"] = "bounded_execute"
    try:
        subprocess.run(
            [sys.executable, "-m", "services.atr_policy_recovery_drill_runner"],
            cwd="/app",
            capture_output=True,
            text=True,
            timeout=300,
            env=env,
        )
    except subprocess.TimeoutExpired:
        logger.error("atr_policy_recovery_drill_runner execute timeout after 300s")
        return False
    except Exception as e:
        logger.error(f"atr_policy_recovery_drill_runner execute error: {e}")
        return False
    return True



    module = os.getenv(
        "OF_GATE_EXPORTERS_SMOKE_MODULE",
        "orderflow_services.of_gate_exporters_smoke_p111",
    ),

    rc, stdout, stderr = run_tool_rc(module, timeout=timeout_s)
    if rc == 0:
        if block_auto_apply:
            _clear_auto_apply_block_if_owned(block_reason, owner="of_gate_exporters_smoke")
        logger.info("OF-Gate exporters smoke-check: OK")
        return True

    blob = ((stdout or "").strip() + "\n" + (stderr or "").strip()).strip()
    payload = {}
    for line in reversed((stdout or "").splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                payload = json.loads(line)
                break
            except Exception:
                pass

    failed = payload.get("failed") if isinstance(payload, dict) else None
    failed_names = []
    if isinstance(failed, list):
        for f in failed[:6]:
            with contextlib.suppress(Exception):
                failed_names.append(str(f.get("name") or f.get("target") or "?")[:64])

    signature = f"rc={rc}|failed={','.join(sorted(failed_names))}"

    if block_auto_apply:
        _set_auto_apply_block(
            block_reason,
            meta={
                "owner": "of_gate_exporters_smoke",
                "rc": int(rc),
                "failed": list(failed_names),
                "sig": signature,
                "module": str(module),
            },
            ttl_s=block_ttl_s,
        ),

    if dedup_enable and not _dedup_allow(signature, cooldown_s=cooldown_s, prefix=dedup_prefix):
        logger.warning(
            f"OF-Gate exporters smoke-check: suppressed duplicate alert (cooldown {cooldown_s}s). sig={signature}"
        ),
        return False

    head = ""
    for line in blob.splitlines():
        line = (line or "").strip()
        if line:
            head = line
            break

    msg = f"OF_GATE_EXPORTERS_SMOKE_P111 rc={rc} failed={failed_names} :: {head[:700]}"
    sid = "of_gate_exporters_smoke:" + hashlib.sha1(signature.encode("utf-8")).hexdigest()[:16]
    sev = "crit" if rc == 2 else "page"
    _notify_stream(msg, severity=sev, sid=sid)
    return False

def run_of_inputs_exporters_smoke_p107() -> bool:
    """Smoke-check OFInputs exporters wiring (P107).

    Checks HTTP /metrics endpoints for a small set of exporters and alerts on failures.

    Exit semantics:
      - module exit=0: OK
      - module exit=2: ALERT (actionable failure, notify)
      - other: ERROR (notify)

    P109 fail-closed: if smoke-check fails, sets a global auto-apply block in Redis;
    cleared automatically when the check recovers (only if owner matches, to avoid
    clobbering manual blocks).
    """
    enabled = os.getenv("ENABLE_OF_INPUTS_EXPORTERS_SMOKE_P107", "1")
    if str(enabled).lower() not in ("1", "true", "yes", "on"):
        return True

    timeout_s = int(os.getenv("OF_INPUTS_EXPORTERS_SMOKE_TIMEOUT_S", "30"))
    cooldown_s = int(os.getenv("OF_INPUTS_EXPORTERS_SMOKE_COOLDOWN_S", str(6 * 3600)))
    dedup_enable = os.getenv("OF_INPUTS_EXPORTERS_SMOKE_DEDUP", "1").lower() in ("1", "true", "yes", "on")
    dedup_prefix = os.getenv("OF_INPUTS_EXPORTERS_SMOKE_DEDUP_PREFIX", "dedup:alert:of_inputs_exporters:")

    # Fail-closed: if wiring/monitoring is broken, block auto-apply (optional).
    block_auto_apply = os.getenv("OF_INPUTS_EXPORTERS_SMOKE_BLOCK_AUTO_APPLY", "1").lower() in ("1", "true", "yes", "on")
    block_reason = os.getenv("OF_INPUTS_EXPORTERS_SMOKE_BLOCK_REASON", "of_inputs_exporters_smoke")
    # Default: keep the block at least as long as dedup cooldown.
    block_ttl_s = int(os.getenv("OF_INPUTS_EXPORTERS_SMOKE_BLOCK_TTL_S", str(max(3600, cooldown_s))))

    module = os.getenv(
        "OF_INPUTS_EXPORTERS_SMOKE_MODULE",
        "orderflow_services.of_inputs_exporters_smoke_p107",
    ),

    rc, stdout, stderr = run_tool_rc(module, timeout=timeout_s)  # type: ignore
    if rc == 0:  # type: ignore
        if block_auto_apply:
            _clear_auto_apply_block_if_owned(block_reason, owner="of_inputs_exporters_smoke")
        logger.info("OFInputs exporters smoke-check: OK")
        return True

    # Parse output to extract failed exporter names
    blob = ((stdout or "").strip() + "\n" + (stderr or "").strip()).strip()
    failed_names: list = []
    for line in blob.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("{") and line.endswith("}"):
            try:
                d = json.loads(line)
                if isinstance(d, dict) and isinstance(d.get("failed"), list):
                    failed_names = list(d["failed"])
                    break
            except Exception:
                pass

    signature = f"rc={rc}|failed={','.join(sorted(failed_names))}"

    if block_auto_apply:
        _set_auto_apply_block(
            block_reason,
            meta={
                "owner": "of_inputs_exporters_smoke",
                "rc": int(rc),
                "failed": list(failed_names),
                "sig": signature,
                "module": str(module),
            },
            ttl_s=block_ttl_s,
        ),

    if dedup_enable and not _dedup_allow(signature, cooldown_s=cooldown_s, prefix=dedup_prefix):
        logger.warning(
            f"OFInputs exporters smoke-check: suppressed duplicate alert (cooldown {cooldown_s}s). sig={signature}"
        ),
        return False

    head = ""
    for line in blob.splitlines():
        line = line.strip()
        if line:
            head = line
            break
    head = (head[:200] + "…") if len(head) > 200 else head

    text = (
        f"OF_INPUTS_EXPORTERS_SMOKE_P107 rc={rc} "
        f"failed={failed_names} :: {head}"
    ),
    sid = "of_inputs_exporters_smoke:" + hashlib.sha1(signature.encode("utf-8")).hexdigest()[:16]
    sev = "crit" if rc == 2 else "page"
    _notify_stream(text, severity=sev, sid=sid, source="of_inputs_exporters_smoke_p107")  # type: ignore
    return False  # type: ignore


def run_feature_registry_contract_smoke_check() -> bool:
    """Run Feature Registry contract smoke-check (P94).

    Behavior:
      - exit=0 -> ok
      - exit=2 -> alert (pins missing or hash mismatch)
      - other  -> error

    Dedup/cooldown:
      - suppress identical alerts for N seconds (default 6h)
        by signature = reason + expected/current hashes (shortened).
    """
    enabled = os.getenv("ENABLE_FEATURE_REGISTRY_CONTRACT_SMOKE")
    if enabled is None:
        # default: enable if edge-stack ML is enabled, otherwise off
        enabled = "1" if os.getenv("ENABLE_NIGHTLY_EDGE_STACK_TRAIN", "0") == "1" else "0"
    if str(enabled).lower() not in ("1", "true", "yes", "on"):
        return True

    py = os.getenv("PYTHON", sys.executable)

    script = os.getenv("FEATURE_REGISTRY_CONTRACT_SMOKE_SCRIPT")
    if not script:
        candidates = [
            "/app/orderflow_services/feature_registry_contract_check_v1.py",
            "/app/tick_flow_full/orderflow_services/feature_registry_contract_check_v1.py",
        ]
        for c in candidates:
            if os.path.exists(c):
                script = c
                break

    if script:
        cmd = [py, script]
    else:
        module = os.getenv(
            "FEATURE_REGISTRY_CONTRACT_SMOKE_MODULE",
            "orderflow_services.feature_registry_contract_check_v1",
        ),
        cmd = [py, "-m", module]

    timeout_s = float(os.getenv("FEATURE_REGISTRY_CONTRACT_SMOKE_TIMEOUT_S", "120"))
    cooldown_s = int(os.getenv("FEATURE_REGISTRY_CONTRACT_SMOKE_COOLDOWN_S", "21600"))  # 6h

    try:
        p = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_s,
            env=os.environ.copy(),
        )
        d = _parse_smoke_output(p.stdout, p.stderr) or {}
        rc = int(p.returncode)

        if rc == 0:
            logger.info("Feature Registry contract smoke-check: OK")
            return True

        if rc == 2:
            reason = (d.get("reason") or "alert")
            cur = (d.get("current") or {}) if isinstance(d.get("current"), dict) else {}
            exp_schema = (d.get("expected_schema_hash") or "")
            exp_cols = (d.get("expected_feature_cols_hash") or "")
            cur_schema = str(cur.get("schema_hash") or d.get("schema_hash") or "")
            cur_cols = str(cur.get("feature_cols_hash") or d.get("feature_cols_hash") or "")

            sig = (
                f"reason={reason}|exp_schema={exp_schema[:16]}|exp_cols={exp_cols[:16]}|"
                f"cur_schema={cur_schema[:16]}|cur_cols={cur_cols[:16]}"
            ),
            if _dedup_allow(sig, cooldown_s=cooldown_s, prefix="dedup:feature_registry_contract:"):  # type: ignore
                pin_key = str(d.get("pin_key") or os.getenv("FEATURE_REGISTRY_PIN_KEY", "cfg:feature_registry:edge_stack"))  # type: ignore
                msg = (
                    "[P94] Feature Registry contract ALERT\n"
                    f"reason={reason}\n"
                    f"pin_key={pin_key}\n"
                    f"expected schema_hash={exp_schema[:16]}…\n"
                    f"expected feature_cols_hash={exp_cols[:16]}…\n"
                    f"current  schema_hash={cur_schema[:16]}…\n"
                    f"current  feature_cols_hash={cur_cols[:16]}…\n"
                    "Action: rollback accidental change OR bump schema ver + seed pins."
                ),
                _notify_stream(msg, severity="crit", source="feature_registry_contract_smoke")  # type: ignore
            return False  # type: ignore

        raw = (d.get("raw") or "")[:600]
        msg = (
            "[P94] Feature Registry contract ERROR\n"
            f"rc={rc}\n"
            f"cmd={' '.join(cmd)}\n"
            f"raw={raw}"
        ),
        _notify_stream(msg, severity="warning", source="feature_registry_contract_smoke")  # type: ignore
        return False  # type: ignore

    except Exception as e:
        msg = f"[P94] Feature Registry contract EXCEPTION: {type(e).__name__}: {e}"
        _notify_stream(msg, severity="warning", source="feature_registry_contract_smoke")
        return False


def run_of_inputs_dlq_auto_replay() -> bool:
    """Run OFInputs DLQ auto replay (P97).

    Behavior:
      - exit=0 -> ok
      - exit=2 -> alert (replay failed)
      - other  -> error

    Guarded by ENABLE_OF_INPUTS_DLQ_AUTO_REPLAY=1.
    """
    enabled = os.getenv("ENABLE_OF_INPUTS_DLQ_AUTO_REPLAY", "0")
    if str(enabled).lower() not in ("1", "true", "yes", "on"):
        return True

    timeout_s = int(os.getenv("OF_INPUTS_DLQ_AUTO_REPLAY_TIMEOUT_S", "120"))
    cooldown_s = int(os.getenv("OF_INPUTS_DLQ_AUTO_REPLAY_COOLDOWN_S", str(6 * 3600)))
    dedup_enable = os.getenv("OF_INPUTS_DLQ_AUTO_REPLAY_DEDUP_ENABLE", "1") in ("1", "true", "yes", "on")
    prefix = os.getenv("OF_INPUTS_DLQ_AUTO_REPLAY_DEDUP_PREFIX", "dedup:alert:of_inputs_dlq_replay:")

    # Commit by default when enabled.
    env = os.environ.copy()
    if "OF_INPUTS_DLQ_COMMIT" not in env:
        env["OF_INPUTS_DLQ_COMMIT"] = os.getenv("OF_INPUTS_DLQ_AUTO_REPLAY_COMMIT", "1")

    # Script/module discovery
    script = os.getenv("OF_INPUTS_DLQ_AUTO_REPLAY_SCRIPT")
    if not script:
        candidates = [
            "/app/orderflow_services/of_inputs_dlq_fixed_replay_p97.py",
            "/app/tick_flow_full/orderflow_services/of_inputs_dlq_fixed_replay_p97.py",
        ]
        for c in candidates:
            if os.path.exists(c):
                script = c
                break

    module = os.getenv("OF_INPUTS_DLQ_AUTO_REPLAY_MODULE", "orderflow_services.of_inputs_dlq_fixed_replay_p97")

    try:
        if script:
            cmd = [sys.executable, script]
        else:
            cmd = [sys.executable, "-m", module]
        result = subprocess.run(
            cmd,
            cwd="/app",
            capture_output=True,
            text=True,
            timeout=timeout_s,
            env=env,
        )
    except subprocess.TimeoutExpired:
        text = f"OF_INPUTS_DLQ_REPLAY timeout after {timeout_s}s"
        _notify_stream(text, severity="crit", sid="of_inputs_dlq_replay:timeout", source="of_inputs_dlq_replay")
        return False
    except Exception as e:
        text = f"OF_INPUTS_DLQ_REPLAY error: {e}"
        _notify_stream(text, severity="crit", sid="of_inputs_dlq_replay:exception", source="of_inputs_dlq_replay")
        return False

    rc = int(result.returncode)
    stdout = result.stdout or ""
    stderr = result.stderr or ""

    if rc == 0:
        logger.info("OFInputs DLQ replay: OK")
        return True

    parsed = _parse_smoke_output(stdout, stderr)
    ok = parsed.get("ok")
    failed = parsed.get("failed")
    replayed = parsed.get("replayed")
    last_err = (parsed.get("last_err") or "")[:128]

    signature = f"rc={rc}|ok={ok}|failed={failed}|replayed={replayed}|err={last_err}"
    if dedup_enable and not _dedup_allow(signature, cooldown_s=cooldown_s, prefix=prefix):
        logger.warning(f"OFInputs DLQ replay: suppressed duplicate alert (cooldown {cooldown_s}s). sig={signature}")
        return False

    head = (parsed.get("raw") or "").strip().replace("\n", " | ")
    head = head[:700]
    text = f"OF_INPUTS_DLQ_REPLAY rc={rc} ok={ok} replayed={replayed} failed={failed} err={last_err} :: {head}"
    sid = "of_inputs_dlq_replay:" + hashlib.sha1(signature.encode("utf-8")).hexdigest()[:16]
    _notify_stream(text, severity="warning", sid=sid, source="of_inputs_dlq_replay")
    return False


def run_of_inputs_dlq_db_archive_p98() -> bool:
    """Run OFInputs DLQ+quarantine → Postgres/Timescale archiver (P98).

    Reads streams stream:dlq:of_inputs and quarantine:signals:of:inputs,
    inserts rows into of_inputs_dlq_events (idempotent, ON CONFLICT DO NOTHING),
    advances checkpoint in Redis, writes metrics hash for exporter.

    Guarded by ENABLE_OF_INPUTS_DLQ_DB_ARCHIVE_P98=1.
    Module: orderflow_services.of_inputs_dlq_archive_to_db_p98
    """
    enabled = os.getenv("ENABLE_OF_INPUTS_DLQ_DB_ARCHIVE_P98", "0")
    if str(enabled).lower() not in ("1", "true", "yes", "on"):
        return True

    timeout_s = int(os.getenv("OF_INPUTS_DLQ_DB_ARCHIVE_TIMEOUT_S", "300"))
    dedup_enable = os.getenv("OF_INPUTS_DLQ_DB_ARCHIVE_DEDUP_ENABLE", "1") in ("1", "true", "yes", "on")
    cooldown_s = int(os.getenv("OF_INPUTS_DLQ_DB_ARCHIVE_COOLDOWN_S", str(6 * 3600)))
    prefix = os.getenv("OF_INPUTS_DLQ_DB_ARCHIVE_DEDUP_PREFIX", "dedup:alert:of_inputs_dlq_db_archive:")

    # auto-migrate: one-time DDL on first run if set
    extra_args = ["--once"]
    if os.getenv("OF_INPUTS_DLQ_DB_ARCHIVE_AUTO_MIGRATE", "0") in ("1", "true", "yes", "on"):
        extra_args.append("--auto-migrate")

    # script/module discovery (mirrors P96/P97 pattern)
    script = os.getenv("OF_INPUTS_DLQ_DB_ARCHIVE_SCRIPT")
    if not script:
        candidates = [
            "/app/orderflow_services/of_inputs_dlq_archive_to_db_p98.py",
            "/app/tick_flow_full/orderflow_services/of_inputs_dlq_archive_to_db_p98.py",
        ]
        for c in candidates:
            if os.path.exists(c):
                script = c
                break

    module = os.getenv("OF_INPUTS_DLQ_DB_ARCHIVE_MODULE", "orderflow_services.of_inputs_dlq_archive_to_db_p98")

    try:
        if script:
            cmd = [sys.executable, script] + extra_args
        else:
            cmd = [sys.executable, "-m", module] + extra_args
        result = subprocess.run(
            cmd,
            cwd="/app",
            capture_output=True,
            text=True,
            timeout=timeout_s,
            env=os.environ.copy(),
        )
    except subprocess.TimeoutExpired:
        text = f"OF_INPUTS_DLQ_DB_ARCHIVE timeout after {timeout_s}s (P98)"
        _notify_stream(text, severity="crit", sid="of_inputs_dlq_db_archive:timeout", source="of_inputs_dlq_db_archive_p98")
        return False
    except Exception as e:
        text = f"OF_INPUTS_DLQ_DB_ARCHIVE error: {e} (P98)"
        _notify_stream(text, severity="crit", sid="of_inputs_dlq_db_archive:exception", source="of_inputs_dlq_db_archive_p98")
        return False

    rc = int(result.returncode)
    stdout = result.stdout or ""
    stderr = result.stderr or ""

    if rc == 0:
        logger.info("OFInputs DLQ DB archive P98: OK")
        return True

    parsed = _parse_smoke_output(stdout, stderr)
    inserted = parsed.get("inserted")
    head = (parsed.get("raw") or "").strip().replace("\n", " | ")[:700]
    signature = f"rc={rc}|inserted={inserted}|head={head[:128]}"

    if dedup_enable and not _dedup_allow(signature, cooldown_s=cooldown_s, prefix=prefix):
        logger.warning(f"OFInputs DLQ DB archive P98: suppressed duplicate alert (cooldown {cooldown_s}s). sig={signature}")
        return False

    text = f"OF_INPUTS_DLQ_DB_ARCHIVE_P98 rc={rc} inserted={inserted} :: {head}"
    sid = "of_inputs_dlq_db_archive:" + hashlib.sha1(signature.encode("utf-8")).hexdigest()[:16]
    _notify_stream(text, severity="warning", sid=sid, source="of_inputs_dlq_db_archive_p98")
    return False



def run_of_inputs_dlq_db_drilldown_p99() -> bool:
    """Run OFInputs DLQ DB drilldown (P99).

    Queries of_inputs_dlq_events table, prints top reasons and last event age.
    Optionally notifies Redis stream if ENABLE_OF_INPUTS_DLQ_DB_NOTIFY=1.

    Guarded by ENABLE_OF_INPUTS_DLQ_DB_DRILLDOWN=1 (default: disabled; requires TRADES_DB_DSN).
    """
    enabled = os.getenv("ENABLE_OF_INPUTS_DLQ_DB_DRILLDOWN", "0")
    if str(enabled).lower() not in ("1", "true", "yes", "on"):
        return True

    dsn = (
        os.getenv("ARCHIVER_PG_DSN")
        or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("TRADES_DB_DSN"))
        or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL"))
        or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("PG_DSN"))
        or ""
    ),
    if not dsn:
        logger.warning("of_inputs_dlq_db_drilldown_p99: no DB DSN configured, skipping")
        return True

    notify = os.getenv("ENABLE_OF_INPUTS_DLQ_DB_NOTIFY", "0") in ("1", "true", "yes", "on")
    lookback_h = int(os.getenv("OF_INPUTS_DLQ_DB_DRILLDOWN_LOOKBACK_H", "1"))
    top_n = int(os.getenv("OF_INPUTS_DLQ_DB_DRILLDOWN_TOP", "10"))
    timeout_s = int(os.getenv("OF_INPUTS_DLQ_DB_DRILLDOWN_TIMEOUT_S", "90"))

    module = os.getenv(
        "OF_INPUTS_DLQ_DB_DRILLDOWN_MODULE",
        "orderflow_services.of_inputs_dlq_db_drilldown_p99",
    ),
    args = ["--lookback-h", str(lookback_h), "--top", str(top_n)]
    if notify:
        args.append("--notify")

    try:
        cmd = [sys.executable, "-m", module] + args
        result = subprocess.run(
            cmd,
            cwd="/app",
            capture_output=True,
            text=True,
            timeout=timeout_s,
            env=os.environ.copy(),
        )
        if result.returncode == 0:
            if result.stdout:
                logger.info(f"of_inputs_dlq_db_drilldown_p99:\n{result.stdout.strip()[:2000]}")
            return True
        else:
            logger.warning(
                f"of_inputs_dlq_db_drilldown_p99 rc={result.returncode} stderr={result.stderr[:300]}"
            ),
            return False
    except subprocess.TimeoutExpired:
        logger.error(f"of_inputs_dlq_db_drilldown_p99 timeout after {timeout_s}s")
        return False
    except Exception as e:
        logger.error(f"of_inputs_dlq_db_drilldown_p99 error: {e}")
        return False


def run_orchestration_composite_preflight_history_rollup() -> bool:
    """P5.6: Incremental Redis-side rollup for orchestration composite preflight history.

    This job advances a Redis Stream cursor and updates bounded hourly/daily bucket
    counters in Redis, so later exporters can build 24h/7d/30d SLO views without
    rescanning the full stream window.
    """
    enabled = os.getenv("ENABLE_ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_ROLLUP", "0")
    if str(enabled).lower() not in ("1", "true", "yes", "on"):
        return True

    module = os.getenv(
        "ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_ROLLUP_MODULE",
        "orderflow_services.orchestration_composite_preflight_history_rollup_v1",
    ),
    timeout_s = int(os.getenv("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_ROLLUP_TIMEOUT_S", "45"))
    env = os.environ.copy()
    try:
        result = subprocess.run(
            [sys.executable, "-m", module],  # type: ignore
            cwd="/app",  # type: ignore
            capture_output=True,
            text=True,
            timeout=timeout_s,
            env=env,
        )
    except subprocess.TimeoutExpired:
        logger.warning(f"orchestration_preflight_history_rollup: timeout after {timeout_s}s")
        return False
    except Exception as e:
        logger.warning(f"orchestration_preflight_history_rollup: error launching subprocess: {e}")
        return False

    if int(result.returncode) == 0:
        if result.stdout:
            logger.info(f"orchestration_preflight_history_rollup: {result.stdout.strip()[:800]}")
        return True

    logger.warning(
        "orchestration_preflight_history_rollup rc=%s stdout=%s stderr=%s",
        result.returncode,
        (result.stdout or "").strip()[:300],
        (result.stderr or "").strip()[:300],
    ),
    return False


def run_orchestration_composite_preflight_history_textfile_exporter() -> bool:
    """P5.6: Textfile exporter for pre-aggregated orchestration preflight history buckets."""
    enabled = os.getenv("ENABLE_ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_TEXTFILE_EXPORTER", "0")
    if str(enabled).lower() not in ("1", "true", "yes", "on"):
        return True

    module = os.getenv(
        "ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_TEXTFILE_EXPORTER_MODULE",
        "orderflow_services.orchestration_composite_preflight_history_textfile_exporter_v1",
    ),
    timeout_s = int(os.getenv("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_TEXTFILE_EXPORTER_TIMEOUT_S", "45"))
    env = os.environ.copy()
    env.setdefault(
        "ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_EXPORT_PATH",
        "/var/lib/node_exporter/textfile_collector/orchestration_composite_preflight_history_rollup.prom",
    ),
    try:
        result = subprocess.run(
            [sys.executable, "-m", module],  # type: ignore
            cwd="/app",  # type: ignore
            capture_output=True,
            text=True,
            timeout=timeout_s,
            env=env,
        )
    except subprocess.TimeoutExpired:
        logger.warning(f"orchestration_preflight_history_textfile_exporter: timeout after {timeout_s}s")
        return False
    except Exception as e:
        logger.warning(f"orchestration_preflight_history_textfile_exporter: error launching subprocess: {e}")
        return False

    if int(result.returncode) == 0:
        logger.info("orchestration_preflight_history_textfile_exporter: OK")
        return True

    logger.warning(
        "orchestration_preflight_history_textfile_exporter rc=%s stdout=%s stderr=%s",
        result.returncode,
        (result.stdout or "").strip()[:300],
        (result.stderr or "").strip()[:300],
    ),
    return False


def run_orchestration_composite_preflight_history_consistency_check() -> bool:
    """P5.7: Check Redis rollup bucket consistency against the source stream.

    The checker scans a bounded time range from the source stream, compares it with
    the Redis-side hourly/daily buckets created by P5.6, writes a JSON report and a
    textfile for node_exporter, and returns non-zero when drift is detected.

    Gate: ENABLE_ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_CONSISTENCY_CHECK=1
    Default window: 168 h (7 days), configurable via
    ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_CONSISTENCY_WINDOW_HOURS.
    """
    enabled = os.getenv("ENABLE_ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_CONSISTENCY_CHECK", "0")
    if str(enabled).lower() not in ("1", "true", "yes", "on"):
        return True

    module = os.getenv(
        "ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_CONSISTENCY_MODULE",
        "orderflow_services.orchestration_composite_preflight_history_consistency_v1",
    ),
    timeout_s = int(os.getenv("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_CONSISTENCY_TIMEOUT_S", "120"))
    env = os.environ.copy()
    env.setdefault("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_CONSISTENCY_MODE", "check")
    env.setdefault(
        "ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_CONSISTENCY_EXPORT_PATH",
        "/var/lib/node_exporter/textfile_collector/orchestration_composite_preflight_history_consistency.prom",
    ),
    try:
        result = subprocess.run(
            [sys.executable, "-m", module],  # type: ignore
            cwd="/app",  # type: ignore
            capture_output=True,
            text=True,
            timeout=timeout_s,
            env=env,
        )
    except subprocess.TimeoutExpired:
        logger.warning(f"orchestration_preflight_history_consistency: timeout after {timeout_s}s")
        return False
    except Exception as e:
        logger.warning(f"orchestration_preflight_history_consistency: error launching subprocess: {e}")
        return False

    if int(result.returncode) == 0:
        if result.stdout:
            logger.info(f"orchestration_preflight_history_consistency: {result.stdout.strip()[:1000]}")
        return True

    logger.warning(
        "orchestration_preflight_history_consistency rc=%s stdout=%s stderr=%s",
        result.returncode,
        (result.stdout or "").strip()[:400],
        (result.stderr or "").strip()[:400],
    ),
    return False


def run_feature_denylist_proposal_exporter() -> bool:
    """P104: Hourly exporter for feature-denylist proposals → node_exporter textfile collector.

    Writes /var/lib/node_exporter/textfile_collector/feature_denylist.prom
    (or FEATURE_DENYLIST_EXPORT_PATH) with low-cardinality Prometheus gauges:
      feature_denylist_proposals_total{status="pending_ab|ab_done|ab_failed|approved"}
      feature_denylist_oldest_pending_age_seconds
      feature_denylist_ab_runner_* (from Redis hash, if available)

    Schedule: hourly at :46 (after nightly AB-runner at 06:45 so metrics are fresh).

    Guards:
      - ENABLE_FEATURE_DENYLIST_EXPORTER=1 required (default: disabled for staged rollout)
      - Module is resolved via FEATURE_DENYLIST_EXPORTER_MODULE env or default path;
        if module is not yet deployed (ImportError/ModuleNotFoundError) → no-op to allow
        phased rollout without breaking the container.
    """
    enabled = os.getenv("ENABLE_FEATURE_DENYLIST_EXPORTER", "0")
    if str(enabled).lower() not in ("1", "true", "yes", "on"):
        return True

    module = os.getenv(
        "FEATURE_DENYLIST_EXPORTER_MODULE",
        "ml_analysis.tools.feature_denylist_proposal_exporter_v1",
    ),
    timeout_s = int(os.getenv("FEATURE_DENYLIST_EXPORTER_TIMEOUT_S", "30"))

    # Pass required env vars to subprocess (inherited from os.environ, but add defaults)
    env = os.environ.copy()
    if "FEATURE_DENYLIST_EXPORT_PATH" not in env:
        env["FEATURE_DENYLIST_EXPORT_PATH"] = "/var/lib/node_exporter/textfile_collector/feature_denylist.prom"
    if "FEATURE_DENYLIST_PROPOSALS_DIR" not in env:
        # Default: look adjacent to feature-selection run dir
        run_dir = os.getenv("FEATURE_SELECTION_RUN_DIR", "/var/lib/trade/feature_selection")
        env.setdefault("FEATURE_DENYLIST_PROPOSALS_DIR", os.path.join(run_dir, "proposals"))

    try:
        cmd = [sys.executable, "-m", module]
        result = subprocess.run(
            cmd,
            cwd="/app",
            capture_output=True,
            text=True,
            timeout=timeout_s,
            env=env,
        )
    except subprocess.TimeoutExpired:
        logger.warning(f"feature_denylist_exporter: timeout after {timeout_s}s (module={module})")
        return False
    except Exception as e:
        logger.warning(f"feature_denylist_exporter: error launching subprocess: {e}")
        return False

    rc = int(result.returncode)
    if rc == 0:
        logger.info("feature_denylist_exporter: OK")
        return True

    stderr = (result.stderr or "").strip()
    stdout = (result.stdout or "").strip()
    # Distinguish between module-not-found (phased rollout no-op) and real error
    if "ModuleNotFoundError" in stderr or "No module named" in stderr:
        # Module not yet deployed — safe no-op, warn but do not alert
        logger.warning(
            f"feature_denylist_exporter: module not found ({module}), skipping (phased rollout). stderr={stderr[:300]}"
        ),
        return True

    logger.warning(
        f"feature_denylist_exporter: rc={rc} stderr={stderr[:300]} stdout={stdout[:300]}"
    ),
    return False


def run_tool(module: str = None, args: list[str] = None, timeout: int = 3600, env_override: dict = None, **kwargs) -> bool:  # type: ignore
    """Run python module in a subprocess.

    Compatibility:
      - accepts legacy keyword aliases: tool_path, timeout_s
    """
    if module is None:
        module = kwargs.get('tool_path') or kwargs.get('tool')
    if module is None:
        raise ValueError('module is required')
    if 'timeout_s' in kwargs and kwargs.get('timeout_s') is not None:
        with contextlib.suppress(Exception):
            timeout = int(kwargs.get('timeout_s'))  # type: ignore
    """Generic tool runner."""
    try:
        cmd = [sys.executable, "-m", module]
        if args:
            cmd.extend(args)

        env = os.environ.copy()
        if env_override:
            env.update(env_override)

        logger.info(f"Running {module} {' '.join(args or [])}...")
        result = subprocess.run(
            cmd,
            cwd="/app",
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
        if result.returncode == 0:
            logger.info(f"{module} completed successfully")
            if result.stdout:
                logger.debug(f"Output: {result.stdout}")
            return True
        else:
            logger.warning(f"{module} failed: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        logger.error(f"{module} timed out after {timeout}s")
        return False
    except Exception as e:
        logger.error(f"{module} error: {e}")
        return False


def run_tool_rc(
    module: str = None,  # type: ignore
    args: list[str] = None,  # type: ignore
    timeout: int = 3600,  # type: ignore
    env_override: dict = None,  # type: ignore
    **kwargs,  # type: ignore
) -> tuple[int, str, str]:
    """Run python module in a subprocess and return (returncode, stdout, stderr).

    Compatibility:
      - accepts legacy keyword aliases: tool_path, timeout_s
    """
    if module is None:
        module = kwargs.get('tool_path') or kwargs.get('tool')
    if module is None:
        raise ValueError('module is required')
    if 'timeout_s' in kwargs and kwargs.get('timeout_s') is not None:
        with contextlib.suppress(Exception):
            timeout = int(kwargs.get('timeout_s'))  # type: ignore
  # type: ignore
    try:
        cmd = [sys.executable, "-m", module]
        if args:
            cmd.extend(args)

        env = os.environ.copy()
        if env_override:
            env.update(env_override)

        logger.info(f"Running {module} {' '.join(args or [])}...")
        result = subprocess.run(
            cmd,
            cwd="/app",
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
        if result.stdout:
            logger.debug(f"Output: {result.stdout.strip()}")
        if result.stderr:
            logger.debug(f"Stderr: {result.stderr.strip()}")
        return int(result.returncode or 0), (result.stdout or ""), (result.stderr or "")
    except subprocess.TimeoutExpired:
        logger.error(f"{module} timed out after {timeout}s")
        return 124, "", f"timeout after {timeout}s"
    except Exception as e:
        logger.error(f"{module} error: {e}")
        return 125, "", str(e)


def _best_effort_notify_telegram(text: str, source: str = "of_gate_contract_smoke") -> None:
    """Best-effort alert: (1) push to Redis notify stream, (2) direct Telegram if configured.

    This is a thin convenience wrapper around _notify_stream for API compatibility.
    """
    msg = (text or "").strip()
    if not msg:
        return
    # Delegate to _notify_stream (already handles Redis + TG internally).
    with contextlib.suppress(Exception):
        _notify_stream(msg, severity="crit", sid=source)


def _format_of_gate_contract_smoke_msg(stdout: str, stderr: str, rc: int) -> str:
    """Format alert message from checker output, preferring JSON payload if present."""
    # Try to extract JSON payload from stdout.
    payload: dict[str, Any] = {}
    for line in reversed((stdout or "").splitlines()):
        if "{" in line and "}" in line:
            try:
                j = line[line.find("{") : line.rfind("}") + 1]
                payload = json.loads(j)
                break
            except Exception:
                continue

    if payload:
        n = payload.get("n")
        bad = payload.get("bad")
        bad_share = payload.get("bad_share")
        stream = payload.get("stream")
        top = payload.get("top_bad_reasons", []) or []
        top_s = ", ".join(
            [f"{it.get('k')}={it.get('n')}" for it in top[:5] if isinstance(it, dict)]
        )[:220]
        return (
            f"OF_GATE_CONTRACT_SMOKE_ALERT rc={rc} bad_share={bad_share} n={n} bad={bad} "  # type: ignore
            f"stream={stream} top=[{top_s}]"
        ),

    # Fallback: include stderr tail.
    err = (stderr or "").strip().splitlines()[-1:]  # last line only
    err_s = err[0] if err else ""
    return f"OF_GATE_CONTRACT_SMOKE_ALERT rc={rc} stderr={err_s[:300]}"



def run_config_drift_monitor() -> bool:
    """Run config drift monitor."""
    return run_tool("tools.config_drift_monitor", timeout=300)



def run_weekly_bench() -> bool:
    """Run weekly latency bench."""
    try:
        logger.info("Running weekly latency bench...")

        # Create bench directory
        bench_dir = "/var/lib/trade/of_reports/out/bench"
        os.makedirs(bench_dir, exist_ok=True)

        baseline_inputs = os.getenv("BASELINE_INPUTS", "")
        if not baseline_inputs:
            logger.error("BASELINE_INPUTS not set")
            return False

        bench_json = f"{bench_dir}/bench.json"

        # Run benchmark
        logger.info(f"Running benchmark on {baseline_inputs}...")
        if not run_tool("tools.bench_replay_throughput", ["--inputs", baseline_inputs, "--out", bench_json], timeout=1800):
            return False

        logger.info("Benchmark completed, checking latency budget...")
        return run_tool("tools.assert_latency_budget", ["--bench-json", bench_json], timeout=60)

    except Exception as e:
        logger.error(f"Weekly bench error: {e}")
        return False


def run_nightly_regress_safe() -> bool:
    """Run nightly regression test with emergency disable."""
    return run_tool("tools.nightly_regress_engine_replay_safe", timeout=3600)


def run_code_audit() -> bool:
    """Run code integrity audit."""
    return run_tool(
        "tools.audit_code_integrity",  # type: ignore
        ["--root", ".", "--out", "/var/lib/trade/of_reports/out/code_audit.json", "--fail-on-dup", "1"],
        timeout=600
    ),

def run_archive_signals_of_inputs() -> bool:
    """Archive signals:of:inputs → NDJSON."""
    return run_tool("tools.archive_signals_of_inputs_v1", timeout=900)


def run_archive_trades_closed() -> bool:
    """Archive trades:closed → NDJSON."""
    return run_tool("tools.archive_trades_closed_v1", timeout=900)


def run_horizon_profile_bootstrap() -> bool:
    """Build horizon hold/decay bootstrap profiles from trades_closed + trades_closed_p0."""
    enabled = os.getenv("HORIZON_PROFILE_BOOTSTRAP_ENABLED", "1")
    if str(enabled).lower() not in ("1", "true", "yes", "on"):
        return True
    return run_tool("services.horizon_profile_bootstrap_service", timeout=900)


def run_atr_policy_bootstrap() -> bool:
    """Run ATR Policy Bootstrap to heal Redis from PostgreSQL."""
    return run_tool("services.atr_policy_bootstrap_service", timeout=300)

def run_live_surface_ab() -> bool:
    """Aggregate canary live-surface vs baseline post-trade A/B."""
    return run_tool("services.live_surface_ab_service", timeout=900)


def run_trailing_surface_ab() -> bool:
    """Aggregate trailing-surface vs baseline post-trade A/B."""
    return run_tool("services.trailing_surface_ab_service", timeout=900)



def run_atr_promotion_policy() -> bool:
    """Build cohort-level stop/ttl + trailing promotion suggestions."""
    return run_tool("services.atr_promotion_policy_service", timeout=900)


def run_atr_policy_reconcile() -> bool:
    """Apply approved/revoked ATR policy decisions."""
    return run_tool("services.atr_policy_reconcile_service", timeout=300)


def run_atr_policy_telegram_digest() -> bool:
    """Send nightly Telegram digest for ATR policy ops."""
    return run_tool("services.atr_policy_telegram_summary_service", timeout=300)


def run_atr_policy_telegram_pack() -> bool:
    """Send nightly one-tap ATR ops pack."""
    return run_tool("services.atr_policy_telegram_pack_service", timeout=300)


def run_atr_policy_sre_digest() -> bool:
    """Send policy workflow SRE health digest to Telegram (Phase 3.7)."""
    return run_tool("services.atr_policy_telegram_sre_digest", timeout=300)

def run_atr_policy_regime_stress_state() -> bool:
    """Evaluate and set regime/stress limits (Phase 5.7)."""
    return run_tool("services.atr_policy_regime_stress_service", timeout=120)

def run_atr_policy_regime_stress_tg_digest() -> bool:
    """Send Telegram digest for regime/stress changes (Phase 5.7)."""
    return run_tool("services.atr_policy_regime_stress_telegram_digest", timeout=120)

def run_atr_policy_portfolio_tg_digest() -> bool:
    """Send Portfolio Gate concentration digest to Telegram."""
    return run_tool("services.atr_policy_portfolio_telegram_digest", timeout=300)

def run_atr_policy_factor_cluster() -> bool:
    """Build factor clusters and update Redis configuration."""
    return run_tool("services.atr_policy_factor_cluster_service", timeout=300)


def run_ensemble_weight_calibration() -> bool:
    """Hourly ensemble weight calibration: compute per-source Sharpe → Redis weights."""
    enabled = os.getenv("ENABLE_ENSEMBLE_WEIGHT_CALIBRATION", "1")
    if str(enabled).lower() not in ("1", "true", "yes", "on"):
        return True
    return run_tool("services.ensemble_weight_calibrator", timeout=300)


def run_archive_inventory_prune() -> bool:
    """Retention + manifest for all archives."""
    return run_tool("tools.archive_inventory_prune_multi_v1", timeout=600)




def _parse_hhmm(s: str, default_h: int, default_m: int) -> tuple[int, int]:
    """
    Parse a "HH:MM" string into (hour, minute) integers.
    Falls back to (default_h, default_m) on any parse error or out-of-range input.
    Used for: OF_GATE_ROLLUPS_REFRESH_SAFE_START_UTC / _SAFE_END_UTC env vars.
    """
    try:
        s = (s or '').strip()
        if not s:
            return default_h, default_m
        hh, mm = s.split(':', 1)
        h = int(hh)
        m = int(mm)
        if h < 0 or h > 23:
            return default_h, default_m
        if m < 0 or m > 59:
            return default_h, default_m
        return h, m
    except Exception:
        return default_h, default_m


def _in_safe_window_utc(now: datetime, start_h: int, start_m: int, end_h: int, end_m: int) -> bool:
    """
    Return True when ``now`` (UTC) falls inside [start, end) window.
    Inclusive start, exclusive end.  Handles midnight wrap-around.
    When start==end the window is considered "always open".
    """
    # inclusive start, exclusive end
    t = now.hour * 60 + now.minute
    s = start_h * 60 + start_m
    e = end_h * 60 + end_m
    if s == e:
        return True  # treat as always allowed
    if s < e:
        return s <= t < e
    # wrap over midnight
    return t >= s or t < e


def _try_acquire_rollups_lock(timeout_s: int) -> bool:
    """Best-effort distributed lock for rollups refresh.

    Prevents duplicate parallel runs when multiple timer workers are deployed.
    Acquisition order:
      1) Redis SET NX EX (if redis lib available and REDIS_URL set)
      2) File lock in /tmp (container-local fallback)

    Returns True if lock acquired (caller may proceed), False if lock is held.
    On any unexpected error, returns True (fail-open: prefer running over blocking).
    """
    ttl = max(60, int(timeout_s) + 600)  # lock TTL = timeout + 10min buffer
    key = os.getenv('OF_GATE_ROLLUPS_REFRESH_LOCK_KEY', 'lock:of_gate_rollups_refresh')
    redis_url = os.getenv('REDIS_URL', '')

    # 1) Redis distributed lock (preferred in multi-replica deployments)
    try:
        import redis as _redis  # type: ignore
        if redis_url:
            r = _redis.Redis.from_url(redis_url, decode_responses=True)
            ok = r.set(key, str(int(time.time())), nx=True, ex=ttl)
            return bool(ok)
    except Exception:
        pass  # fall through to file lock

    # 2) File lock (single-container fallback)
    try:
        lock_path = os.getenv('OF_GATE_ROLLUPS_REFRESH_LOCK_FILE', '/tmp/of_gate_rollups_refresh.lock')
        now = time.time()
        try:
            st = os.stat(lock_path)
            if now - st.st_mtime < ttl:
                return False  # lock file is recent — another instance holds it
        except FileNotFoundError:
            pass
        with open(lock_path, 'w', encoding='utf-8') as f:
            f.write(str(int(now)))
        return True
    except Exception:
        return True  # fail-open on unexpected errors


def _release_rollups_lock() -> None:
    """Release the rollups refresh lock (Redis key + local lock file)."""
    key = os.getenv('OF_GATE_ROLLUPS_REFRESH_LOCK_KEY', 'lock:of_gate_rollups_refresh')
    redis_url = os.getenv('REDIS_URL', '')
    # Release Redis lock
    try:
        import redis as _redis  # type: ignore
        if redis_url:
            r = _redis.Redis.from_url(redis_url, decode_responses=True)
            r.delete(key)
    except Exception:
        pass
    # Release file lock
    try:
        lock_path = os.getenv('OF_GATE_ROLLUPS_REFRESH_LOCK_FILE', '/tmp/of_gate_rollups_refresh.lock')
        if os.path.exists(lock_path):
            os.remove(lock_path)
    except Exception:
        pass


def run_of_gate_rollups_refresh_nightly() -> bool:  # type: ignore
    """Nightly refresh of Timescale CAGG for OF-gate ok_rate.

    Controlled by:
      - ENABLE_OF_GATE_ROLLUPS_REFRESH_NIGHTLY=1
      - OF_GATE_ROLLUPS_REFRESH_DAYS (default 30)
      - OF_GATE_ROLLUPS_REFRESH_SAFE_START_UTC (default 02:30)
      - OF_GATE_ROLLUPS_REFRESH_SAFE_END_UTC (default 05:30)
      - OF_GATE_ROLLUPS_REFRESH_TIMEOUT_S (default 1800)

    Note: scheduler uses UTC (datetime.utcnow()). Safe window is interpreted in UTC.
    """
    if os.getenv('ENABLE_OF_GATE_ROLLUPS_REFRESH_NIGHTLY', '0') != '1':
        return True

    now = datetime.utcnow()
    sh, sm = _parse_hhmm(os.getenv('OF_GATE_ROLLUPS_REFRESH_SAFE_START_UTC', '02:30'), 2, 30)
    eh, em = _parse_hhmm(os.getenv('OF_GATE_ROLLUPS_REFRESH_SAFE_END_UTC', '05:30'), 5, 30)
    if not _in_safe_window_utc(now, sh, sm, eh, em):
        logger.info(
            f"OF-gate rollups refresh: outside safe window UTC {sh:02d}:{sm:02d}-{eh:02d}:{em:02d}, skipping"
        ),
        return True

    days = os.getenv('OF_GATE_ROLLUPS_REFRESH_DAYS', '30').strip() or '30'
    timeout_s = 1800
    try:
        timeout_s = int(os.getenv('OF_GATE_ROLLUPS_REFRESH_TIMEOUT_S', '1800').strip())
    except Exception:
        timeout_s = 1800

    if not _try_acquire_rollups_lock(timeout_s):
        logger.info('OF-gate rollups refresh: lock busy, skipping')
        return True  # skip is not a failure

    try:
        return run_tool(
            'orderflow_services.of_gate_history_migration_v1',  # type: ignore
            ['refresh', '--days', days],
            timeout=timeout_s,
        ),
    finally:
        _release_rollups_lock()


def run_of_gate_rollups_freshness_probe() -> bool:
    """Probe rollups freshness: SELECT max(bucket) from CAGG views, write to Redis hash.

    Controlled by:
      - ENABLE_OF_GATE_ROLLUPS_FRESHNESS_PROBE=1
      - OF_GATE_ROLLUPS_FRESHNESS_TIMEOUT_S (default 60)

    Default enable policy:
      enabled if ENABLE_OF_GATE_ROLLUPS_REFRESH_NIGHTLY=1, unless explicitly disabled.
      Set ENABLE_OF_GATE_ROLLUPS_FRESHNESS_PROBE=0 to force-disable.
    """
    v = os.getenv('ENABLE_OF_GATE_ROLLUPS_FRESHNESS_PROBE')
    if v is None:
        # Default: inherit from nightly refresh gate
        if os.getenv('ENABLE_OF_GATE_ROLLUPS_REFRESH_NIGHTLY', '0') != '1':
            return True  # both disabled: no-op
    else:
        if str(v).strip() != '1':
            return True  # explicitly disabled

    timeout_s = 60
    try:
        timeout_s = int(os.getenv('OF_GATE_ROLLUPS_FRESHNESS_TIMEOUT_S', '60').strip())
    except Exception:
        timeout_s = 60

    return run_tool('orderflow_services.of_gate_rollups_freshness_probe_v1', timeout=timeout_s)


def run_of_gate_timescale_policy_probe() -> bool:
    """Probe Timescale policies/jobs for OF-gate rollups and retention.

    Controlled by:
      - ENABLE_OF_GATE_TIMESCALE_POLICY_PROBE=1
      - OF_GATE_TIMESCALE_POLICY_PROBE_TIMEOUT_S (default 60)
    Default enable policy: enabled if ENABLE_OF_GATE_ROLLUPS_REFRESH_NIGHTLY=1 unless explicitly disabled.
    """
    v = os.getenv('ENABLE_OF_GATE_TIMESCALE_POLICY_PROBE')
    if v is None:
        if os.getenv('ENABLE_OF_GATE_ROLLUPS_REFRESH_NIGHTLY', '0') != '1':
            return True
    else:
        if str(v).strip() != '1':
            return True

    timeout_s = 60
    try:
        timeout_s = int(os.getenv('OF_GATE_TIMESCALE_POLICY_PROBE_TIMEOUT_S', '60').strip())
    except Exception:
        timeout_s = 60
    return run_tool('orderflow_services.of_gate_timescale_policy_probe_v1', timeout=timeout_s)


def run_edge_stack_dataset_build() -> bool:
    """Build edge-stack dataset with archive fallback."""
    return run_tool("tools.edge_stack_dataset_build_v1", timeout=3600)


def run_feature_selection_loop_v1() -> bool:
    """Run Fast Feature Selection Loop v1 on Edge Stack dataset (v5_of)."""
    enabled = os.getenv("ENABLE_FEATURE_SELECTION_LOOP", "1")
    if str(enabled).lower() not in ("1", "true", "yes", "on"):
        return True

    dataset = os.getenv("ML_EDGE_STACK_DATASET_PATH", "/var/lib/trade/ml_models/edge_stack_v1/dataset.ndjson")
    if not os.path.exists(dataset):
        dataset = os.getenv("ML_EDGE_STACK_OOF_DATASET_PATH", "/var/lib/trade/ml_models/edge_stack_v1_oof/edge_train.jsonl")
        if not os.path.exists(dataset):
            return False

    out_dir = os.getenv("FEATURE_SELECTION_LOOP_OUT_DIR", "/var/lib/trade/ml_models/fs_loop_v1")
    schema_ver = os.getenv("FEATURE_SELECTION_LOOP_SCHEMA_VER", "v5_of")
    model = os.getenv("FEATURE_SELECTION_LOOP_MODEL", "gbdt")
    max_val_rows = os.getenv("FEATURE_SELECTION_LOOP_MAX_VAL_ROWS", "250000")

    args = [
        "--data_path", dataset,
        "--schema_ver", schema_ver,
        "--out_dir", out_dir,
        "--model", model,
        "--regime_col", "scenario",
        "--max_val_rows", max_val_rows
    ]
    return run_tool("ml_analysis.tools.feature_selection_loop_v1", args, timeout=7200)



def run_feature_selection_loop_bundle_v1() -> bool:
    """Nightly minimal feature selection loop (importance + stability by regime/hour).

    Guarded by FEATURE_SELECTION_LOOP_BUNDLE_ENABLED=1.
    Writes Redis hash FEATURE_SELECTION_LOOP_METRICS_KEY (default metrics:feature_selection_loop:last).
    """
    v = (os.getenv("FEATURE_SELECTION_LOOP_BUNDLE_ENABLED", "0") or "0").strip()
    if v not in ("1", "true", "TRUE", "yes", "YES"):
        return True
    timeout_s = 3600
    try:
        timeout_s = int(os.getenv("FEATURE_SELECTION_LOOP_BUNDLE_TIMEOUT_S", "3600").strip())
    except Exception:
        timeout_s = 3600
    return run_tool("ml_analysis.tools.nightly_feature_selection_loop_bundle_v1", timeout=timeout_s)


def run_strategy_research_guard_bundle() -> bool:
    """P5 nightly/weekly research guard bundle.

    Computes PSR / DSR / PBO / CSCV on a deterministic research dataset and
    publishes a compact blocker state that later apply/promote flows may consult.
    Safe default is report-only mode.
    """
    if os.getenv("ENABLE_STRATEGY_RESEARCH_GUARD", "1") != "1":
        return True
    return run_tool(
        "ml_analysis.tools.nightly_strategy_research_guard_bundle_v1",  # type: ignore
        timeout=int(os.getenv("STRATEGY_RESEARCH_GUARD_TIMEOUT_S", "1800")),
    ),


def run_strategy_research_stats_bundle() -> bool:
    """P6.1 nightly strategy research stats bundle (PSR / DSR / PBO / CSCV).

    Reads the ML dataset, computes universal metrics and statistical overfitting
    indicators, then writes a compact Redis summary+blocker hash that the
    apply/promote/autopromo guardrails may consult at rollout time.

    Guarded by ENABLE_STRATEGY_RESEARCH_STATS_BUNDLE=1 (default: 1).
    Gate mode defaults to report_only (safe); set STRATEGY_RESEARCH_STATS_GATE_MODE
    to 'soft' or 'hard' to enable blocking behaviour.
    """
    if os.getenv("ENABLE_STRATEGY_RESEARCH_STATS_BUNDLE", "1") not in ("1", "true", "True", "yes", "on"):
        return True
    timeout_s = int(os.getenv("STRATEGY_RESEARCH_STATS_BUNDLE_TIMEOUT_S", "1800") or 1800)
    args = []
    dataset_path = os.getenv("STRATEGY_RESEARCH_STATS_DATASET_PATH", "").strip()
    if dataset_path:
        args += ["--dataset-path", dataset_path]
    gate_mode = os.getenv("STRATEGY_RESEARCH_STATS_GATE_MODE", "report_only").strip()
    if gate_mode:
        args += ["--gate-mode", gate_mode]
    return run_tool(
        "ml_analysis.tools.nightly_strategy_research_stats_bundle_v1",  # type: ignore
        args,
        timeout=timeout_s,
    ),


def run_orchestration_composite_preflight_history_exporter() -> bool:
    """P5.5: Export composite preflight 24h/7d history rollups to textfile collector.

    This is intentionally a textfile collector job instead of another HTTP exporter:
      - history windows are naturally periodic, not high-frequency point-in-time state
      - node_exporter can scrape the last successful rollup atomically
      - bounded labels allow safe SLO views by purpose/source/reason_code

    Guards:
      - ENABLE_ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_EXPORTER=1
      - module may be overridden through ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_EXPORTER_MODULE
    """
    enabled = os.getenv("ENABLE_ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_EXPORTER", "0")
    if str(enabled).lower() not in ("1", "true", "yes", "on"):
        return True

    module = os.getenv(
        "ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_EXPORTER_MODULE",
        "orderflow_services.orchestration_composite_preflight_history_exporter_v1",
    ),
    timeout_s = int(os.getenv("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_TIMEOUT_S", "90"))

    env = os.environ.copy()
    env.setdefault(
        "ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_EXPORT_PATH",
        "/var/lib/node_exporter/textfile_collector/orchestration_composite_preflight_history.prom",
    ),

    try:
        cmd = [sys.executable, "-m", module]
        result = subprocess.run(
            cmd,
            cwd="/app",
            capture_output=True,
            text=True,
            timeout=timeout_s,
            env=env,
        )
        if result.returncode != 0:
            logger.error(
                "orchestration_composite_preflight_history_exporter failed rc=%s stdout=%s stderr=%s",
                result.returncode,
                (result.stdout or "")[:2000],
                (result.stderr or "")[:2000],
            ),
            return False
        if result.stdout:
            logger.info("orchestration_composite_preflight_history_exporter: %s", result.stdout.strip())
        return True
    except (ImportError, ModuleNotFoundError):
        logger.warning("orchestration_composite_preflight_history_exporter module missing; skip phased rollout")
        return True
    except subprocess.TimeoutExpired:
        logger.error("orchestration_composite_preflight_history_exporter timeout after %ss", timeout_s)
        return False
    except Exception as exc:
        logger.error("orchestration_composite_preflight_history_exporter error: %s", exc)
        return False


def run_edge_stack_shadow_eval() -> bool:
    """Run Edge Stack Shadow Eval (P60)."""
    args = []
    if os.getenv("EDGE_STACK_AUTO_PROMOTE_GUARDED") == "1":
        args.extend(["--auto_promote_guarded", "1"])
    return run_tool("tools.edge_stack_shadow_eval_bundle_v1", args, timeout=3600)


def run_nightly_edge_stack_train() -> bool:
    """Run nightly edge_stack_v1 training bundle (P59).

    Uses ml_analysis.tools.nightly_edge_stack_v1_train_bundle which includes:
      - Feature Registry schema pinning (feature_cols_hash / schema_hash)
      - Dataset + train validation guardrails (brier/ECE)
      - Atomic candidate/champion promotion
      - Redis metrics for Prometheus alerts
    """
    args = []
    # auto promote is controlled by env; passing flag keeps logs explicit
    if os.getenv("EDGE_STACK_AUTO_PROMOTE") == "1":
        args.extend(["--auto_promote", "1"])
    return run_tool("ml_analysis.tools.nightly_edge_stack_v1_train_bundle", args, timeout=3600)


def run_nightly_meta_train() -> bool:
    """Run nightly meta-model training."""
    args = []
    if os.getenv("ML_AUTO_CONFIRM_META_MODEL") == "1":
        args.append("--auto-confirm")
    return run_tool("tools.nightly_train_meta_model_bundle", args, timeout=3600)


def run_ml_calibration_health() -> bool:
    """Run ML calibration health monitor."""
    return run_tool("tools.ml_calibration_health_monitor", timeout=600)


def run_nightly_calibration() -> bool:
    """Run nightly gate calibration."""
    return run_tool("tools.nightly_gate_calibrate_bundle", timeout=3600)


def run_nightly_slippage_calibrator() -> bool:
    """Run nightly slippage calibrator."""
    if os.getenv("ENABLE_SLIPPAGE_CALIBRATOR", "1") != "1":
        return False
    timeout_s = 3600
    try:
        timeout_s = int(os.getenv("SLIPPAGE_CALIBRATOR_TIMEOUT_S", "3600").strip())
    except Exception:
        timeout_s = 3600
    return run_tool("ml_analysis.tools.nightly_slippage_calibrator_v1", timeout=timeout_s)


def run_nightly_tca_report() -> bool:
    """Run nightly TCA report bundle over tca_fill_metrics.

    Computes 24h/7d rollups (IS p50/p95/p99, eff_spread p95, realized_spread p50,
    perm_impact p95, adverse-selection neg-share) and publishes:
      - JSON status/report files
      - Redis summary hash state:tca_nightly_report:last  (for Prometheus exporter)

    Controlled by:
      - ENABLE_TCA_NIGHTLY_REPORT=1  (default: 1)
      - TCA_NIGHTLY_REPORT_TIMEOUT_S (default: 1800)
    """
    if os.getenv("ENABLE_TCA_NIGHTLY_REPORT", "1") != "1":
        return True
    timeout_s = 1800
    try:
        timeout_s = int(os.getenv("TCA_NIGHTLY_REPORT_TIMEOUT_S", "1800").strip())
    except Exception:
        timeout_s = 1800
    return run_tool("services.posttrade.tca_nightly_report_v1", timeout=timeout_s)


def run_exec_slippage_eval_rowcount_probe() -> bool:
    """Hourly probe: v_exec_slippage_eval rowcount by bucket (P77).

    Writes Redis state keys (state:exec_slippage_eval:rows_24h*) consumed by
    enforce_bucket_state_exporter_v1 gauges + OF_ExecSlippageEvalRowcount* Prometheus alerts.
    Alerts via notify stream only on soft/hard failure, with cooldown dedup.

    Controlled by:
      - ENABLE_EXEC_SLIP_EVAL_ROWCOUNT_PROBE (default 1)
      - EXEC_SLIP_EVAL_ROWCOUNT_PROBE_TIMEOUT_S (default 120)
      - EXEC_SLIP_EVAL_ROWCOUNT_PROBE_COOLDOWN_S (default 21600)
      - EXEC_SLIP_EVAL_PROBE_MIN_TOTAL_24H (default 30)
      - EXEC_SLIP_EVAL_PROBE_MIN_HVLL_24H (default 5)
    """
    if os.getenv("ENABLE_EXEC_SLIP_EVAL_ROWCOUNT_PROBE", "1") != "1":
        return True
    try:
        rc, out, err = run_tool_rc(  # type: ignore
            "orderflow_services.exec_slippage_eval_rowcount_probe_p77_v1",  # type: ignore
            timeout=int(os.getenv("EXEC_SLIP_EVAL_ROWCOUNT_PROBE_TIMEOUT_S", "120")),
        ),
    except Exception as e:
        _notify_stream(
            f"EXEC_SLIP_EVAL_ROWCOUNT_PROBE exception: {e}",
            severity="crit",
            sid="exec_slip_eval_rowcount_probe:exc",
            source="exec_slip_eval_rowcount_probe",
        ),
        return False

    if int(rc) == 0:  # type: ignore
        return True  # type: ignore

    signature = f"rc={rc}|out={(out or '').strip()[:240]}|err={(err or '').strip()[:240]}"
    cooldown_s = int(os.getenv("EXEC_SLIP_EVAL_ROWCOUNT_PROBE_COOLDOWN_S", str(6 * 3600)))
    prefix = os.getenv("EXEC_SLIP_EVAL_ROWCOUNT_PROBE_DEDUP_PREFIX", "dedup:alert:exec_slip_eval_rowcount_probe:")
    if _dedup_allow(signature, cooldown_s=cooldown_s, prefix=prefix):
        sev = "warning" if int(rc) == 2 else "crit"  # type: ignore
        _notify_stream(  # type: ignore
            f"EXEC_SLIP_EVAL_ROWCOUNT_PROBE rc={rc} :: {signature}",
            severity=sev,
            sid="exec_slip_eval_rowcount_probe",
            source="exec_slip_eval_rowcount_probe",
        ),
    return False


def run_nightly_enforce_bucket_promoter() -> bool:
    """Nightly proposer/applicator for bucket-aware enforcement.

    Controlled by:
      - ENABLE_ENFORCE_BUCKET_PROMOTER=1 (gate)
      - ENFORCE_BUCKET_PROMOTER_TIMEOUT_S (default: 600)
      - PROMOTE_* env vars (see ml_analysis.tools.nightly_enforce_bucket_promoter_v1)
    """
    if os.getenv("ENABLE_ENFORCE_BUCKET_PROMOTER", "0") != "1":
        return True
    timeout_s = 600
    try:
        timeout_s = int(os.getenv("ENFORCE_BUCKET_PROMOTER_TIMEOUT_S", "600").strip())
    except Exception:
        timeout_s = 600

    # Preflight (P78): skip promoter if infra/data not ready
    if os.getenv("ENABLE_ENFORCE_BUCKET_PREFLIGHT", "1") in ("1", "true", "True", "yes", "on"):
        try:
            rc, out, err = run_tool_rc("orderflow_services.enforce_bucket_ops_validate_p78", timeout=60)
            if rc == 0:
                pass
            elif rc == 2:
                logger.warning("EnforceBucket preflight soft-block; promoter skipped. out=%s err=%s", (out or "").strip()[:500], (err or "").strip()[:200])
                return True
            else:
                msg = f"ENFORCE_BUCKET_PREFLIGHT hard-fail rc={rc} out={(out or '').strip()[:500]} err={(err or '').strip()[:200]}"
                _notify_stream(msg, severity="page", sid="enforce_bucket_preflight")
                return False
        except Exception as e:
            _notify_stream(f"ENFORCE_BUCKET_PREFLIGHT exception: {e}", severity="page", sid="enforce_bucket_preflight_exc")
            return False

    return run_tool("ml_analysis.tools.nightly_enforce_bucket_promoter_v1", timeout=timeout_s)


def run_enforce_bucket_promoter_rollback_controller() -> bool:
    """Periodic rollback controller for bucket enforcement.

    Controlled by:
      - ENABLE_ENFORCE_BUCKET_ROLLBACK=1 (gate)
      - ENFORCE_BUCKET_ROLLBACK_TIMEOUT_S (default: 180)

    By default we run with --apply 1 (can be disabled by ENFORCE_BUCKET_ROLLBACK_APPLY=0).
    """
    if os.getenv("ENABLE_ENFORCE_BUCKET_ROLLBACK", "0") != "1":
        return True
    timeout_s = 180
    try:
        timeout_s = int(os.getenv("ENFORCE_BUCKET_ROLLBACK_TIMEOUT_S", "180").strip())
    except Exception:
        timeout_s = 180
    apply = (os.getenv("ENFORCE_BUCKET_ROLLBACK_APPLY", "1") or "1").strip() in ("1","true","True","yes","on")
    args = ["--apply", "1" if apply else "0"]
    return run_tool("orderflow_services.enforce_bucket_promoter_rollback_controller_v1", args, timeout=timeout_s)


def run_nightly_confidence_calibrator_v2() -> bool:
    """Run nightly confidence calibration V2 (Platt/Iso/Beta)."""
    try:
        ok = run_tool(
            module="ml_analysis.tools.nightly_confidence_calibrator_bundle_v2",
            args=os.getenv("NIGHTLY_CONF_CALIBRATOR_V2_ARGS", "").split(),
            timeout=int(os.getenv("NIGHTLY_CONF_CALIBRATOR_V2_TIMEOUT_S", "3600")),
        ),

        # Optional Phase 2: learn confirmation bonus weights from closed trades.
        if ok and os.getenv("RUN_CONF_BONUS_WEIGHT_FIT", "0").lower() in ("1", "true", "yes", "on"):
            ok2 = run_tool(
                module="ml_analysis.tools.fit_confidence_bonus_weights_v1",
                args=os.getenv("CONF_BONUS_WEIGHT_FIT_ARGS", "").split(),
                timeout=int(os.getenv("CONF_BONUS_WEIGHT_FIT_TIMEOUT_S", "1800")),
            ),
            ok = ok and ok2

        return ok  # type: ignore
    except Exception:  # type: ignore
        logger.exception("run_nightly_confidence_calibrator_v2 failed")
        return False

def run_nightly_meta_enforce_ramp() -> bool:
    """Run nightly meta enforce ramp with tick quality gate."""
    # Wrapper for tick quality gated command
    args = [
        "--metrics-url", os.getenv("TICK_GATE_METRICS_SOURCE_URL", "http://python-worker:8000/metrics"),
        "--window-s", "60",
        "--fail-mode", "fail_closed",
        "--",
        sys.executable, "-m", "tools.nightly_meta_enforce_ramp_or_freeze_bundle"
    ]
    return run_tool("tools.run_tick_quality_gated_command", args, timeout=1200)


def run_nightly_meta_self_heal() -> bool:
    """Run nightly meta cells self-heal."""
    return run_tool("tools.nightly_meta_cells_self_heal", timeout=1200)


def run_nightly_meta_stage2_opt() -> bool:
    """Run nightly meta Stage2 optimize share."""
    return run_tool("tools.nightly_meta_stage2_optimize_share_bundle_v4", timeout=1200)


def run_meta_ab_v2_nightly_job_v1() -> bool:
    """Run nightly Meta AB v2 evaluation + optional apply proposal.

    Controlled by:
      - ENABLE_META_AB_V2_NIGHTLY=1 (gate)
      - META_AB_V2_JOB_MODULE (default: services.orderflow.meta_ab_v2_nightly_job_v1)
      - META_AB_V2_NIGHTLY_TIMEOUT_S (default: 2100)
    """
    if os.getenv("ENABLE_META_AB_V2_NIGHTLY", "0") != "1":
        return True  # disabled by env: no-op but not a failure

    module = os.getenv("META_AB_V2_JOB_MODULE", "services.orderflow.meta_ab_v2_nightly_job_v1")
    timeout_s = 2100
    try:
        timeout_s = int(os.getenv("META_AB_V2_NIGHTLY_TIMEOUT_S", "2100").strip())
    except Exception:
        timeout_s = 2100
    return run_tool(module, timeout=timeout_s)




def run_nightly_feature_denylist_proposal_autogen() -> bool:
    """Nightly: generate candidate denylist diff from feature-selection loop output.

    Enabled when ENABLE_FEATURE_DENYLIST_PROPOSAL=1 and FEATURE_SELECTION_RUN_DIR is set.
    The proposal is written to <FEATURE_SELECTION_RUN_DIR>/proposals and requires replay/AB
    confirmation before apply.
    """
    if os.getenv("ENABLE_FEATURE_DENYLIST_PROPOSAL", "0") != "1":
        return True

    fs_dir = (os.environ.get("FEATURE_SELECTION_RUN_DIR") or "").strip()
    if not fs_dir:
        return True

    return run_tool(
        "ml_analysis.tools.autogen_feature_denylist_proposal_v1",  # type: ignore
        ["--fs-run-dir", fs_dir],
        timeout=180,
    ),

def run_close_backfill() -> bool:
    """Run Close Backfill Replay (P55)."""
    if os.getenv("ENABLE_CLOSE_BACKFILL_TIMER", "0") != "1":
        return False
    hours = os.getenv("CLOSE_BACKFILL_HOURS", "48")
    count = os.getenv("CLOSE_BACKFILL_MAX_COUNT", "200000")
    return run_tool("tools.close_backfill_replay_v1", ["--hours", hours, "--count", count], timeout=3600)


def run_signal_quality_kpis() -> bool:
    """Run Signal Quality KPIs (P47/P48)."""
    module = os.getenv("SIGNAL_QUALITY_KPI_MODULE", "tools.signal_quality_kpi_worker_v1")
    timeout = int(os.getenv("SIGNAL_QUALITY_KPI_TIMEOUT_SEC", "900"))
    return run_tool(module, ["--once"], timeout=timeout)


def run_feature_drift_monitor() -> bool:
    """Run Feature Drift Monitor (P49)."""
    if os.getenv("ENABLE_FEATURE_DRIFT_MONITOR", "0") != "1":
        return False
    module = os.getenv("FEATURE_DRIFT_MONITOR_MODULE", "tools.feature_drift_monitor_v1")
    timeout = int(os.getenv("FEATURE_DRIFT_MONITOR_TIMEOUT_SEC", "900"))
    return run_tool(module, ["--once"], timeout=timeout)


def run_confirmations_coverage_nightly() -> bool:
    """Run confirmations coverage nightly job (Stage4 drift guard)."""
    if os.getenv("ENABLE_CONFIRMATIONS_COVERAGE_NIGHTLY", "0") != "1":
        return False
    module = os.getenv("CONFIRMATIONS_COVERAGE_MODULE", "tools.confirmations_coverage_nightly_job_v1")
    timeout = int(os.getenv("CONFIRMATIONS_COVERAGE_TIMEOUT_SEC", "900"))
    return run_tool(module, timeout=timeout)


def run_archive_maintenance() -> bool:
    """Run Archive Inventory and Prune (P57)."""
    archive_dir = os.getenv("ARCHIVE_DIR", "/var/lib/trade/of_inputs_archive")
    retention_days = int(os.getenv("ARCHIVE_RETENTION_DAYS", "30"))
    keep_last = int(os.getenv("ARCHIVE_KEEP_LAST_DAYS", "3"))
    max_gb = float(os.getenv("ARCHIVE_MAX_TOTAL_GB", "100"))

    args = [
        "--dir", archive_dir,
        "--retention-days", str(retention_days),
        "--keep-last-days", str(keep_last),
        "--max-gb", str(max_gb)
    ]
    return run_tool("ml_analysis.tools.archive_inventory_prune_v1", args, timeout=600)


def run_decisions_archive() -> bool:
    """Run Decisions Final Archiver (P63)."""
    return run_tool("tools.archive_decisions_final_v1", timeout=3600)


def _parse_hhmm_utc(s: str, default: str) -> int:
    """Parse 'HH:MM' into minutes from midnight (UTC)."""
    s = (s or "").strip() or default
    try:
        hh, mm = s.split(":", 1)
        hh_i = int(hh)
        mm_i = int(mm)
        if 0 <= hh_i <= 23 and 0 <= mm_i <= 59:
            return hh_i * 60 + mm_i
    except Exception:
        pass
    hh, mm = default.split(":", 1)
    return int(hh) * 60 + int(mm)


def _in_utc_window(now_utc: datetime, start_hhmm: str, end_hhmm: str) -> bool:
    """Safe window guard in UTC; supports wrap-around."""
    start_m = _parse_hhmm_utc(start_hhmm, "02:30")
    end_m = _parse_hhmm_utc(end_hhmm, "05:30")
    cur_m = now_utc.hour * 60 + now_utc.minute
    if start_m <= end_m:
        return start_m <= cur_m <= end_m
    return cur_m >= start_m or cur_m <= end_m


def _file_lock_try(path: str, ttl_sec: int) -> bool:
    """Best-effort lock to prevent parallel nightly runs."""
    try:
        now = time.time()
        if os.path.exists(path):
            st = os.stat(path)
            if now - st.st_mtime < ttl_sec:
                return False
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(str(int(now)))
        return True
    except Exception:
        return True


def run_of_gate_rollups_refresh_nightly() -> bool:
    """P78 — Refresh Timescale continuous aggregates for of_gate ok_rate."""
    if os.getenv("ENABLE_OF_GATE_ROLLUPS_REFRESH_NIGHTLY", "0") != "1":
        return True

    now_utc = datetime.utcnow()
    if not _in_utc_window(
        now_utc,
        os.getenv("OF_GATE_ROLLUPS_REFRESH_SAFE_START_UTC", "02:30"),
        os.getenv("OF_GATE_ROLLUPS_REFRESH_SAFE_END_UTC", "05:30"),
    ):
        logger.info("of_gate rollups refresh: outside safe window, skipping")
        return True

    lock_file = os.getenv("OF_GATE_ROLLUPS_REFRESH_LOCK_FILE", "/tmp/of_gate_rollups_refresh.lock")
    if not _file_lock_try(lock_file, ttl_sec=6 * 3600):
        logger.info("of_gate rollups refresh: lock busy, skipping")
        return True

    days = os.getenv("OF_GATE_ROLLUPS_REFRESH_DAYS", "30")
    timeout_s = int(os.getenv("OF_GATE_ROLLUPS_REFRESH_TIMEOUT_S", "1800"))
    return run_tool(
        "orderflow_services.of_gate_history_migration_v1",  # type: ignore
        ["refresh", "--days", str(days)],
        timeout=timeout_s,
    ),


def run_confidence_calibrator() -> bool:
    """Run nightly confidence calibration (temp/Platt) and deploy latest calibrator."""
    try:
        out_dir = os.getenv("CONF_CAL_OUT_DIR", "/var/lib/trade/of_calibrators")
        reports_dir = os.getenv("CONF_CAL_REPORTS_DIR", "/var/lib/trade/of_reports/out/confidence_cal")
        os.makedirs(out_dir, exist_ok=True)
        os.makedirs(reports_dir, exist_ok=True)

        args = [
            "--redis_url", os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            "--out_dir", out_dir,
            "--reports_dir", reports_dir,
        ]
        return run_tool("ml_analysis.tools.nightly_confidence_calibrator_v1", args, timeout=3600)
    except Exception as e:
        logger.error(f"Confidence calibrator error: {e}")
        return False


def run_confidence_cal_live_health() -> bool:
    """Run live calibration health + rollback loop."""
    return run_tool("ml_analysis.tools.confidence_cal_live_health_loop_v1", timeout=900)


def run_ofc_bench() -> bool:
    """Run OFC build benchmark (Hourly)."""
    golden = os.getenv("OFC_GOLDEN", "/tmp/ofc_golden.ndjson")
    if not os.path.exists(golden):
        return False
    args = [
        "--input", golden,
        "--warmup", os.getenv("OFC_BENCH_WARMUP", "200"),
        "--iters", os.getenv("OFC_BENCH_ITERS", "2000"),
        "--mode", os.getenv("OFC_BENCH_MODE", "restore_each"),
        "--budget-p95-us", os.getenv("OFC_BUDGET_P95_US", "350"),
        "--budget-p99-us", os.getenv("OFC_BUDGET_P99_US", "900"),
    ]
    return run_tool("tools.bench_ofc_build", args, timeout=1200)


def run_ofc_golden_pipeline() -> bool:
    """Run nightly OFC golden pipeline (daily at 03:15)."""
    capture = os.getenv("OFC_CAPTURE", "/tmp/ofc_capture.ndjson")
    golden = os.getenv("OFC_GOLDEN", "/tmp/ofc_golden.ndjson")
    if not os.path.exists(capture):
        logger.warning(f"OFC capture not found at {capture}")
        return False

    # Run shell script for the pipeline
    try:
        logger.info(f"Running nightly OFC golden pipeline (capture={capture})...")
        cmd = ["bash", "tools/ofc_nightly_golden.sh", capture, golden]
        result = subprocess.run(cmd, cwd="/app", capture_output=True, text=True, timeout=1800)
        return result.returncode == 0
    except Exception as e:
        logger.error(f"OFC golden pipeline error: {e}")
        return False


def run_of_gate_missing_leg_report() -> bool:
    """Run OF gate missing leg report (Hourly)."""
    args = [
        "--redis-url", os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
        "--stream", os.getenv("OF_GATE_METRICS_STREAM", RS.OF_GATE_METRICS),
        "--limit", os.getenv("OF_GATE_MISS_LIMIT", "8000"),
        "--only-veto"
    ]
    return run_tool("tools.of_gate_missing_leg_report", args, timeout=600)


def run_of_gate_sre_monitor() -> bool:
    """Run OF gate SRE monitor (ok_rate/soft_rate/no_data) every 15 minutes.

    Enabled by: ENABLE_OF_GATE_SRE_MONITOR_TIMER=1
    Schedule: every 15 minutes at offset :08 (08, 23, 38, 53)
    Monitor has its own Redis-based cooldown for duplicate alert suppression.
    """
    if os.getenv("ENABLE_OF_GATE_SRE_MONITOR_TIMER", "0") != "1":
        return True  # disabled by env: no-op, not a failure
    args = [
        "--redis-url", os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
        "--metrics-stream", os.getenv("OF_GATE_METRICS_STREAM", RS.OF_GATE_METRICS),
        "--notify-stream", os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM),
        "--window-min", os.getenv("SRE_OF_GATE_WINDOW_MIN", "15"),
        "--min-n", os.getenv("SRE_OF_GATE_MIN_N", "200"),
        "--always", os.getenv("SRE_OF_GATE_ALWAYS", "0"),
    ]
    # Note: the monitor has its own Redis-based cooldown for duplicate alerts
    return run_tool("tools.of_gate_sre_monitor", args, timeout=int(os.getenv("SRE_OF_GATE_TIMEOUT_S", "180")))


def run_of_gate_dlq_db_archive_nightly() -> bool:
    """Optional: archive OF-Gate DLQ streams to DB (P83).

    Controlled by:
      - ENABLE_OF_GATE_DLQ_DB_ARCHIVE_NIGHTLY=1
      - OF_GATE_DLQ_DB_ARCHIVE_TIMEOUT_S (default: 1800)
      - OF_GATE_DLQ_DB_ARCHIVE_STREAMS (default: stream:dlq:of_gate_metrics,stream:dlq:of_gate_quarantine)
      - OF_GATE_DLQ_DB_ARCHIVE_SAFE_START_UTC / _SAFE_END_UTC (optional guard)
    """
    if os.getenv("ENABLE_OF_GATE_DLQ_DB_ARCHIVE_NIGHTLY", "0") != "1":
        return False

    # Optional safe-window (UTC)
    safe_start = os.getenv("OF_GATE_DLQ_DB_ARCHIVE_SAFE_START_UTC", "")
    safe_end = os.getenv("OF_GATE_DLQ_DB_ARCHIVE_SAFE_END_UTC", "")
    if safe_start and safe_end:
        try:
            now = datetime.utcnow()
            sh, sm = [int(x) for x in safe_start.split(":", 1)]
            eh, em = [int(x) for x in safe_end.split(":", 1)]
            cur = now.hour * 60 + now.minute
            a = sh * 60 + sm
            b = eh * 60 + em
            in_win = (a <= cur <= b) if a <= b else (cur >= a or cur <= b)
            if not in_win:
                logger.info("DLQ DB archive: outside safe-window, skipping")
                return False
        except Exception:
            pass

    # Best-effort lock (Redis SET NX EX; fallback file lock)
    lock_key = os.getenv("OF_GATE_DLQ_DB_ARCHIVE_LOCK_KEY", "lock:of_gate_dlq_db_archive")
    lock_ttl = int(os.getenv("OF_GATE_DLQ_DB_ARCHIVE_LOCK_TTL_S", "2100"))  # >= timeout
    got_lock = False
    lock_file = os.getenv("OF_GATE_DLQ_DB_ARCHIVE_LOCK_FILE", "/tmp/of_gate_dlq_db_archive.lock")
    try:
        import redis  # type: ignore
        r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
        got_lock = bool(r.set(lock_key, str(int(time.time())), nx=True, ex=lock_ttl))
    except Exception:
        try:
            # file lock: create exclusively
            fd = os.open(lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(int(time.time())).encode("utf-8"))
            os.close(fd)
            got_lock = True
        except Exception:
            got_lock = False

    if not got_lock:
        logger.info("DLQ DB archive: lock busy, skipping")
        return False

    timeout_s = int(os.getenv("OF_GATE_DLQ_DB_ARCHIVE_TIMEOUT_S", "1800"))
    streams = os.getenv(
        "OF_GATE_DLQ_DB_ARCHIVE_STREAMS",
        os.getenv("OF_GATE_DLQ_STREAMS", f"{RS.DLQ_OF_GATE_METRICS},{RS.DLQ_OF_GATE_QUARANTINE}"),
    ),
    batch = os.getenv("OF_GATE_DLQ_DB_ARCHIVE_BATCH", "5000")
    args = ["--streams", streams, "--batch", batch, "--once"]

    ok = run_tool("orderflow_services.of_gate_dlq_archive_to_db_v1", args, timeout=timeout_s)

    # Release file lock (Redis lock expires by TTL)
    try:
        if os.path.exists(lock_file):
            os.remove(lock_file)
    except Exception:
        pass

    return ok


def run_ml_drift_monitor_v1() -> bool:
    """Run ML drift monitor v1 (Every 6h)."""
    return run_tool("tools.ml_drift_monitor_v1", timeout=1200)


def run_tick_time_lag_report() -> bool:
    """Run tick time lag report (Every 2h)."""
    args = [
        "--n", os.getenv("TICK_TIME_LAG_REPORT_N", "50000"),
        "--stream-key", os.getenv("TICK_TIME_STREAM_KEY", RS.TICK_TIME)
    ]
    return run_tool("tools.tick_time_lag_report", args, timeout=600)


def run_tick_time_autotune() -> bool:
    """Run tick time autotune (Every 4h)."""
    args = [
        "--redis", os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
        "--stream", os.getenv("TICK_TIME_STREAM_KEY", RS.TICK_TIME),
        "--count", os.getenv("TICK_TIME_AUTOTUNE_COUNT", "50000")
    ]
    return run_tool("tools.tick_time_autotune", args, timeout=600)


def run_meta_enforce_cov_ops_bundle() -> bool:
    """Run META COV OPS bundle (Every 5m)."""
    return run_tool("orderflow_services.nightly_meta_enforce_cov_ops_bundle_v1", ["--emit-eventlog"], timeout=300)


def run_ofc_replay_capture() -> bool:
    """Run OFC replay capture (Hourly)."""
    cap = os.getenv("OFC_CAPTURE_PATH", "/tmp/ofc_capture.ndjson")
    if not os.path.exists(cap):
        return False
    args = [
        "--in", cap,
        "--out", os.getenv("OFC_REPLAY_OUTPUT", "/tmp/ofc_replayed.ndjson"),
        "--limit", os.getenv("OFC_REPLAY_LIMIT", "0"),
    ]
    if os.getenv("OFC_REPLAY_STRICT") == "1":
        args.append("--strict")
    return run_tool("tools.ofc_replay_capture", args, timeout=1200)


def run_ofc_golden_replay() -> bool:
    """Run OFC golden replay (Hourly)."""
    cap = os.getenv("OFC_CAPTURE_PATH", "/tmp/ofc_capture.ndjson")
    if not os.path.exists(cap):
        return False
    args = ["--path", cap]
    if os.getenv("OFC_REPLAY_LIMIT", "0") != "0":
        args.extend(["--limit", os.getenv("OFC_REPLAY_LIMIT")])  # type: ignore
    if os.getenv("OFC_REPLAY_BASELINE_DIGEST"):  # type: ignore
        args.extend(["--baseline-digest", os.getenv("OFC_REPLAY_BASELINE_DIGEST")])  # type: ignore
    return run_tool("tools.ofc_golden_replay", args, timeout=1200)  # type: ignore


def run_ml_train_edge_stack_mh_v1() -> bool:
    """Run ML Edge Stack MH v1 training (04:20)."""
    dataset = os.getenv("ML_EDGE_STACK_MH_DATASET_PATH", "/var/lib/trade/ml_models/edge_stack_mh_v1/dataset.ndjson")
    if not os.path.exists(dataset):
        return False
    out_dir = os.path.join(os.getenv("ML_EDGE_STACK_MH_MODELS_ROOT", "/var/lib/trade/ml_models"), "edge_stack_mh_v1", datetime.now().strftime("%Y%m%d_%H%M%S"))
    args = [
        "--dataset", dataset,
        "--out-dir", out_dir,
        "--horizons", os.getenv("ML_EDGE_STACK_MH_HORIZONS", "60000,180000,300000"),
        "--n-splits", os.getenv("ML_EDGE_STACK_MH_N_SPLITS", "5"),
        "--unc-k", os.getenv("ML_EDGE_STACK_MH_UNC_K", "0.10"),
        "--time-col", os.getenv("ML_EDGE_STACK_MH_TIME_COL", "ts_ms"),
        "--scenario-col", os.getenv("ML_EDGE_STACK_MH_SCENARIO_COL", "scenario_v4"),
    ]
    return run_tool("tools.train_edge_stack_mh_v1", args, timeout=3600)


def run_ml_train_edge_stack_v1() -> bool:
    """Run ML Edge Stack v1 training (04:10)."""
    dataset = os.getenv("ML_EDGE_STACK_DATASET_PATH", "/var/lib/trade/ml_models/edge_stack_v1/dataset.ndjson")
    if not os.path.exists(dataset):
        return False
    out_dir = os.path.join(os.getenv("ML_EDGE_STACK_MODELS_ROOT", "/var/lib/trade/ml_models"), "edge_stack_v1", datetime.now().strftime("%Y%m%d_%H%M%S"))
    args = [
        "--in", dataset,
        "--out", out_dir,
        "--label-col", os.getenv("ML_EDGE_STACK_LABEL_COL", "y_edge"),
        "--time-col", os.getenv("ML_EDGE_STACK_TIME_COL", "ts_ms"),
        "--n-splits", os.getenv("ML_EDGE_STACK_N_SPLITS", "5"),
        "--seed", os.getenv("ML_EDGE_STACK_SEED", "42"),
    ]
    return run_tool("tools.train_edge_stack_v1", args, timeout=3600)


def run_edge_stack_v1_dataset_build_fallback() -> bool:
    """Build edge-stack dataset with archive fallback (05:00).

    P59: when EDGE_STACK_BUNDLE_ENABLED=1 (default), skip this step entirely
    because the bundle already handles dataset build internally.
    """
    if int(os.getenv("EDGE_STACK_BUNDLE_ENABLED", "1") or 1) == 1:
        # P59 bundle is enabled; avoid duplicate dataset/train steps.
        return False
    args = [
        "--redis_url", os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
        "--signal_stream", os.getenv("ML_REPLAY_STREAM", RS.OF_INPUTS),
        "--closed_stream", os.getenv("TRADES_CLOSED_STREAM", RS.TRADES_CLOSED),
        "--archive_dir", os.getenv("ARCHIVE_DIR", "/var/lib/trade/of_inputs_archive"),
        "--signals_count", os.getenv("SIGNALS_COUNT", "200000"),
        "--closes_count", os.getenv("CLOSES_COUNT", "200000"),
        "--out", os.getenv("EDGE_STACK_DATASET_OUT", "/var/lib/trade/ml_models/edge_stack_v1_oof/edge_train.jsonl"),
    ]
    return run_tool("ml_analysis.tools.build_edge_stack_dataset_fallback_v1", args, timeout=3600)


def run_ml_train_edge_stack_v1_oof() -> bool:
    """Run ML Edge Stack v1 OOF training (05:10).

    P59: when EDGE_STACK_BUNDLE_ENABLED=1 (default), skip this step entirely
    because the bundle already handles OOF training internally.

    Legacy modes (EDGE_STACK_BUNDLE_ENABLED=0):
      1) Feature Registry (рекомендуется): ML_EDGE_STACK_OOF_FEATURE_SCHEMA_VER задан →
         feature_cols берётся из Registry, feature_cols.json не нужен.
      2) Legacy: ML_EDGE_STACK_OOF_FEATURE_SCHEMA_VER не задан →
         используется feature_cols.json (старый путь).
    """
    if int(os.getenv("EDGE_STACK_BUNDLE_ENABLED", "1") or 1) == 1:
        # P59 bundle is enabled; avoid duplicate dataset/train steps.
        return False
    dataset = os.getenv("ML_EDGE_STACK_OOF_DATASET_PATH", "/var/lib/trade/ml_models/edge_stack_v1_oof/edge_train.jsonl")
    if not os.path.exists(dataset):
        return False

    # Читаем schema_ver (приоритет: специфичный → общий ML_FEATURE_SCHEMA_VER)
    schema_ver = os.getenv(
        "ML_EDGE_STACK_OOF_FEATURE_SCHEMA_VER",
        os.getenv("FEATURE_SCHEMA_VER", os.getenv("ML_FEATURE_SCHEMA_VER", "")),
    ),
    schema_ver = (schema_ver or "").strip()  # type: ignore
  # type: ignore
    feature_cols = os.getenv("ML_EDGE_STACK_OOF_FEATURE_COLS_JSON", "/var/lib/trade/ml_models/edge_stack_v1_oof/feature_cols.json")
    # Если schema_ver задан — тренируем без feature_cols.json (registry-derived columns)
    if not schema_ver:
        if not os.path.exists(feature_cols):
            return False

    # Опциональный report.json от builder-а (для hash-check)
    report_json = os.getenv("ML_EDGE_STACK_OOF_DATASET_REPORT_JSON", "")
    if report_json and not os.path.exists(report_json):
        report_json = ""

    out_model = os.path.join(
        os.getenv("ML_EDGE_STACK_OOF_MODELS_ROOT", "/var/lib/trade/ml_models"),
        "edge_stack_v1_oof",
        f"edge_stack_v1_{datetime.now().strftime('%Y%m%d_%H%M%S')}.joblib",
    ),
    args = [
        "--data_jsonl", dataset,
        "--out_model", out_model,
        "--n_splits", os.getenv("ML_EDGE_STACK_OOF_N_SPLITS", "5"),
        "--purge_ms", os.getenv("ML_EDGE_STACK_OOF_PURGE_MS", "300000"),
        "--embargo_ms", os.getenv("ML_EDGE_STACK_OOF_EMBARGO_MS", "300000"),
        "--min_train", os.getenv("ML_EDGE_STACK_OOF_MIN_TRAIN", "500"),
        "--lr_C", os.getenv("ML_EDGE_STACK_OOF_LR_C", "1.0"),
        "--gbdt_max_depth", os.getenv("ML_EDGE_STACK_OOF_GBDT_MAX_DEPTH", "3"),
        "--gbdt_learning_rate", os.getenv("ML_EDGE_STACK_OOF_GBDT_LR", "0.05"),
        "--gbdt_max_iter", os.getenv("ML_EDGE_STACK_OOF_GBDT_MAX_ITER", "400"),
        "--calibrate", os.getenv("ML_EDGE_STACK_OOF_CALIBRATE", "1"),
    ]

    if schema_ver:
        # Registry-режим: передаём schema_ver и связанные параметры
        args += [
            "--feature_schema_ver", schema_ver,
            "--scenario_prefix", os.getenv("ML_EDGE_STACK_OOF_SCENARIO_PREFIX", "bucket:"),
            "--include_time_onehot", os.getenv("ML_EDGE_STACK_OOF_INCLUDE_TIME_ONEHOT", "1"),
            "--require_feature_registry", os.getenv("ML_EDGE_STACK_OOF_REQUIRE_REGISTRY", "1"),
        ]
        if report_json:
            args += ["--dataset_report_json", report_json]
    else:
        # Legacy-режим: передаём feature_cols.json напрямую
        args += ["--feature_cols_json", feature_cols]

    return run_tool("ml_analysis.tools.train_edge_stack_v1_oof", args, timeout=3600)


def run_side_sign_audit() -> bool:
    """Run side sign audit (04:00)."""
    audit_dir = os.getenv("SIDE_SIGN_AUDIT_PATCH_DIR", "/tmp")
    audit_json = os.path.join(audit_dir, f"side_audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    # Note: needs --root /repo which we assume is current dir or mapped.
    # In docker-compose it was --root /repo.
    run_tool("tools.audit_side_sign_usage", ["--root", ".", "--format", "json"], timeout=600) # Output to stdout, maybe redirected in shell
    # The shell command was: python3 -m ... > $$AUDIT_JSON
    # We'll just run it with defaults for now.
    return run_tool("tools.audit_side_sign_usage", ["--root", ".", "--format", "text"], timeout=600)



def run_of_gate_dlq_triage() -> bool:
    """Run OF-Gate DLQ Triage (P84) (Hourly)."""
    if os.getenv("ENABLE_OF_GATE_DLQ_TRIAGE_TIMER", "0") != "1":
        return True

    args = [
        "triage",
        "--limit", os.getenv("OF_GATE_DLQ_TRIAGE_LIMIT", "5000"),
        "--notify"
    ]
    return run_tool("orderflow_services.of_gate_dlq_fixed_replay_p84", args, timeout=600)


def run_of_gate_dlq_auto_replay() -> bool:
    """Run OF-Gate DLQ auto triage+safe replay (P3) (Hourly).

    Enabled by: ENABLE_OF_GATE_DLQ_AUTO_REPLAY_TIMER=1

    Safety controls:
      - OF_GATE_DLQ_AUTO_REPLAY_SAFE_START_UTC / _SAFE_END_UTC (optional guard)
      - Redis/file lock to avoid concurrent runs
      - Restricted fix allowlist (OF_GATE_DLQ_AUTO_ALLOW_FIXES)
      - require-fix enabled by default (skip entries that need no fixes)

    Writes a short summary to notify:telegram when --notify is enabled.
    """
    if os.getenv("ENABLE_OF_GATE_DLQ_AUTO_REPLAY_TIMER", "0") != "1":
        return True

    # Optional safe-window (UTC)
    safe_start = os.getenv("OF_GATE_DLQ_AUTO_REPLAY_SAFE_START_UTC", "")
    safe_end = os.getenv("OF_GATE_DLQ_AUTO_REPLAY_SAFE_END_UTC", "")
    if safe_start and safe_end:
        try:
            now = datetime.utcnow()
            sh, sm = [int(x) for x in safe_start.split(":", 1)]
            eh, em = [int(x) for x in safe_end.split(":", 1)]
            cur = now.hour * 60 + now.minute
            a = sh * 60 + sm
            b = eh * 60 + em
            in_win = (a <= cur <= b) if a <= b else (cur >= a or cur <= b)
            if not in_win:
                logger.info("DLQ auto replay: outside safe-window, skipping")
                return True
        except Exception:
            pass

    timeout_s = int(os.getenv("OF_GATE_DLQ_AUTO_REPLAY_TIMEOUT_S", "900"))

    # Best-effort lock (Redis SET NX EX; fallback file lock)
    lock_key = os.getenv("OF_GATE_DLQ_AUTO_REPLAY_LOCK_KEY", "lock:of_gate_dlq_auto_replay")
    lock_ttl = int(os.getenv("OF_GATE_DLQ_AUTO_REPLAY_LOCK_TTL_S", str(timeout_s + 60)))
    got_lock = False
    lock_file = os.getenv("OF_GATE_DLQ_AUTO_REPLAY_LOCK_FILE", "/tmp/of_gate_dlq_auto_replay.lock")
    try:
        import redis  # type: ignore
        r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
        got_lock = bool(r.set(lock_key, str(int(time.time())), nx=True, ex=lock_ttl))
    except Exception:
        try:
            fd = os.open(lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(int(time.time())).encode("utf-8"))
            os.close(fd)
            got_lock = True
        except Exception:
            got_lock = False

    if not got_lock:
        logger.info("DLQ auto replay: lock busy, skipping")
        return True

    args = [
        "auto",
        "--max-per-stream", os.getenv("OF_GATE_DLQ_AUTO_MAX_PER_STREAM", "2000"),
        "--allow-fixes", os.getenv(
            "OF_GATE_DLQ_AUTO_ALLOW_FIXES",
            "add_schema_name,add_schema_version,coerce_schema_version_int,normalize_ts_ms,ts_from_stream_id,default_missing_legs_empty,coerce_missing_legs_to_json,stringify_missing_legs",
        ),
    ]

    streams = os.getenv("OF_GATE_DLQ_STREAMS", "")
    if streams.strip():
        args += ["--streams", streams]

    target = os.getenv("OF_GATE_DLQ_AUTO_TARGET_STREAM", "")
    if target.strip():
        args += ["--target", target]

    if os.getenv("OF_GATE_DLQ_AUTO_REPLAY_REQUIRE_FIX", "1") == "1":
        args += ["--require-fix"]

    if os.getenv("OF_GATE_DLQ_AUTO_REPLAY_COMMIT", "1") == "1":
        args += ["--commit"]

    if os.getenv("OF_GATE_DLQ_AUTO_REPLAY_DELETE_AFTER", "1") == "1":
        args += ["--delete-after-replay"]

    if os.getenv("OF_GATE_DLQ_AUTO_REPLAY_NOTIFY", "1") == "1":
        args += ["--notify"]

    ok = run_tool("orderflow_services.of_gate_dlq_fixed_replay_p84", args, timeout=timeout_s)

    # Release file lock (Redis lock expires by TTL)
    try:
        if os.path.exists(lock_file):
            os.remove(lock_file)
    except Exception:
        pass

    return ok


def main() -> None:  # type: ignore
    """Main loop."""
    logger.info("OF Timers Worker starting...")
    logger.info("Monitors: Config drift (hourly :15)")
    logger.info("Monitors: OF Gate contract smoke-check (hourly :08)")
    logger.info("Monitors: Prometheus rules bundle smoke-check (hourly :09)")
    logger.info("Monitors: OF Gate DLQ auto replay (hourly :44)")
    logger.info("Monitors: Confidence cal live health (hourly :45)")
    logger.info("Monitors: OFInputs DLQ auto replay (hourly :52) [ENABLE_OF_INPUTS_DLQ_AUTO_REPLAY=1]")
    logger.info("Monitors: ATR Policy Bootstrap (Startup + Nightly 03:30)")
    logger.info("Monitors: ATR Policy Consistency Check (Every 15 min at :04)")
    logger.info("Weekly: Latency bench (Sun 06:40)")
    logger.info("Nightly:")
    logger.info("  01:30 - Meta AB-winner V2 Job")
    logger.info("  02:55 - OF Gate DLQ DB archive (optional)")
    logger.info("  03:10 - Archive signals")
    logger.info("  03:12 - Archive trades:closed")
    logger.info("  03:40 - Archive retention+manifest")
    logger.info("  03:46 - OF-gate Rollups Refresh (guarded)")
    logger.info("  03:50 - Edge-stack dataset build")
    logger.info("  04:05 - Edge Stack Train")
    logger.info("  04:12 - Edge Stack Shadow Eval")
    logger.info("  04:15 - Feature Selection Loop")
    logger.info("  04:20 - Regression Safe")
    logger.info("  04:50 - Code Audit")
    logger.info("  05:10 - Meta-model Train")
    logger.info("  05:20 - ML Calib Health")
    logger.info("  05:25 - Confidence Calibrator")
    logger.info("  05:30 - Nightly Calibration")
    logger.info("  05:32 - Slippage Calibrator")
    logger.info("  05:35 - Nightly Conf Cal V2 (New)")
    logger.info("  05:40 - Archive Maintenance")
    logger.info("  05:55 - Meta AB v2 Winner Eval")
    logger.info("  06:05 - Confirmations Coverage Nightly")
    logger.info("  06:10 - Enforce Bucket Promoter [ENABLE_ENFORCE_BUCKET_PROMOTER=1]")

    logger.info("  06:20 - Meta Enforce Ramp")
    logger.info("  07:10 - Meta Self-Heal")
    logger.info("  07:25 - Meta Stage2 Opt")
    logger.info("  07:40 - Close Backfill")

    # State tracking to prevent double-runs
    last_run = {}

    # Phase 4 Bootstrap on startup
    try:
        from services.atr_policy_bootstrap_service import run_bootstrap
        logger.info("Running ATR Policy Bootstrap on startup...")
        run_bootstrap()
    except ImportError as e:
        logger.warning(f"Could not import ATR Policy bootstrap: {e}")
    except Exception as e:
        logger.error(f"ATR Policy bootstrap failed: {e}")

    while True:
        try:
            now = datetime.utcnow()
            hour = now.hour
            minute = now.minute
            weekday = now.weekday()  # 0=Monday, 6=Sunday

            def should_run(name: str, h: int, m_start: int, m_end: int = None, wd: int = None) -> bool:  # type: ignore
                if wd is not None and weekday != wd:  # type: ignore
                    return False
                if hour != h:
                    return False
                if m_end is None:
                    m_end = m_start + 1
                if minute < m_start or minute >= m_end:
                    return False

                # Check last run time
                last = last_run.get(name, 0)
                if now.timestamp() - last < 120:  # Prevent re-run within 2 mins
                    return False
                return True

            # Hourly:08 OF gate metrics contract smoke-check (P76: writes to sre:of_gate_contract_smoke)
            # exit=0 OK, exit=2 ALERT (bad_share / schema-missing above threshold)
            if minute >= 8 and minute < 9:
                last = last_run.get("of_gate_contract_smoke", 0)
                if now.timestamp() - last > 3500:
                    run_of_gate_contract_smoke_check()
                    last_run["of_gate_contract_smoke"] = now.timestamp()

            # Phase 4: ATR Policy State Consistency check (every 15 min at offset :04)
            if (minute - 4) % 15 == 0:
                last = last_run.get("atr_policy_consistency", 0)
                if now.timestamp() - last > 600:
                    try:
                        from services.atr_policy_state_consistency_checker import run_consistency_check
                        run_consistency_check()
                    except ImportError as e:
                        logger.warning(f"Could not import ATR Policy consistency checker: {e}")
                    except Exception as e:
                        logger.error(f"ATR Policy consistency checker failed: {e}")
                    last_run["atr_policy_consistency"] = now.timestamp()

            # Hourly:09 Prometheus rules bundle smoke-check (writes state:prom_rules_bundle:*)
            if minute >= 9 and minute < 10:
                last = last_run.get("prom_rules_bundle_smoke", 0)
                if now.timestamp() - last > 3500:
                    run_prom_rules_bundle_smoke_check()
                    last_run["prom_rules_bundle_smoke"] = now.timestamp()

            # Hourly:10 Prometheus rules loaded probe (writes state:prom_rules_loaded:*)
            if minute >= 10 and minute < 11:
                last = last_run.get("prom_rules_loaded_probe", 0)
                if now.timestamp() - last > 3500:
                    run_prom_rules_loaded_probe()
                    last_run["prom_rules_loaded_probe"] = now.timestamp()

            # Hourly:11 World-practice trackers smoke-check (bucket/vol/res/fill)
            if minute >= 11 and minute < 12:
                last = last_run.get("world_practice_smoke", 0)
                if now.timestamp() - last > 3500:
                    run_world_practice_smoke_check()
                    last_run["world_practice_smoke"] = now.timestamp()



            # Hourly:13 LOB-pressure smoke-check (P91)
            # exit=0 OK (or no_data), exit=2 ALERT (missing/invalid/stuck)
            if minute >= 13 and minute < 14:
                last = last_run.get("lob_pressure_smoke", 0)
                if now.timestamp() - last > 3500:
                    run_lob_pressure_smoke_check()
                    last_run["lob_pressure_smoke"] = now.timestamp()

            # Hourly:14 OFInputs exporters wiring smoke-check (P107)
            # P109: fail-closed — sets auto-apply block on failure, clears on recovery.
            # Controlled by ENABLE_OF_INPUTS_EXPORTERS_SMOKE_P107=1|0 (default: 1)
            if minute >= 14 and minute < 15:
                # Run P107
                last = last_run.get("of_inputs_exporters_smoke", 0)
                if now.timestamp() - last > 3500:
                    run_of_inputs_exporters_smoke_p107()
                    last_run["of_inputs_exporters_smoke"] = now.timestamp()

                # Run P111
                last_p111 = last_run.get("of_gate_exporters_smoke_p111", 0)
                if now.timestamp() - last_p111 > 3500:
                    run_of_gate_exporters_smoke_p111()
                    last_run["of_gate_exporters_smoke_p111"] = now.timestamp()


            # Hourly:12 Feature Registry contract smoke-check (P94: schema_hash / feature_cols_hash pinning)
            # exit=0 OK, exit=2 ALERT (mismatch or pins missing)
            if minute >= 12 and minute < 13:
                last = last_run.get("feature_registry_contract_smoke", 0)
                if now.timestamp() - last > 3500:
                    run_feature_registry_contract_smoke_check()
                    last_run["feature_registry_contract_smoke"] = now.timestamp()


            if minute >= 15 and minute < 16:
                last = last_run.get("config_drift", 0)
                if now.timestamp() - last > 3500:
                    run_config_drift_monitor()
                    last_run["config_drift"] = now.timestamp()

            # Hourly:16 A8 new-features smoke-check (NaN rate + realized_vol stuck)
            if minute >= 16 and minute < 17:
                last = last_run.get("a8_new_features_smoke", 0)
                if now.timestamp() - last > 3500:
                    run_new_features_smoke_check_a8()
                    last_run["a8_new_features_smoke"] = now.timestamp()

            # Hourly:45 live health loop
            if minute >= 45 and minute < 46:
                last = last_run.get("conf_cal_live_health", 0)
                if now.timestamp() - last > 3500:
                    run_confidence_cal_live_health()
                    last_run["conf_cal_live_health"] = now.timestamp()

            # Hourly:52 OFInputs DLQ auto replay (P97)
            if minute >= 52 and minute < 53:
                last = last_run.get("of_inputs_dlq_replay", 0)
                if now.timestamp() - last > 3500:
                    run_of_inputs_dlq_auto_replay()
                    last_run["of_inputs_dlq_replay"] = now.timestamp()

            # Hourly:00 OFC Bench + Replay
            if minute >= 0 and minute < 1:
                last = last_run.get("ofc_hourly", 0)
                if now.timestamp() - last > 3500:
                    run_ofc_bench()
                    run_ofc_replay_capture()
                    run_ofc_golden_replay()
                    last_run["ofc_hourly"] = now.timestamp()

            # Every 30 min: Edge Stack Shadow Eval
            if minute % 30 == 0:
                last = last_run.get("edge_stack_shadow_30m", 0)
                if now.timestamp() - last > 1700:
                    run_edge_stack_shadow_eval()
                    last_run["edge_stack_shadow_30m"] = now.timestamp()

            # Every 15 min: Factor cluster update
            if minute % 15 == 0:
                last = last_run.get("atr_policy_factor_cluster", 0)
                if now.timestamp() - last > 800:
                    run_atr_policy_factor_cluster()
                    last_run["atr_policy_factor_cluster"] = now.timestamp()

            # Every minute: Regime/Stress Classifier
            last_rs = last_run.get("atr_policy_regime_stress", 0)
            if now.timestamp() - last_rs >= 60:
                run_atr_policy_regime_stress_state()
                last_run["atr_policy_regime_stress"] = now.timestamp()

            # Every 30 min: Regime/Stress TG Digest
            if minute % 30 == 15:
                last_rs_tg = last_run.get("atr_policy_regime_stress_tg", 0)
                if now.timestamp() - last_rs_tg > 1700:
                    run_atr_policy_regime_stress_tg_digest()
                    last_run["atr_policy_regime_stress_tg"] = now.timestamp()

            # Hourly:05 Missing Leg Report
            if minute >= 5 and minute < 6:
                last = last_run.get("missing_leg_report", 0)
                if now.timestamp() - last > 3500:
                    run_of_gate_missing_leg_report()
                    last_run["missing_leg_report"] = now.timestamp()

            # P49: Feature Drift Monitor (hourly at :25)
            # Checks env ENABLE_FEATURE_DRIFT_MONITOR=1 inside
            if minute >= 25 and minute < 26:
                last = last_run.get("feature_drift_monitor", 0)
                if now.timestamp() - last > 3500:
                    if os.getenv("ENABLE_FEATURE_DRIFT_MONITOR", "0") == "1":
                        run_feature_drift_monitor()
                    last_run["feature_drift_monitor"] = now.timestamp()

            # Every 5 min: Meta Cov Ops
            if minute % 5 == 0:
                last = last_run.get("meta_cov_ops", 0)
                if now.timestamp() - last > 280:
                    run_meta_enforce_cov_ops_bundle()
                    last_run["meta_cov_ops"] = now.timestamp()


            # Every 10 min: Enforce bucket rollback controller
            if minute % 10 == 7:
                last = last_run.get("enforce_bucket_rollback", 0)
                if now.timestamp() - last > 550:
                    if os.getenv("ENABLE_ENFORCE_BUCKET_ROLLBACK", "0") == "1":
                        run_enforce_bucket_promoter_rollback_controller()
                    last_run["enforce_bucket_rollback"] = now.timestamp()

            # Every 2 hours: Tick Time Lag Report
            if hour % 2 == 0 and minute == 0:
                last = last_run.get("tick_time_lag", 0)
                if now.timestamp() - last > 3500:
                    run_tick_time_lag_report()
                    last_run["tick_time_lag"] = now.timestamp()

            # Every 4 hours: Tick Time Autotune
            if hour % 4 == 0 and minute == 0:
                last = last_run.get("tick_time_autotune", 0)
                if now.timestamp() - last > 3500:
                    run_tick_time_autotune()
                    last_run["tick_time_autotune"] = now.timestamp()

            # Every 6 hours: ML Drift Monitor V1
            if hour % 6 == 0 and minute == 0:
                last = last_run.get("ml_drift_monitor_v1", 0)
                if now.timestamp() - last > 3500:
                    run_ml_drift_monitor_v1()
                    last_run["ml_drift_monitor_v1"] = now.timestamp()

            # P80: Every 30 min: refresh exec slippage stats MV
            if minute % 30 == 11:
                last = last_run.get("exec_slip_stats_refresh", 0)
                if now.timestamp() - last > 1700:
                    if os.getenv("ENABLE_EXEC_SLIP_STATS_REFRESHER", "0") == "1":
                        run_tool("orderflow_services.refresh_exec_slip_stats_p80", timeout=600)
                    last_run["exec_slip_stats_refresh"] = now.timestamp()

            # P80: Every 10 min: SLO freeze guard (no rollback; just block apply)
            if minute % 10 == 3:
                last = last_run.get("enforce_bucket_slo_freezer", 0)
                if now.timestamp() - last > 550:
                    if os.getenv("ENABLE_ENFORCE_BUCKET_SLO_FREEZER", "0") == "1":
                        run_tool("orderflow_services.enforce_bucket_slo_freezer_p80", timeout=300)
                    last_run["enforce_bucket_slo_freezer"] = now.timestamp()


            # P77: Hourly:13 Exec slippage eval rowcount probe (v_exec_slippage_eval)
            if minute >= 13 and minute < 14:
                last = last_run.get("exec_slip_eval_rowcount_probe", 0)
                if now.timestamp() - last > 3500:
                    run_exec_slippage_eval_rowcount_probe()
                    last_run["exec_slip_eval_rowcount_probe"] = now.timestamp()

            # Ensemble: Hourly:15 per-source Sharpe weight recalculation
            if minute >= 15 and minute < 16:
                last = last_run.get("ensemble_weight_calibration", 0)
                if now.timestamp() - last > 3500:
                    run_ensemble_weight_calibration()
                    last_run["ensemble_weight_calibration"] = now.timestamp()

            # Sunday
            if should_run("weekly_bench", 6, 40, wd=6):
                run_weekly_bench()
                last_run["weekly_bench"] = now.timestamp()

            # Daily Nightly
            tasks = [
                ("of_gate_dlq_db_archive", 2, 55, run_of_gate_dlq_db_archive_nightly),
                ("archive_signals", 3, 10, run_archive_signals_of_inputs),
                ("archive_trades_closed", 3, 12, run_archive_trades_closed),
                ("horizon_profile_bootstrap", 3, 18, run_horizon_profile_bootstrap),
                ("live_surface_ab", 3, 26, run_live_surface_ab),
                ("trailing_surface_ab", 3, 28, run_trailing_surface_ab),
                ("atr_policy_bootstrap", 3, 30, run_atr_policy_bootstrap),
                ("atr_promotion_policy", 3, 32, run_atr_promotion_policy),
                ("atr_policy_reconcile", 3, 36, run_atr_policy_reconcile),
                ("atr_policy_tg_pack", 8, 10, run_atr_policy_telegram_pack),
                ("atr_policy_sre_digest", 8, 20, run_atr_policy_sre_digest),
                ("atr_policy_bootstrap_audit", 8, 35, run_atr_policy_bootstrap_audit),
                ("atr_policy_state_drift_check", 8, 50, run_atr_policy_state_drift_check),
                ("atr_policy_full_recovery_audit", 9, 5, run_atr_policy_full_recovery_audit),
                ("atr_policy_restore_cert_audit", 9, 15, run_atr_policy_restore_cert_audit),
                ("atr_policy_restore_cert_execute", 10, 5, run_atr_policy_restore_cert_execute),
                ("atr_policy_analytics_daily", 10, 20, run_atr_policy_analytics_daily),
                ("atr_policy_analytics_tg_digest", 10, 30, run_atr_policy_analytics_tg_digest),
                ("atr_policy_portfolio_tg_digest", 10, 45, run_atr_policy_portfolio_tg_digest),
                ("archive_decisions", 3, 14, run_decisions_archive),
                ("ofc_golden", 3, 15, run_ofc_golden_pipeline),
                ("archive_prune", 3, 40, run_archive_inventory_prune),
                ("of_gate_rollups_refresh", 3, 46, run_of_gate_rollups_refresh_nightly),
                ("edge_dataset_build", 3, 50, run_edge_stack_dataset_build),
                ("side_sign_audit", 4, 0, run_side_sign_audit),
                ("feature_selection_loop", 4, 2, run_feature_selection_loop_bundle_v1),
                ("edge_stack_v1_train", 4, 10, run_ml_train_edge_stack_v1),
                ("edge_stack_shadow", 4, 12, run_edge_stack_shadow_eval),
                ("feature_selection_loop", 4, 15, run_feature_selection_loop_v1),
                ("edge_stack_mh_train", 4, 20, run_ml_train_edge_stack_mh_v1),
                ("regress_safe", 4, 20, run_nightly_regress_safe),
                # P59 bundle: dataset+validate+train+promote in one step (05:10)
                # When EDGE_STACK_BUNDLE_ENABLED=1 (default), old 05:00/05:10 steps are no-ops.
                ("edge_stack_v1_bundle_p59", 5, 10, run_nightly_edge_stack_train),
                ("edge_stack_v1_dataset", 5, 0, run_edge_stack_v1_dataset_build_fallback),
                ("edge_stack_v1_oof_train", 5, 10, run_ml_train_edge_stack_v1_oof),
                ("strategy_research_guard", 4, 35, run_strategy_research_guard_bundle),
                ("strategy_research_stats", 4, 35, run_strategy_research_stats_bundle),
                ("code_audit", 4, 50, run_code_audit),
                ("meta_train", 5, 10, run_nightly_meta_train),
                ("ml_calib_health", 5, 20, run_ml_calibration_health),
                ("conf_cal", 5, 25, run_confidence_calibrator),
                ("nightly_calib", 5, 30, run_nightly_calibration),
                ("slippage_calib", 5, 32, run_nightly_slippage_calibrator),
                ("tca_nightly_report", 5, 34, run_nightly_tca_report),
                ("conf_cal_v2", 5, 35, run_nightly_confidence_calibrator_v2),
                ("archive_maint", 5, 40, run_archive_maintenance),
                ("meta_ab_v2", 5, 55, run_meta_ab_v2_nightly_job_v1),
                ("confirmations_coverage", 6, 5, run_confirmations_coverage_nightly),
                ("enforce_bucket_promoter", 6, 10, run_nightly_enforce_bucket_promoter),
            ("feature_denylist_proposal_autogen", 6, 25, run_nightly_feature_denylist_proposal_autogen),
                ("meta_ramp", 6, 20, run_nightly_meta_enforce_ramp),
                ("meta_self_heal", 7, 10, run_nightly_meta_self_heal),
                ("meta_stage2_opt", 7, 25, run_nightly_meta_stage2_opt),
                ("close_backfill", 7, 40, run_close_backfill),
            ]

            for name, h, m, func in tasks:
                if should_run(name, h, m):
                    func()
                    last_run[name] = now.timestamp()

            # P48: Signal Quality KPI (every 15 mins at :02)
            # Checks env ENABLE_SIGNAL_QUALITY_KPI_TIMER=1 inside
            if minute >= 2 and minute < 3:
                # We use a 15-min modulo check if we wanted strict 02, 17, 32, 47
                # To run every 15m at offset 2: 2, 17, 32, 47.
                if (minute - 2) % 15 == 0:
                    last = last_run.get("signal_quality_kpi", 0)
                    if now.timestamp() - last > 600:
                         if os.getenv("ENABLE_SIGNAL_QUALITY_KPI_TIMER", "0") == "1":
                             run_signal_quality_kpis()
                         last_run["signal_quality_kpi"] = now.timestamp()

            # P2: OF Gate SRE monitor (every 15 minutes at offset :08)
            # Fires at minutes 8, 23, 38, 53.
            # Enabled by: ENABLE_OF_GATE_SRE_MONITOR_TIMER=1
            if (minute - 8) % 15 == 0:
                last = last_run.get("of_gate_sre_monitor", 0)
                if now.timestamp() - last > 600:  # min 10m between runs
                    if os.getenv("ENABLE_OF_GATE_SRE_MONITOR_TIMER", "0") == "1":
                        run_of_gate_sre_monitor()
                    last_run["of_gate_sre_monitor"] = now.timestamp()

            # P80: Hourly:52 Rollups Freshness Probe (DB view max(bucket) → Redis hash)
            if minute >= 52 and minute < 53:
                last = last_run.get("rollups_freshness_probe", 0)
                if now.timestamp() - last > 3500:
                    run_of_gate_rollups_freshness_probe()
                    last_run["rollups_freshness_probe"] = now.timestamp()

            # P81: Hourly:37 Timescale Policies Probe (jobs/policies existence)
            if minute >= 37 and minute < 38:
                last = last_run.get("timescale_policy_probe", 0)
                if now.timestamp() - last > 3500:
                    run_of_gate_timescale_policy_probe()
                    last_run["timescale_policy_probe"] = now.timestamp()



            # P84: Hourly:42 OF Gate DLQ Triage
            if minute >= 42 and minute < 43:
                last = last_run.get("of_gate_dlq_triage", 0)
                if now.timestamp() - last > 3500:
                    run_of_gate_dlq_triage()
                    last_run["of_gate_dlq_triage"] = now.timestamp()


            # P3: Hourly:44 OF Gate DLQ Auto Replay (safe, restricted)
            if minute >= 44 and minute < 45:
                last = last_run.get("of_gate_dlq_auto_replay", 0)
                if now.timestamp() - last > 3500:
                    run_of_gate_dlq_auto_replay()
                    last_run["of_gate_dlq_auto_replay"] = now.timestamp()

            # P5.6: Incremental orchestration composite preflight rollup every 5 minutes.
            # Advances Redis Stream cursor and updates hourly/daily bucket counters.
            # Guarded by ENABLE_ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_ROLLUP=1.
            if minute % 5 == 1:
                last = last_run.get("orchestration_preflight_history_rollup", 0)
                if now.timestamp() - last > 240:
                    run_orchestration_composite_preflight_history_rollup()
                    last_run["orchestration_preflight_history_rollup"] = now.timestamp()

            # P99: Hourly:46 OFInputs DLQ DB drilldown (reads of_inputs_dlq_events)
            # Guarded by ENABLE_OF_INPUTS_DLQ_DB_DRILLDOWN=1 + TRADES_DB_DSN
            # P104: Feature denylist proposal exporter (textfile collector for node_exporter)
            # Guarded by ENABLE_FEATURE_DENYLIST_EXPORTER=1 + FEATURE_DENYLIST_EXPORT_PATH
            if minute >= 46 and minute < 47:
                last = last_run.get("of_inputs_dlq_db_drilldown", 0)
                if now.timestamp() - last > 3500:
                    run_of_inputs_dlq_db_drilldown_p99()
                    last_run["of_inputs_dlq_db_drilldown"] = now.timestamp()
                last = last_run.get("feature_denylist_exporter", 0)
                if now.timestamp() - last > 3500:
                    run_feature_denylist_proposal_exporter()
                    last_run["feature_denylist_exporter"] = now.timestamp()

            # P5.5: Hourly:49 orchestration composite preflight history exporter
            # Writes node_exporter textfile rollup for 24h/7d block/invalid frequency by
            # purpose/source/reason_code. Guarded by ENABLE_ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_EXPORTER=1.
            if minute >= 49 and minute < 50:
                last = last_run.get("orchestration_composite_preflight_history_exporter", 0)
                if now.timestamp() - last > 3500:
                    run_orchestration_composite_preflight_history_exporter()
                    last_run["orchestration_composite_preflight_history_exporter"] = now.timestamp()

            # P5.6: Hourly textfile export from Redis-side buckets for node_exporter.
            # Reads pre-aggregated hourly/daily bucket hashes, writes .prom file.
            # Guarded by ENABLE_ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_TEXTFILE_EXPORTER=1.
            if minute >= 49 and minute < 50:
                last = last_run.get("orchestration_preflight_history_textfile_exporter", 0)
                if now.timestamp() - last > 3500:
                    run_orchestration_composite_preflight_history_textfile_exporter()
                    last_run["orchestration_preflight_history_textfile_exporter"] = now.timestamp()

            # P5.7: Hourly:52 consistency check + textfile export for rollup drift detection.
            # Scans bounded stream range, compares with Redis buckets, writes report + .prom.
            # Guarded by ENABLE_ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_CONSISTENCY_CHECK=1.
            if minute >= 52 and minute < 53:
                last = last_run.get("orchestration_preflight_history_consistency_check", 0)
                if now.timestamp() - last > 3500:
                    run_orchestration_composite_preflight_history_consistency_check()
                    last_run["orchestration_preflight_history_consistency_check"] = now.timestamp()

            # P98: Hourly:57 OFInputs DLQ+quarantine → Postgres/Timescale archiver
            # Guarded by ENABLE_OF_INPUTS_DLQ_DB_ARCHIVE_P98=1
            if minute >= 57 and minute < 58:
                last = last_run.get("of_inputs_dlq_db_archive_p98", 0)
                if now.timestamp() - last > 3500:
                    run_of_inputs_dlq_db_archive_p98()
                    last_run["of_inputs_dlq_db_archive_p98"] = now.timestamp()

            time.sleep(30)

        except KeyboardInterrupt:
            logger.info("Received interrupt, shutting down...")
            break
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
            time.sleep(60)


if __name__ == "__main__":
    main()
