from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""Redis Stream -> Timescale writer for OFC contextual decision records.

Consumes `decisions:final` (or a dedicated stream if configured), extracts ctx_* fields
from DecisionRecordV1 payloads, and persists rows into `ofc_contextual_decisions`.
The service is fail-open relative to trading path.
"""

import asyncio
import json
import logging
import os
import socket
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

try:
    import redis.asyncio as aioredis  # type: ignore
except Exception:  # pragma: no cover
    aioredis = None

from services.orderflow.ofc_contextual_decision_writer_metrics import build_metrics, start_metrics_server

logger = logging.getLogger("ofc_contextual_decision_writer")


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(_env(name, str(default))))
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        return float(_env(name, str(default)))
    except Exception:
        return float(default)


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return bool(default)
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


def pick_dsn() -> str:
    return (
        (os.getenv("ANALYTICS_DB_DSN") or os.getenv("TRADES_DB_DSN"))
        or os.getenv("TIMESCALE_DSN")
        or os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("ANALYTICS_DSN")
        or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("PG_DSN"))
        or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL"))
        or ""
    )


def _now_ms() -> int:
    return get_ny_time_millis()


def _decode(v: Any) -> Any:
    if isinstance(v, (bytes, bytearray)):
        try:
            return v.decode("utf-8", "replace")
        except Exception:
            return str(v)
    return v


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return int(default)


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _to_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "on"}:
        return True
    if s in {"0", "false", "no", "off"}:
        return False
    return default


def _loads_json(v: Any) -> Optional[dict]:
    if v is None:
        return None
    if isinstance(v, dict):
        return v
    v = _decode(v)
    if not isinstance(v, str):
        v = str(v)
    s = v.strip()
    if not s:
        return None
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _parse_stream_fields(fields: Dict[Any, Any]) -> Dict[str, Any]:
    if b"payload" in fields:
        return _loads_json(fields.get(b"payload")) or {}
    if "payload" in fields:
        return _loads_json(fields.get("payload")) or {}
    out: Dict[str, Any] = {}
    for k, v in fields.items():
        out[str(_decode(k))] = _decode(v)
    return out


def _extract_ctx_source(evt: Dict[str, Any]) -> Dict[str, Any]:
    ofc = evt.get("of_confirm") if isinstance(evt.get("of_confirm"), dict) else {}
    ev = ofc.get("evidence") if isinstance(ofc.get("evidence"), dict) else {}
    return ev if ev else evt


def _normalize_row(evt: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
    src = _extract_ctx_source(evt)
    sid = str(evt.get("sid") or evt.get("signal_id") or "").strip()
    symbol = str(evt.get("symbol") or evt.get("sym") or "").strip()
    decision_ts_ms = _to_int(evt.get("decision_ts_ms") or evt.get("ts_ms") or evt.get("ts_emit_ms"), 0)
    if not sid:
        return None, "missing:sid"
    if not symbol:
        return None, "missing:symbol"
    if decision_ts_ms <= 0:
        return None, "bad:decision_ts_ms"

    ctx_enabled = _to_bool(src.get("ctx_enable") if "ctx_enable" in src else evt.get("ctx_enabled"), False)
    # Accept records even if ctx layer ran in shadow-only, but skip completely empty non-ctx rows.
    has_any_ctx = ctx_enabled or any(k.startswith("ctx_") for k in list(evt.keys()) + list(src.keys()))
    if not has_any_ctx:
        return None, "skip:no_ctx"

    row = {
        "decision_ts_ms": decision_ts_ms,
        "sid": sid,
        "symbol": symbol,
        "direction": str(evt.get("direction") or evt.get("side") or ""),
        "session": str(evt.get("ctx_session") or evt.get("session") or ""),
        "dow": _to_int(evt.get("ctx_dow") or evt.get("dow"), 0),
        "hour_utc": _to_int(evt.get("ctx_hour_utc") or evt.get("hour_utc"), 0),
        "scenario_v4": str(evt.get("scenario_v4") or ""),
        "legacy_rule_score": _to_float(evt.get("of_score_final") or evt.get("raw_score") or evt.get("score"), 0.0),
        "legacy_rule_ok": _to_bool(evt.get("ok"), False),
        "legacy_reason": str(evt.get("reason") or ""),
        "ctx_enabled": bool(ctx_enabled),
        "ctx_mode": str(src.get("ctx_mode") or evt.get("ctx_mode") or "off"),
        "ctx_key": str(src.get("ctx_key") or evt.get("ctx_key") or ""),
        "ctx_bundle_ver": str(src.get("ctx_bundle_ver") or evt.get("ctx_bundle_ver") or ""),
        "ctx_p_rule_raw": _to_float(src.get("ctx_p_rule_raw") if src.get("ctx_p_rule_raw") is not None else evt.get("ctx_p_rule_raw"), 0.0),
        "ctx_p_rule_cal": _to_float(src.get("ctx_p_rule_cal") if src.get("ctx_p_rule_cal") is not None else evt.get("ctx_p_rule_cal"), 0.0),
        "ctx_cost_p50_bps": _to_float(src.get("ctx_cost_p50_bps") if src.get("ctx_cost_p50_bps") is not None else evt.get("ctx_cost_p50_bps"), 0.0),
        "ctx_cost_p90_bps": _to_float(src.get("ctx_cost_p90_bps") if src.get("ctx_cost_p90_bps") is not None else evt.get("ctx_cost_p90_bps"), 0.0),
        "ctx_exec_risk_ref_bps": _to_float(src.get("ctx_exec_risk_ref_bps") if src.get("ctx_exec_risk_ref_bps") is not None else evt.get("ctx_exec_risk_ref_bps"), 0.0),
        "ctx_edge_net_p50_bps": _to_float(src.get("ctx_edge_net_p50_bps") if src.get("ctx_edge_net_p50_bps") is not None else evt.get("ctx_edge_net_p50_bps"), 0.0),
        "ctx_edge_net_p90_bps": _to_float(src.get("ctx_edge_net_p90_bps") if src.get("ctx_edge_net_p90_bps") is not None else evt.get("ctx_edge_net_p90_bps"), 0.0),
        "ctx_decision": str(evt.get("ctx_decision") or ("allow" if _to_bool(src.get("ctx_allow") if src.get("ctx_allow") is not None else evt.get("ctx_allow"), False) else "deny")),
        "ctx_reason": str(src.get("ctx_reason") or evt.get("ctx_reason") or ""),
        "ctx_fallback_level": str(src.get("ctx_fallback_level") or evt.get("ctx_fallback_level") or ""),
        "ctx_shadow_disagree": _to_bool(src.get("ctx_shadow_disagree") if src.get("ctx_shadow_disagree") is not None else evt.get("ctx_shadow_disagree"), False),
        "ctx_infer_latency_us": _to_int(src.get("ctx_infer_latency_us") if src.get("ctx_infer_latency_us") is not None else evt.get("ctx_infer_latency_us"), 0),
        "spread_bps_missing": _to_bool(evt.get("spread_bps_missing"), False),
        "slippage_missing": _to_bool(evt.get("slippage_missing"), False),
    },
    return row, ""


class PgWriter:
    def __init__(self, dsn: str):
        self.dsn = dsn

    def _connect(self):
        try:
            import psycopg  # type: ignore
            return psycopg.connect(self.dsn)
        except Exception:
            import psycopg2  # type: ignore
            return psycopg2.connect(self.dsn)

    def insert_rows(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        conn = self._connect()
        try:
            cur = conn.cursor()
            sql = (
                "INSERT INTO ofc_contextual_decisions ("
                "decision_ts_ms,sid,symbol,direction,session,dow,hour_utc,scenario_v4"
                "legacy_rule_score,legacy_rule_ok,legacy_reason,ctx_enabled,ctx_mode,ctx_key,ctx_bundle_ver"
                "ctx_p_rule_raw,ctx_p_rule_cal,ctx_cost_p50_bps,ctx_cost_p90_bps,ctx_exec_risk_ref_bps"
                "ctx_edge_net_p50_bps,ctx_edge_net_p90_bps,ctx_decision,ctx_reason,ctx_fallback_level"
                "ctx_shadow_disagree,ctx_infer_latency_us,spread_bps_missing,slippage_missing) "
                "VALUES (%(decision_ts_ms)s,%(sid)s,%(symbol)s,%(direction)s,%(session)s,%(dow)s,%(hour_utc)s,%(scenario_v4)s"
                "%(legacy_rule_score)s,%(legacy_rule_ok)s,%(legacy_reason)s,%(ctx_enabled)s,%(ctx_mode)s,%(ctx_key)s,%(ctx_bundle_ver)s"
                "%(ctx_p_rule_raw)s,%(ctx_p_rule_cal)s,%(ctx_cost_p50_bps)s,%(ctx_cost_p90_bps)s,%(ctx_exec_risk_ref_bps)s"
                "%(ctx_edge_net_p50_bps)s,%(ctx_edge_net_p90_bps)s,%(ctx_decision)s,%(ctx_reason)s,%(ctx_fallback_level)s"
                "%(ctx_shadow_disagree)s,%(ctx_infer_latency_us)s,%(spread_bps_missing)s,%(slippage_missing)s) "
                "ON CONFLICT (decision_ts_ms, sid) DO UPDATE SET "
                "ctx_mode=EXCLUDED.ctx_mode, ctx_bundle_ver=EXCLUDED.ctx_bundle_ver, ctx_p_rule_raw=EXCLUDED.ctx_p_rule_raw, "
                "ctx_p_rule_cal=EXCLUDED.ctx_p_rule_cal, ctx_cost_p50_bps=EXCLUDED.ctx_cost_p50_bps, ctx_cost_p90_bps=EXCLUDED.ctx_cost_p90_bps, "
                "ctx_edge_net_p50_bps=EXCLUDED.ctx_edge_net_p50_bps, ctx_edge_net_p90_bps=EXCLUDED.ctx_edge_net_p90_bps, ctx_reason=EXCLUDED.ctx_reason, "
                "ctx_fallback_level=EXCLUDED.ctx_fallback_level, ctx_shadow_disagree=EXCLUDED.ctx_shadow_disagree, ctx_infer_latency_us=EXCLUDED.ctx_infer_latency_us"
            )
            cur.executemany(sql, rows)
            conn.commit()
            return len(rows)
        finally:
            conn.close()


@dataclass
class Cfg:
    redis_url: str
    stream: str
    group: str
    consumer: str
    block_ms: int
    count: int
    dlq_stream: str
    dlq_maxlen: int
    batch_size: int
    metrics_key: str
    metrics_port: int
    fail_sleep_sec: float

    @staticmethod
    def from_env() -> "Cfg":
        host = socket.gethostname()
        return Cfg(
            redis_url=_env("REDIS_URL", "redis://redis-worker-1:6379/0"),
            stream=_env("OFC_CTX_DECISION_STREAM", _env("DECISIONS_FINAL_STREAM", "decisions:final")),
            group=_env("OFC_CTX_DECISION_CG", "ofc_ctx_decision_writer"),
            consumer=_env("OFC_CTX_DECISION_CONSUMER", f"{host}:{os.getpid()}"),
            block_ms=_env_int("OFC_CTX_DECISION_BLOCK_MS", 5000),
            count=_env_int("OFC_CTX_DECISION_COUNT", 128),
            dlq_stream=_env("OFC_CTX_DECISION_DLQ_STREAM", "events:ofc_ctx_decision:dlq"),
            dlq_maxlen=_env_int("OFC_CTX_DECISION_DLQ_MAXLEN", 200000),
            batch_size=_env_int("OFC_CTX_DECISION_BATCH_SIZE", 200),
            metrics_key=_env("OFC_CTX_DECISION_METRICS_KEY", "metrics:ofc_contextual_decision_writer"),
            metrics_port=_env_int("OFC_CTX_DECISION_WRITER_METRICS_PORT", 9831),
            fail_sleep_sec=_env_float("OFC_CTX_DECISION_FAIL_SLEEP_SEC", 1.0),
        )


async def _ensure_group(r: Any, *, stream: str, group: str) -> None:
    while True:
        try:
            await r.xgroup_create(stream, group, id="$", mkstream=True)
            return
        except Exception as e:
            if "BUSYGROUP" in str(e).upper():
                return
            if "LOADING" in str(e).upper():
                await asyncio.sleep(1.0)
                continue
            raise


async def _publish_dlq(r: Any, *, stream: str, payload: Dict[str, Any], reason: str, maxlen: int) -> None:
    try:
        body = {
            "payload": json.dumps(payload, ensure_ascii=False),
            "reason": str(reason),
            "ts_ms": str(_now_ms()),
        },
        await r.xadd(stream, body, maxlen=maxlen, approximate=True)
    except Exception:
        return


async def _update_status(r: Any, key: str, mapping: Dict[str, Any]) -> None:
    try:
        m = {str(k): str(v) for k, v in mapping.items()}
        await r.hset(key, mapping=m)
    except Exception:
        return


async def main() -> None:
    if aioredis is None:
        raise RuntimeError("redis-py is required")
    dsn = pick_dsn()
    if not dsn:
        raise RuntimeError("TRADES_DB_DSN (or TIMESCALE_DSN) must be set")

    cfg = Cfg.from_env()
    metrics = build_metrics()
    start_metrics_server()

    r = aioredis.from_url(cfg.redis_url, decode_responses=False)
    await _ensure_group(r, stream=cfg.stream, group=cfg.group)
    pg = PgWriter(dsn)

    logger.info("ofc_contextual_decision_writer started: stream=%s group=%s", cfg.stream, cfg.group)
    while True:
        try:
            res = await r.xreadgroup(
                groupname=cfg.group,
                consumername=cfg.consumer,
                streams={cfg.stream: ">"},
                count=cfg.count,
                block=cfg.block_ms,
            )
            if not res:
                try:
                    pend = await r.xpending(cfg.stream, cfg.group)
                    pending = int(pend["pending"] if isinstance(pend, dict) else pend[0])
                    metrics.pending_count.set(pending)
                    await _update_status(r, cfg.metrics_key, {"pending_count": pending})
                except Exception:
                    pass
                continue

            ack_ids: List[str] = []
            rows: List[Dict[str, Any]] = []
            dlq_count = 0
            last_error = ""
            for _stream, entries in res:
                for msg_id, fields in entries:
                    payload = _parse_stream_fields(fields)
                    metrics.processed_total.inc()
                    row, reason = _normalize_row(payload)
                    if row is None:
                        if reason != "skip:no_ctx":
                            dlq_count += 1
                            metrics.dlq_total.labels(reason=reason).inc()
                            await _publish_dlq(r, stream=cfg.dlq_stream, payload=payload, reason=reason, maxlen=cfg.dlq_maxlen)
                        ack_ids.append(msg_id)
                        continue
                    lag_ms = max(0, _now_ms() - int(row["decision_ts_ms"]))
                    metrics.redis_lag_ms.observe(lag_ms)
                    rows.append(row)
                    ack_ids.append(msg_id)

            written = 0
            if rows:
                try:
                    written = pg.insert_rows(rows[: cfg.batch_size])
                    metrics.written_total.inc(written)
                    metrics.last_ok.set(1)
                    metrics.last_batch_rows.set(written)
                except Exception as e:
                    last_error = str(e)[:240]
                    metrics.db_fail_total.inc()
                    metrics.last_ok.set(0)
                    logger.exception("ofc_contextual_decision_writer DB failure")
                    await _update_status(
                        r,
                        cfg.metrics_key,
                        {
                            "last_run_ts_ms": _now_ms(),
                            "last_ok": 0,
                            "db_fail_total": 1,
                            "last_error": last_error,
                            "pending_count": len(ack_ids),
                        },
                    )
                    await asyncio.sleep(cfg.fail_sleep_sec)
                    continue

            if ack_ids:
                await r.xack(cfg.stream, cfg.group, *ack_ids)
            try:
                pend = await r.xpending(cfg.stream, cfg.group)
                pending = int(pend["pending"] if isinstance(pend, dict) else pend[0])
            except Exception:
                pending = 0
            metrics.pending_count.set(pending)
            await _update_status(
                r,
                cfg.metrics_key,
                {
                    "last_run_ts_ms": _now_ms(),
                    "last_ok": 1,
                    "last_batch_rows": written,
                    "written_total": written,
                    "dlq_total": dlq_count,
                    "pending_count": pending,
                    "last_error": last_error,
                },
            )
        except Exception:
            logger.exception("ofc_contextual_decision_writer loop failure")
            metrics.last_ok.set(0)
            await asyncio.sleep(cfg.fail_sleep_sec)


if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    raise SystemExit(asyncio.run(main()))
