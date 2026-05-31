from __future__ import annotations

"""P46 — Deterministic Label Join (v1)

Consumes POSITION_CLOSED events from `events:trades`, joins them with unified
DecisionRecord (P45) stored at `decision:{sid}`, and produces two enriched streams:

  1) `trades:closed`         — enriched close events for analytics
  2) `ml_replay_inputs_v1`   — decision+label rows for ML dataset/replay

Key points
- Deterministic time: epoch ms from event (`close_ts_ms`) and decision (`decision_ts_ms`).
- Fail-open: never breaks the system if joiner fails; keeps metrics + can enqueue for backfill.
- Stable dedup: avoids double-writes; missing-decision events are queued for later backfill.

ENV (defaults)
  REDIS_URL=redis://redis-worker-1:6379/0
  TRADE_EVENTS_STREAM=events:trades
  TRADE_EVENTS_GROUP=trade_close_joiner_v1
  TRADE_EVENTS_CONSUMER=<hostname:pid>
  TRADE_EVENTS_BLOCK_MS=5000
  TRADE_EVENTS_COUNT=128

  TRADES_CLOSED_STREAM=trades:closed
  TRADES_CLOSED_MAXLEN=200000
  ML_REPLAY_INPUTS_STREAM=ml_replay_inputs_v1
  ML_REPLAY_INPUTS_MAXLEN=200000

  CLOSE_WAIT_STREAM=trades:close_wait
  CLOSE_WAIT_MAXLEN=50000
  CLOSE_WAIT_SCAN_COUNT=500
  CLOSE_WAIT_BACKFILL_EVERY_SEC=30
  CLOSE_WAIT_MAX_AGE_HOURS=72

  JOIN_DEDUP_TTL_SEC=604800   # 7d
  JOIN_WAIT_TTL_SEC=259200    # 3d

  LABEL_WIN_R_MIN=0.0         # y=1 if r_mult >= this

Usage
  python -m services.orderflow.trade_close_joiner_worker_v1
"""

import asyncio
import json
import logging
import os
import socket
import time
from typing import Any

import redis

from core.redis_keys import STREAM_RETENTION
from core.redis_keys import RedisStreams as RS
from domain.evidence_keys import MetaKeys
from utils.time_utils import get_ny_time_millis

try:
    import redis.asyncio as aioredis
except Exception:  # pragma: no cover
    aioredis = None

from services.orderflow.metrics import (
    log_silent_error,
    trade_close_joiner_backfill_drop_total,
    trade_close_joiner_backfill_ok_total,
    trade_close_joiner_dedup_skipped_total,
    trade_close_joiner_join_ok_total,
    trade_close_joiner_missing_decision_total,
    trade_close_joiner_prob_missing_total,
    trade_close_joiner_prob_source_total,
    trade_close_joiner_seen_total,
    trade_close_joiner_skipped_no_decision_total,
    trade_close_joiner_written_total,
)
from services.orderflow.probability_utils_v1 import extract_prob_with_source
from services.orderflow.utils import _fields_to_dict, _normalize_epoch_ms
import contextlib

logger = logging.getLogger("trade_close_joiner")


def _load_decisionless_kinds() -> frozenset[str]:
    """Sid-kind prefixes whose producers don't publish events:decision_snapshot.

    Closes with these kinds are skipped early in process_close_event:
    no close_wait XADD, no missing_decision_total noise, no backfill rescans.
    Override via env JOINER_DECISIONLESS_KINDS=kind1,kind2 (empty disables skip).
    """
    raw = os.getenv(
        "JOINER_DECISIONLESS_KINDS",
        "iceberg,weak_progress,delta_spike,absorption,weak_recent",
    )
    return frozenset(p.strip().lower() for p in raw.split(",") if p.strip())


DECISIONLESS_KINDS: frozenset[str] = _load_decisionless_kinds()


def _env_int(name: str, default: str) -> int:
    try:
        return int(float(os.getenv(name, default)))
    except Exception:
        return int(float(default))


def _env_float(name: str, default: str) -> float:
    try:
        return float(os.getenv(name, default))
    except Exception:
        return default  # type: ignore
  # type: ignore

def _now_ms() -> int:
    return get_ny_time_millis()


def _loads_json(s: Any) -> dict | None:
    if s is None:
        return None
    if isinstance(s, dict):
        return s
    if not isinstance(s, str):
        try:
            s = s.decode("utf-8", "replace")
        except Exception:
            s = str(s)
    s = s.strip()
    if not s:
        return None
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def parse_trade_event_fields(raw_fields: dict[Any, Any]) -> dict[str, Any]:
    """Parse Redis Stream fields into a merged event dict.

    Supports:
      - Standard format: {payload: "{...json...}"}
      - Legacy format: flattened fields (event_type, symbol, ...)

    Returns a dict that contains both top-level fields and merged JSON payload fields.
    The original JSON payload dict (if any) is also preserved under `_payload`.
    """
    fields = _fields_to_dict(raw_fields)

    payload_obj: dict | None = None
    if "payload" in fields:
        payload_obj = _loads_json(fields.get("payload"))
    elif "json" in fields:
        payload_obj = _loads_json(fields.get("json"))

    if payload_obj:
        out = dict(fields)
        out["_payload"] = payload_obj  # type: ignore
        # payload fields override legacy duplicates  # type: ignore
        out.update(payload_obj)
        return out
    return dict(fields)


def is_position_closed(ev: dict[str, Any]) -> bool:
    et = str(ev.get("event_type") or ev.get("event") or ev.get("type") or "").upper()
    if et in {"POSITION_CLOSED", "CLOSE", "POSITION_CLOSE"}:
        return True
    # fallback heuristic
    if not et and (ev.get("exit_ts_ms") or ev.get("exit_ts") or ev.get("close_ts_ms")) and ("r_mult" in ev or "pnl" in ev or "pnl_net" in ev):
        return True
    return False


def extract_close_info(ev: dict[str, Any]) -> tuple[str, str, int, float | None, dict[str, Any]]:
    sid = str(
        ev.get("sid")
        or ev.get("SID")
        or ev.get("signal_id")
        or ev.get("signalId")
        or ev.get("signal")
        or ""
    ).strip()

    symbol = str(ev.get("symbol") or ev.get("SYMBOL") or "").upper()

    ts_ms = _normalize_epoch_ms(
        ev.get("ts_ms")
        or ev.get("ts")
        or ev.get("exit_ts_ms")
        or ev.get("exit_ts")
        or ev.get("close_ts_ms")
        or ev.get("timestamp")
        or 0
    )

    r_mult: float | None = None
    for k in ("r_mult", "r", "rMultiple", "r_multiple", "rMult"):
        if k in ev and ev.get(k) is not None:
            with contextlib.suppress(Exception):
                r_mult = float(ev.get(k))  # type: ignore
            break  # type: ignore

    # Analytical extra fields for promotion
    extra = {}

    # PnL
    pnl = None
    for k in ("pnl", "pnl_net", "net_pnl", "profit"):
        if k in ev and ev.get(k) is not None:
            try:
                pnl = float(ev.get(k))  # type: ignore
                extra["pnl"] = str(pnl)  # type: ignore
                if k == "pnl_net" or "pnl_net" not in ev:
                    extra["pnl_net"] = str(pnl)
            except Exception:
                pass
            if pnl is not None:
                break

    # Prices
    exit_px = None
    for k in ("exit_px", "close_px", "price", "px", "exit_price"):
        if k in ev and ev.get(k) is not None:
            try:
                exit_px = float(ev.get(k))  # type: ignore
                extra["exit_px"] = str(exit_px)  # type: ignore
            except Exception:
                pass
            if exit_px is not None:
                break

    entry_px = None
    for k in ("entry_px", "open_px", "entry_price", "open_price"):
        if k in ev and ev.get(k) is not None:
            try:
                entry_px = float(ev.get(k))  # type: ignore
                extra["entry_px"] = str(entry_px)  # type: ignore
            except Exception:
                pass
            if entry_px is not None:
                break

    # Qty/Side
    qty = None
    for k in ("qty", "quantity", "lot", "size"):
        if k in ev and ev.get(k) is not None:
            try:
                qty = float(ev.get(k))  # type: ignore
                extra["qty"] = str(qty)  # type: ignore
            except Exception:
                pass
            if qty is not None:
                break

    side = str(ev.get("side") or ev.get("direction") or "").strip().upper()
    if side:
        if side in ("BUY", "LONG"):
            extra["side"] = "LONG"
        elif side in ("SELL", "SHORT"):
            extra["side"] = "SHORT"
        else:
            extra["side"] = side

    # Risk/Fees
    for k in ("risk_usd", "one_r_money", "fees_usd", "fee_bps", "close_reason"):
        if k in ev and ev.get(k) is not None:
            extra[k] = (ev.get(k))

    return sid, symbol, int(ts_ms), r_mult, extra


def _extract_prob(decision: dict[str, Any]) -> float | None:
    # Legacy wrapper kept for minimal churn.
    p, _ = extract_prob_with_source(decision)
    return p


def _compute_pnl_bps_from_extra(extra: dict[str, Any]) -> float | None:
    """Best-effort pnl_bps from joiner `extra` dict.

    Preference order:
      1. extra["pnl_pct"] × 100 (if pre-computed by upstream)
      2. (exit_px - entry_px) / entry_px × 10_000 × side_sign

    Returns None on any failure. Used by JOINER_WRITE_LAST_OUTCOME=1 hash
    writer; never raises (caller wraps in try/except).
    """
    try:
        pct = extra.get("pnl_pct")
        if pct not in (None, ""):
            return float(pct) * 100.0
    except Exception:
        pass
    try:
        entry = float(extra.get("entry_px", 0) or 0)
        exit_ = float(extra.get("exit_px", 0) or 0)
        if entry <= 0 or exit_ <= 0:
            return None
        side = str(extra.get("side", "")).upper()
        sign = 1.0 if side in ("LONG", "BUY") else -1.0
        return sign * (exit_ - entry) / entry * 10_000.0
    except Exception:
        return None


def label_to_calib_result(y: int | None) -> str:
    """Map outcome label to p_edge calibrator's result whitelist.

    - y=1 (r_mult >= LABEL_WIN_R_MIN)  → "WIN"
    - y=0 (r_mult < LABEL_WIN_R_MIN)   → "LOSS"
    - y=None (no r_mult yet, outcome unresolved) → "UNKNOWN"
      The calibrator's whitelist accepts only WIN/LOSS/BE, so UNKNOWN trades
      are dropped via reason="result_invalid". Critical: do NOT map None→BE,
      because that inflates the BE-bucket and trips PEdgeCalibBEBucketInflated.
    """
    if y == 1:
        return "WIN"
    if y == 0:
        return "LOSS"
    return "UNKNOWN"


def compute_label_and_brier(*, decision: dict[str, Any], r_mult: float | None) -> dict[str, Any]:
    win_min = _env_float("LABEL_WIN_R_MIN", "0.0")
    y: int | None = None
    if r_mult is not None:
        y = 1 if float(r_mult) >= float(win_min) else 0

    p, p_source = extract_prob_with_source(decision)

    brier: float | None = None
    if y is not None and p is not None:
        brier = float((p - float(y)) ** 2)

    return {"y": y, "p": p, "p_source": p_source, "brier": brier, "win_r_min": float(win_min)}


async def _ensure_group(r: Any, *, stream: str, group: str) -> None:
    while True:
        try:
            await r.xgroup_create(stream, group, id="$", mkstream=True)
            return
        except Exception as e:
            err = str(e).upper()
            if "BUSYGROUP" in err:
                return
            if "LOADING" in err:
                logger.warning("Redis is loading, waiting 1s...")
                await asyncio.sleep(1.0)
                continue
            raise


async def _xadd_payload(
    r: Any,
    *,
    stream: str,
    fields: dict[str, str],
    maxlen: int,
) -> None:
    await r.xadd(stream, fields=fields, maxlen=maxlen, approximate=True)


async def _write_outputs(
    r: Any,
    *,
    decision: dict[str, Any],
    close_ev: dict[str, Any],
    label: dict[str, Any],
) -> None:
    trades_stream = os.getenv("TRADES_CLOSED_STREAM", RS.TRADES_CLOSED)
    trades_maxlen = _env_int("TRADES_CLOSED_MAXLEN", str(STREAM_RETENTION[RS.TRADES_CLOSED]))

    ml_stream = os.getenv("ML_REPLAY_INPUTS_STREAM", RS.ML_REPLAY_INPUTS)
    ml_maxlen = _env_int("ML_REPLAY_INPUTS_MAXLEN", str(STREAM_RETENTION[RS.ML_REPLAY_INPUTS]))

    sid, symbol, close_ts_ms, r_mult, extra = extract_close_info(close_ev)
    # Decision timestamp: legacy DecisionRecord uses "ts_ms"; new flat
    # decision_snapshot payload uses "decision_ts_ms". Try both.
    decision_ts_ms = int(decision.get("ts_ms") or decision.get("decision_ts_ms") or 0)

    # prefer bucket from close payload, fallback to decision (nested legacy or flat snapshot).
    bucket = str(
        close_ev.get(MetaKeys.ENFORCE_COV_BUCKET)
        or close_ev.get("meta_enforce_bucket")
        or (decision.get("meta", {}) or {}).get("meta_enforce_bucket")
        or decision.get("meta_enforce_bucket")
        or decision.get("meta_enforce_cov_bucket")
        or ""
    )

    model_ver = str(
        (decision.get("ml", {}) or {}).get("model_ver")
        or decision.get("ml_model_ver")
        or decision.get("model_ver")
        or ""
    )

    out_trades = {
        "version": 1,
        "sid": sid,
        "symbol": symbol,
        "close_ts_ms": int(close_ts_ms),
        "r_mult": None if r_mult is None else float(r_mult),
        "y": label.get("y"),
        "p": label.get("p"),
        "brier": label.get("brier"),
        "bucket": bucket,
        "model_ver": model_ver,
        "decision": decision,
        "close": close_ev,
        "join": {
            "decision_ts_ms": int(decision_ts_ms),
            "decision_age_ms": int(max(0, close_ts_ms - decision_ts_ms)) if decision_ts_ms else None,
            "win_r_min": label.get("win_r_min"),
        },
        **extra,
    }

    out_ml = {
        "version": 1,
        "sid": sid,
        "symbol": symbol,
        "ts_ms": int(decision_ts_ms or close_ts_ms),
        "decision": decision,
        "label": {
            "close_ts_ms": int(close_ts_ms),
            "r_mult": None if r_mult is None else float(r_mult),
            "y": label.get("y"),
            "p": label.get("p"),
            "brier": label.get("brier"),
            "win_r_min": label.get("win_r_min"),
        },
        "meta": {
            "bucket": bucket,
            "model_ver": model_ver,
        },
    }

    payload_trades = json.dumps(out_trades, ensure_ascii=False, separators=(",", ":"), default=str)
    payload_ml = json.dumps(out_ml, ensure_ascii=False, separators=(",", ":"), default=str)

    # Calibrator alias fields — p_edge_threshold_calibrator_v1 reads these exact names.
    # Kept separate so both try-path and fallback path use the same values.
    _y = label.get("y")
    _p = label.get("p")
    _result = label_to_calib_result(_y)
    _market_regime = str(
        close_ev.get("entry_regime") or close_ev.get("market_regime")
        or decision.get("market_regime")
        or decision.get("session")  # snapshot fallback (best available approximation)
        or "*"
    )
    _kind = str(decision.get("kind") or decision.get("signal_kind") or "*")

    # P2 ctx_tighten attribution: extract from signal_payload.indicators
    # Written by signal_pipeline._apply_decision when SentimentContextGate /
    # DefiLlamaContextGate fires TIGHTEN. Zero when gate disabled/abstain.
    # Source priority:
    #   1. close_ev.signal_payload.indicators (NestJS-originated close event)
    #   2. decision.indicators_small (decision:{sid} snapshot, includes these fields
    #      since P2.5 added them to _INDICATORS_SMALL_ALLOW in decision_snapshot.py)
    _ctx_senti_bps = 0.0
    _ctx_defi_bps = 0.0
    try:
        _sp = close_ev.get("signal_payload") or {}
        if isinstance(_sp, str):
            import json as _json_mod
            _sp = _json_mod.loads(_sp)
        _inds = _sp.get("indicators") or {} if isinstance(_sp, dict) else {}
        if isinstance(_inds, str):
            import json as _json_mod  # noqa: F811
            _inds = _json_mod.loads(_inds)
        if isinstance(_inds, dict):
            _ctx_senti_bps = float(_inds.get("ctx_sentiment_tighten_bps", 0.0) or 0.0)
            _ctx_defi_bps = float(_inds.get("ctx_defillama_tighten_bps", 0.0) or 0.0)
    except Exception:
        pass
    # Fallback: read from decision.indicators_small (populated by decision_snapshot_writer
    # since _INDICATORS_SMALL_ALLOW now includes these fields — P2.5 fix).
    if _ctx_senti_bps == 0.0 or _ctx_defi_bps == 0.0:
        try:
            _d_inds = decision.get("indicators_small") or {}
            if isinstance(_d_inds, str):
                import json as _json_mod  # noqa: F811
                _d_inds = _json_mod.loads(_d_inds)
            if isinstance(_d_inds, dict):
                if _ctx_senti_bps == 0.0:
                    _ctx_senti_bps = float(_d_inds.get("ctx_sentiment_tighten_bps", 0.0) or 0.0)
                if _ctx_defi_bps == 0.0:
                    _ctx_defi_bps = float(_d_inds.get("ctx_defillama_tighten_bps", 0.0) or 0.0)
        except Exception:
            pass

    # 2026-05-23 calibrator refit support: persist BOTH raw and calibrated
    # probabilities + ml_version so tools/refit_meta_lr_blend_calibrator.py
    # can re-fit cleanly after a calibrator is already deployed. Falls back
    # to `_p` (= label["p"]) for raw while no calibrator is attached upstream.
    _p_raw = decision.get("p_edge_raw")
    if _p_raw is None:
        _p_raw = decision.get("ml_prob")
    if _p_raw is None:
        _p_raw = _p
    _p_cal = decision.get("p_edge_cal")
    if _p_cal is None:
        _p_cal = decision.get("ml_calibrated_prob")
    if _p_cal is None:
        _p_cal = _p
    _ml_version = str(decision.get("ml_version") or decision.get("model_ver") or model_ver or "")

    calib_fields: dict[str, str] = {
        "ml_prob":     "" if _p is None else f"{float(_p):.6f}",
        "p_edge_raw":  "" if _p_raw is None else f"{float(_p_raw):.6f}",
        "p_edge_cal":  "" if _p_cal is None else f"{float(_p_cal):.6f}",
        "ml_version":  _ml_version,
        "r_multiple":  "" if r_mult is None else str(r_mult),
        "result":      _result,
        "ts_close":    str(close_ts_ms),
        "market_regime": _market_regime,
        "kind":        _kind,
        "ctx_sentiment_tighten_bps": f"{_ctx_senti_bps:.4f}",
        "ctx_defillama_tighten_bps": f"{_ctx_defi_bps:.4f}",
    }

    # Prefer simple 'payload' format (standard)
    try:
        pipe = r.pipeline()
        # Serialize decision as signal_payload for reporters
        signal_payload_str = json.dumps(decision, ensure_ascii=False, separators=(",", ":"), default=str)

        pipe.xadd(
            trades_stream,
            fields={
                "sid": sid,
                "symbol": symbol,
                "ts_ms": str(close_ts_ms),
                "r_mult": "" if r_mult is None else str(r_mult),
                "y": "" if label.get("y") is None else (label.get("y")),
                "p": "" if label.get("p") is None else f"{float(label['p']):.6f}",
                "brier": "" if label.get("brier") is None else f"{float(label['brier']):.6f}",
                "bucket": bucket,
                "model_ver": model_ver,
                "payload": payload_trades,
                "signal_payload": signal_payload_str,
                **extra,
                **calib_fields,
            },
            maxlen=trades_maxlen,
            approximate=True,
        )
        pipe.xadd(
            ml_stream,
            fields={
                "sid": sid,
                "symbol": symbol,
                "ts_ms": str(int(decision_ts_ms or close_ts_ms)),
                "y": "" if label.get("y") is None else (label.get("y")),
                "p": "" if label.get("p") is None else f"{float(label['p']):.6f}",
                "brier": "" if label.get("brier") is None else f"{float(label['brier']):.6f}",
                "bucket": bucket,
                "model_ver": model_ver,
                "payload": payload_ml,
                **extra,
            },
            maxlen=ml_maxlen,
            approximate=True,
        )
        # Optional O(1) last-outcome write-through for last_trade_outcome_raw
        # fast path (audit 2026-05-19 Phase 4 follow-up). Off by default;
        # operator enables via JOINER_WRITE_LAST_OUTCOME=1. core.last_trade_outcome_reader
        # checks `trades:last_outcome:{symbol}.pnl_bps` first, falls back to
        # XREVRANGE scan otherwise.
        if os.getenv("JOINER_WRITE_LAST_OUTCOME", "0").lower() in {"1", "true", "yes", "on"}:
            try:
                _pnl_bps = _compute_pnl_bps_from_extra(extra)
                if _pnl_bps is not None:
                    pipe.hset(
                        f"trades:last_outcome:{symbol}",
                        mapping={
                            "pnl_bps": f"{_pnl_bps:.6f}",
                            "ts_ms": str(close_ts_ms),
                            "sid": sid,
                        },
                    )
                    pipe.expire(f"trades:last_outcome:{symbol}", 86400)  # 24h TTL
            except Exception:
                pass  # never block close write on outcome hash failure
        await pipe.execute()
    except Exception:
        # Serialize decision as signal_payload for reporters
        signal_payload_str = json.dumps(decision, ensure_ascii=False, separators=(",", ":"), default=str)

        await _xadd_payload(
            r,
            stream=trades_stream,
            fields={
                "sid": sid,
                "symbol": symbol,
                "ts_ms": str(close_ts_ms),
                "r_mult": "" if r_mult is None else str(r_mult),
                "y": "" if label.get("y") is None else (label.get("y")),  # type: ignore
                "p": "" if label.get("p") is None else f"{float(label['p']):.6f}",  # type: ignore
                "brier": "" if label.get("brier") is None else f"{float(label['brier']):.6f}",
                "bucket": bucket,
                "model_ver": model_ver,
                "payload": payload_trades,
                "signal_payload": signal_payload_str,
                **extra,
                **calib_fields,
            },
            maxlen=trades_maxlen,
        )
        await _xadd_payload(
            r,
            stream=ml_stream,
            fields={
                "sid": sid,
                "symbol": symbol,
                "ts_ms": str(int(decision_ts_ms or close_ts_ms)),
                "y": "" if label.get("y") is None else (label.get("y")),  # type: ignore
                "p": "" if label.get("p") is None else f"{float(label['p']):.6f}",  # type: ignore
                "brier": "" if label.get("brier") is None else f"{float(label['brier']):.6f}",
                "bucket": bucket,
                "model_ver": model_ver,
                "payload": payload_ml,
                **extra,
            },
            maxlen=ml_maxlen,
        )

    trade_close_joiner_written_total.labels(stream=trades_stream, symbol=symbol).inc()
    trade_close_joiner_written_total.labels(stream=ml_stream, symbol=symbol).inc()


async def process_close_event(
    r: Any,
    *,
    close_ev: dict[str, Any],
    from_backfill: bool = False,
) -> bool:
    """Attempt join. Returns True if joined+written, False otherwise."""

    sid, symbol, close_ts_ms, _, extra = extract_close_info(close_ev)
    if not symbol:
        symbol = "unknown"

    if not sid:
        # no sid => can't join
        return False

    # Kind-aware skip: producers for these kinds don't emit events:decision_snapshot,
    # so decision:{sid} will never appear. Avoid blowing up missing_decision_total
    # and close_wait backlog. See JOINER_DECISIONLESS_KINDS env.
    if DECISIONLESS_KINDS:
        sid_kind = sid.split(":", 1)[0].lower() if ":" in sid else ""
        if sid_kind in DECISIONLESS_KINDS:
            trade_close_joiner_skipped_no_decision_total.labels(symbol=symbol, kind=sid_kind).inc()
            return False

    # Dedup keys
    dedup_ttl = _env_int("JOIN_DEDUP_TTL_SEC", "604800")
    wait_ttl = _env_int("JOIN_WAIT_TTL_SEC", "259200")

    done_key = f"join:closed:done:{sid}"
    wait_key = f"join:closed:wait:{sid}"

    # If already joined, skip
    try:
        if await r.exists(done_key):
            trade_close_joiner_dedup_skipped_total.labels(symbol=symbol, where="done").inc()
            return True
    except Exception:
        pass

    # Fetch decision
    decision_raw = None
    try:
        decision_raw = await r.get(f"decision:{sid}")
    except Exception:
        decision_raw = None

    if not decision_raw:
        # Queue for backfill (dedup wait enqueue)
        try:
            enq = await r.set(wait_key, "1", nx=True, ex=wait_ttl)
        except Exception:
            enq = True  # fail-open

        trade_close_joiner_missing_decision_total.labels(symbol=symbol, where="backfill" if from_backfill else "realtime").inc()

        if enq:
            wait_stream = os.getenv("CLOSE_WAIT_STREAM", RS.TRADES_CLOSE_WAIT)
            wait_maxlen = _env_int("CLOSE_WAIT_MAXLEN", str(STREAM_RETENTION[RS.TRADES_CLOSE_WAIT]))

            payload = json.dumps(close_ev, ensure_ascii=False, separators=(",", ":"), default=str)
            try:
                await r.xadd(
                    wait_stream,
                    fields={
                        "sid": sid,
                        "symbol": symbol,
                        "close_ts_ms": str(close_ts_ms or 0),
                        "first_seen_ms": str(_now_ms()),
                        "payload": payload,
                    },
                    maxlen=wait_maxlen,
                    approximate=True,
                )
            except Exception as e:
                log_silent_error(e, kind="joiner_wait_xadd_failed", symbol=symbol, where="trade_close_joiner")
        return False

    decision = _loads_json(decision_raw) or {}
    if not decision:
        return False

    sid, symbol, ts_ms, r_mult, extra = extract_close_info(close_ev)
    label = compute_label_and_brier(decision=decision, r_mult=r_mult)

    # Mark joined (atomic as possible)
    try:
        ok = await r.set(done_key, "1", nx=True, ex=dedup_ttl)
        if not ok:
            trade_close_joiner_dedup_skipped_total.labels(symbol=symbol, where="race").inc()
            return True
    except Exception:
        pass

    with contextlib.suppress(Exception):
        await r.delete(wait_key)

    # Probability extraction health metrics (alerts can be based on ratios)
    where = "backfill" if from_backfill else "realtime"
    if label.get("p") is None:
        trade_close_joiner_prob_missing_total.labels(symbol=symbol, where=where).inc()
    else:
        src = (label.get("p_source") or "unknown")
        trade_close_joiner_prob_source_total.labels(symbol=symbol, source=src).inc()

    await _write_outputs(r, decision=decision, close_ev=close_ev, label=label)
    trade_close_joiner_join_ok_total.labels(symbol=symbol, where="backfill" if from_backfill else "realtime").inc()
    return True


async def backfill_wait_stream(r: Any) -> None:
    wait_stream = os.getenv("CLOSE_WAIT_STREAM", RS.TRADES_CLOSE_WAIT)
    scan = _env_int("CLOSE_WAIT_SCAN_COUNT", "500")
    max_age_hours = _env_float("CLOSE_WAIT_MAX_AGE_HOURS", "72")
    max_age_ms = int(max_age_hours * 3600 * 1000)

    now_ms = _now_ms()

    try:
        batch = await r.xrevrange(wait_stream, max="+", min="-", count=scan)
    except Exception:
        return

    for msg_id, raw_fields in batch:
        try:
            ev = parse_trade_event_fields(raw_fields)
            sid, _, _, _, _ = extract_close_info(ev)
            symbol = (ev.get("symbol") or "unknown").upper()
            first_seen = _normalize_epoch_ms(ev.get("first_seen_ms") or 0)

            # On older entries we only store payload=close_ev JSON
            payload_obj = _loads_json(ev.get("payload")) or ev.get("_payload")
            close_ev = payload_obj if isinstance(payload_obj, dict) else ev

            if not sid:
                # can't join -> drop
                await r.xdel(wait_stream, msg_id)
                trade_close_joiner_backfill_drop_total.labels(symbol=symbol, reason="no_sid").inc()
                continue

            if first_seen and (now_ms - first_seen) > max_age_ms:
                await r.xdel(wait_stream, msg_id)
                trade_close_joiner_backfill_drop_total.labels(symbol=symbol, reason="too_old").inc()
                continue

            # Drop entries whose sid kind is known to have no decision_snapshot
            # producer — they would skip in process_close_event but stay in the
            # wait stream and get re-scanned every backfill cycle (noise).
            if DECISIONLESS_KINDS:
                sid_kind = sid.split(":", 1)[0].lower() if ":" in sid else ""
                if sid_kind in DECISIONLESS_KINDS:
                    await r.xdel(wait_stream, msg_id)
                    trade_close_joiner_backfill_drop_total.labels(symbol=symbol, reason=f"decisionless_kind:{sid_kind}").inc()
                    continue

            ok = await process_close_event(r, close_ev=close_ev, from_backfill=True)
            if ok:
                with contextlib.suppress(Exception):
                    await r.xdel(wait_stream, msg_id)
                trade_close_joiner_backfill_ok_total.labels(symbol=symbol).inc()
        except Exception as e:
            log_silent_error(e, kind="joiner_backfill_error", symbol="unknown", where="trade_close_joiner")


async def run() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    metrics_port = _env_int("JOINER_METRICS_PORT", os.getenv("PROMETHEUS_PORT", "9846"))
    if metrics_port > 0:
        try:
            from prometheus_client import start_http_server
            start_http_server(metrics_port)
            logger.info("trade_close_joiner metrics exposed on :%d", metrics_port)
        except Exception as e:
            logger.warning("trade_close_joiner metrics http_server failed on :%d: %s", metrics_port, e)

    if aioredis is None:
        raise RuntimeError("redis.asyncio is not available")

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    stream = os.getenv("TRADE_EVENTS_STREAM", RS.EVENTS_TRADES)
    group = os.getenv("TRADE_EVENTS_GROUP", "trade_close_joiner_v1")

    consumer = os.getenv("TRADE_EVENTS_CONSUMER")
    if not consumer:
        consumer = f"{socket.gethostname()}:{os.getpid()}"

    block_ms = _env_int("TRADE_EVENTS_BLOCK_MS", "5000")
    count = _env_int("TRADE_EVENTS_COUNT", "128")

    r = aioredis.Redis.from_url(redis_url, decode_responses=False)

    # Retry loop for initial connection
    while True:
        try:
            await _ensure_group(r, stream=stream, group=group)
            break
        except (socket.gaierror, OSError, redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            logger.error(f"❌ Redis connection failed (waiting 5s): {e}")
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            logger.info("🛑 Cancelled during startup")
            return

    last_backfill = 0.0
    backfill_every = float(os.getenv("CLOSE_WAIT_BACKFILL_EVERY_SEC", "30") or 30)

    logger.info(
        "trade_close_joiner started stream=%s group=%s consumer=%s", stream, group, consumer
    )

    while True:
        try:
            # periodic backfill
            now = time.time()
            if backfill_every > 0 and (now - last_backfill) >= backfill_every:
                await backfill_wait_stream(r)
                last_backfill = now

            res = await r.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream: ">"},
                count=count,
                block=block_ms,
            )

            if not res:
                continue

            for _stream_name, msgs in res:
                for msg_id, raw_fields in msgs:
                    try:
                        ev = parse_trade_event_fields(raw_fields)
                        if not is_position_closed(ev):
                            # not our event
                            await r.xack(stream, group, msg_id)
                            continue

                        sid, symbol, _, _, _ = extract_close_info(ev)
                        if not symbol:
                            symbol = "unknown"

                        trade_close_joiner_seen_total.labels(symbol=symbol).inc()

                        # Close event payload to join on
                        close_ev = ev.get("_payload") if isinstance(ev.get("_payload"), dict) else ev

                        await process_close_event(r, close_ev=close_ev, from_backfill=False)  # type: ignore
  # type: ignore
                        await r.xack(stream, group, msg_id)
                    except Exception as e:
                        log_silent_error(e, kind="joiner_loop_error", symbol="unknown", where="trade_close_joiner")
                        try:
                            dlq_stream = os.getenv("TRADE_CLOSE_DLQ_STREAM", RS.DLQ_EVENTS)
                            await r.xadd(
                                dlq_stream,
                                {
                                    b"source_stream": stream.encode() if isinstance(stream, str) else stream,
                                    b"msg_id": str(msg_id).encode() if isinstance(msg_id, str) else msg_id,
                                    b"error": str(e)[:200].encode(),
                                },
                                maxlen=2000,
                                approximate=True,
                            )
                        except Exception:
                            pass
                        with contextlib.suppress(Exception):
                            await r.xack(stream, group, msg_id)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log_silent_error(e, kind="joiner_outer_loop", symbol="unknown", where="trade_close_joiner")
            await asyncio.sleep(1.0)


def main() -> None:
    raise SystemExit(asyncio.run(run()))


if __name__ == "__main__":
    main()
