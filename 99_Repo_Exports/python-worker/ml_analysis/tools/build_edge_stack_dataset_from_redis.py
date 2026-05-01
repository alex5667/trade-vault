from __future__ import annotations
"""Build an edge-stack training dataset by joining Redis streams via SID.

Primary intent: produce JSONL rows for tools.train_edge_stack_v1_oof (OOF stacking).

Sources (defaults):
  - signals (feature snapshot): signals:of:inputs  (field "payload" JSON with indicators + sid)
  - outcomes (labels):          trades:closed        (fields include sid + pnl + risk_usd)

Join key: canonical sid = crypto-of:{SYMBOL}:{ts_ms}

Output JSONL row schema (per line):
  {
    "ts_ms": <int>,
    "close_ts_ms": <int>,
    "sid": "crypto-of:...",
    "symbol": "BTCUSDT",
    "direction": "BUY"|"SELL",
    "scenario": "..." (scenario_v4 from replay),
    "indicators": { ... },        # feature snapshot
    "pnl": <float>,
    "risk_usd": <float>,
    "r_mult": <float>,            # pnl / risk_usd (if risk_usd>0)
    "y": <0|1>                    # r_mult >= y_min_r
  }

CLI examples:
  python -m ml_analysis.tools.build_edge_stack_dataset_from_redis \
    --redis_url redis://localhost:6379/0 \
    --out_jsonl ./edge_train.jsonl \
    --emit_feature_cols_json ./feature_cols.json \
    --out_quarantine_jsonl ./edge_quarantine.jsonl \
    --out_report_json ./edge_dataset_report.json

Notes:
  - This tool is intentionally deterministic: stable sorting by ts_ms then sid.
  - It is safe to run against large streams by limiting COUNT for each stream.
  - v2 adds:
      * quarantine JSONL for dropped records,
      * drop-reason counters + examples,
      * sid mismatch diagnostics for unmatched closes (nearest signal by time per symbol).
"""

from utils.time_utils import get_ny_time_millis

import argparse
import bisect
import gzip
import re
import json
import math
import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple, TYPE_CHECKING, Iterable

if TYPE_CHECKING:  # pragma: no cover
    import redis  # type: ignore


# ---------------------------------------------------------------------------
# Centralized schema choices (avoid drift across tools)
# ---------------------------------------------------------------------------
try:
    from tools.schema_choices_v1 import schema_choices as _schema_choices, normalize_schema_ver as _norm_schema_ver  # type: ignore
except Exception:  # pragma: no cover
    from ml_analysis.tools.schema_choices_v1 import schema_choices as _schema_choices, normalize_schema_ver as _norm_schema_ver  # type: ignore


# ---------------------------------------------------------------------------
# Feature Registry (опциональный импорт — не падает если PYTHONPATH не содержит tick_flow_full)
# При запуске с --feature_schema_ver запросить: PYTHONPATH=./tick_flow_full:./ml_analysis
# ---------------------------------------------------------------------------
try:
    from core.feature_registry import get_edge_stack_feature_spec as _get_edge_stack_spec  # type: ignore
    _REGISTRY_AVAILABLE = True
except ImportError:
    _REGISTRY_AVAILABLE = False
    _get_edge_stack_spec = None  # type: ignore


# ---------------------------------------------------------------------------
# Offline-only derived features (F/G/H) for ablation without touching runtime pipeline.
# ---------------------------------------------------------------------------
try:
    from ml_analysis.common.derived_fgh import derive_fgh_rows  # type: ignore
    _DERIVE_FGH_AVAILABLE = True
except Exception:
    _DERIVE_FGH_AVAILABLE = False
    derive_fgh_rows = None  # type: ignore


FGH_NUMERIC_KEYS: List[str] = [
    # F) leader-relative
    "rel_ofi_ml_norm_btc",
    "rel_lob_micro_shift_bps_btc",
    # G) replenishment imbalance
    "ask_replenish_imb",
    "bid_replenish_imb",
    "lob_replenishment_pressure",
    "replenish_ratio_ask",
    "replenish_ratio_bid",
    "replenish_ratio_diff",
    # H) acceleration / velocity
    "ofi_ml_wsum_vel",
    "micro_shift_bps_vel",
    "ofi_ml_wsum_vel_z_ema",
    "micro_shift_bps_vel_z_ema",
],


def _now_ms() -> int:
    return get_ny_time_millis()


def _as_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, bytes):
        return x.decode("utf-8", "ignore")
    return str(x)


def _as_int(x: Any, default: int = 0) -> int:
    if x is None:
        return int(default)
    if isinstance(x, bool):
        return int(default)
    if isinstance(x, (int, float)):
        try:
            return int(x)
        except Exception:
            return int(default)
    if isinstance(x, bytes):
        try:
            x = x.decode("utf-8", "ignore")
        except Exception:
            return int(default)
    try:
        s = str(x).strip()
        if not s:
            return int(default)
        return int(float(s))
    except Exception:
        return int(default)


def _as_float(x: Any, default: float = 0.0) -> float:
    if x is None:
        return float(default)
    if isinstance(x, bool):
        return float(default)
    if isinstance(x, (int, float)):
        try:
            return float(x)
        except Exception:
            return float(default)
    if isinstance(x, bytes):
        try:
            x = x.decode("utf-8", "ignore")
        except Exception:
            return float(default)
    try:
        s = str(x).strip()
        if not s:
            return float(default)
        return float(s)
    except Exception:
        return float(default)


def _safe_json_loads(x: Any) -> Any:
    if x is None:
        return None
    if isinstance(x, (dict, list)):
        return x
    if isinstance(x, bytes):
        try:
            x = x.decode("utf-8", "ignore")
        except Exception:
            return None
    if not isinstance(x, str):
        return None
    s = x.strip()
    if not s:
        return None
    try:
        obj = json.loads(s)
        return obj
    except Exception:
        return None


def _make_sid(symbol: str, ts_ms: int) -> str:
    sym = (symbol or "").upper()
    return f"crypto-of:{sym}:{int(ts_ms)}"


def _sid_parts(sid: str) -> Tuple[str, int]:
    """Parse canonical/legacy sid → (symbol, ts_ms). Returns ("", 0) on failure."""
    s = _as_str(sid).strip()
    if not s:
        return "", 0
    if s.startswith("crypto-of:"):
        parts = s.split(":")
        if len(parts) >= 3:
            sym = (parts[1] or "").upper()
            try:
                t = int(parts[2])
            except Exception:
                t = 0
            return sym, t
    if "|" in s:
        parts = s.split("|")
        if len(parts) >= 2:
            sym = (parts[0] or "").upper()
            try:
                t = int(parts[1])
            except Exception:
                t = 0
            return sym, t
    return "", 0


def _normalize_sid(raw_sid: Any, *, symbol: str, ts_ms: int) -> str:
    """Normalize/derive canonical sid for joins.

    Accepts:
      - canonical: crypto-of:SYMBOL:ts_ms
      - legacy: crypto-of:SYMBOL:ts_ms:... (extra suffix)
      - loose: {symbol}|{ts_ms}|{direction} (direction ignored)
    """
    s = _as_str(raw_sid).strip()
    if s.startswith("crypto-of:"):
        parts = s.split(":")
        if len(parts) >= 3:
            sym = (parts[1] or symbol or "").upper()
            try:
                t = int(parts[2])
            except Exception:
                t = int(ts_ms)
            return f"crypto-of:{sym}:{t}"
    if "|" in s:
        parts = s.split("|")
        if len(parts) >= 2:
            sym = (parts[0] or symbol or "").upper()
            try:
                t = int(parts[1])
            except Exception:
                t = int(ts_ms)
            return f"crypto-of:{sym}:{t}"
    return _make_sid(symbol, int(ts_ms))


def _norm_dir(v: Any) -> str:
    """Normalize direction to BUY/SELL."""
    s = _as_str(v).strip().upper()
    if s in ("BUY", "LONG", "B"):
        return "BUY"
    if s in ("SELL", "SHORT", "S"):
        return "SELL"
    return ""


def _norm_scenario(v: Any) -> str:
    """Normalize scenario to lowercase string."""
    return _as_str(v).strip().lower()


def _bucket_from_scenario(v: Any) -> str:
    """Map scenario to stable 3-bucket taxonomy (trend/range/other)."""
    s = _norm_scenario(v)
    if s in ("trend", "range"):
        return s
    return "other"


def _decode_fields(fields: Dict[Any, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (fields or {}).items():
        kk = _as_str(k)
        if isinstance(v, bytes):
            out[kk] = v.decode("utf-8", "ignore")
        else:
            out[kk] = v
    return out


def _xrevrange_recent(r: "redis.Redis", stream: str, *, count: int) -> List[Tuple[str, Dict[str, Any]]]:
    """Read last N entries from a Redis stream in reverse order, in batches to avoid blocking."""
    out: List[Tuple[str, Dict[str, Any]]] = []
    last_id = "+"
    remaining = int(count)
    chunk_size = 1000
    
    while remaining > 0:
        batch_size = min(remaining, chunk_size)
        items = r.xrevrange(stream, max=last_id, min="-", count=batch_size)
        if not items:
            break
        if len(items) == 1 and _as_str(items[0][0]) == _as_str(last_id):
            break

        for _id, f in items:
            _id_s = _as_str(_id)
            if _id_s == last_id:
                continue
            out.append((_id_s, _decode_fields(f)))
            remaining -= 1
            if remaining <= 0:
                break

        last_id = _as_str(items[-1][0])
        time.sleep(0.05)  # Yield to Redis to prevent blocking other clients

    return out
# --- Archive fallback (P58) -------------------------------------------------
# Allows building datasets beyond Redis retention by reading NDJSON archives written by stream archivers.
# Expected file names: YYYY-MM-DD.ndjson or YYYY-MM-DD.ndjson.gz (UTC day partition).
_DAY_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\.ndjson(?:\.gz)?$")

def _utc_day_from_ts_ms(ts_ms: int) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(int(ts_ms) / 1000))

def _day_range_from_ms(start_ms: Optional[int], end_ms: Optional[int], *, lookback_days: int) -> Tuple[str, str]:
    now = _now_ms()
    end = int(end_ms) if end_ms and int(end_ms) > 0 else int(now)
    start = int(start_ms) if start_ms and int(start_ms) > 0 else int(end - int(lookback_days) * 86400 * 1000)
    if start > end:
        start, end = end, start
    return _utc_day_from_ts_ms(start), _utc_day_from_ts_ms(end)

def _list_archive_files(archive_dir: str, *, start_ms: Optional[int], end_ms: Optional[int], lookback_days: int) -> List[str]:
    d = str(archive_dir or "").strip()
    if not d:
        return []
    if not os.path.isdir(d):
        return []
    day_a, day_b = _day_range_from_ms(start_ms, end_ms, lookback_days=int(lookback_days))
    names: List[str] = []
    for nm in os.listdir(d):
        m = _DAY_RE.match(str(nm))
        if not m:
            continue
        day = m.group(1)
        if day < day_a or day > day_b:
            continue
        names.append(os.path.join(d, nm))
    names.sort()
    return names

def _open_text_maybe_gzip(path: str):
    if str(path).endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return open(path, "r", encoding="utf-8", errors="replace")

def _read_archive_items(
    archive_dir: str,
    *,
    start_ms: Optional[int],
    end_ms: Optional[int],
    lookback_days: int,
    max_records: int,
) -> Tuple[List[Tuple[str, Dict[str, Any]]], Dict[str, Any]]:
    files = _list_archive_files(archive_dir, start_ms=start_ms, end_ms=end_ms, lookback_days=int(lookback_days))
    items: List[Tuple[str, Dict[str, Any]]] = []
    stats = {"files": files, "lines": 0, "parsed": 0, "json_errors": 0}
    if not files:
        return items, stats
    limit = int(max_records) if int(max_records) > 0 else 10_000_000

    for fp in files:
        if len(items) >= limit:
            break
        try:
            with _open_text_maybe_gzip(fp) as f:
                for ln, line in enumerate(f, start=1):
                    if len(items) >= limit:
                        break
                    s = (line or "").strip()
                    if not s:
                        continue
                    stats["lines"] = int(stats["lines"]) + 1
                    try:
                        obj = json.loads(s)
                    except Exception:
                        stats["json_errors"] = int(stats["json_errors"]) + 1
                        continue
                    if not isinstance(obj, dict):
                        continue
                    stats["parsed"] = int(stats["parsed"]) + 1
                    msg_id = str(obj.get("stream_id") or f"file:{os.path.basename(fp)}:{ln}")
                    items.append((msg_id, obj))
        except Exception:
            continue
    # Apply time filtering using both top-level and payload/meta fields.
    items = _filter_by_time(items, ts_field_candidates=("exit_ts_ms", "ts_ms", "ts", "t"), start_ms=start_ms, end_ms=end_ms)
    return items, stats




def _filter_by_time(
    items: Sequence[Tuple[str, Dict[str, Any]]],
    *,
    ts_field_candidates: Sequence[str],
    start_ms: Optional[int],
    end_ms: Optional[int],
) -> List[Tuple[str, Dict[str, Any]]]:
    if not start_ms and not end_ms:
        return list(items)

    def _pick_ts(f: Dict[str, Any]) -> int:
        ts = 0
        for k in ts_field_candidates:
            if k in f:
                ts = _as_int(f.get(k), 0)
                if ts > 0:
                    break
        if ts <= 0:
            payload = _safe_json_loads(f.get("payload"))
            if isinstance(payload, dict):
                for k in ts_field_candidates:
                    if k in payload:
                        ts = _as_int(payload.get(k), 0)
                        if ts > 0:
                            break
                if ts <= 0:
                    meta = payload.get("meta") or payload.get("metadata")
                    if isinstance(meta, dict):
                        for k in ts_field_candidates:
                            if k in meta:
                                ts = _as_int(meta.get(k), 0)
                                if ts > 0:
                                    break
        # heuristic: seconds → ms
        if 0 < ts < 10_000_000_000:
            ts *= 1000
        return int(ts)

    out: List[Tuple[str, Dict[str, Any]]] = []
    for _id, f in items:
        ts = _pick_ts(f)
        if start_ms and ts and int(ts) < int(start_ms):
            continue
        if end_ms and ts and int(ts) > int(end_ms):
            continue
        out.append((_id, f))
    return out

@dataclass(frozen=True)
class SignalRow:
    sid: str
    ts_ms: int
    symbol: str
    direction: str
    scenario: str
    indicators: Dict[str, Any]


@dataclass(frozen=True)
class CloseRow:
    sid: str
    close_ts_ms: int
    symbol: str
    pnl: float
    risk_usd: float
    # Optional metadata for stronger nearest-join disambiguation.
    direction: str = ""
    scenario: str = ""
    buckets: Dict[str, Any] = field(default_factory=dict, compare=False)


@dataclass
class DropStats:
    max_examples: int = 50
    counts: Dict[str, int] = field(default_factory=dict)
    examples: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)

    def add(self, reason: str, example: Optional[Dict[str, Any]] = None) -> None:
        r = str(reason)
        self.counts[r] = int(self.counts.get(r, 0)) + 1
        if example is None:
            return
        ex = self.examples.get(r)
        if ex is None:
            ex = []
            self.examples[r] = ex
        if len(ex) < int(self.max_examples):
            ex.append(example)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "counts": dict(sorted(self.counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))),
            "examples": self.examples,
        }


@dataclass
class NearestJoinStats:
    max_examples: int = 50
    candidates: List[int] = field(default_factory=list)
    candidates_after_secondary: List[int] = field(default_factory=list)
    ambiguous: int = 0
    ambiguous_examples: List[Dict[str, Any]] = field(default_factory=list)
    bucket_used: int = 0

    def add(
        self,
        *,
        cand_n: int,
        cand2_n: int,
        ambiguous: bool,
        example: Optional[Dict[str, Any]] = None,
        bucket_used: bool = False,
    ) -> None:
        self.candidates.append(int(cand_n))
        self.candidates_after_secondary.append(int(cand2_n))
        if ambiguous:
            self.ambiguous += 1
            if example is not None and len(self.ambiguous_examples) < int(self.max_examples):
                self.ambiguous_examples.append(example)
        if bucket_used:
            self.bucket_used += 1

    def _pct(self, xs: List[int], q: float) -> int:
        if not xs:
            return 0
        s = sorted(int(x) for x in xs)
        idx = int(q * (len(s) - 1))
        idx = max(0, min(len(s) - 1, idx))
        return int(s[idx])

    def summary(self) -> Dict[str, Any]:
        return {
            "n": int(len(self.candidates)),
            "cand_p50": int(self._pct(self.candidates, 0.50)),
            "cand_p95": int(self._pct(self.candidates, 0.95)),
            "cand2_p50": int(self._pct(self.candidates_after_secondary, 0.50)),
            "cand2_p95": int(self._pct(self.candidates_after_secondary, 0.95)),
            "ambiguous": int(self.ambiguous),
            "ambiguous_examples": self.ambiguous_examples,
            "bucket_used": int(self.bucket_used),
        }


class QuarantineWriter:
    def __init__(self, path: str):
        self.path = str(path)
        os.makedirs(os.path.dirname(os.path.abspath(self.path)) or ".", exist_ok=True)
        self._f = open(self.path, "w", encoding="utf-8")

    def write(self, kind: str, reason: str, *, stream: str, msg_id: str, data: Any) -> None:
        rec = {
            "kind": str(kind),
            "reason": str(reason),
            "stream": str(stream),
            "id": str(msg_id),
            "data": _minify(data),
        }
        self._f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")))
        self._f.write("\n")

    def close(self) -> None:
        try:
            self._f.close()
        except Exception:
            pass


def _minify(x: Any, *, max_len: int = 4000, max_items: int = 64) -> Any:
    """Keep quarantine entries small and safe."""
    if x is None:
        return None
    if isinstance(x, (int, float, bool)):
        return x
    if isinstance(x, bytes):
        s = x.decode("utf-8", "ignore")
        return s[:max_len]
    if isinstance(x, str):
        return x[:max_len]
    if isinstance(x, dict):
        out: Dict[str, Any] = {}
        n = 0
        for k, v in x.items():
            if n >= max_items:
                out["__truncated__"] = True
                break
            out[_as_str(k)] = _minify(v, max_len=max_len, max_items=max_items)
            n += 1
        return out
    if isinstance(x, list):
        out = []
        for i, v in enumerate(x):
            if i >= max_items:
                out.append({"__truncated__": True})
                break
            out.append(_minify(v, max_len=max_len, max_items=max_items))
        return out
    return _as_str(x)[:max_len]


def parse_replay_signal(fields: Dict[str, Any]) -> Optional[SignalRow]:
    """Parse one entry from ml_replay_inputs_v1 (or similar)."""
    payload = _safe_json_loads(fields.get("payload"))
    if not isinstance(payload, dict):
        # some deployments may store indicators directly
        payload = fields

    ts_ms = _as_int(payload.get("ts_ms") or fields.get("ts_ms"), 0)
    symbol = _as_str(payload.get("symbol") or fields.get("symbol")).upper()
    direction = _as_str(payload.get("direction") or fields.get("direction")).upper()
    scenario = _as_str(
        payload.get("scenario_v4")
        or payload.get("scenario")
        or fields.get("scenario_v4")
        or fields.get("scenario")
        or ""
    )

    indicators = payload.get("indicators") or payload.get("features") or {}
    if isinstance(indicators, str):
        indicators2 = _safe_json_loads(indicators)
        indicators = indicators2 if isinstance(indicators2, dict) else {}
    if not isinstance(indicators, dict):
        indicators = {}

    # Backward compatibility: some producers may nest the original decision under payload["decision"]
    # (older joiners emitted {sid, decision, close, label} — Commit 8 path without OF-input lookup).
    if (not symbol or ts_ms <= 0) and isinstance(payload.get("decision"), dict):
        dec = payload.get("decision") or {}
        ts_ms = _as_int(ts_ms or dec.get("ts_ms") or dec.get("decision_ts_ms") or 0, 0)
        symbol = _as_str(symbol or dec.get("symbol") or dec.get("sym") or "").upper()
        direction = _as_str(direction or dec.get("direction") or dec.get("side") or "").upper()
        if not scenario:
            scenario = _as_str(dec.get("scenario_v4") or dec.get("scenario") or "")

    # Joiner payloads (trade_close_joiner_worker_v5) may nest the decision record.
    # Prefer decision fields when top-level is missing.
    decision = payload.get("decision") if isinstance(payload.get("decision"), dict) else None
    if decision:
        if ts_ms <= 0:
            ts_ms = _as_int(
                decision.get("ts_ms")
                or ((decision.get("inputs") or {}).get("tick_ts_ms") if isinstance(decision.get("inputs"), dict) else 0),
                0,
            )
        if not symbol:
            symbol = _as_str(decision.get("symbol") or "").upper()
        if not direction:
            direction = _as_str(decision.get("direction") or "").upper()
        if not scenario:
            rule = decision.get("rule") if isinstance(decision.get("rule"), dict) else {}
            scenario = _as_str(
                rule.get("scenario_v4")
                or rule.get("scenario")
                or decision.get("scenario_v4")
                or decision.get("scenario")
                or ""
            )
        if not indicators:
            ind = decision.get("features") or decision.get("indicators") or decision.get("inputs") or {}
            if isinstance(ind, str):
                ind2 = _safe_json_loads(ind)
                ind = ind2 if isinstance(ind2, dict) else {}
            indicators = ind if isinstance(ind, dict) else {}

    # OFInputsV2 flat payload: session one-hot fields live at root level (not under "indicators").
    # Pull them into indicators dict so infer_feature_cols() can pick them up as f_session_* columns.
    _SESSION_KEYS = ("session_asia", "session_eu", "session_us", "session_off")
    if not any(k in indicators for k in _SESSION_KEYS):
        for _sk in _SESSION_KEYS:
            if _sk in payload:
                indicators[_sk] = _as_int(payload.get(_sk), 0)

    raw_sid = (
        payload.get("sid")
        or indicators.get("sid")
        or fields.get("sid")
        or fields.get("signal_id")
        or ""
    )
    if not symbol or ts_ms <= 0:
        # cannot build canonical sid
        return None
    sid = _normalize_sid(raw_sid, symbol=symbol, ts_ms=ts_ms)

    if direction not in ("BUY", "SELL"):
        # normalize to BUY/SELL to be consistent with gate
        if direction in ("LONG", "B"):
            direction = "BUY"
        elif direction in ("SHORT", "S"):
            direction = "SELL"
        else:
            direction = "BUY"

    return SignalRow(
        sid=sid,
        ts_ms=int(ts_ms),
        symbol=symbol,
        direction=direction,
        scenario=str(scenario),
        indicators=indicators,
    )


def parse_trade_closed(fields: Dict[str, Any]) -> Optional[CloseRow]:
    """Parse one entry from trades:closed (or similar)."""

    # trades:closed commonly stores a payload-only JSON field ("payload").
    # Merge payload into the top-level view for backward-compatible parsing.
    payload_obj = _safe_json_loads(fields.get("payload"))
    if isinstance(payload_obj, dict):
        merged: Dict[str, Any] = dict(payload_obj)  # payload takes precedence
        for k, v in fields.items():
            if k not in merged:
                merged[k] = v
        fields = merged

    symbol = str(fields.get("symbol") or fields.get("sym") or fields.get("s") or "").strip()
    close_ts_ms = _as_int(
        fields.get("exit_ts_ms") or fields.get("ts_ms") or fields.get("ts") or 0, 0
    )
    if close_ts_ms <= 0:
        close_ts_ms = 0

    # raw sid might live in fields or in meta/metadata
    raw_sid = fields.get("sid") or fields.get("signal_id") or ""
    meta = _safe_json_loads(fields.get("meta") or fields.get("metadata"))
    if isinstance(meta, dict):
        raw_sid = raw_sid or meta.get("sid") or meta.get("signal_id") or ""
        if not symbol:
            symbol = _as_str(meta.get("symbol") or "").upper()
        if close_ts_ms <= 0:
            close_ts_ms = _as_int(meta.get("exit_ts_ms") or meta.get("ts_ms") or 0, 0)

    if not symbol:
        return None

    raw_sid_str = _as_str(raw_sid).strip()
    if not raw_sid_str:
        return None

    sid = _normalize_sid(raw_sid_str, symbol=symbol, ts_ms=close_ts_ms or 0)

    pnl = _as_float(fields.get("pnl") or fields.get("pnl_net") or 0.0, 0.0)
    risk_usd = _as_float(fields.get("risk_usd") or fields.get("risk_amount") or fields.get("one_r_money") or 0.0, 0.0)
    if risk_usd <= 0.0 and isinstance(meta, dict):
        risk_usd = _as_float(meta.get("risk_usd") or meta.get("risk_amount") or meta.get("one_r_money") or 0.0, 0.0)
    if pnl == 0.0 and isinstance(meta, dict):
        pnl = _as_float(meta.get("pnl") or meta.get("pnl_net") or 0.0, 0.0)

    direction = _norm_dir(fields.get("direction") or fields.get("side") or "")
    scenario = _norm_scenario(fields.get("scenario") or fields.get("scenario_v4") or "")
    buckets: Dict[str, Any] = {}
    for k in ("session_bucket", "spread_bucket", "spread_bps_bucket", "liq_bucket", "vol_bucket", "regime_bucket"):
        if k in fields and fields.get(k) is not None:
            buckets[k] = fields.get(k)
    if isinstance(meta, dict):
        direction = direction or _norm_dir(meta.get("direction") or meta.get("side") or meta.get("pos_side") or "")
        scenario = scenario or _norm_scenario(meta.get("scenario") or meta.get("scenario_v4") or "")
        for k in ("session_bucket", "spread_bucket", "spread_bps_bucket", "liq_bucket", "vol_bucket", "regime_bucket"):
            if k not in buckets:
                v = meta.get(k)
                if v is not None:
                    buckets[k] = v

    return CloseRow(
        sid=sid,
        close_ts_ms=int(close_ts_ms or 0),
        symbol=symbol,
        pnl=float(pnl),
        risk_usd=float(risk_usd),
        direction=str(direction),
        scenario=str(scenario),
        buckets=buckets,
    )


def load_tb_labels_from_stream(
    r: "redis.Redis",
    *,
    stream: str,
    field: str,
    count: int,
) -> Dict[str, Dict[str, Any]]:
    """Load most recent TB labels into {sid -> payload} map."""
    out: Dict[str, Dict[str, Any]] = {}
    try:
        msgs = r.xrevrange(stream, max="+", min="-", count=int(count))
    except Exception:
        return out
    for _id, fields in msgs:
        if not isinstance(fields, dict):
            continue
        raw = fields.get(field)
        payload = _safe_json_loads(raw)
        if not payload:
            continue
        sid = str(payload.get("sid", "") or "")
        if not sid:
            continue
        if sid in out:
            continue  # keep latest
        out[sid] = payload
    return out


def r_mult_and_label(pnl: float, risk_usd: float, *, y_min_r: float) -> Tuple[float, int]:
    if risk_usd <= 0.0:
        return 0.0, 0
    r = float(pnl) / float(risk_usd)
    y = 1 if r >= float(y_min_r) else 0
    return float(r), int(y)


def _build_signal_map(signals: Sequence[SignalRow], *, dedup_signals: str) -> Dict[str, SignalRow]:
    smap: Dict[str, SignalRow] = {}
    for s in signals:
        if not s.sid:
            continue
        if s.sid not in smap:
            smap[s.sid] = s
            continue
        if dedup_signals == "latest":
            if int(s.ts_ms) >= int(smap[s.sid].ts_ms):
                smap[s.sid] = s
        elif dedup_signals == "earliest":
            if int(s.ts_ms) < int(smap[s.sid].ts_ms):
                smap[s.sid] = s
    return smap


def _build_signal_index_by_symbol(signals: Sequence[SignalRow]) -> Dict[str, List[Tuple[int, str]]]:
    by_sym: Dict[str, List[Tuple[int, str]]] = {}
    for s in signals:
        sym = str(s.symbol or "").upper()
        if not sym:
            continue
        by_sym.setdefault(sym, []).append((int(s.ts_ms), str(s.sid)))
    for sym, arr in by_sym.items():
        arr.sort(key=lambda x: (int(x[0]), str(x[1])))
    return by_sym


def _nearest_signal_for_ts(
    arr: List[Tuple[int, str]],
    times: List[int],
    ts_ms: int,
) -> Optional[Tuple[int, str, int]]:
    """Return (signal_ts_ms, signal_sid, delta_ms=ts_ms-signal_ts_ms) for nearest signal."""
    if not arr or not times:
        return None
    pos = bisect.bisect_left(times, int(ts_ms))
    candidates: List[Tuple[int, str]] = []
    if 0 <= pos < len(arr):
        candidates.append(arr[pos])
    if pos - 1 >= 0:
        candidates.append(arr[pos - 1])
    best: Optional[Tuple[int, str, int]] = None
    best_abs: Optional[int] = None
    for t, sid in candidates:
        d = int(ts_ms) - int(t)
        a = abs(int(d))
        if best is None or best_abs is None or a < int(best_abs):
            best = (int(t), str(sid), int(d))
            best_abs = int(a)
    return best


def _split_csv_keys(x: Any) -> List[str]:
    s = _as_str(x).strip()
    if not s:
        return []
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p]


def _nearest_candidates_for_ts(
    arr: List[Tuple[int, str]],
    times: List[int],
    ts_ms: int,
    *,
    tol_ms: int,
    max_scan: int,
) -> List[Tuple[int, str, int]]:
    """Return candidate signals around ts_ms within tol_ms.

    Returns list of (signal_ts_ms, signal_sid, delta_ms=ts_ms-signal_ts_ms) sorted by:
      abs(delta), signal_ts_ms, sid (deterministic).
    """
    if not arr or not times:
        return []
    ts_ms_i = int(ts_ms)
    tol_i = int(tol_ms or 0)
    max_scan_i = max(1, int(max_scan or 1))

    pos = bisect.bisect_left(times, ts_ms_i)
    out: List[Tuple[int, str, int]] = []

    # scan left
    steps = 0
    j = pos - 1
    while j >= 0 and steps < max_scan_i:
        t, sid = arr[j]
        d = ts_ms_i - int(t)
        if tol_i > 0 and abs(int(d)) > tol_i:
            if int(t) < ts_ms_i - tol_i:
                break
        out.append((int(t), str(sid), int(d)))
        j -= 1
        steps += 1

    # scan right
    steps = 0
    j = pos
    while j < len(arr) and steps < max_scan_i:
        t, sid = arr[j]
        d = ts_ms_i - int(t)
        if tol_i > 0 and abs(int(d)) > tol_i:
            if int(t) > ts_ms_i + tol_i:
                break
        out.append((int(t), str(sid), int(d)))
        j += 1
        steps += 1

    if tol_i > 0:
        out = [x for x in out if abs(int(x[2])) <= tol_i]

    out.sort(key=lambda x: (abs(int(x[2])), int(x[0]), str(x[1])))
    return out


def _secondary_match(close: CloseRow, sig: SignalRow, *, mode: str) -> bool:
    m = str(mode or "none").strip().lower()
    if m in ("", "none", "off", "0"):
        return True

    cdir = _norm_dir(getattr(close, "direction", ""))
    sdir = _norm_dir(getattr(sig, "direction", ""))
    csc = _norm_scenario(getattr(close, "scenario", ""))
    ssc = _norm_scenario(getattr(sig, "scenario", ""))

    soft = m.endswith("_soft")

    def _dir_ok() -> bool:
        if not cdir:
            return True if soft else False
        if not sdir:
            return True if soft else False
        return cdir == sdir

    def _sc_ok() -> bool:
        if not csc:
            return True if soft else False
        if not ssc:
            return True if soft else False
        return csc == ssc

    base = m.replace("_soft", "")
    if base == "dir":
        return _dir_ok()
    if base == "scenario":
        return _sc_ok()
    if base in ("dir_scenario", "scenario_dir"):
        return _dir_ok() and _sc_ok()

    # Unknown mode: fail-closed to avoid silent mis-joins
    return False


def _bucket_score(close: CloseRow, sig: SignalRow, keys: Sequence[str]) -> int:
    if not keys:
        return 0
    cb = getattr(close, "buckets", {}) or {}
    ind = getattr(sig, "indicators", {}) or {}
    score = 0
    for k in keys:
        kk = str(k)
        if not kk:
            continue
        cv = cb.get(kk)
        sv = ind.get(kk)
        if cv is None or sv is None:
            continue
        if str(cv) == str(sv):
            score += 1
    return int(score)


def diagnose_unmatched_closes(
    unmatched_closes: Sequence[CloseRow],
    *,
    signal_index_by_symbol: Dict[str, List[Tuple[int, str]]],
    max_examples: int = 50,
) -> Dict[str, Any]:
    """For each unmatched close, find nearest signal by time for same symbol and bucketize deltas."""
    buckets = [
        (1_000, "<=1s"),
        (10_000, "<=10s"),
        (60_000, "<=60s"),
        (300_000, "<=5m"),
        (10**18, ">5m"),
    ],
    counts: Dict[str, int] = {name: 0 for _, name in buckets}
    examples: List[Dict[str, Any]] = []

    for c in unmatched_closes:
        sym = str(c.symbol or "").upper()
        arr = signal_index_by_symbol.get(sym) or []
        if not arr:
            continue
        times = [t for (t, _) in arr]
        pos = bisect.bisect_left(times, int(c.close_ts_ms))
        candidates: List[Tuple[int, str]] = []
        if 0 <= pos < len(arr):
            candidates.append(arr[pos])
        if pos - 1 >= 0:
            candidates.append(arr[pos - 1])
        # choose nearest
        best = None
        best_abs = None
        for t, sid in candidates:
            d = int(c.close_ts_ms) - int(t)
            a = abs(int(d))
            if best is None or a < int(best_abs):
                best = (t, sid, d)
                best_abs = a
        if best is None:
            continue
        t, sid, d = best
        # bucket
        name = None
        for thr, nm in buckets:
            if abs(int(d)) <= int(thr):
                name = nm
                break
        if name is None:
            name = buckets[-1][1]
        counts[name] = int(counts.get(name, 0)) + 1
        if len(examples) < int(max_examples):
            examples.append(
                {
                    "symbol": sym,
                    "sid_close": str(c.sid),
                    "close_ts_ms": int(c.close_ts_ms),
                    "nearest_signal_sid": str(sid),
                    "nearest_signal_ts_ms": int(t),
                    "delta_ms": int(d),
                }
            )

    return {"counts": counts, "examples": examples}


def join_signals_with_closes_v2(
    signals: Sequence[SignalRow],
    closes: Sequence[CloseRow],
    *,
    y_min_r: float,
    dedup_signals: str = "latest",
    drop_invalid_risk: bool = False,
    join_strategy: str = "sid_or_nearest",
    join_tolerance_ms: int = 10_000,
    join_secondary: str = "dir_scenario_soft",
    nearest_max_scan: int = 50,
    join_bucket_keys: Optional[Sequence[str]] = None,
    join_debug: Optional[Dict[str, Any]] = None,
    drop: Optional[DropStats] = None,
    quarantine: Optional[QuarantineWriter] = None,
    closed_stream: str = "",
    tb_by_sid: Optional[Dict[str, Dict[str, Any]]] = None,
    label_source: str = "closed",
    tb_util_min_r: float = 0.0,
) -> Tuple[List[Dict[str, Any]], List[CloseRow]]:
    """Join signals with closes and build JSONL rows.

    join_strategy:
      - "sid": strict join by canonical sid only
      - "nearest": join by nearest signal ts_ms for same symbol (within join_tolerance_ms)
      - "sid_or_nearest": try sid first, then nearest fallback (within join_tolerance_ms)
    """
    smap = _build_signal_map(signals, dedup_signals=dedup_signals)

    js = str(join_strategy or "sid").strip().lower()
    if js not in ("sid", "nearest", "sid_or_nearest"):
        js = "sid"
    tol_ms = int(join_tolerance_ms or 0)
    use_nearest = js in ("nearest", "sid_or_nearest")
    sec_mode = str(join_secondary or "none").strip().lower()
    allowed_sec = {
        "none",
        "dir",
        "scenario",
        "dir_scenario",
        "dir_soft",
        "scenario_soft",
        "dir_scenario_soft",
    }
    if sec_mode not in allowed_sec:
        sec_mode = "none"

    bucket_keys: List[str] = []
    if join_bucket_keys is not None:
        if isinstance(join_bucket_keys, (list, tuple)):
            for it in join_bucket_keys:
                bucket_keys.extend(_split_csv_keys(it))
        else:
            bucket_keys = _split_csv_keys(join_bucket_keys)

    nearest_stats = NearestJoinStats(max_examples=int(getattr(drop, "max_examples", 50)))


    # Optional nearest-join index (built over deduped signals)
    sig_index_by_symbol: Dict[str, List[Tuple[int, str]]] = {}
    sig_times_by_symbol: Dict[str, List[int]] = {}
    if use_nearest:
        uniq_signals = list(smap.values())
        sig_index_by_symbol = _build_signal_index_by_symbol(uniq_signals)
        sig_times_by_symbol = {sym: [t for (t, _sid) in arr] for sym, arr in sig_index_by_symbol.items()}

    out: List[Dict[str, Any]] = []
    unmatched: List[CloseRow] = []

    for c in closes:
        if drop_invalid_risk and float(c.risk_usd) <= 0.0:
            if drop is not None:
                drop.add(
                    "close_invalid_risk",
                    {"sid": str(c.sid), "symbol": str(c.symbol), "risk_usd": float(c.risk_usd), "pnl": float(c.pnl)},
                )
            if quarantine is not None:
                quarantine.write("close", "close_invalid_risk", stream=closed_stream, msg_id="", data={"sid": c.sid})
            continue

        s = None if js == "nearest" else smap.get(c.sid)
        join_method = "sid" if s is not None else "nearest"
        join_bucket_score = 0
        join_cand_n = 0
        join_cand2_n = 0

        if (s is None) and use_nearest and int(c.close_ts_ms) > 0:
            sym = str(c.symbol or "").upper()
            arr = sig_index_by_symbol.get(sym) or []
            times = sig_times_by_symbol.get(sym) or []

            cands = _nearest_candidates_for_ts(
                arr,
                times,
                int(c.close_ts_ms),
                tol_ms=tol_ms,
                max_scan=int(nearest_max_scan),
            )
            join_cand_n = int(len(cands))

            filtered: List[Tuple[int, int, str, int]] = []  # (bucket_score, t, sid, delta)
            for t, sid_near, d_ms in cands:
                s2 = smap.get(str(sid_near))
                if s2 is None:
                    continue
                if not _secondary_match(c, s2, mode=sec_mode):
                    continue
                sc = _bucket_score(c, s2, bucket_keys)
                filtered.append((int(sc), int(t), str(sid_near), int(d_ms)))

            join_cand2_n = int(len(filtered))

            # Record nearest-join diagnostics (even if join fails)
            ambiguous = bool(join_cand2_n > 1)
            ex = None
            if ambiguous:
                ex = {
                    "symbol": sym,
                    "sid_close": str(c.sid),
                    "close_ts_ms": int(c.close_ts_ms),
                    "join_secondary": str(sec_mode),
                    "bucket_keys": list(bucket_keys),
                    "candidates": [
                        {"sid": sid, "ts_ms": int(t), "delta_ms": int(d), "bucket_score": int(sc)}
                        for (sc, t, sid, d) in filtered[:5]
                    ],
                }
            nearest_stats.add(
                cand_n=join_cand_n,
                cand2_n=join_cand2_n,
                ambiguous=ambiguous,
                example=ex,
                bucket_used=bool(bucket_keys) and any(int(sc) > 0 for (sc, _t, _sid, _d) in filtered[:1]),
            )

            if filtered:
                # Pick best deterministically: more bucket matches, then closer in time.
                filtered.sort(key=lambda x: (-int(x[0]), abs(int(x[3])), int(x[1]), str(x[2])))
                best_sc, _best_t, best_sid, _best_d = filtered[0]
                s = smap.get(str(best_sid))
                if s is not None:
                    join_method = "nearest"
                    join_bucket_score = int(best_sc)
            else:
                # No candidate passed the secondary filter (or no candidates within tol)
                if drop is not None:
                    if join_cand_n <= 0 and tol_ms > 0 and arr:
                        best_any = _nearest_signal_for_ts(arr, times, int(c.close_ts_ms))
                        if best_any is not None:
                            _t_near, sid_near, d_ms = best_any
                            drop.add(
                                "join_nearest_too_far",
                                {
                                    "symbol": sym,
                                    "sid_close": str(c.sid),
                                    "close_ts_ms": int(c.close_ts_ms),
                                    "nearest_signal_sid": str(sid_near),
                                    "delta_ms": int(d_ms),
                                    "tolerance_ms": int(tol_ms),
                                },
                            )
                        else:
                            drop.add(
                                "join_nearest_no_candidates",
                                {"symbol": sym, "sid_close": str(c.sid), "close_ts_ms": int(c.close_ts_ms)},
                            )
                    elif join_cand_n <= 0:
                        drop.add(
                            "join_nearest_no_candidates",
                            {"symbol": sym, "sid_close": str(c.sid), "close_ts_ms": int(c.close_ts_ms)},
                        )
                    else:
                        drop.add(
                            "join_secondary_no_match",
                            {
                                "symbol": sym,
                                "sid_close": str(c.sid),
                                "close_ts_ms": int(c.close_ts_ms),
                                "join_secondary": str(sec_mode),
                                "bucket_keys": list(bucket_keys),
                                "nearest_candidates": [
                                    {"sid": str(sid_near), "ts_ms": int(t), "delta_ms": int(d_ms)}
                                    for (t, sid_near, d_ms) in cands[:5]
                                ],
                            },
                        )

        if s is None:
            unmatched.append(c)
            if drop is not None:
                drop.add("join_no_signal", {"sid": str(c.sid), "symbol": str(c.symbol), "close_ts_ms": int(c.close_ts_ms)})
            continue

        r_mult_closed, y_closed = r_mult_and_label(c.pnl, c.risk_usd, y_min_r=y_min_r)

        # Optional TB label override (from labels:tb)
        r_mult = float(r_mult_closed)
        y = int(y_closed)
        src = "closed"
        tb = tb_by_sid.get(str(s.sid)) if isinstance(tb_by_sid, dict) else None
        tb_primary = tb.get("primary", {}) if isinstance(tb, dict) else {}
        tb_meta = tb.get("meta", {}) if isinstance(tb, dict) else {}

        if tb and str(label_source) in ("tb_primary", "tb_util"):
            if str(label_source) == "tb_primary" and isinstance(tb_primary, dict) and tb_primary:
                y = int(tb_primary.get("y_edge", 0) or 0)
                r_mult = float(tb_primary.get("r_mult", 0.0) or 0.0)
                src = "tb_primary"
            elif str(label_source) == "tb_util" and isinstance(tb_meta, dict) and tb_meta:
                util_r = float(tb_meta.get("util_r", 0.0) or 0.0)
                y = 1 if util_r >= float(tb_util_min_r) else 0
                r_mult = float(util_r)
                src = "tb_util"

        out.append(
            {
                "ts_ms": int(s.ts_ms),
                "close_ts_ms": int(c.close_ts_ms or 0),
                "sid": str(s.sid),
                "sid_close": str(c.sid),
                "join_method": str(join_method),
                "join_delta_ms": int(int(c.close_ts_ms or 0) - int(s.ts_ms or 0)),
                "join_secondary": str(sec_mode) if str(join_method) == "nearest" else "",
                "join_bucket_score": int(join_bucket_score),
                "join_candidate_n": int(join_cand_n),
                "join_candidate2_n": int(join_cand2_n),
                "symbol": str(s.symbol),
                "direction": str(s.direction),
                "scenario": str(s.scenario),
                "indicators": s.indicators or {},
                "pnl": float(c.pnl),
                "risk_usd": float(c.risk_usd),
                "label_source": str(src),
                "r_mult_closed": float(r_mult_closed),
                "y_closed": int(y_closed),
                "r_mult": float(r_mult),
                "y": int(y),
                "tb_primary_label": str(tb_primary.get("label", "") or "") if isinstance(tb_primary, dict) else "",
                "tb_primary_ret_bps": float(tb_primary.get("ret_bps", 0.0) or 0.0) if isinstance(tb_primary, dict) else 0.0,
                "tb_primary_y_edge": int(tb_primary.get("y_edge", 0) or 0) if isinstance(tb_primary, dict) else 0,
                "tb_util_r": float(tb_meta.get("util_r", 0.0) or 0.0) if isinstance(tb_meta, dict) else 0.0,
                "tb_exec_cost_r": float(tb_meta.get("exec_cost_r", 0.0) or 0.0) if isinstance(tb_meta, dict) else 0.0,
            }
        )

    out.sort(key=lambda r: (int(r.get("ts_ms", 0)), str(r.get("sid", ""))))
    if join_debug is not None:
        join_debug["join_secondary"] = str(sec_mode)
        join_debug["join_bucket_keys"] = list(bucket_keys)
        join_debug["nearest_max_scan"] = int(nearest_max_scan)
        join_debug["nearest_join"] = nearest_stats.summary()

    return out, unmatched


def join_signals_with_closes(
    signals: Sequence[SignalRow],
    closes: Sequence[CloseRow],
    *,
    y_min_r: float,
    dedup_signals: str = "latest",
) -> List[Dict[str, Any]]:
    """Backward-compatible join (v1 signature)."""
    rows, _unmatched = join_signals_with_closes_v2(
        signals,
        closes,
        y_min_r=y_min_r,
        dedup_signals=dedup_signals,
        drop_invalid_risk=False,
        join_strategy="sid",
    )
    return rows


def validate_feature_cols_strict(
    feature_cols: Sequence[str],
    *,
    strict_feature_cols: bool,
    forbid_scenario_v4_onehot: bool,
) -> None:
    """Strict schema guardrails for feature_cols.

    Purpose:
      - Prevent unbounded cardinality drifts (e.g. scenario_v4_* exploding).
      - Make train-time and serve-time schemas explicitly compatible.

    In strict mode we allow only bucket-based scenarios (bucket:trend|range|other),
    and we forbid raw scenario_v4_* one-hots.
    """
    if not bool(strict_feature_cols):
        return

    if bool(forbid_scenario_v4_onehot):
        bad = [str(c) for c in feature_cols if str(c).startswith("scenario_v4_")]
        if bad:
            ex = bad[0]
            raise ValueError(
                f"forbidden_feature_cols: scenario_v4_* is not allowed in strict mode (n={len(bad)} ex={ex})"
            )


def infer_feature_cols(
    dataset_rows: Sequence[Dict[str, Any]],
    *,
    max_numeric: int = 128,
    include_direction: bool = True,
    include_scenario: bool = True,
    scenario_prefix: str = "bucket:",
    include_time_onehot: bool = True,
    strict_feature_cols: bool = False,
    forbid_scenario_v4_onehot: bool = False,
) -> List[str]:
    """
    Infer feature column order from dataset rows.

    scenario_prefix:
      - "bucket:"      => stable 3-bucket taxonomy (trend/range/other), avoids cardinality blowups.
      - "scenario_v4_" => legacy: one-hot per observed scenario value (up to 64).
      - any other str  => one-hot per observed scenario, prefixed with that string.

    include_time_onehot:
      When True, appends hour:0..hour:23 and dow:0..dow:6 columns for train+serve parity.
    """
    numeric_keys: Dict[str, int] = {}
    scenarios: Dict[str, int] = {}
    directions: Dict[str, int] = {}
    for r in dataset_rows:
        ind = r.get("indicators") or {}
        if isinstance(ind, str):
            ind2 = _safe_json_loads(ind)
            ind = ind2 if isinstance(ind2, dict) else {}
        if isinstance(ind, dict):
            for k, v in ind.items():
                kk = str(k)
                if kk in ("sid", "symbol", "ts_ms", "direction", "scenario", "scenario_v4"):
                    continue
                # Important: do NOT let DQ policy / runtime meta leak into model feature columns.
                # These keys are either constant knobs (dq_policy_*) or decision outputs (dq_*),
                # and including them in inferred feature_cols breaks train==serve stability.
                if kk.startswith("dq_policy_"):
                    continue
                if kk in (
                    "runtime_start_ts_ms",
                    "dq_uptime_sec",
                    "dq_pen",
                    "dq_veto",
                    "dq_level",
                    "dq_health_score",
                ):
                    continue
                fv = _as_float(v, default=float("nan"))
                if math.isnan(fv):
                    continue
                numeric_keys[kk] = numeric_keys.get(kk, 0) + 1

        d = str(r.get("direction") or "").strip().upper()
        if d:
            directions[d] = directions.get(d, 0) + 1

        sc_raw = _norm_scenario(r.get("scenario") or "")
        if sc_raw:
            sc_key = sc_raw
            if str(scenario_prefix) == "bucket:":
                sc_key = _bucket_from_scenario(sc_raw)
            scenarios[sc_key] = scenarios.get(sc_key, 0) + 1

    # numeric indicators -> f_{key} (keep stable order by freq, then name)
    numerics_sorted = sorted(numeric_keys.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
    cols: List[str] = []
    for k, _cnt in numerics_sorted[: int(max_numeric)]:
        cols.append(f"f_{k}")

    # one-hot for direction
    if include_direction:
        for d in ("BUY", "SELL"):
            if d in directions:
                cols.append(f"direction_{d}")

    # one-hot for scenario/bucket
    if include_scenario:
        if str(scenario_prefix) == "bucket:":
            # stable 3-bucket taxonomy (avoid scenario-cardinality blowups)
            for b in ("trend", "range", "other"):
                cols.append(f"bucket:{b}")
        else:
            sc_sorted = sorted(scenarios.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
            for sc, _cnt in sc_sorted[:64]:
                cols.append(f"{scenario_prefix}{sc}")

    # one-hot for UTC hour/day-of-week (train+serve parity with schemas v3/v4)
    if include_time_onehot:
        for h in range(24):
            cols.append(f"hour:{h}")
        for d in range(7):
            cols.append(f"dow:{d}")

    validate_feature_cols_strict(
        cols,
        strict_feature_cols=bool(strict_feature_cols),
        forbid_scenario_v4_onehot=bool(forbid_scenario_v4_onehot),
    )
    return cols


def _write_jsonl(path: str, rows: Sequence[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")



def build_dataset_df(
    redis_url: str,
    lookback_hours: int,
    max_rows: Optional[int] = None,
    signal_stream: str = "signals:of:inputs",
    closed_stream: str = "trades:closed",
) -> pd.DataFrame:
    """
    Helper function for P60 shadow evaluation.
    Builds a joined dataset from Redis and returns it as a pandas DataFrame.
    """
    import pandas as pd
    try:
        import redis
    except ImportError:
        raise ImportError("redis-py is required for build_dataset_df")

    r = redis.Redis.from_url(redis_url, decode_responses=False)

    # Calculate time window
    now = _now_ms()
    since_ms = now - (lookback_hours * 3600 * 1000)

    # Fetch data
    count = max_rows if max_rows and max_rows > 0 else 200000

    sig_items = _xrevrange_recent(r, signal_stream, count=count)
    close_items = _xrevrange_recent(r, closed_stream, count=count)

    sig_items = _filter_by_time(sig_items, ts_field_candidates=("ts_ms", "t", "ts"), start_ms=since_ms, end_ms=None)
    close_items = _filter_by_time(
        close_items, ts_field_candidates=("exit_ts_ms", "ts_ms", "ts"), start_ms=since_ms, end_ms=None
    )

    signals: List[SignalRow] = []
    for msg_id, f in sig_items:
        s = parse_replay_signal(f)
        if s:
            signals.append(s)

    closes: List[CloseRow] = []
    for msg_id, f in close_items:
        c = parse_trade_closed(f)
        if c:
            closes.append(c)

    # Join
    rows, _ = join_signals_with_closes_v2(
        signals,
        closes,
        y_min_r=0.10,  # default
        dedup_signals="latest",
        join_strategy="sid_or_nearest",
    )

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows)


def main(argv: Optional[Sequence[str]] = None) -> int:

    ap = argparse.ArgumentParser()
    ap.add_argument("--redis_url", default=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    ap.add_argument(
        "--signal_stream",
        default=os.environ.get("ML_REPLAY_STREAM", "signals:of:inputs"),
        help="Redis stream with ML inputs (indicators+sid). Accepts aliases (v5_of, ml_replay_inputs_v1) or full stream names (signals:of:inputs).",
    )
    ap.add_argument("--closed_stream", default=os.environ.get("TRADES_CLOSED_STREAM", "trades:closed"))
    ap.add_argument("--signals_count", type=int, default=int(os.environ.get("SIGNALS_COUNT", "200000")))
    ap.add_argument("--closes_count", type=int, default=int(os.environ.get("CLOSES_COUNT", "200000")))
    ap.add_argument("--tb_labels_stream", default=os.environ.get("TB_LABELS_STREAM", "labels:tb"))
    ap.add_argument("--tb_labels_field", default=os.environ.get("TB_LABELS_FIELD", "payload"))
    ap.add_argument("--tb_labels_count", type=int, default=int(os.environ.get("TB_LABELS_COUNT", "200000")))
    ap.add_argument("--label_source", default=os.environ.get("LABEL_SOURCE", "closed"), choices=["closed","tb_primary","tb_util"])
    ap.add_argument("--tb_util_min_r", type=float, default=float(os.environ.get("TB_UTIL_MIN_R", "0.0")))
    ap.add_argument("--since_ms", type=int, default=0)
    ap.add_argument("--until_ms", type=int, default=0)

    # Archive fallback (P58): load beyond Redis retention from NDJSON archives.
    ap.add_argument(
        "--signal_archive_dir",
        default=os.environ.get("SIGNALS_ARCHIVE_DIR", ""),
        help="Directory with YYYY-MM-DD.ndjson(.gz) archives for signals:of:inputs (optional).",
    )
    ap.add_argument(
        "--closed_archive_dir",
        default=os.environ.get("TRADES_CLOSED_ARCHIVE_DIR", ""),
        help="Directory with YYYY-MM-DD.ndjson(.gz) archives for trades:closed (optional).",
    )
    ap.add_argument("--archive_lookback_days", type=int, default=int(os.environ.get("ARCHIVE_LOOKBACK_DAYS", "7")))
    ap.add_argument("--file_fallback", type=int, default=int(os.environ.get("FILE_FALLBACK", "1")))
    ap.add_argument("--file_max_records", type=int, default=int(os.environ.get("FILE_MAX_RECORDS", "500000")))
    ap.add_argument("--file_min_signals", type=int, default=int(os.environ.get("FILE_MIN_SIGNALS", "5000")))
    ap.add_argument("--file_min_closes", type=int, default=int(os.environ.get("FILE_MIN_CLOSES", "500")))



    ap.add_argument("--y_min_r", type=float, default=float(os.environ.get("Y_MIN_R", "0.10")))
    ap.add_argument("--dedup_signals", default="latest", choices=["latest", "earliest", "keep_first"])

    ap.add_argument(
        "--join_strategy",
        default=os.environ.get("JOIN_STRATEGY", "sid_or_nearest"),
        choices=["sid", "nearest", "sid_or_nearest"],
    )
    ap.add_argument(
        "--join_tolerance_ms",
        type=int,
        default=int(os.environ.get("JOIN_TOLERANCE_MS", "10000")),
        help="Max abs(close_ts_ms - signal_ts_ms) to accept nearest join (ms). 0 disables tolerance.",
    )

    ap.add_argument(
        "--join_secondary",
        default=os.environ.get("JOIN_SECONDARY", "dir_scenario_soft"),
        choices=["none", "dir", "scenario", "dir_scenario", "dir_soft", "scenario_soft", "dir_scenario_soft"],
        help="Secondary filter for nearest-join. Uses CloseRow direction/scenario when available; *_soft ignores missing close fields.",
    )
    ap.add_argument(
        "--nearest_max_scan",
        type=int,
        default=int(os.environ.get("NEAREST_MAX_SCAN", "50")),
        help="How many candidate signals to scan on each side of close_ts_ms for nearest-join.",
    )
    ap.add_argument(
        "--join_bucket_keys",
        default=os.environ.get("JOIN_BUCKET_KEYS", ""),
        help="Comma-separated indicator keys used as deterministic tie-breaker for nearest-join (match close.meta vs signal.indicators).",
    )

    ap.add_argument("--drop_invalid_risk", type=int, default=int(os.environ.get("DROP_INVALID_RISK", "0")))
    ap.add_argument("--diagnose_mismatch", type=int, default=int(os.environ.get("DIAGNOSE_MISMATCH", "1")))
    ap.add_argument("--max_examples", type=int, default=int(os.environ.get("MAX_EXAMPLES", "50")))
    ap.add_argument("--out_quarantine_jsonl", default="")
    ap.add_argument("--out_report_json", default="")

    ap.add_argument("--out_jsonl", required=True)
    ap.add_argument("--emit_feature_cols_json", default="")
    # Feature Registry: детерминированный feature_cols из Registry вместо infer_feature_cols()
    # При установке: PYTHONPATH=./tick_flow_full:./ml_analysis (see docs)
    ap.add_argument(
        "--feature_schema_ver",
        default=os.environ.get("ML_FEATURE_SCHEMA_VER", ""),
        choices=_schema_choices(include_empty=True),
        help="Если задан, feature_cols берётся из Feature Registry (детерминированно), "
            "а не из infer_feature_cols() (sample-зависимый). Empty = прежнее поведение.",
    )
    ap.add_argument("--max_numeric", type=int, default=int(os.environ.get("MAX_NUMERIC", "128")))
    ap.add_argument("--include_direction", type=int, default=int(os.environ.get("INCLUDE_DIRECTION", "1")))
    ap.add_argument("--include_scenario", type=int, default=int(os.environ.get("INCLUDE_SCENARIO", "1")))
    ap.add_argument("--scenario_prefix", default=os.environ.get("SCENARIO_PREFIX", "bucket:"))
    ap.add_argument("--include_time_onehot", type=int, default=int(os.environ.get("INCLUDE_TIME_ONEHOT", "1")))

    # Offline-only derived features (F/G/H) for ablation; does NOT touch runtime pipeline.
    ap.add_argument("--derive_fgh", type=int, default=int(os.environ.get("DERIVE_FGH", "0")))
    ap.add_argument("--fgh_leader_symbol", default=os.environ.get("FGH_LEADER_SYMBOL", "BTCUSDT"))
    ap.add_argument(
        "--fgh_leader_max_lag_ms",
        type=int,
        default=int(os.environ.get("FGH_LEADER_MAX_LAG_MS", "2000")),
    )
    ap.add_argument(
        "--fgh_vel_z_alpha",
        type=float,
        default=float(os.environ.get("FGH_VEL_Z_ALPHA", "0.06")),
    )
    ap.add_argument(
        "--fgh_store_debug_flags",
        type=int,
        default=int(os.environ.get("FGH_STORE_DEBUG_FLAGS", "0")),
    )
    ap.add_argument(
        "--fgh_append_feature_cols",
        type=int,
        default=int(os.environ.get("FGH_APPEND_FEATURE_COLS", "1")),
        help="If --emit_feature_cols_json is used, append F/G/H columns to feature_cols (for offline ablation).",
    )
    # Strict feature schema: forbid scenario_v4_* one-hots to guarantee bounded cardinality
    ap.add_argument(
        "--strict_feature_cols",
        type=int,
        default=int(os.environ.get("ML_STRICT_FEATURE_COLS", os.environ.get("STRICT_FEATURE_COLS", "0")) or 0),
    )
    ap.add_argument("--forbid_scenario_v4_onehot", type=int, default=None)

    args = ap.parse_args(list(argv) if argv is not None else None)

    strict_feature_cols = int(getattr(args, "strict_feature_cols", 0) or 0) == 1
    # If not provided via CLI, read from ENV. If still unset, strict -> forbid by default.
    env_forbid = os.environ.get("ML_FORBID_SCENARIO_V4_ONEHOT", os.environ.get("FORBID_SCENARIO_V4_ONEHOT"))
    if getattr(args, "forbid_scenario_v4_onehot", None) is not None:
        forbid_scenario_v4_onehot = int(args.forbid_scenario_v4_onehot or 0) == 1
    elif env_forbid is not None:
        forbid_scenario_v4_onehot = int(env_forbid or "0") == 1
    else:
        # strict mode implies forbid_scenario_v4_onehot unless explicitly overridden
        forbid_scenario_v4_onehot = bool(strict_feature_cols)

    start_ms = int(args.since_ms) if int(args.since_ms) > 0 else None
    end_ms = int(args.until_ms) if int(args.until_ms) > 0 else None

    try:
        import redis  # type: ignore
    except Exception as e:
        raise SystemExit(f"redis-py is required: {e}")

    r = redis.Redis.from_url(args.redis_url, decode_responses=False)

    sig_items = _xrevrange_recent(r, args.signal_stream, count=int(args.signals_count))
    close_items = _xrevrange_recent(r, args.closed_stream, count=int(args.closes_count))

    sig_items = _filter_by_time(sig_items, ts_field_candidates=("ts_ms", "t", "ts"), start_ms=start_ms, end_ms=end_ms)
    close_items = _filter_by_time(
        close_items, ts_field_candidates=("exit_ts_ms", "ts_ms", "ts"), start_ms=start_ms, end_ms=end_ms
    )

    drop = DropStats(max_examples=int(args.max_examples))
    quarantine = QuarantineWriter(args.out_quarantine_jsonl) if args.out_quarantine_jsonl else None

    signals: List[SignalRow] = []
    for msg_id, f in sig_items:
        s = parse_replay_signal(f)
        if s is None:
            drop.add("signal_parse_none", {"id": msg_id})
            if quarantine is not None:
                quarantine.write("signal", "signal_parse_none", stream=str(args.signal_stream), msg_id=msg_id, data=f)
            continue
        signals.append(s)

    closes: List[CloseRow] = []
    for msg_id, f in close_items:
        c = parse_trade_closed(f)
        if c is None:
            drop.add("close_parse_none", {"id": msg_id})
            if quarantine is not None:
                quarantine.write("close", "close_parse_none", stream=str(args.closed_stream), msg_id=msg_id, data=f)
            continue
        # optional risk sanity drop
        if int(args.drop_invalid_risk) == 1 and float(c.risk_usd) <= 0.0:
            drop.add("close_invalid_risk", {"id": msg_id, "sid": c.sid, "symbol": c.symbol, "risk_usd": c.risk_usd})
            if quarantine is not None:
                quarantine.write("close", "close_invalid_risk", stream=str(args.closed_stream), msg_id=msg_id, data=f)
            continue
        closes.append(c)


    # Optional archive fallback: if you request an older window or Redis retention is short,
    # we can augment signals/closes from on-disk NDJSON archives.
    file_stats: Dict[str, Any] = {}
    if int(args.file_fallback) == 1:
        lb_days = int(args.archive_lookback_days)
        max_rec = int(args.file_max_records)

        # signals:of:inputs
        sig_need = bool(str(args.signal_archive_dir).strip()) and (
            start_ms is not None or end_ms is not None or int(len(signals)) < int(args.file_min_signals)
        )
        if sig_need:
            sig_file_items, st = _read_archive_items(
                str(args.signal_archive_dir),
                start_ms=start_ms,
                end_ms=end_ms,
                lookback_days=lb_days,
                max_records=max_rec,
            )
            file_stats["signals_archive"] = st
            file_stats["signals_file_raw"] = int(len(sig_file_items))
            existing_sids = {str(s.sid) for s in signals if str(s.sid)}
            added = 0
            parsed = 0
            for msg_id, f in sig_file_items:
                s = parse_replay_signal(f)
                if s is None:
                    continue
                parsed += 1
                sid = str(s.sid)
                if sid and sid not in existing_sids:
                    signals.append(s)
                    existing_sids.add(sid)
                    added += 1
            file_stats["signals_file_parsed"] = int(parsed)
            file_stats["signals_file_added"] = int(added)

        # trades:closed
        close_need = bool(str(args.closed_archive_dir).strip()) and (
            start_ms is not None or end_ms is not None or int(len(closes)) < int(args.file_min_closes)
        )
        if close_need:
            close_file_items, st = _read_archive_items(
                str(args.closed_archive_dir),
                start_ms=start_ms,
                end_ms=end_ms,
                lookback_days=lb_days,
                max_records=max_rec,
            )
            file_stats["closes_archive"] = st
            file_stats["closes_file_raw"] = int(len(close_file_items))
            existing_ck = {(str(c.sid), int(c.close_ts_ms)) for c in closes if str(c.sid) and int(c.close_ts_ms) > 0}
            added = 0
            parsed = 0
            for msg_id, f in close_file_items:
                c = parse_trade_closed(f)
                if c is None:
                    continue
                parsed += 1
                k = (str(c.sid), int(c.close_ts_ms))
                if k[0] and k[1] > 0 and k in existing_ck:
                    continue
                if int(args.drop_invalid_risk) == 1 and float(c.risk_usd) <= 0.0:
                    continue
                closes.append(c)
                if k[0] and k[1] > 0:
                    existing_ck.add(k)
                    added += 1
            file_stats["closes_file_parsed"] = int(parsed)
            file_stats["closes_file_added"] = int(added)

    dedup = str(args.dedup_signals)
    if dedup == "keep_first":
        dedup = ""

    join_debug: Dict[str, Any] = {}

    tb_by_sid: Optional[Dict[str, Dict[str, Any]]] = None
    if str(args.label_source) in ("tb_primary", "tb_util"):
        tb_by_sid = load_tb_labels_from_stream(
            r,
            stream=str(args.tb_labels_stream),
            field=str(args.tb_labels_field),
            count=int(args.tb_labels_count),
        )

    rows, unmatched = join_signals_with_closes_v2(
        signals,
        closes,
        y_min_r=float(args.y_min_r),
        dedup_signals=(dedup or "latest"),
        drop_invalid_risk=False,
        join_strategy=str(args.join_strategy),
        join_tolerance_ms=int(args.join_tolerance_ms),
        join_secondary=str(args.join_secondary),
        nearest_max_scan=int(args.nearest_max_scan),
        join_bucket_keys=_split_csv_keys(args.join_bucket_keys),
        join_debug=join_debug,
        drop=drop,
        quarantine=quarantine,
        tb_by_sid=tb_by_sid,
        label_source=str(args.label_source),
        tb_util_min_r=float(args.tb_util_min_r),
        closed_stream=str(args.closed_stream),
    )

    derived_fgh: Dict[str, Any] = {}
    if int(getattr(args, "derive_fgh", 0) or 0) == 1:
        if not _DERIVE_FGH_AVAILABLE or derive_fgh_rows is None:
            print("[WARN] derive_fgh requested but ml_analysis.common.derived_fgh is unavailable")
        else:
            derived_fgh = derive_fgh_rows(
                rows,
                leader_symbol=str(getattr(args, "fgh_leader_symbol", "BTCUSDT") or "BTCUSDT"),
                leader_max_lag_ms=int(getattr(args, "fgh_leader_max_lag_ms", 2000) or 2000),
                vel_z_alpha=float(getattr(args, "fgh_vel_z_alpha", 0.06) or 0.06),
                store_debug_flags=int(getattr(args, "fgh_store_debug_flags", 0) or 0) == 1,
            )
            if isinstance(derived_fgh, dict) and derived_fgh.get("ok"):
                print(f"[derive_fgh] ok stats={derived_fgh.get('stats', {})}")

    _write_jsonl(args.out_jsonl, rows)

    mismatch: Dict[str, Any] = {}
    if int(args.diagnose_mismatch) == 1 and unmatched:
        diag_smap = _build_signal_map(signals, dedup_signals=(dedup or "latest"))

        sig_index = _build_signal_index_by_symbol(list(diag_smap.values()))
        mismatch = diagnose_unmatched_closes(unmatched, signal_index_by_symbol=sig_index, max_examples=int(args.max_examples))

    stats: Dict[str, Any] = {
        "signal_stream": str(args.signal_stream),
        "closed_stream": str(args.closed_stream),
        "signals_raw": int(len(sig_items)),
        "signals_parsed": int(len(signals)),
        "closes_raw": int(len(close_items)),
        "closes_parsed": int(len(closes)),
        "joined": int(len(rows)),
        "unmatched_closes": int(len(unmatched)),
        "y_min_r": float(args.y_min_r),
        "since_ms": int(start_ms or 0),
        "until_ms": int(end_ms or 0),
        "generated_ms": _now_ms(),
        # strict schema flags for audit/traceability
        "strict_feature_cols": int(strict_feature_cols),
        "forbid_scenario_v4_onehot": int(bool(forbid_scenario_v4_onehot)),
        "drop": drop.to_dict(),
    }

    if 'derived_fgh' in locals() and isinstance(derived_fgh, dict) and derived_fgh:
        stats["derived_fgh"] = derived_fgh

    # archive fallback diagnostics
    if 'file_stats' in locals() and isinstance(file_stats, dict) and file_stats:
        stats["file_fallback"] = {
            "enabled": True,
            "signal_archive_dir": str(args.signal_archive_dir),
            "closed_archive_dir": str(args.closed_archive_dir),
            "archive_lookback_days": int(args.archive_lookback_days),
            "file_max_records": int(args.file_max_records),
            "signals_file_raw": int(file_stats.get("signals_file_raw", 0)),
            "signals_file_parsed": int(file_stats.get("signals_file_parsed", 0)),
            "signals_file_added": int(file_stats.get("signals_file_added", 0)),
            "closes_file_raw": int(file_stats.get("closes_file_raw", 0)),
            "closes_file_parsed": int(file_stats.get("closes_file_parsed", 0)),
            "closes_file_added": int(file_stats.get("closes_file_added", 0)),
            "signals_archive": file_stats.get("signals_archive", {}),
            "closes_archive": file_stats.get("closes_archive", {}),
        }

    stats["join_strategy"] = str(args.join_strategy)
    stats["join_tolerance_ms"] = int(args.join_tolerance_ms)

    stats["join_secondary"] = str(args.join_secondary)
    stats["nearest_max_scan"] = int(args.nearest_max_scan)
    stats["join_bucket_keys"] = _split_csv_keys(args.join_bucket_keys)
    if join_debug.get("nearest_join"):
        stats["nearest_join"] = join_debug.get("nearest_join")

    if rows:
        jm = [str(r.get("join_method", "")) for r in rows]
        stats["joined_by_sid"] = int(sum(1 for m in jm if m == "sid"))
        stats["joined_by_nearest"] = int(sum(1 for m in jm if m == "nearest"))
    if mismatch:
        stats["mismatch"] = mismatch

    if rows:
        ys = [int(r.get("y", 0)) for r in rows]
        stats["pos_rate"] = float(sum(ys) / max(1, len(ys)))
        rmults = [float(r.get("r_mult", 0.0)) for r in rows]
        rs = sorted(rmults)
        stats["r_mult_p50"] = float(rs[len(rs) // 2])
        stats["r_mult_p95"] = float(rs[int(0.95 * (len(rs) - 1))])

    if args.emit_feature_cols_json:
        feature_ver = _norm_schema_ver(str(getattr(args, "feature_schema_ver", "") or "").strip())
        if feature_ver and _REGISTRY_AVAILABLE:
            # Детерминированный путь: feature_cols из Feature Registry
            spec = _get_edge_stack_spec(feature_ver)
            cols = list(spec.feature_cols)
            stats["feature_registry"] = {
                "ver": spec.ver,
                "source": "registry",
                "feature_cols_hash": spec.feature_cols_hash,
                "n_cols": len(cols),
            }
            print(f"[feature_registry] feature_cols ({len(cols)} кол.) из Registry ver={spec.ver} "
                  f"hash={spec.feature_cols_hash[:16]}…")
        elif feature_ver and not _REGISTRY_AVAILABLE:
            # Запрошен Registry, но недоступен — fallback в infer_feature_cols() + варнинг
            print(f"[WARN] Feature Registry недоступен (проверьте PYTHONPATH). "
                  f"Fallback на infer_feature_cols().")
            cols = infer_feature_cols(
                rows,
                max_numeric=int(args.max_numeric),
                include_direction=int(args.include_direction) == 1,
                include_scenario=int(args.include_scenario) == 1,
                scenario_prefix=str(args.scenario_prefix),
                include_time_onehot=int(args.include_time_onehot) == 1,
                strict_feature_cols=bool(strict_feature_cols),
                forbid_scenario_v4_onehot=bool(forbid_scenario_v4_onehot),
            )
            stats["feature_registry"] = {"ver": feature_ver, "source": "infer_fallback"}
        else:
            # Старый путь: infer_feature_cols()
            cols = infer_feature_cols(
                rows,
                max_numeric=int(args.max_numeric),
                include_direction=int(args.include_direction) == 1,
                include_scenario=int(args.include_scenario) == 1,
                scenario_prefix=str(args.scenario_prefix),
                include_time_onehot=int(args.include_time_onehot) == 1,
                strict_feature_cols=bool(strict_feature_cols),
                forbid_scenario_v4_onehot=bool(forbid_scenario_v4_onehot),
            )

        # If derived F/G/H is enabled, append those numeric keys to feature_cols
        # to allow offline ablation (does not imply runtime support).
        if int(getattr(args, "derive_fgh", 0) or 0) == 1 and int(getattr(args, "fgh_append_feature_cols", 1) or 0) == 1:
            for k in FGH_NUMERIC_KEYS:
                c = "f_" + str(k)
                if c not in cols:
                    cols.append(c)
            stats["derived_fgh_feature_cols_added"] = ["f_" + k for k in FGH_NUMERIC_KEYS]

        os.makedirs(os.path.dirname(os.path.abspath(args.emit_feature_cols_json)) or ".", exist_ok=True)
        with open(args.emit_feature_cols_json, "w", encoding="utf-8") as f:
            json.dump(cols, f, ensure_ascii=False, indent=2)
        stats["feature_cols_n"] = int(len(cols))

    if args.out_report_json:
        os.makedirs(os.path.dirname(os.path.abspath(args.out_report_json)) or ".", exist_ok=True)
        with open(args.out_report_json, "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)

    if quarantine is not None:
        quarantine.close()

    # print(json.dumps(stats, ensure_ascii=False))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


