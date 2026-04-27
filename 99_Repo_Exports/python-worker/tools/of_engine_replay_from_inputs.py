"""Engine-based replay from OFInputs → NDJSON with full evidence/legs_detail/score_breakdown.

Why:
  Golden replay regression harness needs deterministic output from real OFConfirmEngine.build,
  including A3/B2/C2/D-explainability (not just have/need heuristics).

Usage:
  python -m tools.of_engine_replay_from_inputs --inputs /tmp/of_inputs.ndjson --out /tmp/replay.ndjson --tf 1s
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from core.of_confirm_engine import OFConfirmEngine


def iter_ndjson(path: str):
    """Iterator over NDJSON lines."""
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)


def load_inputs(path: str):
    """
    Supports:
      A) Direct OFInputsV1 per line: {...}
      B) Wrapped stream-export per line: {"payload": "{...json...}"} or {"payload": {...}}
    """
    for row in iter_ndjson(path):
        if "payload" in row:
            p = row["payload"]
            if isinstance(p, str):
                yield json.loads(p)
            elif isinstance(p, dict):
                yield p
            else:
                continue
        else:
            yield row


@dataclass
class _WpStub:
    weak_any: bool = False


@dataclass
class _PressureStub:
    def is_pressure_hi(self, *_args: Any, **_kwargs: Any) -> bool:
        return False


class RuntimeStub:
    """
    Minimal runtime object to satisfy OFConfirmEngine.build on replay.
    Anything missing is fail-open via __getattr__.
    """
    def __init__(self) -> None:
        self.dynamic_cfg: Dict[str, Any] = {}
        self.pressure = _PressureStub()
        self.book_churn_hi = 0
        self.last_regime = "na"
        self.last_div = None
        self.last_wp = _WpStub(False)

        # evidence placeholders used by compute_*_recent functions
        self.last_sweep = None
        self.last_reclaim = None
        self.last_obi_event = None
        self.last_iceberg_event = None
        self.last_ofi_event = None
        self.last_fp_edge = None

    def __getattr__(self, _name: str) -> Any:
        return None


def _bool(x: Any) -> bool:
    try:
        if isinstance(x, bool):
            return x
        return bool(int(x))
    except Exception:
        return bool(x)


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(d)


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return int(d)


def build_runtime_from_inputs(inp: Dict[str, Any]) -> RuntimeStub:
    """
    Reconstruct minimal runtime from OFInputsV1 flags.
    Fields we try to use if present:
      sweep_recent, reclaim_recent, obi_stable, iceberg_strict, ofi_stable, ofi_dir_ok, fp_edge_absorb, weak_progress
    """
    ts_ms = _i(inp.get("ts_ms", 0))
    direction = str(inp.get("direction", ""))
    rt = RuntimeStub()

    # optional market context
    if "regime" in inp:
        rt.last_regime = str(inp.get("regime") or "na")
    if "book_churn_hi" in inp:
        rt.book_churn_hi = _i(inp.get("book_churn_hi", 0))

    # sweep/reclaim
    if _bool(inp.get("sweep_recent", 0)):
        rt.last_sweep = {"ts_ms": ts_ms, "dir": direction, "kind": str(inp.get("sweep_kind", "na"))}
    if _bool(inp.get("reclaim_recent", 0)):
        rt.last_reclaim = {"ts_ms": ts_ms, "dir": direction, "kind": str(inp.get("reclaim_kind", "na"))}

    # OBI
    if _bool(inp.get("obi_stable", 0)):
        rt.last_obi_event = {"ts_ms": ts_ms, "dir": direction, "obi": _f(inp.get("obi", 1.0), 1.0), "stable_secs": _f(inp.get("obi_stable_secs", 2.0), 2.0)}

    # iceberg
    if _bool(inp.get("iceberg_strict", 0)):
        rt.last_iceberg_event = {"ts_ms": ts_ms, "dir": direction, "strict": 1, "score": _f(inp.get("iceberg_score", 1.0), 1.0)}

    # OFI (optional advanced) - support both v1 and v2
    version = _i(inp.get("v", 1))
    if version == 2 or _bool(inp.get("ofi_stable", 0)) or _bool(inp.get("ofi_dir_ok", 0)) or "ofi" in inp:
        # For v2, always try to reconstruct OFI event if age_ms is valid
        ofi_age_ms = _i(inp.get("ofi_age_ms", -1))
        if version == 2 and ofi_age_ms >= 0:
            # Reconstruct ts_ms from age
            ofi_ts_ms = ts_ms - ofi_age_ms
        else:
            ofi_ts_ms = ts_ms
        
        rt.last_ofi_event = {
            "ts_ms": ofi_ts_ms,
            "direction": direction,
            "dir": direction,
            "ofi": _f(inp.get("ofi", 0.0), 0.0),
            "ofi_z": _f(inp.get("ofi_z", 0.0), 0.0),
            "stable_secs": _f(inp.get("ofi_stable_secs", 0.0), 0.0),
            "stability_score": _f(inp.get("ofi_stability_score", 0.0), 0.0),
            "dir_ok": int(_bool(inp.get("ofi_dir_ok", 0))),
            "stable": int(_bool(inp.get("ofi_stable", 0))),
        }

    # FP edge absorb (optional advanced) - support both v1 and v2
    if version == 2 or _bool(inp.get("fp_edge_absorb", 0)) or "fp_edge_absorb" in inp:
        # For v2, always try to reconstruct FP edge event if age_ms is valid
        fp_age_ms = _i(inp.get("fp_edge_age_ms", -1))
        if version == 2 and fp_age_ms >= 0:
            # Reconstruct ts_ms from age
            fp_ts_ms = ts_ms - fp_age_ms
        else:
            fp_ts_ms = ts_ms
        
        rt.last_fp_edge = {
            "ts_ms": fp_ts_ms,
            "direction": direction,
            "dir": direction,
            "strength": _f(inp.get("fp_edge_absorb_strength", inp.get("fp_edge_strength", 1.5)), 1.5),
            "value": _f(inp.get("fp_edge_absorb_strength", inp.get("fp_edge_strength", 1.5)), 1.5),
            "p90": 1.0,  # Default normalization
            "bias": direction,
            "range_expansion": int(_bool(inp.get("fp_edge_range_expansion", 0))),
        }

    # weak progress
    rt.last_wp = _WpStub(weak_any=_bool(inp.get("weak_progress", 0)))

    # symbol holder (strategy uses runtime.symbol)
    rt.symbol = str(inp.get("symbol", "") or "")

    return rt


def build_cfg_indicators(inp: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any], Optional[Dict[str, Any]]]:
    """
    OFConfirmEngine.build expects:
      cfg: dict
      indicators: dict
      absorption: optional dict
    We accept either top-level fields or nested indicators.
    """
    cfg = dict(inp.get("cfg") or {})
    indicators = dict(inp.get("indicators") or {})

    # common numeric fields may be on top-level
    if "spread_bps" in inp:
        indicators["spread_bps"] = _f(inp.get("spread_bps"), 0.0)
    if "expected_slippage_bps" in inp:
        indicators["expected_slippage_bps"] = _f(inp.get("expected_slippage_bps"), 0.0)

    # optional: fp edge absorb may be passed via indicators (engine reads indicators)
    if "fp_edge_absorb" in inp:
        indicators["fp_edge_absorb"] = int(_bool(inp.get("fp_edge_absorb", 0)))
    
    # V2 OFI fields (if present, propagate to indicators)
    version = _i(inp.get("v", 1))
    if version == 2:
        for k in ("ofi", "ofi_z", "ofi_stable", "ofi_dir_ok", "ofi_stable_secs", "ofi_stability_score", "ofi_age_ms"):
            if k in inp and k not in indicators:
                indicators[k] = inp[k]
        
        # FP edge fields
        if "fp_edge_absorb" in inp and "fp_edge_absorb" not in indicators:
            indicators["fp_edge_absorb"] = int(_bool(inp.get("fp_edge_absorb", 0)))
        if "fp_edge_absorb_strength" in inp and "fp_edge_absorb_strength" not in indicators:
            indicators["fp_edge_absorb_strength"] = _f(inp.get("fp_edge_absorb_strength"), 0.0)
        if "fp_edge_age_ms" in inp and "fp_edge_age_ms" not in indicators:
            indicators["fp_edge_age_ms"] = _i(inp.get("fp_edge_age_ms"), -1)

    # cancel spike gate inputs (if present)
    for k in ("cancel_bid_rate_ema", "cancel_ask_rate_ema", "taker_buy_rate_ema", "taker_sell_rate_ema", "bucket_id"):
        if k in inp and k not in indicators:
            indicators[k] = inp[k]

    # data health context (optional)
    for k in ("data_health", "book_health_ok", "source_consistency_ok"):
        if k in inp and k not in indicators:
            indicators[k] = inp[k]

    # propagate sid for deterministic canary-share ENFORCE
    if "sid" in inp:
        indicators["sid"] = str(inp.get("sid") or "")
    elif "signal_id" in inp:
        indicators["sid"] = str(inp.get("signal_id") or "")

    absorption = None
    if "absorption" in inp and isinstance(inp["absorption"], dict):
        absorption = inp["absorption"]

    return cfg, indicators, absorption


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", required=True, help="NDJSON from signals:of:inputs (payload field) OR direct OFInputsV1 per line")
    ap.add_argument("--out", required=True, help="output NDJSON from OFConfirmEngine.build()")
    ap.add_argument("--tf", default="1s")
    ap.add_argument("--sort", type=int, default=1, help="sort output deterministically by (ts_ms,symbol,direction)")
    args = ap.parse_args()

    engine = OFConfirmEngine()
    out_rows: List[Dict[str, Any]] = []

    for inp in load_inputs(args.inputs):
        symbol = str(inp.get("symbol", "") or "")
        direction = str(inp.get("direction", "") or "")
        ts_ms = _i(inp.get("ts_ms", 0))

        # Force deterministic time for replay
        if hasattr(engine, "set_replay_time_ms"):
            engine.set_replay_time_ms(ts_ms)
        delta_z = _f(inp.get("delta_z", 0.0))
        price = _f(inp.get("price", inp.get("entry_price", 0.0)), 0.0)

        # sid is essential for outcome join
        sid = str(inp.get("sid", inp.get("signal_id", "")) or "")

        runtime = build_runtime_from_inputs(inp)
        cfg, indicators, absorption = build_cfg_indicators(inp)

        ofc, dec = engine.build(
            symbol=symbol,
            tf=str(args.tf),
            direction=direction,
            tick_ts_ms=ts_ms,
            price=price,
            delta_z=delta_z,
            runtime=runtime,
            cfg=cfg,
            indicators=indicators,
            absorption=absorption,
        )
        if ofc is None:
            continue

        # OFConfirmV3 -> dict
        row = ofc.to_dict() if hasattr(ofc, "to_dict") else dict(ofc)
        row["sid"] = sid
        out_rows.append(row)

    if args.sort == 1:
        out_rows.sort(key=lambda x: (int(x.get("ts_ms", 0) or 0), str(x.get("symbol", "")), str(x.get("direction", ""))))

    with open(args.out, "w", encoding="utf-8") as f:
        for r in out_rows:
            f.write(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()

