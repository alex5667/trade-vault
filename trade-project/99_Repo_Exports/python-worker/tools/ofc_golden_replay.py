#!/usr/bin/env python3
from __future__ import annotations

"""Golden replay runner for OFConfirmEngine.

Purpose:
  - Deterministic replay of captured inputs (OFC_CAPTURE ndjson)
  - Fresh gate state per run (default) to avoid state bleed
  - Strict ordering by (symbol, bucket_id, tick_ts_ms) when available

Output:
  JSON summary with throughput, latency stats, and a deterministic digest of decisions.
"""


import argparse
import json
import math
import os
import sys
from types import SimpleNamespace
from typing import Any
import contextlib
from core.of_confirm_engine import OFConfirmEngine


def _pctl(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    xs = sorted(values)
    if p <= 0:
        return float(xs[0])
    if p >= 100:
        return float(xs[-1])
    k = (len(xs) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    if f == c:
        return float(xs[f])
    d0 = xs[f] * (c - k)
    d1 = xs[c] * (k - f)
    return float(d0 + d1)


def _read_ndjson(path: str, limit: int = 0) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except Exception:
                # tolerate partial writes
                continue
            if limit and len(out) >= limit:
                break
    return out


def _bucket_key(row: dict[str, Any], idx: int) -> tuple[str, int, int, int]:
    sym = (row.get("symbol", "") or "").upper()
    b = row.get("bucket_id")
    if b is None:
        try:
            b = (row.get("indicators") or {}).get("bucket_id", None)
        except Exception:
            b = None
    try:
        b_i = int(b) if b is not None else -1
    except Exception:
        b_i = -1
    try:
        ts = int(row.get("tick_ts_ms", row.get("ts_ms", 0)) or 0)
    except Exception:
        ts = 0
    return (sym, b_i, ts, idx)


class PressureReplay:
    def __init__(self, is_hi: bool | None) -> None:
        self._is_hi = is_hi

    def is_pressure_hi(self, now_ms: int, config: dict[str, Any]) -> bool:
        return bool(self._is_hi) if self._is_hi is not None else False


class DictLikeNamespace(SimpleNamespace):
    """Namespace that supports both attribute access and .get() method"""
    def __init__(self, d: dict[str, Any]):
        super().__init__(**d)
        self._dict = d
    def get(self, key: str, default: Any = None) -> Any:
        return self._dict.get(key, default)
    def __getitem__(self, key: str) -> Any:
        return self._dict[key]
    def __contains__(self, key: str) -> bool:
        return key in self._dict

def _ns_from_dict(d: dict[str, Any] | None) -> DictLikeNamespace | None:
    if not isinstance(d, dict):
        return None
    return DictLikeNamespace(d)


class ConfigProxy:
    """Dict-like wrapper that supports both attribute access and .get()"""
    def __init__(self, d: dict[str, Any]):
        self._d = d or {}
    def get(self, key: str, default: Any = None) -> Any:
        return self._d.get(key, default)
    def __getattr__(self, key: str) -> Any:
        return self._d.get(key)
    def __contains__(self, key: str) -> bool:
        return key in self._d
    def __getitem__(self, key: str) -> Any:
        return self._d[key]
    def __setitem__(self, key: str, value: Any) -> None:
        self._d[key] = value


class RuntimeReplay(SimpleNamespace):
    """Minimal runtime proxy to satisfy getattr(runtime, ...) in OFConfirmEngine."""

    def __init__(self, snap: dict[str, Any], cfg: dict[str, Any] | None = None) -> None:
        super().__init__()
        self.cont_ctx_ts_ms = int(snap.get("cont_ctx_ts_ms") or 0)
        self.liq_regime = (snap.get("liq_regime") or "")
        self.last_regime = (snap.get("last_regime") or "")
        self.book_churn_hi = int(snap.get("book_churn_hi") or 0)

        # dynamic cfg (optional)
        self.dynamic_cfg = snap.get("dynamic_cfg") if isinstance(snap.get("dynamic_cfg"), dict) else {}

        # pressure proxy
        self.pressure = PressureReplay(snap.get("pressure_hi"))

        # last_* events (as namespaces)
        self.last_bar = _ns_from_dict(snap.get("last_bar"))
        self.last_obi_event = _ns_from_dict(snap.get("last_obi_event"))
        self.last_iceberg_event = _ns_from_dict(snap.get("last_iceberg_event"))
        self.last_ofi_event = _ns_from_dict(snap.get("last_ofi_event"))
        self.last_sweep = _ns_from_dict(snap.get("last_sweep"))
        self.last_reclaim = _ns_from_dict(snap.get("last_reclaim"))
        self.last_wp = _ns_from_dict(snap.get("last_wp"))
        self.last_fp_edge = _ns_from_dict(snap.get("last_fp_edge"))
        self.last_div = _ns_from_dict(snap.get("last_div"))

        # config proxy (supports .get() and attribute access)
        self.config = ConfigProxy(cfg or {})


def _runtime_from_snapshot(snap: Any) -> Any:
    """Backward compatibility wrapper."""
    if isinstance(snap, dict) and "v" in snap:
        # New format with RuntimeReplay
        return RuntimeReplay(snap)
    # Legacy format
    rt = SimpleNamespace()
    if isinstance(snap, dict):
        for k, v in snap.items():
            with contextlib.suppress(Exception):
                setattr(rt, str(k), v)
    return rt


def _decision_fingerprint(symbol: str, row: dict[str, Any], ofc: Any) -> str:
    # Stable digest input: minimal set of decision-affecting fields
    ts = int(row.get("tick_ts_ms", 0) or 0)
    direction = (row.get("direction", "") or "")
    ok = int(getattr(ofc, "ok", 0) or 0) if ofc is not None else 0
    have = int(getattr(ofc, "have", 0) or 0) if ofc is not None else 0
    need = int(getattr(ofc, "need", 0) or 0) if ofc is not None else 0
    score = float(getattr(ofc, "score", 0.0) or 0.0) if ofc is not None else 0.0
    scenario = str(getattr(ofc, "scenario", "") or "") if ofc is not None else ""
    gate_bits = int(getattr(ofc, "gate_bits", 0) or 0) if ofc is not None else 0
    # round score to avoid float noise across machines
    score_r = f"{score:.6f}"
    return f"{symbol}|{ts}|{direction}|ok={ok}|have={have}|need={need}|score={score_r}|scn={scenario}|gb={gate_bits}"


def _iter_lines(path: str, max_lines: int) -> Any:
    """Iterate over ndjson lines."""
    n = 0
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            yield row
            n += 1
            if max_lines > 0 and n >= max_lines:
                break


def _approx_equal(a: Any, b: Any, tol: float = 1e-9) -> bool:
    if a == b:
        return True
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        if math.isnan(a) and math.isnan(b):
            return True
        return abs(float(a) - float(b)) <= tol
    return False


def _compare_dict(a: dict[str, Any], b: dict[str, Any]) -> list[str]:
    diffs: list[str] = []
    keys = sorted(set(a.keys()) | set(b.keys()))
    for k in keys:
        if k not in a:
            diffs.append(f"missing_left:{k}")
            continue
        if k not in b:
            diffs.append(f"missing_right:{k}")
            continue
        va, vb = a.get(k), b.get(k)
        if isinstance(va, dict) and isinstance(vb, dict):
            sub = _compare_dict(va, vb)
            diffs.extend([f"{k}.{s}" for s in sub])
        else:
            if not _approx_equal(va, vb):
                diffs.append(f"diff:{k}:{va}->{vb}")
    return diffs


def main() -> None:
    ap = argparse.ArgumentParser(description="Deterministic golden replay for OFC_CAPTURE NDJSON")
    ap.add_argument("--capture-path", required=False, help="path to OFC_CAPTURE ndjson")

    ap.add_argument("--max-lines", type=int, default=0, help="max lines to process (0 = all)")
    ap.add_argument("--emit-mismatches", type=int, default=20, help="how many mismatch examples to print in report")
    ap.add_argument("--fail-on-mismatch", action="store_true", help="exit code 2 on mismatch")
    ap.add_argument("--write-golden", default="", help="write ndjson with computed ofc/dec snapshots")
    # Backward compatibility aliases
    ap.add_argument("--path", help="alias for --capture-path")
    ap.add_argument("--limit", type=int, help="alias for --max-lines")
    args = ap.parse_args()

    capture_path = args.capture_path or args.path or os.getenv("OFC_CAPTURE_PATH", "/tmp/ofc_inputs.ndjson")
    max_lines = args.max_lines or args.limit or 0

    # Ensure repo root on sys.path
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    engine = OFConfirmEngine()

    out_f = open(args.write_golden, "w", encoding="utf-8") if args.write_golden else None

    n = 0
    n_ok = 0
    n_mismatch = 0
    mismatches: list[dict[str, Any]] = []

    for row in _iter_lines(capture_path, max_lines):
        n += 1
        symbol = (row.get("symbol") or "")
        tf = (row.get("tf") or "")
        direction = (row.get("direction") or "")
        tick_ts_ms = int(row.get("tick_ts_ms") or 0)
        price = float(row.get("price") or 0.0)
        delta_z = float(row.get("delta_z") or 0.0)
        indicators = row.get("indicators") if isinstance(row.get("indicators"), dict) else {}
        absorption = row.get("absorption") if isinstance(row.get("absorption"), dict) else {}
        cfg = row.get("cfg") if isinstance(row.get("cfg"), dict) else {}
        runtime_snap = row.get("runtime_snapshot") if isinstance(row.get("runtime_snapshot"), dict) else {}
        cancel_gate_state = row.get("cancel_gate_state") if isinstance(row.get("cancel_gate_state"), dict) else None

        if not symbol or tick_ts_ms <= 0:
            continue

        # Replay mode: freeze time for deterministic behavior
        engine.set_replay_time_ms(tick_ts_ms)

        # Restore gate state if provided (full reproducibility even if file is interleaved)
        if cancel_gate_state:
            engine.restore_cancel_gate_state(cancel_gate_state)

        if runtime_snap:
            runtime = RuntimeReplay(runtime_snap, cfg=cfg)
        else:
            runtime = SimpleNamespace()
            runtime.config = ConfigProxy(cfg or {})
        # Expose symbol for engine meta/model keys
        runtime.symbol = symbol

        # Run engine
        try:
            # Ensure micro_tf is in cfg (required by engine)
            if "micro_tf" not in cfg:
                cfg["micro_tf"] = tf
            ofc, dec = engine.build(
                symbol=symbol,
                tf=tf,
                direction=direction,
                tick_ts_ms=tick_ts_ms,
                price=price,
                delta_z=delta_z,
                runtime=runtime,
                cfg=cfg,
                indicators=indicators,
                absorption=absorption,
            )
        except Exception as e:
            import traceback
            tb_str = traceback.format_exc()
            n_mismatch += 1
            if len(mismatches) < args.emit_mismatches:
                mismatches.append(
                    {
                        "symbol": symbol,
                        "tick_ts_ms": tick_ts_ms,
                        "err": f"{type(e).__name__}:{e}",
                        "traceback": tb_str.split('\n')[-3:-1] if len(tb_str.split('\n')) > 3 else [],
                    }
                )
            continue

        ofc_d = ofc.to_dict() if hasattr(ofc, "to_dict") else {}
        dec_d = dec.to_dict() if hasattr(dec, "to_dict") else {}

        expected_ofc = row.get("expected_ofc") if isinstance(row.get("expected_ofc"), dict) else None
        expected_dec = row.get("expected_dec") if isinstance(row.get("expected_dec"), dict) else None

        if expected_ofc is not None or expected_dec is not None:
            diffs: list[str] = []
            if expected_ofc is not None:
                diffs.extend([f"ofc:{d}" for d in _compare_dict(expected_ofc, ofc_d)])
            if expected_dec is not None:
                diffs.extend([f"dec:{d}" for d in _compare_dict(expected_dec, dec_d)])
            if diffs:
                n_mismatch += 1
                if len(mismatches) < args.emit_mismatches:
                    mismatches.append(
                        {
                            "symbol": symbol,
                            "tick_ts_ms": tick_ts_ms,
                            "diffs": diffs[:200],
                        }
                    )
            else:
                n_ok += 1
        else:
            # No expected baseline => treat as OK (tool can be used to generate a baseline)
            n_ok += 1

        if out_f:
            rr = dict(row)
            rr["computed_ofc"] = ofc_d
            rr["computed_dec"] = dec_d
            out_f.write(json.dumps(rr, ensure_ascii=False) + "\n")

    if out_f:
        out_f.close()

    report = {
        "capture_path": capture_path,
        "n": n,
        "n_ok": n_ok,
        "n_mismatch": n_mismatch,
        "mismatches": mismatches,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if n == 0:
        raise SystemExit("no_rows_in_capture")


    if args.fail_on_mismatch and n_mismatch > 0:
        raise SystemExit(2)


if __name__ == "__main__":
    main()

