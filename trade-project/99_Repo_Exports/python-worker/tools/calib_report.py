from __future__ import annotations

"""
Calib report tool:
Answers the 5 calibration questions and produces a single artifact (json + md):
  1) bars_to_ready (static -> calib)
  2) eff_quote_th magnitude (last)
  3) stability (drift/dispersion)
  4) audit volume (count/rate), dedup hints
  5) mismatch (optional: if replay/prod normalized files provided)

Inputs:
  - calib_effq_norm.ndjson  (required)
  - of_inputs.ndjson        (optional)
  - calib_effq_replay_norm.ndjson (optional; mismatch)

Output:
  - calib_report.json
  - calib_report.md
"""

import argparse
import json
import math
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _load_ndjson(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def _pct(xs: list[float], p: float) -> float:
    if not xs:
        return 0.0
    ys = sorted(xs)
    if p <= 0:
        return float(ys[0])
    if p >= 100:
        return float(ys[-1])
    k = (len(ys) - 1) * (p / 100.0)
    lo = int(math.floor(k))
    hi = int(math.ceil(k))
    if lo == hi:
        return float(ys[lo])
    a = ys[lo]
    b = ys[hi]
    w = k - lo
    return float(a + (b - a) * w)


def _mean(xs: list[float]) -> float:
    return float(sum(xs) / len(xs)) if xs else 0.0


def _std(xs: list[float]) -> float:
    if len(xs) < 2:
        return 0.0
    m = _mean(xs)
    v = sum((x - m) ** 2 for x in xs) / (len(xs) - 1)
    return float(math.sqrt(v))


def _safe_float(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else d
    except Exception:
        return d


@dataclass
class Key:
    symbol: str
    regime: str

    def as_str(self) -> str:
        return f"{self.symbol}:{self.regime}"


def _group_by(rows: Iterable[dict[str, Any]]) -> dict[Key, list[dict[str, Any]]]:
    out: dict[Key, list[dict[str, Any]]] = {}
    for r in rows:
        sym = (r.get("symbol", "") or "")
        reg = (r.get("regime", "na") or "na")
        k = Key(sym, reg)
        out.setdefault(k, []).append(r)
    for k in out:
        out[k].sort(key=lambda x: int(x.get("ts_ms", 0) or 0))
    return out


def _bars_to_ready(audit_rows: list[dict[str, Any]], tf_ms: int) -> int:
    """
    Approx bars until src becomes calib_* from static.
    Uses timestamps; bars ~= dt/tf_ms.
    """
    if not audit_rows:
        return -1
    t0 = int(audit_rows[0].get("ts_ms", 0) or 0)
    for r in audit_rows:
        src = (r.get("src", "") or "")
        if src and src != "static" and src.startswith("calib"):
            t1 = int(r.get("ts_ms", 0) or 0)
            if t0 > 0 and t1 >= t0 and tf_ms > 0:
                return int((t1 - t0) // tf_ms)
            return 0
    return -1


def _audit_rate(audit_rows: list[dict[str, Any]]) -> tuple[int, float]:
    if not audit_rows:
        return (0, 0.0)
    t0 = int(audit_rows[0].get("ts_ms", 0) or 0)
    t1 = int(audit_rows[-1].get("ts_ms", 0) or 0)
    n = len(audit_rows)
    dt_s = max(1.0, (t1 - t0) / 1000.0) if t1 >= t0 and t0 > 0 else float(n)
    return (n, float(n / dt_s))


def _stability_stats(audit_rows: list[dict[str, Any]], last_n: int = 200) -> dict[str, Any]:
    """
    volatility: std and (p95-p05)/median on last N points.
    """
    xs = [_safe_float(r.get("eff_quote_th", 0.0), 0.0) for r in audit_rows]
    xs = [x for x in xs if x > 0]
    if not xs:
        return {"n": 0, "std": 0.0, "p95_p05_over_med": 0.0, "p95": 0.0, "p05": 0.0, "median": 0.0}
    tail = xs[-last_n:] if len(xs) > last_n else xs
    med = _pct(tail, 50)
    p95 = _pct(tail, 95)
    p05 = _pct(tail, 5)
    denom = max(1e-12, abs(med))
    return {
        "n": len(tail),
        "std": _std(tail),
        "p95_p05_over_med": float((p95 - p05) / denom),
        "p95": p95,
        "p05": p05,
        "median": med,
        "last": float(tail[-1]),
    }


def _mismatch_count(prod: list[dict[str, Any]], replay: list[dict[str, Any]]) -> int:
    """
    Compare normalized rows 1:1 by index (ts_ms, symbol, regime ordering assumed).
    """
    if not prod or not replay:
        return 0
    n = min(len(prod), len(replay))
    mis = 0
    for i in range(n):
        a = prod[i]
        b = replay[i]
        # compare stable fields
        for k in ("symbol", "regime", "ts_ms", "src", "n", "eff_quote_th", "min_quote_delta"):
            if a.get(k) != b.get(k):
                mis += 1
                break
    # count extra as mismatches
    mis += abs(len(prod) - len(replay))
    return int(mis)


def _render_md(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Calibration Passport Report\n")
    lines.append(f"- Generated rows: {report.get('rows_total', 0)}\n")
    lines.append(f"- Keys (symbol:regime): {report.get('keys_total', 0)}\n")
    if report.get("mismatch_total", 0):
        lines.append(f"- **Mismatch total (prod vs replay): {report.get('mismatch_total')}**\n")
    lines.append("\n## Per symbol/regime\n")
    lines.append("| key | bars_to_ready | last eff_quote_th | vol std | (p95-p05)/med | audit_count | audit_rate/s | src_last | n_last | mismatch |\n")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---|---:|---:|\n")
    for k, row in report["by_key"].items():
        st = row["stability"]
        lines.append(
            f"| {k} | {row['bars_to_ready']} | {st.get('last', 0.0):.6g} | {st.get('std', 0.0):.3g} | {st.get('p95_p05_over_med', 0.0):.3g} | "
            f"{row['audit_count']} | {row['audit_rate_s']:.4g} | {row['src_last']} | {row['n_last']} | {row.get('mismatch', 0)} |\n"
        )
    lines.append("\n## Notes\n")
    lines.append("- `bars_to_ready=-1` means source never switched to calibrated within captured window.\n")
    lines.append("- High `(p95-p05)/med` indicates threshold instability; consider raising strictness or disabling abs_lvl as gate evidence.\n")
    return "".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--calib", required=True, help="calib_effq_norm.ndjson")
    ap.add_argument("--of-inputs", default="", help="of_inputs.ndjson (optional)")
    ap.add_argument("--replay", default="", help="calib_effq_replay_norm.ndjson (optional)")
    ap.add_argument("--tf-ms", type=int, default=1000, help="microbar tf_ms for bars_to_ready estimation")
    ap.add_argument("--out-json", default="calib_report.json")
    ap.add_argument("--out-md", default="calib_report.md")
    args = ap.parse_args()

    calib_rows = _load_ndjson(Path(args.calib))
    replay_rows = _load_ndjson(Path(args.replay)) if args.replay else []

    grouped = _group_by(calib_rows)
    grouped_replay = _group_by(replay_rows) if replay_rows else {}

    by_key: dict[str, Any] = {}
    mismatch_total = 0
    for key, rows in grouped.items():
        k = key.as_str()
        bars = _bars_to_ready(rows, int(args.tf_ms))
        cnt, rate = _audit_rate(rows)
        st = _stability_stats(rows)
        src_last = str(rows[-1].get("src", "static") or "static") if rows else "static"
        n_last = int(rows[-1].get("n", 0) or 0) if rows else 0

        mm = 0
        if replay_rows:
            rrows = grouped_replay.get(key, [])
            mm = _mismatch_count(rows, rrows)
            mismatch_total += mm

        by_key[k] = {
            "symbol": key.symbol,
            "regime": key.regime,
            "bars_to_ready": bars,
            "audit_count": cnt,
            "audit_rate_s": rate,
            "src_last": src_last,
            "n_last": n_last,
            "stability": st,
            "mismatch": mm,
        }

    report = {
        "rows_total": len(calib_rows),
        "keys_total": len(by_key),
        "mismatch_total": mismatch_total,
        "by_key": dict(sorted(by_key.items(), key=lambda kv: kv[0])),
    }

    Path(args.out_json).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    Path(args.out_md).write_text(_render_md(report), encoding="utf-8")


if __name__ == "__main__":
    main()
