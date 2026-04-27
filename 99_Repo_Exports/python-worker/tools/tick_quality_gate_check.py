"""Tick quality gate from Prometheus /metrics.

Purpose
  Provide a deterministic, automation-friendly gate that can be used by ramp
  scripts (or operators) to stop increasing exposure when tick quality degrades.

Signals (read from /metrics)
  - Gauges (EMA):
      tick_unknown_side_ema{symbol}
      tick_ts_source_now_ema{symbol}
      tick_ts_source_stream_id_ema{symbol}
      tick_event_stream_skew_abs_ema_ms{symbol}
      tick_event_age_abs_ema_ms{symbol}

  - Histograms (windowed via two scrapes):
      tick_ingest_process_ms_bucket{symbol,le}
      tick_ingest_e2e_delay_ms_bucket{symbol,le}

Exit codes
  0  PASS
  2  FAIL (one or more thresholds breached)
  1  INSUFFICIENT_DATA (missing metrics / zero samples)

This tool is stdlib-only.
"""

from __future__ import annotations

import argparse
import os
import json
import math
import re
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

# Exit codes
# 0  PASS
# 1  INSUFFICIENT_DATA
# 2  FAIL
# 3  METRICS_UNAVAILABLE (network/connection error scraping /metrics)
_EXIT_METRICS_UNAVAILABLE = 3


LABEL_RE = re.compile(r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)\{(?P<labels>.*)\}$")


def _parse_labels(lbls: str) -> Dict[str, str]:
    # Prometheus label values are double-quoted with escapes.
    # We implement a minimal parser sufficient for our metrics.
    out: Dict[str, str] = {}
    s = lbls.strip()
    if not s:
        return out
    i = 0
    n = len(s)
    while i < n:
        # key
        k0 = i
        while i < n and s[i] not in "=":
            i += 1
        key = s[k0:i].strip()
        if i >= n or s[i] != "=":
            break
        i += 1
        if i >= n or s[i] != '"':
            break
        i += 1
        # value
        val_chars: List[str] = []
        while i < n:
            c = s[i]
            if c == "\\":
                if i + 1 < n:
                    val_chars.append(s[i + 1])
                    i += 2
                    continue
                i += 1
                continue
            if c == '"':
                i += 1
                break
            val_chars.append(c)
            i += 1
        out[key] = "".join(val_chars)
        # optional comma
        while i < n and s[i] in ", ":
            i += 1
    return out


def _metric_key(name: str, labels: Dict[str, str]) -> Tuple[str, Tuple[Tuple[str, str], ...]]:
    return name, tuple(sorted(labels.items()))


def parse_prometheus_text(text: str) -> Dict[Tuple[str, Tuple[Tuple[str, str], ...]], float]:
    """Parse Prometheus exposition format into a flat dict."""
    out: Dict[Tuple[str, Tuple[Tuple[str, str], ...]], float] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        # split by whitespace: <metric> <value> [timestamp]
        parts = line.split()
        if len(parts) < 2:
            continue
        m = parts[0]
        v = parts[1]
        try:
            val = float(v)
        except Exception:
            continue
        labels: Dict[str, str] = {}
        name = m
        mm = LABEL_RE.match(m)
        if mm:
            name = mm.group("name")
            labels = _parse_labels(mm.group("labels"))
        out[_metric_key(name, labels)] = val
    return out


def fetch_metrics(
    url: str,
    timeout_s: float = 5.0,
    retries: int = 3,
    retry_delay_s: float = 5.0,
) -> Dict[Tuple[str, Tuple[Tuple[str, str], ...]], float]:
    """Fetch and parse Prometheus text metrics from *url*.

    Retries up to *retries* times (with *retry_delay_s* sleep between attempts)
    before re-raising the final URLError/OSError so callers can handle
    transient connection errors (e.g. target not yet ready at startup).
    """
    req = urllib.request.Request(url, headers={"User-Agent": "tick-quality-gate/1.0"})
    last_exc: Exception = RuntimeError("no attempts made")
    for attempt in range(max(retries, 1)):
        try:
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                data = resp.read().decode("utf-8", errors="replace")
            return parse_prometheus_text(data)
        except (urllib.error.URLError, OSError) as exc:
            last_exc = exc
            if attempt < retries - 1:
                sys.stderr.write(
                    f"[tick-gate] fetch attempt {attempt + 1}/{retries} failed: {exc}; "
                    f"retrying in {retry_delay_s}s\n"
                )
                time.sleep(retry_delay_s)
    raise last_exc


def _labels_to_dict(lbl_tuple: Tuple[Tuple[str, str], ...]) -> Dict[str, str]:
    return {k: v for k, v in lbl_tuple}


def find_gauge(
    m: Dict[Tuple[str, Tuple[Tuple[str, str], ...]], float],
    name: str,
    symbol: Optional[str],
    default: Optional[float] = None,
) -> Optional[float]:
    for (n, lbls), v in m.items():
        if n != name:
            continue
        d = _labels_to_dict(lbls)
        if symbol is None:
            return v
        if d.get("symbol") == symbol:
            return v
    return default


@dataclass
class Histogram:
    # bucket upper bound -> count
    buckets: Dict[float, float]
    count: float


def _histogram_from_snapshot(
    snap: Dict[Tuple[str, Tuple[Tuple[str, str], ...]], float],
    base_name: str,
    symbol: Optional[str],
) -> Optional[Histogram]:
    buckets: Dict[float, float] = {}
    total_count: Optional[float] = None

    for (n, lbls), v in snap.items():
        if n == base_name + "_count":
            d = _labels_to_dict(lbls)
            if symbol is None or d.get("symbol") == symbol:
                total_count = v
        if n != base_name + "_bucket":
            continue
        d = _labels_to_dict(lbls)
        if symbol is not None and d.get("symbol") != symbol:
            continue
        le = d.get("le")
        if le is None:
            continue
        try:
            ub = float(le)
        except Exception:
            if le == "+Inf":
                ub = float("inf")
            else:
                continue
        buckets[ub] = v

    if not buckets:
        return None
    if total_count is None:
        # infer from +Inf bucket
        total_count = buckets.get(float("inf"), 0.0)
    return Histogram(buckets=buckets, count=float(total_count))


def histogram_window_delta(h1: Histogram, h2: Histogram) -> Histogram:
    # h2 is later than h1
    out: Dict[float, float] = {}
    all_ubs = set(h1.buckets.keys()) | set(h2.buckets.keys())
    for ub in all_ubs:
        out[ub] = float(h2.buckets.get(ub, 0.0)) - float(h1.buckets.get(ub, 0.0))
    cnt = float(h2.count) - float(h1.count)
    # clamp negatives to 0 (counter reset)
    for ub in list(out.keys()):
        if out[ub] < 0.0:
            out[ub] = 0.0
    if cnt < 0.0:
        cnt = 0.0
    return Histogram(buckets=out, count=cnt)


def histogram_quantile(q: float, h: Histogram) -> Optional[float]:
    """Approx quantile from cumulative bucket counts.

    For window deltas, bucket values are cumulative counts.
    We return the first bucket upper bound where cum >= q * total.
    """
    if h.count <= 0.0:
        return None
    qq = min(max(float(q), 0.0), 1.0)
    target = qq * float(h.count)

    # sort by upper bound
    items = sorted(h.buckets.items(), key=lambda x: x[0])
    for ub, cum in items:
        if cum >= target:
            return ub
    # fallback: +Inf
    return float("inf")


def _safe_float(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    try:
        if math.isnan(float(x)):
            return None
        return float(x)
    except Exception:
        return None


def _status(pass_ok: bool, insufficient: bool) -> str:
    if insufficient:
        return "insufficient_data"
    return "pass" if pass_ok else "fail"


def _emit_unavailable(
    args: "argparse.Namespace",
    url: str,
    sym: Optional[str],
    window_s: int,
    exc: Exception,
) -> None:
    """Emit a metrics_unavailable result when the /metrics endpoint is unreachable."""
    sys.stderr.write(f"[tick-gate] metrics unavailable at {url}: {exc}\n")
    result: Dict = {
        "status": "metrics_unavailable",
        "metrics_url": url,
        "symbol": sym,
        "window_s": window_s,
        "error": str(exc),
    }
    if getattr(args, "json", False):
        sys.stdout.write(json.dumps(result, ensure_ascii=False) + "\n")
    else:
        sys.stdout.write(f"status=metrics_unavailable url={url} error={exc}\n")


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--metrics-url", default="http://localhost:8000/metrics")
    ap.add_argument("--symbol", default=None, help="Optional symbol label (e.g. BTCUSDT).")
    ap.add_argument("--window-s", type=int, default=int(float(os.getenv("TICK_GATE_WINDOW_S", "60"))))
    ap.add_argument("--timeout-s", type=float, default=5.0)

    ap.add_argument("--process-p99-ms", type=float, default=float(os.getenv("TICK_GATE_PROCESS_P99_MS", "25")))
    ap.add_argument("--e2e-p99-ms", type=float, default=float(os.getenv("TICK_GATE_E2E_P99_MS", "5000")))
    ap.add_argument("--unknown-side-ema", type=float, default=float(os.getenv("TICK_GATE_UNKNOWN_SIDE_EMA", "0.10")))
    ap.add_argument("--ts-now-ema", type=float, default=float(os.getenv("TICK_GATE_TS_NOW_EMA", "0.05")))
    ap.add_argument("--ts-stream-id-ema", type=float, default=float(os.getenv("TICK_GATE_TS_STREAM_ID_EMA", "0.20")))
    ap.add_argument("--skew-ema-ms", type=float, default=float(os.getenv("TICK_GATE_SKEW_EMA_MS", "30000")))
    ap.add_argument("--age-ema-ms", type=float, default=float(os.getenv("TICK_GATE_AGE_EMA_MS", "30000")))
    ap.add_argument("--json", action="store_true")

    args = ap.parse_args(argv)

    url = str(args.metrics_url)
    sym = args.symbol
    window_s = max(int(args.window_s), 5)

    # Scrape #1
    t0 = time.time()
    try:
        m1 = fetch_metrics(url, timeout_s=float(args.timeout_s))
    except (urllib.error.URLError, OSError) as exc:
        _emit_unavailable(args, url, sym, window_s, exc)
        return _EXIT_METRICS_UNAVAILABLE

    # Scrape #2 after window
    time.sleep(window_s)
    t1 = time.time()
    try:
        m2 = fetch_metrics(url, timeout_s=float(args.timeout_s))
    except (urllib.error.URLError, OSError) as exc:
        _emit_unavailable(args, url, sym, window_s, exc)
        return _EXIT_METRICS_UNAVAILABLE
    dt_s = max(t1 - t0, 0.001)

    # Gauges
    g_unknown = _safe_float(find_gauge(m2, "tick_unknown_side_ema", sym))
    g_now = _safe_float(find_gauge(m2, "tick_ts_source_now_ema", sym))
    g_stream = _safe_float(find_gauge(m2, "tick_ts_source_stream_id_ema", sym))
    g_skew = _safe_float(find_gauge(m2, "tick_event_stream_skew_abs_ema_ms", sym))
    g_age = _safe_float(find_gauge(m2, "tick_event_age_abs_ema_ms", sym))

    # Histograms (windowed)
    h_proc_1 = _histogram_from_snapshot(m1, "tick_ingest_process_ms", sym)
    h_proc_2 = _histogram_from_snapshot(m2, "tick_ingest_process_ms", sym)
    h_e2e_1 = _histogram_from_snapshot(m1, "tick_ingest_e2e_delay_ms", sym)
    h_e2e_2 = _histogram_from_snapshot(m2, "tick_ingest_e2e_delay_ms", sym)

    proc_p99 = None
    e2e_p99 = None
    proc_n = 0.0
    e2e_n = 0.0

    if h_proc_1 and h_proc_2:
        hd = histogram_window_delta(h_proc_1, h_proc_2)
        proc_n = float(hd.count)
        proc_p99 = _safe_float(histogram_quantile(0.99, hd))

    if h_e2e_1 and h_e2e_2:
        hd = histogram_window_delta(h_e2e_1, h_e2e_2)
        e2e_n = float(hd.count)
        e2e_p99 = _safe_float(histogram_quantile(0.99, hd))

    # Evaluate
    insufficient = False
    reasons: List[str] = []

    # Gauges are optional but recommended; lack -> insufficient.
    for name, val in (
        ("tick_unknown_side_ema", g_unknown),
        ("tick_ts_source_now_ema", g_now),
        ("tick_ts_source_stream_id_ema", g_stream),
        ("tick_event_stream_skew_abs_ema_ms", g_skew),
        ("tick_event_age_abs_ema_ms", g_age),
    ):
        if val is None:
            insufficient = True
            reasons.append(f"missing:{name}")

    # Histograms need at least some samples.
    if proc_p99 is None or proc_n <= 0.0:
        insufficient = True
        reasons.append("missing:tick_ingest_process_ms")
    if e2e_p99 is None or e2e_n <= 0.0:
        insufficient = True
        reasons.append("missing:tick_ingest_e2e_delay_ms")

    pass_ok = True
    if not insufficient:
        if g_unknown is not None and g_unknown > float(args.unknown_side_ema):
            pass_ok = False
            reasons.append(f"breach:unknown_side_ema>{args.unknown_side_ema}")
        if g_now is not None and g_now > float(args.ts_now_ema):
            pass_ok = False
            reasons.append(f"breach:ts_now_ema>{args.ts_now_ema}")
        if g_stream is not None and g_stream > float(args.ts_stream_id_ema):
            pass_ok = False
            reasons.append(f"breach:ts_stream_id_ema>{args.ts_stream_id_ema}")
        if g_skew is not None and g_skew > float(args.skew_ema_ms):
            pass_ok = False
            reasons.append(f"breach:skew_ema_ms>{args.skew_ema_ms}")
        if g_age is not None and g_age > float(args.age_ema_ms):
            pass_ok = False
            reasons.append(f"breach:age_ema_ms>{args.age_ema_ms}")

        if proc_p99 is not None and proc_p99 > float(args.process_p99_ms):
            pass_ok = False
            reasons.append(f"breach:process_p99_ms>{args.process_p99_ms}")
        if e2e_p99 is not None and e2e_p99 > float(args.e2e_p99_ms):
            pass_ok = False
            reasons.append(f"breach:e2e_p99_ms>{args.e2e_p99_ms}")

    result = {
        "status": _status(pass_ok, insufficient),
        "metrics_url": url,
        "symbol": sym,
        "window_s": window_s,
        "dt_s": dt_s,
        "gauges": {
            "unknown_side_ema": g_unknown,
            "ts_source_now_ema": g_now,
            "ts_source_stream_id_ema": g_stream,
            "skew_abs_ema_ms": g_skew,
            "age_abs_ema_ms": g_age,
        },
        "hist": {
            "process": {"p99_ms": proc_p99, "n": proc_n},
            "e2e": {"p99_ms": e2e_p99, "n": e2e_n},
        },
        "thresholds": {
            "unknown_side_ema": float(args.unknown_side_ema),
            "ts_now_ema": float(args.ts_now_ema),
            "ts_stream_id_ema": float(args.ts_stream_id_ema),
            "skew_ema_ms": float(args.skew_ema_ms),
            "age_ema_ms": float(args.age_ema_ms),
            "process_p99_ms": float(args.process_p99_ms),
            "e2e_p99_ms": float(args.e2e_p99_ms),
        },
        "reasons": reasons,
    }

    if args.json:
        sys.stdout.write(json.dumps(result, ensure_ascii=False) + "\n")
    else:
        sys.stdout.write(f"status={result['status']} window_s={window_s} symbol={sym or '*'}\n")
        for k, v in result["gauges"].items():
            sys.stdout.write(f"  gauge.{k}={v}\n")
        sys.stdout.write(f"  hist.process.p99_ms={proc_p99} n={proc_n}\n")
        sys.stdout.write(f"  hist.e2e.p99_ms={e2e_p99} n={e2e_n}\n")
        if reasons:
            sys.stdout.write("  reasons=" + ",".join(reasons) + "\n")

    if insufficient:
        return 1
    return 0 if pass_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
