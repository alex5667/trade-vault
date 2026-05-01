#!/usr/bin/env python3
"""
paper_vs_demo_compare.py — Compare paper trades (trade_monitor) vs demo trades (binance_executor_demo).

Reads both streams from Redis, matches by SID, and shows per-trade and aggregate
divergence in entry_price, exit_price, PnL, trailing SL, and timing.

Usage:
    python3 paper_vs_demo_compare.py               # last 200 entries from each stream
    python3 paper_vs_demo_compare.py --count 500    # more history
    python3 paper_vs_demo_compare.py --symbol BTCUSDT  # filter by symbol
    python3 paper_vs_demo_compare.py --json         # JSON output

ENV:
    REDIS_URL=redis://scanner-redis-worker-1:6379/0  (default)
    TRADES_CLOSED_STREAM_NAME=trades:closed          (paper trades)
    EXEC_STREAM=orders:exec                          (demo trades)
"""
import argparse
import json
import os
import sys
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple

try:
    import redis
except ImportError:
    print("pip install redis", file=sys.stderr)
    sys.exit(1)


# ── Config ──────────────────────────────────────────────────────────────────
REDIS_URL = os.getenv("REDIS_URL", os.getenv("REPORTS_REDIS_URL", "redis://scanner-redis-worker-1:6379/0"))
PAPER_STREAM = os.getenv("TRADES_CLOSED_STREAM_NAME", "trades:closed")
DEMO_STREAM = os.getenv("EXEC_STREAM", "orders:exec")


@dataclass
class TradeEntry:
    sid: str = ""
    symbol=""
    direction: str = ""
    entry_price: float = 0.0
    exit_price: float = 0.0
    pnl_net: float = 0.0
    lot: float = 0.0
    close_reason: str = ""
    trailing_started: bool = False
    ts_open_ms: int = 0
    ts_close_ms: int = 0
    source: str = ""


def _f(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _s(v, default=""):
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    return str(v) if v else default


def parse_paper_entry(data: dict) -> TradeEntry:
    """Parse a trades:closed stream entry."""
    g = lambda k: _s(data.get(k, b""))
    return TradeEntry(
        sid=g("sid"),
        symbol=g("symbol"),
        direction=g("direction"),
        entry_price=_f(data.get(b"entry_price", data.get(b"entry_px"))),
        exit_price=_f(data.get(b"exit_price", data.get(b"exit_px"))),
        pnl_net=_f(data.get(b"pnl_net", data.get(b"pnl"))),
        lot=_f(data.get(b"lot", data.get(b"qty"))),
        close_reason=g("close_reason") or g("close_reason_detail") or g("reason"),
        trailing_started=_s(data.get(b"trailing_started")) in ("1", "True", "true"),
        ts_open_ms=int(_f(data.get(b"entry_ts_ms", data.get(b"ts_open_ms")))),
        ts_close_ms=int(_f(data.get(b"exit_ts_ms", data.get(b"ts_close_ms")))),
        source="paper",
    )


def parse_demo_entry(data: dict) -> Optional[TradeEntry]:
    """Parse an orders:exec stream entry (only CLOSE/FILLED events)."""
    g = lambda k: _s(data.get(k, b""))
    event_type = g("event_type") or g("type") or g("status")
    # We want the final close event
    if event_type.upper() not in ("CLOSE", "CLOSED", "FILLED", "SL_HIT", "TP_HIT",
                                   "TRAILING_STOP", "FORCE_CLOSE", "FINALIZED"):
        return None
    return TradeEntry(
        sid=g("sid"),
        symbol=g("symbol"),
        direction=g("direction") or g("side"),
        entry_price=_f(data.get(b"entry_price", data.get(b"entry_px"))),
        exit_price=_f(data.get(b"exit_price", data.get(b"exit_px", data.get(b"close_price")))),
        pnl_net=_f(data.get(b"pnl_net", data.get(b"pnl", data.get(b"realized_pnl")))),
        lot=_f(data.get(b"lot", data.get(b"qty", data.get(b"quantity")))),
        close_reason=g("close_reason") or g("reason") or event_type,
        trailing_started=_s(data.get(b"trailing_started", b"")) in ("1", "True", "true"),
        ts_open_ms=int(_f(data.get(b"entry_ts_ms", data.get(b"ts_open_ms")))),
        ts_close_ms=int(_f(data.get(b"exit_ts_ms", data.get(b"ts_close_ms", data.get(b"ts_ms"))))),
        source="demo",
    )


@dataclass
class Comparison:
    sid: str
    symbol: str
    direction: str
    paper_entry: float
    demo_entry: float
    entry_delta_bps: float
    paper_exit: float
    demo_exit: float
    exit_delta_bps: float
    paper_pnl: float
    demo_pnl: float
    pnl_delta: float
    paper_reason: str
    demo_reason: str
    same_reason: bool
    paper_trailing: bool
    demo_trailing: bool


def bps_diff(a: float, b: float) -> float:
    """(a - b) / mid * 10000 bps."""
    mid = (a + b) / 2.0
    if mid <= 0:
        return 0.0
    return (a - b) / mid * 10_000.0


def compare(paper: TradeEntry, demo: TradeEntry) -> Comparison:
    return Comparison(
        sid=paper.sid,
        symbol=paper.symbol,
        direction=paper.direction,
        paper_entry=paper.entry_price,
        demo_entry=demo.entry_price,
        entry_delta_bps=bps_diff(paper.entry_price, demo.entry_price),
        paper_exit=paper.exit_price,
        demo_exit=demo.exit_price,
        exit_delta_bps=bps_diff(paper.exit_price, demo.exit_price),
        paper_pnl=paper.pnl_net,
        demo_pnl=demo.pnl_net,
        pnl_delta=paper.pnl_net - demo.pnl_net,
        paper_reason=paper.close_reason,
        demo_reason=demo.close_reason,
        same_reason=(paper.close_reason.upper().replace("_", "") ==
                     demo.close_reason.upper().replace("_", "")),
        paper_trailing=paper.trailing_started,
        demo_trailing=demo.trailing_started,
    )


def main():
    parser = argparse.ArgumentParser(description="Paper vs Demo trade comparison")
    parser.add_argument("--count", type=int, default=200, help="Max entries to read from each stream")
    parser.add_argument("--symbol", type=str, default="", help="Filter by symbol")
    parser.add_argument("--json", action="store_true", help="JSON output")
    parser.add_argument("--redis-url", type=str, default=REDIS_URL, help="Redis URL")
    args = parser.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=False)
    try:
        r.ping()
    except redis.ConnectionError as e:
        print(f"❌ Redis connection failed: {e}", file=sys.stderr)
        sys.exit(1)

    # ── Read paper trades ──
    paper_raw = r.xrevrange(PAPER_STREAM.encode(), count=args.count)
    paper_by_sid: Dict[str, TradeEntry] = {}
    for mid, data in paper_raw:
        t = parse_paper_entry(data)
        if t.sid and (not args.symbol or t.symbol.upper() == args.symbol.upper()):
            paper_by_sid[t.sid] = t

    # ── Read demo trades ──
    demo_raw = r.xrevrange(DEMO_STREAM.encode(), count=args.count * 5)  # more events, not all are closes
    demo_by_sid: Dict[str, TradeEntry] = {}
    for mid, data in demo_raw:
        t = parse_demo_entry(data)
        if t and t.sid and (not args.symbol or t.symbol.upper() == args.symbol.upper()):
            if t.sid not in demo_by_sid:  # keep latest
                demo_by_sid[t.sid] = t

    # ── Match by SID ──
    matched_sids = set(paper_by_sid.keys()) & set(demo_by_sid.keys())
    paper_only = set(paper_by_sid.keys()) - set(demo_by_sid.keys())
    demo_only = set(demo_by_sid.keys()) - set(paper_by_sid.keys())

    comparisons: List[Comparison] = []
    for sid in sorted(matched_sids):
        comparisons.append(compare(paper_by_sid[sid], demo_by_sid[sid]))

    if args.json:
        print(json.dumps({
            "matched": [asdict(c) for c in comparisons],
            "paper_only_count": len(paper_only),
            "demo_only_count": len(demo_only),
            "paper_only_sids": sorted(paper_only)[:20],
            "demo_only_sids": sorted(demo_only)[:20],
        }, indent=2, default=str))
        return

    # ── Text report ──
    print("=" * 100)
    print(f"  PAPER vs DEMO Trade Comparison Report")
    print(f"  Paper stream: {PAPER_STREAM}  ({len(paper_by_sid)} closed trades)")
    print(f"  Demo stream:  {DEMO_STREAM}  ({len(demo_by_sid)} close events)")
    print(f"  Matched by SID: {len(comparisons)}")
    print(f"  Paper-only: {len(paper_only)}  |  Demo-only: {len(demo_only)}")
    print("=" * 100)

    if not comparisons:
        print("\n⚠️  No matching SIDs found. This may mean:")
        print("  - No trades have completed on both systems yet")
        print("  - SIDs differ between paper and demo paths")
        print("  - Stream names are wrong")
        if paper_by_sid:
            print(f"\n  Sample paper SIDs: {list(paper_by_sid.keys())[:5]}")
        if demo_by_sid:
            print(f"  Sample demo SIDs: {list(demo_by_sid.keys())[:5]}")
        return

    # ── Per-trade table ──
    print(f"\n{'SID':>12} {'Symbol':>10} {'Dir':>5} │ "
          f"{'Δentry(bps)':>12} {'Δexit(bps)':>12} │ "
          f"{'Paper PnL':>12} {'Demo PnL':>12} {'ΔPNL':>10} │ "
          f"{'Paper reason':>16} {'Demo reason':>16}")
    print("─" * 140)

    total_entry_delta = []
    total_exit_delta = []
    total_pnl_paper = 0.0
    total_pnl_demo = 0.0
    same_reason_count = 0

    for c in comparisons:
        sid_short = c.sid[-10:] if len(c.sid) > 10 else c.sid
        print(f"{sid_short:>12} {c.symbol:>10} {c.direction:>5} │ "
              f"{c.entry_delta_bps:>+12.2f} {c.exit_delta_bps:>+12.2f} │ "
              f"{c.paper_pnl:>12.2f} {c.demo_pnl:>12.2f} {c.pnl_delta:>+10.2f} │ "
              f"{c.paper_reason:>16} {c.demo_reason:>16}")
        total_entry_delta.append(abs(c.entry_delta_bps))
        total_exit_delta.append(abs(c.exit_delta_bps))
        total_pnl_paper += c.paper_pnl
        total_pnl_demo += c.demo_pnl
        if c.same_reason:
            same_reason_count += 1

    # ── Summary ──
    n = len(comparisons)
    avg_entry = sum(total_entry_delta) / n if n else 0
    avg_exit = sum(total_exit_delta) / n if n else 0
    med_entry = sorted(total_entry_delta)[n // 2] if n else 0
    med_exit = sorted(total_exit_delta)[n // 2] if n else 0

    print("─" * 140)
    print(f"\n📊 Summary ({n} matched trades):")
    print(f"  Entry price Δ:  avg={avg_entry:.2f} bps  median={med_entry:.2f} bps")
    print(f"  Exit price Δ:   avg={avg_exit:.2f} bps  median={med_exit:.2f} bps")
    print(f"  Total PnL paper:  ${total_pnl_paper:+.2f}")
    print(f"  Total PnL demo:   ${total_pnl_demo:+.2f}")
    print(f"  PnL divergence:   ${total_pnl_paper - total_pnl_demo:+.2f}")
    print(f"  Same close reason: {same_reason_count}/{n} ({100*same_reason_count/n:.0f}%)" if n else "")
    print()

    # Quality score
    if avg_entry < 5 and avg_exit < 10:
        print("✅ Quality: EXCELLENT — paper and demo are closely aligned")
    elif avg_entry < 10 and avg_exit < 20:
        print("🟡 Quality: GOOD — minor divergence, expected for paper simulation")
    else:
        print("🔴 Quality: DIVERGENT — significant differences, investigate cause")


if __name__ == "__main__":
    main()
