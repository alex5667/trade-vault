#!/usr/bin/env python3
"""Check whether signals:of:inputs has enough data across v15_of new groups
to safely train a v15_of model.

Closes the readiness-gate gap identified in audit 2026-05-19:
core/external_features_payload_v1.py emits 156 keys (Phase 8.2/8.3/8.4/8.5/
P1/P2/P3/4.x) that are absent in v14_of and present in v15_of, but a sizable
subset (audit 2026-05-18: 85 of 156) is perma-zero in golden fixtures
because their upstream producers (Bybit ingest, Hawkes, CMC/DefiLlama,
breadth-v2, liq-imbalance, sector-agg) are not yet wired or hadn't been
running long enough.

Without this gate, training v15_of immediately yields a model that learns a
constant-zero pattern on those keys → biased predictions + train/serve skew
once upstreams turn on.

Mirrors `check_v14_oe_readiness.evaluate_readiness`:

    from tools.check_v15_of_readiness import evaluate_readiness
    res = evaluate_readiness()  # ready: bool, coverage: float, ...

Exit codes (CLI):
    0 — ready (all required groups ≥ MIN_COVERAGE and stream span ≥ MIN_DAYS)
    1 — not ready yet (prints reason)
    2 — Redis unreachable
"""
from __future__ import annotations

import json
import os
import sys
import time

# v15_of new-group required canaries — at least one key per group should be
# present-and-non-zero in ≥ MIN_COVERAGE of sampled entries to consider the
# upstream "wired".
#
# Keys chosen as canary representatives (cheapest non-zero signal per group):
V15_REQUIRED_BY_GROUP: dict[str, list[str]] = {
    # Phase 8.2 — cyclical time + sector breadth + news
    "p82_time_cyc": ["hour_sin", "hour_cos", "sector_breadth_1m"],
    # Phase 8.3 — taker ratios + force-order
    "p83_taker_force": ["taker_buy_sell_ratio", "force_order_long_notional_1m"],
    # Phase 8.4 — Hawkes/VPIN (heavy upstream dependency)
    "p84_hawkes_vpin": ["hawkes_taker_buy_lam", "vpin_tox_ema"],
    # Phase 8.5 — cross-venue sanity (Binance ↔ Bybit)
    "p85_cross_venue": ["cross_venue_dislocation_bps"],
    # Phase 8.5 — CoinGecko macro
    "p85_coingecko": ["cg_btc_dom_pct"],
    # Phase 8.5 — Deribit ext
    "p85_deribit_ext": ["deribit_perp_basis_bps"],
    # Phase 8.5 — DefiLlama
    "p85_defillama": ["dl_stablecoin_mcap_usd"],
    # P1 — Deribit term structure
    "p1_deribit_term": ["deribit_btc_iv_7d", "deribit_iv_term_structure_7d_30d"],
    # P1 — Breadth 5m + rel strength
    "p1_breadth_relstr": ["market_breadth_vol_5m", "symbol_rel_strength_vs_btc_1m"],
    # P2 — Bybit cross-venue
    "p2_bybit": ["bybit_ret_1m", "binance_bybit_price_diff_bps"],
    # P3 — Fear & Greed delta + fallback feeds
    "p3_fg_cp_cmc_dl": ["fear_greed_delta_1d", "cp_btc_dom_pct", "cmc_btc_dom_pct"],
    # Deriv base + PIT priors + macro cal
    "deriv_pit_macro": [
        "funding_rate", "prior_winrate_symbol_kind_7d", "macro_event_severity",
    ],
    # Roll VPIN + sector agg + liqmap alias
    "roll_vpin_liqmap": ["vpin_tox_1m", "sector_delta_z_median", "liq_heatmap_density_above"],
}

MIN_COVERAGE = 0.50      # ≥50% of sampled entries should have non-zero canary per group
MIN_DAYS = 5
SAMPLE_SIZE = 200


def _parse_payload(data: dict) -> dict:
    raw = data.get("payload") or data.get("data") or ""
    if isinstance(raw, str) and raw.strip().startswith("{"):
        try:
            return json.loads(raw)
        except Exception:
            pass
    return data


def _is_nonzero(v) -> bool:
    if v is None:
        return False
    try:
        return float(v) != 0.0
    except (TypeError, ValueError):
        return bool(v)


def evaluate_readiness(redis_client=None) -> dict:
    """Library form. Returns dict with keys:
        ready (bool)
        coverage (float)             — min coverage across required groups
        span_days (float)
        sampled (int)
        group_coverage (dict[str, float])
        reasons (list[str])
        error (str, optional)
    """
    out: dict = {
        "ready": False,
        "coverage": 0.0,
        "span_days": 0.0,
        "sampled": 0,
        "group_coverage": {},
        "reasons": [],
        "min_coverage": MIN_COVERAGE,
        "min_days": MIN_DAYS,
    }
    stream = os.getenv("OF_INPUTS_STREAM", "signals:of:inputs")
    try:
        r = redis_client or _get_redis_or_raise()
    except Exception as e:
        out["error"] = f"redis_unreachable: {e}"
        out["reasons"].append(out["error"])
        return out

    try:
        entries = r.xrevrange(stream, count=SAMPLE_SIZE)
    except Exception as e:
        out["error"] = f"xrevrange_failed: {e}"
        out["reasons"].append(out["error"])
        return out

    if not entries:
        out["error"] = f"stream_empty: {stream}"
        out["reasons"].append(out["error"])
        return out

    now_ms = int(time.time() * 1000)
    # span = actual stream age using first entry, not just the 200-sample window
    newest_id = entries[0][0]
    oldest_sample_id = entries[-1][0]
    try:
        first_entries: list = r.xrange(stream, "-", "+", count=1)  # type: ignore[assignment]
        first_id = first_entries[0][0] if first_entries else oldest_sample_id
    except Exception:
        first_id = oldest_sample_id
    try:
        first_ms = int(str(first_id).split("-")[0])
    except Exception:
        first_ms = now_ms
    span_days = (now_ms - first_ms) / 86_400_000

    total = len(entries)
    # group → count of entries where ≥1 canary in group is present-and-non-zero
    group_hits: dict[str, int] = {g: 0 for g in V15_REQUIRED_BY_GROUP}
    for _xid, data in entries:
        payload = _parse_payload(data)
        ind = payload.get("indicators", payload)
        if not isinstance(ind, dict):
            continue
        for group, canaries in V15_REQUIRED_BY_GROUP.items():
            if any(_is_nonzero(ind.get(k)) for k in canaries):
                group_hits[group] += 1

    group_coverage = {g: (group_hits[g] / total) for g in V15_REQUIRED_BY_GROUP}
    out.update({
        "group_coverage": group_coverage,
        "coverage": min(group_coverage.values()) if group_coverage else 0.0,
        "span_days": span_days,
        "sampled": total,
        "newest_id": str(newest_id),
        "oldest_id": str(first_id),
    })

    for g, cov in group_coverage.items():
        if cov < MIN_COVERAGE:
            out["reasons"].append(f"group {g} coverage {cov:.1%} < {MIN_COVERAGE:.0%}")
    if span_days < MIN_DAYS:
        out["reasons"].append(f"span {span_days:.1f}d < {MIN_DAYS}d")

    out["ready"] = not out["reasons"]
    return out


def _get_redis_or_raise():
    """Connect to Redis.

    Resolution order:
      1. REDIS_URL env (e.g. `redis://redis-worker-1:6379/0`) — used by
         in-container callers including the nightly_v15_of_train_bundle wrapper
         and the v15-of-train-timer compose service.
      2. REDIS_WORKER_HOST + REDIS_WORKER_PORT — host-side ad-hoc CLI usage
         (defaults `localhost:63791` map to the worker-1 extern port).

    decode_responses=True so xrevrange returns str entries.
    """
    import redis
    url = os.getenv("REDIS_URL", "").strip()
    if url:
        r = redis.Redis.from_url(url, decode_responses=True, socket_connect_timeout=3)
    else:
        host = os.getenv("REDIS_WORKER_HOST", "localhost")
        port = int(os.getenv("REDIS_WORKER_PORT", "63791"))
        r = redis.Redis(host=host, port=port, decode_responses=True, socket_connect_timeout=3)
    r.ping()
    return r


def main() -> None:
    try:
        r = _get_redis_or_raise()
    except Exception as e:
        host = os.getenv("REDIS_WORKER_HOST", "localhost")
        port = int(os.getenv("REDIS_WORKER_PORT", "63791"))
        print(f"ERROR: Redis unreachable at {host}:{port} — {e}")
        sys.exit(2)

    res = evaluate_readiness(redis_client=r)
    stream = os.getenv("OF_INPUTS_STREAM", "signals:of:inputs")
    if res.get("error", "").startswith("stream_empty"):
        print(f"FAIL: stream '{stream}' is empty or does not exist")
        sys.exit(1)

    total = res["sampled"]
    print(f"stream:        {stream}")
    print(f"sampled:       {total} entries")
    print(f"span:          {res['span_days']:.1f} days (oldest entry {res.get('oldest_id', '-')})")
    print(f"min coverage:  {res['coverage']:.1%} (across {len(res['group_coverage'])} groups)")
    print(f"\nPer-group coverage (out of {total}):")
    for g, cov in sorted(res["group_coverage"].items()):
        mark = "✓" if cov >= MIN_COVERAGE else "✗"
        canaries = ", ".join(V15_REQUIRED_BY_GROUP[g])
        print(f"  [{mark}] {g:<24s} {cov:>6.1%}  ({canaries})")

    if res["ready"]:
        print(f"\nREADY: all groups ≥ {MIN_COVERAGE:.0%} coverage, span ≥ {MIN_DAYS}d")
        print("Next step: enable v15_of nightly_train bundle.")
        sys.exit(0)
    print(f"\nNOT READY: {'; '.join(res['reasons'])}")
    sys.exit(1)


if __name__ == "__main__":
    main()
