from __future__ import annotations

import argparse
import json
import os
from collections.abc import Iterable
from typing import Any

import redis


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if (v is not None and str(v).strip() != "") else default


def _to_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def _to_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", "ignore")
    return str(v)


def _json_get(cli: redis.Redis, key: str) -> dict[str, Any]:
    raw = cli.get(key)
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _hash_get(cli: redis.Redis, key: str) -> dict[str, Any]:
    raw = cli.hgetall(key)
    if not raw:
        return {}
    return {_to_str(k): _to_str(v) for k, v in raw.items()}


def _stream_presence(cli: redis.Redis, stream: str) -> dict[str, Any]:
    try:
        xlen = int(cli.xlen(stream))
    except Exception:
        xlen = 0
    return {"stream": stream, "xlen": xlen, "nonempty": xlen > 0}


def _recent_indicators(cli: redis.Redis, stream: str, count: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for _id, fields in cli.xrevrange(stream, "+", "-", count=int(count)):
        payload = fields.get("payload")
        if not payload:
            continue
        try:
            data = json.loads(payload)
        except Exception:
            continue
        inds = data.get("indicators")
        if isinstance(inds, dict):
            out.append(inds)
    return out


def _coverage(rows: Iterable[dict[str, Any]], keys: list[str]) -> dict[str, dict[str, int]]:
    agg = {k: {"present": 0, "nonzero": 0} for k in keys}
    for row in rows:
        for k in keys:
            if k in row:
                agg[k]["present"] += 1
                if _to_float(row.get(k)) != 0.0:
                    agg[k]["nonzero"] += 1
    return agg


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Audit runtime readiness of feature groups from Redis.")
    ap.add_argument("--redis_url", default=_env("REDIS_URL", "redis://localhost:6379/0"))
    ap.add_argument("--signal_stream", default=_env("OF_INPUTS_STREAM", "signals:of:inputs"))
    ap.add_argument("--recent_count", type=int, default=int(_env("RUNTIME_FEATURE_AUDIT_RECENT_COUNT", "20")))
    ap.add_argument("--out_json", default="")
    args = ap.parse_args(argv)

    cli = redis.Redis.from_url(args.redis_url, decode_responses=True)
    rows = _recent_indicators(cli, args.signal_stream, args.recent_count)

    report = {
        "signal_stream": args.signal_stream,
        "recent_count": int(args.recent_count),
        "groups": {
            "vol": {
                "coverage": _coverage(rows, ["vol_fast_bps", "vol_slow_bps", "vol_ratio_z", "vol_regime_code", "vol_of_vol"]),
            },
            "liqmap": {
                "stream": _stream_presence(cli, "stream:liq_evt"),
                "snapshots": {
                    key: _json_get(cli, key)
                    for key in (
                        "liqmap:snapshot:BTCUSDT:5m",
                        "liqmap:snapshot:BTCUSDT:1h",
                        "liqmap:snapshot:ETHUSDT:5m",
                        "liqmap:snapshot:SOLUSDT:5m",
                    )
                },
                "coverage": _coverage(rows, ["liqmap_levels_n", "liqmap_5m_levels_n", "liqmap_1h_levels_n", "liq_heatmap_density_above", "liq_heatmap_density_below"]),
            },
            "hawkes": {
                "source": {"ctx:hawkes:BTCUSDT": _hash_get(cli, "ctx:hawkes:BTCUSDT")},
                "coverage": _coverage(rows, ["hawkes_taker_buy_lam", "hawkes_limit_add_lam", "added_bid_rate_ema", "added_ask_rate_ema"]),
            },
            "pit": {
                "source": {
                    "pit_priors:rolling:7d:BTCUSDT:default:all": _hash_get(cli, "pit_priors:rolling:7d:BTCUSDT:default:all"),
                    "pit_priors:rolling:30d:BTCUSDT:default:all": _hash_get(cli, "pit_priors:rolling:30d:BTCUSDT:default:all"),
                },
                "coverage": _coverage(rows, ["prior_ev_r_median", "prior_median_mfe_r_30d"]),
            },
            "tca": {
                "source": {
                    "tca:ema:BTCUSDT:default:us": _hash_get(cli, "tca:ema:BTCUSDT:default:us"),
                    "tca:ema:BTCUSDT:default:all": _hash_get(cli, "tca:ema:BTCUSDT:default:all"),
                },
                "coverage": _coverage(rows, ["tca_eff_spread_bps_ema", "tca_realized_spread_5s_bps_ema", "tca_perm_impact_5s_bps_ema", "tca_samples"]),
            },
            "external": {
                "source": {
                    "ctx:deribit:global": _json_get(cli, "ctx:deribit:global"),
                    "runtime:provider:coinmarketcap:global": _hash_get(cli, "runtime:provider:coinmarketcap:global"),
                    "runtime:defillama:stablecoins": _hash_get(cli, "runtime:defillama:stablecoins"),
                    "runtime:defillama:perps_oi": _hash_get(cli, "runtime:defillama:perps_oi"),
                    "runtime:coingecko:global": _hash_get(cli, "runtime:coingecko:global"),
                },
                "coverage": _coverage(rows, ["cmc_btc_dom_pct", "dl_perps_oi_delta_1d_pct", "deribit_eth_funding_8h", "deribit_vol_regime_code"]),
            },
        },
    }

    if args.out_json:
        with open(args.out_json, "w", encoding="utf-8") as fh:
            json.dump(report, fh, ensure_ascii=False, indent=2)

    print(json.dumps(report, ensure_ascii=False))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
