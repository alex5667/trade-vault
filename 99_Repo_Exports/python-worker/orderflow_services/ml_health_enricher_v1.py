from __future__ import annotations

#!/usr/bin/env python3
from utils.time_utils import get_ny_time_millis

"""Phase 0.1 health enricher for `ml_model_runtime_1m`.

Consumes existing drift/calibration artifacts and enriches recent runtime rows with:
- psi_top_json / ks_top_json from feature drift batch report
- ece / brier from model-family specific status files where available

Fail-open and scanner_infra-only.
""",
import json
import os
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from prometheus_client import Gauge, Histogram, start_http_server
import contextlib

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

try:
    import psycopg2  # type: ignore
    from psycopg2.extras import Json  # type: ignore
except Exception:  # pragma: no cover
    psycopg2 = None  # type: ignore
    Json = None  # type: ignore


REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
DB_DSN = os.getenv("TRADES_DB_DSN", "")
DB_ENABLE = os.getenv("ML_HEALTH_ENRICH_DB_ENABLE", "1") == "1"
PORT = int(os.getenv("ML_HEALTH_ENRICH_EXPORTER_PORT", "9855"))
INTERVAL_S = float(os.getenv("ML_HEALTH_ENRICH_INTERVAL_S", "120"))
LOOKBACK_MIN = int(os.getenv("ML_HEALTH_ENRICH_LOOKBACK_MIN", "15"))
OUT_SUMMARY_KEY = os.getenv("ML_HEALTH_ENRICH_SUMMARY_KEY", "metrics:ml:health_enriched:last")
DRIFT_HASH_KEY = os.getenv("FEATURE_DRIFT_BATCH_METRICS_KEY", "metrics:feature_drift_batch:last")
EDGE_STACK_STATUS_FILE = os.getenv("EDGE_STACK_SHADOW_STATUS_FILE", "/var/lib/trade/of_reports/out/edge_stack/shadow_status.json")
META_MODEL_HEALTH_JSON = os.getenv("META_MODEL_HEALTH_JSON", "")
ML_SCORER_HEALTH_JSON = os.getenv("ML_SCORER_HEALTH_JSON", "")


UP = Gauge("ml_health_enricher_up", "1 if enrichment loop is healthy")
LAST_RUN_TS = Gauge("ml_health_enricher_last_run_ts_seconds", "Last successful enrichment loop")
ROWS_UPDATED = Gauge("ml_health_enricher_rows_updated", "Last run updated rows", ["family"])
LAST_PSI_N = Gauge("ml_health_enricher_last_psi_top_n", "Last drift enrichment psi top count")
LAST_KS_N = Gauge("ml_health_enricher_last_ks_top_n", "Last drift enrichment ks top count")
LOOP_LAT = Histogram("ml_health_enricher_loop_seconds", "Loop duration seconds")


@dataclass
class FamilyMetrics:
    ece: float | None
    brier: float | None


def _now_ms() -> int:
    return get_ny_time_millis()


def _as_str(v: Any, d: str = "") -> str:
    try:
        if v is None:
            return d
        if isinstance(v, (bytes, bytearray)):
            return bytes(v).decode("utf-8", errors="ignore")
        return str(v)
    except Exception:
        return d


def _as_float(v: Any, d: float | None = None) -> float | None:
    try:
        if v is None or v == "":
            return d
        x = float(v)
        return x
    except Exception:
        return d


def _read_json(path: str) -> dict[str, Any]:
    if not path:
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _drift_top_from_report(report_json_path: str) -> tuple[list[str], list[str]]:
    rep = _read_json(report_json_path)
    rows = rep.get("features") if isinstance(rep.get("features"), list) else []
    ranked_psi: list[tuple[float, str]] = []
    ranked_ks: list[tuple[float, str]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        f = _as_str(row.get("feature"), "")
        if not f:
            continue
        psi = _as_float(row.get("psi"), 0.0) or 0.0
        ks = _as_float(row.get("ks_stat"), 0.0) or 0.0
        ranked_psi.append((psi, f))
        ranked_ks.append((ks, f))
    ranked_psi.sort(reverse=True)
    ranked_ks.sort(reverse=True)
    return [f for _, f in ranked_psi[:5]], [f for _, f in ranked_ks[:5]]


def _family_metrics() -> dict[str, FamilyMetrics]:
    out: dict[str, FamilyMetrics] = {}

    edge = _read_json(EDGE_STACK_STATUS_FILE)
    try:
        cal = (((edge.get("champion") or {}).get("metrics") or {}).get("cal") or {})
        out["edge_stack_v1"] = FamilyMetrics(
            ece=_as_float(cal.get("ece"), None),
            brier=_as_float(cal.get("brier"), None),
        )
    except Exception:
        out["edge_stack_v1"] = FamilyMetrics(None, None)

    meta = _read_json(META_MODEL_HEALTH_JSON)
    if meta:
        out["meta_lr"] = FamilyMetrics(
            ece=_as_float(meta.get("ece"), None),
            brier=_as_float(meta.get("brier"), None),
        )

    scorer = _read_json(ML_SCORER_HEALTH_JSON)
    if scorer:
        for fam in ("ml_scorer_v2", "ml_scorer_v3"):
            out[fam] = FamilyMetrics(
                ece=_as_float(scorer.get("ece"), None),
                brier=_as_float(scorer.get("brier"), None),
            )
    return out


def _db_update_family(family: str, ece: float | None, brier: float | None, psi_top: Sequence[str], ks_top: Sequence[str]) -> int:
    if not DB_ENABLE or not DB_DSN or psycopg2 is None:
        return 0
    conn = None
    try:
        print(f"Connecting to DB for {family}", flush=True)
        conn = psycopg2.connect(DB_DSN)
        conn.autocommit = True
        print(f"Connected to DB for {family}, taking cursor", flush=True)
        cur = conn.cursor()
        ts_from = _now_ms() - LOOKBACK_MIN * 60_000
        sql = """,
        UPDATE ml_model_runtime_1m
           SET ece = COALESCE(%s, ece),
               brier = COALESCE(%s, brier),
               psi_top_json = %s,
               ks_top_json = %s
         WHERE ts_ms >= %s
           AND model_id LIKE %s,
        """,
        print(f"Executing query for {family}", flush=True)
        cur.execute(
            sql,
            (
                ece,
                brier,
                Json(list(psi_top)) if Json is not None else json.dumps(list(psi_top)),
                Json(list(ks_top)) if Json is not None else json.dumps(list(ks_top)),
                ts_from,
                f"{family}:%",
            ),
        )
        n = int(cur.rowcount or 0)
        cur.close()
        return n
    except Exception:
        return 0
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def main() -> int:
    if redis is None:
        raise RuntimeError("redis-py is required")
    cli = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    start_http_server(PORT)
    while True:
        t0 = time.perf_counter()
        try:
            print("Starting loop", flush=True)
            drift = cli.hgetall(DRIFT_HASH_KEY) or {}
            print("Fetched drift", flush=True)
            psi_top, ks_top = _drift_top_from_report(_as_str(drift.get("report_json"), ""))
            print("Parsed drift", flush=True)
            LAST_PSI_N.set(float(len(psi_top)))
            LAST_KS_N.set(float(len(ks_top)))
            fam = _family_metrics()
            print("Fetched family metrics", flush=True)
            updated_total = 0
            for family, metrics in fam.items():
                print(f"Updating db for {family}", flush=True)
                n = _db_update_family(family, metrics.ece, metrics.brier, psi_top, ks_top)
                updated_total += n
                ROWS_UPDATED.labels(family=family).set(float(n))
            with contextlib.suppress(Exception):
                cli.hset(
                    OUT_SUMMARY_KEY,
                    mapping={
                        "updated_ts_ms": str(_now_ms()),
                        "rows_updated": str(updated_total),
                        "psi_top_json": json.dumps(psi_top, separators=(",", ":")),
                        "ks_top_json": json.dumps(ks_top, separators=(",", ":")),
                    }
                )
            LAST_RUN_TS.set(time.time())
            UP.set(1)
        except Exception:
            import traceback
            with open("/app/err.txt", "w") as f:
                traceback.print_exc(file=f)
            UP.set(0)
        LOOP_LAT.observe(max(0.0, time.perf_counter() - t0))
        time.sleep(INTERVAL_S)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
