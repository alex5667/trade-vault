#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
"""nightly_conf_score_weight_tuning_bundle_v1.py

Phase2 production contour: nightly tuning job that:
  1) Exports replay inputs slice (signals:of:inputs archive) -> NDJSON
  2) Exports POSITION_CLOSED events from Postgres/Timescale -> NDJSON
  3) Builds training dataset parquet (inputs+closed join)
  4) Tunes confidence scorer weights (regime-aware, anti-correlation, synergy, sweep-typed) -> JSON
  5) Publishes the JSON to Redis per-symbol config hash (config:orderflow:<symbol>)
  6) Emits low-cardinality status to settings:dynamic_cfg for Prometheus exporter/alerts

This job is designed to be run under systemd timer.

Env (core):
  REDIS_URL: redis://.../0
  TRADES_DB_DSN: Postgres DSN for table position_events
  REPLAY_INPUTS_ARCHIVE_DIR: dir created by replay_inputs_archiver.py
  CONF_SCORE_TUNING_OUT_DIR: output bundle dir

Env (tuning control):
  CONF_SCORE_TUNING_DAYS: lookback window (default 30)
  CONF_SCORE_TUNING_MIN_JOINED_ROWS: min joined samples to publish (default 300)
  CONF_SCORE_TUNING_APPLY: 1 to publish to Redis (default 0 = dry-run)
  CONF_SCORE_TUNING_SYMBOLS: comma-separated symbols to publish (default BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT)
  CONF_SCORE_TUNING_FIELD: redis hash field (default conf_score_weight_tuning_json)
  CONF_SCORE_TUNING_BACKUP_FIELD: redis hash field for previous value (default conf_score_weight_tuning_json_prev)
  CONF_SCORE_TUNING_MAX_RUNTIME_SEC: overall watchdog (default 1800)

Notes:
  - The runtime loader must parse conf_score_weight_tuning_json into cfg['conf_score_weight_tuning'].
"""

from utils.time_utils import get_ny_time_millis

import json
import logging
import os
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import redis  # type: ignore


logger = logging.getLogger("conf_score_tuning")


def _utc_ts_ms() -> int:
    return get_ny_time_millis()


def _ensure_dir(p: str) -> None:
    Path(p).mkdir(parents=True, exist_ok=True)


def _json_dump(path: str, obj: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _json_load(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _run(cmd: List[str], *, timeout_s: int = 900) -> Tuple[int, str, str]:
    """Run external command; return (rc, stdout, stderr)."""
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
    return int(p.returncode), str(p.stdout), str(p.stderr)


def _redis_from_env() -> "redis.Redis":
    url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    return redis.Redis.from_url(url, decode_responses=True)


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default))))
    except Exception:
        return int(default)


def _env_str(name: str, default: str = "") -> str:
    return str(os.getenv(name, default) or default).strip()


def _split_syms(s: str) -> List[str]:
    out: List[str] = []
    for x in (s or "").split(","):
        x = x.strip().upper()
        if x:
            out.append(x)
    return out


@dataclass
class BundlePaths:
    base_dir: str
    run_id: str

    @property
    def run_dir(self) -> str:
        return str(Path(self.base_dir) / self.run_id)

    @property
    def inputs_ndjson(self) -> str:
        return str(Path(self.run_dir) / "inputs.ndjson")

    @property
    def closed_ndjson(self) -> str:
        return str(Path(self.run_dir) / "closed.ndjson")

    @property
    def dataset_parquet(self) -> str:
        return str(Path(self.run_dir) / "dataset.parquet")

    @property
    def tuning_json(self) -> str:
        return str(Path(self.run_dir) / "conf_score_weight_tuning.json")

    @property
    def status_json(self) -> str:
        return str(Path(self.run_dir) / "status.json")

    @property
    def latest_link(self) -> str:
        return str(Path(self.base_dir) / "latest")


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    start_wall = time.time()
    watchdog_s = _env_int("CONF_SCORE_TUNING_MAX_RUNTIME_SEC", 1800)

    # Inputs
    archive_dir = _env_str("REPLAY_INPUTS_ARCHIVE_DIR", "/var/lib/trade/replay_inputs_archives")
    trades_dsn = _env_str("TRADES_DB_DSN", "")

    out_base = _env_str("CONF_SCORE_TUNING_OUT_DIR", "/var/lib/trade/conf_score_weight_tuning")
    days = _env_int("CONF_SCORE_TUNING_DAYS", 30)
    min_joined = _env_int("CONF_SCORE_TUNING_MIN_JOINED_ROWS", 300)

    apply = _env_int("CONF_SCORE_TUNING_APPLY", 0) == 1
    symbols = _split_syms(_env_str("CONF_SCORE_TUNING_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT"))
    field = _env_str("CONF_SCORE_TUNING_FIELD", "conf_score_weight_tuning_json")
    backup_field = _env_str("CONF_SCORE_TUNING_BACKUP_FIELD", "conf_score_weight_tuning_json_prev")

    dyn_key = _env_str("CONF_SCORE_TUNING_DYN_CFG_KEY", "settings:dynamic_cfg")

    now_ms = _utc_ts_ms()
    start_ms = now_ms - int(days) * 86400 * 1000

    run_id = time.strftime("%Y%m%d_%H%M%S", time.gmtime(now_ms / 1000.0)) + "_" + uuid.uuid4().hex[:8]
    paths = BundlePaths(base_dir=out_base, run_id=run_id)

    _ensure_dir(paths.run_dir)

    status: Dict[str, Any] = {
        "run_id": run_id,
        "ts_ms": now_ms,
        "window": {"days": days, "start_ts_ms": start_ms, "end_ts_ms": now_ms},
        "apply": int(apply),
        "symbols": symbols,
        "paths": {
            "run_dir": paths.run_dir,
            "inputs": paths.inputs_ndjson,
            "closed": paths.closed_ndjson,
            "dataset": paths.dataset_parquet,
            "tuning": paths.tuning_json,
        },
        "steps": {},
        "ok": 0,
        "error": "",
    }

    r = _redis_from_env()

    def _dyn_update(m: Dict[str, Any]) -> None:
        # store low-cardinality metrics in dyn cfg
        try:
            r.hset(dyn_key, mapping={k: str(v) for k, v in m.items()})
        except Exception:
            return

    _dyn_update({
        "conf_score_tuning_last_ts_ms": now_ms,
        "conf_score_tuning_last_ok": 0,
        "conf_score_tuning_last_run_id": run_id,
        "conf_score_tuning_last_apply": int(apply),
    })

    # Resolve script paths (relative to repo root)
    repo_root = Path(__file__).resolve().parents[1]
    s_export_inputs = repo_root / "ml_analysis" / "tools" / "export_replay_inputs_ndjson_v1.py"
    s_export_closed = repo_root / "ml_analysis" / "tools" / "export_position_closed_ndjson_v1.py"
    s_build_ds = repo_root / "ml_analysis" / "tools" / "build_dataset_from_inputs_outcomes_v2.py"
    s_tune = repo_root / "ml_analysis" / "tools" / "tune_conf_score_weights_v1.py"

    for p in (s_export_inputs, s_export_closed, s_build_ds, s_tune):
        if not p.exists():
            status["error"] = f"missing script: {p}"
            _json_dump(paths.status_json, status)
            raise SystemExit(status["error"])

    try:
        # Step 1: export inputs
        t0 = time.time()
        cmd = [sys.executable, str(s_export_inputs),
               "--archive-dir", archive_dir,
               "--start-ts-ms", str(start_ms),
               "--end-ts-ms", str(now_ms),
               "--out", paths.inputs_ndjson]
        rc, out, err = _run(cmd, timeout_s=min(900, watchdog_s))
        status["steps"]["export_inputs"] = {"rc": rc, "stdout": out[-4000:], "stderr": err[-4000:], "sec": round(time.time()-t0, 3)}
        if rc != 0:
            raise RuntimeError(f"export_inputs failed rc={rc}")

        # Step 2: export closed from Postgres
        t0 = time.time()
        cmd = [sys.executable, str(s_export_closed),
               "--dsn", trades_dsn,
               "--start-ts-ms", str(start_ms),
               "--end-ts-ms", str(now_ms),
               "--out", paths.closed_ndjson]
        rc, out, err = _run(cmd, timeout_s=min(900, watchdog_s))
        status["steps"]["export_closed"] = {"rc": rc, "stdout": out[-4000:], "stderr": err[-4000:], "sec": round(time.time()-t0, 3)}
        if rc != 0:
            raise RuntimeError(f"export_closed failed rc={rc}")

        # Step 3: build dataset parquet
        t0 = time.time()
        cmd = [sys.executable, str(s_build_ds),
               "--inputs", paths.inputs_ndjson,
               "--closed", paths.closed_ndjson,
               "--out", paths.dataset_parquet]
        rc, out, err = _run(cmd, timeout_s=min(1200, watchdog_s))
        status["steps"]["build_dataset"] = {"rc": rc, "stdout": out[-4000:], "stderr": err[-4000:], "sec": round(time.time()-t0, 3)}
        if rc != 0:
            raise RuntimeError(f"build_dataset failed rc={rc}")

        ds_summary_path = paths.dataset_parquet + ".json"
        ds_summary = _json_load(ds_summary_path) if os.path.exists(ds_summary_path) else {}
        joined_rows = int(ds_summary.get("joined_rows", 0) or 0)
        pos_rate = float(ds_summary.get("pos_rate", 0.0) or 0.0)

        status["dataset"] = {"summary": ds_summary, "joined_rows": joined_rows, "pos_rate": pos_rate}

        _dyn_update({
            "conf_score_tuning_last_dataset_joined_rows": joined_rows,
            "conf_score_tuning_last_pos_rate": pos_rate,
        })

        if joined_rows < min_joined:
            logger.warning(f"joined_rows too low ({joined_rows} < {min_joined}); bypassing tuning step")
            _json_dump(paths.tuning_json, {"bypassed_due_to_low_rows": True})
        else:
            # Step 4: tune weights
            t0 = time.time()
            cmd = [sys.executable, str(s_tune),
                   "--parquet", paths.dataset_parquet,
                   "--out-json", paths.tuning_json]
            rc, out, err = _run(cmd, timeout_s=min(1200, watchdog_s))
            status["steps"]["tune"] = {"rc": rc, "stdout": out[-4000:], "stderr": err[-4000:], "sec": round(time.time()-t0, 3)}
            if rc != 0:
                raise RuntimeError(f"tune failed rc={rc}")

        tuning = _json_load(paths.tuning_json)
        if not isinstance(tuning, dict) or not tuning:
            raise RuntimeError("tuning json is empty or invalid")

        status["tuning"] = {
            "keys": sorted(list(tuning.keys()))[:50],
            "regimes": sorted(list((tuning.get("by_regime") or {}).keys())) if isinstance(tuning.get("by_regime"), dict) else [],
        }

        # Step 5: publish to Redis (optional)
        published = 0
        if apply:
            payload = json.dumps(tuning, ensure_ascii=False)
            for sym in symbols:
                key = f"config:orderflow:{sym}"
                try:
                    prev = r.hget(key, field)
                    if prev:
                        r.hset(key, backup_field, prev)
                    r.hset(key, mapping={
                        field: payload,
                        "conf_score_weight_tuning_generated_at_ms": str(now_ms),
                        "conf_score_weight_tuning_run_id": run_id,
                    })
                    published += 1
                except Exception as exc:
                    logger.warning("publish failed symbol=%s err=%s", sym, exc)

        status["published"] = {"requested": int(apply), "count": published, "symbols": symbols[:]}

        # Step 6: update latest link + status
        try:
            latest = Path(paths.latest_link)
            if latest.is_symlink() or latest.exists():
                latest.unlink()
            latest.symlink_to(Path(paths.run_dir))
        except Exception:
            pass

        status["ok"] = 1

        _dyn_update({
            "conf_score_tuning_last_ok": 1,
            "conf_score_tuning_last_published": published,
        })

    except Exception as exc:  # noqa: BLE001
        status["error"] = str(exc)
        _dyn_update({
            "conf_score_tuning_last_ok": 0,
            "conf_score_tuning_last_error": str(exc)[:200],
        })
        logger.exception("bundle failed")
        raise

    finally:
        status["wall_sec"] = round(time.time() - start_wall, 3)
        _json_dump(paths.status_json, status)

        # watchdog enforcement
        if time.time() - start_wall > watchdog_s:
            _dyn_update({"conf_score_tuning_last_error": "watchdog exceeded"})


if __name__ == "__main__":
    main()
