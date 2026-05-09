from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Meta Guardrails (P11).

Goal:
  - Check nightly dataset for "hard" breakages:
    1. Schema mismatch (model.schema != dataset.schema)
    2. Global missingness > 5% (default)
    3. Critical feature missingness > 20% (default)
  - If triggered -> set meta_guard_freeze=1 in Redis dynamic cfg.
  - Ramp script (P5) will see this and force SHADOW mode / share=0.

Usage:
  python meta_guardrails_v1.py \
    --model-json /path/to/meta_model.json \
    --dataset-parquet /path/to/nightly.parquet \
    --apply=1
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import redis

# P16: DQ-aware latch from report
try:
    from tools.meta_dq_rules_v1 import dq_freeze_decision
except Exception:
    dq_freeze_decision = None


def _try_load_cfg2(redis_url: str) -> dict[str, Any]:
    if not redis_url:
        return {}
    try:
        import redis  # type: ignore
        r = redis.Redis.from_url(redis_url, decode_responses=True)
        d = r.hgetall("settings:dynamic_cfg") or {}
        return {k: v for k, v in d.items() if k}
    except Exception:
        return {}


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _load_json(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _get_redis_client(url: str) -> redis.Redis:
    return redis.Redis.from_url(url, decode_responses=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    # Input
    ap.add_argument("--model-json", required=True, help="Path to meta model JSON (to check schema/features)")
    ap.add_argument("--dataset-parquet", required=True, help="Path to nightly parquet dataset")
    ap.add_argument("--fallback-model-json", default="", help="Optional fallback model if primary missing")
    ap.add_argument("--report-json", default="", help="Optional quality report JSON to apply DQ latch")
    ap.add_argument("--notify-stream", default=os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM), help="Redis stream for fallback notification")

    # Thresholds
    ap.add_argument("--max-miss-mean", type=float, default=float(os.getenv("META_GUARD_MAX_MISS_MEAN", "0.05")))
    ap.add_argument("--max-miss-crit", type=float, default=float(os.getenv("META_GUARD_MAX_MISS_CRIT", "0.20")))

    # Critical features (comma-separated or use defaults)
    # Default for v4: qimb_wmean, ofi_ml_norm, mp_mid_bps, obi_dw
    default_crit = "qimb_wmean,ofi_ml_norm,mp_mid_bps,obi_dw"
    default_crit_v6 = "exec_pen,have_need_ratio,book_staleness_ms,last_spread_z,book_rate_z,pressure_sps"
    ap.add_argument("--crit-features", default=os.getenv("META_GUARD_CRIT_FEATURES", default_crit))

    # Schema enforcement
    # We expect the model used in production to match this schema
    ap.add_argument("--expected-schema", default=os.getenv("META_SCHEMA", ""))
    # Also allows overriding required schema check
    ap.add_argument("--require-schema", default=os.getenv("META_SCHEMA_REQUIRED", ""))

    # Redis integration
    ap.add_argument("--apply", type=int, default=int(os.getenv("META_GUARD_APPLY", "0")))
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    ap.add_argument("--dyn-key", default=os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg"))
    ap.add_argument("--freeze-key", default="meta_guard_freeze")
    ap.add_argument("--reason-key", default="meta_guard_reason")

    # Prometheus textfile
    ap.add_argument("--prom-textfile", default=os.getenv("META_GUARD_PROM_TEXTFILE", ""))
    ap.add_argument("--ignore-dq", action="store_true", help="Ignore DQ latch (emergency)")

    args = ap.parse_args()

    # P30: Initialize control variables
    freeze = False
    reason: list[str] = []

    # 1. Load Artefacts
    r_state = _get_redis_client(args.redis_url)
    state_key = f"{args.dyn_key}:model_state"
    last_state = r_state.get(state_key) or "ok"

    model_path = args.model_json
    current_state = "ok"

    if not os.path.exists(model_path):
        current_state = "fallback"
        if args.fallback_model_json and os.path.exists(args.fallback_model_json):
            # Transition to fallback
            if last_state != "fallback":
                print(f"WARNING: Primary model {model_path} missing. Falling back to {args.fallback_model_json}")
                try:
                    msg = (
                        "<b>GUARDRAILS FALLBACK</b>\n"
                        f"Nightly model missing: <code>{model_path}</code>\n"
                        f"Switching to stability: <code>{args.fallback_model_json}</code>"
                    )
                    r_state.xadd(args.notify_stream, {
                        "type": "text",
                        "text": msg,
                        "ts": str(get_ny_time_millis())
                    }, maxlen=200000, approximate=True)
                except Exception as ne:
                    print(f"ERROR: Failed to notify fallback: {ne}")

            model_path = args.fallback_model_json
        else:
            print(f"FATAL: Model JSON not found: {model_path}")
            sys.exit(1)
    else:
        # Transition back to OK (Recovery)
        if last_state == "fallback":
            print(f"INFO: Primary model {model_path} recovered. Switching back from fallback.")
            try:
                msg = (
                    "<b>GUARDRAILS RECOVERED</b>\n"
                    f"Nightly model back: <code>{model_path}</code>\n"
                    "Resuming prioritized monitoring."
                )
                r_state.xadd(args.notify_stream, {
                    "type": "text",
                    "text": msg,
                    "ts": str(get_ny_time_millis())
                }, maxlen=200000, approximate=True)
            except Exception as ne:
                print(f"ERROR: Failed to notify recovery: {ne}")

    # Update state in Redis
    if current_state != last_state:
        try:
            r_state.set(state_key, current_state)
        except Exception as se:
            print(f"ERROR: Failed to update model state in Redis: {se}")

    if not os.path.exists(args.dataset_parquet):
        print(f"FAIL: Dataset not found: {args.dataset_parquet}. Triggering freeze (Fail-Closed).")
        freeze = True
        reason.append("Dataset not found")
        # Go straight to Redis update
    else:
        try:
            model_meta = _load_json(model_path)

            # Identify active features
            features = model_meta.get("features") or model_meta.get("feature_names") or []
            if not features:
                print("WARNING: No features found in model JSON. Skipping feature-based checks.")

            model_schema = model_meta.get("schema", "unknown")
            schema_name = model_meta.get("schema_name", "") or model_schema

            # P29: schema-aware defaults
            if args.crit_features.strip() == default_crit and str(schema_name).startswith('meta_feat_v6'):
                print(f"INFO: Detected v6 schema '{schema_name}', switching default critical features to v6 set.")
                args.crit_features = default_crit_v6

            print(f"INFO: Model Schema: {schema_name}")
            print(f"INFO: Active Critical Features: {args.crit_features}")

            cfg2: dict[str, Any] = _try_load_cfg2(getattr(args, "redis_url", ""))

            # --- P16: DQ latch from quality report ---
            if (not args.ignore_dq) and args.report_json and dq_freeze_decision is not None:
                rp = Path(args.report_json)
                if rp.exists():
                    try:
                        report = json.loads(rp.read_text(encoding="utf-8"))
                        freeze_dq, reason_dq, details_dq = dq_freeze_decision(report, cfg2=cfg2, schema_name=schema_name)
                    except Exception:
                        freeze_dq, reason_dq, details_dq = (False, "dq_eval_error", {})
                    if freeze_dq:
                        os.environ["META_GUARD_REASON_EXTRA"] = json.dumps({"dq": details_dq}, ensure_ascii=False)
                        print(f"DQ Latch Triggered: {reason_dq}")
                        freeze = True
                        reason.append(f"dq_latch:{reason_dq}")

            # 2. Schema Check
            model_schema = model_meta.get("schema", "unknown")
            required_schema = args.require_schema if args.require_schema else args.expected_schema

            if required_schema and model_schema != required_schema:
                freeze = True
                msg = f"Schema mismatch: model={model_schema} required={required_schema}"
                reason.append(msg)
                print(f"FAIL: {msg}")
            else:
                print(f"OK: Schema check passed ({model_schema})")

            # 3. Load Data & Check Missingness
            df = pd.read_parquet(args.dataset_parquet)
            if df.empty:
                print(f"FAIL: Dataset {args.dataset_parquet} is empty.")
                freeze = True
                reason.append("Empty dataset")
                df = pd.DataFrame()

            valid_features = [f for f in features if f in df.columns]
            missing_features = [f for f in features if f not in df.columns]

            if missing_features and "indicators" in df.columns:
                print(f"INFO: {len(missing_features)} features missing from top-level. Attempting to unpack 'indicators'...")
                try:
                    mask = df["indicators"].notna()
                    if mask.any():
                        dicts = df.loc[mask, "indicators"].tolist()
                        df_ind = pd.DataFrame(dicts)
                        df_ind.index = df.index[mask]
                        df = df.join(df_ind, rsuffix="_ind")
                        valid_features = [f for f in features if f in df.columns]
                        missing_features = [f for f in features if f not in df.columns]
                        print(f"INFO: After unpacking, found {len(valid_features)}/{len(features)} features.")
                except Exception as e:
                    print(f"WARNING: Failed to unpack indicators: {e}")

            miss_map = {}
            if valid_features:
                miss_series = df[valid_features].isna().mean()
                miss_map = miss_series.to_dict()
                avg_miss = miss_series.mean()
                print(f"INFO: Avg missing rate: {avg_miss:.4f}")

                if avg_miss > args.max_miss_mean:
                    freeze = True
                    msg = f"High global missing: {avg_miss:.4f} > {args.max_miss_mean}"
                    reason.append(msg)
                    print(f"FAIL: {msg}")

                crit_list = [x.strip() for x in args.crit_features.split(",") if x.strip()]
                for cf in crit_list:
                    if cf in miss_map:
                        val = miss_map[cf]
                        if val > args.max_miss_crit:
                            freeze = True
                            msg = f"Critical feature '{cf}' missing: {val:.4f} > {args.max_miss_crit}"
                            reason.append(msg)
                            print(f"FAIL: {msg}")
                    elif cf in missing_features:
                        freeze = True
                        msg = f"Critical feature '{cf}' not found in dataset columns"
                        reason.append(msg)
                        print(f"FAIL: {msg}")
            else:
                if features:
                    freeze = True
                    msg = "No valid features found in DataFrame columns to check"
                    reason.append(msg)
                    print(f"FAIL: {msg}")
                else:
                    print("WARNING: No features defined in model JSON. Skipping feature-based checks.")

        except Exception as e:
            print(f"FATAL: Guardrails processing error: {e}")
            freeze = True
            reason.append(f"fatal_error:{type(e).__name__}")

    # 4. Action
    final_decision = 1 if freeze else 0
    final_reason = "; ".join(reason) if reason else "ok"

    print(f"DECISION: freeze={final_decision} reason='{final_reason}'")

    # Output to Prom
    if args.prom_textfile:
        try:
            with open(args.prom_textfile + ".tmp", "w") as f:
                f.write("# HELP meta_guard_freeze 1 if guardrails triggered freeze\n")
                f.write("# TYPE meta_guard_freeze gauge\n")
                f.write(f"meta_guard_freeze {final_decision}\n")
                f.write(f"meta_guard_missing_mean {avg_miss if 'avg_miss' in locals() else 0.0}\n")
            os.replace(args.prom_textfile + ".tmp", args.prom_textfile)
        except Exception as e:
            print(f"ERROR: failed to write prom file: {e}")

    # Redis
    if args.apply:
        r = _get_redis_client(args.redis_url)
        # We write to dynamic cfg
        # If freeze=0, we still write it to clear any previous freeze
        # But we might want to ONLY clear if we are sure?
        # Yes, guardrails run nightly, so they authorize the NEXT day.
        # If passed -> freeze=0. If failed -> freeze=1.

        updates = {
            args.freeze_key: str(final_decision),
            args.reason_key: final_reason
        }
        r.hset(args.dyn_key, mapping=updates)
        print(f"APPLIED to Redis: {updates}")

if __name__ == "__main__":
    main()
