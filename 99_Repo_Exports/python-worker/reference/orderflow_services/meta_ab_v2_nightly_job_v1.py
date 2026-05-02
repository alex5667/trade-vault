from __future__ import annotations
"""
Stage4: Meta AB-winner v2 nightly oneshot job.

Responsibilities (minimal, feature-flagged):
- Load dataset parquet (meta_inputs_outcomes_v2.parquet)
- Run tools.meta_ab_winner_evaluator_v2.evaluate_v2()
- Recommend next challenger share (recommend_next_share), respecting freeze max_ab_share
- Write JSON report atomically
- Optional: export compact row into Timescale/Postgres
- Optional: apply meta_ab_share into Redis cfg2 for selected symbols

Exit codes:
  0 ok / skipped (disabled)
  2 missing inputs (dataset/models)
  3 evaluation failed
"""

from utils.time_utils import get_ny_time_millis

import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

try:
    from services.orderflow.meta_ab_v2_policy_guardrail_v1 import decide_meta_ab_v2_policy  # type: ignore
except Exception:  # pragma: no cover
    from meta_ab_v2_policy_guardrail_v1 import decide_meta_ab_v2_policy  # type: ignore

def _now_ms() -> int:
    return get_ny_time_millis()


def _env_bool(name: str, default: bool = False) -> bool:
    v = str(os.getenv(name, "1" if default else "0")).strip().lower()
    return v in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, str(default))).strip())
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        v = os.getenv(name)
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[{ts}] meta_ab_v2_nightly_job: {msg}", flush=True)


def _safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _atomic_write_json(path: str, obj: dict) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _file_age_hours(path: str) -> Optional[float]:
    try:
        st = os.stat(path)
        return max(0.0, (time.time() - float(st.st_mtime)) / 3600.0)
    except Exception:
        return None


def _parse_symbols() -> list[str]:
    # Priority: META_AB_SYMBOLS > CANARY_SYMBOLS > empty
    raw = (os.getenv("META_AB_SYMBOLS", "") or "").strip()
    if not raw:
        raw = (os.getenv("CANARY_SYMBOLS", "") or "").strip()
    syms = []
    for s in raw.split(","):
        s = s.strip().upper()
        if s:
            syms.append(s)
    # de-dup
    out = []
    seen = set()
    for s in syms:
        if s not in seen:
            out.append(s)
            seen.add(s)
    return out


def _read_freeze_max_share() -> Optional[float]:
    # best-effort: integrate with your Stage4 freeze file
    try:
        from core.meta_freeze_file import get_meta_freeze_state  # type: ignore

        st = get_meta_freeze_state()
        # st may be dataclass (MetaFreezeState) or dict depending on repo version
        v = None
        if isinstance(st, dict):
            v = st.get("max_ab_share")
        else:
            v = getattr(st, "max_ab_share", None)
        if v is None:
            return None
        fv = float(v)
        if fv != fv:
            return None
        return max(0.0, min(1.0, fv))
    except Exception:
        return None


def _policy_env_overrides(cfg) -> dict[str, Any]:
    """
    Policy guardrail configuration.
    Fail-closed by default: if uncertain -> HOLD (no share change).
    """
    # Defaults from evaluator cfg when available
    require_ci = bool(_env_int("META_AB_REQUIRE_CI_POSITIVE", 1))
    return {
        "enabled": _env_bool("META_AB_POLICY_ENABLED", True),
        "fail_closed": _env_bool("META_AB_POLICY_FAIL_CLOSED", True),
        "allow_decrease": _env_bool("META_AB_POLICY_ALLOW_DECREASE", True),
        "require_winner_challenger_for_increase": _env_bool("META_AB_POLICY_REQUIRE_WINNER_CHALLENGER", True),
        "require_ci_positive_for_increase": _env_bool("META_AB_POLICY_REQUIRE_CI_POSITIVE", require_ci),
        "min_n_eligible": _env_int("META_AB_POLICY_MIN_N_ELIGIBLE", getattr(cfg, "min_n", 1000)),
        "min_delta_exp_r": _env_float("META_AB_POLICY_MIN_DELTA_EXPR", getattr(cfg, "min_delta_exp_r", 0.002)),
        "max_delta_tail": _env_float("META_AB_POLICY_MAX_DELTA_TAIL", getattr(cfg, "tail_slack", 0.01)),
        "max_step": _env_float("META_AB_POLICY_MAX_STEP", getattr(cfg, "ramp_step", 0.05)),
        "max_share": _env_float("META_AB_POLICY_MAX_SHARE", getattr(cfg, "max_share", 0.50)),
    }


def _redis_connect():
    import redis  # type: ignore
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    return redis.Redis.from_url(redis_url, decode_responses=True)


def _read_current_share_from_redis(symbols: list[str], prefix: str, field: str) -> Optional[float]:
    if not symbols:
        return None
    try:
        r = _redis_connect()
        for sym in symbols:
            k = f"{prefix}{sym}"
            v = r.hget(k, field)
            if v is None:
                continue
            fv = float(v)
            if fv == fv:
                return max(0.0, min(1.0, fv))
    except Exception:
        return None
    return None


def _apply_share_to_redis(symbols: list[str], share_next: float, winner: str, report_compact: dict) -> None:
    if not symbols:
        _log("apply enabled but META_AB_SYMBOLS/CANARY_SYMBOLS empty; skipping apply")
        return

    prefix = os.getenv("CFG_HASH_PREFIX", "config:orderflow:")
    share_field = os.getenv("META_AB_SHARE_FIELD", "meta_ab_share")
    winner_field = os.getenv("META_AB_WINNER_FIELD", "meta_ab_winner_v2")
    ts_field = os.getenv("META_AB_TS_FIELD", "meta_ab_v2_ts_ms")

    notify_stream = os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram")

    r = _redis_connect()
    pipe = r.pipeline()

    for sym in symbols:
        k = f"{prefix}{sym}"
        pipe.hset(k, share_field, f"{share_next:.6f}")
        pipe.hset(k, winner_field, str(winner))
        pipe.hset(k, ts_field, str(_now_ms()))
    pipe.execute()

    # best-effort notification (single message)
    try:
        msg = {
            "kind": "meta_ab_winner_v2",
            "ts_ms": str(_now_ms()),
            "symbols": ",".join(symbols),
            "winner": str(winner),
            "share_next": f"{share_next:.6f}",
            "summary": json.dumps(report_compact, ensure_ascii=False),
        }
        r.xadd(notify_stream, msg, maxlen=200000, approximate=True)
    except Exception:
        pass


@dataclass
class TimescaleConfig:
    enabled: bool
    dsn: str
    table: str
    auto_ddl: bool


def _try_timescale_insert(cfg: TimescaleConfig, report: dict) -> None:
    if not cfg.enabled:
        return
    dsn = (cfg.dsn or "").strip()
    if not dsn:
        _log("Timescale export enabled but PG_DSN empty; skipping")
        return

    table = (cfg.table or "").strip()
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", table):
        _log(f"Timescale export skipped: invalid table name '{cfg.table}'")
        return

    conn = None
    try:
        try:
            import psycopg2  # type: ignore
            conn = psycopg2.connect(dsn)
            conn.autocommit = True
            cur = conn.cursor()
        except Exception:
            import psycopg  # type: ignore
            conn = psycopg.connect(dsn, autocommit=True)
            cur = conn.cursor()

        if cfg.auto_ddl:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table} (
                  ts TIMESTAMPTZ NOT NULL,
                  ts_ms BIGINT NOT NULL,
                  run_id TEXT NOT NULL,
                  winner TEXT NOT NULL,
                  reason TEXT NOT NULL,
                  p_min DOUBLE PRECISION NOT NULL,
                  n_total INTEGER NOT NULL,
                  n_eligible INTEGER NOT NULL,
                  share_current DOUBLE PRECISION NOT NULL,
                  share_next DOUBLE PRECISION NOT NULL,
                  action TEXT NOT NULL,
                  delta_exp_r DOUBLE PRECISION NOT NULL,
                  delta_tail_rate DOUBLE PRECISION NOT NULL,
                  champion_model TEXT NOT NULL,
                  challenger_model TEXT NOT NULL,
                  report_json JSONB NOT NULL
                );
                """
            )
            try:
                cur.execute(f"SELECT create_hypertable('{table}', 'ts', if_not_exists => TRUE);")
            except Exception:
                pass

        ts_ms = int(report.get("ts_ms") or 0)
        run_id = str(report.get("run_id") or datetime.now(timezone.utc).isoformat(timespec="seconds"))

        winner = str(report.get("winner") or "tie")
        reason = str(report.get("reason") or "")

        cfg_obj = report.get("config") or {}
        p_min = float(cfg_obj.get("p_min") or report.get("p_min") or 0.0)

        counts = report.get("counts") or {}
        n_total = int(counts.get("n_total") or counts.get("n_total_rows") or 0)
        n_eligible = int(counts.get("n_eligible") or counts.get("eligible") or 0)

        ramp = report.get("ramp") or {}
        share_current = float(ramp.get("share_current") or 0.0)
        share_next = float(ramp.get("share_next") or share_current)
        action = str(ramp.get("action") or "hold")

        delta = report.get("delta") or {}
        delta_exp_r = float(delta.get("exp_r_per_candidate") or 0.0)
        delta_tail = float(delta.get("tail_rate_per_candidate") or 0.0)

        champ = str(report.get("champion_model") or "")
        chall = str(report.get("challenger_model") or "")

        cur.execute(
            f"""
            INSERT INTO {table} (
              ts, ts_ms, run_id, winner, reason, p_min, n_total, n_eligible,
              share_current, share_next, action,
              delta_exp_r, delta_tail_rate,
              champion_model, challenger_model, report_json
            ) VALUES (
              %s,%s,%s,%s,%s,%s,%s,%s,
              %s,%s,%s
              %s,%s,
              %s,%s,%s
            );
            """
            (
                datetime.now(timezone.utc),
                ts_ms,
                run_id,
                winner,
                reason,
                p_min,
                n_total,
                n_eligible,
                share_current,
                share_next,
                action,
                delta_exp_r,
                delta_tail,
                champ,
                chall,
                json.dumps(report, ensure_ascii=False),
            )
        )
        _log(f"Timescale inserted into {table}: winner={winner} share_next={share_next:.4f}")

    except Exception as e:
        _log(f"Timescale export failed: {type(e).__name__}: {e}")
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def _compact(rep: dict) -> dict:
    # keep compact and stable (for notify + audit)
    pol = rep.get("policy") or {}
    return {
        "winner": rep.get("winner"),
        "reason": rep.get("reason"),
        "counts": rep.get("counts"),
        "delta": rep.get("delta"),
        "ci": rep.get("ci"),
        "strata_top": rep.get("strata_top"),
        "policy": {
            "blocked": bool(pol.get("blocked", False)),
            "allow_apply": bool(pol.get("allow_apply", False)),
            "blocked_reasons": pol.get("blocked_reasons", []),
            "action_raw": pol.get("action_raw"),
            "action_final": pol.get("action_final"),
            "share_next_raw": pol.get("share_next_raw"),
            "share_next_final": pol.get("share_next_final"),
        }
    }


def main() -> int:
    if not _env_bool("ENABLE_META_AB_V2_NIGHTLY", False):
        _log("disabled (ENABLE_META_AB_V2_NIGHTLY!=1); exit")
        return 0

    dataset_parquet = os.getenv(
        "META_AB_DATASET_PARQUET",
        "/var/lib/trade/of_reports/datasets/meta_inputs_outcomes_v2.parquet",
    )
    champion_model = (os.getenv("META_MODEL_PATH", "") or "").strip()
    challenger_model = (os.getenv("META_MODEL_CHALLENGER_PATH", "") or "").strip()

    if not os.path.exists(dataset_parquet):
        _log(f"dataset parquet missing: {dataset_parquet}")
        return 2
    if not champion_model or not os.path.exists(champion_model):
        _log(f"champion model missing: META_MODEL_PATH={champion_model}")
        return 2
    if not challenger_model or not os.path.exists(challenger_model):
        _log(f"challenger model missing: META_MODEL_CHALLENGER_PATH={challenger_model}")
        return 2

    max_age_h = float(os.getenv("META_AB_V2_DATASET_MAX_AGE_H", "36") or 36)
    age = _file_age_hours(dataset_parquet)
    if age is not None and age > max_age_h:
        _log(f"dataset too old: age_h={age:.1f} > max_age_h={max_age_h}; skipping")
        return 0

    out_json = os.getenv(
        "META_AB_V2_OUT_JSON",
        "/var/lib/trade/of_reports/out/meta_ab_v2/ab_v2_report.json",
    )
    _safe_mkdir(os.path.dirname(out_json) or ".")

    # share source: Redis (preferred) or ENV fallback
    symbols = _parse_symbols()
    cfg_prefix = os.getenv("CFG_HASH_PREFIX", "config:orderflow:")
    share_field = os.getenv("META_AB_SHARE_FIELD", "meta_ab_share")

    share_current_env = _env_float("META_AB_CHALLENGER_SHARE", 0.0)
    share_current = share_current_env
    if _env_bool("META_AB_SHARE_FROM_REDIS", True):
        v = _read_current_share_from_redis(symbols, cfg_prefix, share_field)
        if v is not None:
            share_current = v

    # evaluator config (mostly via ENV)
    p_min = _env_float("META_P_MIN", 0.55)

    try:
        from tools import meta_ab_winner_evaluator_v2 as abv2  # type: ignore

        cfg = abv2.V2Config(
            p_min=p_min,
            label_col=os.getenv("META_AB_LABEL_COL", "y"),
            r_col=os.getenv("META_AB_R_COL", "r_mult"),
            ok_col=os.getenv("META_AB_OK_COL", "ok"),
            min_n=_env_int("META_AB_MIN_ELIGIBLE", 1000),
            min_delta_exp_r=_env_float("META_AB_MIN_DELTA_EXPR", 0.002),
            tail_r=_env_float("META_AB_TAIL_R", -1.0),
            tail_slack=_env_float("META_AB_TAIL_SLACK", 0.01),
            bootstrap=_env_int("META_AB_BOOTSTRAP", 1),
            boot_n=_env_int("META_AB_BOOT_N", 400),
            boot_alpha=_env_float("META_AB_BOOT_ALPHA", 0.10),
            boot_seed=_env_int("META_AB_BOOT_SEED", 1337),
            require_ci_positive=_env_int("META_AB_REQUIRE_CI_POSITIVE", 1),
            strata_cols=tuple((os.getenv("META_AB_STRATA_COLS", "symbol") or "symbol").split(",")),
            strata_topk=_env_int("META_AB_STRATA_TOPK", 10),
            current_share=float(share_current),
            ramp_step=_env_float("META_AB_RAMP_STEP", 0.05),
            max_share=_env_float("META_AB_MAX_SHARE", 0.50),
        )

        df = abv2.load_dataset(dataset_parquet, p_min)
        champ = abv2._load_meta_model(champion_model)
        chall = abv2._load_meta_model(challenger_model)

        rep: dict[str, Any] = abv2.evaluate_v2(df, champ, chall, cfg)

        freeze_max = _read_freeze_max_share()
        share_next, action = abv2.recommend_next_share(
            str(rep.get("winner") or "tie"),
            float(share_current),
            cfg,
            freeze_max,
        )

        share_next_raw = float(share_next)
        action_raw = str(action)

        # Policy guardrail (fail-closed): may override ramp/apply -> HOLD
        dec = decide_meta_ab_v2_policy(
            rep=rep,
            cfg=cfg,
            share_current=float(share_current),
            share_next_raw=share_next_raw,
            action_raw=action_raw,
            freeze_max_share=freeze_max,
            env_overrides=_policy_env_overrides(cfg),
        )
        share_next = float(dec.share_next_final)
        action = str(dec.action_final)

        rep["policy"] = {
            "checked": True,
            "blocked": bool(dec.blocked),
            "allow_apply": bool(dec.allow_apply),
            "blocked_reasons": list(dec.reasons),
            "share_next_raw": share_next_raw,
            "action_raw": action_raw,
            "share_next_final": float(share_next),
            "action_final": str(action),
        }

        rep["ts_ms"] = int(rep.get("ts_ms") or _now_ms())
        rep["run_id"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        rep["champion_model"] = champion_model
        rep["challenger_model"] = challenger_model
        rep["ramp"] = {
            "share_current": float(share_current),
            "share_next": float(share_next),
            "action": str(action),
            "freeze_max_share": freeze_max,
        }

        _atomic_write_json(out_json, rep)
        _log(f"report written: {out_json} winner={rep.get('winner')} share_next={share_next:.4f} action={action}")

        # Timescale export (optional)
        ts_cfg = TimescaleConfig(
            enabled=_env_bool("ENABLE_META_AB_V2_TS_EXPORT", True),
            dsn=os.getenv("PG_DSN", ""),
            table=os.getenv("META_AB_V2_TS_TABLE", "meta_ab_eval_v2"),
            auto_ddl=_env_bool("META_AB_V2_TS_AUTO_DDL", True),
        )
        _try_timescale_insert(ts_cfg, rep)

        # Apply to Redis cfg2 (optional)
        if _env_bool("META_AB_V2_APPLY", False):
            allow_apply = bool((rep.get("policy") or {}).get("allow_apply", False))
            if allow_apply:
                _apply_share_to_redis(symbols, float(share_next), str(rep.get("winner") or "tie"), _compact(rep))
            else:
                _log(f"apply blocked (policy): reasons={(rep.get('policy') or {}).get('blocked_reasons', [])}")

        return 0

    except Exception as e:
        _log(f"evaluation failed: {type(e).__name__}: {e}")
        # best-effort: write failure report (so exporter shows something)
        fail = {
            "ts_ms": _now_ms(),
            "run_id": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "winner": "tie",
            "reason": f"{type(e).__name__}: {e}",
            "counts": {"n_total": 0, "n_eligible": 0},
            "delta": {"exp_r_per_candidate": 0.0, "tail_rate_per_candidate": 0.0},
            "ramp": {"share_current": float(share_current_env), "share_next": float(share_current_env), "action": "hold"},
            "champion_model": champion_model,
            "challenger_model": challenger_model,
        }
        try:
            _atomic_write_json(out_json, fail)
        except Exception:
            pass
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
