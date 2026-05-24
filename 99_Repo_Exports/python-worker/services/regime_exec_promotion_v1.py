"""Phase C.3 (P1): nightly promotion service для regime_exec.

Pipeline:
  1) Читает rolling 14d bucket-метрики из view `strategy_bucket_outcomes_14d`.
  2) Применяет gates (n, ev_r_after_costs, bootstrap CI, drawdown proxy).
  3) Каждый прошедший bucket получает решение `enforce_proposed`,
     каждый прошедший SAFETY gate но без EV-edge → `shadow`,
     иначе → `skip`.
  4) Решения пишутся в hypertable `strategy_bucket_metrics` (журнал).
  5) Подмножество с decision=enforce_proposed формирует snapshot
     Redis key `autocal:regime_exec:state` (HMAC-подписанный).
  6) Engine читает Redis в runtime — Phase A/B инфраструктура уже работает.

Безопасность:
  - SHADOW default: PROMOTION_ENFORCE=0 → Redis snapshot не пишется.
  - HMAC обязателен в ENFORCE: если REGIME_EXEC_AUTOCAL_HMAC_SECRET пуст,
    runner отказывается писать в Redis (защита от disabled-HMAC overrides).
  - Все промоушены логируются в Prometheus.
"""

from __future__ import annotations

import dataclasses
import hashlib
import hmac
import json
import logging
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Iterable

logger = logging.getLogger("regime_exec_promotion")


# ─────────────────────────────── гейты / пороги ─────────────────────────────────
@dataclass
class PromotionGates:
    min_n: int = 300
    min_ev_r: float = 0.05
    min_avg_r: float = 0.0
    max_timeout_rate: float = 0.70
    z_alpha: float = 1.96  # 95% normal-approx CI

    @classmethod
    def from_env(cls) -> "PromotionGates":
        def _f(name: str, default: float) -> float:
            try:
                return float(os.getenv(name, str(default)))
            except (TypeError, ValueError):
                return default

        def _i(name: str, default: int) -> int:
            try:
                return int(os.getenv(name, str(default)))
            except (TypeError, ValueError):
                return default

        return cls(
            min_n=_i("PROMOTION_MIN_N", 300),
            min_ev_r=_f("PROMOTION_MIN_EV_R", 0.05),
            min_avg_r=_f("PROMOTION_MIN_AVG_R", 0.0),
            max_timeout_rate=_f("PROMOTION_MAX_TIMEOUT_RATE", 0.70),
            z_alpha=_f("PROMOTION_Z_ALPHA", 1.96),
        )


# ──────────────────────────── bucket / decision shape ───────────────────────────
@dataclass
class BucketRow:
    symbol: str
    regime_label: str       # vol_label или composite — зависит от как заполняется в outcomes
    scenario: str
    direction: str
    n: int
    win_rate: float
    avg_r: float
    ev_r_after_costs: float
    mfe_r_p50: float | None = None
    mfe_r_p90: float | None = None
    mae_r_p50: float | None = None
    mae_r_p90: float | None = None
    timeout_rate: float = 0.0


@dataclass
class BucketDecision:
    bucket_key: str            # "GLOBAL|<regime>|<scenario>" или "{SYMBOL}|{regime}|{scenario}"
    decision: str              # enforce_proposed | shadow | skip
    reason: str
    n: int
    ev_r: float
    avg_r: float
    ci_low: float
    ci_high: float
    proposed_policy: dict[str, Any] = dataclasses.field(default_factory=dict)


# ────────────────────────────── статистика ──────────────────────────────────────
def _bootstrap_ci(values: list[float], z: float = 1.96) -> tuple[float, float]:
    """Normal-approx CI для среднего. Bootstrap-real можно подключить позже —
    для гейта enough/insufficient этого достаточно (берёт sd/sqrt(n) × z)."""
    n = len(values)
    if n < 2:
        return float("nan"), float("nan")
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values) / (n - 1)
    sd = math.sqrt(var)
    half = z * sd / math.sqrt(n)
    return mean - half, mean + half


# ──────────────────────────── promotion core ────────────────────────────────────
def _bucket_key_for_row(row: BucketRow) -> str:
    """Engine ожидает: "{SYMBOL}|{VOL}|{TREND}" с fallback к "GLOBAL|...".

    Здесь мы пишем GLOBAL-уровневые решения (per-symbol мало данных в общем
    случае). Если promoted bucket имеет n*per_symbol достаточно — следующая
    версия может писать per-symbol overrides.
    """
    return f"GLOBAL|{row.regime_label}|{row.scenario}"


def evaluate_bucket(row: BucketRow, gates: PromotionGates) -> BucketDecision:
    """Применяет гейты и формирует decision для одного bucket-а.

    Не делает I/O. Pure function — легко тестируется.
    """
    bucket_key = _bucket_key_for_row(row)

    # SAFETY: too few samples → не трогаем (skip).
    if row.n < gates.min_n:
        return BucketDecision(
            bucket_key=bucket_key, decision="skip",
            reason=f"n={row.n}<{gates.min_n}",
            n=row.n, ev_r=row.ev_r_after_costs, avg_r=row.avg_r,
            ci_low=float("nan"), ci_high=float("nan"),
        )

    # Грубая CI (использует только агрегаты — sd не хранится). Используем
    # эвристику: sd ≈ |avg_r - ev_r| как proxy дисперсии rewards.
    # Promotion-runner перешлёт точные sd, когда выкатим bootstrap на источнике.
    sd_proxy = max(abs(row.avg_r - row.ev_r_after_costs), 0.05)
    half = gates.z_alpha * sd_proxy / math.sqrt(row.n)
    ci_low = row.ev_r_after_costs - half
    ci_high = row.ev_r_after_costs + half

    if row.timeout_rate > gates.max_timeout_rate:
        return BucketDecision(
            bucket_key=bucket_key, decision="skip",
            reason=f"timeout_rate={row.timeout_rate:.2f}>{gates.max_timeout_rate:.2f}",
            n=row.n, ev_r=row.ev_r_after_costs, avg_r=row.avg_r,
            ci_low=ci_low, ci_high=ci_high,
        )

    if row.avg_r < gates.min_avg_r:
        return BucketDecision(
            bucket_key=bucket_key, decision="skip",
            reason=f"avg_r={row.avg_r:.3f}<{gates.min_avg_r:.3f}",
            n=row.n, ev_r=row.ev_r_after_costs, avg_r=row.avg_r,
            ci_low=ci_low, ci_high=ci_high,
        )

    if row.ev_r_after_costs < gates.min_ev_r:
        return BucketDecision(
            bucket_key=bucket_key, decision="shadow",
            reason=f"ev_r={row.ev_r_after_costs:.3f}<{gates.min_ev_r:.3f}",
            n=row.n, ev_r=row.ev_r_after_costs, avg_r=row.avg_r,
            ci_low=ci_low, ci_high=ci_high,
        )

    # Главный гейт: нижняя граница CI должна быть > 0 (значимое preimage).
    if ci_low <= 0:
        return BucketDecision(
            bucket_key=bucket_key, decision="shadow",
            reason=f"ci_low={ci_low:.3f}<=0",
            n=row.n, ev_r=row.ev_r_after_costs, avg_r=row.avg_r,
            ci_low=ci_low, ci_high=ci_high,
        )

    # Прошли все гейты — предлагаем enforce. Конкретная policy задаётся
    # маппером — здесь это TP1=1.0R по умолчанию, profile подбирается по
    # сценарию (rocket_v1 для trending, range_protective для range).
    policy = _propose_policy(row)
    return BucketDecision(
        bucket_key=bucket_key, decision="enforce_proposed",
        reason="all_gates_passed",
        n=row.n, ev_r=row.ev_r_after_costs, avg_r=row.avg_r,
        ci_low=ci_low, ci_high=ci_high,
        proposed_policy=policy,
    )


def _propose_policy(row: BucketRow) -> dict[str, Any]:
    """Маппер bucket → execution policy.

    Базовая логика — без ML, на хорошо известных сценариях:
      - trending bucket с MFE_p50 > 1.0R → tp1_target_r=1.5, rocket_v1
      - range bucket с маленьким MFE → tp1=0.3, range_protective
      - всё прочее → консервативные defaults (engine fallback)
    """
    scen = row.scenario.lower()
    mfe_p50 = row.mfe_r_p50 or 0.0
    if "trend" in scen or "shock" in scen:
        if mfe_p50 >= 1.0:
            return {
                "tp1_target_r": 1.5,
                "tp_ratios": [0.40, 0.30, 0.30],
                "trail_profile": "rocket_v1",
                "atr_mult": 1.0,
                "reason": "trending+ev_r_edge",
            }
        return {
            "tp1_target_r": 1.0,
            "tp_ratios": [0.50, 0.30, 0.20],
            "trail_profile": "rocket_v1",
            "reason": "trending+modest_mfe",
        }
    if "range" in scen:
        return {
            "tp1_target_r": 0.3,
            "tp_ratios": [0.70, 0.30],
            "trail_profile": "range_protective",
            "reason": "range+fast_scalp",
        }
    return {
        "tp1_target_r": 1.0,
        "trail_profile": "protective_only",
        "reason": "fallback_conservative",
    }


# ────────────────────────────── snapshot builder ────────────────────────────────
def build_snapshot(
    decisions: Iterable[BucketDecision],
    *,
    hmac_secret: str = "",
    ts_ms: int | None = None,
) -> dict[str, Any]:
    """Собирает payload для Redis key autocal:regime_exec:state.

    Только enforce_proposed buckets попадают в snapshot (engine применяет их).
    Если hmac_secret пуст, sig не добавляется (engine fail-open: примет без проверки).
    """
    ts = ts_ms if ts_ms is not None else int(time.time() * 1000)
    buckets: dict[str, dict[str, Any]] = {}
    for d in decisions:
        if d.decision != "enforce_proposed":
            continue
        buckets[d.bucket_key] = d.proposed_policy

    payload: dict[str, Any] = {"ts_ms": ts, "buckets": buckets}

    if hmac_secret:
        canon = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        sig = hmac.new(hmac_secret.encode(), canon, hashlib.sha256).hexdigest()
        payload["sig"] = sig
    return payload


# ────────────────────────── runner glue (I/O isolated) ─────────────────────────
class PromotionRunner:
    """Сборщик пайплайна. I/O вынесен в callbacks для тестируемости."""

    def __init__(
        self,
        *,
        fetch_rows,
        write_decision,
        publish_snapshot,
        gates: PromotionGates | None = None,
        enforce: bool = False,
        hmac_secret: str = "",
    ):
        self.fetch_rows = fetch_rows                  # () -> Iterable[BucketRow]
        self.write_decision = write_decision          # (BucketDecision) -> None
        self.publish_snapshot = publish_snapshot      # (dict) -> None
        self.gates = gates or PromotionGates.from_env()
        self.enforce = enforce
        self.hmac_secret = hmac_secret

    def run_once(self) -> list[BucketDecision]:
        decisions: list[BucketDecision] = []
        for row in self.fetch_rows():
            d = evaluate_bucket(row, self.gates)
            try:
                self.write_decision(d)
            except Exception as e:
                logger.warning("write_decision failed for %s: %s", d.bucket_key, e)
            decisions.append(d)

        snapshot = build_snapshot(decisions, hmac_secret=self.hmac_secret)
        if not self.enforce:
            logger.info(
                "PROMOTION SHADOW: %d enforce_proposed buckets (not published, ENFORCE=0)",
                len(snapshot["buckets"]),
            )
            return decisions

        if not self.hmac_secret:
            logger.error("PROMOTION refusing to publish: HMAC secret empty in enforce mode")
            return decisions

        try:
            self.publish_snapshot(snapshot)
            logger.info(
                "✅ PROMOTION published %d enforce_proposed buckets",
                len(snapshot["buckets"]),
            )
        except Exception as e:
            logger.error("publish_snapshot failed: %s", e)
        return decisions
