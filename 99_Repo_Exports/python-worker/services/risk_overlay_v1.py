"""Phase E.1 (P4): Risk overlay поверх стратегии.

Расширяет существующий PortfolioExposureGate (raw notional sums) тремя
slow-changing рисковыми ограничителями:

  1) portfolio_heat — суммарное "тепло" по открытым позициям. Для каждой
     позиции тепло = текущий drawdown в R-multiple (если позиция в минусе),
     иначе 0. Идея: учитываем уже наколенный риск, а не только потенциальный.
  2) correlated_symbols_exposure — сумма notional на симвoлах одной
     корреляционной группы. Дефолтные группы: BTC_CLUSTER, ETH_CLUSTER,
     ALTS_HIGH_BETA, MEME, OTHER.
  3) consecutive_loss_bucket_limit — N подряд убыточных сделок в одном
     bucket (symbol, regime) → cooldown.

Все три — SHADOW по умолчанию. EntryPolicyGate подключит их позже как
VETO_PORTFOLIO_HEAT / VETO_CORRELATION / VETO_CONSEC_LOSS.

Pure module (без I/O). State (open positions, recent losses) подаётся в
evaluate().
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Iterable


# ──────────────────────────────── correlation map ───────────────────────────────
_DEFAULT_GROUPS: dict[str, set[str]] = {
    "BTC_CLUSTER": {"BTCUSDT", "BTCDOMUSDT"},
    "ETH_CLUSTER": {"ETHUSDT", "ETHBTCUSDT"},
    "ALTS_HIGH_BETA": {
        "SOLUSDT", "AVAXUSDT", "DOTUSDT", "NEARUSDT", "ATOMUSDT",
        "ADAUSDT", "MATICUSDT", "LINKUSDT",
    },
    "MEME": {"PEPEUSDT", "DOGEUSDT", "SHIBUSDT", "WIFUSDT", "BONKUSDT"},
}


def correlation_group_for(symbol: str, groups: dict[str, set[str]] | None = None) -> str:
    g = groups or _DEFAULT_GROUPS
    s = symbol.upper()
    for name, members in g.items():
        if s in members:
            return name
    return "OTHER"


# ─────────────────────────────── input shapes ───────────────────────────────────
@dataclass
class OpenPositionInfo:
    symbol: str
    notional_usd: float
    unrealized_r: float            # R-multiple (negative for losing); 0 для break-even


@dataclass
class RecentTradeOutcome:
    symbol: str
    bucket: str                    # "{symbol}|{regime}|{scenario}" или иной
    r_multiple: float              # < 0 = loss, > 0 = win
    ts_ms: int


@dataclass
class RiskLimits:
    max_portfolio_heat_r: float = 5.0           # |sum negative-R|, например 5R
    max_correlation_group_usd: float = 1_000.0  # сумма notional на группу
    max_consecutive_losses: int = 4             # подряд в одном bucket
    cooldown_lookback_ms: int = 6 * 3600 * 1000  # окно для подсчёта подряд
    enabled: bool = True
    enforce: bool = False           # SHADOW default

    @classmethod
    def from_env(cls) -> "RiskLimits":
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

        def _b(name: str, default: bool) -> bool:
            v = os.getenv(name)
            if v is None:
                return default
            return v.strip().lower() in {"1", "true", "yes", "on"}

        return cls(
            max_portfolio_heat_r=_f("RISK_OVERLAY_MAX_HEAT_R", 5.0),
            max_correlation_group_usd=_f("RISK_OVERLAY_MAX_GROUP_USD", 1000.0),
            max_consecutive_losses=_i("RISK_OVERLAY_MAX_CONSEC_LOSSES", 4),
            cooldown_lookback_ms=_i("RISK_OVERLAY_LOOKBACK_MS", 6 * 3600 * 1000),
            enabled=_b("RISK_OVERLAY_ENABLED", True),
            enforce=_b("RISK_OVERLAY_ENFORCE", False),
        )


@dataclass
class RiskOverlayDecision:
    veto: bool                        # должен ли блокировать новый сигнал
    reason_code: str | None
    shadow: bool                      # True если решение в shadow (enforce=False)
    portfolio_heat_r: float
    group_notional_usd: float
    correlation_group: str
    consec_losses: int
    details: dict = field(default_factory=dict)


# ────────────────────────────── compute / evaluate ──────────────────────────────
def compute_portfolio_heat_r(open_positions: Iterable[OpenPositionInfo]) -> float:
    """Суммарный отрицательный R по открытым позициям.

    Возвращаем модуль суммы negative-R: 3 позиции по -0.5R → heat = 1.5.
    Прибыльные позиции вклада не дают.
    """
    heat = 0.0
    for p in open_positions:
        if p.unrealized_r < 0:
            heat += -p.unrealized_r
    return heat


def correlated_group_notional(
    open_positions: Iterable[OpenPositionInfo],
    *,
    symbol: str,
    groups: dict[str, set[str]] | None = None,
) -> tuple[str, float]:
    """notional на группе, к которой принадлежит symbol.

    Возвращает (group, total_notional_in_group).
    """
    group = correlation_group_for(symbol, groups)
    total = 0.0
    for p in open_positions:
        if correlation_group_for(p.symbol, groups) == group:
            total += p.notional_usd
    return group, total


def consecutive_losses(
    recent: Iterable[RecentTradeOutcome],
    *,
    bucket: str,
    now_ms: int,
    lookback_ms: int,
) -> int:
    """Считает подряд идущие убытки в одном bucket в окне lookback.

    Парадигма: самая свежая сделка первой; идём вспять, пока r_multiple<0.
    Считаем только сделки с тем же bucket и в окне.
    """
    in_window = [
        t for t in recent
        if t.bucket == bucket and (now_ms - t.ts_ms) <= lookback_ms
    ]
    in_window.sort(key=lambda t: t.ts_ms, reverse=True)
    n = 0
    for t in in_window:
        if t.r_multiple < 0:
            n += 1
        else:
            break
    return n


def evaluate_risk_overlay(
    *,
    symbol: str,
    bucket: str,
    open_positions: list[OpenPositionInfo],
    new_position_notional_usd: float,
    recent_outcomes: list[RecentTradeOutcome],
    now_ms: int,
    limits: RiskLimits,
    groups: dict[str, set[str]] | None = None,
) -> RiskOverlayDecision:
    """Применяет три ограничителя. Возвращает решение veto/no-veto + reason."""
    heat = compute_portfolio_heat_r(open_positions)
    group, group_notional = correlated_group_notional(
        open_positions, symbol=symbol, groups=groups,
    )
    consec = consecutive_losses(
        recent_outcomes, bucket=bucket, now_ms=now_ms,
        lookback_ms=limits.cooldown_lookback_ms,
    )

    if not limits.enabled:
        return RiskOverlayDecision(
            veto=False, reason_code=None, shadow=True,
            portfolio_heat_r=heat, group_notional_usd=group_notional,
            correlation_group=group, consec_losses=consec,
            details={"enabled": False},
        )

    reason: str | None = None
    if heat >= limits.max_portfolio_heat_r:
        reason = f"VETO_PORTFOLIO_HEAT:{heat:.2f}>={limits.max_portfolio_heat_r:.2f}"
    elif (group_notional + new_position_notional_usd) > limits.max_correlation_group_usd:
        reason = (
            f"VETO_CORRELATION:{group}={group_notional:.0f}+{new_position_notional_usd:.0f}"
            f">{limits.max_correlation_group_usd:.0f}"
        )
    elif consec >= limits.max_consecutive_losses:
        reason = f"VETO_CONSEC_LOSS:{bucket}={consec}>={limits.max_consecutive_losses}"

    return RiskOverlayDecision(
        veto=bool(reason and limits.enforce),
        reason_code=reason,
        shadow=not limits.enforce,
        portfolio_heat_r=heat,
        group_notional_usd=group_notional,
        correlation_group=group,
        consec_losses=consec,
    )
