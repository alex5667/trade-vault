#!/usr/bin/env python3
"""
Оффлайн-джоб для расчета baseline-квантилей по L3-метрикам.

Вытаскивает данные из Timescale, считает baseline по signal_family,
генерирует YAML-конфиг для CryptoConfScorer.
"""

import asyncio
import math
import os
from dataclasses import dataclass
from typing import Any

import asyncpg
import numpy as np
import pandas as pd
import yaml


@dataclass
class BaselineJobConfig:
    """Конфигурация джоба"""
    pg_dsn: str
    lookback_days: int = 60
    min_signals: int = 200     # минимальное кол-во сигналов в группе
    min_trades: int = 50       # минимальное кол-во сделок в группе

    yaml_output_path: str | None = "crypto_conf_scorer_baseline.yaml"
    insert_to_db: bool = True


# SQL для вытаскивания сигналов с результатами
SIGNALS_WITH_PERF_SQL = """
SELECT
    s.ts                      AS ts_signal,
    s.signal_id,
    s.symbol,
    s.signal_family,
    s.direction,
    s.conf_score,

    s.l3_spread_bps,
    s.l3_microprice_shift_bps_20,
    s.l3_obi_persistence_score,
    s.l3_cancel_to_trade_bid_5s,
    s.l3_cancel_to_trade_ask_5s,
    s.l3_cancel_to_trade_bid_20s,
    s.l3_cancel_to_trade_ask_20s,

    t.r,
    t.hit
FROM signal_facts s
JOIN trade_performance t
  ON t.signal_id = s.signal_id
WHERE s.ts >= now() - ($1::int || ' days')::interval
  AND s.l3_spread_bps IS NOT NULL
  AND s.l3_obi_persistence_score IS NOT NULL
ORDER BY s.ts DESC
"""


def _safe_quantile(series: pd.Series, q: float) -> float:
    """Безопасный расчет квантиля"""
    s = series.dropna()
    if s.empty:
        return float("nan")
    return float(s.quantile(q))


def _expectancy_r(r: pd.Series) -> float:
    """Расчет expectancy R"""
    if r.empty:
        return float("nan")
    return float(r.mean())


def _hit_rate(hit: pd.Series) -> float:
    """Расчет hit rate"""
    s = hit.dropna()
    if s.empty:
        return float("nan")
    return float(s.mean())


async def fetch_signals_with_perf(cfg: BaselineJobConfig) -> pd.DataFrame:
    """
    Вытаскиваем сигналы с результатами из Timescale.
    """
    conn = await asyncpg.connect(cfg.pg_dsn)
    try:
        rows = await conn.fetch(SIGNALS_WITH_PERF_SQL, cfg.lookback_days)
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        return df
    finally:
        await conn.close()


def compute_group_baseline(
    g: pd.DataFrame,
    as_of_ts: pd.Timestamp,
    lookback_days: int,
    min_signals: int,
    min_trades: int,
) -> dict[str, Any] | None:
    """
    Расчет baseline для одной группы (symbol, signal_family, direction).
    """
    n_signals = len(g)
    n_trades = g["r"].notna().sum()

    if n_signals < min_signals or n_trades < min_trades:
        return None

    # Базовые метрики
    hit_rate = _hit_rate(g["hit"].astype(float))
    expectancy = _expectancy_r(g["r"].astype(float))

    # R-квантиля
    r_p25 = _safe_quantile(g["r"], 0.25)
    r_p50 = _safe_quantile(g["r"], 0.50)
    r_p75 = _safe_quantile(g["r"], 0.75)

    # Spread
    spread = g["l3_spread_bps"].abs()
    spread_p50 = _safe_quantile(spread, 0.50)
    spread_p80 = _safe_quantile(spread, 0.80)
    spread_p95 = _safe_quantile(spread, 0.95)

    # OBI persistence
    obi = g["l3_obi_persistence_score"].clip(lower=0.0, upper=1.0)
    obi_p25 = _safe_quantile(obi, 0.25)
    obi_p50 = _safe_quantile(obi, 0.50)
    obi_p75 = _safe_quantile(obi, 0.75)

    # Microprice drift по модулю (в bp)
    mp_abs = g["l3_microprice_shift_bps_20"].abs()
    mp_abs_p50 = _safe_quantile(mp_abs, 0.50)
    mp_abs_p80 = _safe_quantile(mp_abs, 0.80)

    # Cancel-to-trade
    canc_bid = 0.5 * (g["l3_cancel_to_trade_bid_5s"] + g["l3_cancel_to_trade_bid_20s"])
    canc_ask = 0.5 * (g["l3_cancel_to_trade_ask_5s"] + g["l3_cancel_to_trade_ask_20s"])
    canc_bid_p50 = _safe_quantile(canc_bid, 0.50)
    canc_bid_p80 = _safe_quantile(canc_bid, 0.80)
    canc_ask_p50 = _safe_quantile(canc_ask, 0.50)
    canc_ask_p80 = _safe_quantile(canc_ask, 0.80)

    # === Выводим thresholds из квантилей ===
    # Spread:
    #  - max_ok  ~ медиана спреда на "успешных" режимах → p50
    #  - hard    ~ p95 (край хвоста, выше — почти не торгуем)
    l3_spread_max_ok = float(spread_p50) if not math.isnan(spread_p50) else 5.0
    l3_spread_hard = float(spread_p95) if not math.isnan(spread_p95) else 20.0

    # OBI persistence:
    #  - good_min ~ где уже заметен устойчивый перекос → p50
    #  - bad_max  ~ уровень, ниже которого OBI считаем "нет сигнала" → p25
    l3_obi_good_min = float(obi_p50) if not math.isnan(obi_p50) else 0.5
    l3_obi_bad_max = float(obi_p25) if not math.isnan(obi_p25) else 0.2

    # Cancel-to-trade:
    #  - soft ~ p50
    #  - hard ~ p80
    canc_values = [canc_bid_p50, canc_ask_p50, canc_bid_p80, canc_ask_p80]
    valid_canc = [v for v in canc_values if not math.isnan(v)]
    if valid_canc:
        canc_soft = float(np.median(valid_canc[:2]))  # p50 bid/ask
        canc_hard = float(np.median(valid_canc[2:]))  # p80 bid/ask
    else:
        canc_soft, canc_hard = 2.0, 5.0

    # Microprice drift:
    #  - max_bps ~ p80 модуля → сколько обычно "нормально"
    l3_mp_drift_max = float(mp_abs_p80) if not math.isnan(mp_abs_p80) else 5.0

    symbol = g["symbol"].iloc[0]
    family = g["signal_family"].iloc[0]
    direction = int(g["direction"].iloc[0])

    return {
        "as_of_ts": as_of_ts,
        "symbol": symbol,
        "signal_family": family,
        "direction": direction,
        "lookback_days": lookback_days,
        "n_signals": int(n_signals),
        "n_trades": int(n_trades),
        "hit_rate": float(hit_rate) if not math.isnan(hit_rate) else 0.0,
        "expectancy_r": float(expectancy) if not math.isnan(expectancy) else 0.0,
        "r_p25": float(r_p25) if not math.isnan(r_p25) else 0.0,
        "r_p50": float(r_p50) if not math.isnan(r_p50) else 0.0,
        "r_p75": float(r_p75) if not math.isnan(r_p75) else 0.0,
        "spread_p50": float(spread_p50) if not math.isnan(spread_p50) else 0.0,
        "spread_p80": float(spread_p80) if not math.isnan(spread_p80) else 0.0,
        "spread_p95": float(spread_p95) if not math.isnan(spread_p95) else 0.0,
        "obi_persist_p25": float(obi_p25) if not math.isnan(obi_p25) else 0.0,
        "obi_persist_p50": float(obi_p50) if not math.isnan(obi_p50) else 0.0,
        "obi_persist_p75": float(obi_p75) if not math.isnan(obi_p75) else 0.0,
        "mp_drift_abs_p50": float(mp_abs_p50) if not math.isnan(mp_abs_p50) else 0.0,
        "mp_drift_abs_p80": float(mp_abs_p80) if not math.isnan(mp_abs_p80) else 0.0,
        "canc_bid_p50": float(canc_bid_p50) if not math.isnan(canc_bid_p50) else 0.0,
        "canc_bid_p80": float(canc_bid_p80) if not math.isnan(canc_bid_p80) else 0.0,
        "canc_ask_p50": float(canc_ask_p50) if not math.isnan(canc_ask_p50) else 0.0,
        "canc_ask_p80": float(canc_ask_p80) if not math.isnan(canc_ask_p80) else 0.0,
        "l3_spread_max_ok_bps": l3_spread_max_ok,
        "l3_spread_hard_limit_bps": l3_spread_hard,
        "l3_cancel_soft": canc_soft,
        "l3_cancel_hard": canc_hard,
        "l3_obi_good_min": l3_obi_good_min,
        "l3_obi_bad_max": l3_obi_bad_max,
        "l3_mp_drift_max_bps": l3_mp_drift_max,
    }


def compute_baseline_table(
    df: pd.DataFrame,
    cfg: BaselineJobConfig,
) -> list[dict[str, Any]]:
    """
    Расчет baseline для всех групп.
    """
    if df.empty:
        return []

    as_of_ts = pd.Timestamp.utcnow()
    groups = df.groupby(["symbol", "signal_family", "direction"], dropna=False)
    rows: list[dict[str, Any]] = []

    for _, g in groups:
        row = compute_group_baseline(
            g,
            as_of_ts=as_of_ts,
            lookback_days=cfg.lookback_days,
            min_signals=cfg.min_signals,
            min_trades=cfg.min_trades,
        )
        if row is not None:
            rows.append(row)

    return rows


async def insert_baseline_rows(
    cfg: BaselineJobConfig,
    rows: list[dict[str, Any]],
) -> None:
    """
    Вставка baseline-строк в TimescaleDB.
    """
    if not rows or not cfg.insert_to_db:
        return

    conn = await asyncpg.connect(cfg.pg_dsn)
    try:
        sql = """
        INSERT INTO signal_family_baseline (
            as_of_ts, symbol, signal_family, direction, lookback_days,
            n_signals, n_trades,
            hit_rate, expectancy_r,
            r_p25, r_p50, r_p75,
            spread_p50, spread_p80, spread_p95,
            obi_persist_p25, obi_persist_p50, obi_persist_p75,
            mp_drift_abs_p50, mp_drift_abs_p80,
            canc_bid_p50, canc_bid_p80,
            canc_ask_p50, canc_ask_p80,
            l3_spread_max_ok_bps, l3_spread_hard_limit_bps,
            l3_cancel_soft, l3_cancel_hard,
            l3_obi_good_min, l3_obi_bad_max,
            l3_mp_drift_max_bps
        ) VALUES (
            $1,$2,$3,$4,$5,
            $6,$7,
            $8,$9,
            $10,$11,$12,
            $13,$14,$15,
            $16,$17,$18,
            $19,$20,
            $21,$22,
            $23,$24,
            $25,$26,
            $27,$28,
            $29,$30,
            $31
        )
        """

        async with conn.transaction():
            for r in rows:
                await conn.execute(
                    sql,
                    r["as_of_ts"],
                    r["symbol"],
                    r["signal_family"],
                    r["direction"],
                    r["lookback_days"],
                    r["n_signals"],
                    r["n_trades"],
                    r["hit_rate"],
                    r["expectancy_r"],
                    r["r_p25"],
                    r["r_p50"],
                    r["r_p75"],
                    r["spread_p50"],
                    r["spread_p80"],
                    r["spread_p95"],
                    r["obi_persist_p25"],
                    r["obi_persist_p50"],
                    r["obi_persist_p75"],
                    r["mp_drift_abs_p50"],
                    r["mp_drift_abs_p80"],
                    r["canc_bid_p50"],
                    r["canc_bid_p80"],
                    r["canc_ask_p50"],
                    r["canc_ask_p80"],
                    r["l3_spread_max_ok_bps"],
                    r["l3_spread_hard_limit_bps"],
                    r["l3_cancel_soft"],
                    r["l3_cancel_hard"],
                    r["l3_obi_good_min"],
                    r["l3_obi_bad_max"],
                    r["l3_mp_drift_max_bps"],
                )
    finally:
        await conn.close()


def build_yaml_config(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Генерация YAML-конфига для CryptoConfScorer.
    """
    if not rows:
        return {}

    df = pd.DataFrame(rows)

    # "дефолтный" профиль – медиана по всем
    def median_or_default(col: str, default: float) -> float:
        values = df[col].dropna()
        return float(values.median()) if not values.empty else default

    default_l3 = {
        "spread_max_ok_bps": median_or_default("l3_spread_max_ok_bps", 5.0),
        "spread_hard_limit_bps": median_or_default("l3_spread_hard_limit_bps", 20.0),
        "cancel_soft": median_or_default("l3_cancel_soft", 2.0),
        "cancel_hard": median_or_default("l3_cancel_hard", 5.0),
        "obi_good_min": median_or_default("l3_obi_good_min", 0.5),
        "obi_bad_max": median_or_default("l3_obi_bad_max", 0.2),
        "mp_drift_max_bps": median_or_default("l3_mp_drift_max_bps", 5.0),
    }

    # По символам и фемили — overrides
    by_symbol: dict[str, Any] = {}
    grouped = df.groupby(["symbol", "signal_family", "direction"])

    for (symbol, family, direction), g in grouped:
        row = g.iloc[0]  # baseline на группу уже один

        l3 = {
            "spread_max_ok_bps": float(row["l3_spread_max_ok_bps"]),
            "spread_hard_limit_bps": float(row["l3_spread_hard_limit_bps"]),
            "cancel_soft": float(row["l3_cancel_soft"]),
            "cancel_hard": float(row["l3_cancel_hard"]),
            "obi_good_min": float(row["l3_obi_good_min"]),
            "obi_bad_max": float(row["l3_obi_bad_max"]),
            "mp_drift_max_bps": float(row["l3_mp_drift_max_bps"]),
        }

        dir_key = {+1: "long", -1: "short", 0: "neutral"}.get(direction, "neutral")

        by_symbol.setdefault(symbol, {})
        by_symbol[symbol].setdefault(family, {})
        by_symbol[symbol][family][dir_key] = {"l3": l3}

    return {
        "crypto_conf_scorer": {
            "default": {"l3": default_l3},
            "by_symbol": by_symbol,
        }
    }


def write_yaml_config(cfg: BaselineJobConfig, data: dict[str, Any]) -> None:
    """
    Сохранение YAML-конфига на диск.
    """
    if not cfg.yaml_output_path or not data:
        return

    with open(cfg.yaml_output_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)


async def run_baseline_job() -> None:
    """
    Основная функция джоба.
    """
    cfg = BaselineJobConfig(
        pg_dsn=os.environ.get("ANALYTICS_DB_DSN", os.environ.get("DATABASE_URL")),
        lookback_days=int(os.getenv("BASELINE_LOOKBACK_DAYS", "60")),
        min_signals=int(os.getenv("BASELINE_MIN_SIGNALS", "200")),
        min_trades=int(os.getenv("BASELINE_MIN_TRADES", "50")),
        yaml_output_path=os.getenv("BASELINE_YAML_PATH", "crypto_conf_scorer_baseline.yaml"),
        insert_to_db=os.getenv("BASELINE_INSERT_DB", "1") == "1",
    )

    print(f"Starting baseline job: lookback={cfg.lookback_days}d, min_signals={cfg.min_signals}")

    # Вытаскиваем данные
    df = await fetch_signals_with_perf(cfg)
    if df.empty:
        print("No signal data found for baseline calculation")
        return

    print(f"Fetched {len(df)} signal-performance records")

    # Считаем baseline
    rows = compute_baseline_table(df, cfg)
    print(f"Computed baseline for {len(rows)} signal groups")

    # Сохраняем в DB
    if cfg.insert_to_db:
        await insert_baseline_rows(cfg, rows)
        print("Inserted baseline data to database")

    # Генерируем YAML
    yaml_data = build_yaml_config(rows)
    write_yaml_config(cfg, yaml_data)
    print(f"Generated YAML config: {cfg.yaml_output_path}")

    print("Baseline job completed successfully")


class SignalFamilyBaselineJob:
    """
    Класс для выполнения baseline джоба.
    Обертка над функцией run_baseline_job.
    """

    def __init__(self, config: BaselineJobConfig):
        self.config = config

    async def run(self) -> None:
        """Запустить джоб с текущей конфигурацией"""
        # Сохраняем старую конфигурацию и устанавливаем новую
        old_env = {}
        for key in ["DATABASE_URL", "BASELINE_LOOKBACK_DAYS", "BASELINE_MIN_SIGNALS",
                    "BASELINE_MIN_TRADES", "BASELINE_YAML_PATH", "BASELINE_INSERT_DB"]:
            old_env[key] = os.environ.get(key)

        try:
            # Устанавливаем переменные окружения из конфига
            os.environ["DATABASE_URL"] = self.config.pg_dsn
            os.environ["BASELINE_LOOKBACK_DAYS"] = str(self.config.lookback_days)
            os.environ["BASELINE_MIN_SIGNALS"] = str(self.config.min_signals)
            os.environ["BASELINE_MIN_TRADES"] = str(self.config.min_trades)
            if hasattr(self.config, 'yaml_output_path'):
                os.environ["BASELINE_YAML_PATH"] = self.config.yaml_output_path
            if hasattr(self.config, 'insert_to_db'):
                os.environ["BASELINE_INSERT_DB"] = "1" if self.config.insert_to_db else "0"

            await run_baseline_job()
        finally:
            # Восстанавливаем старые переменные окружения
            for key, value in old_env.items():
                if value is not None:
                    os.environ[key] = value
                elif key in os.environ:
                    del os.environ[key]


if __name__ == "__main__":
    asyncio.run(run_baseline_job())
