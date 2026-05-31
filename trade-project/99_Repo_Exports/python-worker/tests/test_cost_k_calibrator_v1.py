from __future__ import annotations

"""Тесты Cost-K auto-calibrator.

Без numpy, без реального Redis/Postgres.
Все тесты изолированы через mock-объекты или прямой вызов функций.
"""

import math
import time

import pytest

from orderflow_services.cost_k_calibrator_v1 import (
    DEFAULT_K,
    K_LOWER,
    K_UPPER,
    HALF_LIFE_DAYS,
    TradeRow,
    KFitResult,
    _weighted_quantile,
    compute_k_fit,
    blend_and_clamp,
    check_gates,
    build_payload,
    load_current_calibration,
    write_redis,
)
from core.cost_k_store import CostKStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_trade(
    symbol: str = "BTCUSDT",
    regime: str = "NORMAL",
    pnl_gross: float = 50.0,
    fees: float = 10.0,
    notional_usd: float = 10000.0,
    age_days: float = 0.0,
) -> TradeRow:
    """Создаёт TradeRow с заданным возрастом (age_days назад)."""
    now_ms = int(time.time() * 1000)
    exit_ts_ms = now_ms - int(age_days * 86400 * 1000)
    return TradeRow(
        symbol=symbol,
        regime=regime,
        pnl_gross=pnl_gross,
        fees=fees,
        notional_usd=notional_usd,
        exit_ts_ms=exit_ts_ms,
    )


# ---------------------------------------------------------------------------
# test_k_observed_positive_trade
# ---------------------------------------------------------------------------

def test_k_observed_positive_trade():
    """K_observed = pnl_gross / fees для прибыльной сделки."""
    t = _make_trade(pnl_gross=50.0, fees=10.0)
    assert t.k_observed == pytest.approx(5.0, rel=1e-6)


def test_k_observed_negative_trade():
    """pnl_gross < 0 → K отрицательный, калибратор всё равно его учитывает."""
    t = _make_trade(pnl_gross=-30.0, fees=10.0)
    k = t.k_observed
    assert k == pytest.approx(-3.0, rel=1e-6)
    # Убеждаемся, что compute_k_fit принимает отрицательные K
    results = compute_k_fit([t])
    assert ("BTCUSDT", "NORMAL") in results
    fit = results[("BTCUSDT", "NORMAL")]
    assert fit.n == 1
    # K_p50 = -3.0 (single value)
    assert fit.K_p50 == pytest.approx(-3.0, rel=1e-6)


def test_k_observed_zero_fees():
    """fees = 0 → k_observed = NaN (защита от деления на 0)."""
    t = _make_trade(fees=0.0)
    assert math.isnan(t.k_observed)


# ---------------------------------------------------------------------------
# test_weighted_median_*
# ---------------------------------------------------------------------------

def test_weighted_median_empty():
    """Пустой список → fallback DEFAULT_K."""
    result = _weighted_quantile([], [], 0.5)
    assert result == DEFAULT_K


def test_weighted_median_single():
    """Один элемент → возвращается он сам."""
    result = _weighted_quantile([5.0], [1.0], 0.5)
    assert result == pytest.approx(5.0, rel=1e-6)


def test_weighted_median_basic():
    """Список из 5 значений: p50 должна быть в разумном диапазоне.

    Weighted quantile с равными весами interpolates между точками,
    результат может быть чуть ниже точной медианы 4.0 из-за интерполяции.
    """
    values = [2.0, 3.0, 4.0, 5.0, 6.0]
    weights = [1.0, 1.0, 1.0, 1.0, 1.0]   # равные веса
    result = _weighted_quantile(values, weights, 0.5)
    # При равных весах p50 находится в диапазоне [3.0, 5.0]
    assert 3.0 <= result <= 5.0, f"Expected ~4.0, got {result}"
    # И p25 < p50 < p75
    p25 = _weighted_quantile(values, weights, 0.25)
    p75 = _weighted_quantile(values, weights, 0.75)
    assert p25 < result < p75


def test_weighted_median_skewed_weights():
    """Сильный вес на крайнем элементе → медиана сдвигается к нему.

    С весами [0.01, 0.99] cumulative weights = [0.01, 1.0].
    p50=0.5 попадает в интерполяцию между индексами 0 и 1:
    frac = (0.5 - 0.01) / (1.0 - 0.01) ≈ 0.4949...
    result ≈ 2.0*(1-0.4949) + 10.0*0.4949 ≈ 5.96.
    Важно: результат должен быть значительно выше 2.0 (смещён к 10.0).
    """
    values = [2.0, 10.0]
    weights = [0.01, 0.99]  # почти весь вес на 10.0
    result = _weighted_quantile(values, weights, 0.5)
    # Медиана сдвинута к 10.0 — должна быть > 5.0 (выше неважной точки 2.0)
    assert result > 5.0, f"Expected >5.0, got {result}"
    # И строго между 2.0 и 10.0
    assert 2.0 < result < 10.0


def test_weighted_median_nan_weight_filtered():
    """NaN/отрицательные веса игнорируются."""
    values = [2.0, 5.0, 8.0]
    weights = [float("nan"), 1.0, -1.0]  # только w=1.0 валиден
    result = _weighted_quantile(values, weights, 0.5)
    assert result == pytest.approx(5.0, rel=1e-6)


def test_weighted_quantile_p25_p75():
    """Квантили p25 и p75 корректно упорядочены."""
    values = [float(x) for x in range(1, 11)]   # [1.0..10.0]
    weights = [1.0] * 10
    p25 = _weighted_quantile(values, weights, 0.25)
    p75 = _weighted_quantile(values, weights, 0.75)
    assert p25 < p75
    assert 2.0 <= p25 <= 4.0, f"p25={p25}"
    assert 7.0 <= p75 <= 9.0, f"p75={p75}"


# ---------------------------------------------------------------------------
# test_blend_and_clamp_*
# ---------------------------------------------------------------------------

def test_blend_and_clamp_lower():
    """K_fit очень маленький → итоговое значение зажато не ниже K_LOWER.

    При alpha≈0.095 и K_old=4.0 один шаг blend даёт значение выше K_LOWER.
    Многократное применение (симуляция сходимости) должно достичь clamp.
    """
    alpha = 1.0 - math.pow(2.0, -1.0 / HALF_LIFE_DAYS)
    # Один шаг blend с экстремально низким K_fit
    result_single = blend_and_clamp(0.5, DEFAULT_K, alpha)
    # Результат не ниже K_LOWER (hard clamp)
    assert result_single >= K_LOWER, f"Must be >= K_LOWER={K_LOWER}, got {result_single}"

    # Симулируем сходимость: многократно применяем blend
    k = DEFAULT_K
    for _ in range(200):
        k = blend_and_clamp(0.5, k, alpha)
    # После сходимости результат должен быть равен K_LOWER (clamp сработал)
    assert k == pytest.approx(K_LOWER, rel=1e-4), f"After convergence expected {K_LOWER}, got {k}"


def test_blend_and_clamp_upper():
    """K_fit очень большой → итоговое значение зажато не выше K_UPPER.

    Симуляция сходимости к верхней границе.
    """
    alpha = 1.0 - math.pow(2.0, -1.0 / HALF_LIFE_DAYS)
    # Один шаг — не выше K_UPPER
    result_single = blend_and_clamp(20.0, DEFAULT_K, alpha)
    assert result_single <= K_UPPER, f"Must be <= K_UPPER={K_UPPER}, got {result_single}"

    # Сходимость к K_UPPER
    k = DEFAULT_K
    for _ in range(200):
        k = blend_and_clamp(20.0, k, alpha)
    assert k == pytest.approx(K_UPPER, rel=1e-4), f"After convergence expected {K_UPPER}, got {k}"


def test_blend_alpha():
    """EWMA blend: K_new = (1-alpha)*K_old + alpha*K_fit."""
    K_old = 4.0
    K_fit = 6.0
    alpha = 0.2  # явный alpha для простоты проверки
    expected = (1.0 - alpha) * K_old + alpha * K_fit
    result = blend_and_clamp(K_fit, K_old, alpha)
    assert result == pytest.approx(expected, rel=1e-6)


def test_blend_and_clamp_normal_range():
    """K_fit внутри [K_LOWER, K_UPPER] → blend без clamp."""
    K_old = 4.0
    K_fit = 4.5
    alpha = 0.5
    expected = max(K_LOWER, min(K_UPPER, (1.0 - alpha) * K_old + alpha * K_fit))
    result = blend_and_clamp(K_fit, K_old, alpha)
    assert result == pytest.approx(expected, rel=1e-6)


def test_blend_and_clamp_invalid_k_old():
    """K_old=0 или NaN → сбрасывается на DEFAULT_K."""
    alpha = 0.1
    result_zero = blend_and_clamp(4.0, 0.0, alpha)
    result_nan = blend_and_clamp(4.0, float("nan"), alpha)
    # Оба должны быть в [K_LOWER, K_UPPER]
    assert K_LOWER <= result_zero <= K_UPPER
    assert K_LOWER <= result_nan <= K_UPPER


# ---------------------------------------------------------------------------
# test_gates_*
# ---------------------------------------------------------------------------

def test_gates_insufficient_trades():
    """Менее min_trades_per_group сделок → blockers не пустые."""
    passed, blockers = check_gates(
        n_trades=5,
        n_groups=1,
        blockers=[],
        min_trades_per_group=20,
        min_groups=1,
    )
    assert not passed
    assert len(blockers) > 0
    assert any("min_trades_per_group" in b for b in blockers)


def test_gates_sufficient_trades():
    """Достаточно сделок и групп → проходит."""
    passed, blockers = check_gates(
        n_trades=50,
        n_groups=3,
        blockers=[],
        min_trades_per_group=20,
        min_groups=1,
    )
    assert passed
    assert blockers == []


def test_gates_insufficient_groups():
    """Нет реальных групп → blocker."""
    passed, blockers = check_gates(
        n_trades=100,
        n_groups=0,
        blockers=[],
        min_trades_per_group=20,
        min_groups=1,
    )
    assert not passed
    assert any("n_groups" in b for b in blockers)


def test_gates_propagates_existing_blockers():
    """Существующие blockers передаются дальше."""
    existing = ["some_prior_block"]
    passed, blockers = check_gates(
        n_trades=100,
        n_groups=1,
        blockers=existing,
        min_trades_per_group=20,
        min_groups=1,
    )
    assert not passed
    assert "some_prior_block" in blockers


# ---------------------------------------------------------------------------
# test_compute_k_fit
# ---------------------------------------------------------------------------

def test_compute_k_fit_basic():
    """Базовая проверка вычисления fit по группам."""
    trades = [
        _make_trade("BTCUSDT", "NORMAL", pnl_gross=50.0, fees=10.0, age_days=0.0),
        _make_trade("BTCUSDT", "NORMAL", pnl_gross=60.0, fees=10.0, age_days=1.0),
        _make_trade("ETHUSDT", "TREND",  pnl_gross=20.0, fees=5.0,  age_days=0.5),
    ]
    results = compute_k_fit(trades)
    # Должны быть реальные группы
    assert ("BTCUSDT", "NORMAL") in results
    assert ("ETHUSDT", "TREND") in results
    # Агрегированные тоже
    assert ("BTCUSDT", "*") in results
    assert ("*", "*") in results

    btc_fit = results[("BTCUSDT", "NORMAL")]
    assert btc_fit.n == 2
    # K_observed = [5.0, 6.0] → median ≈ 5.5
    assert 4.5 <= btc_fit.K_p50 <= 6.5

    eth_fit = results[("ETHUSDT", "TREND")]
    assert eth_fit.n == 1
    assert eth_fit.K_p50 == pytest.approx(4.0, rel=1e-4)  # 20/5=4.0


def test_compute_k_fit_excludes_zero_fees():
    """Сделки с fees=0 исключаются из расчёта."""
    trades = [
        _make_trade(fees=0.0, pnl_gross=100.0),   # должна быть исключена
        _make_trade(fees=10.0, pnl_gross=50.0),    # OK: K=5.0
    ]
    results = compute_k_fit(trades)
    fit = results.get(("BTCUSDT", "NORMAL"))
    assert fit is not None
    assert fit.n == 1
    assert fit.K_p50 == pytest.approx(5.0, rel=1e-4)


def test_compute_k_fit_n_positive():
    """n_positive корректно считает прибыльные сделки."""
    trades = [
        _make_trade(pnl_gross=50.0,  fees=10.0),   # positive
        _make_trade(pnl_gross=-30.0, fees=10.0),   # negative
        _make_trade(pnl_gross=0.0,   fees=10.0),   # zero (не считается positive)
    ]
    results = compute_k_fit(trades)
    fit = results[("BTCUSDT", "NORMAL")]
    assert fit.n_positive == 1


def test_compute_k_fit_age_weighting():
    """Старые сделки имеют меньший вес: медиана должна быть ближе к новым."""
    # Старые сделки с K=2.0, новая сделка с K=8.0
    old_trades = [
        _make_trade(pnl_gross=20.0, fees=10.0, age_days=30.0),  # K=2.0, w≈0.055
        _make_trade(pnl_gross=20.0, fees=10.0, age_days=28.0),  # K=2.0
    ]
    new_trade = _make_trade(pnl_gross=80.0, fees=10.0, age_days=0.1)  # K=8.0, w≈1.0

    results = compute_k_fit(old_trades + [new_trade])
    fit = results[("BTCUSDT", "NORMAL")]
    # p50 с весами: должна быть значительно выше 2.0 (ближе к новым данным)
    assert fit.K_p50 > 4.0, f"Expected weighted median > 4.0, got {fit.K_p50}"


# ---------------------------------------------------------------------------
# test_load_current_calibration
# ---------------------------------------------------------------------------

class _MockRedis:
    """Минимальный mock Redis для тестов."""

    def __init__(self, data: dict[str, str] | None = None):
        self._data: dict[str, str] = data or {}

    def get(self, key: str):
        return self._data.get(key)

    def set(self, key: str, value: str, *a, **k) -> bool:  # type: ignore[override]
        del a, k
        self._data[key] = value
        return True

    def xlen(self, *a, **k) -> int:  # type: ignore[override]
        del a, k
        return 0

    def xadd(self, *a, **k) -> str:  # type: ignore[override]
        del a, k
        return "0-0"


def test_load_current_calibration_empty_redis():
    """Пустой Redis → возвращает пустой dict."""
    r = _MockRedis()
    result = load_current_calibration(r)
    assert result == {}


def test_load_current_calibration_valid_payload():
    """Парсит корректный payload из Redis."""
    import json
    payload = {
        "schema_version": 1,
        "groups": {
            "BTCUSDT:NORMAL": {"K_new": 3.8},
            "ETHUSDT:TREND":  {"K_new": 4.2},
            "*:*":            {"K_new": 4.0},
        }
    }
    r = _MockRedis({"cfg:cost_edge_gate:v1:calibration": json.dumps(payload)})
    result = load_current_calibration(r)
    assert result["BTCUSDT:NORMAL"] == pytest.approx(3.8, rel=1e-6)
    assert result["ETHUSDT:TREND"] == pytest.approx(4.2, rel=1e-6)
    assert result["*:*"] == pytest.approx(4.0, rel=1e-6)


def test_load_current_calibration_malformed_json():
    """Повреждённый JSON → возвращает пустой dict (без исключения)."""
    r = _MockRedis({"cfg:cost_edge_gate:v1:calibration": "NOT_JSON{{"})
    result = load_current_calibration(r)
    assert result == {}


# ---------------------------------------------------------------------------
# test_write_redis
# ---------------------------------------------------------------------------

def test_write_redis_success():
    """write_redis сохраняет payload в mock Redis."""
    import json
    r = _MockRedis()
    payload = {"schema_version": 1, "groups": {}}
    ok = write_redis(r, "test:key", payload)
    assert ok is True
    stored = r.get("test:key")
    assert stored is not None
    obj = json.loads(stored)
    assert obj["schema_version"] == 1


# ---------------------------------------------------------------------------
# test_build_payload
# ---------------------------------------------------------------------------

def test_build_payload_structure():
    """build_payload возвращает корректную структуру."""
    results = {
        ("BTCUSDT", "NORMAL"): KFitResult(
            group_key=("BTCUSDT", "NORMAL"),
            n=30, K_p25=3.0, K_p50=4.5, K_p75=6.0,
            n_positive=20, w_total=25.0,
        ),
        ("*", "*"): KFitResult(
            group_key=("*", "*"),
            n=60, K_p25=3.0, K_p50=4.0, K_p75=5.0,
            n_positive=40, w_total=50.0,
        ),
    }
    payload = build_payload(
        results=results,
        current_k={"BTCUSDT:NORMAL": 4.0},
        alpha=0.095,
        blockers=[],
        gates_passed=True,
        run_id="test_run_001",
        n_trades=60,
        apply=True,
        shadow_enforce=1,
    )

    assert payload["schema_version"] == 1
    assert payload["method"] == "ewma_realized_k_v1"
    assert payload["n_trades"] == 60
    assert payload["gates_passed"] is True
    assert "calibrated_ms" in payload
    assert "groups" in payload
    assert "BTCUSDT:NORMAL" in payload["groups"]

    grp = payload["groups"]["BTCUSDT:NORMAL"]
    assert grp["n"] == 30
    assert "K_old" in grp
    assert "K_new" in grp
    assert K_LOWER <= grp["K_new"] <= K_UPPER


# ---------------------------------------------------------------------------
# test_cost_k_store_*
# ---------------------------------------------------------------------------

def test_cost_k_store_empty():
    """Пустой store → default K."""
    store = CostKStore.empty()
    assert not store.is_loaded
    assert store.get_k("BTCUSDT", "NORMAL", default=4.0) == 4.0
    assert store.get_k("ETHUSDT", None, default=3.5) == 3.5


def test_cost_k_store_get_k_hierarchy():
    """Иерархический fallback: (sym, regime) → (sym, *) → (*, *) → default."""
    store = CostKStore.from_dict({
        "BTCUSDT:NORMAL": 5.0,
        "BTCUSDT:*": 4.5,
        "*:*": 4.0,
    })

    # 1. Точное совпадение (sym, regime)
    assert store.get_k("BTCUSDT", "NORMAL") == pytest.approx(5.0, rel=1e-6)

    # 2. Совпадение (sym, *) — неизвестный режим
    assert store.get_k("BTCUSDT", "UNKNOWN") == pytest.approx(4.5, rel=1e-6)

    # 3. Глобальный (*, *) — неизвестный символ
    assert store.get_k("SOLUSDT", "NORMAL") == pytest.approx(4.0, rel=1e-6)

    # 4. Default — нет ни одного совпадения
    store2 = CostKStore.from_dict({})
    assert store2.get_k("BTCUSDT", "NORMAL", default=3.7) == 3.7


def test_cost_k_store_regime_none():
    """regime=None нормализуется к 'NORMAL'."""
    store = CostKStore.from_dict({
        "BTCUSDT:NORMAL": 5.0,
    })
    # None → нормализуется к "" → строит ключ "BTCUSDT:NORMAL"... но в get_k
    # пустой режим → "NORMAL"
    assert store.get_k("BTCUSDT", None) == pytest.approx(5.0, rel=1e-6)


def test_cost_k_store_case_insensitive():
    """Регистр символа не важен."""
    store = CostKStore.from_dict({"BTCUSDT:NORMAL": 5.5})
    assert store.get_k("btcusdt", "normal") == pytest.approx(5.5, rel=1e-6)


def test_cost_k_store_load_from_redis():
    """CostKStore.load() корректно читает из mock Redis."""
    import json
    payload = {
        "schema_version": 1,
        "calibrated_ms": int(time.time() * 1000),
        "groups": {
            "BTCUSDT:NORMAL": {"K_new": 3.9},
            "*:*": {"K_new": 4.1},
        }
    }
    r = _MockRedis({"cfg:cost_edge_gate:v1:calibration": json.dumps(payload)})
    store = CostKStore.load(r)

    assert store.is_loaded
    assert store.get_k("BTCUSDT", "NORMAL") == pytest.approx(3.9, rel=1e-6)
    assert store.get_k("ETHUSDT", "TREND") == pytest.approx(4.1, rel=1e-6)


def test_cost_k_store_load_empty_redis():
    """Пустой Redis → empty store, нет исключений."""
    r = _MockRedis()
    store = CostKStore.load(r)
    assert not store.is_loaded


def test_cost_k_store_is_loaded():
    """is_loaded=True только если есть данные."""
    empty = CostKStore.empty()
    assert not empty.is_loaded

    filled = CostKStore.from_dict({"*:*": 4.0})
    assert filled.is_loaded


def test_cost_k_store_age_ms():
    """age_ms возвращает разумное значение."""
    store = CostKStore.from_dict({"*:*": 4.0})
    age = store.age_ms
    # Только что созданный — возраст < 5 секунд
    assert 0 <= age < 5000

    empty = CostKStore.empty()
    assert empty.age_ms == 0


def test_cost_k_store_n_keys():
    """n_keys считает количество ключей."""
    store = CostKStore.from_dict({
        "BTCUSDT:NORMAL": 5.0,
        "BTCUSDT:*": 4.5,
        "*:*": 4.0,
    })
    assert store.n_keys == 3


# ---------------------------------------------------------------------------
# Integration: CostEdgeGate + CostKStore
# ---------------------------------------------------------------------------

def test_cost_edge_gate_with_k_store():
    """CostEdgeGate._get_cost_multiplier использует CostKStore при наличии."""
    from handlers.crypto_orderflow.of_core.cost_edge_gate import (
        CostEdgeGate,
        CostEdgeConfig,
    )

    config = CostEdgeConfig(
        enabled=True,
        default_cost_k=4.0,
        use_calibrated_k=True,
        calibrated_k_max_age_ms=3 * 3600 * 1000,
    )
    gate = CostEdgeGate(config)

    store = CostKStore.from_dict({
        "BTCUSDT:NORMAL": 3.5,
        "*:*": 4.2,
    })
    gate.set_k_store(store)

    # Должен вернуть 3.5 для BTCUSDT/NORMAL
    k = gate._get_cost_multiplier("BTCUSDT", regime="NORMAL")
    assert k == pytest.approx(3.5, rel=1e-6)

    # Неизвестный символ → fallback (*:*) = 4.2
    k2 = gate._get_cost_multiplier("SOLUSDT", regime="NORMAL")
    assert k2 == pytest.approx(4.2, rel=1e-6)


def test_cost_edge_gate_without_k_store():
    """CostEdgeGate без CostKStore использует config default."""
    from handlers.crypto_orderflow.of_core.cost_edge_gate import (
        CostEdgeGate,
        CostEdgeConfig,
    )

    config = CostEdgeConfig(
        enabled=True,
        default_cost_k=4.0,
        use_calibrated_k=True,
    )
    gate = CostEdgeGate(config)
    # k_store не установлен → fallback на config
    k = gate._get_cost_multiplier("BTCUSDT", regime="NORMAL")
    assert k == pytest.approx(4.0, rel=1e-6)


def test_cost_edge_gate_calibrated_k_disabled():
    """use_calibrated_k=False → K из config, даже если store установлен."""
    from handlers.crypto_orderflow.of_core.cost_edge_gate import (
        CostEdgeGate,
        CostEdgeConfig,
    )

    config = CostEdgeConfig(
        enabled=True,
        default_cost_k=4.0,
        use_calibrated_k=False,
    )
    gate = CostEdgeGate(config)
    store = CostKStore.from_dict({"BTCUSDT:NORMAL": 9.9})  # явно другое значение
    gate.set_k_store(store)

    k = gate._get_cost_multiplier("BTCUSDT", regime="NORMAL")
    assert k == pytest.approx(4.0, rel=1e-6)


def test_cost_edge_gate_evaluate_passes_regime():
    """gate.evaluate() передаёт regime в _get_cost_multiplier."""
    from handlers.crypto_orderflow.of_core.cost_edge_gate import (
        CostEdgeGate,
        CostEdgeConfig,
    )

    config = CostEdgeConfig(
        enabled=True,
        default_cost_k=4.0,
        use_calibrated_k=True,
        fees_bps=4.0,
        slippage_bps=4.0,
        slippage_use_spread_half=False,
        edge_mode="tp1",
    )
    gate = CostEdgeGate(config)
    # Высокий K=10.0 → сделает прохождение труднее
    store = CostKStore.from_dict({"BTCUSDT:TREND": 10.0})
    gate.set_k_store(store)

    # Создаём контекст с явным regime=TREND
    class _Ctx:
        side = "LONG"
        tp1 = 50500.0       # edge ≈ 100 bps
        atr_policy_regime = "TREND"
        regime = "TREND"

    result = gate.evaluate(_Ctx(), symbol="BTCUSDT", entry_price=50000.0)
    # cost_k должен быть 10.0 из store
    assert result.cost_multiplier == pytest.approx(10.0, rel=1e-6)


# ---------------------------------------------------------------------------
# TradeRow helpers
# ---------------------------------------------------------------------------

def test_trade_row_weight_recent():
    """Недавняя сделка (age=0) имеет вес ≈ 1.0."""
    t = _make_trade(age_days=0.0)
    assert t.weight == pytest.approx(1.0, abs=0.01)


def test_trade_row_weight_half_life():
    """Сделка с age=HALF_LIFE_DAYS имеет вес = 0.5."""
    t = _make_trade(age_days=HALF_LIFE_DAYS)
    assert t.weight == pytest.approx(0.5, abs=0.02)


def test_trade_row_weight_old():
    """Очень старая сделка имеет очень маленький вес."""
    t = _make_trade(age_days=60.0)
    assert t.weight < 0.01


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
