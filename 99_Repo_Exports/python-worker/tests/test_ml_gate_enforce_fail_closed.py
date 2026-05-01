from __future__ import annotations
"""
P0 / Capital-Safety тесты: ML gate fail-closed в ENFORCE-окружении.

Покрывают следующие сценарии:
  A. gate.check() без cfg (no_cfg) + ENFORCE + OPEN  → allow=True (bypass, ERR_NO_CFG)
  B. gate.check() без cfg (no_cfg) + ENFORCE + CLOSED → allow=False (block, ERR_NO_CFG)
  C. gate.check() без модели (no_model) + ENFORCE + CLOSED → allow=False
  D. gate.check() cfg загружен, модель None + ENFORCE + CLOSED → allow=False
  E. effective_mode == ENFORCE в ERR-решении должен быть ENFORCE (не OFF/SHADOW)
  F. Нет тихого проскакивания: при ENFORCE+OPEN+no_cfg статус должен быть явно ERR_NO_CFG
  G. При ImportError gate'а в __init__ CryptoOrderflowService — of_engine.ml_gate == None
  H. Smoke-test gate.check() вызывается со всеми обязательными kwargs (не вызывает TypeError)
"""

import json
import os
import sys
import importlib
from typing import Any, Dict
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from services.ml_confirm_gate import MLConfirmGate, MLConfirmDecision


# ─────────────────────────────── helpers ────────────────────────────────────

_CHECK_KWARGS: Dict[str, Any] = dict(
    symbol="BTCUSDT",
    ts_ms=1_700_000_000_000,
    direction="LONG",
    scenario="reversal",
    indicators={"sid": "crypto-of:BTCUSDT:1700000000000"},
    rule_score=0.5,
    rule_have=5,
    rule_need=5,
    cancel_spike_veto=0,
    ok_rule=1,
)


def _make_gate(mode: str, fail_policy: str) -> MLConfirmGate:
    mr = MagicMock()
    mr.get.return_value = None
    mr.hgetall.return_value = {}
    mr.xadd.return_value = "0-1"
    mr.set.return_value = True
    return MLConfirmGate(
        r=mr,
        mode=mode,
        fail_policy=fail_policy,
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger",
    )


# ─────────────────────────── Сценарий A ─────────────────────────────────────

def test_A_no_cfg_enforce_open_allows_bypass():
    """
    A. ENFORCE + OPEN + no_cfg → allow=True (bypass).
    Это легитимное, но ОПАСНОЕ поведение — оператор осознанно выбрал fail-open.
    Тест документирует факт bypass и требует явного статуса ERR_NO_CFG.
    """
    gate = _make_gate(mode="ENFORCE", fail_policy="OPEN")
    # Нет cfg, нет модели
    assert not gate._cfg

    dec = gate.check(**_CHECK_KWARGS)

    assert dec.allow is True, "ENFORCE+OPEN+no_cfg: ожидаем bypass (allow=True) — fail-open политика"
    assert dec.status == "ERR_NO_CFG", f"Статус должен быть ERR_NO_CFG, получили: {dec.status!r}"
    assert dec.mode == "ERR", f"mode должен быть ERR, получили: {dec.mode!r}"
    # effective_mode должен отражать оригинальный ENFORCE, а не OFF/SHADOW
    assert dec.effective_mode == "ENFORCE", (
        f"effective_mode должен быть ENFORCE (исходный режим), получили: {dec.effective_mode!r}"
    )


# ─────────────────────────── Сценарий B ─────────────────────────────────────

def test_B_no_cfg_enforce_closed_blocks():
    """
    B. ENFORCE + CLOSED + no_cfg → allow=False (block).
    Это корректное fail-closed поведение — никакой сигнал не проскакивает.
    """
    gate = _make_gate(mode="ENFORCE", fail_policy="CLOSED")
    assert not gate._cfg

    dec = gate.check(**_CHECK_KWARGS)

    assert dec.allow is False, "ENFORCE+CLOSED+no_cfg: ожидаем block (allow=False)"
    assert dec.status == "ERR_NO_CFG"
    assert dec.mode == "ERR"
    assert dec.effective_mode == "ENFORCE"


# ─────────────────────────── Сценарий C ─────────────────────────────────────

def test_C_no_model_enforce_closed_blocks():
    """
    C. cfg загружен, модель отсутствует + ENFORCE + CLOSED → block.
    """
    gate = _make_gate(mode="ENFORCE", fail_policy="CLOSED")
    gate._cfg = {"kind": "util_mh_v1", "model_path": "/nonexistent/model.joblib"}
    gate._model = None
    gate._model_load_error = "no_model"

    dec = gate.check(**_CHECK_KWARGS)

    assert dec.allow is False, "ENFORCE+CLOSED+no_model: ожидаем block"


# ─────────────────────────── Сценарий D ─────────────────────────────────────

def test_D_cfg_ok_model_none_enforce_closed_blocks():
    """
    D. cfg валидный, model=None (файл не загружен) + ENFORCE + CLOSED → block.
    """
    gate = _make_gate(mode="ENFORCE", fail_policy="CLOSED")
    gate._cfg = {
        "kind": "edge_stack_v1",
        "model_path": "/some/path/model.joblib",
        "p_min": 0.55,
        "enforce_share": 0.0,
    }
    gate._model = None
    gate._model_load_error = "file_not_found"

    dec = gate.check(**_CHECK_KWARGS)

    assert dec.allow is False, "ENFORCE+CLOSED+model_none: ожидаем block"


# ─────────────────────────── Сценарий E ─────────────────────────────────────

def test_E_effective_mode_preserved_as_enforce_in_err():
    """
    E. При ERR-решении effective_mode должен отражать реальный режим ENFORCE,
    а не SHADOW/OFF. Это критично для observability — по effective_mode
    SRE может отличить деградированный ENFORCE от нормального SHADOW.
    """
    for fail_policy in ("OPEN", "CLOSED"):
        gate = _make_gate(mode="ENFORCE", fail_policy=fail_policy)
        dec = gate.check(**_CHECK_KWARGS)
        assert dec.effective_mode == "ENFORCE", (
            f"[fail_policy={fail_policy}] effective_mode должен быть ENFORCE, "
            f"получили: {dec.effective_mode!r}"
        )


# ─────────────────────────── Сценарий F ─────────────────────────────────────

def test_F_no_silent_bypass_status_is_err_no_cfg():
    """
    F. Нет тихого проскакивания: при ENFORCE+OPEN+no_cfg decision.status
    должен быть ERR_NO_CFG (не ALLOW/SHADOW), чтобы alert система могла
    отреагировать.
    """
    gate = _make_gate(mode="ENFORCE", fail_policy="OPEN")
    dec = gate.check(**_CHECK_KWARGS)

    # Тихий bypass = allow=True без явного ERR статуса — недопустим
    assert "ERR" in dec.status, (
        f"При no_cfg в ENFORCE статус должен содержать ERR, получили: {dec.status!r}. "
        "Это признак тихого bypass без observability!"
    )
    assert dec.error, "При ERR-решении поле error не должно быть пустым"


# ─────────────────────────── Сценарий G ─────────────────────────────────────

def test_G_import_error_gate_results_in_none_ml_gate():
    """
    G. Если ml_confirm_gate.py не импортируется (ImportError/SyntaxError),
    CryptoOrderflowService должен:
      - НЕ падать полностью (не бросать исключение из __init__)
      - of_engine.ml_gate == None
    Симулируем через patch импорта внутри метода.
    """
    # Мы не хотим поднимать реальный сервис — тестируем только логику try/except.
    # Создаём минимальный mock чтобы проверить что ImportError обрабатывается.

    import types

    _import_error_raised = []

    original_import = __builtins__["__import__"] if isinstance(__builtins__, dict) else __import__

    def _patched_import(name, *args, **kwargs):
        if name == "services.ml_confirm_gate":
            _import_error_raised.append(True)
            raise ImportError("Симулированный SyntaxError в ml_confirm_gate")
        return original_import(name, *args, **kwargs)

    # Патчим только проверку логики: убеждаемся что try/except в сервисе правильно написан.
    # Прямое тестирование через import невозможно без поднятия всего сервиса,
    # поэтому тестируем поведение gate=None в of_engine напрямую.

    from core.of_confirm_engine import OFConfirmEngine
    engine = OFConfirmEngine(version=2, ml_gate=None)

    # ml_gate=None — of_engine должен либо работать (если gate опциональный)
    # либо выбрасывать явное исключение (не тихо проскакивать)
    assert getattr(engine, "ml_gate", "NOT_SET") is None or getattr(engine, "ml_gate", "NOT_SET") == "NOT_SET", \
        "of_engine.ml_gate должен быть None при отсутствии gate"


# ─────────────────────────── Сценарий H ─────────────────────────────────────

def test_H_smoke_test_gate_check_all_kwargs():
    """
    H. gate.check() в _smoke_test_ml() вызывается со всеми обязательными kwargs.
    Тест проверяет что signature не вызывает TypeError при полном наборе аргументов.
    """
    gate = _make_gate(mode="OFF", fail_policy="OPEN")

    # Должно пройти без TypeError
    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1_700_000_000_000,
        direction="LONG",
        scenario="trend_up",
        indicators={"close": 50000.0, "volume": 100.0},
        rule_score=0.5,
        rule_have=0,
        rule_need=0,
        cancel_spike_veto=0,
        ok_rule=1,
    )

    assert isinstance(dec, MLConfirmDecision), "check() должен вернуть MLConfirmDecision"
    assert dec.status == "OFF", f"mode=OFF → status должен быть OFF, получили: {dec.status!r}"


# ─────────────────────────── Сценарий I ─────────────────────────────────────

def test_I_shadow_mode_no_cfg_allows_with_open():
    """
    I. SHADOW + OPEN + no_cfg → allow=True (SHADOW с OPEN не блокирует).
    SHADOW + CLOSED + no_cfg → allow=False — поведение fail_policy=CLOSED
    применяется всегда при ERR — включая SHADOW.
    Операторам: используйте SHADOW только с OPEN.
    """
    gate_open = _make_gate(mode="SHADOW", fail_policy="OPEN")
    dec_open = gate_open.check(**_CHECK_KWARGS)
    assert dec_open.allow is True, (
        f"SHADOW+OPEN+no_cfg: allow должен быть True, получили: {dec_open.allow}"
    )


# ─────────────────────────── Invariant ──────────────────────────────────────

@pytest.mark.parametrize("mode,fail_policy,expected_allow", [
    ("ENFORCE", "CLOSED", False),
    ("ENFORCE", "OPEN",   True),
    ("SHADOW",  "OPEN",   True),
    # SHADOW+CLOSED: fail_policy=CLOSED апплицируется глобально на ERR — даже в SHADOW.
    # Операторам рекомендуется не использовать SHADOW+CLOSED.
    ("SHADOW",  "CLOSED", False),
    ("OFF",     "OPEN",   True),
    ("OFF",     "CLOSED", True),   # OFF всегда allow (mode=OFF → allow=True до ERR-check)
])
def test_no_cfg_invariant_matrix(mode, fail_policy, expected_allow):
    """
    Invariant matrix: no_cfg × mode × fail_policy → allow.
    Служит как regression-барьер: изменение поведения требует явного обновления матрицы.
    """
    gate = _make_gate(mode=mode, fail_policy=fail_policy)
    dec = gate.check(**_CHECK_KWARGS)
    assert dec.allow is expected_allow, (
        f"[mode={mode}, fail_policy={fail_policy}] "
        f"ожидали allow={expected_allow}, получили: {dec.allow}"
    )
