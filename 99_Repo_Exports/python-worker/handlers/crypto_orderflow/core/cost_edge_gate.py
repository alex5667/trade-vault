from __future__ import annotations

"""
Фильтр Cost Edge Gate - Предотвращает торговлю ниже транзакционных издержек.

Этот модуль реализует фильтр, который отклоняет сигналы, где ожидаемая прибыль
недостаточно превышает транзакционные издержки (комиссии + проскальзывание).

Математическое обоснование:
    expected_edge_bps > (fees_bps + slippage_bps) * cost_multiplier
    
    Где:
    - expected_edge_bps: Ожидаемое движение цены в базисных пунктах
    - fees_bps: Торговые комиссии за круг (вход + выход)
    - slippage_bps: Ожидаемая стоимость проскальзывания
    - cost_multiplier: Фактор безопасности (обычно 3-5x)

Дизайн:
    - Специфичные для символа пороги (напр., строже для BTC/ETH)
    - Множественные методы оценки эджа (TP1, R:R, ATR)
    - Оценка проскальзывания на основе спреда
    - Детальное логирование решений вето

ENV Конфигурация:
    EDGE_COST_GATE_ENABLED: Включить/выключить фильтр (1/0)
    EDGE_COST_K: Дефолтный множитель затрат (напр. 4.0)
    EDGE_COST_K_BTCUSDT: Специфичный множитель для BTC (напр. 5.0)
    EDGE_COST_K_ETHUSDT: Специфичный множитель для ETH (напр. 4.5)
    EDGE_FEES_BPS_DEFAULT: Дефолтные комиссии в bps (напр. 8.0 за круг)
    EDGE_SLIPPAGE_BPS_DEFAULT: Дефолтная оценка проскальзывания (напр. 4.0)
    EDGE_SLIPPAGE_USE_SPREAD_HALF: Использовать 0.5 * спред как проскальзывание (1/0)
    EDGE_EXPECTED_MOVE_MODE: Метод оценки эджа (tp1|rr|atr)
    LOG_EDGE_VETO: Включить детальное логирование (1/0)
"""


import math
import os
from dataclasses import dataclass
from typing import Any
import contextlib

EPS_BPS = 0.1  # Порог равенства для сравнения float

def _isfinite(x: Any) -> bool:
    """Проверяет, является ли число конечным float."""
    try:
        f = float(x)
        return math.isfinite(f)
    except (TypeError, ValueError):
        return False

@dataclass
class CostEdgeConfig:
    """Конфигурация для фильтра Cost Edge Gate."""

    # Включить/выключить фильтр
    enabled: bool = True

    # Множитель затрат - требуемый эдж должен превышать затраты * K
    default_cost_k: float = 4.0
    symbol_cost_k: dict[str, float] = None  # Специфичные для символа множители

    # Торговые издержки в базисных пунктах (bps)
    fees_bps: float = 4.0  # Комиссия за круг (вход + выход)
    slippage_bps: float = 4.0  # Ожидаемое проскальзывание
    slippage_use_spread_half: bool = True  # Использовать 0.5 * спред как оценку проскальзывания

    # Buffer BPS (дополнительный запас, не входящий в slippage)
    buffer_bps: float = 0.0
    symbol_buffer_bps: dict[str, float] = None

    # Метод оценки эджа
    edge_mode: str = "tp1"  # tp1 | rr | atr

    # Логирование
    log_veto: bool = True

    def __post_init__(self):
        if self.symbol_cost_k is None:
            self.symbol_cost_k = {}
        if self.symbol_buffer_bps is None:
            self.symbol_buffer_bps = {}

    @classmethod
    def from_env(cls) -> CostEdgeConfig:
        """Создает конфигурацию из переменных окружения."""

        # Парсим флаг включения
        enabled = bool(int(os.getenv("EDGE_COST_GATE_ENABLED", "1")))

        # Парсим множители затрат
        default_k = float(os.getenv("EDGE_COST_K", "4.0"))
        symbol_k = {}

        # Парсим буфер
        default_buffer = float(os.getenv("EDGE_COST_BUFFER_BPS", "0.0"))
        symbol_buffer = {}

        # Специфичные для символа настройки (K и Buffer)
        for key, value in os.environ.items():
            if key.startswith("EDGE_COST_K_") and key != "EDGE_COST_K":
                symbol = key.replace("EDGE_COST_K_", "").upper()
                with contextlib.suppress(ValueError, TypeError):
                    symbol_k[symbol] = float(value)
            elif key.startswith("EDGE_COST_BUFFER_BPS_") and key != "EDGE_COST_BUFFER_BPS":
                symbol = key.replace("EDGE_COST_BUFFER_BPS_", "").upper()
                with contextlib.suppress(ValueError, TypeError):
                    symbol_buffer[symbol] = float(value)

        # Парсим компоненты затрат
        fees_bps = float(os.getenv("EDGE_FEES_BPS_DEFAULT", "4.0"))
        slippage_bps = float(os.getenv("EDGE_SLIPPAGE_BPS_DEFAULT", "4.0"))
        use_spread = bool(int(os.getenv("EDGE_SLIPPAGE_USE_SPREAD_HALF", "1")))

        # Парсим режим эджа
        edge_mode = os.getenv("EDGE_EXPECTED_MOVE_MODE", "tp1").lower()

        # Парсим флаг логирования
        log_veto = bool(int(os.getenv("LOG_EDGE_VETO", "1")))

        return cls(
            enabled=enabled,
            default_cost_k=default_k,
            symbol_cost_k=symbol_k,
            fees_bps=fees_bps,
            slippage_bps=slippage_bps,
            slippage_use_spread_half=use_spread,
            buffer_bps=default_buffer,
            symbol_buffer_bps=symbol_buffer,
            edge_mode=edge_mode,
            log_veto=log_veto,
        )


@dataclass
class CostEdgeResult:
    """Результат оценки cost edge gate."""

    passed: bool  # True если сигнал проходит фильтр
    expected_edge_bps: float  # Ожидаемое движение цены (bps)
    total_costs_bps: float  # Общие транзакционные издержки (bps)
    cost_multiplier: float  # Примененный множитель затрат (K)
    required_edge_bps: float  # Минимальный требуемый эдж (costs * K)
    edge_ratio: float  # Отношение эджа к требуемому (>1.0 для прохода)

    # Разбив затрат
    fees_bps: float
    slippage_bps: float
    buffer_bps: float

    # Metadata
    symbol: str
    edge_source: str  # Method used for edge estimation (tp1/rr/atr)
    veto_reason: str | None = None

    # NEW: короткий код для метрик/БД
    reason_code: str = "OK"

    @property
    def veto(self) -> bool:
        return not self.passed

    def __str__(self) -> str:
        """Structured log string representation."""
        status = "PASS" if self.passed else "VETO"
        return (
            f"CostEdge {status} symbol={self.symbol} "
            f"exp_bps={self.expected_edge_bps:.1f} req_bps={self.required_edge_bps:.1f} "
            f"k={self.cost_multiplier:.1f} "
            f"fees_bps={self.fees_bps:.1f} slip_bps={self.slippage_bps:.1f} "
            f"buf_bps={self.buffer_bps:.1f} total_costs_bps={self.total_costs_bps:.1f} "
            f"ratio={self.edge_ratio:.2f} src={self.edge_source}"
        )


class CostEdgeGate:
    """
    Фильтр, который отклоняет сигналы, где ожидаемый эдж не превышает затраты.
    
    Использование:
        gate = CostEdgeGate.from_env()
        result = gate.evaluate(ctx, symbol="BTCUSDT", entry_price=50000.0)
        logger.info(str(result))
        if not result.passed:
            # Отклоняем сигнал
            return
    """

    def __init__(self, config: CostEdgeConfig):
        self.config = config

    @classmethod
    def from_env(cls) -> CostEdgeGate:
        """Создает гейт из переменных окружения."""
        return cls(CostEdgeConfig.from_env())

    def _get_cost_multiplier(self, symbol: str) -> float:
        """Возвращает множитель затрат для символа."""
        # Возвращаем сырое значение, clamp будет в evaluate
        return self.config.symbol_cost_k.get(symbol.upper(), self.config.default_cost_k)

    def _get_buffer_bps(self, symbol: str) -> float:
        """Возвращает buffer bps для символа."""
        # Возвращаем сырое значение, clamp будет в evaluate
        return self.config.symbol_buffer_bps.get(symbol.upper(), self.config.buffer_bps)

    def _estimate_slippage_bps(self, ctx: Any, entry_price: float) -> float:
        """
        Оценивает проскальзывание в базисных пунктах.
        
        Если EDGE_SLIPPAGE_USE_SPREAD_HALF=1, использует 0.5 * спред.
        Иначе, использует настроенное значение по умолчанию.
        """
        if not self.config.slippage_use_spread_half:
            return self.config.slippage_bps

        # Пытаемся получить спред из контекста
        spread = None

        # Проверяем различные возможные местоположения данных спреда
        if hasattr(ctx, "spread_bps"):
            val = getattr(ctx, "spread_bps", 0.0)
            if val is not None:
                with contextlib.suppress(ValueError, TypeError):
                    spread = float(val)
        elif hasattr(ctx, "bid") and hasattr(ctx, "ask"):
            try:
                bid = float(getattr(ctx, "bid", 0.0))
                ask = float(getattr(ctx, "ask", 0.0))
                if bid > 0 and ask > 0 and entry_price > 0:
                    spread = ((ask - bid) / entry_price) * 10000.0  # Конвертируем в bps
            except (TypeError, ValueError):
                pass

        # Если спред доступен, используем половину как оценку проскальзывания
        if spread is not None and _isfinite(spread) and spread > 0:
            return spread * 0.5

        # Фоллбек на настроенное значение по умолчанию
        return self.config.slippage_bps

    def _estimate_edge_bps(self, ctx: Any, symbol: str, entry_price: float) -> tuple[float, str]:
        """
        Оценивает ожидаемое движение цены в базисных пунктах.
        
        Returns:
            (edge_bps, source_method)
        """
        mode = self.config.edge_mode

        try:
            entry_price = float(entry_price)
            if not _isfinite(entry_price) or entry_price <= 0:
                return 0.0, "bad_price"
        except (TypeError, ValueError):
            return 0.0, "bad_price"

        # Метод 1: Дистанция до TP1
        if mode == "tp1":
            tp1_price = getattr(ctx, "tp1", None) or (getattr(ctx, "tp_levels", [None]) or [None])[0]
            if tp1_price:
                try:
                    tp1_price = float(tp1_price)
                    if _isfinite(tp1_price) and tp1_price > 0:
                        side = getattr(ctx, "side", "LONG")
                        if side == "LONG":
                            move_bps = ((tp1_price - entry_price) / entry_price) * 10000.0
                        else:  # SHORT
                            move_bps = ((entry_price - tp1_price) / entry_price) * 10000.0

                        if move_bps > 0:
                            return abs(move_bps), "tp1"
                except (TypeError, ValueError):
                    pass

        # Метод 2: Отношение Риск:Прибыль (R:R)
        if mode == "rr" or mode == "tp1":  # Fallback from tp1
            rr = getattr(ctx, "rr", None) or (getattr(ctx, "tp_rr", [None]) or [None])[0]
            sl_price = getattr(ctx, "sl", None)

            if rr is not None and sl_price is not None:
                try:
                    rr = float(rr)
                    sl_price = float(sl_price)
                    if _isfinite(rr) and _isfinite(sl_price) and sl_price > 0:
                        side = getattr(ctx, "side", "LONG")
                        if side == "LONG":
                            risk_bps = ((entry_price - sl_price) / entry_price) * 10000.0
                        else:  # SHORT
                            risk_bps = ((sl_price - entry_price) / entry_price) * 10000.0

                        if risk_bps > 0:
                            edge_bps = abs(risk_bps) * rr
                            return edge_bps, "rr"
                except (TypeError, ValueError):
                    pass

        # Метод 3: Оценка на основе ATR
        if mode == "atr" or mode == "tp1" or mode == "rr":  # Last fallback
            atr = getattr(ctx, "atr", None)
            if atr:
                try:
                    atr = atr
                    if _isfinite(atr) and atr > 0:
                        # Assume TP1 is typically 0.5-1.0 * ATR
                        atr_mult = float(getattr(ctx, "tp1_atr_mult", 0.8))
                        edge_bps = (atr * atr_mult / entry_price) * 10000.0
                        return abs(edge_bps), "atr"
                except (TypeError, ValueError):
                    pass

        # Оценка эджа недоступна
        return 0.0, "none"

    def evaluate(
        self,
        ctx: Any,
        symbol: str,
        entry_price: float,
    ) -> CostEdgeResult:
        """
        Оценивает, проходит ли сигнал через cost edge gate.
        
        Args:
            ctx: Контекст сигнала с ценовыми уровнями, ATR и т.д.
            symbol: Торговый символ (напр., "BTCUSDT")
            entry_price: Предлагаемая цена входа
            
        Returns:
            CostEdgeResult с решением pass/fail и детализацией
        """

        # Подготовка и валидация входных данных
        cost_k = float(self._get_cost_multiplier(symbol))
        buffer_bps = float(self._get_buffer_bps(symbol))

        # 1. Hard clamps для конфигурации
        # Если cost_k недопустимый, сбрасываем на дефолт (защита от <= 0)
        if not _isfinite(cost_k) or cost_k <= 0:
            cost_k = float(self.config.default_cost_k)
            # Если и дефолт сломан, форсируем 4.0
            if cost_k <= 0:
                cost_k = 4.0

        # Buffer не может быть отрицательным (защита от "чит-кода")
        buffer_bps = 0.0 if (not _isfinite(buffer_bps) or buffer_bps < 0) else buffer_bps

        # 2. Оценка затрат и очистка
        fees_bps = float(self.config.fees_bps)
        slippage_bps = float(self._estimate_slippage_bps(ctx, entry_price))

        # Sanitize bps values (защита от NaN/Negative)
        if not _isfinite(fees_bps) or fees_bps < 0:
            fees_bps = 0.0
        if not _isfinite(slippage_bps) or slippage_bps < 0:
            slippage_bps = 0.0

        total_costs_bps = fees_bps + slippage_bps + buffer_bps
        required_edge_bps = total_costs_bps * cost_k

        # 3. Оценка эджа и очистка
        edge_bps, edge_source = self._estimate_edge_bps(ctx, symbol, entry_price)
        edge_bps = float(edge_bps) if _isfinite(edge_bps) else 0.0
        edge_source = edge_source or "none"

        # 4. Расчет edge ratio
        # Если required=0, то любой положительный edge дает бесконечный ratio
        if required_edge_bps > 0:
            edge_ratio = edge_bps / required_edge_bps
        else:
            edge_ratio = float("inf") if edge_bps > 0 else 0.0

        # 5. Принятие решения (с использованием Epsilon для стабильности на границах)
        # passed = edge >= required (с допуском EPS_BPS)
        passed = (edge_bps + EPS_BPS) >= required_edge_bps

        reason_code = "OK"

        if not passed:
            if edge_source == "none":
                reason_code = "VETO_COST_NO_EDGE"
            elif edge_source == "bad_price":
                 reason_code = "VETO_COST_BAD_INPUT"
            elif edge_bps < required_edge_bps:
                reason_code = "VETO_COST_LT_REQUIRED"
            else:
                reason_code = "VETO_COST"

        # Формируем структурную строку для __str__ и (опционально) для veto_reason
        # Чтобы не дублировать логику, создадим объект результата, а затем, если нужно, выдернем str()

        res = CostEdgeResult(
            passed=passed,
            expected_edge_bps=edge_bps,
            total_costs_bps=total_costs_bps,
            cost_multiplier=cost_k,
            required_edge_bps=required_edge_bps,
            edge_ratio=edge_ratio,
            fees_bps=fees_bps,
            slippage_bps=slippage_bps,
            buffer_bps=buffer_bps,
            symbol=symbol,
            edge_source=edge_source,
            veto_reason=None, # Заполним ниже с использованием str(res)
            reason_code=reason_code
        )

        if not passed:
             if edge_source == "none" or edge_source == "bad_price":
                res.veto_reason = "no_edge_estimate_available"
             else:
                # Используем структурное представление для veto_reason
                res.veto_reason = str(res)

        return res


def safe_float(val: Any, default: float = 0.0) -> float:
    """Безопасно конвертирует значение в float."""
    try:
        f = float(val)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default

