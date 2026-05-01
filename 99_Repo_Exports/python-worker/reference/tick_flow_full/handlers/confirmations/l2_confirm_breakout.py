from __future__ import annotations
import math
from dataclasses import dataclass
from typing import Any, Optional

from .result import ConfirmResult
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Snapshot, L2Level
from .reason_utils import normalize_and_u16

# Структурированные коды причин живут в signal_scoring/reason_registry.py (единственный источник правды).
# Мы храним "строки" здесь, чтобы избежать импорта множества enum по всему репозиторию.
VETO_WALL_NEAR = "VETO_WALL_NEAR"
OK = "OK"

def _f(x: Any) -> Optional[float]:
    try:
        v = float(x)
    except Exception:
        return None
    if not math.isfinite(v):
        return None
    return v

@dataclass
class BreakoutConfirmCfg:
    l2_stale_ms: int = 1500
    min_wall_notional: float = 25_000.0
    max_near_wall_bps: float = 4.0

class L2ConfirmBreakout:
    """
    Валидатор L2 breakout без привязки к хендлеру.
    Возвращает структурированные флаги; kind_rules.py использует флаги для эвристик ложного пробоя.

    ЖЁСТКАЯ УНИФИКАЦИЯ (вариант B):
      near_big_wall (wall_near) => VETO, а не soft-fail.
    Почему:
      - для breakout это чаще структурно плохая сделка (пробой упирается в крупный оффер/бид),
      - нужен стабильный reason_code="VETO_WALL_NEAR" для калибровки/метрик,
      - дальнейший скоринг не должен "спасать" такие кандидаты.
    """
    def __init__(self, cfg: Optional[BreakoutConfirmCfg] = None, **kwargs: Any) -> None:
        if cfg is None and kwargs:
            cfg = BreakoutConfirmCfg(
                max_near_wall_bps=kwargs.get("wall_within_bps", 4.0),
                min_wall_notional=kwargs.get("min_wall_notional", 25_000.0),
            )
        self.cfg = cfg or BreakoutConfirmCfg()

    def _get_l2(self, ctx: Any) -> Optional[L2Snapshot]:
        return getattr(ctx, "l2", None) or getattr(ctx, "l2_snapshot", None) or getattr(ctx, "book", None)

    def _is_stale(self, ctx: Any) -> bool:
        ts = _f(getattr(ctx, "ts_ms", None))
        l2_ts = _f(getattr(ctx, "l2_ts_ms", None))
        if ts is None or l2_ts is None:
            return False
        return (ts - l2_ts) > float(self.cfg.l2_stale_ms)

    def confirm(
        self,
        *,
        ctx: Any,
        side: int | str,
        level_price: float,
        l2: Optional[L2Snapshot] = None,
        **_: Any,  # допускаем лишние kwargs от оберток/движков при рефакторинге
    ) -> ConfirmResult:
        if isinstance(side, int):
            side = "buy" if side > 0 else "sell"
        side = side.lower()
        flags: dict[str, Any] = {}
        reasons: list[str] = []
        parts: dict[str, Any] = {}

        if self._is_stale(ctx):
            flags["l2_stale"] = True
            reasons.append("l2_stale")
            parts["l2_stale_ms"] = float(self.cfg.l2_stale_ms)
            # Политика Breakout: stale L2 это fail-closed в ConfirmationsEngine,
            # но оставляем этот валидатор явным и структурированным тоже.
            rc, u16 = normalize_and_u16("VETO_L2_STALE")
            return ConfirmResult(
                passed=False,
                veto=True,
                score01=0.0,
                reason_code=rc,
                reason_u16=u16,
                parts=parts,
                flags=flags,
                reasons=reasons,
            )

        # разрешаем явную инъекцию (тесты / движок предоставляет l2), иначе читаем из ctx
        if l2 is None:
            l2 = self._get_l2(ctx)
        if l2 is None:
            # fail-open: нет L2
            flags["l2_missing"] = True
            parts["l2_missing"] = 1
            rc, u16 = normalize_and_u16(OK)
            return ConfirmResult(passed=True, veto=False, score01=1.0, reason_code=rc, reason_u16=u16, parts=parts, flags=flags, reasons=reasons)

        px = _f(getattr(ctx, "price", None) or getattr(ctx, "last_price", None))
        lvl = _f(level_price)
        if px is None or lvl is None or lvl <= 0:
            flags["bad_inputs"] = True
            parts["bad_inputs"] = 1
            rc, u16 = normalize_and_u16(OK)
            return ConfirmResult(passed=True, veto=False, score01=1.0, reason_code=rc, reason_u16=u16, parts=parts, flags=flags, reasons=reasons)

        # Детекция "близкой стены" сразу после пробоя: большая противоположная стена слишком близко снижает качество.
        near_wall_bps = None
        wall_notional = None
        if side.lower() in ("buy", "up", "long"):
            # после пробоя вверх, ближайшая стена асков выше уровня
            asks = getattr(l2, "asks", None) or []
            best = None
            for lv in asks:
                if not isinstance(lv, L2Level):
                    continue
                if lv.price is None:
                    continue
                if lv.price >= lvl:
                    if best is None or lv.price < best.price:
                        best = lv
            if best is not None:
                wall_notional = _f(getattr(best, "notional", None)) or _f(getattr(best, "price", 0.0)) * (_f(getattr(best, "size", 0.0)) or 0.0)
                near_wall_bps = abs(best.price - lvl) / lvl * 10_000.0
        else:
            # после пробоя вниз, ближайшая стена бидов ниже уровня
            bids = getattr(l2, "bids", None) or []
            best = None
            for lv in bids:
                if not isinstance(lv, L2Level):
                    continue
                if lv.price is None:
                    continue
                if lv.price <= lvl:
                    if best is None or lv.price > best.price:
                        best = lv
            if best is not None:
                wall_notional = _f(getattr(best, "notional", None)) or _f(getattr(best, "price", 0.0)) * (_f(getattr(best, "size", 0.0)) or 0.0)
                near_wall_bps = abs(lvl - best.price) / lvl * 10_000.0

        if near_wall_bps is not None:
            flags["near_wall_bps"] = near_wall_bps
            parts["near_wall_bps"] = float(near_wall_bps)
        if wall_notional is not None:
            flags["near_wall_notional"] = wall_notional
            parts["near_wall_notional"] = float(wall_notional)

        # 9.4/9.x (Вариант B "жёстче"):
        # Для breakout это структурно плохо (цена пробивается в большую противоположную стену).
        # Делаем это VETO со стабильным структурированным reason_code.
        if (near_wall_bps is not None and wall_notional is not None) and (wall_notional >= self.cfg.min_wall_notional) and (near_wall_bps <= self.cfg.max_near_wall_bps):
            flags["near_big_wall"] = True
            reasons.append("near_big_wall")
            parts["min_wall_notional"] = float(self.cfg.min_wall_notional)
            parts["max_near_wall_bps"] = float(self.cfg.max_near_wall_bps)
            rc, u16 = normalize_and_u16("VETO_WALL_NEAR")
            return ConfirmResult(
                passed=False,
                veto=True,
                flags=flags,
                reasons=reasons,
                score01=0.0,
                reason_code=rc,
                reason_u16=u16,
            )

        rc, u16 = normalize_and_u16(OK)
        return ConfirmResult(passed=True, veto=False, score01=1.0, reason_code=rc, reason_u16=u16, parts=parts, flags=flags, reasons=reasons)
