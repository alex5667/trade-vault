import logging
from dataclasses import dataclass
from typing import Any


@dataclass
class CoinGeckoMacroGateResult:
    risk_off: bool
    alt_weakness: bool
    confidence_penalty: float
    risk_mult: float
    reason: str

class CoinGeckoMacroGate:
    """
    Quality Gate для оценки макро-режима на основе данных CoinGecko.
    Выдает рекомендации по ужесточению порогов входа (confidence_penalty) 
    и снижению риска (risk_mult).
    """

    def __init__(self,
                 default_risk_mult: float = 0.8,
                 default_confidence_penalty: float = 2.0,
                 stable_dom_mom_risk_off_th: float = 0.05,
                 macro_stale_mode: str = "mild_tighten_only"):
        self.default_risk_mult = default_risk_mult
        self.default_confidence_penalty = default_confidence_penalty
        self.stable_dom_mom_risk_off_th = stable_dom_mom_risk_off_th
        self.macro_stale_mode = macro_stale_mode
        self.logger = logging.getLogger("coingecko_macro_gate")

    def evaluate(self, indicators: dict[str, Any], direction: str) -> CoinGeckoMacroGateResult:
        """
        Оценивает переданные индикаторы и возвращает результат.
        В indicators ожидаются ключи, добавленные через CoinGeckoSnapshotReader:
        - cg_stable_dom_mom
        - cg_btc_dom_mom
        - cg_symbol_rel_strength_btc_1h
        """
        res = CoinGeckoMacroGateResult(
            risk_off=False,
            alt_weakness=False,
            confidence_penalty=0.0,
            risk_mult=1.0,
            reason=""
        )

        # Если данных нет или они просрочены (Quality < 0.3), возвращаем fail-open
        q = float(indicators.get("cg_quality", 0.0))
        
        if q < 0.3:
            indicators["macro_gate_reason"] = "cg_missing_fail_open"
            return res
        elif q < 0.8:
            indicators["macro_tighten_add_bps"] = max(indicators.get("macro_tighten_add_bps", 0.0), 1.0)
            indicators["macro_gate_reason"] = "cg_stale_mild_tighten"
            if self.macro_stale_mode == "mild_tighten_only":
                return res
        else:
            indicators["macro_gate_reason"] = "cg_normal"

        stable_dom_mom = float(indicators.get("cg_stable_dom_mom", 0.0) or 0.0)
        btc_dom_mom = float(indicators.get("cg_btc_dom_mom", 0.0) or 0.0)
        rel_str_btc = float(indicators.get("cg_symbol_rel_strength_btc_1h", 0.0) or 0.0)
        sector_mcap_change = float(indicators.get("cg_sector_mcap_change_24h", 0.0) or 0.0)

        # Helper calculations
        def clamp01(v: float) -> float:
            return max(0.0, min(1.0, v))

        def pos_norm(v: float, base: float) -> float:
            if v <= 0: return 0.0
            return clamp01(v / base)

        stable_risk = pos_norm(stable_dom_mom, 0.05)
        btc_risk = pos_norm(btc_dom_mom, 0.05)

        # Leverage stress is handled by Binance-native V13 now
        deriv_stress = 0.0

        # Sector weakness: negative 24h change is weak
        sector_weakness = pos_norm(-sector_mcap_change, 5.0)

        cg_macro_risk_score = clamp01(
            (stable_risk * 0.4) +
            (btc_risk * 0.3) +
            (deriv_stress * 0.2) +
            (sector_weakness * 0.1)
        )

        indicators["cg_macro_risk_score"] = cg_macro_risk_score

        reasons = []

        # 1. Оценка перетока в стейблкоины (Risk-Off)
        # Если доминация стейблов растет, а мы хотим купить (BUY), это опасно
        if stable_dom_mom > self.stable_dom_mom_risk_off_th and direction == "BUY":
            res.risk_off = True
            res.confidence_penalty += self.default_confidence_penalty
            res.risk_mult = min(res.risk_mult, self.default_risk_mult)
            reasons.append(f"RiskOff(stable_mom={stable_dom_mom:.3f})")

        # 2. Оценка слабости альткоинов при росте доминации битка
        # Если биток доминирует, а альт падает относительно битка, покупать альт опасно
        if btc_dom_mom > 0 and rel_str_btc < 0 and direction == "BUY":
            res.alt_weakness = True
            res.confidence_penalty += self.default_confidence_penalty
            res.risk_mult = min(res.risk_mult, self.default_risk_mult)
            reasons.append(f"AltWeakness(btc_mom={btc_dom_mom:.3f}, rel_str={rel_str_btc:.3f})")

        # 3. Комплексная оценка Macro Risk Score
        # Добавляем серьезный штраф при высоком общем риске рынка
        if cg_macro_risk_score >= 0.70 and direction == "BUY" and rel_str_btc < 0:
            res.risk_off = True
            res.confidence_penalty += 3.0
            res.risk_mult = min(res.risk_mult, 0.5)
            reasons.append(f"HighMacroRisk(score={cg_macro_risk_score:.2f})")

        if reasons:
            res.reason = " | ".join(reasons)

        return res
