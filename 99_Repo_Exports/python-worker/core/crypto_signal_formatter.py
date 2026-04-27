"""
CryptoSignalFormatter

Унифицированный формат сигналов для криптовалютных инструментов (BTCUSDT, ETHUSDT и т.д.).
Формирует Telegram-сообщение в стиле XAUUSD formatter:

🚨 🔴 BTCUSDT SHORT @ 52000.00, Volume 5.00 USDT
📝 mix:p_delta=0.13,p_speed=0.04,p_cluster=0.07,p_legacy=0.05
🛑 SL 52500.00 | TP1 51000.00 (RR 2.0); TP2 50000.00 (RR 3.3); TP3 49000.00 (RR 4.5)
🕐 21:52:26 07.11.2025 UTC
🔧 Source: CryptoOrderFlow | ID: crypto-of:BTCUSDT:1762552346426
📊 ATR=1.20 | Conf=29%

Примечание: Volume отображается в USDT (размер позиции в долларах), а не в количестве монет.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence
try:
    from core.telegram_confirmations import build_compact_confirmations
except Exception:  # pragma: no cover
    # Safe fallback: formatter must not break signal delivery if the helper is absent.
    def build_compact_confirmations(*, indicators=None, confirmations=None) -> str:
        return ""


@dataclass
class CryptoSignal:
    """Структура данных для крипто сигнала."""

    sid: str
    symbol: str
    side: str
    entry: float
    sl: float
    tp_levels: List[float]
    lot: float
    atr: float
    confidence: float
    ts: int
    source: str
    reason_mix: Dict[str, float] = field(default_factory=dict)
    confirmations: Sequence[str] = field(default_factory=list)
    trail_profile: Optional[str] = None  # Профиль трейлинга (rocket_v1, lock_and_trail, etc.)
    trail_after_tp1: bool = False  # Флаг включения трейлинга после TP1
    position_size_usd: Optional[float] = None  # Размер позиции в USDT
    deposit: Optional[float] = None  # Размер депозита для расчета процента
    leverage: Optional[float] = None  # Плечо для расчета размера с плечом
    config_params: Optional[Dict[str, Any]] = None  # Параметры конфигурации, использованные для генерации сигнала
    indicators: Dict[str, Any] = field(default_factory=dict)  # Индикаторы и gate evidence (включая cancellation spike)
    validation_status: Optional[str] = None  # Статус валидации: "passed", "failed", "bypassed"
    validation_reason: Optional[str] = None  # Причина статуса валидации
    atr_sel_tf: Optional[str] = None  # Выбранный таймфрейм ATR
    atr_sel_age: Optional[int] = None  # Возраст выбранного ATR в мс


class CryptoSignalFormatter:
    """Formatter, формирующий строку для Telegram в едином стиле."""

    SIDE_EMOJI = {
        "LONG": "🟢",
        "SHORT": "🔴",
    }

    DEFAULT_REASON_KEYS = ("p_delta", "p_speed", "p_cluster", "p_legacy")

    @classmethod
    def _smart_format_price(cls, price: float) -> str:
        """
        Форматирует цену умно:
        - < 1.0 -> до 6-8 знаков (чтобы видеть значащие цифры)
        - < 1000 -> 2-4 знака
        - >= 1000 -> 2 знака
        """
        if price == 0:
            return "0.00"
        
        abs_p = abs(price)
        if abs_p < 0.00001:
            return f"{price:.8f}"
        if abs_p < 0.01:
            return f"{price:.6f}"
        if abs_p < 1.0:
            return f"{price:.5f}"
        if abs_p < 10.0:
            return f"{price:.4f}"
        if abs_p < 1000.0:
            return f"{price:.2f}"
        return f"{price:.2f}"

    @classmethod
    def _smart_format_number(cls, num: float) -> str:
        """
        Форматирует большие числа с суффиксами K, M.
        """
        abs_n = abs(num)
        if abs_n >= 1_000_000:
            return f"{num / 1_000_000:.1f}M"
        if abs_n >= 1_000:
            return f"{num / 1_000:.1f}K"
        return f"{num:.2f}"


    @classmethod
    def format_telegram_message(cls, signal: CryptoSignal) -> str:
        """Собирает текст сообщения."""

        direction_emoji = cls.SIDE_EMOJI.get(str(signal.side or "").upper(), "⚪")
        time_str = datetime.fromtimestamp(signal.ts / 1000, tz=timezone.utc).strftime(
            "%H:%M:%S %d.%m.%Y UTC"
        )
        tp_parts: List[str] = []
        stop_dist = abs(signal.entry - signal.sl) or 1e-6
        is_rocket_v1 = (signal.trail_profile == "rocket_v1")

        start_p_str = cls._smart_format_price(signal.entry)
        sl_str = cls._smart_format_price(signal.sl)

        # Добавляем размер стопа в ATR (по запросу)
        if signal.atr > 0:
            # stop_dist вычислен выше (строка 140)
            sl_atr = stop_dist / signal.atr
            sl_str = f"{sl_str} ({sl_atr:.2f} ATR)"

        # Если работал SLQ (умный стоп), показываем "SL {actual} (def {original})"
        if signal.config_params and signal.config_params.get("slq_used") == 1:
             orig_mult = signal.config_params.get("slq_original_mult")
             if orig_mult is not None and signal.atr > 0:
                 # Reconstruct default SL
                 # Assuming default mode was ATR (SLQ only works in ATR mode)
                 def_dist = signal.atr * float(orig_mult)
                 if str(signal.side or "").upper() == "LONG":
                     def_sl = signal.entry - def_dist
                 else:
                     def_sl = signal.entry + def_dist
                 
                 # Если разница существенна, показываем
                 if abs(def_sl - signal.sl) > (signal.entry * 0.0001):
                     def_sl_str = cls._smart_format_price(def_sl)
                     sl_str = f"{sl_str} (def {def_sl_str})"

        for i, tp in enumerate(signal.tp_levels[:3], start=1):
            tp_str = cls._smart_format_price(tp)
            if is_rocket_v1 and i == 1:
                # Для rocket_v1 TP1 показываем динамически рассчитанный (TP-Entry)/ATR
                mult = abs(tp - signal.entry) / (signal.atr if signal.atr > 0 else 1.0)
                tp_parts.append(f"TP{i} {tp_str} ({mult:.2f} ATR)")
            else:
                # Для остальных TP показываем RR (и ATR расстояние)
                rr = abs(tp - signal.entry) / stop_dist
                rr_str = f"RR {rr:.1f}"
                
                if signal.atr > 0:
                    dist_atr = abs(tp - signal.entry) / signal.atr
                    rr_str = f"{rr_str}, {dist_atr:.2f} ATR"
                
                tp_parts.append(f"TP{i} {tp_str} ({rr_str})")

        tp_line = "; ".join(tp_parts) if tp_parts else "TP1 n/a"

        mix_line = cls._format_mix(signal.reason_mix)
        compact = build_compact_confirmations(indicators=signal.indicators, confirmations=signal.confirmations)
        if compact:
            mix_line = f"{mix_line} | {compact}" if mix_line else compact

        trailing_profile = signal.trail_profile or "rocket_v1"
        is_trailing_active = (trailing_profile and str(trailing_profile).lower() != "none")
        
        if is_trailing_active:
            mode_str = "после TP1" if signal.trail_after_tp1 else "активен"
            trailing_line = f"🔄 Trailing Stop: ВКЛ ({mode_str}, профиль: {trailing_profile})"
        else:
            trailing_line = "🔄 Trailing Stop: ВЫКЛ"

        # Рассчитываем процент от депозита и размер с плечом
        deposit_pct_str = ""
        leverage_size_str = ""

        import os
        leverage_val = signal.leverage if signal.leverage is not None else float(os.getenv("ACCOUNT_LEVERAGE", "100"))

        # position_size_usd трактуем как маржу.
        position_size = signal.position_size_usd
        # Fallback: если нет маржи — трактуем lot как маржу в USDT (наследие)
        if position_size is None and signal.lot > 0:
            position_size = signal.lot

        margin_str = f"{position_size:.2f} USDT" if position_size else "n/a"
        nominal_size_str = "n/a"

        if position_size:
            if signal.deposit:
                pct = (position_size / signal.deposit) * 100
                deposit_pct_str = f"{pct:.2f}% dep"
            
            if leverage_val > 1:
                leverage_size_str = f"{leverage_val:.0f}x"
                nominal = position_size * leverage_val
                nominal_size_str = f"{nominal:.2f} USDT"
        
        lot_str = f"{signal.lot} {signal.symbol}" if signal.lot else ""
        position_line = f"Margin {margin_str} ({deposit_pct_str}) Position {nominal_size_str} ({leverage_size_str}) | {lot_str}"

        lines = [
            f"🚨 {direction_emoji} {signal.symbol} {str(signal.side or '').upper()} @ {start_p_str}",
            position_line
        ]
        if mix_line:
            lines.append(f"📝 {mix_line}")
        lines.append(f"🛑 SL {sl_str} | {tp_line}")
        lines.append(trailing_line)
        lines.append(f"🕐 {time_str}")
        lines.append(f"🔧 Source: {signal.source} | ID: {signal.sid}")
        # ATR тоже форматируем умно
        atr_str = cls._smart_format_price(signal.atr)
        
        # Индикаторы для расширенного вывода
        ind = signal.indicators or {}
        atr_bps = ind.get("atr_bps")
        atr_bps_th = ind.get("atr_bps_th")
        tier = ind.get("atr_floor_tier")
        rg = ind.get("atr_floor_rg") or ind.get("atr_gate_rg") # fallback
        
        atr_line = f"📊 ATR={atr_str}"
        if signal.atr_sel_tf:
            age_s = f"{signal.atr_sel_age/1000:.1f}s" if signal.atr_sel_age is not None else "na"
            atr_line += f" [{signal.atr_sel_tf}, age {age_s}]"

        if atr_bps is not None and atr_bps_th is not None:
             tier_str = f"T{tier}" if tier is not None else "T?"
             atr_line += f" ({float(atr_bps):.1f} bps) | Th={float(atr_bps_th):.1f} bps ({tier_str}, {rg})"
        
        atr_line += f" | Conf={int(signal.confidence * 100)}%"
        lines.append(atr_line)

        # Phase E: Telegram compact evidence
        evi = build_compact_confirmations(indicators=signal.indicators, confirmations=signal.confirmations)
        if evi:
            lines.append(f"🧾 {evi}")

        # Добавляем блок с информацией о валидации
        if signal.validation_status:
            status_emoji = {
                "passed": "✅",
                "failed": "❌",
                "bypassed": "⚠️"
            }.get(signal.validation_status, "❓")

            reason_text = f" ({signal.validation_reason})" if signal.validation_reason else ""
            validation_line = f"{status_emoji} Validation: {str(signal.validation_status or '').upper()}{reason_text}"
            lines.append(validation_line)

        # Cancellation Spike Evidence
        if ind.get("cancel_spike_veto") is not None:
            is_veto = int(ind.get("cancel_spike_veto", 0)) == 1
            reason = ind.get("cancel_spike_reason", "unknown")
            ratio = ind.get("cancel_spike_ratio_support", 0.0)
            z = ind.get("cancel_spike_z_support", 0.0)
            bid_ema = ind.get("cancel_spike_bid_rate_ema", 0.0)
            ask_ema = ind.get("cancel_spike_ask_rate_ema", 0.0)
            
            if is_veto:
                lines.append(f"🚫 <b>Cancellation Spike Veto</b>: {reason}")
            else:
                lines.append(f"🔍 <b>Cancellation Spike Monitor</b>: {reason}")
                
            lines.append(f"   ↳ Ratio={ratio:.2f} | Z={z:.2f} | Bid EMA={bid_ema:.2f} | Ask EMA={ask_ema:.2f}")

        # Shadow Mode notice
        is_shadow = os.getenv("ENTRY_POLICY_SHADOW", "0").lower() in ("1", "true", "yes", "on")
        if is_shadow:
            lines.append("👀 <b>Mode: SHADOW</b> (Сделка не будет открыта в мониторе)")

        # Добавляем Strong/Weak статус (если есть)
        strong_ok = -1
        config_params = signal.config_params
        if config_params and "strong_gate_ok" in config_params:
            strong_ok = int(config_params["strong_gate_ok"])
        
        if strong_ok != -1:
            if strong_ok == 1:
                lines.append("✅ <b>Strong (Сильный)</b>: Соответствует критериям гейта.")
            else:
                if signal.confidence >= 0.70:
                    lines.append(f"✅ <b>Strong (High Conf)</b>: Высокая уверенность {int(signal.confidence*100)}% (Gate skipped).")
                else:
                    lines.append("⚠️ <b>Weak (Слабый)</b>: Не дотягивает до критериев.")

        return "\n".join(lines)

    @classmethod
    def _format_mix(cls, reason_mix: Dict[str, float]) -> str:
        """Формирует строку вида mix:p_delta=0.13,..."""

        if not reason_mix:
            return ""
        parts = []
        for key in cls.DEFAULT_REASON_KEYS:
            if key in reason_mix:
                val_str = cls._smart_format_number(reason_mix[key])
                parts.append(f"{key}={val_str}")
        # Добавляем остальные ключи, если есть
        for key, value in reason_mix.items():
            if key not in cls.DEFAULT_REASON_KEYS:
                val_str = cls._smart_format_number(value)
                parts.append(f"{key}={val_str}")
        if not parts:
            return ""
        return f"mix:{','.join(parts)}"
