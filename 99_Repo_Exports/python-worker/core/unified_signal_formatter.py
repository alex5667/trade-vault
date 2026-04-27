from utils.time_utils import get_ny_time_millis
"""
Унифицированный форматер сигналов для всех торговых инструментов.

Заменяет специфичные форматеры (xauusd_signal_formatter.py) на универсальный,
который автоматически адаптируется под тип инструмента.
"""

import time
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional, Sequence
from .confidence_utils import normalize_confidence_pct, confidence_pct_to_ratio
from .instrument_config import get_specs, SymbolSpecs
from .crypto_signal_formatter import CryptoSignal, CryptoSignalFormatter
from common.qf_codes import unpack_qf_u16, qf_labels_from_codes
from signal_scoring.reason_registry import u16_to_reason_code
from signal_scoring.wire_u16 import unpack_u16
from signal_scoring.reason_codes import legacy_reason_to_code


@dataclass
class Signal:
    """
    Унифицированная структура торгового сигнала.

    Универсальна для всех типов инструментов (XAUUSD, Crypto, Forex и т.д.)
    """
    sid: str                        # Уникальный ID сигнала
    symbol: str                     # Символ инструмента (XAUUSD, BTCUSD и т.д.)
    side: str                       # Направление: LONG | SHORT
    entry: float                    # Цена входа
    sl: float                       # Stop Loss
    tp_levels: List[float]          # Take Profit уровни (массив)
    lot: float                      # Размер позиции (лот или USDT для крипты)
    source: str                     # Источник сигнала (OrderFlow, TA, etc)
    reason: str                     # Причина/описание сигнала
    confidence: float               # Уверенность в сигнале (0-100)
    atr: float                      # Текущий ATR
    ts: int                         # Timestamp (milliseconds)
    indicators: Dict[str, Any]      # Дополнительные индикаторы
    metadata: Optional[Dict[str, Any]] = None  # Метаданные (symbol specs и т.д.)
    entry_tag: str = ""             # Тег типа входа для агрегации метрик
    trail_after_tp1: bool = False   # Включить трейлинг после TP1
    trail_profile: str = "rocket_v1"  # Профиль трейлинга
    position_size_usd: Optional[float] = None  # Размер позиции в USDT (для крипты)
    deposit: Optional[float] = None  # Размер депозита для расчета процента
    leverage: Optional[float] = None  # Плечо для расчета размера с плечом
    signal_settings: Optional[Dict[str, Any]] = None  # Настройки, использованные при генерации сигнала
    
    def __post_init__(self):
        """Автоматическое заполнение metadata из SymbolSpecs"""
        if self.metadata is None:
            try:
                specs = get_specs(self.symbol)
                self.metadata = {
                    "contract_size": specs.contract_size,
                    "lot_step": specs.lot_step,
                    "price_decimals": specs.price_decimals,
                    "volume_decimals": specs.volume_decimals
                }
            except ValueError:
                # Символ не найден в реестре - используем defaults
                self.metadata = {}


class UnifiedSignalFormatter:
    """
    Универсальный форматер сигналов для всех инструментов.
    
    Автоматически адаптирует формат под тип инструмента (количество знаков,
    единицы измерения, стиль сообщения и т.д.)
    """
    
    @staticmethod
    def _safe_float(v: Any) -> float:
        try:
            if v is None:
                return float("nan")
            return float(v)
        except Exception:
            return float("nan")

    @staticmethod
    def _clamp01(v: float, hi: float = 0.99) -> float:
        import math
        if math.isnan(v) or math.isinf(v):
            return 0.0
        if v < 0.0:
            v = -v
        if v > hi:
            return hi
        return v

    @staticmethod
    def normalize_confidence_pct(conf: Any) -> Any:
        # Tuple[float, float]
        # Returns: (confidence_pct in 0..100, confidence_ratio in 0..1)
        import math
        x = UnifiedSignalFormatter._safe_float(conf)
        if math.isnan(x) or math.isinf(x):
            return 0.0, 0.0
        pct = max(0.0, min(100.0, x))
        ratio = pct / 100.0
        return pct, ratio

    @staticmethod
    def create_signal_id(symbol: str, side: str, price: float, ts: int) -> str:
        """
        Генерирует уникальный ID сигнала.
        
        Формат: {SYMBOL}:{SIDE}:{PRICE_INT}:{TIMESTAMP}
        
        Args:
            symbol: Символ инструмента
            side: Направление (LONG/SHORT)
            price: Цена входа
            ts: Timestamp в миллисекундах
            
        Returns:
            Уникальный ID сигнала
        """
        price_int = int(price * 100)  # Умножаем на 100 для уникальности
        return f"{symbol}:{side}:{price_int}:{ts}"
    
    @staticmethod
    def format_redis_payload(signal: Signal) -> Dict[str, str]:
        """
        Формирует payload для публикации в Redis Stream.
        
        Args:
            signal: Объект Signal
            
        Returns:
            Словарь с полями для XADD в Redis
        """
        message = UnifiedSignalFormatter.format_telegram_message(signal)
        
        # Calculate confidence values safely
        raw_conf = getattr(signal, "confidence", None)
        if hasattr(signal, "confidence_pct"):
             raw_conf = signal.confidence_pct
        confidence_pct, confidence_ratio = UnifiedSignalFormatter.normalize_confidence_pct(raw_conf)

        payload = {
            "sid": signal.sid,
            "symbol": signal.symbol,
            "side": signal.side,
            "entry": str(signal.entry),
            "sl": str(signal.sl),
            "tp_levels": ",".join(str(tp) for tp in signal.tp_levels),
            "lot": str(signal.lot),
            "source": signal.source,
            "reason": signal.reason,
            "confidence_pct": f"{confidence_pct:.2f}",
            "confidence": f"{confidence_pct:.2f}",
            "confidence_ratio": f"{confidence_ratio:.4f}",
            "atr": str(signal.atr),
            "ts": str(signal.ts),
            "trail_after_tp1": str(signal.trail_after_tp1).lower(),
            "trail_profile": signal.trail_profile,
            "text": message,
        }
        
        # ✅ Для крипты добавляем position_size_usd
        if signal.position_size_usd is not None:
            payload["position_size_usd"] = str(signal.position_size_usd)
        
        # Добавляем индикаторы (сериализуем)
        indicators = getattr(signal, "indicators", None) or {}
        mix = UnifiedSignalFormatter._build_mix_dict(signal, [])
        payload["mix_p_delta"] = f"{mix.get('p_delta', 0.0):.2f}"
        payload["mix_p_speed"] = f"{mix.get('p_speed', 0.0):.2f}"

        for key, value in indicators.items():
            if isinstance(value, (int, float, str, bool)):
                payload[f"ind_{key}"] = str(value)
        
        # Добавляем metadata
        if signal.metadata:
            for key, value in signal.metadata.items():
                payload[f"meta_{key}"] = str(value)

        # Expand QF codes -> labels ONLY here (publisher boundary).
        try:
            labels = dict(getattr(signal, "labels", None) or {})
            # Prefer packed format if present.
            qf_codes = []
            qf16 = getattr(signal, "qf16", None)
            if qf16:
                qf_codes = unpack_qf_u16(qf16)
            else:
                qf_codes = list(getattr(signal, "qf", None) or getattr(signal, "qf_codes", None) or [])
            if qf_codes:
                labels.update(qf_labels_from_codes(qf_codes))
            signal.labels = labels
        except Exception:
            # fail-open: do not break formatting
            pass

        return payload

    @staticmethod
    def format(payload: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(payload)

        # Prefer compact rc16/rc over string reason_code.
        # Это позволяет постепенно выключить RC_KEEP_STR в outbox, не ломая downstream.
        if not out.get("reason_code"):
            rc16 = out.get("rc16")
            rc = out.get("rc")
            v = None
            if rc16:
                v = unpack_u16(str(rc16))
            if v is None and rc is not None:
                try:
                    v = int(rc)
                except Exception:
                    v = None
            if v is not None:
                out["reason_code"] = u16_to_reason_code(v)

        # Last-chance fallback: legacy "reason" field (старые payload'ы/ветки).
        if not out.get("reason_code") and out.get("reason"):
            try:
                out["reason_code"] = legacy_reason_to_code(str(out["reason"]))
            except Exception:
                pass

        # Optional: expose numeric reason too (useful for debugging, but можно удалять в финальном паблишере)
        # if "rc" in out: keep

        # existing formatting...
        # confidence normalization etc.
        return out

    @staticmethod
    def format_telegram_message(signal: Signal, emoji: str = "🚨") -> str:
        """
        Формирует сообщение для Telegram.
        
        Автоматически адаптирует формат под тип инструмента:
        - Для XAUUSD/XAGUSD: 2-3 знака после запятой
        - Для Crypto (high price): 2 знака ($50000.00)
        - Для Crypto (low price): 4 знака ($0.0001)
        
        Args:
            signal: Объект Signal
            emoji: Эмодзи для сообщения
            
        Returns:
            Форматированное сообщение для Telegram
        """
        symbol_upper = signal.symbol.upper()

        is_crypto = symbol_upper.endswith("USDT") or symbol_upper in {"BTCUSD", "ETHUSD"}
        is_precious = symbol_upper in {"XAUUSD", "XAGUSD"}
        if is_crypto or is_precious:
            import os
            
            timestamp_ms = signal.ts or get_ny_time_millis()
            base_confirmations = UnifiedSignalFormatter._extract_confirmations(signal)
            reason_confirmation = UnifiedSignalFormatter._reason_to_confirmation(signal.reason)
            if reason_confirmation:
                base_confirmations.append(reason_confirmation)
            mix_dict = UnifiedSignalFormatter._build_mix_dict(signal, base_confirmations)
            # Canonical: percent 0..100 in payloads/models. Ratio is derived only locally.
            confidence_pct = normalize_confidence_pct(
                getattr(signal, "confidence_pct", None) if hasattr(signal, "confidence_pct") else getattr(signal, "confidence", 0.0)
            )
            confidence_ratio = confidence_pct_to_ratio(confidence_pct)
            
            # Получаем настройки депозита и плеча из signal или из ENV
            deposit = signal.deposit if signal.deposit is not None else float(os.getenv("ACCOUNT_DEPOSIT_USD", "100"))
            leverage = signal.leverage if signal.leverage is not None else float(os.getenv("ACCOUNT_LEVERAGE", "100"))
            
            crypto_signal = CryptoSignal(
                sid=signal.sid,
                symbol=signal.symbol,
                side=signal.side.upper(),
                entry=signal.entry,
                sl=signal.sl,
                tp_levels=signal.tp_levels,
                lot=signal.lot,
                atr=max(signal.atr, 1e-6),
                confidence=confidence_ratio,
                ts=timestamp_ms,
                source=signal.source or "OrderFlow",
                reason_mix=mix_dict,
                confirmations=base_confirmations,
                position_size_usd=signal.position_size_usd,
                deposit=deposit,
                leverage=leverage,
                trail_profile=signal.trail_profile,
                trail_after_tp1=signal.trail_after_tp1,
                config_params=signal.signal_settings,
                indicators=signal.indicators or {},
            )
            text = CryptoSignalFormatter.format_telegram_message(crypto_signal)
            if emoji and emoji != "🚨" and text.startswith("🚨"):
                text = text.replace("🚨", emoji, 1)
            return text

        if signal.metadata and "price_decimals" in signal.metadata:
            price_decimals = signal.metadata["price_decimals"]
        else:
            # Auto-detect по цене
            if signal.entry > 100:
                price_decimals = 2
            elif signal.entry > 1:
                price_decimals = 3
            else:
                price_decimals = 4
        
        # Определяем количество знаков для лота
        if signal.metadata and "volume_decimals" in signal.metadata:
            volume_decimals = signal.metadata["volume_decimals"]
        else:
            volume_decimals = 2
        
        # Формируем основное сообщение
        msg = f"{emoji} {signal.symbol} {signal.side} @ {signal.entry:.{price_decimals}f}"
        msg += f", Volume {signal.lot:.{volume_decimals}f} lot. "
        msg += f"{signal.reason}\n"
        
        # Stop Loss
        msg += f"SL {signal.sl:.{price_decimals}f}"
        
        # Take Profit уровни с Risk/Reward
        if signal.tp_levels:
            msg += " | "
            rr = 1.0
            for i, tp in enumerate(signal.tp_levels):
                if i > 0:
                    msg += "; "
                msg += f"TP{i+1} {tp:.{price_decimals}f} (RR {rr:.1f})"
                rr += 1.0
        
        msg += "\n"
        
        # Дополнительная информация
        msg += f"📊 ATR: {signal.atr:.{price_decimals}f} | "
        msg += f"Confidence: {signal.confidence:.0f}% | "
        msg += f"Source: {signal.source}"

        # Добавляем настройки сигнала, если они есть
        if signal.signal_settings:
            msg += "\n\n⚙️ **Signal Settings:**\n"
            settings = signal.signal_settings

            # Основные thresholds
            if 'breakoutZThreshold' in settings:
                msg += f"• Breakout Z: {settings['breakoutZThreshold']}\n"
            if 'absorptionZThreshold' in settings:
                msg += f"• Absorption Z: {settings['absorptionZThreshold']}\n"
            if 'extremeZThreshold' in settings:
                msg += f"• Extreme Z: {settings['extremeZThreshold']}\n"
            if 'mainZThreshold' in settings:
                msg += f"• Main Z: {settings['mainZThreshold']}\n"

            # OBI settings
            if 'obiSustainedMinSamples' in settings:
                msg += f"• OBI Min Samples: {settings['obiSustainedMinSamples']}\n"
            if 'obiSustainedMinFraction' in settings:
                msg += f"• OBI Min Fraction: {settings['obiSustainedMinFraction']}\n"

            # Delta bucket
            if 'deltaBucketMs' in settings:
                msg += f"• Delta Bucket: {settings['deltaBucketMs']}ms\n"

            # Burstiness
            if 'burstRatioMin' in settings:
                msg += f"• Burst Ratio Min: {settings['burstRatioMin']}\n"
            if 'fanoMin' in settings:
                msg += f"• Fano Min: {settings['fanoMin']}\n"

            # Execution filters
            if 'execFiltersEnabled' in settings:
                msg += f"• Exec Filters: {'ON' if settings['execFiltersEnabled'] else 'OFF'}\n"
            if 'etaMaxSec' in settings:
                msg += f"• ETA Max: {settings['etaMaxSec']}s\n"

            # Confidence
            if 'minSignalConfidence' in settings:
                msg += f"• Min Confidence: {settings['minSignalConfidence']}%\n"

            # TP shifts
            if 'tp1ShiftMult' in settings and settings['tp1ShiftMult'] != 1.0:
                msg += f"• TP1 Shift: {settings['tp1ShiftMult']}x\n"

        return msg

    @staticmethod
    def _build_mix_dict(signal: Signal, confirmations: Sequence[str]) -> Dict[str, float]:
        import math
        indicators = signal.indicators or {}
        mix: Dict[str, float] = {}

        # 1. p_delta
        if "p_delta" in indicators:
             v = UnifiedSignalFormatter._safe_float(indicators.get("p_delta"))
             mix["p_delta"] = round(UnifiedSignalFormatter._clamp01(v), 2)
        else:
            delta_val = UnifiedSignalFormatter._safe_float(indicators.get("delta"))
            if not math.isnan(delta_val):
                # Default legacy scaling (fallback)
                mix["p_delta"] = round(UnifiedSignalFormatter._clamp01(abs(delta_val) / 20.0), 2)

        # 2. p_speed
        if "p_speed" in indicators:
             v = UnifiedSignalFormatter._safe_float(indicators.get("p_speed"))
             mix["p_speed"] = round(UnifiedSignalFormatter._clamp01(v), 2)
        else:
            # aliasing: z_delta (old) / delta_z (new)
            z = UnifiedSignalFormatter._safe_float(indicators.get("z_delta"))
            if math.isnan(z):
                z = UnifiedSignalFormatter._safe_float(indicators.get("delta_z"))
            
            if not math.isnan(z):
                mix["p_speed"] = round(UnifiedSignalFormatter._clamp01(abs(z) / 6.0), 2)
        
        # 3. p_cluster (OBI)
        if "p_cluster" in indicators:
             mix["p_cluster"] = round(abs(float(indicators["p_cluster"])), 2)
        else:
            obi = indicators.get("obi")
            if obi is not None:
                # Scale OBI (typ. 0.5..5.0) -> 0..1
                # Old was / 8.0 -> 0.5 at OBI=4.0
                mix["p_cluster"] = round(min(0.99, abs(float(obi)) / 5.0), 2)

        # 4. p_legacy (weak progress)
        if "p_legacy" in indicators:
             mix["p_legacy"] = round(abs(float(indicators["p_legacy"])), 2)
        else:
            weak = indicators.get("weak_progress")
            if weak is not None and weak not in (False, True):
                 # Weak progress count (typ. 20..100)
                mix["p_legacy"] = round(min(0.99, abs(float(weak)) / 100.0), 2)
        
        # Keep legacy fields if needed
        z_legacy = UnifiedSignalFormatter._safe_float(indicators.get("z_delta"))
        if not math.isnan(z_legacy):
            mix["z_delta"] = z_legacy
        z_new = UnifiedSignalFormatter._safe_float(indicators.get("delta_z"))
        if not math.isnan(z_new):
            mix["delta_z"] = z_new

        if confirmations:
            mix["p_confirm"] = float(len(confirmations))
        if not mix and indicators:
            for key, value in indicators.items():
                if key.startswith("p_"):
                    try:
                        mix[key] = round(abs(float(value)), 2)
                    except (TypeError, ValueError):
                        continue
        return mix

    @staticmethod
    def _extract_confirmations(signal: Signal) -> List[str]:
        indicators = signal.indicators or {}
        confirmations: List[str] = []
        if "obi" in indicators and abs(float(indicators["obi"])) > 0.4:
            confirmations.append(f"obi={float(indicators['obi']):.2f}")
        if "weak_progress" in indicators and abs(float(indicators["weak_progress"])) > 20:
            confirmations.append(f"weak={float(indicators['weak_progress']):.0f}")
        return confirmations

    @staticmethod
    def _reason_to_confirmation(reason: Optional[str]) -> Optional[str]:
        if not reason:
            return None
        clean = " ".join(str(reason).strip().split())
        if not clean:
            return None
        return f"reason={clean}"
    
    @staticmethod
    def format_audit_payload(signal: Signal, extra_context: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Формирует полный payload для audit stream (обучение ML моделей).
        
        Args:
            signal: Объект Signal
            extra_context: Дополнительный контекст (obi, weak_progress и т.д.)
            
        Returns:
            Полный словарь для аудита
        """
        audit = asdict(signal)
        
        # Добавляем дополнительный контекст
        if extra_context:
            audit["extra_context"] = extra_context
        
        # Timestamp в human-readable формате (UTC)
        from core.utc_utils import utc_from_timestamp_ms
        audit["ts_human"] = utc_from_timestamp_ms(signal.ts).isoformat()
        
        return audit
    
    @staticmethod
    def format_order_push_payload(signal: Signal) -> Dict[str, Any]:
        """
        Формирует payload для /orders/push endpoint (go-gateway или MT5).
        
        Args:
            signal: Объект Signal
            
        Returns:
            Словарь для HTTP POST /orders/push
        """
        return {
            "symbol": signal.symbol,
            "side": signal.side.lower(),  # long/short (lowercase для MT5)
            "entry": signal.entry,
            "sl": signal.sl,
            "tp": signal.tp_levels[0] if signal.tp_levels else signal.entry,  # Первый TP
            "lot": signal.lot,
            "source": signal.source,
            "signal_id": signal.sid,
            "confidence": signal.confidence,
            "metadata": {
                "reason": signal.reason,
                "atr": signal.atr,
                "ts": signal.ts,
                "indicators": signal.indicators
            }
        }
    
    @staticmethod
    def parse_from_redis(fields: Dict[str, str]) -> Signal:
        """
        Парсит Signal из Redis stream fields.
        
        Args:
            fields: Поля из Redis XREAD/XREADGROUP
            
        Returns:
            Объект Signal
            
        Raises:
            ValueError: Если обязательные поля отсутствуют
        """
        # Парсим TP levels
        tp_levels_str = fields.get("tp_levels", "")
        tp_levels = [float(tp) for tp in tp_levels_str.split(",")] if tp_levels_str else []
        
        # Парсим indicators
        indicators = {}
        for key, value in fields.items():
            if key.startswith("ind_"):
                indicator_name = key[4:]  # Убираем префикс "ind_"
                try:
                    # Пытаемся преобразовать в float
                    indicators[indicator_name] = float(value)
                except ValueError:
                    # Если не получается - оставляем как строку
                    indicators[indicator_name] = value
        
        # Парсим metadata
        metadata = {}
        for key, value in fields.items():
            if key.startswith("meta_"):
                meta_key = key[5:]  # Убираем префикс "meta_"
                try:
                    metadata[meta_key] = float(value)
                except ValueError:
                    metadata[meta_key] = value
        
        return Signal(
            sid=fields.get("sid") or fields.get("signal_id", ""),
            symbol=fields.get("symbol", ""),
            side=fields.get("side", ""),
            entry=float(fields.get("entry", 0)),
            sl=float(fields.get("sl", 0)),
            tp_levels=tp_levels,
            lot=float(fields.get("lot", 0)),
            source=fields.get("source", ""),
            reason=fields.get("reason", ""),
            confidence=float(fields.get("confidence", 0)),
            atr=float(fields.get("atr", 0)),
            ts=int(fields.get("ts", get_ny_time_millis())),
            indicators=indicators,
            metadata=metadata if metadata else None
        )


# ═════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═════════════════════════════════════════════════════════════════════

def create_signal(
    symbol: str,
    side: str,
    entry: float,
    sl: float,
    tp_levels: List[float],
    lot: float,
    source: str,
    reason: str,
    confidence: float,
    atr: float,
    ts: Optional[int] = None,
    indicators: Optional[Dict[str, Any]] = None,
    signal_settings: Optional[Dict[str, Any]] = None,
    entry_tag: str = ""
) -> Signal:
    """
    Вспомогательная функция для создания Signal объекта.

    Args:
        symbol: Символ инструмента
        side: Направление (LONG/SHORT)
        entry: Цена входа
        sl: Stop Loss
        tp_levels: Take Profit уровни
        lot: Размер позиции
        source: Источник сигнала
        reason: Описание
        confidence: Уверенность (0-100)
        atr: Текущий ATR
        ts: Timestamp (опционально, по умолчанию current time)
        indicators: Дополнительные индикаторы (опционально)
        signal_settings: Настройки, использованные при генерации сигнала (опционально)

    Returns:
        Объект Signal
    """
    if ts is None:
        ts = get_ny_time_millis()
    
    if indicators is None:
        indicators = {}
    
    sid = UnifiedSignalFormatter.create_signal_id(symbol, side, entry, ts)
    
    return Signal(
        sid=sid,
        symbol=symbol,
        side=side,
        entry=entry,
        sl=sl,
        tp_levels=tp_levels,
        lot=lot,
        source=source,
        reason=reason,
        confidence=confidence,
        atr=atr,
        ts=ts,
        indicators=indicators,
        entry_tag=entry_tag,
        signal_settings=signal_settings
    )

