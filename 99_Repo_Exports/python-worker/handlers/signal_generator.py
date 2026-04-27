# signal_generator.py
"""
Функционал генерации сигналов, извлеченный из base_orderflow_handler.py
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

from typing import Optional, Dict, Any, Tuple
import time

from contexts import OrderflowSignalContext
from signals.outbox_utils import PublishResult, build_level_key_breakout, build_level_key_extreme, build_level_key_sweep, nearest_pivot_key, price_bin_key, normalize_to_bucket
from .regime_gate import RegimeGateCfg, regime_allows
# from common.log import setup_logger
def setup_logger(name):
    import logging
    return logging.getLogger(name)


class SignalGenerator:
    """
    Сервис для генерации и публикации сигналов.
    """

    def __init__(self, symbol: str, config: Any, outbox: Any, cooldown: Any = None,
                 dedup_bucket_ms: int = 60000, config_manager: Any = None, health_metrics: Any = None):
        self.symbol = symbol
        self.config = config
        self.outbox = outbox
        self.cooldown = cooldown
        self.dedup_bucket_ms = dedup_bucket_ms
        self.config_manager = config_manager
        self.health_metrics = health_metrics
        self.logger = setup_logger(f"SignalGenerator:{symbol}")

        # Настройки генерации сигналов
        self.min_trades_breakout = int(getattr(config, "min_trades_breakout", 20))
        self.burst_ratio_min = float(getattr(config, "burst_ratio_min", 1.6))
        self.fano_min = float(getattr(config, "fano_min", 1.5))
        self.flip_ratio_max = float(getattr(config, "flip_ratio_max", 0.70))
        self.imbalance_min = float(getattr(config, "imbalance_min", 0.20))

        self.z_enter = float(getattr(config, "signal_z_enter", 1.5))
        self.z_breakout = float(getattr(config, "signal_z_breakout", 2.0))

        # Кулдаун (атомарный, на базе Redis)
        # Если уже есть отдельный сервис, полагаемся только на его Redis клиент:
        #   cooldown.redis  OR  cooldown.client  OR  cooldown itself being a Redis client.
        self._cooldown_default_ms = {
            "breakout": 30_000,
            "extreme": 30_000,
            "obi_spike": 15_000,
            "absorption": 60_000,
            "sweep": 15_000,
            "default": 10_000,
        }

        # Конфигурация гейта по режиму рынка
        self.regime_gate = RegimeGateCfg(
            breakout_min_score=float(getattr(config, "regime_breakout_min_score", 0.0)),
            extreme_min_score=float(getattr(config, "regime_extreme_min_score", 0.0)),
            obi_spike_min_score=float(getattr(config, "regime_obi_spike_min_score", 0.0)),
            absorption_max_score=float(getattr(config, "regime_absorption_max_score", 0.0)),
            allow_sweep_any=bool(getattr(config, "regime_allow_sweep_any", True)),
        )

        # если True и ctx.regime_score отсутствует -> сигнал не генерируем
        self.regime_require_score = bool(getattr(config, "regime_require_score", False))

        # Confidence computation weights (were hardcoded 0.4, 0.4, 0.2)
        self._conf_w_obi = float(getattr(config, "conf_weight_obi", 0.4))
        self._conf_w_z = float(getattr(config, "conf_weight_z", 0.4))
        self._conf_w_burst = float(getattr(config, "conf_weight_burst", 0.2))
        self._conf_obi_normalizer = float(getattr(config, "conf_obi_normalizer", 0.5))
        self._conf_z_normalizer = float(getattr(config, "conf_z_normalizer", 2.0))
        self._conf_burst_fail_score = float(getattr(config, "conf_burst_fail_score", 0.3))

    def _cooldown_period_ms(self, *, kind_lc: str, timeframe_s: int) -> int:
        """
        Центральное место для TTL кулдауна.
        Можно расширять: разные TTL для kind/timeframe.
        """
        # по умолчанию: бакет 60с для сигналов
        base = int(getattr(self.config, "cooldown_ms", 60_000) or 60_000)
        return max(50, base)

    def _cooldown_reserve(self, *, family: str, timeframe_s: int, kind_lc: str, level_key: str, ts_ms: int):
        if not self.cooldown:
            # Нет сервиса кулдауна - разрешаем все сигналы
            redis_key = f"cooldown:{family}:{self.symbol}:{timeframe_s}:{kind_lc}:{level_key}"
            return True, redis_key, "no_cooldown"
        return self.cooldown.reserve(family=family, timeframe_s=timeframe_s, kind_lc=kind_lc, level_key=level_key, ts_ms=ts_ms)

    def _nums(self, ctx: OrderflowSignalContext) -> Tuple[float, float, float]:
        """Безопасное извлечение числовых значений из контекста."""
        z = float(getattr(ctx, "z_delta", 0.0) or 0.0)
        obi = float(getattr(ctx, "obi", 0.0) or 0.0)
        price = float(getattr(ctx, "price", 0.0) or 0.0)
        return z, obi, price

    # -----------------------
    # Кулдаун: атомарная резервация (SET NX PX) + освобождение при ошибке
    # -----------------------



    def _burst_gate_ok(self, ctx: OrderflowSignalContext, signal_type: str = "bar") -> bool:
        """Проверка гейта качества всплеска."""
        bs = getattr(ctx, "burst_stats", None)
        if not bs:
            return True
        trade_count = int(getattr(bs, "trade_count_bucket", 0) or 0)
        burst_ratio = float(getattr(bs, "burst_ratio", 0.0) or 0.0)
        fano = float(getattr(bs, "fano_counts", 0.0) or 0.0)
        flip_ratio = float(getattr(bs, "flip_ratio", 1.0) or 1.0)
        # Получение специфичных для типа порогов из config_manager
        if hasattr(self, 'config_manager') and self.config_manager:
            min_trades_breakout = self.config_manager.get_min_trades_breakout(signal_type)
            burst_ratio_min = self.config_manager.get_min_burst_ratio(signal_type)
            fano_min = getattr(self, 'fano_min', 1.5)
            flip_ratio_max = getattr(self, 'flip_ratio_max', 0.70)
        else:
            # Фоллбек на устаревшие атрибуты
            min_trades_breakout = getattr(self, 'min_trades_breakout', 20)
            burst_ratio_min = getattr(self, 'burst_ratio_min', 1.6)
            fano_min = getattr(self, 'fano_min', 1.5)
            flip_ratio_max = getattr(self, 'flip_ratio_max', 0.70)

        return (
            trade_count >= min_trades_breakout
            and burst_ratio >= burst_ratio_min
            and fano >= fano_min
            and flip_ratio <= flip_ratio_max
        )

    def _exec_quality_ok(self, ctx: OrderflowSignalContext, impulse_side: str, signal_type: str = "bar") -> bool:
        """Проверка качества исполнения."""
        # Проверка гейта всплеска
        if not self._burst_gate_ok(ctx, signal_type):
            return False

        # Проверка устаревания L2 - не использовать устаревшие OBI/L2
        if getattr(ctx, 'l2_is_stale', True):
            return False

        # Примечание: Гейт режима теперь применяется ранее в методе generate()

        imbalance_threshold = float(self.imbalance_min)

        # Предпочтение OBI20 (по всей ленте), если доступно/валидно
        use_obi20 = bool(getattr(self, "use_obi20_exec_gate", True))
        if use_obi20 and hasattr(ctx, "obi_20_valid"):
            if not bool(getattr(ctx, "obi_20_valid", False)):
                return False
            obi_exec = float(getattr(ctx, "obi_avg_20", 0.0))
        else:
            # фоллбек на obi_avg (legacy)
            obi_exec = float(getattr(ctx, "obi_avg", 0.0))

        # CVD Momentum Gate (CVD должно подтверждать направление, если задан порог)
        cvd_div = float(getattr(ctx, "cvd_divergence", 0.0))
        cvd_min = float(getattr(self.config, "cvd_min_divergence", 0.0))

        if impulse_side == "buy":
            # OBI Threshold Check
            if obi_exec < imbalance_threshold:
                return False
            # CVD Threshold Check (CVD должен расти)
            if cvd_min > 0.0 and cvd_div < cvd_min:
                return False
        elif impulse_side == "sell":
            # OBI Threshold Check
            if obi_exec > -imbalance_threshold:
                return False
            # CVD Threshold Check (CVD должен падать)
            if cvd_min > 0.0 and cvd_div > -cvd_min:
                return False

        # Опциональный фильтр противоречий: сдвиг микроцены не должен противоречить стороне
        # (дешево, очень эффективно против спуфинга / искажений дальних стен)
        if bool(getattr(self, "use_microprice_contradiction_gate", True)):
            mp = float(getattr(ctx, "microprice_shift_bps_20", 0.0))
            tol = float(getattr(self, "microprice_contra_tol_bps", 0.0))
            if impulse_side == "buy" and mp < -tol:
                return False
            if impulse_side == "sell" and mp > tol:
                return False

        return True



    def _compute_confidence(self, ctx: OrderflowSignalContext) -> Tuple[float, Dict[str, float]]:
        """Вычисление уверенности сигнала."""
        z, obi, _ = self._nums(ctx)
        c_obi = min(abs(obi) / max(self._conf_obi_normalizer, 1e-9), 1.0)
        c_z = min(abs(z) / max(self._conf_z_normalizer, 1e-9), 1.0)
        c_burst = 1.0 if self._burst_gate_ok(ctx) else self._conf_burst_fail_score
        w_obi, w_z, w_burst = self._conf_w_obi, self._conf_w_z, self._conf_w_burst
        conf = c_obi * w_obi + c_z * w_z + c_burst * w_burst
        return conf, {"obi": c_obi, "delta": c_z, "burst": c_burst}

    def _custom_signal_conditions(self, ctx: OrderflowSignalContext) -> Dict[str, Any]:
        """Проверка кастомных условий сигнала."""
        # Заглушка для кастомных условий - разрешить по умолчанию
        return {}

    def generate(self, ctx: OrderflowSignalContext, signal_type: str = "bar") -> PublishResult:
        """Генерация сигнала из контекста."""
        z0, _, _ = self._nums(ctx)
        reserved = False
        cooldown_key: Optional[str] = None
        cooldown_token: Optional[str] = None

        # базовый уровень
        if abs(z0) < self.z_enter:
            return PublishResult(sent=False, dedup=False, msg_id=None)

        direction = 1 if z0 > 0 else -1
        impulse_side = "buy" if direction > 0 else "sell"

        if not self._exec_quality_ok(ctx, impulse_side, signal_type):
            # Отслеживание отклонения гейтом качества
            if self.health_metrics:
                try:
                    self.health_metrics.on_quality_gate_rejection(self.symbol, signal_type)
                except Exception:
                    pass
            return PublishResult(sent=False, dedup=False, msg_id=None)

        # Определяем тип сигнала для дедупа (паттерно-специфичный)
        # Порядок важен: от самых сильных к самым слабым сигналам
        if abs(z0) >= getattr(ctx, 'extreme_z_threshold', 3.0):  # extreme threshold
            signal_kind = "EXTREME"
        elif abs(z0) >= self.z_breakout:
            signal_kind = "BREAKOUT"
        elif getattr(ctx, 'weak_progress', False) or getattr(ctx, 'absorption_score', 0) > 0.1:
            signal_kind = "ABSORPTION"
        elif getattr(ctx, 'obi_spike', False) or abs(getattr(ctx, 'obi', 0)) > getattr(ctx, 'obi_spike_threshold', 0.8):
            signal_kind = "OBI_SPIKE"
        else:
            signal_kind = "SWEEP"

        # --- Regime gate (единое место принятия решения) ---
        # regime_score: [-1..+1], где <0 = range/mean-reversion, >=0 = trend/mixed
        rscore_raw = getattr(ctx, "regime_score", None)
        rlabel = str(getattr(ctx, "regime_label", "") or "")

        if rscore_raw is None:
            if self.regime_require_score:
                # Нет режима — режем, чтобы не торговать "вслепую"
                return PublishResult(sent=False, dedup=False, msg_id=None)
            rscore = 0.0  # по умолчанию считаем mixed
        else:
            rscore = float(rscore_raw or 0.0)

        if not regime_allows(signal_kind.lower(), rscore, self.regime_gate):
            # Сигнал не публикуем (и это НЕ дедуп)
            return PublishResult(sent=False, dedup=False, msg_id=None)

        # Формируем level_key с помощью утилит (НИКОГДА не пустой!)
        price = float(getattr(ctx, "price", 0.0) or 0.0)
        # pivots теперь всегда приходят из бандла CacheService -> data_processor прикрепляет ctx.pivots
        pivots = getattr(ctx, "pivots", None)
        nearest_pivot = None
        try: nearest_pivot = nearest_pivot_key(price, pivots) if pivots else None
        except Exception: nearest_pivot = None

        if signal_kind == "BREAKOUT":
            # BREAKOUT: реальный уровень или не публикуем вообще
            lvl = getattr(ctx, 'level_key', None) or getattr(ctx, 'level_name', None)
            if lvl and hasattr(ctx, 'level_price'):
                # Используем существующий level_key или строим новый
                level_key = build_level_key_breakout(lvl)
            else:
                level_key = None

            if level_key is None:
                # Нет реального уровня - не публикуем сигнал (return early)
                return PublishResult(sent=False, dedup=False, msg_id=None)

        elif signal_kind == "EXTREME":
            # EXTREME: nearest pivot + price bin для лучшего дедупа
            level_key = build_level_key_extreme(
                price=price,
                pivots=pivots,
                z=z0,
                price_step=0.5,  # можно параметризовать
                include_z_bin=False  # можно включить через env
            )

        elif signal_kind in ("SWEEP", "ABSORPTION", "OBI_SPIKE"):
            # SWEEP/ABSORPTION/OBI_SPIKE: ближайший пивот
            level_key = build_level_key_sweep(price=price, pivots=pivots)

        else:
            # Fallback для неизвестных типов
            level_key = build_level_key_sweep(price=price, pivots=pivots)

        # Финальная проверка - level_key не должен быть пустым
        if not level_key or level_key in ("", "none", "na"):
            level_key = price_bin_key(price, 0.5)

        # Нормализуем ts_ms для дедупа
        ts_ms = int(getattr(ctx, "ts", 0) or get_ny_time_millis())
        ts_ms_normalized = normalize_to_bucket(ts_ms, self.dedup_bucket_ms)

        # гейт кулдауна (атомарный): резервация слота для (symbol, family, timeframe, kind, level_key)
        family = getattr(ctx, "family", "of")
        tf = int(getattr(ctx, "timeframe_s", 60) or 60)

        kind_lc = signal_kind.lower()
        if self.cooldown:
            # Предпочтительно: reserve() -> (ok, redis_key, token), чтобы безопасно освободить при сбое.
            if hasattr(self.cooldown, "reserve") and hasattr(self.cooldown, "release"):
                ok, cooldown_key, cooldown_token = self.cooldown.reserve(
                    family=str(family),
                    timeframe_s=int(tf),
                    kind_lc=str(kind_lc),
                    level_key=str(level_key),
                    ts_ms=int(ts_ms),  # real ts (not normalized)
                )
                if not ok:
                    # подавлено кулдауном (rate-limit / дедупликация)
                    if self.health_metrics:
                        try:
                            self.health_metrics.on_cooldown_hit(self.symbol)
                        except Exception:
                            pass
                    return PublishResult(sent=False, dedup=True, msg_id=None)
                reserved = True
                if self.health_metrics:
                    try:
                        self.health_metrics.on_cooldown_miss(self.symbol)
                    except Exception:
                        pass
            else:
                # Фоллбек обратной совместимости (без семантики освобождения)
                if not self.cooldown.acquire(
                    kind=str(kind_lc),
                    level_key=str(level_key),
                    ts_ms=int(ts_ms),
                    family=str(family),
                    timeframe_s=int(tf),
                ):
                    if self.health_metrics:
                        try:
                            self.health_metrics.on_cooldown_hit(self.symbol)
                        except Exception:
                            pass
                    return PublishResult(sent=False, dedup=True, msg_id=None)
                if self.health_metrics:
                    try:
                        self.health_metrics.on_cooldown_miss(self.symbol)
                    except Exception:
                        pass

        conf, breakdown = self._compute_confidence(ctx)

        # Гейт уверенности: отклонение сигналов ниже минимального порога (специфично для типа)
        min_conf = 0.0
        if self.config_manager:
            try:
                if signal_type.lower() == "bucket":
                    min_conf = self.config_manager.get_min_confidence_bucket(self.symbol)
                else:
                    min_conf = self.config_manager.get_min_confidence_bar(self.symbol)
            except Exception:
                min_conf = 0.0

        if min_conf > 0.0 and conf < min_conf:
            self.logger.debug(f"Signal rejected by {signal_type} confidence gate: {conf:.3f} < {min_conf:.3f}")
            # Отслеживание отклонения гейтом качества
            if self.health_metrics:
                try:
                    self.health_metrics.on_quality_gate_rejection(self.symbol, signal_type)
                except Exception:
                    pass
            return PublishResult(sent=False, dedup=False, msg_id=None)

        envelope = {
            "symbol": getattr(ctx, "symbol", self.symbol),
            "ts_ms": ts_ms_normalized,  # нормализованный для дедупа
            "bucket_ts_ms": ts_ms_normalized,  # стабильный бакетный timestamp для dedup (приоритет в _choose_dedup_ts_ms)
            "direction": direction,
            "kind": signal_kind,  # паттерно-специфичный для дедупа
            "signal_type": signal_kind.lower(),  # оригинальный тип для логики
            "level_key": level_key,  # никогда не пустой
            "price": float(getattr(ctx, "price", 0.0) or 0.0),
            "confidence": conf,
            "breakdown": breakdown,
            # regime в верхнем уровне (удобно downstream'ам)
            "regime_label": rlabel,
            "regime_score": float(rscore),
            "cvd_5m": float(getattr(ctx, "cvd_5m", 0.0)),
            "cvd_divergence": float(getattr(ctx, "cvd_divergence", 0.0)),
            "context": {
                "price": float(getattr(ctx, "price", 0.0) or 0.0),
                "z_delta": z0,
                "obi": float(getattr(ctx, "obi", 0.0) or 0.0),
                "level_key": level_key,
                "nearest_pivot": nearest_pivot,
                # regime и в context тоже (для обратной совместимости, если кто-то читает только context)
                "regime_label": rlabel,
                "regime_score": float(rscore),
                "cvd_5m": float(getattr(ctx, "cvd_5m", 0.0)),
                "cvd_divergence": float(getattr(ctx, "cvd_divergence", 0.0)),
            },
        }

        # (опционально) если хотите видеть причины reject в логах/метриках:
        # reason = regime_reject_reason(signal_kind.lower(), rscore, self.regime_gate)
        # но мы сюда не попадём, т.к. уже return выше

        try:
            result = self.outbox.publish(envelope)
            # Если мы зарезервировали слот кулдауна, но публикация НЕ прошла,
            # освобождаем его, чтобы не блокировать будущие сигналы между процессами.
            if reserved and (not bool(getattr(result, "sent", False))):
                try:
                    self.cooldown.release(str(cooldown_key), str(cooldown_token))
                except Exception:
                    pass
            return result
        except Exception:
            if reserved:
                try:
                    self.cooldown.release(str(cooldown_key), str(cooldown_token))
                except Exception:
                    pass
            self.logger.exception("Сбой публикации в Outbox")
            return PublishResult(sent=False, dedup=False, msg_id=None)
