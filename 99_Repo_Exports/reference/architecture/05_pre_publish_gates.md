# Этап 5: Система гейтов (Pre-Publish Gates)

## Что это и зачем?
ML-модели хороши, но они не знают о реалиях рынка "здесь и сейчас": сломалось ли подключение к бирже, вышли ли новости, работает ли брокер нормально. Gates — это набор фильтров (Validators), каждый из которых проверяет одну конкретную вещь. Только пройдя ВСЕ фильтры, сигнал становится торговым ордером.

Паттерн проектирования: **Chain of Responsibility** (цепочка обязанностей).

Файл с реализацией: `python-worker/handlers/crypto_orderflow/utils/pre_publish_gates.py`

---

## Общая структура

```python
# Все гейты реализованы как CompositeValidator (из crypto_orderflow_quality.py):
@dataclass(frozen=True)
class CompositeValidator:
    validators: List[Validator]

    def validate(self, ctx: Any, cand: Candidate) -> QualityState:
        q = QualityState()
        for v in self.validators:
            if q.veto:   # Если уже есть VETO — прерываем цепочку
                break
            v.validate(ctx, cand, q)  # Каждый валидатор может поставить veto
        return q

# QualityState — это контейнер результатов:
class QualityState:
    veto: bool = False         # True если хоть один Gate заблокировал
    veto_reason: str = ""      # Причина (для метрик/логов)
    flags: dict = {}           # Все собранные данные (для дебага)
    
    def veto_with(self, reason: str):
        self.veto = True
        self.veto_reason = reason
```

---

## 1. Hard Data Quality Gate (HardDataQualityGate)

**Задача**: Проверить, что данные, на которых построен сигнал, вообще актуальны.

```python
# Из pre_publish_gates.py
class HardDataQualityGate:
    """
    Жесткие проверки качества данных.
    Veto если данные устарели или неполны.
    """
    def validate(self, ctx: Any) -> GateDecision:
        # Проверка 1: Книга ордеров актуальна?
        book_age_ms = int(time.time() * 1000) - int(getattr(ctx, "book_ts_ms", 0))
        max_stale_ms = int(os.getenv("DATA_ATR_STALE_MAX_MS", "60000"))
        
        if book_age_ms > max_stale_ms:
            return GateDecision.DENY(f"book_stale:{book_age_ms}ms > {max_stale_ms}ms")
        
        # Проверка 2: ATR посчитан?
        atr = float(getattr(ctx, "atr", 0.0) or 0.0)
        if atr <= 0.0 or math.isnan(atr):
            return GateDecision.DENY("atr_unavailable")
            # Без ATR невозможно правильно поставить стоп!
        
        # Проверка 3: Дыры в потоке тиков?
        dq_flags = getattr(ctx, "dq_flags", [])
        if "tick_gap_critical" in dq_flags:
            return GateDecision.DENY("tick_gap_critical")
        
        return GateDecision.ALLOW()
```

**Почему ATR обязателен?** ATR (Average True Range) — это средний диапазон свечи. Если ATR = 250 USDT, а мы ставим стоп в 5 USDT — нас выбьет на первом же случайном колебании. Без ATR стоп = лотерея.

---

## 2. Regime Session Gate (RegimeSessionGate)

**Задача**: Проверить, что текущий рыночный режим согласован с типом сигнала.

```python
# Пример логики (упрощено):
class RegimeSessionGate:
    def validate(self, ctx: Any, cand: Candidate) -> GateDecision:
        regime = str(getattr(ctx, "regime", "unknown")).lower()
        
        # Матрица разрешений: какие сигналы работают в каком режиме
        REGIME_ALLOWED_KINDS = {
            "trend_up":   ["breakout", "extreme", "obi_spike"],
            "trend_down": ["absorption", "extreme", "obi_spike"],
            "chop":       ["absorption"],       # В боковике — только фады
            "expansion":  ["breakout", "extreme"],
        }
        
        allowed = REGIME_ALLOWED_KINDS.get(regime, ["breakout", "absorption", "extreme"])
        
        if cand.kind not in allowed:
            return GateDecision.DENY(f"kind={cand.kind} not_allowed in regime={regime}")
        
        # Дополнительная проверка: в режиме "chop" требуем меньший объем
        if regime == "chop":
            obi = float(getattr(ctx, "obi_avg", 0.0))
            if abs(obi) < 0.3:  # Слабый перекос в боковике
                return GateDecision.DENY("chop_weak_obi")
        
        return GateDecision.ALLOW()
```

**Почему это важно?** В боковом рынке (Chop) Breakout-паттерны дают ~40% win-rate. Модель их "видит", но это убыточно. Gейт запрещает торговать против математики.

---

## 3. Feature Drift Gate (ML Data Drift Guard)

**Задача**: Убедиться что рынок не "сдрейфовал" от того, на чём обучалась модель.

**Аналогия**: Вы обучили врача ставить диагнозы по симптомам X, Y, Z. Если пришла пациентка с симптомами A, B — модель скорее всего ошибется, но уверенно скажет "OK". Drift Guard это замечает.

```python
# Как работает Drift Guard:
class FeatureDriftGate:
    def __init__(self):
        # Эти значения были при обучении модели (Golden Thresholds):
        self.golden = {
            "taker_rate_ema": {"mean": 0.53, "std": 0.08},
            "spread_bps":     {"mean": 1.2,  "std": 0.4},
        }
    
    def validate(self, ctx: Any) -> GateDecision:
        for feature_name, golden_stats in self.golden.items():
            current_val = float(getattr(ctx, feature_name, 0.0))
            
            # Z-score отклонения от обученного распределения
            z_drift = abs(
                (current_val - golden_stats["mean"]) / (golden_stats["std"] + 1e-8)
            )
            
            threshold = float(os.getenv("FEATURE_DRIFT_Z_THRESHOLD", "3.0"))
            if z_drift > threshold:
                # Рынок сдрейфовал! Модель ненадёжна
                profile = os.getenv("FEATURE_DRIFT_PROFILE", "soft")
                
                if profile == "hard":
                    return GateDecision.DENY(f"drift:{feature_name} z={z_drift:.1f}")
                elif profile == "tighten":
                    ctx.min_confidence_override = 90.0  # Повышаем порог
                else: # "soft"
                    logger.warning("Feature drift: %s z=%.1f", feature_name, z_drift)
        
        return GateDecision.ALLOW()
```

Алерты SRE: `meta:drift_freeze` — публикуется когда Drift Gate переходит в режим `hard`.

---

## 4. SMT Coherence Gate (Smart Money Theory)

**Задача**: Не торговать "против паровоза". BTC определяет движение альткоинов.

```python
class SmtCoherenceGate:
    """
    Smart Money Theory: лидеры (BTC, ETH) задают направление.
    Если лидер уверенно падает — не покупаем альткоины.
    """
    def validate(self, ctx: Any, cand: Candidate) -> GateDecision:
        leaders = ["BTCUSDT", "ETHUSDT"]
        
        for leader in leaders:
            # Читаем текущий сигнал лидера из кэша
            leader_signal = self._read_leader_signal(leader)
            if not leader_signal:
                continue
            
            leader_direction = leader_signal.get("direction")  # "BUY" или "SELL"
            leader_confidence = leader_signal.get("confidence", 0.0)
            min_conf = float(os.getenv("SMT_LEADER_CONF_MIN_SCORE", "65.0"))
            
            # Лидер уверенно идет против нашей сделки?
            if leader_confidence >= min_conf:
                our_side = "BUY" if cand.direction > 0 else "SELL"
                if leader_direction != our_side:
                    # BTC продают с 80% уверенностью, а мы хотим купить DOGE?
                    return GateDecision.DENY(
                        f"smt_diverged: {leader} says {leader_direction} ({leader_confidence:.0f}%)"
                         f" but we want {our_side}"
                    )
        
        return GateDecision.ALLOW()
```

---

## 5. Edge Cost Gate (Математическое ожидание ≥ Затраты)

**Задача**: Убедиться что потенциальная прибыль перевешивает реальные затраты на исполнение.

```python
# Из handlers/crypto_orderflow/utils/edge_cost_gate.py
class EdgeCostGate:
    """
    EV (Expected Value) > Execution Cost → торгуем.
    Иначе → VETO (убыточно на дистанции).
    """
    def validate(self, ctx: Any, cand: Candidate) -> GateDecision:
        # Затраты на исполнение (в базисных пунктах):
        taker_fee_bps = float(os.getenv("TAKER_FEE_BPS", "4.0"))  # 0.04%
        spread_bps = float(getattr(ctx, "spread_bps", 0.0))
        slippage_ema_bps = float(getattr(ctx, "slippage_ema_bps", 0.0))
        
        total_cost_bps = taker_fee_bps + (spread_bps / 2) + slippage_ema_bps
        
        # Потенциальная прибыль (Expected Value):
        tp1_hit_prob = float(getattr(ctx, "tp1_hit_prob", 0.5))    # Вероятность TP1
        rr_ratio = float(getattr(ctx, "rr_ratio", 2.0))             # Risk/Reward
        
        # EV = P(win) * reward - P(loss) * risk
        ev_bps = (tp1_hit_prob * rr_ratio - (1 - tp1_hit_prob)) * 10.0
        
        if ev_bps <= total_cost_bps:
            return GateDecision.DENY(
                f"negative_ev: ev={ev_bps:.1f}bps <= cost={total_cost_bps:.1f}bps"
            )
        
        # Проверка спреда отдельно
        spread_max = float(os.getenv("SIGNAL_MAX_SPREAD_BPS", "30.0"))
        if spread_bps > spread_max:
            return GateDecision.DENY(f"spread_too_wide: {spread_bps:.1f} > {spread_max:.1f}")
        
        return GateDecision.ALLOW()
```

---

## 6. MinIntervalValidator — защита от спама

```python
# Из crypto_orderflow_quality.py (реальный код)
@dataclass(frozen=True)
class MinIntervalValidator(Validator):
    min_interval_ms: int  # Минимальное время между двумя сигналами

    def validate(self, ctx: Any, cand: Candidate, q: QualityState) -> None:
        if self.min_interval_ms <= 0:
            return  # Отключено
        
        last_ts = int(getattr(ctx, "_last_signal_ts_ms", 0) or 0)
        ts = int(getattr(ctx, "ts", 0) or 0)
        
        if ts > 0 and last_ts > 0:
            elapsed_ms = ts - last_ts
            if elapsed_ms < self.min_interval_ms:
                q.veto_with("min_interval")
                # Например: MIN_SIGNAL_INTERVAL_SEC=60 → между сигналами минимум минута
```

---

## 7. Диагностика Veto

Если Gate поставил VETO — это не конец: сигнал попадает в диагностику:

```python
# После прохождения gate pipeline:
quality = composite_validator.validate(ctx, cand)

if quality.veto:
    # Пишем в Prometheus (метрика для графиков/алертов)
    signals_veto_total.labels(
        reason_code=quality.veto_reason,
        kind=cand.kind,
        symbol=ctx.symbol
    ).inc()
    
    # Диагностический стрим (для ресёрча, НЕ для торговли)
    await redis.xadd("stream:signals:diagnostics", {
        "data": json.dumps({
            "tradeable": False,
            "symbol": ctx.symbol,
            "kind": cand.kind,
            "reason": quality.veto_reason,
            "flags": quality.flags,  # Все собранные метрики
            "confidence": ctx.confidence,
        })
    })
```

Это золото для Data Scientist: видно ПОЧЕМУ хорошие сигналы блокируются.
