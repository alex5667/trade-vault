# Этап 3: Генерация кандидатов (Detector & Handler)

## Что это и зачем?
После очистки тиков они поступают в "мозг" системы — движок детекции паттернов. Здесь накапливается история рыночной активности и определяется: "Что сейчас происходит с рынком? Стоит ли торговать?".

Основной файл: `python-worker/handlers/crypto_orderflow/core/crypto_orderflow_detector.py`

---

## 1. Концепция: что такое SymbolRuntime?

Для каждой торговой пары (например BTCUSDT) создается объект `SymbolRuntime` — это "живая память" системы о данном активе:

```python
# Упрощенное представление SymbolRuntime
class SymbolRuntime:
    symbol: str = "BTCUSDT"
    
    # Текущий стакан
    book_bids: list = [...]  # [{price: 64000, qty: 1.5}, ...]
    book_asks: list = [...]  # [{price: 64001, qty: 0.8}, ...]
    spread_bps: float = 1.5  # Спред в базисных пунктах
    
    # Накопленные метрики
    z_delta: float = 2.3        # Z-Score потока ордеров
    obi_avg: float = 0.45       # Дисбаланс стакана
    book_churn: float = 1.2     # "Токсичность" стакана
    
    # Исторические данные
    regime: str = "trend_up"    # Текущий режим рынка
    atr: float = 250.0          # Волатильность (Average True Range)
    
    # Уровни старших таймфреймов
    htf_levels: dict = {
        "pdh": 65000.0,    # Previous Day High
        "pdl": 63000.0,    # Previous Day Low
        "1h_ema": 64200.0  # EMA на часовике
    }
```

---

## 2. CVD и Delta Z-Score (главная метрика импульса)

**CVD (Cumulative Volume Delta)** — это разница между объемом агрессивных покупателей и продавцов. Представьте: биткоин стоит $64000. Если кто-то "бьет" по стакану маркет-ордерами на покупку — CVD растет. Если продает — падает.

```python
# Как вычисляется CVD (упрощенно):
for tick in received_ticks:
    if tick["side"] == "B":   # Покупатель инициировал
        cvd_window += tick["qty"]
    else:                      # Продавец инициировал
        cvd_window -= tick["qty"]

# Z-Score: насколько сильный импульс по сравнению с историей?
# Z = (current_value - historical_mean) / historical_std
z_delta = (cvd_window - cvd_mean) / cvd_std

# Пример:
# z_delta = 3.5  → очень сильные покупки (3.5 стандартных отклонения)
# z_delta = -2.0 → умеренные продажи
# z_delta = 0.3  → тихо, ничего не происходит
```

Порог срабатывания детектора: `delta_z_threshold=3.1` (настраивается в конфиге символа).

---

## 3. OBI (Order Book Imbalance) — дисбаланс стакана

OBI измеряет, насколько перекошен стакан. Если в бидах (покупки) стоит намного больше ликвидности, чем в асках (продажи) — это поддержка снизу.

```python
# Вычисление OBI (из handlers/crypto_orderflow/core/):
def compute_obi(bids: list, asks: list, depth: int = 15) -> float:
    """
    OBI = (bid_volume - ask_volume) / (bid_volume + ask_volume)
    Результат: от -1.0 (все в асках) до +1.0 (все в бидах)
    """
    bid_vol = sum(level["qty"] for level in bids[:depth])
    ask_vol = sum(level["qty"] for level in asks[:depth])
    total = bid_vol + ask_vol
    if total <= 0:
        return 0.0
    return (bid_vol - ask_vol) / total

# Интерпретация:
# obi = +0.85 → стакан сильно перегружен бидами → поддержка снизу
# obi = -0.60 → давление продавцов → сопротивление сверху
# obi = +0.05 → баланс, нейтрально
```

Переменные: `BREAKOUT_REQUIRE_OBI=true`, `OBI_STABLE_SCORE_MIN=0.85`.

---

## 4. Основной Детектор (CryptoEventDetector)

Вот реальный код из `crypto_orderflow_detector.py`. Детектор смотрит на `z_delta` и `obi`, и решает: какой паттерн произошел.

```python
# Реальный код из crypto_orderflow_detector.py:
class CryptoEventDetector:
    def detect(self, ctx: Any) -> List[Candidate]:
        """Возвращает список обнаруженных событий (кандидатов)."""
        out: List[Candidate] = []

        z = float(getattr(ctx, "z_delta", 0.0))
        z_abs = abs(z)
        price = float(getattr(ctx, "price", 0.0))
        
        # 0) Всплеск дисбаланса стакана (OBI Spike)
        obi_sustained = bool(getattr(ctx, "obi_sustained", False))
        obi_avg = float(getattr(ctx, "obi_avg", 0.0))
        
        if obi_sustained and abs(obi_avg) >= self.cfg.obi_spike_thr:
            # Стакан перекошен и держится нужное время
            out.append(Candidate(
                kind="obi_spike",
                direction=1 if obi_avg > 0 else -1,  # 1=BUY, -1=SELL
                raw_score=float(obi_avg),
                level_key=self._nearest_pivot_key(price, pivots),
                reasons=[f"obi_spike avg={obi_avg:.3f}"],
            ))
        
        # Если z_delta ниже базового порога — нет события движения
        if z_abs < self.cfg.main_z_threshold:
            return out  # Только возможный OBI Spike
        
        # 1) Absorption (Поглощение): сильный импульс БЕЗ движения цены
        if z_abs >= self.cfg.absorption_z_threshold:
            out.append(Candidate(
                kind="absorption",
                direction=-1,       # Fade (против движения)
                raw_score=float(-z),  # Отрицательный знак = разворот
                level_key=self._nearest_pivot_key(price, pivots),
                reasons=[f"absorption spike z={z:.3f}"],
            ))
        
        # 2) Breakout (Пробой): сильный импульс + пробой ключевого уровня
        if z_abs >= self.cfg.breakout_z_threshold:
            crossed_level = self._breakout_cross_info(price, z > 0, pivots)
            if crossed_level:  # Только если есть пробой уровня
                out.append(Candidate(
                    kind="breakout",
                    direction=1,    # По тренду
                    raw_score=float(z),
                    level_key=str(crossed_level),
                    reasons=[f"breakout cross={crossed_level} z={z:.3f}"],
                ))
        
        # 3) Extreme (Экстремальный импульс): z очень высокий
        if z_abs >= self.cfg.extreme_z_threshold:
            out.append(Candidate(
                kind="extreme",
                direction=1,
                raw_score=float(z),
                level_key="na",
                reasons=[f"extreme z={z:.3f} thr={self.cfg.extreme_z_threshold:.3f}"],
            ))
        
        return out  # Список всех найденных событий в этом тике
```

**Конфигурация DetectorCfg** (пример для BTCUSDT):
```yaml
# Из docker-compose-crypto-orderflow.yml
BTCUSDT_DELTA_Z_THRESHOLD=3.10        # Базовый порог для включения детектора
BTCUSDT_ABSORPTION_Z_THRESHOLD=2.5   # Для поглощения (мягче)
BTCUSDT_BREAKOUT_Z_THRESHOLD=3.10    # Для пробоя
BTCUSDT_EXTREME_Z_THRESHOLD=5.0      # Для экстремальных событий
```

---

## 5. Валидаторы (Quality Layer) — первые фильтры кандидата

После создания `Candidate` по нему немедленно "пробегают" валидаторы из `crypto_orderflow_quality.py`. Каждый валидатор может поставить `veto_with("reason")` — и кандидат мертв.

### SpreadValidator — контроль спреда
```python
@dataclass(frozen=True)
class SpreadValidator(Validator):
    spread_max_bps: float  # максимальный допустимый спред

    def validate(self, ctx: Any, cand: Candidate, q: QualityState) -> None:
        sp = float(getattr(ctx, "spread_bps", 0.0))  # текущий спред
        q.add_flag("spread_bps", sp)                  # логируем для отладки
        
        if sp > self.spread_max_bps:
            q.veto_with(f"spread>{self.spread_max_bps:.2f}bps")
            # Пример: spread_bps=15.0 > max=10.0 → VETO "spread>10.00bps"
```

### OBIBreakoutValidator — проверка поддержки стакана для Breakout
```python
@dataclass(frozen=True)
class OBIBreakoutValidator(Validator):
    require_obi: bool    # Требовать поддержку стакана?
    require_obi20: bool  # Ещё строже: 20 секунд устойчивой поддержки?

    def validate(self, ctx: Any, cand: Candidate, q: QualityState) -> None:
        if cand.kind != "breakout":
            return  # Только для пробоев

        z = float(getattr(ctx, "z_delta", 0.0))
        obi_sustained = bool(getattr(ctx, "obi_sustained", False))
        obi_avg = float(getattr(ctx, "obi_avg", 0.0))
        
        # OBI "подтверждает" пробой, если он в той же сторону что и z
        obi_confirms = obi_sustained and (obi_avg * z > 0.0)
        
        if self.require_obi and not obi_confirms:
            q.veto_with("breakout_requires_obi")
            # Пробой без стакана = VETO (вероятно ложный сигнал)
```

### ModeValidator — контекст режима рынка
```python
class ModeValidator(Validator):
    def validate(self, ctx: Any, cand: Candidate, q: QualityState) -> None:
        mode = str(getattr(ctx, "market_mode", "mixed")).lower()
        z_abs = abs(float(getattr(ctx, "z_delta", 0.0)))
        
        # Breakout в режиме mean-reversion требует СИЛЬНЕЕ импульс (защита)
        if cand.kind == "breakout" and mode == "meanrev":
            thr = float(getattr(ctx, "_breakout_thr", 0.0))
            if z_abs < (thr * 1.2):  # Усиленный порог на 20%
                q.veto_with("breakout_in_meanrev_requires_stronger_z")
        
        # Absorption в бычьем тренде = опасно торговать против
        if cand.kind == "absorption" and mode == "momentum":
            q.veto_with("absorption_in_momentum")
```

---

## 6. Итог: Объект Candidate

После прохождения всех базовых валидаторов, остаётся "живой" `Candidate`:

```python
# Реальная структура (из crypto_orderflow_pipeline_types.py)
@dataclass
class Candidate:
    kind: str           # "breakout" | "absorption" | "extreme" | "obi_spike"
    direction: int      # +1 = лонг, -1 = шорт
    raw_score: float    # Базовая сила сигнала (z_delta)
    level_key: str      # Ключ уровня (например "pdh" или "1h_ema")
    reasons: list       # Список причин срабатывания
    
    # После валидации (QualityState):
    quality_flags: dict = {
        "spread_bps": 1.5,
        "obi_confirms": True,
        "market_mode": "trend_up",
        "regime_score": 0.75
    }

# Этот объект передаётся дальше в:
# → ScoreModel (LightGBM) для оценки вероятности успеха
# → ML Confirm Gate (второй эшелон)
# → Pre-Publish Gates (финальные бизнес-фильтры)
```
