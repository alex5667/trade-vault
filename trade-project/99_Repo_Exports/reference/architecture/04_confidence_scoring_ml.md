# Этап 4: Оценка уверенности (Confidence Scorer & ML Models)

## Что это и зачем?
Детекторы могут "видеть" паттерн там, где его нет — это называется False Positive (ложное срабатывание). Мы не можем полагаться только на правила. Здесь вступают в игру **Machine Learning модели**: они обучены на исторических данных и могут оценить, насколько _вероятно_, что сигнал окажется прибыльным.

Аналогия: опытный трейдер, увидев паттерн, думает: "Я видел это 500 раз. 70% случаев этот пробой отрабатывал." Модель LightGBM делает именно это — выдает вероятность.

---

## 1. ScoreModel — LightGBM (Primary Scorer)

**Что такое LightGBM?** Это алгоритм градиентного бустинга (Gradient Boosted Decision Trees). Проще говоря: ансамбль из сотен маленьких "деревьев решений", каждое из которых исправляет ошибки предыдущего. LightGBM особенно хорош для задач классификации на табличных данных.

```python
# Файл: handlers/scoring/score_model.py
import lightgbm as lgb

class ScoreModel:
    def __init__(self, model_path: str):
        # Загрузка обученной модели из файла
        self.model = lgb.Booster(model_file=model_path)
        # Пример пути: "python-worker/ml_models/scorer_model.lgb"
    
    def predict(self, features: dict) -> float:
        """
        Возвращает вероятность успеха сделки (0.0 - 1.0)
        """
        # Собираем вектор признаков в нужном порядке
        X = np.array([[
            features["delta_z"],           # Сила CVD импульса
            features["obi_score"],          # Дисбаланс стакана
            features["book_churn"],         # Токсичность стакана
            features["spread_bps"],         # Текущий спред
            features["taker_rate_ema"],     # Агрессивность покупок (EMA)
            features["geometry_score"],     # Близость к уровням HTF
            features["atr"],                # Волатильность
            features["regime_score"],       # Режим рынка
        ]])
        
        # Предсказание (raw probability)
        raw_prob = self.model.predict(X)[0]
        return float(raw_prob)  # Например: 0.73 (73% вероятность успеха)
```

### Изокалибровка (Calibration)
Сырой выход LightGBM не всегда хорошо откалиброван — 73% модели не обязательно означают, что 73 сделки из 100 будут прибыльными. Поэтому применяется **Isotonic Regression**:

```python
from sklearn.calibration import IsotonicRegression

# После обучения LightGBM:
calibrator = IsotonicRegression(out_of_bounds="clip")
calibrator.fit(y_scores_train, y_true_train)

# Применение в продакшене:
raw_score = model.predict(X)[0]           # Например: 0.68
calibrated_prob = calibrator.transform([raw_score])[0]  # Стало: 0.71

# Конвертируем в проценты для пользователя
confidence = calibrated_prob * 100         # = 71.0%
```

### Порог отсечения (Hard Floor)
```python
# Из docker-compose-crypto-orderflow.yml
MIN_SIGNAL_CONFIDENCE=70        # Глобальный минимум
MIN_SIGNAL_CONFIDENCE__XAUUSD=20  # Для золота (другой профиль сигналов)

# Логика:
if confidence < float(os.getenv("CRYPTO_SIGNAL_MIN_CONF", "70")):
    logger.debug("Сигнал отброшен: confidence=%.1f%% < min=70%%", confidence)
    return None  # Мусор, дальше не идем
```

---

## 2. ML Confirm Gate (OFConfirmEngine) — Второй Эшелон

Это дополнительная проверка. Даже если Primary Scorer сказал "ОК" — Confirm Gate может не согласиться. Аналогия: старший врач перепроверяет диагноз ординатора.

```python
# Файл: core/of_confirm_engine.py
class OFConfirmEngine:
    """
    Двухуровневая система проверки:
    1. ML Confirm Gate (sklearn/joblib модель)
    2. Правила (условия пропуска при неуверенности)
    """
    def __init__(self, version: int = 2, ml_gate=None):
        self.ml_gate = ml_gate   # Объект MLConfirmGate

    def confirm(self, ctx) -> ConfirmResult:
        if not self.ml_gate:
            return ConfirmResult(decision="ALLOW_RULE", reason="ml_gate_disabled")
        
        # Получаем вероятность от L2 модели
        p = self.ml_gate.predict(ctx)  # Например: 0.62
        
        # Жесткий минимум
        p_min_hard = float(os.getenv("ML_CONFIRM_P_MIN_HARD_FLOOR", "0.40"))
        if p < p_min_hard:
            return ConfirmResult(decision="DENY", reason="below_hard_floor", p=p)
        
        # Зона неуверенности (Abstain Band)
        # Если модель не уверена (0.49-0.51) — воздерживаемся
        p_min = float(os.getenv("ML_CONFIRM_P_MIN", "0.55"))
        abstain = float(os.getenv("ML_CONFIRM_ABSTAIN_BAND", "0.02"))
        
        if p < p_min - abstain:
            return ConfirmResult(decision="DENY", reason="below_p_min", p=p)
        elif p < p_min + abstain:
            return ConfirmResult(decision="ALLOW_RULE_ABSTAIN", reason="abstain_band", p=p)
        else:
            return ConfirmResult(decision="CONFIRM", reason="above_p_min", p=p)
```

---

## 3. Режимы работы (SHADOW vs ENFORCE)

Это крайне важная концепция для безопасного деплоя ML-моделей:

```yaml
# docker-compose-crypto-orderflow.yml
ML_CONFIRM_MODE=SHADOW   # или ENFORCE
```

| Режим | Поведение | Когда использовать |
|-------|-----------|-------------------|
| `SHADOW` | Gate считает, но НЕ блокирует. Пишет в метрики | Тестирование новой модели |
| `ENFORCE` | Gate реально блокирует слабые сигналы | Продакшен после проверки |

```python
# Как это реализовано:
mode = os.getenv("ML_CONFIRM_MODE", "SHADOW").upper()
result = self.of_engine.confirm(ctx)

if mode == "SHADOW":
    # Пишем результат в Prometheus для мониторинга
    ml_confirm_shadow_total.labels(decision=result.decision).inc()
    # НО сигнал пропускаем! (fail-open)
    return True  # Всегда ALLOW в shadow

elif mode == "ENFORCE":
    if result.decision == "DENY":
        signals_veto_total.labels(reason="ml_confirm_deny").inc()
        return False  # Реально блокируем
    return True
```

---

## 4. A/B Тестирование моделей (Canary Rollout)

```yaml
# Новую модель тестируем на 10% трафика
CONF_CAL_AB_MODE=split
CONF_CAL_AB_SHARE=0.10          # 10% сигналов → Challenger модель
CONF_CAL_CHALLENGER_BUNDLE_PATH=/models/challenger_v2.joblib
CONF_CAL_AB_STICKY_KEY=symbol|session  # Один инструмент = всегда одна модель
```

```python
# Логика роутинга:
def route_to_model(ctx) -> str:
    # Детерминированно (одна пара всегда в одну модель):
    key = f"{ctx.symbol}|{ctx.session}"
    h = int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    ab_share = float(os.getenv("CONF_CAL_AB_SHARE", "0.0"))
    if (h % 1000) < (ab_share * 1000):
        return "challenger"  # 10% → challenger_v2
    return "champion"        # 90% → текущая модель
```

---

## 5. Асинхронное обновление конфигов

ML-модели загружаются при старте, но их конфиги (пороги, метаданные) обновляются без остановки сервиса:

```python
# Фоновая задача (crypto_orderflow_service.py):
async def _maintain_ml_gate_loop(self) -> None:
    """Каждые 30 секунд обновляем конфиги ML Gate из Redis."""
    interval = 30.0
    
    while not self._shutdown:
        await asyncio.sleep(interval)
        
        gate = self.of_engine.ml_gate
        if gate and hasattr(gate, "refresh_async"):
            t0 = time.time()
            await gate.refresh_async(self.main)  # Синхронно из Redis
            dt = time.time() - t0
            
            if dt > 0.5:   # Если обновление заняло > 500ms
                logger.warning("⚠️ ML gate async refresh took %.1fms", dt * 1000)
```

Это позволяет менять пороги в production в режиме реального времени без перезапуска контейнера.

---

## 6. Fail-Open (Безопасный фоллбэк)

```python
# Если ML Gate упала с ошибкой — мы не крашим, а пропускаем сигнал
try:
    result = self.ml_gate.predict(ctx)
except Exception as exc:
    logger.error("ML Gate error: %s. Using fallback.", exc)
    fallback_enabled = os.getenv("ML_MODEL_FALLBACK_ENABLE", "1") == "1"
    if fallback_enabled:
        # Используем Primary Scorer как фоллбэк
        return self._primary_scorer_result(ctx)
    else:
        return ConfirmResult(decision="ERROR", reason="gate_exception")
```

**Философия fail-open**: В торговых системах лучше пропустить несколько плохих сигналов, чем заблокировать всю торговлю из-за временного сбоя инфраструктуры.
