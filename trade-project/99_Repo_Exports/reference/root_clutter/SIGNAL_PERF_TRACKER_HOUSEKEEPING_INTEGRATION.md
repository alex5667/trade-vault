# Интеграция Housekeeping для SignalPerformanceTracker

## Обзор

Документ описывает интеграцию механизма финализации зависших позиций в `SignalPerformanceTracker` для исправления утечек памяти и искажений статистики.

## Проблема

### До интеграции

1. **Утечка памяти**: позиции, которые "вошли, но не вышли" (entry без exit), оставались в `_states` навсегда
2. **Искажение статистики**: `Outcome.EXPIRED_NO_TARGET` существовал, но никогда не использовался
3. **Поздние события**: после рестарта/лага "поздние exit события" могли пересоздать state и исказить статистику
4. **Не идемпотентная финализация**: повторные вызовы могли дублировать записи

### Последствия

- Рост памяти со временем (утечка)
- Неточная статистика по TTD/MFE/MAE
- Невозможность корректно оценить качество сигналов
- Проблемы после рестартов системы

## Решение

### 1. Расширение конфигурации (`_RuntimeCfg`)

**Файл**: `python-worker/handlers/crypto_orderflow/config/runtime_config.py`

Добавлены новые параметры:

```python
@dataclass(frozen=True)
class _RuntimeCfg:
    # ... существующие поля ...
    
    # NEW: финализация "вошли, но не вышли"
    max_lifetime_bars_after_entry: int      # TTL в барах после entry
    max_lifetime_ms_after_entry: int        # Fallback TTL в миллисекундах (0 = выключено)
    housekeeping_every_ms: int              # Частота housekeeping (троттлинг)
    expiry_bars: int                        # TTL до входа (для pre-entry expiry)
```

**Переменные окружения**:

```bash
# Рекомендуемые значения для production
ORDERFLOW_EXPIRY_BARS=60                           # 60 баров до входа
ORDERFLOW_MAX_LIFETIME_BARS_AFTER_ENTRY=180        # 3 часа после входа (180 минут)
ORDERFLOW_MAX_LIFETIME_MS_AFTER_ENTRY=0            # Отключен (используем баровый TTL)
ORDERFLOW_HOUSEKEEPING_EVERY_MS=1000               # Проверка раз в секунду
```

### 2. Расширение модели состояния (`SignalPerfState`)

**Файл**: `python-worker/signal_exec/performance_tracker.py`

Добавлены поля для отслеживания bar-индексов и причины финализации:

```python
@dataclass
class SignalPerfState:
    # ... существующие поля ...
    
    # NEW: бар-индексы для TTL в барах
    bar_signal: Optional[int] = None        # Бар сигнала
    bar_entry: Optional[int] = None         # Бар входа
    bar_exit: Optional[int] = None          # Бар выхода
    
    # NEW: причина финализации (для аудита)
    finalize_reason: Optional[str] = None
```

### 3. Housekeeping механизм

**Файл**: `python-worker/signal_exec/performance_tracker.py`

#### 3.1. Инициализация

```python
class SignalPerformanceTracker:
    def __init__(
        self, 
        # ... существующие параметры ...
        max_lifetime_bars_after_entry: int = 180,
        max_lifetime_ms_after_entry: int = 0,
        housekeeping_every_ms: int = 1000,
    ):
        # ... существующая инициализация ...
        
        # NEW: параметры housekeeping
        self.max_lifetime_bars_after_entry = max_lifetime_bars_after_entry
        self.max_lifetime_ms_after_entry = max_lifetime_ms_after_entry
        
        # NEW: троттлинг (не O(N) на каждом тике)
        self._housekeeping_sampler = TimeSampler(housekeeping_every_ms / 1000.0)
        
        # NEW: LRU для защиты от поздних событий
        self._finalized_lru: deque[str] = deque(maxlen=4096)
```

#### 3.2. Автоматический вызов через `on_bar`

```python
def on_bar(self, symbol: str, bar: Bar1m, bar_idx: Optional[int] = None) -> None:
    """
    NEW: периодически вызывает housekeeping для финализации зависших позиций.
    """
    # Периодически проверяем зависшие state
    now_ts_ms = int(bar.ts.timestamp() * 1000)
    if self._housekeeping_sampler.hit():
        self._housekeep_expired(now_ts_ms=now_ts_ms, now_bar=bar_idx)
    
    # ... существующая обработка баров ...
```

#### 3.3. Логика housekeeping

```python
def _housekeep_expired(self, now_ts_ms: int, now_bar: Optional[int]) -> None:
    """
    КРИТИЧНО: закрывает дыру "вошли, но не вышли".

    Правила:
    A) Если entry был и exit нет -> после max_lifetime_bars_after_entry
       -> EXPIRED_NO_TARGET + finalize.
    
    B) Если entry не было и сигнал протух по expiry_bars
       -> EXPIRED_NO_TARGET + finalize.
    """
    if not self._states:
        return

    # Безопасное удаление: snapshot списка
    items = list(self._states.items())
    for signal_id, st in items:
        if st.finalized:
            self._states.pop(signal_id, None)
            continue

        # A) Вошли, но не вышли
        if st.ts_entry is not None and st.ts_exit is None:
            # 1) Баровый TTL (предпочтительно)
            if now_bar is not None and st.bar_entry is not None:
                held_bars = now_bar - st.bar_entry
                if held_bars >= self.max_lifetime_bars_after_entry:
                    self._finalize_and_store(
                        st,
                        reason=f"expired_no_target: held_bars={held_bars}",
                    )
                    continue

            # 2) Fallback TTL по времени
            if self.max_lifetime_ms_after_entry > 0:
                held_ms = now_ts_ms - int(st.ts_entry.timestamp() * 1000)
                if held_ms >= self.max_lifetime_ms_after_entry:
                    self._finalize_and_store(
                        st,
                        reason=f"expired_no_target: held_ms={held_ms}",
                    )
                    continue

        # B) Сигнал протух до входа
        if st.ts_entry is None and st.ts_exit is None:
            if now_bar is not None and st.bar_signal is not None:
                age_bars = now_bar - st.bar_signal
                if age_bars >= st.expiry_bars:
                    self._finalize_and_store(
                        st,
                        reason=f"expired_pre_entry: age_bars={age_bars}",
                    )
                    continue
```

### 4. Идемпотентная финализация

```python
def _finalize_and_store(self, state: SignalPerfState, reason: Optional[str] = None) -> None:
    """
    Финализация должна быть:
    - идемпотентной (двойной вызов не ломает статистику)
    - удалять state из памяти (исправляет утечки)
    - не давать "поздним" событиям воскресить state
    """
    # NEW: идемпотентность
    if state.finalized:
        return
    
    state.finalized = True
    state.finalize_reason = reason or "normal_finalization"

    # 1) NEW: сохранить signal_id в LRU
    self._finalized_lru.append(state.signal_id)

    # 2) Сохранить в БД (outcome может быть EXPIRED_NO_TARGET)
    # ... существующая логика сохранения ...

    # 3) NEW: удалить из активных (критично!)
    self._states.pop(state.signal_id, None)
```

### 5. Защита от поздних событий

```python
def on_execution_event(
    self,
    signal_id: str,
    event_type: str,
    ts: datetime,
    price: float,
    bar_idx: Optional[int] = None,
) -> None:
    """
    NEW: игнорирует события для финализированных signal_id.
    """
    # NEW: защита от поздних событий
    if self._is_recently_finalized(signal_id):
        # Поздний exit/entry после финализации - игнорируем
        return
    
    # ... существующая обработка событий ...

def _is_recently_finalized(self, signal_id: str) -> bool:
    """
    Проверяет, был ли signal_id недавно финализирован.
    O(N) по deque, но maxlen небольшой (4096).
    """
    return signal_id in self._finalized_lru
```

## Тестирование

### Файл тестов

`tests/test_expired_no_target.py` - полный набор тестов для проверки функциональности.

### Запуск тестов

```bash
# Из корня проекта
cd /home/alex/front/trade/scanner_infra
python -m pytest tests/test_expired_no_target.py -v

# Или напрямую
python tests/test_expired_no_target.py
```

### Покрытие тестами

1. ✅ **Финализация по TTL в барах** - позиция удаляется после max_lifetime_bars_after_entry
2. ✅ **Финализация по TTL в миллисекундах** - fallback для случаев без bar_idx
3. ✅ **Защита от поздних exit** - поздние события игнорируются
4. ✅ **Защита от поздних entry** - поздние входы игнорируются
5. ✅ **Pre-entry expiry** - сигналы без входа финализируются
6. ✅ **Идемпотентность** - повторная финализация безопасна
7. ✅ **Интеграция с on_bar** - автоматический вызов housekeeping
8. ✅ **Нормальный exit** - корректная работа до истечения TTL

## Использование

### Создание tracker с housekeeping

```python
from signal_exec.performance_tracker import SignalPerformanceTracker
from signal_exec.repository import SignalRepository

# Создаем tracker с настройками housekeeping
tracker = SignalPerformanceTracker(
    repo=SignalRepository(database_url),
    ttd_target_R=1.0,
    max_ttd_bars=30,
    bus=None,
    max_lifetime_bars_after_entry=180,      # 3 часа после входа
    max_lifetime_ms_after_entry=0,          # Отключен (используем баровый TTL)
    housekeeping_every_ms=1000,             # Проверка раз в секунду
)
```

### Регистрация сигнала с bar_idx

```python
# При регистрации сигнала передаем текущий bar_idx
tracker.register_signal(
    ctx=signal_context,
    plan=execution_plan,
    bar_idx=current_bar_idx,  # NEW: для TTL в барах
)
```

### Обработка баров с bar_idx

```python
# При обработке баров передаем bar_idx
tracker.on_bar(
    symbol="BTCUSDT",
    bar=bar_1m,
    bar_idx=current_bar_idx,  # NEW: для housekeeping
)
```

### Обработка событий с bar_idx

```python
# При обработке execution событий передаем bar_idx
tracker.on_execution_event(
    signal_id="sig_123",
    event_type="ENTRY_FILLED",
    ts=entry_timestamp,
    price=entry_price,
    bar_idx=current_bar_idx,  # NEW: для отслеживания
)
```

## Мониторинг

### Метрики для отслеживания

1. **Количество активных states** - `len(tracker._states)`
2. **Количество финализированных по TTL** - фильтр по `finalize_reason` содержит "expired_no_target"
3. **Размер LRU** - `len(tracker._finalized_lru)`
4. **Частота housekeeping** - логирование вызовов `_housekeep_expired`

### Логирование

Рекомендуется добавить логирование в `_housekeep_expired`:

```python
def _housekeep_expired(self, now_ts_ms: int, now_bar: Optional[int]) -> None:
    if not self._states:
        return
    
    finalized_count = 0
    items = list(self._states.items())
    
    for signal_id, st in items:
        # ... логика финализации ...
        if st.finalized:  # если финализировали на этой итерации
            finalized_count += 1
    
    if finalized_count > 0:
        logger.info(
            f"Housekeeping: finalized {finalized_count} expired positions "
            f"(active={len(self._states)}, lru_size={len(self._finalized_lru)})"
        )
```

## Миграция существующих систем

### Шаг 1: Обновление конфигурации

Добавьте переменные окружения в `docker-compose.yml` или `.env`:

```yaml
environment:
  - ORDERFLOW_EXPIRY_BARS=60
  - ORDERFLOW_MAX_LIFETIME_BARS_AFTER_ENTRY=180
  - ORDERFLOW_MAX_LIFETIME_MS_AFTER_ENTRY=0
  - ORDERFLOW_HOUSEKEEPING_EVERY_MS=1000
```

### Шаг 2: Обновление кода

Если вы используете `SignalPerformanceTracker`, обновите вызовы:

```python
# Старый код
tracker.register_signal(ctx, plan)
tracker.on_bar(symbol, bar)
tracker.on_execution_event(signal_id, event_type, ts, price)

# Новый код (с bar_idx)
tracker.register_signal(ctx, plan, bar_idx=current_bar_idx)
tracker.on_bar(symbol, bar, bar_idx=current_bar_idx)
tracker.on_execution_event(signal_id, event_type, ts, price, bar_idx=current_bar_idx)
```

### Шаг 3: Тестирование

1. Запустите тесты: `pytest tests/test_expired_no_target.py -v`
2. Проверьте логи на наличие финализаций
3. Мониторьте размер `_states` - должен стабилизироваться

### Шаг 4: Очистка старых данных

Если в production уже есть зависшие states, можно:

1. **Перезапустить сервис** - при старте `_states` будет пуст
2. **Ручная очистка** - вызвать `_housekeep_expired` с текущим временем
3. **Мониторинг** - отследить стабилизацию памяти

## Производительность

### Сложность операций

- `_housekeep_expired`: O(N) где N = количество активных states
- `_is_recently_finalized`: O(M) где M = размер LRU (4096)
- Троттлинг: вызов раз в `housekeeping_every_ms` (по умолчанию 1 сек)

### Рекомендации

1. **housekeeping_every_ms**: 1000-5000 мс для production
2. **LRU maxlen**: 4096 достаточно для большинства случаев
3. **max_lifetime_bars_after_entry**: 3 * expiry_bars как минимум

### Оптимизация

Если `_is_recently_finalized` становится узким местом (маловероятно):

```python
# Заменить deque на OrderedDict + set для O(1)
from collections import OrderedDict

self._finalized_lru = OrderedDict()  # {signal_id: timestamp}
self._finalized_set = set()          # для O(1) проверки

def _is_recently_finalized(self, signal_id: str) -> bool:
    return signal_id in self._finalized_set
```

## Обратная совместимость

### Параметры по умолчанию

Все новые параметры имеют разумные значения по умолчанию:

```python
max_lifetime_bars_after_entry=180      # 3 часа
max_lifetime_ms_after_entry=0          # Отключен
housekeeping_every_ms=1000             # 1 секунда
```

### Опциональные параметры

Все новые параметры методов опциональны:

```python
# Работает без bar_idx (fallback на TTL по времени)
tracker.register_signal(ctx, plan)  # bar_idx=None
tracker.on_bar(symbol, bar)         # bar_idx=None
```

## Заключение

Интеграция housekeeping механизма решает критические проблемы:

1. ✅ **Утечка памяти** - исправлена через автоматическую финализацию
2. ✅ **Искажение статистики** - исправлено через идемпотентную финализацию
3. ✅ **Поздние события** - защита через LRU финализированных signal_id
4. ✅ **Outcome.EXPIRED_NO_TARGET** - теперь реально используется

Система теперь корректно обрабатывает все edge cases и гарантирует точность статистики.

## Дополнительные материалы

- **Тесты**: `tests/test_expired_no_target.py`
- **Конфигурация**: `python-worker/handlers/crypto_orderflow/config/runtime_config.py`
- **Tracker**: `python-worker/signal_exec/performance_tracker.py`
- **Модели**: `python-worker/signal_exec/models.py`

## Контакты

При возникновении вопросов или проблем:
1. Проверьте тесты: `pytest tests/test_expired_no_target.py -v`
2. Проверьте логи на наличие финализаций
3. Мониторьте размер `_states` и `_finalized_lru`

