### Цель
Предотвратить потерю финальных батчей данных (ticks/books) в `scanner-go-worker` во время штатного выключения контейнера (SIGTERM).

### Что мы имеем
В данный момент, функция `BatchTickPublisher.worker()` и `Close()` жестко захардкожены на таймаут в 2 секунды. Во время спайков трафика этого времени недостаточно для того, чтобы весь накопленный буфер корректно отправился в Redis через пайплайн до того, как контекст отменяется или docker принудительно убивает контейнер (SIGKILL).

### План решения
1. Извлечь жестко заданные 2 секунды и задать их через конфигурацию окружения `DRAIN_TIMEOUT_SEC=10` (по умолчанию 10 секунд).
2. Заменить таймаут во время `flushCtx` в `batch_publisher.go:worker` на новое значение из `DRAIN_TIMEOUT_SEC`.
3. Заменить таймаут закрытия самого паблишера и воркера ликвидаций в `main.go`.
4. Явно добавить `stop_grace_period: 30s` и переменную `DRAIN_TIMEOUT_SEC` в настройки `docker-compose-go-workers.yml` и `.env`.

### Детали реализации

- **`go-worker/internal/redis/batch_publisher.go`**
  - Добавлена функция `drainTimeoutFromEnv()`, читающая переменную `DRAIN_TIMEOUT_SEC`.
  - В структуру `BatchTickPublisher` добавлено поле `drainTimeout`, которое инициализируется один раз при создании через `NewBatchTickPublisher()`.
  - Применен этот таймаут внутри `worker()` для `flushCtx` и отправки в DLQ в `Publish()`.

- **`go-worker/cmd/worker/main.go`**
  - Заменено значение с `2 * time.Second` на чтение ENV в переменной `drainTimeoutSec := getEnvInt("DRAIN_TIMEOUT_SEC", 10)` перед остановкой сервисов, передавая его в `batchPublisher.Close()` и в `liqController.Stop()`.

- **`docker-compose-go-workers.yml`**
  - Во все `go-worker-*` сервисы явно добавлен `stop_grace_period: 30s` (чтобы дать приложению завершить flush, даже если это занимает время).
  - В каждый `environment:` блок добавлено `DRAIN_TIMEOUT_SEC=${DRAIN_TIMEOUT_SEC:-10}`.

- **`.env / .env.example`**
  - Задокументирован и установлен `DRAIN_TIMEOUT_SEC=10`.

### Тестирование
Для теста можно послать SIGTERM (`docker stop scanner-go-worker-1m`) во время интенсивного входящего потока данных по WebSocket из Binance и проследить за логами остановки. Если flush успевает завершиться — мы не увидим сообщений об абортах соединений из Redis (context deadline exceeded) во время grace period-а.

### Роллаут / Откат
Отправляем сборки на стейджинг (canary), после этого деплоим в прод. Откат тривиальный: `git revert` и `docker-compose up -d --build`.

**Чек-лист готовности:**
- [x] Код `main.go` обновлен.
- [x] Код `batch_publisher.go` обновлен.
- [x] Docker конфигурации синхронизированы.
