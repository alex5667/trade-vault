# Часто задаваемые вопросы (FAQ)

Этот раздел отвечает на вопросы, которые чаще всего задают новые участники команд и дежурные инженеры. При появлении новых вопросов дополняйте список.

---

## 1. Общие вопросы

**Q: С чего начать знакомство с проектом?**  
A: Пройдите `overview.md`, затем `architecture.md`, выполните шаги из `setup.md`, изучите `data_flow.md`. Для углубления — `services.md` и `operations.md`.

**Q: Где найти список команд Makefile?**  
A: Выполните `make help` или посмотрите таблицу в `documentation/README.md#быстрый-справочник`.

**Q: Какую версию Go/Python использовать?**  
A: Go ≥ 1.22, Python ≥ 3.11. Смотрите `setup.md#требования-к-окружению`.

---

## 2. Ingestion и тики

**Q: Как проверить, что тики приходят?**  
A: `make tick-streams` покажет lag и задержки. Также можно выполнить `redis-cli xlen stream:tick_btcusdt`.

**Q: Что делать, если Binance WebSocket отключается?**  
A: Проверьте логи `go-worker`, убедитесь, что ключи корректны. При необходимости перезапустите сервис (`make go-worker-restart`). Если проблема мультирегиональная, переключитесь на резервный источник (см. `documentation/ticks/`).

**Q: Что такое P4.1 и временные метки t0-t5?**  
A: Это унифицированный контракт задержек. t0 — декодирование на входе, t1 — запись в Redis, t2 — начало логики, t3 — генерация сигнала, t4 — запись в журнал, t5 — ответ биржи. Позволяет точно найти узкое место. См. `architecture.md#p41-unified-latency-contract`.

**Q: Как реплейить исторические тики?**  
A: Используйте `make replay-ticks FILE=fixtures/...`. Подробности в `data_flow.md#реплей-данных`.

---

## 3. Сигналы и трейлинг

**Q: Где лежат профили трейлинга?**  
A: В Redis Hash `profiles:trailing:*`. Управление описано в `trading_workflow/tp1_trailing.md` и `services.md#tp1_trailing_orchestrator`.

**Q: Как увидеть детали конкретного сигнала?**  
A: `redis-cli hgetall signals:<sid>` или `make signal-inspect SID=<sid>`.

**Q: Почему сигнал не попал в очередь ордеров?**  
A: Проверьте `filtered_signal_writer` — возможно, его заблокировал риск-фильтр. Подробнее в `troubleshooting.md#signal-hub-и-risk-filters`.

**Q: Как работают OrderFlow handlers?**  
A: Handlers наследуются от `base_orderflow_handler.py` и реализуют специфическую логику для каждого символа. Они рассчитывают delta, OBI, spike detection и weak progress. Подробности в `services.md#orderflow-handlers`.

**Q: Что такое G0-G15 gates?**  
A: Это цепочка независимых проверок сигнала. G6 — Strong Gate (OF confirm), G10 — ML Gate, G12 — Confidence. Если любой гейт в `ENFORCE` режиме дает вето — сигнал блокируется. См. `services.md#cryptoorderflow`.

**Q: Что такое calibration и как она влияет на сигналы?**  
A: Auto calibration service оптимизирует пороги сигналов на основе исторических данных. Она влияет на качество сигналов через threshold tuning. Смотрите `services.md#auto-calibration-service`.

---

## 4. Go gateway и MT5

**Q: Как протестировать Go gateway?**  
A: Используйте `make gateway-test`. Он проверит `/healthz`, `/orders/push`, `/orders/poll`.

**Q: Где настроить токены для MT5?**  
A: В `.env.local` (`MT5_EVENT_TOKEN`, `MT5_ORDER_TOKEN`). Не забудьте обновить конфиг в MT5 (`TickBridge`).

**Q: Что делать, если MT5 не подтверждает команды?**  
A: Проверьте `/orders/ack` в логах gateway, состояние MT5 терминала. Если проблема не решается — эскалируйте `@trading-ops`.

**Q: Почему мы перешли на Journal-First исполнение?**  
A: Для детерминизма и консистентности. Запись в `orders:exec` гарантирует, что даже при падении экзекутора мы сможем восстановить состояние и избежать дублей. См. `architecture.md#journal-first`.

---

## 5. Отчёты и мониторинг

**Q: Где посмотреть результаты Signal Performance Tracker?**  
A: В `stats:{strategy}:{symbol}:{tf}` (Redis), в разделах Grafana и в Telegram уведомлениях. Команда `make tracker-stats` показывает статус.

**Q: Как включить/отключить Telegram-уведомления?**  
A: Переменная `TELEGRAM_NOTIFICATIONS_ENABLED` в конфиге tracker или worker. После изменения перезапустите сервис.

**Q: Где лежат отчёты?**  
A: Каталог `reports/`. Формат CSV/JSON, название включает дату и стратегию.

---

## 6. Аналитика и GPU

**Q: Как работает Analytics V2/V3?**  
A: Это продвинутая система аналитики с ROC-кривым, threshold tuning и ML-интеграцией. Она анализирует качество сигналов и автоматически оптимизирует параметры. Управление через `make analytics-start`.

**Q: Что делать, если GPU compute service не работает?**  
A: Проверьте CUDA установку (`nvidia-smi`), переменные окружения (`CUDA_VISIBLE_DEVICES`), и логи сервиса. Возможно, нужно перезапустить с `GPU_COMPUTE_ENABLED=false` для fallback на CPU.

**Q: Как экспортировать данные для ML моделей?**  
A: Используйте dataset export functionality. Настройте `EXPORT_WINDOW_DAYS` и формат (Parquet/CSV), затем запустите через `make analytics-export`. Результаты сохраняются в `data/exports/`.

**Q: Что если ML модель "протухла" (Drift)?**  
A: Проверьте Grafana дашборд `Drift Detection`. Если метрика PSI > 0.2, модель требует переобучения. Гейт G10 автоматически перейдет в `SHADOW`, если включен safeguard. См. `operations.md#ml-governance`.

**Q: Что показывают ROC-кривые?**  
A: ROC (Receiver Operating Characteristic) показывает качество бинарной классификации сигналов. Высокий AUC означает хорошую дискриминацию между profitable и unprofitable сигналами.

---

## 7. Разработка и тесты

**Q: Какие тесты обязательны перед коммитом?**  
A: `make lint-go`, `make lint-python`, `make test-go`, `make test-python`, `make docs-lint` для документации. CI прогоняет весь набор.

**Q: Как локально отладить сервис?**  
A: Используйте `make <service>-start` и `make <service>-logs`. Для Python сервисов также доступен режим `make <service>-dev` (с hot reload).

**Q: Где посмотреть код стайл и требования к PR?**  
A: `documentation/DEVELOPMENT.md` содержит разделы по Python/Go и чек-листы PR.

---

## 8. Инфраструктура

**Q: Как проверить статус Redis?**  
A: `make redis-stats`, `redis-cli info`, Grafana `System Health`.

**Q: Как снять бэкап Redis?**  
A: `make backup-redis`. Для восстановления — `make restore-redis BACKUP=<path>`.

**Q: Что делать при нехватке диска?**  
A: Очистить временные файлы (`make clean`), prune docker volumes (`docker volume prune`), архивировать отчёты.

---

## 9. Процессы и документация

**Q: Как обновить документацию?**  
A: Внести правки, запустить `make docs-lint`, добавить пункт в `documentation/README.md#что-изменилось`, сообщить в `#scanner_docs`.

**Q: Где хранятся ADR?**  
A: `docs/adr/`. При архитектурных изменениях создавайте новый ADR и ссылку в `architecture.md`.

**Q: Как предложить улучшение?**  
A: Создать issue с тегом `enhancement`, обсудить в `#scanner_docs`, обновить `roadmap.md` после согласования.

---

## 10. Служба поддержки

**Q: Кто on-call сегодня?**  
A: Посмотрите расписание в PagerDuty или в закреплённом сообщении `#scanner_ops`.

**Q: Куда писать, если документация устарела?**  
A: В `#scanner_docs` или создать issue `documentation/full_guide`.

**Q: Как эскалировать P1 инцидент?**  
A: Следуйте `operations.md#алерты-и-эскалация`. PagerDuty уведомит on-call, при необходимости подключайте менеджеров.

---

Если возник вопрос, которого нет в списке — добавьте его сюда с понятным ответом. FAQ экономит время всех команд.
