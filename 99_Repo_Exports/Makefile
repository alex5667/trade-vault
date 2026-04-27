SHELL := /bin/bash
export TZ := UTC
export DOCKER_BUILDKIT := 1
export COMPOSE_DOCKER_CLI_BUILD := 1

.PHONY: help status logs diagnose health up up-bg update down restart rebuild clean redis-stats redis-flush redis-clear-data redis-memory signal-status signal-logs hub-status hub-logs gateway-status gateway-logs telegram-status telegram-logs worker-status worker-logs atr-status atr-logs orderflow-status orderflow-logs tracker-status tracker-logs tracker-restart check-xauusd-services check-telegram check-redis-streams test-tracker-telegram send-report-now db-check redis-check service-check full-status quick-status emergency-stop emergency-fix performance-fix optimize-redis circuit-reset network-test docker-cleanup volume-list volume-inspect backup-redis restore-redis export-signals check-signals seed-test paper-status paper-logs backtest-status monitor-resources api-test gateway-test hub-test signal-test notification-test full-system-check quick-restart safe-restart trailing-status trailing-logs trailing-stats trailing-test trailing-profiles trailing-help trailing-start trailing-stop trailing-restart trailing-health trailing-autotune-start trailing-autotune-stop trailing-autotune-status trailing-autotune-logs bootstrap-baselines calibration-run calibration-check calibration-auto-start calibration-auto-stop calibration-auto-status calibration-auto-logs calibration-auto-enable calibration-auto-disable relcal-analyze relcal-export relcal-plot relcal-help quality-offline quality-online quality-run quality-check quality-help news-feature-store-start news-feature-store-stop news-feature-store-status news-feature-store-logs news-calendar-store-start news-calendar-store-stop news-calendar-store-status news-calendar-store-logs news-calendar-ingestor-start news-calendar-ingestor-stop news-calendar-ingestor-status news-calendar-ingestor-logs news-pipeline-status news-pipeline-logs news-pipeline-start news-pipeline-stop test-contract test-ofc-contract test-orphan-housekeep of-nightly-calibrate-manual of-nightly-meta-train-manual meta-train-manual meta-quality-manual meta-train-status meta-quality-status meta-train-logs meta-quality-logs regress-safe-manual regress-engine-status regress-engine-logs ofc-validate ofc-fill-expected ofc-replay-strict ofc-bench ofc-golden-suite ofc-golden-clean pre-pull binance-reporter-status binance-reporter-logs binance-reporter-restart binance-report-now binance-executor-status binance-executor-logs binance-executor-restart binance-executor-start binance-executor-stop binance-status binance-logs exec-health-service-identity-status exec-health-service-identity-logs exec-health-freeze-status exec-health-freeze-prepare exec-health-freeze-approve exec-health-freeze-commit exec-health-freeze-force exec-health-freeze-undo composite-preflight-exporter-status composite-preflight-exporter-logs composite-preflight-exporter-restart composite-preflight-exporter-metrics ab-rollback-guard-v2-status ab-rollback-guard-v2-logs ab-rollback-guard-v2-restart ab-rollback-guard-v2-start ab-rollback-guard-v2-stop ab-rollback-guard-v2-audit docs-lint

docs-lint:
	@chmod +x scripts/docs_lint.sh
	@./scripts/docs_lint.sh

help:
	@echo "╔═══════════════════════════════════════════════════════════════╗"
	@echo "║        📊 Scanner Infrastructure Makefile 📊                 ║"
	@echo "╚═══════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "🎯 Основные команды:"
	@echo "  make up                - Запуск всех сервисов с логами"
	@echo "  make up-bg             - Запуск всех сервисов в фоне"
	@echo "  make down              - Остановка всех сервисов"
	@echo "  make restart           - Перезапуск всех сервисов"
	@echo "  make rebuild           - Полная пересборка и перезапуск"
	@echo "  make status            - Статус всех контейнеров"
	@echo "  make logs              - Все логи в реальном времени"
	@echo "  make health            - Проверка здоровья всех сервисов"
	@echo "  make diagnose          - Диагностика проблем системы"
	@echo ""
	@echo "📊 Local Calibration (автоматическая настройка порогов):"
	@echo "  make calibration-run       - Запустить настройку калибровки (автоматически при make up)"
	@echo "  make calibration-check     - Проверить результаты калибровки"
	@echo "  make calibration-help      - Справка по системе калибровки"
	@echo "🤖 Auto Confidence Calibration (автоматическое обучение уверенности):"
	@echo "  make calibration-auto-start  - Запустить планировщик (внутри контейнера)"
	@echo "  make calibration-auto-stop   - Остановить планировщик"
	@echo "  make calibration-auto-status - Статус планировщика"
	@echo "  make calibration-auto-logs   - Логи планировщика"
	@echo "  make relcal-analyze    - Анализ данных reliability calibrator"
	@echo "  make relcal-export     - Экспорт данных reliability calibrator в CSV"
	@echo "  make relcal-plot       - Графики калибровочных кривых"
	@echo "  make relcal-help       - Справка по reliability calibration"
	@echo ""
	@echo "📈 Signal Quality (оценка качества по истории):"

	@echo "  make quality-offline   - Offline качество по историческим данным"
	@echo "  make quality-online    - Online качество (rolling окно)"
	@echo "  make quality-run       - Полный цикл качества сигналов"
	@echo "  make quality-status    - Статус Quality Worker & Exporter"
	@echo "  make quality-logs      - Логи Quality Worker & Exporter"

	@echo "  make quality-check     - Проверить данные качества"
	@echo "  make quality-test      - Запустить unit-тесты"
	@echo "  make quality-help      - Справка по системе качества"
	@echo ""
	@echo "🛡️ Confidence Guardrails:"
	@echo "  make guardrails-apply [APPLY=1] - Run confidence guardrails manually (dry-run default)"
	@echo ""
	@echo "🤖 Trailing Autotune:"
	@echo "  make trailing-autotune-start - Запустить воркеры автотюнинга"
	@echo ""
	@echo "📊 Signal Performance Tracker (быстрый доступ):"
	@echo "  make tracker-stats     - Статистика по всем источникам сигналов"
	@echo "  make send-real-report  - Отправить отчет в Telegram СЕЙЧАС"
	@echo "  make tracker-logs      - Логи в реальном времени"
	@echo "  make tracker-restart   - Перезапуск трекера"
	@echo ""
	@echo "🎯 TP1 Trailing System (новое!):"
	@echo "  make trailing-status   - Статус TP Event Listener"
	@echo "  make trailing-logs     - Логи trailing system"
	@echo "  make trailing-stats    - Статистика трейлинга"
	@echo "  make trailing-test     - Интеграционный тест"
	@echo "  make trailing-profiles - Показать доступные профили"
	@echo "  make trailing-help     - Полная справка по trailing system"
	@echo ""
	@echo "🤖 Trailing Autotune (автоматическая настройка трейлинга):"
	@echo "  make trailing-autotune-start  - Запустить оба воркера автотюнинга"
	@echo "  make trailing-autotune-stop   - Остановить воркеры автотюнинга"
	@echo "  make trailing-autotune-status - Статус воркеров автотюнинга"
	@echo "  make trailing-autotune-logs   - Логи воркеров автотюнинга"
	@echo ""
	@echo "🤖 TM Autopilot (автоматическая оптимизация TM политик):"
	@echo "  make tm-autopilot-logs        - Логи TM Autopilot"
	@echo "  make tm-autopilot-restart     - Перезапуск TM Autopilot"
	@echo ""
#	@echo "📡 MT5 Event Executor (новое!):"
#	@echo "  make mt5-executor-start  - Запустить MT5 Event Executor"
#	@echo "  make mt5-executor-status - Статус executor"
#	@echo "  make mt5-executor-logs   - Логи executor"
#	@echo "  make mt5-executor-test   - Тест endpoint"
#	@echo ""

	@echo "📰 News Pipeline (новостной пайплайн с ultra-low latency):"
	@echo "  make news-pipeline-start      - Запустить полный новостной пайплайн"
	@echo "  make news-pipeline-stop       - Остановить полный новостной пайплайн"
	@echo "  make news-pipeline-status     - Статус всех компонентов пайплайна"
	@echo "  make news-pipeline-logs       - Логи всех компонентов пайплайна"
	@echo "  make news-feature-store-start - Запустить News Feature Store Worker"
	@echo "  make news-calendar-store-start- Запустить News Calendar Store Worker"
	@echo "  make news-calendar-ingestor-start - Запустить Calendar Ingestor (standby)"
	@echo ""
	@echo "📊 Мониторинг по сервисам:"
	@echo "  make signal-status     - Статус signal-generator"
	@echo "  make hub-status        - Статус signal-hub и aggregated-hub"
	@echo "  make gateway-status    - Статус go-gateway"
	@echo "  make telegram-status   - Статус telegram-worker"
	@echo "  make worker-status     - Статус всех Python workers (shards 1, 1b, 2)"
	@echo "  make atr-status        - Статус ATR workers"
	@echo "  make orderflow-status  - Статус orderflow handler"
	@echo "  make tracker-status    - Статус signal performance tracker"
	@echo "  make paper-status      - Статус paper executor"
	@echo "  make backtest-status   - Статус backtest сервисов"
	@echo "  make trailing-autotune-status - Статус trailing autotune"
	@echo "  make recs-status       - Статус сервиса одобрения рекомендаций"
	@echo "  make recs-logs         - Логи сервиса одобрения рекомендаций"
	@echo "  make of-timers-status  - Статус OF Timers Worker (config drift, weekly bench)"
	@echo "  make of-timers-logs     - Логи OF Timers Worker"
	@echo "  make monitoring-smoke-exporter-status - Статус Monitoring Smoke Exporter"
	@echo "  make monitoring-smoke-exporter-logs   - Логи Monitoring Smoke Exporter"
	@echo "  make health-reporter-status  - Статус Health Reporter (ATR/CVD/Streams)"
	@echo "  make health-reporter-logs     - Логи Health Reporter"
	@echo "  make health-reporter-restart - Перезапуск Health Reporter"
	@echo "  make binance-reporter-status  - Статус Binance Account Reporter"
	@echo "  make binance-reporter-logs    - Логи Binance Account Reporter"
	@echo "  make binance-reporter-restart - Перезапуск Binance Account Reporter"
	@echo "  make binance-report-now       - 📤 Отправить Binance account report СЕЙЧАС"
	@echo "  make of-timers-restart  - Перезапуск OF Timers Worker"
	@echo "  make meta-enforce-ramp-status  - Статус Meta ENFORCE ramp-or-freeze timer"
	@echo "  make meta-enforce-ramp-logs    - Логи Meta ENFORCE ramp-or-freeze timer"
	@echo "  make meta-enforce-ramp-restart - Перезапуск Meta ENFORCE ramp-or-freeze timer"
	@echo "  make meta-stage2-opt-status    - Статус Meta Stage2 optimize share timer"
	@echo "  make meta-stage2-opt-logs      - Логи Meta Stage2 optimize share timer"
	@echo "  make meta-stage2-opt-restart   - Перезапуск Meta Stage2 optimize share timer"
	@echo "  make ml-rollout-guard-status   - Статус ML rollout guard timer"
	@echo "  make ml-rollout-guard-logs     - Логи ML rollout guard timer"
	@echo "  make ml-rollout-guard-restart  - Перезапуск ML rollout guard timer"
	@echo "  make ml-outcome-joiner-status  - Статус ML outcome joiner worker"
	@echo "  make ml-outcome-joiner-logs    - Логи ML outcome joiner worker"
	@echo "  make ml-outcome-joiner-restart - Перезапуск ML outcome joiner worker"
	@echo "  make ml-promotion-ladder-status  - Статус ML promotion ladder timer"
	@echo "  make ml-promotion-ladder-logs    - Логи ML promotion ladder timer"
	@echo "  make ml-promotion-ladder-restart - Перезапуск ML promotion ladder timer"
	@echo "  make ml-threshold-proposer-status  - Статус ML threshold proposer timer"
	@echo "  make ml-threshold-proposer-logs    - Логи ML threshold proposer timer"
	@echo "  make ml-threshold-proposer-restart - Перезапуск ML threshold proposer timer"
	@echo "  make ml-threshold-proposer-v2-status  - Статус ML threshold proposer v2 timer"
	@echo "  make ml-threshold-proposer-v2-logs    - Логи ML threshold proposer v2 timer"
	@echo "  make ml-threshold-proposer-v2-restart - Перезапуск ML threshold proposer v2 timer"
	@echo "  make ml-rollback-proposer-status  - Статус ML rollback proposer timer"
	@echo "  make ml-rollback-proposer-logs    - Логи ML rollback proposer timer"
	@echo "  make ml-rollback-proposer-restart - Перезапуск ML rollback proposer timer"
	@echo "  make ml-train-tb-v10-2            - Ручной запуск обучения ML на TB labels (v10.2)"
	@echo "  make ml-manual-train              - Ручной запуск обучения ML моделей (LR + GBDT)"
	@echo "  make ml-approve                   - Активация новой ML модели (manual approval)"
	@echo "  make meta-status-snapshot-status  - Статус Meta Status Snapshot timer"
	@echo "  make meta-status-snapshot-logs    - Логи Meta Status Snapshot timer"
	@echo "  make close-wait-drainer-status    - Статус Close Wait Drainer"
	@echo "  make close-wait-drainer-logs      - Логи Close Wait Drainer"
	@echo "  make close-wait-drainer-logs      - Логи Close Wait Drainer"
	@echo "  make conf-score-tuning-status     - Статус Confidence Scorer Weight Tuning таймера и экспортера"
	@echo "  make conf-score-tuning-logs       - Логи Confidence Scorer Weight Tuning таймера"
	@echo "  make exec-health-service-identity-status - Статус Service Identity Exporter"
	@echo "  make exec-health-service-identity-logs   - Логи Service Identity Exporter"
	@echo "  make composite-preflight-exporter-status  - Статус P5.4 Composite Preflight Exporter (порт 9837)"
	@echo "  make composite-preflight-exporter-logs    - Логи P5.4 Composite Preflight Exporter"
	@echo "  make composite-preflight-exporter-restart - Перезапуск P5.4 Composite Preflight Exporter"
	@echo "  make composite-preflight-exporter-metrics - Проверить метрики /metrics (summary)"
	@echo ""
	@echo "📊 P63 Decision Coverage:"
	@echo "  make decision-coverage-up         - Запустить/обновить Decision Coverage сервисы"
	@echo "  make decision-coverage-logs       - Логи Decision Coverage Exporter"
	@echo ""
	@echo "📈 MetaModel A/B & Drift Management:"
	@echo "  make meta-ab-status               - Статус MetaModel A/B тестирования (Champion/Challenger)"
	@echo "  make meta-ab-logs                 - Логи MetaModel A/B Evaluator"
	@echo "  make meta-ab-eval-manual          - Ручной запуск A/B оценки"
	@echo "  make meta-drift-status            - Статус MetaModel Drift Guard"
	@echo "  make meta-drift-logs              - Логи MetaModel Drift Guard"
	@echo "  make meta-drift-suggest-manual    - Ручной запуск Drift Guard (режим Proposal)"
	@echo "  make entry-policy-apply-overrides - Применить одобренные overrides (включая freeze)"
	@echo "  make meta-freeze-open             - Установить MetaModel Freeze в OPEN (veto off)"
	@echo "  make meta-freeze-closed           - Установить MetaModel Freeze в CLOSED (veto on)"
	@echo "  make meta-freeze-reset            - Сбросить MetaModel Freeze (unfreeze)"
	@echo ""
	@echo "🔐 P9 ExecHealth Dual-Control Freeze/Thaw (двойной контроль разморозки):"
	@echo "  Переменные: OPERATOR= APPROVER= REASON= TICKET= NONCE= REQUEST_ID= MINUTES="
	@echo "  make exec-health-freeze-status              - Текущий статус freeze (nonce, request_id, thaw_status)"
	@echo "  make exec-health-freeze-prepare   OPERATOR=alice REASON='...' TICKET=INC-42 NONCE=<nonce>"
	@echo "                                              - Фаза 1: зарегистрировать запрос (→ request_id)"
	@echo "  make exec-health-freeze-approve   APPROVER=bob REQUEST_ID=<request_id>"
	@echo "                                              - Фаза 2: одобрить запрос (APPROVER ≠ OPERATOR)"
	@echo "  make exec-health-freeze-commit    OPERATOR=alice REQUEST_ID=<request_id>"
	@echo "                                              - Фаза 3: финальная подпись, снять заморозку"
	@echo "  make exec-health-freeze-force     OPERATOR=alice REASON='maint' TICKET=CHG-1 MINUTES=30"
	@echo "                                              - Принудительная заморозка (maintenance window)"
	@echo "  make exec-health-freeze-undo      OPERATOR=alice REASON='...' TICKET=INC-42 NONCE=<nonce>"
	@echo "                                              - P8 legacy single-operator thaw (если REQUIRE_DUAL=0)"
	@echo ""
	@echo "  ⏰ Ручные таймеры:"
	@echo "  make of-nightly-calibrate-manual  - Ручной запуск nightly calibration (05:30 UTC)"
	@echo "  make of-nightly-meta-train-manual  - Ручной запуск nightly meta train (05:10 UTC)"
	@echo "  make meta-train-manual            - Ручной запуск nightly meta training v1 (04:30 UTC)"
	@echo "  make meta-nightly-manual          - Ручной запуск nightly meta pipeline v1 (P7 Unified)"
	@echo "  make meta-quality-manual          - Ручной запуск nightly meta quality report v1 (06:30 UTC)"
	@echo "  make meta-train-status            - Проверить статус мета-обучения (sidecar)"
	@echo "  make meta-quality-status          - Проверить отчет качества мета-модели"
	@echo "  make meta-status-snapshot-manual  - Ручной запуск meta status snapshot"
	@echo "  make of-conf-cal-promote-manual   - Ручной запуск promotion calibrator bundle (shadow proof)"
	@echo "  make of-conf-cal-promote-status   - Статус последнего promotion check"
	@echo "  make meta-train-logs              - Логи таймера обучения"
	@echo "  make meta-quality-logs            - Логи таймера отчета качества"

	@echo "  make regress-safe-manual          - Ручной запуск regress safe (04:20 UTC)"
	@echo "  make quality-status               - Статус Quality Worker & Exporter"
	@echo "  make quality-logs                 - Логи Quality Worker & Exporter"
	@echo "  make quality-restart              - Перезапуск Quality сервисов"
	@echo "  make fgh-dataset-builder-status   - Статус FGH Dataset Builder Timer"
	@echo "  make fgh-dataset-builder-logs     - Логи FGH Dataset Builder Timer"
	@echo "  make ablation-timer-status        - Статус Ablation E-block Timer"
	@echo "  make ablation-timer-logs          - Логи Ablation E-block Timer"
	@echo ""
	@echo "📊 v10 Triple-Barrier Labeling Pipeline:"
	@echo "  make v10-tb-pipeline-redis    - Полный пайплайн v10 из Redis Stream тиков (рекомендуется)"
	@echo "  make v10-tb-label-redis       - Генерация лейблов из Redis Stream тиков (v10)"
	@echo "  make v10-tb-pipeline         - Полный пайплайн (автоопределение Redis → экспорт → лейблы → датасет)"
	@echo "  make v10-tb-check-redis       - Проверка наличия данных в Redis streams"
	@echo "  make v10-tb-export-from-docker - Ручной экспорт из Docker Redis (если автоопределение не работает)"
	@echo "  make v10-tb-export-inputs     - Экспорт inputs из Redis (шаг 0)"
	@echo "  make v10-tb-export-ticks      - Только экспорт тиков (шаг 1)"
	@echo "  make v10-tb-label             - Только генерация лейблов (шаг 2)"
	@echo "  make v10-tb-build-dataset     - Только построение датасета (шаг 3)"
	@echo "  Пример: make v10-tb-pipeline SYMBOLS=BTCUSDT"
	@echo "  Примечание: Команда автоматически использует Docker Redis как основной источник (локальный как fallback)"
	@echo ""
	@echo "🚀 Manual ML Pipeline (Recovery/Force):"
	@echo "  make ml-manual-pipeline HOURS=24 - Полный цикл (Label + Train) за последние N часов"
	@echo "  make ml-v10-force-label HOURS=24 - Принудительная генерация лейблов (v10)"
	@echo "  make ml-v10-manual-train HOURS=24 - Принудительный запуск обучения (v10.4)"
	@echo ""
	@echo "📋 Логи по сервисам:"
	@echo "  make signal-logs       - Логи signal-generator"
	@echo "  make hub-logs          - Логи signal-hub"
	@echo "  make gateway-logs      - Логи go-gateway"
	@echo "  make telegram-logs     - Логи telegram-worker"
	@echo "  make worker-logs       - Логи всех Python workers (shards 1, 1b, 2)"
	@echo "  make atr-logs          - Логи ATR workers"
	@echo "  make orderflow-logs    - Логи orderflow handler"
	@echo "  make tracker-logs      - Логи signal performance tracker"
	@echo "  make paper-logs        - Логи paper executor"
	@echo "  make trailing-autotune-logs - Логи trailing autotune"
	@echo "  make prometheus-filtered-logs - Фильтрованные логи Prometheus (каждое 10000-е)"
	@echo "  make grafana-filtered-logs   - Фильтрованные логи Grafana (каждое 10000-е)"
	@echo "  📖 LOG_FILTERING_README.md   - Документация по фильтрации логов"
	@echo ""
	@echo "🔄 Dynamic Symbols (hot-reload без перезапуска):"
	@echo "  make symbol-add SYMBOL=BTCUSD         - Добавить символ"
	@echo "  make symbol-remove SYMBOL=BTCUSD      - Удалить символ"
	@echo "  make symbol-set SYMBOLS='XAUUSD BTC'  - Установить список"
	@echo "  make symbol-list                      - Показать активные"
	@echo "  make symbol-status                    - Статус handlers"
	@echo ""
	@echo "🔴 Redis команды:"
	@echo "  make redis-stats       - Статистика Redis (память, ключи, подключения)"
	@echo "  make redis-flush       - Очистка Redis (требует подтверждение)"
	@echo "  make redis-clear-data  - Очистка данных по сигналам, сделкам и volume"
	@echo "  make redis-memory      - Мониторинг памяти Redis"
	@echo "  make redis-check       - Проверка подключения к Redis"
	@echo "  make backup-redis      - Backup Redis данных"
	@echo "  make restore-redis     - Восстановление Redis данных"
	@echo "  make optimize-redis    - Оптимизация Redis производительности"
	@echo ""
	@echo "🔍 Проверки системы:"
	@echo "  make full-status           - Полный статус всех компонентов"
	@echo "  make quick-status          - Быстрая проверка критичных сервисов"
	@echo "  make check-xauusd-services - Проверка 3 сервисов обработки XAUUSD"
	@echo "  make check-telegram        - Проверка Telegram интеграции"
	@echo "  make check-redis-streams   - Проверка всех Redis Streams"
	@echo "  make service-check         - Проверка работы всех сервисов"
	@echo "  make full-system-check     - Полная диагностика системы"
	@echo "  make db-check              - Проверка подключения к БД (если есть)"
	@echo "  make network-test          - Проверка Docker сети"
	@echo ""
	@echo "🧪 Тестирование:"
	@echo "  make test-contract          - 🛡️  Contract-тесты с жесткими проверками"
	@echo "  make test-ofc-contract      - 🛡️  OFC runtime_snapshot contract тест"
	@echo "  make api-test              - Тест всех API endpoint'ов"
	@echo "  make gateway-test          - Тест go-gateway API"
	@echo "  make hub-test              - Тест signal-hub"
	@echo "  make signal-test           - Тест signal-generator"
	@echo "  make notification-test     - Тест telegram notifications"
	@echo "  make test-tracker-telegram - Тест отправки статистики трекера"
	@echo "  make send-report-now       - 📊 Отправить отчет по сигналам СЕЙЧАС"
	@echo "  make send-real-report      - 📊 Отправить РЕАЛЬНЫЙ отчет (Signal Tracker)"
	@echo "  make tracker-stats         - 📊 Показать статистику трекера по источникам"
	@echo "  make tracker-logs          - 📋 Логи Signal Tracker (реальное время)"
	@echo "  make tracker-restart       - 🔄 Перезапуск Signal Tracker"
	@echo "  make test-reporting        - 🧪 Тест системы отчетов (ReportingService + Redis)"
	@echo "  make test-orphan-housekeep - 🧪 Тест orphan housekeeping (TradeMonitor)"
	@echo "  make logs-reporter         - 📋 Логи periodic-reporter сервиса"
	@echo "  make restart-reporter      - 🔄 Перезапуск periodic-reporter"
	@echo "  make seed-test             - Тест заполнения данных"
	@echo ""
	@echo "🔄 OFC Golden Replay:"
	@echo "  make ofc-validate          - Валидация OFC capture"
	@echo "  make ofc-fill-expected      - Заполнение expected полей в capture"
	@echo "  make ofc-replay-strict      - Строгий replay OFC golden"
	@echo "  make ofc-replay-validate    - Валидация детерминизма replay"
	@echo "  make ofc-bench              - Бенчмарк производительности OFC build"
	@echo "  make ofc-golden-suite       - End-to-end: validate → fill → replay → bench"
	@echo "  make ofc-golden-clean       - Очистка golden файлов"
	@echo ""
	@echo "⏱️  Tick Time Observability:"
	@echo "  make tick-time-lag-report           - Отчет по квантилям tick time lag"
	@echo "  make tick-time-lag-report-json      - Отчет в JSON формате"
	@echo "  make tick-time-lag-report-timer-status - Статус таймера отчета"
	@echo "  make tick-time-lag-report-timer-logs - Логи таймера отчета"
	@echo "  make tick-time-autotune              - Автоподбор порогов tick time policy"
	@echo "  make tick-time-autotune-timer-status - Статус таймера autotune"
	@echo "  make tick-time-autotune-timer-logs   - Логи таймера autotune"
	@echo ""
	@echo "🎓 Edge Stack v1 OOF Training:"
	@echo "  make edge-stack-v1-build-dataset    - Построить train dataset из metrics:ml_confirm + trades:closed"
	@echo "  make edge-stack-v1-train            - Обучить edge_stack_v1 OOF модель"
	@echo "  make edge-stack-v1-dataset-timer-status - Статус dataset builder timer"
	@echo "  make edge-stack-v1-dataset-timer-logs - Логи dataset builder timer"
	@echo "  make edge-stack-v1-train-timer-status - Статус training timer"
	@echo "  make edge-stack-v1-train-timer-logs - Логи training timer"
	@echo ""
	@echo "🔧 Исправления и обслуживание:"
	@echo "  make emergency-stop    - Экстренная остановка перегруженных сервисов"
	@echo "  make emergency-fix     - Экстренное исправление системы"
	@echo "  make performance-fix   - Исправление проблем производительности"
	@echo "  make circuit-reset     - Сброс Circuit Breakers"
	@echo "  make quick-restart     - Быстрый перезапуск критичных сервисов"
	@echo "  make safe-restart      - Безопасный перезапуск с проверками"
	@echo "  make monitor-resources - Мониторинг использования ресурсов"
	@echo ""
	@echo "🧹 Очистка:"
	@echo "  make clean             - Полная очистка (контейнеры + volumes)"
	@echo "  make docker-cleanup    - Очистка неиспользуемых Docker ресурсов"
	@echo "  make clean-build-cache - Очистка Docker build cache"
	@echo "  make volume-list       - Список всех volumes"
	@echo "  make volume-inspect    - Детальная информация о volumes"
	@echo ""
	@echo "🔧 Docker Build (при ошибках сборки):"
	@echo "  make build-legacy      - Сборка с legacy builder"
	@echo "  make rebuild-legacy    - Полная пересборка с legacy builder"
	@echo "  make safe-rebuild      - Безопасная пересборка через скрипт"
	@echo "  make up-legacy         - Запуск с legacy builder"
	@echo ""
	@echo "📺 Telegram Каналы:"
	@echo "  make add-telegram-channel CHANNEL=name        - Добавить канал"
	@echo "  make add-telegram-channels CHANNELS=c1,c2     - Добавить несколько"
	@echo "  make remove-telegram-channel CHANNEL=name     - Удалить канал"
	@echo "  make list-telegram-channels                   - Список каналов"
	@echo "  make check-telegram-channels                  - Проверить каналы"
	@echo "  make clear-telegram-channels                  - Удалить все каналы"
	@echo "  make restart-telegram-worker                  - Перезапуск worker"
	@echo ""
	@echo "📊 Экспорт и анализ:"
	@echo "  make export-signals    - Экспорт сигналов из Redis"
	@echo "  make check-signals     - Проверка качества сигналов"
	@echo ""
	@echo "Примеры:"
	@echo "  make up                # Запуск системы"
	@echo "  make full-status       # Проверка статуса"
	@echo "  make signal-logs       # Просмотр логов генератора сигналов"
	@echo "  make redis-stats       # Проверка Redis"
	@echo "  make emergency-fix     # Быстрое исправление проблем"
	@echo ""

# ═══════════════════════════════════════════════════════════════
# A/B Entry Policy Testing
# ═══════════════════════════════════════════════════════════════

entry-policy-ab-setup:
	@echo "🧪 Настройка A/B/C тестирования Entry Policy..."
	@echo ""
	@echo "1. Запись конфига A (Control/Enforce)..."
	@docker exec redis-worker-1 redis-cli SET cfg:entry_policy:overrides:A '{"version":1,"overrides":{"ENTRY_POLICY_SHADOW":"0"}}'
	@echo "2. Запись конфига B (Experiment/Shadow/Relaxed)..."
	@docker exec redis-worker-1 redis-cli SET cfg:entry_policy:overrides:B '{"version":1,"overrides":{"ENTRY_POLICY_SHADOW":"1","SMT_COH_THRESHOLD":"0.68","SMT_LEADER_CONF_MIN_SCORE":"0.68"}}'
	@echo "3. Запись конфига C (Experiment/Shadow/Strict)..."
	@docker exec redis-worker-1 redis-cli SET cfg:entry_policy:overrides:C '{"version":1,"overrides":{"ENTRY_POLICY_SHADOW":"1","SMT_COH_THRESHOLD":"0.75","SMT_LEADER_CONF_MIN_SCORE":"0.75"}}'
	@echo "4. Включение роутинга (A=34%, B=33%, C=33%)..."
	@docker exec redis-worker-1 redis-cli SET cfg:entry_policy:ab:config '{"version":1,"enabled":1,"split_pct_b":33,"split_pct_c":33,"key_a":"cfg:entry_policy:overrides:A","key_b":"cfg:entry_policy:overrides:B","key_c":"cfg:entry_policy:overrides:C","poll_ms":2000}'
	@echo ""
	@echo "✅ A/B/C тестирование настроено и включено (A=34%, B=33%, C=33%)"

entry-policy-ab-status:
	@echo "📊 Статус A/B тестирования..."
	@echo ""
	@echo "AB Config:"
	@docker exec redis-worker-1 redis-cli GET cfg:entry_policy:ab:config
	@echo ""
	@echo "Config A:"
	@docker exec redis-worker-1 redis-cli GET cfg:entry_policy:overrides:A
	@echo ""
	@echo "Config B:"
	@docker exec redis-worker-1 redis-cli GET cfg:entry_policy:overrides:B

# ═══════════════════════════════════════════════════════════════
# Основные команды Docker Compose
# ═══════════════════════════════════════════════════════════════

COMPOSE_FILE ?= docker-compose.yml
COMPOSE_TRAILING ?= docker-compose.tp-trailing.yml
# COMPOSE_MT5_EXECUTOR ?= docker-compose.mt5-executor.yml
COMPOSE_TIMERS ?= docker-compose-timers.yml
COMPOSE_TB_LABELER ?= docker-compose.tb-labeler.yml
COMPOSE_TB_LABELER_V10_2 ?= docker-compose.tb-labeler-v10-2.yml

COMPOSE_PYTHON_WORKERS ?= docker-compose-python-workers.yml
COMPOSE_UTILITIES ?= docker-compose-utilities.yml

# Build robustness parameters
PRUNE_ON_UP ?= 0
BUILD_RETRIES ?= 2
BASE_IMAGES := golang:1.22-alpine python:3.11-slim redis:7.2-alpine postgres:16-alpine

# Авто-определение актуальной команды docker compose.
# Если доступен плагин `docker compose` (v2+), используем его.
# Иначе fallback на бинарь `docker-compose`.
DOCKER_COMPOSE_BIN := docker compose

DC := $(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f docker-compose.auto-apply-guard-exporter.yml -f python-worker/orderflow_services/deploy/compose/docker-compose.strategy-research-guard-exporter-v1.yml -f python-worker/orderflow_services/deploy/compose/docker-compose.orchestration-composite-preflight-exporter-v1.yml -f python-worker/orderflow_services/deploy/compose/docker-compose.strategy-research-stats-alert-policy-exporter-v1.yml -f python-worker/orderflow_services/deploy/compose/docker-compose.ofc-contextual-runtime-health-exporter-v1.yml -f python-worker/orderflow_services/deploy/compose/docker-compose.ofc-contextual-runtime-summary-writer-v1.yml -f python-worker/orderflow_services/deploy/compose/docker-compose.ofc-contextual-runtime-v1.yml
DC_WITH_TRAILING := $(DC) -f $(COMPOSE_TRAILING) -f $(COMPOSE_TB_LABELER)
# DC_WITH_TRAILING := $(DC) -f $(COMPOSE_TRAILING) -f $(COMPOSE_MT5_EXECUTOR) -f $(COMPOSE_TB_LABELER)
DC_FULL := $(DC)

up:
	@export GIT_SHA=$$(git rev-parse --short HEAD 2>/dev/null || echo "unknown"); \
	export EXEC_HEALTH_DEPLOY_ID=$${EXEC_HEALTH_DEPLOY_ID:-$$GIT_SHA}; \
	echo "🔖 Deploy ID: $$EXEC_HEALTH_DEPLOY_ID (git SHA передаётся в ExecHealth SLO)"
	@EXISTING=$$(docker ps -a --filter "name=scanner-" --format "{{.Names}}" 2>/dev/null | wc -l); \
	REDIS_EXISTING=$$(docker ps -a --filter "name=redis" --format "{{.Names}}" 2>/dev/null | wc -l); \
	if [ $$EXISTING -gt 0 ] || [ $$REDIS_EXISTING -gt 0 ]; then \
		echo "⚠️  Найдено $$EXISTING существующих контейнеров scanner-* и $$REDIS_EXISTING redis-*"; \
		echo "🛑 Остановка и удаление существующих контейнеров..."; \
		docker ps -a --filter "name=scanner-" --format "{{.Names}}\t{{.Status}}" | sed 's/^/  - /'; \
		docker ps -a --filter "name=redis" --format "{{.Names}}\t{{.Status}}" | sed 's/^/  - /'; \
		echo ""; \
		echo "Остановка через docker-compose..."; \
		$(DC_WITH_TRAILING) down --remove-orphans 2>/dev/null || true; \
		echo "Прямое удаление всех scanner-* и redis-* контейнеров..."; \
		docker ps -a --filter "name=scanner-" --format "{{.Names}}" | xargs -r docker rm -f 2>/dev/null || true; \
		docker ps -a --filter "name=redis" --format "{{.Names}}" | xargs -r docker rm -f 2>/dev/null || true; \
		docker ps -a --filter "name=go-gateway" --format "{{.Names}}" | xargs -r docker rm -f 2>/dev/null || true; \
		echo "✅ Контейнеры остановлены и удалены"; \
		sleep 2; \
	else \
		echo "✅ Нет существующих контейнеров"; \
	fi
	@echo ""
	@echo "🌐 Создание/проверка Docker сетей (scanner-infra, scanner-core, scanner-timers, scanner-ops)..."
	@./scripts/create-networks.sh
	@echo ""
	@echo "🌐 Проверка Docker сети trade-network..."
	@if ! docker network ls --format "{{.Name}}" | grep -qx "trade-network"; then \
		echo "⚠️  Сеть trade-network не найдена"; \
		echo "🔧 Создание сети trade-network..."; \
		docker network create trade-network >/dev/null && echo "✅ Сеть trade-network создана" || { echo "❌ Не удалось создать сеть"; exit 1; }; \
	else \
		echo "✅ Сеть trade-network уже существует"; \
	fi
	@echo ""
	@echo "🚀 Проверка GPU поддержки..."
	@if command -v nvidia-smi >/dev/null 2>&1; then \
		echo "✅ NVIDIA GPU драйвер обнаружен"; \
		GPU_NAME=$$(nvidia-smi --query-gpu=name --format=csv,noheader,nounits 2>/dev/null | head -1); \
		if [ -n "$$GPU_NAME" ]; then \
			echo "   GPU: $$GPU_NAME"; \
		fi; \
		if docker info 2>/dev/null | grep -q "nvidia"; then \
			echo "✅ NVIDIA Docker runtime настроен"; \
			echo "🚀 GPU будет использоваться для python-worker"; \
		elif docker run --help 2>/dev/null | grep -q "\-\-gpus"; then \
			echo "✅ Docker поддерживает --gpus флаг"; \
			echo "🚀 GPU будет использоваться для python-worker"; \
		else \
			echo "⚠️  NVIDIA Docker runtime не настроен, GPU может быть недоступен"; \
			echo "   Установите: sudo apt-get install -y nvidia-docker2 && sudo systemctl restart docker"; \
		fi; \
	else \
		echo "⚠️  nvidia-smi не найден, GPU недоступен (система будет работать на CPU)"; \
	fi
	@echo ""
	@echo "🔧 Проверка Docker Compose версии..."
	@if docker compose version >/dev/null 2>&1; then \
		DC_VERSION=$$(docker compose version 2>/dev/null | head -1); \
		echo "✅ Используется Docker Compose v2: $$DC_VERSION"; \
	else \
		echo "⚠️  Docker Compose v2 не найден, используется fallback"; \
	fi
	@echo ""
	@echo "🚀 Запуск всех сервисов с логами (включая TP Event Listener и Trailing Autotune)..."
	@echo "   GPU поддержка: через переменные окружения NVIDIA_VISIBLE_DEVICES=all"
	@echo "   Trailing Autotune: включен (автоматическая настройка трейлинга)"
	@echo "   Trailing Autotune: включен (автоматическая настройка трейлинга)"
	@if [ "$(PRUNE_ON_UP)" = "1" ]; then \
		echo "🔧 Очистка BuildKit кеша перед сборкой..."; \
		docker buildx prune -f >/dev/null 2>&1 || true; \
	fi
	@$(MAKE) pre-pull
	@echo "🔧 Запуск сборки батчами по 5 сервисов (для снижения нагрузки на BuildKit)..."
	@for i in $$(seq 1 $(BUILD_RETRIES)); do \
		echo "🛠️ Попытка сборки $$i из $(BUILD_RETRIES)..."; \
		SUCCESS=1; \
		if [ $$i -gt 1 ]; then \
			echo "🧹 Очистка BuildKit cache перед повтором (попытка $$i)..."; \
			docker builder prune -f >/dev/null 2>&1 || true; \
		fi; \
		SERVICES=$$($(DC_WITH_TRAILING) --profile default config --services 2>/dev/null | grep -v "^[[:space:]]*$$" | grep -v "^WARN" | grep -v "^[[:digit:]]*$$"); \
		if [ -z "$$SERVICES" ]; then \
			echo "⚠️ Ошибка получения списка сервисов, попытка собрать все сразу..."; \
			COMPOSE_PARALLEL_LIMIT=2 ./scripts/with-buildkit.sh $(DC_WITH_TRAILING) --profile default build || SUCCESS=0; \
		else \
			rm -f .build_batches.tmp; \
			echo "$$SERVICES" | xargs -n 5 > .build_batches.tmp; \
			while read -r batch; do \
				if [ -z "$$batch" ]; then continue; fi; \
				echo "🛠️ Сборка батча (параллельность 2): $$batch"; \
				if ! COMPOSE_PARALLEL_LIMIT=2 ./scripts/with-buildkit.sh $(DC_WITH_TRAILING) --profile default build $$batch; then \
					SUCCESS=0; \
					break; \
				fi; \
			done < .build_batches.tmp; \
			rm -f .build_batches.tmp; \
		fi; \
		if [ $$SUCCESS -eq 1 ]; then echo "✅ Сборка успешно завершена"; break; fi; \
		if [ $$i -eq $(BUILD_RETRIES) ]; then echo "❌ Сборка не удалась после $(BUILD_RETRIES) попыток"; exit 1; fi; \
		echo "⚠️ Сборка прервана, ожидание перед повтором..."; \
		sleep 5; \
	done
	@echo "🚀 Запуск сервисов (поэтапный старт для снижения нагрузки на Docker DNS)..."
	@echo ""
	@echo "📦 Phase 1/3: Infrastructure (Redis + Postgres)..."
	@./scripts/with-buildkit.sh $(DC_WITH_TRAILING) --profile default up -d \
		redis redis-ticks redis-ticks-b redis-worker-1 redis-worker-2 redis-worker-1b redis-worker-2b \
		redis-exporter-worker-1 redis-exporter-worker-1b redis-exporter-worker-2 redis-exporter-worker-2b \
		postgres migration-runner
	@echo "⏳ Waiting for Redis + Postgres to be healthy..."
	@for i in $$(seq 1 30); do \
		R=$$(docker inspect --format='{{.State.Health.Status}}' redis-worker-1 2>/dev/null); \
		P=$$(docker inspect --format='{{.State.Health.Status}}' scanner-postgres 2>/dev/null); \
		if [ "$$R" = "healthy" ] && [ "$$P" = "healthy" ]; then \
			echo "✅ Infrastructure healthy after $${i}s"; break; \
		fi; \
		sleep 1; \
	done
	@echo "⏳ Waiting for redis-ticks + redis-ticks-b to be healthy (start_period=300s, up to 5m)..."
	@for i in $$(seq 1 60); do \
		RT=$$(docker inspect --format='{{.State.Health.Status}}' redis-ticks 2>/dev/null); \
		RTB=$$(docker inspect --format='{{.State.Health.Status}}' redis-ticks-b 2>/dev/null); \
		if [ "$$RT" = "healthy" ] && [ "$$RTB" = "healthy" ]; then \
			echo "✅ redis-ticks + redis-ticks-b healthy after $$((i * 5))s"; break; \
		fi; \
		if [ $$i -eq 60 ]; then \
			echo "⚠️  redis-ticks status=$$RT redis-ticks-b status=$$RTB (timeout, continuing anyway)"; \
		fi; \
		sleep 5; \
	done
	@echo ""
	@echo "📦 Phase 2a/4: Go gateway + high-frequency workers (1m, 5m, 15m, 1h, 4h)..."
	@./scripts/with-buildkit.sh $(DC_WITH_TRAILING) --profile default up -d \
		go-gateway go-worker-1m go-worker-5m go-worker-15m go-worker-1h go-worker-4h
	@echo "⏳ Waiting 10s for DNS to stabilize..."
	@sleep 10
	@echo ""
	@echo "📦 Phase 2b/4: Low-frequency Go workers (1d, 1w, 1month, 3month, 1y)..."
	@./scripts/with-buildkit.sh $(DC_WITH_TRAILING) --profile default up -d \
		go-worker-1d go-worker-1w go-worker-1month go-worker-3month go-worker-1y
	@echo "⏳ Waiting 10s for DNS to stabilize..."
	@sleep 10
	@echo ""
	@echo "📦 Phase 2c/4: Signal chain + Python services..."
	@./scripts/with-buildkit.sh $(DC_WITH_TRAILING) --profile default up -d \
		signal-hub signal-dispatcher python-worker execution-gate-service of-confirm-service \
		atr-worker regime-worker regime-storage news-ingestor-go
	@echo "⏳ Waiting 20s for signal chain + DNS to stabilize..."
	@sleep 20
	@echo "⏳ Verifying infrastructure still healthy..."
	@docker exec redis-worker-1 redis-cli ping > /dev/null 2>&1 && echo "✅ Redis OK" || echo "⚠️ Redis not responding"
	@docker exec scanner-postgres pg_isready -q 2>/dev/null && echo "✅ Postgres OK" || echo "⚠️ Postgres not responding"
	@echo ""
	@echo "📦 Phase 3/3: All remaining services..."
	@./scripts/with-buildkit.sh $(DC_WITH_TRAILING) --profile default up -d
	@echo "⏳ Waiting 30s for Phase 3 services to stabilize before additional launches..."
	@sleep 30
	@echo ""
	@echo "🛡️  Checking Database Integrity..."
	@./scripts/ensure_db.sh
	@echo ""
	@echo "🎯 Ожидание запуска сервисов перед калибровкой..."
	@sleep 10
	@echo "📊 Запуск автоматической настройки локальной калибровки..."
	@./python-worker/scripts/setup_and_run_calibration.sh || echo "⚠️  Calibration skipped (DB not ready) — run: make calibration-run"
	@echo "🛡️  Applying DQ Gate Penalty-Only Config..."
	@cat python-worker/ops/dynamic_cfg_examples/dq_gate_penalty_only.hset | docker exec -i redis-worker-1 redis-cli >/dev/null || echo "⚠️  Failed to apply DQ gate config (Redis may not be ready)"
	@echo ""
	@echo "🤖 Запуск автоматического обучения калибровки уверенности..."
	@make calibration-auto-start
	@echo ""
	@echo "📰 Запуск пайплайна новостей..."
	@./start-news-pipeline.sh
	@echo ""
	@echo "🔔 Запуск ML RCA Notification Bridge..."
	@$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_PYTHON_WORKERS) up -d ml-rca-notification-bridge
	@echo "🤖 Инициализация ML Confirm конфигурации..."
	@make ml-confirm-init-manual || echo "⚠️  ML Confirm init failed (non-critical)"
	@echo "🛡️  Запуск Active Symbol Guard Repair Worker..."
	@COMPOSE_IGNORE_ORPHANS=True $(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f docker-compose-timers.yml up -d active-symbol-guard-repair-worker


	@echo "🏆 AB Winner Evaluator V3 (Docker) запущен в составе стека..."
	@echo ""
	@echo "📋 Запуск сервиса одобрения рекомендаций..."
	@echo "   - recs-callback-worker (обработка callbacks из bot-nest)"
	@echo "   - recs-callback-worker-v2 (двухфазный PROPOSE: preview2 -> confirm)"
	@if [ -f "docker-compose-crypto-orderflow.yml" ]; then \
		echo "⏳ Waiting 15s for bridge stabilization..."; \
		sleep 15; \
		echo "🚀 Services from crypto-orderflow and timers are already started by main docker-compose.yml"; \
		# echo "🚀 Запуск crypto-orderflow-service и сервисов одобрения рекомендаций..."; \
		# $(DOCKER_COMPOSE_BIN) -f docker-compose-crypto-orderflow.yml up -d crypto-orderflow-service crypto-orderflow-service-2 recs-callback-worker recs-callback-worker-v2 2>/dev/null && \
		# echo "✅ Crypto-orderflow-service и сервисы одобрения рекомендаций запущены" || echo "⚠️  Ошибка запуска сервисов"; \
		echo "🚀 Запуск nightly meta pipeline..."; \
		$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TIMERS) up -d scanner-meta-nightly-pipeline scanner-meta-nightly-v7-pipeline scanner-meta-audit-v7 2>/dev/null && \
		echo "✅ Nightly meta pipeline (v4, v7, audit) запущен" || echo "⚠️  Ошибка запуска nightly meta pipeline"; \
		echo "⏰ Запуск Auto-Train Calibration Timer..."; \
		$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TIMERS) up -d auto-train-conf-calibration-timer 2>/dev/null && \
		echo "✅ Auto-Train Calibration Timer запущен" || echo "⚠️  Ошибка запуска Auto-Train Calibration Timer"; \
		echo "⏰ Запуск Binance Execution / Reporter (P5+P9)..."; \
		$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f docker-compose-binance.yml -f config/docker-compose.execution-p5.override.yml -f config/docker-compose.execution-p9.override.yml up -d binance-executor binance-account-reporter active-symbol-guard-exporter 2>/dev/null && \
		echo "✅ Binance Execution и Reporter запущены (P5+P9 overlay)" || echo "⚠️  Ошибка запуска Binance сервисов"; \
		echo "⏰ Запуск OFC таймеров (валидация, replay, fill expected, benchmark, smoke-exporter)..."; \
		$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f docker-compose-crypto-orderflow.yml up -d of-timers-worker archiver-signals-1 of-inputs-v3-circuit-exporter prom-rules-bundle-smoke-timer of-gate-contract-smoke-exporter 2>/dev/null && \
		echo "✅ OFC таймеры запущены" || echo "⚠️  Ошибка запуска OFC таймеров"; \
		echo "⏰ Запуск ML таймеров (edge_stack_v1 training, edge_stack_v1_oof training, drift monitoring)..."; \
		$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f docker-compose-crypto-orderflow.yml up -d ml-train-edge-stack-v1-timer ml-train-edge-stack-v1-oof-timer ml-drift-monitor-v1-timer 2>/dev/null && \
		echo "✅ ML таймеры запущены" || echo "⚠️  Ошибка запуска ML таймеров"; \
		echo "⏰ Запуск edge_stack_v1 dataset builder таймера..."; \
		$(DOCKER_COMPOSE_BIN) -f docker-compose-crypto-orderflow.yml up -d edge-stack-v1-dataset-timer 2>/dev/null && \
		echo "✅ Edge stack v1 dataset timer запущен" || echo "⚠️  Ошибка запуска edge stack v1 dataset timer"; \
		echo "⏰ Запуск таймеров рекомендаций (trailing recommender, xau aggregator, ab winner lcb)..."; \
		$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TIMERS) up -d trailing-recommender-timer xau-aggregator-timer ab-winner-lcb-timer 2>/dev/null && \
		echo "✅ Таймеры рекомендаций запущены" || echo "⚠️  Ошибка запуска таймеров рекомендаций"; \
		echo "🚀 Запуск Telegram ChatOps Freeze Bot..."; \
		$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f docker-compose-crypto-orderflow.yml up -d chatops-telegram-freeze-bot 2>/dev/null && \
		echo "✅ Telegram ChatOps Freeze Bot запущен" || echo "⚠️  Ошибка запуска ChatOps Bot"; \
		echo "⏰ Запуск tick time observability таймера..."; \
		$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f docker-compose-crypto-orderflow.yml up -d tick-time-lag-report-timer tick-time-autotune-timer 2>/dev/null && \
		echo "✅ Tick time lag report timer запущен" || echo "⚠️  Ошибка запуска tick time lag report timer"; \
		echo "⏰ Запуск side sign audit timer (Step 9: static audit for SELL-bias patterns)..."; \
		echo "📊 Шаг 15: Tick-quality EMAs и Prometheus алерты интегрированы в realtime..."; \
		$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f docker-compose-crypto-orderflow.yml up -d side-sign-audit-timer 2>/dev/null && \
		echo "✅ Side sign audit timer запущен" || echo "⚠️  Ошибка запуска side sign audit timer"; \
		echo "🚀 Запуск Triple Barrier Labeler (v10.2 - Production)..."; \
		$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TB_LABELER_V10_2) up -d --remove-orphans && \
		echo "✅ TB Labeler v10.2 запущен" || echo "⚠️  Ошибка запуска TB Labeler v10.2"; \
		echo "⏰ Запуск replay inputs archiver (P0: archive signals:of:inputs -> ml_replay_inputs_v1)..."; \
		$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f docker-compose-timers.yml up -d replay-archiver 2>/dev/null && \
		echo "✅ Replay inputs archiver запущен" || echo "⚠️  Ошибка запуска replay inputs archiver"; \
		echo "⏰ Запуск Meta Coverage Ops (P38)..."; \
		$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f docker-compose-crypto-orderflow.yml up -d meta-cov-rollout-exporter meta-enforce-cov-auto-apply-timer 2>/dev/null && \
		echo "✅ Meta Coverage Ops запущены" || echo "⚠️  Ошибка запуска Meta Coverage Ops"; \
		echo "⏰ Запуск Close Wait Drainer, Trade Close Joiner, Conf Cal Health Loop & Guardrails Timers..."; \
		$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TIMERS) up -d close-wait-drainer-exporter trade-close-joiner-v5 conf-cal-live-exporter conf-cal-health-loop conf-score-guard-apply-timer conf-score-guard-autopromo-timer conf-score-guard-exporter 2>/dev/null && \
		echo "✅ Close Wait Drainer, Trade Close Joiner, Conf Cal Health Loop & Guardrails Timers запущены" || echo "⚠️  Ошибка запуска таймеров"; \
		echo "⏰ Запуск P5.7 Execution Audit Chain (checker + runbook server)..."; \
		mkdir -p runtime/execution_audit_chain runtime/node_exporter_textfile 2>/dev/null || true; \
		COMPOSE_PROFILES=ops $(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TIMERS) up -d execution-audit-chain-checker execution-audit-runbook-server 2>/dev/null && \
		echo "✅ P5.7 Execution Audit Chain checker + runbook server запущены (порт 8777)" || echo "⚠️  Ошибка запуска P5.7 Execution Audit Chain (non-critical)"; \
		echo "⏰ Запуск P4 ExecHealth SLO Checker + Exporter (порт 9824)..."; \
		GIT_SHA=$$(git rev-parse --short HEAD 2>/dev/null || echo "unknown"); \
		EXEC_HEALTH_DEPLOY_ID=$${EXEC_HEALTH_DEPLOY_ID:-$$GIT_SHA} $(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TIMERS) up -d exec-health-slo-checker exec-health-slo-exporter 2>/dev/null && \
		echo "✅ P4 ExecHealth SLO Checker + Exporter запущены (порт 9824, deploy_id=$$GIT_SHA)" || echo "⚠️  Ошибка запуска P4 ExecHealth SLO сервисов (non-critical)"; \
		echo "⏰ Запуск OFC Contextual Runtime Health Exporter..."; \
		$(DOCKER_COMPOSE_BIN) -f python-worker/orderflow_services/deploy/compose/docker-compose.ofc-contextual-runtime-health-exporter-v1.yml up -d 2>/dev/null && \
		echo "✅ OFC Contextual Runtime Health Exporter запущен" || echo "⚠️  Ошибка запуска OFC Contextual Health Exporter"; \
		echo "⏰ Запуск OFC Contextual Runtime Summary Writer..."; \
		$(DOCKER_COMPOSE_BIN) -f python-worker/orderflow_services/deploy/compose/docker-compose.ofc-contextual-runtime-summary-writer-v1.yml up -d 2>/dev/null && \
		echo "✅ OFC Contextual Runtime Summary Writer запущен" || echo "⚠️  Ошибка запуска OFC Contextual Summary Writer"; \
		echo "⏰ Запуск Continuation Context Window Calibrator..."; \
		$(DOCKER_COMPOSE_BIN) -f python-worker/orderflow_services/deploy/compose/docker-compose.cont-ctx-window-calibrator-v1.yml up -d 2>/dev/null && \
		echo "✅ Continuation Context Window Calibrator запущен (mode=RECOMMEND)" || echo "⚠️  Ошибка запуска Cont Ctx Window Calibrator"; \
		echo "📊 Запуск Trade Monitor (paper vs demo report)..."; \
		$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_PYTHON_WORKERS) up -d trade-monitor 2>/dev/null && \
		echo "✅ Trade Monitor запущен" || echo "⚠️  Ошибка запуска Trade Monitor"; \
		echo "⏰ Запуск P3 Fallback Plane, P3.1 Handoff, P3.2 Local Report, P3.3 Offline Debug, P3.4 Emergency Summarize, P3.5-P3.9 RCA Rollout Engine..."; \
		$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TIMERS) up -d scanner-local-fallback-plane-v3-0 scanner-vertex-local-fallback-handoff-v3-1 scanner-local-report-handoff-rewire-v3-2 scanner-offline-debug-handoff-rewire-v3-3 scanner-emergency-summarize-handoff-rewire-v3-4 scanner-route-incident-rca-shadow-handoff-v3-5 scanner-route-incident-rca-shadow-comparator-v3-6 scanner-route-incident-rca-mirror-governor-v3-7 scanner-route-incident-rca-mirror-verification-v3-8 scanner-route-incident-rca-mirror-rollout-controller-v3-9 scanner-route-incident-rca-mirror-slo-v3-10 scanner-route-incident-rca-mirror-retry-v3-10 scanner-route-incident-rca-mirror-escalations-v3-10 scanner-route-incident-rca-mirror-incident-bundles-v3-11 scanner-route-incident-rca-mirror-rca-bridge-v3-12 scanner-route-incident-rca-mirror-vertex-rca-consumer-v3-13 scanner-route-incident-rca-mirror-vertex-feedback-governor-v3-13 scanner-route-incident-rca-mirror-rca-experiment-harness-v3-14 scanner-route-incident-rca-mirror-rca-evaluator-v3-15 scanner-route-incident-rca-mirror-rca-winner-apply-v3-16 scanner-route-incident-rca-mirror-rca-winner-apply-verification-v3-17 scanner-route-incident-rca-mirror-rca-winner-apply-slo-analytics-v3-18 scanner-route-incident-rca-mirror-rca-winner-apply-retry-controller-v3-18 scanner-route-incident-rca-mirror-rca-winner-apply-auto-escalation-summarizer-v3-18 scanner-route-incident-rca-mirror-rca-winner-apply-incident-bundles-v3-19 scanner-route-incident-rca-mirror-rca-winner-apply-bundle-rca-bridge-v3-20 scanner-route-incident-rca-mirror-rca-winner-apply-vertex-rca-consumer-v3-21 scanner-route-incident-rca-mirror-rca-winner-apply-vertex-feedback-governor-v3-21 scanner-route-incident-rca-mirror-rca-winner-apply-experiment-harness-v3-22 scanner-route-incident-rca-mirror-rca-winner-apply-evaluator-v3-23 scanner-route-incident-rca-mirror-rca-winner-apply-apply-controller-v3-24 scanner-route-incident-rca-mirror-rca-winner-apply-verification-v3-25 scanner-route-incident-rca-mirror-rca-winner-apply-apply-slo-analytics-v3-26 scanner-route-incident-rca-mirror-rca-winner-apply-apply-retry-controller-v3-26 scanner-route-incident-rca-mirror-rca-winner-apply-apply-auto-escalation-summarizer-v3-26 scanner-route-incident-rca-mirror-rca-winner-apply-apply-incident-bundle-builder-v3-27 scanner-route-incident-rca-mirror-rca-winner-apply-apply-bundle-rca-bridge-v3-28 scanner-route-incident-rca-mirror-rca-winner-apply-apply-vertex-rca-consumer-v3-29 scanner-route-incident-rca-mirror-rca-winner-apply-apply-vertex-feedback-governor-v3-29 2>/dev/null && \
		echo "✅ P3.0–P3.29 запущены" || echo "⚠️  Ошибка веб-запуска Fallback Plane"; \
		echo "⏰ Запуск P3.57.4 Replay Gate..."; \
		$(DOCKER_COMPOSE_BIN) -f orderflow_services/docker_compose_fragment_ml_phase3_57_4_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_replay_gate_v1.yml up -d ml-route-incident-rca-apply-replay-gate-v3-57-4 2>/dev/null && \
		echo "✅ P3.57.4 Replay Gate запущен" || echo "⚠️  Ошибка запуска P3.57.4 Replay Gate"; \
	else \
		echo "⚠️  docker-compose-crypto-orderflow.yml не найден, пропускаем запуск сервисов"; \
	fi
	@sleep 3
	@echo ""
	@echo "⏰ Запуск таймеров (периодические задачи)..."
	@echo "   - OF Gate SRE Monitor (каждые 10 минут)"
	@echo "   - Hard-stop unclamp v7 (каждые 5 минут, budget limiter + двухфазный PROPOSE)"
	@echo "   - Meta ENFORCE ramp-or-freeze (ежедневно в 06:20)"
	@echo "   - Nightly calibration (ежедневно в 05:30)"
	@echo "   - Meta model training (ежедневно в 05:10)"
	@echo "   - Nightly Confidence Scorer Weight Tuning (ежедневно в 02:35, export + tune + Redis config publish)"
	@echo "   - ML train nightly v2 (ежедневно в 05:30, LR + GBDT с калибровкой, schema v2)"
	@echo "   - ML SRE Monitor (каждые 10 минут, метрики качества ML: p_edge, missing_rate, err_rate, latency)"
	@echo "   - ML Rollout Guard (каждый час, freeze/unfreeze proposals на основе метрик)"
	@echo "   - ML rollout guard v3 (каждый час, dual-window health, freeze floor + clamp per-symbol maps, auto-unfreeze)"
	@echo "   - ML outcome joiner v3 (daemon, bucket-aware + exec_risk_norm, events:trades -> metrics:ml_outcome)"
	@echo "   - Meta Model LR v1 Training (ежедневно в 05:00 UTC)"
	@echo "   - ML promotion ladder v3 (ежедневно в 06:35, per-symbol shares + utility gates + range exec veto)"
	@echo "   - ML threshold proposer v1 (ежедневно в 06:45, p_min proposals with Approve/Reject)"
	@echo "   - ML threshold proposer v2 utility (ежедневно в 06:55, v7: utility + calib + drift-guard)"
	@echo "   - ML rollback proposer (каждый час, propose restore *_prev if safety/utility fails)"
	@echo "   - ML TB train v10.3 timer (ежедневно в 05:40, utility + strict OOF + threshold opt)"
	@echo "   - ML promo callbacks worker v10.3 (daemon, handles Approve/Reject for v10.3 models)"
	@echo "   - ML golden replay timer (ежедневно в 05:05, export inputs -> replay -> diff)"
	@echo "   - Feature Denylist AB runner (ежедневно в 06:45, pending_ab -> ab_done/ab_failed + metric exports)"
	@echo "   - Tick quality policy/recommendations (каждые 6 часов, Step 14)"
	@echo "   - Tick quality alerts (EMA-based, Step 15)"
	@echo "   - Side sign audit timer (ежедневно в 04:00, static audit for SELL-bias patterns)"
	@echo "   - Nightly engine regress safe (ежедневно в 04:30, replay baseline -> diff vs current engine)"
	@echo "   - P59 Edge Stack train exporter (порт 9813, metrics:edge_stack_train:last → Prometheus)"
	@echo "   - P60 Edge Stack shadow exporter (порт 8012, shadow_status.json → Prometheus)"
	@echo "   - FGH Offline Edge Stack Dataset Builder (ежедневно в 01:30 UTC)"
	@echo "   - Ablation E-block + Denylist Auto-Generator Timer (ежедневно в 06:15 UTC)"
	@if [ -f "$(COMPOSE_TIMERS)" ]; then \
		$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TIMERS) up -d && \
		echo "✅ Таймеры запущены (включая Meta ENFORCE ramp-or-freeze, ML rollout guard v3, ML outcome joiner v3, ML promotion ladder v3, ML threshold proposer v1/v2, ML rollback proposer, ML TB train v10.3)" || echo "⚠️  Ошибка запуска таймеров"; \
	else \
		echo "⚠️  $(COMPOSE_TIMERS) не найден, пропускаем запуск таймеров"; \
	fi
	@echo "📰 Запуск News Agent на minik (192.168.0.121)..."
	@if [ -f "scripts/deploy-news-agent-to-minik.sh" ] && ssh -o ConnectTimeout=3 -o BatchMode=yes alex@192.168.0.121 "true" 2>/dev/null; then \
		echo "  📡 Minik доступен — деплой news_agent..."; \
		./scripts/deploy-news-agent-to-minik.sh all 2>&1 | tail -5 && \
		echo "✅ News Agent развёрнут и запущен на minik" || echo "⚠️  Ошибка развёртывания News Agent на minik (non-critical)"; \
	else \
		echo "⚠️  Minik недоступен — News Agent пропущен (запускается только на minik)"; \
	fi
	@echo "🚀 Запуск Trade-side Consumer для новостей (P4)..."
	@if [ -f "../news_agent/docker-compose-trade-news-cache.yml" ]; then \
		$(DOCKER_COMPOSE_BIN) -f ../news_agent/docker-compose-trade-news-cache.yml up -d --build 2>/dev/null && \
		echo "✅ Trade-side Consumer запущен" || echo "⚠️  Ошибка запуска Trade-side Consumer"; \
	fi
	@sleep 2
	@echo ""
	@echo "📊 Мониторинг (Grafana + Alertmanager) отключён из auto-start. Запуск вручную: make monitoring-start"
	# @$(MAKE) monitoring-start || echo "⚠️  Ошибка запуска мониторинга (non-critical)"
	@echo ""
	@echo "📊 Запуск Metrics Proxy..."
	@if [ -f "docker-compose-infrastructure.yml" ]; then \
		$(DOCKER_COMPOSE_BIN) -f docker-compose-infrastructure.yml up -d metrics-proxy 2>/dev/null && \
		echo "✅ Metrics Proxy запущен" || echo "⚠️  Ошибка запуска Metrics Proxy"; \
	fi
	@$(MAKE) logs

up-bg:
	@echo "🔍 Проверка существующих контейнеров..."
	@EXISTING=$$(docker ps -a --filter "name=scanner-" --format "{{.Names}}" 2>/dev/null | wc -l); \
	REDIS_EXISTING=$$(docker ps -a --filter "name=redis" --format "{{.Names}}" 2>/dev/null | wc -l); \
	if [ $$EXISTING -gt 0 ] || [ $$REDIS_EXISTING -gt 0 ]; then \
		echo "⚠️  Найдено $$EXISTING существующих контейнеров scanner-* и $$REDIS_EXISTING redis-*"; \
		echo "🛑 Остановка и удаление существующих контейнеров..."; \
		docker ps -a --filter "name=scanner-" --format "{{.Names}}\t{{.Status}}" | sed 's/^/  - /'; \
		docker ps -a --filter "name=redis" --format "{{.Names}}\t{{.Status}}" | sed 's/^/  - /'; \
		echo ""; \
		echo "Остановка через docker-compose..."; \
		$(DC_WITH_TRAILING) down --remove-orphans 2>/dev/null || true; \
		echo "Прямое удаление всех scanner-* и redis-* контейнеров..."; \
		docker ps -a --filter "name=scanner-" --format "{{.Names}}" | xargs -r docker rm -f 2>/dev/null || true; \
		docker ps -a --filter "name=redis" --format "{{.Names}}" | xargs -r docker rm -f 2>/dev/null || true; \
		docker ps -a --filter "name=go-gateway" --format "{{.Names}}" | xargs -r docker rm -f 2>/dev/null || true; \
		echo "✅ Контейнеры остановлены и удалены"; \
		sleep 2; \
	else \
		echo "✅ Нет существующих контейнеров"; \
	fi
	@echo ""
	@echo "🌐 Проверка Docker сети trade-network..."
	@if ! docker network ls --format "{{.Name}}" | grep -qx "trade-network"; then \
		echo "⚠️  Сеть trade-network не найдена"; \
		echo "🔧 Создание сети trade-network..."; \
		docker network create trade-network >/dev/null && echo "✅ Сеть trade-network создана" || { echo "❌ Не удалось создать сеть"; exit 1; }; \
	else \
		echo "✅ Сеть trade-network уже существует"; \
	fi
	@echo ""
	@echo "🔧 Запуск сборки батчами по 5 сервисов (для снижения нагрузки на BuildKit)..."
	@SERVICES=$$($(DC_WITH_TRAILING) --profile default config --services 2>/dev/null); \
	if [ -z "$$SERVICES" ]; then \
		echo "⚠️ Ошибка получения списка сервисов, попытка собрать все сразу..."; \
		COMPOSE_PARALLEL_LIMIT=2 ./scripts/with-buildkit.sh $(DC_WITH_TRAILING) --profile default build; \
	else \
		echo "$$SERVICES" | xargs -n 5 > .build_batches_bg.tmp; \
		while read -r batch; do \
			if [ -z "$$batch" ]; then continue; fi; \
			echo "🛠️ Сборка батча (параллельность 2): $$batch"; \
			COMPOSE_PARALLEL_LIMIT=2 ./scripts/with-buildkit.sh $(DC_WITH_TRAILING) --profile default build $$batch || exit 1; \
		done < .build_batches_bg.tmp; \
		rm -f .build_batches_bg.tmp; \
	fi
	@echo "🚀 Запуск сервисов в фоне (поэтапный старт для снижения нагрузки на Docker DNS)..."
	@echo ""
	@echo "📦 Phase 1/3: Infrastructure (Redis + Postgres)..."
	@./scripts/with-buildkit.sh $(DC_WITH_TRAILING) --profile default up -d \
		redis redis-ticks redis-ticks-b redis-worker-1 redis-worker-2 redis-worker-1b redis-worker-2b \
		redis-exporter-worker-1 redis-exporter-worker-1b redis-exporter-worker-2 redis-exporter-worker-2b \
		postgres migration-runner
	@echo "⏳ Waiting 15s for infrastructure to stabilize..."
	@sleep 15
	@echo ""
	@echo "📦 Phase 2a/4: Go gateway + high-frequency workers (1m, 5m, 15m, 1h, 4h)..."
	@./scripts/with-buildkit.sh $(DC_WITH_TRAILING) --profile default up -d \
		go-gateway go-worker-1m go-worker-5m go-worker-15m go-worker-1h go-worker-4h
	@echo "⏳ Waiting 10s for DNS to stabilize..."
	@sleep 10
	@echo ""
	@echo "📦 Phase 2b/4: Low-frequency Go workers (1d, 1w, 1month, 3month, 1y)..."
	@./scripts/with-buildkit.sh $(DC_WITH_TRAILING) --profile default up -d \
		go-worker-1d go-worker-1w go-worker-1month go-worker-3month go-worker-1y
	@echo "⏳ Waiting 10s for DNS to stabilize..."
	@sleep 10
	@echo ""
	@echo "📦 Phase 2c/4: Signal chain + Python services..."
	@./scripts/with-buildkit.sh $(DC_WITH_TRAILING) --profile default up -d \
		signal-hub signal-dispatcher python-worker execution-gate-service of-confirm-service \
		atr-worker regime-worker regime-storage news-ingestor-go
	@echo "⏳ Waiting 15s for signal chain + DNS to stabilize..."
	@sleep 15
	@echo ""
	@echo "📦 Phase 3/3: All remaining services..."
	@./scripts/with-buildkit.sh $(DC_WITH_TRAILING) --profile default up -d
	@echo ""
	@echo "🛡️  Checking Database Integrity..."
	@./scripts/ensure_db.sh
	@echo "✅ Сервисы запущены в фоне"
	@echo "   - Core services: signal-generator, aggregated-hub, go-gateway, telegram-worker"
	@echo "   - TP Event Listener: scanner-tp-event-listener (TP1 Trailing System)"
	@echo "   - Trailing Autotune: scanner-trailing-autotune, scanner-trailing-autotune-24h"
	@echo ""
	@echo "🎯 Ожидание запуска сервисов перед калибровкой..."
	@sleep 10
	@echo "📊 Запуск автоматической настройки локальной калибровки..."
	@./python-worker/scripts/setup_and_run_calibration.sh || echo "⚠️  Calibration skipped (DB not ready) — run: make calibration-run"
	@echo "🛡️  Applying DQ Gate Penalty-Only Config..."
	@cat python-worker/ops/dynamic_cfg_examples/dq_gate_penalty_only.hset | docker exec -i redis-worker-1 redis-cli >/dev/null || echo "⚠️  Failed to apply DQ gate config (Redis may not be ready)"
	@echo ""
	@echo "🤖 Запуск автоматического обучения калибровки уверенности..."
	@make calibration-auto-start
	@echo ""
	@echo "📰 Запуск пайплайна новостей..."
	@./start-news-pipeline.sh
	@echo ""
	@echo "🔔 Запуск ML RCA Notification Bridge..."
	@$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_PYTHON_WORKERS) up -d ml-rca-notification-bridge

	@echo "🏆 AB Winner Evaluator V3 (Docker) запущен в фоне..."
	@echo ""
	@echo "📋 Запуск сервиса одобрения рекомендаций..."
	@echo "   - recs-callback-worker (обработка callbacks из bot-nest)"
	@echo "   - recs-callback-worker-v2 (двухфазный PROPOSE: preview2 -> confirm)"
	@if [ -f "docker-compose-crypto-orderflow.yml" ]; then \
		echo "🚀 Запуск crypto-orderflow-service и сервисов одобрения рекомендаций..."; \
		$(DOCKER_COMPOSE_BIN) -f docker-compose-crypto-orderflow.yml up -d crypto-orderflow-service crypto-orderflow-service-2 recs-callback-worker recs-callback-worker-v2 2>/dev/null && \
		echo "✅ Crypto-orderflow-service и сервисы одобрения рекомендаций запущены" || echo "⚠️  Ошибка запуска сервисов"; \
		echo "⏰ Запуск Binance Execution / Reporter (P5+P9)..."; \
		$(DOCKER_COMPOSE_BIN) -f docker-compose-binance.yml -f config/docker-compose.execution-p5.override.yml -f config/docker-compose.execution-p9.override.yml up -d binance-executor binance-account-reporter active-symbol-guard-exporter 2>/dev/null && \
		echo "✅ Binance Execution и Reporter запущены (P5+P9 overlay)" || echo "⚠️  Ошибка запуска Binance сервисов"; \

		echo "⏰ Запуск OFC таймеров (валидация, replay, fill expected, benchmark)..."; \
		$(DOCKER_COMPOSE_BIN) -f docker-compose-crypto-orderflow.yml up -d of-timers-worker archiver-signals-1 of-inputs-v3-circuit-exporter prom-rules-bundle-smoke-timer 2>/dev/null && \
		echo "✅ OFC таймеры запущены" || echo "⚠️  Ошибка запуска OFC таймеров"; \
		echo "⏰ Запуск ML таймеров (edge_stack_v1 training, edge_stack_v1_oof training, drift monitoring)..."; \
		$(DOCKER_COMPOSE_BIN) -f docker-compose-crypto-orderflow.yml up -d ml-train-edge-stack-v1-timer ml-train-edge-stack-v1-oof-timer ml-drift-monitor-v1-timer 2>/dev/null && \
		echo "✅ ML таймеры запущены" || echo "⚠️  Ошибка запуска ML таймеров"; \
		echo "⏰ Запуск edge_stack_v1 dataset builder таймера..."; \
		$(DOCKER_COMPOSE_BIN) -f docker-compose-crypto-orderflow.yml up -d edge-stack-v1-dataset-timer 2>/dev/null && \
		echo "✅ Edge stack v1 dataset timer запущен" || echo "⚠️  Ошибка запуска edge stack v1 dataset timer"; \
		echo "🚀 Запуск Telegram ChatOps Freeze Bot..."; \
		$(DOCKER_COMPOSE_BIN) -f docker-compose-crypto-orderflow.yml up -d chatops-telegram-freeze-bot 2>/dev/null && \
		echo "✅ Telegram ChatOps Freeze Bot запущен" || echo "⚠️  Ошибка запуска ChatOps Bot"; \
		echo "⏰ Запуск tick time observability таймера..."; \
		$(DOCKER_COMPOSE_BIN) -f docker-compose-crypto-orderflow.yml up -d tick-time-lag-report-timer tick-time-autotune-timer 2>/dev/null && \
		echo "✅ Tick time lag report timer запущен" || echo "⚠️  Ошибка запуска tick time lag report timer"; \
		echo "⏰ Запуск replay inputs archiver (P0: archive signals:of:inputs -> ml_replay_inputs_v1)..."; \
		$(DOCKER_COMPOSE_BIN) -f docker-compose-crypto-orderflow.yml up -d replay-archiver 2>/dev/null && \
		echo "✅ Replay inputs archiver запущен" || echo "⚠️  Ошибка запуска replay inputs archiver"; \
		echo "⏰ Запуск Meta Coverage Ops (P38)..."; \
		$(DOCKER_COMPOSE_BIN) -f docker-compose-crypto-orderflow.yml up -d meta-cov-rollout-exporter meta-enforce-cov-auto-apply-timer 2>/dev/null && \
		echo "✅ Meta Coverage Ops запущены" || echo "⚠️  Ошибка запуска Meta Coverage Ops"; \
		echo "⏰ Запуск P5.7 Execution Audit Chain (checker + runbook server)..."; \
		mkdir -p runtime/execution_audit_chain runtime/node_exporter_textfile 2>/dev/null || true; \
		COMPOSE_PROFILES=ops $(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TIMERS) up -d execution-audit-chain-checker execution-audit-runbook-server 2>/dev/null && \
		echo "✅ P5.7 Execution Audit Chain checker + runbook server запущены (порт 8777)" || echo "⚠️  Ошибка запуска P5.7 Execution Audit Chain (non-critical)"; \
		echo "⏰ Запуск P4 ExecHealth SLO Checker + Exporter (порт 9824)..."; \
		GIT_SHA=$$(git rev-parse --short HEAD 2>/dev/null || echo "unknown"); \
		EXEC_HEALTH_DEPLOY_ID=$${EXEC_HEALTH_DEPLOY_ID:-$$GIT_SHA} $(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TIMERS) up -d exec-health-slo-checker exec-health-slo-exporter 2>/dev/null && \
		echo "✅ P4 ExecHealth SLO Checker + Exporter запущены (порт 9824, deploy_id=$$GIT_SHA)" || echo "⚠️  Ошибка запуска P4 ExecHealth SLO сервисов (non-critical)"; \
		echo "⏰ Запуск OFC Contextual Runtime Health Exporter..."; \
		$(DOCKER_COMPOSE_BIN) -f python-worker/orderflow_services/deploy/compose/docker-compose.ofc-contextual-runtime-health-exporter-v1.yml up -d 2>/dev/null && \
		echo "✅ OFC Contextual Runtime Health Exporter запущен" || echo "⚠️  Ошибка запуска OFC Contextual Health Exporter"; \
		echo "⏰ Запуск OFC Contextual Runtime Summary Writer..."; \
		$(DOCKER_COMPOSE_BIN) -f python-worker/orderflow_services/deploy/compose/docker-compose.ofc-contextual-runtime-summary-writer-v1.yml up -d 2>/dev/null && \
		echo "✅ OFC Contextual Runtime Summary Writer запущен" || echo "⚠️  Ошибка запуска OFC Contextual Summary Writer"; \
		echo "⏰ Запуск Continuation Context Window Calibrator..."; \
		$(DOCKER_COMPOSE_BIN) -f python-worker/orderflow_services/deploy/compose/docker-compose.cont-ctx-window-calibrator-v1.yml up -d 2>/dev/null && \
		echo "✅ Continuation Context Window Calibrator запущен (mode=RECOMMEND)" || echo "⚠️  Ошибка запуска Cont Ctx Window Calibrator"; \
		echo "📊 Запуск Trade Monitor (paper vs demo report)..."; \
		$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_PYTHON_WORKERS) up -d trade-monitor 2>/dev/null && \
		echo "✅ Trade Monitor запущен" || echo "⚠️  Ошибка запуска Trade Monitor"; \
		echo "⏰ Запуск P3.57.4 Replay Gate..."; \
		$(DOCKER_COMPOSE_BIN) -f orderflow_services/docker_compose_fragment_ml_phase3_57_4_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_replay_gate_v1.yml up -d ml-route-incident-rca-apply-replay-gate-v3-57-4 2>/dev/null && \
		echo "✅ P3.57.4 Replay Gate запущен" || echo "⚠️  Ошибка запуска P3.57.4 Replay Gate"; \
	else \
		echo "⚠️  docker-compose-crypto-orderflow.yml не найден, пропускаем запуск сервисов"; \
	fi
	@echo ""
	@echo "📊 Запуск Metrics Proxy (Background)..."
	@if [ -f "docker-compose-infrastructure.yml" ]; then \
		$(DOCKER_COMPOSE_BIN) -f docker-compose-infrastructure.yml up -d metrics-proxy 2>/dev/null && \
		echo "✅ Metrics Proxy запущен" || echo "⚠️  Ошибка запуска Metrics Proxy"; \
	fi
	@echo ""
	@echo "⏰ Запуск таймеров (периодические задачи)..."
	@echo "   - OF Gate SRE Monitor (каждые 10 минут)"
	@echo "   - Hard-stop unclamp v7 (каждые 5 минут, budget limiter + двухфазный PROPOSE)"
	@echo "   - Meta ENFORCE ramp-or-freeze (ежедневно в 06:20)"
	@echo "   - Nightly calibration (ежедневно в 05:30)"
	@echo "   - Meta model training (ежедневно в 05:10)"
	@echo "   - ML edge stack dataset builder v3 (ежедневно в 05:00, малый прогон sanity → большой прогон, quarantine + mismatch diagnostics, проверка joined, nearest-join fallback с tolerance)"
	@echo "   - ML edge stack v1 OOF training (ежедневно в 05:10, проверка report.json перед обучением, joined >= 2000, pos_rate разумный)"
	@echo "   - ML nightly pipeline (ежедневно в 05:30)"
	@echo "   - ML nightly train and report (ежедневно в 05:10, export + train + Telegram approve/reject)"
	@echo "   - Nightly Confidence Scorer Weight Tuning (ежедневно в 02:35, export + tune + Redis config publish)"
	@echo "   - ML rollout guard v3 (каждый час, dual-window health, freeze floor + clamp per-symbol maps, auto-unfreeze)"
	@echo "   - ML outcome joiner v3 (daemon, bucket-aware + exec_risk_norm, events:trades -> metrics:ml_outcome)"
	@echo "   - Meta Model LR v1 Training (ежедневно в 05:00 UTC)"
	@echo "   - ML promotion ladder v3 (ежедневно в 06:35, per-symbol shares + utility gates + range exec veto)"
	@echo "   - ML threshold proposer v1 (ежедневно в 06:45, p_min proposals with Approve/Reject)"
	@echo "   - ML threshold proposer v2 utility (ежедневно в 06:55, v7: utility + calib + drift-guard)"
	@echo "   - ML rollback proposer (каждый час, propose restore *_prev if safety/utility fails)"
	@echo "   - ML TB train v10.4 (ежедневно в 05:10, util_mh multi-horizon expected utility)"
	@echo "   - ML promo callbacks worker v10.4 (daemon, handles Approve/Reject for v10.4 models)"
	@echo "   - ML golden replay timer (ежедневно в 05:05, export inputs -> replay -> diff)"
	@echo "   - Feature Denylist AB runner (ежедневно в 06:45, pending_ab -> ab_done/ab_failed + metric exports)"
	@echo "   - ML golden replay timer (ежедневно в 05:05, export inputs -> replay -> diff)"
	@echo "   - Confidence Drift Monitor (every 3h, checks median/MAD drift)"
	@echo "   - FGH Offline Edge Stack Dataset Builder (ежедневно в 01:30 UTC)"
	@echo "   - Ablation E-block + Denylist Auto-Generator Timer (ежедневно в 06:15 UTC)"
	@if [ -f "$(COMPOSE_TIMERS)" ]; then \
		$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TIMERS) up -d && \
		echo "✅ Таймеры запущены (включая ML edge stack dataset builder, Meta ENFORCE ramp-or-freeze, ML nightly pipeline, ML rollout guard v3, ML outcome joiner v3, ML promotion ladder v3, ML threshold proposer v1/v2, ML rollback proposer, ML TB train v10.4, Confidence Drift Monitor)" || echo "⚠️  Ошибка запуска таймеров"; \
	else \
		echo "⚠️  $(COMPOSE_TIMERS) не найден, пропускаем запуск таймеров"; \
	fi
	@echo "⏰ Запуск таймеров News Agent (P2/P3)..."
	@if [ -f "../news_agent/docker-compose-news.yml" ]; then \
		$(DOCKER_COMPOSE_BIN) -f ../news_agent/docker-compose-news.yml up -d news_agent_timer news_feedback_batch 2>/dev/null && \
		echo "✅ News Agent Timers (timer, feedback_batch) запущены" || echo "⚠️  Ошибка запуска News Agent Timers"; \
	fi
	@echo "🚀 Запуск Trade-side Consumer для новостей (P4)..."
	@if [ -f "../news_agent/docker-compose-trade-news-cache.yml" ]; then \
		$(DOCKER_COMPOSE_BIN) -f ../news_agent/docker-compose-trade-news-cache.yml up -d --build 2>/dev/null && \
		echo "✅ Trade-side Consumer запущен" || echo "⚠️  Ошибка запуска Trade-side Consumer"; \
	fi
	@sleep 2
	@sleep 3
	@echo ""
	@echo "📊 Инициализация Regime Quantiles..."
	@./scripts/setup_regime_quantiles.sh || echo "⚠️ Regime quantiles setup failed (non-critical)"
	@echo ""
	@echo "⏰ OF Timers Worker запускается через docker-compose-timers.yml..."
	@echo "   (Config drift monitor: hourly at :15, Weekly bench: Sunday at 06:40, Nightly regress safe: daily at 04:20)"
	@echo "   (Hard-stop unclamp v7: every 5 minutes, budget limiter + двухфазный PROPOSE, per-symbol seg-health + quotas)"
	@echo "   (Meta ENFORCE ramp-or-freeze: daily at 06:20, stratified worst-case DiD with freeze cells)"
	@echo "   (Meta cells self-heal: daily at 07:10, staged unfreeze + auto-refreeze)"
	@echo "   (Meta Stage2 optimize share v4: daily at 07:25, adaptive budgets + global budget)"
	@echo ""
	@echo "📊 Проверьте статус: make status"
	@echo "🎯 Проверьте trailing: make trailing-status"
	@echo "🤖 Проверьте autotune: make trailing-autotune-status"
	@echo "🏆 Проверьте suggerster: make ab-suggester-status"
	@echo "📋 Проверьте рекомендации: make recs-status"

# ═══════════════════════════════════════════════════════════════
# Rolling Update (без рестарта Redis/Postgres)
# ═══════════════════════════════════════════════════════════════
#
# Используйте `make update` вместо `make up` когда нужно обновить
# только application-сервисы без перезапуска Redis/Postgres.
# Это предотвращает волну Connection Reset / Timeout ошибок.
#

update:
	@echo "╔═══════════════════════════════════════════════════════════════╗"
	@echo "║  🔄 Rolling Update (Redis/Postgres НЕ перезапускаются)       ║"
	@echo "╚═══════════════════════════════════════════════════════════════╝"
	@echo ""
	@REDIS_START=$$(docker inspect redis-worker-1 --format '{{.State.StartedAt}}' 2>/dev/null || echo "NOT_RUNNING"); \
	PG_START=$$(docker inspect scanner-postgres --format '{{.State.StartedAt}}' 2>/dev/null || echo "NOT_RUNNING"); \
	if [ "$$REDIS_START" = "NOT_RUNNING" ] || [ "$$PG_START" = "NOT_RUNNING" ]; then \
		echo "❌ Redis или Postgres не запущены. Используйте 'make up' для полного старта."; \
		exit 1; \
	fi; \
	echo "✅ Redis-worker-1 up since: $$REDIS_START"; \
	echo "✅ Postgres up since: $$PG_START"
	@echo ""
	@echo "📦 Phase 1/4: Пересборка изменённых образов..."
	@COMPOSE_PARALLEL_LIMIT=2 ./scripts/with-buildkit.sh $(DC_WITH_TRAILING) --profile default build
	@echo ""
	@echo "📦 Phase 2/4: Обновление Go workers..."
	@$(DC_WITH_TRAILING) up -d --no-deps --force-recreate \
		go-gateway go-worker-1m go-worker-5m go-worker-15m go-worker-1h go-worker-4h \
		go-worker-1d go-worker-1w go-worker-1month go-worker-3month go-worker-1y
	@echo "⏳ Waiting 5s for Go workers to stabilize..."
	@sleep 5
	@echo ""
	@echo "📦 Phase 3/4: Обновление signal chain + Python services..."
	@$(DC_WITH_TRAILING) up -d --no-deps --force-recreate \
		signal-hub signal-dispatcher python-worker execution-gate-service of-confirm-service \
		atr-worker regime-worker regime-storage news-ingestor-go
	@echo "⏳ Waiting 10s for signal chain to stabilize..."
	@sleep 10
	@echo ""
	@echo "📦 Phase 4/4: Все остальные application-сервисы..."
	@$(DC_WITH_TRAILING) --profile default up -d
	@echo ""
	@REDIS_END=$$(docker inspect redis-worker-1 --format '{{.State.StartedAt}}' 2>/dev/null); \
	PG_END=$$(docker inspect scanner-postgres --format '{{.State.StartedAt}}' 2>/dev/null); \
	echo "✅ Rolling update завершён"; \
	echo "   Redis-worker-1 uptime preserved: $$REDIS_END"; \
	echo "   Postgres uptime preserved: $$PG_END"
	@echo ""
	@echo "⏰ Обновление таймеров..."
	@if [ -f "$(COMPOSE_TIMERS)" ]; then \
		$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TIMERS) up -d && \
		echo "✅ Таймеры обновлены" || echo "⚠️  Ошибка обновления таймеров"; \
	fi
	@echo ""
	@echo "📊 Проверьте статус: make status"

# ═══════════════════════════════════════════════════════════════
# AB Entry Policy Suggester (V3)
# ═══════════════════════════════════════════════════════════════

ab-suggester-run:
	@echo "🏆 Запуск AB Suggester (V3) One-Shot..."
	@$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_PYTHON_WORKERS) run --rm ab-winner-suggester-v3 python -m services.ab_winner_suggester_service_v3

ab-suggester-restart:
	@echo "🔄 Перезапуск AB Suggester (V3)..."
	@$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_PYTHON_WORKERS) restart ab-winner-suggester-v3

ab-suggester-status:
	@echo "📊 Статус AB Suggester (V3):"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "ab-winner-(evaluator|suggester)" || echo "❌ Не запущены"
	@echo ""
	@echo "📋 Последние логи V3 Suggester:"
	@docker logs --tail=10 scanner-ab-winner-suggester-v3

ab-apply-runner-logs:
	@echo "📋 Логи AB Apply Runner:"
	@docker logs -f ab-winner-apply-runner 2>&1 || echo "❌ Сервис не найден"

# ═══════════════════════════════════════════════════════════════
# MetaModel A/B & Drift Management
# ═══════════════════════════════════════════════════════════════

meta-ab-status:
	@echo "📊 Статус MetaModel A/B тестирования:"
	@docker exec redis-worker-1 redis-cli GET meta_ab:last_report || echo "❌ Отчет не найден"

meta-ab-winner-status:
	@echo "📊 Статус Stage 4 MetaModel A/B Winner Evaluator:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep "meta-ab-winner-evaluator-v1"
	@echo ""
	@echo "Current Share in Redis:"
	@docker exec redis-worker-1 redis-cli HGET settings:dynamic_cfg meta_ab_challenger_share

meta-ab-logs:
	@echo "📋 Логи MetaModel A/B Evaluator:"
	@docker logs scanner-meta-ab-evaluator

meta-ab-winner-logs:
	@echo "📋 Логи Stage 4 MetaModel A/B Winner Evaluator:"
	@docker logs scanner-meta-ab-winner-evaluator-v1

meta-ab-eval-manual:
	@echo "🧪 Ручной запуск MetaModel A/B оценки..."
	@docker exec scanner-meta-ab-evaluator python3 -m tools.meta_ab_evaluator_v1 --promote

meta-ab-winner-run-manual:
	@echo "🧪 Ручной запуск Stage 4 MetaModel A/B Winner Evaluator..."
	@docker exec scanner-meta-ab-winner-evaluator-v1 sh -c "python3 -u tools/meta_ab_winner_evaluator_v1.py \
		--dataset \"\$$META_TRAIN_NDJSON\" \
		--model-champion \"\$$META_MODEL_CHAMPION_PATH\" \
		--model-challenger \"\$$META_MODEL_CHALLENGER_PATH\" \
		--apply"

# ── A/B Winner Evaluator v2 (fixed p_min + bootstrap CI + stratified guard) ──
meta-ab-winner-v2-status:
	@echo "📊 Статус Stage 4 MetaModel A/B Winner Evaluator v2:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep "meta-ab-winner-evaluator-v2" || echo "❌ Not running"
	@echo ""
	@echo "Current Share in Redis:"
	@docker exec redis-worker-1 redis-cli HGET settings:dynamic_cfg meta_ab_challenger_share
	@echo ""
	@echo "Last Winner:"
	@docker exec redis-worker-1 redis-cli HGET settings:dynamic_cfg meta_ab_last_winner

meta-ab-winner-v2-logs:
	@echo "📋 Логи Stage 4 MetaModel A/B Winner Evaluator v2:"
	@docker logs --tail=100 scanner-meta-ab-winner-evaluator-v2

meta-ab-winner-v2-run-manual:
	@echo "🧪 Ручной запуск Stage 4 MetaModel A/B Winner Evaluator v2 (fixed p_min + CI + strata)..."
	@docker exec scanner-meta-ab-winner-evaluator-v2 sh -c "python3 -u -m tools.meta_ab_winner_evaluator_v2 \
		--in-ndjson \"\$$META_TRAIN_NDJSON\" \
		--champion-model \"\$$META_MODEL_CHAMPION_PATH\" \
		--challenger-model \"\$$META_MODEL_CHALLENGER_PATH\" \
		--apply \"\$$META_AB_APPLY\""

meta-drift-status:
	@echo "📊 Статус MetaModel Drift Guard:"
	@docker exec redis-worker-1 redis-cli GET meta_drift:last_report || echo "❌ Отчет не найден"

meta-drift-logs:
	@echo "📋 Логи MetaModel Drift Guard:"
	@docker logs scanner-meta-drift-guard

meta-drift-guard-manual:
	@echo "🛡️ Ручной запуск MetaModel Drift Guard (Legacy Direct Write)..."
	@docker exec scanner-meta-drift-guard python3 -m tools.meta_drift_guard_v1 --write-freeze --notify

meta-drift-suggest-manual:
	@echo "🛡️ Ручной запуск MetaModel Drift Guard (Suggestion Mode)..."
	@docker exec scanner-meta-drift-guard python3 -m tools.meta_drift_guard_v1 --emit-suggestion --unfreeze-on-ok --notify

entry-policy-apply-overrides:
	@echo "🚀 Применение одобренных overrides (kind=overrides_v1)..."
	@docker exec scanner-trade-ml-sre-monitor-timer python3 -m services.entry_policy_apply_runner_v2 --kind overrides_v1

# ═══════════════════════════════════════════════════════════════
# P9 ExecHealth Dual-Control Freeze / Thaw
#
# Переменные (из ENV или аргументов make):
#   OPERATOR    — оператор (фаза 1 / фаза 3)
#   APPROVER    — второй оператор (фаза 2, ≠ OPERATOR)
#   REASON      — причина
#   TICKET      — номер тикета
#   NONCE       — pending_ack_nonce (из exec-health-freeze-status)
#   REQUEST_ID  — active_thaw_request_id (из exec-health-freeze-status)
#   MINUTES     — длительность force-freeze (default 30)
# ═══════════════════════════════════════════════════════════════

# Контейнер, в котором работает override CLI
EXEC_HEALTH_CTR ?= scanner-exec-health-slo-autoguard
# Путь к скрипту внутри контейнера
EXEC_HEALTH_OVERRIDE_CMD ?= python3 -m orderflow_services.exec_health_freeze_override_v1

exec-health-freeze-status:
	@echo "📊 ExecHealth Freeze Status (P9 dual-control)..."
	@docker exec $(EXEC_HEALTH_CTR) $(EXEC_HEALTH_OVERRIDE_CMD) status

exec-health-freeze-prepare:
	@echo "🔐 P9 Фаза 1: prepare-thaw (Оператор А)..."
	@echo "   OPERATOR=$(OPERATOR) REASON=$(REASON) TICKET=$(TICKET) NONCE=$(NONCE)"
	@[ -n "$(OPERATOR)" ] || (echo "❌ OPERATOR is required: make exec-health-freeze-prepare OPERATOR=alice REASON='...' TICKET=INC-42 NONCE=<nonce>"; exit 1)
	@[ -n "$(REASON)" ]   || (echo "❌ REASON is required"; exit 1)
	@[ -n "$(TICKET)" ]   || (echo "❌ TICKET is required"; exit 1)
	@[ -n "$(NONCE)" ]    || (echo "❌ NONCE is required (run: make exec-health-freeze-status)"; exit 1)
	@docker exec $(EXEC_HEALTH_CTR) $(EXEC_HEALTH_OVERRIDE_CMD) prepare-thaw \
		--operator "$(OPERATOR)" \
		--reason "$(REASON)" \
		--ticket "$(TICKET)" \
		--nonce "$(NONCE)"

exec-health-freeze-approve:
	@echo "✅ P9 Фаза 2: approve-thaw (Оператор B ≠ A)..."
	@echo "   APPROVER=$(APPROVER) REQUEST_ID=$(REQUEST_ID)"
	@[ -n "$(APPROVER)" ]    || (echo "❌ APPROVER is required: make exec-health-freeze-approve APPROVER=bob REQUEST_ID=<id>"; exit 1)
	@[ -n "$(REQUEST_ID)" ]  || (echo "❌ REQUEST_ID is required (run: make exec-health-freeze-status)"; exit 1)
	@docker exec $(EXEC_HEALTH_CTR) $(EXEC_HEALTH_OVERRIDE_CMD) approve-thaw \
		--approver "$(APPROVER)" \
		--request-id "$(REQUEST_ID)"

exec-health-freeze-commit:
	@echo "🔓 P9 Фаза 3: commit-thaw (снятие заморозки + HMAC-SHA256 dual-control подпись)..."
	@echo "   OPERATOR=$(OPERATOR) REQUEST_ID=$(REQUEST_ID)"
	@[ -n "$(OPERATOR)" ]    || (echo "❌ OPERATOR is required: make exec-health-freeze-commit OPERATOR=alice REQUEST_ID=<id>"; exit 1)
	@[ -n "$(REQUEST_ID)" ]  || (echo "❌ REQUEST_ID is required (run: make exec-health-freeze-status)"; exit 1)
	@docker exec $(EXEC_HEALTH_CTR) $(EXEC_HEALTH_OVERRIDE_CMD) commit-thaw \
		--operator "$(OPERATOR)" \
		--request-id "$(REQUEST_ID)"

exec-health-freeze-force:
	@echo "🔒 ExecHealth: принудительная заморозка (maintenance window)..."
	@echo "   OPERATOR=$(OPERATOR) REASON=$(REASON) TICKET=$(TICKET) MINUTES=$(or $(MINUTES),30)"
	@[ -n "$(OPERATOR)" ] || (echo "❌ OPERATOR is required: make exec-health-freeze-force OPERATOR=alice REASON='maint' TICKET=CHG-1 MINUTES=30"; exit 1)
	@[ -n "$(REASON)" ]   || (echo "❌ REASON is required"; exit 1)
	@[ -n "$(TICKET)" ]   || (echo "❌ TICKET is required"; exit 1)
	@docker exec $(EXEC_HEALTH_CTR) $(EXEC_HEALTH_OVERRIDE_CMD) freeze \
		--operator "$(OPERATOR)" \
		--reason "$(REASON)" \
		--ticket "$(TICKET)" \
		--minutes "$(or $(MINUTES),30)"

exec-health-freeze-undo:
	@echo "⚠️  P8 legacy single-operator thaw (только при EXEC_HEALTH_REQUIRE_DUAL_CONTROL_ACK=0)..."
	@echo "   OPERATOR=$(OPERATOR) REASON=$(REASON) TICKET=$(TICKET) NONCE=$(NONCE)"
	@[ -n "$(OPERATOR)" ] || (echo "❌ OPERATOR is required"; exit 1)
	@[ -n "$(REASON)" ]   || (echo "❌ REASON is required"; exit 1)
	@[ -n "$(TICKET)" ]   || (echo "❌ TICKET is required"; exit 1)
	@[ -n "$(NONCE)" ]    || (echo "❌ NONCE is required"; exit 1)
	@docker exec $(EXEC_HEALTH_CTR) $(EXEC_HEALTH_OVERRIDE_CMD) thaw \
		--operator "$(OPERATOR)" \
		--reason "$(REASON)" \
		--ticket "$(TICKET)" \
		--nonce "$(NONCE)"

meta-freeze-open:
	@echo "🔓 Установка MetaModel Freeze в OPEN (veto=0)..."
	@docker exec redis-worker-1 redis-cli SET cfg:meta_model_freeze 1
	@echo "⚠️ Не забудьте установить META_FREEZE_MODE=OPEN в ENV если нужно глобально"

meta-freeze-closed:
	@echo "🔒 Установка MetaModel Freeze в CLOSED (veto=1)..."
	@docker exec redis-worker-1 redis-cli SET cfg:meta_model_freeze 1
	@echo "⚠️ Не забудьте установить META_FREEZE_MODE=CLOSED в ENV если нужно глобально"

meta-train-manual:
	@echo "🤖 Running nightly meta training manual..."
	@docker exec scanner-meta-model-train-timer python3 tools/train_meta_model_lr_v4.py \
		--in-parquet /var/lib/trade/of_reports/datasets/meta_train.parquet \
		--label-col y \
		--schema ${META_SCHEMA:-meta_feat_v3} \
		--out-json /var/lib/scanner/models/meta_lr_champion.json

ml-confirm-init-manual:
	@echo "🤖 Initializing ML Confirm config (redis-worker-1 via orderflow)..."
	@docker exec scanner-crypto-orderflow python3 tools/init_ml_confirm_on_startup.py || echo "⚠️  orderflow init failed"
	@echo "🤖 Propagating ML Confirm config to all Redis workers (1b, 2, 2b)..."
	@CFG=$$(docker exec redis-worker-1 redis-cli GET cfg:ml_confirm:champion 2>/dev/null); \
	if [ -n "$$CFG" ]; then \
		for inst in redis-worker-1b redis-worker-2 redis-worker-2b; do \
			docker exec $$inst redis-cli SET cfg:ml_confirm:champion "$$CFG" > /dev/null 2>&1 \
				&& echo "  ✅ $$inst: cfg:ml_confirm:champion seeded" \
				|| echo "  ⚠️  $$inst: seed failed (non-critical)"; \
		done; \
	else \
		echo "  ⚠️  cfg:ml_confirm:champion not found on redis-worker-1, skipping propagation"; \
	fi

of-conf-cal-promote-manual:
	@echo "🚀 Running Confidence Calibration Promotion Manager (manual)..."
	@docker exec scanner-conf-cal-promotion-timer python3 -m ml_analysis.tools.conf_cal_promotion_manager_v1

of-conf-cal-promote-status:
	@echo "📊 Status of last Confidence Calibration Promotion:"
	@docker exec scanner-conf-cal-promotion-timer cat /shared/conf_cal/promotion_status.json 2>/dev/null || echo "❌ No status file found"

of-conf-cal-promote-proof:
	@echo "🛡️  Current Shadow Proof State:"
	@docker exec scanner-conf-cal-promotion-timer cat /shared/conf_cal/proof_state.json 2>/dev/null || echo "❌ No proof file found"

# ═══════════════════════════════════════════════════════════════
# Confidence Score Guardrails
# ═══════════════════════════════════════════════════════════════

guardrails-apply-logs:
	@echo "📋 Logs for Guardrails Apply Timer:"
	@docker logs -f scanner-conf-score-guard-apply-timer

guardrails-apply-restart:
	@echo "🔄 Restarting Guardrails Apply Timer..."
	@docker restart scanner-conf-score-guard-apply-timer

guardrails-exporter-logs:
	@echo "📋 Logs for Guardrails Exporter:"
	@docker logs -f scanner-conf-score-guard-exporter

guardrails-apply-manual:
	@echo "🛡️  Manual run of Guardrails Apply..."
	@docker exec scanner-conf-score-guard-apply-timer python3 -m orderflow_services.conf_score_guardrails_apply_v1 \
	  --drift-report /var/lib/trade/of_reports/drift/latest.json \
	  --apply 1 \
	  --redis-url redis://redis-worker-1:6379/0 \
	  --state-path /var/lib/trade/of_reports/conf_score_guard_state.json

meta-nightly-manual:
	@echo "🤖 Running nightly meta pipeline manual (P7 Unified)..."
	@docker exec scanner-meta-nightly-pipeline python3 -m tools.nightly_meta_pipeline_v1 \
		--in-parquet /var/lib/trade/of_reports/datasets/nightly_meta_v4.parquet \
		--out-model-json /var/lib/trade/of_reports/models/meta_lr_v4_nightly.json \
		--out-report-json /var/lib/trade/of_reports/reports/meta_report_v4_nightly.json \
		--prom-textfile /var/lib/node_exporter/textfile_collector/meta_quality_v4.prom \
		--label-col y \
		--build-dataset \
		--apply-ramp \
		--ramp-state /var/lib/trade/of_reports/states/ramp_v4.json \
		--schema $${META_SCHEMA:-meta_feat_v3} \
		--canary-symbols "$${CANARY_SYMBOLS:-BTCUSDT,ETHUSDT}" \
		--pg-fallback "$${PG_FALLBACK:-1}"



quality-status:
	@echo "📊 Signal Quality Status (P53):"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "joiner-v5|kpi-v3|exporter-v3" || echo "❌ Не запущены"

quality-logs:
	@echo "📋 Signal Quality Logs (P53):"
	@docker logs --tail=20 scanner-trade-close-joiner-v5
	@docker logs --tail=20 scanner-signal-quality-kpi-v3
	@docker logs --tail=20 scanner-signal-quality-exporter-v3

quality-restart:
	@echo "🔄 Перезапуск Signal Quality воркеров (P53)..."
	@$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TIMERS) restart trade-close-joiner-v5 signal-quality-kpi-v3 signal-quality-exporter-v3
quality-test:
	@echo "🧪 Running Signal Quality Unit Tests (P53)..."
	@export PYTHONPATH=$$(pwd)/python-worker && pytest python-worker/tests/test_p53_joiner_v5.py python-worker/tests/test_p53_kpi_v3.py

meta-train-status:
	@echo "📊 Meta Train Status (Sidecar):"
	@docker exec scanner-meta-model-train-timer cat /var/lib/scanner/models/meta_lr_champion.json.meta.json || echo "❌ No meta found"

meta-quality-status:
	@echo "📊 Meta Quality Report:"
	@docker exec scanner-meta-model-quality-report-timer cat /var/lib/node_exporter/textfile_collector/meta_quality.prom || echo "❌ No report found"

meta-train-logs:
	@docker logs scanner-meta-model-train-timer

meta-quality-logs:
	@docker logs scanner-meta-model-quality-report-timer

meta-status-snapshot-status:
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep meta-status-snapshot || echo "❌ Not running"

meta-status-snapshot-logs:
	@docker logs -f scanner-meta-status-snapshot-timer

meta-status-snapshot-restart:
	@docker restart scanner-meta-status-snapshot-timer

meta-status-snapshot-manual:
	@echo "🤖 Running meta status snapshot manually..."
	@docker exec scanner-meta-status-snapshot-timer python3 -m tools.meta_status_snapshot_v1 \
		--model-json /var/lib/trade/of_reports/models/meta_lr_v4_nightly.json \
		--report-json /var/lib/trade/of_reports/reports/meta_report_v4_nightly.json \
		--prom-textfile /var/lib/node_exporter/textfile_collector/meta_status.prom

meta-train-restart:
	@echo "🔄 Перезапуск Meta Model Train Timer..."
	@docker restart scanner-meta-model-train-timer

meta-train-health:
	@echo "🏥 Проверка здоровья Meta Model Train..."
	@docker exec scanner-meta-model-train-timer python3 -m tools.check_meta_model_train_health --print_json

meta-freeze-reset:
	@echo "🔄 Сброс MetaModel Freeze (unfreeze)..."
	@docker exec redis-worker-1 redis-cli SET cfg:meta_model_freeze 0

pre-pull:
	@echo "🚚 Предварительная загрузка базовых образов..."
	@for img in $(BASE_IMAGES); do \
		echo "   - Pulling $$img..."; \
		docker pull $$img >/dev/null 2>&1 || echo "⚠️ Не удалось загрузить $$img (пропускаем)"; \
	done
	@echo "✅ Предварительная загрузка завершена"

# ═══════════════════════════════════════════════════════════════
# Autopilot & Guardrails (SRE)
# ═══════════════════════════════════════════════════════════════

autopilot-status:
	@echo "📊 Статус Autopilot & Guardrails:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep autopilot || echo "❌ Autopilot сервисы не запущены"

autopilot-logs:
	@echo "📋 Логи Autopilot Loop (Policy Suggester):"
	@docker logs -f scanner-autopilot 2>&1 || echo "❌ scanner-autopilot не найден"

autopilot-reporter-logs:
	@echo "📋 Логи Autopilot Reporter:"
	@docker logs -f scanner-autopilot-reporter 2>&1 || echo "❌ scanner-autopilot-reporter не найден"

guardrail-logs:
	@echo "📋 Логи SRE Guardrail (Auto-Rollback):"
	@docker logs -f scanner-autopilot-guardrail 2>&1 || echo "❌ scanner-autopilot-guardrail не найден"

tm-autopilot-logs:
	@echo "📋 Логи TM Autopilot:"
	@docker logs -f scanner-tm-autopilot 2>&1 || echo "❌ scanner-tm-autopilot не найден"

tm-autopilot-restart:
	@echo "🔄 Перезапуск TM Autopilot..."
	@docker restart scanner-tm-autopilot

tm-autopilot-manual-tune:
	@echo "🔧 Запуск ручного тюнинга TM Autopilot..."
	@python python-worker/tools/tm_policy_tuner.py \
		--input /tmp/closed_7d.ndjson \
		--min-n 30 \
		--lcb-z 1.645 \
		--out-json /tmp/autopilot_report.json \
		--out-md /tmp/autopilot_report.md

entry-policy-autopilot-logs:
	@echo "📋 Логи Entry Policy Autopilot Scheduler:"
	@docker logs -f scanner-entry-policy-autopilot 2>&1 || echo "❌ scanner-entry-policy-autopilot не найден"

safety-guard-logs:
	@echo "📋 Логи Policy Safety Guard (Realtime):"
	@docker logs -f scanner-entry-policy-safety-guard 2>&1 || echo "❌ scanner-entry-policy-safety-guard не найден"

of-reports-status:
	@echo "📊 Статус OF Reports Autopilot:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep of-reports-autopilot || echo "❌ of-reports-autopilot не запущен"

of-reports-logs:
	@echo "📋 Логи OF Reports Autopilot:"
	@docker logs -f of-reports-autopilot 2>&1 || echo "❌ of-reports-autopilot не найден"

recs-status:
	@echo "📊 Статус сервиса одобрения рекомендаций:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep recs-callback-worker || echo "❌ Сервис рекомендаций не запущен"

recs-logs:
	@echo "📋 Логи сервиса одобрения рекомендаций (Ctrl+C для выхода)..."
	@docker logs -f recs-callback-worker 2>&1 || echo "❌ Сервис не найден"

down:
	@echo "🛑 Остановка Python воркеров новостей..."
	@trap '' INT TERM; \
	for pattern in "news_pipeline.feature_store_worker" "news_pipeline.calendar_store_worker" "news_pipeline.analyzer_worker" "news_pipeline.calendar_ingestor_worker"; do \
		pids=$$(pgrep -f "[p]ython.*$$pattern" 2>/dev/null || true); \
		if [ -n "$$pids" ]; then \
			echo "  Остановка процессов: $$pattern (PIDs: $$pids)"; \
			for pid in $$pids; do \
				if kill -0 $$pid 2>/dev/null; then \
					kill -TERM $$pid 2>/dev/null || true; \
				fi; \
			done; \
			sleep 3; \
			remaining=$$(pgrep -f "[p]ython.*$$pattern" 2>/dev/null || true); \
			if [ -n "$$remaining" ]; then \
				echo "  ⚠️ Процессы не завершились, принудительная остановка: $$remaining"; \
				for pid in $$remaining; do \
					if kill -0 $$pid 2>/dev/null; then \
						kill -KILL $$pid 2>/dev/null || true; \
					fi; \
				done; \
				sleep 1; \
			fi; \
		else \
			echo "  ✓ Процессы $$pattern не найдены"; \
		fi; \
	done; \
	trap - INT TERM
	@echo "🛑 Остановка всех сервисов (включая TP Event Listener)..."
	@$(DC) --profile default down --remove-orphans || true
	@$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_TB_LABELER) down --remove-orphans 2>/dev/null || true
	@$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_TB_LABELER_V10_2) down --remove-orphans 2>/dev/null || true
	@echo ""
	@echo "✅ Сервисы и воркеры остановлены"

restart:
	@echo "🔄 Перезапуск всех сервисов..."
	$(DC) restart
	@echo "✅ Сервисы перезапущены"

periodic-report:
	@echo "📬 Принудительная отправка периодического отчета через PeriodicReporter..."
	@PY_BIN="python-worker/.venv/bin/python"; \
	if [ ! -x "$$PY_BIN" ]; then \
		echo "🔧 Создаю виртуальное окружение для python-worker..."; \
		python3 -m venv python-worker/.venv; \
		echo "📦 Устанавливаю зависимости для python-worker..."; \
		python-worker/.venv/bin/python -m pip install --upgrade pip >/dev/null; \
		python-worker/.venv/bin/python -m pip install -r python-worker/requirements.txt >/dev/null; \
	elif ! python-worker/.venv/bin/python -c "import schedule" >/dev/null 2>&1; then \
		echo "📦 Доустанавливаю зависимости для python-worker (schedule)..."; \
		python-worker/.venv/bin/python -m pip install schedule >/dev/null; \
	fi; \
	PY_BIN_ABS="$$(pwd)/$$PY_BIN"; \
	echo "🐍 Используем интерпретатор: $$PY_BIN_ABS"; \
	cd python-worker && REDIS_URL="$${REDIS_URL:-redis://localhost:6380/0}" REDIS_HOST="$${REDIS_HOST:-localhost}" REDIS_PORT="$${REDIS_PORT:-6380}" PYTHONPATH="$$PYTHONPATH:.:.." "$$PY_BIN_ABS" -c 'from services.periodic_reporter import PeriodicReporter; PeriodicReporter().send_periodic_report()'

rebuild:
	@echo "🔨 Полная пересборка (включая TP Event Listener)..."
	$(DC_WITH_TRAILING) --profile default down --remove-orphans
	@echo "🔧 Запуск сборки батчами по 5 сервисов (для снижения нагрузки на BuildKit)..."
	@SERVICES=$$($(DC_WITH_TRAILING) --profile default config --services 2>/dev/null); \
	if [ -z "$$SERVICES" ]; then \
		echo "⚠️ Ошибка получения списка сервисов, попытка собрать все сразу..."; \
		COMPOSE_PARALLEL_LIMIT=2 ./scripts/with-buildkit.sh $(DC_WITH_TRAILING) --profile default build; \
	else \
		echo "$$SERVICES" | xargs -n 5 > .build_batches_rebuild.tmp; \
		while read -r batch; do \
			if [ -z "$$batch" ]; then continue; fi; \
			echo "🛠️ Сборка батча (параллельность 2): $$batch"; \
			COMPOSE_PARALLEL_LIMIT=2 ./scripts/with-buildkit.sh $(DC_WITH_TRAILING) --profile default build $$batch || exit 1; \
		done < .build_batches_rebuild.tmp; \
		rm -f .build_batches_rebuild.tmp; \
	fi
	./scripts/with-buildkit.sh $(DC_WITH_TRAILING) --profile default up -d --force-recreate
	@echo "✅ Пересборка завершена"
	@echo "🎯 Проверьте trailing: make trailing-status"

clean:
	@echo "⚠️⚠️⚠️  ОПАСНАЯ ОПЕРАЦИЯ ⚠️⚠️⚠️"
	@echo "🗑️  Полная очистка (containers + volumes + ВСЕ ДАННЫЕ)"
	@echo ""
	@read -p "Вы ДЕЙСТВИТЕЛЬНО уверены? Введите 'DELETE' для подтверждения: " confirm && \
	if [ "$$confirm" = "DELETE" ]; then \
		echo "🗑️  Удаление всех данных..."; \
		$(DC) down -v --remove-orphans; \
		echo "✅ Очистка завершена"; \
	else \
		echo "❌ Отменено (требуется ввести DELETE)"; \
	fi

bootstrap-baselines:
	@echo "🔍 Checking baseline signals..."
	@if [ ! -s "python-worker/of_reports_baselines/inputs_canary.ndjson" ]; then \
		echo "⚠️  Baseline inputs missing or empty. Bootstrapping from Redis..."; \
		cd python-worker && REDIS_URL=redis://localhost:6379/0 BASELINE_DIR=of_reports_baselines python3 -m tools.bootstrap_regress_baseline || echo "⚠️  Bootstrap failed (non-critical)"; \
	else \
		echo "✅ Baseline signals already exist."; \
	fi

# ═══════════════════════════════════════════════════════════════
# Статус и мониторинг
# ═══════════════════════════════════════════════════════════════

status:
	@echo "📊 Статус всех контейнеров:"
	@$(DC) ps

health:
	@echo "🏥 Проверка здоровья сервисов:"
	@echo ""
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "(scanner-|redis)"

full-status:
	@echo "╔═══════════════════════════════════════════════════════════════╗"
	@echo "║               📊 ПОЛНЫЙ СТАТУС СИСТЕМЫ 📊                    ║"
	@echo "╚═══════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "🐳 Docker контейнеры:"
	@$(DC) ps
	@echo ""
	@echo "🔴 Redis сервисы:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep redis || echo "❌ Redis не запущен"
	@echo ""
	@echo "🚀 Signal сервисы:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "(signal|hub)" || echo "❌ Signal сервисы не запущены"
	@echo ""
	@echo "🌐 Gateway сервисы:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep gateway || echo "❌ Gateway не запущен"
	@echo ""
	@echo "💬 Telegram сервисы:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep telegram || echo "❌ Telegram не запущен"
	@echo ""
	@echo "📈 Использование ресурсов:"
	@docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}" | grep scanner || true
	@echo ""
	@echo "⏰ Таймеры (Docker Loop):"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep timer || echo "❌ Таймеры не запущены"

quick-status:
	@echo "⚡ Быстрая проверка критичных сервисов:"
	@echo ""
	@SERVICES="scanner-redis redis-worker-1 scanner-redis-worker-2 scanner-signal-generator scanner-signal-hub go-gateway scanner-telegram-worker"; \
	for svc in $$SERVICES; do \
		if docker ps --format "{{.Names}}" | grep -q "$$svc"; then \
			echo "✅ $$svc"; \
		else \
			echo "❌ $$svc"; \
		fi; \
	done

# ═══════════════════════════════════════════════════════════════
# Логи по сервисам
# ═══════════════════════════════════════════════════════════════

logs:
	@echo "📋 Все логи в реальном времени..."
	$(DC) logs -f

signal-logs:
	@echo "📋 Логи signal-generator..."
	docker logs -f scanner-signal-generator 2>&1 || echo "❌ scanner-signal-generator не найден"

hub-logs:
	@echo "📋 Логи signal-hub и aggregated-hub..."
	docker logs -f scanner-signal-hub 2>&1 &
	docker logs -f scanner-aggregated-hub 2>&1 || true

gateway-logs:
	@echo "📋 Логи go-gateway..."
	docker logs -f go-gateway 2>&1 || echo "❌ go-gateway не найден"

telegram-logs:
	@echo "📋 Логи telegram-worker..."
	docker logs -f scanner-telegram-worker 2>&1 || echo "❌ telegram-worker не найден"

worker-logs:
	@echo "📋 Логи всех Python-воркеров (shards 1, 1b, 2)..."
	@if [ "$$(docker ps -aq -f name=scanner-python-worker)" ]; then \
		echo "--- (Вывод последних 50 строк первого найденного шарда) ---"; \
		docker logs --tail 50 $$(docker ps -aq -f name=scanner-python-worker | head -n 1) 2>&1; \
		echo "--- (Для просмотра всех логов используйте: make worker-logs-all) ---"; \
	else \
		echo "❌ Python-воркеры не найдены"; \
	fi

worker-logs-all:
	@echo "📋 Стриминг логов всех шардов..."
	docker compose -f docker-compose-python-workers.yml logs -f --tail 100

atr-logs:
	@echo "📋 Логи ATR workers..."
	docker logs -f scanner-atr-worker 2>&1 || docker logs -f scanner-python-worker 2>&1 | grep -i atr || echo "❌ ATR worker не найден"

orderflow-logs:
	@echo "📋 Логи orderflow handler..."
	docker logs -f scanner-python-worker 2>&1 | grep -i orderflow || echo "❌ Orderflow handler не найден"

paper-logs:
	@echo "📋 Логи paper executor..."
	docker logs -f scanner-paper-executor 2>&1 || echo "❌ paper-executor не найден"

# ═══════════════════════════════════════════════════════════════
# Статус по сервисам
# ═══════════════════════════════════════════════════════════════

signal-status:
	@echo "📊 Статус signal-generator:"
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep signal-generator || echo "❌ Не найден"
	@echo ""
	@docker logs scanner-signal-generator --tail 10 2>&1 | grep -E "(ERROR|INFO|Started)" | tail -5 || true

hub-status:
	@echo "📊 Статус signal-hub и aggregated-hub:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "(signal-hub|aggregated-hub)" || echo "❌ Не найдены"
	@echo ""
	@echo "signal-hub последние логи:"
	@docker logs scanner-signal-hub --tail 5 2>&1 | grep -E "(ERROR|INFO)" || true
	@echo ""
	@echo "aggregated-hub последние логи:"
	@docker logs scanner-aggregated-hub --tail 5 2>&1 | grep -E "(ERROR|INFO)" || true

gateway-status:
	@echo "📊 Статус go-gateway:"
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep gateway || echo "❌ Не найден"
	@echo ""
	@echo "Проверка API:"
	@curl -s http://localhost:8090/healthz 2>&1 || echo "❌ API недоступен"

telegram-status:
	@echo "📊 Статус telegram-worker:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep telegram || echo "❌ Не найден"

worker-status:
	@echo "📊 Статус Python-воркеров (шарды)..."
	@docker ps -a --filter "name=scanner-python-worker" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

atr-status:
	@echo "📊 Статус ATR workers:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep -i atr || echo "❌ ATR workers не найдены"

orderflow-status:
	@echo "📊 Статус orderflow handler:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "(orderflow|python-worker)" || echo "❌ Не найден"

tracker-status:
	@echo "📊 Статус Signal Performance Tracker:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep "signal-tracker" || echo "❌ Не запущен"
	@echo ""
	@echo "Последние 30 строк логов:"
	@docker logs --tail=30 scanner-signal-tracker 2>&1 || echo "❌ Логи недоступны"

check-xauusd-services:
	@echo "🔍 Проверка всех 3 сервисов обработки XAUUSD..."
	@bash scripts/check_xauusd_services.sh

check-telegram:
	@echo "🔍 Проверка Telegram интеграции во всех сервисах..."
	@bash scripts/check_telegram_integration.sh

check-redis-streams:
	@echo "🔍 Проверка всех Redis Streams..."
	@bash scripts/check_redis_streams.sh

test-tracker-telegram:
	@echo "📤 Тест отправки статистики в Telegram..."
	@python3 scripts/test_tracker_telegram.py

send-report-now:
	@echo "📊 Формирование и отправка отчета по сигналам СЕЙЧАС..."
	@python3 scripts/send_report_now.py

send-real-report:
	@echo "📊 Отправка РЕАЛЬНОГО отчета через ReportingService..."
	@docker cp test_send_report.py scanner-aggregated-hub:/app/test_send_report.py 2>/dev/null
	@docker exec scanner-aggregated-hub python3 /app/test_send_report.py

test-reporting:
	@echo "🧪 Тестирование системы отчетов..."
	@python3 scripts/test_reporting.py

test-orphan-housekeep:
	@echo "🧪 Тест orphan housekeeping (TradeMonitor)..."
	@python3 -m pytest tests/test_trade_monitor_orphan_housekeep.py -v

logs-reporter:
	@echo "📋 Логи periodic-reporter сервиса..."
	@$(DOCKER_COMPOSE_BIN) logs -f --tail=100 periodic-reporter

restart-reporter:
	@echo "🔄 Перезапуск periodic-reporter..."
	@$(DOCKER_COMPOSE_BIN) restart periodic-reporter
	@echo "✅ Periodic reporter перезапущен"
	@$(DOCKER_COMPOSE_BIN) logs --tail=20 periodic-reporter

tracker-stats:
	@echo "📊 Статистика Signal Performance Tracker..."
	@echo ""
	@echo "=== ОБЩАЯ СТАТИСТИКА ==="
	@docker exec redis-worker-1 redis-cli HGETALL stats:orderflow:XAUUSD:tick | sed 'N;s/\n/ /' | while read key value; do printf "  %-20s %s\n" "$$key:" "$$value"; done
	@echo ""
	@echo "=== ПО ИСТОЧНИКАМ (динамически из Redis) ==="
	@sources=$$(docker exec redis-worker-1 redis-cli SMEMBERS stats:sources:orderflow:XAUUSD:tick 2>/dev/null); \
	if [ -z "$$sources" ]; then \
		echo "  ⚠️ Нет данных (позиции ещё не закрылись)"; \
	else \
		for source in $$sources; do \
			echo ""; \
			trades=$$(docker exec redis-worker-1 redis-cli HGET stats:orderflow:XAUUSD:tick:$$source total_trades 2>/dev/null || echo "0"); \
			wr=$$(docker exec redis-worker-1 redis-cli HGET stats:orderflow:XAUUSD:tick:$$source winrate 2>/dev/null || echo "0"); \
			pnl=$$(docker exec redis-worker-1 redis-cli HGET stats:orderflow:XAUUSD:tick:$$source total_pnl 2>/dev/null || echo "0"); \
			if [ "$$source" = "OrderFlow" ]; then emoji="📊"; \
			elif [ "$$source" = "AggregatedHub-V2" ]; then emoji="🎯"; \
			elif [ "$$source" = "TechnicalAnalysis" ]; then emoji="📈"; \
			else emoji="🔸"; fi; \
			echo "$$emoji $$source:"; \
			echo "  Сделок: $$trades | WinRate: $$wr% | P&L: \$$$$pnl"; \
		done; \
	fi
	@echo ""

tracker-restart:
	@echo "🔄 Перезапуск Signal Performance Tracker..."
	@$(DOCKER_COMPOSE_BIN) restart signal-performance-tracker
	@echo "✅ Signal Tracker перезапущен"
	@sleep 5
	@docker logs scanner-signal-tracker --tail=20

tracker-logs:
	@echo "📋 Логи Signal Performance Tracker..."
	@docker logs -f scanner-signal-tracker

paper-status:
	@echo "📊 Статус paper executor:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep paper || echo "❌ Не найден"

backtest-status:
	@echo "📊 Статус backtest сервисов:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep backtest || echo "❌ Backtest не запущены"

backtest-logs:
	@echo "📋 Логи backtest сервисов..."
	@docker logs -f scanner-backtest 2>&1 || echo "❌ backtest не найден"

trailing-autotune-logs:
	@echo "🤖 Логи Trailing Autotune воркеров (Ctrl+C для выхода)..."
	@echo "Базовый воркер (автонастройка):"
	@docker logs -f scanner-trailing-autotune 2>&1 &
	@sleep 1
	@echo ""
	@echo "24h воркер (скользящее окно):"
	@docker logs -f scanner-trailing-autotune-24h 2>&1 || echo "❌ Один из воркеров не найден"

trailing-autotune-status:
	@echo "🤖 Статус Trailing Autotune воркеров:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep trailing-autotune || echo "❌ Trailing autotune воркеры не запущены"

# ═══════════════════════════════════════════════════════════════
# Redis команды
# ═══════════════════════════════════════════════════════════════

redis-stats:
	@echo "📊 Статистика Redis:"
	@echo ""
	@echo "=== ОСНОВНОЙ REDIS (scanner-redis) ==="
	@docker exec scanner-redis redis-cli INFO memory 2>/dev/null | grep -E "(used_memory_human|maxmemory_human|used_memory_peak_human)" || echo "❌ scanner-redis недоступен"
	@echo ""
	@docker exec scanner-redis redis-cli INFO stats 2>/dev/null | grep -E "(total_connections_received|total_commands_processed|instantaneous_ops_per_sec)" || true
	@echo ""
	@echo "=== КЛЮЧИ ==="
	@docker exec scanner-redis redis-cli DBSIZE 2>/dev/null || echo "❌ Не удалось получить количество ключей"
	@echo ""
	@echo "=== WORKER-1 REDIS ==="
	@docker exec redis-worker-1 redis-cli INFO memory 2>/dev/null | grep -E "(used_memory_human)" || echo "❌ redis-worker-1 недоступен"
	@echo ""
	@echo "=== WORKER-2 REDIS ==="
	@docker exec scanner-redis-worker-2 redis-cli INFO memory 2>/dev/null | grep -E "(used_memory_human)" || echo "❌ redis-worker-2 недоступен"

redis-memory:
	@echo "💾 Мониторинг памяти Redis:"
	@echo ""
	@docker exec scanner-redis redis-cli INFO memory 2>/dev/null || echo "❌ scanner-redis недоступен"

redis-check:
	@echo "🔍 Проверка подключения к Redis:"
	@echo ""
	@echo "scanner-redis:"
	@docker exec scanner-redis redis-cli PING 2>/dev/null && echo "✅ OK" || echo "❌ Недоступен"
	@echo ""
	@echo "redis-worker-1:"
	@docker exec redis-worker-1 redis-cli PING 2>/dev/null && echo "✅ OK" || echo "❌ Недоступен"
	@echo ""
	@echo "redis-worker-2:"
	@docker exec scanner-redis-worker-2 redis-cli PING 2>/dev/null && echo "✅ OK" || echo "❌ Недоступен"

redis-flush:
	@echo "⚠️⚠️⚠️  ОПАСНАЯ ОПЕРАЦИЯ ⚠️⚠️⚠️"
	@echo "🗑️  Очистка всех данных Redis"
	@echo ""
	@read -p "Вы уверены? Введите 'FLUSH' для подтверждения: " confirm && \
	if [ "$$confirm" = "FLUSH" ]; then \
		docker exec scanner-redis redis-cli FLUSHALL && \
		echo "✅ Redis очищен"; \
	else \
		echo "❌ Отменено"; \
	fi

redis-clear-data:
	@echo "🧹 Очистка данных по сигналам, сделкам и volume..."
	@echo ""
	@bash scripts/clear_trades_and_signals.sh --yes
	@echo ""
	@bash scripts/clear_volume_data.sh --yes
	@echo ""
	@echo "✅ Все данные очищены успешно"

optimize-redis:
	@echo "🚀 Оптимизация Redis..."
	@echo ""
	@echo "Применение оптимизаций:"
	@docker exec scanner-redis redis-cli CONFIG SET maxmemory-policy allkeys-lru 2>/dev/null || echo "⚠️ Не удалось установить maxmemory-policy"
	# @docker exec scanner-redis redis-cli CONFIG SET save "" 2>/dev/null || echo "⚠️ Не удалось отключить RDB снапшоты"
	@echo "✅ Оптимизации применены"

backup-redis:
	@echo "💾 Создание backup Redis..."
	@mkdir -p ./backups/redis
	@BACKUP_FILE="./backups/redis/backup_$$(date +%Y%m%d_%H%M%S).rdb"; \
	docker exec scanner-redis redis-cli SAVE 2>/dev/null && \
	docker cp scanner-redis:/data/dump.rdb $$BACKUP_FILE && \
	echo "✅ Backup создан: $$BACKUP_FILE" || echo "❌ Ошибка создания backup"

restore-redis:
	@echo "📥 Восстановление Redis из backup..."
	@ls -lht ./backups/redis/*.rdb 2>/dev/null | head -5 || echo "❌ Backup файлы не найдены"
	@echo ""
	@read -p "Введите путь к файлу для восстановления: " backup_file && \
	if [ -f "$$backup_file" ]; then \
		docker stop scanner-redis && \
		docker cp $$backup_file scanner-redis:/data/dump.rdb && \
		docker start scanner-redis && \
		echo "✅ Данные восстановлены"; \
	else \
		echo "❌ Файл не найден"; \
	fi

# ═══════════════════════════════════════════════════════════════
# Диагностика и исправления
# ═══════════════════════════════════════════════════════════════

diagnose:
	@echo "🔍 Диагностика системы..."
	@echo ""
	@echo "=== Статус контейнеров ==="
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.RunningFor}}" | grep scanner || echo "Нет scanner контейнеров"
	@echo ""
	@echo "=== Использование ресурсов ==="
	@docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}" | grep scanner || echo "Нет активных контейнеров"
	@echo ""
	@echo "=== Проверка Redis ==="
	@make redis-check
	@echo ""
	@echo "=== Последние ошибки ==="
	@docker logs scanner-signal-generator --tail 10 2>&1 | grep -i error || echo "Нет ошибок в signal-generator"

full-system-check:
	@echo "╔═══════════════════════════════════════════════════════════════╗"
	@echo "║          🔍 ПОЛНАЯ ДИАГНОСТИКА СИСТЕМЫ 🔍                    ║"
	@echo "╚═══════════════════════════════════════════════════════════════╝"
	@echo ""
	@make full-status
	@echo ""
	@make redis-stats
	@echo ""
	@make diagnose

emergency-stop:
	@echo "🚨 ЭКСТРЕННАЯ ОСТАНОВКА..."
	@echo ""
	@echo "⚠️  Остановка перегруженных сервисов"
	@read -p "Продолжить? (y/N): " confirm && \
	if [ "$$confirm" = "y" ]; then \
		docker stop scanner-signal-generator scanner-signal-hub scanner-aggregated-hub 2>/dev/null || true; \
		echo "✅ Критичные сервисы остановлены"; \
	fi

emergency-fix:
	@echo "🚨 ЭКСТРЕННОЕ ИСПРАВЛЕНИЕ..."
	@echo ""
	@echo "1. Очистка Redis кэша..."
	@docker exec scanner-redis redis-cli FLUSHALL 2>/dev/null || echo "⚠️ Redis недоступен"
	@echo ""
	@echo "2. Перезапуск критичных сервисов..."
	@docker restart scanner-redis redis-worker-1 scanner-redis-worker-2 2>/dev/null || true
	@sleep 5
	@docker restart scanner-signal-generator scanner-signal-hub go-gateway 2>/dev/null || true
	@echo ""
	@echo "✅ Экстренные исправления применены"
	@echo "📊 Проверьте статус: make quick-status"

performance-fix:
	@echo "🔧 Исправление производительности..."
	@echo ""
	@echo "1. Оптимизация Redis..."
	@make optimize-redis
	@echo ""
	@echo "2. Уменьшение нагрузки на контейнеры..."
	@docker update --memory="512m" scanner-signal-generator 2>/dev/null || true
	@echo ""
	@echo "✅ Исправления применены"

circuit-reset:
	@echo "🔄 Сброс Circuit Breakers..."
	@docker exec scanner-python-worker python -c "import redis; r = redis.Redis(host='scanner-redis', port=6379); r.delete('circuit:*')" 2>/dev/null || echo "⚠️ Не удалось сбросить circuit breakers"
	@echo "✅ Circuit Breakers сброшены"

monitor-resources:
	@echo "📊 Мониторинг ресурсов системы..."
	@echo ""
	@docker stats --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}"

quick-restart:
	@echo "⚡ Быстрый перезапуск критичных сервисов..."
	@docker restart scanner-signal-generator scanner-signal-hub go-gateway
	@echo "✅ Сервисы перезапущены"

safe-restart:
	@echo "🛡️  Безопасный перезапуск с проверками..."
	@echo ""
	@echo "1. Проверка текущего состояния..."
	@make quick-status
	@echo ""
	@echo "2. Остановка сервисов..."
	@docker stop scanner-signal-generator scanner-signal-hub scanner-aggregated-hub 2>/dev/null || true
	@sleep 3
	@echo ""
	@echo "3. Запуск сервисов..."
	@docker start scanner-redis redis-worker-1 scanner-redis-worker-2 2>/dev/null || true
	@sleep 5
	@docker start scanner-signal-generator scanner-signal-hub go-gateway 2>/dev/null || true
	@sleep 5
	@echo ""
	@echo "4. Проверка после перезапуска..."
	@make quick-status
	@echo "✅ Безопасный перезапуск завершен"

# ═══════════════════════════════════════════════════════════════
# Очистка и обслуживание
# ═══════════════════════════════════════════════════════════════

docker-cleanup:
	@echo "🧹 Очистка неиспользуемых Docker ресурсов..."
	@docker system prune -f
	@echo "✅ Очистка завершена"

volume-list:
	@echo "💾 Список всех volumes:"
	@docker volume ls --filter "name=scanner_infra"

volume-inspect:
	@echo "🔍 Детальная информация о volumes:"
	@docker volume inspect scanner-redis-data 2>/dev/null || echo "❌ scanner-redis-data не найден"

# ═══════════════════════════════════════════════════════════════
# Тестирование
# ═══════════════════════════════════════════════════════════════

api-test:
	@echo "🧪 Тестирование всех API..."
	@echo ""
	@echo "=== GO-GATEWAY ==="
	@curl -s http://localhost:8090/healthz || echo "❌ Gateway недоступен"
	@echo ""

gateway-test:
	@echo "🧪 Тест go-gateway API..."
	@echo ""
	@echo "GET /healthz:"
	@curl -s http://localhost:8090/healthz || echo "❌ Недоступен"
	@echo ""

hub-test:
	@echo "🧪 Тест signal-hub..."
	@docker logs scanner-signal-hub --tail 20 2>&1 | grep -E "(Started|Running|Error)" || echo "❌ Нет логов"

signal-test:
	@echo "🧪 Тест signal-generator..."
	@docker logs scanner-signal-generator --tail 20 2>&1 | grep -E "(Started|Generated|Error)" || echo "❌ Нет логов"

notification-test:
	@echo "🧪 Тест telegram notifications..."
	@docker logs scanner-telegram-worker --tail 20 2>&1 | grep -E "(Sent|Error|Message)" || echo "❌ Нет логов"

seed-test:
	@echo "🧪 Тест заполнения данных..."
	@echo "⚠️  Не реализовано"

# ═══════════════════════════════════════════════════════════════
# Дополнительные утилиты
# ═══════════════════════════════════════════════════════════════

service-check:
	@echo "🔍 Проверка работы всех сервисов..."
	@echo ""
	@make redis-check
	@echo ""
	@echo "=== Signal Services ==="
	@docker ps --format "{{.Names}}" | grep signal && echo "✅ Signal services running" || echo "❌ Signal services down"
	@echo ""
	@echo "=== Gateway ==="
	@docker ps --format "{{.Names}}" | grep gateway && echo "✅ Gateway running" || echo "❌ Gateway down"

db-check:
	@echo "🔍 Проверка подключения к БД..."
	@echo "⚠️  Не используется (система без БД)"

network-test:
	@echo "🌐 Проверка Docker сетей..."
	@for NET in scanner-infra scanner-core scanner-timers scanner-ops; do \
		COUNT=$$(docker network inspect $$NET 2>/dev/null | grep -c '"Name"' || echo 0); \
		echo "  $$NET: $$COUNT containers"; \
	done

export-signals:
	@echo "📊 Экспорт сигналов из Redis..."
	@docker exec scanner-redis redis-cli KEYS "signals:*" 2>/dev/null || echo "❌ Не удалось получить ключи"

check-signals:
	@echo "🔍 Проверка качества сигналов..."
	@docker exec scanner-redis redis-cli KEYS "signals:*" 2>/dev/null | wc -l || echo "0"

# ═══════════════════════════════════════════════════════════════════════════
# MULTI-SYMBOL ORDERFLOW COMMANDS (новая архитектура)
# ═══════════════════════════════════════════════════════════════════════════

multi-up:
	@echo "🚀 Запуск Multi-Symbol OrderFlow сервиса..."
	@$(DOCKER_COMPOSE_BIN) up -d multi-symbol-orderflow
	@echo "✅ Сервис запущен"
	@echo "📊 Мониторинг: make multi-monitor"

multi-down:
	@echo "⏹️  Остановка Multi-Symbol OrderFlow сервиса..."
	@$(DOCKER_COMPOSE_BIN) stop multi-symbol-orderflow
	@echo "✅ Сервис остановлен"

multi-restart:
	@echo "🔄 Перезапуск Multi-Symbol OrderFlow сервиса..."
	@$(DOCKER_COMPOSE_BIN) restart multi-symbol-orderflow
	@echo "✅ Сервис перезапущен"

multi-logs:
	@echo "📋 Логи Multi-Symbol OrderFlow (Ctrl+C для выхода)..."
	@$(DOCKER_COMPOSE_BIN) logs -f multi-symbol-orderflow

multi-status:
	@echo "📊 Статус Multi-Symbol OrderFlow сервиса:"
	@echo "═══════════════════════════════════════════════════════════════"
	@docker ps --filter name=multi-orderflow --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" || echo "❌ Сервис не запущен"
	@echo ""
	@echo "Health:"
	@docker inspect scanner-multi-orderflow --format='{{.State.Health.Status}}' 2>/dev/null || echo "none"
	@echo ""
	@echo "Restarts:"
	@docker inspect scanner-multi-orderflow --format='{{.RestartCount}}' 2>/dev/null || echo "0"

multi-monitor:
	@echo "📊 Live мониторинг Multi-Symbol OrderFlow..."
	@./scripts/monitor_multi_symbol.sh

multi-stats:
	@echo "📊 Статистика ресурсов Multi-Symbol OrderFlow:"
	@docker stats scanner-multi-orderflow --no-stream

multi-errors:
	@echo "❌ Последние ошибки (last 10):"
	@docker logs scanner-multi-orderflow 2>&1 | grep "❌" | tail -10

multi-signals:
	@echo "📤 Последние сигналы (last 10):"
	@docker logs scanner-multi-orderflow 2>&1 | grep "Сигнал опубликован" | tail -10

# ═══════════════════════════════════════════════════════════════════════════
# DYNAMIC SYMBOLS MANAGEMENT (hot-reload без перезапуска)
# ═══════════════════════════════════════════════════════════════════════════

symbol-add:
	@if [ -z "$(SYMBOL)" ]; then \
		echo "❌ Error: SYMBOL not specified"; \
		echo "Usage: make symbol-add SYMBOL=BTCUSD"; \
		exit 1; \
	fi
	@echo "➕ Добавление символа: $(SYMBOL)"
	@./scripts/manage_symbols.sh add $(SYMBOL)

symbol-remove:
	@if [ -z "$(SYMBOL)" ]; then \
		echo "❌ Error: SYMBOL not specified"; \
		echo "Usage: make symbol-remove SYMBOL=BTCUSD"; \
		exit 1; \
	fi
	@echo "➖ Удаление символа: $(SYMBOL)"
	@./scripts/manage_symbols.sh remove $(SYMBOL)

symbol-set:
	@if [ -z "$(SYMBOLS)" ]; then \
		echo "❌ Error: SYMBOLS not specified"; \
		echo "Usage: make symbol-set SYMBOLS='XAUUSD BTCUSD ETHUSD'"; \
		exit 1; \
	fi
	@echo "📋 Установка списка символов: $(SYMBOLS)"
	@./scripts/manage_symbols.sh set $(SYMBOLS)

symbol-list:
	@echo "📊 Активные символы:"
	@./scripts/manage_symbols.sh list

symbol-status:
	@echo "📊 Статус handlers по символам:"
	@./scripts/manage_symbols.sh status

# Примеры использования:
#   make symbol-add SYMBOL=BTCUSD           # Добавить Bitcoin
#   make symbol-remove SYMBOL=ETHUSD        # Удалить Ethereum  
#   make symbol-set SYMBOLS="XAUUSD BTCUSD" # Установить список
#   make symbol-list                        # Показать активные
#   make symbol-status                      # Статус handlers

# ═══════════════════════════════════════════════════════════════════════════
# A/B TESTING & MIGRATION COMMANDS
# ═══════════════════════════════════════════════════════════════════════════

ab-start:
	@echo "🧪 Запуск A/B тестирования (старый + новый handler параллельно)..."
	@echo ""
	@echo "Шаг 1: Pre-flight проверки..."
	@./scripts/migration_plan.sh step1
	@echo ""
	@echo "Шаг 2: Включение старого XAU handler в python-worker..."
	@echo "   Установка XAU_HANDLER_ENABLED=true в docker-compose.yml"
	@echo "   (Временно для A/B тестирования)"
	@echo ""
	@echo "⚠️  ВАЖНО: Для A/B тестирования нужно вручную:"
	@echo "   1. Изменить в docker-compose.yml: XAU_HANDLER_ENABLED=true (в python-worker)"
	@echo "   2. $(DOCKER_COMPOSE_BIN) restart python-worker"
	@echo "   3. $(DOCKER_COMPOSE_BIN) up -d multi-symbol-orderflow"
	@echo ""
	@echo "ИЛИ используйте автоматический скрипт:"
	@echo "   ./scripts/migration_plan.sh all"

ab-check:
	@echo "📊 Проверка результатов A/B тестирования..."
	@python scripts/ab_testing_compare.py --duration 24 --output ab_test_report.json
	@echo ""
	@echo "📝 Отчет сохранен: ab_test_report.json"

ab-compare:
	@echo "📊 Быстрое сравнение (last 1 hour):"
	@echo ""
	@echo "Старый handler:"
	@docker logs scanner-python-worker --since 1h 2>&1 | grep -c "Сигнал опубликован" || echo "0"
	@echo ""
	@echo "Новый handler:"
	@docker logs scanner-multi-orderflow --since 1h 2>&1 | grep -c "Сигнал опубликован" || echo "0"

migrate-step1:
	@./scripts/migration_plan.sh step1

migrate-step2:
	@./scripts/migration_plan.sh step2

migrate-step3:
	@./scripts/migration_plan.sh step3

migrate-step4:
	@./scripts/migration_plan.sh step4

migrate-step5:
	@./scripts/migration_plan.sh step5

migrate-rollback:
	@echo "🔙 Rollback к старому handler..."
	@echo ""
	@echo "Остановка нового сервиса..."
	@$(DOCKER_COMPOSE_BIN) stop multi-symbol-orderflow
	@echo ""
	@echo "Запуск старого сервиса..."
	@$(DOCKER_COMPOSE_BIN) --profile legacy up -d python-worker
	@sleep 5
	@echo ""
	@if docker ps | grep -q "python-worker"; then \
		echo "✅ Rollback успешен - python-worker запущен"; \
		docker ps | grep python-worker; \
	else \
		echo "❌ Ошибка rollback - проверьте docker ps"; \
	fi

migrate-all:
	@echo "🚀 Полная автоматическая миграция (steps 1-2)..."
	@./scripts/migration_plan.sh all

# ═══════════════════════════════════════════════════════════════════════════
# TESTING COMMANDS
# ═══════════════════════════════════════════════════════════════════════════

test-unit:
	@echo "🧪 Запуск unit тестов..."
	@cd python-worker && pytest tests/test_instrument_config.py tests/test_unified_signal_formatter.py tests/test_handler_factory.py -v

test-all:
	@echo "🧪 Запуск всех тестов..."
	@cd python-worker && pytest tests/ -v

test-coverage:
	@echo "📊 Тестовое покрытие..."
	@cd python-worker && pytest tests/ --cov=core --cov=handlers --cov-report=term-missing

test-contract:
	@echo "🛡️  Запуск contract-тестов с жесткими проверками..."
	@echo "   STRICT_TRADEABLE_CONTRACTS=1"
	@cd python-worker && STRICT_TRADEABLE_CONTRACTS=1 pytest -q tests/test_contract_* --tb=short 2>/dev/null || (echo "⚠️  Некоторые тесты требуют Hypothesis (pip install hypothesis)" && echo "Запуск только базовых contract-тестов..." && STRICT_TRADEABLE_CONTRACTS=1 pytest -q tests/test_contract_outboxwriter_meta_merge.py --tb=short)
	@echo "✅ Contract-тесты завершены"

test-ofc-contract:
	@echo "🛡️  Запуск OFC runtime_snapshot contract теста..."
	@cd python-worker && pytest tests/test_runtime_snapshot_contract.py -v
	@echo "✅ OFC contract тест завершен"

# ═══════════════════════════════════════════════════════════════════════════
# TELEGRAM CHANNELS MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════

.PHONY: add-telegram-channel add-telegram-channels remove-telegram-channel list-telegram-channels check-telegram-channels clear-telegram-channels restart-telegram-worker

add-telegram-channel:
	@if [ -z "$(CHANNEL)" ]; then \
		echo "❌ Ошибка: укажите CHANNEL=название_канала"; \
		echo "Пример: make add-telegram-channel CHANNEL=crypto_signals"; \
		exit 1; \
	fi
	@echo "➕ Добавление Telegram канала: @$(CHANNEL)"
	@redis-cli SADD telegram:channels:usernames "$(CHANNEL)" > /dev/null
	@echo "✅ Канал @$(CHANNEL) добавлен"
	@make list-telegram-channels

add-telegram-channels:
	@if [ -z "$(CHANNELS)" ]; then \
		echo "❌ Ошибка: укажите CHANNELS=канал1,канал2,канал3"; \
		echo "Пример: make add-telegram-channels CHANNELS=\"crypto_signals,forex_pro,btc_analysis\""; \
		exit 1; \
	fi
	@echo "➕ Добавление нескольких Telegram каналов..."
	@IFS=','; for channel in $(CHANNELS); do \
		redis-cli SADD telegram:channels:usernames "$$channel" > /dev/null; \
		echo "  ✅ Добавлен: @$$channel"; \
	done
	@echo ""
	@make list-telegram-channels

remove-telegram-channel:
	@if [ -z "$(CHANNEL)" ]; then \
		echo "❌ Ошибка: укажите CHANNEL=название_канала"; \
		echo "Пример: make remove-telegram-channel CHANNEL=old_channel"; \
		exit 1; \
	fi
	@echo "➖ Удаление Telegram канала: @$(CHANNEL)"
	@redis-cli SREM telegram:channels:usernames "$(CHANNEL)" > /dev/null
	@echo "✅ Канал @$(CHANNEL) удалён"
	@make list-telegram-channels

list-telegram-channels:
	@echo ""
	@echo "📋 Активные Telegram каналы:"
	@echo "══════════════════════════════════════════════"
	@COUNT=$$(redis-cli SCARD telegram:channels:usernames 2>/dev/null || echo "0"); \
	if [ "$$COUNT" -eq "0" ]; then \
		echo "⚠️  Нет настроенных каналов"; \
		echo ""; \
		echo "Добавьте каналы с помощью:"; \
		echo "  make add-telegram-channel CHANNEL=my_channel"; \
		echo "  make add-telegram-channels CHANNELS=\"channel1,channel2\""; \
	else \
		redis-cli SMEMBERS telegram:channels:usernames | sort | while read channel; do \
			echo "  📺 @$$channel"; \
		done; \
		echo ""; \
		echo "Всего каналов: $$COUNT"; \
	fi
	@echo "══════════════════════════════════════════════"
	@echo ""

check-telegram-channels:
	@echo "🔍 Проверка конфигурации Telegram каналов..."
	@echo ""
	@COUNT=$$(redis-cli SCARD telegram:channels:usernames 2>/dev/null || echo "0"); \
	if [ "$$COUNT" -eq "0" ]; then \
		echo "❌ ПРОБЛЕМА: Нет настроенных каналов!"; \
		echo ""; \
		echo "Telegram Worker не будет парсить сигналы без каналов."; \
		echo ""; \
		echo "Добавьте каналы:"; \
		echo "  make add-telegram-channel CHANNEL=crypto_signals"; \
		echo "  make add-telegram-channels CHANNELS=\"channel1,channel2,channel3\""; \
		echo ""; \
		exit 1; \
	else \
		echo "✅ Найдено каналов: $$COUNT"; \
		echo ""; \
		make list-telegram-channels; \
		echo ""; \
		echo "Проверка Telegram Worker..."; \
		if docker ps | grep -q scanner-telegram-worker; then \
			echo "✅ Telegram Worker запущен"; \
		else \
			echo "⚠️  Telegram Worker НЕ запущен"; \
			echo "Запустите: make restart-telegram-worker"; \
		fi; \
	fi

clear-telegram-channels:
	@echo "⚠️  ВНИМАНИЕ: Это удалит ВСЕ Telegram каналы!"
	@echo ""
	@read -p "Вы уверены? (yes/no): " confirm; \
	if [ "$$confirm" != "yes" ]; then \
		echo "Отменено"; \
		exit 0; \
	fi
	@redis-cli DEL telegram:channels:usernames > /dev/null
	@echo "✅ Все каналы удалены"

restart-telegram-worker:
	@echo "🔄 Перезапуск Telegram Worker..."
	@$(DOCKER_COMPOSE_BIN) restart scanner-telegram-worker 2>/dev/null || docker restart scanner-telegram-worker 2>/dev/null || echo "⚠️  Worker не найден"
	@echo "✅ Telegram Worker перезапущен"
	@sleep 3
	@echo ""
	@echo "📋 Последние логи:"
	@echo "══════════════════════════════════════════════"
	@docker logs scanner-telegram-worker --tail 20 2>/dev/null || echo "⚠️  Логи недоступны"

# ═══════════════════════════════════════════════════════════════════════════
# DOCKER BUILD FIXES
# ═══════════════════════════════════════════════════════════════════════════

.PHONY: build-legacy rebuild-legacy up-legacy safe-rebuild clean-build-cache

build-legacy:
	@echo "🔨 Сборка с legacy builder (без buildkit)..."
	@export DOCKER_BUILDKIT=0 && export COMPOSE_DOCKER_CLI_BUILD=0 && $(DOCKER_COMPOSE_BIN) build --progress=plain

rebuild-legacy:
	@echo "🔨 Полная пересборка с legacy builder..."
	@$(DOCKER_COMPOSE_BIN) down
	@export DOCKER_BUILDKIT=0 && export COMPOSE_DOCKER_CLI_BUILD=0 && $(DOCKER_COMPOSE_BIN) build --no-cache --progress=plain
	@$(DOCKER_COMPOSE_BIN) up -d
	@echo ""
	@echo "✅ Система пересобрана и запущена"
	@make status

up-legacy:
	@echo "🚀 Запуск с legacy builder..."
	@export DOCKER_BUILDKIT=0 && export COMPOSE_DOCKER_CLI_BUILD=0 && $(DOCKER_COMPOSE_BIN) up -d

safe-rebuild:
	@echo "🛡️ Безопасная пересборка через скрипт..."
	@./safe_rebuild.sh

clean-build-cache:
	@echo "🧹 Очистка Docker build cache..."
	@docker builder prune -af
	@echo "✅ Build cache очищен"

# ═══════════════════════════════════════════════════════════════════════════
# REDIS TICKS MANAGEMENT - Dedicated instance for tick data
# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════
# PEL Cleanup: Remove zombie Redis Stream consumers (WorkerLagP99High prevention)
# ═══════════════════════════════════════════════════════════════

.PHONY: pel-cleanup pel-cleanup-dry

pel-cleanup:
	@echo "🧹 PEL Cleanup: удаление zombie consumers из Redis Stream групп..."
	@echo "   Предотвращает WorkerLagP99High после make up (lag poisoning от старых PEL)"
	@bash scripts/pel_cleanup.sh

pel-cleanup-dry:
	@echo "🔍 PEL Cleanup (DRY-RUN): показывает zombie consumers без удаления..."
	@bash scripts/pel_cleanup.sh --dry-run --verbose

.PHONY: ticks-status ticks-logs ticks-cli ticks-info ticks-groups ticks-streams ticks-stats ticks-consumers ticks-memory ticks-trim ticks-test

ticks-status:
	@echo "╔═══════════════════════════════════════════════════════════════╗"
	@echo "║           📊 Redis Ticks Instance Status 📊                  ║"
	@echo "╚═══════════════════════════════════════════════════════════════╝"
	@echo ""
	@docker ps --filter "name=scanner-redis-ticks" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" || echo "⚠️  redis-ticks не запущен"

ticks-logs:
	@echo "📋 Redis Ticks logs (Ctrl+C для выхода)..."
	@docker logs -f scanner-redis-ticks

ticks-cli:
	@echo "🔧 Подключение к redis-ticks CLI..."
	@docker exec -it scanner-redis-ticks redis-cli

ticks-info:
	@echo "╔═══════════════════════════════════════════════════════════════╗"
	@echo "║           📊 Redis Ticks Information 📊                      ║"
	@echo "╚═══════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "📊 General Info:"
	@docker exec scanner-redis-ticks redis-cli INFO server | grep -E "redis_version|uptime_in_seconds|tcp_port"
	@echo ""
	@echo "💾 Memory Stats:"
	@docker exec scanner-redis-ticks redis-cli INFO memory | grep -E "used_memory_human|maxmemory_human|used_memory_peak_human"
	@echo ""
	@echo "📈 Stats:"
	@docker exec scanner-redis-ticks redis-cli INFO stats | grep -E "total_connections_received|total_commands_processed|instantaneous_ops_per_sec"
	@echo ""
	@echo "📊 Keyspace:"
	@docker exec scanner-redis-ticks redis-cli INFO keyspace

TICKS_REDIS_CONTAINER ?= redis-worker-1
TICKS_REDIS_CLI = docker exec $(TICKS_REDIS_CONTAINER) redis-cli

ticks-streams:
	@echo "╔═══════════════════════════════════════════════════════════════╗"
	@echo "║           📊 Redis Ticks Streams 📊                          ║"
	@echo "╚═══════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "📊 Available streams:"
	@$(TICKS_REDIS_CLI) --scan --pattern "stream:tick_*" || echo "No tick streams found"
	@$(TICKS_REDIS_CLI) --scan --pattern "stream:book_*" || echo "No book streams found"
	@echo ""
	@echo "📊 Stream lengths:"
	@for stream in $$($(TICKS_REDIS_CLI) --scan --pattern "stream:tick_*" 2>/dev/null); do \
		length=$$($(TICKS_REDIS_CLI) XLEN $$stream 2>/dev/null); \
		echo "  $$stream: $$length messages"; \
	done

ticks-groups:
	@echo "╔═══════════════════════════════════════════════════════════════╗"
	@echo "║           📊 Redis Ticks Consumer Groups 📊                  ║"
	@echo "╚═══════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "Finding all consumer groups..."
	@for stream in $$($(TICKS_REDIS_CLI) --scan --pattern "stream:tick_*" 2>/dev/null); do \
		echo ""; \
		echo "📊 Stream: $$stream"; \
		$(TICKS_REDIS_CLI) XINFO GROUPS $$stream 2>/dev/null | grep -E "name|consumers|pending|last-delivered-id" || echo "  No consumer groups"; \
	done

ticks-consumers:
	@echo "╔═══════════════════════════════════════════════════════════════╗"
	@echo "║           📊 Active Consumers per Group 📊                   ║"
	@echo "╚═══════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "Checking active consumers..."
	@for stream in $$($(TICKS_REDIS_CLI) --scan --pattern "stream:tick_*" 2>/dev/null); do \
		echo ""; \
		echo "📊 Stream: $$stream"; \
		for group in $$($(TICKS_REDIS_CLI) XINFO GROUPS $$stream 2>/dev/null | grep "name" | awk '{print $$2}'); do \
			echo "  Consumer Group: $$group"; \
			$(TICKS_REDIS_CLI) XINFO CONSUMERS $$stream $$group 2>/dev/null | grep -E "name|pending|idle" || echo "    No active consumers"; \
		done; \
	done

ticks-stats:
	@echo "╔═══════════════════════════════════════════════════════════════╗"
	@echo "║           📊 Redis Ticks Statistics 📊                       ║"
	@echo "╚═══════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "📊 Overall Statistics:"
	@$(TICKS_REDIS_CLI) INFO stats
	@echo ""
	@echo "📊 Persistence:"
	@$(TICKS_REDIS_CLI) INFO persistence

ticks-memory:
	@echo "╔═══════════════════════════════════════════════════════════════╗"
	@echo "║           💾 Redis Ticks Memory Usage 💾                     ║"
	@echo "╚═══════════════════════════════════════════════════════════════╝"
	@echo ""
	@$(TICKS_REDIS_CLI) MEMORY STATS

ticks-trim:
	@echo "╔═══════════════════════════════════════════════════════════════╗"
	@echo "║           🧹 Trimming Old Ticks 🧹                           ║"
	@echo "╚═══════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "⚠️  This will trim tick streams to 10000 messages"
	@echo "Press Ctrl+C to cancel, or Enter to continue..."
	@read dummy
	@for stream in $$($(TICKS_REDIS_CLI) --scan --pattern "stream:tick_*" 2>/dev/null); do \
		echo "🧹 Trimming $$stream..."; \
		$(TICKS_REDIS_CLI) XTRIM $$stream MAXLEN ~ 10000; \
		echo "✅ $$stream trimmed"; \
	done
	@echo ""
	@echo "✅ All tick streams trimmed"

ticks-test:
	@echo "╔═══════════════════════════════════════════════════════════════╗"
	@echo "║           🧪 Testing Redis Ticks Connection 🧪               ║"
	@echo "╚═══════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "1. Testing connection..."
	@$(TICKS_REDIS_CLI) PING
	@echo ""
	@echo "2. Testing write (adding test tick)..."
	@$(TICKS_REDIS_CLI) XADD test:tick_stream '*' ts "$(shell date +%s%3N)" bid 3955.50 ask 3955.60
	@echo ""
	@echo "3. Testing read..."
	@$(TICKS_REDIS_CLI) XREVRANGE test:tick_stream + - COUNT 1
	@echo ""
	@echo "4. Cleanup test stream..."
	@$(TICKS_REDIS_CLI) DEL test:tick_stream
	@echo ""
	@echo "✅ Redis ticks connection test complete"

ticks-help:
	@echo "╔═══════════════════════════════════════════════════════════════╗"
	@echo "║           📚 Redis Ticks Commands Help 📚                    ║"
	@echo "╚═══════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "Available commands:"
	@echo "  make ticks-status     - Show redis-ticks container status"
	@echo "  make ticks-logs       - View redis-ticks logs"
	@echo "  make ticks-cli        - Connect to redis-ticks CLI"
	@echo "  make ticks-info       - Show general redis-ticks information"
	@echo "  make ticks-streams    - List all tick streams and lengths"
	@echo "  make ticks-groups     - Show all consumer groups"
	@echo "  make ticks-consumers  - Show active consumers per group"
	@echo "  make ticks-stats      - Show detailed statistics"
	@echo "  make ticks-memory     - Show memory usage details"
	@echo "  make ticks-trim       - Trim old ticks (cleanup)"
	@echo "  make ticks-test       - Test redis-ticks connection"
	@echo ""
	@echo "ℹ️  Current target container: $(TICKS_REDIS_CONTAINER) (override via TICKS_REDIS_CONTAINER=...)."
	@echo "For more info, see: TICKS_CONSUMER_GROUP_GUIDE.md"

# ═══════════════════════════════════════════════════════════════════════════
# TP1 TRAILING SYSTEM COMMANDS
# ═══════════════════════════════════════════════════════════════════════════

.PHONY: trailing-status trailing-logs trailing-stats trailing-test trailing-profiles trailing-help trailing-start trailing-stop trailing-restart trailing-health

trailing-status:
	@echo "🎯 Статус TP Event Listener:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep tp-event-listener || echo "❌ TP Event Listener не запущен"

trailing-logs:
	@echo "📋 Логи TP Event Listener (Ctrl+C для выхода)..."
	@docker logs -f scanner-tp-event-listener

trailing-stats:
	@echo "📊 Статистика TP1 Trailing System:"
	@echo ""
	@docker exec scanner-tp-event-listener python -c "\
from services.tp_event_listener import TPEventListener; \
listener = TPEventListener(); \
listener._log_stats()" 2>/dev/null || echo "❌ Listener недоступен"

trailing-test:
	@echo "🧪 Интеграционный тест TP1 Trailing System..."
	@echo ""
	@make -f Makefile.trailing integration-test

trailing-profiles:
	@echo "📋 Доступные профили трейлинга:"
	@echo ""
	@docker exec scanner-tp-event-listener python -c "\
from services.trailing_profiles import TrailingProfilesRegistry; \
registry = TrailingProfilesRegistry(); \
for name in registry.list_names(): \
    profile = registry.get(name); \
    print(f'{name:20} mode={profile.mode:6} atr_mult={profile.atr_mult:.2f} - {profile.comment}')" 2>/dev/null || echo "❌ Listener недоступен"

trailing-health:
	@echo "🏥 Health check TP Event Listener:"
	@echo ""
	@docker exec scanner-tp-event-listener python -c "\
from services.tp_event_listener import TPEventListener; \
import json; \
listener = TPEventListener(); \
health = listener.health_check(); \
print(json.dumps(health, indent=2))" 2>/dev/null || echo "❌ Listener недоступен"

trailing-start:
	@echo "🚀 Запуск TP Event Listener..."
	@$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TRAILING) up -d tp-event-listener
	@echo "✅ TP Event Listener запущен"
	@sleep 3
	@make trailing-status

trailing-stop:
	@echo "🛑 Остановка TP Event Listener..."
	@$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TRAILING) stop tp-event-listener
	@echo "✅ TP Event Listener остановлен"

trailing-restart:
	@echo "🔄 Перезапуск TP Event Listener..."
	@$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TRAILING) restart tp-event-listener
	@echo "✅ TP Event Listener перезапущен"
	@sleep 3
	@make trailing-logs --tail 20

trailing-help:
	@echo "╔═══════════════════════════════════════════════════════════════╗"
	@echo "║           🎯 TP1 Trailing System Help 🎯                     ║"
	@echo "╚═══════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "Система автоматического трейлинга после TP1"
	@echo ""
	@echo "Основные команды:"
	@echo "  make trailing-status     - Статус сервиса"
	@echo "  make trailing-logs       - Логи в реальном времени"
	@echo "  make trailing-stats      - Статистика работы"
	@echo "  make trailing-test       - Интеграционный тест"
	@echo "  make trailing-profiles   - Доступные профили"
	@echo "  make trailing-health     - Health check"
	@echo ""
	@echo "Управление сервисом:"
	@echo "  make trailing-start      - Запустить listener"
	@echo "  make trailing-stop       - Остановить listener"
	@echo "  make trailing-restart    - Перезапустить listener"
	@echo ""
	@echo "Документация:"
	@echo "  📖 Quick Start:  docs/tp1-trailing/QUICKSTART.md"
	@echo "  📖 Full Docs:    docs/tp1-trailing/TP1_TRAILING_SYSTEM.md"
	@echo "  📖 Deployment:   docs/tp1-trailing/DEPLOYMENT_GUIDE.md"
	@echo ""
	@echo "Дополнительные команды (Makefile.trailing):"
	@echo "  make -f Makefile.trailing help"
	@echo ""

close-wait-drainer-status:
	@echo "📊 Статус Close Wait Drainer:"
	@$(DC_TIMERS) ps close-wait-drainer close-wait-drainer-exporter
	@echo ""
	@echo "Метрики из Redis:"
	@docker exec redis-worker-1 redis-cli HGETALL metrics:close_wait_drainer

close-wait-drainer-logs:
	@echo "📋 Логи Close Wait Drainer (последние 50 строк):"
	@$(DC_TIMERS) logs --tail=50 -f close-wait-drainer close-wait-drainer-exporter

# ═══════════════════════════════════════════════════════════════════════════
# MT5 EVENT EXECUTOR COMMANDS
# ═══════════════════════════════════════════════════════════════════════════

# .PHONY: mt5-executor-start mt5-executor-stop mt5-executor-status mt5-executor-logs mt5-executor-test mt5-executor-health mt5-executor-stats

# mt5-executor-start:
# 	@echo "🚀 Запуск MT5 Event Executor..."
# 	@$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_MT5_EXECUTOR) up -d mt5-event-executor
# 	@echo "✅ MT5 Event Executor запущен"
# 	@sleep 3
# 	@make mt5-executor-status

# mt5-executor-stop:
# 	@echo "🛑 Остановка MT5 Event Executor..."
# 	@$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_MT5_EXECUTOR) stop mt5-event-executor
# 	@echo "✅ MT5 Event Executor остановлен"

# mt5-executor-status:
# 	@echo "📡 Статус MT5 Event Executor:"
# 	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep mt5-event-executor || echo "❌ MT5 Event Executor не запущен"

# mt5-executor-logs:
# 	@echo "📋 Логи MT5 Event Executor (Ctrl+C для выхода)..."
# 	@docker logs -f scanner-mt5-event-executor

# mt5-executor-test:
# 	@echo "🧪 Тест MT5 Event Executor endpoint..."
# 	@echo ""
# 	@echo "Health check:"
# 	@curl -s http://localhost:8091/health | jq . || echo "❌ Endpoint недоступен"
# 	@echo ""
# 	@echo "Stats:"
# 	@curl -s http://localhost:8091/stats | jq . || echo "❌ Stats недоступны"

# mt5-executor-health:
# 	@echo "🏥 Health check MT5 Event Executor:"
# 	@curl -s http://localhost:8091/health | jq .

# mt5-executor-stats:
# 	@echo "📊 Статистика MT5 Event Executor:"
# 	@curl -s http://localhost:8091/stats | jq .

# ═══════════════════════════════════════════════════════════════════════════



# ═══════════════════════════════════════════════════════════════════════════
# TRAILING AUTOTUNE COMMANDS
# ═══════════════════════════════════════════════════════════════════════════

.PHONY: trailing-autotune-start trailing-autotune-stop trailing-autotune-status trailing-autotune-logs

trailing-autotune-start:
	@echo "🤖 Запуск Trailing Autotune воркеров..."
	@echo "Запуск базового воркера (автонастройка трейлинга):"
	@$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) up -d scanner-trailing-autotune
	@echo "Запуск 24h воркера (скользящее окно):"
	@$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) up -d scanner-trailing-autotune-24h
	@echo "✅ Trailing Autotune воркеры запущены"
	@sleep 3
	@make trailing-autotune-status

trailing-autotune-stop:
	@echo "🛑 Остановка Trailing Autotune воркеров..."
	@$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) stop scanner-trailing-autotune scanner-trailing-autotune-24h
	@echo "✅ Trailing Autotune воркеры остановлены"

trailing-autotune-restart:
	@echo "🔄 Перезапуск Trailing Autotune воркеров..."
	@$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) restart scanner-trailing-autotune scanner-trailing-autotune-24h
	@echo "✅ Trailing Autotune воркеры перезапущены"
	@sleep 3
	@make trailing-autotune-status

# ═══════════════════════════════════════════════════════════════════════════
# LOCAL CALIBRATION COMMANDS
# ═══════════════════════════════════════════════════════════════════════════

.PHONY: calibration-run calibration-check calibration-help calibration-auto-start calibration-auto-stop calibration-auto-status calibration-auto-enable calibration-auto-disable relcal-analyze relcal-export relcal-plot relcal-help

calibration-run:
	@echo "🎯 Запуск настройки локальной калибровки (внутри контейнера)..."
	@$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) run --rm calibration-scheduler /app/python-worker/scripts/setup_and_run_calibration.sh

calibration-check:
	@echo "📊 Проверка результатов локальной калибровки..."
	@python3 python-worker/scripts/check_calibration_results.py


relcal-analyze:
	@echo "🔍 Анализ данных reliability calibrator..."
	@cd python-worker/scripts && python analyze_reliability_calibration.py

relcal-export:
	@echo "📤 Экспорт данных reliability calibrator в CSV..."
	@cd python-worker/scripts && python export_reliability_calibration.py

relcal-plot:
	@echo "📊 Построение графиков reliability calibrator..."
	@cd python-worker/scripts && python plot_reliability_calibration.py

relcal-help:
	@echo "╔═══════════════════════════════════════════════════════════════╗"
	@echo "║        📊 Reliability Calibration Analysis Help 📊           ║"
	@echo "╚═══════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "Система анализа калибровки надежности сигналов (confidence -> hit-rate)"
	@echo ""
	@echo "Основные команды:"
	@echo "  make relcal-analyze     - Быстрый анализ данных в терминале"
	@echo "  make relcal-export      - Экспорт данных в CSV для внешнего анализа"
	@echo "  make relcal-plot        - Построение графиков калибровочных кривых"
	@echo ""
	@echo "Outcomes (что анализируется):"
	@echo "  tp2                     - TP2 hit (компромисс entry quality)"
	@echo "  nosl_after_tp1          - TP1 hit + не SL (management quality)"
	@echo "  nosl_after_tp1_t500     - То же + survived ≥500ms после TP1"
	@echo "  nosl_after_tp1_t2000    - То же + survived ≥2000ms после TP1"
	@echo ""
	@echo "Переменные окружения:"
	@echo "  REL_CAL_OUTCOMES        - Список outcomes для анализа"
	@echo "  REL_CAL_ENABLED         - Включить сбор данных (1/0)"
	@echo ""
	@echo "Примеры использования:"
	@echo "  make relcal-analyze                           # Анализ в терминале"
	@echo "  make relcal-export && make relcal-plot       # Экспорт + графики"
	@echo ""
	@echo "Файлы создаются в: reliability_calibration_export/"

calibration-auto-start:
	@echo "🤖 Запуск автоматического обучения калибровки уверенности (Docker)..."
	@$(DC) -f $(COMPOSE_FILE) -f $(COMPOSE_TIMERS) up -d auto-train-conf-calibration-timer
	@echo "✅ Auto-train scheduler запущен"
	@sleep 3
	@make calibration-auto-status

calibration-auto-stop:
	@echo "🛑 Остановка автоматического обучения калибровки..."
	@$(DC) -f $(COMPOSE_FILE) -f $(COMPOSE_TIMERS) stop auto-train-conf-calibration-timer
	@echo "✅ Auto-train scheduler остановлен"

calibration-auto-status:
	@echo "📊 Статус автоматического обучения калибровки:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep scanner-auto-train-conf-calibration-timer || echo "❌ Service не запущен"

calibration-auto-logs:
	@echo "📋 Логи автоматического обучения калибровки (Ctrl+C для выхода)..."
	@docker logs -f scanner-auto-train-conf-calibration-timer

calibration-help:
	@echo "╔═══════════════════════════════════════════════════════════════╗"
	@echo "║           📊 Local Calibration System Help 📊                ║"
	@echo "╚═══════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "Система локальной калибровки порогов сигналов"
	@echo ""
	@echo "Основные команды:"
	@echo "  make calibration-run     - Запустить настройку калибровки"
	@echo "  make calibration-check   - Проверить результаты калибровки"
	@echo ""
	@echo "Автоматический планировщик (внутри контейнера):"
	@echo "  make calibration-auto-start  - Запустить планировщик калибровки"
	@echo "  make calibration-auto-stop   - Остановить планировщик"
	@echo "  make calibration-auto-status - Статус планировщика"
	@echo "  make calibration-auto-logs   - Логи планировщика"
	@echo "  Расписание: Азия 02:10 | Европа 10:10 | США 18:10 (Europe/Kyiv)"
	@echo ""
	@echo "Автоматический запуск:"
	@echo "  make up                  - Запускает калибровку автоматически"
	@echo ""
	@echo "Документация:"
	@echo "  📖 Setup Guide:  python-worker/CALIBRATION_SETUP.md"
	@echo "  📖 Scripts:      python-worker/scripts/"
	@echo ""
	@echo "Переменные окружения:"
	@echo "  PG_DSN                   - PostgreSQL connection string"
	@echo "  CALIB_LOOKBACK_DAYS      - Период анализа истории (дни)"
	@echo "  CALIB_MIN_TRADES_CLUSTER - Минимальное количество сделок"
	@echo ""

# ═══════════════════════════════════════════════════════════════════════════
# SIGNAL QUALITY COMMANDS
# ═══════════════════════════════════════════════════════════════════════════

.PHONY: quality-offline quality-online quality-run quality-check quality-help

quality-offline:
	@echo "📊 Запуск offline-вычисления качества сигналов..."
	@python3 python-worker/scripts/run_signal_quality_offline.py

quality-online:
	@echo "📊 Запуск online-вычисления качества сигналов..."
	@python3 python-worker/scripts/run_signal_quality_online.py

quality-run:
	@echo "📊 Запуск полного цикла качества сигналов..."
	@echo "Шаг 1: Offline качество..."
	@make quality-offline
	@echo ""
	@echo "Шаг 2: Online качество..."
	@make quality-online
	@echo "✅ Полный цикл качества завершен"

quality-check:
	@echo "📊 Проверка качества сигналов в БД..."
	@echo "TODO: Implement quality check script"

quality-help:
	@echo "╔═══════════════════════════════════════════════════════════════╗"
	@echo "║           📈 Signal Quality System Help 📈                   ║"
	@echo "╚═══════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "Система оценки качества сигналов на основе исторических данных"
	@echo ""
	@echo "Основные команды:"
	@echo "  make quality-offline      - Вычислить offline качество по истории"
	@echo "  make quality-online       - Вычислить online качество (rolling)"
	@echo "  make quality-run          - Запустить полный цикл качества"
	@echo "  make quality-check        - Проверить данные качества"
	@echo "  make quality-help         - Справка по системе качества"
	@echo ""
	@echo "Документация:"
	@echo "  📖 Architecture:  SIGNAL_QUALITY_SYSTEM.md"
	@echo "  📖 Migration:     python-worker/migrations/003_add_signal_quality_tables.sql"
	@echo "  📖 Scripts:       python-worker/scripts/run_signal_quality_*.py"
	@echo ""
	@echo "Переменные окружения:"
	@echo "  PG_DSN                   - PostgreSQL connection string"
	@echo "  QUALITY_HORIZON          - R горизонт ('R_main', 'R_30m')"
	@echo "  QUALITY_LOOKBACK_DAYS    - Период истории для offline (дни)"
	@echo "  QUALITY_ROLLING_WINDOW   - Размер rolling окна для online"

# ═══════════════════════════════════════════════════════════════════════════

guardrails-apply:
	@echo "🛡️ Применение Confidence Guardrails (dry-run по умолчанию)..."
	@$(DOCKER_COMPOSE_BIN) run --rm -e APPLY_GUARDRAILS=$(APPLY) confidence-drift-monitor-timer \
		bash ml_analysis/tools/run_confidence_drift_monitor.sh

# NEWS PIPELINE COMMANDS
# ═══════════════════════════════════════════════════════════════════════════

.PHONY: news-feature-store-start news-feature-store-stop news-feature-store-status news-feature-store-logs news-calendar-store-start news-calendar-store-stop news-calendar-store-status news-calendar-store-logs news-calendar-ingestor-start news-calendar-ingestor-stop news-calendar-ingestor-status news-calendar-ingestor-logs news-pipeline-status news-pipeline-logs news-pipeline-start news-pipeline-stop

news-feature-store-start:
	@echo "📰 Запуск News Feature Store Worker..."
	@cd python-worker && python -m news_pipeline.feature_store_worker &
	@echo "✅ News Feature Store Worker запущен в фоне (PID: $$!)"
	@sleep 2
	@make news-feature-store-status

news-feature-store-stop:
	@echo "🛑 Остановка News Feature Store Worker..."
	@pkill -f "news_pipeline.feature_store_worker" || echo "⚠️ Процесс не найден"
	@echo "✅ News Feature Store Worker остановлен"

news-feature-store-status:
	@echo "📊 Статус News Feature Store Worker:"
	@ps aux | grep -v grep | grep "news_pipeline.feature_store_worker" | head -1 | awk '{print "✅ PID: "$$2" | CPU: "$$3"% | MEM: "$$4"% | Started: "$$9}' || echo "❌ News Feature Store Worker не запущен"

news-feature-store-logs:
	@echo "📋 Логи News Feature Store Worker (Ctrl+C для выхода)..."
	@echo "⚠️ Логи доступны только через Docker или отдельный файл логов"
	@echo "Используйте: make news-feature-store-status"

news-calendar-store-start:
	@echo "📅 Запуск News Calendar Store Worker..."
	@cd python-worker && python -m news_pipeline.calendar_store_worker &
	@echo "✅ News Calendar Store Worker запущен в фоне (PID: $$!)"
	@sleep 2
	@make news-calendar-store-status

news-calendar-store-stop:
	@echo "🛑 Остановка News Calendar Store Worker..."
	@pkill -f "news_pipeline.calendar_store_worker" || echo "⚠️ Процесс не найден"
	@echo "✅ News Calendar Store Worker остановлен"

news-calendar-store-status:
	@echo "📊 Статус News Calendar Store Worker:"
	@ps aux | grep -v grep | grep "news_pipeline.calendar_store_worker" | head -1 | awk '{print "✅ PID: "$$2" | CPU: "$$3"% | MEM: "$$4"% | Started: "$$9}' || echo "❌ News Calendar Store Worker не запущен"

news-calendar-store-logs:
	@echo "📋 Логи News Calendar Store Worker (Ctrl+C для выхода)..."
	@echo "⚠️ Логи доступны только через Docker или отдельный файл логов"
	@echo "Используйте: make news-calendar-store-status"

news-calendar-ingestor-start:
	@echo "📅 Запуск News Calendar Ingestor Worker (standby)..."
	@cd python-worker && python -m news_pipeline.calendar_ingestor_worker &
	@echo "✅ News Calendar Ingestor Worker запущен в фоне (PID: $$!)"
	@sleep 2
	@make news-calendar-ingestor-status

news-calendar-ingestor-stop:
	@echo "🛑 Остановка News Calendar Ingestor Worker..."
	@pkill -f "news_pipeline.calendar_ingestor_worker" || echo "⚠️ Процесс не найден"
	@echo "✅ News Calendar Ingestor Worker остановлен"

news-calendar-ingestor-status:
	@echo "📊 Статус News Calendar Ingestor Worker (standby):"
	@ps aux | grep -v grep | grep "news_pipeline.calendar_ingestor_worker" | head -1 | awk '{print "✅ PID: "$$2" | CPU: "$$3"% | MEM: "$$4"% | Started: "$$9}' || echo "❌ News Calendar Ingestor Worker не запущен"

news-calendar-ingestor-logs:
	@echo "📋 Логи News Calendar Ingestor Worker (Ctrl+C для выхода)..."
	@echo "⚠️ Логи доступны только через Docker или отдельный файл логов"
	@echo "Используйте: make news-calendar-ingestor-status"

news-pipeline-start:
	@echo "📰 Запуск полного новостного пайплайна..."
	@echo "Запуск News Feature Store Worker..."
	@make news-feature-store-start
	@echo ""
	@echo "Запуск News Calendar Store Worker..."
	@make news-calendar-store-start
	@echo ""
	@echo "Запуск News Calendar Ingestor Worker (standby)..."
	@make news-calendar-ingestor-start
	@echo ""
	@echo "✅ Полный новостной пайплайн запущен"

news-pipeline-stop:
	@echo "🛑 Остановка полного новостного пайплайна..."
	@echo "Остановка News Feature Store Worker..."
	@make news-feature-store-stop
	@echo ""
	@echo "Остановка News Calendar Store Worker..."
	@make news-calendar-store-stop
	@echo ""
	@echo "Остановка News Calendar Ingestor Worker..."
	@make news-calendar-ingestor-stop
	@echo ""
	@echo "✅ Полный новостной пайплайн остановлен"

news-pipeline-status:
	@echo "╔═══════════════════════════════════════════════════════════════╗"
	@echo "║           📰 News Pipeline Status 📰                         ║"
	@echo "╚═══════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "News Feature Store Worker:"
	@make news-feature-store-status | sed 's/^/  /'
	@echo ""
	@echo "News Calendar Store Worker:"
	@make news-calendar-store-status | sed 's/^/  /'
	@echo ""
	@echo "News Calendar Ingestor Worker (standby):"
	@make news-calendar-ingestor-status | sed 's/^/  /'

news-pipeline-logs:
	@echo "📋 Логи новостного пайплайна (Ctrl+C для выхода)..."
	@echo "⚠️ Логи доступны только через Docker или отдельные файлы логов"
	@echo "Используйте индивидуальные команды:"
	@echo "  make news-feature-store-logs"
	@echo "  make news-calendar-store-logs"
	@echo "  make news-calendar-ingestor-logs"

# ------------------------------------------------------------------------------
# Log Filtering System
# ------------------------------------------------------------------------------
.PHONY: prometheus-filtered-logs grafana-filtered-logs

prometheus-filtered-logs:
	@echo "📊 Фильтрованные логи Prometheus (каждое 10000-е сообщение)"
	@echo "Использование: $(DOCKER_COMPOSE_BIN) logs -f scanner-prometheus | $(MAKE) prometheus-filtered-logs"
	@$(DOCKER_COMPOSE_BIN) logs -f scanner-prometheus 2>/dev/null | ./log_filter.py prometheus || echo "Ошибка: убедитесь что $(DOCKER_COMPOSE_BIN) запущен"

grafana-filtered-logs:
	@echo "📊 Фильтрованные логи Grafana (каждое 10000-е сообщение)"
	@echo "Использование: $(DOCKER_COMPOSE_BIN) logs -f scanner-grafana | $(MAKE) grafana-filtered-logs"
	@$(DOCKER_COMPOSE_BIN) logs -f scanner-grafana 2>/dev/null | ./log_filter.py grafana || echo "Ошибка: убедитесь что $(DOCKER_COMPOSE_BIN) запущен"

# ═══════════════════════════════════════════════════════════════
# OF Timers (Config Drift Monitor, Weekly Bench)
# ═══════════════════════════════════════════════════════════════

timers-up:
	@echo "⏰ Запуск всех таймеров из docker-compose-timers.yml..."
	@if [ -f "$(COMPOSE_TIMERS)" ]; then \
		$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TIMERS) up -d && \
		echo "✅ Таймеры запущены" || echo "⚠️  Ошибка запуска таймеров"; \
	else \
		echo "⚠️  $(COMPOSE_TIMERS) не найден"; \
	fi

timers-down:
	@echo "⏰ Остановка всех таймеров..."
	@if [ -f "$(COMPOSE_TIMERS)" ]; then \
		$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TIMERS) down && \
		echo "✅ Таймеры остановлены" || echo "⚠️  Ошибка остановки таймеров"; \
	else \
		echo "⚠️  $(COMPOSE_TIMERS) не найден"; \
	fi

timers-status:
	@echo "📊 Статус таймеров:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep timer || echo "❌ Таймеры не запущены"

of-timers-install:
	@echo "⏰ OF Timers Worker уже интегрирован в docker-compose-timers.yml"
	@echo "   Запускается автоматически при make up"
	@echo ""
	@echo "📋 Для ручного управления используйте:"
	@echo "   make of-timers-status  - Статус воркера"
	@echo "   make of-timers-logs    - Логи воркера"
	@echo "   make of-timers-restart - Перезапуск воркера"

conf-score-tuning-status:
	@echo "📊 Статус Confidence Scorer Weight Tuning:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep "conf-score-tuning" || echo "❌ Таймер или экспортер не запущены"

conf-score-tuning-logs:
	@echo "📋 Логи Confidence Scorer Weight Tuning Nightly Timer (последние 50 строк):"
	@docker logs --tail=50 scanner-conf-score-tuning-nightly-timer || echo "❌ Ошибка получения логов таймера"
	@echo ""
	@echo "📋 Логи Confidence Scorer Weight Tuning Exporter (последние 50 строк):"
	@docker logs --tail=50 scanner-conf-score-tuning-exporter || echo "❌ Ошибка получения логов экспортера"

of-timers-status:
	@echo "📊 Статус OF Timers Worker:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep of-timers-worker || echo "❌ OF Timers Worker не запущен"
	@echo ""
	@echo "📅 Расписание:"
	@echo "   - Config Drift Monitor: каждый час в :15 минут"
	@echo "   - Weekly Bench: воскресенье в 06:40 UTC"

of-timers-logs:
	@echo "📋 Логи OF Timers Worker (Ctrl+C для выхода)..."
	@docker logs -f scanner-of-timers-worker 2>&1 || echo "❌ OF Timers Worker не найден"

of-timers-restart:
	@echo "🔄 Перезапуск OF Timers Worker..."
	@docker restart scanner-of-timers-worker 2>/dev/null || echo "⚠️ OF Timers Worker не найден или не запущен"
	@echo "✅ Перезапуск выполнен"

# ── Стек мониторинга (Grafana + Alertmanager) ────────────────────────────────
# Prometheus консолидирован на minik (порт 9090). Здесь только Grafana + Alertmanager.

monitoring-start:
	@echo "📊 Запуск Grafana + Alertmanager + Smoke Exporter (Prometheus на minik:9090)..."
	@$(DOCKER_COMPOSE_BIN) -f docker-compose-monitoring.yml up -d grafana alertmanager-telegram-webhook alertmanager 2>/dev/null && \
		echo "✅ Grafana + Alertmanager запущены" || echo "⚠️  Ошибка запуска мониторинга"
	@$(DOCKER_COMPOSE_BIN) -f docker-compose-crypto-orderflow.yml up -d monitoring-smoke-exporter 2>/dev/null && \
		echo "✅ Smoke Exporter запущен" || echo "⚠️  Ошибка запуска Smoke Exporter"

monitoring-stop:
	@echo "🛑 Остановка Grafana + Alertmanager (Prometheus на minik)..."
	@docker stop scanner-grafana scanner-alertmanager alertmanager-telegram-webhook 2>/dev/null || true
	@docker rm scanner-grafana scanner-alertmanager alertmanager-telegram-webhook 2>/dev/null || true
	@echo "✅ Мониторинг остановлен"

monitoring-status:
	@echo "📊 Статус стека мониторинга:"
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "prometheus|grafana|alertmanager" || echo "❌ Контейнеры мониторинга не запущены"

monitoring-smoke-exporter-status:
	@echo "📊 Статус Monitoring Smoke Exporter:"
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep "monitoring-smoke-exporter" || echo "❌ Не запущен"

monitoring-smoke-exporter-logs:
	@echo "📋 Логи Monitoring Smoke Exporter:"
	@docker logs -f scanner-monitoring-smoke-exporter

# ── Развёртывание на minik (Prometheus + Grafana) ───────────────────────────
# Prometheus (9090) и Grafana (3001) на minik (192.168.0.121)

minik-deploy-prom-rules:
	@echo "📊 Развёртывание Prometheus rule files на minik..."
	@./scripts/deploy-prom-rules-to-minik.sh

minik-deploy-grafana:
	@echo "📊 Развёртывание Grafana на minik..."
	@./scripts/deploy-grafana-to-minik.sh

minik-deploy-all: minik-deploy-prom-rules minik-deploy-grafana
	@echo "✅ Полное развёртывание мониторинга на minik завершено"
	@echo "   Prometheus: http://192.168.0.121:9090"
	@echo "   Grafana:    http://192.168.0.121:3002 (admin/admin)"

health-reporter-status:
	@echo "📊 Статус Health Reporter:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep health-reporter || echo "❌ Health Reporter не запущен"

health-reporter-logs:
	@echo "📋 Логи Health Reporter (Ctrl+C для выхода)..."
	@docker logs -f scanner-health-reporter 2>&1 || echo "❌ Health Reporter не найден"

health-reporter-restart:
	@echo "🔄 Перезапуск Health Reporter..."
	@docker restart scanner-health-reporter 2>/dev/null || echo "⚠️ Health Reporter не найден или не запущен"
	@echo "✅ Перезапуск выполнен"

# ── Binance USDT-M Account Reporter (managed via docker-compose-binance.yml) ──
# Full implementations are at the end of the Makefile (binance-* section).

binance-reporter-status:
	@docker ps -a --filter "name=scanner-binance-account-reporter" --format "table {{.Names}}\t{{.Status}}\t{{.RunningFor}}"

binance-reporter-logs:
	@docker logs -f scanner-binance-account-reporter

binance-reporter-restart:
	@docker restart scanner-binance-account-reporter && echo "✅ binance-account-reporter restarted"

binance-report-now:
	@echo "📤 Forcing immediate Binance account report..."
	@docker exec scanner-binance-account-reporter python -c "import os,sys;sys.path.insert(0,'/app');from services.binance_account_reporter import build_snapshot,format_report;from services.binance_futures_client import BinanceFuturesREST;from services.telegram.telegram_client import TelegramClient;c=BinanceFuturesREST(api_key=os.environ['BINANCE_API_KEY'],api_secret=os.environ['BINANCE_API_SECRET'],base_url=os.environ.get('BINANCE_FUTURES_BASE_URL','https://fapi.binance.com'));s=build_snapshot(client=c,topn_positions=5,include_open_orders=True);m=format_report(s);print(m);tg=TelegramClient.from_env();tg.send_text(m) if tg else None;print('Done')"
	@echo "✅ Report sent"


trade-events-status:
	@echo "📊 Статус Trade Events Writer (P44):"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "trade-events|meta-enforce-cov" || echo "❌ Trade Events components не запущены"

trade-events-logs:
	@echo "📋 Логи Trade Events Writer (Ctrl+C для выхода)..."
	@docker logs -f scanner-meta-enforce-cov-auto-apply-timer 2>&1 || echo "❌ Meta ENFORCE auto-apply timer не найден"

meta-enforce-ramp-status:
	@echo "📊 Статус Meta ENFORCE ramp-or-freeze timer:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep meta-enforce-ramp-timer || echo "❌ Meta ENFORCE ramp timer не запущен"
	@echo ""
	@echo "📅 Расписание:"
	@echo "   - Запуск: ежедневно в 06:20 UTC"
	@echo "   - Метод: stratified worst-case DiD (symbol × regime_bucket)"
	@echo "   - Действия: ramp bundle (если OK) или freeze bundle (если worst_case_failed)"

meta-enforce-ramp-logs:
	@echo "📋 Логи Meta ENFORCE ramp-or-freeze timer (Ctrl+C для выхода)..."
	@docker logs -f scanner-of-nightly-meta-enforce-ramp-timer 2>&1 || echo "❌ Meta ENFORCE ramp timer не найден"

meta-enforce-ramp-restart:
	@echo "🔄 Перезапуск Meta ENFORCE ramp-or-freeze timer..."
	@docker restart scanner-of-nightly-meta-enforce-ramp-timer 2>/dev/null || echo "⚠️ Meta ENFORCE ramp timer не найден или не запущен"

# ═══════════════════════════════════════════════════════════════
# Meta Stage2 Optimize Share Timer
# ═══════════════════════════════════════════════════════════════

meta-stage2-opt-status:
	@echo "📊 Статус Meta Stage2 optimize share timer (v3):"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep meta-stage2-opt-timer || echo "❌ Meta Stage2 optimize timer не запущен"
	@echo ""
	@echo "📅 Расписание:"
	@echo "   - Запуск: ежедневно в 06:25 UTC"
	@echo "   - Метод: multi-objective per-cell share optimization (SYMBOL|bucket) + group caps + per-symbol budget"
	@echo "   - Действия: оптимизация share с отдельными grid для trend/range, лимит шага, turnover-proxy"
	@echo "   - Версия: v3 (multi-objective + group caps + per-symbol budget + optional coupling)"

meta-stage2-opt-logs:
	@echo "📋 Логи Meta Stage2 optimize share timer (Ctrl+C для выхода)..."
	@docker logs -f scanner-of-nightly-meta-stage2-opt-timer 2>&1 || echo "❌ Meta Stage2 optimize timer не найден"

meta-stage2-opt-restart:
	@echo "🔄 Перезапуск Meta Stage2 optimize share timer..."
	@docker restart scanner-of-nightly-meta-stage2-opt-timer 2>/dev/null || echo "⚠️ Meta Stage2 optimize timer не найден или не запущен"
	@echo "✅ Перезапуск выполнен"

# ═══════════════════════════════════════════════════════════════
# ML Rollout Guard Timer
# ═══════════════════════════════════════════════════════════════

ml-rollout-guard-status:
	@echo "📊 Статус ML rollout guard timer:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep ml-rollout-guard-timer || echo "❌ ML rollout guard timer не запущен"
	@echo ""
	@echo "📅 Расписание:"
	@echo "   - Запуск: каждый час в :00 минут"
	@echo "   - Функция: автоматические freeze/unfreeze предложения на основе метрик"
	@echo "   - FREEZE: снижение enforce_share до min(cur, 0.05) при плохих метриках"
	@echo "   - UNFREEZE: восстановление pre_freeze_share после 7 хороших запусков"

ml-rollout-guard-logs:
	@echo "📋 Логи ML rollout guard timer (Ctrl+C для выхода)..."
	@docker logs -f scanner-ml-rollout-guard-timer-v3 2>&1 || echo "❌ ML rollout guard timer не найден"

ml-rollout-guard-restart:
	@echo "🔄 Перезапуск ML rollout guard timer..."
	@docker restart scanner-ml-rollout-guard-timer-v3 2>/dev/null || echo "⚠️ ML rollout guard timer не найден или не запущен"
	@echo "✅ Перезапуск выполнен"

# ═══════════════════════════════════════════════════════════════
# ML Outcome Joiner Worker
# ═══════════════════════════════════════════════════════════════

ml-outcome-joiner-status:
	@echo "📊 Статус ML outcome joiner worker:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep ml-outcome-joiner || echo "❌ ML outcome joiner не запущен"
	@echo ""
	@echo "📅 Функция:"
	@echo "   - Читает events:trades (POSITION_CLOSED)"
	@echo "   - Объединяет с ml:pred:<sid> (pred cache)"
	@echo "   - Пишет metrics:ml_outcome (y, brier, brier_chal, r_mult)"

ml-outcome-joiner-logs:
	@echo "📋 Логи ML outcome joiner worker (Ctrl+C для выхода)..."
	@docker logs -f scanner-ml-outcome-joiner-v3 2>&1 || echo "❌ ML outcome joiner не найден"

ml-outcome-joiner-restart:
	@echo "🔄 Перезапуск ML outcome joiner worker..."
	@docker restart scanner-ml-outcome-joiner-v3 2>/dev/null || echo "⚠️ ML outcome joiner не найден или не запущен"
	@echo "✅ Перезапуск выполнен"

# ═══════════════════════════════════════════════════════════════
# ML Promotion Ladder Timer
# ═══════════════════════════════════════════════════════════════

ml-promotion-ladder-status:
	@echo "📊 Статус ML promotion ladder timer:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep ml-promotion-ladder-timer || echo "❌ ML promotion ladder timer не запущен"
	@echo ""
	@echo "📅 Расписание:"
	@echo "   - Запуск: ежедневно в 05:05 UTC"
	@echo "   - Функция: оценка outcomes + health, предложения promote challenger / increase share"
	@echo "   - Действия: двухфазный PROPOSE (preview2 -> confirm -> reject)"

ml-promotion-ladder-logs:
	@echo "📋 Логи ML promotion ladder timer (Ctrl+C для выхода)..."
	@docker logs -f scanner-ml-promotion-ladder-timer-v3 2>&1 || echo "❌ ML promotion ladder timer не найден"

ml-promotion-ladder-restart:
	@echo "🔄 Перезапуск ML promotion ladder timer..."
	@docker restart scanner-ml-promotion-ladder-timer-v3 2>/dev/null || echo "⚠️ ML promotion ladder timer не найден или не запущен"
	@echo "✅ Перезапуск выполнен"

# ═══════════════════════════════════════════════════════════════
# ML Threshold Proposer Timer
# ═══════════════════════════════════════════════════════════════

ml-threshold-proposer-status:
	@echo "📊 Статус ML threshold proposer timer:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep ml-threshold-proposer-timer || echo "❌ ML threshold proposer timer не запущен"
	@echo ""
	@echo "📅 Расписание:"
	@echo "   - Запуск: ежедневно в 06:45 UTC"
	@echo "   - Функция: предложения p_min порогов по символам/бакетам с Approve/Reject"

ml-threshold-proposer-logs:
	@echo "📋 Логи ML threshold proposer timer (Ctrl+C для выхода)..."
	@docker logs -f scanner-ml-threshold-proposer-timer 2>&1 || echo "❌ ML threshold proposer timer не найден"

ml-threshold-proposer-restart:
	@echo "🔄 Перезапуск ML threshold proposer timer..."
	@docker restart scanner-ml-threshold-proposer-timer 2>/dev/null || echo "⚠️ ML threshold proposer timer не найден или не запущен"
	@echo "✅ Перезапуск выполнен"

# ═══════════════════════════════════════════════════════════════
# ML Threshold Proposer V2 Timer (v7: utility + drift-guard)
# ═══════════════════════════════════════════════════════════════

ml-threshold-proposer-v2-status:
	@echo "📊 Статус ML threshold proposer v2 timer:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep ml-threshold-proposer-v2-timer || echo "❌ ML threshold proposer v2 timer не запущен"
	@echo ""
	@echo "📅 Расписание:"
	@echo "   - Запуск: ежедневно в 06:55 UTC"
	@echo "   - Функция: v7 utility-based p_min proposals + drift-guard + calibration"

ml-threshold-proposer-v2-logs:
	@echo "📋 Логи ML threshold proposer v2 timer (Ctrl+C для выхода)..."
	@docker logs -f scanner-ml-threshold-proposer-v2-timer 2>&1 || echo "❌ ML threshold proposer v2 timer не найден"

ml-threshold-proposer-v2-restart:
	@echo "🔄 Перезапуск ML threshold proposer v2 timer..."
	@docker restart scanner-ml-threshold-proposer-v2-timer 2>/dev/null || echo "⚠️ ML threshold proposer v2 timer не найден или не запущен"
	@echo "✅ Перезапуск выполнен"

# ═══════════════════════════════════════════════════════════════
# ML Training Timer v9
# ═══════════════════════════════════════════════════════════════

train-meta-model-v9-lr-timer-status:
	@echo "📊 Статус ML Training Timer v9:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep scanner-train-meta-model-lr-v9-timer || echo "❌ ML Training Timer v9 не запущен"
	@echo ""
	@echo "📅 Расписание:"
	@echo "   - Запуск: ежедневно в 05:15 UTC"

train-meta-model-v9-lr-timer-logs:
	@echo "📋 Логи ML Training Timer v9 (Ctrl+C для выхода)..."
	@docker logs -f scanner-train-meta-model-lr-v9-timer 2>&1 || echo "❌ ML Training Timer v9 не найден"

train-meta-model-v9-lr-timer-restart:
	@echo "🔄 Перезапуск ML Training Timer v9..."
	@docker restart scanner-train-meta-model-lr-v9-timer 2>/dev/null || echo "⚠️ ML Training Timer v9 не найден или не запущен"
	@echo "✅ Перезапуск выполнен"

# ═══════════════════════════════════════════════════════════════
# ML Rollback Proposer Timer
# ═══════════════════════════════════════════════════════════════

ml-rollback-proposer-status:
	@echo "📊 Статус ML rollback proposer timer:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep ml-rollback-proposer-timer || echo "❌ ML rollback proposer timer не запущен"
	@echo ""
	@echo "📅 Расписание:"
	@echo "   - Запуск: каждый час"
	@echo "   - Функция: предложение rollback к *_prev если safety/utility проваливается"

ml-rollback-proposer-logs:
	@echo "📋 Логи ML rollback proposer timer (Ctrl+C для выхода)..."
	@docker logs -f scanner-ml-rollback-proposer-timer 2>&1 || echo "❌ ML rollback proposer timer не найден"

ml-rollback-proposer-restart:
	@echo "🔄 Перезапуск ML rollback proposer..."
	@$(DOCKER_COMPOSE_BIN) restart ml-rollback-proposer-timer
	@echo "✅ ML rollback proposer перезапущен"

ab-rollback-guard-v2-status:
	@echo "📊 Статус AB RollbackGuard V2 (enforce mode):"
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.RunningFor}}" | grep ab-rollback-guard-v2 || echo "❌ scanner-ab-rollback-guard-v2 не запущен"
	@echo ""
	@echo "🔑 Текущий режим в Redis (last rollback records):"
	@docker exec redis-worker-1 redis-cli --scan --pattern "cfg:entry_policy:rollback:v1:*" 2>/dev/null | head -5 || true
	@echo ""
	@echo "📋 Режим: AB_RB_MODE=enforce"
	@echo "   - Catastrophic (d_lcb ≤ cat_lcb): авто-revert active_arm + Telegram ⛔"
	@echo "   - Normal degradation: Telegram 🚨 /approve_rollback"

ab-rollback-guard-v2-logs:
	@echo "📋 Логи AB RollbackGuard V2 (Ctrl+C для выхода)..."
	@docker logs -f scanner-ab-rollback-guard-v2 2>&1 || echo "❌ scanner-ab-rollback-guard-v2 не найден"

ab-rollback-guard-v2-restart:
	@echo "🔄 Перезапуск AB RollbackGuard V2..."
	@$(DC_WITH_TRAILING) restart ab-rollback-guard-v2
	@echo "✅ AB RollbackGuard V2 перезапущен"

ab-rollback-guard-v2-start:
	@echo "🚀 Запуск AB RollbackGuard V2..."
	@$(DC_WITH_TRAILING) up -d --no-deps ab-rollback-guard-v2
	@echo "✅ AB RollbackGuard V2 запущен"

demo-reconcile-status:
	@echo "📊 Статус Demo Reconcile Timer:"
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.RunningFor}}" | grep demo-reconcile || echo "❌ scanner-demo-reconcile-timer не запущен"
	@echo ""
	@echo "ℹ️  Включение: DEMO_RECONCILE_ENABLE=1 в .env, затем make demo-reconcile-restart"

demo-reconcile-logs:
	@echo "📋 Логи Demo Reconcile Timer (Ctrl+C для выхода)..."
	@docker logs -f scanner-demo-reconcile-timer 2>&1 || echo "❌ scanner-demo-reconcile-timer не найден"

demo-reconcile-restart:
	@echo "🔄 Перезапуск Demo Reconcile Timer..."
	@$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TIMERS) restart demo-reconcile-timer
	@echo "✅ Demo Reconcile Timer перезапущен"

demo-reconcile-start:
	@echo "🚀 Запуск Demo Reconcile Timer..."
	@$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TIMERS) up -d --no-deps demo-reconcile-timer
	@echo "✅ Demo Reconcile Timer запущен"

demo-reconcile-stop:
	@echo "🛑 Остановка Demo Reconcile Timer..."
	@$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TIMERS) stop demo-reconcile-timer
	@echo "✅ Demo Reconcile Timer остановлен"

demo-reconcile-now:
	@echo "📤 Запуск Demo Reconcile вручную..."
	@docker exec scanner-demo-reconcile-timer python3 -m tools.cron_demo_reconcile || \
		echo "❌ Не удалось запустить (контейнер не запущен?)"

ab-rollback-guard-v2-stop:
	@echo "🛑 Остановка AB RollbackGuard V2..."
	@$(DC_WITH_TRAILING) stop ab-rollback-guard-v2
	@echo "✅ AB RollbackGuard V2 остановлен"

ab-rollback-guard-v2-audit:
	@echo "🔍 Последние события из stream:ab:rollback_audit:"
	@docker exec redis-worker-1 redis-cli XREVRANGE stream:ab:rollback_audit + - COUNT 10 2>/dev/null || echo "❌ Redis недоступен или stream пуст"


replay-archiver-status:
	@echo "📊 Статус Replay Archiver:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep replay-archiver || echo "❌ Не запущен"

replay-archiver-logs:
	@echo "📋 Логи Replay Archiver..."
	@$(DOCKER_COMPOSE_BIN) logs -f replay-archiver replay-archiver-exporter 2>&1 || echo "❌ Replay Archiver не найден"

ml-drift-monitor-status:
	@echo "📊 ML Drift Monitor Timer Status:"
	@docker ps --filter "name=scanner-ml-drift-monitor-v1-timer" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" || echo "Container not found"

ml-drift-monitor-logs:
	@echo "📋 ML Drift Monitor Timer Logs (last 100 lines):"
	@docker logs --tail 100 -f scanner-ml-drift-monitor-v1-timer 2>&1 || echo "Container not found"

ml-drift-monitor-restart:
	@echo "🔄 Перезапуск ML Drift Monitor Timer..."
	@docker restart scanner-ml-drift-monitor-v1-timer 2>/dev/null || echo "⚠️ ML drift monitor timer не найден или не запущен"
	@echo "✅ Перезапуск выполнен"

ml-drift-monitor-run:
	@echo "🔍 Running ML Drift Monitor (one-time)..."
	@docker exec -i scanner-ml-drift-monitor-v1-timer python3 tools/ml_drift_monitor_v1.py --window-hours 6 --baseline-hours 168 --emit || echo "Container not running or error occurred"

# ═══════════════════════════════════════════════════════════════
# Manual ML Model Training
# ═══════════════════════════════════════════════════════════════

ml-manual-train:
	@echo "🤖 Запуск ручного обучения ML моделей (LR + GBDT)..."
	@if [ -z "$(PARQUET)" ]; then \
		LATEST=$$(find /var/lib/trade/of_reports/out/ -name "dataset.parquet" -size +1k -printf "%T@ %p\n" 2>/dev/null | sort -n | tail -n 1 | awk '{print $$2}'); \
		if [ -n "$$LATEST" ]; then \
			PARQUET_VAL="$$LATEST"; \
			echo "💡 Using latest found parquet: $$PARQUET_VAL"; \
		else \
			PARQUET_VAL="/var/lib/trade/of_reports/out/dataset.parquet"; \
			echo "⚠️  PARQUET not set and none found in subdirs, using default: $$PARQUET_VAL"; \
		fi \
	else \
		PARQUET_VAL="$(PARQUET)"; \
	fi; \
	DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1 $(DOCKER_COMPOSE_BIN) -f docker-compose-timers.yml run --rm of-meta-train-v2 python3 tools/train_meta_model_lr_v1.py \
		--parquet $$PARQUET_VAL \
		--out /var/lib/trade/of_reports/models/meta_lr.json \
		--label-col y \
		--ts-col ts_ms \
		--threshold 0.55

ml-approve:
	@echo "✅ Активация новой ML модели (manual approval)..."
	@$(DOCKER_COMPOSE_BIN) -f docker-compose-timers.yml run --rm of-meta-train-v2 python3 tools/ml_manual_approve.py --force

ml-verify-infra:
	@echo "🧪 Verifying ML Training Infrastructure (GPU + Paths + Environment)..."
	@PARQUET=/var/lib/trade/of_reports/out/infra_verify_balanced.parquet make ml-manual-train

tb-labeler-process:
	@echo "🔄 Запуск обработки TB labeler (v10.1) вручную..."
	@echo "   Обработка pending/new сообщений, direct stream и due jobs"
	@DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1 $(DOCKER_COMPOSE_BIN) -f docker-compose-timers.yml run --rm \
		-e REDIS_URL=${REDIS_URL:-redis://redis-worker-1:6379/0} \
		-e PYTHONPATH=.:/app \
		ml-train-tb-v10-2-timer \
		python3 -m tools.tb_labeler_process_once --all --limit 5000 --since-hours ${TB_SINCE_HOURS:-24}
	@echo ""
	@echo "✅ Обработка завершена. Проверьте: docker exec redis-worker-1 redis-cli XINFO STREAM labels:tb"

ml-train-tb-v10-2:
	@echo "🧪 Запуск обучения ML модели на TB labels (v10.2)..."
	@echo "   Экспорт → Построение датасета → Обучение stack → Telegram Approve/Reject"
	@echo ""
	@DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1 $(DOCKER_COMPOSE_BIN) -f docker-compose-timers.yml run --rm \
		-e REDIS_URL=${REDIS_URL:-redis://redis-worker-1:6379/0} \
		-e OF_INPUTS_STREAM=$${OF_INPUTS_STREAM:-signals:of:inputs} \
		-e OF_INPUTS_FIELD=$${OF_INPUTS_FIELD:-payload} \
		-e TB_LABELS_STREAM=$${TB_LABELS_STREAM:-labels:tb} \
		-e TB_LABELS_FIELD=$${TB_LABELS_FIELD:-payload} \
		-e TELEGRAM_NOTIFY_STREAM=$${TELEGRAM_NOTIFY_STREAM:-notify:telegram} \
		-e ML_TB_SINCE_HOURS=$${ML_TB_SINCE_HOURS:-72} \
		-e ML_MODELS_ROOT=$${ML_MODELS_ROOT:-/var/lib/trade/ml_models} \
		-e ML_CFG_CHAMPION_KEY=$${ML_CFG_CHAMPION_KEY:-cfg:ml_confirm:champion} \
		-e ML_CFG_CHALLENGER_KEY=$${ML_CFG_CHALLENGER_KEY:-cfg:ml_confirm:challenger} \
		-e TB_PRIMARY_H_MS=$${TB_PRIMARY_H_MS:-180000} \
		-e ML_TB_LABEL_COL=$${ML_TB_LABEL_COL:-y_edge} \
		ml-train-tb-v10-2-timer \
		python3 -m tools.ml_train_report_tb_v10_2 --since-hours $${ML_TB_SINCE_HOURS:-72} --label-col $${ML_TB_LABEL_COL:-y_edge}
	@echo ""
	@echo "✅ Обучение завершено. Проверьте Telegram для Approve/Reject."

# ═══════════════════════════════════════════════════════════════
# Manual Timer Execution (для записи в ручном режиме)
# ═══════════════════════════════════════════════════════════════

of-nightly-calibrate-manual:
	@echo "⏰ Ручной запуск nightly calibration (05:30 UTC)..."
	@echo "   Запуск: tools.nightly_gate_calibrate_bundle"
	@if ! docker ps --format "{{.Names}}" | grep -q "^scanner-of-nightly-calibrate-timer$$"; then \
		echo "❌ Контейнер scanner-of-nightly-calibrate-timer не запущен"; \
		echo ""; \
		echo "   Для запуска контейнера выполните:"; \
		echo "   $(DOCKER_COMPOSE_BIN) -f docker-compose-timers.yml up -d of-nightly-calibrate-timer"; \
		echo ""; \
		echo "   Или запустите все таймеры:"; \
		echo "   make timers-up"; \
		exit 1; \
	fi
	@echo "   Используется контейнер: scanner-of-nightly-calibrate-timer"
	@docker exec scanner-of-nightly-calibrate-timer python3 -m tools.nightly_gate_calibrate_bundle || (echo "⚠️  Ошибка выполнения команды (код: $$?)" && exit 1)
	@echo ""
	@echo "✅ Nightly calibration завершена."

of-nightly-meta-train-manual:
	@echo "⏰ Ручной запуск nightly meta train (05:10 UTC)..."
	@echo "   Запуск: tools.nightly_train_meta_model_lr_v1 --apply"
	@if ! docker ps --format "{{.Names}}" | grep -q "^scanner-meta-model-train-timer$$"; then \
		echo "❌ Контейнер scanner-meta-model-train-timer не запущен"; \
		echo ""; \
		echo "   Для запуска контейнера выполните:"; \
		echo "   $(DOCKER_COMPOSE_BIN) -f docker-compose-timers.yml up -d scanner-meta-model-train-timer"; \
		echo ""; \
		exit 1; \
	fi
	@echo "   Используется контейнер: scanner-meta-model-train-timer"
	@docker exec scanner-meta-model-train-timer python3 -m tools.nightly_train_meta_model_lr_v1 --apply || (echo "⚠️  Ошибка выполнения команды (код: $$?)" && exit 1)
	@echo ""
	@echo "✅ Nightly meta train завершена."

of-nightly-meta-train-v3-manual:
	@echo "⏰ Ручной запуск nightly meta train V3 (05:10 UTC)..."
	@echo "   Запуск: tools.nightly_train_meta_model_v3_bundle --auto-confirm"
	@if ! docker ps --format "{{.Names}}" | grep -q "^scanner-of-meta-train-v3$$"; then \
		echo "❌ Контейнер scanner-of-meta-train-v3 не запущен"; \
		exit 1; \
	fi
	@docker exec scanner-of-meta-train-v3 python3 -m tools.nightly_train_meta_model_v3_bundle --auto-confirm || (echo "⚠️  Ошибка выполнения команды (код: $$?)" && exit 1)
	@echo ""
	@echo "✅ Nightly meta train V3 завершена."

# meta-train manual targets consolidated above

meta-quality-manual:
	@echo "⏰ Ручной запуск nightly quality report..."
	@if ! docker ps --format "{{.Names}}" | grep -q "^scanner-of-nightly-quality-timer$$"; then \
		echo "❌ Контейнер scanner-of-nightly-quality-timer не запущен"; \
		exit 1; \
	fi
	@docker exec scanner-of-nightly-quality-timer sh -c "python3 -m tools.meta_model_quality_report_v1 --in-parquet /var/lib/trade/of_reports/events/meta_features_\$$(date -d 'yesterday' +%Y-%m-%d).parquet --model-json /var/lib/trade/of_reports/models/meta_lr.json --label-col label_15m" || echo "⚠️  Ошибка (возможно нет файла)"

meta-auto-ramp-manual:
	@echo "⏰ Ручной запуск nightly auto-ramp..."
	@if ! docker ps --format "{{.Names}}" | grep -q "^scanner-of-nightly-auto-ramp-timer$$"; then \
		echo "❌ Контейнер scanner-of-nightly-auto-ramp-timer не запущен"; \
		exit 1; \
	fi
	@docker exec scanner-of-nightly-auto-ramp-timer python3 -m tools.meta_auto_ramp_v1 --report-json /var/lib/trade/of_reports/out/meta_quality_report.json --apply 0

regress-safe-manual:
	@echo "⏰ Ручной запуск engine regress (nightly_regress_engine_replay_safe)..."
	@CONTAINER=scanner-nightly-regress-engine-timer; \
	if ! docker ps --format "{{.Names}}" | grep -q "^$$CONTAINER$$"; then \
		echo "❌ Контейнер $$CONTAINER не запущен"; \
		echo "   Запустите: docker compose -f docker-compose-timers.yml up -d nightly-regress-engine-timer"; \
		exit 1; \
	fi; \
	echo "   Используется контейнер: $$CONTAINER"; \
	docker exec $$CONTAINER python3 -m tools.nightly_regress_engine_replay_safe || (echo "⚠️  Regress failed (exit $$?)" && exit 1)
	@echo ""
	@echo "✅ Regress safe завершена."

regress-engine-status:
	@echo "📊 Статус nightly-regress-engine-timer:"
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.RunningFor}}" | grep nightly-regress-engine-timer || echo "❌ Контейнер не запущен"
	@echo ""
	@echo "📋 Redis regress status:"
	@docker exec redis-worker-1 redis-cli MGET sre:regress:last_status sre:regress:pass_streak sre:regress:last_ts_ms 2>/dev/null | paste - - - | awk '{print "  status="$$1" streak="$$2" ts_ms="$$3}'

regress-engine-logs:
	@echo "📋 Логи nightly-regress-engine-timer (последние 50 строк):"
	@docker logs --tail=50 scanner-nightly-regress-engine-timer 2>&1

# ═══════════════════════════════════════════════════════════════
# OFC Golden Replay Commands
# ═══════════════════════════════════════════════════════════════

OFC_CAPTURE ?= /tmp/ofc_inputs.ndjson
OFC_GOLDEN ?= /tmp/ofc_golden.ndjson
OFC_BENCH_WARMUP ?= 200
OFC_BENCH_ITERS ?= 2000
OFC_BENCH_MODE ?= restore_each
OFC_BUDGET_P95_US ?= 350
OFC_BUDGET_P99_US ?= 900

ofc-validate:
	@echo "🔍 Валидация OFC capture..."
	@cd python-worker && make -f make.ofc.mk ofc-validate OFC_CAPTURE=$(OFC_CAPTURE)
	@echo "✅ Валидация завершена"

ofc-fill-expected:
	@echo "📝 Заполнение expected полей в OFC capture..."
	@cd python-worker && make -f make.ofc.mk ofc-fill-expected OFC_CAPTURE=$(OFC_CAPTURE) OFC_GOLDEN=$(OFC_GOLDEN)
	@echo "✅ Заполнение завершено"

ofc-replay-strict:
	@echo "🔄 Строгий replay OFC golden..."
	@cd python-worker && make -f make.ofc.mk ofc-replay OFC_GOLDEN=$(OFC_GOLDEN) OFC_REPLAY_STRICT=1
	@echo "✅ Replay завершен"

ofc-replay-validate:
	@echo "🔍 Валидация детерминизма OFC replay..."
	@cd python-worker && python3 tools/ofc_replay_validate.py --ndjson $(OFC_CAPTURE) --max-rows $(OFC_REPLAY_MAX_ROWS)
	@echo "✅ Валидация завершена"

ofc-bench:
	@echo "⚡ Бенчмарк производительности OFC build..."
	@cd python-worker && make -f make.ofc.mk ofc-bench OFC_GOLDEN=$(OFC_GOLDEN) \
		OFC_BENCH_WARMUP=$(OFC_BENCH_WARMUP) OFC_BENCH_ITERS=$(OFC_BENCH_ITERS) \
		OFC_BENCH_MODE=$(OFC_BENCH_MODE) OFC_BUDGET_P95_US=$(OFC_BUDGET_P95_US) \
		OFC_BUDGET_P99_US=$(OFC_BUDGET_P99_US)
	@echo "✅ Бенчмарк завершен"

ofc-golden-suite:
	@echo "🎯 End-to-end OFC golden suite: validate → fill → replay → bench..."
	@cd python-worker && make -f make.ofc.mk ofc-golden-suite \
		OFC_CAPTURE=$(OFC_CAPTURE) OFC_GOLDEN=$(OFC_GOLDEN) \
		OFC_BENCH_WARMUP=$(OFC_BENCH_WARMUP) OFC_BENCH_ITERS=$(OFC_BENCH_ITERS) \
		OFC_BENCH_MODE=$(OFC_BENCH_MODE) OFC_BUDGET_P95_US=$(OFC_BUDGET_P95_US) \
		OFC_BUDGET_P99_US=$(OFC_BUDGET_P99_US)
	@echo "✅ Golden suite завершен"

ofc-golden-clean:
	@echo "🧹 Очистка golden файлов..."
	@cd python-worker && make -f make.ofc.mk ofc-golden-clean OFC_GOLDEN=$(OFC_GOLDEN)
	@echo "✅ Очистка завершена"

# ═══════════════════════════════════════════════════════════════
# Tick Time Observability
# ═══════════════════════════════════════════════════════════════

tick-time-lag-report:
	@echo "📊 Tick time lag quantile report..."
	@cd python-worker && python3 -m tools.tick_time_lag_report \
		--n $${TICK_TIME_LAG_REPORT_N:-50000} \
		--stream-key $${TICK_TIME_STREAM_KEY:-metrics:tick_time} \
		$${TICK_TIME_LAG_REPORT_SYMBOL:+--symbol $${TICK_TIME_LAG_REPORT_SYMBOL}} \
		$${TICK_TIME_LAG_REPORT_JSON:+--json}
	@echo "✅ Отчет завершен"

tick-time-lag-report-json:
	@echo "📊 Tick time lag quantile report (JSON)..."
	@cd python-worker && python3 -m tools.tick_time_lag_report \
		--n $${TICK_TIME_LAG_REPORT_N:-50000} \
		--stream-key $${TICK_TIME_STREAM_KEY:-metrics:tick_time} \
		$${TICK_TIME_LAG_REPORT_SYMBOL:+--symbol $${TICK_TIME_LAG_REPORT_SYMBOL}} \
		--json

tick-time-lag-report-timer-status:
	@echo "⏰ Статус tick time lag report timer..."
	@$(DOCKER_COMPOSE_BIN) -f docker-compose-crypto-orderflow.yml ps tick-time-lag-report-timer || echo "⚠️  Timer не запущен"

tick-time-lag-report-timer-logs:
	@echo "📋 Логи tick time lag report timer..."
	@$(DOCKER_COMPOSE_BIN) -f docker-compose-crypto-orderflow.yml logs -f tick-time-lag-report-timer

tick-time-autotune:
	@echo "🔧 Running tick time autotune (policy threshold suggestions)..."
	@cd python-worker && python3 -m tools.tick_time_autotune \
		--redis $${REDIS_URL:-redis://redis-worker-1:6379/0} \
		--stream $${TICK_TIME_STREAM_KEY:-metrics:tick_time} \
		--count $${TICK_TIME_AUTOTUNE_COUNT:-50000} \
		$${TICK_TIME_AUTOTUNE_SYMBOL:+--symbol $${TICK_TIME_AUTOTUNE_SYMBOL}}

tick-time-autotune-timer-status:
	@echo "📊 Tick Time Autotune Timer Status:"
	@$(DOCKER_COMPOSE_BIN) -f docker-compose-crypto-orderflow.yml ps tick-time-autotune-timer || echo "⚠️  Timer не запущен"

tick-time-autotune-timer-logs:
	@echo "📋 Логи tick time autotune timer..."
	@$(DOCKER_COMPOSE_BIN) -f docker-compose-crypto-orderflow.yml logs -f tick-time-autotune-timer

# ═══════════════════════════════════════════════════════════════
# Edge Stack v1 OOF Training Pipeline
# ═══════════════════════════════════════════════════════════════

edge-stack-v1-build-dataset-small:
	@echo "📊 Building edge_stack_v1 train dataset (small sanity run, 5-20k messages)..."
	@export PYTHONPATH=./ml_analysis:$$PYTHONPATH && \
	python3 -m ml_analysis.tools.build_edge_stack_dataset_from_redis \
		--redis_url $${REDIS_URL:-redis://redis-worker-1:6379/0} \
		--signal_stream $${ML_REPLAY_STREAM:-ml_replay_inputs_v1} \
		--closed_stream $${TRADES_CLOSED_STREAM:-trades:closed} \
		--signals_count $${SIGNALS_COUNT_SMALL:-20000} \
		--closes_count $${CLOSES_COUNT_SMALL:-20000} \
		--y_min_r $${Y_MIN_R:-0.10} \
		--drop_invalid_risk $${DROP_INVALID_RISK:-1} \
		--diagnose_mismatch $${DIAGNOSE_MISMATCH:-1} \
		--max_examples $${MAX_EXAMPLES_SMALL:-50} \
		--out_jsonl $${EDGE_STACK_DATASET_OUT_SMALL:-/var/lib/trade/ml_models/edge_stack_v1_oof/edge_train.small.jsonl} \
		--out_quarantine_jsonl $${EDGE_STACK_QUARANTINE_OUT_SMALL:-/var/lib/trade/ml_models/edge_stack_v1_oof/edge_quarantine.small.jsonl} \
		--out_report_json $${EDGE_STACK_REPORT_JSON_SMALL:-/var/lib/trade/ml_models/edge_stack_v1_oof/edge_dataset_report.small.json}
	@echo "✅ Small dataset build completed. Check edge_dataset_report.small.json for joined/drop stats."

edge-stack-v1-build-dataset:
	@echo "📊 Building edge_stack_v1 train dataset from Redis streams (ml_replay_inputs_v1 + trades:closed) v2..."
	@export PYTHONPATH=./ml_analysis:$$PYTHONPATH && \
	python3 -m ml_analysis.tools.build_edge_stack_dataset_from_redis \
		--redis_url $${REDIS_URL:-redis://redis-worker-1:6379/0} \
		--signal_stream $${ML_REPLAY_STREAM:-ml_replay_inputs_v1} \
		--closed_stream $${TRADES_CLOSED_STREAM:-trades:closed} \
		--signals_count $${SIGNALS_COUNT:-200000} \
		--closes_count $${CLOSES_COUNT:-200000} \
		--y_min_r $${Y_MIN_R:-0.10} \
		--drop_invalid_risk $${DROP_INVALID_RISK:-1} \
		--diagnose_mismatch $${DIAGNOSE_MISMATCH:-1} \
		--max_examples $${MAX_EXAMPLES_LARGE:-100} \
		--out_jsonl $${EDGE_STACK_DATASET_OUT:-/var/lib/trade/ml_models/edge_stack_v1_oof/edge_train.jsonl} \
		--emit_feature_cols_json $${EDGE_STACK_FEATURE_COLS_JSON:-/var/lib/trade/ml_models/edge_stack_v1_oof/feature_cols.json} \
		--out_quarantine_jsonl $${EDGE_STACK_QUARANTINE_OUT:-/var/lib/trade/ml_models/edge_stack_v1_oof/edge_quarantine.jsonl} \
		--out_report_json $${EDGE_STACK_REPORT_JSON:-/var/lib/trade/ml_models/edge_stack_v1_oof/edge_dataset_report.json}
	@echo "✅ Dataset build completed. Check edge_dataset_report.json for joined/drop/mismatch stats."

edge-stack-v1-train:
	@echo "🎓 Training edge_stack_v1 OOF model..."
	@export PYTHONPATH=./ml_analysis:$$PYTHONPATH && \
	python3 -m ml_analysis.tools.train_edge_stack_v1_oof \
		--data_jsonl $${EDGE_STACK_DATASET_PATH:-/var/lib/trade/ml_models/edge_stack_v1_oof/edge_train.jsonl} \
		--out_model $${EDGE_STACK_MODEL_OUT:-/var/lib/trade/ml_models/edge_stack_v1_oof/edge_stack_v1.joblib} \
		--feature_cols_json $${EDGE_STACK_FEATURE_COLS_JSON:-/var/lib/trade/ml_models/edge_stack_v1_oof/feature_cols.json} \
		--n_splits $${EDGE_STACK_N_SPLITS:-5} \
		--purge_ms $${EDGE_STACK_PURGE_MS:-300000} \
		--embargo_ms $${EDGE_STACK_EMBARGO_MS:-300000} \
		--min_train $${EDGE_STACK_MIN_TRAIN:-500} \
		--calibrate $${EDGE_STACK_CALIBRATE:-1}
	@echo "✅ Training completed"

edge-stack-v1-dataset-timer-status:
	@echo "⏰ Статус edge_stack_v1 dataset builder timer..."
	@$(DOCKER_COMPOSE_BIN) -f docker-compose-crypto-orderflow.yml ps edge-stack-v1-dataset-timer || echo "⚠️  Timer не запущен"

edge-stack-v1-dataset-timer-logs:
	@echo "📋 Логи edge_stack_v1 dataset builder timer..."
	@$(DOCKER_COMPOSE_BIN) -f docker-compose-crypto-orderflow.yml logs -f edge-stack-v1-dataset-timer

edge-stack-v1-train-timer-status:
	@echo "⏰ Статус edge_stack_v1 training timer..."
	@$(DOCKER_COMPOSE_BIN) -f docker-compose-crypto-orderflow.yml ps ml-train-edge-stack-v1-oof-timer || echo "⚠️  Timer не запущен"

edge-stack-v1-train-timer-logs:
	@echo "📋 Логи edge_stack_v1 training timer..."
	@$(DOCKER_COMPOSE_BIN) -f docker-compose-crypto-orderflow.yml logs -f ml-train-edge-stack-v1-oof-timer

# ═══════════════════════════════════════════════════════════════
# v10 Triple-Barrier Labeling Pipeline
# ═══════════════════════════════════════════════════════════════

## v10-tb-pipeline: Полный пайплайн v10 triple-barrier labeling (экспорт тиков → лейблы → датасет)
## Параметры (можно переопределить через переменные окружения):
##   TICK_STREAM - Redis stream для тиков (если указан, используется вместо SYMBOLS)
##   SYMBOLS - Список символов через запятую (по умолчанию: BTCUSDT,ETHUSDT, используется если TICK_STREAM не указан)
##   SINCE_HOURS - Количество часов истории (по умолчанию: 24)
##   INPUTS_FILE - Путь к файлу inputs NDJSON (по умолчанию: /tmp/of_inputs.ndjson)
##   TICKS_FILE - Путь к файлу тиков NDJSON (по умолчанию: /tmp/ticks_24h.ndjson)
##   TB_LABELS_FILE - Путь к файлу лейблов NDJSON (по умолчанию: /tmp/tb_labels.ndjson)
##   DATASET_FILE - Путь к выходному parquet файлу (по умолчанию: /tmp/ml_dataset_tb.parquet)
##   H_MS - Горизонт в миллисекундах (по умолчанию: 180000 = 3 минуты)
##   TP_K_ATR - Множитель ATR для TP (по умолчанию: 1.0)
##   SL_K_ATR - Множитель ATR для SL (по умолчанию: 1.0)
##   REDIS_URL - URL Redis (по умолчанию: auto - сначала Docker Redis, затем localhost как fallback)
##   OF_INPUTS_STREAM - Redis stream для inputs (по умолчанию: signals:of:inputs)
##   OF_INPUTS_MAX_RECORDS - Максимальное количество записей для экспорта inputs (по умолчанию: 100000)
## Примечание: Если INPUTS_FILE не существует, он будет автоматически экспортирован из Redis
## Пример: make v10-tb-pipeline TICK_STREAM=stream:tick_BTCUSDT SYMBOLS=BTCUSDT INPUTS_FILE=/data/of_inputs.ndjson
v10-tb-pipeline:
	@echo "🚀 Запуск v10 Triple-Barrier Labeling Pipeline..."
	@echo ""
	@echo "📋 Параметры:"
	@echo "   TICK_STREAM: $${TICK_STREAM:-stream:tick_BTCUSDT}"
	@echo "   SYMBOLS: $${SYMBOLS:-BTCUSDT,ETHUSDT}"
	@echo "   SINCE_HOURS: $${SINCE_HOURS:-24}"
	@echo "   INPUTS_FILE: $${INPUTS_FILE:-/tmp/of_inputs.ndjson}"
	@echo "   TICKS_FILE: $${TICKS_FILE:-/tmp/ticks_24h.ndjson}"
	@echo "   TB_LABELS_FILE: $${TB_LABELS_FILE:-/tmp/tb_labels.ndjson}"
	@echo "   DATASET_FILE: $${DATASET_FILE:-/tmp/ml_dataset_tb.parquet}"
	@echo "   H_MS: $${H_MS:-180000}"
	@echo "   TP_K_ATR: $${TP_K_ATR:-1.0}"
	@echo "   SL_K_ATR: $${SL_K_ATR:-1.0}"
	@echo "   REDIS_URL: $${REDIS_URL:-auto (автоопределение)}"
	@echo "   OF_INPUTS_STREAM: $${OF_INPUTS_STREAM:-signals:of:inputs}"
	@echo ""
	@USE_DOCKER_INPUTS=0; \
	REDIS_URL_INPUTS=""; \
	if [ ! -f "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" ]; then \
		echo "⚠️  Файл inputs не найден: $${INPUTS_FILE:-/tmp/of_inputs.ndjson}"; \
		echo "📥 Шаг 0/4: Экспорт inputs из Redis stream: $${OF_INPUTS_STREAM:-signals:of:inputs}..."; \
		if [ -z "$$REDIS_URL" ] || [ "$$REDIS_URL" = "auto" ]; then \
			if docker exec scanner-python-worker python3 -c "import redis; r=redis.Redis.from_url('redis://redis-worker-1:6379/0', decode_responses=True, socket_connect_timeout=2); length=r.xlen('signals:of:inputs'); exit(0 if length > 0 else 1)" 2>/dev/null; then \
				REDIS_URL_INPUTS="redis://redis-worker-1:6379/0"; \
				USE_DOCKER_INPUTS=1; \
				echo "   ✅ Используется Docker Redis (основной источник данных)"; \
			elif python3 -c "import redis; r=redis.Redis.from_url('redis://localhost:6379/0', decode_responses=True, socket_connect_timeout=2); length=r.xlen('signals:of:inputs'); exit(0 if length > 0 else 1)" 2>/dev/null; then \
				REDIS_URL_INPUTS="redis://localhost:6379/0"; \
				echo "   ✅ Используется локальный Redis (fallback, данные найдены)"; \
			else \
				REDIS_URL_INPUTS="redis://redis-worker-1:6379/0"; \
				USE_DOCKER_INPUTS=1; \
				echo "   ⚠️  Docker Redis недоступен, пытаемся использовать как fallback"; \
			fi; \
		else \
			REDIS_URL_INPUTS="$$REDIS_URL"; \
			if echo "$$REDIS_URL" | grep -q "redis-worker-1"; then \
				USE_DOCKER_INPUTS=1; \
			fi; \
			echo "   ✅ Используется указанный REDIS_URL: $$REDIS_URL"; \
		fi; \
		if [ "$$USE_DOCKER_INPUTS" = "1" ]; then \
			docker exec -w /app/python-worker scanner-python-worker python3 -m tools.export_of_inputs_ndjson \
			--redis-url "$$REDIS_URL_INPUTS" \
			--stream "$${OF_INPUTS_STREAM:-signals:of:inputs}" \
			--out "/tmp/of_inputs.ndjson" \
			--max-records "$${OF_INPUTS_MAX_RECORDS:-100000}" \
			--batch 2000 2>&1 | grep -E "(OK:|ERROR:|written)" || true; \
			docker cp scanner-python-worker:/tmp/of_inputs.ndjson "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" 2>/dev/null || (echo "❌ Ошибка копирования файла из Docker" && exit 1); \
		else \
			cd python-worker && \
			PYTHONPATH=".:.." python -m tools.export_of_inputs_ndjson \
			--redis-url "$$REDIS_URL_INPUTS" \
			--stream "$${OF_INPUTS_STREAM:-signals:of:inputs}" \
			--out "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" \
			--max-records "$${OF_INPUTS_MAX_RECORDS:-100000}" \
			--batch 2000 2>&1 | tee /tmp/export_inputs.log || (echo "❌ Ошибка экспорта inputs. Проверьте логи выше." && exit 1); \
		fi; \
		if [ ! -f "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" ] || [ ! -s "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" ]; then \
			echo ""; \
			echo "❌ Файл inputs пуст или не создан после экспорта."; \
			echo ""; \
			echo "💡 Возможные причины:"; \
			echo "   1. В Redis stream '$${OF_INPUTS_STREAM:-signals:of:inputs}' нет данных (0 записей)"; \
			echo "   2. Stream не существует или имеет другое имя"; \
			echo "   3. Redis недоступен или используется другой URL"; \
			echo "   4. Данные находятся в Docker Redis (требуется доступ из контейнера)"; \
			echo ""; \
			echo "🔧 Решения:"; \
			echo "   - Экспортируйте из Docker Redis: make v10-tb-export-from-docker"; \
			echo "   - Проверьте наличие данных: make v10-tb-check-redis"; \
			echo "   - Проверьте Docker Redis: docker exec redis-worker-1 redis-cli XLEN $${OF_INPUTS_STREAM:-signals:of:inputs}"; \
			echo "   - Укажите другой stream: OF_INPUTS_STREAM=stream:of:inputs make v10-tb-pipeline"; \
			echo "   - Укажите существующий файл: INPUTS_FILE=/path/to/existing_inputs.ndjson make v10-tb-pipeline"; \
			exit 1; \
		fi; \
		RECORDS_COUNT=$$(wc -l < "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" 2>/dev/null || echo "0"); \
		echo "✅ Шаг 0 завершён: экспортировано $$RECORDS_COUNT записей в $${INPUTS_FILE:-/tmp/of_inputs.ndjson}"; \
		echo ""; \
	fi
	@if [ ! -f "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" ] || [ ! -s "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" ]; then \
		echo "❌ Файл inputs не существует или пуст: $${INPUTS_FILE:-/tmp/of_inputs.ndjson}"; \
		echo ""; \
		echo "💡 Решения:"; \
		echo "   - Экспортируйте inputs вручную: make v10-tb-export-inputs"; \
		echo "   - Укажите существующий файл: INPUTS_FILE=/path/to/inputs.ndjson make v10-tb-pipeline"; \
		echo "   - Проверьте наличие данных в Redis stream"; \
		exit 1; \
	fi
	@echo "📊 Шаг 1/3: Экспорт тиков/баров в NDJSON..."
	@USE_DOCKER_TICKS=0; \
	REDIS_URL_TICKS=""; \
	if [ -z "$$REDIS_URL" ] || [ "$$REDIS_URL" = "auto" ]; then \
		echo "   🔍 Автоопределение источника данных для тиков..."; \
		if docker exec scanner-python-worker python3 -c "import redis; r=redis.Redis.from_url('redis://redis-worker-1:6379/0', decode_responses=True, socket_connect_timeout=2); length=r.xlen('stream:tick_BTCUSDT'); exit(0 if length > 0 else 1)" 2>/dev/null; then \
			REDIS_URL_TICKS="redis://redis-worker-1:6379/0"; \
			USE_DOCKER_TICKS=1; \
			echo "   ✅ Используется Docker Redis для тиков (основной источник данных)"; \
		elif python3 -c "import redis; r=redis.Redis.from_url('redis://localhost:6379/0', decode_responses=True, socket_connect_timeout=2); length=r.xlen('stream:tick_BTCUSDT'); exit(0 if length > 0 else 1)" 2>/dev/null; then \
			REDIS_URL_TICKS="redis://localhost:6379/0"; \
			echo "   ✅ Используется локальный Redis для тиков (fallback, данные найдены)"; \
		else \
			REDIS_URL_TICKS="redis://redis-worker-1:6379/0"; \
			USE_DOCKER_TICKS=1; \
			echo "   ⚠️  Docker Redis недоступен, пытаемся использовать как fallback"; \
		fi; \
	else \
		REDIS_URL_TICKS="$$REDIS_URL"; \
		if echo "$$REDIS_URL" | grep -q "redis-worker-1"; then \
			USE_DOCKER_TICKS=1; \
		fi; \
		echo "   ✅ Используется указанный REDIS_URL: $$REDIS_URL"; \
	fi; \
	if [ "$$USE_DOCKER_TICKS" = "1" ]; then \
		echo "   🐳 Экспорт из Docker Redis через scanner-python-worker..."; \
		if [ -n "$${TICK_STREAM:-}" ]; then \
			docker exec -w /app/python-worker scanner-python-worker python3 -m tools.export_ticks_window_ndjson_v1 \
			--redis-url "$$REDIS_URL_TICKS" \
			--stream "$$TICK_STREAM" \
			--since-hours "$${SINCE_HOURS:-24}" \
			--out "/tmp/ticks_24h.ndjson" \
			--payload-field data \
			--ts-field ts \
			--price-field mid \
			--symbol-field symbol \
			--max-scan 800000 2>&1 | tail -1 || (echo "❌ Ошибка экспорта тиков из Docker" && exit 1); \
			docker cp scanner-python-worker:/tmp/ticks_24h.ndjson "$${TICKS_FILE:-/tmp/ticks_24h.ndjson}" 2>/dev/null || (echo "❌ Ошибка копирования файла из Docker" && exit 1); \
		else \
			docker exec -w /app/python-worker scanner-python-worker python3 -m tools.export_ticks_window_ndjson_v1 \
			--redis-url "$$REDIS_URL_TICKS" \
			--symbols "$${SYMBOLS:-BTCUSDT,ETHUSDT}" \
			--since-hours "$${SINCE_HOURS:-24}" \
			--out "/tmp/ticks_24h.ndjson" \
			--payload-field data \
			--ts-field ts \
			--price-field mid \
			--symbol-field symbol \
			--max-scan 800000 2>&1 | tail -1 || (echo "❌ Ошибка экспорта тиков из Docker" && exit 1); \
			docker cp scanner-python-worker:/tmp/ticks_24h.ndjson "$${TICKS_FILE:-/tmp/ticks_24h.ndjson}" 2>/dev/null || (echo "❌ Ошибка копирования файла из Docker" && exit 1); \
		fi; \
	else \
		echo "   📡 Экспорт из локального Redis..."; \
		cd python-worker && \
		if [ -n "$${TICK_STREAM:-}" ]; then \
			PYTHONPATH=".:.." python -m tools.export_ticks_window_ndjson_v1 \
			--redis-url "$$REDIS_URL_TICKS" \
			--stream "$$TICK_STREAM" \
			--since-hours "$${SINCE_HOURS:-24}" \
			--out "$${TICKS_FILE:-/tmp/ticks_24h.ndjson}" \
			--payload-field data \
			--ts-field ts \
			--price-field mid \
			--symbol-field symbol \
			--max-scan 800000 || (echo "❌ Ошибка экспорта тиков из локального Redis" && exit 1); \
		else \
			PYTHONPATH=".:.." python -m tools.export_ticks_window_ndjson_v1 \
			--redis-url "$$REDIS_URL_TICKS" \
			--symbols "$${SYMBOLS:-BTCUSDT,ETHUSDT}" \
			--since-hours "$${SINCE_HOURS:-24}" \
			--out "$${TICKS_FILE:-/tmp/ticks_24h.ndjson}" \
			--payload-field data \
			--ts-field ts \
			--price-field mid \
			--symbol-field symbol \
			--max-scan 800000 || (echo "❌ Ошибка экспорта тиков из локального Redis" && exit 1); \
		fi; \
	fi
	@if [ ! -f "$${TICKS_FILE:-/tmp/ticks_24h.ndjson}" ] || [ ! -s "$${TICKS_FILE:-/tmp/ticks_24h.ndjson}" ]; then \
		echo ""; \
		echo "⚠️  Файл тиков пуст или не создан."; \
		echo ""; \
		echo "💡 Возможные причины:"; \
		echo "   1. В Redis streams нет данных для указанных символов"; \
		echo "   2. Streams не существуют (например, stream:tick_BTCUSDT)"; \
		echo "   3. Данные находятся в Docker Redis"; \
		echo ""; \
		echo "🔧 Решения:"; \
		echo "   - Проверьте наличие данных: make v10-tb-check-redis"; \
		echo "   - Проверьте Docker Redis: docker exec redis-worker-1 redis-cli XLEN stream:tick_BTCUSDT"; \
		echo "   - Укажите другие символы: SYMBOLS=SYMBOL1,SYMBOL2 make v10-tb-pipeline"; \
		echo "   - Укажите конкретный stream: TICK_STREAM=stream:tick_SYMBOL make v10-tb-pipeline"; \
		echo "   - Экспортируйте тики из Docker Redis вручную"; \
		exit 1; \
	fi
	@echo "✅ Шаг 1 завершён: тики экспортированы в $${TICKS_FILE:-/tmp/ticks_24h.ndjson}"
	@echo ""
	@echo "🏷️  Шаг 2/3: Генерация лейблов triple-barrier..."
	@cd python-worker && \
		PYTHONPATH=".:.." python -m tools.label_triple_barrier_from_ticks_v1 \
		--inputs "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" \
		--ticks "$${TICKS_FILE:-/tmp/ticks_24h.ndjson}" \
		--out "$${TB_LABELS_FILE:-/tmp/tb_labels.ndjson}" \
		--h-ms "$${H_MS:-180000}" \
		--tp-k-atr "$${TP_K_ATR:-1.0}" \
		--sl-k-atr "$${SL_K_ATR:-1.0}" \
		--fallback-tp-bps 30 \
		--fallback-sl-bps 30 \
		--adv-max 1.2 || (echo "❌ Ошибка генерации лейблов" && exit 1)
	@echo "✅ Шаг 2 завершён: лейблы сгенерированы в $${TB_LABELS_FILE:-/tmp/tb_labels.ndjson}"
	@echo ""
	@echo "📦 Шаг 3/3: Построение датасета для обучения..."
	@cd python-worker && \
		PYTHONPATH=".:.." python -m tools.build_dataset_from_inputs_outcomes_v4_tb \
		--inputs "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" \
		--tb-labels "$${TB_LABELS_FILE:-/tmp/tb_labels.ndjson}" \
		--out "$${DATASET_FILE:-/tmp/ml_dataset_tb.parquet}" || (echo "❌ Ошибка построения датасета" && exit 1)
	@echo "✅ Шаг 3 завершён: датасет создан в $${DATASET_FILE:-/tmp/ml_dataset_tb.parquet}"
	@echo ""
	@echo "🎉 Пайплайн v10 triple-barrier labeling успешно завершён!"
	@echo ""
	@echo "📁 Результирующие файлы:"
	@echo "   - Тики: $${TICKS_FILE:-/tmp/ticks_24h.ndjson}"
	@echo "   - Лейблы: $${TB_LABELS_FILE:-/tmp/tb_labels.ndjson}"
	@echo "   - Датасет: $${DATASET_FILE:-/tmp/ml_dataset_tb.parquet}"

## v10-tb-check-redis: Проверка наличия данных в Redis streams
v10-tb-check-redis:
	@echo "🔍 Проверка наличия данных в Redis streams..."
	@echo ""
	@echo "🔌 Подключение к Redis: $${REDIS_URL:-redis://localhost:6379/0}"
	@python3 -c "import redis; r=redis.Redis.from_url('$${REDIS_URL:-redis://localhost:6379/0}', decode_responses=True, socket_connect_timeout=2); r.ping(); print('   ✅ Redis доступен')" 2>/dev/null || (echo "   ❌ Redis недоступен. Проверьте REDIS_URL или запустите Redis." && exit 1)
	@echo ""
	@echo "📊 Stream inputs: $${OF_INPUTS_STREAM:-signals:of:inputs}"
	@python3 -c "import redis, sys; r=redis.Redis.from_url('$${REDIS_URL:-redis://localhost:6379/0}', decode_responses=True, socket_connect_timeout=2); stream='$${OF_INPUTS_STREAM:-signals:of:inputs}'; length=r.xlen(stream); print(f'   Записей: {length}'); print('   ✅ Stream существует и содержит данные' if length > 0 else '   ⚠️  Stream пуст или не существует'); [print(f'   💡 Найден альтернативный stream \"{alt}\" с {r.xlen(alt)} записями') for alt in ['stream:of:inputs', 'signals:of:inputs'] if alt != stream and r.xlen(alt) > 0]" 2>/dev/null || echo "   ❌ Ошибка проверки stream"
	@echo ""
	@echo "📊 Тики (проверка для символов из SYMBOLS):"
	@python3 -c "import redis; r=redis.Redis.from_url('$${REDIS_URL:-redis://localhost:6379/0}', decode_responses=True, socket_connect_timeout=2); symbols=[s.strip().upper() for s in '$${SYMBOLS:-BTCUSDT,ETHUSDT}'.split(',') if s.strip()]; found_any=False; [print(f'   {\"✅\" if (length:=r.xlen(f\"stream:tick_{sym}\")) > 0 else \"⚠️\"} stream:tick_{sym}: {length} записей') or (found_any := True if length > 0 else found_any) for sym in symbols]; print('   💡 Попробуйте проверить другие streams или укажите другие SYMBOLS') if not found_any else None" 2>/dev/null || echo "   ❌ Ошибка проверки тиков"
	@echo ""
	@echo ""
	@echo "🐳 Проверка Docker Redis (если локальный Redis пуст):"
	@docker exec redis-worker-1 redis-cli XLEN signals:of:inputs 2>/dev/null | awk '{if ($$1 > 0) print "   ✅ Docker Redis signals:of:inputs: " $$1 " записей"; else print "   ⚠️  Docker Redis signals:of:inputs: пуст"}' || echo "   ⚠️  Docker Redis недоступен"
	@docker exec redis-worker-1 redis-cli XLEN stream:tick_BTCUSDT 2>/dev/null | awk '{if ($$1 > 0) print "   ✅ Docker Redis stream:tick_BTCUSDT: " $$1 " записей"; else print "   ⚠️  Docker Redis stream:tick_BTCUSDT: пуст"}' || echo "   ⚠️  Docker Redis недоступен"
	@echo ""
	@echo "💡 Полезные команды:"
	@echo "   - Использовать Docker Redis: docker exec redis-worker-1 redis-cli XLEN signals:of:inputs"
	@echo "   - Экспорт из Docker: docker exec redis-worker-1 redis-cli --raw XRANGE signals:of:inputs - + COUNT 10"
	@echo "   - Настроить проброс портов Docker Redis для локального доступа"

## v10-tb-export-inputs: Экспорт inputs из Redis (шаг 0, опционально)
v10-tb-export-inputs:
	@echo "📥 Экспорт inputs из Redis stream: $${OF_INPUTS_STREAM:-signals:of:inputs}..."
	@$(DOCKER_COMPOSE_BIN) -f docker-compose.yml -f docker-compose-utilities.yml run --rm tb-pipeline-runner \
		python -m tools.export_of_inputs_ndjson \
		--redis-url "$${REDIS_URL:-redis://redis-worker-1:6379/0}" \
		--stream "$${OF_INPUTS_STREAM:-signals:of:inputs}" \
		--out "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" \
		--max-records "$${OF_INPUTS_MAX_RECORDS:-100000}" \
		--batch 2000
	@if [ -f "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" ] && [ -s "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" ]; then \
		RECORDS_COUNT=$$(wc -l < "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" 2>/dev/null || echo "0"); \
		echo "✅ Экспортировано $$RECORDS_COUNT записей в $${INPUTS_FILE:-/tmp/of_inputs.ndjson}"; \
	else \
		echo "⚠️  Файл пуст или не создан. Проверьте наличие данных в stream."; \
		echo "   Используйте: make v10-tb-check-redis"; \
	fi

## v10-tb-export-from-docker: Экспорт inputs и тиков из Docker Redis
v10-tb-export-from-docker:
	@echo "🐳 Экспорт данных из Docker Redis..."
	@echo ""
	@echo "📥 Шаг 1/2: Экспорт inputs из Docker Redis..."
	@echo "   Используется: через контейнер tb-pipeline-runner (docker-compose-utilities.yml)"; \
	$(DOCKER_COMPOSE_BIN) -f docker-compose.yml -f docker-compose-utilities.yml run --rm tb-pipeline-runner \
		python -m tools.export_of_inputs_ndjson \
		--redis-url "redis://redis-worker-1:6379/0" \
		--stream "$${OF_INPUTS_STREAM:-signals:of:inputs}" \
		--out "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" \
		--max-records "$${OF_INPUTS_MAX_RECORDS:-100000}" \
		--batch 2000;
	else \
		echo "❌ Не удалось подключиться к Docker Redis."; \
		echo "   Попробуйте:"; \
		echo "   1. Убедитесь, что контейнеры redis-worker-1 и scanner-python-worker запущены"; \
		echo "   2. Или используйте проброс портов Docker Redis на localhost"; \
		exit 1; \
	fi || (echo "❌ Ошибка экспорта inputs." && exit 1)
	@if [ -f "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" ] && [ -s "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" ]; then \
		RECORDS_COUNT=$$(wc -l < "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" 2>/dev/null || echo "0"); \
		echo "✅ Экспортировано $$RECORDS_COUNT записей inputs в $${INPUTS_FILE:-/tmp/of_inputs.ndjson}"; \
	else \
		echo "⚠️  Файл inputs пуст или не создан"; \
		exit 1; \
	fi
	@echo ""
	@echo "📥 Шаг 2/2: Экспорт тиков из Docker Redis..."
	@if python3 -c "import redis; r=redis.Redis.from_url('redis://redis-worker-1:6379/0', decode_responses=True, socket_connect_timeout=2); r.ping()" 2>/dev/null; then \
		cd python-worker && \
		if [ -n "$${TICK_STREAM:-}" ]; then \
			PYTHONPATH=".:.." python -m tools.export_ticks_window_ndjson_v1 \
			--redis-url "redis://redis-worker-1:6379/0" \
			--stream "$$TICK_STREAM" \
			--since-hours "$${SINCE_HOURS:-24}" \
			--out "$${TICKS_FILE:-/tmp/ticks_24h.ndjson}" \
			--payload-field data \
			--ts-field ts \
			--price-field mid \
			--symbol-field symbol \
			--max-scan 800000; \
		else \
			PYTHONPATH=".:.." python -m tools.export_ticks_window_ndjson_v1 \
			--redis-url "redis://redis-worker-1:6379/0" \
			--symbols "$${SYMBOLS:-BTCUSDT,ETHUSDT}" \
			--since-hours "$${SINCE_HOURS:-24}" \
			--out "$${TICKS_FILE:-/tmp/ticks_24h.ndjson}" \
			--payload-field data \
			--ts-field ts \
			--price-field mid \
			--symbol-field symbol \
			--max-scan 800000; \
		fi; \
	elif docker exec scanner-python-worker python3 -c "import redis; r=redis.Redis.from_url('redis://redis-worker-1:6379/0', decode_responses=True, socket_connect_timeout=2); r.ping()" 2>/dev/null; then \
		if [ -n "$${TICK_STREAM:-}" ]; then \
			docker exec -w /app/python-worker scanner-python-worker python3 -m tools.export_ticks_window_ndjson_v1 \
			--redis-url "redis://redis-worker-1:6379/0" \
			--stream "$$TICK_STREAM" \
			--since-hours "$${SINCE_HOURS:-24}" \
			--out "/tmp/ticks_24h.ndjson" \
			--payload-field data \
			--ts-field ts \
			--price-field mid \
			--symbol-field symbol \
			--max-scan 800000 && \
			docker cp scanner-python-worker:/tmp/ticks_24h.ndjson "$${TICKS_FILE:-/tmp/ticks_24h.ndjson}" 2>/dev/null; \
		else \
			docker exec -w /app/python-worker scanner-python-worker python3 -m tools.export_ticks_window_ndjson_v1 \
			--redis-url "redis://redis-worker-1:6379/0" \
			--symbols "$${SYMBOLS:-BTCUSDT,ETHUSDT}" \
			--since-hours "$${SINCE_HOURS:-24}" \
			--out "/tmp/ticks_24h.ndjson" \
			--payload-field data \
			--ts-field ts \
			--price-field mid \
			--symbol-field symbol \
			--max-scan 800000 && \
			docker cp scanner-python-worker:/tmp/ticks_24h.ndjson "$${TICKS_FILE:-/tmp/ticks_24h.ndjson}" 2>/dev/null; \
		fi; \
	else \
		echo "❌ Не удалось подключиться к Docker Redis."; \
		exit 1; \
	fi || (echo "❌ Ошибка экспорта тиков." && exit 1)
	@if [ -f "$${TICKS_FILE:-/tmp/ticks_24h.ndjson}" ] && [ -s "$${TICKS_FILE:-/tmp/ticks_24h.ndjson}" ]; then \
		RECORDS_COUNT=$$(wc -l < "$${TICKS_FILE:-/tmp/ticks_24h.ndjson}" 2>/dev/null || echo "0"); \
		echo "✅ Экспортировано $$RECORDS_COUNT записей тиков в $${TICKS_FILE:-/tmp/ticks_24h.ndjson}"; \
	else \
		echo "⚠️  Файл тиков пуст или не создан"; \
		exit 1; \
	fi
	@echo ""
	@echo "🎉 Экспорт из Docker Redis завершён!"
	@echo "   Теперь можно запустить: make v10-tb-pipeline"

## v10-tb-export-ticks: Только экспорт тиков (шаг 1)
v10-tb-export-ticks:
	@echo "📊 Экспорт тиков/баров в NDJSON..."
	@cd python-worker && \
		if [ -n "$${TICK_STREAM:-}" ]; then \
			PYTHONPATH=".:.." python -m tools.export_ticks_window_ndjson_v1 \
			--redis-url "$${REDIS_URL:-redis://localhost:6379/0}" \
			--stream "$$TICK_STREAM" \
			--since-hours "$${SINCE_HOURS:-24}" \
			--out "$${TICKS_FILE:-/tmp/ticks_24h.ndjson}" \
			--payload-field data \
			--ts-field ts \
			--price-field mid \
			--symbol-field symbol \
			--max-scan 800000; \
		else \
			PYTHONPATH=".:.." python -m tools.export_ticks_window_ndjson_v1 \
			--redis-url "$${REDIS_URL:-redis://localhost:6379/0}" \
			--symbols "$${SYMBOLS:-BTCUSDT,ETHUSDT}" \
			--since-hours "$${SINCE_HOURS:-24}" \
			--out "$${TICKS_FILE:-/tmp/ticks_24h.ndjson}" \
			--payload-field data \
			--ts-field ts \
			--price-field mid \
			--symbol-field symbol \
			--max-scan 800000; \
		fi

## v10-tb-label: Только генерация лейблов (шаг 2)
v10-tb-label:
	@echo "🏷️  Генерация лейблов triple-barrier..."
	@$(DOCKER_COMPOSE_BIN) -f docker-compose.yml -f docker-compose-utilities.yml run --rm tb-pipeline-runner \
		python -m tools.label_triple_barrier_from_ticks_v1 \
		--inputs "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" \
		--ticks "$${TICKS_FILE:-/tmp/ticks_24h.ndjson}" \
		--out "$${TB_LABELS_FILE:-/tmp/tb_labels.ndjson}" \
		--h-ms "$${H_MS:-180000}" \
		--tp-k-atr "$${TP_K_ATR:-1.0}" \
		--sl-k-atr "$${SL_K_ATR:-1.0}" \
		--fallback-tp-bps 30 \
		--fallback-sl-bps 30 \
		--adv-max 1.2

## v10-tb-build-dataset: Только построение датасета (шаг 3)
v10-tb-build-dataset:
	@echo "📦 Построение датасета для обучения..."
	@$(DOCKER_COMPOSE_BIN) -f docker-compose.yml -f docker-compose-utilities.yml run --rm tb-pipeline-runner \
		python -m tools.build_dataset_from_inputs_outcomes_v4_tb \
		--inputs "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" \
		--tb-labels "$${TB_LABELS_FILE:-/tmp/tb_labels.ndjson}" \
		--out "$${DATASET_FILE:-/tmp/ml_dataset_tb.parquet}"

## v10-tb-label-redis: Генерация лейблов triple-barrier напрямую из Redis Stream тиков (v10)
## Параметры:
##   INPUTS_FILE - Путь к файлу inputs NDJSON (по умолчанию: /tmp/of_inputs.ndjson)
##   TB_LABELS_FILE - Путь к файлу лейблов NDJSON (по умолчанию: /tmp/of_inputs.tb_labeled.ndjson)
##   REDIS_URL - URL Redis (по умолчанию: redis://redis-worker-1:6379/0)
##   TICK_STREAM_PREFIX - Префикс для Redis Stream тиков (по умолчанию: stream:tick_)
##   SINCE_HOURS - Количество часов истории (по умолчанию: 168 = 7 дней)
##   MAX_ROWS_PER_SYMBOL - Максимальное количество тиков на символ (по умолчанию: 3000000)
##   H_MS - Горизонт в миллисекундах (по умолчанию: 180000 = 3 минуты)
##   TP_K_ATR - Множитель ATR для TP (по умолчанию: 1.0)
##   SL_K_ATR - Множитель ATR для SL (по умолчанию: 1.0)
##   FALLBACK_TP_BPS - Fallback TP в базисных пунктах (по умолчанию: 30.0)
##   FALLBACK_SL_BPS - Fallback SL в базисных пунктах (по умолчанию: 30.0)
## Пример: make v10-tb-label-redis INPUTS_FILE=/tmp/of_inputs.ndjson SINCE_HOURS=168
v10-tb-label-redis:
	@echo "🏷️  Генерация лейблов triple-barrier из Redis Stream тиков (v10)..."
	@echo ""
	@echo "📋 Параметры:"
	@echo "   INPUTS_FILE: $${INPUTS_FILE:-/tmp/of_inputs.ndjson}"
	@echo "   TB_LABELS_FILE: $${TB_LABELS_FILE:-/tmp/of_inputs.tb_labeled.ndjson}"
	@echo "   REDIS_URL: $${REDIS_URL:-redis://redis-worker-1:6379/0}"
	@echo "   TICK_STREAM_PREFIX: $${TICK_STREAM_PREFIX:-stream:tick_}"
	@echo "   SINCE_HOURS: $${SINCE_HOURS:-168}"
	@echo "   MAX_ROWS_PER_SYMBOL: $${MAX_ROWS_PER_SYMBOL:-3000000}"
	@echo "   H_MS: $${H_MS:-180000}"
	@echo "   TP_K_ATR: $${TP_K_ATR:-1.0}"
	@echo "   SL_K_ATR: $${SL_K_ATR:-1.0}"
	@echo "   FALLBACK_TP_BPS: $${FALLBACK_TP_BPS:-30.0}"
	@echo "   FALLBACK_SL_BPS: $${FALLBACK_SL_BPS:-30.0}"
	@echo ""
	@if [ ! -f "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" ] || [ ! -s "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" ]; then \
		echo "❌ Файл inputs не существует или пуст: $${INPUTS_FILE:-/tmp/of_inputs.ndjson}"; \
		echo ""; \
		echo "💡 Решения:"; \
		echo "   - Экспортируйте inputs: make v10-tb-export-inputs"; \
		echo "   - Или используйте полный пайплайн: make v10-tb-pipeline-redis"; \
		exit 1; \
	fi
	@$(DOCKER_COMPOSE_BIN) -f docker-compose.yml -f docker-compose-utilities.yml run --rm tb-pipeline-runner \
		python -m tools.label_triple_barrier_from_redis_ticks_v10 \
		--inputs "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" \
		--out "$${TB_LABELS_FILE:-/tmp/of_inputs.tb_labeled.ndjson}" \
		--ticks-redis-url "$${REDIS_URL:-redis://redis-worker-1:6379/0}" \
		--tick-stream-prefix "$${TICK_STREAM_PREFIX:-stream:tick_}" \
		--since-hours "$${SINCE_HOURS:-168}" \
		--max-rows-per-symbol "$${MAX_ROWS_PER_SYMBOL:-3000000}" \
		--h-ms "$${H_MS:-180000}" \
		--tp-k-atr "$${TP_K_ATR:-1.0}" \
		--sl-k-atr "$${SL_K_ATR:-1.0}" \
		--fallback-tp-bps "$${FALLBACK_TP_BPS:-30.0}" \
		--fallback-sl-bps "$${FALLBACK_SL_BPS:-30.0}" || (echo "❌ Ошибка генерации лейблов" && exit 1)
	@if [ -f "$${TB_LABELS_FILE:-/tmp/of_inputs.tb_labeled.ndjson}" ] && [ -s "$${TB_LABELS_FILE:-/tmp/of_inputs.tb_labeled.ndjson}" ]; then \
		RECORDS_COUNT=$$(wc -l < "$${TB_LABELS_FILE:-/tmp/of_inputs.tb_labeled.ndjson}" 2>/dev/null || echo "0"); \
		echo ""; \
		echo "✅ Лейблы сгенерированы: $$RECORDS_COUNT записей в $${TB_LABELS_FILE:-/tmp/of_inputs.tb_labeled.ndjson}"; \
	else \
		echo ""; \
		echo "❌ Файл лейблов пуст или не создан"; \
		exit 1; \
	fi

## v10-tb-pipeline-redis: Полный пайплайн v10 triple-barrier labeling из Redis Stream тиков
## Параметры (можно переопределить через переменные окружения):
##   INPUTS_FILE - Путь к файлу inputs NDJSON (по умолчанию: /tmp/of_inputs.ndjson)
##   TB_LABELS_FILE - Путь к файлу лейблов NDJSON (по умолчанию: /tmp/of_inputs.tb_labeled.ndjson)
##   DATASET_FILE - Путь к выходному parquet файлу (по умолчанию: /tmp/ml_dataset_tb.parquet)
##   REDIS_URL - URL Redis (по умолчанию: redis://redis-worker-1:6379/0)
##   TICK_STREAM_PREFIX - Префикс для Redis Stream тиков (по умолчанию: stream:tick_)
##   SINCE_HOURS - Количество часов истории (по умолчанию: 168 = 7 дней)
##   MAX_ROWS_PER_SYMBOL - Максимальное количество тиков на символ (по умолчанию: 3000000)
##   H_MS - Горизонт в миллисекундах (по умолчанию: 180000 = 3 минуты)
##   TP_K_ATR - Множитель ATR для TP (по умолчанию: 1.0)
##   SL_K_ATR - Множитель ATR для SL (по умолчанию: 1.0)
##   FALLBACK_TP_BPS - Fallback TP в базисных пунктах (по умолчанию: 30.0)
##   FALLBACK_SL_BPS - Fallback SL в базисных пунктах (по умолчанию: 30.0)
##   OF_INPUTS_STREAM - Redis stream для inputs (по умолчанию: signals:of:inputs)
##   OF_INPUTS_MAX_SCAN - Максимальное количество записей для экспорта inputs (по умолчанию: 800000)
## Примечание: Если INPUTS_FILE не существует, он будет автоматически экспортирован из Redis
## Пример: make v10-tb-pipeline-redis SINCE_HOURS=168 MAX_ROWS_PER_SYMBOL=3000000
v10-tb-pipeline-redis:
	@echo "🚀 Запуск v10 Triple-Barrier Labeling Pipeline из Redis Stream тиков (Docker)..."
	@echo ""
	@echo "📋 Параметры:"
	@echo "   INPUTS_FILE: $${INPUTS_FILE:-/tmp/of_inputs.ndjson}"
	@echo "   TB_LABELS_FILE: $${TB_LABELS_FILE:-/tmp/of_inputs.tb_labeled.ndjson}"
	@echo "   DATASET_FILE: $${DATASET_FILE:-/tmp/ml_dataset_tb.parquet}"
	@echo "   REDIS_URL: $${REDIS_URL:-redis://redis-worker-1:6379/0}"
	@echo "   TICK_STREAM_PREFIX: $${TICK_STREAM_PREFIX:-stream:tick_}"
	@echo "   SINCE_HOURS: $${SINCE_HOURS:-168}"
	@echo "   MAX_ROWS_PER_SYMBOL: $${MAX_ROWS_PER_SYMBOL:-3000000}"
	@echo "   H_MS: $${H_MS:-180000}"
	@echo "   TP_K_ATR: $${TP_K_ATR:-1.0}"
	@echo "   SL_K_ATR: $${SL_K_ATR:-1.0}"
	@echo "   FALLBACK_TP_BPS: $${FALLBACK_TP_BPS:-30.0}"
	@echo "   FALLBACK_SL_BPS: $${FALLBACK_SL_BPS:-30.0}"
	@echo "   OF_INPUTS_STREAM: $${OF_INPUTS_STREAM:-signals:of:inputs}"
	@echo "   OF_INPUTS_MAX_SCAN: $${OF_INPUTS_MAX_SCAN:-800000}"
	@echo ""
	@if [ ! -f "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" ] || [ ! -s "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" ]; then \
		echo "⚠️  Файл inputs не найден: $${INPUTS_FILE:-/tmp/of_inputs.ndjson}"; \
		echo "📥 Шаг 0/3: Экспорт inputs из Redis stream: $${OF_INPUTS_STREAM:-signals:of:inputs}..."; \
		$(DOCKER_COMPOSE_BIN) -f docker-compose.yml -f docker-compose-utilities.yml run --rm tb-pipeline-runner \
		python -m tools.export_of_inputs_ndjson \
		--out "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" \
		--since-hours "$${SINCE_HOURS:-168}" \
		--max-scan "$${OF_INPUTS_MAX_SCAN:-800000}" || (echo "❌ Ошибка экспорта inputs" && exit 1); \
		if [ ! -f "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" ] || [ ! -s "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" ]; then \
			echo ""; \
			echo "❌ Файл inputs пуст или не создан после экспорта."; \
			echo ""; \
			echo "💡 Возможные причины:"; \
			echo "   1. В Redis stream '$${OF_INPUTS_STREAM:-signals:of:inputs}' нет данных"; \
			echo "   2. Stream не существует или имеет другое имя"; \
			echo "   3. Redis недоступен"; \
			echo ""; \
			echo "🔧 Решения:"; \
			echo "   - Проверьте наличие данных: make v10-tb-check-redis"; \
			echo "   - Укажите другой stream: OF_INPUTS_STREAM=stream:of:inputs make v10-tb-pipeline-redis"; \
			echo "   - Укажите существующий файл: INPUTS_FILE=/path/to/existing_inputs.ndjson make v10-tb-pipeline-redis"; \
			exit 1; \
		fi; \
		RECORDS_COUNT=$$(wc -l < "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" 2>/dev/null || echo "0"); \
		echo "✅ Шаг 0 завершён: экспортировано $$RECORDS_COUNT записей в $${INPUTS_FILE:-/tmp/of_inputs.ndjson}"; \
		echo ""; \
	fi
	@echo "🏷️  Шаг 1/3: Генерация лейблов triple-barrier из Redis Stream тиков..."
	@$(DOCKER_COMPOSE_BIN) -f docker-compose.yml -f docker-compose-utilities.yml run --rm tb-pipeline-runner \
		python -m tools.label_triple_barrier_from_redis_ticks_v10 \
		--inputs "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" \
		--out "$${TB_LABELS_FILE:-/tmp/of_inputs.tb_labeled.ndjson}" \
		--ticks-redis-url "$${REDIS_URL:-redis://redis-worker-1:6379/0}" \
		--tick-stream-prefix "$${TICK_STREAM_PREFIX:-stream:tick_}" \
		--since-hours "$${SINCE_HOURS:-168}" \
		--max-rows-per-symbol "$${MAX_ROWS_PER_SYMBOL:-3000000}" \
		--h-ms "$${H_MS:-180000}" \
		--tp-k-atr "$${TP_K_ATR:-1.0}" \
		--sl-k-atr "$${SL_K_ATR:-1.0}" \
		--fallback-tp-bps "$${FALLBACK_TP_BPS:-30.0}" \
		--fallback-sl-bps "$${FALLBACK_SL_BPS:-30.0}" || (echo "❌ Ошибка генерации лейблов" && exit 1)
	@echo "✅ Шаг 1 завершён: лейблы сгенерированы в $${TB_LABELS_FILE:-/tmp/of_inputs.tb_labeled.ndjson}"
	@echo ""
	@echo "📦 Шаг 2/3: Построение датасета для обучения..."
	@$(DOCKER_COMPOSE_BIN) -f docker-compose.yml -f docker-compose-utilities.yml run --rm tb-pipeline-runner \
		python -m tools.build_dataset_from_inputs_outcomes_v4_tb \
		--inputs "$${INPUTS_FILE:-/tmp/of_inputs.ndjson}" \
		--tb-labels "$${TB_LABELS_FILE:-/tmp/of_inputs.tb_labeled.ndjson}" \
		--out "$${DATASET_FILE:-/tmp/ml_dataset_tb.parquet}" || (echo "❌ Ошибка построения датасета" && exit 1)
	@echo "✅ Шаг 2 завершён: датасет создан в $${DATASET_FILE:-/tmp/ml_dataset_tb.parquet}"
	@echo ""
	@echo "🎉 Пайплайн v10 triple-barrier labeling из Redis Stream успешно завершён!"
	@echo ""
	@echo "📁 Результирующие файлы (на хосте в /tmp):"
	@echo "   - Inputs: $${INPUTS_FILE:-/tmp/of_inputs.ndjson}"
	@echo "   - Лейблы: $${TB_LABELS_FILE:-/tmp/of_inputs.tb_labeled.ndjson}"
	@echo "   - Датасет: $${DATASET_FILE:-/tmp/ml_dataset_tb.parquet}"

# ═══════════════════════════════════════════════════════════════
# Manual ML Pipeline Triggers (Recovery/Force)
# ═══════════════════════════════════════════════════════════════

HOURS ?= 24

ml-v10-force-label:
	@echo "🏷️  Generating TB v10 labels (Manual Run)..."
	@REDIS_WORKER_IP=$$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' redis-worker-1); \
	REDIS_TICKS_IP=$$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' redis-ticks); \
	echo "   Redis Worker IP: $$REDIS_WORKER_IP"; \
	echo "   Redis Ticks IP:  $$REDIS_TICKS_IP"; \
	docker exec -i scanner-ml-triple-barrier-v10-timer sh -c '\
		echo "Preparing output directories..."; \
		mkdir -p /var/lib/trade/ml_runs/tb_v10; \
		TS=$$(date +%Y%m%d_%H%M%S); \
		INPUTS=/var/lib/trade/ml_runs/tb_v10/of_inputs_$$TS.ndjson; \
		LABELS=/var/lib/trade/ml_runs/tb_v10/tb_labels_$$TS.ndjson; \
		DATASET=/var/lib/trade/ml_runs/tb_v10/ml_dataset_tb_$$TS.parquet; \
		echo "Step 0: Exporting OF inputs to $$INPUTS (since $(HOURS)h)..."; \
		START_MS=$$(date -d "$(HOURS) hours ago" +%s000); \
		python3 -m tools.export_of_inputs_ndjson --redis-url redis://'"$$REDIS_WORKER_IP"':6379/0 --out $$INPUTS --start-id $$START_MS-0 --max-records 800000; \
		echo "Step 1: Labeling to $$LABELS (v10)..."; \
		python3 -m tools.label_triple_barrier_from_redis_ticks_v10 \
			--inputs $$INPUTS \
			--out $$LABELS \
			--ticks-redis-url redis://'"$$REDIS_TICKS_IP"':6379/0 \
			--since-hours $(HOURS) \
			--h-ms 180000; \
		echo "Step 1.5: Uploading labels to Redis labels:tb..."; \
		python3 -m tools.upload_labels_to_redis --path $$LABELS --redis-url redis://'"$$REDIS_WORKER_IP"':6379/0; \
		echo "Step 2: Building dataset $$DATASET ..."; \
		python3 -m tools.build_dataset_from_inputs_outcomes_v4_tb --inputs $$INPUTS --tb-labels $$LABELS --out $$DATASET; \
		echo "✅ Labeling pipeline completed. Dataset: $$DATASET"'

ml-v10-manual-train:
	@echo "🧠 Training TB v10.4 Model (Manual Run)..."
	@REDIS_WORKER_IP=$$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' redis-worker-1); \
	docker exec -i scanner-ml-train-tb-v10-4-timer sh -c '\
		echo "Starting manual training (v10.4) with $(HOURS)h window..."; \
		export REDIS_URL=redis://'"$$REDIS_WORKER_IP"':6379/0; \
		python3 -m tools.ml_train_report_tb_v10_4 --since-hours $(HOURS) --horizons 60000,180000,300000; \
		echo "✅ Manual training completed."'

ml-manual-pipeline:
	@echo "🚀 Starting Full Manual ML Pipeline (HOURS=$(HOURS))..."
	@$(MAKE) ml-v10-force-label HOURS=$(HOURS)
	@echo ""
	@$(MAKE) ml-v10-manual-train HOURS=$(HOURS)
	@echo ""
	@echo "🎉 Full manual pipeline finished successfully."

# Manual training for v10.4 (Triple Barrier)
# Usage: make ml-manual-train-v10-4 [HOURS=24]
ml-manual-train-v10-4:
	@echo "Running manual training for v10.4 with HOURS=$(if $(HOURS),$(HOURS),24)"
	docker exec -i scanner-ml-train-tb-v10-4-timer python3 -m tools.ml_train_report_tb_v10_4 --since-hours $(if $(HOURS),$(HOURS),24)

edge-stack-status:
	@echo "📊 Статус Edge Stack v1 Train:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep "scanner-edge-stack-train-exporter" || echo "❌ Not running"
	@echo ""
	@echo "Last metrics:"
	@docker exec scanner-edge-stack-train-exporter python3 -c "import urllib.request; print(urllib.request.urlopen('http://localhost:9813/metrics').read().decode('utf-8'))" 2>/dev/null | grep "edge_stack_train_" || echo "⚠️ Metrics unavailable"

edge-stack-logs:
	@echo "📋 Логи Edge Stack v1 Bundle (via timers worker):"
	@docker logs --tail=200 scanner-of-timers-worker | grep "nightly_edge_stack_v1_train_bundle" || echo "No recent logs"
	@echo ""
	@echo "📋 Логи Edge Stack Exporter:"
	@docker logs --tail=50 scanner-edge-stack-train-exporter


trade-close-joiner-status:
	@echo "📊 Статус Trade Close Joiner..."
	@$(DC_TIMERS) ps trade-close-joiner-v5

trade-close-joiner-logs:
	@echo "📋 Логи Trade Close Joiner..."
	@$(DC_TIMERS) logs -f trade-close-joiner-v5

# ═══════════════════════════════════════════════════════════════
# P63 Decision Coverage
# ═══════════════════════════════════════════════════════════════

decision-coverage-up:
	@echo "🚀 Updating P63 Decision Coverage services..."
	@$(DOCKER_COMPOSE_BIN) -f docker-compose-crypto-orderflow.yml up -d --build of-timers-worker
	@$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TIMERS) up -d --build scanner-decision-coverage-exporter

decision-coverage-logs:
	@echo "📋 Logs for Decision Coverage Exporter:"
	@$(DC_TIMERS) logs -f scanner-decision-coverage-exporter

ml-confidence-drift-manual:
	@echo "📊 Ручной запуск отчета дрифта уверенности..."
	@$(DOCKER_COMPOSE_BIN) -f $(COMPOSE_PYTHON_WORKERS) run --rm python-worker ml_analysis/tools/run_confidence_drift_monitor.sh

	@docker logs -f scanner-decision-coverage-exporter

dr-uplift-eval-status:
	@docker ps -a | grep dr-uplift-eval || echo "No uplift eval containers found"

dr-uplift-eval-logs:
	@docker logs -f scanner-dr-uplift-eval-timer
print-dc-with-trailing: ; @echo $(DC_WITH_TRAILING)

soak-timer-status:
	@docker ps | grep soak-dq-level-share-timer

soak-timer-logs:
	@docker logs -f scanner-soak-dq-level-share-timer

# ═══════════════════════════════════════════════════════════════
# Binance Execution Services (testnet / production)
# Services start automatically with `make up` via docker-compose-binance.yml
# To toggle testnet/prod: change BINANCE_FUTURES_BASE_URL in .env
# ═══════════════════════════════════════════════════════════════

binance-status:
	@echo "📊 Binance Execution Services"
	@docker ps -a --filter "name=scanner-binance" --format "table {{.Names}}\t{{.Status}}\t{{.RunningFor}}"

binance-executor-status:
	@echo "📊 Binance Executor status:"
	@docker ps -a --filter "name=scanner-binance-executor" --format "table {{.Names}}\t{{.Status}}\t{{.RunningFor}}"

binance-executor-logs:
	@docker logs -f scanner-binance-executor

binance-executor-restart:
	@echo "🔄 Restarting binance-executor..."
	@docker restart scanner-binance-executor
	@echo "✅ binance-executor restarted"

binance-executor-start:
	@echo "🚀 Starting binance-executor..."
	@$(DOCKER_COMPOSE_BIN) -f docker-compose-binance.yml up -d binance-executor

binance-executor-stop:
	@echo "⏹️  Stopping binance-executor..."
	@$(DOCKER_COMPOSE_BIN) -f docker-compose-binance.yml stop binance-executor

# ═══════════════════════════════════════════════════════════════
# P6: Runbook Server + Consistency Checker + Healthcheck
# ═══════════════════════════════════════════════════════════════

.PHONY: p6-runbook-server-status p6-runbook-server-logs p6-runbook-server-restart \
        p6-consistency-check p6-healthcheck p6-test

## p6-runbook-server-status: Status of the trade-runbook-server container
p6-runbook-server-status:
	@echo "📋 P6 Runbook Server status:"
	@docker ps -a --filter "name=trade-runbook-server" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" || true
	@echo ""
	@echo "Healthz:"
	@curl -sf http://localhost:${RUNBOOK_SERVER_PORT:-18080}/healthz 2>/dev/null && echo " ✅ OK" || echo " ❌ not reachable"

## p6-runbook-server-logs: Tail logs of the trade-runbook-server container
p6-runbook-server-logs:
	@docker logs -f trade-runbook-server 2>&1 || echo "❌ trade-runbook-server not found"

## p6-runbook-server-restart: Restart the trade-runbook-server container
p6-runbook-server-restart:
	@echo "🔄 Restarting trade-runbook-server..."
	@docker restart trade-runbook-server 2>/dev/null || \
		$(DOCKER_COMPOSE_BIN) -f docker-compose-timers.yml up -d trade-runbook-server
	@echo "✅ trade-runbook-server restarted"

## p6-consistency-check: Run the Redis↔SQL consistency check manually (requires EXECUTION_JOURNAL_DSN)
p6-consistency-check:
	@echo "🔍 Running execution consistency check..."
	@if [ -z "$${EXECUTION_JOURNAL_DSN}" ]; then \
		echo "❌ EXECUTION_JOURNAL_DSN is not set. Export it before running this target."; \
		exit 1; \
	fi
	@cd python-worker && python scripts/check_execution_consistency.py \
		--redis-url "$${REDIS_URL:-redis://redis-worker-1:6379/0}" \
		--journal-dsn "$${EXECUTION_JOURNAL_DSN}" \
		--exec-stream "$${EXEC_STREAM:-orders:exec}"

## p6-healthcheck: Run the full execution healthcheck manually and write JSON report
p6-healthcheck:
	@echo "🏥 Running execution healthcheck..."
	@mkdir -p "$${RUNBOOK_REPORT_DIR:-./var/runbook-reports}"
	@cd python-worker && python scripts/execution_healthcheck.py \
		--redis-url "$${REDIS_URL:-redis://redis-worker-1:6379/0}" \
		--journal-dsn "$${EXECUTION_JOURNAL_DSN:-}" \
		--report-dir "$${RUNBOOK_REPORT_DIR:-./var/runbook-reports}"; \
	STATUS=$$?; \
	echo ""; \
	if [ $$STATUS -eq 0 ]; then echo "✅ Health: ok"; \
	elif [ $$STATUS -eq 1 ]; then echo "⚠️  Health: warning"; \
	else echo "🚨 Health: critical"; fi; \
	exit $$STATUS

## p6-test: Run P6 unit tests (consistency checker + runbook server)
p6-test:
	@echo "🧪 Running P6 tests..."
	@cd python-worker && python -m pytest \
		tests/binance_execution/test_execution_consistency_checker_v1.py \
		tests/binance_execution/test_runbook_server_v1.py \
		-v --tb=short
	@echo "✅ P6 tests completed"

# ═══════════════════════════════════════════════════════════════
# P5.7 Execution Audit Chain (checker + runbook server)
# ═══════════════════════════════════════════════════════════════

## execution-audit-status: Статус P5.7 execution audit-chain checker и runbook server
execution-audit-status:
	@echo "📊 P5.7 Execution Audit Chain status:"
	@docker ps --filter "name=scanner-execution-audit" --format "  {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || true
	@echo ""
	@echo "Runtime dirs:"
	@ls -lh runtime/execution_audit_chain/ 2>/dev/null || echo "  runtime/execution_audit_chain: not found"
	@echo ""
	@echo "Latest report:"
	@cat runtime/execution_audit_chain/latest_execution_audit_chain.json 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print('  total_broken:', d.get('total_broken'), '| seed_rows:', d.get('seed_rows'), '| generated_at:', d.get('generated_at_iso'))" 2>/dev/null || echo "  (no report yet)"

## execution-audit-logs: Логи P5.7 execution audit-chain checker
execution-audit-logs:
	@docker logs scanner-execution-audit-chain-checker --tail 100 -f 2>/dev/null || echo "⚠️  Container scanner-execution-audit-chain-checker not running"

## execution-audit-runbook-logs: Логи P5.7 runbook server
execution-audit-runbook-logs:
	@docker logs scanner-execution-audit-runbook-server --tail 100 -f 2>/dev/null || echo "⚠️  Container scanner-execution-audit-runbook-server not running"

## execution-audit-start: Запуск P5.7 execution audit-chain checker и runbook server
execution-audit-start:
	@echo "⏰ Запуск P5.7 Execution Audit Chain..."
	@mkdir -p runtime/execution_audit_chain runtime/node_exporter_textfile
	@COMPOSE_PROFILES=ops $(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TIMERS) up -d execution-audit-chain-checker execution-audit-runbook-server && \
		echo "✅ P5.7 Execution Audit Chain запущен (runbook: http://localhost:8777/api/audit-chain/latest)" || \
		echo "⚠️  Ошибка запуска P5.7 Execution Audit Chain"

## execution-audit-stop: Остановка P5.7 execution audit-chain сервисов
execution-audit-stop:
	@echo "🛑 Остановка P5.7 Execution Audit Chain..."
	@COMPOSE_PROFILES=ops $(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TIMERS) stop execution-audit-chain-checker execution-audit-runbook-server 2>/dev/null && \
		echo "✅ P5.7 Execution Audit Chain остановлен" || echo "⚠️  Ошибка остановки"

## execution-audit-restart: Перезапуск P5.7 execution audit-chain сервисов
execution-audit-restart:
	@echo "🔄 Перезапуск P5.7 Execution Audit Chain..."
	@COMPOSE_PROFILES=ops $(DOCKER_COMPOSE_BIN) -f $(COMPOSE_FILE) -f $(COMPOSE_TIMERS) restart execution-audit-chain-checker execution-audit-runbook-server 2>/dev/null && \
		echo "✅ P5.7 Execution Audit Chain перезапущен" || echo "⚠️  Ошибка перезапуска"

## execution-audit-test: Запуск P5.7 unit-тестов (scheduler + assets + compose wiring)
execution-audit-test:
	@echo "🧪 Running P5.7 Execution Audit Chain tests..."
	@cd python-worker && python -m pytest \
		scripts/test_execution_audit_chain_scheduler_p57_v1.py \
		scripts/test_p57_execution_audit_assets_v1.py \
		scripts/test_p57_execution_audit_compose_wiring_v1.py \
		scripts/test_p56_audit_chain_assets_v1.py \
		scripts/test_check_execution_audit_chain_p56_v1.py \
		-v --tb=short
	@echo "✅ P5.7 tests completed"

## execution-audit-report: Показать последний audit report из runbook server
execution-audit-report:
	@echo "📋 Latest execution audit-chain report (http://localhost:8777/api/audit-chain/latest):"
	@curl -s http://127.0.0.1:8777/api/audit-chain/latest 2>/dev/null | python3 -m json.tool || \
		echo "⚠️  Runbook server not responding (start with: make execution-audit-start)"

# ═══════════════════════════════════════════════════════════════
# P16 ExecHealth Reconnect Chaos Suite
# ═══════════════════════════════════════════════════════════════

## exec-health-reconnect-chaos-test: Run the chaos harness manually to verify self-healing
exec-health-reconnect-chaos-test:
	@echo "🔥 Запуск ExecHealth Reconnect Chaos Harness..."
	@docker exec -i scanner-exec-health-reconnect-chaos-timer python3 -m orderflow_services.exec_health_freeze_reconnect_chaos_harness_v1 --mode reconnect-both
	@echo "✅ Chaos run completed. Check metrics at port 9834 or Redis events."

## exec-health-reconnect-chaos-logs: Logs of the chaos timer
exec-health-reconnect-chaos-logs:
	@docker logs scanner-exec-health-reconnect-chaos-timer --tail 50 -f 2>/dev/null || echo "⚠️  Container scanner-exec-health-reconnect-chaos-timer not running"

## exec-health-reconnect-chaos-status: Status of the chaos timer
exec-health-reconnect-chaos-status:
	@echo "📊 ExecHealth Reconnect Chaos status:"
	@docker ps --filter "name=scanner-exec-health-reconnect-chaos-timer" --format "  {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || echo "❌ Not running"

# ═══════════════════════════════════════════════════════════════
# P17 ExecHealth Nightly Reconnect Chaos Smoke
# ═══════════════════════════════════════════════════════════════

## exec-health-reconnect-nightly-status: Status of the nightly smoke job
exec-health-reconnect-nightly-status:
	@echo "📊 ExecHealth Nightly Reconnect Smoke status:"
	@docker ps --filter "name=scanner-exec-health-reconnect-nightly-timer" --format "  {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || echo "❌ Not running"
	@echo ""
	@echo "Latest report:"
	@cat /var/lib/trade/exec_health_reconnect_smoke/latest_report.json 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print('  ok:', d.get('ok'), '| ts_ms:', d.get('ts_ms'), '| host:', d.get('host'))" 2>/dev/null || echo "  (no report yet)"

## exec-health-reconnect-nightly-logs: Logs of the nightly smoke job
exec-health-reconnect-nightly-logs:
	@docker logs scanner-exec-health-reconnect-nightly-timer --tail 100 -f 2>/dev/null || echo "⚠️  Container scanner-exec-health-reconnect-nightly-timer not running"

## exec-health-reconnect-nightly-test: Run the nightly smoke runner manually inside container
exec-health-reconnect-nightly-test:
	@echo "🌙 Запуск Nightly ExecHealth Reconnect Smoke Runner..."
	@docker exec -i scanner-exec-health-reconnect-nightly-timer python3 -m orderflow_services.exec_health_freeze_reconnect_nightly_smoke_v1
	@echo "✅ Nightly smoke runner completed."

# ═══════════════════════════════════════════════════════════════
# P5.4 Orchestration Composite Preflight Exporter
# Exporter reads persisted composite preflight state from Redis
# (ORCHESTRATION_PREFLIGHT_STATE_PREFIX:<purpose>) and translates
# it into low-cardinality Prometheus metrics (port 9837).
# Alerts: TradeOrchestrationCompositePreflightBlockedCrit,
#         TradeOrchestrationCompositePreflightInvalidWarn,
#         TradeOrchestrationCompositePreflightStateStaleWarn
# ═══════════════════════════════════════════════════════════════

## composite-preflight-exporter-status: Status of P5.4 Composite Preflight Exporter
composite-preflight-exporter-status:
	@echo "📊 P5.4 Composite Preflight Exporter status:"
	@docker ps --filter "name=scanner-orchestration-composite-preflight-exporter" \
		--format "  {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null \
		|| echo "❌ Container not found"

## composite-preflight-exporter-logs: Tail logs of the P5.4 exporter
composite-preflight-exporter-logs:
	@docker logs scanner-orchestration-composite-preflight-exporter --tail 80 -f 2>/dev/null \
		|| echo "⚠️  Container scanner-orchestration-composite-preflight-exporter not running"

## composite-preflight-exporter-restart: Restart the P5.4 exporter container
composite-preflight-exporter-restart:
	@echo "🔄 Перезапуск P5.4 Composite Preflight Exporter..."
	@docker restart scanner-orchestration-composite-preflight-exporter 2>/dev/null \
		|| echo "⚠️  Container not running, starting via compose..."; \
	$(DOCKER_COMPOSE_BIN) -f python-worker/orderflow_services/deploy/compose/docker-compose.orchestration-composite-preflight-exporter-v1.yml up -d 2>/dev/null
	@echo "✅ Done"

## composite-preflight-exporter-metrics: Print summary metrics from /metrics endpoint
composite-preflight-exporter-metrics:
	@echo "📈 P5.4 Composite Preflight Exporter — summary metrics:"
	@docker run --rm --network trade-network alpine/curl -s \
		http://scanner-orchestration-composite-preflight-exporter:9837/metrics 2>/dev/null \
		| grep -E "^orchestration_composite_preflight_(exporter_up|exporter_redis|present_total|block_total|invalid_total|ok_total|purposes_total)" \
		|| echo "⚠️  Could not reach exporter on trade-network:9837"

# ═══════════════════════════════════════════════════════════════
# News Agent (minik 192.168.0.121)
# ═══════════════════════════════════════════════════════════════

minik-news-agent-deploy:
	@echo "📰 Развёртывание News Agent на minik..."
	@./scripts/deploy-news-agent-to-minik.sh all

minik-news-agent-sync:
	@echo "📦 Синхронизация News Agent на minik (без запуска)..."
	@./scripts/deploy-news-agent-to-minik.sh sync

minik-news-agent-up:
	@echo "🚀 Запуск News Agent на minik..."
	@./scripts/deploy-news-agent-to-minik.sh up

minik-news-agent-down:
	@echo "🛑 Остановка News Agent на minik..."
	@./scripts/deploy-news-agent-to-minik.sh down

minik-news-agent-logs:
	@./scripts/deploy-news-agent-to-minik.sh logs

minik-news-agent-status:
	@./scripts/deploy-news-agent-to-minik.sh status

minik-news-agent-restart:
	@echo "🔄 Перезапуск News Agent на minik..."
	@./scripts/deploy-news-agent-to-minik.sh down
	@./scripts/deploy-news-agent-to-minik.sh up

