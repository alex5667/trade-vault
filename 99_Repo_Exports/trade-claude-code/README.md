# Trade Project — Claude Code Setup

## Структура
```
trade/                          ← корень проекта
├── CLAUDE.md                   ← автоматически загружается Claude Code
└── .claude/
    ├── agents/
    │   └── agents.md           ← specialist agents (@trade-lead, @sre-rollout, ...)
    ├── skills/                 ← /slash-команды + автозагрузка по контексту
    │   ├── trade-project-core/SKILL.md
    │   ├── trade-python-signal-engine/SKILL.md
    │   ├── trade-go-redis-ingest/SKILL.md
    │   ├── trade-new-signal/SKILL.md
    │   ├── trade-rollout/SKILL.md
    │   ├── ... (46 skills total)
    └── commands/               ← legacy формат (backward compat, тоже работает)
        ├── trade-new-signal.md
        ├── trade-rollout.md
        └── ...
```

## Установка

```bash
# 1. Скопировать в корень проекта trade
cp -r trade-claude-code/CLAUDE.md /path/to/trade/
cp -r trade-claude-code/.claude /path/to/trade/

# 2. Проверить структуру
ls /path/to/trade/.claude/skills/ | head -10

# 3. Запустить Claude Code из корня проекта
cd /path/to/trade
claude
```

## Slash Commands

### Основные workflows
```
/trade-new-signal <идея>         — новый сигнал / детектор / gate
/trade-rollout <изменение>       — staged rollout plan
/trade-parallel-investigation <проблема>  — cross-service расследование
/trade-audit                     — полный аудит сервиса
/trade-release-gate              — pre-release checklist
/trade-postmortem                — postmortem шаблон
/trade-failure-drill             — failure drill
/trade-latency-audit             — latency profiling
/trade-quality-gate              — DQ gate review
/trade-replay                    — replay / regression pack
/trade-regression-pack           — regression тесты
/trade-contract-check            — DTO / contract check
/trade-execution-policy-review   — Execution FSM review
```

### Fast (низкая стоимость, быстро)
```
/trade-fast-fix <описание>       — изолированный fix
/trade-fast-test-gen             — генерация тестов
/trade-fast-log-triage           — triage логов
/trade-fast-contract-check       — быстрая проверка контракта
/trade-fast-doc-update           — обновление документации
```

### Pro (premium reasoning, planning mode)
```
/trade-pro-architecture          — архитектурный дизайн
/trade-pro-incident              — production incident
/trade-pro-ml-gate-review        — ML gate review
/trade-pro-rollout-review        — критический rollout review
/trade-pro-schema-change         — schema migration review
```

### Специализированные
```
/trade-exchange-adapter-review   — exchange adapter review
/trade-backtest-validity-review  — backtest hygiene check
/trade-storage-retention-review  — retention policy review
/trade_virtual_trades_last_hour  — анализ виртуальных сделок (полный)
/trade_virtual_trades_last_hour_fast  — быстрый анализ сделок
/tasks                           — выполнить задачи из Telegram inbox
```

## Agents (специалисты)

Claude Code автоматически использует агентов через workflows.
Можно вызвать явно в промпте: `act as @trade-lead and ...`

| Agent | Домен |
|---|---|
| @trade-lead | Оркестратор, cross-service, merge |
| @go-ingest-engineer | Go, WebSocket, Redis Streams write |
| @python-signal-engineer | Python workers, детекторы, features |
| @platform-api-ui-engineer | NestJS, Next.js, DTO, WS |
| @timeseries-dba | Postgres, TimescaleDB, schema |
| @ml-replay-engineer | ML features, replay, DQ gate |
| @sre-rollout | Rollout, SRE, Prometheus, alerts |
| @microstructure-analyst | Market microstructure, edge, regime |
| @execution-risk-analyst | FSM, orders, kill switch |
| @contract-governor | API contracts, backward compat |
| @latency-benchmarker | Latency profiling, hot path |
| @resilience-drillmaster | Chaos, failure drills |
| @backtest-validity-reviewer | Backtest hygiene |
| @exchange-adapter-engineer | Binance adapter |
| @storage-retention-governor | Retention, tiering |

## Skills (автозагрузка)

Claude Code автоматически подключает релевантные skills по контексту задачи.
Принудительный вызов: `/trade-project-core` и т.д.

Ключевые триггеры автозагрузки:
- `TRADE:` или `tr:` в промпте → `trade-project-core`
- Python, детекторы, сигналы → `trade-python-signal-engine`
- Go, WebSocket, ingest → `trade-go-redis-ingest`
- rollout, SRE, Prometheus → `trade-observability-rollout`
- DQ, время, bad data → `trade-data-quality-time`

## Конфигурация бюджета skills

Если Claude не видит все skills, увеличь бюджет:
```bash
# В .env или shell
export SLASH_COMMAND_TOOL_CHAR_BUDGET=50000
```

## Пример использования

```
# Новый сигнал
/trade-new-signal добавить VPIN-based flow imbalance детектор для 5m таймфрейма

# Rollout изменения
/trade-rollout включить DQ gate v2 в STRICT режиме для BTCUSDT

# Расследование проблемы
/trade-parallel-investigation lag spike в Python consumer на stream:ticks_raw в 03:00 UTC

# Быстрый fix
/trade-fast-fix исправить off-by-one в rolling window depth_imbalance_5

# Latency audit
/trade-latency-audit измерить E2E latency от WebSocket тика до WS push на фронт
```
