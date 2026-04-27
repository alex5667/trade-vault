# Scanner Infrastructure - Documentation

## 📚 Вся документация перенесена в папку `docs/`

### 🚀 Быстрый доступ

**Начните здесь:**
- **[docs/НАЧАТЬ_ЗДЕСЬ.md](docs/НАЧАТЬ_ЗДЕСЬ.md)** - Для новичков (RU)
- **[docs/ЗАПУСТИТЬ_СЕЙЧАС.md](docs/ЗАПУСТИТЬ_СЕЙЧАС.md)** - Быстрый запуск (RU)
- **[docs/README.md](docs/README.md)** - Главный README

**TP1 Trailing System (новое!):**
- **[docs/tp1-trailing/README.md](docs/tp1-trailing/README.md)** - Индекс TP1 Trailing
- **[docs/tp1-trailing/QUICKSTART.md](docs/tp1-trailing/TP1_TRAILING_QUICKSTART.md)** - Быстрый старт

**Основная документация:**
- **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** - Архитектура
- **[docs/SERVICES.md](docs/SERVICES.md)** - Все сервисы
- **[docs/CONFIGURATION.md](docs/CONFIGURATION.md)** - Конфигурация
- **[docs/EXAMPLES.md](docs/EXAMPLES.md)** - Примеры

---

## 📁 Структура документации

```
docs/
├── tp1-trailing/       # TP1 Trailing System (8 файлов)
├── guides/             # Руководства и quick starts (5 файлов)
├── setup/              # Инструкции по установке (8 файлов)
├── fixes/              # Исправления и hotfixes (7 файлов)
├── metrics/            # Метрики и мониторинг (8 файлов)
├── sessions/           # Журналы сессий (7 файлов)
└── archives/           # Архивные документы (6 файлов)
```

**Итого: 71 файл документации**

---

## 🎯 По категориям

### TP1 Trailing System
**[docs/tp1-trailing/](docs/tp1-trailing/)**
- README.md - Индекс
- QUICKSTART.md - Быстрый старт
- TP1_TRAILING_SYSTEM.md - Техническая документация
- DEPLOYMENT_GUIDE.md - Deployment guide
- И ещё 4 файла

### Guides & Quick Starts
**[docs/guides/](docs/guides/)**
- QUICKSTART_SIGNALS.md
- REPORTING_QUICKSTART.md
- TRACKING_QUICK_GUIDE.md
- QUICK_SEND_REPORT.md
- TRACKING_CHEATSHEET.txt

### Setup & Configuration
**[docs/setup/](docs/setup/)**
- AGGREGATED_HUB_V2_REDIS_TICKS_SETUP.md
- TICKS_REDIS_SETUP_COMPLETE.md
- XAUUSD_SETUP_COMPLETE.md
- MIGRATION_V2_COMPLETE.md
- И ещё 4 файла

### Fixes & Hotfixes
**[docs/fixes/](docs/fixes/)**
- REDIS_CONNECTION_FIX.md
- FIX_SUMMARY_REPORTING.md
- VALIDATION_FIX.md
- И ещё 4 файла

### Metrics & Monitoring
**[docs/metrics/](docs/metrics/)**
- METRICS_QUICK_REFERENCE.md
- SIGNAL_TRACKER_INTEGRATION.md
- HOURLY_REPORTS_SUMMARY.md
- И ещё 5 файлов

### Sessions & Reports
**[docs/sessions/](docs/sessions/)**
- SESSION_COMPLETE_2025-11-06.md
- PRODUCTION_STATUS_2025-11-06.md
- И ещё 5 файлов

---

## 🚀 Быстрый старт

```bash
# 1. Запуск системы
make up

# 2. Проверка статуса
make status

# 3. TP1 Trailing
make trailing-status
make trailing-test

# 4. Мониторинг
make tracker-stats
```

---

## 📖 Полный индекс

**[docs/README_INDEX.md](docs/README_INDEX.md)** - Полный индекс всей документации

---

**Last Updated**: 2025-11-06  
**Team**: Senior Go/Python Developer + Senior Trading Systems Analyst
