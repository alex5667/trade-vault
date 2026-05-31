# Documentation Index - Quick Navigation

Быстрая навигация по всей документации scanner_infra.

---

## 🎯 Начать здесь

| Категория | Файл | Описание |
|-----------|------|----------|
| 🚀 Start | [НАЧАТЬ_ЗДЕСЬ.md](НАЧАТЬ_ЗДЕСЬ.md) | Для новичков (RU) |
| ⚡ Quick | [ЗАПУСТИТЬ_СЕЙЧАС.md](ЗАПУСТИТЬ_СЕЙЧАС.md) | Быстрый запуск |
| 📖 Main | [README.md](README.md) | Главный README |
| 🏗️ Arch | [ARCHITECTURE.md](ARCHITECTURE.md) | Архитектура |
| 🎯 TP1 | [tp1-trailing/README.md](tp1-trailing/README.md) | TP1 Trailing System |

---

## 📁 Категории (7 папок, 73 файла)

### 🎯 [tp1-trailing/](tp1-trailing/) - TP1 Trailing System (9 файлов)
**Система автоматического трейлинга после TP1**
- [README.md](tp1-trailing/README.md) - Индекс
- [QUICKSTART.md](tp1-trailing/QUICKSTART.md) - Быстрый старт  
- [TP1_TRAILING_SYSTEM.md](tp1-trailing/TP1_TRAILING_SYSTEM.md) - Техническая документация
- [DEPLOYMENT_GUIDE.md](tp1-trailing/TP1_TRAILING_DEPLOYMENT_GUIDE.md) - Deployment
- И ещё 5 файлов

### 📘 [guides/](guides/) - Руководства (5 файлов)
**Quick starts и практические руководства**
- [QUICKSTART_SIGNALS.md](guides/QUICKSTART_SIGNALS.md)
- [REPORTING_QUICKSTART.md](guides/REPORTING_QUICKSTART.md)
- [TRACKING_QUICK_GUIDE.md](guides/TRACKING_QUICK_GUIDE.md)
- [QUICK_SEND_REPORT.md](guides/QUICK_SEND_REPORT.md)
- [TRACKING_CHEATSHEET.txt](guides/TRACKING_CHEATSHEET.txt)

### ⚙️ [setup/](setup/) - Установка (8 файлов)
**Инструкции по установке и настройке**
- [AGGREGATED_HUB_V2_REDIS_TICKS_SETUP.md](setup/AGGREGATED_HUB_V2_REDIS_TICKS_SETUP.md)
- [TICKS_REDIS_SETUP_COMPLETE.md](setup/TICKS_REDIS_SETUP_COMPLETE.md)
- [XAUUSD_SETUP_COMPLETE.md](setup/XAUUSD_SETUP_COMPLETE.md)
- [MIGRATION_V2_COMPLETE.md](setup/MIGRATION_V2_COMPLETE.md)
- И ещё 4 файла

### 🔧 [fixes/](fixes/) - Исправления (7 файлов)
**Документация по исправлениям проблем**
- [REDIS_CONNECTION_FIX.md](fixes/REDIS_CONNECTION_FIX.md)
- [FIX_SUMMARY_REPORTING.md](fixes/FIX_SUMMARY_REPORTING.md)
- [VALIDATION_FIX.md](fixes/VALIDATION_FIX.md)
- И ещё 4 файла

### 📊 [metrics/](metrics/) - Метрики (8 файлов)
**Метрики, мониторинг и отчёты**
- [METRICS_QUICK_REFERENCE.md](metrics/METRICS_QUICK_REFERENCE.md)
- [SIGNAL_TRACKER_INTEGRATION.md](metrics/SIGNAL_TRACKER_INTEGRATION.md)
- [HOURLY_REPORTS_SUMMARY.md](metrics/HOURLY_REPORTS_SUMMARY.md)
- И ещё 5 файлов

### 📅 [sessions/](sessions/) - Сессии (7 файлов)
**Журналы рабочих сессий и изменений**
- [SESSION_COMPLETE_2025-11-06.md](sessions/SESSION_COMPLETE_2025-11-06.md)
- [PRODUCTION_STATUS_2025-11-06.md](sessions/PRODUCTION_STATUS_2025-11-06.md)
- И ещё 5 файлов

### 📦 [archives/](archives/) - Архив (6 файлов)
**Архивные документы и старые сводки**
- [FINAL_SUMMARY_COMPLETE.md](archives/FINAL_SUMMARY_COMPLETE.md)
- [FINAL_SUMMARY.txt](archives/FINAL_SUMMARY.txt)
- И ещё 4 файла

---

## 🚀 Быстрый старт

```bash
# 1. Основная система
cat docs/НАЧАТЬ_ЗДЕСЬ.md

# 2. TP1 Trailing
cat docs/tp1-trailing/README.md

# 3. Запуск
make up                    # Всё запускается автоматически!

# 4. Статус
make status                # Общий статус
make trailing-status       # TP1 Trailing статус
```

---

## 🔍 Поиск документации

### По теме

**Signals:**
- guides/QUICKSTART_SIGNALS.md
- XAUUSD_QUICK_START.md

**Trading:**
- tp1-trailing/ (вся папка)
- XAUUSD_* файлы

**Setup:**
- setup/ (вся папка)
- CONFIGURATION.md

**Monitoring:**
- metrics/ (вся папка)
- guides/TRACKING_QUICK_GUIDE.md

**Troubleshooting:**
- fixes/ (вся папка)
- QUICK_FIX_GUIDE.md

---

## 📖 Файлы в корне docs/

1. **README_INDEX.md** - Полный индекс (этот файл теперь INDEX.md)
2. **README.md** - Главный README
3. **ARCHITECTURE.md** - Архитектура (34KB, 619 строк)
4. **SERVICES.md** - Сервисы (45KB, 1642 строки)
5. **CONFIGURATION.md** - Конфигурация (19KB, 740 строк)
6. **EXAMPLES.md** - Примеры (33KB, 1218 строк)
7. **MIGRATION_TO_DOCS_FOLDER.md** - Этот файл миграции

И ещё 15+ файлов (XAUUSD_*, FIX_*, TRACKER_*, и т.д.)

---

## ✅ Преимущества новой структуры

1. ✅ **Чистый корень проекта** - только код и конфигурация
2. ✅ **Организованная документация** - 7 категорий
3. ✅ **Легкий поиск** - по папкам и индексам
4. ✅ **Масштабируемость** - легко добавлять новые документы
5. ✅ **Профессиональный вид** - enterprise-grade организация

---

## 🎉 Готово!

Документация полностью организована и готова к использованию.

**Начните с:**
- `../DOCUMENTATION.md` (в корне)
- `README_INDEX.md` (полный индекс)
- `tp1-trailing/README.md` (TP1 Trailing)

**Happy coding!** 🚀
