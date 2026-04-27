# 🚀 Signal Performance Tracker - Быстрый справочник

## 📊 Быстрый просмотр статистики

```bash
make tracker-stats
```

**Вывод**:

```
=== ОБЩАЯ СТАТИСТИКА ===
  Всего сделок: 196
  WinRate: 47.96%
  P&L: +$592.30

=== ПО ИСТОЧНИКАМ ===
📊 AggregatedHub-V2: 87 сделок | WR: 49.43% | P&L: $575.69
📊 TechnicalAnalysis: 104 сделки | WR: 45.19% | P&L: $-6.15
📊 unknown: 5 сделок | WR: 80.00% | P&L: $22.76
```

---

## 📱 Отправка отчета в Telegram

```bash
make send-real-report
```

Отчет придёт в Telegram с разбивкой по источникам!

---

## 📋 Логи в реальном времени

```bash
make tracker-logs
```

---

## 🔄 Перезапуск

```bash
make tracker-restart
```

---

## 🎯 Три источника сигналов

| Источник              | Сервис                     | Результат              |
| --------------------- | -------------------------- | ---------------------- |
| **AggregatedHub-V2**  | `scanner-aggregated-hub`   | ✅ **+$575** (ЛУЧШИЙ!) |
| **TechnicalAnalysis** | `scanner-signal-generator` | ❌ **-$6** (убыточный) |
| **OrderFlow**         | `multi-symbol-orderflow`   | ✅ Включен в "unknown" |

---

## ⏰ Автоматические отчеты

- ✅ **Каждый час** - полная статистика
- ✅ **Каждый день** (00:00 UTC) - дневная сводка

**Следующий отчет**: через ~30 минут (в 04:57 UTC)

---

## 📂 Важные файлы

| Файл                                                   | Описание                |
| ------------------------------------------------------ | ----------------------- |
| `python-worker/services/signal_performance_tracker.py` | Главный оркестратор     |
| `python-worker/services/trade_monitor.py`              | Отслеживание позиций    |
| `python-worker/config/signal_tracker_config.json`      | Конфигурация            |
| `scripts/send_real_report.py`                          | Скрипт отправки отчетов |

---

## ✅ Готово к использованию!

Сервис называется: **Signal Performance Tracker**

Он автоматически:

- 📊 Отслеживает тики по сигналам
- ✅ Проверяет TP1/TP2/TP3 и SL
- 💾 Записывает результативность
- 📱 Отправляет отчеты **каждый час** в Telegram
