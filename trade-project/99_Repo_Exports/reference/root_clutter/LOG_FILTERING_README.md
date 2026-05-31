# Log Filtering System

Система фильтрации логов для уменьшения шума в логах путем вывода только каждого N-го повторяющегося сообщения.

## 🎯 Проблема

В высоконагруженных системах некоторые компоненты генерируют огромное количество повторяющихся логов:

- **Prometheus**: "compact blocks", "write block completed", "Head GC", "Creating checkpoint"
- **Grafana**: "Update check succeeded", "All modules healthy"
- **Background services**: "starting module" сообщения

Эти логи засоряют вывод и усложняют мониторинг важных событий.

## ✅ Решение

Система фильтрации логов с configurable сэмплингом:

- **log_filter.py** - простой фильтр (каждое 10000-е сообщение)
- **log_filter_advanced.py** - продвинутый фильтр с настройками

## 🚀 Использование

### Простой фильтр (рекомендуется)

```bash
# Prometheus логи
docker-compose logs -f scanner-prometheus | ./log_filter.py prometheus

# Grafana логи
docker-compose logs -f scanner-grafana | ./log_filter.py grafana

# Background services
docker-compose logs -f | ./log_filter.py background
```

### Продвинутый фильтр

```bash
# Prometheus с кастомным интервалом
docker-compose logs -f scanner-prometheus | ./log_filter_advanced.py -n 5000 -t prometheus

# Только подсчет (без вывода сообщений)
docker-compose logs -f scanner-prometheus | ./log_filter_advanced.py --count-only

# Кастомный паттерн
docker-compose logs -f some-service | ./log_filter_advanced.py -p "custom.*pattern"
```

### Makefile команды

```bash
# Фильтрованные логи Prometheus
make prometheus-filtered-logs

# Фильтрованные логи Grafana
make grafana-filtered-logs
```

## ⚙️ Настройки

### Типы логов

| Тип | Паттерн | Примеры сообщений |
|-----|---------|-------------------|
| `prometheus` | `scanner-prometheus.*(?:write block completed\|Head GC\|Creating checkpoint\|compact blocks\|Deleting obsolete block)` | "compact blocks", "write block completed" |
| `grafana` | `scanner-grafana.*(?:Update check succeeded\|All modules healthy\|Starting MultiOrg Alertmanager)` | "Update check succeeded" |
| `background` | `logger=backgroundsvcs\.managerAdapter.*msg="module (?:starting|stopped)" module=\*.*` | "starting module", "module stopped" |

### Параметры

- **`-n, --interval`**: интервал сэмплинга (default: 10000)
- **`-p, --pattern`**: кастомный regex паттерн
- **`-t, --type`**: тип логов (prometheus/grafana/background)
- **`--count-only`**: только подсчет, без вывода сообщений

## 📊 Формат вывода

```
[10000] scanner-prometheus | time=... msg="compact blocks" ...
[20000] scanner-prometheus | time=... msg="write block completed" ...
[30000] scanner-prometheus | time=... msg="Head GC completed" ...
```

Где число в скобках - общее количество обработанных сообщений этого типа.

## 🛠️ Добавление новых типов логов

### 1. Обновить patterns в скриптах

```python
patterns = {
    'new_type': r'your.*regex.*pattern',
    # ...
}
```

### 2. Добавить команду в Makefile

```makefile
new-type-filtered-logs:
	@echo "📊 Фильтрованные логи New Type"
	@docker-compose logs -f new-service | ./log_filter.py new_type
```

### 3. Обновить документацию

Добавить строку в таблицу типов логов выше.

## 🔍 Отладка

### Проверить паттерн

```bash
# Показать все совпадающие сообщения (без фильтрации)
docker-compose logs scanner-prometheus | grep -E "write block completed|Head GC|Creating checkpoint|compact blocks|Deleting obsolete block"
```

### Проверить счетчик

```bash
# Только подсчет сообщений
docker-compose logs scanner-prometheus | ./log_filter_advanced.py --count-only -t prometheus
```

## 📈 Эффективность

Для типичного Prometheus с компактизацией каждые 5-10 минут:

- **Без фильтрации**: 1000+ сообщений в час
- **С фильтрацией**: 3-6 сообщений в час (каждое 10000-е)

## 🎯 Интеграция с мониторингом

### Лог ротация

```bash
# Сохранять отфильтрованные логи
make prometheus-filtered-logs > prometheus_filtered_$(date +%Y%m%d).log 2>&1 &
```

### Alerting

```bash
# Мониторить что фильтр работает
FILTERED_COUNT=$(docker-compose logs scanner-prometheus | ./log_filter_advanced.py --count-only -t prometheus | tail -1 | grep -o '[0-9]\+')
if [ "$FILTERED_COUNT" -lt 10 ]; then
    echo "WARNING: Low filtered message count: $FILTERED_COUNT"
fi
```

## 🐛 Troubleshooting

**Фильтр не выводит сообщения:**
- Проверьте что сервис запущен: `docker-compose ps`
- Проверьте логи без фильтра: `docker-compose logs scanner-prometheus | head -10`
- Проверьте паттерн: `docker-compose logs scanner-prometheus | grep -E "your.pattern"`

**Слишком много/мало сообщений:**
- Увеличьте/уменьшите интервал: `./log_filter.py -n 5000`
- Измените паттерн для большей специфичности

**Высокая загрузка CPU:**
- Используйте `--count-only` для подсчета без вывода
- Уменьшите частоту проверки логов

## 📝 Примеры использования

### Мониторинг Prometheus в фоне

```bash
# В отдельном терминале
make prometheus-filtered-logs

# В основном терминале работать с системой
make status
make health
```

### Анализ паттернов логов

```bash
# Посмотреть распределение типов сообщений
docker-compose logs scanner-prometheus --tail=1000 | grep "scanner-prometheus" | sed 's/.*msg="//;s/".*//' | sort | uniq -c | sort -nr
```

### Автоматическая ротация

```bash
# Добавить в crontab
0 * * * * cd /path/to/scanner && make prometheus-filtered-logs >> prometheus_hourly_$(date +\%H).log 2>&1
```
