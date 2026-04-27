# Настройка логирования PostgreSQL

## Описание проблемы
PostgreSQL логировал каждое SQL сообщение типа STATEMENT, что приводило к большому объему логов в высоконагруженных системах.

## Решение
Настроено выборочное логирование с помощью параметра `log_statement_sample_rate = 0.0001`, что означает логирование только каждого 10000-го SQL запроса.

## Измененные файлы

### 1. `postgresql.conf` (новый файл)
```ini
# PostgreSQL configuration for scanner-infra
log_statement = 'all'                    # Log all SQL statements
log_statement_sample_rate = 0.0001      # Log only 0.01% of statements (every ~10000th)
log_min_duration_statement = 0          # Log all statements (use sample_rate to control volume)
```

### 2. `docker-compose.yml`
Обновлена конфигурация сервиса postgres:
- Добавлен volume mapping для `postgresql.conf`
- Изменена команда запуска на использование кастомного конфига

## Проверка настроек

```bash
# Проверить параметры логирования
docker-compose exec postgres psql -U postgres -d trade -c "SHOW log_statement;"
docker-compose exec postgres psql -U postgres -d trade -c "SHOW log_statement_sample_rate;"

# Посмотреть логи STATEMENT
docker-compose logs postgres | grep STATEMENT
```

## Тестирование

Для тестирования логирования можно использовать скрипт `test_postgres_logging.py`:

```bash
python test_postgres_logging.py
```

Скрипт выполнит 50000 запросов и покажет, логируются ли только ~5 сообщений STATEMENT.

## Результат
Теперь PostgreSQL будет логировать только каждый 10000-й SQL запрос, значительно сократив объем логов при сохранении возможности мониторинга активности базы данных.
