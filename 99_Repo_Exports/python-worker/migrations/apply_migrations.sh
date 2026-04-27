#!/bin/sh
set -e

# Ensure we are in the script's directory (POSIX-compatible, no BASH_SOURCE)
DIR="$( cd "$( dirname "$0" )" >/dev/null 2>&1 && pwd )"
cd "$DIR" || exit 1

# Если задан PG_DSN — разбираем его, иначе используем переменные по отдельности
if [ -n "${PG_DSN:-}" ]; then
    # Передаём DSN напрямую в psql через PGPASSWORD + параметры
    export DATABASE_URL="$PG_DSN"
    PSQL_OPTS="-d $PG_DSN"
else
    export PGHOST="${PGHOST:-scanner-postgres}"
    export PGPORT="${PGPORT:-5432}"
    export PGUSER="${PGUSER:-postgres}"
    export PGDATABASE="${PGDATABASE:-scanner_analytics}"
    PSQL_OPTS=""
fi

echo "📋 Применение миграций к базе данных (${PGHOST:-из PG_DSN}:${PGPORT:-})..."

# Применяем все миграции по порядку из известных директорий
# Мы проверяем несколько путей, так как структура в контейнере может отличаться от локальной
for d in . ../db/migrations ../../db/migrations /app/db/migrations; do
    if [ -d "$d" ]; then
        echo "📂 Поиск миграций в: $d"
        # Собираем список файлов и применяем их
        # Используем подпапку чтобы psql корректно находил файлы если они ссылаются на что-то рядом
        (
            cd "$d" || exit 1
            for f in $(ls *.sql 2>/dev/null | sort); do
                echo "Применяем $f (из $d)..."
                # shellcheck disable=SC2086
                psql $PSQL_OPTS -f "$f"
            done
        )
    fi
done

echo "✅ Все миграции применены успешно"
