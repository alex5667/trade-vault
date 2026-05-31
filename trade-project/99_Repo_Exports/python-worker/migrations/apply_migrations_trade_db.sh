#!/bin/sh
# apply_migrations_trade_db.sh
# Применяет миграции к базе "trade" (не scanner_analytics).
# Запускается отдельным migration-runner-trade контейнером.
set -e

DIR="$( cd "$( dirname "$0" )" >/dev/null 2>&1 && pwd )"
cd "$DIR" || exit 1

# DSN обязателен — trade DB не является дефолтной
if [ -z "${TRADE_PG_DSN:-}" ]; then
    echo "❌ TRADE_PG_DSN не задан — невозможно применить миграции к trade DB" >&2
    exit 1
fi

PSQL_OPTS="-d ${TRADE_PG_DSN}"

echo "📋 Применение миграций к базе TRADE (${TRADE_PG_DSN%%@*}@...)..."

# Список файлов, которые применяются к trade DB (в порядке применения)
TRADE_MIGRATIONS="
20260416_phase5_provenance.sql
"

for f in $TRADE_MIGRATIONS; do
    f_trimmed="$(echo "$f" | tr -d '[:space:]')"
    [ -z "$f_trimmed" ] && continue
    if [ -f "$f_trimmed" ]; then
        echo "▶ Применяем $f_trimmed..."
        # shellcheck disable=SC2086
        psql $PSQL_OPTS -f "$f_trimmed"
        echo "✓ $f_trimmed применена"
    else
        echo "⚠ Файл $f_trimmed не найден — пропускаем"
    fi
done

echo "✅ Миграции trade DB применены успешно"
