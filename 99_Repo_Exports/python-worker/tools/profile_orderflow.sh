#!/usr/bin/env bash
# =============================================================================
# profile_orderflow.sh — CPU профилирование crypto_orderflow_service.py
#
# Использует py-spy для non-intrusive профилирования (не нужно менять код).
# Профиль снимается прямо внутри Docker контейнера.
#
# Использование:
#   ./tools/profile_orderflow.sh                   # профилирует scanner-crypto-orderflow
#   ./tools/profile_orderflow.sh meme              # профилирует scanner-crypto-orderflow-meme
#   ./tools/profile_orderflow.sh meme-2 60         # профилирует meme-2, длительность 60 сек
#
# Результат:
#   /tmp/of_profile_<container>_<timestamp>.svg    (flamegraph, открывается браузером)
#   stdout: top functions в виде таблицы
# =============================================================================

set -euo pipefail

VARIANT="${1:-}"   # "", "2", "meme", "meme-2"
DURATION="${2:-30}"  # секунды профилирования

# Собираем имя контейнера
if [[ -z "$VARIANT" ]]; then
    CONTAINER="scanner-crypto-orderflow"
elif [[ "$VARIANT" == "2" ]]; then
    CONTAINER="scanner-crypto-orderflow-2"
elif [[ "$VARIANT" == "meme" ]]; then
    CONTAINER="scanner-crypto-orderflow-meme"
elif [[ "$VARIANT" == "meme-2" ]]; then
    CONTAINER="scanner-crypto-orderflow-meme-2"
else
    CONTAINER="scanner-crypto-orderflow-${VARIANT}"
fi

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTFILE="/tmp/of_profile_${CONTAINER}_${TIMESTAMP}.svg"

echo "🔍 Профилирование контейнера: $CONTAINER"
echo "   Длительность: ${DURATION}s"
echo "   Вывод: $OUTFILE"
echo ""

# Проверяем, запущен ли контейнер
if ! docker inspect "$CONTAINER" &>/dev/null; then
    echo "❌ Контейнер '$CONTAINER' не найден."
    echo "   Доступные контейнеры:"
    docker ps --format "{{.Names}}" | grep -E "orderflow|scanner" || true
    exit 1
fi

# Устанавливаем py-spy внутри контейнера (если ещё нет)
echo "📦 Проверяем py-spy в контейнере..."
if ! docker exec "$CONTAINER" sh -c "command -v py-spy" &>/dev/null; then
    echo "   Устанавливаем py-spy..."
    docker exec "$CONTAINER" pip install py-spy --quiet || {
        echo "❌ Не удалось установить py-spy. Попробуйте вручную:"
        echo "   docker exec -it $CONTAINER pip install py-spy"
        exit 1
    }
fi

# Находим PID основного процесса Python (crypto_orderflow_service)
echo "🔍 Ищем PID процесса crypto_orderflow_service..."
PID=$(docker exec "$CONTAINER" sh -c \
    "ps aux | grep 'crypto_orderflow_service' | grep -v grep | awk '{print \$1}' | head -1")

if [[ -z "$PID" ]]; then
    echo "❌ Процесс crypto_orderflow_service не найден внутри $CONTAINER"
    echo "   Все Python процессы:"
    docker exec "$CONTAINER" ps aux | grep python || true
    exit 1
fi

echo "   PID: $PID"
echo ""

# ─── 1. Top table (быстрый обзор) ────────────────────────────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 TOP FUNCTIONS (${DURATION}s sample):"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker exec "$CONTAINER" py-spy top \
    --pid "$PID" \
    --duration "$DURATION" \
    --nonblocking \
    2>&1 || {
        echo "⚠️  py-spy top не удался. Пробуем flamegraph..."
    }

# ─── 2. Flamegraph SVG ────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔥 FLAMEGRAPH (${DURATION}s, async-aware):"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

INNER_SVG="/tmp/profile_${TIMESTAMP}.svg"

docker exec "$CONTAINER" py-spy record \
    --pid "$PID" \
    --duration "$DURATION" \
    --output "$INNER_SVG" \
    --format speedscope \
    --nonblocking \
    2>&1 && \
docker cp "$CONTAINER:$INNER_SVG" "$OUTFILE" && \
echo "✅ Flamegraph сохранён: $OUTFILE" || {

    # Запасной вариант: speedscope → svg через py-spy native svg
    docker exec "$CONTAINER" py-spy record \
        --pid "$PID" \
        --duration "$DURATION" \
        --output "/tmp/profile_${TIMESTAMP}_native.svg" \
        --nonblocking 2>&1 || true
    docker cp "$CONTAINER:/tmp/profile_${TIMESTAMP}_native.svg" "$OUTFILE" 2>/dev/null && \
    echo "✅ Flamegraph (native svg) сохранён: $OUTFILE" || \
    echo "⚠️  Flamegraph не удалось сохранить. Используйте только top-вывод выше."
}

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📌 Интерпретация:"
echo "   • Если asyncio/event_loop занимает >80% → узкое место в питоне"
echo "   • Если redis/xreadgroup → медленное IO с Redis"
echo "   • Если ML/numpy/torch → инференс блокирует Event Loop"
echo "   • Если json.loads/json.dumps → сериализация под нагрузкой"
echo ""
echo "📂 Для просмотра flamegraph откройте в браузере:"
echo "   xdg-open $OUTFILE  (Linux)"
echo "   # Или: python3 -m http.server 9999 --directory /tmp/"
echo "   # затем откройте http://localhost:9999/$(basename $OUTFILE)"
