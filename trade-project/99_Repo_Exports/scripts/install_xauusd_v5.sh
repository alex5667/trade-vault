#!/bin/bash
#
# XAUUSD Order Flow v5.0.0 - Установка "под ключ"
#
# Автоматическая установка всех компонентов v5
#

set -e

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                                                                ║"
echo "║     XAUUSD Order Flow v5.0.0 - Установка под ключ             ║"
echo "║                                                                ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Определяем текущую директорию
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKER_DIR="$SCRIPT_DIR/python-worker"

echo "📁 Директория проекта: $SCRIPT_DIR"
echo "📁 Python worker: $WORKER_DIR"
echo ""

# ═══════════════════════════════════════════════════════════════
# 1. Проверка зависимостей
# ═══════════════════════════════════════════════════════════════

echo "🔍 Проверка зависимостей..."

if ! command -v python3 &> /dev/null; then
    echo "❌ python3 не найден. Установите: sudo apt install python3"
    exit 1
fi

if ! command -v pip3 &> /dev/null; then
    echo "❌ pip3 не найден. Установите: sudo apt install python3-pip"
    exit 1
fi

if ! command -v redis-cli &> /dev/null; then
    echo "⚠️  redis-cli не найден. Установите: sudo apt install redis-tools"
fi

echo "✅ Основные зависимости проверены"
echo ""

# ═══════════════════════════════════════════════════════════════
# 2. Установка Python пакетов
# ═══════════════════════════════════════════════════════════════

echo "📦 Установка Python пакетов..."
cd "$WORKER_DIR"

pip3 install --user -q redis requests pytz fastapi uvicorn[standard] \
     pandas numpy pyarrow pytest matplotlib scikit-learn

echo "✅ Python пакеты установлены"
echo ""

# ═══════════════════════════════════════════════════════════════
# 3. User-level systemd setup
# ═══════════════════════════════════════════════════════════════

echo "🔧 Настройка user-level systemd units..."

# Создать директорию для user units
mkdir -p ~/.config/systemd/user

# Скопировать unit files
cp "$SCRIPT_DIR/deploy/systemd/user/"*.service ~/.config/systemd/user/

echo "✅ Unit files скопированы в ~/.config/systemd/user/"

# Включить linger (сервисы работают после logout)
if ! loginctl show-user "$USER" | grep -q "Linger=yes"; then
    echo "🔧 Включение linger..."
    loginctl enable-linger "$USER" 2>/dev/null || echo "⚠️  Не удалось включить linger (требуется для auto-start after reboot)"
fi

# Reload systemd
systemctl --user daemon-reload

echo "✅ Systemd настроен"
echo ""

# ═══════════════════════════════════════════════════════════════
# 4. Запуск тестов
# ═══════════════════════════════════════════════════════════════

echo "🧪 Запуск тестов..."
cd "$WORKER_DIR"

if pytest tests/ -q 2>/dev/null; then
    echo "✅ Все тесты прошли успешно"
else
    echo "⚠️  Некоторые тесты не прошли (проверьте pytest output)"
fi
echo ""

# ═══════════════════════════════════════════════════════════════
# 5. Опциональный запуск сервисов
# ═══════════════════════════════════════════════════════════════

echo "🚀 Хотите запустить сервисы сейчас? (y/n)"
read -r -p "> " response

if [[ "$response" =~ ^[Yy]$ ]]; then
    echo ""
    echo "🔄 Запуск сервисов..."
    
    systemctl --user enable --now xau-atr.service
    systemctl --user enable --now xau-labeler.service
    
    sleep 2
    
    echo ""
    echo "📊 Статус сервисов:"
    systemctl --user status xau-atr.service --no-pager -l
    echo ""
    systemctl --user status xau-labeler.service --no-pager -l
    echo ""
    
    echo "✅ Сервисы запущены"
else
    echo ""
    echo "ℹ️  Сервисы не запущены. Запустить вручную:"
    echo "   systemctl --user start xau-atr.service"
    echo "   systemctl --user start xau-labeler.service"
fi

echo ""

# ═══════════════════════════════════════════════════════════════
# 6. Финальные инструкции
# ═══════════════════════════════════════════════════════════════

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                                                                ║"
echo "║     ✅ XAUUSD Order Flow v5.0.0 установлен!                    ║"
echo "║                                                                ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "📚 Следующие шаги:"
echo ""
echo "1. Проверить сервисы:"
echo "   systemctl --user status xau-atr.service"
echo "   systemctl --user status xau-labeler.service"
echo ""
echo "2. Проверить ATR в Redis:"
echo "   redis-cli GET atr:val:XAUUSD:1m"
echo ""
echo "3. Проверить labels stream:"
echo "   redis-cli XLEN labels:trades"
echo ""
echo "4. Включить v5 features в handler:"
echo "   export ATR_SOURCE=redis"
echo "   export ATR_TF=1m"
echo "   export USE_TELEGRAM_BUTTONS=1"
echo "   docker-compose restart python-worker"
echo ""
echo "5. Прочитать документацию:"
echo "   cat docs/XAUUSD/V5_RELEASE_NOTES.md"
echo "   cat docs/XAUUSD/LABELING_GUIDE.md"
echo "   cat deploy/systemd/user/README_USER_SYSTEMD.md"
echo ""
echo "📊 Полезные команды:"
echo "   ./XAUUSD_COMMANDS.sh status"
echo "   journalctl --user -u xau-atr.service -f"
echo "   journalctl --user -u xau-labeler.service -f"
echo ""
echo "🎉 Успешной торговли! 📈💰"
echo ""

