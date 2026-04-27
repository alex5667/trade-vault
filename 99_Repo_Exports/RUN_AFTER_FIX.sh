#!/bin/bash
# Скрипт для запуска системы после исправлений Signal Performance Tracker

set -e

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║   Запуск Scanner Infrastructure после исправлений              ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Цвета
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

# Функция с паузой
pause() {
    echo ""
    read -p "Нажмите Enter для продолжения..." </dev/tty
}

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Шаг 1: Проверка Telegram credentials${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

if [ -z "$TELEGRAM_BOT_TOKEN" ] || [ -z "$TELEGRAM_CHAT_ID" ]; then
    echo -e "${YELLOW}⚠️  Telegram credentials не установлены${NC}"
    echo ""
    echo "Варианты:"
    echo ""
    echo "1. Установить через переменные окружения:"
    echo "   export TELEGRAM_BOT_TOKEN='your_token'"
    echo "   export TELEGRAM_CHAT_ID='your_chat_id'"
    echo ""
    echo "2. Создать .env файл:"
    echo "   cat > .env << EOF"
    echo "   TELEGRAM_BOT_TOKEN=your_token"
    echo "   TELEGRAM_CHAT_ID=your_chat_id"
    echo "   EOF"
    echo ""
    echo -e "${RED}ВАЖНО: Без credentials статистика не будет отправляться в Telegram!${NC}"
    echo ""
    
    read -p "Установлены credentials? (y/n): " answer
    if [ "$answer" != "y" ]; then
        echo "Прервано. Установите credentials и запустите скрипт снова."
        exit 1
    fi
else
    echo -e "${GREEN}✅ TELEGRAM_BOT_TOKEN установлен${NC}"
    echo -e "${GREEN}✅ TELEGRAM_CHAT_ID установлен${NC}"
fi

echo ""
pause

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Шаг 2: Остановка текущей системы${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

echo "Останавливаю все сервисы..."
make down || true

echo -e "${GREEN}✅ Система остановлена${NC}"
echo ""
pause

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Шаг 3: Запуск системы с новым сервисом${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

echo "Запускаю систему в фоновом режиме..."
echo "Это займет около 1-2 минут..."
echo ""

make up-bg

echo ""
echo -e "${GREEN}✅ Система запущена${NC}"
echo ""
echo "Ожидание инициализации (30 секунд)..."
sleep 30

echo ""
pause

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Шаг 4: Проверка Signal Performance Tracker${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

make tracker-status

echo ""
pause

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Шаг 5: Проверка всех 3 сервисов XAUUSD${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

make check-xauusd-services

echo ""
pause

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Шаг 6: Тест отправки статистики в Telegram${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

echo "Отправляю тестовое сообщение в Telegram..."
echo ""

make test-tracker-telegram

echo ""
echo -e "${YELLOW}Проверьте Telegram - должно прийти сообщение со статистикой!${NC}"
echo ""

pause

echo -e "${GREEN}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║                    ✅ ВСЕ ГОТОВО!                              ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo "Система успешно запущена и проверена!"
echo ""
echo "📊 Что происходит сейчас:"
echo "   ✅ Multi-Symbol OrderFlow Handler обрабатывает тики"
echo "   ✅ Aggregated Hub V2 генерирует сигналы"
echo "   ✅ Signal Performance Tracker собирает статистику"
echo "   ✅ Go Gateway управляет ордерами"
echo ""
echo "📬 Когда ожидать отчеты:"
echo "   📊 Периодическая статистика: каждые 3 часа"
echo "   📊 Ежедневная сводка: в 00:00 UTC"
echo ""
echo "🔍 Мониторинг:"
echo "   make tracker-logs          # Логи трекера в реальном времени"
echo "   make check-xauusd-services # Проверка всех сервисов"
echo "   make tracker-status        # Статус трекера"
echo ""
echo "📚 Документация:"
echo "   COMPLETE_FIX_REPORT.md     # Полный отчет об исправлениях"
echo "   QUICK_FIX_GUIDE.md         # Быстрая инструкция"
echo "   documentation/             # Полная документация проекта"
echo ""
echo -e "${GREEN}🎉 Готово к production использованию!${NC}"
echo ""

