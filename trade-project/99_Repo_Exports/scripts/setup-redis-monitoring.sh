#!/bin/bash

# Setup Redis Monitoring для scanner-infra
# Установка и настройка полного мониторинга Redis

set -e

# Цвета
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}🚀 Setup Redis Monitoring для scanner-infra${NC}"
echo -e "${BLUE}===========================================${NC}"

# Функция проверки зависимостей
check_dependencies() {
    echo -e "${YELLOW}🔍 Проверка зависимостей...${NC}"
    
    # Проверяем Docker
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}❌ Docker не установлен${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ Docker установлен${NC}"
    
    # Проверяем redis-cli
    if ! command -v redis-cli &> /dev/null; then
        echo -e "${YELLOW}⚠️  redis-cli не установлен, будет использован Docker wrapper${NC}"
    else
        echo -e "${GREEN}✅ redis-cli установлен${NC}"
    fi
    
    # Проверяем jq
    if ! command -v jq &> /dev/null; then
        echo -e "${YELLOW}⚠️  jq не установлен, некоторые функции могут работать ограниченно${NC}"
    else
        echo -e "${GREEN}✅ jq установлен${NC}"
    fi
}

# Функция настройки внешних подключений
setup_external_access() {
    echo -e "${YELLOW}🔧 Настройка внешних подключений...${NC}"
    
    if [ -f "./redis-external-access-setup.sh" ]; then
        ./redis-external-access-setup.sh setup
    else
        echo -e "${RED}❌ Скрипт redis-external-access-setup.sh не найден${NC}"
        exit 1
    fi
}

# Функция настройки мониторинга памяти
setup_memory_monitoring() {
    echo -e "${YELLOW}📊 Настройка мониторинга памяти...${NC}"
    
    # Проверяем, что скрипты существуют
    if [ ! -f "./redis-memory-monitor-v2.sh" ]; then
        echo -e "${RED}❌ Скрипт redis-memory-monitor-v2.sh не найден${NC}"
        exit 1
    fi
    
    # Делаем скрипты исполняемыми
    chmod +x redis-memory-monitor-v2.sh
    chmod +x redis-connect.sh
    
    echo -e "${GREEN}✅ Скрипты мониторинга настроены${NC}"
}

# Функция настройки systemd сервиса
setup_systemd_service() {
    echo -e "${YELLOW}⚙️  Настройка systemd сервиса...${NC}"
    
    # Копируем сервис в systemd
    sudo cp redis-memory-monitor.service /etc/systemd/system/
    
    # Перезагружаем systemd
    sudo systemctl daemon-reload
    
    # Включаем сервис
    sudo systemctl enable redis-memory-monitor.service
    
    echo -e "${GREEN}✅ Systemd сервис настроен${NC}"
    echo -e "${BLUE}📋 Управление сервисом:${NC}"
    echo -e "  • Запуск: sudo systemctl start redis-memory-monitor"
    echo -e "  • Остановка: sudo systemctl stop redis-memory-monitor"
    echo -e "  • Статус: sudo systemctl status redis-memory-monitor"
    echo -e "  • Логи: sudo journalctl -u redis-memory-monitor -f"
}

# Функция создания cron задачи для очистки
setup_cron_cleanup() {
    echo -e "${YELLOW}⏰ Настройка автоматической очистки...${NC}"
    
    # Создаем скрипт для cron
    cat > redis-cleanup-cron.sh << 'CRON_EOF'
#!/bin/bash
# Автоматическая очистка Redis каждые 6 часов

cd /home/alex/front/trade/scanner_infra
./redis-memory-monitor-v2.sh cleanup >> /tmp/redis-cleanup.log 2>&1
CRON_EOF

    chmod +x redis-cleanup-cron.sh
    
    # Добавляем в crontab
    (crontab -l 2>/dev/null; echo "0 */6 * * * /home/alex/front/trade/scanner_infra/redis-cleanup-cron.sh") | crontab -
    
    echo -e "${GREEN}✅ Автоматическая очистка настроена (каждые 6 часов)${NC}"
}

# Функция тестирования
test_setup() {
    echo -e "${YELLOW}🧪 Тестирование настройки...${NC}"
    
    # Тест 1: Подключение к Redis
    echo -e "${BLUE}🔍 Тест 1: Подключение к Redis${NC}"
    if ./redis-connect.sh ping | grep -q "PONG"; then
        echo -e "${GREEN}✅ Подключение к Redis работает${NC}"
    else
        echo -e "${RED}❌ Подключение к Redis не работает${NC}"
        return 1
    fi
    
    # Тест 2: Мониторинг памяти
    echo -e "${BLUE}🔍 Тест 2: Мониторинг памяти${NC}"
    if ./redis-memory-monitor-v2.sh monitor > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Мониторинг памяти работает${NC}"
    else
        echo -e "${RED}❌ Мониторинг памяти не работает${NC}"
        return 1
    fi
    
    # Тест 3: Systemd сервис
    echo -e "${BLUE}🔍 Тест 3: Systemd сервис${NC}"
    if sudo systemctl is-enabled redis-memory-monitor.service > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Systemd сервис настроен${NC}"
    else
        echo -e "${YELLOW}⚠️  Systemd сервис не настроен${NC}"
    fi
    
    echo -e "${GREEN}🎉 Все тесты пройдены!${NC}"
}

# Функция показа справки
show_help() {
    echo -e "${BLUE}Setup Redis Monitoring для scanner-infra${NC}"
    echo
    echo "Использование: $0 [команда]"
    echo
    echo "Команды:"
    echo "  setup      - Полная настройка мониторинга (по умолчанию)"
    echo "  test       - Тестирование настройки"
    echo "  start      - Запуск мониторинга"
    echo "  stop       - Остановка мониторинга"
    echo "  status     - Статус мониторинга"
    echo "  logs       - Просмотр логов"
    echo "  help       - Показать эту справку"
    echo
    echo "Примеры:"
    echo "  $0 setup"
    echo "  $0 test"
    echo "  $0 start"
}

# Функция запуска мониторинга
start_monitoring() {
    echo -e "${YELLOW}🚀 Запуск мониторинга...${NC}"
    
    # Запускаем systemd сервис
    sudo systemctl start redis-memory-monitor.service
    
    # Проверяем статус
    sudo systemctl status redis-memory-monitor.service --no-pager
}

# Функция остановки мониторинга
stop_monitoring() {
    echo -e "${YELLOW}⏹️  Остановка мониторинга...${NC}"
    
    # Останавливаем systemd сервис
    sudo systemctl stop redis-memory-monitor.service
    
    echo -e "${GREEN}✅ Мониторинг остановлен${NC}"
}

# Функция показа статуса
show_status() {
    echo -e "${BLUE}📊 Статус мониторинга Redis${NC}"
    echo -e "${BLUE}============================${NC}"
    
    # Статус systemd сервиса
    echo -e "${YELLOW}🔧 Systemd сервис:${NC}"
    sudo systemctl status redis-memory-monitor.service --no-pager
    
    echo
    echo -e "${YELLOW}📈 Текущее состояние Redis:${NC}"
    ./redis-memory-monitor-v2.sh monitor
}

# Функция показа логов
show_logs() {
    echo -e "${BLUE}📋 Логи мониторинга Redis${NC}"
    echo -e "${BLUE}==========================${NC}"
    
    # Логи systemd сервиса
    echo -e "${YELLOW}🔧 Логи systemd сервиса:${NC}"
    sudo journalctl -u redis-memory-monitor.service --no-pager -n 20
    
    echo
    echo -e "${YELLOW}📊 Логи мониторинга памяти:${NC}"
    if [ -f "/tmp/redis-memory-monitor-v2.log" ]; then
        tail -20 /tmp/redis-memory-monitor-v2.log
    else
        echo "Логи мониторинга памяти не найдены"
    fi
}

# Основная функция настройки
setup_monitoring() {
    echo -e "${BLUE}🚀 Запуск полной настройки мониторинга Redis${NC}"
    
    check_dependencies
    setup_external_access
    setup_memory_monitoring
    setup_systemd_service
    setup_cron_cleanup
    test_setup
    
    echo
    echo -e "${GREEN}🎉 Настройка завершена!${NC}"
    echo -e "${BLUE}📋 Доступные команды:${NC}"
    echo -e "  • ./redis-connect.sh ping"
    echo -e "  • ./redis-memory-monitor-v2.sh monitor"
    echo -e "  • ./redis-memory-monitor-v2.sh daemon"
    echo -e "  • sudo systemctl start redis-memory-monitor"
    echo -e "  • sudo systemctl status redis-memory-monitor"
}

# Основная логика
case "${1:-setup}" in
    "setup")
        setup_monitoring
        ;;
    "test")
        test_setup
        ;;
    "start")
        start_monitoring
        ;;
    "stop")
        stop_monitoring
        ;;
    "status")
        show_status
        ;;
    "logs")
        show_logs
        ;;
    "help"|"-h"|"--help")
        show_help
        ;;
    *)
        echo -e "${RED}❌ Неизвестная команда: $1${NC}"
        show_help
        exit 1
        ;;
esac
