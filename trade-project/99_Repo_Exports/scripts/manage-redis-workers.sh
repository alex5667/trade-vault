#!/bin/bash

# Управление Redis воркерами для scanner-infra

set -e

# Цвета
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

# Функция показа справки
show_help() {
    echo -e "${BLUE}Управление Redis воркерами для scanner-infra${NC}"
    echo
    echo "Использование: $0 [команда]"
    echo
    echo "Команды:"
    echo "  start-all     - Запустить все Redis воркеры"
    echo "  stop-all      - Остановить все Redis воркеры"
    echo "  restart-all   - Перезапустить все Redis воркеры"
    echo "  status        - Показать статус всех воркеров"
    echo "  logs          - Показать логи всех воркеров"
    echo "  stats         - Показать статистику ресурсов"
    echo "  test          - Тестировать все воркеры"
    echo "  help          - Показать эту справку"
    echo
    echo "Примеры:"
    echo "  $0 start-all"
    echo "  $0 status"
    echo "  $0 test"
}

# Функция запуска всех воркеров
start_all_workers() {
    echo -e "${YELLOW}🚀 Запуск всех Redis воркеров...${NC}"
    
    docker-compose up -d redis redis-worker-1 redis-worker-2
    
    echo -e "${GREEN}✅ Все воркеры запущены${NC}"
}

# Функция остановки всех воркеров
stop_all_workers() {
    echo -e "${YELLOW}⏹️  Остановка всех Redis воркеров...${NC}"
    
    docker-compose stop redis redis-worker-1 redis-worker-2
    
    echo -e "${GREEN}✅ Все воркеры остановлены${NC}"
}

# Функция перезапуска всех воркеров
restart_all_workers() {
    echo -e "${YELLOW}🔄 Перезапуск всех Redis воркеров...${NC}"
    
    stop_all_workers
    sleep 5
    start_all_workers
    
    echo -e "${GREEN}✅ Все воркеры перезапущены${NC}"
}

# Функция показа статуса
show_status() {
    echo -e "${BLUE}📊 Статус Redis воркеров${NC}"
    echo -e "${BLUE}========================${NC}"
    
    # Основной Redis
    echo -e "${YELLOW}🔧 Основной Redis (порт 6379):${NC}"
    if docker ps | grep -q "scanner-redis"; then
        echo -e "${GREEN}✅ Запущен${NC}"
        docker exec scanner-redis redis-cli ping 2>/dev/null | grep -q "PONG" && echo -e "${GREEN}✅ Отвечает${NC}" || echo -e "${RED}❌ Не отвечает${NC}"
    else
        echo -e "${RED}❌ Остановлен${NC}"
    fi
    
    # Воркер 1
    echo -e "${YELLOW}🔧 Воркер 1 (порт 6380):${NC}"
    if docker ps | grep -q "scanner-redis-worker-1"; then
        echo -e "${GREEN}✅ Запущен${NC}"
        docker exec scanner-redis-worker-1 redis-cli ping 2>/dev/null | grep -q "PONG" && echo -e "${GREEN}✅ Отвечает${NC}" || echo -e "${RED}❌ Не отвечает${NC}"
    else
        echo -e "${RED}❌ Остановлен${NC}"
    fi
    
    # Воркер 2
    echo -e "${YELLOW}🔧 Воркер 2 (порт 6381):${NC}"
    if docker ps | grep -q "scanner-redis-worker-2"; then
        echo -e "${GREEN}✅ Запущен${NC}"
        docker exec scanner-redis-worker-2 redis-cli ping 2>/dev/null | grep -q "PONG" && echo -e "${GREEN}✅ Отвечает${NC}" || echo -e "${RED}❌ Не отвечает${NC}"
    else
        echo -e "${RED}❌ Остановлен${NC}"
    fi
}

# Функция показа логов
show_logs() {
    echo -e "${BLUE}📋 Логи Redis воркеров${NC}"
    echo -e "${BLUE}====================${NC}"
    
    echo -e "${YELLOW}🔧 Основной Redis:${NC}"
    docker logs scanner-redis --tail 10
    
    echo -e "${YELLOW}🔧 Воркер 1:${NC}"
    docker logs scanner-redis-worker-1 --tail 10
    
    echo -e "${YELLOW}🔧 Воркер 2:${NC}"
    docker logs scanner-redis-worker-2 --tail 10
}

# Функция показа статистики
show_stats() {
    echo -e "${BLUE}📊 Статистика ресурсов Redis воркеров${NC}"
    echo -e "${BLUE}=====================================${NC}"
    
    docker stats scanner-redis scanner-redis-worker-1 scanner-redis-worker-2 --no-stream
}

# Функция тестирования
test_workers() {
    echo -e "${BLUE}🧪 Тестирование Redis воркеров${NC}"
    echo -e "${BLUE}==============================${NC}"
    
    # Тест основного Redis
    echo -e "${YELLOW}🔍 Тест основного Redis (localhost:6379):${NC}"
    if timeout 5 redis-cli -h localhost -p 6379 ping 2>/dev/null | grep -q "PONG"; then
        echo -e "${GREEN}✅ Основной Redis работает${NC}"
    else
        echo -e "${RED}❌ Основной Redis не работает${NC}"
    fi
    
    # Тест воркера 1
    echo -e "${YELLOW}🔍 Тест воркера 1 (localhost:6380):${NC}"
    if timeout 5 redis-cli -h localhost -p 6380 ping 2>/dev/null | grep -q "PONG"; then
        echo -e "${GREEN}✅ Воркер 1 работает${NC}"
    else
        echo -e "${RED}❌ Воркер 1 не работает${NC}"
    fi
    
    # Тест воркера 2
    echo -e "${YELLOW}🔍 Тест воркера 2 (localhost:6381):${NC}"
    if timeout 5 redis-cli -h localhost -p 6381 ping 2>/dev/null | grep -q "PONG"; then
        echo -e "${GREEN}✅ Воркер 2 работает${NC}"
    else
        echo -e "${RED}❌ Воркер 2 не работает${NC}"
    fi
}

# Основная логика
case "${1:-help}" in
    "start-all")
        start_all_workers
        ;;
    "stop-all")
        stop_all_workers
        ;;
    "restart-all")
        restart_all_workers
        ;;
    "status")
        show_status
        ;;
    "logs")
        show_logs
        ;;
    "stats")
        show_stats
        ;;
    "test")
        test_workers
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
