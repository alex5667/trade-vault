#!/bin/bash

# Скрипт установки Redis Streams Cleanup как systemd сервис

set -e

# Цвета
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}🚀 Установка Redis Streams Cleanup Service${NC}"
echo -e "${BLUE}========================================${NC}"

# Проверяем, что мы root
if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}❌ Этот скрипт должен быть запущен от root${NC}"
    echo -e "${YELLOW}Запустите: sudo ./install-redis-cleanup.sh${NC}"
    exit 1
fi

# Копируем файлы сервиса
echo -e "${YELLOW}📁 Копирование файлов сервиса...${NC}"
cp redis-cleanup.service /etc/systemd/system/
cp redis-cleanup.timer /etc/systemd/system/

# Делаем скрипт очистки исполняемым
echo -e "${YELLOW}🔧 Настройка прав доступа...${NC}"
chmod +x redis-streams-cleanup.sh

# Перезагружаем systemd
echo -e "${YELLOW}🔄 Перезагрузка systemd...${NC}"
systemctl daemon-reload

# Включаем и запускаем сервис
echo -e "${YELLOW}🚀 Включение и запуск сервиса...${NC}"
systemctl enable redis-cleanup.service
systemctl enable redis-cleanup.timer
systemctl start redis-cleanup.timer

# Проверяем статус
echo -e "${YELLOW}📊 Проверка статуса...${NC}"
systemctl status redis-cleanup.timer --no-pager -l

echo -e "\n${GREEN}✅ Redis Streams Cleanup Service успешно установлен!${NC}"
echo -e "${BLUE}📅 Очистка будет выполняться каждые 6 часов:${NC}"
echo -e "${YELLOW}  00:00, 06:00, 12:00, 18:00${NC}"
echo -e "\n${BLUE}🔧 Полезные команды:${NC}"
echo -e "${YELLOW}  systemctl status redis-cleanup.timer${NC}"
echo -e "${YELLOW}  systemctl list-timers${NC}"
echo -e "${YELLOW}  journalctl -u redis-cleanup.service -f${NC}"
echo -e "${YELLOW}  ./redis-streams-cleanup.sh setup  # Ручной запуск${NC}" 