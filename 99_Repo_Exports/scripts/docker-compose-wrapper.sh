#!/bin/bash

# Обертка для docker-compose с автоматической очисткой портов

# Функция для очистки портов
clean_ports() {
    echo "🧹 Автоматическая очистка портов..."
    ./clear-ports.sh
    if [ $? -ne 0 ]; then
        echo "❌ Ошибка при очистке портов"
        exit 1
    fi
}

# Проверяем, нужно ли очищать порты
case "$1" in
    "up"|"up -d"|"up --build"|"up -d --build")
        echo "🚀 Запуск с автоматической очисткой портов..."
        clean_ports
        ;;
    "start")
        echo "🚀 Запуск с автоматической очисткой портов..."
        clean_ports
        ;;
esac

# Запускаем оригинальный docker-compose с переданными аргументами
docker-compose "$@"
