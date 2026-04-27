#!/bin/bash
set -e

BACKUP_DIR="/media/alex/DATA_COLD/minik_backup"
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="${BACKUP_DIR}/minik_full_${DATE}.tar.gz"
LOG_FILE="${BACKUP_DIR}/minik_full_${DATE}.log"

echo "==================================================="
echo "Создание полного бэкапа minik -> ${BACKUP_FILE}"
echo "==================================================="
echo -n "Введите sudo пароль для alex@minik: "
read -s SUDO_PASS
echo ""
echo "Запуск архивации через SSH..."

# Используем sudo -S для чтения пароля из stdin. 
# Stderr команды tar перенаправляем в локальный файл логов, чтобы не смешивать с потоком данных (stdout).
echo "$SUDO_PASS" | ssh minik "sudo -S tar -czpf - \
    --exclude=/proc \
    --exclude=/sys \
    --exclude=/dev \
    --exclude=/run \
    --exclude=/mnt \
    --exclude=/media \
    --exclude=/tmp \
    --exclude=/var/tmp \
    / 2>>/tmp/tar_error.log" > "${BACKUP_FILE}"

echo "Архив успешно скачан."
echo "Размер архива:"
ls -lh "${BACKUP_FILE}"

echo "Запуск проверки целостности архива (tar -tzf)..."
if tar -tzf "${BACKUP_FILE}" > /dev/null; then
    echo "==================================================="
    echo "УСПЕХ: Бэкап ${BACKUP_FILE} проверен и является целостным."
else
    echo "ОШИБКА: Проверка бэкапа не пройдена. Архив поврежден или неполный."
    exit 1
fi
