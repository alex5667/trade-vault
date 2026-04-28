#!/bin/bash
# Автоматическая синхронизация Obsidian Vault с GitHub
# Время запуска: 02:45 UTC (05:45 EEST)

cd /home/alex/Apps/Obsidian/trade-vault || exit 1

# Проверяем, есть ли изменения
if [ -z "$(git status --porcelain)" ]; then
    echo "$(date +'%Y-%m-%d %H:%M:%S') - Нет изменений для коммита."
    exit 0
fi

git add .
git commit -m "Auto-sync vault notes $(date +'%Y-%m-%d %H:%M:%S')"
git push -u origin main

echo "$(date +'%Y-%m-%d %H:%M:%S') - Синхронизация завершена успешно."
