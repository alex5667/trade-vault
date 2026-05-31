#!/bin/bash
# Автоматическая синхронизация Obsidian Vault с GitHub
# Время запуска: 02:45 UTC (05:45 EEST)

cd /home/alex/Apps/Obsidian/trade-vault || exit 1

# Явно задаем SSH-команду для использования ключа в окружении cron без SSH-агента
export GIT_SSH_COMMAND="ssh -i /home/alex/.ssh/id_ed25519 -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new"

# Проверяем локальные изменения и неотправленные коммиты
HAS_CHANGES=false
if [ -n "$(git status --porcelain)" ]; then
    HAS_CHANGES=true
fi

HAS_UNPUSHED=false
if [ -n "$(git log @{u}.. 2>/dev/null)" ]; then
    HAS_UNPUSHED=true
fi

if [ "$HAS_CHANGES" = false ] && [ "$HAS_UNPUSHED" = false ]; then
    echo "$(date +'%Y-%m-%d %H:%M:%S') - Нет изменений для коммита и отправки."
    exit 0
fi

if [ "$HAS_CHANGES" = true ]; then
    git add .
    git commit -m "Auto-sync vault notes $(date +'%Y-%m-%d %H:%M:%S')"
fi

echo "$(date +'%Y-%m-%d %H:%M:%S') - Выполняется git push..."
if git push -u origin main; then
    echo "$(date +'%Y-%m-%d %H:%M:%S') - Синхронизация завершена успешно."
    exit 0
else
    echo "$(date +'%Y-%m-%d %H:%M:%S') - Ошибка при отправке в удаленный репозиторий (git push)." >&2
    exit 1
fi
