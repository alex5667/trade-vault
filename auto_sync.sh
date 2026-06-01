#!/bin/bash
# Автоматическая двусторонняя синхронизация Obsidian Vault (GitHub + Google Drive)
# Время запуска: 02:45 UTC (05:45 EEST)

VAULT_DIR="/home/alex/Apps/Obsidian/trade-vault"
GDRIVE_DEST="gdrive:"
GDRIVE_FOLDER_ID="1KvXSt851J7W6HmZEAR9WrHDWIw4I-zWO"

cd "$VAULT_DIR" || exit 1

echo "$(date +'%Y-%m-%d %H:%M:%S') - Запуск скрипта синхронизации..."

# --- 1. Синхронизация с GitHub ---
export GIT_SSH_COMMAND="ssh -i /home/alex/.ssh/id_ed25519 -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new"

HAS_CHANGES=false
if [ -n "$(git status --porcelain)" ]; then
    HAS_CHANGES=true
fi

HAS_UNPUSHED=false
if [ -n "$(git log @{u}.. 2>/dev/null)" ]; then
    HAS_UNPUSHED=true
fi

if [ "$HAS_CHANGES" = true ]; then
    git add .
    git commit -m "Auto-sync vault notes $(date +'%Y-%m-%d %H:%M:%S')"
fi

if [ "$HAS_CHANGES" = true ] || [ "$HAS_UNPUSHED" = true ]; then
    echo "$(date +'%Y-%m-%d %H:%M:%S') - Выполняется git push..."
    if git push -u origin main; then
        echo "$(date +'%Y-%m-%d %H:%M:%S') - Синхронизация с GitHub завершена успешно."
    else
        echo "$(date +'%Y-%m-%d %H:%M:%S') - Ошибка при отправке в GitHub (git push)." >&2
    fi
else
    echo "$(date +'%Y-%m-%d %H:%M:%S') - Нет изменений для коммита в GitHub."
fi

# --- 2. Синхронизация с Google Drive (rclone) ---
echo "$(date +'%Y-%m-%d %H:%M:%S') - Выполняется rclone sync в Google Drive..."

# Используем rclone sync для зеркалирования локальной папки в облако.
# Ключ --exclude ".git/**" пропускает служебную папку git, так как там тысячи мелких файлов, 
# которые сильно замедляют синхронизацию с облаком и не нужны для простого просмотра заметок.
if rclone sync "$VAULT_DIR" "$GDRIVE_DEST" --drive-root-folder-id="$GDRIVE_FOLDER_ID" --exclude ".git/**"; then
    echo "$(date +'%Y-%m-%d %H:%M:%S') - Синхронизация с Google Drive завершена успешно."
    exit 0
else
    echo "$(date +'%Y-%m-%d %H:%M:%S') - Ошибка при выполнении rclone sync." >&2
    exit 1
fi
