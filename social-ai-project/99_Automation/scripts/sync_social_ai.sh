#!/bin/bash
# Ежедневное обновление данных проекта social-ai-infra (репозиторий tik) в Obsidian.
# Расписание: 00:45 UTC (03:45 EEST) через cron.
# Шаги: 1) экспорт markdown из репозитория в vault; 2) генерация индекс-заметки;
#       3) переиспользование проверенного auto_sync.sh (git push + rclone -> Google Drive).
set -uo pipefail

REPO_DIR="/home/alex/front/tik"
VAULT_DIR="/home/alex/Apps/Obsidian/trade-vault"
PROJECT_DIR="$VAULT_DIR/social-ai-project"
EXPORT_DIR="$PROJECT_DIR/99_Repo_Exports"
INDEX_FILE="$PROJECT_DIR/00_Index/social-ai-index.md"
LOG_DIR="$PROJECT_DIR/99_Automation/logs"

mkdir -p "$EXPORT_DIR" "$LOG_DIR"
TS="$(date +'%Y-%m-%d_%H-%M-%S')"
LOG="$LOG_DIR/update_${TS}.log"

exec >>"$LOG" 2>&1
echo "=== START $(date --iso-8601=seconds) ==="

if [ ! -d "$REPO_DIR" ]; then
    echo "$(date +'%T') - ОШИБКА: репозиторий $REPO_DIR не найден." >&2
    exit 1
fi

# --- 1. Экспорт markdown из репозитория в vault (зеркало, с удалением исчезнувших) ---
rsync -a --delete --delete-excluded --prune-empty-dirs \
    --exclude='.git/***' \
    --exclude='node_modules/***' \
    --exclude='.claude/***' \
    --exclude='.codex/***' \
    --exclude='.agent/***' \
    --exclude='.pytest_cache/***' \
    --include='*/' \
    --include='*.md' \
    --exclude='*' \
    "$REPO_DIR/" "$EXPORT_DIR/"
echo "$(date +'%T') - Экспорт markdown из репозитория завершён ($(find "$EXPORT_DIR" -name '*.md' | wc -l) файлов)."

# --- 2. Генерация индекс-заметки ---
REPO_HEAD="$(git -C "$REPO_DIR" rev-parse --short HEAD 2>/dev/null || echo 'n/a')"
REPO_MSG="$(git -C "$REPO_DIR" log -1 --pretty=%s 2>/dev/null || echo 'n/a')"
REPO_BRANCH="$(git -C "$REPO_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || echo 'n/a')"
{
    echo "---"
    echo "title: social-ai-infra — индекс проекта"
    echo "updated: $(date --iso-8601=seconds)"
    echo "source_repo: $REPO_DIR"
    echo "repo_branch: $REPO_BRANCH"
    echo "repo_commit: $REPO_HEAD"
    echo "tags: [social-ai, project, moc]"
    echo "---"
    echo
    echo "# social-ai-infra — индекс проекта"
    echo
    echo "> Автообновление 1 раз в сутки в **00:45 UTC** (03:45 EEST)."
    echo "> Последнее обновление: **$(date --iso-8601=seconds)**"
    echo "> Репозиторий: \`$REPO_DIR\` · ветка \`$REPO_BRANCH\` · коммит \`$REPO_HEAD\` — $REPO_MSG"
    echo
    echo "## Экспортированные документы"
    echo
    cd "$EXPORT_DIR" || exit 1
    find . -name '*.md' | sort | while read -r f; do
        rel="${f#./}"
        echo "- [[99_Repo_Exports/$rel|$rel]]"
    done
} > "$INDEX_FILE"
echo "$(date +'%T') - Индекс-заметка обновлена: $INDEX_FILE"

# --- 3. Синхронизация vault (git + Google Drive) через проверенный скрипт ---
if [ -x "$VAULT_DIR/auto_sync.sh" ]; then
    echo "$(date +'%T') - Запуск auto_sync.sh (git push + rclone)..."
    "$VAULT_DIR/auto_sync.sh"
    echo "$(date +'%T') - auto_sync.sh завершён (код $?)."
else
    echo "$(date +'%T') - ПРЕДУПРЕЖДЕНИЕ: $VAULT_DIR/auto_sync.sh не найден/не исполняемый — синк пропущен." >&2
fi

echo "=== DONE $(date --iso-8601=seconds) ==="
