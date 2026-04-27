#!/bin/bash

BACKUP_DIR="./config_backups"
TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
BACKUP_PATH="$BACKUP_DIR/backup_$TIMESTAMP"

echo "🗄️  Создание backup конфигурации..."
echo ""

# Создать директорию для backup
mkdir -p "$BACKUP_PATH"

# Список файлов для backup
FILES_TO_BACKUP=(
    "docker-compose.yml"
    "go-worker/infra/redisclient/client.go"
    "python-worker/core/redis_client.py"
    "Dockerfile.cleanup"
    "redis-monitor.sh"
    "redis-health-check.sh"
    "redis-stress-monitor.sh"
    "REDIS_HOST_MIGRATION.md"
    "REDIS_HOST_QUICK_REF.md"
    "REDIS_TROUBLESHOOTING.md"
)

echo "Копирование файлов..."
copied=0
for file in "${FILES_TO_BACKUP[@]}"; do
    if [ -f "$file" ]; then
        # Создать структуру директорий
        dir=$(dirname "$file")
        mkdir -p "$BACKUP_PATH/$dir"
        
        # Копировать файл
        cp "$file" "$BACKUP_PATH/$file"
        echo "  ✅ $file"
        ((copied++))
    else
        echo "  ⚠️  $file (не найден)"
    fi
done

# Сохранить информацию о backup
cat > "$BACKUP_PATH/backup_info.txt" << ENDINFO
Backup создан: $(date '+%Y-%m-%d %H:%M:%S')
Хост: $(hostname)
Скопировано файлов: $copied

Git информация:
$(git log -1 --oneline 2>/dev/null || echo "Git не доступен")

Docker сервисы (на момент backup):
$(docker-compose ps 2>/dev/null | grep scanner || echo "Docker не доступен")

Redis статус:
$(docker exec scanner-redis-worker-1 redis-cli INFO clients 2>/dev/null | grep connected_clients || echo "Redis недоступен")
ENDINFO

# Создать архив
cd "$BACKUP_DIR"
tar -czf "backup_$TIMESTAMP.tar.gz" "backup_$TIMESTAMP" 2>/dev/null

if [ $? -eq 0 ]; then
    rm -rf "backup_$TIMESTAMP"
    backup_size=$(du -h "backup_$TIMESTAMP.tar.gz" | cut -f1)
    
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "✅ Backup создан успешно!"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "📦 Архив: config_backups/backup_$TIMESTAMP.tar.gz"
    echo "📊 Размер: $backup_size"
    echo "📁 Файлов: $copied"
    echo ""
    echo "Для восстановления:"
    echo "  tar -xzf config_backups/backup_$TIMESTAMP.tar.gz"
    echo ""
    
    # Показать список всех backup'ов
    echo "Доступные backup'ы:"
    ls -lh "$BACKUP_DIR"/*.tar.gz 2>/dev/null | tail -5 | awk '{print "  " $9 " (" $5 ")"}'
    
    # Удалить старые backup'ы (оставить последние 10)
    backup_count=$(ls -1 "$BACKUP_DIR"/*.tar.gz 2>/dev/null | wc -l)
    if [ "$backup_count" -gt 10 ]; then
        echo ""
        echo "Удаление старых backup'ов (оставлено последние 10)..."
        ls -t "$BACKUP_DIR"/*.tar.gz | tail -n +11 | xargs rm -f
    fi
else
    echo "❌ Ошибка при создании архива"
    exit 1
fi

