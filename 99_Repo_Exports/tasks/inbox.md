# Task Inbox

Этот файл заполняется Telegram-ботом через `antigravity_task_watcher.py`.

Запустить вотчер:
```bash
source .env && python3 scripts/antigravity_task_watcher.py \
  --redis-url "$REDIS_URL" \
  --output tasks/inbox.md \
  --poll-interval 30
```

Выполнить накопившиеся задачи в Claude Code:
```
/tasks
```

<!-- Задачи будут добавлены сюда автоматически -->
