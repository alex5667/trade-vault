---
name: tasks
description: Execute pending tasks from Telegram inbox
---

# Execute Telegram Tasks

This workflow reads tasks from `tasks/inbox.md` (populated by the Telegram task bot)
and executes them one by one.

## Steps

1. Read the file `tasks/inbox.md` in the project root to see all pending tasks.

2. For each pending task (sections starting with `## Task #`):
   - Read the task description (the text below the `**From:**` line).
   - Execute the task as if the user typed it directly into the chat.
   - After completing each task, mark it as done by sending `/done <task_id>` 
     via Redis. Run this command:
     ```bash
     // turbo
     source .env 2>/dev/null && python3 -c "
     import redis, os
     r = redis.Redis.from_url(os.environ.get('REDIS_URL', 'redis://127.0.0.1:6379/0'), decode_responses=True)
     import json
     tasks = r.lrange('antigravity:inbox', 0, -1)
     for raw in tasks:
         t = json.loads(raw)
         if t.get('id') == '<TASK_ID>':
             t['status'] = 'done'
             r.rpush('antigravity:done', json.dumps(t))
             r.lrem('antigravity:inbox', 1, raw)
             print(f'Task #{t[\"id\"]} marked done')
             break
     "
     ```
     Replace `<TASK_ID>` with the actual task ID from the markdown.

3. After all tasks are processed, regenerate `tasks/inbox.md` by running:
   ```bash
   // turbo
   source .env 2>/dev/null && python3 scripts/antigravity_task_watcher.py --redis-url "$REDIS_URL" --output tasks/inbox.md --poll-interval 0 2>/dev/null &
   sleep 2 && kill %1 2>/dev/null
   ```

4. Report completion to the user.


# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.

# Security Restrictions: TELEGRAM READ-ONLY MODE
**CRITICAL REQUIREMENT:** Когда вы выполняете задачи из Telegram (через этот рабочий процесс / очередь), вы должны применять режим СТРОГОГО ЧТЕНИЯ (READ-ONLY) к кодовой базе.
ЗАПРЕЩЕНО:
- Изменять, редактировать, переписывать или удалять любые файлы исходного кода проекта.
- Вносить изменения в конфигурацию, если это явно не санкционировано в обход этого правила.
РАЗРЕШЕНО:
- Получать информацию о работе проекта, инфраструктуры, метриках.
- Выполнять SQL запросы к базе данных (Postgres/Timescale) или запросы в Redis (только чтение: SELECT, GET, LRANGE, etc).
- Запускать внутренние LLM модели или логику для анализа логов или данных.
Если запрос через Telegram требует изменения кода - вы ДОЛЖНЫ отказаться, сославшись на то, что это Telegram-бот для управления и мониторинга, а изменение кода через него запрещено.
