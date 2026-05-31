#!/usr/bin/env python3
"""
PreToolUse hook: блокирует деструктивные команды в production-контексте.

Политика:
- FLUSHALL / FLUSHDB → block (потеря всех данных Redis)
- DROP DATABASE / DROP TABLE без WHERE → block
- rm -rf корневых путей → block
- kubectl delete --all → block
- Любая команда с явными prod-признаками в non-prod запросе → warn
"""

import json
import re
import sys

BLOCKED_PATTERNS = [
    (r"\bredis-cli\b.*\bFLUSHALL\b", "redis FLUSHALL уничтожит все данные"),
    (r"\bredis-cli\b.*\bFLUSHDB\b", "redis FLUSHDB уничтожит текущую БД"),
    (r"\bDROP\s+DATABASE\b", "DROP DATABASE — необратимо"),
    (r"\bDROP\s+TABLE\b(?!.*IF\s+EXISTS)", "DROP TABLE без IF EXISTS — проверь"),
    (r"\bDELETE\s+FROM\b.*\bWHERE\s+1\s*=\s*1\b", "DELETE WHERE 1=1 — удалит все строки"),
    (r"\brm\s+-rf\s+/\b", "rm -rf / — деструктивно"),
    (r"\brm\s+-rf\s+/home\b", "rm -rf /home — деструктивно"),
    (r"\bkubectl\s+delete\s+.*--all\b", "kubectl delete --all — опасно в prod"),
]

WARN_PATTERNS = [
    (r"\bsystemctl\s+(stop|restart)\b", "остановка/рестарт сервиса"),
    (r"\bdocker\s+rm\s+-f\b", "принудительное удаление контейнера"),
    (r"\bgit\s+push\s+.*--force\b", "force push"),
]


def main():
    hook_input = json.load(sys.stdin)
    tool_name = hook_input.get("tool_name", "")
    tool_input = hook_input.get("tool_input", {})

    if tool_name != "Bash":
        sys.exit(0)

    command = tool_input.get("command", "")

    # Check blocked patterns - hard block
    for pattern, reason in BLOCKED_PATTERNS:
        if re.search(pattern, command, re.IGNORECASE):
            print(
                json.dumps({
                    "decision": "block",
                    "reason": f"🛑 BLOCKED: {reason}\nКоманда: {command}\n"
                              f"Если уверен — подтверди явно в следующем сообщении."
                })
            )
            sys.exit(0)

    # Check warn patterns - ask for confirmation
    for pattern, reason in WARN_PATTERNS:
        if re.search(pattern, command, re.IGNORECASE):
            # Just log to stderr as warning, don't block
            print(f"⚠️  Потенциально опасная операция: {reason}", file=sys.stderr)
            break

    # Allow
    sys.exit(0)


if __name__ == "__main__":
    main()
