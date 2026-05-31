import re

makefile_path = "/home/alex/front/trade/scanner_infra/Makefile"
with open(makefile_path, "r") as f:
    makefile = f.read()

insertion = """\t\techo "⏰ Запуск Binance Execution / Reporter..."; \\
\t\t$(DOCKER_COMPOSE_BIN) -f docker-compose-binance.yml up -d binance-executor binance-account-reporter 2>/dev/null && \\
\t\techo "✅ Binance Execution и Reporter запущены" || echo "⚠️  Ошибка запуска Binance сервисов"; \\
"""

# Let's insert it right before OFC timers in both `up:` and `up-bg:`
target = '\t\techo "⏰ Запуск OFC таймеров (валидация, replay, fill expected, benchmark)..."; \\'
makefile = makefile.replace(target, insertion + "\n" + target)

with open(makefile_path, "w") as f:
    f.write(makefile)

print("Makefile updated successfully.")
