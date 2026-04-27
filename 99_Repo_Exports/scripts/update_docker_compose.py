#!/usr/bin/env python3
"""
Обновляет docker-compose.yml для добавления переменных окружения второго Redis
"""

def update_docker_compose():
    """Обновляет конфигурацию в docker-compose.yml"""

    docker_compose_file = '/home/alex/front/trade/scanner_infra/docker-compose.yml'

    print("🔧 Обновление docker-compose.yml...")

    # Читаем файл
    with open(docker_compose_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Ищем секцию python-worker environment
    updated_lines = []
    in_python_worker = False
    in_environment = False
    redis_signals_added = False

    for i, line in enumerate(lines):  # noqa: B007
        updated_lines.append(line)

        # Проверяем, находимся ли мы в секции python-worker
        if 'python-worker:' in line and not in_python_worker:
            in_python_worker = True

        # Проверяем, находимся ли мы в секции environment
        if in_python_worker and 'environment:' in line:
            in_environment = True

        # Добавляем переменные для второго Redis после REDIS_SIGNALS_PORT
        if in_environment and 'REDIS_SIGNALS_PORT=' in line and not redis_signals_added:
            # Добавляем переменные для второго Redis
            indent = line[:len(line) - len(line.lstrip())]
            updated_lines.append(f"{indent}- REDIS_SIGNALS_HOST_2=redis-worker-2\n")
            updated_lines.append(f"{indent}- REDIS_SIGNALS_PORT_2=6379\n")
            redis_signals_added = True
            print("✅ Добавлены переменные окружения для redis-worker-2")

        # Выходим из секции environment при depends_on
        if in_environment and 'depends_on:' in line:
            in_environment = False
            in_python_worker = False

    # Записываем обратно
    with open(docker_compose_file, 'w', encoding='utf-8') as f:
        f.writelines(updated_lines)

    print("✅ docker-compose.yml обновлен!")

if __name__ == "__main__":
    update_docker_compose()
