# 🔧 Docker Build Fix - Исправление ошибок сборки

## ❌ Проблема

```
panic: runtime error: makeslice: len out of range
panic: runtime error: invalid memory address or nil pointer dereference
ERROR: Service 'go-worker-4h' failed to build
```

**Причина:** Ошибка в Docker buildkit/buildx при отображении прогресса сборки.

---

## ✅ Решение

### Вариант 1: Использовать Legacy Builder (рекомендуется)

```bash
# Экспорт переменных окружения
export DOCKER_BUILDKIT=0
export COMPOSE_DOCKER_CLI_BUILD=0

# Перезапуск сборки
make rebuild

# Или docker-compose напрямую
docker-compose build --no-cache
docker-compose up -d
```

### Вариант 2: Использовать --progress=plain

```bash
# В docker-compose.yml добавить:
x-build-args: &build-args
  BUILDKIT_PROGRESS: plain

# Или при сборке:
docker-compose build --progress=plain
```

### Вариант 3: Очистить Docker cache

```bash
# Полная очистка
docker system prune -af --volumes

# Затем пересборка
docker-compose build --no-cache
docker-compose up -d
```

### Вариант 4: Обновить Docker

```bash
# Обновить Docker до последней версии
sudo apt update
sudo apt install docker-ce docker-ce-cli containerd.io

# Или через snap
sudo snap refresh docker
```

---

## 🚀 Быстрое исправление (Make команды)

Добавьте в Makefile:

```makefile
.PHONY: build-legacy rebuild-legacy up-legacy

build-legacy:
	@echo "🔨 Сборка с legacy builder (без buildkit)..."
	@export DOCKER_BUILDKIT=0 && export COMPOSE_DOCKER_CLI_BUILD=0 && docker-compose build

rebuild-legacy:
	@echo "🔨 Полная пересборка с legacy builder..."
	@export DOCKER_BUILDKIT=0 && export COMPOSE_DOCKER_CLI_BUILD=0 && docker-compose build --no-cache
	@docker-compose up -d
	@echo "✅ Система пересобрана и запущена"

up-legacy:
	@echo "🚀 Запуск с legacy builder..."
	@export DOCKER_BUILDKIT=0 && export COMPOSE_DOCKER_CLI_BUILD=0 && docker-compose up -d
```

**Использование:**

```bash
# Сборка с legacy builder
make build-legacy

# Полная пересборка
make rebuild-legacy

# Запуск
make up-legacy
```

---

## 🔍 Диагностика

### Проверка текущего builder

```bash
# Проверить переменные окружения
echo "DOCKER_BUILDKIT=$DOCKER_BUILDKIT"
echo "COMPOSE_DOCKER_CLI_BUILD=$COMPOSE_DOCKER_CLI_BUILD"

# Проверить версию
docker version
docker-compose version
docker buildx version
```

### Проверка доступности памяти

```bash
# Память Docker
docker info | grep -i memory

# Системная память
free -h

# Docker диски
docker system df
```

---

## 🛠️ Пошаговое исправление

### Шаг 1: Остановить все контейнеры

```bash
docker-compose down
```

### Шаг 2: Очистить build cache

```bash
# Опция 1: Только build cache
docker builder prune -af

# Опция 2: Полная очистка (осторожно!)
docker system prune -af --volumes
```

### Шаг 3: Отключить buildkit

```bash
export DOCKER_BUILDKIT=0
export COMPOSE_DOCKER_CLI_BUILD=0

# Сохранить в .bashrc для постоянного использования
echo 'export DOCKER_BUILDKIT=0' >> ~/.bashrc
echo 'export COMPOSE_DOCKER_CLI_BUILD=0' >> ~/.bashrc
```

### Шаг 4: Пересобрать с нуля

```bash
docker-compose build --no-cache go-worker-4h

# Или все сервисы
docker-compose build --no-cache
```

### Шаг 5: Запустить

```bash
docker-compose up -d
```

---

## 🎯 Для конкретно go-worker-4h

### Сборка только этого сервиса

```bash
# С legacy builder
export DOCKER_BUILDKIT=0
docker-compose build --no-cache go-worker-4h
docker-compose up -d go-worker-4h

# Проверка
docker logs scanner-go-worker-4h --tail 30
```

### Если проблема persist

**Проверьте Dockerfile:**

```bash
# Посмотреть какой Dockerfile используется
grep -A 5 "go-worker-4h:" docker-compose.yml | grep dockerfile

# Проверить синтаксис
cat go-worker/Dockerfile
```

**Возможные проблемы в Dockerfile:**

1. Некорректный синтаксис
2. Слишком большой контекст сборки
3. Проблемы с многоэтапной сборкой (multi-stage)

---

## 🐛 Workaround для buildkit паники

### .env файл

Создайте/обновите `.env` в корне проекта:

```bash
# Отключить buildkit
DOCKER_BUILDKIT=0
COMPOSE_DOCKER_CLI_BUILD=0

# Для buildx использовать plain progress
BUILDKIT_PROGRESS=plain
```

### docker-compose.yml

Добавьте в начало файла:

```yaml
version: '3.8'

# Глобальные настройки сборки
x-build-defaults: &build-defaults
  args:
    BUILDKIT_INLINE_CACHE: 0
```

---

## 📊 Альтернативные решения

### 1. Использовать готовый образ

```yaml
services:
  go-worker-4h:
    image: your-registry/go-worker:latest # Вместо build
    # build:
    #   context: ./go-worker
```

### 2. Собрать локально

```bash
cd go-worker
docker build -t scanner_infra_go-worker-4h:latest .
cd ..

# Затем в docker-compose.yml использовать готовый образ
```

### 3. Использовать docker build напрямую

```bash
# Вместо docker-compose build
docker build -t scanner_infra_go-worker-4h:latest ./go-worker

# Затем запуск через docker-compose
docker-compose up -d
```

---

## ✅ Финальная команда

**Самый надёжный способ:**

```bash
#!/bin/bash

# 1. Остановить
docker-compose down

# 2. Отключить buildkit
export DOCKER_BUILDKIT=0
export COMPOSE_DOCKER_CLI_BUILD=0

# 3. Очистить cache
docker builder prune -af

# 4. Пересобрать
docker-compose build --no-cache --progress=plain

# 5. Запустить
docker-compose up -d

# 6. Проверить
docker ps
docker logs scanner-go-worker-4h --tail 30
```

**Сохраните как скрипт:**

```bash
# Создать скрипт
cat > safe_rebuild.sh << 'SCRIPT'
#!/bin/bash
set -e

echo "🔨 Безопасная пересборка с legacy builder..."

export DOCKER_BUILDKIT=0
export COMPOSE_DOCKER_CLI_BUILD=0

docker-compose down
docker builder prune -af
docker-compose build --no-cache --progress=plain
docker-compose up -d

echo "✅ Готово!"
SCRIPT

chmod +x safe_rebuild.sh

# Запустить
./safe_rebuild.sh
```

---

## 📚 Дополнительные ресурсы

- [Docker BuildKit Issues](https://github.com/moby/buildkit/issues)
- [Docker Compose Build](https://docs.docker.com/compose/compose-file/build/)

---

**Используйте legacy builder для обхода проблемы buildkit!** 🚀
