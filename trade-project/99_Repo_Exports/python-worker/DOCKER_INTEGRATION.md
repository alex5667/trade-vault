# OF Gate SRE Monitoring - Docker Integration Guide

## Вариант 1: Добавить в существующий python-worker контейнер

### 1. Обновить Dockerfile

Добавить в `python-worker/Dockerfile`:

```dockerfile
# Install cron
RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*

# Copy crontab
COPY crontab.of-gate-sre /etc/cron.d/of-gate-sre
RUN chmod 0644 /etc/cron.d/of-gate-sre && \
    crontab /etc/cron.d/of-gate-sre && \
    touch /var/log/cron.log

# Copy entrypoint
COPY entrypoint-sre.sh /entrypoint-sre.sh
RUN chmod +x /entrypoint-sre.sh
```

### 2. Обновить docker-compose.yml

Изменить entrypoint для `scanner-python-worker`:

```yaml
services:
  scanner-python-worker:
    # ... existing config ...
    entrypoint: ["/entrypoint-sre.sh"]
    command: ["python", "main.py"]  # or your existing command
    volumes:
      - /var/lib/trade:/var/lib/trade  # для артефактов
```

---

## Вариант 2: Отдельный sidecar контейнер (рекомендуется)

Добавить в `docker-compose.yml`:

```yaml
services:
  scanner-of-gate-sre:
    build:
      context: ./python-worker
      dockerfile: Dockerfile
    container_name: scanner-of-gate-sre
    restart: unless-stopped
    environment:
      - REDIS_URL=redis://redis-worker-1:6379/0
      - OF_GATE_METRICS_STREAM=metrics:of_gate
      - NOTIFY_TELEGRAM_STREAM=notify:telegram
    volumes:
      - /var/lib/trade:/var/lib/trade
    networks:
      - scanner-network
    entrypoint: ["/entrypoint-sre.sh"]
    command: ["tail", "-f", "/dev/null"]  # keep alive for cron
```

**Преимущества:**
- Изолированный мониторинг
- Не влияет на основной worker
- Легко включить/выключить

---

## Вариант 3: Простой loop (без cron)

```yaml
services:
  scanner-of-gate-sre:
    build:
      context: ./python-worker
    container_name: scanner-of-gate-sre
    restart: unless-stopped
    environment:
      - REDIS_URL=redis://redis-worker-1:6379/0
    volumes:
      - /var/lib/trade:/var/lib/trade
    command: >
      sh -c "
        while true; do
          echo '[SRE] Running hourly checks...'
          python -m tools.of_gate_sre_monitor --always 0 || true
          python -m tools.bench_of_gate_latency --out /var/lib/trade/of_bench/latency_bench_last.json || true
          
          # Nightly jobs (check hour)
          HOUR=\$$(date +%H)
          if [ \"\$$HOUR\" = \"02\" ]; then
            echo '[SRE] Running nightly golden snapshot...'
            python -m tools.of_gate_golden_snapshot --window-hours 24 --baseline /var/lib/trade/of_gate_golden/baseline.json --out-dir /var/lib/trade/of_gate_golden || true
          fi
          if [ \"\$$HOUR\" = \"03\" ]; then
            echo '[SRE] Running nightly replay...'
            python -m tools.golden_replay_of_confirm_from_redis --out-dir /var/lib/trade/of_replay --since-hours 24 --baseline /var/lib/trade/of_replay/baseline.ndjson || true
          fi
          
          sleep 3600
        done
      "
```

---

## Применение

### Шаг 1: Выбрать вариант и обновить конфиги

### Шаг 2: Пересобрать контейнер

```bash
cd /home/alex/front/trade/scanner_infra
docker-compose build scanner-python-worker  # или scanner-of-gate-sre
```

### Шаг 3: Перезапустить

```bash
docker-compose up -d scanner-python-worker  # или scanner-of-gate-sre
```

### Шаг 4: Проверить логи

```bash
# Для cron варианта
docker exec scanner-of-gate-sre tail -f /var/log/of_gate_sre.log

# Для loop варианта
docker logs -f scanner-of-gate-sre
```

---

## Проверка работы

```bash
# Проверить cron jobs
docker exec scanner-of-gate-sre crontab -l

# Проверить последние результаты
docker exec scanner-of-gate-sre cat /var/lib/trade/of_bench/latency_bench_last.json

# Проверить алерты в Redis
docker exec redis-worker-1 redis-cli XREVRANGE notify:telegram + - COUNT 5
```

---

## Baseline уже создан

✅ OF Gate Golden Snapshot: `/var/lib/trade/of_gate_golden/baseline.json`
✅ OF Confirm Replay: `/var/lib/trade/of_replay/baseline.ndjson` (в процессе)

Nightly jobs будут сравнивать с этим baseline.
