# Мониторинг и Обслуживание Новостного Пайплайна

## Обзор

Мониторинг новостного пайплайна включает отслеживание здоровья всех компонентов, производительности обработки, качества данных и влияния на торговые сигналы. Система использует комбинацию health checks, метрик, алертов и логирования для обеспечения надежной работы.

## Архитектура Мониторинга

### Компоненты Мониторинга

#### Health Checks
- **HTTP Endpoints**: `/health` для каждого сервиса
- **Redis Keys**: Heartbeat ключи для отслеживания активности
- **Stream Monitoring**: Проверка длины очередей и скорости обработки

#### Метрики
- **Prometheus**: Коллекция и хранение метрик
- **Grafana**: Визуализация и дашборды
- **Custom Exporters**: Специфические метрики для новостного пайплайна

#### Логирование
- **Structured Logging**: JSON логи с контекстом
- **Log Aggregation**: Централизованное хранение и поиск
- **Sampling**: Выборочное логирование для высоконагруженных компонентов

#### Алerts
- **PagerDuty/OpsGenie**: Критические алерты
- **Email/Slack**: Информационные уведомления
- **Escalation**: Автоматическое повышение приоритета

## Health Checks

### HTTP Health Endpoints

#### Go News Ingestor
```go
func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
    // Проверка Redis подключения
    if err := s.redis.Ping(r.Context()).Err(); err != nil {
        http.Error(w, "redis_error", 500)
        return
    }

    // Проверка heartbeat
    lastIngest, err := s.redis.Get(r.Context(), "hb:news").Result()
    if err != nil || lastIngest == "" {
        http.Error(w, "no_heartbeat", 500)
        return
    }

    // Проверка свежести данных (не старше 10 минут)
    lastTs, _ := strconv.ParseInt(lastIngest, 10, 64)
    if time.Now().UnixMilli()-lastTs > 10*60*1000 {
        http.Error(w, "stale_data", 500)
        return
    }

    w.WriteHeader(200)
    w.Write([]byte("ok"))
}
```

#### Python Services
```python
@app.get("/health")
def health_check():
    """Комплексная проверка здоровья Python сервиса"""
    health = {
        "status": "healthy",
        "timestamp": time.time(),
        "checks": {}
    }

    try:
        # Redis connectivity
        redis.ping()
        health["checks"]["redis"] = "ok"
    except Exception as e:
        health["checks"]["redis"] = f"error: {e}"
        health["status"] = "unhealthy"

    try:
        # Stream length check
        raw_len = redis.xlen("news:raw")
        analysis_len = redis.xlen("news:analysis")

        health["checks"]["streams"] = {
            "news_raw": raw_len,
            "news_analysis": analysis_len
        }

        # Alert on queue buildup
        if raw_len > 10000 or analysis_len > 50000:
            health["status"] = "degraded"

    except Exception as e:
        health["checks"]["streams"] = f"error: {e}"
        health["status"] = "unhealthy"

    # LLM API check (sampled)
    if random.random() < 0.1:  # 10% of checks
        try:
            # Quick API test (not full analysis)
            llm_client.test_connection()
            health["checks"]["llm"] = "ok"
        except Exception as e:
            health["checks"]["llm"] = f"error: {e}"

    status_code = 200 if health["status"] == "healthy" else 503
    return JSONResponse(content=health, status_code=status_code)
```

### Docker Health Checks
```yaml
# docker-compose.yml
services:
  news-ingestor-go:
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8097/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s

  news-analyzer:
    healthcheck:
      test: ["CMD", "python", "-c", "import requests; requests.get('http://localhost:8098/health')"]
      interval: 60s
      timeout: 15s
      retries: 3
```

## Метрики

### Prometheus Metrics

#### Business Metrics
```python
# Количество обработанных новостей
NEWS_PROCESSED = Counter(
    'news_processed_total',
    'Total news items processed',
    ['source', 'status']
)

# Качество анализа
ANALYSIS_QUALITY = Histogram(
    'news_analysis_quality',
    'Analysis quality distribution',
    buckets=[0.1, 0.3, 0.5, 0.7, 0.9]
)

# Risk distribution
NEWS_RISK_DISTRIBUTION = Histogram(
    'news_risk_score',
    'Distribution of news risk scores',
    buckets=[0.0, 0.2, 0.4, 0.6, 0.8, 1.0]
)

# Grade distribution
NEWS_GRADE_DISTRIBUTION = Histogram(
    'news_grade_distribution',
    'Distribution of news grades',
    buckets=[0, 1, 2, 3, 4]
)
```

#### System Metrics
```python
# Latency метрики
INGESTION_LATENCY = Histogram(
    'news_ingestion_latency_seconds',
    'Time to ingest news from source'
)

ANALYSIS_LATENCY = Histogram(
    'news_analysis_latency_seconds',
    'Time to analyze news with LLM'
)

AGGREGATION_LATENCY = Histogram(
    'news_aggregation_latency_seconds',
    'Time to update feature aggregates'
)

# Error rates
INGESTION_ERRORS = Counter(
    'news_ingestion_errors_total',
    'Ingestion errors by source',
    ['source', 'error_type']
)

ANALYSIS_ERRORS = Counter(
    'news_analysis_errors_total',
    'Analysis errors by type',
    ['error_type']
)

# Queue metrics
STREAM_LENGTH = Gauge(
    'redis_stream_length',
    'Current length of Redis streams',
    ['stream']
)

PENDING_MESSAGES = Gauge(
    'redis_stream_pending',
    'Pending messages in consumer groups',
    ['stream', 'group']
)
```

#### Custom Exporters
```python
class NewsPipelineExporter:
    """Custom Prometheus exporter for news pipeline metrics"""

    def __init__(self, redis_client):
        self.redis = redis_client

    def collect(self):
        """Collect custom metrics"""

        # News source activity
        sources = ["cryptopanic", "fmp", "newsapi", "rss"]
        for source in sources:
            count = self.redis.get(f"metrics:news:processed:{source}") or 0
            yield GaugeMetricFamily(
                f'news_source_activity_{source}',
                f'News processed from {source}',
                value=int(count)
            )

        # Feature freshness
        symbols = ["BTCUSDT", "ETHUSDT", "GLOBAL"]
        for symbol in symbols:
            key = f"news:agg:{symbol}"
            data = self.redis.hgetall(key)

            if data and "last_ts_ms" in data:
                last_update = int(data["last_ts_ms"])
                age_seconds = (time.time() * 1000 - last_update) / 1000

                yield GaugeMetricFamily(
                    f'news_feature_age_seconds',
                    'Age of news features in seconds',
                    value=age_seconds,
                    labels={'symbol': symbol}
                )

        # LLM API quota
        quota_used = self.redis.get("metrics:llm:quota_used") or 0
        yield GaugeMetricFamily(
            'llm_api_quota_used',
            'LLM API quota used today',
            value=int(quota_used)
        )
```

## Логирование

### Structured Logging
```python
def log_news_ingestion(item: NewsRawItem, status: str, error: Optional[str] = None):
    """Структурированное логирование ingestion"""

    log_entry = {
        "timestamp": time.time(),
        "level": "INFO" if status == "success" else "ERROR",
        "component": "news_ingestor",
        "operation": "ingest_news",
        "news_uid": item.uid,
        "source": item.source,
        "title_length": len(item.title),
        "url_domain": extract_domain(item.url),
        "status": status,
        "symbol": item.symbol,
        "asset_class": item.asset_class,
    }

    if error:
        log_entry["error"] = error
        log_entry["error_type"] = type(error).__name__

    # Sampling для high-volume
    if random.random() < 0.1:  # 10% sampling
        json_logger.info("news_ingestion", log_entry)
    elif status == "error":  # Все ошибки логируем
        json_logger.error("news_ingestion", log_entry)
```

### Analysis Logging
```python
def log_llm_analysis(news_uid: str, analysis: Dict[str, Any], latency_ms: float):
    """Логирование результатов LLM анализа"""

    log_entry = {
        "timestamp": time.time(),
        "component": "news_analyzer",
        "operation": "llm_analysis",
        "news_uid": news_uid,
        "latency_ms": latency_ms,
        "risk_score": analysis.get("risk"),
        "surprise_score": analysis.get("surprise"),
        "confidence": analysis.get("confidence"),
        "tags": analysis.get("tags", []),
        "primary_tag": analysis.get("primary_tag"),
        "summary_length": len(analysis.get("summary", "")),
        "model": os.getenv("GEMINI_MODEL", "gemini-1.5-pro"),
    }

    # Условное логирование по важности
    grade = compute_news_grade_id(
        news_risk=analysis.get("risk", 0),
        confidence=analysis.get("confidence", 0),
        primary_tag_id=get_primary_tag_id(analysis.get("primary_tag", ""))
    )

    if grade >= 3 or random.random() < 0.05:  # High importance or 5% sampling
        json_logger.info("llm_analysis", log_entry)
```

### Debug Logging
```python
def debug_signal_enrichment(ctx, news_features: NewsFeatures):
    """Отладочное логирование обогащения сигналов"""

    if not should_debug_log(ctx.symbol):
        return

    debug_entry = {
        "timestamp": time.time(),
        "component": "signal_enrichment",
        "signal_id": ctx.signal_id,
        "symbol": ctx.symbol,
        "news_available": news_features is not None,
    }

    if news_features:
        debug_entry.update({
            "news_risk": news_features.news_risk,
            "surprise_score": news_features.surprise_score,
            "news_grade": news_features.news_grade_id,
            "confidence": news_features.confidence,
            "primary_tag": news_features.primary_tag_id,
            "event_tminus_sec": news_features.event_tminus_sec,
            "horizon_sec": news_features.horizon_sec,
        })

    json_logger.debug("signal_news_enrichment", debug_entry)
```

## Алerts и Уведомления

### Critical Alerts
```yaml
# Prometheus Alert Rules
groups:
  - name: news_pipeline_critical
    rules:
      - alert: NewsIngestionDown
        expr: up{job="news-ingestor-go"} == 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "News ingestion is down"
          description: "Go news ingestor has been down for 5 minutes"

      - alert: LLM_API_Failing
        expr: rate(news_analysis_errors_total{error_type="llm_api"}[5m]) > 0.8
        for: 10m
        labels:
          severity: critical
        annotations:
          summary: "LLM API failing at high rate"
          description: "LLM API error rate > 80% for 10 minutes"

      - alert: NewsQueueOverflow
        expr: redis_stream_length{stream="news:raw"} > 50000
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "News processing queue overflow"
          description: "Raw news queue length > 50k messages"
```

### Warning Alerts
```yaml
  - name: news_pipeline_warnings
    rules:
      - alert: HighAnalysisLatency
        expr: histogram_quantile(0.95, rate(news_analysis_latency_seconds_bucket[5m])) > 30
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "High news analysis latency"
          description: "95th percentile analysis latency > 30s"

      - alert: StaleNewsFeatures
        expr: news_feature_age_seconds > 1800
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "Stale news features"
          description: "News features older than 30 minutes"

      - alert: LowAnalysisQuality
        expr: histogram_quantile(0.5, rate(news_analysis_quality_bucket[10m])) < 0.6
        for: 20m
        labels:
          severity: warning
        annotations:
          summary: "Low analysis quality"
          description: "Median analysis confidence < 0.6 for 20 minutes"
```

### Info Alerts
```yaml
  - name: news_pipeline_info
    rules:
      - alert: SourceRateLimitHit
        expr: rate(news_ingestion_errors_total{error_type="rate_limit"}[1m]) > 0
        labels:
          severity: info
        annotations:
          summary: "News source rate limit hit"
          description: "External API rate limit encountered"

      - alert: HighVolumeNewsEvent
        expr: rate(news_processed_total[5m]) > 100
        labels:
          severity: info
        annotations:
          summary: "High volume news event"
          description: "News processing rate > 100/min"
```

## Grafana Dashboards

### News Pipeline Overview
```
Dashboard: News Pipeline Overview
├── News Processing Rate (per source)
├── Analysis Quality Metrics
├── Queue Lengths (Raw/Analysis)
├── Error Rates by Component
├── LLM API Usage & Quota
└── Feature Freshness by Symbol
```

### Trading Impact Dashboard
```
Dashboard: News Trading Impact
├── Signal Filtering Rate (by news grade)
├── Position Size Adjustments
├── Stop Loss Modifications
├── Trade PnL by News Context
├── Win Rate vs News Risk
└── Sharpe Ratio by News Grade
```

### System Performance Dashboard
```
Dashboard: News System Performance
├── Component Health Status
├── Memory/CPU Usage per Service
├── Redis Performance Metrics
├── Network I/O per Component
├── Database Query Performance
└── External API Response Times
```

## Troubleshooting

### Распространенные Проблемы

#### 1. News Ingestion Stopped
```bash
# Проверить здоровье сервиса
curl http://localhost:8097/health

# Проверить логи
docker logs news-ingestor-go --tail 100

# Проверить Redis connectivity
redis-cli ping

# Проверить heartbeat
redis-cli get hb:news

# Перезапустить если необходимо
docker restart news-ingestor-go
```

#### 2. LLM Analysis Failing
```bash
# Проверить API ключ
echo $GEMINI_API_KEY | head -c 10  # Показать первые 10 символов

# Проверить квоту
curl -H "x-goog-api-key: $GEMINI_API_KEY" \
     "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro?key=$GEMINI_API_KEY"

# Проверить логи анализатора
docker logs news-analyzer --tail 50

# Проверить rate limits
redis-cli get "metrics:llm:quota_used"
```

#### 3. High Queue Backlog
```bash
# Проверить длину очередей
redis-cli xlen news:raw
redis-cli xlen news:analysis

# Проверить pending messages
redis-cli xpending news:raw news-analyzer

# Масштабировать анализаторы
docker-compose up --scale news-analyzer=3

# Очистить старые сообщения если необходимо
redis-cli xtrim news:raw maxlen 10000
```

#### 4. Stale News Features
```bash
# Проверить время последнего обновления
redis-cli hget news:agg:BTCUSDT last_ts_ms

# Проверить логи feature store
docker logs news-feature-store --tail 30

# Перезапустить feature store
docker restart news-feature-store

# Принудительная ребилда фич (осторожно!)
redis-cli KEYS "news:agg:*" | xargs -n 1 redis-cli DEL
```

#### 5. Signal Enrichment Issues
```bash
# Проверить логи enricher
grep "news_enrich" /var/log/python-worker.log | tail -20

# Проверить data quality flags
grep "data_quality_flags" /var/log/python-worker.log | tail -10

# Проверить кеш производительности
redis-cli info stats | grep keyspace_hits
```

### Debug Commands

#### Полная Диагностика Системы
```bash
#!/bin/bash
# news_pipeline_diag.sh

echo "=== News Pipeline Diagnostics ==="

echo "1. Service Health:"
curl -s http://localhost:8097/health || echo "Go ingestor: DOWN"
curl -s http://localhost:8098/health || echo "News analyzer: DOWN"

echo "2. Queue Status:"
echo "Raw news queue: $(redis-cli xlen news:raw)"
echo "Analysis queue: $(redis-cli xlen news:analysis)"

echo "3. Processing Rates (last 5 min):"
echo "News processed: $(redis-cli get metrics:news:processed:total || echo 0)"
echo "Analysis errors: $(redis-cli get metrics:analysis:errors || echo 0)"

echo "4. Feature Freshness:"
for symbol in BTCUSDT ETHUSDT GLOBAL; do
    ts=$(redis-cli hget news:agg:$symbol last_ts_ms 2>/dev/null || echo 0)
    age=$(( ($(date +%s) * 1000 - ts) / 1000 / 60 ))  # minutes
    echo "$symbol: ${age}min ago"
done

echo "5. External API Status:"
# Test LLM API (sample)
if [ $((RANDOM % 10)) -eq 0 ]; then
    curl -s -H "x-goog-api-key: $GEMINI_API_KEY" \
         "https://generativelanguage.googleapis.com/v1beta/models" | \
         jq '.models[0].name' || echo "LLM API: ERROR"
fi
```

#### Инструмент Резолвинга Incident
```python
class IncidentResolver:
    """Автоматизированный incident response"""

    def __init__(self, redis_client, docker_client):
        self.redis = redis_client
        self.docker = docker_client

    def resolve_common_issues(self):
        """Решение типовых проблем"""

        # Issue 1: Queue overflow
        raw_len = self.redis.xlen("news:raw")
        if raw_len > 100000:
            self._scale_analyzers(5)
            self._alert_engineers("Queue overflow detected", f"Raw queue: {raw_len}")

        # Issue 2: High error rate
        error_rate = self._calculate_error_rate("news_analysis_errors_total", 300)
        if error_rate > 0.5:
            self._restart_service("news-analyzer")
            self._switch_to_backup_llm()

        # Issue 3: Stale data
        if self._check_feature_staleness() > 1800:  # 30 min
            self._restart_service("news-feature-store")
            self._rebuild_features()

    def _scale_analyzers(self, count: int):
        """Масштабирование анализаторов"""
        subprocess.run(["docker-compose", "up", "--scale", f"news-analyzer={count}"])

    def _restart_service(self, service: str):
        """Перезапуск сервиса"""
        subprocess.run(["docker", "restart", service])

    def _alert_engineers(self, title: str, message: str):
        """Отправка алерта инженерам"""
        # Integration with PagerDuty/Slack/etc
        pass
```

## Maintenance Procedures

### Ежедневное Обслуживание
```bash
# 1. Проверка здоровья всех компонентов
./health_check.sh

# 2. Мониторинг размеров очередей
./queue_monitor.sh

# 3. Проверка использования API квот
./quota_check.sh

# 4. Ротация логов
logrotate /etc/logrotate.d/news-pipeline
```

### Еженедельное Обслуживание
```bash
# 1. Очистка старых данных
redis-cli KEYS "news:analysis:*" | grep -E ":2[0-9]{13}" | xargs redis-cli DEL  # Старше 2 недель

# 2. Оптимизация Redis
redis-cli BGSAVE

# 3. Проверка индексов БД
./db_maintenance.sh

# 4. Обновление моделей LLM (если доступны)
./update_llm_models.sh
```

### Ежемесячное Обслуживание
```bash
# 1. Полная ребилда фич
./rebuild_features.sh

# 2. Анализ производительности
./performance_analysis.sh

# 3. Обновление конфигураций
./config_update.sh

# 4. Тестирование disaster recovery
./disaster_recovery_test.sh
```

## Производительность и Масштабирование

### Бенчмарки Производительности

| Компонент | P50 Latency | P95 Latency | Throughput |
|-----------|-------------|-------------|------------|
| News Ingestion | 50ms | 200ms | 1000 news/min |
| LLM Analysis | 2.1s | 4.8s | 25 news/min |
| Feature Aggregation | 15ms | 50ms | 2000 updates/min |
| Signal Enrichment | 5ms | 20ms | 10000 signals/min |

### Масштабирование

#### Вертикальное Масштабирование
```yaml
# Увеличение ресурсов для CPU-intensive задач
news-analyzer:
  deploy:
    resources:
      limits:
        cpus: '2.0'
        memory: 4G
      reservations:
        cpus: '1.0'
        memory: 2G
```

#### Горизонтальное Масштабирование
```yaml
# Множественные инстансы анализаторов
news-analyzer:
  deploy:
    replicas: 3
    placement:
      constraints:
        - node.labels.type == analysis
```

#### Auto-scaling
```yaml
# Автомасштабирование по длине очереди
news-analyzer:
  deploy:
    replicas: 1..10
  autoscale:
    min: 1
    max: 10
    target_queue_length: 1000
```

## Безопасность Мониторинга

### Защита Метрик
```nginx
# Nginx config для защиты /metrics
location /metrics {
    allow 10.0.0.0/8;  # Internal network only
    deny all;
    auth_basic "Metrics";
    auth_basic_user_file /etc/nginx/.htpasswd;
}
```

### Аудит Логов
```python
def audit_log_access(user: str, action: str, resource: str):
    """Аудит доступа к логам"""
    audit_entry = {
        "timestamp": time.time(),
        "user": user,
        "action": action,
        "resource": resource,
        "ip": get_client_ip(),
        "user_agent": get_user_agent()
    }

    # Запись в secure audit log
    secure_logger.info("audit_log_access", audit_entry)
```

### Incident Response
```yaml
# Incident Response Plan
incident_response:
  severity_levels:
    critical:
      - response_time: "15 minutes"
      - communication: "immediate"
      - escalation: "on-call engineer + manager"
    high:
      - response_time: "1 hour"
      - communication: "slack channel"
      - escalation: "team lead"
    medium:
      - response_time: "4 hours"
      - communication: "email"
      - escalation: "next business day"
```

## Заключение

Комплексный мониторинг новостного пайплайна обеспечивает надежную работу системы и своевременное реагирование на проблемы. Ключевые принципы:

1. **Proactive Monitoring**: Предиктивные метрики и алерты
2. **Comprehensive Coverage**: Мониторинг всех компонентов и интеграций
3. **Automated Response**: Автоматизированное разрешение типовых проблем
4. **Clear Escalation**: Четкие процедуры реагирования на инциденты
5. **Continuous Improvement**: Регулярный анализ и оптимизация

Эффективный мониторинг позволяет поддерживать высокую доступность и качество новостного пайплайна, что критически важно для надежности торговых сигналов.
