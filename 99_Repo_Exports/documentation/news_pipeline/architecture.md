# Архитектура Новостного Пайплайна

## Обзор Архитектуры

Новостной пайплайн построен на принципах распределенной обработки данных в реальном времени с использованием Redis Streams для коммуникации между компонентами. Система спроектирована для высокой надежности, масштабируемости и отказоустойчивости.

## Компонентная Архитектура

### 1. Источники Данных (Sources Layer)

#### Поддерживаемые Источники

##### RSS Feeds
```go
// RSS источник - основной источник для структурированных новостей
type RSSSource struct {
    Name          string
    URLs          []string
    HTTPTimeout   time.Duration
    UserAgent     string
    NewsUIDBucket string
}
```

**Особенности:**
- Поддержка множества RSS URLs
- Автоматическое обнаружение новых статей
- Дедупликация по URL и заголовку
- Настраиваемый User-Agent для обхода блокировок

##### CryptoPanic API
```python
# Специализированный источник для крипто-новостей
cryptopanic_config = {
    "enabled": true,
    "currencies": ["BTC", "ETH", "SOL", "BNB"],
    "filter": "important",
    "kind": "news",
    "region": "en"
}
```

**Особенности:**
- Фильтрация по важности (important/hot)
- Поддержка мульти-валюты
- Региональная фильтрация
- Структурированные данные с категориями

##### Financial Modeling Prep (FMP)
```python
# Экономические данные и календарь событий
fmp_config = {
    "enabled": true,
    "tickers": ["SPY", "QQQ", "AAPL", "MSFT", "NVDA"],
    "economic": {
        "countries": ["US", "EU"],
        "importance": ["High", "Medium"]
    }
}
```

**Особенности:**
- Экономический календарь с важностью событий
- Акции и ETF данные
- Крипто-валюты
- Фильтрация по странам и важности

##### NewsAPI
```python
# Общий новостной API с гибкой фильтрацией
newsapi_config = {
    "enabled": true,
    "q": "(bitcoin OR ethereum OR FOMC OR CPI OR NFP)",
    "language": "en"
}
```

**Особенности:**
- Гибкий поисковый запрос
- Фильтрация по языку
- Источники с рейтингом достоверности

### 2. Ингестор (Ingestion Layer)

#### Go News Ingestor (Primary)
```go
type Pipeline struct {
    Redis            *redis.Client
    StreamNewsRaw    string
    StreamCalEvents  string
    StreamNewsHB     string
    StreamCalHB      string
    DedupeTTL        time.Duration
    MaxStreamLen     int64
    HeartbeatTTL     time.Duration
    InstanceID       string
    Logger           *log.Logger
}
```

**Ключевые Функции:**
- **Лидер-Элекшен**: Redis-based выбор лидера для предотвращения дублирования
- **Дедупликация**: TTL-based предотвращение повторной обработки
- **Нормализация**: Стандартизация данных от разных источников
- **Хартбиты**: Мониторинг здоровья компонентов

#### Python Standby Ingestor
```python
class StandbyIngestor:
    def __init__(self, redis_client, sources_config):
        self.redis = redis_client
        self.sources = sources_config
        self.leader_lock_key = "news:ingestor:leader"
        self.health_key = "news:health:last_ingest_ts_ms"
```

**Роль в системе:**
- Мониторинг здоровья Go ингестора
- Автоматический failover при отказе primary
- Синхронизация с leader election системой

#### Календарный Ингестор
```go
type FMPCalendarSource struct {
    Name          string
    APIKey        string
    BaseURL       string
    HTTPTimeout   time.Duration
    UserAgent     string
    LookaheadDays int
    BackDays      int
    Countries     []string
    Importance    []string
    Enabled       bool
}
```

**Особенности:**
- Предварительная загрузка календаря на 14 дней вперед
- Фильтрация по важности событий
- Многострановая поддержка
- Автоматическое обновление

### 3. Анализатор (Analysis Layer)

#### LLM Клиент (Gemini)
```python
class GeminiHTTPClient(LLMClient):
    def __init__(self):
        self.api_key = os.getenv("GEMINI_API_KEY")
        self.model = os.getenv("GEMINI_MODEL", "gemini-1.5-pro")
        self.timeout_sec = float(os.getenv("GEMINI_TIMEOUT_SEC", "10"))
        self.max_retries = int(os.getenv("GEMINI_RETRIES", "2"))
        self.temperature = float(os.getenv("GEMINI_TEMPERATURE", "0.2"))
        self.max_tokens = int(os.getenv("GEMINI_MAX_TOKENS", "256"))
```

**Prompt Engineering:**
```python
prompt = (
    "Return ONLY compact JSON object with keys:\n"
    "risk (0..1 float), surprise (-1..1 float), confidence (0..1 float),\n"
    "tags (array of strings from allowed set),\n"
    "primary_tag (string from same set), summary (<=160 chars).\n\n"
    f"allowed_tags={sorted(self.allowed_tags)}\n"
    f"source={source}\nurl={url}\ntitle={title}\n"
)
```

#### Оценка Риска и Важности
- **Risk Score (0-1)**: Вероятность влияния на рынок
- **Surprise Score (-1..1)**: Неожиданность события
- **Confidence Score (0-1)**: Уверенность модели в анализе
- **Tags**: Категоризация по типам событий

#### Stream Worker Pattern
```python
class NewsAnalyzerWorker(StreamWorker):
    def handle_message(self, msg_id: str, fields: Dict[str, Any]) -> None:
        # 1. Idempotency check
        # 2. LLM analysis
        # 3. Store heavy data
        # 4. Emit to analysis stream
```

### 4. Хранилище Фич (Feature Store Layer)

#### Redis Streams Архитектура
```
news:raw         → Сырые новости от ингестора
news:analysis    → Проанализированные новости
news:agg:{SYMBOL} → Агрегированные фичи по символу
news:analysis:{UID} → Детальный JSON анализ
```

#### EMA Агрегация
```python
def ema(prev: float, x: float, alpha: float) -> float:
    return (alpha * x) + ((1.0 - alpha) * prev)

# alpha = 2 / (half_life_minutes + 1)
# half_life = 30 минут для новостного риска
```

**Преимущества EMA:**
- Экспоненциальное затухание старых новостей
- Низкая латентность вычислений
- Автоматическая адаптация к волатильности

#### TTL Управление
```python
ANALYSIS_TTL_SEC = 259200  # 3 дня для детального анализа
ANALYSIS_DONE_TTL_SEC = 604800  # 7 дней для дедупликации
FEATURE_TTL_SEC = 3600  # 1 час для агрегированных фич
```

## Поток Данных

### Основной Pipeline

```
1. Источники → 2. Ингестор → 3. Анализатор → 4. Feature Store → 5. Tick Loop
     ↓             ↓             ↓              ↓              ↓
   RSS/API     news:raw     news:analysis   news:agg:*     ctx.news
```

### Детальный Поток

#### Шаг 1: Ингест
```python
# NewsRawItem → Redis Stream
raw_item = NewsRawItem(
    uid=stable_uid(url, title, published_ts),
    source="cryptopanic",
    title=title,
    url=url,
    ts_ms=published_ts,
    symbol="BTC",
    asset_class="crypto"
)
```

#### Шаг 2: Анализ
```python
# LLM анализ одного сообщения
analysis = llm.analyze(title=title, url=url, source=source)

# Результат
{
    "risk": 0.8,
    "surprise": 0.3,
    "tags": ["fomc", "rates"],
    "primary_tag": "fomc",
    "confidence": 0.9,
    "summary": "Fed signals potential rate cut..."
}
```

#### Шаг 3: Агрегация
```python
# EMA обновление для каждого затронутого символа
for symbol in impacted_symbols:
    key = f"news:agg:{symbol}"
    current_risk = redis.hget(key, "risk") or 0.0
    new_risk = ema(float(current_risk), analysis.risk, alpha)
    redis.hset(key, "risk", new_risk)
```

## Масштабируемость и Надежность

### Горизонтальное Масштабирование

#### Множественные Анализаторы
```python
# Каждый инстанс использует уникальный consumer name
analyzer_1 = NewsAnalyzerWorker(consumer="analyzer-1")
analyzer_2 = NewsAnalyzerWorker(consumer="analyzer-2")
analyzer_3 = NewsAnalyzerWorker(consumer="analyzer-3")
```

#### Redis Cluster
- Автоматическое шардирование
- Репликация для высокой доступности
- Автоматический failover

### Отказоустойчивость

#### Leader Election
```python
# Redis-based distributed lock
leader_key = "news:ingestor:leader"
if redis.set(leader_key, instance_id, ex=30, nx=True):
    # I am the leader
    start_ingestion()
```

#### Health Monitoring
```python
# Heartbeat pattern
redis.set("hb:news", int(time.time()*1000), ex=60)
redis.set("hb:calendar", int(time.time()*1000), ex=60)
```

#### Graceful Degradation
```python
# Fail-open при недоступности LLM
if not gemini_api_key:
    return {
        "risk": 0.0,
        "surprise": 0.0,
        "tags": [],
        "confidence": 0.0,
        "summary": title[:160]
    }
```

## Производительность

### Оптимизации

#### Batch Processing
```python
# Обработка пачками для снижения latency
msgs = redis.xreadgroup(
    groupname=group,
    consumername=consumer,
    streams={stream: ">"},
    count=100,  # batch size
    block=2000  # 2 секунды ожидания
)
```

#### Connection Pooling
```python
# Redis connection pool
r = redis.Redis(
    host='redis-worker-1',
    port=6379,
    db=0,
    decode_responses=True,
    max_connections=20,
    retry_on_timeout=True
)
```

#### Memory Management
```python
# Ограничение длины streams
redis.xtrim("news:raw", maxlen=10000, approximate=False)
redis.xtrim("news:analysis", maxlen=50000, approximate=False)
```

### Ресурсные Требования

| Компонент | CPU | RAM | Disk | Network |
|-----------|-----|-----|------|---------|
| Go Ingestor | 0.5 | 256MB | Low | Medium |
| News Analyzer | 1.0 | 1GB | Low | High (LLM) |
| Feature Store | 0.5 | 512MB | Low | Medium |
| Calendar Store | 0.5 | 512MB | Low | Low |

## Мониторинг и Observability

### Метрики

#### Business Metrics
- Количество обработанных новостей в минуту
- Средний risk score по категориям
- Точность LLM анализа (confidence distribution)

#### System Metrics
- Redis memory usage
- Stream lengths (news:raw, news:analysis)
- Consumer lag (XPENDING)
- API response times

#### Health Checks
```python
# HTTP health endpoints
@app.get("/health")
def health():
    # Redis ping
    # Stream accessibility
    # LLM API availability
    return {"status": "healthy"}
```

### Логирование

#### Структурированное Логирование
```python
log.info("news_processed", {
    "uid": news_uid,
    "source": source,
    "risk_score": risk,
    "processing_time_ms": processing_time,
    "symbols_affected": len(symbols)
})
```

#### Алерты

- **High Priority**: LLM API недоступен > 5 минут
- **Medium Priority**: Consumer lag > 1000 сообщений
- **Low Priority**: Redis memory > 80%

## Безопасность

### API Keys Management
```bash
# Environment variables only
GEMINI_API_KEY=your_secure_key
CRYPTOPANIC_AUTH_TOKEN=your_token
FMP_API_KEY=your_key

# Docker secrets в production
echo "your_key" | docker secret create gemini_api_key
```

### Network Security
- Redis доступ только из внутренней сети
- API calls через HTTPS с certificate validation
- Rate limiting на внешние API

### Data Validation
```python
# Input sanitization
def sanitize_input(text: str) -> str:
    return text.replace('<', '&lt;').replace('>', '&gt;')[:1000]
```

## Тестирование

### Unit Tests
```python
def test_ema_calculation():
    assert ema(0.5, 0.8, 0.2) == 0.58

def test_risk_score_clamping():
    assert clamp01(1.5) == 1.0
    assert clamp01(-0.5) == 0.0
```

### Integration Tests
```python
def test_full_pipeline():
    # 1. Inject news to news:raw
    # 2. Wait for analysis in news:analysis
    # 3. Verify aggregation in news:agg:*
    # 4. Check ctx.news enrichment
```

### Load Testing
- 1000 новостей/минуту
- 10 параллельных анализаторов
- Redis кластер с 3 нодами

## Deployment Patterns

### Docker Compose (Development)
```yaml
services:
  news-ingestor-go:
    build: ./go-news-services
    environment:
      - REDIS_URL=redis://redis:6379/0
      - NEWS_SOURCES_JSON=${NEWS_SOURCES_JSON}

  news-analyzer:
    build: ./python-worker
    command: python -m news_pipeline.analyzer_worker
    environment:
      - GEMINI_API_KEY=${GEMINI_API_KEY}
```

### Kubernetes (Production)
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: news-analyzer
spec:
  replicas: 3
  selector:
    matchLabels:
      app: news-analyzer
  template:
    spec:
      containers:
      - name: analyzer
        image: news-analyzer:latest
        env:
        - name: GEMINI_API_KEY
          valueFrom:
            secretKeyRef:
              name: llm-secrets
              key: gemini-api-key
```

### Serverless (Future)
- AWS Lambda для анализаторов
- Google Cloud Functions для ингесторов
- Redis Streams как event backbone
