# Новостной Пайплайн Проекта

## Обзор

Новостной пайплайн представляет собой распределенную систему реального времени для сбора, анализа и интеграции новостной информации в процесс принятия торговых решений. Система состоит из нескольких компонентов, работающих совместно для обеспечения надежной и быстрой обработки новостей.

## Архитектура

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Источники     │───▶│   Ингестор      │───▶│   Анализатор    │───▶│   Хранилище    │
│   Новостей      │    │   (Go/Python)   │    │   (LLM-based)   │    │   Фич (Redis)  │
│                 │    │                 │    │                 │    │                 │
│ • RSS Feeds     │    │ • Дедупликация  │    │ • Оценка риска  │    │ • Агрегация     │
│ • CryptoPanic   │    │ • Нормализация  │    │ • Анализ тегов  │    │ • EMA фильтр    │
│ • Financial     │    │ • Redis Streams │    │ • Gemini LLM    │    │ • TTL управление│
│ • NewsAPI       │    │ • Leader Election│    │ • Confidence    │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
                                                       │
                                                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Tick Loop     │◀───│   Контекст      │    │   Торговые      │    │   Мониторинг    │
│   (Python)      │    │   Обогащения    │───▶│   Сигналы       │───▶│   и Алерты      │
│                 │    │   (ctx.news)    │    │   (Signals)     │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Компоненты

### 1. Источники Новостей (`sources/`)
- **RSS Feeds**: Структурированные новостные фиды
- **CryptoPanic API**: Специализированные крипто-новости
- **Financial Modeling Prep**: Экономические данные и календарь
- **NewsAPI**: Общие новости с фильтрацией

### 2. Ингестор (`ingestor/`)
- **Go News Ingestor**: Основной ингестор с лидер-элекшеном
- **Python Standby**: Резервный ингестор для отказоустойчивости
- **Календарь Событий**: Экономический календарь FMP

### 3. Анализатор (`analyzer/`)
- **LLM Анализ**: Google Gemini для оценки новостей
- **Оценка Риска**: 0-1 шкала важности новости
- **Анализ Тегов**: Классификация по категориям (FOMC, CPI, etc.)
- **Confidence Score**: Уверенность в анализе

### 4. Хранилище Фич (`feature_store/`)
- **Redis Streams**: Потоковая обработка анализов
- **EMA Агрегация**: Экспоненциальное сглаживание рисков
- **TTL Управление**: Автоматическая очистка старых данных
- **Многосимвольная Поддержка**: Агрегация по символам

## Ключевые Фичи

### Модели Данных

#### NewsRawItem (Входные данные)
```python
@dataclass(frozen=True, slots=True)
class NewsRawItem:
    uid: str           # Уникальный идентификатор новости (SHA256)
    source: str        # Источник (rss, cryptopanic, fmp, newsapi)
    title: str         # Заголовок новости
    url: str          # URL источника
    ts_ms: int        # Время публикации (Unix ms)
    symbol: str       # Связанный символ (опционально)
    asset_class: str  # Класс актива (crypto, equity, forex, commodity)
    raw_data: dict    # Оригинальные данные от источника
```

#### NormalizedNewsItem (Нормализованные данные)
```python
@dataclass(frozen=True, slots=True)
class NormalizedNewsItem:
    uid: str
    source: str
    title: str
    description: str
    url: str
    published_at: datetime
    published_ts_ms: int
    symbol: str
    asset_class: str
    tags: List[str]
    language: str
    sentiment: str
    metadata: Dict[str, Any]
```

#### NewsAnalysis (Результат LLM анализа)
```python
@dataclass(frozen=True, slots=True)
class NewsAnalysis:
    uid: str
    ts_ms: int
    symbol: str
    asset_class: str

    risk: float        # 0..1: Уровень риска/важности
    surprise: float    # -1..1: Неожиданность события
    confidence: float  # 0..1: Уверенность модели
    tags_mask: int     # Битовая маска тегов
    primary_tag_id: int # ID основного тега

    summary: str       # Короткое резюме новости
    processing_time_ms: float
    model_used: str
    tokens_used: Optional[int]
```

#### NewsFeatures (Фичи для сигналов)
```python
@dataclass(frozen=True, slots=True)
class NewsFeatures:
    # Основные метрики
    news_risk: float = 0.0          # 0..1: Агрегированный риск (EMA)
    surprise_score: float = 0.0     # -1..1: Неожиданность (EMA)
    news_grade_id: int = 0           # 0..4: Уровень важности

    # Категоризация
    tags_mask: int = 0               # Битовая маска активных тегов
    primary_tag_id: int = 0          # ID основного тега

    # Календарные события
    event_tminus_sec: int = -1       # Секунд до события (<0 = нет)
    event_grade_id: int = 0          # Важность предстоящего события

    # Метаданные
    confidence: float = 0.0          # Уверенность анализа
    horizon_sec: int = 0             # Рекомендованный horizon
    ref: str = ""                    # Ссылка на анализ
    asof_ts_ms: int = 0              # Время обновления

    # Вычисляемые свойства
    @property
    def has_high_risk_news(self) -> bool:
        return self.news_grade_id >= 3

    @property
    def has_critical_news(self) -> bool:
        return self.news_grade_id >= 4

    @property
    def has_macro_news(self) -> bool:
        return (self.tags_mask & TAG_MASK_MACRO) != 0

    @property
    def event_imminent(self) -> bool:
        return self.event_tminus_sec > 0 and self.event_tminus_sec <= 3600
```

### Система Тегов Новостей

Система использует расширенную битовую маску для категоризации новостей с поддержкой иерархической классификации:

#### Основные Теги
```python
# Экономические индикаторы (высокая важность)
TAG_CPI = 1 << 0          # Consumer Price Index
TAG_PPI = 1 << 1          # Producer Price Index
TAG_FOMC = 1 << 2         # Federal Open Market Committee
TAG_FED_SPEECH = 1 << 3   # Выступления ФРС
TAG_NFP = 1 << 4          # Non-Farm Payrolls
TAG_RATES = 1 << 5        # Процентные ставки
TAG_INFLATION = 1 << 6    # Инфляция
TAG_ECB = 1 << 7          # Европейский центробанк
TAG_BOE = 1 << 8          # Банк Англии
TAG_MACRO = 1 << 15       # Макроэкономика (общий)

# Крипто-специфичные события
TAG_CRYPTO_REGULATION = 1 << 9    # Регулирование крипты
TAG_EXCHANGE_OUTAGE = 1 << 10     # Отключения бирж
TAG_SECURITY_INCIDENT = 1 << 11   # Кибератаки/безопасность
TAG_HACK = 1 << 12                # Хаки и взломы
TAG_LIQUIDATION = 1 << 13         # Массовые ликвидации

# Рынковые события
TAG_ETF_FLOWS = 1 << 14           # Потоки ETF
TAG_EARNINGS = 1 << 16            # Корпоративные отчеты
TAG_GEOPOLITICS = 1 << 17         # Геополитика
TAG_WAR_GEO = 1 << 18             # Военные конфликты

# Рыночные режимы
TAG_RISK_OFF = 1 << 19            # Risk-off события
TAG_RISK_ON = 1 << 20             # Risk-on события
TAG_MARKET_CRASH = 1 << 21        # Обвалы рынка
TAG_MARKET_RALLY = 1 << 22        # Ралли рынка
```

#### Предопределенные Маски для Фильтрации
```python
# Групповые маски для быстрой проверки
TAG_MASK_MACRO_HIGH = (
    TAG_CPI | TAG_PPI | TAG_FOMC | TAG_FED_SPEECH |
    TAG_NFP | TAG_RATES | TAG_INFLATION | TAG_ECB |
    TAG_BOE | TAG_MACRO
)

TAG_MASK_CRYPTO_SHOCK = (
    TAG_CRYPTO_REGULATION | TAG_EXCHANGE_OUTAGE |
    TAG_SECURITY_INCIDENT | TAG_HACK | TAG_LIQUIDATION
)

TAG_MASK_EQUITIES = (
    TAG_EARNINGS | TAG_ETF_FLOWS
)

TAG_MASK_GEO = (
    TAG_GEOPOLITICS | TAG_WAR_GEO
)

TAG_MASK_RISK_REGIME = (
    TAG_RISK_OFF | TAG_RISK_ON
)

TAG_MASK_ALL = 0xFFFFFFFF  # Все теги
```

#### Primary Tag IDs (для обратной совместимости)
```python
PRIMARY_TAG_ID = {
    "cpi": 1, "ppi": 2, "fomc": 3, "fed_speech": 4, "nfp": 5,
    "rates": 6, "inflation": 7, "ecb": 8, "boe": 9, "crypto_reg": 10,
    "exchange_outage": 11, "security_incident": 12, "hack": 13,
    "etf_flows": 14, "earnings": 15, "geopolitics": 16, "macro": 17,
    "risk_off": 18, "risk_on": 19, "liquidation": 20
}
```

### Структура Redis Ключей

#### Streams (Потоки данных)
```
# Основные потоки новостей
news:raw              # Stream сырых новостей от ингестора
news:analysis         # Stream проанализированных новостей
calendar:events       # Stream календарных событий

# Детальные анализы (TTL 3 дня)
news:analysis:{UID}   # Полный JSON анализ новости

# Агрегированные фичи по символам (TTL 1 час)
news:agg:{SYMBOL}     # Агрегированные метрики (BTCUSDT, ETHUSDT, GLOBAL)
news:agg:rolling:{SYMBOL}  # Rolling aggregates

# Календарные данные
calendar:next:{CCY}   # Следующее событие по валюте (USD, EUR, GBP)
calendar:events:{ID}  # Детали события
```

#### Service Coordination
```
# Лидер-элекшен
news:ingestor:leader   # Текущий лидер ингестора
news:analyzer:leader   # Лидер анализатора (если используется)

# Хартбиты сервисов
hb:news               # Хартбит новостного ингестора
hb:calendar           # Хартбит календаря
hb:news_analyzer      # Хартбит анализатора

# Service discovery
services:news:{ID}    # Регистрация инстансов сервисов
```

#### Metrics & Monitoring
```
# Метрики обработки
metrics:news:processed:{SOURCE}    # Количество обработанных новостей
metrics:news:errors:{TYPE}         # Ошибки по типам
metrics:analysis:latency:{MODEL}   # Латентность анализа
metrics:analysis:quality:{LEVEL}   # Качество анализа

# Агрегированные метрики
metrics:news:total_processed       # Общее количество
metrics:news:active_sources        # Активные источники
metrics:analysis:avg_confidence    # Средняя уверенность
```

#### Cache & Deduplication
```
# Кеш RSS фидов
rss:cache:{URL_HASH}  # Кешированные RSS фиды (TTL 5 мин)

# Дедупликация новостей
dedup:{CONTENT_HASH}  # Ключи дедупликации (TTL 6 часов)

# Analysis cache (для ускорения)
analysis:cache:{TITLE_HASH}  # Кеш анализов (TTL 1 час)
```

#### Configuration & State
```
# Конфигурация источников
config:news:sources    # JSON конфигурация источников
config:news:filters    # Фильтры качества

# Состояние компонентов
state:news:last_processed:{SOURCE}  # Последняя обработанная новость
state:calendar:last_event          # Последнее календарное событие
```

#### Примеры использования Redis CLI
```bash
# Просмотр длины потоков
redis-cli XLEN news:raw
redis-cli XLEN news:analysis

# Просмотр агрегированных данных
redis-cli HGETALL "news:agg:BTCUSDT"

# Просмотр детального анализа
redis-cli GET "news:analysis:abc123..."

# Хартбиты
redis-cli GET "hb:news"
redis-cli GET "hb:calendar"

# Метрики
redis-cli KEYS "metrics:news:*"
redis-cli GET "metrics:news:total_processed"
```

## Конфигурация

### Environment Variables

#### Основные Настройки
```bash
# Redis подключение
REDIS_URL=redis://redis-worker-1:6379/0
REDIS_POOL_SIZE=20
REDIS_CONNECT_TIMEOUT_SEC=10

# Service identity
SERVICE_NAME=news-pipeline
INSTANCE_ID=$(hostname)
POD_NAME=${SERVICE_NAME}-${INSTANCE_ID}

# Логирование
LOG_LEVEL=INFO
LOG_FORMAT=json
LOG_FILE=/var/log/news-pipeline.log

# Debug режим
NEWS_DEBUG=false
ANALYSIS_DEBUG=false
```

#### API Ключи и Аутентификация
```bash
# Внешние API
CRYPTOPANIC_AUTH_TOKEN=your_cryptopanic_token
FMP_API_KEY=your_fmp_key
NEWSAPI_KEY=your_newsapi_key
GEMINI_API_KEY=your_gemini_key

# Прокси (опционально)
HTTP_PROXY=http://proxy.company.com:8080
HTTPS_PROXY=http://proxy.company.com:8080
NO_PROXY=localhost,127.0.0.1,.local

# SSL/TLS настройки
SSL_VERIFY=true
SSL_CERT_PATH=/etc/ssl/certs/
```

#### Тайминги и Производительность
```bash
# EMA параметры
NEWS_RISK_HALF_LIFE_SEC=1800        # 30 мин для EMA риска
NEWS_SURPRISE_HALF_LIFE_SEC=900     # 15 мин для EMA surprise
CALENDAR_RISK_HALF_LIFE_SEC=3600    # 1 час для календарных событий

# Collection intervals
COLLECTION_INTERVAL_SEC=60           # Интервал сбора новостей
ANALYSIS_BATCH_SIZE=10               # Размер пачки для анализа
FEATURE_UPDATE_INTERVAL_SEC=30       # Интервал обновления фич

# Timeouts
HTTP_TIMEOUT_SEC=10                  # HTTP таймаут
ANALYSIS_TIMEOUT_SEC=30              # Таймаут анализа
REDIS_TIMEOUT_SEC=5                  # Redis таймаут
```

#### Качество и Фильтры
```bash
# Пороги качества
MIN_TITLE_LENGTH=10
MAX_TITLE_LENGTH=500
MIN_CONFIDENCE_THRESHOLD=0.3
DUPLICATE_TIME_WINDOW_SEC=3600

# LLM параметры
GEMINI_MODEL=gemini-1.5-pro
GEMINI_TEMPERATURE=0.2
GEMINI_MAX_TOKENS=512
GEMINI_RETRIES=2

# Rate limiting
API_RPM_LIMIT=60
BURST_LIMIT=10
```

#### Масштабирование
```bash
# Concurrency
MAX_CONCURRENT_SOURCES=5
MAX_CONCURRENT_ANALYSIS=3
MAX_REDIS_CONNECTIONS=20

# Resource limits
MEMORY_LIMIT_MB=1024
CPU_LIMIT_CORES=1.0

# Health checks
HEALTH_CHECK_INTERVAL_SEC=30
HEALTH_CHECK_TIMEOUT_SEC=5
```

### News Sources JSON Configuration

#### Полная Конфигурация
```json
{
  "version": "1.0",
  "providers": ["cryptopanic", "fmp", "newsapi", "rss"],

  "cryptopanic": {
    "enabled": true,
    "auth_token": "${CRYPTOPANIC_AUTH_TOKEN}",
    "currencies": ["BTC", "ETH", "SOL", "BNB", "ADA", "DOT"],
    "filter": "important",
    "kind": "news",
    "region": "en",
    "max_age_hours": 24,
    "batch_size": 50
  },

  "fmp": {
    "enabled": true,
    "api_key": "${FMP_API_KEY}",
    "tickers": ["SPY", "QQQ", "AAPL", "MSFT", "NVDA", "TSLA"],
    "economic": {
      "countries": ["US", "EU", "UK", "JP"],
      "importance": ["High", "Medium"],
      "indicators": ["GDP", "CPI", "NFP", "FOMC", "ECB", "BOE"]
    },
    "calendar_lookahead_days": 30,
    "news_limit": 100
  },

  "newsapi": {
    "enabled": true,
    "api_key": "${NEWSAPI_KEY}",
    "query": "(bitcoin OR ethereum OR crypto OR blockchain OR \"federal reserve\" OR FOMC OR CPI OR NFP)",
    "language": "en",
    "sort_by": "publishedAt",
    "page_size": 50,
    "sources_filter": ["reuters", "bloomberg", "coindesk", "cointelegraph"]
  },

  "rss": {
    "enabled": true,
    "feeds": [
      {
        "url": "https://www.ecb.europa.eu/rss/press.html",
        "category": "central_bank",
        "priority": "high"
      },
      {
        "url": "https://coindesk.com/arc/outboundfeeds/rss/",
        "category": "crypto",
        "priority": "high"
      },
      {
        "url": "https://www.coindesk.com/arc/outboundfeeds/rss/",
        "category": "crypto",
        "priority": "high"
      },
      {
        "url": "https://cointelegraph.com/rss",
        "category": "crypto",
        "priority": "medium"
      }
    ],
    "refresh_interval_sec": 300,
    "timeout_sec": 10
  },

  "filters": {
    "quality": {
      "min_title_length": 20,
      "max_title_length": 200,
      "spam_keywords": ["free money", "guaranteed", "100% profit"],
      "required_domains": ["reuters.com", "bloomberg.com", "coindesk.com"]
    },
    "relevance": {
      "crypto_keywords": ["bitcoin", "ethereum", "crypto", "blockchain"],
      "macro_keywords": ["fed", "ecb", "cpi", "nfp", "gdp", "inflation"],
      "min_relevance_score": 0.3
    }
  },

  "processing": {
    "deduplication": {
      "enabled": true,
      "ttl_sec": 21600,
      "similarity_threshold": 0.85
    },
    "normalization": {
      "timezone": "UTC",
      "language": "en",
      "max_description_length": 1000
    }
  }
}
```

#### Минимальная Конфигурация (Только RSS)
```json
{
  "providers": ["rss"],
  "rss": {
    "enabled": true,
    "feeds": [
      {
        "url": "https://coindesk.com/arc/outboundfeeds/rss/",
        "category": "crypto"
      }
    ]
  }
}
```

### News Sources JSON

```json
{
  "providers": ["cryptopanic", "fmp", "newsapi", "rss"],
  "cryptopanic": {
    "enabled": true,
    "currencies": ["BTC", "ETH", "SOL"],
    "filter": "important",
    "kind": "news"
  },
  "fmp": {
    "enabled": true,
    "tickers": ["SPY", "QQQ", "AAPL"],
    "economic": {
      "countries": ["US", "EU"],
      "importance": ["High", "Medium"]
    }
  },
  "rss": {
    "enabled": true,
    "urls": [
      "https://www.ecb.europa.eu/rss/press.html",
      "https://coindesk.com/arc/outboundfeeds/rss/"
    ]
  }
}
```

## Развертывание

### Docker Compose Profiles

#### Полное Развертывание
```bash
# Запуск всего новостного пайплайна
docker-compose --profile news-pipeline up -d

# С мониторингом
docker-compose --profile news-pipeline --profile monitoring up -d

# С отладкой
docker-compose --profile news-pipeline --profile debug up -d
```

#### Компонентное Развертывание
```bash
# Только ingestion (сбор новостей)
docker-compose up -d news-ingestor-go

# Только анализ
docker-compose up -d news-analyzer redis-worker-1

# Только feature store
docker-compose up -d news-feature-store redis-worker-1

# Минимальная конфигурация для тестирования
docker-compose --profile news-minimal up -d
```

### Docker Compose Services

#### Основные Сервисы
```yaml
services:
  news-ingestor-go:
    image: news-ingestor:latest
    environment:
      - REDIS_URL=redis://redis-worker-1:6379/0
      - CRYPTOPANIC_AUTH_TOKEN=${CRYPTOPANIC_AUTH_TOKEN}
      - FMP_API_KEY=${FMP_API_KEY}
      - SERVICE_NAME=news-ingestor
      - INSTANCE_ID=go-1
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8097/health"]
      interval: 30s
      timeout: 10s
      retries: 3
    deploy:
      resources:
        limits:
          cpus: '0.5'
          memory: 256M
        reservations:
          cpus: '0.25'
          memory: 128M

  news-analyzer:
    image: news-analyzer:latest
    environment:
      - REDIS_URL=redis://redis-worker-1:6379/0
      - GEMINI_API_KEY=${GEMINI_API_KEY}
      - GEMINI_MODEL=gemini-1.5-pro
      - BATCH_SIZE=10
      - MAX_CONCURRENT_BATCHES=3
    healthcheck:
      test: ["CMD", "python", "-c", "import requests; requests.get('http://localhost:8098/health')"]
      interval: 60s
      timeout: 15s
      retries: 3
    deploy:
      resources:
        limits:
          cpus: '1.0'
          memory: 1G
        reservations:
          cpus: '0.5'
          memory: 512M

  news-feature-store:
    image: news-feature-store:latest
    environment:
      - REDIS_URL=redis://redis-worker-1:6379/0
      - UPDATE_INTERVAL_SEC=30
      - CACHE_TTL_MS=1500
    deploy:
      resources:
        limits:
          cpus: '0.5'
          memory: 512M
        reservations:
          cpus: '0.25'
          memory: 256M
```

### Ручной Запуск и Разработка

#### Go Ingestor
```bash
# Из директории go-news-services
cd go-news-services

# Сборка
go build -o bin/news-ingestor cmd/news-ingestor/main.go

# Запуск
./bin/news-ingestor

# С отладкой
LOG_LEVEL=DEBUG NEWS_DEBUG=true ./bin/news-ingestor
```

#### Python Components
```bash
# Из директории python-worker
cd python-worker

# Анализатор новостей
python -m news_pipeline.analyzer_worker

# Feature store
python -m news_pipeline.feature_store_worker

# С отладкой
NEWS_DEBUG=true python -m news_pipeline.analyzer_worker
```

#### Development Mode
```bash
# Запуск с hot reload (если используется)
export FLASK_ENV=development
export NEWS_DEBUG=true
export ANALYSIS_DEBUG=true

# Python компоненты
python -m news_pipeline.analyzer_worker --reload
python -m news_pipeline.feature_store_worker --reload
```

### Production Deployment

#### Kubernetes Manifests
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: news-ingestor
  labels:
    app: news-ingestor
spec:
  replicas: 2
  selector:
    matchLabels:
      app: news-ingestor
  template:
    metadata:
      labels:
        app: news-ingestor
    spec:
      containers:
      - name: ingestor
        image: news-ingestor:v1.2.3
        env:
        - name: REDIS_URL
          value: "redis://redis-cluster:6379/0"
        - name: CRYPTOPANIC_AUTH_TOKEN
          valueFrom:
            secretKeyRef:
              name: news-api-keys
              key: cryptopanic-token
        resources:
          requests:
            cpu: 250m
            memory: 256Mi
          limits:
            cpu: 500m
            memory: 512Mi
        livenessProbe:
          httpGet:
            path: /health
            port: 8097
          initialDelaySeconds: 30
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /health
            port: 8097
          initialDelaySeconds: 5
          periodSeconds: 10
```

#### Environment Variables для Production
```bash
# Production настройки
ENVIRONMENT=production
LOG_LEVEL=INFO
LOG_FORMAT=json

# Redis кластер
REDIS_URL=redis://redis-cluster:6379/0
REDIS_SENTINEL_MASTER=news-redis
REDIS_SENTINEL_HOSTS=redis-sentinel-1:26379,redis-sentinel-2:26379

# Secrets
CRYPTOPANIC_AUTH_TOKEN=/secrets/cryptopanic-token
GEMINI_API_KEY=/secrets/gemini-key

# Monitoring
PROMETHEUS_PUSH_GATEWAY=http://prometheus-pushgateway:9091
GRAFANA_ENDPOINT=http://grafana:3000

# Rate limiting
API_RPM_LIMIT=100
BURST_LIMIT=20
```

### Масштабирование

#### Горизонтальное Масштабирование
```bash
# Запуск нескольких инстансов анализаторов
docker-compose up --scale news-analyzer=5

# Kubernetes HPA (Horizontal Pod Autoscaler)
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: news-analyzer-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: news-analyzer
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
```

#### Вертикальное Масштабирование
```yaml
# Увеличение ресурсов для LLM processing
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

## Мониторинг и Observability

### Health Checks

#### HTTP Endpoints
```bash
# Go News Ingestor
curl -s http://localhost:8097/health | jq .

# Python News Analyzer
curl -s http://localhost:8098/health | jq .

# News Feature Store
curl -s http://localhost:8099/health | jq .
```

#### Health Response Format
```json
{
  "status": "healthy",
  "timestamp": 1703123456789,
  "version": "1.2.3",
  "checks": {
    "redis": "ok",
    "api_keys": "ok",
    "memory_usage": "ok",
    "error_rate": "ok"
  },
  "metrics": {
    "uptime_sec": 3600,
    "processed_news": 1500,
    "active_sources": 4,
    "queue_length": 25
  }
}
```

### Redis Мониторинг

#### Потоки и Очереди
```bash
# Длины основных потоков
redis-cli XLEN news:raw
redis-cli XLEN news:analysis
redis-cli XLEN calendar:events

# Pending сообщения в consumer groups
redis-cli XPENDING news:raw news-analyzer
redis-cli XPENDING news:analysis news-feature-store

# Информация о группах потребителей
redis-cli XINFO GROUPS news:raw
redis-cli XINFO GROUPS news:analysis
```

#### Агрегированные Данные
```bash
# Новости по символам
redis-cli HGETALL "news:agg:BTCUSDT"
redis-cli HGETALL "news:agg:ETHUSDT"
redis-cli HGETALL "news:agg:GLOBAL"

# Календарные события
redis-cli HGETALL "calendar:next:USD"
redis-cli HGETALL "calendar:next:EUR"

# Детальный анализ
redis-cli GET "news:analysis:$(redis-cli HGET news:agg:BTCUSDT last_ref)"
```

#### Хартбиты и Статус
```bash
# Хартбиты сервисов
redis-cli GET "hb:news"
redis-cli GET "hb:calendar"
redis-cli GET "hb:news_analyzer"

# Регистрация сервисов
redis-cli KEYS "services:news:*"
redis-cli HGETALL "services:news:ingestor-1"
```

#### Метрики Обработки
```bash
# Общие метрики
redis-cli GET "metrics:news:total_processed"
redis-cli GET "metrics:analysis:total_analyzed"

# Метрики по источникам
redis-cli KEYS "metrics:news:processed:*"
redis-cli GET "metrics:news:processed:cryptopanic"

# Ошибки
redis-cli KEYS "metrics:news:errors:*"
redis-cli GET "metrics:news:errors:rate_limit"
```

### Prometheus Metrics

#### Business Metrics
```python
# Метрики в Prometheus формате
NEWS_INGESTION_RATE = Counter('news_ingestion_total', 'News ingested', ['source'])
ANALYSIS_COMPLETION_RATE = Counter('news_analysis_total', 'News analyzed', ['model'])
FEATURE_UPDATE_LATENCY = Histogram('news_feature_update_duration', 'Feature update time')
SIGNAL_ENRICHMENT_TIME = Histogram('news_signal_enrichment_duration', 'Signal enrichment time')
```

#### System Metrics
```python
# Ресурсы и производительность
MEMORY_USAGE = Gauge('news_service_memory_bytes', 'Memory usage', ['service'])
CPU_USAGE = Gauge('news_service_cpu_percent', 'CPU usage', ['service'])
REDIS_CONNECTIONS = Gauge('redis_connections_active', 'Active Redis connections')
QUEUE_LENGTH = Gauge('news_queue_length', 'Queue length', ['queue'])
```

### Grafana Dashboards

#### News Pipeline Overview
```
📊 Dashboard: News Pipeline Overview
├── News Ingestion Rate (by source) - График скорости ingestion
├── Analysis Quality Metrics - Уверенность, latency
├── Queue Health - Длины очередей, pending messages
├── Error Rates - Ошибки по типам и источникам
├── LLM API Usage - Квота, стоимость, rate limits
└── Feature Freshness - Актуальность данных по символам
```

#### Trading Impact Dashboard
```
📊 Dashboard: News Trading Impact
├── Signal Filtering Rate - Процент отфильтрованных сигналов
├── Weight Adjustments - Изменения веса сигналов
├── Position Size Changes - Корректировка размеров позиций
├── Trade PnL by News Context - Прибыль по новостному контексту
├── Win Rate vs News Risk - Соотношение побед по уровню риска
└── Sharpe Ratio by News Grade - Шарп по уровням важности
```

#### System Performance Dashboard
```
📊 Dashboard: News System Performance
├── Service Health Status - Статус всех компонентов
├── Memory/CPU Usage - Ресурсы по сервисам
├── Redis Performance - Latency, connections, memory
├── Network I/O - Входящий/исходящий трафик
├── External API Response Times - Время ответа внешних API
└── Error Rates and Alerts - Ошибки и алерты
```

### Alerting

#### Critical Alerts
```yaml
# Prometheus Alert Rules
groups:
  - name: news_pipeline_critical
    rules:
      - alert: NewsIngestionDown
        expr: up{job="news-ingestor"} == 0
        for: 5m
        labels:
          severity: critical

      - alert: NewsQueueOverflow
        expr: redis_stream_length{stream="news:raw"} > 50000
        for: 5m
        labels:
          severity: critical

      - alert: LLM_API_Failing
        expr: rate(news_analysis_errors_total{error_type="llm_api"}[5m]) > 0.8
        for: 10m
        labels:
          severity: critical
```

#### Warning Alerts
```yaml
  - name: news_pipeline_warnings
    rules:
      - alert: HighAnalysisLatency
        expr: histogram_quantile(0.95, rate(news_analysis_latency_seconds_bucket[5m])) > 30
        for: 10m
        labels:
          severity: warning

      - alert: StaleNewsFeatures
        expr: news_feature_age_seconds > 1800
        for: 15m
        labels:
          severity: warning
```

### Logging

#### Structured Logging Format
```json
{
  "timestamp": "2024-01-15T10:30:45.123Z",
  "level": "INFO",
  "component": "news_analyzer",
  "operation": "llm_analysis",
  "news_uid": "abc123...",
  "symbol": "BTCUSDT",
  "risk_score": 0.85,
  "confidence": 0.92,
  "processing_time_ms": 2150,
  "model": "gemini-1.5-pro",
  "tokens_used": 145
}
```

#### Log Aggregation Queries
```bash
# Ошибки анализа
grep '"level":"ERROR"' /var/log/news-pipeline.log | jq -r '.message'

# Высокорискованные новости
grep '"risk_score":[0-9]\.[8-9]' /var/log/news-pipeline.log

# Производительность по моделям
grep '"component":"news_analyzer"' /var/log/news-pipeline.log | \
  jq -r '.processing_time_ms' | \
  awk '{sum+=$1; count++} END {print "Avg latency:", sum/count, "ms"}'
```

### Debug Tools

#### News Pipeline Diagnostics
```bash
#!/bin/bash
# news_diag.sh - Полная диагностика пайплайна

echo "=== News Pipeline Diagnostics ==="
echo

echo "1. Service Health:"
curl -s http://localhost:8097/health | jq -r '.status' || echo "Go ingestor: DOWN"
curl -s http://localhost:8098/health | jq -r '.status' || echo "Analyzer: DOWN"
curl -s http://localhost:8099/health | jq -r '.status' || echo "Feature Store: DOWN"

echo
echo "2. Queue Status:"
echo "Raw news: $(redis-cli XLEN news:raw)"
echo "Analysis: $(redis-cli XLEN news:analysis)"
echo "Pending: $(redis-cli XPENDING news:raw news-analyzer | wc -l)"

echo
echo "3. Recent Activity:"
echo "Last heartbeat: $(redis-cli GET hb:news)"
echo "Total processed: $(redis-cli GET metrics:news:total_processed)"

echo
echo "4. Feature Freshness:"
for symbol in BTCUSDT ETHUSDT GLOBAL; do
    ts=$(redis-cli HGET news:agg:$symbol last_ts_ms 2>/dev/null || echo 0)
    age=$(( ($(date +%s) * 1000 - ts) / 1000 / 60 ))
    echo "$symbol: ${age}min ago"
done

echo
echo "5. Error Rates (last hour):"
redis-cli KEYS "metrics:news:errors:*" | while read key; do
    count=$(redis-cli GET "$key" 2>/dev/null || echo 0)
    echo "$key: $count"
done
```

## Документация

- [Архитектура и Дизайн](architecture.md) - Детальное описание компонентов
- [Источники Новостей](sources.md) - Настройка и конфигурация источников
- [LLM Анализ](analysis.md) - Система анализа новостей
- [Хранилище и Агрегация](storage.md) - Работа с данными в Redis
- [Интеграция в Сигналы](integration.md) - Использование в торговле
- [Мониторинг и Обслуживание](monitoring.md) - Операционные аспекты
- [Примеры и Конфигурации](examples.md) - Практические примеры

## Производительность и Масштабирование

### Бенчмарки Производительности

#### Throughput Metrics
| Компонент | P50 Latency | P95 Latency | Throughput | Notes |
|-----------|-------------|-------------|------------|-------|
| News Ingestion | 50ms | 200ms | 1000 news/min | RSS + API sources |
| LLM Analysis | 2.1s | 4.8s | 25 news/min | Gemini 1.5 Pro |
| Feature Aggregation | 15ms | 50ms | 2000 updates/min | EMA calculations |
| Signal Enrichment | 5ms | 20ms | 10000 signals/min | In-memory cache |
| Redis Operations | 2ms | 10ms | 5000 ops/sec | Single instance |

#### Memory Usage
| Компонент | Base Memory | Peak Memory | Notes |
|-----------|-------------|-------------|-------|
| Go Ingestor | 128MB | 256MB | +50MB per source |
| News Analyzer | 512MB | 1GB | +200MB per concurrent analysis |
| Feature Store | 256MB | 512MB | +100MB per 1000 symbols |
| Redis | 256MB | 2GB | Depends on retention policy |

#### CPU Usage
| Компонент | Base CPU | Peak CPU | Scaling Factor |
|-----------|----------|----------|----------------|
| Go Ingestor | 0.1 cores | 0.5 cores | Linear with sources |
| News Analyzer | 0.2 cores | 1.0 cores | Linear with batch size |
| Feature Store | 0.05 cores | 0.3 cores | Linear with updates |

### Оптимизации Производительности

#### 1. Batch Processing
```python
# Оптимальный размер пачки для разных операций
INGESTION_BATCH_SIZE = 100    # Для сбора новостей
ANALYSIS_BATCH_SIZE = 10       # Для LLM анализа (баланс latency/throughput)
FEATURE_UPDATE_BATCH_SIZE = 50 # Для обновления фич

# Асинхронная обработка пачек
async def process_batch_optimized(items: List[Any]) -> List[Any]:
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_BATCHES)

    async def process_single(item):
        async with semaphore:
            return await process_item(item)

    tasks = [process_single(item) for item in items]
    return await asyncio.gather(*tasks)
```

#### 2. Connection Pooling
```python
# Redis connection pool
REDIS_POOL_CONFIG = {
    'max_connections': 20,
    'decode_responses': True,
    'socket_timeout': 5,
    'socket_connect_timeout': 5,
    'socket_keepalive': True,
    'socket_keepalive_options': {
        socket.TCP_KEEPIDLE: 60,
        socket.TCP_KEEPINTVL: 30,
        socket.TCP_KEEPCNT: 3
    }
}

# HTTP client pooling
HTTP_POOL_CONFIG = {
    'limit': 100,              # Max concurrent requests
    'limit_per_host': 10,      # Per host limit
    'ttl_dns_cache': 300,      # DNS cache TTL
}
```

#### 3. Caching Strategy
```python
# Multi-level caching
class MultiLevelCache:
    def __init__(self):
        self.l1_cache = {}  # In-memory, short TTL
        self.l2_cache = redis.Redis()  # Redis, medium TTL
        self.l3_cache = {}  # Fallback, long TTL

    async def get(self, key: str) -> Optional[Any]:
        # L1: Fast in-memory
        if key in self.l1_cache:
            return self.l1_cache[key]['data']

        # L2: Redis
        data = await self.l2_cache.get(f"cache:{key}")
        if data:
            self.l1_cache[key] = {'data': data, 'timestamp': time.time()}
            return data

        # L3: Compute/External
        data = await self._compute_data(key)
        await self._store_all_levels(key, data)
        return data
```

#### 4. Memory Optimization
```python
# Efficient data structures
@dataclass(frozen=True, slots=True)
class CompactNewsItem:
    uid: str
    ts_ms: int
    risk: int      # float * 1000 как int
    surprise: int  # float * 1000 как int
    tags: int      # bitmask
    primary_tag: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            'uid': self.uid,
            'ts_ms': self.ts_ms,
            'risk': self.risk / 1000,
            'surprise': self.surprise / 1000,
            'tags_mask': self.tags,
            'primary_tag_id': self.primary_tag
        }
```

### Масштабирование

#### Горизонтальное Масштабирование

##### Service Replication
```yaml
# Kubernetes deployment с HPA
apiVersion: apps/v1
kind: Deployment
metadata:
  name: news-analyzer
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: analyzer
        resources:
          requests:
            cpu: 500m
            memory: 1Gi
          limits:
            cpu: 1000m
            memory: 2Gi
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: news-analyzer-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: news-analyzer
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: External
    external:
      metric:
        name: news_queue_length
      target:
        type: AverageValue
        averageValue: "100"
```

##### Data Partitioning
```python
# Partitioning по символам
SYMBOL_PARTITIONS = {
    'crypto': ['BTC', 'ETH', 'BNB', 'ADA', 'SOL'],
    'equity': ['AAPL', 'GOOGL', 'MSFT', 'TSLA'],
    'forex': ['EURUSD', 'GBPUSD', 'USDJPY']
}

def get_partition(symbol: str) -> str:
    for partition, symbols in SYMBOL_PARTITIONS.items():
        if any(symbol.startswith(s) for s in symbols):
            return partition
    return 'global'

# Redis кластер с partitioning
async def get_partitioned_redis(symbol: str) -> redis.Redis:
    partition = get_partition(symbol)
    return REDIS_CLUSTERS[partition]
```

#### Вертикальное Масштабирование

##### Resource Allocation
```yaml
# Разные профили для разных нагрузок
services:
  news-analyzer-light:
    # Для низкой нагрузки
    deploy:
      resources:
        limits:
          cpus: '0.5'
          memory: 512M

  news-analyzer-standard:
    # Стандартная нагрузка
    deploy:
      resources:
        limits:
          cpus: '1.0'
          memory: 1G

  news-analyzer-heavy:
    # Высокая нагрузка
    deploy:
      resources:
        limits:
          cpus: '2.0'
          memory: 2G
        reservations:
          cpus: '1.0'
          memory: 1G
```

##### Dynamic Scaling
```python
class DynamicScaler:
    def __init__(self, k8s_client):
        self.k8s = k8s_client
        self.metrics_window = 300  # 5 minutes

    async def scale_based_on_load(self):
        # Получить текущие метрики
        queue_length = await self.get_queue_length()
        cpu_usage = await self.get_avg_cpu_usage()
        analysis_latency = await self.get_analysis_latency()

        # Логика масштабирования
        if queue_length > 1000 or cpu_usage > 80 or analysis_latency > 5000:
            await self.scale_up()
        elif queue_length < 100 and cpu_usage < 30 and analysis_latency < 2000:
            await self.scale_down()

    async def scale_up(self):
        current_replicas = await self.get_current_replicas()
        new_replicas = min(current_replicas * 1.5, MAX_REPLICAS)
        await self.set_replicas(int(new_replicas))

    async def scale_down(self):
        current_replicas = await self.get_current_replicas()
        new_replicas = max(current_replicas * 0.8, MIN_REPLICAS)
        await self.set_replicas(int(new_replicas))
```

### High Availability

#### Multi-Region Deployment
```yaml
# Развертывание в нескольких регионах
regions:
  - name: us-east
    primary: true
    services:
      - news-ingestor-us
      - news-analyzer-us
  - name: eu-west
    primary: false
    services:
      - news-ingestor-eu
      - news-analyzer-eu

# Cross-region replication
replication:
  enabled: true
  source_region: us-east
  target_regions: [eu-west]
  replication_lag_max: 30s
```

#### Circuit Breaker Pattern
```python
class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.last_failure_time = 0
        self.state = 'closed'

    def call(self, func, *args, **kwargs):
        if self.state == 'open':
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = 'half-open'
            else:
                raise CircuitBreakerOpen("Circuit breaker is open")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e

    def _on_success(self):
        self.failure_count = 0
        self.state = 'closed'

    def _on_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = 'open'
```

### Performance Monitoring

#### Real-time Metrics
```python
class PerformanceMonitor:
    def __init__(self):
        self.metrics = defaultdict(list)
        self.alerts = []

    def record_metric(self, name: str, value: float, tags: Dict[str, str] = None):
        """Запись метрики с тегами"""
        self.metrics[name].append({
            'value': value,
            'timestamp': time.time(),
            'tags': tags or {}
        })

        # Ограничение размера истории
        if len(self.metrics[name]) > 1000:
            self.metrics[name] = self.metrics[name][-500:]

    def get_percentile(self, name: str, percentile: float) -> float:
        """Расчет перцентиля"""
        values = [m['value'] for m in self.metrics[name][-100:]]
        if not values:
            return 0.0

        values.sort()
        index = int(len(values) * percentile / 100)
        return values[min(index, len(values) - 1)]

    def detect_anomalies(self, name: str) -> List[str]:
        """Обнаружение аномалий"""
        values = [m['value'] for m in self.metrics[name][-50:]]
        if len(values) < 10:
            return []

        mean = statistics.mean(values)
        std = statistics.stdev(values)

        anomalies = []
        for i, value in enumerate(values[-5:]):  # Последние 5 значений
            z_score = abs(value - mean) / (std or 1)
            if z_score > 3:  # 3 sigma
                anomalies.append(f"High anomaly in {name}: {value:.2f} (z={z_score:.1f})")

        return anomalies
```

### Cost Optimization

#### API Usage Optimization
```python
class APIUsageOptimizer:
    def __init__(self):
        self.usage_by_hour = defaultdict(float)
        self.cost_by_provider = defaultdict(float)

    def optimize_request(self, provider: str, request_type: str) -> bool:
        """Оптимизировать запрос на основе стоимости и лимитов"""
        current_hour = int(time.time() / 3600)

        # Проверить hourly limits
        if self.usage_by_hour[current_hour] > HOURLY_LIMITS[provider]:
            return False

        # Проверить cost budget
        estimated_cost = self.estimate_cost(provider, request_type)
        if self.cost_by_provider[provider] + estimated_cost > DAILY_BUDGETS[provider]:
            return False

        return True

    def estimate_cost(self, provider: str, request_type: str) -> float:
        """Оценка стоимости запроса"""
        costs = {
            'gemini': {
                'analysis': 0.001,  # $0.001 per request
                'batch_analysis': 0.002
            },
            'cryptopanic': {
                'news': 0.0005
            }
        }
        return costs.get(provider, {}).get(request_type, 0.0)
```

### Производственные Рекомендации

#### Resource Allocation Guidelines
- **Development**: 1-2 CPU cores, 2-4GB RAM
- **Staging**: 2-4 CPU cores, 4-8GB RAM
- **Production**: 4-8 CPU cores, 8-16GB RAM per service
- **High Traffic**: 8-16 CPU cores, 16-32GB RAM, multiple instances

#### Monitoring Thresholds
- **Latency**: P95 < 5s для анализа, < 100ms для ingestion
- **Error Rate**: < 5% для всех компонентов
- **Queue Length**: < 1000 для raw queue, < 5000 для analysis queue
- **Memory Usage**: < 80% от лимита
- **CPU Usage**: < 70% среднее, < 90% peak

#### Backup and Recovery
- **Data Retention**: 30 дней для детальных анализов, 7 дней для агрегаций
- **Backup Frequency**: Ежечасно для critical data, ежедневно для full backup
- **Recovery Time**: < 15 минут для critical services, < 1 час для full recovery
- **Data Consistency**: Регулярные проверки integrity всех хранилищ

## Безопасность

- API ключи хранятся в environment variables
- Docker secrets для production deployment
- Ограничение доступа к Redis внутренними сетями
- Rate limiting на внешние API

## Troubleshooting и Обслуживание

### Распространенные Проблемы и Решения

#### 1. Отсутствие Новостей (No News Ingestion)

**Симптомы:**
- Поток `news:raw` пустой или не обновляется
- Нет хартбита `hb:news`
- Сервис news-ingestor не отвечает

**Диагностика:**
```bash
# Проверить статус сервиса
curl http://localhost:8097/health

# Проверить логи
docker logs news-ingestor-go --tail 50

# Проверить API ключи
echo $CRYPTOPANIC_AUTH_TOKEN | head -c 10  # Показать первые 10 символов

# Проверить RSS фиды
curl -I https://coindesk.com/arc/outboundfeeds/rss/
```

**Решения:**
```bash
# Перезапустить сервис
docker restart news-ingestor-go

# Проверить конфигурацию источников
redis-cli GET config:news:sources

# Временная конфигурация только RSS
export NEWS_SOURCES_JSON='{"providers":["rss"],"rss":{"enabled":true,"feeds":[{"url":"https://coindesk.com/arc/outboundfeeds/rss/","category":"crypto"}]}}'
```

#### 2. LLM Анализ Падает (Analysis Failures)

**Симптомы:**
- Поток `news:analysis` не заполняется
- Высокий error rate в метриках
- Таймауты в логах

**Диагностика:**
```bash
# Проверить API ключ Gemini
curl -H "x-goog-api-key: $GEMINI_API_KEY" \
     "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro?key=$GEMINI_API_KEY"

# Проверить квоту
redis-cli GET "metrics:llm:quota_used"

# Проверить логи анализатора
docker logs news-analyzer --tail 100 | grep ERROR
```

**Решения:**
```bash
# Перезапустить анализатор
docker restart news-analyzer

# Переключиться на другой модель
export GEMINI_MODEL=gemini-pro

# Временно отключить анализ (fallback mode)
export LLM_ANALYSIS_ENABLED=false
```

#### 3. Redis Ошибки (Redis Connection Issues)

**Симптомы:**
- Connection timeouts в логах
- Потери данных
- Медленная работа

**Диагностика:**
```bash
# Проверить подключение
redis-cli ping

# Проверить использование памяти
redis-cli info memory

# Проверить количество соединений
redis-cli info clients

# Проверить persistence
redis-cli info persistence
```

**Решения:**
```bash
# Перезапустить Redis
docker restart redis-worker-1

# Очистить старые данные
redis-cli KEYS "news:analysis:*" | head -1000 | xargs redis-cli DEL

# Увеличить память Redis
redis-cli config set maxmemory 2gb
redis-cli config set maxmemory-policy allkeys-lru
```

#### 4. Проблемы Лидер-Элекшена (Leader Election Issues)

**Симптомы:**
- Несколько инстансов считают себя лидерами
- Отсутствие обновлений данных
- Конфликты в логах

**Диагностика:**
```bash
# Проверить текущего лидера
redis-cli GET "news:ingestor:leader"

# Проверить TTL ключа лидера
redis-cli TTL "news:ingestor:leader"

# Проверить сетевую связность
docker exec news-ingestor-1 ping redis-worker-1
```

**Решения:**
```bash
# Принудительно сбросить лидера
redis-cli DEL "news:ingestor:leader"

# Перезапустить все инстансы
docker-compose restart news-ingestor-go

# Проверить instance IDs
docker logs news-ingestor-1 | grep "instance_id"
```

#### 5. Высокая Латентность Анализа (High Analysis Latency)

**Симптомы:**
- processing_time_ms > 10 секунд
- Очереди растут
- Таймауты клиентов

**Диагностика:**
```bash
# Проверить среднюю латентность
redis-cli --eval latency.lua

# Проверить размер пачек
redis-cli GET "metrics:analysis:batch_size"

# Проверить concurrency
docker stats news-analyzer
```

**Решения:**
```bash
# Уменьшить размер пачки
export ANALYSIS_BATCH_SIZE=5

# Увеличить таймаут
export ANALYSIS_TIMEOUT_SEC=60

# Масштабировать анализаторы
docker-compose up --scale news-analyzer=3
```

#### 6. Низкое Качество Анализа (Low Analysis Quality)

**Симптомы:**
- confidence < 0.5
- Неправильные теги
- Неточные summary

**Диагностика:**
```bash
# Проверить метрики качества
redis-cli KEYS "metrics:analysis:quality:*"

# Посмотреть примеры плохого анализа
redis-cli KEYS "news:analysis:*" | head -5 | xargs -I {} redis-cli GET {}
```

**Решения:**
```bash
# Обновить промпт
export ANALYSIS_PROMPT_VERSION=v2

# Изменить модель
export GEMINI_MODEL=gemini-1.5-pro

# Увеличить temperature для креативности
export GEMINI_TEMPERATURE=0.3
```

### Debug Режимы

#### Полный Debug
```bash
export LOG_LEVEL=DEBUG
export NEWS_DEBUG=true
export ANALYSIS_DEBUG=true
export REDIS_DEBUG=true

# С трассировкой
export TRACE_ANALYSIS=true
export TRACE_INGESTION=true
```

#### Компонентный Debug
```bash
# Только ingestion
export INGESTION_DEBUG=true

# Только analysis
export ANALYSIS_DEBUG=true

# Только feature store
export FEATURE_STORE_DEBUG=true
```

### Maintenance Scripts

#### Ежедневное Обслуживание
```bash
#!/bin/bash
# daily_maintenance.sh

echo "=== Daily News Pipeline Maintenance ==="

# 1. Health checks
echo "Health checks..."
curl -f http://localhost:8097/health > /dev/null && echo "✓ Ingestor OK" || echo "✗ Ingestor FAIL"
curl -f http://localhost:8098/health > /dev/null && echo "✓ Analyzer OK" || echo "✗ Analyzer FAIL"

# 2. Queue monitoring
echo "Queue lengths..."
raw_len=$(redis-cli XLEN news:raw)
analysis_len=$(redis-cli XLEN news:analysis)
echo "Raw queue: $raw_len, Analysis queue: $analysis_len"

# 3. Clean old data
echo "Cleaning old analysis data..."
redis-cli KEYS "news:analysis:*" | grep -E ":2[0-9]{13}" | head -100 | xargs redis-cli DEL

# 4. Log rotation
echo "Rotating logs..."
logrotate /etc/logrotate.d/news-pipeline

echo "Maintenance completed"
```

#### Еженедельное Обслуживание
```bash
#!/bin/bash
# weekly_maintenance.sh

echo "=== Weekly News Pipeline Maintenance ==="

# 1. Full health assessment
echo "Full system health check..."
./health_check.sh

# 2. Performance analysis
echo "Performance analysis..."
redis-cli --eval performance_analysis.lua

# 3. Data consistency check
echo "Data consistency check..."
redis-cli --eval consistency_check.lua

# 4. Update models/configs
echo "Checking for updates..."
./check_updates.sh

# 5. Backup critical data
echo "Creating backup..."
./create_backup.sh

echo "Weekly maintenance completed"
```

### Очистка Данных

#### Безопасная Очистка
```bash
# Очистка только новостных данных (без системных)
redis-cli DEL news:raw news:analysis calendar:events
redis-cli KEYS "news:agg:*" | xargs redis-cli DEL
redis-cli KEYS "calendar:next:*" | xargs redis-cli DEL
redis-cli KEYS "news:analysis:*" | xargs redis-cli DEL

# Очистка метрик (осторожно!)
redis-cli KEYS "metrics:news:*" | xargs redis-cli DEL
redis-cli KEYS "metrics:analysis:*" | xargs redis-cli DEL

# Полная очистка (только для development)
redis-cli FLUSHDB  # или FLUSHALL для всех БД
```

#### Selective Cleanup
```bash
# Удалить новости старше 24 часов
redis-cli XRANGE news:raw - + | jq -r '.[] | select(.timestamp < '$(( $(date +%s) - 86400 ))') | .id' | xargs redis-cli XDEL news:raw

# Очистить старые детальные анализы
redis-cli KEYS "news:analysis:*" | while read key; do
    age=$(( $(date +%s) - $(redis-cli TTL $key 2>/dev/null || echo 0) ))
    if [ $age -gt 604800 ]; then  # 7 дней
        redis-cli DEL $key
    fi
done
```

### Performance Optimization

#### Redis Optimization
```bash
# Оптимизация Redis для новостного трафика
redis-cli config set maxmemory 4gb
redis-cli config set maxmemory-policy allkeys-lru
redis-cli config set tcp-keepalive 300
redis-cli config set timeout 300

# Создание индексов для быстрого поиска
redis-cli SET news:index:by_symbol:BTCUSDT "news:agg:BTCUSDT"
redis-cli SADD news:symbols "BTCUSDT" "ETHUSDT" "GLOBAL"
```

#### Application Optimization
```bash
# Оптимизация Go ингестора
export GOGC=100  # Более частная сборка мусора
export GOMAXPROCS=2  # Количество ядер

# Оптимизация Python
export PYTHONOPTIMIZE=1
export PYTHONDONTWRITEBYTECODE=1

# Batch processing
export ANALYSIS_BATCH_SIZE=20
export INGESTION_BATCH_SIZE=100
```

### Emergency Procedures

#### Полная Остановка Системы
```bash
# Graceful shutdown
docker-compose stop news-analyzer
docker-compose stop news-feature-store
docker-compose stop news-ingestor-go

# Force stop if needed
docker-compose down --timeout 30
```

#### Disaster Recovery
```bash
# 1. Остановить все сервисы
docker-compose down

# 2. Восстановить из бэкапа
./restore_from_backup.sh latest

# 3. Проверить консистентность
./consistency_check.sh

# 4. Запустить сервисы по порядку
docker-compose up -d redis-worker-1
docker-compose up -d news-ingestor-go
docker-compose up -d news-analyzer
docker-compose up -d news-feature-store
```

#### Incident Response
```bash
# 1. Оценить ситуацию
./incident_assessment.sh

# 2. Изолировать проблему
./isolate_problem.sh

# 3. Применить workaround
./apply_workaround.sh

# 4. Уведомить команду
./notify_team.sh

# 5. Документировать инцидент
./document_incident.sh
```
