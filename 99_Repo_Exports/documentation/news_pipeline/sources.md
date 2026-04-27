# Источники Новостей

## Обзор

Новостной пайплайн поддерживает несколько типов источников новостей, каждый из которых имеет свои особенности и конфигурацию. Все источники реализованы с принципом "fail-open" - если источник недоступен или неправильно настроен, система продолжает работать с остальными источниками.

## Типы Источников

### 1. RSS Feeds

#### Описание
RSS (Really Simple Syndication) - наиболее надежный и бесплатный источник новостей. Поддерживает структурированные фиды от новостных агентств, банков и регуляторов.

#### Конфигурация
```json
{
  "rss": {
    "enabled": true,
    "urls": [
      "https://www.ecb.europa.eu/rss/press.html",
      "https://www.coindesk.com/arc/outboundfeeds/rss/",
      "https://cointelegraph.com/rss",
      "https://decrypt.co/feed",
      "https://bitcoinmagazine.com/.rss/full/",
      "https://thedefiant.io/rss.xml"
    ]
  }
}
```

#### Преимущества
- **Бесплатный доступ** - не требует API ключей
- **Надежность** - структурированные данные
- **Регулярность** - предсказуемые обновления
- **Контроль** - можно выбирать авторитетные источники

#### Недостатки
- **Задержка** - новости появляются после публикации на сайте
- **Ограниченный охват** - только источники с RSS
- **Качество** - зависит от качества RSS фида

#### Примеры URL
```json
[
  "https://feeds.reuters.com/reuters/topNews",
  "https://feeds.bbci.co.uk/news/rss.xml",
  "https://rss.cnn.com/rss/edition.rss",
  "https://feeds.npr.org/1001/rss.xml",
  "https://www.economist.com/finance-and-economics/rss.xml",
  "https://www.ft.com/rss/home/uk"
]
```

#### Реализация
```go
type RSSSource struct {
    cfg    Config
    client *http.Client
    parser *gofeed.Parser
}

func (s *RSSSource) Fetch(ctx context.Context) ([]ingestor.NewsRawItem, error) {
    for _, url := range s.cfg.URLs {
        feed, err := s.parser.ParseURLWithContext(url, ctx)
        if err != nil {
            continue // fail-open per URL
        }

        for _, item := range feed.Items {
            newsItem := ingestor.NewsRawItem{
                UID:       stableUID(item.Link, item.Title, s.cfg.NewsUIDBucket),
                Source:    "rss",
                Title:     item.Title,
                URL:       item.Link,
                TsMs:      item.PublishedParsed.UnixMilli(),
                Symbol:    extractSymbol(item.Title), // optional
                AssetClass: extractAssetClass(item.Title), // optional
            }
            out = append(out, newsItem)
        }
    }
    return out, nil
}
```

### 2. CryptoPanic API

#### Описание
CryptoPanic - специализированная платформа для крипто-новостей с профессиональным анализом и категоризацией. Предоставляет структурированные данные с фильтрацией по важности и валютам.

#### Конфигурация
```json
{
  "cryptopanic": {
    "enabled": true,
    "currencies": ["BTC", "ETH", "SOL", "BNB", "ADA", "DOT"],
    "filter": "important",
    "kind": "news",
    "region": "en"
  }
}
```

#### Параметры

| Параметр | Описание | Значения | По умолчанию |
|----------|----------|----------|-------------|
| `currencies` | Валюты для фильтрации | BTC, ETH, SOL, etc. | Все валюты |
| `filter` | Уровень важности | `important`, `hot`, `rising`, `bullish`, `bearish` | `important` |
| `kind` | Тип контента | `news`, `media` | `news` |
| `region` | Регион | `en`, `de`, `es`, `fr`, etc. | `en` |

#### API Key
```bash
# Получение ключа
CRYPTOPANIC_AUTH_TOKEN=your_token_here
```

#### Преимущества
- **Крипто-специфично** - фокус на цифровые активы
- **Категоризация** - теги и важность
- **Реальное время** - быстрая публикация
- **Качество** - профессиональный контент

#### Недостатки
- **Платный** - требуется подписка для полного доступа
- **Ограничение** - только крипто-новости
- **Rate limits** - ограничения на запросы

#### Пример Ответа API
```json
{
  "results": [
    {
      "id": 12345,
      "title": "Bitcoin ETF Sees Record Inflows as Institutional Interest Grows",
      "url": "https://cryptopanic.com/news/12345/bitcoin-etf-record-inflows",
      "published_at": "2024-01-15T10:30:00Z",
      "currencies": [{"code": "BTC", "title": "Bitcoin"}],
      "tags": ["ETF", "institutional", "bullish"],
      "kind": "news",
      "domain": "cryptopanic.com",
      "votes": {"positive": 85, "negative": 12, "important": 73, "liked": 45}
    }
  ]
}
```

#### Реализация
```go
func (s *Source) Fetch(ctx context.Context) ([]ingestor.NewsRawItem, error) {
    token := os.Getenv("CRYPTOPANIC_AUTH_TOKEN")
    if token == "" {
        return nil, nil // fail-open
    }

    url := fmt.Sprintf("%s/api/v1/posts/?auth_token=%s&currencies=%s&filter=%s",
        s.cfg.BaseURL, token,
        strings.Join(s.cfg.Currencies, ","),
        s.cfg.Filter)

    resp, err := s.hc.Get(url)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    var apiResp struct {
        Results []map[string]any `json:"results"`
    }

    if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
        return nil, err
    }

    var out []ingestor.NewsRawItem
    for _, item := range apiResp.Results {
        title := item["title"].(string)
        url := item["url"].(string)
        published := item["published_at"].(string)

        newsItem := ingestor.NewsRawItem{
            UID:    stableUID(url, title, s.cfg.NewsUIDBucket),
            Source: "cryptopanic",
            Title:  title,
            URL:    url,
            TsMs:   parseTimeMs(published),
            Symbol: extractCryptoSymbol(title),
        }
        out = append(out, newsItem)
    }
    return out, nil
}
```

### 3. Financial Modeling Prep (FMP)

#### Описание
FMP предоставляет финансовые данные, включая новости по акциям, экономические индикаторы и календарь событий. Идеально для традиционных финансовых рынков.

#### Конфигурация
```json
{
  "fmp": {
    "enabled": true,
    "tickers": ["SPY", "QQQ", "AAPL", "MSFT", "NVDA", "TSLA"],
    "economic": {
      "countries": ["US", "EU", "UK"],
      "importance": ["High", "Medium"]
    }
  }
}
```

#### API Key
```bash
FMP_API_KEY=your_api_key_here
```

#### Экономический Календарь
```json
{
  "economic": {
    "countries": ["US", "EU", "UK", "JP"],
    "importance": ["High", "Medium", "Low"],
    "indicators": ["GDP", "CPI", "NFP", "FOMC"]
  }
}
```

#### Преимущества
- **Широкий охват** - акции, ETF, экономика
- **Структурированные данные** - символы, даты, категории
- **Календарь событий** - предварительная информация
- **Надежность** - профессиональный источник

#### Недостатки
- **Стоимость** - платные тарифы
- **Rate limits** - ограничения на запросы
- **Фокус на США** - меньше данных по другим рынкам

#### Пример Запроса Новостей
```go
func (s *StockNewsSource) Fetch(ctx context.Context) ([]ingestor.NewsRawItem, error) {
    key := os.Getenv("FMP_API_KEY")
    if key == "" {
        return nil, nil
    }

    url := fmt.Sprintf("%s/api/v3/stock_news?tickers=%s&limit=%d&apikey=%s",
        s.cfg.BaseURL,
        strings.Join(s.cfg.Tickers, ","),
        s.cfg.Limit,
        key)

    resp, err := s.hc.Get(url)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    var rows []struct {
        Symbol        string `json:"symbol"`
        PublishedDate string `json:"publishedDate"`
        Title         string `json:"title"`
        URL           string `json:"url"`
        Text          string `json:"text"`
        Site          string `json:"site"`
    }

    if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
        return nil, err
    }

    var out []ingestor.NewsRawItem
    for _, row := range rows {
        newsItem := ingestor.NewsRawItem{
            UID:       stableUID(row.URL, row.Title, s.cfg.NewsUIDBucket),
            Source:    "fmp",
            Title:     row.Title,
            URL:       row.URL,
            TsMs:      parseFMPTimeMs(row.PublishedDate),
            Symbol:    row.Symbol,
            AssetClass: "equity",
        }
        out = append(out, newsItem)
    }
    return out, nil
}
```

#### Календарь Экономических Событий
```go
type FMPCalendarSource struct {
    APIKey        string
    BaseURL       string
    HTTPTimeout   time.Duration
    LookaheadDays int
    BackDays      int
    Countries     []string
    Importance    []string
    Enabled       bool
}

func (s *FMPCalendarSource) Fetch(ctx context.Context) ([]ingestor.CalendarEvent, error) {
    // Получение предстоящих экономических событий
    url := fmt.Sprintf("%s/api/v3/economic_calendar?apikey=%s",
        s.BaseURL, s.APIKey)

    // Фильтрация по странам и важности
    events := filterEvents(apiResponse, s.Countries, s.Importance)

    var out []ingestor.CalendarEvent
    for _, event := range events {
        calEvent := ingestor.CalendarEvent{
            EventID:   event.ID,
            Title:     event.Event,
            TsMs:      parseTimeMs(event.Date),
            GradeID:   importanceToGrade(event.Importance),
            Currency:  event.Currency,
            Region:    event.Country,
            Symbols:   []string{event.Currency}, // или связанные активы
        }
        out = append(out, calEvent)
    }
    return out, nil
}
```

### 4. NewsAPI

#### Описание
NewsAPI предоставляет доступ к тысячам новостных источников с гибкой фильтрацией и поиском. Подходит для широкого мониторинга новостей с возможностью таргетированного поиска по темам.

#### Конфигурация
```json
{
  "newsapi": {
    "enabled": true,
    "api_key": "${NEWSAPI_KEY}",
    "query": "(bitcoin OR ethereum OR crypto OR \"federal reserve\" OR FOMC OR CPI OR NFP)",
    "language": "en",
    "sort_by": "publishedAt",
    "page_size": 50,
    "sources_filter": ["reuters", "bloomberg", "coindesk", "cointelegraph"],
    "domains_filter": ["coindesk.com", "cointelegraph.com", "reuters.com"],
    "exclude_domains": ["spam-site.com", "low-quality-news.com"],
    "max_age_hours": 24,
    "batch_size": 25,
    "rate_limit_rpm": 50
  }
}
```

#### API Key и Аутентификация
```bash
# Получение API ключа
NEWSAPI_KEY=your_api_key_here

# Проверка ключа
curl "https://newsapi.org/v2/top-headlines?country=us&apiKey=${NEWSAPI_KEY}"
```

#### Расширенные Параметры Поиска

| Параметр | Тип | Описание | Пример |
|----------|-----|----------|---------|
| `q` | string | Поисковый запрос | `(bitcoin OR ethereum) AND (price OR market)` |
| `sources` | array | Конкретные источники | `["reuters", "bloomberg", "cnn"]` |
| `domains` | array | Домены источников | `["coindesk.com", "cointelegraph.com"]` |
| `excludeDomains` | array | Исключаемые домены | `["spam-site.com"]` |
| `language` | string | Язык новостей | `en`, `es`, `fr`, `de`, `it` |
| `sortBy` | string | Сортировка | `relevancy`, `popularity`, `publishedAt` |
| `pageSize` | int | Размер страницы | `50` (макс 100) |
| `page` | int | Номер страницы | `1` |
| `from` | date | Дата начала | `2024-01-01` |
| `to` | date | Дата окончания | `2024-01-15` |

#### Примеры Поисковых Запросов

##### Криптовалюты
```json
{
  "query": "(bitcoin OR ethereum OR crypto OR blockchain) AND (price OR market OR trading)",
  "language": "en",
  "sort_by": "publishedAt",
  "domains_filter": ["coindesk.com", "cointelegraph.com", "decrypt.co"]
}
```

##### Экономические Новости
```json
{
  "query": "(\"federal reserve\" OR FOMC OR CPI OR NFP OR GDP OR inflation OR rates)",
  "language": "en",
  "sources_filter": ["reuters", "bloomberg", "wsj"],
  "max_age_hours": 12
}
```

#### Преимущества
- **Широкий охват** - более 70,000 новостных источников
- **Гибкий поиск** - сложные boolean запросы
- **Качественные источники** - фильтрация по авторитетным изданиям
- **Многоязычность** - поддержка 10+ языков
- **Структурированные данные** - богатые метаданные

#### Недостатки и Решения
- **Rate limits**: 500 запросов/день бесплатно, платные тарифы для большего
- **Качество**: Требуется тщательная фильтрация источников
- **Реклама**: Возможны спонсорские материалы
- **Задержка**: Новости появляются после публикации

#### Оптимизации Производительности

##### Качественная Фильтрация Источников
```python
HIGH_QUALITY_SOURCES = {
    'crypto': ['coindesk', 'cointelegraph', 'decrypt', 'the-block'],
    'finance': ['reuters', 'bloomberg', 'wsj', 'ft', 'cnbc'],
    'economics': ['reuters', 'bloomberg', 'wsj', 'economist']
}

def filter_high_quality_sources(articles: List[Dict]) -> List[Dict]:
    """Фильтрация по качественным источникам"""
    filtered = []
    for article in articles:
        source_id = article.get('source', {}).get('id', '')
        if source_id in HIGH_QUALITY_SOURCES['crypto'] + \
                       HIGH_QUALITY_SOURCES['finance'] + \
                       HIGH_QUALITY_SOURCES['economics']:
            filtered.append(article)
    return filtered
```

##### Обнаружение Спама
```python
SPAM_INDICATORS = [
    'buy now', 'limited time', 'guaranteed returns',
    '100% profit', 'secret strategy', 'millionaire',
    'free money', 'urgent investment'
]

def filter_spam_articles(articles: List[Dict]) -> List[Dict]:
    """Фильтрация спам-статей"""
    filtered = []
    for article in articles:
        title = article.get('title', '').lower()
        description = article.get('description', '').lower()

        is_spam = any(indicator in title or indicator in description
                     for indicator in SPAM_INDICATORS)

        if not is_spam:
            filtered.append(article)

    return filtered
```

## Конфигурация Источников

### Общая Структура
```json
{
  "providers": ["cryptopanic", "fmp", "newsapi", "rss"],
  "cryptopanic": {
    "enabled": true,
    "currencies": ["BTC", "ETH"],
    "filter": "important"
  },
  "fmp": {
    "enabled": true,
    "tickers": ["SPY", "AAPL"],
    "economic": {
      "countries": ["US"],
      "importance": ["High"]
    }
  },
  "newsapi": {
    "enabled": true,
    "q": "bitcoin OR ethereum"
  },
  "rss": {
    "enabled": true,
    "urls": ["https://example.com/rss"]
  }
}
```

### Environment Variables
```bash
# API ключи
CRYPTOPANIC_AUTH_TOKEN=cp_token_here
FMP_API_KEY=fmp_token_here
NEWSAPI_KEY=newsapi_token_here

# Общие настройки
HTTP_TIMEOUT_SEC=10
USER_AGENT="NewsPipeline/1.0"
NEWS_UID_BUCKET_HOURS=6

# Redis
REDIS_URL=redis://redis-worker-1:6379/0

# Конфигурация источников
NEWS_SOURCES_JSON='{"providers": ["rss"], "rss": {"enabled": true}}'
```

### Fail-Open Логика
```python
def load_sources_config() -> SourcesConfig:
    raw_json = os.getenv("NEWS_SOURCES_JSON", "").strip()
    if not raw_json:
        # Дефолтная конфигурация - только RSS
        raw = {
            "providers": ["rss"],
            "rss": {"enabled": True, "urls": DEFAULT_RSS_URLS},
        }

    # Проверка доступности API ключей
    have_cp = bool(os.getenv("CRYPTOPANIC_AUTH_TOKEN"))
    have_fmp = bool(os.getenv("FMP_API_KEY"))
    have_newsapi = bool(os.getenv("NEWSAPI_KEY"))

    # Включение провайдеров только при наличии ключей
    flags = ProviderFlags(
        cryptopanic=_enabled("cryptopanic") and have_cp,
        fmp=_enabled("fmp") and have_fmp,
        newsapi=_enabled("newsapi") and have_newsapi,
        rss=_enabled("rss") if "rss" in raw else True,  # RSS по умолчанию
    )
```

## Мониторинг Источников

### Расширенные Метрики
```python
class SourceMetricsCollector:
    """Сборщик метрик по источникам"""

    def __init__(self, redis_client):
        self.redis = redis_client
        self.metrics_ttl = 86400 * 30  # 30 дней

    async def record_ingestion(self, source: str, count: int, latency_ms: float):
        """Запись метрики ingestion"""
        key = f"metrics:sources:{source}:ingestion"

        await self.redis.hincrby(key, "total_count", count)
        await self.redis.hincrbyfloat(key, "total_latency", latency_ms)
        await self.redis.expire(key, self.metrics_ttl)

        # Средняя latency
        total_count = await self.redis.hget(key, "total_count")
        total_latency = await self.redis.hget(key, "total_latency")

        if total_count and total_latency:
            avg_latency = float(total_latency) / int(total_count)
            await self.redis.hset(key, "avg_latency_ms", avg_latency)

    async def record_api_call(self, source: str, endpoint: str, status_code: int,
                            response_time_ms: float, error: Optional[str] = None):
        """Запись метрики API вызова"""
        key = f"metrics:sources:{source}:api"

        await self.redis.hincrby(key, f"status_{status_code}", 1)
        await self.redis.hincrbyfloat(key, "total_response_time", response_time_ms)

        if error:
            await self.redis.hincrby(key, f"error_{error}", 1)

        await self.redis.expire(key, self.metrics_ttl)

    async def record_quality_metrics(self, source: str, articles_count: int,
                                   quality_score: float, spam_detected: int):
        """Запись метрик качества"""
        key = f"metrics:sources:{source}:quality"

        await self.redis.hincrby(key, "articles_processed", articles_count)
        await self.redis.hincrbyfloat(key, "quality_sum", quality_score * articles_count)
        await self.redis.hincrby(key, "spam_detected", spam_detected)

        # Расчет средней quality
        total_articles = await self.redis.hget(key, "articles_processed")
        quality_sum = await self.redis.hget(key, "quality_sum")

        if total_articles and quality_sum:
            avg_quality = float(quality_sum) / int(total_articles)
            await self.redis.hset(key, "avg_quality", avg_quality)

        await self.redis.expire(key, self.metrics_ttl)

    async def get_source_stats(self, source: str) -> Dict[str, Any]:
        """Получение статистики по источнику"""
        stats = {}

        # Ingestion stats
        ingestion_key = f"metrics:sources:{source}:ingestion"
        ingestion_data = await self.redis.hgetall(ingestion_key)
        stats['ingestion'] = {
            'total_count': int(ingestion_data.get('total_count', 0)),
            'avg_latency_ms': float(ingestion_data.get('avg_latency_ms', 0))
        }

        # API stats
        api_key = f"metrics:sources:{source}:api"
        api_data = await self.redis.hgetall(api_key)
        stats['api'] = {
            'calls': sum(int(v) for k, v in api_data.items() if k.startswith('status_')),
            'errors': sum(int(v) for k, v in api_data.items() if k.startswith('error_')),
            'avg_response_time': float(api_data.get('avg_response_time', 0))
        }

        # Quality stats
        quality_key = f"metrics:sources:{source}:quality"
        quality_data = await self.redis.hgetall(quality_key)
        stats['quality'] = {
            'articles_processed': int(quality_data.get('articles_processed', 0)),
            'avg_quality': float(quality_data.get('avg_quality', 0)),
            'spam_detected': int(quality_data.get('spam_detected', 0))
        }

        return stats
```

### Health Checks
```python
class SourceHealthChecker:
    """Проверка здоровья источников"""

    def __init__(self, redis_client, config: Dict[str, Any]):
        self.redis = redis_client
        self.config = config
        self.check_interval_sec = config.get('health_check_interval', 300)  # 5 мин

    async def check_all_sources(self) -> Dict[str, Dict[str, Any]]:
        """Проверка всех источников"""
        results = {}

        # RSS sources
        rss_sources = self.config.get('rss', {}).get('feeds', [])
        for feed in rss_sources:
            source_name = f"rss_{feed['url'].split('/')[-1]}"
            results[source_name] = await self.check_rss_feed(feed['url'])

        # API sources
        api_sources = ['cryptopanic', 'fmp', 'newsapi']
        for source in api_sources:
            if source in self.config and self.config[source].get('enabled', False):
                results[source] = await self.check_api_source(source)

        return results

    async def check_rss_feed(self, url: str) -> Dict[str, Any]:
        """Проверка RSS фида"""
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                start_time = time.time()
                async with session.get(url) as response:
                    response_time = time.time() - start_time

                    if response.status == 200:
                        content = await response.text()
                        # Простая проверка на XML
                        if '<?xml' in content or '<rss' in content:
                            return {
                                'status': 'healthy',
                                'response_time_ms': response_time * 1000,
                                'last_check': time.time()
                            }

                    return {
                        'status': 'unhealthy',
                        'error': f'HTTP {response.status}',
                        'response_time_ms': response_time * 1000,
                        'last_check': time.time()
                    }

        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'last_check': time.time()
            }

    async def check_api_source(self, source: str) -> Dict[str, Any]:
        """Проверка API источника"""
        try:
            # Для каждого источника свой метод проверки
            if source == 'cryptopanic':
                return await self._check_cryptopanic_api()
            elif source == 'fmp':
                return await self._check_fmp_api()
            elif source == 'newsapi':
                return await self._check_newsapi_api()

        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'last_check': time.time()
            }

    async def _check_cryptopanic_api(self) -> Dict[str, Any]:
        """Проверка CryptoPanic API"""
        api_key = os.getenv('CRYPTOPANIC_AUTH_TOKEN')
        if not api_key:
            return {'status': 'disabled', 'reason': 'no_api_key'}

        url = f"https://cryptopanic.com/api/v1/posts/?auth_token={api_key}&filter=hot&limit=1"

        async with aiohttp.ClientSession() as session:
            start_time = time.time()
            async with session.get(url) as response:
                response_time = time.time() - start_time

                if response.status == 200:
                    data = await response.json()
                    if 'results' in data:
                        return {
                            'status': 'healthy',
                            'response_time_ms': response_time * 1000,
                            'quota_remaining': response.headers.get('X-RateLimit-Remaining', 'unknown'),
                            'last_check': time.time()
                        }

                return {
                    'status': 'unhealthy',
                    'error': f'API returned {response.status}',
                    'response_time_ms': response_time * 1000,
                    'last_check': time.time()
                }
```

### Алерты и Мониторинг
```python
class SourceAlertManager:
    """Менеджер алертов для источников"""

    def __init__(self, redis_client, alert_config: Dict[str, Any]):
        self.redis = redis_client
        self.config = alert_config

    async def check_alerts(self, source: str, metrics: Dict[str, Any]):
        """Проверка условий для алертов"""
        alerts = []

        # Алерт на недоступность источника
        health_key = f"health:sources:{source}"
        health_data = await self.redis.get(health_key)

        if health_data:
            health = json.loads(health_data)
            if health.get('status') == 'unhealthy':
                # Проверка длительности проблемы
                unhealthy_duration = time.time() - health.get('last_check', 0)
                if unhealthy_duration > self.config.get('unhealthy_threshold_sec', 300):  # 5 мин
                    alerts.append({
                        'type': 'source_unhealthy',
                        'source': source,
                        'severity': 'critical',
                        'message': f'Source {source} has been unhealthy for {unhealthy_duration:.0f}s',
                        'duration_sec': unhealthy_duration
                    })

        # Алерт на низкое качество
        quality_key = f"metrics:sources:{source}:quality"
        quality_data = await self.redis.hgetall(quality_key)

        if quality_data:
            avg_quality = float(quality_data.get('avg_quality', 1.0))
            if avg_quality < self.config.get('quality_threshold', 0.6):
                alerts.append({
                    'type': 'low_quality',
                    'source': source,
                    'severity': 'warning',
                    'message': f'Source {source} quality dropped to {avg_quality:.2f}',
                    'quality_score': avg_quality
                })

        # Алерт на rate limit
        api_key = f"metrics:sources:{source}:api"
        api_data = await self.redis.hgetall(api_key)

        if api_data:
            rate_limit_errors = sum(int(v) for k, v in api_data.items()
                                  if k.startswith('error_rate_limit'))
            if rate_limit_errors > self.config.get('rate_limit_alert_threshold', 5):
                alerts.append({
                    'type': 'rate_limit',
                    'source': source,
                    'severity': 'warning',
                    'message': f'Source {source} hit rate limit {rate_limit_errors} times',
                    'error_count': rate_limit_errors
                })

        # Отправка алертов
        for alert in alerts:
            await self.send_alert(alert)

    async def send_alert(self, alert: Dict[str, Any]):
        """Отправка алерта"""
        alert_key = f"alerts:sources:{int(time.time())}"
        await self.redis.setex(alert_key, 86400, json.dumps(alert))  # 24 часа

        # Здесь можно добавить интеграцию с PagerDuty, Slack и т.д.
        logger.warning(f"Source alert: {alert['message']}")

        # Отправка в систему алертов
        await self._send_to_alert_system(alert)
```

### Производственные Конфигурации

#### High Volume Configuration
```json
{
  "sources": {
    "rss": {
      "feeds": [
        {"url": "https://coindesk.com/arc/outboundfeeds/rss/", "priority": "high"},
        {"url": "https://cointelegraph.com/rss", "priority": "high"},
        {"url": "https://www.cnbc.com/id/100003114/device/rss/rss.html", "priority": "medium"}
      ],
      "batch_size": 20,
      "refresh_interval_sec": 180,
      "timeout_sec": 15
    },
    "cryptopanic": {
      "enabled": true,
      "batch_size": 25,
      "rate_limit_rpm": 45,
      "cache_ttl_sec": 300
    },
    "fmp": {
      "enabled": true,
      "batch_size": 15,
      "rate_limit_rpm": 30,
      "cache_ttl_sec": 600
    },
    "newsapi": {
      "enabled": true,
      "batch_size": 10,
      "rate_limit_rpm": 50,
      "cache_ttl_sec": 180
    }
  },
  "monitoring": {
    "health_check_interval_sec": 300,
    "metrics_retention_days": 30,
    "alert_thresholds": {
      "unhealthy_threshold_sec": 300,
      "quality_threshold": 0.6,
      "rate_limit_alert_threshold": 5
    }
  }
}
```

#### Cost Optimized Configuration
```json
{
  "sources": {
    "rss": {
      "feeds": [
        {"url": "https://coindesk.com/arc/outboundfeeds/rss/", "priority": "high"}
      ],
      "batch_size": 50,
      "refresh_interval_sec": 600,
      "timeout_sec": 30
    },
    "cryptopanic": {
      "enabled": true,
      "batch_size": 50,
      "rate_limit_rpm": 10,
      "cache_ttl_sec": 1800
    },
    "newsapi": {
      "enabled": true,
      "batch_size": 25,
      "rate_limit_rpm": 10,
      "cache_ttl_sec": 3600
    }
  }
}
```

## Оптимизация

### Rate Limiting
```python
class SourceRateLimiter:
    def __init__(self, requests_per_minute: int):
        self.capacity = requests_per_minute
        self.tokens = requests_per_minute
        self.last_refill = time.time()
        self.lock = threading.Lock()

    def acquire(self) -> bool:
        with self.lock:
            now = time.time()
            elapsed = now - self.last_refill
            self.tokens = min(self.capacity, self.tokens + elapsed * (self.capacity / 60))
            self.last_refill = now

            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False
```

### Кеширование
```python
# Кеш для RSS фидов
rss_cache = redis.Redis(db=1)  # Отдельная БД для кеша

def get_cached_feed(url: str, ttl_sec: int = 300) -> str:
    cache_key = f"rss:cache:{hash(url)}"
    cached = rss_cache.get(cache_key)
    if cached:
        return cached.decode()

    # Скачивание и кеширование
    feed_content = download_feed(url)
    rss_cache.setex(cache_key, ttl_sec, feed_content)
    return feed_content
```

### Параллельная Обработка
```python
async def fetch_all_sources(sources: List[Source]) -> List[NewsItem]:
    """Параллельная загрузка из всех источников"""
    tasks = [asyncio.create_task(source.fetch()) for source in sources]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_news = []
    for result in results:
        if isinstance(result, Exception):
            log.error(f"Source failed: {result}")
            continue
        all_news.extend(result)

    return all_news
```

## Безопасность

### API Keys
- Хранение только в environment variables
- Ротация ключей каждые 90 дней
- Мониторинг использования ключей
- Ограничение доступа к конфигурационным файлам

### Network Security
```python
# HTTPS-only для API вызовов
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = True
ssl_context.verify_mode = ssl.CERT_REQUIRED

# Timeout для предотвращения зависаний
timeout = aiohttp.ClientTimeout(total=10, connect=5)

async with aiohttp.ClientSession(timeout=timeout, connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
    async with session.get(url) as response:
        return await response.text()
```

### Data Validation
```python
def validate_news_item(item: NewsRawItem) -> bool:
    """Валидация новости перед обработкой"""
    if not item.title or len(item.title.strip()) < 10:
        return False
    if not item.url or not item.url.startswith(('http://', 'https://')):
        return False
    if item.ts_ms > time.time() * 1000 + 3600000:  # Не более чем час в будущем
        return False
    return True
```

## Примеры Конфигураций

### Базовая Конфигурация (Только RSS)
```json
{
  "providers": ["rss"],
  "rss": {
    "enabled": true,
    "urls": [
      "https://www.ecb.europa.eu/rss/press.html",
      "https://feeds.reuters.com/reuters/topNews"
    ]
  }
}
```

### Крипто-Фокус
```json
{
  "providers": ["cryptopanic", "rss"],
  "cryptopanic": {
    "enabled": true,
    "currencies": ["BTC", "ETH", "SOL", "ADA"],
    "filter": "important"
  },
  "rss": {
    "enabled": true,
    "urls": [
      "https://coindesk.com/arc/outboundfeeds/rss/",
      "https://cointelegraph.com/rss"
    ]
  }
}
```

### Полная Финансовая Конфигурация
```json
{
  "providers": ["cryptopanic", "fmp", "newsapi", "rss"],
  "cryptopanic": {
    "enabled": true,
    "currencies": ["BTC"],
    "filter": "important"
  },
  "fmp": {
    "enabled": true,
    "tickers": ["SPY", "QQQ", "AAPL", "TSLA"],
    "economic": {
      "countries": ["US"],
      "importance": ["High", "Medium"]
    }
  },
  "newsapi": {
    "enabled": true,
    "q": "(bitcoin OR ethereum OR FOMC OR CPI OR NFP)"
  },
  "rss": {
    "enabled": true,
    "urls": [
      "https://www.ecb.europa.eu/rss/press.html",
      "https://www.ft.com/rss/home/uk"
    ]
  }
}
```

## Troubleshooting

### Распространенные Проблемы

1. **Источник не возвращает новости**
   ```bash
   # Проверить доступность URL
   curl -I https://example.com/rss

   # Проверить API ключ
   curl "https://api.example.com/news?key=YOUR_KEY"
   ```

2. **API Rate Limit**
   ```python
   # Добавить задержку между запросами
   time.sleep(60 / requests_per_minute)

   # Использовать несколько ключей
   api_keys = ["key1", "key2", "key3"]
   current_key = api_keys[request_count % len(api_keys)]
   ```

3. **Низкое качество новостей**
   ```python
   # Добавить фильтры качества
   if len(title) < 20 or "spam" in title.lower():
       continue

   # Использовать whitelist источников
   allowed_domains = ["reuters.com", "bloomberg.com", "coindesk.com"]
   if not any(domain in url for domain in allowed_domains):
       continue
   ```

4. **Дублирование новостей**
   ```python
   # Улучшить UID генерацию
   uid = stable_uid(url, title, published_date, source)

   # Добавить контент-хеш
   content_hash = hashlib.md5(content.encode()).hexdigest()
   uid = f"{base_uid}:{content_hash[:8]}"
   ```
