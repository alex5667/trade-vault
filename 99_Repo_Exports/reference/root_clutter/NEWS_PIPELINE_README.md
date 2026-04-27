# News Pipeline

Real-time news processing pipeline for trading signals with LLM analysis and calendar events.

## Architecture

The pipeline consists of several services that work together to process news and calendar data:

### Services

1. **news-ingestor-go** (Go) - Primary news ingestor with leader election
2. **news-ingestor-py-standby** (Python) - Standby news ingestor with leader-lock failover
3. **news-analyzer** (Python) - LLM-based news analysis
4. **news-feature-store** (Python) - News aggregation with EMA
5. **calendar-feature-store** (Python) - Calendar event processing
6. **news-watchdog** (Go) - Health monitoring

### Leader Election System

The news ingestors use Redis-based leader election for high availability:

- **Go ingestor** is the primary leader that actively ingests news
- **Python standby** monitors health metrics and takes over if Go fails
- **Leader lock** (`news:ingestor:leader`) prevents duplicate ingestion
- **Health tracking** (`news:health:last_ingest_ts_ms`) monitors last successful ingestion
- **Automatic failover** occurs within 3 minutes if primary fails

### Data Flow

```
RSS Sources → news-ingestor → news:raw stream → news-analyzer → news:analysis stream
                                                                ↓
                                                        news-feature-store → news:agg:*
                                                                ↓
                                                        tick-loop → ctx.news
```

## Configuration

### Environment Variables

Copy `news-pipeline.env.example` to `news-pipeline.env` and customize:

```bash
cp news-pipeline.env.example news-pipeline.env
```

### Key Settings

- `NEWS_SOURCES_JSON`: JSON configuration for all news providers (see example below)
- `CRYPTOPANIC_AUTH_TOKEN`: API key for CryptoPanic news
- `FMP_API_KEY`: API key for Financial Modeling Prep
- `NEWSAPI_KEY`: API key for NewsAPI
- `GEMINI_API_KEY`: Your Google Gemini API key for news analysis
- `REDIS_URL`: Redis connection string
- `NEWS_RISK_HALF_LIFE_SEC`: EMA half-life for risk aggregation (default: 30 min)

### News Sources JSON Example

```json
{
  "providers": ["rss", "cryptopanic", "fmp", "newsapi"],
  "cryptopanic": {
    "enabled": true,
    "currencies": ["BTC", "ETH", "SOL", "BNB"],
    "filter": "important",
    "kind": "news",
    "region": "en"
  },
  "fmp": {
    "enabled": true,
    "tickers": ["SPY", "QQQ", "AAPL", "MSFT", "NVDA"],
    "economic": {
      "countries": ["US", "EU"],
      "importance": ["High", "Medium"]
    }
  },
  "newsapi": {
    "enabled": true,
    "q": "(bitcoin OR ethereum OR FOMC OR CPI OR NFP)",
    "language": "en"
  },
  "rss": {
    "enabled": true,
    "urls": ["https://www.ecb.europa.eu/rss/press.html"]
  }
}
```

## Deployment

### Using Docker Compose

The news pipeline is included in the main `docker-compose.yml` via `docker-compose-news-pipeline.yml`.

Start all services:
```bash
docker-compose up -d
```

Start only news pipeline:
```bash
docker-compose --profile news-pipeline up -d
```

### Individual Services

Start specific service:
```bash
docker-compose up -d news-ingestor-go
docker-compose up -d news-analyzer
```

## Monitoring

### Health Checks

- **news-ingestor-go**: Health endpoint at `:8097/health`
- **news-watchdog**: Monitors heartbeat keys `hb:news` and `hb:calendar`

### Logs

Check service logs:
```bash
docker-compose logs -f news-analyzer
```

### Redis Keys

Monitor Redis streams and keys:
```bash
# Check stream lengths
redis-cli XLEN news:raw
redis-cli XLEN news:analysis

# Check aggregations
redis-cli HGETALL "news:agg:BTCUSDT"
redis-cli GET "calendar:next:USD"

# Check heartbeats
redis-cli GET "hb:news"
redis-cli GET "hb:calendar"
```

## Development

### Local Testing

1. Start Redis:
```bash
docker run -d -p 6379:6379 redis:7-alpine
```

2. Set environment variables:
```bash
export REDIS_URL=redis://localhost:6379/0
export NEWS_RSS_URLS=https://feeds.reuters.com/reuters/topNews
```

3. Run services locally:
```bash
# Go services
cd go-news-services && go run cmd/news-ingestor/main.go

# Python services
cd python-worker && python -m news_pipeline.analyzer_worker
```

### Testing

Run tests:
```bash
cd python-worker && python -m pytest tests/test_news_enricher.py -v
```

## API Keys

### Gemini API

1. Get API key from [Google AI Studio](https://makersuite.google.com/app/apikey)
2. Set in `news-pipeline.env`:
```bash
GEMINI_API_KEY=your_actual_api_key_here
```

## Troubleshooting

### Common Issues

1. **No news appearing**: Check RSS URLs are accessible
2. **LLM analysis failing**: Verify Gemini API key and quota
3. **Redis connection errors**: Check Redis is running and accessible
4. **Leader election issues**: Check network connectivity between containers

### Debug Mode

Enable debug logging:
```bash
export LOG_LEVEL=DEBUG
```

### Reset Pipeline

Clear Redis data:
```bash
redis-cli FLUSHALL
```

## Performance

### Resource Requirements

- **news-ingestor-go**: 256MB RAM, 0.5 CPU
- **news-analyzer**: 1GB RAM, 1.0 CPU (LLM processing)
- **news-feature-store**: 512MB RAM, 0.5 CPU
- **calendar-feature-store**: 512MB RAM, 0.5 CPU

### Scaling

- Multiple analyzer instances can run with different consumer names
- Feature stores are stateless and can be scaled horizontally
- Use Redis cluster for high availability

## Security

- Store API keys in environment variables, not in code
- Use Docker secrets in production deployments
- Limit Redis network access to internal networks only
