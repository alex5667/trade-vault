# Новостной пайплайн - переменные окружения

## Основные настройки

### DEDUPE_TTL
TTL для ключей дедупликации в Redis (защита от повторной обработки).
- **Дефолт**: 7*24*time.Hour (7 дней)
- **Пример**: `DEDUPE_TTL=168h`

### NEWS_UID_BUCKET
Бакет для генерации UID новостей (влияет на частоту генерации новых UID для одной и той же новости).
- **Дефолт**: 6*time.Hour (6 часов)
- **Рекомендуемые значения**:
  - `6h` - более "живой" (меньше повторов, дефолт)
  - `24h` - сбалансированный
- **Пример**: `NEWS_UID_BUCKET=6h`

## Источники новостей

### NEWS_SOURCES_JSON
JSON конфигурация источников новостей.
- **Дефолт**: RSS только
- **Пример с API ключами**:
```json
{
  "providers": ["cryptopanic", "fmp", "newsapi", "rss"],
  "cryptopanic": {"enabled": true, "currencies": ["BTC","ETH"]},
  "fmp": {"enabled": true, "tickers": ["SPY","QQQ"]}, 
  "newsapi": {"enabled": true, "q": "(bitcoin OR crypto)"},
  "rss": {"enabled": true, "urls": ["https://cointelegraph.com/rss"]}
}
```

## API ключи
- `CRYPTOPANIC_AUTH_TOKEN` - для CryptoPanic API
- `FMP_API_KEY` - для Financial Modeling Prep API  
- `NEWSAPI_KEY` - для NewsAPI

## Примеры использования

### Минимальная конфигурация (только RSS)
```bash
export NEWS_SOURCES_JSON='{"providers":["rss"],"rss":{"enabled":true}}'
```

### Полная конфигурация с API
```bash
export NEWS_UID_BUCKET=6h
export NEWS_SOURCES_JSON="$(cat default_news_sources.json)"
export CRYPTOPANIC_AUTH_TOKEN="your_key"
export FMP_API_KEY="your_key"
export NEWSAPI_KEY="your_key"
```
