#!/usr/bin/env bash
# ── deploy-news-agent-to-minik.sh ──────────────────────────────────────────
# Синхронизирует news_agent проект на minik и запускает docker compose.
#
# Использование:
#   ./scripts/deploy-news-agent-to-minik.sh          # sync + build + up
#   ./scripts/deploy-news-agent-to-minik.sh sync     # только sync
#   ./scripts/deploy-news-agent-to-minik.sh up       # только запуск (без sync)
#   ./scripts/deploy-news-agent-to-minik.sh down      # остановка на minik
#   ./scripts/deploy-news-agent-to-minik.sh logs      # логи на minik
#   ./scripts/deploy-news-agent-to-minik.sh status    # статус контейнеров
# ────────────────────────────────────────────────────────────────────────────
set -euo pipefail

MINIK_HOST="${MINIK_HOST:-192.168.0.121}"
MINIK_USER="${MINIK_USER:-alex}"
MAIN_HOST="${MAIN_HOST:-192.168.0.168}"

# Paths
LOCAL_NEWS_AGENT="/home/alex/front/trade/news_agent"
REMOTE_NEWS_AGENT="/opt/trade-agent/news_agent"

# Redis credentials from main host
REDIS_PASSWORD="rJaZri08lLMXPQv0Y-3zXGxGsB_5w44Dh5YvkL6_XAY"
REDIS_USER="go_gateway"
REDIS_URL="redis://${REDIS_USER}:${REDIS_PASSWORD}@${MAIN_HOST}:63791/0"

# Load local credentials if available
if [ -f .env ]; then
    TRADING_PASSWORD=$(grep "^TRADING_PASSWORD=" .env | cut -d'=' -f2-)
    export TRADING_PASSWORD
fi

# Postgres from main host (port 5434 → 5432 inside container)
PG_DSN="postgresql://trading:${TRADING_PASSWORD:-trading_password}@${MAIN_HOST}:5434/scanner_analytics"

REMOTE_DC="docker compose -f ${REMOTE_NEWS_AGENT}/docker-compose-news.yml -f ${REMOTE_NEWS_AGENT}/docker-compose.override.minik.yml --env-file ${REMOTE_NEWS_AGENT}/.env.minik"

SSH="ssh -o ConnectTimeout=10 -o BatchMode=yes ${MINIK_USER}@${MINIK_HOST}"

ACTION="${1:-all}"

# ── Functions ──────────────────────────────────────────────────────────────

sync_files() {
    echo "📦 Синхронизация news_agent → ${MINIK_USER}@${MINIK_HOST}:${REMOTE_NEWS_AGENT}/"
    ${SSH} "mkdir -p ${REMOTE_NEWS_AGENT}"

    rsync -avz --delete \
        --exclude='.git' \
        --exclude='__pycache__' \
        --exclude='.pytest_cache' \
        --exclude='.ruff_cache' \
        --exclude='*.diff' \
        --exclude='services/telegram_worker/sessions' \
        --exclude='*.pdf' \
        --exclude='*.zip' \
        --exclude='.env' \
        --exclude='.env.example' \
        --exclude='.env.example.orig' \
        --exclude='.env.example.rej' \
        --exclude='*.orig' \
        --exclude='*.rej' \
        -e "ssh -o ConnectTimeout=10" \
        "${LOCAL_NEWS_AGENT}/" "${MINIK_USER}@${MINIK_HOST}:${REMOTE_NEWS_AGENT}/"

    echo "✅ Файлы синхронизированы"
}

ensure_network() {
    echo "🌐 Проверка Docker сети scanner-network на minik..."
    ${SSH} '
        if ! docker network ls --format "{{.Name}}" | grep -qx "scanner-network"; then
            echo "  Создание scanner-network..."
            docker network create scanner-network >/dev/null && echo "  ✅ scanner-network создана" || echo "  ⚠️  Ошибка создания сети"
        else
            echo "  ✅ scanner-network уже существует"
        fi
    '
}

generate_override() {
    echo "📝 Генерация docker-compose.override.minik.yml..."
    ${SSH} "cat > ${REMOTE_NEWS_AGENT}/docker-compose.override.minik.yml" <<'OVEOF'
# Auto-generated override for minik — DNS + extra_hosts for LAN connectivity
services:
  news_ingest:
    dns:
      - 8.8.8.8
      - 1.1.1.1
    extra_hosts:
      - "host.docker.internal:host-gateway"
  news_norm:
    dns:
      - 8.8.8.8
      - 1.1.1.1
    extra_hosts:
      - "host.docker.internal:host-gateway"
  news_reasoner:
    dns:
      - 8.8.8.8
      - 1.1.1.1
    extra_hosts:
      - "host.docker.internal:host-gateway"
  news_feedback_batch:
    dns:
      - 8.8.8.8
      - 1.1.1.1
    extra_hosts:
      - "host.docker.internal:host-gateway"
  news_agent_timer:
    dns:
      - 8.8.8.8
      - 1.1.1.1
    extra_hosts:
      - "host.docker.internal:host-gateway"
  news_label_ingest:
    dns:
      - 8.8.8.8
      - 1.1.1.1
    extra_hosts:
      - "host.docker.internal:host-gateway"
  news_training_batch:
    dns:
      - 8.8.8.8
      - 1.1.1.1
    extra_hosts:
      - "host.docker.internal:host-gateway"
  news_trade_reco:
    dns:
      - 8.8.8.8
      - 1.1.1.1
    extra_hosts:
      - "host.docker.internal:host-gateway"
OVEOF
    echo "✅ Override создан"
}

generate_env() {
    echo "📝 Генерация .env.minik на ${MINIK_HOST}..."
    ${SSH} "cat > ${REMOTE_NEWS_AGENT}/.env.minik" <<ENVEOF
# === Auto-generated for minik ($(date -u +%Y-%m-%dT%H:%M:%SZ)) ===
# Redis / Postgres — main host over LAN
REDIS_URL=${REDIS_URL}
PG_DSN=${PG_DSN}

# === Streams ===
NEWS_STREAM_RAW=stream:news_raw
NEWS_STREAM_NORM=stream:news_norm
NEWS_STREAM_EVENTS=stream:news_events
NEWS_STREAM_SIGNALS=stream:signals_news
NEWS_STREAM_DLQ=stream:news_dlq

NEWS_CONSUMER_GROUP_RAW=news_raw_g
NEWS_CONSUMER_GROUP_NORM=news_norm_g
NEWS_CONSUMER_ID=minik-worker-1

# === Ingest sources ===
NEWS_RSS_URLS=https://news.google.com/rss/search?q=bitcoin&hl=en&gl=US&ceid=US:en
NEWS_RSS_POLL_SEC=30

NEWS_GDELT_ENABLE=0
NEWS_GDELT_BASE_URL=https://api.gdeltproject.org/api/v2/doc/doc
NEWS_GDELT_QUERY=bitcoin OR BTC OR ethereum OR ETH
NEWS_GDELT_MAX_RECORDS=50

NEWS_NEWSAPI_ENABLE=0
NEWS_FINNHUB_ENABLE=0
NEWS_ALPHAVANTAGE_ENABLE=0
NEWSAPI_KEY=changeme
FINNHUB_TOKEN=changeme
ALPHAVANTAGE_KEY=changeme

# === Prefilter / Dedup ===
NEWS_MAX_DOC_CHARS=8000
NEWS_DEDUP_URL_TTL_SEC=86400
NEWS_DEDUP_CONTENT_TTL_SEC=604800
NEWS_DEDUP_INFLIGHT_TTL_SEC=600
NEWS_PREFILTER_ENABLE=1
NEWS_SYMBOL_WHITELIST=BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT,DOGEUSDT

NEWS_UNIVERSE_DYNAMIC_ENABLE=1
NEWS_UNIVERSE_SYMBOLS_KEY=trade:universe:symbols
NEWS_UNIVERSE_ALIASES_KEY=trade:universe:aliases
NEWS_UNIVERSE_REFRESH_SEC=30

NEWS_TIME_MAX_PAST_MS=604800000
NEWS_TIME_MAX_FUTURE_MS=300000

NEWS_NEAR_DUP_ENABLE=1
NEWS_NEAR_DUP_HAMMING_MAX=3
NEWS_NEAR_DUP_BANDS=4
NEWS_NEAR_DUP_BAND_BITS=16
NEWS_NEAR_DUP_BUCKET_SIZE=200
NEWS_NEAR_DUP_TTL_SEC=604800
NEWS_NEAR_DUP_KEY_PREFIX=news:lsh:simhash:v1

NEWS_SUPERVISOR_INTERVAL_SEC=5
NEWS_SUPERVISOR_MAX_RESTARTS=10
NEWS_SUPERVISOR_WINDOW_SEC=300
NEWS_SUPERVISOR_COOLDOWN_SEC=60

# === LLM Router (P2) ===
NEWS_PROVIDER_ORDER=gemini,qwen,kimi
NEWS_PROVIDER_TIMEOUT_MS=5000
NEWS_PROVIDER_RETRIES=1
NEWS_PROVIDER_RETRY_BACKOFF_MS=250
NEWS_PROVIDER_RATE_LIMIT_PER_MIN=0
NEWS_LLM_TEMPERATURE=0.0
NEWS_LLM_MAX_COST_PER_CALL_USD=0.02
NEWS_LLM_COST_PER_1K_PROMPT_USD=0.0
NEWS_LLM_COST_PER_1K_COMPLETION_USD=0.0

NEWS_LLM_MODEL_GEMINI=gemini/gemini-1.5-flash
NEWS_LLM_MODEL_QWEN=qwen/qwen-turbo
NEWS_LLM_MODEL_KIMI=openai/moonshot-v1-8k

GEMINI_API_KEY=changeme
QWEN_API_KEY=changeme
KIMI_API_KEY=changeme

# === Hard caps ===
NEWS_LLM_MAX_CALLS_PER_DAY=2500
NEWS_LLM_BUDGET_DAILY_USD=10.0

# === Publish thresholds ===
NEWS_MIN_IMPACT_PUBLISH=0.40
NEWS_PRIOR_TTL_MS=900000

# === P3: Trust & Relevance ===
NEWS_TRUST_LOOKBACK_DAYS=30
NEWS_CORROBORATION_N=2
NEWS_CORROBORATION_WINDOW_SEC=3600
NEWS_TRUST_SMOOTH_ALPHA=2.0
NEWS_TRUST_SMOOTH_BETA=2.0
NEWS_MARKET_LABEL_WINDOW_SEC=3600
NEWS_MARKET_RET_SOFT_BPS=40
NEWS_MARKET_VOL_SOFT_BPS=80
NEWS_MARKET_SCORE_WEIGHT_RET=0.6
NEWS_MARKET_SCORE_WEIGHT_VOL=0.4

# === Trade integration (P4) ===
NEWS_PRIOR_GATE_PROFILE=tighten
NEWS_PRIOR_MIN_IMPACT=0.40
NEWS_PRIOR_TIGHTEN_MIN_CONF=75.0
NEWS_PRIOR_TIGHTEN_RISK_MULT=0.70
NEWS_PRIOR_HARD_MAX_CRED=0.35
NEWS_PRIOR_HARD_REQUIRE_CONFLICT=1
NEWS_PRIOR_HARD_PUMP_FLAG=pump_suspect

NEWS_PRIOR_PROVIDER_MODE=both
NEWS_PRIOR_KEY_PREFIX=news:prior:
NEWS_PRIOR_PROVIDER_BLOCK_MS=250
NEWS_PRIOR_PROVIDER_POLL_SEC=5
NEWS_PRIOR_CACHE_TTL_MS=900000
NEWS_PRIOR_CACHE_MAX_SYMBOLS=2048

# === P4 trade-side cache (Go) ===
TRADE_NEWS_RECO_STREAM_IN=stream:trade_recos_news
TRADE_NEWS_RECO_GROUP=trade_news_reco_cache_g
TRADE_NEWS_RECO_CONSUMER=trade_news_reco_cache_1
TRADE_NEWS_RECO_KEY_PREFIX=trade:cache:news_reco:
TRADE_NEWS_RECO_DLQ_STREAM=stream:trade_news_reco_cache_dlq
TRADE_NEWS_RECO_BLOCK_MS=5000
TRADE_NEWS_RECO_COUNT=64
TRADE_NEWS_RECO_TTL_MIN_MS=1000
TRADE_NEWS_RECO_TTL_MAX_MS=3600000

TRADE_NEWS_RECO_MAP_ENABLE=1
TRADE_NEWS_RECO_MAP_KEY=trade:cache:news_reco_map
TRADE_NEWS_RECO_MAP_FLUSH_MS=250
TRADE_NEWS_RECO_MAP_SWEEP_MS=1000
TRADE_NEWS_RECO_MAP_REFRESH_MS=2500
TRADE_NEWS_RECO_MAP_MAX_SYMBOLS=500
TRADE_NEWS_RECO_MAP_MAX_BYTES=1500000
TRADE_NEWS_RECO_MAP_TTL_MAX_MS=3600000

TRADE_NEWS_RECO_CLAIM_ENABLE=1
TRADE_NEWS_RECO_CLAIM_IDLE_MS=60000
TRADE_NEWS_RECO_CLAIM_EVERY_S=15
TRADE_NEWS_RECO_CLAIM_COUNT=128

# === news_labeler marketdata ===
KLINE_STREAM_KEY=stream:klines_1m
KLINE_STREAM_GROUP=news_labeler_g
KLINE_STREAM_BLOCK_MS=5000
KLINE_STREAM_COUNT=200
KLINE_STREAM_CLAIM_IDLE_MS=60000

NEWS_STREAM_GROUPS=stream:news_raw=news_raw_g;stream:news_norm=news_norm_g

METRICS_PORT=9210
ENVEOF

    echo "✅ .env.minik создан"
    # Symlink .env → .env.minik (original compose uses env_file: [.env] in x-common anchor)
    ${SSH} "ln -sf ${REMOTE_NEWS_AGENT}/.env.minik ${REMOTE_NEWS_AGENT}/.env"
    echo "✅ .env → .env.minik symlink создан"
}

build_and_up() {
    ensure_network
    echo "🔨 Сборка и запуск news_agent на minik..."
    ${SSH} "cd ${REMOTE_NEWS_AGENT} && ${REMOTE_DC} build --parallel 2>/dev/null || ${REMOTE_DC} build"
    ${SSH} "cd ${REMOTE_NEWS_AGENT} && ${REMOTE_DC} up -d"
    echo ""
    echo "✅ News Agent запущен на minik (${MINIK_HOST})"
    ${SSH} "cd ${REMOTE_NEWS_AGENT} && ${REMOTE_DC} ps"
}

do_up() {
    echo "🚀 Запуск news_agent на minik (без пересборки)..."
    ${SSH} "cd ${REMOTE_NEWS_AGENT} && ${REMOTE_DC} up -d"
    ${SSH} "cd ${REMOTE_NEWS_AGENT} && ${REMOTE_DC} ps"
}

do_down() {
    echo "🛑 Остановка news_agent на minik..."
    ${SSH} "cd ${REMOTE_NEWS_AGENT} && ${REMOTE_DC} down --remove-orphans"
    echo "✅ News Agent остановлен"
}

do_logs() {
    echo "📋 Логи news_agent на minik..."
    ${SSH} "cd ${REMOTE_NEWS_AGENT} && ${REMOTE_DC} logs -f --tail=100"
}

do_status() {
    echo "📊 Статус news_agent на minik..."
    ${SSH} "cd ${REMOTE_NEWS_AGENT} && ${REMOTE_DC} ps"
}

# ── Main ──────────────────────────────────────────────────────────────────

case "${ACTION}" in
    sync)
        sync_files
        generate_env
        ;;
    up)
        do_up
        ;;
    down)
        do_down
        ;;
    logs)
        do_logs
        ;;
    status)
        do_status
        ;;
    build)
        sync_files
        generate_env
        generate_override
        build_and_up
        ;;
    all)
        sync_files
        generate_env
        generate_override
        build_and_up
        ;;
    *)
        echo "Usage: $0 {all|sync|build|up|down|logs|status}"
        exit 1
        ;;
esac
