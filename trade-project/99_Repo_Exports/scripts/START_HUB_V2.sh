#!/bin/bash
# START_HUB_V2.sh - Запуск AggregatedSignalHubV2

set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Aggregated Signal Hub V2 Launcher${NC}"
echo -e "${BLUE}========================================${NC}"

# Директория проекта
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

# Конфигурация по умолчанию
SYMBOL="${SYMBOL:-XAUUSD}"
REDIS_URL="${REDIS_URL:-redis://scanner-redis-worker-1:6379/0}"
TICK_STREAM="${TICK_STREAM:-ticks:${SYMBOL}}"
PRINTS_STREAM="${PRINTS_STREAM:-prints:${SYMBOL}}"
MODE="${MODE:-live}"

# Пороги и веса
HUB_CONFIDENCE_THR="${HUB_CONFIDENCE_THR:-0.62}"
HUB_MIN_SIG_INT_SEC="${HUB_MIN_SIG_INT_SEC:-180}"
W_DELTA_PRO="${W_DELTA_PRO:-0.50}"
W_SPEED="${W_SPEED:-0.15}"
W_CLUSTER="${W_CLUSTER:-0.25}"
W_LEGACY="${W_LEGACY:-0.10}"

# Writer config
MIN_CONF="${MIN_CONF:-60.0}"
HUB_COOLDOWN="${HUB_COOLDOWN:-300}"
RISK_PCT="${RISK_PCT:-1.0}"
SL_MULT="${SL_MULT:-1.5}"
TP_MULTS="${TP_MULTS:-2.0,3.0,4.0}"

# Parquet sink (опционально)
PARQUET_LABELS_DIR="${PARQUET_LABELS_DIR:-}"

# Вывод конфигурации
echo -e "${GREEN}Configuration:${NC}"
echo -e "  Symbol: ${YELLOW}${SYMBOL}${NC}"
echo -e "  Redis: ${YELLOW}${REDIS_URL}${NC}"
echo -e "  Mode: ${YELLOW}${MODE}${NC}"
echo -e "  Tick stream: ${YELLOW}${TICK_STREAM}${NC}"
echo -e "  Prints stream: ${YELLOW}${PRINTS_STREAM}${NC}"
echo ""
echo -e "${GREEN}Thresholds:${NC}"
echo -e "  Confidence: ${YELLOW}${HUB_CONFIDENCE_THR}${NC}"
echo -e "  Min interval: ${YELLOW}${HUB_MIN_SIG_INT_SEC}s${NC}"
echo -e "  Cooldown: ${YELLOW}${HUB_COOLDOWN}s${NC}"
echo ""
echo -e "${GREEN}Weights:${NC}"
echo -e "  Delta Pro: ${YELLOW}${W_DELTA_PRO}${NC}"
echo -e "  Speed: ${YELLOW}${W_SPEED}${NC}"
echo -e "  Cluster: ${YELLOW}${W_CLUSTER}${NC}"
echo -e "  Legacy: ${YELLOW}${W_LEGACY}${NC}"
echo ""

# Проверка зависимостей
echo -e "${BLUE}Checking dependencies...${NC}"

if ! command -v python3 &> /dev/null; then
    echo -e "${RED}ERROR: python3 not found${NC}"
    exit 1
fi

# Переход в директорию python-worker
cd python-worker

# Проверка наличия файла
if [ ! -f "aggregated_signal_hub_v2.py" ]; then
    echo -e "${RED}ERROR: aggregated_signal_hub_v2.py not found${NC}"
    exit 1
fi

# Экспорт переменных окружения
export SYMBOL
export REDIS_URL
export TICK_STREAM
export PRINTS_STREAM
export HUB_CONFIDENCE_THR
export HUB_MIN_SIG_INT_SEC
export W_DELTA_PRO
export W_SPEED
export W_CLUSTER
export W_LEGACY
export MIN_CONF
export HUB_COOLDOWN
export RISK_PCT
export SL_MULT
export TP_MULTS
export PARQUET_LABELS_DIR

echo -e "${GREEN}Starting Hub V2...${NC}"
echo ""

# Запуск
if [ "$MODE" = "replay" ]; then
    if [ -z "$REPLAY_CSV" ]; then
        echo -e "${RED}ERROR: REPLAY_CSV not set for replay mode${NC}"
        exit 1
    fi
    REPLAY_SPEED="${REPLAY_SPEED:-0.0}"
    echo -e "${YELLOW}Replay mode: ${REPLAY_CSV} (speed=${REPLAY_SPEED})${NC}"
    python3 aggregated_signal_hub_v2.py --mode=replay --replay-csv="${REPLAY_CSV}" --replay-speed="${REPLAY_SPEED}"
else
    echo -e "${YELLOW}Live mode: reading from Redis streams${NC}"
    python3 aggregated_signal_hub_v2.py --mode=live --symbol="${SYMBOL}"
fi

