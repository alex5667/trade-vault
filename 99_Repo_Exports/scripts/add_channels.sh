#!/bin/bash
# Добавляем каналы в Redis
REDIS_CONTAINER="scanner-redis-new"

# Список каналов
channels=(
  "ArbitrageScanner"
  "CyberTrade2024"
  "RocketwalletsignalsTG"
  "Signals_Wallet_Rocket" 
  "TradingSignalsFree007"
  "arbitrage_scanner_oficial"
  "binancesignalsgroup"
  "cryptowhalewatch"
  "future_signals_gold"
  "pumpingcrypto"
  "signalscrypto"
  "tradingsignalsview"
  "whale_tracker_signals"
)

echo "Добавление каналов в Redis..."
for channel in "${channels[@]}"; do
  echo "Добавляю канал: $channel"
  docker exec $REDIS_CONTAINER redis-cli sadd "telegram:channels:usernames" "$channel"
done

echo "Проверка добавленных каналов:"
docker exec $REDIS_CONTAINER redis-cli scard "telegram:channels:usernames"
docker exec $REDIS_CONTAINER redis-cli smembers "telegram:channels:usernames"
