#!/bin/bash
# Мониторинг событий Telegram в реальном времени

echo "================================================"
echo "МОНИТОРИНГ СОБЫТИЙ TELEGRAM WORKER"
echo "================================================"
echo ""
echo "Подписанные каналы:"
docker exec scanner-redis redis-cli SMEMBERS telegram:channels:usernames | grep -i rocket
echo ""
echo "Статус @RocketwalletsignalsTG:"
docker exec scanner-redis redis-cli GET "telegram:channel:@RocketwalletsignalsTG:status"
echo ""
echo "================================================"
echo "ЛОГИ В РЕАЛЬНОМ ВРЕМЕНИ (Ctrl+C для выхода)"
echo "================================================"
echo ""

docker logs scanner-telegram-worker --follow 2>&1 | grep --line-buffered -E "(🔔|📨|СОБЫТИЕ|СООБЩЕНИЕ|Rocket|ПРИНУДИТЕЛЬНАЯ|Catch up)"

