#!/bin/bash
# Скрипт для загрузки каналов Telegram в Redis через docker exec

echo "📺 Загрузка каналов Telegram в Redis..."

# Список каналов
CHANNELS=(
    "@cryptoninjas_tradings"
    "@fatpigsignals"
    "@FatPigSignalsgrams"
    "@BinanceKillers"
    "@BinanceKillersVip1"
    "@wolfoftrading"
    "@WolfofTrading_officials"
    "@wolfoftrading_Live"
    "@Signals_Wallet_Rocket"
    "@RocketWallet_Official"
    "@RocketWallet_signaIs"
    "@whalepumpgroup23"
    "@cryptowhalepumpsvip"
    "@Cryptowhalepump_official"
    "@dash2_trade"
    "@Dash2TradeOfficialsTG1"
    "@learn2tradenews"
    "@Learn2TradeOriginal1"
    "@onwardbtc_official"
    "@coincodecap"
    "@CoinCodeCap_Classic_Signals"
    "@Classic_Coincodecap"
    "@Coin_CodeCapClassic"
    "@BitcoinBullets"
    "@BitcoinBullets_TG"
    "@cryptosignals0rg"
    "@commas"
    "@wallstreetqueenofficial"
    "@Wallstreetqueenoffical_Live"
    "@wallstreetqueenofficalTG1"
    "@verifiedcryptonews"
    "@ravensignalspro"
    "@altsignals"
    "@cryptoclubpump"
    "@cryptoclubpumpsignal"
    "@universalcryptosignals"
    "@crypto_yoda_channel"
    "@dailyforex1"
    "@forexsignalstrialgroup"
    "@top_tradingsignals"
    "@anabelsignals"
    "@mathtradingcrypto"
    "@BobrovskiyTrade"
    "@fxkillerpubliclink"
    "@Crypto_god_Signals_MR1"
    "@RocketwalletsignalsTG"
    "@my_trd_56_bot"
)

# Очищаем старые данные
echo "🧹 Очистка старых данных..."
docker exec scanner-redis redis-cli -p 6379 DEL telegram:channels:usernames
docker exec scanner-redis redis-cli -p 6379 DEL telegram:channels:usernames:json

# Добавляем каналы в SET
echo "📝 Добавление каналов в SET..."
for channel in "${CHANNELS[@]}"; do
    docker exec scanner-redis redis-cli -p 6379 SADD telegram:channels:usernames "$channel"
    echo "  ✅ $channel"
done

# Создаем JSON массив
JSON_ARRAY="["
for i in "${!CHANNELS[@]}"; do
    if [ $i -eq 0 ]; then
        JSON_ARRAY+="\"${CHANNELS[$i]}\""
    else
        JSON_ARRAY+=",\"${CHANNELS[$i]}\""
    fi
done
JSON_ARRAY+="]"

# Сохраняем JSON
echo "💾 Сохранение JSON..."
docker exec scanner-redis redis-cli -p 6379 SET telegram:channels:usernames:json "$JSON_ARRAY"

# Устанавливаем статусы каналов
echo "🔄 Установка статусов каналов..."
for channel in "${CHANNELS[@]}"; do
    docker exec scanner-redis redis-cli -p 6379 SET "telegram:channel:${channel}:status" "ACTIVE" > /dev/null
done

echo ""
echo "🎉 Загрузка завершена!"
echo ""
echo "📊 Статистика:"
echo "   Всего каналов: ${#CHANNELS[@]}"
echo ""
echo "🔍 Проверка:"
docker exec scanner-redis redis-cli -p 6379 SCARD telegram:channels:usernames
echo "   каналов в SET"
echo ""

# Показываем первые 10 каналов
echo "📺 Первые 10 каналов:"
docker exec scanner-redis redis-cli -p 6379 SMEMBERS telegram:channels:usernames | head -10

