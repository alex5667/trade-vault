#!/bin/bash
#####################################################################
# 🎯 SENIOR DEV: Enterprise Channel Loader
# Purpose: Load ALL Telegram channels to scanner-redis-worker-1
# Author: Senior Dev Team
# Date: 2025-10-24
#####################################################################

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
REDIS_CONTAINER="redis-worker-1"
REDIS_KEY="telegram:channels:usernames"

# 🎯 COMPLETE channel list (56 channels including ALL CoinCodeCap & Learn2Trade)
CHANNELS=(
    # FatPig signals
    "@cryptoninjas_tradings"
    "@fatpigsignals"
    "@FatPigSignalsgrams"
    
    # Binance Killers
    "@BinanceKillers"
    "@BinanceKillersVip1"
    
    # Wolf signals
    "@wolfoftrading"
    "@WolfofTrading_officials"
    "@wolfoftrading_Live"
    
    # Rocket Wallet
    "@Signals_Wallet_Rocket"
    "@RocketWallet_Official"
    "@RocketWallet_signaIs"
    "@RocketwalletsignalsTG"
    
    # Whale Pump
    "@whalepumpgroup23"
    "@cryptowhalepumpsvip"
    "@Cryptowhalepump_official"
    
    # Dash2Trade
    "@dash2_trade"
    "@Dash2TradeOfficialsTG1"
    
    # 🎯 Learn2Trade (CRITICAL - was missing!)
    "@learn2tradenews"
    "@Learn2TradeOriginal1"
    
    # Other signals
    "@onwardbtc_official"
    
    # 🎯 CoinCodeCap (ALL variants)
    "@coincodecap"
    "@CoinCodeCap_Classic_Signals"
    "@Classic_Coincodecap"
    "@Coin_CodeCapClassic"
    
    # Bitcoin Bullets
    "@BitcoinBullets"
    "@BitcoinBullets_TG"
    
    # Other providers
    "@cryptosignals0rg"
    "@commas"
    "@wallstreetqueenofficial"
    "@Wallstreetqueenoffical_Live"
    "@wallstreetqueenofficialTG1"
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
    "@my_trd_56_bot"
)

echo -e "${BLUE}================================================================================${NC}"
echo -e "${BLUE}🎯 SENIOR DEV: Loading Telegram Channels to Redis${NC}"
echo -e "${BLUE}================================================================================${NC}"
echo ""

# Check if Redis container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${REDIS_CONTAINER}$"; then
    echo -e "${RED}❌ Redis container '${REDIS_CONTAINER}' is not running!${NC}"
    echo -e "${YELLOW}Available Redis containers:${NC}"
    docker ps --format '{{.Names}}' | grep redis
    exit 1
fi

echo -e "${GREEN}✅ Redis container found: ${REDIS_CONTAINER}${NC}"
echo ""

# Test Redis connection
if ! docker exec ${REDIS_CONTAINER} redis-cli PING > /dev/null 2>&1; then
    echo -e "${RED}❌ Cannot connect to Redis in container ${REDIS_CONTAINER}${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Redis connection OK${NC}"
echo ""

# Clear old data
echo -e "${YELLOW}🧹 Clearing old channel data...${NC}"
docker exec ${REDIS_CONTAINER} redis-cli DEL ${REDIS_KEY} > /dev/null

# Load channels with progress bar
echo -e "${BLUE}📝 Loading ${#CHANNELS[@]} channels...${NC}"
echo ""

COUNTER=0
for channel in "${CHANNELS[@]}"; do
    COUNTER=$((COUNTER + 1))
    docker exec ${REDIS_CONTAINER} redis-cli SADD ${REDIS_KEY} "$channel" > /dev/null
    
    # Progress indicator
    if [ $((COUNTER % 10)) -eq 0 ]; then
        echo -e "${GREEN}   ... loaded ${COUNTER}/${#CHANNELS[@]} channels (${COUNTER}00%)${NC}"
    fi
    
    # Set channel status to ACTIVE
    docker exec ${REDIS_CONTAINER} redis-cli SET "telegram:channel:${channel}:status" "ACTIVE" > /dev/null
done

echo ""
echo -e "${GREEN}✅ All ${#CHANNELS[@]} channels loaded!${NC}"
echo ""

# Verification
LOADED_COUNT=$(docker exec ${REDIS_CONTAINER} redis-cli SCARD ${REDIS_KEY})
echo -e "${BLUE}================================================================================${NC}"
echo -e "${BLUE}📊 VERIFICATION${NC}"
echo -e "${BLUE}================================================================================${NC}"
echo -e "  Expected channels: ${#CHANNELS[@]}"
echo -e "  Loaded channels:   ${LOADED_COUNT}"

if [ "$LOADED_COUNT" -eq "${#CHANNELS[@]}" ]; then
    echo -e "${GREEN}  Status: ✅ SUCCESS (100%)${NC}"
else
    echo -e "${YELLOW}  Status: ⚠️  WARNING (${LOADED_COUNT}/${#CHANNELS[@]})${NC}"
fi

echo ""

# Show key channels
echo -e "${BLUE}🔍 Key channels verification:${NC}"
for key_channel in "@Learn2TradeOriginal1" "@learn2tradenews" "@CoinCodeCap_Classic_Signals"; do
    if docker exec ${REDIS_CONTAINER} redis-cli SISMEMBER ${REDIS_KEY} "$key_channel" | grep -q "1"; then
        echo -e "  ${GREEN}✅ ${key_channel}${NC}"
    else
        echo -e "  ${RED}❌ ${key_channel}${NC}"
    fi
done

echo ""
echo -e "${BLUE}================================================================================${NC}"
echo -e "${GREEN}🎉 Channel loading completed!${NC}"
echo -e "${BLUE}================================================================================${NC}"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo -e "  1. Restart telegram-worker: ${GREEN}docker restart scanner-telegram-worker${NC}"
echo -e "  2. Monitor logs: ${GREEN}docker logs -f scanner-telegram-worker${NC}"
echo -e "  3. Expected result: ${GREEN}42 out of 42 active channels${NC}"
echo ""

