#!/usr/bin/env python3
"""
Скрипт для загрузки каналов Telegram в Redis
"""
import redis
import json

# Список каналов для мониторинга
CHANNELS = [
    "@cryptoninjas_tradings",
    "@fatpigsignals",
    "@FatPigSignalsgrams",
    "@BinanceKillers",
    "@BinanceKillersVip1",
    "@wolfoftrading",
    "@WolfofTrading_officials",
    "@wolfoftrading_Live",
    "@Signals_Wallet_Rocket",
    "@RocketWallet_Official",
    "@RocketWallet_signaIs",
    "@whalepumpgroup23",
    "@cryptowhalepumpsvip",
    "@Cryptowhalepump_official",
    "@dash2_trade",
    "@Dash2TradeOfficialsTG1",
    "@learn2tradenews",
    "@Learn2TradeOriginal1",
    "@onwardbtc_official",
    "@coincodecap",
    "@CoinCodeCap_Classic_Signals",
    "@Classic_Coincodecap",
    "@Coin_CodeCapClassic",
    "@BitcoinBullets",
    "@BitcoinBullets_TG",
    "@cryptosignals0rg",
    "@commas",
    "@wallstreetqueenofficial",
    "@Wallstreetqueenoffical_Live",
    "@wallstreetqueenofficalTG1",
    "@verifiedcryptonews",
    "@ravensignalspro",
    "@altsignals",
    "@cryptoclubpump",
    "@cryptoclubpumpsignal",
    "@universalcryptosignals",
    "@crypto_yoda_channel",
    "@dailyforex1",
    "@forexsignalstrialgroup",
    "@top_tradingsignals",
    "@anabelsignals",
    "@mathtradingcrypto",
    "@BobrovskiyTrade",
    "@fxkillerpubliclink",
    "@Crypto_god_Signals_MR1",
    "@RocketwalletsignalsTG",
    "@my_trd_56_bot"
]

def load_channels_to_redis():
    """Загружает каналы в Redis"""
    try:
        # Подключаемся к Redis
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

        # Проверяем подключение
        r.ping()
        print("✅ Подключение к Redis успешно")

        # Загружаем каналы в SET
        redis_key = "telegram:channels:usernames"
        r.delete(redis_key)  # Очищаем старые данные

        for channel in CHANNELS:
            r.sadd(redis_key, channel)
            print(f"📺 Добавлен канал: {channel}")

        # Сохраняем также в JSON формате
        json_key = "telegram:channels:usernames:json"
        r.set(json_key, json.dumps(CHANNELS))

        # Устанавливаем статус каналов как ACTIVE
        for channel in CHANNELS:
            status_key = f"telegram:channel:{channel}:status"
            r.set(status_key, "ACTIVE")
            print(f"✅ Статус канала {channel}: ACTIVE")

        print(f"\n🎉 Успешно загружено {len(CHANNELS)} каналов в Redis")
        print("📊 Ключи в Redis:")
        print(f"   - {redis_key} (SET)")
        print(f"   - {json_key} (JSON)")
        print("   - telegram:channel:*:status (STATUS)")

        return True

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

if __name__ == "__main__":
    success = load_channels_to_redis()
    exit(0 if success else 1)
