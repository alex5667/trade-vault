#!/usr/bin/env python3
"""
Простой тест XAU OrderFlow Handler - проверяем только основные функции
"""

import redis
import time
from collections import deque

# Простая конфигурация
REDIS_HOST = "localhost"
REDIS_PORT = 6379
TICK_STREAM = "stream:tick_XAUUSD"
NOTIFY_STREAM = "notify:telegram"

class SimpleXAUTester:
    """Упрощённый тестер XAU handler"""

    def __init__(self):
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self.delta_window = deque(maxlen=120)
        self.processed_ticks = 0

    def test_redis_connection(self):
        """Тест подключения к Redis"""
        try:
            result = self.redis_client.ping()
            print(f"✅ Redis connection: {result}")
            return True
        except Exception as e:
            print(f"❌ Redis connection failed: {e}")
            return False

    def test_tick_stream(self):
        """Тест чтения тиков из stream"""
        try:
            # Проверяем длину stream
            length = self.redis_client.xlen(TICK_STREAM)
            print(f"✅ Tick stream length: {length}")

            if length > 0:
                # Читаем последние 5 тиков
                ticks = self.redis_client.xrevrange(TICK_STREAM, count=5)
                print("✅ Last 5 ticks:")
                for tick_id, fields in ticks:
                    print(f"   {tick_id}: {fields}")
                return True
            else:
                print("⚠️ No ticks found in stream")
                return False
        except Exception as e:
            print(f"❌ Tick stream test failed: {e}")
            return False

    def create_consumer_group(self):
        """Создаём consumer group"""
        try:
            self.redis_client.xgroup_create(TICK_STREAM, "test-group", id='$', mkstream=True)
            print("✅ Consumer group created")
            return True
        except Exception as e:
            if "BUSYGROUP" in str(e):
                print("ℹ️ Consumer group already exists")
                return True
            else:
                print(f"❌ Failed to create consumer group: {e}")
                return False

    def test_consume_ticks(self, duration=10):
        """Тест потребления тиков"""
        print(f"🔄 Testing tick consumption for {duration} seconds...")

        consumer_name = f"test-consumer-{int(time.time())}"
        start_time = time.time()
        tick_count = 0

        try:
            while time.time() - start_time < duration:
                messages = self.redis_client.xreadgroup(
                    "test-group",
                    consumer_name,
                    {TICK_STREAM: '>'},
                    count=10,
                    block=1000
                )

                if messages:
                    for stream, items in messages:  # noqa: B007
                        for msg_id, fields in items:
                            tick_count += 1
                            if tick_count % 10 == 0:
                                print(f"   Processed {tick_count} ticks...")

                            # Простая обработка
                            try:
                                bid = float(fields.get('bid', 0))
                                ask = float(fields.get('ask', 0))
                                mid = (bid + ask) / 2 if bid and ask else 0

                                # Простое delta calculation
                                delta = 1.0 if bid > ask * 0.999 else -1.0
                                self.delta_window.append(delta)

                                # Проверяем на простой сигнал
                                if len(self.delta_window) >= 60:
                                    self._check_simple_signal(mid)

                            except Exception as e:
                                print(f"⚠️ Error processing tick {msg_id}: {e}")

                            # ACK сообщение
                            try:
                                self.redis_client.xack(TICK_STREAM, "test-group", msg_id)
                            except Exception as e:
                                print(f"⚠️ ACK error: {e}")

            print(f"✅ Consumed {tick_count} ticks in {duration} seconds")
            return tick_count > 0

        except Exception as e:
            print(f"❌ Tick consumption failed: {e}")
            return False

    def _check_simple_signal(self, price):
        """Простая проверка сигнала"""
        if len(self.delta_window) < 60:
            return

        # Простая логика: если последние 20 тиков все положительные
        recent = list(self.delta_window)[-20:]
        if all(d > 0 for d in recent):
            self._send_test_signal("LONG", price, "Simple bullish momentum")
        elif all(d < 0 for d in recent):
            self._send_test_signal("SHORT", price, "Simple bearish momentum")

    def _send_test_signal(self, side, price, reason):
        """Отправляем тестовый сигнал"""
        try:
            signal = {
                "text": f"🧪 TEST SIGNAL: XAUUSD {side} @ {price:.2f}. {reason}",
                "side": side,
                "price": f"{price:.2f}",
                "note": f"Test signal: {reason}",
                "test": "true"
            }

            self.redis_client.xadd(NOTIFY_STREAM, signal, maxlen=100)
            print(f"📤 Test signal sent: {signal['text']}")

        except Exception as e:
            print(f"❌ Failed to send test signal: {e}")

    def test_signal_publishing(self):
        """Тест публикации сигналов"""
        try:
            test_signal = {
                "text": "🧪 Test message from XAU tester",
                "test": "true",
                "timestamp": str(int(time.time()))
            }

            result = self.redis_client.xadd(NOTIFY_STREAM, test_signal, maxlen=100)
            print(f"✅ Test signal published: {result}")

            # Проверяем что сигнал дошёл
            signals = self.redis_client.xrevrange(NOTIFY_STREAM, count=1)
            if signals:
                latest_id, latest_data = signals[0]
                if latest_data.get("test") == "true":
                    print(f"✅ Test signal confirmed: {latest_data}")
                    return True

            return False
        except Exception as e:
            print(f"❌ Signal publishing test failed: {e}")
            return False

    def run_all_tests(self):
        """Запуск всех тестов"""
        print("=" * 60)
        print("🧪 XAU OrderFlow Handler - Simple Test")
        print("=" * 60)

        tests = [
            ("Redis Connection", self.test_redis_connection),
            ("Tick Stream", self.test_tick_stream),
            ("Consumer Group", self.create_consumer_group),
            ("Signal Publishing", self.test_signal_publishing),
        ]

        passed = 0
        for name, test_func in tests:
            print(f"\n🔍 Testing {name}...")
            if test_func():
                passed += 1

        print(f"\n📊 Test Results: {passed}/{len(tests)} passed")

        if passed == len(tests):
            print("✅ All basic tests passed! Testing tick consumption...")
            return self.test_consume_ticks(30)  # 30 секунд потребления
        else:
            print("❌ Some basic tests failed")
            return False

def main():
    """Главная функция"""
    tester = SimpleXAUTester()
    success = tester.run_all_tests()

    if success:
        print("\n✅ XAU Handler test completed successfully!")
        print("💡 The system can process ticks and generate signals")
    else:
        print("\n❌ XAU Handler test failed!")
        print("💡 Check Redis connection and tick stream")

    return 0 if success else 1

if __name__ == "__main__":
    exit(main())
