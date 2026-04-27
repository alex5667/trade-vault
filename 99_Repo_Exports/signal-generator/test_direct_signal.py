#!/usr/bin/env python3
"""
Тестовый скрипт для прямой отправки сигнала в go-gateway
Минует всю логику индикаторов и сразу отправляет сигнал
"""
import requests
import json
import sys
import time

GATEWAY_URL = "http://localhost:8090"

def send_test_signal(side="LONG"):
    """Отправить тестовый сигнал"""
    
    # Тестовый сигнал
    signal = {
        'sid': f'test-{int(time.time())}',
        'symbol': 'XAUUSD',
        'side': side,
        'lot': 0.01,
        'sl': 2760.00 if side == "LONG" else 2780.00,
        'tp_levels': [2765.00, 2770.00, 2775.00] if side == "LONG" else [2775.00, 2770.00, 2765.00]
    }
    
    print("="*60)
    print(f"🚀 Отправка тестового сигнала {side}")
    print("="*60)
    print(f"Gateway: {GATEWAY_URL}")
    print(f"Signal: {json.dumps(signal, indent=2)}")
    print("="*60)
    
    try:
        response = requests.post(
            f"{GATEWAY_URL}/orders/enqueue",
            json=signal,
            timeout=5
        )
        
        print(f"\n📊 Response:")
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Success: {json.dumps(result, indent=2)}")
            return True
        else:
            print(f"❌ Error: {response.text}")
            return False
            
    except requests.exceptions.ConnectionError:
        print(f"❌ Connection Error: Cannot connect to {GATEWAY_URL}")
        print(f"   Make sure go-gateway is running!")
        return False
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False


def test_health():
    """Проверка работы go-gateway"""
    print("\n🔍 Проверка подключения к go-gateway...")
    
    try:
        response = requests.get(f"{GATEWAY_URL}/healthz", timeout=3)
        if response.status_code == 200:
            print(f"✅ go-gateway доступен!")
            data = response.json()
            print(f"   Status: {data.get('status', 'unknown')}")
            return True
        else:
            print(f"⚠️ go-gateway ответил с кодом {response.status_code}")
            return False
    except:
        print(f"❌ go-gateway недоступен на {GATEWAY_URL}")
        print(f"   Проверьте: docker ps | grep go-gateway")
        return False


if __name__ == "__main__":
    print("\n" + "="*60)
    print("TEST SIGNAL SENDER - Прямая отправка сигнала")
    print("="*60 + "\n")
    
    # Проверка healthcheck
    if not test_health():
        print("\n⚠️ Невозможно продолжить - go-gateway недоступен")
        sys.exit(1)
    
    # Отправка сигнала
    side = sys.argv[1].upper() if len(sys.argv) > 1 else "LONG"
    
    if side not in ["LONG", "SHORT"]:
        print(f"❌ Неверное направление: {side}")
        print("   Использование: python test_direct_signal.py [LONG|SHORT]")
        sys.exit(1)
    
    success = send_test_signal(side)
    
    if success:
        print("\n" + "="*60)
        print("✅ Тестовый сигнал успешно отправлен!")
        print("="*60)
        print("\n📝 Проверьте:")
        print("   1. Логи go-gateway: docker logs scanner-go-gateway")
        print("   2. Telegram бот - сигнал должен появиться там")
        sys.exit(0)
    else:
        print("\n" + "="*60)
        print("❌ Ошибка отправки сигнала")
        print("="*60)
        sys.exit(1)

