import time
import sys
from .redis_client import get_redis


def wait_for_redis() -> bool:
    """
    Проверяет доступность Redis с повторными попытками подключения
    
    Returns:
        bool: True если подключение установлено, False если не удалось подключиться
    """
    r = get_redis()
    max_attempts = 10
    
    for i in range(max_attempts):
        try:
            r.ping()
            print("✅ Подключение к Redis установлено")
            sys.stdout.flush()
            return True
        except Exception as e:
            print(f"⚠️ Ожидание Redis (попытка {i+1}/{max_attempts}): {e}")
            sys.stdout.flush()
            time.sleep(2)
    
    print("✅ Продолжение работы...")
    sys.stdout.flush()
    return False 