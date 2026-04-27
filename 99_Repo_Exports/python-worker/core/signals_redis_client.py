"""
Redis клиент для публикации сигналов на порт 6380 (redis-worker-1).
Используется для записи всех обработанных сигналов в Redis Streams.
"""
import os
import redis  # type: ignore
import time
import sys

def get_env(key, default_value):
    """
    Получает значение переменной окружения или возвращает значение по умолчанию
    
    Args:
        key (str): Имя переменной окружения
        default_value (str): Значение по умолчанию
        
    Returns:
        str: Значение переменной окружения или default_value
    """
    return os.environ.get(key, default_value)

def get_signals_redis(retry_attempts=3, retry_delay=1) -> redis.Redis:
    """
    Создает и возвращает подключение к Redis для сигналов (порт 6380).
    
    Args:
        retry_attempts (int): Количество попыток подключения
        retry_delay (int): Задержка между попытками в секундах
    
    Returns:
        redis.Redis: объект клиента Redis с настроенным подключением к порту 6380
    
    Подключение настроено для автоматической декодировки ответов Redis из байтов в строки,
    что упрощает работу с данными на стороне Python.
    """
    # Получаем хост и порт Redis для сигналов из переменных окружения
    # По умолчанию используем redis-worker-1:6379 (внутренний порт контейнера)
    redis_host = get_env("REDIS_SIGNALS_HOST", "redis-worker-1")
    redis_port = int(get_env("REDIS_SIGNALS_PORT", "6379"))
    
    # Попытки подключения с повторами при ошибках
    for attempt in range(retry_attempts):
        try:
            # Создаем клиент Redis с более надежными настройками соединения
            client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=0,
                socket_timeout=120,  # УВЕЛИЧЕНО для больших нагрузок
                socket_connect_timeout=10,
                health_check_interval=30,
                max_connections=100,  # МАКСИМАЛЬНЫЙ пул соединений
                retry_on_error=[redis.exceptions.ConnectionError, redis.exceptions.TimeoutError],
                socket_keepalive=True,
                decode_responses=True  # автоматически декодировать ответы в строки
            )
            
            # Проверяем подключение простой командой
            client.ping()
            
            # Если успешно, возвращаем клиент
            print(f"✅ Redis клиент для сигналов подключен к {redis_host}:{redis_port}")
            sys.stdout.flush()
            return client
        
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            # Если это не последняя попытка, пробуем снова
            if attempt < retry_attempts - 1:
                print(f"⚠️ Ошибка подключения к Redis для сигналов (попытка {attempt+1}/{retry_attempts}): {e}")
                print(f"⏳ Повторная попытка через {retry_delay} сек...")
                sys.stdout.flush()
                time.sleep(retry_delay)
            else:
                print(f"❌ Не удалось подключиться к Redis для сигналов после {retry_attempts} попыток: {e}")
                sys.stdout.flush()
                # Возвращаем клиент даже если ping не удался, чтобы код мог продолжить работу
                # В этом случае другие компоненты будут обрабатывать ошибки соединения
                return client
        
        except Exception as e:
            print(f"❌ Неожиданная ошибка при подключении к Redis для сигналов: {e}")
            sys.stdout.flush()
            # Возвращаем клиент, работоспособность которого не проверена
            return client
