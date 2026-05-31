"""
Клиент для работы с двумя Redis инстансами одновременно.
Публикует данные и в redis-worker-1 (порт 6380) и в redis-worker-2 (порт 6381).
"""
import concurrent.futures
import os
import random
import sys
import time

import redis
import contextlib

try:
    from prometheus_client import Counter as _Counter
    _SECONDARY_XADD_ERRORS = _Counter(
        "dual_redis_secondary_xadd_errors_total",
        "Secondary Redis XADD failures (fire-and-forget path)",
        ["stream"],
    )
except Exception:
    _SECONDARY_XADD_ERRORS = None  # type: ignore[assignment]

# Bounded pool for fire-and-forget secondary XADD — prevents unbounded thread spawn on burst.
# 8 workers cover typical burst (≤8 concurrent XADD inflight); extras queue, never block primary.
_SECONDARY_POOL = concurrent.futures.ThreadPoolExecutor(
    max_workers=int(os.environ.get("DUAL_REDIS_SECONDARY_POOL_SIZE", "8")),
    thread_name_prefix="dual_redis_secondary",
)

def get_env(key, default_value):
    """Получает значение переменной окружения или возвращает значение по умолчанию"""
    return os.environ.get(key, default_value)


class DualRedisClient:
    """Класс для работы с двумя Redis инстансами одновременно."""

    def __init__(self, retry_attempts=3, retry_delay=1):
        """
        Инициализирует подключения к двум Redis инстансам.
        
        Args:
            retry_attempts: Количество попыток подключения
            retry_delay: Задержка между попытками в секундах
        """
        # Подключение к redis-worker-1 (порт 6380)
        redis_host_1 = get_env("REDIS_SIGNALS_HOST", "redis-worker-1")
        redis_port_1 = int(get_env("REDIS_SIGNALS_PORT", "6379"))  # type: ignore

        # Подключение к redis-worker-2 (порт 6381)
        redis_host_2 = get_env("REDIS_SIGNALS_HOST_2", "redis-worker-2")
        redis_port_2 = int(get_env("REDIS_SIGNALS_PORT_2", "6379"))  # type: ignore

        import urllib.parse
        redis_url = get_env("REDIS_URL", "")
        url_user = ""
        url_pass = ""
        if redis_url.startswith("redis://"):  # type: ignore
            parsed = urllib.parse.urlparse(redis_url)
            if parsed.username:
                url_user = parsed.username
            if parsed.password:
                url_pass = parsed.password

        # Получаем учетные данные для DUAL_REDIS (redis-worker-1 / redis-worker-2)
        # Приоритет: credentials из REDIS_URL (если есть и user и pass),
        # иначе набор из переменных окружения.

        redis_user = url_user
        redis_pass = url_pass

        if not redis_user and not redis_pass:
            redis_user = get_env("REDIS_SIGNALS_USER",
                                 get_env("REDIS_USER",
                                         get_env("REDIS_WORKER_USERNAME",
                                                 get_env("GO_WORKER_REDIS_USER", ""))))

            redis_pass = get_env("REDIS_SIGNALS_PASS",
                                 get_env("REDIS_PASS",
                                         get_env("GO_WORKER_REDIS_PASS", "")))

        self.client_1 = self._create_client(redis_host_1, redis_port_1, "redis-worker-1", retry_attempts, retry_delay, redis_user, redis_pass)
        self.client_2 = self._create_client(redis_host_2, redis_port_2, "redis-worker-2", retry_attempts, retry_delay, redis_user, redis_pass)

    def _create_client(self, host, port, name, retry_attempts, retry_delay, username, password):
        """Создает подключение к Redis с повторными попытками."""
        for attempt in range(retry_attempts):
            try:
                client = redis.Redis(
                    host=host,
                    port=port,
                    db=0,
                    username=username if username else None,
                    password=password if password else None,
                    socket_timeout=10,           # was 120s — too long, blocks event loop on dead conn
                    socket_connect_timeout=5,
                    health_check_interval=30,
                    max_connections=50,          # was 100 — fewer per-instance conns
                    retry_on_error=[redis.exceptions.ConnectionError, redis.exceptions.TimeoutError],
                    socket_keepalive=True,
                    decode_responses=True
                )

                client.ping()
                print(f"✅ Подключение к {name} ({host}:{port}) успешно!")
                sys.stdout.flush()
                return client

            except Exception as e:
                if attempt < retry_attempts - 1:
                    print(f"⚠️ Ошибка подключения к {name} (попытка {attempt+1}/{retry_attempts}): {e}")
                    print(f"⏳ Повторная попытка через {retry_delay} сек...")
                    sys.stdout.flush()
                    time.sleep(retry_delay)
                else:
                    print(f"❌ Не удалось подключиться к {name} после {retry_attempts} попыток: {e}")
                    sys.stdout.flush()
                    return client

        return None

    def script_load(self, script):
        """Загружает Lua-скрипт в оба инстанса Redis."""
        sha1 = None
        sha2 = None
        if self.client_1:
            try:
                sha1 = self.client_1.script_load(script)
            except Exception as e:
                print(f"❌ DualRedis: script_load failed on client_1: {e}")
        if self.client_2:
            try:
                sha2 = self.client_2.script_load(script)
            except Exception as e:
                print(f"❌ DualRedis: script_load failed on client_2: {e}")
        return sha1 or sha2

    # Sentinel для определения "maxlen не передан явно"
    _DUAL_DEFAULT_MAXLEN: int = int(os.environ.get("DUAL_XADD_DEFAULT_MAXLEN", "10000"))
    # Логируем warning только один раз на поток — без импорта warnings ради минимальных зависимостей
    _warned_streams: set = set()

    def xadd(self, stream_name, fields, maxlen=None, approximate=True, **kwargs):
        """
        Добавляет сообщение в оба Redis стрима.

        Performance: primary XADD (worker-1) is SYNCHRONOUS for delivery guarantee.
        Secondary XADD (worker-2) is FIRE-AND-FORGET via background thread to avoid
        doubling signal emit latency (was causing P99=30ms instead of SLO=8ms).

        Args:
            stream_name: Имя стрима
            fields: Поля сообщения
            maxlen: Максимальная длина стрима.
                    Передавайте явно (например, maxlen=50_000).
                    Если не передан — берётся DUAL_XADD_DEFAULT_MAXLEN из ENV
                    (по умолчанию 10 000 = MAXLEN_PER_SYMBOL).
                    Дефолт 1000 удалён: он обрезал стрим до ~1.5 мин данных.
            approximate: Приблизительная очистка (~)

        Returns:
            message_id from primary Redis (worker-1); secondary is async.
        """
        if maxlen is None:
            maxlen = self._DUAL_DEFAULT_MAXLEN
            if stream_name not in self._warned_streams:
                self._warned_streams.add(stream_name)
                print(
                    f"⚠️  DualRedisClient.xadd: stream={stream_name!r} — maxlen не передан явно, "
                    f"используется DUAL_XADD_DEFAULT_MAXLEN={maxlen}. "
                    "Укажите maxlen= явно, чтобы избежать случайной обрезки стрима.",
                    flush=True,
                )

        message_id_1 = None

        # PRIMARY: Synchronous XADD to worker-1 (delivery guarantee)
        if self.client_1:
            try:
                message_id_1 = self.client_1.xadd(
                    stream_name,
                    fields,
                    maxlen=maxlen,
                    approximate=approximate,
                )
            except Exception as e:
                print(f"❌ Ошибка публикации в redis-worker-1: {e}")
                sys.stdout.flush()

        # SECONDARY: Fire-and-forget to worker-2 via bounded thread pool (no blocking).
        # _SECONDARY_POOL caps concurrency at DUAL_REDIS_SECONDARY_POOL_SIZE (default 8).
        if self.client_2:
            _client_2 = self.client_2
            _fields = fields
            _maxlen = maxlen
            _approximate = approximate
            _stream = stream_name

            def _secondary_xadd():
                try:
                    _client_2.xadd(_stream, _fields, maxlen=_maxlen, approximate=_approximate)
                except Exception as _sec_err:
                    if _SECONDARY_XADD_ERRORS is not None:
                        with contextlib.suppress(Exception):
                            _SECONDARY_XADD_ERRORS.labels(stream=_stream).inc()
                    # sample 1% to avoid log flood
                    if random.random() < 0.01:
                        print(f"⚠️ secondary xadd failed stream={_stream}: {_sec_err}", flush=True)

            _SECONDARY_POOL.submit(_secondary_xadd)

        return message_id_1, None  # caller receives primary ID; secondary is async

    def set(self, key, value, ex=None, **kwargs):
        """Устанавливает значение в оба Redis."""
        result_1 = None
        result_2 = None

        if self.client_1:
            try:
                result_1 = self.client_1.set(key, value, ex=ex, **kwargs)
            except Exception as e:
                print(f"❌ Ошибка SET в redis-worker-1: {e}")
                sys.stdout.flush()

        if self.client_2:
            try:
                result_2 = self.client_2.set(key, value, ex=ex, **kwargs)
            except Exception as e:
                print(f"❌ Ошибка SET в redis-worker-2: {e}")
                sys.stdout.flush()

        return result_1 or result_2

    def incr(self, key, amount=1):
        """
        Инкрементирует счётчик в Redis.

        Оба инстанса инкрементируются ровно по одному разу.
        Возвращает значение из первого доступного; при полном отказе — поднимает исключение.
        """
        result = None
        last_error = None

        if self.client_1:
            try:
                result = self.client_1.incr(key, amount)
            except Exception as exc:
                last_error = exc

        if self.client_2:
            try:
                res2 = self.client_2.incr(key, amount)
                if result is None:
                    result = res2
            except Exception as exc:
                if result is None:
                    last_error = exc

        if result is not None:
            return result

        if last_error:
            raise last_error

        return None

    def ping(self):
        """Проверяет подключение к обоим Redis."""
        result_1 = False
        result_2 = False

        if self.client_1:
            with contextlib.suppress(Exception):
                result_1 = self.client_1.ping()

        if self.client_2:
            with contextlib.suppress(Exception):
                result_2 = self.client_2.ping()

        return result_1 or result_2

    def eval(self, script, numkeys, *args):
        """Выполняет Lua скрипт в обоих Redis."""
        res1 = None
        res2 = None
        last_error = None

        if self.client_1:
            try:
                res1 = self.client_1.eval(script, numkeys, *args)
            except Exception as e:
                print(f"❌ DualRedis: eval failed on client_1: {e}")
                last_error = e

        if self.client_2:
            try:
                res2 = self.client_2.eval(script, numkeys, *args)
            except Exception as e:
                print(f"❌ DualRedis: eval failed on client_2: {e}")
                last_error = e if res1 is None else last_error

        if last_error and res1 is None and res2 is None:
            raise last_error

        return res1 or res2

    def evalsha(self, sha, numkeys, *args):
        """Выполняет Lua скрипт (по SHA) в обоих Redis."""
        res1 = None
        res2 = None
        last_error = None

        if self.client_1:
            try:
                res1 = self.client_1.evalsha(sha, numkeys, *args)
            except redis.exceptions.NoScriptError:
                # Если скрипта нет хоть на одном - нужно чтобы выше поймали и сделали EVAL
                print("⚠️ DualRedis: NOSCRIPT on client_1, raising to trigger fallback")
                raise
            except Exception as e:
                print(f"❌ DualRedis: evalsha failed on client_1: {e}")
                last_error = e

        if self.client_2:
            try:
                res2 = self.client_2.evalsha(sha, numkeys, *args)
            except redis.exceptions.NoScriptError:
                print("⚠️ DualRedis: NOSCRIPT on client_2, raising to trigger fallback")
                raise
            except Exception as e:
                print(f"❌ DualRedis: evalsha failed on client_2: {e}")
                last_error = e if res1 is None else last_error

        if last_error and res1 is None and res2 is None:
            raise last_error

        return res1 or res2

    def get(self, key):
        """Получает значение из первого доступного Redis."""
        if self.client_1:
            try:
                return self.client_1.get(key)
            except Exception:
                pass

        if self.client_2:
            try:
                return self.client_2.get(key)
            except Exception:
                pass

        return None


def get_dual_signals_redis(retry_attempts=3, retry_delay=1):
    """
    Создает и возвращает подключение к двум Redis для сигналов.
    
    Returns:
        DualRedisClient: Клиент для работы с двумя Redis одновременно
    """
    return DualRedisClient(retry_attempts, retry_delay)
