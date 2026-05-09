from __future__ import annotations


def is_transient_error(e: BaseException) -> bool:
    """
    Единый источник истины по transient-классификации.
    Идея: максимум "fail-open" для сетевых/redis временных проблем,
    минимум ложных poison/DLQ из-за временной деградации.
    """
    # --- Типы исключений (первичный сигнал) ---
    try:
        from redis.exceptions import (
            BusyLoadingError,
            ClusterDownError,
            MasterDownError,
            ReadOnlyError,
            TryAgainError,
        )
        from redis.exceptions import (
            ConnectionError as RedisConnectionError,
        )
        from redis.exceptions import (
            TimeoutError as RedisTimeoutError,
        )

        if isinstance(
            e,
            (
                RedisConnectionError,
                RedisTimeoutError,
                BusyLoadingError,
                TryAgainError,
                ClusterDownError,
                MasterDownError,
                ReadOnlyError,
            )
        ):
            return True
    except Exception:
        pass

    # Сетевые/OS transient
    if isinstance(
        e,
        (
            TimeoutError,
            ConnectionError,
            BrokenPipeError,
            ConnectionResetError,
            ConnectionAbortedError,
            OSError,
        )
    ):
        # OSError слишком широкий; дальше фильтруем по errno/сообщению
        pass

    # --- errno фильтр для OSError ---
    try:
        import errno as _errno

        if isinstance(e, OSError):
            if getattr(e, "errno", None) in (
                _errno.ECONNRESET,
                _errno.ECONNABORTED,
                _errno.EPIPE,
                _errno.ETIMEDOUT,
                _errno.EHOSTUNREACH,
                _errno.ENETUNREACH,
                _errno.ENETDOWN,
                _errno.ECONNREFUSED,
                _errno.EAGAIN,
                _errno.EWOULDBLOCK,
            ):
                return True
    except Exception:
        pass

    # --- Fallback по тексту (последний уровень) ---
    msg = (str(e) or "").lower()
    tokens = (
        "timeout",
        "timed out",
        "read timeout",
        "write timeout",
        "connection",
        "connection reset",
        "reset by peer",
        "broken pipe",
        "temporarily",
        "try again",
        "busy loading",
        "loading the dataset",
        "readonly",
        "master is down",
        "cluster down",
        "eof",
        "server closed the connection",
        "i/o error",
    )
    return any(t in msg for t in tokens)
