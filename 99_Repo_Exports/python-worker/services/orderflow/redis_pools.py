"""
RedisPoolSet — создаёт и держит все Redis-клиенты CryptoOrderflowService.

Раньше ~200 строк get_async_redis_client()-вызовов жили в __init__.
Теперь вся логика пулов в одном месте: размеры, таймауты, URL-резолвинг,
warn-лог при нехватке соединений.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass

import redis.asyncio as aioredis

from services.orderflow.service_config import RedisPoolCfg

logger = logging.getLogger("redis_pools")


def _make_pool(
    url: str,
    max_connections: int,
    sock_to: float,
    conn_to: float,
    hc_interval: int = 30,
) -> aioredis.Redis:
    from core.redis_client import get_async_redis_client  # локальный импорт чтобы не тянуть при тестах
    return get_async_redis_client(
        url=url,
        max_connections=max_connections,
        socket_timeout=sock_to,
        socket_connect_timeout=conn_to,
        decode_responses=True,
        health_check_interval=hc_interval,
    )


@dataclass
class RedisPoolSet:
    """Набор Redis-клиентов для одного инстанса CryptoOrderflowService.

    Атрибуты:
        main            — hot-path: сигналы, конфиг-чтение (main Redis)
        ticks           — tick/book streams (redis-ticks)
        config          — OrderFlowConfigLoader (изолирован от hot-path)
        notify          — Telegram / notify stream (может совпадать с main)
        health          — HealthMetrics background loop
        health_contract — SLO-flush loop (hc=0, отдельный пул)
        ml_gate         — ML gate background refresh
        publish         — Dedicated pool for AsyncSignalPublisher to prevent XADD latency spikes
    """
    main: aioredis.Redis
    ticks: aioredis.Redis
    config: aioredis.Redis
    notify: aioredis.Redis
    health_contract: aioredis.Redis
    ml_gate: aioredis.Redis
    publish: aioredis.Redis
    state: aioredis.Redis

    @classmethod
    def build(cls, cfg: RedisPoolCfg, redis_dsn: str, ticks_dsn: str) -> "RedisPoolSet":
        """Создаёт все пулы из конфига. Единственная точка входа."""

        def pool(url: str, max_conn: int, hc: int = 0) -> aioredis.Redis:
            return _make_pool(
                url=url,
                max_connections=max_conn,
                sock_to=cfg.sock_to,
                conn_to=cfg.conn_to,
                hc_interval=hc,
            )

        main = pool(redis_dsn, cfg.main_max, hc=cfg.hc_interval)
        ticks = pool(ticks_dsn, cfg.ticks_max, hc=cfg.hc_interval)

        config_url = os.getenv("ORDERFLOW_CONFIG_REDIS_URL", redis_dsn)
        config_client = _make_pool(
            url=config_url,
            max_connections=cfg.config_max,
            sock_to=cfg.resolved_config_sock_to(),
            conn_to=cfg.conn_to,
            hc_interval=cfg.hc_interval,
        )

        notify_client = cls._resolve_notify(main, redis_dsn, cfg)

        health_contract = _make_pool(
            url=redis_dsn,
            max_connections=cfg.health_contract_max,
            sock_to=5.0,
            conn_to=5.0,
            hc_interval=0,
        )

        ml_gate_url = os.getenv("ML_GATE_REDIS_URL", redis_dsn)
        ml_gate = pool(ml_gate_url, cfg.ml_gate_max, hc=cfg.hc_interval)

        publish_url = os.getenv("PUBLISH_REDIS_URL", redis_dsn)
        publish = pool(publish_url, cfg.publish_max, hc=cfg.hc_interval)

        state_url = os.getenv("REDIS_STATE_URL", redis_dsn)
        state = pool(state_url, cfg.state_max, hc=cfg.hc_interval)

        inst = cls(
            main=main,
            ticks=ticks,
            config=config_client,
            notify=notify_client,
            health_contract=health_contract,
            ml_gate=ml_gate,
            publish=publish,
            state=state,
        )
        inst._log_pool_info(cfg, redis_dsn, ticks_dsn, config_url, ml_gate_url, state_url)
        inst._warn_if_pool_too_small(cfg)
        return inst

    @staticmethod
    def _resolve_notify(
        main: aioredis.Redis,
        redis_dsn: str,
        cfg: RedisPoolCfg,
    ) -> aioredis.Redis:
        from core.redis_client import normalize_redis_url, get_async_redis_client
        notify_url_raw = os.getenv("CRYPTO_NOTIFY_REDIS_URL", os.getenv("REDIS_URL"))
        if notify_url_raw and normalize_redis_url(notify_url_raw) != normalize_redis_url(redis_dsn):
            logger.info("🔗 Separate notify Redis: max_conn=%d url=%s", cfg.notify_max, notify_url_raw)
            return get_async_redis_client(
                url=notify_url_raw,
                max_connections=cfg.notify_max,
                socket_timeout=cfg.sock_to,
                socket_connect_timeout=cfg.conn_to,
                decode_responses=True,
                health_check_interval=cfg.hc_interval,
            )
        logger.info("🔗 Reusing main Redis pool for notifications")
        return main

    @staticmethod
    def _log_pool_info(
        cfg: RedisPoolCfg,
        redis_dsn: str,
        ticks_dsn: str,
        config_url: str,
        ml_gate_url: str,
        state_url: str,
    ) -> None:
        logger.info(
            "🔗 Redis pools: main_max=%d ticks_max=%d config_max=%d ml_gate_max=%d "
            "health_contract_max=%d publish_max=%d state_max=%d sock_to=%.1fs conn_to=%.1fs hc=%ds",
            cfg.main_max, cfg.ticks_max, cfg.config_max, cfg.ml_gate_max,
            cfg.health_contract_max, cfg.publish_max, cfg.state_max, cfg.sock_to, cfg.conn_to, cfg.hc_interval,
        )
        logger.info("   main=%s  ticks=%s  config=%s  ml_gate=%s  state=%s",
                    redis_dsn, ticks_dsn, config_url, ml_gate_url, state_url)

    @staticmethod
    def _warn_if_pool_too_small(cfg: RedisPoolCfg) -> None:
        overhead = 50
        max_sym = max(0, (cfg.ticks_max - overhead) // 2)
        if max_sym < 10:
            logger.warning(
                "⚠️ ticks pool слишком мал: ticks_max=%d поддерживает ~%d символов. "
                "Увеличьте REDIS_TICKS_MAX_CONNECTIONS.",
                cfg.ticks_max, max_sym,
            )

    async def close_all(self) -> None:
        """Закрывает все клиенты. Игнорирует ошибки (shutdown path)."""
        seen: set[int] = set()
        for client in (
            self.main, self.ticks, self.config,
            self.health_contract, self.ml_gate,
            self.publish, self.state,
        ):
            oid = id(client)
            if oid in seen:
                continue
            seen.add(oid)
            # notify может совпадать с main — пропускаем дубликат
            try:
                await client.aclose()
            except Exception as exc:
                logger.debug("Redis close error (ignored): %s", exc)

        # notify отдельно: может быть == main (уже закрыт) или отдельный клиент
        if id(self.notify) not in seen:
            try:
                await self.notify.aclose()
            except Exception:
                pass
