"""
BalanceProvider — динамический источник баланса счёта для сайзинга позиций.

Приоритет источников (fail-open на каждом уровне):
  1. In-process cache           (TTL = BALANCE_CACHE_TTL_S, default 30s)
  2. Redis snapshot             (key = account:snapshot:binance_usdtm)
  3. Binance REST /fapi/v2/account (cold fallback при устаревшем снапшоте)
  4. ACCOUNT_DEPOSIT_USD env    (static fallback)

ENV:
  BALANCE_PROVIDER_MODE     redis_first | direct | static   (default: redis_first)
  BALANCE_MAX_STALENESS_S   сколько секунд Redis-снапшот считается свежим  (default: 300)
  BALANCE_CACHE_TTL_S       in-process cache TTL в секундах                (default: 30)
  ACCOUNT_DEPOSIT_USD       статичный fallback в USDT                      (default: 1000)
  ACCOUNT_SNAPSHOT_KEY      Redis-ключ snapshot                             (default: account:snapshot:binance_usdtm)
"""
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Optional, Tuple

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ENV helpers
# ---------------------------------------------------------------------------

def _env_float(name: str, default: float) -> float:
    try:
        v = os.getenv(name, "")
        if v and v.strip():
            return float(v)
    except Exception:
        pass
    return float(default)


def _env_str(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


# ---------------------------------------------------------------------------
# In-process cache
# ---------------------------------------------------------------------------

@dataclass
class _CacheEntry:
    wallet_balance: float
    available_balance: float
    source: str
    fetched_at: float = field(default_factory=time.monotonic)

    def age_s(self) -> float:
        return time.monotonic() - self.fetched_at


class _InProcessCache:
    """Thread-safe single-entry cache."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entry: Optional[_CacheEntry] = None

    def get(self, ttl_s: float) -> Optional[_CacheEntry]:
        with self._lock:
            if self._entry is not None and self._entry.age_s() < ttl_s:
                return self._entry
            return None

    def set(self, entry: _CacheEntry) -> None:
        with self._lock:
            self._entry = entry

    def invalidate(self) -> None:
        with self._lock:
            self._entry = None


_GLOBAL_CACHE = _InProcessCache()


# ---------------------------------------------------------------------------
# BalanceProvider
# ---------------------------------------------------------------------------

class BalanceProvider:
    """
    Потокобезопасный провайдер баланса для position sizing.

    Использование:
        bp = BalanceProvider.from_env()              # cold construct
        bp = BalanceProvider.from_ctx(ctx)           # из execution context
        wallet = bp.get_wallet_balance()             # никогда не бросает
    """

    def __init__(
        self,
        *,
        redis_client: Any = None,           # redis-py client или совместимый
        binance_client: Any = None,         # BinanceFuturesREST / BinanceFuturesClient
        mode: str = "redis_first",          # redis_first | direct | static
        max_staleness_s: float = 300.0,
        cache_ttl_s: float = 30.0,
        static_deposit: float = 1000.0,
        snapshot_key: str = "account:snapshot:binance_usdtm",
    ) -> None:
        self._redis = redis_client
        self._binance = binance_client
        self._mode = mode.strip().lower()
        self._max_staleness_s = float(max_staleness_s)
        self._cache_ttl_s = float(cache_ttl_s)
        self._static_deposit = float(static_deposit)
        self._snapshot_key = snapshot_key

    # ------------------------------------------------------------------
    # Factory methods
    # ------------------------------------------------------------------

    @classmethod
    def from_env(cls, *, redis_client: Any = None, binance_client: Any = None) -> "BalanceProvider":
        """Собрать провайдер из ENV vars."""
        return cls(
            redis_client=redis_client,
            binance_client=binance_client,
            mode=_env_str("BALANCE_PROVIDER_MODE", "redis_first"),
            max_staleness_s=_env_float("BALANCE_MAX_STALENESS_S", 300.0),
            cache_ttl_s=_env_float("BALANCE_CACHE_TTL_S", 30.0),
            static_deposit=_env_float("ACCOUNT_DEPOSIT_USD", 1000.0),
            snapshot_key=_env_str("ACCOUNT_SNAPSHOT_KEY", "account:snapshot:binance_usdtm"),
        )

    @classmethod
    def from_ctx(cls, ctx: Any) -> "BalanceProvider":
        """
        Собрать провайдер из execution context.
        ctx.balance_provider → возвращаем если это BalanceProvider.
        ctx.redis + ctx.binance_client → конструируем.
        """
        bp = getattr(ctx, "balance_provider", None)
        if isinstance(bp, BalanceProvider):
            return bp

        redis_client = getattr(ctx, "redis", None)
        binance_client = getattr(ctx, "binance_client", None)
        return cls.from_env(redis_client=redis_client, binance_client=binance_client)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_wallet_balance(self) -> float:
        """Вернуть walletBalance в USDT. Никогда не бросает, всегда > 0."""
        wallet, _ = self._resolve()
        return wallet

    def get_available_balance(self) -> float:
        """Вернуть availableBalance в USDT. Никогда не бросает, всегда > 0."""
        _, available = self._resolve()
        return available

    def get_balances(self) -> Tuple[float, float]:
        """Вернуть (wallet_balance, available_balance)."""
        return self._resolve()

    def invalidate_cache(self) -> None:
        """Принудительно сбросить in-process cache (например, после открытия позиции)."""
        _GLOBAL_CACHE.invalidate()

    # ------------------------------------------------------------------
    # Internal resolution chain
    # ------------------------------------------------------------------

    def _resolve(self) -> Tuple[float, float]:
        # 1. In-process cache
        cached = _GLOBAL_CACHE.get(self._cache_ttl_s)
        if cached is not None:
            log.debug("[BalanceProvider] cache hit: wallet=%.2f available=%.2f (age=%.1fs src=%s)",
                      cached.wallet_balance, cached.available_balance,
                      cached.age_s(), cached.source)
            return cached.wallet_balance, cached.available_balance

        # 2. По режиму
        if self._mode == "static":
            return self._static_fallback("mode=static")

        if self._mode == "direct":
            r = self._try_binance_rest()
            if r is not None:
                return r
            return self._static_fallback("direct_failed")

        # default: redis_first
        r = self._try_redis_snapshot()
        if r is not None:
            return r
        r = self._try_binance_rest()
        if r is not None:
            return r
        return self._static_fallback("all_sources_failed")

    def _try_redis_snapshot(self) -> Optional[Tuple[float, float]]:
        """Читает account:snapshot из Redis. None если недоступно или устарело."""
        if self._redis is None:
            return None
        try:
            raw = self._redis.get(self._snapshot_key)
            if not raw:
                log.debug("[BalanceProvider] Redis snapshot missing: key=%s", self._snapshot_key)
                return None

            data = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
            ts_ms = float(data.get("ts_ms") or 0)
            age_s = (get_ny_time_millis() - ts_ms) / 1000.0

            if age_s > self._max_staleness_s:
                log.warning("[BalanceProvider] Redis snapshot stale: age=%.1fs > max=%.1fs → cold fallback",
                            age_s, self._max_staleness_s)
                return None

            wallet = float(data.get("wallet_balance") or 0.0)
            available = float(data.get("available_balance") or 0.0)

            if wallet <= 0:
                log.warning("[BalanceProvider] Redis snapshot has wallet_balance=%.4f → skip", wallet)
                return None

            entry = _CacheEntry(
                wallet_balance=wallet,
                available_balance=available,
                source=f"redis_snapshot(age={age_s:.1f}s)",
            )
            _GLOBAL_CACHE.set(entry)
            log.info("[BalanceProvider] wallet_balance=%.2f available=%.2f (source=redis_snapshot age=%.1fs)",
                     wallet, available, age_s)
            return wallet, available

        except Exception as exc:
            log.warning("[BalanceProvider] Redis read error: %s", exc)
            return None

    def _try_binance_rest(self) -> Optional[Tuple[float, float]]:
        """Прямой вызов GET /fapi/v2/account. None если нет клиента или ошибка."""
        if self._binance is None:
            # Попытка lazy-init из ENV
            self._binance = self._lazy_init_binance()

        if self._binance is None:
            return None

        try:
            t0 = time.monotonic()
            acct = self._binance.get_account()
            latency_ms = (time.monotonic() - t0) * 1000

            wallet = float(acct.get("totalWalletBalance") or 0.0)
            available = float(acct.get("availableBalance") or 0.0)

            if wallet <= 0:
                log.warning("[BalanceProvider] REST returned wallet_balance=%.4f", wallet)
                return None

            entry = _CacheEntry(
                wallet_balance=wallet,
                available_balance=available,
                source=f"binance_rest(latency={latency_ms:.0f}ms)",
            )
            _GLOBAL_CACHE.set(entry)
            log.info("[BalanceProvider] wallet_balance=%.2f available=%.2f (source=binance_rest latency=%.0fms)",
                     wallet, available, latency_ms)
            return wallet, available

        except Exception as exc:
            log.warning("[BalanceProvider] Binance REST error: %s", exc)
            return None

    def _static_fallback(self, reason: str) -> Tuple[float, float]:
        """Последний резерв — ACCOUNT_DEPOSIT_USD."""
        deposit = _env_float("ACCOUNT_DEPOSIT_USD", self._static_deposit)
        if deposit <= 0:
            deposit = self._static_deposit
        log.warning("[BalanceProvider] using static fallback=%.2f reason=%s", deposit, reason)
        # Кэшируем статик чтобы не спамить логи
        entry = _CacheEntry(
            wallet_balance=deposit,
            available_balance=deposit,
            source=f"static({reason})",
        )
        _GLOBAL_CACHE.set(entry)
        return deposit, deposit

    def _lazy_init_binance(self) -> Any:
        """Попытка создать BinanceFuturesREST из ENV только при наличии ключей."""
        try:
            from services.binance_futures_client import BinanceFuturesREST
            return BinanceFuturesREST.from_env()
        except Exception:
            try:
                from binance_futures_client import BinanceFuturesREST  # type: ignore
                return BinanceFuturesREST.from_env()
            except Exception:
                return None
