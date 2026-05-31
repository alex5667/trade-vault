from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class _RuntimeCfg:
    """
    All runtime flags are read once at init to avoid os.getenv in hot paths.
    """
    qf_pack_u16: bool
    strict_reason_codes: bool
    audit_compact: bool
    candidate_log_every_ms: int
    signal_log_every_ms: int
    pack_soft_u16: bool

    # --- NEW: финализация "вошли, но не вышли" ---
    # Сколько баров можно держать позицию после entry без exit-события.
    # По истечению -> Outcome.EXPIRED_NO_TARGET + finalize (удаление из памяти, фиксация статистики).
    max_lifetime_bars_after_entry: int

    # Fallback по времени (мс) на случай если bar-index не всегда доступен.
    # 0 = выключено.
    max_lifetime_ms_after_entry: int

    # Как часто делать housekeeping (чтобы не O(N) на каждом тике).
    # В тестах можно дергать напрямую, а в бою — раз в N мс.
    housekeeping_every_ms: int

    # Сколько баров до входа (для контроля протухших сигналов без входа).
    # Уже есть в других местах, но явно добавляем для консистентности.
    expiry_bars: int

    @staticmethod
    def _b(env: str, default: str = "0") -> bool:
        v = os.getenv(env, default).strip().lower()
        return v in {"1", "true", "yes", "y", "on"}

    @staticmethod
    def _i(env: str, default: str) -> int:
        try:
            return int(os.getenv(env, default))
        except Exception:
            return default

    @classmethod
    def from_env(cls) -> _RuntimeCfg:
        # Базовые параметры протухания
        expiry_bars = cls._i("ORDERFLOW_EXPIRY_BARS", "60")

        # NEW: TTL после входа (в барах).
        # Рекомендуемое стартовое: 3 * expiry_bars, но лучше задавать явно.
        max_lifetime_bars_after_entry = cls._i(
            "ORDERFLOW_MAX_LIFETIME_BARS_AFTER_ENTRY",
            str(max(3 * expiry_bars, 180)),
        )

        # NEW: fallback TTL по времени (мс). Например 60 минут = 3_600_000.
        # 0 = выключено (используется только баровый TTL).
        max_lifetime_ms_after_entry = cls._i(
            "ORDERFLOW_MAX_LIFETIME_MS_AFTER_ENTRY",
            "0",
        )

        # NEW: как часто запускать housekeeping (мс).
        # В production рекомендуется 1000-5000 мс для снижения нагрузки.
        housekeeping_every_ms = cls._i("ORDERFLOW_HOUSEKEEPING_EVERY_MS", "1000")

        return cls(
            qf_pack_u16=cls._b("QF_PACK_U16", "1"),
            strict_reason_codes=cls._b("STRICT_REASON_CODES", "0"),
            audit_compact=cls._b("AUDIT_COMPACT", "1"),
            candidate_log_every_ms=cls._i("CANDIDATE_LOG_EVERY_MS", "5000"),
            signal_log_every_ms=cls._i("SIGNAL_LOG_EVERY_MS", "0"),  # keep 0 if you already do "1 signal = 1 JSON"
            pack_soft_u16=cls._b("PACK_SOFT_U16", "1"),
            # NEW fields
            max_lifetime_bars_after_entry=max_lifetime_bars_after_entry,
            max_lifetime_ms_after_entry=max_lifetime_ms_after_entry,
            housekeeping_every_ms=housekeeping_every_ms,
            expiry_bars=expiry_bars,
        )
