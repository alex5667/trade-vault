"""core/symbols_config_v1.py — single source of truth for the crypto symbol list.

Background
----------
Per-producer env names (``QDP_SYMBOLS``, ``CDP_SYMBOLS``, ``RTP_SYMBOLS``,
``CVP_SYMBOLS``) all defaulted to a hardcoded fallback
``"BTCUSDT,ETHUSDT,SOLUSDT,1000PEPEUSDT"``. Easy to drift: update
``CRYPTO_SYMBOLS`` in ``.env`` and a forgotten producer keeps the old list.

This module makes ``CRYPTO_SYMBOLS`` the canonical env name and provides
one function callers use. Per-producer env names are accepted as
**deprecated aliases** for back-compat but emit a runtime warning.

Usage
-----
::

    from core.symbols_config_v1 import get_crypto_symbols
    SYMBOLS = get_crypto_symbols()                    # canonical
    SYMBOLS = get_crypto_symbols(aliases=["QDP_SYMBOLS"])  # legacy override
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger("symbols_config_v1")

# Default fallback used ONLY when neither CRYPTO_SYMBOLS nor any legacy
# alias is set. Defined in one place so any drift is loud at code review.
_DEFAULT = "BTCUSDT,ETHUSDT,SOLUSDT,1000PEPEUSDT"


def _split_symbols(raw: str) -> list[str]:
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


def get_crypto_symbols(
    aliases: tuple[str, ...] | list[str] | None = None,
    *,
    default: str | None = None,
) -> list[str]:
    """Return the canonical crypto symbol list.

    Precedence:
      1. ``CRYPTO_SYMBOLS`` env (single source of truth).
      2. Any deprecated per-producer alias in ``aliases`` order
         (warns once per process).
      3. ``default`` argument or the module fallback.
    """
    canonical = os.getenv("CRYPTO_SYMBOLS", "").strip()
    if canonical:
        return _split_symbols(canonical)

    for alias in aliases or ():
        v = os.getenv(alias, "").strip()
        if not v:
            continue
        if alias not in _warned_aliases:
            logger.warning(
                "symbol-list env %r is deprecated; set CRYPTO_SYMBOLS instead",
                alias,
            )
            _warned_aliases.add(alias)
        return _split_symbols(v)

    return _split_symbols(default if default is not None else _DEFAULT)


_warned_aliases: set[str] = set()
