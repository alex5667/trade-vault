# news_pipeline/tag_registry.py
from __future__ import annotations

import importlib
import os
from collections.abc import Iterable
from dataclasses import dataclass

MASK64 = (1 << 64) - 1


@dataclass(frozen=True)
class TagRegistry:
    """
    Единый слой доступа к тегам:
    - tag -> bit position (0..63)
    - tag -> primary_tag_id (int)
    """
    tag_bits: dict[str, int]
    primary_tag_id: dict[str, int]

    def bitpos(self, tag: str) -> int | None:
        return self.tag_bits.get(tag)

    def bit(self, tag: str) -> int:
        p = self.bitpos(tag)
        return (1 << p) if p is not None and 0 <= p < 64 else 0

    def mask_any(self, tags: Iterable[str]) -> int:
        m = 0
        for t in tags:
            m |= self.bit(t)
        return m & MASK64

    def has_any(self, mask: int, tags: Iterable[str]) -> bool:
        want = self.mask_any(tags)
        return (int(mask) & MASK64) & want != 0

    def primary_id(self, tag: str, default: int = 0) -> int:
        return int(self.primary_tag_id.get(tag, default))


def _load_from_module(mod_name: str) -> TagRegistry | None:
    try:
        m = importlib.import_module(mod_name)
    except Exception:
        return None

    tag_bits = getattr(m, "TAG_BITS", None)
    primary = getattr(m, "PRIMARY_TAG_ID", None)

    if isinstance(tag_bits, dict) and isinstance(primary, dict):
        # нормализуем ключи в lower (на случай разнобоя)
        tb = {str(k).lower(): int(v) for k, v in tag_bits.items()}
        pr = {str(k).lower(): int(v) for k, v in primary.items()}
        return TagRegistry(tag_bits=tb, primary_tag_id=pr)

    return None


def load_tag_registry() -> TagRegistry:
    """
    Источник истины: tags.py (новый формат).
    Путь задаётся через ENV NEWS_TAGS_MODULE.
    """
    mod = os.getenv("NEWS_TAGS_MODULE", "news_pipeline.tags").strip()

    reg = _load_from_module(mod)
    if reg:
        return reg

    # fallback: старые константы (если вдруг tags.py отсутствует)
    # ВАЖНО: это только аварийный режим совместимости.
    # Если вы хотите, можно удалить fallback полностью.
    try:
        m = importlib.import_module("news_pipeline.models")
        # ожидаем что там TAG_... уже в виде 1<<bit
        # построим TAG_BITS из этих констант грубо (по позиции первого установленного бита)
        tb: dict[str, int] = {}
        pr: dict[str, int] = {}
        for name in dir(m):
            if name.startswith("TAG_"):
                val = getattr(m, name)
                if isinstance(val, int) and val > 0:
                    bitpos = (val.bit_length() - 1)
                    tb[name.lower()] = bitpos
        return TagRegistry(tag_bits=tb, primary_tag_id=pr)
    except Exception:
        return TagRegistry(tag_bits={}, primary_tag_id={})
