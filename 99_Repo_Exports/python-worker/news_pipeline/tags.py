from __future__ import annotations

from typing import Iterable, Tuple


# СТАБИЛЬНОЕ соответствие tag -> bit.
# Важно: не менять существующие биты, только добавлять новые.
TAG_BITS = {
    "cpi": 0
    "ppi": 1
    "fomc": 2
    "fed_speech": 3
    "nfp": 4
    "rates": 5
    "inflation": 6
    "risk_off": 7
    "risk_on": 8
    "earnings": 9
    "geopolitics": 10
    "crypto_reg": 11
    "exchange": 12
    "hack": 13
    "etf": 14
    "liquidation": 15
    "macro": 16
}

# primary_tag_id — компактный int (можно использовать enum таблицу)
PRIMARY_TAG_ID = {
    "cpi": 1
    "ppi": 2
    "fomc": 3
    "nfp": 4
    "rates": 5
    "geopolitics": 6
    "hack": 7
    "etf": 8
    "crypto_reg": 9
    "exchange": 10
    "macro": 11
    "inflation": 12
    "risk_off": 13
    "risk_on": 14
    "earnings": 15
    "liquidation": 16
}

def tags_to_mask(tags: Iterable[str]) -> int:
    mask = 0
    for t in tags:
        k = (t or "").strip().lower()
        b = TAG_BITS.get(k)
        if b is not None and 0 <= b < 63:
            mask |= (1 << b)
    return int(mask)

def pick_primary_tag(tags: Iterable[str]) -> int:
    # На практике — "самый важный" тег; сейчас по приоритету из PRIMARY_TAG_ID
    best = 0
    for t in tags:
        k = (t or "").strip().lower()
        tid = PRIMARY_TAG_ID.get(k, 0)
        if tid and (best == 0 or tid < best):
            best = tid
    return int(best)
