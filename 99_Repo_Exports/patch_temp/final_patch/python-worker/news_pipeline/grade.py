# news_pipeline/grade.py
from __future__ import annotations



from news_pipeline.tags import TAG_BITS

U64_MASK = (1 << 64) - 1


def _bit(name: str) -> int:
    b = TAG_BITS.get(name)
    return (1 << b) if b is not None and 0 <= b < 64 else 0


# Группы тегов через masks (быстро, без аллокаций списков в tick-loop)
MASK_MACRO_HIGH = (
    _bit("cpi") | _bit("ppi") | _bit("fomc") | _bit("nfp") | _bit("rates") | _bit("inflation") | _bit("fed_speech") | _bit("macro")
)
MASK_CRYPTO_SHOCK = (_bit("hack") | _bit("exchange") | _bit("crypto_reg") | _bit("liquidation"))
MASK_RISK_REGIME = (_bit("risk_off") | _bit("risk_on"))
MASK_EQUITIES = (_bit("earnings") | _bit("etf"))
MASK_GEO = _bit("geopolitics")


def compute_news_grade_id(
    *,
    news_risk: float,
    confidence: float,
    primary_tag_id: int,
    tags_mask: int = 0,
) -> int:
    """
    news_grade_id: 0..4
    - 0: ignore / none
    - 1: low
    - 2: medium
    - 3: high
    - 4: critical

    Вход:
    - news_risk: 0..1 (агрегированный риск)
    - confidence: 0..1 (надежность анализа)
    - tags_mask: uint64 (категории)
    """

    # clamp
    r = float(news_risk)
    if r < 0.0:
        r = 0.0
    if r > 1.0:
        r = 1.0

    c = float(confidence)
    if c < 0.0:
        c = 0.0
    if c > 1.0:
        c = 1.0

    tm = int(tags_mask) & U64_MASK

    # базовый score: риск, слегка взвешенный уверенностью
    # (если confidence=0 => всё равно не обнуляем полностью, чтобы не пропускать "очевидные" негативные сюжеты)
    score = r * (0.6 + 0.4 * c)

    # теги усиливают "важность" при умеренном риске
    if (tm & MASK_MACRO_HIGH) != 0:
        score += 0.10
    if (tm & MASK_CRYPTO_SHOCK) != 0:
        score += 0.12
    if (tm & MASK_GEO) != 0:
        score += 0.08
    if (tm & MASK_EQUITIES) != 0:
        score += 0.05
    if (tm & MASK_RISK_REGIME) != 0:
        score += 0.04

    # ограничим
    if score > 1.0:
        score = 1.0

    # пороги (можете потом вынести в ENV при желании)
    # критический: высокий риск + либо сильные теги, либо высокая уверенность
    if score >= 0.78:
        return 4
    if score >= 0.58:
        return 3
    if score >= 0.36:
        return 2
    if score >= 0.20:
        return 1
    return 0


def compute_horizon_sec(*, primary_tag_id: int, tags_mask: int = 0) -> int:
    """
    horizon_sec: как долго "влияние" новости имеет смысл учитывать в торговых фильтрах.
    Важно: это не TTL Redis (TTL отдельно), это торговая семантика.
    """
    tm = int(tags_mask) & U64_MASK

    # "быстрые" сюжеты (часто отрабатывают быстро)
    if (tm & _bit("liquidation")) != 0:
        return 2 * 3600

    # режимные (risk-on/off)
    if (tm & MASK_RISK_REGIME) != 0:
        return 4 * 3600

    # макро-ивенты часто держат волатильность сессию/день
    if (tm & MASK_MACRO_HIGH) != 0:
        return 12 * 3600

    # крипто-шоки могут держать дольше
    if (tm & (_bit("hack") | _bit("exchange"))) != 0:
        return 24 * 3600
    if (tm & _bit("crypto_reg")) != 0:
        return 24 * 3600

    # геополитика — часто дольше
    if (tm & MASK_GEO) != 0:
        return 48 * 3600

    # equities/earnings
    if (tm & MASK_EQUITIES) != 0:
        return 12 * 3600

    # fallback по primary_tag_id (если tags_mask пустой/не заполнен)
    # (оставьте короткий дефолт, чтобы не "вечно" держать шум)
    if int(primary_tag_id) != 0:
        return 12 * 3600

    return 6 * 3600

