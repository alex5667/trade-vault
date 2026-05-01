# news_pipeline/grade.py
from __future__ import annotations

from typing import Any

# Grade semantics (used by consumers):
#   0 = none/ignore
#   1 = low
#   2 = medium
#   3 = high
#   4 = extreme


def _clamp(x: float, lo: float, hi: float) -> float:
    """
    Fail-open clamp for numeric inputs.
    """
    try:
        x = float(x)
    except Exception:
        return lo
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def compute_grade_id(*, risk: float, surprise: float, confidence: float) -> int:
    """
    Grade semantics:
      0 = none/ignore
      1 = low
      2 = medium
      3 = high
      4 = extreme

    Inputs:
      - risk: strictly 0..1 (clamped)
      - surprise: -1..+1 typical, sign matters
      - confidence: 0..1 (clamped)

    Sign-aware intensity:
      - surprise>=0 (risk-on): stronger weight
      - surprise<0  (risk-off): still important, but with smaller weight/mult
    """
    r = _clamp(risk or 0.0, 0.0, 1.0)
    s = _clamp(float(surprise or 0.0), -1.0, 1.0)
    s_mag = abs(s)
    c = _clamp(confidence or 0.0, 0.0, 1.0)

    # Surprise weights by sign (defaults match your recommendation)
    pos_w = float(os.getenv("NEWS_GRADE_SURPRISE_POS_W", "0.85"))
    neg_w = float(os.getenv("NEWS_GRADE_SURPRISE_NEG_W", "0.70"))
    pos_scale = float(os.getenv("NEWS_GRADE_SURPRISE_POS_SCALE", "1.10"))
    neg_scale = float(os.getenv("NEWS_GRADE_SURPRISE_NEG_SCALE", "0.95"))

    if s >= 0.0:
        intensity = max(r, pos_w * s_mag) * pos_scale
    else:
        intensity = max(r, neg_w * s_mag) * neg_scale

    intensity = _clamp(intensity, 0.0, 1.0)

    # confidence damping: do not let low-confidence events jump grades too easily.
    # c=1.0 -> *1.0, c=0.0 -> *min_mult
    min_mult = float(os.getenv("NEWS_GRADE_CONF_MIN_MULT", "0.35"))
    score = intensity * (min_mult + (1.0 - min_mult) * c)

    # thresholds for 0..4 boundaries (defaults match wide hysteresis-friendly levels)
    t1 = float(os.getenv("NEWS_GRADE_T1", "0.15"))
    t2 = float(os.getenv("NEWS_GRADE_T2", "0.30"))
    t3 = float(os.getenv("NEWS_GRADE_T3", "0.50"))
    t4 = float(os.getenv("NEWS_GRADE_T4", "0.70"))

    if score < t1:
        return 0
    if score < t2:
        return 1
    if score < t3:
        return 2
    if score < t4:
        return 3
    return 4


def compute_grade_id(*, risk: float, surprise: float, confidence: float) -> int:
    """
    Deterministic grade computation for UI/scorers/trading filters.

    Inputs:
      - risk: strictly 0..1
      - surprise: typical -1..+1 (we use magnitude)
      - confidence: 0..1 (clamped)

    Design goals:
      - устойчивость к шуму: границы не слишком плотные
      - подавление при низкой уверенности: избегаем флаппинга
    """
    r = _clamp(float(risk or 0.0), 0.0, 1.0)
    s_mag = _clamp(abs(float(surprise or 0.0)), 0.0, 1.0)
    c = _clamp(float(confidence or 0.0), 0.0, 1.0)

    # Интенсивность: риск доминирует, но surprise тоже важен.
    intensity = max(r, 0.85 * s_mag)

    # Confidence gating: 0..1 -> 0.5..1.0 (не убиваем полностью, но заметно снижаем).
    conf_mul = 0.5 + 0.5 * c
    score = intensity * conf_mul

    # Пороги подобраны так, чтобы grade менялся редко при небольших колебаниях EWMA.
    if score < 0.15:
        return 0
    if score < 0.30:
        return 1
    if score < 0.50:
        return 2
    if score < 0.70:
        return 3
    return 4


# Existing compute_horizon_sec(...) stays as-is (tag-based mapping).
# We only add a grade-aware wrapper so feature_store can stay thin.
def compute_horizon_sec_with_grade(*, base_horizon_sec: int, grade_id: int) -> int:
    """
    Adjusts base horizon by grade.

    Rules:
      - grade 0: ignore -> horizon 0
      - grade 1: slightly shorter (fast decay)
      - grade 2: base
      - grade 3: longer
      - grade 4: longest (capped)
    """
    try:
        g = int(grade_id)
    except Exception:
        g = 0

    base = int(base_horizon_sec or 0)
    if base <= 0:
        return 0

    if g <= 0:
        return 0
    if g == 1:
        return max(60, int(base * 0.75))
    if g == 2:
        return base
    if g == 3:
        return int(base * 1.50)

    # g >= 4
    # Cap to 72h to prevent runaway long blocks.
    return min(int(base * 2.0), 72 * 3600)


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

