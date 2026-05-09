from __future__ import annotations

from dataclasses import dataclass

from core.instrument_config import get_liquidity_class


def classify_symbol(symbol: str) -> str:
    # Single source of truth from core/instrument_config.py
    return str(get_liquidity_class(symbol) or "mid")


@dataclass(frozen=True)
class LiquidityThresholds:
    # spread
    spread_bad_bp: float
    spread_crit_bp: float
    # depth
    depth_good_usd: float
    depth_bad_usd: float
    depth_crit_usd: float
    # book update rate
    book_rate_good_hz: float
    book_rate_bad_hz: float
    book_rate_crit_hz: float
    # score thresholds
    thin_score: float
    stressed_score: float
    # min_conf bump (strategy overlay)
    min_conf_bump_thin_pct: float
    min_conf_bump_stressed_pct: float

    def to_cfg_overrides(self) -> dict[str, float]:
        return {
            "liq_spread_bad_bp": self.spread_bad_bp,
            "liq_spread_crit_bp": self.spread_crit_bp,
            "liq_depth_good_usd": self.depth_good_usd,
            "liq_depth_bad_usd": self.depth_bad_usd,
            "liq_depth_crit_usd": self.depth_crit_usd,
            "liq_book_rate_good_hz": self.book_rate_good_hz,
            "liq_book_rate_bad_hz": self.book_rate_bad_hz,
            "liq_book_rate_crit_hz": self.book_rate_crit_hz,
            "liq_thin_score": self.thin_score,
            "liq_stressed_score": self.stressed_score,
            "liq_min_conf_bump_thin_pct": self.min_conf_bump_thin_pct,
            "liq_min_conf_bump_stressed_pct": self.min_conf_bump_stressed_pct,
        }


# Seed thresholds (good starting points; tune via histograms/percentiles)
DEFAULT_BY_CLASS: dict[str, LiquidityThresholds] = {
    # Majors: tighter spreads, high depth, high book rate
    "majors": LiquidityThresholds(
        spread_bad_bp=15.0, spread_crit_bp=30.0,
        depth_good_usd=600_000.0, depth_bad_usd=180_000.0, depth_crit_usd=90_000.0,
        book_rate_good_hz=50.0, book_rate_bad_hz=20.0, book_rate_crit_hz=8.0,
        thin_score=0.65, stressed_score=0.40,
        min_conf_bump_thin_pct=2.0, min_conf_bump_stressed_pct=5.0,
    ),
    # Large caps: solid but not BTC/ETH
    "large": LiquidityThresholds(
        spread_bad_bp=20.0, spread_crit_bp=40.0,
        depth_good_usd=300_000.0, depth_bad_usd=90_000.0, depth_crit_usd=45_000.0,
        book_rate_good_hz=25.0, book_rate_bad_hz=10.0, book_rate_crit_hz=4.0,
        thin_score=0.62, stressed_score=0.38,
        min_conf_bump_thin_pct=3.0, min_conf_bump_stressed_pct=7.0,
    ),
    # Mid caps: меньше глубина и ниже rate
    "mid": LiquidityThresholds(
        spread_bad_bp=25.0, spread_crit_bp=55.0,
        depth_good_usd=140_000.0, depth_bad_usd=45_000.0, depth_crit_usd=22_000.0,
        book_rate_good_hz=15.0, book_rate_bad_hz=6.0, book_rate_crit_hz=2.0,
        thin_score=0.60, stressed_score=0.35,
        min_conf_bump_thin_pct=4.0, min_conf_bump_stressed_pct=9.0,
    ),
    # Memes: book rate значительно ниже, spread шире, глубина меньше
    "memes": LiquidityThresholds(
        spread_bad_bp=35.0, spread_crit_bp=80.0,
        depth_good_usd=70_000.0, depth_bad_usd=22_000.0, depth_crit_usd=11_000.0,
        book_rate_good_hz=8.0, book_rate_bad_hz=3.0, book_rate_crit_hz=1.0,
        thin_score=0.58, stressed_score=0.33,
        min_conf_bump_thin_pct=5.0, min_conf_bump_stressed_pct=10.0,
    ),
}


def get_thresholds(symbol: str) -> LiquidityThresholds:
    cls = classify_symbol(symbol)
    return DEFAULT_BY_CLASS.get(cls, DEFAULT_BY_CLASS["mid"])
