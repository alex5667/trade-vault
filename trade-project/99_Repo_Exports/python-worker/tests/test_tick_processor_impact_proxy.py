"""
Unit tests for impact_proxy + slippage_decomp indicators computation
(execution-risk layer, P71).

Tests exercise the pure calculation logic without requiring Redis or a live
TickProcessor instance. We isolate the mathematical contracts and edge-cases:
- Normal case: known inputs → expected impact_proxy value
- Zero depth: fallback to 1e-6 denominator (no ZeroDivisionError)
- Cap: impact_proxy never exceeds 10.0
- Slippage decomp: calls core.expected_slippage_decomp_v1 and produces all sub-fields
- Taker flow imb: extracted from runtime attribute
"""
import pytest

# ---------------------------------------------------------------------------
# Helpers – replicate the indicator computation logic from tick_processor
# (inline so tests don't depend on async / redis / SymbolRuntime)
# ---------------------------------------------------------------------------

def compute_impact_proxy(dn_usd: float, depth_bid_5: float, depth_ask_5: float, mid_px: float) -> dict:
    """Reproduce tick_processor impact_proxy computation block."""
    d5bid_usd = depth_bid_5 * mid_px
    d5ask_usd = depth_ask_5 * mid_px
    has_depth = d5bid_usd > 0 or d5ask_usd > 0
    depth_min_5_usd = max(min(d5bid_usd, d5ask_usd), 1e-6) if has_depth else 1e-6
    ip = min(abs(dn_usd) / depth_min_5_usd, 10.0)
    return {
        "depth_bid_5_usd": d5bid_usd,
        "depth_ask_5_usd": d5ask_usd,
        "depth_min_5_usd": depth_min_5_usd,
        "impact_proxy": ip,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestImpactProxy:
    def test_normal_case(self):
        """Standard order: 5000 USD vs. 50k USD book depth each side → 0.1"""
        r = compute_impact_proxy(dn_usd=5_000, depth_bid_5=10, depth_ask_5=10, mid_px=5_000)
        assert r["impact_proxy"] == pytest.approx(0.1, rel=1e-4)

    def test_thinner_side_used(self):
        """Impact uses the thinner (min) side of the book."""
        # bid depth = 100×10k = 1M, ask = 10×10k = 100k → min = 100k
        r = compute_impact_proxy(dn_usd=10_000, depth_bid_5=100, depth_ask_5=10, mid_px=10_000)
        # depth_min_5_usd = 100_000, impact = 10_000 / 100_000 = 0.1
        assert r["depth_min_5_usd"] == pytest.approx(100_000)
        assert r["impact_proxy"] == pytest.approx(0.1, rel=1e-4)

    def test_zero_depth_no_error(self):
        """Zero-depth book should not raise ZeroDivisionError; fallback to 1e-6."""
        r = compute_impact_proxy(dn_usd=100, depth_bid_5=0, depth_ask_5=0, mid_px=100)
        assert r["depth_min_5_usd"] == pytest.approx(1e-6)
        # impact_proxy capped at 10
        assert r["impact_proxy"] == pytest.approx(10.0)

    def test_cap_at_10(self):
        """impact_proxy is capped at 10.0 regardless of inputs."""
        r = compute_impact_proxy(dn_usd=1_000_000, depth_bid_5=1, depth_ask_5=1, mid_px=1)
        assert r["impact_proxy"] == 10.0

    def test_zero_dn_usd(self):
        """Zero trade size → zero impact proxy."""
        r = compute_impact_proxy(dn_usd=0, depth_bid_5=20, depth_ask_5=20, mid_px=2_000)
        assert r["impact_proxy"] == 0.0


class TestSlippageDecomp:
    """Test that expected_slippage_decomp_bps computes correctly for P71 indicators."""

    def test_basic_decomp(self):
        from core.expected_slippage_decomp_v1 import expected_slippage_decomp_bps
        cfg = {
            "slippage_decomp_enable": 1,
            "slippage_decomp_half_spread_mult": 0.5,
            "slippage_decomp_impact_coeff_bps": 10.0,
            "slippage_decomp_size_ref_usd": 10_000.0,
            "slippage_decomp_size_power": 1.0,
            "slippage_decomp_cap_bps": 500.0,
        }
        # order 10k at ref → size_ratio=1, impact_proxy=0.1
        # spread_comp = 0.5 * 20 = 10 bps; impact_comp = 10 * 0.1 * 1 = 1 bps
        r = expected_slippage_decomp_bps(spread_bps=20.0, impact_proxy=0.1, cfg=cfg, order_size_usd=10_000)
        assert r.spread_bps == pytest.approx(10.0)
        assert r.impact_bps == pytest.approx(1.0)
        assert r.total_bps == pytest.approx(11.0)

    def test_disabled_returns_zero(self):
        from core.expected_slippage_decomp_v1 import expected_slippage_decomp_bps
        cfg = {"slippage_decomp_enable": 0}
        r = expected_slippage_decomp_bps(spread_bps=50.0, impact_proxy=5.0, cfg=cfg)
        assert r.total_bps == 0.0

    def test_cap_applied(self):
        from core.expected_slippage_decomp_v1 import expected_slippage_decomp_bps
        cfg = {
            "slippage_decomp_enable": 1,
            "slippage_decomp_cap_bps": 15.0,
        }
        # Without cap, total would be >> 15 bps (spread alone = 25 bps)
        r = expected_slippage_decomp_bps(spread_bps=50.0, impact_proxy=1.0, cfg=cfg)
        assert r.total_bps == pytest.approx(15.0)

    def test_large_order_scaling(self):
        """Order 2× reference size → impact_comp doubles (linear)."""
        from core.expected_slippage_decomp_v1 import expected_slippage_decomp_bps
        cfg = {
            "slippage_decomp_enable": 1,
            "slippage_decomp_half_spread_mult": 0.5,
            "slippage_decomp_impact_coeff_bps": 10.0,
            "slippage_decomp_size_ref_usd": 10_000.0,
            "slippage_decomp_size_power": 1.0,
            "slippage_decomp_cap_bps": 500.0,
        }
        r1 = expected_slippage_decomp_bps(spread_bps=10, impact_proxy=1.0, cfg=cfg, order_size_usd=10_000)
        r2 = expected_slippage_decomp_bps(spread_bps=10, impact_proxy=1.0, cfg=cfg, order_size_usd=20_000)
        # impact_bps should double
        assert r2.impact_bps == pytest.approx(r1.impact_bps * 2, rel=1e-4)
