"""Unit tests for 2026-05-30 counter-trend regime hard veto gate (P1 + P2).

P1 covers:
  - master switch OFF (default) → no-op
  - shadow mode → no veto, ctx annotated, soft-flag added
  - enforce mode → veto with reason VETO_COUNTER_TREND_BLOCK
  - SHORT × trending_bull / LONG × trending_bear → блок
  - bypass kinds, dwell hysteresis, alias resolution, fail-open

P2 covers (2026-05-30):
  - P2.2 MTF: slow=trending_bull + micro=trend_micro_up → mtf_confirmed, block (dwell bypassed)
  - P2.2 MTF: slow=trending_bull + micro=trend_micro_down → mtf_conflict, skip block (reversal)
  - P2.2 MTF disabled → micro regime ignored even when conflict
  - P2.3 per-symbol: extra block regimes added for specific symbol
  - P2.3 per-symbol: other symbols not affected by per-symbol extra
  - P2.4 freq cap: returns VETO_CT_FREQ_CAP when count > cap and enforce mode
  - P2.4 freq cap: shadow mode only annotates, no veto
  - P2.4 freq cap disabled → no Redis I/O, no effect
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
import pytest

from handlers.crypto_orderflow.utils.entry_policy_gate import EntryPolicyGate


def _ctx(
    *,
    regime: str | None = None,
    dwell_ms: float | None = None,
    regime_micro: str | None = None,
    redis_client: Any = None,
) -> SimpleNamespace:
    ind: dict[str, Any] = {}
    if regime is not None:
        ind["regime"] = regime
    if regime_micro is not None:
        ind["regime_micro_1m"] = regime_micro
    obj = SimpleNamespace(
        spread_bps=5.0,
        burst_flip_ratio=0.0,
        cancel_to_trade=0.0,
        indicators=ind,
    )
    if dwell_ms is not None:
        obj.regime_dwell_ms = dwell_ms
    if redis_client is not None:
        obj._redis = redis_client
    return obj


# ────────────────────────────────────────────────────────────────────────────
# Master switch
# ────────────────────────────────────────────────────────────────────────────

def test_counter_trend_disabled_by_default(monkeypatch):
    """ENABLED=0 → no veto, no ctx annotation, even on SHORT × trending_bull."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.delenv("COUNTER_TREND_HARD_VETO_ENABLED", raising=False)

    g = EntryPolicyGate.from_env()
    ctx = _ctx(regime="trending_bull")
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="of", side="SHORT")

    assert d.veto is False
    assert getattr(ctx, "counter_trend_block_alarm", 0) == 0


# ────────────────────────────────────────────────────────────────────────────
# Shadow mode
# ────────────────────────────────────────────────────────────────────────────

def test_counter_trend_shadow_annotates_no_veto(monkeypatch):
    """SHADOW: ctx помечен, soft_flag добавлен, но veto=False."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "shadow")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(regime="trending_bull")
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="of", side="SHORT")

    assert d.veto is False
    assert getattr(ctx, "counter_trend_block_alarm", 0) == 1
    assert getattr(ctx, "counter_trend_block_regime", "") == "trending_bull"
    assert getattr(ctx, "counter_trend_block_mode", "") == "shadow"
    notes = getattr(ctx, "counter_trend_block_notes", "")
    assert "dir=SHORT" in notes
    assert "regime=trending_bull" in notes


# ────────────────────────────────────────────────────────────────────────────
# Enforce mode
# ────────────────────────────────────────────────────────────────────────────

def test_counter_trend_enforce_vetoes_short_in_bull(monkeypatch):
    """ENFORCE: SHORT × trending_bull → VETO_COUNTER_TREND_BLOCK."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "enforce")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(regime="trending_bull")
    d = g.evaluate(ctx=ctx, symbol="1000PEPEUSDT", kind="of", side="SHORT")

    assert d.veto is True
    assert d.reason_code == "VETO_COUNTER_TREND_BLOCK"
    assert "dir=SHORT" in d.notes
    assert "regime=trending_bull" in d.notes


def test_counter_trend_enforce_vetoes_long_in_bear(monkeypatch):
    """ENFORCE (симметричный): LONG × trending_bear → VETO."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "enforce")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(regime="trending_bear")
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="of", side="LONG")

    assert d.veto is True
    assert d.reason_code == "VETO_COUNTER_TREND_BLOCK"
    assert "dir=LONG" in d.notes
    assert "regime=trending_bear" in d.notes


def test_counter_trend_enforce_vetoes_short_in_expansion(monkeypatch):
    """ENFORCE: SHORT × expansion → VETO (expansion в default short_block_regimes)."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "enforce")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(regime="expansion")
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="delta_spike", side="SHORT")

    assert d.veto is True
    assert d.reason_code == "VETO_COUNTER_TREND_BLOCK"


# ────────────────────────────────────────────────────────────────────────────
# Direction × regime mismatch (тренд за нас)
# ────────────────────────────────────────────────────────────────────────────

def test_counter_trend_long_in_bull_allowed(monkeypatch):
    """LONG × trending_bull = тренд-следование, не блокируется."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "enforce")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(regime="trending_bull")
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="of", side="LONG")

    assert d.veto is False
    assert getattr(ctx, "counter_trend_block_alarm", 0) == 0


def test_counter_trend_short_in_bear_allowed(monkeypatch):
    """SHORT × trending_bear = тренд-следование, не блокируется."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "enforce")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(regime="trending_bear")
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="of", side="SHORT")

    assert d.veto is False


def test_counter_trend_short_in_range_allowed(monkeypatch):
    """SHORT × range — НЕ в default short_block_regimes (range уже не -EV)."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "enforce")
    # Defaults: short_regimes = trending_bull,expansion (range НЕ в списке)

    g = EntryPolicyGate.from_env()
    ctx = _ctx(regime="range")
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="of", side="SHORT")

    assert d.veto is False


# ────────────────────────────────────────────────────────────────────────────
# Bypass kinds
# ────────────────────────────────────────────────────────────────────────────

def test_counter_trend_bypass_kind_liq_cascade_reverse(monkeypatch):
    """liq_cascade_reverse — reversal play, не должен блокироваться."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "enforce")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(regime="trending_bull")
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="liq_cascade_reverse", side="SHORT")

    assert d.veto is False
    assert getattr(ctx, "counter_trend_block_alarm", 0) == 0


def test_counter_trend_bypass_kind_reversal_v1(monkeypatch):
    """reversal_v1 — bypass."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "enforce")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(regime="trending_bear")
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="reversal_v1", side="LONG")

    assert d.veto is False


# ────────────────────────────────────────────────────────────────────────────
# Regime alias canonicalisation
# ────────────────────────────────────────────────────────────────────────────

def test_counter_trend_regime_alias_uptrend(monkeypatch):
    """uptrend → canonicalised to trending_bull → блок."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "enforce")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(regime="uptrend")  # alias
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="of", side="SHORT")

    assert d.veto is True
    assert d.reason_code == "VETO_COUNTER_TREND_BLOCK"
    assert "regime=trending_bull" in d.notes  # canonical


def test_counter_trend_regime_alias_downtrend(monkeypatch):
    """downtrend → trending_bear → LONG block."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "enforce")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(regime="DOWNTREND")  # case-insensitive
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="of", side="LONG")

    assert d.veto is True
    assert "regime=trending_bear" in d.notes


# ────────────────────────────────────────────────────────────────────────────
# Hysteresis / dwell-time
# ────────────────────────────────────────────────────────────────────────────

def test_counter_trend_dwell_below_threshold_skipped(monkeypatch):
    """regime_dwell_ms < DWELL_MS → пропуск (избегаем flip-flop)."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "enforce")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_DWELL_MS", "300000")  # 5 min

    g = EntryPolicyGate.from_env()
    # Только 2 минуты в regime — пропуск
    ctx = _ctx(regime="trending_bull", dwell_ms=120_000)
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="of", side="SHORT")

    assert d.veto is False


def test_counter_trend_dwell_above_threshold_blocks(monkeypatch):
    """regime_dwell_ms >= DWELL_MS → нормальный блок."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "enforce")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_DWELL_MS", "300000")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(regime="trending_bull", dwell_ms=600_000)  # 10 min
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="of", side="SHORT")

    assert d.veto is True


# ────────────────────────────────────────────────────────────────────────────
# Fail-open guards
# ────────────────────────────────────────────────────────────────────────────

def test_counter_trend_regime_unresolved_fail_open(monkeypatch):
    """regime=None → fail-open, no block."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "enforce")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(regime=None)
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="of", side="SHORT")

    assert d.veto is False


def test_counter_trend_regime_na_fail_open(monkeypatch):
    """regime='na' → fail-open."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "enforce")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(regime="na")
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="of", side="SHORT")

    assert d.veto is False


# ────────────────────────────────────────────────────────────────────────────
# Custom regime lists
# ────────────────────────────────────────────────────────────────────────────

def test_counter_trend_custom_short_regime_list(monkeypatch):
    """Custom CSV — например добавили squeeze."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "enforce")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_SHORT_REGIMES", "trending_bull,squeeze")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(regime="squeeze")
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="of", side="SHORT")

    assert d.veto is True
    assert "regime=squeeze" in d.notes


# ════════════════════════════════════════════════════════════════════════════
# P2.2 Multi-timeframe regime (micro-regime confirmation / conflict)
# ════════════════════════════════════════════════════════════════════════════

def _ct_enforce_mtf_env(monkeypatch, *, mtf_enabled: bool = True) -> None:
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "enforce")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_SHORT_REGIMES", "trending_bull")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_DWELL_MS", "300000")
    monkeypatch.setenv("CT_MTF_ENABLED", "1" if mtf_enabled else "0")


def test_ct_p2_mtf_confirmed_blocks_and_skips_dwell(monkeypatch):
    """P2.2: slow=trending_bull + micro=trend_micro_up → mtf_confirmed → block even
    when dwell_ms is below threshold (dwell guard bypassed on MTF-confirmed trend)."""
    _ct_enforce_mtf_env(monkeypatch)
    g = EntryPolicyGate.from_env()
    # dwell_ms=60000 < 300000 → would normally skip; MTF-confirmed bypasses dwell
    ctx = _ctx(regime="trending_bull", dwell_ms=60_000.0, regime_micro="trend_micro_up")
    d = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="of", side="SHORT")

    assert d.veto is True, "MTF-confirmed trend should bypass dwell and block"
    assert "mtf=confirmed" in (d.notes or "")


def test_ct_p2_mtf_conflict_skips_block(monkeypatch):
    """P2.2: slow=trending_bull + micro=trend_micro_down (conflict) → skip block.
    Micro is reversing → counter-trend SHORT may be valid here."""
    _ct_enforce_mtf_env(monkeypatch)
    g = EntryPolicyGate.from_env()
    ctx = _ctx(regime="trending_bull", dwell_ms=600_000.0, regime_micro="trend_micro_down")
    d = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="of", side="SHORT")

    assert d.veto is False, "MTF conflict should skip block (potential reversal)"
    assert getattr(ctx, "ct_mtf_conflict", 0) == 1


def test_ct_p2_mtf_disabled_ignores_micro(monkeypatch):
    """P2.2: CT_MTF_ENABLED=0 → micro regime is ignored even when it would conflict."""
    _ct_enforce_mtf_env(monkeypatch, mtf_enabled=False)
    g = EntryPolicyGate.from_env()
    # micro says trending_bear (conflict), but MTF disabled → still blocks
    ctx = _ctx(regime="trending_bull", dwell_ms=600_000.0, regime_micro="trend_micro_down")
    d = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="of", side="SHORT")

    assert d.veto is True, "MTF disabled → micro regime not consulted → normal block"


def test_ct_p2_mtf_micro_only_annotation(monkeypatch):
    """P2.2: slow regime NOT in block_set, but micro=trend_micro_up → annotation only."""
    _ct_enforce_mtf_env(monkeypatch)
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_SHORT_REGIMES", "expansion")  # not trending_bull
    g = EntryPolicyGate.from_env()
    ctx = _ctx(regime="trending_bull", dwell_ms=600_000.0, regime_micro="trend_micro_up")
    d = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="of", side="SHORT")

    # slow regime not in block_set → no veto, but ctx annotated
    assert d.veto is False
    assert getattr(ctx, "ct_micro_only_align", 0) == 1


# ════════════════════════════════════════════════════════════════════════════
# P2.3 Per-symbol override
# ════════════════════════════════════════════════════════════════════════════

def test_ct_p2_per_symbol_extra_regime_blocks(monkeypatch):
    """P2.3: per-symbol adds 'range' to block list for PEPEUSDT."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "enforce")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_SHORT_REGIMES", "trending_bull")
    monkeypatch.setenv("COUNTER_TREND_PER_SYMBOL_SHORT_REGIMES", "1000PEPEUSDT:trending_bull+range")

    g = EntryPolicyGate.from_env()
    # range is not in global list, but is in PEPE extra → should block
    ctx = _ctx(regime="range", dwell_ms=600_000.0)
    d = g.evaluate(ctx=ctx, symbol="1000PEPEUSDT", kind="of", side="SHORT")

    assert d.veto is True, "PEPE per-symbol range should be blocked"
    assert "regime=range" in (d.notes or "")


def test_ct_p2_per_symbol_extra_does_not_affect_other_symbols(monkeypatch):
    """P2.3: per-symbol PEPE extra does not block range for ETHUSDT."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "enforce")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_SHORT_REGIMES", "trending_bull")
    monkeypatch.setenv("COUNTER_TREND_PER_SYMBOL_SHORT_REGIMES", "1000PEPEUSDT:trending_bull+range")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(regime="range", dwell_ms=600_000.0)
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="of", side="SHORT")

    assert d.veto is False, "range is not in global SHORT block list for ETHUSDT"


# ════════════════════════════════════════════════════════════════════════════
# P2.4 Frequency cap
# ════════════════════════════════════════════════════════════════════════════

def _make_freq_redis(*, current_count: int) -> object:
    """Fake Redis stub: incr returns current_count, expire is no-op."""
    class _FakeRedis:
        def incr(self, _key: str) -> int:
            return current_count
        def expire(self, _key: str, _ttl: int) -> None:
            pass
    return _FakeRedis()


def test_ct_p2_freq_cap_enforce_vetoes_when_exceeded(monkeypatch):
    """P2.4: when count > cap and enforce mode → VETO_CT_FREQ_CAP."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "shadow")  # CT itself is shadow
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_SHORT_REGIMES", "trending_bull")
    monkeypatch.setenv("CT_FREQ_CAP_ENABLED", "1")
    monkeypatch.setenv("CT_FREQ_CAP_PER_HOUR", "3")
    monkeypatch.setenv("CT_FREQ_CAP_MODE", "enforce")

    g = EntryPolicyGate.from_env()
    # count=4 > cap=3 → freq cap fires
    ctx = _ctx(regime="trending_bull", dwell_ms=600_000.0, redis_client=_make_freq_redis(current_count=4))
    d = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="of", side="SHORT")

    assert d.veto is True
    assert d.reason_code == "VETO_CT_FREQ_CAP"
    assert "count=4" in (d.notes or "")


def test_ct_p2_freq_cap_shadow_no_veto(monkeypatch):
    """P2.4: cap exceeded but mode=shadow → annotates but does NOT veto."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "shadow")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_SHORT_REGIMES", "trending_bull")
    monkeypatch.setenv("CT_FREQ_CAP_ENABLED", "1")
    monkeypatch.setenv("CT_FREQ_CAP_PER_HOUR", "2")
    monkeypatch.setenv("CT_FREQ_CAP_MODE", "shadow")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(regime="trending_bull", dwell_ms=600_000.0, redis_client=_make_freq_redis(current_count=10))
    d = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="of", side="SHORT")

    assert d.veto is False, "freq cap shadow → no veto"


def test_ct_p2_freq_cap_disabled_no_redis_call(monkeypatch):
    """P2.4: CT_FREQ_CAP_ENABLED=0 → freq cap method exits immediately (no Redis I/O)."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "enforce")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_SHORT_REGIMES", "trending_bull")
    monkeypatch.setenv("CT_FREQ_CAP_ENABLED", "0")

    g = EntryPolicyGate.from_env()
    # Pass a Redis stub that raises on any call to ensure no I/O occurs
    class _NeverCallRedis:
        def incr(self, *a, **kw):
            raise AssertionError("should not call Redis when freq cap disabled")
        def expire(self, *a, **kw):
            raise AssertionError("should not call Redis when freq cap disabled")
    ctx = _ctx(regime="range", dwell_ms=600_000.0, redis_client=_NeverCallRedis())
    # regime=range not in block list → no ct_hit → freq cap never evaluated anyway,
    # but even if it were, _eval_ct_freq_cap must not call Redis.
    g._eval_ct_freq_cap(ctx=ctx, symbol="BTCUSDT", side_norm="SHORT")  # type: ignore[attr-defined]
    # No AssertionError → Redis was not called ✓


# ────────────────────────────────────────────────────────────────────────────
# Canary by kind allowlist (2026-05-30 Day 2)
# COUNTER_TREND_HARD_VETO_ENFORCE_KINDS форсирует enforce только для перечисленных
# kinds; остальные kinds остаются в shadow независимо от глобального MODE.
# ────────────────────────────────────────────────────────────────────────────

def test_ct_canary_kind_in_allowlist_enforces(monkeypatch):
    """MODE=shadow + ENFORCE_KINDS=delta_spike + kind=delta_spike → VETO."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "shadow")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENFORCE_KINDS", "delta_spike")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(regime="trending_bull", dwell_ms=600_000.0)
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="delta_spike", side="SHORT")

    assert d.veto is True
    assert d.reason_code == "VETO_COUNTER_TREND_BLOCK"
    assert getattr(ctx, "_ct_canary_active", 0) == 1
    assert getattr(ctx, "_ct_mode_resolved", None) == "enforce"


def test_ct_canary_kind_outside_allowlist_stays_shadow(monkeypatch):
    """MODE=shadow + ENFORCE_KINDS=delta_spike + kind=of → annotate only, no veto."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "shadow")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENFORCE_KINDS", "delta_spike")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(regime="trending_bull", dwell_ms=600_000.0)
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="of", side="SHORT")

    assert d.veto is False
    assert getattr(ctx, "counter_trend_block_alarm", 0) == 1
    assert getattr(ctx, "_ct_canary_active", 0) == 1
    assert getattr(ctx, "_ct_mode_resolved", None) == "shadow"


def test_ct_canary_kind_in_allowlist_with_global_enforce_stays_enforce(monkeypatch):
    """MODE=enforce + ENFORCE_KINDS=delta_spike + kind=delta_spike → VETO (нормально)."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "enforce")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENFORCE_KINDS", "delta_spike")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(regime="trending_bull", dwell_ms=600_000.0)
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="delta_spike", side="SHORT")

    assert d.veto is True
    assert d.reason_code == "VETO_COUNTER_TREND_BLOCK"


def test_ct_canary_kind_outside_allowlist_with_global_enforce_downgrades(monkeypatch):
    """MODE=enforce + ENFORCE_KINDS=delta_spike + kind=of → downgrade to shadow.

    Семантика allowlist: enforce только для перечисленных kinds, остальные kinds
    защищены от случайного enforce при глобальном переключении.
    """
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "enforce")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENFORCE_KINDS", "delta_spike")

    g = EntryPolicyGate.from_env()
    ctx = _ctx(regime="trending_bull", dwell_ms=600_000.0)
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="of", side="SHORT")

    assert d.veto is False
    assert getattr(ctx, "_ct_mode_resolved", None) == "shadow"


def test_ct_canary_empty_allowlist_uses_global_mode(monkeypatch):
    """Пустой ENFORCE_KINDS → backward-compatible: используется глобальный MODE."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "enforce")
    monkeypatch.delenv("COUNTER_TREND_HARD_VETO_ENFORCE_KINDS", raising=False)

    g = EntryPolicyGate.from_env()
    ctx = _ctx(regime="trending_bull", dwell_ms=600_000.0)
    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="of", side="SHORT")

    assert d.veto is True
    assert getattr(ctx, "_ct_canary_active", 0) == 0


def test_ct_canary_multi_kind_allowlist(monkeypatch):
    """ENFORCE_KINDS=delta_spike,sweep → оба enforce, iceberg остаётся shadow."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENABLED", "1")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_MODE", "shadow")
    monkeypatch.setenv("COUNTER_TREND_HARD_VETO_ENFORCE_KINDS", "delta_spike,sweep")

    g = EntryPolicyGate.from_env()

    # delta_spike → enforce
    ctx1 = _ctx(regime="trending_bull", dwell_ms=600_000.0)
    d1 = g.evaluate(ctx=ctx1, symbol="ETHUSDT", kind="delta_spike", side="SHORT")
    assert d1.veto is True

    # sweep → enforce
    ctx2 = _ctx(regime="trending_bull", dwell_ms=600_000.0)
    d2 = g.evaluate(ctx=ctx2, symbol="ETHUSDT", kind="sweep", side="SHORT")
    assert d2.veto is True

    # iceberg → shadow (вне allowlist)
    ctx3 = _ctx(regime="trending_bull", dwell_ms=600_000.0)
    d3 = g.evaluate(ctx=ctx3, symbol="ETHUSDT", kind="iceberg", side="SHORT")
    assert d3.veto is False
    assert getattr(ctx3, "counter_trend_block_alarm", 0) == 1

