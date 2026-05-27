from __future__ import annotations

"""
EntryPolicyGate: spread shock / burst flip / cancel-to-trade + feature drift alarm.

Goals:
  - avoid cutting too many signals by default (soft mode is audit/tighten, not veto)
  - allow switching to hard mode from docker-compose via env

Modes:
  GATE_PROFILE=default|soft|strict|hard
    - default/soft: never veto by entry-policy alone; only annotate ctx and optionally tighten
    - strict: may veto on extreme spread shock (configurable)
    - hard: veto on threshold breach

Feature drift alarm:
  - tracks baseline distributions for a few features (EMA mean/absdev)
  - if drift spikes: either (a) tighten by annotating ctx, or (b) veto in hard profile
  - designed fail-open: never breaks signal publishing if Redis is down

BookTradeConsistency integration:
  - reads ctx.book_trade_consistency_stale_book_ms / book_trade_consistency_adverse_cross_bps
    annotated by DataQualityGate → BookTradeConsistencyGate
  - soft path: adds tighten_k if soft thresholds breached
  - hard path: veto on ENTRY_BOOK_STALE_HARD_MS / ENTRY_ADVERSE_CROSS_HARD_BPS
"""


import json
import math
import os
from dataclasses import dataclass
from typing import Any


def _is_async_redis(rc: Any) -> bool:
    """Detect redis.asyncio.Redis (and similar) reliably.

    `inspect.iscoroutinefunction(rc.get)` is unreliable: redis-py's async client
    methods are not declared with `async def` at the class level (they're wrapped),
    so the inspect check returns False even for async clients. We instead check the
    module name of the class (`redis.asyncio.*`).
    """
    if rc is None:
        return False
    try:
        mod = type(rc).__module__ or ""
        return "asyncio" in mod or "aioredis" in mod
    except Exception:
        return False


def _sync_redis_for_autocal(ctx: Any = None) -> Any:
    """Return a sync Redis client for calibrator persist/restore.

    Prefers ctx.redis if it is already a sync client (e.g. FakeRedis in tests);
    falls back to _get_sync_redis() when ctx.redis is async (aioredis) or None.
    """
    if ctx is not None:
        rc = getattr(ctx, "redis", None)
        if rc is not None and not _is_async_redis(rc):
            return rc
    try:
        from handlers.crypto_orderflow.config.handler_config import _get_sync_redis
        return _get_sync_redis()
    except Exception:
        return None

from domain.time_utils import normalize_ts_ms, session_from_ts_ms
from utils.time_utils import get_ny_time_millis
from handlers.crypto_orderflow.utils.drift_reader import load_drift_active_factor
from core.entry_policy_freeze import EntryPolicyFreezeV1
from core.spread_staleness_calibrator import SpreadStalenessCalibrator
from core.adverse_cross_calibrator import (
    AdverseCrossCalibrator,
    DEFAULT_ADVERSE_CROSS_SOFT_BPS,
    DEFAULT_ADVERSE_CROSS_HARD_BPS,
)
from core.burst_c2t_calibrator import BurstC2TCalibrator
from core.btc_drop_reader import get_btc_ret_5m

try:
    from prometheus_client import Counter, Gauge
    _ac_cal_soft_bps = Gauge(
        "entry_adverse_cross_cal_soft_bps",
        "Calibrated soft adverse-cross threshold per regime (q90)",
        ["regime"],
    )
    _ac_cal_hard_bps = Gauge(
        "entry_adverse_cross_cal_hard_bps",
        "Calibrated hard adverse-cross threshold per regime (q98)",
        ["regime"],
    )
    _ac_cal_n = Gauge(
        "entry_adverse_cross_cal_n",
        "Total observations fed to AdverseCrossCalibrator per regime",
        ["regime"],
    )
    _ac_cal_loss_floor = Gauge(
        "entry_adverse_cross_cal_loss_floor_active",
        "1 if precision-on-loss floor is constraining hard threshold, else 0",
        ["regime"],
    )
    _ac_veto_total = Counter(
        "entry_adverse_cross_veto_total",
        "Total VETO_TRADE_ADVERSE_CROSS decisions (hard profile)",
        ["symbol", "src"],
    )
    _btc_drop_block_long_total = Counter(
        "entry_btc_drop_block_long_total",
        "Cross-asset BTC-drop LONG-block decisions (Plan 3.4)",
        ["symbol", "mode", "decision"],
    )
    _btc_drop_block_long_ret_g = Gauge(
        "entry_btc_drop_block_long_btc_ret_5m",
        "Last observed BTC 5m fractional return at gate evaluation time",
    )
    _daily_dd_veto_total = Counter(
        "entry_daily_dd_killswitch_veto_total",
        "Total VETO_DAILY_DD_KILLSWITCH decisions emitted by EntryPolicyGate",
        ["symbol"],
    )
    _htf_long_bias_total = Counter(
        "entry_htf_long_bias_total",
        "HTF LONG bias gate decisions — Plan 1.2",
        ["symbol", "mode", "decision"],
    )
    # 2026-05-23: RR floor counter (item 3 of low-WR fix)
    _setup_rr_below_threshold = Counter(
        "setup_rr_below_threshold_total",
        "Signals with tp1/sl below ENTRY_RR_MIN floor (item 3 stop-bleed)",
        ["kind", "regime", "decision"],
    )
    # 2026-05-26 P0.7: KIND_KILL_LIST veto counter
    _kind_kill_list_total = Counter(
        "entry_kind_kill_list_veto_total",
        "Total VETO_KIND_KILL_LIST decisions emitted by EntryPolicyGate",
        ["kind", "side", "symbol"],
    )
    # 2026-05-23: maker-only candidate annotation (item 4). SHADOW until
    # order_open_service.py learns to read ctx.exec_maker_only_required.
    _exec_maker_only_required = Counter(
        "exec_maker_only_required_total",
        "Signals annotated for maker-only execution by kind",
        ["kind", "decision"],
    )
except Exception:
    Counter = Gauge = None  # type: ignore[assignment,misc]
    _ac_cal_soft_bps = _ac_cal_hard_bps = _ac_cal_n = _ac_cal_loss_floor = _ac_veto_total = None  # type: ignore[assignment]
    _btc_drop_block_long_total = None  # type: ignore[assignment]
    _btc_drop_block_long_ret_g = None  # type: ignore[assignment]
    _daily_dd_veto_total = None  # type: ignore[assignment]
    _htf_long_bias_total = None  # type: ignore[assignment]
    _setup_rr_below_threshold = None  # type: ignore[assignment]
    _kind_kill_list_total = None  # type: ignore[assignment]


@dataclass(frozen=True)
class GateDecision:
    apply: bool
    veto: bool
    reason_code: str
    notes: str = ""


def _env_bool(name: str, default: bool) -> bool:
    try:
        v = os.getenv(name, "")
        if v == "":
            return bool(default)
        return v.strip().lower() in {"1", "true", "yes", "on"}
    except Exception:
        return bool(default)


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return default
        return float(v)
    except Exception:
        return default




def _spread_bps_from_ctx(ctx: Any) -> float:
    # Prefer already computed ctx.spread_bps; fallback to bid/ask
    sp = _safe_float(getattr(ctx, "spread_bps", None), 0.0)
    if sp > 0:
        return float(sp)
    bid = _safe_float(getattr(ctx, "bid", None) or getattr(ctx, "b", None), 0.0)
    ask = _safe_float(getattr(ctx, "ask", None) or getattr(ctx, "a", None), 0.0)
    mid = _safe_float(getattr(ctx, "mid", None) or getattr(ctx, "price", None), 0.0)
    if mid > 0 and ask > 0 and bid > 0 and ask >= bid:
        return float((ask - bid) / mid * 10_000.0)
    return 0.0


def _check_freeze_active(ctx: Any, symbol: str, group: str, scenario: str) -> tuple[bool, str]:
    """
    Check if freeze is active for this symbol:group:scenario.
    Returns (is_frozen, veto_reason).
    Fail-open: any error returns (False, "").
    """
    try:
        # Use sync client: prefers ctx.redis if sync (tests), falls back to _get_sync_redis() in prod.
        redis_client = _sync_redis_for_autocal(ctx)
        if redis_client is None:
            return False, ""

        freeze_key = f"cfg:entry_policy:freeze:v1:{symbol}:{group}:{scenario}"
        raw_freeze = redis_client.get(freeze_key)
        if not raw_freeze:
            return False, ""

        fz, err = EntryPolicyFreezeV1.from_json(raw_freeze)
        if not fz or err:
            return False, ""

        if not fz.is_active():
            return False, ""

        if fz.mode != "hard":
            # shadow mode: log but don't veto
            return False, ""

        return True, f"freeze_active_since={fz.created_ts_ms} mode=hard reason={fz.reason_code}"
    except Exception:
        return False, ""


class EntryPolicyGate:
    @staticmethod
    def from_env() -> EntryPolicyGate:
        return EntryPolicyGate()

    def __init__(self) -> None:
        # Toggle
        self.enabled = _env_bool("ENTRY_POLICY_ENABLED", True)

        # Conservative defaults to avoid "cutting too many signals".
        self.spread_shock_bps = _safe_float(os.getenv("ENTRY_SPREAD_SHOCK_BPS", "35"), 35.0)
        self.spread_shock_bps_hard = _safe_float(os.getenv("ENTRY_SPREAD_SHOCK_BPS_HARD", "60"), 60.0)
        self.burst_flip_max = _safe_float(os.getenv("ENTRY_BURST_FLIP_MAX", "0.85"), 0.85)
        self.c2t_max = _safe_float(os.getenv("ENTRY_C2T_MAX", "8.0"), 8.0)

        # Feature drift (off by default) — reads pre-computed drift state from FeatureDriftAlarm
        self.drift_enabled = _env_bool("FEATURE_DRIFT_ENABLED", False)

        # Optional diagnostics stream (audit)
        self.diag_stream = (os.getenv("ENTRY_POLICY_DIAG_STREAM", "") or "")

        # BookTradeConsistencyGate integration thresholds.
        # Soft thresholds: annotate tighten_k, but do not veto.
        # Hard thresholds: only applied when GATE_PROFILE=hard.
        self.book_stale_soft_ms = _safe_float(os.getenv("ENTRY_BOOK_STALE_SOFT_MS", "600"), 600.0)
        self.book_stale_hard_ms = _safe_float(os.getenv("ENTRY_BOOK_STALE_HARD_MS", "1200"), 1200.0)
        # Static fallback defaults (overridden by calibrator when warm + enforce).
        self.adverse_cross_soft_bps = _safe_float(os.getenv("ENTRY_ADVERSE_CROSS_SOFT_BPS", "0.5"), DEFAULT_ADVERSE_CROSS_SOFT_BPS)
        self.adverse_cross_hard_bps = _safe_float(os.getenv("ENTRY_ADVERSE_CROSS_HARD_BPS", "1.5"), DEFAULT_ADVERSE_CROSS_HARD_BPS)

        # Adaptive adverse-cross calibrator (P²-quantile per symbol × session).
        # ADVERSE_CROSS_CAL_ENFORCE=1 → use calibrated thresholds; 0 (default) → shadow only.
        # ADVERSE_CROSS_CAL_MIN_SAMPLES → warmup guard (default 500).
        self._adverse_calib = AdverseCrossCalibrator(
            min_samples=int(os.getenv("ADVERSE_CROSS_CAL_MIN_SAMPLES", "500") or "500"),
            enforce=_env_bool("ADVERSE_CROSS_CAL_ENFORCE", False),
            outcome_min_losses=int(os.getenv("ADVERSE_CROSS_CAL_MIN_LOSSES", "30") or "30"),
        )
        # Snapshot throttle: HSET every ADVERSE_CROSS_CAL_SNAPSHOT_SEC seconds (default 60).
        self._ac_snap_interval_ms = int(os.getenv("ADVERSE_CROSS_CAL_SNAPSHOT_SEC", "60") or "60") * 1000
        self._ac_last_snap_ms: int = 0
        # Lazy-load flag: attempt to restore state from Redis on first evaluate() with a redis client.
        self._ac_loaded: bool = False

        # Adaptive spread/staleness calibrator (P1 autocalibrators roadmap).
        self._spread_calib = SpreadStalenessCalibrator(
            min_samples=int(os.getenv("SPREAD_CALIB_MIN_SAMPLES", "200") or "200"),
            enforce=_env_bool("SPREAD_STALENESS_CALIB_ENFORCE", False),
        )

        # Adaptive burst_flip / c2t calibrator (P2 autocalibrators roadmap).
        # auto_enforce=True: per-regime auto-apply once min_samples reached.
        self._burst_c2t_calib = BurstC2TCalibrator(
            min_samples=int(os.getenv("BURST_C2T_CAL_MIN_SAMPLES", "300") or "300"),
            enforce=_env_bool("BURST_C2T_CAL_ENFORCE", False),
            auto_enforce=_env_bool("BURST_C2T_CAL_AUTO_ENFORCE", True),
        )

        # ── Plan 1.2: HTF LONG bias gate ─────────────────────────────────────
        # Block LONG when macro indicators signal a bear regime. Uses v14_of OE
        # keys: cg_rel_strength_btc_1h / btc_ret_1m / symbol_rel_strength_vs_btc_1m
        # / market_breadth_ret_5m. Requires HTF_LONG_BIAS_REQUIRE_N or more of
        # these to be negative to fire.
        #
        # Auto-promoter: if HTF_LONG_BIAS_MODE=shadow, flips to enforce-equivalent
        # once min_hours + min_hits + min_evals criteria are met (6h / 50 hits /
        # 200 evals by default). Manual rollback: HDEL autocal:htf_long_bias:state global.
        #
        # ENV knobs:
        #   HTF_LONG_BIAS_ENABLED            master switch                (default 0)
        #   HTF_LONG_BIAS_MODE               shadow|enforce               (default shadow)
        #   HTF_LONG_BIAS_REQUIRE_N          min bear indicators to fire  (default 2)
        #   HTF_LONG_BIAS_AUTO_PROMOTE       enable auto-promoter         (default 1)
        #   HTF_LONG_BIAS_AUTO_PROMOTE_MIN_HOURS   (default 6)
        #   HTF_LONG_BIAS_AUTO_PROMOTE_MIN_HITS    (default 50)
        #   HTF_LONG_BIAS_AUTO_PROMOTE_MIN_EVALS   (default 200)
        self.htf_long_bias_enabled = _env_bool("HTF_LONG_BIAS_ENABLED", False)
        self.htf_long_bias_mode = (
            os.getenv("HTF_LONG_BIAS_MODE", "shadow") or "shadow"
        ).strip().lower()
        self.htf_long_bias_require_n = max(1, int(os.getenv("HTF_LONG_BIAS_REQUIRE_N", "2") or "2"))
        _ap_enabled = _env_bool("HTF_LONG_BIAS_AUTO_PROMOTE", True) and self.htf_long_bias_enabled
        from core.htf_long_bias_autopromoter import HtfLongBiasAutoPromoter
        self._htf_long_bias_promoter = HtfLongBiasAutoPromoter(
            enabled=_ap_enabled,
            min_hours=float(os.getenv("HTF_LONG_BIAS_AUTO_PROMOTE_MIN_HOURS", "6") or "6"),
            min_hits=int(os.getenv("HTF_LONG_BIAS_AUTO_PROMOTE_MIN_HITS", "50") or "50"),
            min_evals=int(os.getenv("HTF_LONG_BIAS_AUTO_PROMOTE_MIN_EVALS", "200") or "200"),
        )

        # ── Plan 3.4 Cross-asset correlation gate ─────────────────────────────
        # Block LONG on alts (ETH/SOL/...) when BTC drops ≥ |threshold| over the
        # last 5 min. Primary source: ctx.indicators["btc_ret_5m"] (v14_of OE);
        # fallback: tick-stream reader (core.btc_drop_reader). BTCUSDT itself is
        # exempt by default — a BTC crash is a different setup, not an alt-block.
        #
        # ENV knobs:
        #   BTC_DROP_BLOCK_LONG_ENABLED  master switch                (default 0)
        #   BTC_DROP_BLOCK_LONG_MODE     shadow|enforce               (default enforce)
        #   BTC_DROP_BLOCK_LONG_PCT_5M   fractional drop threshold    (default -0.01 = -1%)
        #   BTC_DROP_BLOCK_LONG_EXEMPT   csv of symbols never blocked (default BTCUSDT)
        self.btc_drop_block_long_enabled = _env_bool("BTC_DROP_BLOCK_LONG_ENABLED", False)
        self.btc_drop_block_long_mode = (
            os.getenv("BTC_DROP_BLOCK_LONG_MODE", "enforce") or "enforce"
        ).strip().lower()
        self.btc_drop_block_long_pct_5m = _safe_float(
            os.getenv("BTC_DROP_BLOCK_LONG_PCT_5M", "-0.01"), -0.01
        )
        _exempt_raw = (os.getenv("BTC_DROP_BLOCK_LONG_EXEMPT", "BTCUSDT") or "").strip()
        self.btc_drop_block_long_exempt: frozenset[str] = frozenset(
            s.strip().upper() for s in _exempt_raw.split(",") if s.strip()
        )

    # ── adverse-cross calibrator persistence ──────────────────────────────────

    def snapshot_to_redis(self, redis: Any, now_ms: int) -> None:
        """Write per-regime calibrator state to Redis HSET (best-effort)."""
        from core.redis_keys import RK  # local to avoid circular import at module level
        try:
            for regime_key in list(self._adverse_calib._n.keys()):
                sym_part = regime_key.split(":")[0].upper()
                state = self._adverse_calib.dump_regime_state(
                    symbol=sym_part, regime=regime_key, updated_ts_ms=now_ms,
                )
                redis.hset(RK.AUTOCAL_ADVERSE_CROSS, regime_key, json.dumps(state))
        except Exception:
            pass
        try:
            for regime_key in list(self._spread_calib._n.keys()):
                sym_part = regime_key.split(":")[0].upper()
                state = self._spread_calib.dump_regime_state(
                    symbol=sym_part, regime=regime_key, updated_ts_ms=now_ms,
                )
                redis.hset(RK.AUTOCAL_SPREAD_STALENESS, regime_key, json.dumps(state))
        except Exception:
            pass
        try:
            for regime_key in list(self._burst_c2t_calib._n.keys()):
                sym_part = regime_key.split(":")[0].upper()
                state = self._burst_c2t_calib.dump_regime_state(
                    symbol=sym_part, regime=regime_key, updated_ts_ms=now_ms,
                )
                redis.hset(RK.AUTOCAL_BURST_C2T, regime_key, json.dumps(state))
        except Exception:
            pass
        try:
            mapping = self._htf_long_bias_promoter.dump_all()
            if mapping:
                redis.hset(RK.AUTOCAL_HTF_LONG_BIAS, mapping=mapping)
        except Exception:
            pass

    def load_from_redis(self, redis: Any) -> None:
        """Restore per-regime calibrator state from Redis HGETALL (best-effort)."""
        from core.redis_keys import RK
        try:
            raw_map = redis.hgetall(RK.AUTOCAL_ADVERSE_CROSS)
            if raw_map:
                for raw_val in raw_map.values():
                    if isinstance(raw_val, (bytes, bytearray)):
                        raw_val = raw_val.decode("utf-8", "ignore")
                    state = AdverseCrossCalibrator.loads(raw_val)
                    if state:
                        self._adverse_calib.load_regime_state(state)
        except Exception:
            pass
        try:
            from core.spread_staleness_calibrator import SpreadStalenessCalibrator
            raw_map = redis.hgetall(RK.AUTOCAL_SPREAD_STALENESS)
            if raw_map:
                for raw_val in raw_map.values():
                    if isinstance(raw_val, (bytes, bytearray)):
                        raw_val = raw_val.decode("utf-8", "ignore")
                    state = SpreadStalenessCalibrator.loads(raw_val)
                    if state:
                        self._spread_calib.load_regime_state(state)
        except Exception:
            pass
        try:
            raw_map = redis.hgetall(RK.AUTOCAL_BURST_C2T)
            if raw_map:
                for raw_val in raw_map.values():
                    if isinstance(raw_val, (bytes, bytearray)):
                        raw_val = raw_val.decode("utf-8", "ignore")
                    try:
                        state = json.loads(raw_val) if isinstance(raw_val, str) else None
                    except Exception:
                        state = None
                    if state:
                        self._burst_c2t_calib.load_regime_state(state)
        except Exception:
            pass
        try:
            raw_map = redis.hgetall(RK.AUTOCAL_HTF_LONG_BIAS)
            if raw_map:
                self._htf_long_bias_promoter.load_mapping(raw_map)
        except Exception:
            pass

    def _resolve_side(self, *, ctx: Any, side: Any) -> str:
        """Normalize side/direction → 'LONG' | 'SHORT' | ''. Reads ctx fallback when arg empty."""
        cand: Any = side
        if cand is None or (isinstance(cand, str) and not cand.strip()):
            cand = (
                getattr(ctx, "direction", None)
                or getattr(ctx, "side", None)
                or getattr(ctx, "side_str", None)
            )
        if isinstance(cand, str):
            s = cand.strip().upper()
            if s in {"LONG", "BUY"}:
                return "LONG"
            if s in {"SHORT", "SELL"}:
                return "SHORT"
            return ""
        if isinstance(cand, (int, float)):
            try:
                v = float(cand)
            except Exception:
                return ""
            if v > 0:
                return "LONG"
            if v < 0:
                return "SHORT"
        return ""

    def _eval_btc_drop_block_long(
        self, *, ctx: Any, symbol: str, side_norm: str,
    ) -> tuple[bool, float | None, str]:
        """Plan 3.4: block LONG on alts when BTC fell ≥ |threshold| over last 5m.

        Returns (hit, btc_ret_5m, notes). hit=True only when side_norm == "LONG",
        symbol is not in the exempt list, and a valid btc_ret_5m sample is
        available and ≤ threshold. Reads ctx.indicators first (treats exact 0.0
        as missing), then falls back to core.btc_drop_reader. Fail-open on any
        exception.
        """
        if not self.btc_drop_block_long_enabled or side_norm != "LONG":
            return False, None, ""
        sym_u = (symbol or "").strip().upper()
        if sym_u in self.btc_drop_block_long_exempt:
            return False, None, ""

        # Primary: v14_of OE indicator (already in ctx). Treat exact 0.0 as
        # "missing" (warm-up / not yet populated) to avoid false negatives.
        btc_ret: float | None = None
        ind = getattr(ctx, "indicators", None)
        if isinstance(ind, dict):
            raw = ind.get("btc_ret_5m")
            if raw is not None:
                try:
                    v = float(raw)
                    if math.isfinite(v) and v != 0.0:
                        btc_ret = v
                except Exception:
                    btc_ret = None

        src = "ctx"
        if btc_ret is None:
            try:
                btc_ret = get_btc_ret_5m()
                src = "reader"
            except Exception:
                btc_ret = None

        if btc_ret is None or not math.isfinite(btc_ret):
            return False, None, ""

        hit = btc_ret <= self.btc_drop_block_long_pct_5m
        if not hit:
            return False, btc_ret, ""
        notes = (
            f"btc_ret_5m={btc_ret:.5f}<=thr={self.btc_drop_block_long_pct_5m:.5f} "
            f"src={src}"
        )
        return True, btc_ret, notes[:256]

    def _eval_htf_long_bias(
        self, *, ctx: Any, side_norm: str,
    ) -> tuple[bool, str]:
        """Plan 1.2: block LONG in macro-bear via v14_of OE rel_strength indicators.

        Returns (hit, notes). hit=True only when side_norm == 'LONG', the gate is
        enabled, and HTF_LONG_BIAS_REQUIRE_N or more bear indicators are negative.
        Fail-open on any exception.
        """
        if not self.htf_long_bias_enabled or side_norm != "LONG":
            return False, ""
        try:
            ind = getattr(ctx, "indicators", None) or {}
            BEAR_CHECKS = [
                ("cg_rel_strength_btc_1h", "cg_rel_str<0"),
                ("btc_ret_1m", "btc_ret_1m<0"),
                ("symbol_rel_strength_vs_btc_1m", "sym_rel_str<0"),
                ("market_breadth_ret_5m", "breadth<0"),
            ]
            fired = []
            for key, label in BEAR_CHECKS:
                raw = ind.get(key)
                if raw is None:
                    continue
                try:
                    v = float(raw)
                    if math.isfinite(v) and v < 0:
                        fired.append(label)
                except Exception:
                    pass
            if len(fired) < self.htf_long_bias_require_n:
                return False, ""
            notes = f"n={len(fired)}/{self.htf_long_bias_require_n} fired={','.join(fired)}"
            return True, notes[:256]
        except Exception:
            return False, ""

    def evaluate(self, *, ctx: Any, symbol: str, kind: str, side: Any = "") -> GateDecision:
        if not self.enabled:
            return GateDecision(False, False, "OK", "disabled")

        # Lazy-load calibrator state from Redis on first call.
        # Prefers ctx.redis if sync (tests); falls back to _get_sync_redis() in prod.
        if not self._ac_loaded:
            _sync_rc = _sync_redis_for_autocal(ctx)
            if _sync_rc is not None:
                self.load_from_redis(_sync_rc)
            self._ac_loaded = True

        # P0: Check if entry policy freeze (shadow or hard) is active for this symbol:group:scenario
        ab_group = str(getattr(ctx, "ab_group", None) or "default").lower()
        scenario = str(getattr(ctx, "scenario", None) or "na").lower()
        is_frozen, freeze_reason = _check_freeze_active(ctx, symbol.upper(), ab_group, scenario)
        if is_frozen:
            return GateDecision(True, True, "VETO_FREEZE_ACTIVE", freeze_reason)

        # P0: Daily equity-drawdown kill-switch. Account-wide (NOT symbol-scoped).
        # Reader is fail-open; vetoes only when daily-dd service has kill_armed=1 AND mode=enforce.
        try:
            from services.daily_dd_reader import is_armed as _daily_dd_is_armed
            dd_armed, dd_reason = _daily_dd_is_armed(ctx)
        except Exception:
            dd_armed, dd_reason = False, ""
        if dd_armed:
            try:
                if _daily_dd_veto_total is not None:
                    _daily_dd_veto_total.labels(symbol=symbol.upper()).inc()
            except Exception:
                pass
            return GateDecision(True, True, "VETO_DAILY_DD_KILLSWITCH", dd_reason or "daily_dd_breach")

        # ── 2026-05-26 P0.7: KIND_KILL_LIST circuit breaker ──────────────────
        # CSV токенов kind[:SIDE[:SYMBOL]] (case-insensitive). Match-семантика:
        # длиннее = специфичнее. Чтобы временно блокировать (kind,side,symbol)
        # bucket с плохой WR. Rollback: убрать токен.
        try:
            _kill_csv = os.getenv("KIND_KILL_LIST") or ""
            if _kill_csv.strip():
                _kind_l = str(kind or "").strip().lower()
                _side_l = str(side or "").strip().upper()
                _sym_l = str(symbol or "").strip().upper()
                for _tok in _kill_csv.split(","):
                    _tok = _tok.strip()
                    if not _tok:
                        continue
                    _parts = [p.strip() for p in _tok.split(":")]
                    _k = (_parts[0] if len(_parts) > 0 else "").lower()
                    _s = (_parts[1] if len(_parts) > 1 else "").upper()
                    _y = (_parts[2] if len(_parts) > 2 else "").upper()
                    if _k and _k != _kind_l:
                        continue
                    if _s and _s != _side_l:
                        continue
                    if _y and _y != _sym_l:
                        continue
                    try:
                        if _kind_kill_list_total is not None:
                            _kind_kill_list_total.labels(
                                kind=_kind_l, side=_side_l or "NA", symbol=_sym_l,
                            ).inc()
                    except Exception:
                        pass
                    return GateDecision(
                        True, True, "VETO_KIND_KILL_LIST",
                        f"matched={_tok} (kind={_kind_l} side={_side_l} sym={_sym_l})",
                    )
        except Exception:
            pass  # fail-open

        # ── 2026-05-23 RR floor (item 3 stop-bleed) ──────────────────────────
        # Bounded-SL floor в low-vol режиме коллапсирует RR до ~1.0, при котором
        # WR breakeven > 55% (с 8 bps round-trip fees). Vето сигналы с tp1/sl < min.
        # Default: SHADOW (только инкремент counter, no veto).
        if _env_bool("ENTRY_RR_MIN_ENABLED", False):
            try:
                ind = getattr(ctx, "indicators", None) or {}
                _tp = _safe_float(
                    ind.get("liqmap_gate_reward_bps")
                    or ind.get("tp1_bps")
                    or ind.get("pred_tp1_bps")
                    or getattr(ctx, "tp1_bps", None),
                    0.0,
                )
                _sl = _safe_float(
                    ind.get("liqmap_gate_risk_bps")
                    or ind.get("sl_bps")
                    or getattr(ctx, "sl_bps", None),
                    0.0,
                )
                _rr_min = _safe_float(os.getenv("ENTRY_RR_MIN", "1.3"), 1.3)
                _rr = (_tp / _sl) if _sl > 0 else 0.0
                _rr_breach = _tp > 0 and _sl > 0 and _rr < _rr_min
                if _rr_breach:
                    _enforce_bin = _env_bool("ENTRY_RR_MIN_ENFORCE", False)
                    # 2026-05-26 P0.3: canary % via hash(sid) % 100.
                    # Sticky per-sid: тот же sid всегда попадает или нет в canary.
                    _canary_pct = _safe_float(os.getenv("ENTRY_RR_MIN_ENFORCE_CANARY_PCT", "0"), 0.0)
                    _in_canary = False
                    if _canary_pct > 0.0:
                        try:
                            _sid_for_hash = str(getattr(ctx, "sid", "") or getattr(ctx, "signal_id", "") or "")
                            if _sid_for_hash:
                                import hashlib as _hl
                                _h = int(_hl.md5(_sid_for_hash.encode("utf-8")).hexdigest()[:8], 16)
                                _in_canary = (_h % 100) < int(_canary_pct)
                        except Exception:
                            _in_canary = False
                    _enforce = _enforce_bin or _in_canary
                    _reg = (ind.get("regime") or "unknown").lower()
                    try:
                        if _setup_rr_below_threshold is not None:
                            _setup_rr_below_threshold.labels(
                                kind=str(kind), regime=_reg,
                                decision="VETO" if _enforce else "SHADOW",
                            ).inc()
                    except Exception:
                        pass
                    try:
                        ctx.rr_below_min_shadow = 1
                        ctx.rr_value = _rr
                    except Exception:
                        pass
                    if _enforce:
                        return GateDecision(
                            True, True, "VETO_RR_BELOW_MIN",
                            f"rr={_rr:.2f} < min={_rr_min:.2f} tp={_tp:.1f}bps sl={_sl:.1f}bps",
                        )
            except Exception:
                pass  # fail-open

        # ── 2026-05-23 item 4: maker-only candidate annotation ───────────────
        # Сейчас 287% fees от gross profit при taker entry. Помечаем continuation
        # для maker-only execution. SHADOW: только ctx.exec_maker_only_required=1
        # + counter. ENFORCE требует patch в order_open_service (отдельный PR).
        try:
            _mo_kinds = {
                k.strip().lower()
                for k in (os.getenv("EXEC_MAKER_ONLY_KINDS") or "").split(",")
                if k.strip()
            }
            _kind_lc = str(kind).lower()
            if _mo_kinds and _kind_lc in _mo_kinds:
                # Two-level enforce gate:
                #   EXEC_MAKER_ONLY_ENFORCE        — global on/off
                #   EXEC_MAKER_ONLY_KINDS_ENFORCE  — CSV canary subset; when set,
                #     only kinds listed here flip from SHADOW to ENFORCE.
                #     Empty/unset → all matched kinds enforce (when global=1).
                # Canary rollout (2026-05-24): start with "iceberg" alone, watch
                # exec_maker_only_required_total{decision="ENFORCE"} + fill rate +
                # MAKER_TIMEOUT events for 24-48h before expanding the list.
                _global_enforce = _env_bool("EXEC_MAKER_ONLY_ENFORCE", False)
                _canary_csv = (os.getenv("EXEC_MAKER_ONLY_KINDS_ENFORCE") or "").strip()
                if _canary_csv:
                    _canary_kinds = {
                        k.strip().lower() for k in _canary_csv.split(",") if k.strip()
                    }
                    _mo_enforce = _global_enforce and _kind_lc in _canary_kinds
                else:
                    _mo_enforce = _global_enforce
                try:
                    ctx.exec_maker_only_required = 1
                    ctx.exec_maker_only_enforce = 1 if _mo_enforce else 0
                except Exception:
                    pass
                try:
                    if _exec_maker_only_required is not None:
                        _exec_maker_only_required.labels(
                            kind=str(kind),
                            decision="ENFORCE" if _mo_enforce else "SHADOW",
                        ).inc()
                except Exception:
                    pass
        except Exception:
            pass

        profile = (os.getenv("GATE_PROFILE", "") or "").strip().lower()
        if profile in {"", "normal"}:
            # FEATURE_DRIFT_PROFILE=soft|tighten|hard maps to GATE_PROFILE conventions
            _fdp = (os.getenv("FEATURE_DRIFT_PROFILE", "") or "").strip().lower()
            if _fdp == "hard":
                profile = "hard"
            elif _fdp in {"tighten", "strict"}:
                profile = "strict"
            else:
                profile = "default"

        # Strict timestamp normalization (single source of truth)
        ts_raw = getattr(ctx, "ts_ms", None) or getattr(ctx, "ts", None) or 0
        tsm = int(normalize_ts_ms(ts_raw))
        sess = "na"
        if tsm > 0:
            try:
                sess = str(getattr(ctx, "session", None) or session_from_ts_ms(tsm) or "na")
            except Exception:
                sess = "na"

        spread_bps = _spread_bps_from_ctx(ctx)
        burst_flip = _safe_float(
            getattr(ctx, "burst_flip_ratio", None)
            or getattr(ctx, "burst_flip", None)
            or getattr(ctx, "flip_ratio", None),
            0.0,
        )
        c2t = _safe_float(
            getattr(ctx, "cancel_to_trade", None)
            or getattr(ctx, "cancel_to_trade_ratio", None)
            or getattr(ctx, "c2t_ratio", None),
            0.0,
        )

        # Adaptive burst_flip / c2t thresholds (auto_enforce after warmup)
        _bct_regime = f"{(symbol or '').lower()}:{sess}"
        self._burst_c2t_calib.observe(regime=_bct_regime, burst_flip=burst_flip, c2t=c2t)
        _bct = self._burst_c2t_calib.thresholds(
            regime=_bct_regime,
            default_burst_flip=self.burst_flip_max,
            default_c2t=self.c2t_max,
        )

        soft_flags = []
        if spread_bps > 0 and spread_bps >= self.spread_shock_bps:
            soft_flags.append(f"spread_shock={spread_bps:.1f}bps")
        if burst_flip > 0 and burst_flip >= _bct.burst_flip_max:
            soft_flags.append(f"burst_flip={burst_flip:.3f}")
        if c2t > 0 and c2t >= _bct.c2t_max:
            soft_flags.append(f"c2t={c2t:.3f}")

        # Plan 3.4: Cross-asset BTC-drop LONG-block on alts.
        side_norm = self._resolve_side(ctx=ctx, side=side)
        btc_drop_hit, btc_drop_ret, btc_drop_notes = self._eval_btc_drop_block_long(
            ctx=ctx, symbol=symbol, side_norm=side_norm,
        )
        try:
            if _btc_drop_block_long_ret_g is not None and btc_drop_ret is not None:
                _btc_drop_block_long_ret_g.set(btc_drop_ret)  # type: ignore[union-attr]
        except Exception:
            pass
        if btc_drop_hit:
            soft_flags.append(f"btc_drop_block_long:{btc_drop_notes}")

        # Plan 1.2: evaluate + observe early so state is captured in the throttled
        # Redis snapshot that runs below (before the veto decision path).
        htf_hit, htf_notes = self._eval_htf_long_bias(ctx=ctx, side_norm=side_norm)
        htf_promoted = False
        if side_norm == "LONG" and self.htf_long_bias_enabled:
            self._htf_long_bias_promoter.observe(symbol=None, hit=htf_hit, now_ms=tsm)
            htf_promoted = self._htf_long_bias_promoter.is_promoted(symbol=None, now_ms=tsm)

        # Feature drift alarm (fail-open): reads pre-computed result from FeatureDriftAlarm.
        drift_hit = False
        drift_notes = ""
        drift_factor = 1.0
        drift_score = float("nan")
        drift_feat = ""
        if self.drift_enabled:
            try:
                # Use sync client: drift_reader does plain hgetall(); ctx.redis may be async.
                redis_client = _sync_redis_for_autocal(ctx)
                venue = str(getattr(ctx, "venue", None) or "na").lower()
                tf = str(getattr(ctx, "tf", None) or getattr(ctx, "timeframe", None) or "na").lower()
                drift_factor, drift_score, drift_feat = load_drift_active_factor(
                    redis_client,
                    symbol=symbol.upper(),
                    venue=venue,
                    session=sess,
                    tf=tf,
                    kind=kind,
                )
                if not math.isfinite(drift_factor) or drift_factor <= 0:
                    drift_factor = 1.0
                if drift_factor > 1.0:
                    drift_hit = True
                    drift_notes = f"factor={drift_factor:.3f} score={drift_score:.2f} feat={drift_feat}"
            except Exception:
                drift_hit = False
                drift_factor = 1.0

        # =====================================================================
        # BookTradeConsistencyGate soft tighten path.
        # Reads fields annotated by DataQualityGate (via BookTradeConsistencyGate.
        # evaluate()) on the same ctx object. Never fails — all reads are guarded.
        # =====================================================================
        btc_stale_ms = _safe_float(getattr(ctx, 'book_trade_consistency_stale_book_ms', None), 0.0)
        btc_cross_bps = _safe_float(getattr(ctx, 'book_trade_consistency_adverse_cross_bps', None), 0.0)

        # Adaptive adverse-cross calibrator: observe every tick, get per-regime thresholds.
        regime_key = f"{symbol.lower()}:{sess}"
        ac_soft_bps = self.adverse_cross_soft_bps
        ac_hard_bps = self.adverse_cross_hard_bps
        ac_src = "static"
        ac_n = 0
        ac_loss_floor = False
        try:
            self._adverse_calib.observe(regime=regime_key, cross_bps=btc_cross_bps)
            _ac_th = self._adverse_calib.thresholds(
                regime=regime_key,
                default_soft=self.adverse_cross_soft_bps,
                default_hard=self.adverse_cross_hard_bps,
            )
            ac_soft_bps = _ac_th.adverse_cross_soft_bps
            ac_hard_bps = _ac_th.adverse_cross_hard_bps
            ac_src = _ac_th.src
            ac_n = _ac_th.n
            ac_loss_floor = _ac_th.loss_floor_active
        except Exception:
            pass
        try:
            if _ac_cal_soft_bps is not None:
                _ac_cal_soft_bps.labels(regime=regime_key).set(ac_soft_bps)
                _ac_cal_hard_bps.labels(regime=regime_key).set(ac_hard_bps)  # type: ignore[union-attr]
                _ac_cal_n.labels(regime=regime_key).set(ac_n)  # type: ignore[union-attr]
                _ac_cal_loss_floor.labels(regime=regime_key).set(1 if ac_loss_floor else 0)  # type: ignore[union-attr]
        except Exception:
            pass
        # Throttled Redis snapshot: HSET every _ac_snap_interval_ms milliseconds.
        # Throttled Redis snapshot: HSET every _ac_snap_interval_ms milliseconds.
        # Prefers ctx.redis if sync (tests); falls back to _get_sync_redis() in prod.
        if tsm > 0 and (tsm - self._ac_last_snap_ms) >= self._ac_snap_interval_ms:
            _snap_rc = _sync_redis_for_autocal(ctx)
            if _snap_rc is not None:
                self.snapshot_to_redis(_snap_rc, tsm)
                self._ac_last_snap_ms = tsm

        # Adaptive spread/staleness calibrator: observe + get per-(symbol×session) budgets.
        ss_spread_soft = self.spread_shock_bps
        ss_spread_hard = self.spread_shock_bps_hard
        ss_stale_soft = self.book_stale_soft_ms
        ss_stale_hard = self.book_stale_hard_ms
        ss_src = "static"
        ss_n = 0
        try:
            self._spread_calib.observe(
                regime=regime_key, spread_bps=spread_bps, book_age_ms=btc_stale_ms,
            )
            _ss_th = self._spread_calib.thresholds(
                regime=regime_key,
                default_spread_shock_bps=self.spread_shock_bps,
                default_spread_shock_bps_hard=self.spread_shock_bps_hard,
                default_book_stale_soft_ms=self.book_stale_soft_ms,
                default_book_stale_hard_ms=self.book_stale_hard_ms,
            )
            ss_spread_soft = _ss_th.spread_shock_bps
            ss_spread_hard = _ss_th.spread_shock_bps_hard
            ss_stale_soft = _ss_th.book_stale_soft_ms
            ss_stale_hard = _ss_th.book_stale_hard_ms
            ss_src = _ss_th.src
            ss_n = _ss_th.n
        except Exception:
            pass

        btc_soft_hit = (
            (btc_stale_ms > 0 and btc_stale_ms >= ss_stale_soft)
            or (btc_cross_bps > 0 and btc_cross_bps >= ac_soft_bps)
        )
        if btc_soft_hit:
            soft_flags.append(
                f"book_consistency:stale={btc_stale_ms:.0f}ms,cross={btc_cross_bps:.3f}bps"
            )

        # Annotate ctx for downstream tightening (EdgeCostGate multiplies K)
        try:
            if soft_flags:
                ctx.entry_policy_flags = list(soft_flags)
                # Mild in default/soft; stronger in strict/hard.
                ctx.entry_policy_tighten_k = 1.1 if profile in {"default", "soft"} else 1.25
            if drift_hit:
                ctx.feature_drift_alarm = 1
                ctx.feature_drift_notes = drift_notes[:256]
                ctx.feature_drift_tighten_k = 1.15 if profile in {"default", "soft"} else 1.35
                ctx.feature_drift_factor = drift_factor
                ctx.feature_drift_score = drift_score if math.isfinite(drift_score) else -1.0
                ctx.feature_drift_feat = drift_feat
            # Expose calibrated adverse-cross budgets for observability / downstream.
            ctx.adverse_cross_cal_soft_bps = ac_soft_bps
            ctx.adverse_cross_cal_hard_bps = ac_hard_bps
            ctx.adverse_cross_cal_src = ac_src
            ctx.adverse_cross_cal_n = ac_n
            # Phase 2 feed prep: persist raw adverse cross at entry time so that
            # a trades:closed consumer can call observe_outcome() with this value.
            ctx.adverse_cross_bps_at_entry = btc_cross_bps
            # Expose calibrated spread/staleness budgets.
            ctx.ss_cal_spread_soft = ss_spread_soft
            ctx.ss_cal_spread_hard = ss_spread_hard
            ctx.ss_cal_stale_soft = ss_stale_soft
            ctx.ss_cal_stale_hard = ss_stale_hard
            ctx.ss_cal_src = ss_src
            ctx.ss_cal_n = ss_n
            # Plan 3.4: cross-asset BTC-drop block-LONG observability.
            if btc_drop_hit:
                ctx.btc_drop_block_long_alarm = 1
                ctx.btc_drop_block_long_notes = btc_drop_notes
                ctx.btc_drop_block_long_mode = self.btc_drop_block_long_mode
                if btc_drop_ret is not None:
                    ctx.btc_drop_block_long_btc_ret_5m = btc_drop_ret
        except Exception:
            pass

        # Optional audit stream (never affects decision)
        if self.diag_stream:
            try:
                redis_client = getattr(ctx, "redis", None)
                if redis_client is not None and (soft_flags or drift_hit):
                    ev = {
                        "ts_ms": get_ny_time_millis(),
                        "symbol": symbol,
                        "kind": str(kind),
                        "session": str(sess),
                        "spread_bps": float(spread_bps),
                        "burst_flip_ratio": float(burst_flip),
                        "cancel_to_trade": float(c2t),
                        "soft_flags": soft_flags,
                        "drift": int(drift_hit),
                        "drift_factor": float(drift_factor),
                        "drift_score": float(drift_score) if math.isfinite(drift_score) else -1.0,
                        "drift_feat": drift_feat[:64],
                        "drift_notes": drift_notes[:256],
                        "profile": profile,
                        "btc_stale_ms": float(btc_stale_ms),
                        "btc_cross_bps": float(btc_cross_bps),
                        "ac_soft_bps": ac_soft_bps,
                        "ac_hard_bps": ac_hard_bps,
                        "ac_src": ac_src,
                        "ac_n": ac_n,
                    }
                    redis_client.xadd(self.diag_stream, {"data": json.dumps(ev, ensure_ascii=False)}, maxlen=50000, approximate=True)
            except Exception:
                pass

        # Plan 3.4: Cross-asset BTC-drop LONG-block path (runs BEFORE profile-based
        # gating so it can veto even when GATE_PROFILE=default/soft). Single ENV
        # switch shadow|enforce — no auto-promoter.
        if btc_drop_hit:
            mode = self.btc_drop_block_long_mode
            if mode == "enforce":
                try:
                    if _btc_drop_block_long_total is not None:
                        _btc_drop_block_long_total.labels(
                            symbol=symbol, mode=mode, decision="VETO",
                        ).inc()
                except Exception:
                    pass
                return GateDecision(True, True, "VETO_BTC_DROP_BLOCK_LONG", btc_drop_notes)
            else:
                try:
                    if _btc_drop_block_long_total is not None:
                        _btc_drop_block_long_total.labels(
                            symbol=symbol, mode=mode, decision="SHADOW",
                        ).inc()
                except Exception:
                    pass

        # Plan 1.2: HTF LONG bias veto (evaluate + observe already ran above the
        # snapshot block; htf_hit / htf_promoted are already computed).
        if htf_hit:
            try:
                ctx.htf_long_bias_alarm = 1
                ctx.htf_long_bias_notes = htf_notes
                ctx.htf_long_bias_promoted = htf_promoted
            except Exception:
                pass
            htf_enforce = self.htf_long_bias_mode == "enforce" or htf_promoted
            try:
                if _htf_long_bias_total is not None:
                    _htf_long_bias_total.labels(
                        symbol=symbol,
                        mode=self.htf_long_bias_mode,
                        decision="VETO" if htf_enforce else "SHADOW",
                    ).inc()
            except Exception:
                pass
            if htf_enforce:
                return GateDecision(True, True, "VETO_HTF_LONG_BIAS_BEAR", htf_notes)

        # ── Phase E: Risk Overlay (portfolio_heat / correlation / consec_loss) ─
        # Fail-open: любая ошибка импорта или вычисления → пропускаем сигнал.
        # Shadow default (RISK_OVERLAY_ENFORCE=0): только аннотирует ctx.
        # Enforce (RISK_OVERLAY_ENFORCE=1): veto с reason_code из overlay.
        if _env_bool("RISK_OVERLAY_ENABLED", True):
            try:
                from services.risk_overlay_v1 import (
                    OpenPositionInfo,
                    RecentTradeOutcome,
                    RiskLimits,
                    evaluate_risk_overlay,
                )
                _ro_limits = RiskLimits.from_env()
                # open_positions — подставляется trade_monitor через ctx.open_positions
                _ro_positions: list[OpenPositionInfo] = []
                for _p in (getattr(ctx, "open_positions", None) or []):
                    if isinstance(_p, dict):
                        _ro_positions.append(OpenPositionInfo(
                            symbol=str(_p.get("symbol", symbol)),
                            notional_usd=_safe_float(_p.get("notional_usd"), 0.0),
                            unrealized_r=_safe_float(_p.get("unrealized_r"), 0.0),
                        ))
                # recent_outcomes — подставляется pipeline через ctx.recent_outcomes
                _ro_recent: list[RecentTradeOutcome] = []
                for _t in (getattr(ctx, "recent_outcomes", None) or []):
                    if isinstance(_t, dict):
                        _ro_recent.append(RecentTradeOutcome(
                            symbol=str(_t.get("symbol", "")),
                            bucket=str(_t.get("bucket", "")),
                            r_multiple=_safe_float(_t.get("r_multiple"), 0.0),
                            ts_ms=int(_t.get("ts_ms", 0)),
                        ))
                _ro_notional = _safe_float(
                    getattr(ctx, "position_notional_usd", None)
                    or getattr(ctx, "notional_usd", None),
                    100.0,
                )
                _ro_bucket = f"{symbol}|{kind}"
                _ro_decision = evaluate_risk_overlay(
                    symbol=symbol,
                    bucket=_ro_bucket,
                    open_positions=_ro_positions,
                    new_position_notional_usd=_ro_notional,
                    recent_outcomes=_ro_recent,
                    now_ms=tsm if tsm > 0 else 1,
                    limits=_ro_limits,
                )
                # Аннотируем ctx для observability всегда
                try:
                    ctx.risk_overlay_heat_r = _ro_decision.portfolio_heat_r
                    ctx.risk_overlay_group = _ro_decision.correlation_group
                    ctx.risk_overlay_group_notional = _ro_decision.group_notional_usd
                    ctx.risk_overlay_consec_losses = _ro_decision.consec_losses
                    ctx.risk_overlay_shadow = _ro_decision.shadow
                    if _ro_decision.reason_code:
                        ctx.risk_overlay_breach = _ro_decision.reason_code
                except Exception:
                    pass
                if _ro_decision.veto:
                    return GateDecision(
                        True, True,
                        _ro_decision.reason_code or "VETO_RISK_OVERLAY",
                        f"heat={_ro_decision.portfolio_heat_r:.2f} "
                        f"group={_ro_decision.correlation_group} "
                        f"consec={_ro_decision.consec_losses}",
                    )
            except Exception:
                pass  # fail-open

        # Decision policy:
        #   default/soft: do not veto (не режем поток)
        #   strict: veto only on extreme spread shock
        #   hard: veto on policy flags and/or drift and/or book-trade consistency
        if profile in {"default", "soft"}:
            return GateDecision(True, False, "OK", "audit_only")

        if spread_bps > 0 and spread_bps >= ss_spread_hard:
            return GateDecision(True, True, "VETO_SPREAD_SHOCK", f"spread_bps={spread_bps:.1f} >= hard={ss_spread_hard:.1f} src={ss_src}")

        if profile == "hard":
            # BookTradeConsistency hard veto checks (only in hard profile).
            if btc_stale_ms > 0 and ss_stale_hard > 0 and btc_stale_ms >= ss_stale_hard:
                return GateDecision(
                    True, True, "VETO_BOOK_STALE",
                    f"book_stale_ms={btc_stale_ms:.0f} >= hard={ss_stale_hard:.0f} src={ss_src}",
                )
            if btc_cross_bps > 0 and ac_hard_bps > 0 and btc_cross_bps >= ac_hard_bps:
                try:
                    if _ac_veto_total is not None:
                        _ac_veto_total.labels(symbol=symbol, src=ac_src).inc()
                except Exception:
                    pass
                return GateDecision(
                    True, True, "VETO_TRADE_ADVERSE_CROSS",
                    f"cross_bps={btc_cross_bps:.3f} >= hard={ac_hard_bps:.3f} src={ac_src}",
                )
            if soft_flags:
                return GateDecision(True, True, "VETO_ENTRY_POLICY", ";".join(soft_flags)[:256])
            if drift_hit:
                return GateDecision(True, True, "VETO_FEATURE_DRIFT", drift_notes[:256])

        return GateDecision(True, False, "OK", "pass")


def write_entry_policy_diag(redis_client: Any, *, stream: str, maxlen: int, event: dict[str, Any]) -> None:
    """
    Standalone diagnostic helper for out-of-band entry policy logging.
    Used by CryptoOrderFlowHandler to log veto/delay decisions to a dedicated stream.
    """
    if not stream or redis_client is None:
        return
    try:
        # standard outbox JSON packing: {"data": "<json>"}
        payload = {"data": json.dumps(event, ensure_ascii=False, separators=(",", ":"))}
        redis_client.xadd(stream, payload, maxlen=int(maxlen), approximate=True)
    except Exception:
        pass
