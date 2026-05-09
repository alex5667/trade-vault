from __future__ import annotations

import os
from collections import deque
from datetime import UTC, datetime
from typing import Any

from core.crypto_orderflow_detectors import classify_signed_qty
from utils.time_utils import get_ny_time_millis

# ---------------------------------------------------------------------------
# GPURingBuffer — lazy import, CPU fallback if CuPy unavailable
# ---------------------------------------------------------------------------
try:
    from gpu.gpu_ring_buffer import GPURingBuffer as _GPURingBuffer  # type: ignore
    _GPU_RING_AVAILABLE = True
except Exception:
    _GPURingBuffer = None  # type: ignore
    _GPU_RING_AVAILABLE = False


def _alpha(period: int) -> float:
    p = int(period)
    if p <= 1:
        return 1.0
    return 2.0 / (p + 1.0)


def _utc_day_key(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=UTC)
    return dt.strftime("%Y-%m-%d")


def _utc_week_key(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=UTC)
    iso = dt.isocalendar()
    return f"{iso.year:04d}-W{iso.week:02d}"


def _median(xs: list[float]) -> float:
    if not xs:
        return 0.0
    xs2 = sorted(xs)
    n = len(xs2)
    mid = n // 2
    if n % 2 == 1:
        return float(xs2[mid])
    return 0.5 * (float(xs2[mid - 1]) + float(xs2[mid]))


def _median_inline(xs: list[float]) -> float:
    """Inline median helper for TickCVDState."""
    n = len(xs)
    if n <= 0:
        return 0.0
    ys = sorted(xs)
    mid = n // 2
    if n % 2 == 1:
        return float(ys[mid])
    return 0.5 * (float(ys[mid - 1]) + float(ys[mid]))


class TickCVDState:
    """
    Tick-level CVD state (per symbol).

    Hot-path requirements:
    - update() must be O(1) per tick (no sorting/median on every tick).
    - robust stats (median/MAD) computed on-demand (e.g. on delta_spike).

    Deterministic reset:
    - reset decision depends ONLY on tick.ts (epoch ms), not wall clock.
    - if tick.ts is missing/invalid, reset is skipped (fail-open) and flagged.
    """

    def __init__(
        self,
        symbol: str,
        reset_mode: str = "day",
        ema_period_delta: int = 10,
        ema_period_cvd: int = 20,
        robust_window: int = 500,
        redis_client: Any = None,
    ):
        self.symbol = symbol
        # Optional: inject Redis client for SRE counters (best-effort, safe if None)
        self.redis = redis_client
        self.reset_mode = reset_mode  # "none" | "day" | "week"
        self.ema_period_delta = ema_period_delta
        self.ema_period_cvd = ema_period_cvd
        self.robust_window = robust_window
        self.robust_eps = 1e-9
        self.mad_scale = 1.4826  # consistent with normal dist

        # state (streaming)
        self.cvd_tick = 0.0
        self.ema_delta = 0.0
        self.ema_cvd = 0.0
        self.cvd_slope = 0.0  # Phase A: placeholder or simple delta ema
        self.last_delta_tick = 0.0

        self.last_ts_ms: int | None = None
        self.last_reset_key: str | None = None
        self.reset_count = 0
        self.reset_skipped_bad_time = 0

        # ring buffer for robust stats
        self._rb: deque[float] = deque(maxlen=robust_window)

        # --- Source-consistency / quarantine (two-baseline defense) ---
        self._q_until_ms: int = 0
        self._q_reason: str = ""
        self._jump_ts: deque[int] = deque()
        self._ema_abs_delta_qty: float = 0.0
        self._ema_abs_delta_usd: float = 0.0
        self._jump_events_total: int = 0
        self._tick_i: int = 0

        # robust scale for normalization (median abs delta USD)
        self._med_window = int(os.getenv("CVD_MEDIAN_WINDOW", "120"))
        self._med_recalc_every = int(os.getenv("CVD_MED_RECALC_EVERY", "50"))
        self._abs_delta_usd_buf: deque[float] = deque(maxlen=self._med_window)
        self._median_abs_delta_usd: float = 0.0

        # GPU ring buffer for median(abs_delta_usd) — eliminates sorted() spike
        # Lazy init on first push (avoids CUDA JIT at import time).
        self._usd_ring: Any | None = None
        self._usd_ring_init: bool = False

        # external CVD (if provided by upstream worker) to detect two-baseline offset
        self._cvd_ext_prev: float | None = None

        # out-of-order handling
        self._ooo_max_lag_ms = int(os.getenv("CVD_OOO_MAX_LAG_MS", "2000"))

        # env-config (read once; hot reload can be added later)
        self._q_enable = os.getenv("CVD_QUARANTINE_ENABLE", "0").strip().lower() in {"1", "true", "yes"}
        self._jump_abs_qty = float(os.getenv("CVD_JUMP_ABS_QTY", "5000"))
        self._jump_abs_usd = float(os.getenv("CVD_JUMP_ABS_USD", "2000000"))
        self._jump_rel_k = float(os.getenv("CVD_JUMP_REL_K", "8.0"))
        self._jump_window_ms = int(os.getenv("CVD_JUMP_WINDOW_MS", "180000"))
        self._jump_k_events = int(os.getenv("CVD_JUMP_K_EVENTS", "2"))
        self._q_ttl_ms = int(os.getenv("CVD_QUARANTINE_TTL_MS", "900000"))

        rk = os.getenv("CVD_EXPECTED_RESET_KEYS", "reset,cvd_reset,reset_flag")
        self._expected_reset_keys: list[str] = [x.strip() for x in rk.split(",") if x.strip()]

    def apply_config(self, cfg: dict[str, Any]) -> None:
        """
        Hot reload config for this state.
        Fail-open: ignore broken values.
        """
        try:
            rm = str(cfg.get("cvd_reset_mode", self.reset_mode) or self.reset_mode)
            if rm in ("none", "day", "week"):
                self.reset_mode = rm
        except Exception:
            pass

        try:
            self.ema_period_delta = int(cfg.get("cvd_ema_period_delta", self.ema_period_delta))
            if self.ema_period_delta < 1:
                self.ema_period_delta = 1
        except Exception:
            pass

        try:
            self.ema_period_cvd = int(cfg.get("cvd_ema_period_cvd", self.ema_period_cvd))
            if self.ema_period_cvd < 1:
                self.ema_period_cvd = 1
        except Exception:
            pass

        try:
            rw = int(cfg.get("cvd_robust_w", self.robust_window))
            if rw < 20:
                rw = 20
            # resize ring buffer maxlen if needed
            if self._rb.maxlen != rw:
                old = list(self._rb)
                self._rb = deque(old[-rw:], maxlen=rw)
                self.robust_window = rw
        except Exception:
            pass

    def _median(self, xs: list[float]) -> float:
        n = len(xs)
        if n <= 0:
            return 0.0
        ys = sorted(xs)
        mid = n // 2
        if n % 2 == 1:
            return float(ys[mid])
        return 0.5 * (float(ys[mid - 1]) + float(ys[mid]))

    def _ensure_usd_ring(self) -> None:
        """Lazy init of GPURingBuffer for abs_delta_usd median (no CUDA JIT at import)."""
        if self._usd_ring_init:
            return
        self._usd_ring_init = True
        if _GPU_RING_AVAILABLE and _GPURingBuffer is not None:
            try:
                self._usd_ring = _GPURingBuffer(
                    window_size=self._med_window,
                    min_n=16,
                    use_gpu=None,  # auto-detect: GPU if CuPy available, CPU otherwise
                )
            except Exception:
                self._usd_ring = None

    def _maybe_recalc_median(self) -> None:
        """Recalculate median(abs_delta_usd) every _med_recalc_every ticks.

        Hot path order:
          1. GPURingBuffer GPU path (bulk sync deque → GPU, then cp.sort median, ~8-12µs total)
          2. CPU sorted() fallback  O(N log N, ~15-30µs)

        DESIGN: GPU ring is sync'd in batch here (not per-tick) to avoid memcpyAsync
        overhead on every tick. Per-tick hot path overhead = 0.
        """
        if (self._tick_i % max(1, self._med_recalc_every)) != 0:
            return
        if len(self._abs_delta_usd_buf) < 16:
            return

        # --- GPU path: bulk sync + compute ---
        self._ensure_usd_ring()
        if self._usd_ring is not None:
            try:
                # Bulk-sync the deque to the GPU ring (only runs every N ticks)
                xs = list(self._abs_delta_usd_buf)
                # Reset ring and reload from current deque state
                self._usd_ring._head = 0
                self._usd_ring._count = 0
                for v in xs:
                    self._usd_ring.push(v)
                med, _mad, n = self._usd_ring.compute_stats()
                if n >= 16:
                    self._median_abs_delta_usd = float(med)
                    return
            except Exception:
                pass  # fall through to CPU

        # --- CPU fallback: O(N log N) ---
        self._median_abs_delta_usd = float(self._median(list(self._abs_delta_usd_buf)))

    def _is_expected_reset(self, tick: dict[str, Any]) -> bool:
        for k in self._expected_reset_keys:
            try:
                if tick.get(k):
                    return True
            except Exception:
                continue
        return False

    def _extract_external_cvd_usd(self, tick: dict[str, Any]) -> float | None:
        # If upstream injects CVD level (two-baseline can be detected by cvd_jump_usd)
        for k in ("cvd_usd", "cvd_notional", "cvd_tick_usd", "cvd_close_usd", "cvd"):
            v = tick.get(k)
            if v is None:
                continue
            try:
                return float(v)
            except Exception:
                continue
        return None

    def _compute_reset_key(self, ts_ms: int) -> str | None:
        if self.reset_mode == "none":
            return None
        if self.reset_mode == "day":
            return _utc_day_key(ts_ms)
        if self.reset_mode == "week":
            return _utc_week_key(ts_ms)
        return None

    def _do_reset(self, new_key: str | None) -> None:
        """
        Hard reset of session-dependent state.
        """
        self.cvd_tick = 0.0
        self.ema_delta = 0.0
        self.ema_cvd = 0.0
        self.cvd_slope = 0.0
        self.last_delta_tick = 0.0
        self._rb.clear()
        self.last_ts_ms = None
        self.last_reset_key = new_key
        self.reset_count += 1

    def update(self, tick: dict[str, Any]) -> None:
        """
        O(1) update. No heavy computations.
        """
        # ts_ms must be int epoch ms; taken from tick.ts
        ts_ms: int | None = None
        try:
            tsv = tick.get("ts_ms") or tick.get("ts") or tick.get("T")
            if tsv is not None:
                ts_ms = int(tsv)
        except Exception:
            ts_ms = None

        # out-of-order: quarantine immediately (bad time)
        ts_ms_int = int(ts_ms or 0) if ts_ms else 0
        if self._q_enable and self.last_ts_ms is not None and ts_ms_int > 0 and int(self.last_ts_ms or 0) > 0 and ts_ms_int < (int(self.last_ts_ms) - self._ooo_max_lag_ms):
            nm = int(ts_ms_int or get_ny_time_millis())
            self._q_until_ms = nm + int(self._q_ttl_ms)
            self._q_reason = f"out_of_order ts={ts_ms_int} < last={int(self.last_ts_ms)} (lag_ms>{self._ooo_max_lag_ms})"
            # do not clear jump_ts; keep history

        # Deterministic reset (only if ts_ms is valid)
        if ts_ms is None or ts_ms <= 0:
            self.reset_skipped_bad_time += 1
        else:
            rk = self._compute_reset_key(ts_ms)
            if rk is not None:
                if self.last_reset_key is None:
                    # first valid key => set baseline without counting as "reset"
                    self.last_reset_key = rk
                elif rk != self.last_reset_key:
                    self._do_reset(rk)

        self._tick_i += 1

        # signed delta tick (same semantics as delta_spike detector)
        d = classify_signed_qty(tick)
        self.last_delta_tick = d
        self.cvd_tick += d
        if self._q_enable and self.last_ts_ms is not None and ts_ms_int and int(self.last_ts_ms or 0) and ts_ms_int < (int(self.last_ts_ms) - self._ooo_max_lag_ms):
            nm = int(ts_ms_int or get_ny_time_millis())
            self._q_until_ms = nm + int(self._q_ttl_ms)
            self._q_reason = f"out_of_order ts={ts_ms_int} < last={int(self.last_ts_ms)} (lag_ms>{self._ooo_max_lag_ms})"
            # do not clear jump_ts; keep history

        # --- quarantine: two-baseline/jump detection (USD-normalized) ---
        if self._q_enable:
            nm = int(ts_ms_int or get_ny_time_millis())
            # price (best-effort) to compute notional scale
            try:
                px = float(tick.get("price") or tick.get("p") or 0.0)
            except Exception:
                px = 0.0

            ad_qty = abs(float(d))
            ad_usd = abs(float(d) * float(px)) if px > 0 else 0.0

            # maintain robust scale (median abs delta USD)
            if ad_usd > 0:
                self._abs_delta_usd_buf.append(float(ad_usd))
                # NOTE: GPU ring sync is done in _maybe_recalc_median() in batches (every N ticks).
                # Do NOT push to ring every tick — memcpyAsync overhead would add ~2-5µs per tick.
                self._maybe_recalc_median()

            # EMA scale (fallback if median not available)
            a = 2.0 / (max(2, int(self.ema_period_delta)) + 1.0)
            if self._ema_abs_delta_qty == 0.0 and self.last_ts_ms is None:
                self._ema_abs_delta_qty = ad_qty
            else:
                self._ema_abs_delta_qty = self._ema_abs_delta_qty + a * (ad_qty - self._ema_abs_delta_qty)

            if self._ema_abs_delta_usd == 0.0 and self.last_ts_ms is None:
                self._ema_abs_delta_usd = ad_usd
            else:
                self._ema_abs_delta_usd = self._ema_abs_delta_usd + a * (ad_usd - self._ema_abs_delta_usd)

            # decide baseline metric: external CVD jump if provided; else delta jump
            expected_reset = self._is_expected_reset(tick)
            jump_value_usd: float = 0.0
            jump_kind: str = ""
            cvd_ext = self._extract_external_cvd_usd(tick)
            if (cvd_ext is not None) and (self._cvd_ext_prev is not None) and (not expected_reset):
                jump_value_usd = abs(float(cvd_ext) - float(self._cvd_ext_prev))
                jump_kind = "cvd_jump_usd"
            if cvd_ext is not None:
                self._cvd_ext_prev = float(cvd_ext)

            if jump_kind == "":
                # fallback: delta jump in USD/qty
                if px > 0 and ad_usd > 0:
                    jump_value_usd = ad_usd
                    jump_kind = "delta_jump_usd"
                else:
                    jump_value_usd = ad_qty
                    jump_kind = "delta_jump_qty"

            # scale for thresholding
            scale_usd = float(self._median_abs_delta_usd or self._ema_abs_delta_usd or 0.0)
            thr_usd = max(float(self._jump_abs_usd), float(self._jump_rel_k) * max(1e-9, scale_usd))
            thr_qty = max(float(self._jump_abs_qty), float(self._jump_rel_k) * max(1e-9, float(self._ema_abs_delta_qty)))

            is_jump = False
            if jump_kind in {"cvd_jump_usd", "delta_jump_usd"}:
                if (not expected_reset) and jump_value_usd > thr_usd:
                    is_jump = True
            else:
                if (not expected_reset) and jump_value_usd > thr_qty:
                    is_jump = True

            if is_jump:
                self._jump_events_total += 1
                self._jump_ts.append(nm)
                # SRE counter (best-effort): cvd_jump_total{symbol}
                try:
                    r = getattr(self, "redis", None)
                    sym = str(self.symbol or "na")
                    if r is not None and sym and sym != "na":
                        r.incr(f"metrics:cvd_jump_total:{sym}")
                        r.expire(f"metrics:cvd_jump_total:{sym}", int(os.getenv("METRICS_COUNTER_TTL_SEC", "604800")))
                except Exception:
                    pass
                # drop old
                while self._jump_ts and (nm - int(self._jump_ts[0])) > int(self._jump_window_ms):
                    self._jump_ts.popleft()
                if len(self._jump_ts) >= int(self._jump_k_events):
                    self._q_until_ms = nm + int(self._q_ttl_ms)
                    if jump_kind in {"cvd_jump_usd", "delta_jump_usd"}:
                        self._q_reason = f"{jump_kind} v={jump_value_usd:.0f} > thr_usd={thr_usd:.0f} (med_abs_usd={scale_usd:.0f})"
                    else:
                        self._q_reason = f"{jump_kind} v={jump_value_usd:.2f} > thr_qty={thr_qty:.2f} (ema_abs_qty={self._ema_abs_delta_qty:.2f})"
                    self._jump_ts.clear()

        # ring buffer for robust stats
        self._rb.append(float(d))

        # EMA updates
        a_d = _alpha(self.ema_period_delta)
        if self.ema_delta == 0.0 and self.last_ts_ms is None:
            self.ema_delta = d
        else:
            self.ema_delta = self.ema_delta + a_d * (d - self.ema_delta)

        a_c = _alpha(self.ema_period_cvd)
        if self.ema_cvd == 0.0 and self.last_ts_ms is None:
            self.ema_cvd = self.cvd_tick
        else:
            self.ema_cvd = self.ema_cvd + a_c * (self.cvd_tick - self.ema_cvd)

        # slope: for phase A keep it equal to ema_delta
        self.cvd_slope = self.ema_delta

        self.last_ts_ms = ts_ms if ts_ms else None

    def quarantine_active(self, now_ms: int | None = None) -> bool:
        if not self._q_enable:
            return False
        nm = int(now_ms or get_ny_time_millis())
        return int(self._q_until_ms or 0) > nm

    def quarantine_reason(self) -> str:
        return str(self._q_reason or "")

    def robust_snapshot(self) -> dict[str, float]:
        """
        On-demand robust stats (median/MAD) over ring buffer values.
        GPU path: cp.median on float32 array — O(N) vs O(N log N) sort.
        CPU fallback: Python sorted() — always safe.
        """
        xs = list(self._rb)
        n = len(xs)
        if n < 20:
            return {
                "delta_med": 0.0,
                "delta_mad": 0.0,
                "delta_robust_z": 0.0,
                "delta_n": float(n),
            }

        # GPU fast-path (activate at n >= 50 — transfer is ~10µs, compute ~0.5µs)
        if n >= 50:
            try:
                from common.gpu_service import get_gpu_service
                _gpu = get_gpu_service()
                if _gpu.available:
                    import cupy as cp
                    x_gpu = cp.asarray(xs, dtype=cp.float32)
                    med = float(cp.median(x_gpu))
                    mad = float(cp.median(cp.abs(x_gpu - med)))
                    denom = (self.mad_scale * mad) + self.robust_eps
                    rz = (self.last_delta_tick - med) / denom if denom > 0 else 0.0
                    return {
                        "delta_med": float(med),
                        "delta_mad": float(mad),
                        "delta_robust_z": float(rz),
                        "delta_n": float(n),
                    }
            except Exception:
                pass  # Fail open → CPU fallback below

        # CPU fallback: O(N log N) sort
        med = _median(xs)
        devs = [abs(x - med) for x in xs]
        mad = _median(devs)
        denom = (self.mad_scale * mad) + self.robust_eps
        rz = (self.last_delta_tick - med) / denom if denom > 0 else 0.0
        return {
            "delta_med": float(med),
            "delta_mad": float(mad),
            "delta_robust_z": float(rz),
            "delta_n": float(n),
        }


    def indicators_light(self) -> dict[str, Any]:
        """
        Lightweight indicators (no heavy stats).
        Safe to attach to every signal/event.
        """
        nm = int(self.last_ts_ms or get_ny_time_millis())
        q_active = int(1 if self.quarantine_active(nm) else 0)
        return {
            "cvd_tick": float(self.cvd_tick),
            "cvd_ema": float(self.ema_cvd),
            "ema_delta": float(self.ema_delta),
            "cvd_slope": float(self.cvd_slope),
            "delta_tick": float(self.last_delta_tick),
            "cvd_reset_mode": str(self.reset_mode),
            "cvd_resets": int(self.reset_count),
            "cvd_reset_skipped_bad_time": int(self.reset_skipped_bad_time),
            # quarantine flags (consumed by OFConfirm/ML)
            "cvd_quarantine_active": int(q_active),
            "cvd_quarantine_until_ms": int(self._q_until_ms or 0),
            "cvd_quarantine_reason": str(self._q_reason or ""),
            "cvd_jump_events_total": int(self._jump_events_total),
            "cvd_median_abs_delta_usd": float(self._median_abs_delta_usd or 0.0),
        }
