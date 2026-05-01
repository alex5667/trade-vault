from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import time
import os
import math
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, List, Optional, Tuple

from types import SimpleNamespace
from news_pipeline.enricher_sync import NewsEnricherSync
from common.news_gate import NewsGate
from core.smt_symbol_snapshot import SymbolSnapshot
from services.smt_logic import decide_smt
from common.news_gate import NewsGate


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d

def _s(x: Any) -> str:
    return str(x) if x is not None else ""

def _read_calendar_agg(redis: Any, asset_class: str) -> Dict[str, Any]:
    """
    Read calendar aggregate state from Redis:
      key = calendar:agg:{asset_class}
    Expected fields include:
      event_tminus_sec (int), event_grade_id (int), event_ts_ms or event_time_ms (optional)
    Fail-open: returns {}.
    """
    ac = (asset_class or "").strip().lower()
    if not ac:
        return {}
    try:
        # sync redis client in this service
        d = redis.hgetall(f"calendar:agg:{ac}")
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}

def _news_gate_from_agg(agg: Dict[str, Any], now_ms: int, pre_sec: int, post_sec: int, hi_grade: int = 4) -> Tuple[int, int, str]:
    """
    Determine if we are within high-impact window.
    Returns: (blocked(0/1), until_ts_ms, reason)
    """
    if not agg:
        return 0, 0, ""
    grade = _i(agg.get("event_grade_id", agg.get("grade_id", 0)), 0)
    if grade < int(hi_grade):
        return 0, 0, ""
    tminus = _i(agg.get("event_tminus_sec", 10**9), 10**9)
    # event_ts_ms optional; if present, compute until precisely
    ev_ts = _i(agg.get("event_ts_ms", agg.get("event_time_ms", 0)), 0)
    pre = int(pre_sec)
    post = int(post_sec)
    if pre < 0: pre = 0
    if post < 0: post = 0
    # Block if -pre <= tminus <= post (tminus is "seconds until event", can be negative after)
    if -pre <= tminus <= post:
        if ev_ts > 0:
            until = int(ev_ts + post * 1000)
        else:
            # fallback: now + remaining window
            remain_sec = int(post - tminus) if tminus <= post else 0
            until = int(now_ms + remain_sec * 1000)
        return 1, until, "NEWS_HIGH_IMPACT_WINDOW"
    return 0, 0, ""


def _now_ms() -> int:
    return get_ny_time_millis()


def _safe_float(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return d
        return v
    except Exception:
        return d


def _b2s(x: Any) -> str:
    if isinstance(x, bytes):
        return x.decode("utf-8", errors="ignore")
    return str(x)


def _read_price_latest(redis_client: Any, symbol: str) -> Tuple[float, int]:
    """
    Expected key:
      price:latest:{SYMBOL}
    Expected fields (best-effort):
      mid, ts_ms
    Returns (mid, ts_ms). If missing -> (0,0).
    """
    try:
        d = redis_client.hgetall(f"price:latest:{symbol}") or {}
    except Exception:
        return 0.0, 0
    dd: Dict[str, str] = {}
    try:
        for k, v in dict(d).items():
            dd[_b2s(k)] = _b2s(v)
    except Exception:
        return 0.0, 0
    mid = _safe_float(dd.get("mid") or 0.0, 0.0)
    ts_ms = int(_safe_float(dd.get("ts_ms") or 0.0, 0.0))
    return mid, ts_ms


def _logret(p0: float, p1: float) -> float:
    if p0 <= 0 or p1 <= 0:
        return 0.0
    return math.log(p1 / p0)


def _corr(xs: List[float], ys: List[float]) -> float:
    n = min(len(xs), len(ys))
    if n < 8:
        return 0.0
    x = xs[-n:]
    y = ys[-n:]
    mx = sum(x) / n
    my = sum(y) / n
    vx = 0.0
    vy = 0.0
    c = 0.0
    for i in range(n):
        dx = x[i] - mx
        dy = y[i] - my
        c += dx * dy
        vx += dx * dx
        vy += dy * dy
    den = math.sqrt(vx * vy)
    if den <= 1e-18:
        return 0.0
    return float(c / den)


def _corr_lag(lead: List[float], lagg: List[float], lag: int) -> float:
    """
    corr(lead[t], lagg[t+lag]) for lag>=0
    i.e. lead "leads" lagg by lag steps.
    """
    if lag <= 0:
        return _corr(lead, lagg)
    if len(lead) <= lag or len(lagg) <= lag:
        return 0.0
    return _corr(lead[:-lag], lagg[lag:])


@dataclass
class BundleSpec:
    bundle_id: str
    symbols: List[str]


def _parse_bundles_from_env() -> List[BundleSpec]:
    """
    Expected env:
      SMT_BUNDLE_1=btc_eth_sol:BTCUSDT,ETHUSDT,SOLUSDT
      SMT_BUNDLE_2=...
    Or single:
      SMT_COH_BUNDLE=btc_eth_sol (then must exist SMT_BUNDLE_1/..)
    """
    bundles: List[BundleSpec] = []
    # Collect SMT_BUNDLE_* in order.
    for i in range(1, 32):
        v = os.getenv(f"SMT_BUNDLE_{i}", "") or ""
        v = v.strip()
        if not v:
            continue
        # format: id:SYM1,SYM2,SYM3
        try:
            bid, rest = v.split(":", 1)
            syms = [s.strip().upper() for s in rest.split(",") if s.strip()]
            bid = (bid or "").strip()
            if bid and syms:
                bundles.append(BundleSpec(bundle_id=bid, symbols=syms))
        except Exception:
            continue
    return bundles


@dataclass
class SmtAggregatorConfig:
    window_n: int
    max_lag: int
    leader_confirm_min_bps: float
    leader_dir_window: int
    coh_min_corr: float
    price_stale_ms: int
    write_key_prefix: str = "smt:bundle:v1:"

    @classmethod
    def from_env(cls) -> "SmtAggregatorConfig":
        def _i(name: str, d: int) -> int:
            try:
                return int(float(os.getenv(name, str(d))))
            except Exception:
                return d
        def _f(name: str, d: float) -> float:
            try:
                return float(os.getenv(name, str(d)))
            except Exception:
                return d
        return cls(
            window_n=_i("SMT_COH_WINDOW_N", 180),              # returns samples
            max_lag=_i("SMT_COH_MAX_LAG", 5),                  # steps
            leader_confirm_min_bps=_f("SMT_LEADER_CONFIRM_MIN_BPS", 12.0),
            leader_dir_window=_i("SMT_LEADER_DIR_WINDOW", 6),
            coh_min_corr=_f("SMT_COH_MIN_CORR", 0.55),
            price_stale_ms=_i("SMT_PRICE_STALE_MS", 7_000),
        )


class SmtBundleAggregator:
    """
    Periodically computes bundle state from latest mid prices and writes:
      HSET smt:bundle:v1:{bundle_id} leader,leader_dir,leader_confirm,coh,ts_ms

    IMPORTANT:
      - fail-open: if prices missing -> do not write garbage, keep previous.
      - state is used by pre-publish gate; must be robust.
    """
    def __init__(self, *, redis_client: Any, bundles: List[BundleSpec], cfg: Optional[SmtAggregatorConfig] = None) -> None:
        self.redis = redis_client
        self.bundles = bundles
        self.cfg = cfg or SmtAggregatorConfig.from_env()
        # in-memory histories
        self._last_mid: Dict[str, float] = {}
        self._last_ts: Dict[str, int] = {}
        self._rets: Dict[str, Deque[float]] = {}
        for b in bundles:
            for s in b.symbols:
                self._rets.setdefault(s, deque(maxlen=max(32, self.cfg.window_n)))

        # publish targets
        self.smt_setup_stream = str(os.getenv("SMT_SETUP_STREAM", "stream:smt:setup"))
        
        self._enricher = NewsEnricherSync(redis=self.redis)
        self._news = NewsGate(
            redis_client=self.redis,
            asset_class=str(os.getenv("NEWS_ASSET_CLASS", "crypto")),
            window_sec=int(os.getenv("NEWS_GATE_WINDOW_SEC", "300")),
            grade_min=int(os.getenv("NEWS_GATE_GRADE_MIN", "4")),
            manual_key=str(os.getenv("NEWS_GATE_MANUAL_KEY", "news:hi:active")),
            cal_agg_prefix=str(os.getenv("NEWS_GATE_CAL_AGG_PREFIX", "calendar:agg:")),
            # soft gate parameters
            soft_enabled=True,
            soft_window_sec=int(os.getenv("NEWS_GATE_WINDOW_SEC", "300")),
            soft_grade_min=2,
            soft_grade2_bps=5000,
            soft_grade3_bps=3500,
            soft_grade4_bps=2500,
            soft_news_k=0.9,
            soft_news_min_bps=2500,
        )

    def _load_snapshot(self, sym: str) -> Optional[SymbolSnapshot]:
        try:
            raw = self.redis.get(f"smt:snap:{sym}")
            if not raw:
                return None
            d = json.loads(raw)
            snap = SymbolSnapshot.from_dict(d)
            if snap.symbol:
                return snap
        except Exception:
            return None
        return None

    def _publish_smt_setup(self, payload: Dict[str, Any], bundle_id: str) -> None:
        """
        Publish SMT as SETUP (navigator), not as direct trade signal.
        Downstream should trigger entry only after retest + of_confirm.
        """
        try:
            msg = {
                "type": "smt_setup",
                "bundle": bundle_id,
                "ts_ms": int(payload.get("ts_ms") or get_ny_time_millis()),
                "payload": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            }
            self.redis.xadd(self.smt_setup_stream, msg, maxlen=20000, approximate=True)
        except Exception:
            pass

    def _update_returns_from_latest(self, symbol: str) -> bool:
        mid, ts = _read_price_latest(self.redis, symbol)
        if mid <= 0 or ts <= 0:
            return False
        now = _now_ms()
        if self.cfg.price_stale_ms > 0 and abs(now - ts) > self.cfg.price_stale_ms:
            return False
        p0 = self._last_mid.get(symbol, 0.0)
        if p0 > 0:
            r = _logret(p0, mid)
            self._rets[symbol].append(float(r))
        self._last_mid[symbol] = float(mid)
        self._last_ts[symbol] = int(ts)
        return True

    def _leader_score(self, sym_i: str, syms: List[str]) -> Tuple[float, Dict[str, int]]:
        """
        Score = sum over others of max corr over lags [0..max_lag]
        Returns (score, best_lag_by_other)
        """
        ri = list(self._rets.get(sym_i) or [])
        if len(ri) < 12:
            return 0.0, {}
        best_lags: Dict[str, int] = {}
        score = 0.0
        for sym_j in syms:
            if sym_j == sym_i:
                continue
            rj = list(self._rets.get(sym_j) or [])
            if len(rj) < 12:
                continue
            best = 0.0
            best_lag = 0
            for lag in range(0, max(0, self.cfg.max_lag) + 1):
                c = _corr_lag(ri, rj, lag)
                if c > best:
                    best = c
                    best_lag = lag
            if best > 0:
                score += best
                best_lags[sym_j] = best_lag
        return float(score), best_lags

    def _dir_from_returns(self, rs: List[float]) -> str:
        w = max(2, int(self.cfg.leader_dir_window))
        xs = rs[-w:]
        s = sum(xs)
        return "UP" if s >= 0 else "DOWN"

    def _confirm_leader(self, rs: List[float]) -> int:
        """
        leader_confirm=1 if:
          - abs(cum_move_bps over last W) >= leader_confirm_min_bps
          - and sign consistency >= 4/6 (default)
        """
        w = max(4, int(self.cfg.leader_dir_window))
        xs = rs[-w:]
        if len(xs) < w:
            return 0
        cum = sum(xs)
        cum_bps = abs(cum) * 10_000.0
        if cum_bps < float(self.cfg.leader_confirm_min_bps):
            return 0
        sgn = 1 if cum >= 0 else -1
        agree = 0
        for r in xs:
            if (r >= 0 and sgn > 0) or (r < 0 and sgn < 0):
                agree += 1
        return 1 if agree >= max(3, w - 2) else 0

    def compute_bundle_state(self, b: BundleSpec) -> Optional[Dict[str, Any]]:
        # update returns from latest for all symbols
        ok_any = False
        for s in b.symbols:
            ok_any = self._update_returns_from_latest(s) or ok_any
        if not ok_any:
            return None

        # ensure enough returns for bundle
        if any(len(self._rets.get(s) or []) < 12 for s in b.symbols):
            return None

        # pick leader
        best_sym = ""
        best_score = -1.0
        best_lags: Dict[str, int] = {}
        for s in b.symbols:
            sc, lags = self._leader_score(s, b.symbols)
            if sc > best_score:
                best_score = sc
                best_sym = s
                best_lags = lags
        if not best_sym:
            return None

        r_leader = list(self._rets[best_sym])
        leader_dir = self._dir_from_returns(r_leader)
        leader_confirm = self._confirm_leader(r_leader)

        # coherence: dir agreement + timing (median lag normalized) + quality (OF_ok ∧ Zone_ok share)
        dir_agree = 0
        qual_ok = 0
        lags_list: List[int] = []
        
        # Load snapshots for quality check
        snaps_by_sym: Dict[str, SymbolSnapshot] = {}
        try:
            for s in b.symbols:
                snap = self._load_snapshot(s)
                if snap is not None:
                    snaps_by_sym[str(snap.symbol)] = snap
        except Exception:
            snaps_by_sym = {}

        z_th = float(os.getenv("SMT_QUAL_Z_MIN", "2.0"))

        for s in b.symbols:
            rs = list(self._rets[s])
            d = self._dir_from_returns(rs)
            if d == leader_dir:
                dir_agree += 1
            if s != best_sym:
                lag = int(best_lags.get(s, 0))
                lags_list.append(lag)
                
                # Quality V2: OF_ok ∧ Zone_ok
                snap = snaps_by_sym.get(s)
                if snap is not None:
                    of_strong = int(getattr(snap, "of_strong", 0) or 0) == 1
                    dz = float(getattr(snap, "delta_z", 0.0) or 0.0)
                    wp = int(getattr(snap, "weak_progress", 0) or 0) == 1
                    of_ok = bool(of_strong or (abs(dz) >= z_th and wp))
                    zone_ok = bool(int(getattr(snap, "zone_ok", 0) or 0) == 1 or
                                   int(getattr(snap, "near_zone", 0) or 0) == 1 or
                                   int(getattr(snap, "abs_lvl_ok", 0) or 0) == 1)
                    if of_ok and zone_ok:
                        qual_ok += 1
                else:
                    # fallback: corr threshold logic (legacy)
                    c = _corr_lag(r_leader, rs, lag)
                    if c >= float(self.cfg.coh_min_corr):
                        qual_ok += 1

        dir_agree_share = float(dir_agree) / float(max(1, len(b.symbols)))
        if lags_list:
            lags_list_sorted = sorted(lags_list)
            med = lags_list_sorted[len(lags_list_sorted) // 2]
        else:
            med = 0
        timing = 1.0
        if self.cfg.max_lag > 0:
            timing = 1.0 - (float(med) / float(self.cfg.max_lag))
            timing = max(0.0, min(1.0, timing))
        
        # quality share uses only non-leader members
        denom_q = max(1, len(b.symbols) - 1)
        qual_share = float(qual_ok) / float(denom_q)

        # weights (env-tunable)
        try:
            w_dir = float(os.getenv("SMT_COH_W_DIR", "0.5"))
            w_time = float(os.getenv("SMT_COH_W_TIME", "0.2"))
            w_qual = float(os.getenv("SMT_COH_W_QUAL", "0.3"))
        except Exception:
            w_dir, w_time, w_qual = 0.5, 0.2, 0.3
        coh = w_dir * dir_agree_share + w_time * timing + w_qual * qual_share
        coh = max(0.0, min(1.0, float(coh)))

        return {
            "bundle_id": b.bundle_id,
            "leader": best_sym,
            "leader_dir": leader_dir,
            "leader_confirm": int(leader_confirm),
            "coh": float(coh),
            "ts_ms": int(_now_ms()),
        }

    def write_bundle_state(self, st: Dict[str, Any]) -> None:
        key = f"{self.cfg.write_key_prefix}{st.get('bundle_id')}"
        try:
            self.redis.hset(
                key,
                mapping={
                    "leader": str(st.get("leader") or ""),
                    "leader_dir": str(st.get("leader_dir") or ""),
                    "leader_confirm": str(int(st.get("leader_confirm") or 0)),
                    "coh": f"{float(st.get('coh') or 0.0):.6f}",
                    "ts_ms": str(int(st.get("ts_ms") or _now_ms())),
                }
            )
        except Exception:
            # fail-open
            return

    def tick_once(self) -> int:
        """
        Compute and write all bundles once.
        Returns number of updated bundles.
        """
        n = 0
        for b in self.bundles:
            st = self.compute_bundle_state(b)
            if st is None:
                continue

            # 1. Write legacy state (updated with new quality metric in 'coh')
            self.write_bundle_state(st)
            n += 1

            # 2. SMT V2 Decision
            try:
                leader_sym = str(st.get("leader"))
                coh = float(st.get("coh") or 0.0)
                ts_ms = int(st.get("ts_ms") or _now_ms())

                # --- NEWS GATE (calendar:agg) ---
                # BundleSpec may include asset_class; fallback env SMT_NEWS_ASSET_CLASS
                asset_class = getattr(b, "asset_class", None) or os.getenv("SMT_NEWS_ASSET_CLASS", "crypto")

                # Read snapshots again (or reuse if cached, but for simplicity re-read logic flow)
                snaps: List[SymbolSnapshot] = []
                for s in b.symbols:
                    snap = self._load_snapshot(s)
                    if snap is not None:
                        snaps.append(snap)
                
                leader_snap = next((x for x in snaps if x.symbol == leader_sym), None)
                
                # If leader missing, we can't decide properly.
                if leader_snap is None:
                    continue

                # News gate (bundle-level). Deterministic ts_ms: prefer leader_snap.ts_ms
                ts_ms = int(getattr(leader_snap, "ts_ms", 0) or 0) or get_ny_time_millis()

                # Attach news context
                ctx = SimpleNamespace(symbol=leader_sym, news=None, data_quality_flags=[])
                self._enricher.attach(ctx, asset_class=asset_class, now_ts_ms=ts_ms)

                # Prepare news features for soft gate
                news_risk = ctx.news.news_risk if ctx.news else None
                news_grade_id = ctx.news.news_grade_id if ctx.news else None
                confidence = ctx.news.confidence if ctx.news else None
                horizon_sec = ctx.news.horizon_sec if ctx.news else None
                asof_ts_ms = ctx.news.asof_ts_ms if ctx.news else None

                gate_decision = self._news.decide(
                    now_ts_ms=ts_ms,
                    symbols=tuple(b.symbols),
                    news_risk=news_risk,
                    news_grade_id=news_grade_id,
                    confidence=confidence,
                    horizon_sec=horizon_sec,
                    asof_ts_ms=asof_ts_ms,
                )

                # Config wrapper for decision
                d_cfg = {
                    "smt_coh_threshold": float(os.getenv("SMT_COH_THR", "0.65")),
                    "smt_leader_conf_min_score": float(os.getenv("SMT_LEADER_CONF_MIN_SCORE", "0.65")),
                    "smt_basket_k": int(os.getenv("SMT_BASKET_K", "2")),
                    "smt_rank_mode": str(os.getenv("SMT_RANK_MODE", "ts")),
                    "smt_rank_ts_window": int(os.getenv("SMT_RANK_TS_WINDOW", "240")),
                    "smt_zone_max_bp": float(os.getenv("SMT_ZONE_MAX_BP", "15.0")),
                    "smt_leader_min_of_score": float(os.getenv("SMT_LEADER_MIN_OF_SCORE", "1.0")),
                    "gate_decision": gate_decision,  # new: full decision object
                    "news_blocked": 1 if gate_decision.hard_block else 0,
                    "news_reason": str(gate_decision.hard_reason or ""),
                    "news_until_ts_ms": int(gate_decision.until_ts_ms or 0),
                    "risk_factor_bps": gate_decision.risk_factor_bps,
                }
                
                dec = decide_smt(leader_snap, snaps, coh=coh, cfg=d_cfg)

                # Apply hard block: force kind none for audit
                if gate_decision.hard_block:
                    # override decision for hard block
                    dec.kind = "none"
                    dec.pick = None
                    dec.reason = f"HARD_BLOCK:{gate_decision.hard_reason}"
                    dec.news_blocked = 1
                    dec.news_until_ts_ms = int(gate_decision.until_ts_ms or 0)
                else:
                    dec.news_blocked = 0
                    dec.news_until_ts_ms = 0
                
                # Override leader_confirm with snap-based truth if possible:
                # confirmed iff decision is continuation and leader has strong-of+closeCross,
                # OR you can read it directly from leader_confirm_reject_v2 via dec.reason.
                leader_confirm_v2 = 1 if (dec.kind == "continuation" and float(dec.conf_score or 0.0) > 0) else 0

                # Update state with decision
                st["decision"] = dec.kind
                st["pick"] = dec.pick
                st["trend_dir"] = dec.trend_dir
                st["div"] = dec.div
                st["reason"] = dec.reason
                st["leader_conf_score"] = float(getattr(dec, "conf_score", 0.0) or 0.0)
                st["leader_reject_score"] = float(getattr(dec, "reject_score", 0.0) or 0.0)
                st["news_blocked"] = int(getattr(dec, "news_blocked", 0) or 0)
                st["news_until_ts_ms"] = int(getattr(dec, "news_until_ts_ms", 0) or 0)

                # Save full gate decision for audit and risk analysis
                st["gate_decision"] = {
                    "hard_block": gate_decision.hard_block,
                    "hard_reason": gate_decision.hard_reason,
                    "until_ts_ms": gate_decision.until_ts_ms,
                    "risk_factor_bps": gate_decision.risk_factor_bps,
                    "soft_reasons": gate_decision.soft_reasons,
                    "dq_flags": gate_decision.dq_flags,
                    "meta": gate_decision.meta,
                }
                
                # Re-write state with extended fields
                key = f"{self.cfg.write_key_prefix}{b.bundle_id}"
                mapping = {
                    "decision": str(dec.kind),
                    "pick": str(dec.pick or ""),
                    "trend_dir": str(dec.trend_dir),
                    "div": str(dec.div or ""),
                    "reason": str(dec.reason),
                    "leader_conf_score": f"{float(getattr(dec, 'conf_score', 0.0) or 0.0):.4f}",
                    "leader_confirm": str(int(leader_confirm_v2)),
                    "news_blocked": str(int(getattr(dec, "news_blocked", news_blocked) or 0)),
                    "news_until_ts_ms": str(int(getattr(dec, "news_until_ts_ms", news_until_ts_ms) or 0)),
                    "risk_factor_bps": str(int(getattr(dec, "risk_factor_bps", 10000) or 10000)),
                }
                self.redis.hset(key, mapping=mapping)

                # Emit SETUP if actionable (navigator)
                # Never emit setups during news block.
                if int(getattr(dec, "news_blocked", 0) or 0) == 0 and dec.kind in ("continuation", "reversal") and dec.pick:
                    out = {
                        "ts_ms": int(ts_ms),
                        "kind": dec.kind,
                        "leader": dec.leader,
                        "coh": float(dec.coh),
                        "trend_dir": dec.trend_dir,
                        "pick": dec.pick,
                        "div": dec.div,
                        "reason": dec.reason,
                        "leader_conf_score": float(getattr(dec, "conf_score", 0.0) or 0.0),
                        "news_blocked": int(getattr(dec, "news_blocked", 0) or 0),
                        "news_until_ts_ms": int(getattr(dec, "news_until_ts_ms", 0) or 0),
                        "risk_factor_bps": int(getattr(dec, "risk_factor_bps", 10000) or 10000),
                    }
                    self._publish_smt_setup(out, b.bundle_id)

            except Exception:
                pass

        return n


def build_default_aggregator(redis_client: Any) -> SmtBundleAggregator:
    bundles = _parse_bundles_from_env()
    return SmtBundleAggregator(redis_client=redis_client, bundles=bundles, cfg=SmtAggregatorConfig.from_env())
