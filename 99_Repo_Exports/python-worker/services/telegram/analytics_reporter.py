from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any

from services.telegram.telegram_client import TelegramClient
from utils.time_utils import get_ny_time_millis

# NOTE: AnalyticsReporter is intentionally fail-open: any internal error must not break emits/publishers.


def _now_ms() -> int:
    return get_ny_time_millis()


@dataclass
class PairCounts:
    hits: int = 0
    writes: int = 0
    downstream_dups: int = 0
    # soft reasons (u16) aggregated for explanation of score downscales
    soft_u16_counts: dict[int, int] = field(default_factory=dict)

    def ratio(self) -> float:
        d = self.hits + self.writes
        return float(self.hits) / float(d) if d > 0 else 0.0


class NoopAnalyticsReporter:
    def record_sem_dedup(self, *, symbol: str, kind: str, hit: bool) -> None:
        return
    def record_downstream_dup(self, *, symbol: str, kind: str) -> None:
        return
    def record_soft_reasons(self, *, symbol: str, kind: str, payload: dict[str, Any]) -> None:
        return
    def maybe_flush(self, *, now_ms: int | None = None) -> None:
        return


class AnalyticsReporter:
    """
    Periodic Telegram analytics for:
      - sem_dedup_ratio (per kind/symbol) => "пережимаете" / "недожимаете"
      - sem_dedup_hits_total (top offenders) with explanation
      - (bonus) top soft reasons (u16) that dominate downscales

    This reporter is intentionally in-memory and cheap.
    It must be called from hot path (emitter) but flush is rate-limited.

    Env:
      ANALYTICS_TG_ENABLE=1
      ANALYTICS_TG_INTERVAL_S=60
      SEM_DEDUP_ALERT_MIN_EVENTS=50
      SEM_DEDUP_ALERT_HIGH=0.60
      SEM_DEDUP_ALERT_LOW=0.05
      # Per-kind overrides (comma-separated): e.g. "breakout=0.55,absorption=0.55,extreme=0.75"
      SEM_DEDUP_ALERT_HIGH_BY_KIND=...
      SEM_DEDUP_ALERT_LOW_BY_KIND=...

      # Impact alert: semantic hits rising but downstream duplicates not dropping
      SEM_DEDUP_IMPACT_ENABLE=1
      SEM_DEDUP_IMPACT_MIN_EVENTS=50
      SEM_DEDUP_IMPACT_HITS_GROWTH_PCT=0.20     # +20% hits vs prev window
      SEM_DEDUP_IMPACT_DUP_DROP_PCT_MIN=0.10    # expect at least -10% downstream dups

      # JSONL logging:
      ANALYTICS_TG_LOG_JSONL_PATH=/var/log/sem_dedup_analytics.jsonl
      SEM_DEDUP_ALERT_COOLDOWN_S=300  (avoid spam)
      ANALYTICS_SOFT_TOPN=3
    """

    def __init__(self, *, tg: TelegramClient) -> None:
        self._tg = tg
        self._enable = os.getenv("ANALYTICS_TG_ENABLE", "1").lower() in {"1","true","yes"}
        self._interval_s = int(os.getenv("ANALYTICS_TG_INTERVAL_S", "60"))
        self._min_events = int(os.getenv("SEM_DEDUP_ALERT_MIN_EVENTS", "50"))
        self._high_default = float(os.getenv("SEM_DEDUP_ALERT_HIGH", "0.60"))
        self._low_default = float(os.getenv("SEM_DEDUP_ALERT_LOW", "0.05"))
        self._cooldown_s = int(os.getenv("SEM_DEDUP_ALERT_COOLDOWN_S", "300"))
        self._soft_topn = int(os.getenv("ANALYTICS_SOFT_TOPN", "3"))

        # per-kind overrides
        self._high_by_kind = self._parse_kind_thresholds(os.getenv("SEM_DEDUP_ALERT_HIGH_BY_KIND", ""))
        self._low_by_kind = self._parse_kind_thresholds(os.getenv("SEM_DEDUP_ALERT_LOW_BY_KIND", ""))

        # impact settings
        self._impact_enable = os.getenv("SEM_DEDUP_IMPACT_ENABLE", "1").lower() in {"1","true","yes"}
        self._impact_min_events = int(os.getenv("SEM_DEDUP_IMPACT_MIN_EVENTS", "50"))
        self._impact_hits_growth = float(os.getenv("SEM_DEDUP_IMPACT_HITS_GROWTH_PCT", "0.20"))
        self._impact_dup_drop_min = float(os.getenv("SEM_DEDUP_IMPACT_DUP_DROP_PCT_MIN", "0.10"))

        # jsonl log path (optional)
        self._jsonl_path = os.getenv("ANALYTICS_TG_LOG_JSONL_PATH", "").strip()

        self._next_flush_ms: int = 0  # flush on first call
        self._last_sent_ms: int = 0
        # key: (symbol, kind)
        self._pairs: dict[tuple[str,str], PairCounts] = {}

        # Previous window snapshot for impact evaluation (same shape as _pairs but stored as raw counts)
        self._prev_window: dict[tuple[str,str], PairCounts] = {}

    @staticmethod
    def _parse_kind_thresholds(s: str) -> dict[str, float]:
        """
        Parse "kind=0.55,absorption=0.55,extreme=0.75" -> {"kind":0.55,...}
        Fail-open: ignore bad tokens.
        """
        out: dict[str, float] = {}
        if not s:
            return out
        for part in s.split(","):
            p = part.strip()
            if not p or "=" not in p:
                continue
            k, v = p.split("=", 1)
            k = k.strip()
            v = v.strip()
            if not k:
                continue
            try:
                out[k] = float(v)
            except Exception:
                continue
        return out

    def _thr_high(self, kind: str) -> float:
        return float(self._high_by_kind.get(kind, self._high_default))

    def _thr_low(self, kind: str) -> float:
        return float(self._low_by_kind.get(kind, self._low_default))

    def record_sem_dedup(self, *, symbol: str, kind: str, hit: bool) -> None:
        if not self._enable:
            return
        key = (symbol or "", kind or "")
        pc = self._pairs.get(key)
        if pc is None:
            pc = PairCounts()
            self._pairs[key] = pc
        if hit:
            pc.hits += 1
        else:
            pc.writes += 1

    def record_downstream_dup(self, *, symbol: str, kind: str) -> None:
        """
        Call this from downstream consumer/publisher when you drop a duplicate message (idempotency hit).
        This is the "impact" ground-truth: do duplicates downstream actually go down after semantic dedup?
        """
        if not self._enable:
            return
        key = (symbol or "", kind or "")
        pc = self._pairs.get(key)
        if pc is None:
            pc = PairCounts()
            self._pairs[key] = pc
        pc.downstream_dups += 1

    def record_soft_reasons(self, *, symbol: str, kind: str, payload: dict[str, Any]) -> None:
        if not self._enable:
            return
        key = (symbol or "", kind or "")
        pc = self._pairs.get(key)
        if pc is None:
            pc = PairCounts()
            self._pairs[key] = pc
        # Prefer compact u16 list in payload.
        soft_u16 = payload.get("soft_u16")
        if isinstance(soft_u16, list):
            for x in soft_u16:
                try:
                    u = int(x)
                except Exception:
                    continue
                if u <= 0:
                    continue
                pc.soft_u16_counts[u] = pc.soft_u16_counts.get(u, 0) + 1

    def maybe_flush(self, *, now_ms: int | None = None) -> None:
        if not self._enable:
            return
        tms = int(now_ms) if now_ms is not None else _now_ms()
        if tms < self._next_flush_ms:
            return
        self._next_flush_ms = tms + self._interval_s * 1000

        # cooldown to avoid Telegram spam if emitter is extremely busy
        if self._last_sent_ms and (tms - self._last_sent_ms) < self._cooldown_s * 1000:
            # still rotate window (reset) to keep data "fresh"
            self._pairs = {}
            return

        msg = self._build_message()
        # rotate window snapshots:
        self._prev_window = self._pairs
        self._pairs = {}
        if not msg:
            return
        ok = self._tg.send_text(msg)
        if ok:
            self._last_sent_ms = tms
        # always attempt JSONL logging (even if TG fails)
        self._append_jsonl(msg)

    def _append_jsonl(self, msg: str) -> None:
        """
        TG message is a single-line JSON. We also persist it to jsonl for later offline analysis.
        Fail-open: ignore all IO errors.
        """
        if not self._jsonl_path:
            return
        try:
            with open(self._jsonl_path, "a", encoding="utf-8") as f:
                f.write(msg)
                f.write("\n")
        except Exception:
            return

    def _build_message(self) -> str:
        # Gather offenders (per-kind thresholds)
        offenders_high: list[tuple[str,str,float,int,int,int]] = []  # sym, kind, ratio, hits, writes, dups
        offenders_low: list[tuple[str,str,float,int,int,int]] = []
        total_hits = 0
        total_writes = 0
        total_dups = 0

        for (sym, kind), pc in self._pairs.items():
            total_hits += pc.hits
            total_writes += pc.writes
            total_dups += pc.downstream_dups
            n = pc.hits + pc.writes
            if n < self._min_events:
                continue
            r = pc.ratio()
            hi = self._thr_high(kind)
            lo = self._thr_low(kind)
            if r >= hi:
                offenders_high.append((sym, kind, r, pc.hits, pc.writes, pc.downstream_dups)),
            elif r <= lo:
                offenders_low.append((sym, kind, r, pc.hits, pc.writes, pc.downstream_dups)),

        if not offenders_high and not offenders_low:
            return ""

        offenders_high.sort(key=lambda x: x[2], reverse=True)
        offenders_low.sort(key=lambda x: x[2])

        # --- Impact evaluation ---
        impact = self._compute_impact()

        summary_parts = []
        if offenders_high:
            summary_parts.append(f"Пережимаете ({len(offenders_high)} пар)")
        if offenders_low:
            summary_parts.append(f"Недожимаете ({len(offenders_low)} пар)")

        payload: dict[str, Any] = {
            "type": "sem_dedup_analytics",
            "summary": " | ".join(summary_parts),
            "ts_ms": _now_ms(),
            "window_s": int(self._interval_s),
            "totals": {
                "sem_dedup_hits_total": int(total_hits),
                "sem_dedup_writes_total": int(total_writes),
                "sem_dedup_ratio": float(total_hits) / float(total_hits + total_writes) if (total_hits + total_writes) else 0.0,
                "downstream_dups_total": int(total_dups),
            },
            "thresholds": {
                "default_high": float(self._high_default),
                "default_low": float(self._low_default),
                "high_by_kind": dict(self._high_by_kind),
                "low_by_kind": dict(self._low_by_kind),
                "min_events": int(self._min_events),
            },
            "impact": impact,
            "offenders": {
                "overtight": [],
                "undertight": [],
                "top_hits": [],
            }
        }

        if offenders_high:
            for sym, kind, r, h, w, d in offenders_high[:6]:
                payload["offenders"]["overtight"].append({
                    "symbol": sym,
                    "kind": kind,
                    "ratio": float(r),
                    "hits": int(h),
                    "writes": int(w),
                    "downstream_dups": int(d),
                    "high_thr": float(self._thr_high(kind)),
                    "hint": "ослабьте semantic key (уберите level для части kinds), увеличьте bucket_ms или уменьшите TTL",
                })

        if offenders_low:
            for sym, kind, r, h, w, d in offenders_low[:6]:
                payload["offenders"]["undertight"].append({
                    "symbol": sym,
                    "kind": kind,
                    "ratio": float(r),
                    "hits": int(h),
                    "writes": int(w),
                    "downstream_dups": int(d),
                    "low_thr": float(self._thr_low(kind)),
                    "hint": "усильте semantic key: venue+timeframe, включите level для breakout/absorption, увеличьте TTL или уменьшите bucket_ms",
                })

        # sem_dedup_hits_total explanation: top offenders by absolute hits
        top_by_hits = []
        for (sym, kind), pc in self._pairs.items():
            if pc.hits > 0:
                top_by_hits.append((pc.hits, sym, kind, pc.writes, pc.ratio(), pc.soft_u16_counts))
        top_by_hits.sort(key=lambda x: x[0], reverse=True)
        if top_by_hits:
            for hits, sym, kind, writes, r, soft_counts in top_by_hits[:6]:
                row: dict[str, Any] = {
                    "symbol": sym,
                    "kind": kind,
                    "hits": int(hits),
                    "writes": int(writes),
                    "ratio": float(r),
                }
                # soft reasons: show top-N u16 ids (wire-stable). Human mapping can be done in dashboards.
                if soft_counts:
                    items = sorted(soft_counts.items(), key=lambda kv: kv[1], reverse=True)[: max(0, self._soft_topn)]
                    if items:
                        row["soft_top"] = [{"u16": int(u), "count": int(c)} for (u,c) in items]
                payload["offenders"]["top_hits"].append(row)

        # Single-line JSON for Telegram + JSONL file.
        try:
            return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        except Exception:
            return ""

    def _compute_impact(self) -> dict[str, Any]:
        """
        Impact logic:
          alert if semantic hits grow, but downstream duplicates do NOT drop.
        Requires downstream to call record_downstream_dup().
        If downstream data is missing => impact.status = "unknown".
        """
        if not self._impact_enable:
            return {"status": "disabled"}

        # If downstream never reported anything, we cannot judge impact.
        any_downstream = any(pc.downstream_dups > 0 for pc in self._pairs.values())
        prev_any_downstream = any(pc.downstream_dups > 0 for pc in self._prev_window.values()) if self._prev_window else False
        if not any_downstream and not prev_any_downstream:
            return {"status": "unknown", "reason": "no_downstream_dup_signals"}

        bad_pairs: list[dict[str, Any]] = []
        for key, pc in self._pairs.items():
            sym, kind = key
            n = pc.hits + pc.writes
            if n < self._impact_min_events:
                continue
            prev = self._prev_window.get(key)
            if prev is None:
                continue
            prev_n = prev.hits + prev.writes
            if prev_n < self._impact_min_events:
                continue

            # growth in semantic hits
            hits_now = float(pc.hits)
            hits_prev = float(prev.hits)
            if hits_prev <= 0:
                continue
            hits_growth = (hits_now - hits_prev) / max(hits_prev, 1.0)

            # drop in downstream duplicates (we want it to go down)
            d_now = float(pc.downstream_dups)
            d_prev = float(prev.downstream_dups)
            if d_prev <= 0:
                continue
            dup_drop = (d_prev - d_now) / max(d_prev, 1.0)

            if hits_growth >= self._impact_hits_growth and dup_drop < self._impact_dup_drop_min:
                bad_pairs.append({
                    "symbol": sym,
                    "kind": kind,
                    "hits_prev": int(prev.hits),
                    "hits_now": int(pc.hits),
                    "hits_growth": float(hits_growth),
                    "dups_prev": int(prev.downstream_dups),
                    "dups_now": int(pc.downstream_dups),
                    "dup_drop": float(dup_drop),
                    "hint": "semantic key вероятно блокирует не те дубли (ложные блокировки) или downstream dedup считает по другой оси",
                })

        if bad_pairs:
            return {
                "status": "bad",
                "hits_growth_pct": float(self._impact_hits_growth),
                "dup_drop_min_pct": float(self._impact_dup_drop_min),
                "pairs": bad_pairs[:10],
            }
        return {"status": "ok"}
