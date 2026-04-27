from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
import time
from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class SemDedupPolicy:
    """
    Heuristics for Telegram alerts:
      - over-suppression: ratio >= over_ratio, enough events
      - under-suppression: ratio <= under_ratio, enough events
      - hits spike: hits/min >= hits_spike_per_min, enough events
    """
    window_sec: int = 300
    min_events: int = 30
    over_ratio: float = 0.60
    under_ratio: float = 0.05
    hits_spike_per_min: int = 200
    top_n: int = 6


class TelegramSink:
    """Minimal sink interface (adapt to your bot/service)."""
    def send(self, text: str) -> bool:  # pragma: no cover
        raise NotImplementedError


class SemDedupReporter:
    """
    Pulls cumulative sem_dedup counters from UnifiedSignalEmitter snapshot,
    computes interval deltas and sends Telegram diagnostics with explanations.
    """
    def __init__(self, *, emitter: Any, tg: TelegramSink, logger: Any, policy: Optional[SemDedupPolicy] = None) -> None:
        self._emitter = emitter
        self._tg = tg
        self._logger = logger
        self._policy = policy or SemDedupPolicy(
            window_sec=int(os.getenv("SEM_DEDUP_REPORT_WINDOW_SEC", "300")),
            min_events=int(os.getenv("SEM_DEDUP_REPORT_MIN_EVENTS", "30")),
            over_ratio=float(os.getenv("SEM_DEDUP_OVER_RATIO", "0.60")),
            under_ratio=float(os.getenv("SEM_DEDUP_UNDER_RATIO", "0.05")),
            hits_spike_per_min=int(os.getenv("SEM_DEDUP_HITS_SPIKE_PER_MIN", "200")),
            top_n=int(os.getenv("SEM_DEDUP_REPORT_TOP_N", "6")),
        )
        self._prev: Optional[dict[str, Any]] = None
        self._prev_ms: Optional[int] = None

    def _now_ms(self) -> int:
        return get_ny_time_millis()

    def _parse_key(self, k: str) -> tuple[str, str]:
        if "|" in k:
            a, b = k.split("|", 1)
            return a or "unknown", b or "unknown"
        return "unknown", k or "unknown"

    def _delta_map(self, cur: dict[str, int], prev: dict[str, int]) -> dict[str, int]:
        out: dict[str, int] = {}
        keys = set(cur.keys()) | set(prev.keys())
        for k in keys:
            out[k] = max(0, int(cur.get(k, 0)) - int(prev.get(k, 0)))
        return out

    def _ratio(self, hits: int, writes: int) -> float:
        d = hits + writes
        return float(hits) / float(d) if d > 0 else 0.0

    def _mk_knobs_over(self, *, bucket_ms: int, level_decimals: int, ttl_ms: int) -> str:
        # When "over-suppressing": loosen dedup => narrower equivalence classes / shorter memory
        return (
            "Рекомендации (ослабить dedup):\n"
            f"• уменьшить OUTBOX_SEM_DEDUP_TTL_MS (сейчас {ttl_ms})\n"
            f"• уменьшить OUTBOX_SEM_DEDUP_BUCKET_MS (сейчас {bucket_ms})\n"
            f"• увеличить OUTBOX_SEM_DEDUP_LEVEL_DECIMALS (сейчас {level_decimals})\n"
            "• (точечно) отключать sem-dedup для kind, где важны частые повторы"
        )

    def _mk_knobs_under(self, *, bucket_ms: int, level_decimals: int, ttl_ms: int) -> str:
        # When "under-suppressing": tighten dedup => broader equivalence classes / longer memory
        return (
            "Рекомендации (усилить dedup):\n"
            f"• увеличить OUTBOX_SEM_DEDUP_TTL_MS (сейчас {ttl_ms})\n"
            f"• увеличить OUTBOX_SEM_DEDUP_BUCKET_MS (сейчас {bucket_ms})\n"
            f"• уменьшить OUTBOX_SEM_DEDUP_LEVEL_DECIMALS (сейчас {level_decimals})\n"
            "• проверить, что level_price и side действительно заполняются (иначе ключ «размазывается»)"
        )

    def _format_top(self, *, dh: dict[str, int], dw: dict[str, int]) -> list[tuple[str, int, int, float]]:
        rows: list[tuple[str, int, int, float]] = []
        keys = set(dh.keys()) | set(dw.keys())
        for k in keys:
            h = int(dh.get(k, 0))
            w = int(dw.get(k, 0))
            r = self._ratio(h, w)
            if h + w > 0:
                rows.append((k, h, w, r))
        # sort by ratio desc, then by volume desc
        rows.sort(key=lambda x: (x[3], x[1] + x[2]), reverse=True)
        return rows[: max(1, int(self._policy.top_n))]

    def run_once(self, *, now_ms: Optional[int] = None) -> Optional[str]:
        if now_ms is None:
            now_ms = self._now_ms()

        snap = self._emitter.get_sem_stats_snapshot()
        if not snap.get("enabled", False):
            # reporter is meant for sem-dedup path; silently do nothing
            self._prev = snap
            self._prev_ms = now_ms
            return None

        if self._prev is None or self._prev_ms is None:
            self._prev = snap
            self._prev_ms = now_ms
            return None

        dt_ms = max(1, int(now_ms) - int(self._prev_ms))
        dt_min = float(dt_ms) / 60000.0

        cur_hits = dict(snap.get("hits") or {})
        cur_writes = dict(snap.get("writes") or {})
        prev_hits = dict(self._prev.get("hits") or {})
        prev_writes = dict(self._prev.get("writes") or {})

        dh = self._delta_map(cur_hits, prev_hits)
        dw = self._delta_map(cur_writes, prev_writes)

        total_hits = sum(dh.values())
        total_writes = sum(dw.values())
        total_events = total_hits + total_writes
        ratio = self._ratio(total_hits, total_writes)
        hits_per_min = int(round(float(total_hits) / max(dt_min, 1e-9)))

        bucket_ms = int(snap.get("bucket_ms") or 0)
        level_decimals = int(snap.get("level_decimals") or 0)
        ttl_ms = int(snap.get("ttl_ms") or 0)

        # Decide if we should alert
        reasons: list[str] = []
        if total_events >= self._policy.min_events and ratio >= self._policy.over_ratio:
            reasons.append(f"ПЕРЕЖИМ: sem_dedup_ratio={ratio:.2f} (>= {self._policy.over_ratio:.2f}) при events={total_events}")
        if total_events >= self._policy.min_events and ratio <= self._policy.under_ratio:
            reasons.append(f"НЕДОЖИМ: sem_dedup_ratio={ratio:.2f} (<= {self._policy.under_ratio:.2f}) при events={total_events}")
        if total_events >= self._policy.min_events and hits_per_min >= self._policy.hits_spike_per_min:
            reasons.append(f"СПАЙК: sem_dedup_hits_total rate={hits_per_min}/min (>= {self._policy.hits_spike_per_min}/min)")

        if not reasons:
            self._prev = snap
            self._prev_ms = now_ms
            return None

        top = self._format_top(dh=dh, dw=dw)

        lines: list[str] = []
        lines.append("🧠 Semantic dedup монитор")
        lines.append(f"Окно: {int(dt_ms/1000)}s (policy window_sec={self._policy.window_sec})")
        lines.append(f"Сводка: hits={total_hits}, writes={total_writes}, ratio={ratio:.2f}, hits_rate≈{hits_per_min}/min")
        lines.append(f"Конфиг: BUCKET_MS={bucket_ms}, LEVEL_DECIMALS={level_decimals}, TTL_MS={ttl_ms}")
        lines.append("")
        lines.append("Триггеры:")
        for r in reasons:
            lines.append(f"• {r}")
        lines.append("")
        lines.append("ТОП по kind/symbol (interval):")
        for k, h, w, r in top:
            sym, kind = self._parse_key(k)
            vol = h + w
            lines.append(f"• {sym} / {kind}: hits={h}, writes={w}, ratio={r:.2f}, events={vol}")
        lines.append("")

        # Explanations + knob guidance
        if ratio >= self._policy.over_ratio:
            lines.append(
                "Пояснение: высокий sem_dedup_ratio означает, что semantic key слишком часто блокирует записи.\n"
                "Риск: вы теряете легитимные повторные сигналы (особенно на быстрых рынках/частых ретестах)."
            )
            lines.append(self._mk_knobs_over(bucket_ms=bucket_ms, level_decimals=level_decimals, ttl_ms=ttl_ms))
        elif ratio <= self._policy.under_ratio:
            lines.append(
                "Пояснение: низкий sem_dedup_ratio означает, что semantic key почти не блокирует записи.\n"
                "Риск: downstream будет видеть дубли (нагрузка, шум, повторные алерты)."
            )
            lines.append(self._mk_knobs_under(bucket_ms=bucket_ms, level_decimals=level_decimals, ttl_ms=ttl_ms))

        # Extra analytics specifically about hits_total
        lines.append("")
        lines.append("Аналитика по sem_dedup_hits_total (interval):")
        top_hits = sorted(((k, v) for k, v in dh.items() if v > 0), key=lambda x: x[1], reverse=True)[: self._policy.top_n]
        if not top_hits:
            lines.append("• hits=0 в этом окне")
        else:
            for k, v in top_hits:
                sym, kind = self._parse_key(k)
                per_min = int(round(float(v) / max(dt_min, 1e-9)))
                lines.append(f"• {sym} / {kind}: hits={v} (≈{per_min}/min)")
            lines.append("Пояснение: рост hits_total — это рост подавленных дублей. Если одновременно падает полезный сигнал-флоу — вы пережимаете.")

        # advance window
        self._prev = snap
        self._prev_ms = now_ms
        return "\n".join(lines)

    def maybe_send(self) -> bool:
        try:
            msg = self.run_once()
            if not msg:
                return False
            ok = self._tg.send(msg)
            return bool(ok)
        except Exception as e:
            try:
                self._logger.exception(f"SemDedupReporter failed: {e}")
            except Exception:
                pass
            return False
