from __future__ import annotations

"""
common/signal_metrics.py
-----------------------
Единая "точка правды" для метрик сигналов/вето, чтобы:
  1) candidates_total учитывал и veto (до emitter), и sent (через emitter),
  2) signals_veto имел реальную причину (reason) из ConfirmationsEngine,
  3) защитные метрики (touch/spread/cooldown) работали даже когда veto происходит ДО emitter,
  4) всё было fail-open и не влияло на trading-path.

ВАЖНО:
 - emitter НЕ должен инкрементить candidates_total, иначе будет double-count.
 - handler делает candidates_total + (при veto) signals_veto{reason}.
 - emitter делает signals_sent и veto причины dedup/publish_failed, а также "labels-driven" защитные.
"""

from typing import Any

from common.reason_normalizer import normalize_reason, reason_family
from common.veto_reason_reporter import VetoTopNReporter


class SignalMetrics:
    def __init__(self, metrics: Any) -> None:
        self._m = metrics
        # reporter подключается позже (когда доступен emitter)
        self._veto_reporter: VetoTopNReporter | None = None

    def attach_veto_reporter(self, reporter: VetoTopNReporter) -> None:
        # Одна точка подключения, чтобы не тащить emitter внутрь метрик напрямую.
        self._veto_reporter = reporter

    def _inc(self, name: str, value: int = 1, tags: dict[str, Any] | None = None) -> None:
        m = self._m
        if m is None or not hasattr(m, "inc"):
            return
        try:
            m.inc(name, int(value), tags)
        except Exception:
            return

    def _obs(self, name: str, value: float, tags: dict[str, Any] | None = None) -> None:
        m = self._m
        if m is None or not hasattr(m, "observe"):
            return
        try:
            m.observe(name, float(value), tags)
        except Exception:
            return

    def _base_tags(self, *, ctx: Any, kind: str) -> dict[str, str]:
        # Нормализуем теги так, чтобы дашборды были стабильны.
        tags: dict[str, str] = {"kind": (kind or "unknown")}
        try:
            sym = getattr(ctx, "symbol", None) or ""
            tf = getattr(ctx, "timeframe", None) or ""
            ven = getattr(ctx, "venue", None) or ""
            fam = getattr(ctx, "family", None) or ""
            if sym:
                tags["symbol"] = str(sym)
            if tf:
                tags["timeframe"] = tf
            if ven:
                tags["venue"] = str(ven)
            if fam:
                tags["family"] = str(fam)
        except Exception:
            pass
        return tags

    def candidate(self, *, ctx: Any, kind: str) -> None:
        # candidates_total{kind,...} — всегда на кандидате, ДО любых veto/emit.
        self._inc("candidates_total", 1, self._base_tags(ctx=ctx, kind=kind))

    def veto(self, *, ctx: Any, kind: str, reason: str) -> None:
        # signals_veto{kind,reason} — reason НОРМАЛИЗОВАН, чтобы не взрывать кардинальность.
        reason_norm = normalize_reason(reason, kind=kind)
        fam = reason_family(reason_norm)
        tags = self._base_tags(ctx=ctx, kind=kind)
        tags["reason"] = (reason_norm or "unknown_veto")
        tags["reason_family"] = (fam or "unknown")
        self._inc("signals_veto", 1, tags)

        # Защитные метрики — считаем и на veto-ветке тоже, иначе в дашборде "дыра".
        self._maybe_protective_from_reason(ctx=ctx, kind=kind, reason=reason_norm)

        # Top-N veto reporter (Telegram-ready) — тоже на veto-ветке.
        # Fail-open: любые ошибки тут не должны ломать торговый путь.
        try:
            if self._veto_reporter is not None:
                self._veto_reporter.record(
                    ctx=ctx,
                    kind=(kind or "unknown"),
                    reason_norm=reason_norm,
                    reason_family=(fam or "unknown"),
                    reason_raw=(reason or ""),
                )
        except Exception:
            pass

    def observe_scores(self, *, ctx: Any, kind: str, conf_factor01: float, final_score: float) -> None:
        tags = self._base_tags(ctx=ctx, kind=kind)
        # hist-like метрики: conf_factor_hist{kind}, final_score_hist{kind}
        self._obs("conf_factor_hist", float(conf_factor01), tags)
        self._obs("final_score_hist", float(final_score), tags)

    def _maybe_protective_from_reason(self, *, ctx: Any, kind: str, reason: str) -> None:
        r = (reason or "").lower()
        tags = self._base_tags(ctx=ctx, kind=kind)

        # Мягкое сопоставление по подстрокам (fail-open).
        # Если вы хотите строго — позже можно заменить на таблицу reason->metric.
        if "cooldown" in r:
            self._inc("cooldown_drops", 1, tags)
        if "spread" in r:
            self._inc("spread_filter_drops", 1, tags)
        if "touch" in r:
            self._inc("touch_suppressed_total", 1, tags)
