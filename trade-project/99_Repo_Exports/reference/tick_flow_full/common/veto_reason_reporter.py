from __future__ import annotations

"""
common/veto_reason_reporter.py
------------------------------
Top-N агрегация veto-reasons -> 1 компактное сообщение в outbox_labels (label_update),
чтобы downstream (TG/WS) отправлял человеку "что именно блокирует эмит".

Задача: убрать спам и сохранить смысл:
  - считаем окно W (default 5m)
  - строим топ-N reasons по (kind,symbol)
  - алертим только когда:
        total >= MIN_TOTAL
    AND (top_share >= ALERT_SHARE OR top_count >= ALERT_COUNT)
  - вводим cooldown, чтобы не алертить постоянно одно и то же

Поля payload:
  kind="label_update" (у вас уже отдельный outbox_labels)
  labels: {"analytics":1,"type":"veto_topn", ...}
  text: готовый human-readable текст
"""

import os
import time
import hashlib
from dataclasses import dataclass
from typing import Any, Callable, Optional


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


@dataclass(frozen=True)
class _Key:
    symbol: str
    kind: str


class VetoTopNReporter:
    def __init__(
        self,
        *,
        emitter: Any,
        logger: Any,
        now_ms_fn: Optional[Callable[[], int]] = None,
    ) -> None:
        self._emitter = emitter
        self._logger = logger
        self._now_ms = now_ms_fn or (lambda: int(time.time() * 1000))

        # окно агрегации
        self._win_ms = _env_int("VETO_TOPN_WINDOW_MS", 300_000)  # 5m
        self._n = _env_int("VETO_TOPN_N", 5)
        self._min_total = _env_int("VETO_TOPN_MIN_TOTAL", 30)
        self._alert_share = _env_float("VETO_TOPN_ALERT_SHARE", 0.50)
        self._alert_count = _env_int("VETO_TOPN_ALERT_COUNT", 50)
        self._cooldown_ms = _env_int("VETO_TOPN_COOLDOWN_MS", 600_000)  # 10m
        # "Смена доминанты" — отдельный алерт (ещё один ¼ гайки)
        self._change_min_share = _env_float("VETO_TOPN_CHANGE_MIN_SHARE", 0.35)
        self._change_cooldown_ms = _env_int("VETO_TOPN_CHANGE_COOLDOWN_MS", 900_000)  # 15m

        # "⅛ гайки": alert по смене FAMILY (ещё более устойчиво и почти без кардинальности)
        # Это полезнее, чем смена конкретного reason, потому что:
        #  - reason может "дрожать" от мелких деталей,
        #  - family показывает смену класса проблем (L2 gate -> confidence gate -> spread gate).
        self._fam_change_min_share = _env_float("VETO_TOPN_FAMILY_CHANGE_MIN_SHARE", 0.45)
        self._fam_change_cooldown_ms = _env_int("VETO_TOPN_FAMILY_CHANGE_COOLDOWN_MS", 900_000)  # 15m
        # "следующий микро-дожим": алертим смену family только если доминанта стала
        # заметно "сильнее/концентрированнее", чем была в прошлом окне.
        # Это убирает FP, когда family "переключилась" на волоске 0.48 -> 0.50.
        self._fam_change_min_delta = _env_float("VETO_TOPN_FAMILY_CHANGE_MIN_DELTA", 0.20)
        # "ещё ¼ гайки": смена family должна сопровождаться реальным ухудшением,
        # а не просто перестановкой причин внутри того же объёма veto.
        #
        # Идея: алертить family-change только если:
        #   - total_veto вырос заметно (abs delta) ИЛИ
        #   - total_veto вырос заметно (ratio к прошлому окну)
        #
        # Это режет FP, когда "топ-проблема" сменилась, но общий поток veto не изменился.
        self._fam_change_min_total_delta = _env_int("VETO_TOPN_FAMILY_CHANGE_MIN_TOTAL_DELTA", 10)
        self._fam_change_min_total_ratio = _env_float("VETO_TOPN_FAMILY_CHANGE_MIN_TOTAL_RATIO", 1.15)  # +15%

        # state
        self._last_flush_ms = self._now_ms()
        self._total: dict[_Key, int] = {}
        self._counts: dict[tuple[_Key, str], int] = {}
        self._family_counts: dict[tuple[_Key, str], int] = {}
        self._cooldown: dict[tuple[_Key, str], int] = {}  # (key, top_reason)->last_emit_ms
        self._last_top_reason: dict[_Key, str] = {}       # last window top reason per (symbol,kind)
        self._last_change_emit_ms: dict[_Key, int] = {}   # cooldown for "change" alerts
        self._last_top_family: dict[_Key, str] = {}       # last window top family per (symbol,kind)
        self._last_family_change_emit_ms: dict[_Key, int] = {}
        self._last_top_family_share: dict[_Key, float] = {}  # last window top family share per (symbol,kind)
        self._last_total_veto: dict[_Key, int] = {}          # last window total veto per (symbol,kind)

    def record(self, *, ctx: Any, kind: str, reason_norm: str, reason_family: str, reason_raw: str) -> None:
        try:
            sym = str(getattr(ctx, "symbol", "") or "")
        except Exception:
            sym = ""
        k = _Key(symbol=sym or "unknown", kind=str(kind or "unknown"))

        self._total[k] = int(self._total.get(k, 0) + 1)
        rk = (k, str(reason_norm or "unknown_veto"))
        self._counts[rk] = int(self._counts.get(rk, 0) + 1)
        fk = (k, str(reason_family or "unknown"))
        self._family_counts[fk] = int(self._family_counts.get(fk, 0) + 1)

        # opportunistic flush (cheap)
        self.maybe_flush(ctx=ctx)

    def maybe_flush(self, *, ctx: Any) -> None:
        now = self._now_ms()
        if (now - self._last_flush_ms) < self._win_ms:
            return
        self._last_flush_ms = now

        # Снимем срез и очистим (fixed memory)
        total = self._total
        counts = self._counts
        fam_counts = self._family_counts
        self._total = {}
        self._counts = {}
        self._family_counts = {}

        for key, t in total.items():
            if t < self._min_total:
                continue

            # собрать reasons по key
            items: list[tuple[str, int]] = []
            for (k2, reason_norm), c in counts.items():
                if k2 == key:
                    items.append((reason_norm, int(c)))
            if not items:
                continue

            items.sort(key=lambda x: x[1], reverse=True)
            top_reason, top_count = items[0]
            top_share = (float(top_count) / float(max(1, t)))

            # top family for this (symbol, kind)
            fam_items: list[tuple[str, int]] = []
            for (k2, fam), c in fam_counts.items():
                if k2 == key:
                    fam_items.append((fam, int(c)))
            fam_items.sort(key=lambda x: x[1], reverse=True)
            top_fam = fam_items[0][0] if fam_items else "unknown"
            top_fam_share = (float(fam_items[0][1]) / float(max(1, t))) if fam_items else 0.0

            # ---- ¼ гайки: алерт "доминанта сменилась" ----
            # Нужен для релизов порогов/политик: видно, что теперь основная причина veto другая.
            # Чтобы не шуметь при плоском распределении: требуем минимальную долю доминанты.
            prev_top = self._last_top_reason.get(key)
            self._last_top_reason[key] = top_reason
            if prev_top and prev_top != top_reason and top_share >= self._change_min_share:
                last_change = int(self._last_change_emit_ms.get(key, 0))
                if (now - last_change) >= self._change_cooldown_ms:
                    self._last_change_emit_ms[key] = now
                    try:
                        self._emit_change(
                            ctx=ctx,
                            key=key,
                            total=t,
                            prev_top=prev_top,
                            new_top=top_reason,
                            new_top_share=top_share,
                            fam_counts=fam_counts,
                            now_ms=now,
                        )
                    except Exception as e:
                        try:
                            self._logger.exception(f"VetoTopNReporter change emit failed: {e}")
                        except Exception:
                            pass

            # ---- "⅛ гайки": alert "доминанта FAMILY сменилась" ----
            prev_fam = self._last_top_family.get(key)
            prev_fam_share = float(self._last_top_family_share.get(key, 0.0))
            self._last_top_family[key] = top_fam
            self._last_top_family_share[key] = float(top_fam_share)
            prev_total = self._last_total_veto.get(key)
            self._last_total_veto[key] = int(t)

            # delta считается относительно "прошлой доминанты" (предыдущего окна):
            # это простой, устойчивый критерий "насколько поменялась концентрация veto"
            # при смене класса проблем.
            fam_share_delta = float(top_fam_share) - float(prev_fam_share)

            # "¼ гайки" (volume gate):
            # не алертим смену family, если общий объём veto не ухудшился.
            # - abs gate защищает при малых N (важнее "+10 veto" чем "+15%" от 10).
            # - ratio gate защищает при больших N (важнее "+15%" чем "+10" при 1000).
            total_delta = (int(t) - int(prev_total)) if prev_total is not None else 0
            total_ratio = (float(t) / float(max(1, int(prev_total)))) if prev_total is not None else 0.0
            total_gate_pass = True
            if prev_total is not None:
                total_gate_pass = (
                    total_delta >= int(self._fam_change_min_total_delta)
                    or total_ratio >= float(self._fam_change_min_total_ratio)
                )
            if prev_fam and prev_fam != top_fam and top_fam_share >= self._fam_change_min_share:
                # микро-дожим: если концентрация почти не изменилась — не спамим.
                if fam_share_delta < self._fam_change_min_delta:
                    continue
                # "¼ гайки": если объём veto не ухудшился — не спамим смену family.
                if not total_gate_pass:
                    continue
                last_fam = int(self._last_family_change_emit_ms.get(key, 0))
                if (now - last_fam) >= self._fam_change_cooldown_ms:
                    self._last_family_change_emit_ms[key] = now
                    try:
                        self._emit_family_change(
                            ctx=ctx,
                            key=key,
                            total=t,
                            prev_total=int(prev_total) if prev_total is not None else None,
                            total_delta=int(total_delta),
                            total_ratio=float(total_ratio),
                            prev_family=prev_fam,
                            new_family=top_fam,
                            new_family_share=top_fam_share,
                            new_family_share_delta=fam_share_delta,
                            top_reason=top_reason,
                            top_reason_share=top_share,
                            now_ms=now,
                        )
                    except Exception as e:
                        try:
                            self._logger.exception(f"VetoTopNReporter family change emit failed: {e}")
                        except Exception:
                            pass

            if not (top_share >= self._alert_share or top_count >= self._alert_count):
                continue

            cd_key = (key, top_reason)
            last_emit = int(self._cooldown.get(cd_key, 0))
            if (now - last_emit) < self._cooldown_ms:
                continue
            self._cooldown[cd_key] = now

            # emit analytics summary to outbox_labels via label_update
            try:
                self._emit_summary(
                    ctx=ctx,
                    key=key,
                    total=t,
                    items=items[: max(1, self._n)],
                    top_reason=top_reason,
                    top_share=top_share,
                    fam_counts=fam_counts,
                    now_ms=now,
                )
            except Exception as e:
                try:
                    self._logger.exception(f"VetoTopNReporter emit failed: {e}")
                except Exception:
                    pass

    def _emit_summary(
        self,
        *,
        ctx: Any,
        key: _Key,
        total: int,
        items: list[tuple[str, int]],
        top_reason: str,
        top_share: float,
        fam_counts: dict[tuple[_Key, str], int],
        now_ms: int,
    ) -> None:
        # Human-readable текст (готовый для TG).
        lines = []
        lines.append(f"VETO Top reasons (window={int(self._win_ms/1000)}s)")
        lines.append(f"symbol={key.symbol} kind={key.kind} total_veto={total}")
        lines.append(f"top={top_reason} share={top_share:.2f} ({items[0][1]}/{total})")
        lines.append("")
        lines.append("Top-N:")
        for r, c in items:
            share = float(c) / float(max(1, total))
            lines.append(f"  - {r}: {c} ({share:.2f})")

        # Top family (ещё более грубо, почти без кардинальности)
        fam_items: list[tuple[str, int]] = []
        for (k2, fam), c in fam_counts.items():
            if k2 == key:
                fam_items.append((fam, int(c)))
        fam_items.sort(key=lambda x: x[1], reverse=True)
        if fam_items:
            fam, c = fam_items[0]
            lines.append("")
            lines.append(f"Top family: {fam} ({c}/{total} = {float(c)/float(max(1,total)):.2f})")

        lines.append("")
        lines.append("Action hint:")
        lines.append(self._hint_for_top(top_reason, key.kind))

        text = "\n".join(lines)

        sid = hashlib.sha1(f"veto_topn|{key.symbol}|{key.kind}|{top_reason}|{now_ms//self._win_ms}".encode("utf-8")).hexdigest()
        payload = {
            "kind": "label_update",
            "symbol": key.symbol,
            "ts": now_ms,
            "signal_id": sid,
            "title": "veto_topn",
            "text": text,
            "labels": {
                "analytics": 1,
                "type": "veto_topn",
                "symbol": key.symbol,
                "signal_kind": key.kind,
                "top_reason": top_reason,
                "top_share": float(top_share),
                "total_veto": int(total),
                "window_sec": int(self._win_ms / 1000),
            },
        }
        # dedup=True безопасно: sid стабилен на окно
        self._emitter.emit(payload, labels=None, dedup=True)

    def _hint_for_top(self, top_reason: str, kind: str) -> str:
        r = (top_reason or "").lower()
        k = (kind or "").lower()

        if r == "bo_l2_fail_closed":
            return "Breakout fail-closed: проверьте L2 feed/стейлнес, L2 TTL, и stale thresholds. Без книги breakout блокируется намеренно."
        if "conf_below_min" in r:
            return "Confidence ниже минимума: посмотрите conf_factor_hist/final_score_hist, и min_conf per symbol (не завышен ли порог)."
        if r == "spread_filter_veto":
            return "Фильтр спреда: проверьте spread_bps, impact, и не слишком ли агрессивный reject. Можно перевести часть в scaling вместо veto."
        if r == "cooldown":
            return "Cooldown: возможно слишком короткий bucket/частые кандидаты. Проверьте cooldown window и dedup policies."
        if r == "touch_suppressed":
            return "Touch suppression: вероятно уровень часто 'шумно' трогается. Проверьте touch rules / wall-distance thresholds."
        if r == "l3_missing":
            return "L3 отсутствует: должно быть fail-open со штрафом (l3_score=0.5). Если видите veto — проверьте reason mapping."
        if r.startswith("bo_l2_"):
            return "Breakout L2 veto: проверьте confirm_breakout thresholds (wall distance / imbalance / stale L2)."
        if r.startswith("l2_") or r.startswith("l3_"):
            return "Book/L3 veto: проверьте quality flags и staleness. Лучше сначала смотреть distribution по qf codes."
        return "Сфокусируйтесь на top_reason: сравните p95 лаги, стейлнес L2/L3, и пороги min_conf/spread/cooldown."

    def _emit_change(
        self,
        *,
        ctx: Any,
        key: _Key,
        total: int,
        prev_top: str,
        new_top: str,
        new_top_share: float,
        fam_counts: dict[tuple[_Key, str], int],
        now_ms: int,
    ) -> None:
        lines = []
        lines.append("VETO dominant reason changed")
        lines.append(f"symbol={key.symbol} kind={key.kind} total_veto={total}")
        lines.append(f"prev={prev_top}")
        lines.append(f"new={new_top} share={new_top_share:.2f}")

        fam_items: list[tuple[str, int]] = []
        for (k2, fam), c in fam_counts.items():
            if k2 == key:
                fam_items.append((fam, int(c)))
        fam_items.sort(key=lambda x: x[1], reverse=True)
        if fam_items:
            fam, c = fam_items[0]
            lines.append(f"top_family={fam} ({c}/{total}={float(c)/float(max(1,total)):.2f})")

        lines.append("")
        lines.append("Why it matters:")
        lines.append("  - часто это эффект релиза порогов/политик или деградации данных (L2/L3 lag).")
        lines.append("  - сравните до/после по l2_stale_rate, l3_missing_rate, tick_lag_ms_p95, conf_factor_hist.")

        text = "\n".join(lines)
        sid = hashlib.sha1(
            f"veto_change|{key.symbol}|{key.kind}|{prev_top}|{new_top}|{now_ms//self._win_ms}".encode("utf-8")
        ).hexdigest()
        payload = {
            "kind": "label_update",
            "symbol": key.symbol,
            "ts": now_ms,
            "signal_id": sid,
            "title": "veto_topn_change",
            "text": text,
            "labels": {
                "analytics": 1,
                "type": "veto_topn_change",
                "symbol": key.symbol,
                "signal_kind": key.kind,
                "prev_top_reason": prev_top,
                "new_top_reason": new_top,
                "new_top_share": float(new_top_share),
                "total_veto": int(total),
                "window_sec": int(self._win_ms / 1000),
            },
        }
        self._emitter.emit(payload, labels=None, dedup=True)

    def _emit_family_change(
        self,
        *,
        ctx: Any,
        key: _Key,
        total: int,
        prev_total: Optional[int],
        total_delta: int,
        total_ratio: float,
        prev_family: str,
        new_family: str,
        new_family_share: float,
        new_family_share_delta: float,
        top_reason: str,
        top_reason_share: float,
        now_ms: int,
    ) -> None:
        # Это самый "дешёвый" алерт: family почти не имеет кардинальности.
        lines = []
        lines.append("VETO dominant FAMILY changed")
        lines.append(f"symbol={key.symbol} kind={key.kind} total_veto={total}")
        lines.append(f"prev_family={prev_family}")
        lines.append(f"new_family={new_family} share={new_family_share:.2f} delta_vs_prev={new_family_share_delta:+.2f}")
        if prev_total is not None:
            lines.append(f"total_veto prev={int(prev_total)} now={int(total)} delta={int(total_delta)} ratio={float(total_ratio):.2f}")
        lines.append("")
        lines.append("Current window top reason (for context):")
        lines.append(f"  - {top_reason} ({top_reason_share:.2f})")
        lines.append("")
        lines.append("Fast triage checklist:")
        if new_family == "book_l2_gate":
            lines.append("  - проверьте l2_stale_rate / l2_missing_rate, задержки стакана, health L2 feed.")
            lines.append("  - для breakout это fail-closed: рост veto ожидаем при деградации L2.")
        elif new_family == "l3_quality":
            lines.append("  - проверьте l3_missing_rate, lag L3, cancel_to_trade/obi_sustained источники.")
            lines.append("  - по политике: l3 missing не должен veto'ить, но снижает conf_factor.")
        elif new_family == "confidence_gate":
            lines.append("  - проверьте conf_factor_hist и min_conf пороги по symbol/kind.")
            lines.append("  - возможен 'пережим' после релиза порогов/весов.")
        elif new_family == "spread_gate":
            lines.append("  - проверьте spread_filter_drops, spread_bps p95, widening по venue.")
        elif new_family == "cooldown_gate":
            lines.append("  - проверьте cooldown_drops, дедуп/кулдауны, частоту кандидатов.")
        elif new_family == "touch_gate":
            lines.append("  - проверьте touch_suppressed_total, настройки touch/tick-size/levels.")
        else:
            lines.append("  - проверьте общий health: tick_lag_ms_p95, missing rates, релизные изменения порогов.")

        text = "\n".join(lines)
        sid = hashlib.sha1(
            f"veto_fam_change|{key.symbol}|{key.kind}|{prev_family}|{new_family}|{now_ms//self._win_ms}".encode("utf-8")
        ).hexdigest()
        payload = {
            "kind": "label_update",
            "symbol": key.symbol,
            "ts": now_ms,
            "signal_id": sid,
            "title": "veto_topn_family_change",
            "text": text,
            "labels": {
                "analytics": 1,
                "type": "veto_topn_family_change",
                "symbol": key.symbol,
                "signal_kind": key.kind,
                "prev_family": prev_family,
                "new_family": new_family,
                "new_family_share": float(new_family_share),
                "new_family_share_delta": float(new_family_share_delta),
                "prev_total_veto": int(prev_total) if prev_total is not None else None,
                "total_veto_delta": int(total_delta),
                "total_veto_ratio": float(total_ratio),
                "top_reason": top_reason,
                "top_reason_share": float(top_reason_share),
                "total_veto": int(total),
                "window_sec": int(self._win_ms / 1000),
            },
        }
        self._emitter.emit(payload, labels=None, dedup=True)
