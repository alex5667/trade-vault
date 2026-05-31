from __future__ import annotations

import os
import time
from collections import defaultdict, deque
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any

from common.qf_codes import QF
from signal_scoring.reason_codes import ReasonCode
from signal_scoring.reason_registry import map_legacy_reason_code, reason_code_to_u16
import contextlib


def _label_sanitize(v: Any, *, max_len: int = 80) -> str:
    """
    Prom/StatsD tags/labels должны быть короткими и "безопасными".
    - режем длину, чтобы не взорвать кардинальность
    - нормализуем пробелы/таб/переводы строк
    """
    s = (v or "").strip()
    s = " ".join(s.split())
    if len(s) > max_len:
        s = s[:max_len]
    return s


class _Metrics:
    """
    Минимальный интерфейс метрик.
    Поддерживает и StatsD-стиль, и Prom-обёртки.
    """
    def incr(self, name: str, value: int = 1, tags: dict[str, str] | None = None) -> None:  # pragma: no cover
        raise NotImplementedError


@dataclass(frozen=True)
class LegacyMapAlertConfig:
    """
    "1/1024 гайки": алерт на внезапный рост legacy reason codes.

    Логика (простая и безопасная):
      - храним timestamps маппингов в скользящем окне window_s
      - если за окно набралось >= min_events -> триггерим notify (с cooldown)
    """
    window_s: int = int(os.getenv("REASON_LEGACY_ALERT_WINDOW_S", "300"))          # 5 минут
    min_events: int = int(os.getenv("REASON_LEGACY_ALERT_MIN_EVENTS", "50"))       # порог "всплеска"
    cooldown_s: int = int(os.getenv("REASON_LEGACY_ALERT_COOLDOWN_S", "900"))      # 15 минут, чтобы не спамить


# -----------------------------------------
# 1/64 гайки: ReasonCode -> allowed kinds
# -----------------------------------------
#
# Зачем:
# - У вас "reason" исторически был строкой (bo_l2_stale, spread_wide, ...)
# - Теперь появляется structured reason_code (VETO_*)
# - При рефакторингах легко "поехать": например, вернуть VETO_REGIME_RANGE_BREAKOUT
#   для absorption/extreme по ошибке (copy/paste).
#
# Решение:
# - Для каждого VETO_* кода задаём whitelist kinds (либо "*" = разрешено везде).
# - Дальше в Engine нормализуем: если reason_code не разрешён для данного kind,
#   то превращаем в VETO_UNKNOWN и сохраняем отладочную метку в parts.
#
# Это не меняет "veto/non-veto" (сигнал по-прежнему блокируется),
# но делает причину строго корректной и стабильной для дашбордов/калибровки.


ANY: set[str] = {"*"}


def _k(*xs: str) -> set[str]:
    return set(xs)


@dataclass(frozen=True)
class ReasonPolicyEntry:
    """
    allowed_kinds:
      - {"*"} => разрешено для любого kind
      - {"breakout","absorption",...} => whitelist
    mismatch_severity:
      - "warn": нештатно, но возможно (эволюция пайплайна)
      - "error": почти наверняка баг/дрейф контракта
    """
    allowed_kinds: set[str]
    mismatch_severity: str = "warn"  # "warn" | "error"


# ВАЖНО:
# - Делайте явное покрытие для ВСЕХ VETO_* кодов (тест ниже это гарантирует).
# - Для общих причин используйте "*" (ANY), чтобы не "пережимать".
POLICY: dict[str, ReasonPolicyEntry] = {
    # ---------- universal gates ----------
    ReasonCode.VETO_SPREAD_WIDE.value: ReasonPolicyEntry(ANY, "warn"),
    ReasonCode.VETO_CONF_BELOW_MIN.value: ReasonPolicyEntry(ANY, "warn"),
    ReasonCode.VETO_L3_SPOOF_RISK.value: ReasonPolicyEntry(ANY, "warn"),
    ReasonCode.VETO_WALL_NEAR.value: ReasonPolicyEntry(ANY, "warn"),

    # ---------- L2: fail-closed breakout ----------
    # По вашей политике: breakout без L2 = не торгуем.
    ReasonCode.VETO_L2_MISSING.value: ReasonPolicyEntry(_k("breakout"), "error"),
    ReasonCode.VETO_L2_STALE.value: ReasonPolicyEntry(_k("breakout"), "error"),
    ReasonCode.VETO_L2_BAD.value: ReasonPolicyEntry(_k("breakout"), "error"),

    # ---------- regime gates ----------
    # "range breakout" по смыслу относится к breakout (не absorption/extreme).
    ReasonCode.VETO_REGIME_RANGE_BREAKOUT.value: ReasonPolicyEntry(_k("breakout"), "error"),

    # ---------- placeholders for future ----------
    # Если у вас уже есть другие VETO_* коды — добавьте их сюда.
    # Тест test_reason_policy_covers_all_veto_codes заставит это сделать.
}


def get_policy(reason_code: str) -> ReasonPolicyEntry | None:
    return POLICY.get(reason_code)


def is_reason_allowed_for_kind(reason_code: str, kind: str) -> bool:
    p = get_policy(reason_code)
    if not p:
        return False
    if "*" in p.allowed_kinds:
        return True
    return (kind or "") in p.allowed_kinds


def normalize_reason_for_kind(*, reason_code: str, kind: str, parts: dict) -> tuple[str, str]:
    """
    Return a reason_code that is valid for this kind.
    If mismatch -> VETO_UNKNOWN and annotate parts (debuggable, dashboard-friendly).
    Returns: (normalized_reason_code, mismatch_severity)
    """
    p = get_policy(reason_code)
    if p and is_reason_allowed_for_kind(reason_code, kind):
        return reason_code, p.mismatch_severity

    # Preserve a breadcrumb for debugging/counterfactuals.
    try:
        parts.setdefault("reason_kind_mismatch", {})
        if isinstance(parts["reason_kind_mismatch"], dict):
            parts["reason_kind_mismatch"].update(
                {"kind": (kind or ""), "reason_code": (reason_code or "")}
            )
        else:
            parts["reason_kind_mismatch"] = {"kind": (kind or ""), "reason_code": (reason_code or "")}
    except Exception:
        # fail-open: parts are best-effort
        pass

    sev = (p.mismatch_severity if p else "error")
    return ReasonCode.VETO_UNKNOWN.value, sev


MetricInc = Callable[[str, dict[str, str], int], None]
AlertEmit = Callable[[dict[str, Any]], None]


class ReasonMismatchMonitor:
    """
    Монитор контрактных дрейфов:
      1) kind<->reason mismatch (политика нормализует reason)  -> reason_kind_mismatch_total
      2) legacy->canonical mapping (registry)                  -> reason_legacy_mapped_total (то, что вы просили)

    Также умеет "мягкий" алерт на всплеск legacy-кодов через notify callback.
    """
    def __init__(
        self,
        *,
        metrics: _Metrics | None = None,
        notify: Callable[[dict[str, Any]], None] | None = None,
        alert_cfg: LegacyMapAlertConfig | None = None,
    ) -> None:
        self._m = metrics
        self._notify = notify
        self._alert_cfg = alert_cfg or LegacyMapAlertConfig()
        # key=(kind,from,to) -> deque[timestamps]
        self._legacy_ts: defaultdict[tuple[str, str, str], deque[float]] = defaultdict(deque)
        # key=(kind,from,to) -> last alert time
        self._legacy_last_alert: dict[tuple[str, str, str], float] = {}

    def observe(self, *, kind: str, original_rc: str, normalized_rc: str, mismatch_sev: str) -> None:
        # существующая метрика/логика mismatch (оставляем, как у вас было/ожидалось)
        if self._m is not None:
            with contextlib.suppress(Exception):
                self._m.incr(
                    "reason_kind_mismatch_total",
                    1,
                    tags={
                        "kind": _label_sanitize(kind),
                        "from": _label_sanitize(original_rc),
                        "to": _label_sanitize(normalized_rc),
                        "sev": _label_sanitize(mismatch_sev),
                    }
                )

    def observe_legacy_map(self, *, kind: str, rc_from: str, rc_to: str, now_s: float | None = None) -> None:
        """
        ВАЖНО: это вызывается ТОЛЬКО если registry реально сделал mapping.
        """
        k = _label_sanitize(kind)
        f = _label_sanitize(rc_from)
        t = _label_sanitize(rc_to)
        key = (k, f, t)
        now = float(now_s if now_s is not None else time.time())

        # 1) requested metric: reason_legacy_mapped_total{kind,from,to}
        if self._m is not None:
            with contextlib.suppress(Exception):
                self._m.incr("reason_legacy_mapped_total", 1, tags={"kind": k, "from": f, "to": t})

        # 2) rolling-window spike detection (optional notify)
        dq = self._legacy_ts[key]
        dq.append(now)
        win = max(1, int(self._alert_cfg.window_s))
        cutoff = now - win
        while dq and dq[0] < cutoff:
            dq.popleft()

        if self._notify is None:
            return

        # cooldown per (kind,from,to)
        last = float(self._legacy_last_alert.get(key, 0.0) or 0.0)
        if (now - last) < max(1, int(self._alert_cfg.cooldown_s)):
            return

        if len(dq) >= max(1, int(self._alert_cfg.min_events)):
            # фиксируем и шлём "объяснимый" алерт downstream (TG/WS/лог)
            self._legacy_last_alert[key] = now
            try:
                self._notify(
                    {
                        "kind": "diag_reason_legacy_spike",
                        "ts": int(now * 1000),
                        "severity": "warning",
                        "labels": {
                            "kind": k,
                            "from": f,
                            "to": t,
                            "window_s": win,
                            "count": len(dq),
                            "hint": (
                                "Всплеск legacy reason_code: где-то начали отдавать старые строки "
                                "(или откатили часть кода). Проверьте места формирования veto reasons "
                                "и совместимость версий producer/consumer."
                            )
                        }
                    }
                )
            except Exception:
                # fail-open: алерт не должен ломать сигналинг
                pass


def patch_validation_reason_for_kind(
    *,
    validation: Validation,
    kind: str,
    monitor: ReasonMismatchMonitor | None = None,
) -> Validation:
    """
    Convenience wrapper: mutate/return a Validation with kind-safe reason_code + reason_u16.
    Avoids importing engine.Validation here (string-typed to keep imports light).
    """
    if not getattr(validation, "veto", False):
        return validation

    parts = dict(getattr(validation, "parts", {}) or {})
    rc_in = str(getattr(validation, "reason_code", "") or "")

    # ----------------------------
    # STAGE 1: legacy -> canonical
    # ----------------------------
    flags = list(getattr(validation, "flags", None) or [])
    rc1, legacy_orig = map_legacy_reason_code(rc_in)
    if legacy_orig is not None and rc1 != rc_in:
        # маркируем "переименование" отдельно от kind-mismatch
        try:
            code = int(QF.REASON_LEGACY_MAPPED)
            if code not in flags:
                flags.append(code)
            parts.setdefault("reason_legacy_mapped_flag", 1)
            parts.setdefault("reason_code_original_legacy", legacy_orig)
            parts.setdefault("reason_code_after_legacy_map", rc1)
        except Exception:
            pass

    # ----------------------------
    # STAGE 2: kind-safe policy
    # ----------------------------
    new_rc, sev = normalize_reason_for_kind(reason_code=rc1, kind=kind, parts=parts)

    # 1/256 гайки: если reason нормализован => это контрактный дрейф/несовместимость.
    # Пробрасываем в qf-флаги, чтобы это было видно в payload (qf/qf16) и в метриках по qf.
    if new_rc != rc1:
        try:
            code = int(QF.REASON_KIND_MISMATCH)
            if code not in flags:
                flags.append(code)
            # также оставим "breadcrumb" в parts (удобно для отладки без unpack qf16)
            parts.setdefault("reason_kind_mismatch_flag", 1)
            parts.setdefault("reason_kind_mismatch_sev", sev)
            parts.setdefault("reason_code_original", rc1)
        except Exception:
            # fail-open: flags/parts best-effort
            pass

    if monitor is not None:
        # 1) legacy-map metric/alert (то, что вы просили в 1/1024)
        if legacy_orig is not None and rc1 != rc_in:
            with contextlib.suppress(Exception):
                monitor.observe_legacy_map(kind=kind, rc_from=legacy_orig, rc_to=rc1)
        # 2) mismatch metric
        try:
            # original_rc = до policy (после legacy-map), чтобы монитор не шумел на переименования
            monitor.observe(kind=kind, original_rc=rc1, normalized_rc=new_rc, mismatch_sev=sev)
        except Exception:
            pass

    if new_rc == rc_in and parts is getattr(validation, "parts", None) and flags == getattr(validation, "flags", None):
        return validation

    # Update u16 after normalization
    u16 = int(reason_code_to_u16(new_rc) or 0)
    return replace(validation, reason_code=new_rc, reason_u16=u16, parts=parts, flags=flags)
