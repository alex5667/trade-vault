from __future__ import annotations

"""Telegram compact confirmations formatting.

Goal
----
Keep Telegram messages short and actionable:
- show only compact confirmation evidence
- avoid dumping full indicators
- keep stable order for parsing by humans

This helper is designed to be called from core/crypto_signal_formatter.py.

Supported evidence (best-effort)
--------------------------------
- reclaim (bool/int)
- obi_stable_secs + obi_stability_score
- iceberg_strict (bool/int)
- fp_edge_absorb (bool/int) + optional strength
- weak_progress ratios (weak_range_atr/weak_body_atr or weak_recent_cnt)

The function accepts a dict of indicators and a list of confirmation strings
("key=value"). It prefers indicators when available.
"""

from collections.abc import Iterable
from typing import Any
import contextlib


def _clamp01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


def _parse_confirmations(confirmations: Iterable[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for c in confirmations or []:
        try:
            if not c:
                continue
            if "=" in c:
                k, v = c.split("=", 1)
                out[str(k).strip()] = str(v).strip()
            else:
                out[str(c).strip()] = "1"
        except Exception:
            continue
    return out


def build_compact_confirmations(
    *,
    indicators: dict[str, Any] | None = None,
    confirmations: Iterable[str] | None = None,
) -> str:
    """Return a single-line compact evidence string.

    Example:
      "reclaim, obi=2.3s q=0.92, ice, fp=1.4x, weakP=0.27, weak5=3/5"

    Returns empty string if nothing meaningful.
    """

    ind = indicators or {}
    conf = _parse_confirmations(confirmations or [])

    parts: list[str] = []

    # liquidity regime (only if thin/stressed)
    liq_rg = ind.get("liq_regime") or conf.get("liq_regime")
    liq_sc = ind.get("liq_score") or conf.get("liq_score")
    try:
        if liq_rg and str(liq_rg) != "normal":
            if liq_sc is not None:
                parts.append(f"liq={str(liq_rg)} {float(liq_sc):.2f}")
            else:
                parts.append(f"liq={str(liq_rg)}")
    except Exception:
        pass

    # reclaim
    reclaim = ind.get("reclaim")
    if reclaim is None:
        reclaim = conf.get("reclaim")
    if reclaim is not None:
        try:
            if int(reclaim) == 1:
                parts.append("reclaim")
        except Exception:
            # allow bool
            if bool(reclaim):
                parts.append("reclaim")

    # OBI stable
    obi_secs = ind.get("obi_stable_secs")
    if obi_secs is None:
        obi_secs = ind.get("obi_stable")  # fallback
    if obi_secs is None:
        # some pipelines encode as confirmation "obi_stable=2.0s"
        v = conf.get("obi_stable")
        if v is not None:
            try:
                obi_secs = float(str(v).replace("s", "").strip())
            except Exception:
                obi_secs = None

    obi_q = ind.get("obi_stability_score")
    if obi_q is None:
        v = conf.get("obi_q")
        if v is not None:
            try:
                obi_q = float(v)
            except Exception:
                obi_q = None

    if obi_secs is not None:
        try:
            secs_f = float(obi_secs)
            if secs_f > 0:
                if obi_q is not None:
                    q = _clamp01(float(obi_q))
                    parts.append(f"obi={secs_f:.1f}s q={q:.2f}")
                else:
                    parts.append(f"obi={secs_f:.1f}s")
        except Exception:
            pass

    # CVD reclaim ratio (bonus-layer confirmation)
    cvdR = None
    # Try indicators first
    cvdR_val = ind.get("cvd_reclaim_ratio")
    if cvdR_val is not None:
        with contextlib.suppress(Exception):
            cvdR = float(cvdR_val)
    # Fallback to confirmations list (cvdR=X.XX)
    if cvdR is None:
        v = conf.get("cvdR")
        if v is not None:
            with contextlib.suppress(Exception):
                cvdR = float(v)

    if cvdR is not None and cvdR > 0:
        with contextlib.suppress(Exception):
            parts.append(f"cvdR={cvdR:.2f}")

    # iceberg strict
    ice = ind.get("iceberg_strict")
    if ice is None:
        ice = conf.get("iceberg_strict")
    if ice is not None:
        try:
            if int(ice) == 1:
                parts.append("ice")
        except Exception:
            if bool(ice):
                parts.append("ice")

    # OFI stable
    ofi_secs = ind.get("ofi_stable_secs")
    if ofi_secs is None:
        v = conf.get("ofi_stable")
        if v is not None:
            try:
                ofi_secs = float(str(v).replace("s", "").strip())
            except Exception:
                ofi_secs = None

    ofi_q = ind.get("ofi_q")
    if ofi_q is None:
        v = conf.get("ofi_q")
        if v is not None:
            try:
                ofi_q = float(v)
            except Exception:
                ofi_q = None

    if ofi_secs is not None:
        try:
            secs_f = float(ofi_secs)
            if secs_f > 0:
                if ofi_q is not None:
                    q = _clamp01(float(ofi_q))
                    parts.append(f"ofi={secs_f:.1f}s q={q:.2f}")
                else:
                    parts.append(f"ofi={secs_f:.1f}s")
        except Exception:
            pass

    # fp_edge_absorb
    fp = ind.get("fp_edge_absorb")
    if fp is None:
        fp = conf.get("fp_edge_absorb")
    if fp is not None:
        try:
            if int(fp) == 1:
                fp_strength = ind.get("fp_edge_absorb_strength")
                if fp_strength is None:
                    fp_strength = ind.get("fp_edge_strength")
                if fp_strength is not None:
                    try:
                        fs = float(fp_strength)
                        if fs > 0:
                            parts.append(f"fp={fs:.2f}x")
                        else:
                            parts.append("fp")
                    except Exception:
                        parts.append("fp")
                else:
                    parts.append("fp")
        except Exception:
            if bool(fp):
                parts.append("fp")

    # weak progress / trend
    # 1) per-bar normalized ratio (preferred)
    wp = ind.get("weak_range_atr")
    if wp is None:
        wp = ind.get("weak_progress_atr")  # legacy naming
    if wp is not None:
        try:
            wpf = float(wp)
            if wpf > 0:
                parts.append(f"weakP={wpf:.2f}")
        except Exception:
            pass

    # 2) trend window
    wcnt = ind.get("weak_recent_cnt")
    if wcnt is None:
        wcnt = ind.get("weak_recent_count")
    wwin = ind.get("weak_recent_window")
    if wcnt is not None:
        try:
            c = int(wcnt)
            if wwin is not None:
                w = int(wwin)
                parts.append(f"weak{w}={c}/{w}")
            else:
                parts.append(f"weakN={c}")
        except Exception:
            pass

    # Hidden Divergence
    hdiv = ind.get("hidden_div_used")
    if hdiv is not None and int(hdiv) == 1:
        parts.append("hDiv")

    # cvd reclaim (bonus-only)
    cvdr = ind.get("cvd_reclaim")
    if cvdr is None:
        cvdr = conf.get("cvd_reclaim")
    if cvdr is not None:
        try:
            if int(cvdr) == 1:
                parts.append("cvdR")
        except Exception:
            if bool(cvdr):
                parts.append("cvdR")

    return ", ".join(parts)
