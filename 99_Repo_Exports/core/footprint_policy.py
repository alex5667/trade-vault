from __future__ import annotations

from typing import Any, Dict, List, Tuple


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(d)


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return int(d)


def is_soft_confirmation(conf: str) -> bool:
    """
    Soft confirmations НЕ должны влиять на min_confirmations gate по умолчанию,
    иначе вы резко увеличите pass-rate сигналов.
    """
    k = (conf or "").split("=", 1)[0].strip().lower()
    return k in ("fp_imb",)


def fp_confirmations_from_microbar(last_bar: Any, direction: str, cfg: Dict[str, Any]) -> List[str]:
    """
    Генерирует fp_* confirmations из last_bar (микро-бара), без тяжёлых расчётов.
    Все признаки уже вычислены на bar_close и лежат в last_bar.fp_*.

    Разделяем:
    - fp_imb: чистый дисбаланс (оперативный контроль, может быть полезен для импульсных входов)
    - fp_absorb: "low progress + high imbalance + high absorb_score" (абсорбция/разворот)
    """
    out: List[str] = []
    if last_bar is None:
        return out

    if not bool(getattr(last_bar, "fp_enabled", False)):
        return out

    n_b = _i(getattr(last_bar, "fp_n_buckets", 0), 0)
    if n_b <= 0:
        return out

    imb = _f(getattr(last_bar, "fp_max_imbalance", 0.0), 0.0)          # 0..1
    score = _f(getattr(last_bar, "fp_absorb_score", 0.0), 0.0)         # >0
    prog = _f(getattr(last_bar, "fp_progress", 1.0), 1.0)              # 0..1
    bias = str(getattr(last_bar, "fp_absorption_bias", "NONE") or "NONE").upper()

    # -------------------------------
    # A) Imbalance (independent)
    # -------------------------------
    # В Telegram попадёт fp_imb=0.xx даже если absorb_score низкий.
    fp_imb_min = _f(cfg.get("fp_imb_min", 0.80), 0.80)
    fp_imb_min_buckets = _i(cfg.get("fp_imb_min_buckets", 8), 8)
    if imb >= fp_imb_min and n_b >= fp_imb_min_buckets:
        out.append(f"fp_imb={imb:.2f}")

    # -------------------------------
    # B) Absorption (combo)
    # -------------------------------
    fp_abs_min_score = _f(cfg.get("fp_absorb_min_score", 1.00), 1.00)
    fp_abs_min_imb = _f(cfg.get("fp_absorb_min_imbalance", 0.65), 0.65)
    fp_abs_max_prog = _f(cfg.get("fp_absorb_max_progress", 0.35), 0.35)
    fp_abs_require_bias = bool(cfg.get("fp_absorb_require_bias_match", True))

    dir_u = str(direction or "").upper()
    bias_ok = (bias in ("LONG", "SHORT") and bias == dir_u) if fp_abs_require_bias else (bias in ("LONG", "SHORT"))

    if (score >= fp_abs_min_score) and (imb >= fp_abs_min_imb) and (prog <= fp_abs_max_prog) and bias_ok:
        out.append(f"fp_absorb={score:.2f}")

    return out
