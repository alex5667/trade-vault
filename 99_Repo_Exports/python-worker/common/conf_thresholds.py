from __future__ import annotations

from collections.abc import Mapping


def _f(env: Mapping[str, str], k: str, d: float) -> float:
    try:
        v = env.get(k)
        if v is None or v == "":
            return d
        return float(v)
    except Exception:
        return d


def compute_conf_thresholds(env: Mapping[str, str], sym_u: str) -> tuple[float, float]:
    """
    Чистая функция для hot-path порогов уверенности.

    СОВМЕСТИМО с текущей логикой из crypto_orderflow_handler.py:
      min_conf = ENV[MIN_CONF_{SYM}]               fallback ENV[MIN_CONF_DEFAULT]
      min_cf   = ENV[MIN_CONF_FACTOR_{SYM}]       fallback ENV[MIN_CONF_FACTOR_DEFAULT]

    Почему отдельная функция:
      - легко тестировать,
      - не тянет os.getenv(),
      - можно переиспользовать в разных местах (handler/pipeline).
    """
    sym_u = (sym_u or "").strip()
    min_conf_default = _f(env, "MIN_CONF_DEFAULT", 50.0)
    min_cf_default = _f(env, "MIN_CONF_FACTOR_DEFAULT", 0.45)
    min_conf = _f(env, f"MIN_CONF_{sym_u}", min_conf_default)
    min_cf = _f(env, f"MIN_CONF_FACTOR_{sym_u}", min_cf_default)
    return float(min_conf), float(min_cf)


def should_log_edge_veto(env: Mapping[str, str]) -> bool:
    try:
        v = (env.get("LOG_EDGE_VETO", "0") or "0").strip().lower()
        return v in {"1", "true", "yes", "y", "on"}
    except Exception:
        return False
