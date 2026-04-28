"""Feature Registry — единый источник правды для набора и порядка ML-фич.

Цель: исключить «column drift» между датасетами, обучением и продом.
Вместо sample-зависимого infer_feature_cols() можно указать --feature_schema_ver
и получить детерминированный список колонок + SHA-256 хэш для аудита.

Поддерживаемые версии схемы:
  v2  — ядро v2 (25 фич): num×15 + bool×9 + dir×2 + bucket×3 + hour×24 + dow×7
  v3  — v2 + online-friendly extras (29 num, 9 bool)
  v4_of — полная онлайн-схема v4 (48 num, 21 bool) из MLFeatureSchemaV4OF
  v5_of — v4_of + расширения (core+extras) из MLFeatureSchemaV5OF
  v5_of_stable — v5_of минус denylist (feature_denylist_v1.json), для stable training baseline

Публичный API:
  get_schema_info(ver: str) → FeatureSchemaInfo
  get_edge_stack_feature_spec(ver: str) → EdgeStackFeatureSpec
  get_schema(ver)           — алиас для backward compat

"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import warnings

from core.feature_denylist_v1 import denylist_flat, load_feature_denylist

logger = logging.getLogger(__name__)

DEPRECATED_SCHEMAS = {1, 2, 3, 4}

def _check_schema_deprecation(ver: str | int) -> None:
    try:
        ver_str = str(ver).lower()
        if ver_str.startswith("v"):
            ver_str = ver_str[1:]
        match = re.match(r"^(\d+)", ver_str)
        if match:
            ver_num = int(match.group(1))
            if ver_num in DEPRECATED_SCHEMAS:
                msg = f"Feature schema version v{ver_num} is DEPRECATED (causes data leakage). See DEPRECATED_SCHEMAS."
                warnings.warn(msg, DeprecationWarning, stacklevel=3)
                logger.error(msg)
    except Exception as e:
        logger.debug(f"Deprecation check skipped: {e}")


from dataclasses import dataclass
from typing import Dict, List, Optional


def _sha256_16(items: List[str]) -> str:
    """16-символьный SHA-256 от упорядоченного списка строк — короткий хэш для логов.

    Используется в train_edge_stack_v1_oof для feature_cols_hash в артефакте модели.
    """
    payload = "\n".join([str(x) for x in items]).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FeatureSchemaInfo:
    """Immutable descriptor returned by get_schema_info().

    Attributes:
        ver:          строка-версия схемы (e.g. "v4_of")
        feature_names: упорядоченный список имён фич в формате "n:key" / "b:key"
                       / "dir:LONG" / "bucket:trend" / "hour:0" / "dow:0" —
                       эти имена используются при vectorize() и в модели.
        column_names:  безопасные имена для DataFrame / Parquet (: → _).
        schema_hash:   SHA-256 (hex, 64 символа) от JSON-сериализации feature_names.
                       Стабилен при одинаковом порядке — используйте для аудита.
    """

    ver: str
    feature_names: List[str]
    column_names: List[str]
    schema_hash: str

    # Удобный метод: dict для сохранения в meta.json
    def to_dict(self) -> Dict:
        return {
            "ver": self.ver,
            "schema_hash": self.schema_hash,
            "n_features": len(self.feature_names),
            "feature_names": list(self.feature_names),
            "column_names": list(self.column_names),
        }


@dataclass(frozen=True)
class EdgeStackFeatureSpec:
    """feature_cols для build_edge_stack_dataset_from_redis.

    feature_cols соответствует формату, производимому infer_feature_cols():
      "f_{num_key}", "direction_BUY", "direction_SELL",
      "bucket:trend", "bucket:range", "bucket:other",
      "hour:0".."hour:23", "dow:0".."dow:6"

    feature_cols_hash — SHA-256 от JSON-сериализации feature_cols.
    """

    ver: str
    feature_cols: List[str]
    feature_cols_hash: str

    def to_dict(self) -> Dict:
        return {
            "ver": self.ver,
            "feature_cols_hash": self.feature_cols_hash,
            "n_cols": len(self.feature_cols),
            "feature_cols": list(self.feature_cols),
        }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _sha256_of_list(lst: List[str]) -> str:
    """Детерминированный SHA-256 от упорядоченного списка строк."""
    payload = json.dumps(list(lst), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _safe_col(name: str) -> str:
    """Заменяет ':' на '_' в имени колонки для совместимости с Parquet/DataFrame."""
    return name.replace(":", "_")


def _strip_prefix(name: str) -> str:
    """Удаляет n:/b: префикс из имени фичи для получения raw-ключа индикатора.

    Используется при построении f_{key} колонок для edge-stack.
    Пример: 'n:delta_z' → 'delta_z', 'b:ofi_stable' → 'ofi_stable'.
    """
    if name.startswith("n:") or name.startswith("b:"):
        return name[2:]
    return name


def _make_schema_info(ver: str, feature_names: List[str]) -> FeatureSchemaInfo:
    col_names = [_safe_col(n) for n in feature_names]
    h = _sha256_of_list(feature_names)
    return FeatureSchemaInfo(
        ver=ver,
        feature_names=list(feature_names),
        column_names=col_names,
        schema_hash=h,
    )


def _make_edge_stack_spec(ver: str, feature_cols: List[str]) -> EdgeStackFeatureSpec:
    # IMPORTANT: this hash algorithm MUST match _sha256_16() in train_edge_stack_v1_oof.py
    # (line 104: hashlib.sha256("\n".join(items).encode()).hexdigest()[:16])
    # Trainer uses this to validate feature_cols via dataset_report.feature_registry.feature_cols_hash
    payload = "\n".join([str(x) for x in feature_cols]).encode("utf-8")
    h = hashlib.sha256(payload).hexdigest()[:16]
    return EdgeStackFeatureSpec(
        ver=ver,
        feature_cols=list(feature_cols),
        feature_cols_hash=h,
    )


# ---------------------------------------------------------------------------
# v2 schema — ядро 25 фич (зафиксировано, не менять без смены схемы)
# ---------------------------------------------------------------------------

_V2_NUM_KEYS: List[str] = [
    "delta_z",
    "ofi_z",
    "ofi_stability_score",
    "obi",
    "obi_z",
    "spread_bps",
    "expected_slippage_bps",
    "exec_risk_norm",
    "liq_score",
    "book_staleness_ms",
    "pressure",
    "triggers_per_min",
    "rule_score",
    "rule_have",
    "rule_need",
]

_V2_BOOL_KEYS: List[str] = [
    "ofi_stable",
    "ofi_dir_ok",
    "obi_stable",
    "iceberg_strict",
    "fp_edge_absorb",
    "abs_lvl_ok",
    "reclaim_recent",
    "sweep_recent",
    "cancel_spike_veto",
]


def _build_feature_names(
    num_keys: List[str],
    bool_keys: List[str],
    *,
    with_dir: bool = True,
    with_bucket: bool = True,
    with_time: bool = True,
) -> List[str]:
    """Строит feature_names по num+bool ключам + optional блоки."""
    names: List[str] = []
    names += [f"n:{k}" for k in num_keys]
    names += [f"b:{k}" for k in bool_keys]
    if with_dir:
        names += ["dir:LONG", "dir:SHORT"]
    if with_bucket:
        names += ["bucket:trend", "bucket:range", "bucket:other"]
    if with_time:
        names += [f"hour:{h}" for h in range(24)]
        names += [f"dow:{d}" for d in range(7)]
    return names


def _build_edge_stack_cols(
    num_keys: List[str],
    bool_keys: List[str],
) -> List[str]:
    """Строит feature_cols в формате build_edge_stack_dataset_from_redis.infer_feature_cols().

    Формат: f_{num_key}, direction_BUY, direction_SELL,
            bucket:trend/range/other, hour:0..23, dow:0..6
    """
    cols: List[str] = []
    # numeric (в том же порядке что num_keys — детерминированно)
    cols += [f"f_{k}" for k in num_keys]
    # direction
    cols += ["direction_BUY", "direction_SELL"]
    # bucket (3-taxonomy)
    cols += ["bucket:trend", "bucket:range", "bucket:other"]
    # time one-hots
    cols += [f"hour:{h}" for h in range(24)]
    cols += [f"dow:{d}" for d in range(7)]
    return cols


# ---------------------------------------------------------------------------
# v3 schema — v2 + online-friendly extras
# ---------------------------------------------------------------------------

_V3_NUM_KEYS: List[str] = _V2_NUM_KEYS + [
    # v3 online-friendly extras
    "adverse_proxy",
    "lambda_taker",
    "lambda_cancel",
    "lambda_spread_widen",
    "cont_ctx_age_ms",
    "hidden_ctx_recent",
]

_V3_BOOL_KEYS: List[str] = _V2_BOOL_KEYS  # bool-блок не расширяется в v3


# ---------------------------------------------------------------------------
# v4_of schema — читаем из канонического MLFeatureSchemaV4OF
# ---------------------------------------------------------------------------

def _get_v4_of_keys():
    """Импортирует num_keys + bool_keys из MLFeatureSchemaV4OF (runtime import для гибкости)."""
    try:
        from core.ml_feature_schema_v4_of import MLFeatureSchemaV4OF  # type: ignore
        schema = MLFeatureSchemaV4OF()
        return list(schema.num_keys), list(schema.bool_keys)
    except ImportError:
        # Если запускаем вне PYTHONPATH tick_flow_full, возвращаем захардкоженный список
        # (актуален для тестов вне контейнера)
        return _V3_NUM_KEYS + [
            "mae_r", "mfe_r",  # v3 backward-compat (from prev closed trades)
            "mp_mid_bps", "mp_shift_bps",
            "depth_bid_5", "depth_ask_5",
            "book_slope_bid", "book_slope_ask",
            "book_convex_bid", "book_convex_ask",
            "obi_dw",
            "book_rate_hz", "book_rate_z", "book_churn_score",
            "cancel_spike_score", "data_health",
            "vol_fast_bps", "vol_slow_bps",
            "res_curr_ratio", "res_recovery_ms", "res_speed_per_s",
            "atr_bps", "atr_age_ms",
            "rsi_price", "rsi_cvd", "div_strength", "sweep_div_match",
            "microbar_range_bps", "microbar_body_bps",
            "microbar_vwap_mid_bps", "microbar_close_mid_bps",
        ], [
            "ofi_stable", "ofi_dir_ok", "obi_stable",
            "iceberg_strict", "fp_edge_absorb", "abs_lvl_ok",
            "reclaim_recent", "sweep_recent", "cancel_spike_veto",
            "book_health_ok", "atr_bad", "cvd_quarantine_active",
            "conf_rsi_agree", "conf_div_match",
            "conf_sweep_eqh", "conf_sweep_eql",
            "conf_sweep", "conf_sweep_recent",
            "conf_abs_lvl_ok", "conf_fp_edge_absorb",
            "conf_iceberg_strict", "conf_weak_progress",
        ]


# ---------------------------------------------------------------------------
# v5_of schema — v4_of + extras (MLFeatureSchemaV5OF)
# ---------------------------------------------------------------------------

def _get_v5_of_keys():
    """Импортирует num_keys + bool_keys из MLFeatureSchemaV5OF."""
    try:
        from core.ml_feature_schema_v5_of import MLFeatureSchemaV5OF  # type: ignore
        schema = MLFeatureSchemaV5OF()
        return list(schema.num_keys), list(schema.bool_keys)
    except ImportError:
        # fallback: best-effort superset over v4_of (keeps code runnable in minimal env)
        return _get_v4_of_keys()

def _get_v5_of_stable_keys():
    """v5_of_stable = v5_of minus denylisted keys."""
    num_keys, bool_keys = _get_v5_of_keys()
    dn, db = _load_feature_denylist()
    if not dn and not db:
        return num_keys, bool_keys
    num_f = [k for k in num_keys if str(k) not in dn]
    bool_f = [k for k in bool_keys if str(k) not in db]
    return num_f, bool_f





# ---------------------------------------------------------------------------
# v6_of schema — v5_of + flow + realized adverse drift (MLFeatureSchemaV6OF)
# ---------------------------------------------------------------------------

def _get_v6_of_keys():
    """Импортирует num_keys + bool_keys из MLFeatureSchemaV6OF."""
    try:
        from core.ml_feature_schema_v6_of import MLFeatureSchemaV6OF  # type: ignore
        schema = MLFeatureSchemaV6OF()
        return list(schema.num_keys), list(schema.bool_keys)
    except ImportError:
        # fallback: best-effort superset over v5_of (keeps code runnable in minimal env)
        return _get_v5_of_keys()


def _get_v6_of_stable_keys():
    """v6_of_stable = v6_of minus denylisted keys."""
    num_keys, bool_keys = _get_v6_of_keys()
    dn, db = _load_feature_denylist()
    if not dn and not db:
        return num_keys, bool_keys
    num_f = [k for k in num_keys if str(k) not in dn]
    bool_f = [k for k in bool_keys if str(k) not in db]
    return num_f, bool_f





# ---------------------------------------------------------------------------
# v7_of schema — v6_of + A5 flags + Hawkes split + VPIN-like toxicity
# ---------------------------------------------------------------------------

def _get_v7_of_keys():
    """Импортирует num_keys + bool_keys из MLFeatureSchemaV7OF."""
    try:
        from core.ml_feature_schema_v7_of import MLFeatureSchemaV7OF  # type: ignore
        schema = MLFeatureSchemaV7OF()
        return list(schema.num_keys), list(schema.bool_keys)
    except ImportError:
        # fallback: best-effort superset over v6_of
        return _get_v6_of_keys()


def _get_v7_of_stable_keys():
    """v7_of_stable = v7_of minus denylisted keys."""
    from core.ml_feature_schema_v7_of import MLFeatureSchemaV7OFStable
    s = MLFeatureSchemaV7OFStable()
    return list(s.num_keys), list(s.bool_keys)


# ---------------------------------------------------------------------------
# v9_of schema — 100% coverage of current signals:of:inputs indicators
# Derived from infer_feature_cols() on run 20260303_175837 (1771 joined signals)
# ---------------------------------------------------------------------------

def _get_v9_of_keys() -> tuple:
    """Returns (num_keys, bool_keys) for v9_of.

    v9_of is a pinned snapshot of infer_feature_cols() output on 2026-03-03.
    All 128 keys are numeric (float/int) from indicators dict.
    No separate bool_keys — boolean indicators included as numeric (0/1).
    """
    try:
        from core.ml_feature_schema_v9_of import V9_OF_NUMERIC_KEYS  # type: ignore
        return list(V9_OF_NUMERIC_KEYS), []  # v9_of has no separate bool block
    except ImportError:
        # fallback: use v7_of keys
        return _get_v7_of_keys()


# ---------------------------------------------------------------------------
# v10_of schema — v9_of + Group1 stream-proven + Group2A-E new indicators
# Created 2026-03-15: 165 numeric keys (128 base + 37 new)
# ---------------------------------------------------------------------------

def _get_v10_of_keys() -> tuple:
    """Returns (num_keys, bool_keys) for v10_of.

    v10_of = v9_of (128) + 37 additional keys across 6 groups:
      Group 1  (16): stream-proven indicators already in signals:of:inputs
      Group 2A  (4): Adverse Selection / VPIN extension
      Group 2B  (4): Order Book microstructure (restored from v4_of + new)
      Group 2C  (5): Momentum / Technical Analysis
      Group 2D  (4): Execution Quality (post-trade rolling averages)
      Group 2E  (4): Context / External (fail-open until go-worker pipeline)

    All keys are numeric (float/int); no separate bool block.
    """
    try:
        from core.ml_feature_schema_v10_of import V10_OF_NUMERIC_KEYS  # type: ignore
        return list(V10_OF_NUMERIC_KEYS), []  # v10_of has no separate bool block
    except ImportError:
        # fallback: use v9_of keys
        return _get_v9_of_keys()


# ---------------------------------------------------------------------------
# v11_of schema — v10_of + GroupA-F regime/microstructure/interactions
# Created 2026-03-15: 193 numeric keys (165 base + 28 new)
# ---------------------------------------------------------------------------

def _get_v11_of_keys() -> tuple:
    """Returns (num_keys, bool_keys) for v11_of.

    v11_of = v10_of (165) + 28 additional keys across 6 groups:
      Group A (5): Regime / Structural Context
      Group B (5): Trade History / Session Context
      Group C (4): Cross-Asset / Correlation
      Group D (5): Order Flow Microstructure Extensions
      Group E (4): Signal Self-Awareness
      Group F (5): Derived / Interaction Features

    All keys are numeric (float/int); no separate bool block.
    """
    try:
        from core.ml_feature_schema_v11_of import V11_OF_NUMERIC_KEYS  # type: ignore
        return list(V11_OF_NUMERIC_KEYS), []  # v11_of has no separate bool block
    except ImportError:
        # fallback: use v10_of keys
        return _get_v10_of_keys()


# ---------------------------------------------------------------------------
# v12_of schema — v11_of + GroupMA-ME new signals + GroupMX derived
# Created 2026-03-16: ~214 numeric keys (193 base + 21 new)
# ---------------------------------------------------------------------------

def _get_v12_of_keys() -> tuple:
    """Returns (num_keys, bool_keys) for v12_of.

    v12_of = v11_of (193) + 21 additional keys across 6 groups:
      Group MA (4): Microstructure / Trade-by-trade
      Group MB (4): Order Book Dynamics (velocity, not snapshot)
      Group MC (3): Temporal / Seasonality fine-grained
      Group MD (3): Cross-Asset / Macro extensions
      Group ME (3): Self-Referential / Meta-Signal
      Group MX (4): Medium-priority derived / interaction features

    All keys are numeric (float/int); no separate bool block.
    """
    try:
        from core.ml_feature_schema_v12_of import V12_OF_NUMERIC_KEYS  # type: ignore
        return list(V12_OF_NUMERIC_KEYS), []  # v12_of has no separate bool block
    except ImportError:
        # fallback: use v11_of keys
        return _get_v11_of_keys()


# ---------------------------------------------------------------------------
# v13_of schema — v12_of + GroupNA-NX advanced vol/liquidity/toxicity/macro
# Created 2026-03-17: ~242 numeric keys (214 base + 28 new)
# ---------------------------------------------------------------------------

def _get_v13_of_keys() -> tuple:
    """Returns (num_keys, bool_keys) for v13_of.

    v13_of = v12_of (214) + 28 additional keys across 7 groups:
      Group NA (4): Advanced Volatility Estimation (OHLC-based)
      Group NB (4): Academic Liquidity Metrics
      Group NC (4): Order Flow Toxicity
      Group ND (5): Cross-Asset / Macro Extended
      Group NE (3): Entropy / Information Theory
      Group NF (3): Mean Reversion / Stationarity
      Group NX (5): Advanced Interaction Features

    All keys are numeric (float/int); no separate bool block.
    """
    try:
        from core.ml_feature_schema_v13_of import V13_OF_NUMERIC_KEYS  # type: ignore
        return list(V13_OF_NUMERIC_KEYS), []  # v13_of has no separate bool block
    except ImportError:
        # fallback: use v12_of keys
        return _get_v12_of_keys()

# ---------------------------------------------------------------------------
# Caches (module-level, deterministic)
# ---------------------------------------------------------------------------

_SCHEMA_CACHE: dict[str, FeatureSchemaInfo] = {}
_EDGE_CACHE: dict[str, EdgeStackFeatureSpec] = {}


def _load_feature_denylist() -> tuple[set, set]:
    """Load denylist as (deny_num, deny_bool).

    Backward-compatible wrapper used by v5_of_stable/v6_of_stable.
    Denylist keys are raw indicator keys.

    Fail-open: any error returns empty sets.
    """
    dl = load_feature_denylist()
    dn = set(dl.deny_num) | (set(dl.deny_all) if dl.deny_all else set())
    db = set(dl.deny_bool) | (set(dl.deny_all) if dl.deny_all else set())
    return dn, db


def _denylist_features() -> set[str]:
    """Flat denylist set applied to both num/bool keys (used by v7_of_stable)."""
    return set(denylist_flat())


# (Duplicate _get_v5_of_stable_keys removed)
def get_schema_info(ver: str) -> FeatureSchemaInfo:
    """Возвращает FeatureSchemaInfo для заданной версии схемы.

    Args:
        ver: одно из "v2", "v3", "v4_of", "v5_of", "v5_of_stable", "v6_of", "v6_of_stable", "v7_of", "v7_of_stable", "v9_of", "v10_of", "v11_of", "v12_of", "v13_of"

    Returns:
        FeatureSchemaInfo — неизменяемый дескриптор схемы.

    Raises:
        ValueError: если версия не поддерживается.
    """
    v = str(ver or "").strip().lower()
    
    _check_schema_deprecation(v)

    if v in _SCHEMA_CACHE:
        return _SCHEMA_CACHE[v]

    if v == "v2":
        names = _build_feature_names(_V2_NUM_KEYS, _V2_BOOL_KEYS)
    elif v == "v3":
        names = _build_feature_names(_V3_NUM_KEYS, _V3_BOOL_KEYS)
    elif v in ("v4_of", "v4"):
        num_keys, bool_keys = _get_v4_of_keys()
        names = _build_feature_names(num_keys, bool_keys)
        v = "v4_of"
    elif v in ("v5_of", "v5"):
        num_keys, bool_keys = _get_v5_of_keys()
        names = _build_feature_names(num_keys, bool_keys)
        v = "v5_of"
    elif v in ("v5_of_stable", "v5_stable", "v5stable"):
        num_keys, bool_keys = _get_v5_of_stable_keys()
        names = _build_feature_names(num_keys, bool_keys)
        v = "v5_of_stable"
    elif v in ("v7_of", "v7"):
        num_keys, bool_keys = _get_v7_of_keys()
        names = _build_feature_names(num_keys, bool_keys)
        v = "v7_of"
    elif v in ("v7_of_stable", "v7_stable", "v7stable"):
        num_keys, bool_keys = _get_v7_of_stable_keys()
        names = _build_feature_names(num_keys, bool_keys)
        v = "v7_of_stable"
    elif v in ("v6_of", "v6"):
        num_keys, bool_keys = _get_v6_of_keys()
        names = _build_feature_names(num_keys, bool_keys)
        v = "v6_of"
    elif v in ("v6_of_stable", "v6_stable", "v6stable"):
        num_keys, bool_keys = _get_v6_of_stable_keys()
        names = _build_feature_names(num_keys, bool_keys)
        v = "v6_of_stable"
    elif v in ("v9_of", "v9"):
        num_keys, bool_keys = _get_v9_of_keys()
        names = _build_feature_names(num_keys, bool_keys)
        v = "v9_of"
    elif v in ("v10_of", "v10"):
        num_keys, bool_keys = _get_v10_of_keys()
        names = _build_feature_names(num_keys, bool_keys)
        v = "v10_of"
    elif v in ("v11_of", "v11"):
        num_keys, bool_keys = _get_v11_of_keys()
        names = _build_feature_names(num_keys, bool_keys)
        v = "v11_of"
    elif v in ("v12_of", "v12"):
        num_keys, bool_keys = _get_v12_of_keys()
        names = _build_feature_names(num_keys, bool_keys)
        v = "v12_of"
    elif v in ("v13_of", "v13"):
        num_keys, bool_keys = _get_v13_of_keys()
        names = _build_feature_names(num_keys, bool_keys)
        v = "v13_of"
    else:
        raise ValueError(
            f"Unsupported feature schema version: {ver!r}. "
            f"Supported: 'v2', 'v3', 'v4_of', 'v5_of', 'v5_of_stable', 'v6_of', 'v6_of_stable', 'v7_of', 'v7_of_stable', 'v9_of', 'v10_of', 'v11_of', 'v12_of', 'v13_of'."
        )

    info = _make_schema_info(v, names)
    _SCHEMA_CACHE[v] = info
    return info


def get_edge_stack_feature_spec(
    schema_ver: str,
    scenario_prefix: str = "bucket:",
    include_direction: bool = True,
    include_scenario: bool = True,
    include_bucket2_onehot: bool = False,
    include_time_onehot: Optional[bool] = None,
    sort_numeric_keys: bool = True,
    max_numeric: int = 128,
    strict_feature_cols: bool = False,
    forbid_scenario_v4_onehot: bool = False,
    include_session_onehot: Optional[bool] = None,
) -> EdgeStackFeatureSpec:
    """Детерминированный edge-stack feature_cols из MLFeatureSchema (v2/v3/v4_of/v5_of/v6_of/v7_of).

    Заменяет sample-зависимый infer_feature_cols() версионным allowlist-ом —
    training не может дрейфовать по составу колонок.

    Правила:
      - Числовые/bool фичи → "f_{key}"
      - direction → direction_BUY / direction_SELL
      - scenario → bucket:* (рекомендуется) или scenario_v4_* (устарело)
      - bucket2 → bucket2:* (добавочная категоризация; включается только при include_bucket2_onehot=True)
      - time one-hots hour:/dow: включены по умолчанию для v3+

    strict_feature_cols + forbid_scenario_v4_onehot:
      - При True отвергают scenario_v4_* (ValueError)

    max_numeric:
      - Ограничивает только f_* колонки; direction/scenario/time всегда сохраняются.

    Backward compat: `get_edge_stack_feature_spec(ver)` (positional) по-прежнему работает.
    """
    ver = str(schema_ver or "").strip().lower()
    
    _check_schema_deprecation(ver)

    # нормализация алиасов
    if ver == "v4of":
        ver = "v4_of"
    if ver == "v5of":
        ver = "v5_of"
    if ver in ("v5_of_stable", "v5stable", "v5_stable"):
        ver = "v5_of_stable"
    if ver == "v6of":
        ver = "v6_of"
    if ver in ("v6_of_stable", "v6stable", "v6_stable"):
        ver = "v6_of_stable"
    if ver == "v11":
        ver = "v11_of"
    if ver == "v12":
        ver = "v12_of"
    if ver == "v13":
        ver = "v13_of"
    if ver == "v10":
        ver = "v10_of"
    if ver == "v9":
        ver = "v9_of"

    # расширенный allowlist: v2/v3/v4/v4_of/v5_of/v5_of_stable/v6_of/v6_of_stable/v7_of/v7_of_stable/v9_of/v10_of/v11_of/v12_of/v13_of
    if ver not in ("v2", "v3", "v4", "v4_of", "v5", "v5_of", "v5_of_stable", "v6", "v6_of", "v6_of_stable", "v7", "v7_of", "v7_of_stable", "v9", "v9_of", "v10", "v10_of", "v11", "v11_of", "v12", "v12_of", "v13", "v13_of"):
        raise ValueError(
            f"edge-stack registry supports v2/v3/v4/v4_of/v5_of/v5_of_stable/v6_of/v6_of_stable/v7_of/v7_of_stable/v9_of/v10_of/v11_of/v12_of/v13_of only, got {schema_ver!r}"
        )

    # Числовой номер версии для умолчаний
    # NOTE: v9_of/v10_of use ver_num=7 so that session_* one-hots ARE included.
    # Session fields (session_asia/eu/us/off) are published as first-class OFInputsV2
    # fields in signals:of:inputs, so train==serve parity is guaranteed.
    try:
        if ver in ("v9_of", "v9", "v10_of", "v10", "v11_of", "v11", "v12_of", "v12", "v13_of", "v13"):
            ver_num = 7  # include session one-hots (OFInputsV2 publishes them)
        elif ver in ("v7_of", "v7_of_stable"):
            ver_num = 7
        elif ver in ("v6_of", "v6_of_stable"):
            ver_num = 6
        elif ver in ("v5_of", "v5_of_stable"):
            ver_num = 5
        elif ver == "v4_of":
            ver_num = 4
        else:
            ver_num = int(ver.lstrip("v"))
    except Exception:
        ver_num = 0

    # Получаем num_keys + bool_keys из нужной схемы
    if ver == "v2":
        num_keys, bool_keys = list(_V2_NUM_KEYS), list(_V2_BOOL_KEYS)
    elif ver == "v3":
        num_keys, bool_keys = list(_V3_NUM_KEYS), list(_V3_BOOL_KEYS)
    elif ver in ("v4", "v4_of"):
        num_keys, bool_keys = _get_v4_of_keys()
        ver = "v4_of"  # канонизируем v4 → v4_of для кэша
    elif ver in ("v5", "v5_of"):
        num_keys, bool_keys = _get_v5_of_keys()
        ver = "v5_of"
    else:  # v6/v7/v9+ allowlist
        if ver in ("v7_of", "v7"):
            num_keys, bool_keys = _get_v7_of_keys()
            ver = "v7_of"
        elif ver == "v7_of_stable":
            num_keys, bool_keys = _get_v7_of_stable_keys()
            ver = "v7_of_stable"
        elif ver in ("v9_of", "v9"):
            num_keys, bool_keys = _get_v9_of_keys()
            ver = "v9_of"
        elif ver in ("v10_of", "v10"):
            num_keys, bool_keys = _get_v10_of_keys()
            ver = "v10_of"
        elif ver in ("v11_of", "v11"):
            num_keys, bool_keys = _get_v11_of_keys()
            ver = "v11_of"
        elif ver in ("v12_of", "v12"):
            num_keys, bool_keys = _get_v12_of_keys()
            ver = "v12_of"
        elif ver in ("v13_of", "v13"):
            num_keys, bool_keys = _get_v13_of_keys()
            ver = "v13_of"
        elif ver in ("v6_of", "v6"):
            num_keys, bool_keys = _get_v6_of_keys()
            ver = "v6_of"
        elif ver == "v6_of_stable":
            num_keys, bool_keys = _get_v6_of_stable_keys()
            ver = "v6_of_stable"
        elif ver == "v5_of_stable":
            num_keys, bool_keys = _get_v5_of_stable_keys()
            ver = "v5_of_stable"
        else:
            num_keys, bool_keys = _get_v5_of_keys()
            ver = "v5_of"

    # Строим f_* колонки детерминированно
    fkeys: List[str] = []
    for k in num_keys:
        s = str(k)
        if s.startswith("n:"):
            fkeys.append(_strip_prefix(s))
        elif s.startswith("b:"):
            pass  # bool-ключи обработаны ниже через bool_keys
        elif s.startswith("c:") or s.startswith("dir:") or s.startswith("bucket:") or s.startswith("hour:") or s.startswith("dow:"):
            continue  # categorical — обрабатываются отдельно
        else:
            fkeys.append(s)  # raw indicator key (v4 float/int)
    # bool_keys (b:*) → f_{key} (без b: prefix)
    for k in bool_keys:
        s = str(k)
        if s.startswith("b:"):
            fkeys.append(_strip_prefix(s))
        elif s not in ("dir:LONG", "dir:SHORT", "bucket:trend", "bucket:range", "bucket:other"):
            if not (s.startswith("hour:") or s.startswith("dow:") or s.startswith("c:")):
                fkeys.append(s)

    if sort_numeric_keys:
        fkeys = sorted(set(fkeys))
    else:
        # сохраняем порядок схемы, устраняем дубли
        seen: set = set()
        out_k: List[str] = []
        for k in fkeys:
            if k not in seen:
                out_k.append(k)
                seen.add(k)
        fkeys = out_k

    # Ограничиваем только f_* часть; direction/scenario/time ниже всегда сохраняются
    try:
        mn = int(max_numeric)
    except Exception:
        mn = 0
        
    # v9_of/v10_of/v11_of override max_numeric=128 backward-compatible default
    if ver in ("v9_of", "v10_of", "v11_of", "v12_of", "v13_of") and mn == 128:
        mn = 0
        
    if mn > 0 and len(fkeys) > mn:
        fkeys = fkeys[:mn]

    cols: List[str] = [f"f_{k}" for k in fkeys]

    # direction block
    if include_direction:
        cols += ["direction_BUY", "direction_SELL"]

    # scenario block
    if include_scenario:
        prefix = str(scenario_prefix or "bucket:").strip()
        if prefix == "bucket:":
            cols += ["bucket:trend", "bucket:range", "bucket:other"]
        else:
            # scenario_v4_* (устаревший, legacy)
            cols += ["scenario_v4_trend", "scenario_v4_range", "scenario_v4_other"]

        # bucket2 block (separate prefix, does NOT replace bucket:)
        # Используется только новыми моделями/датасетами. Старые модели не должны
        # внезапно получить новые one-hot колонки.
        if bool(include_bucket2_onehot):
            cols += ["bucket2:breakout", "bucket2:reversal", "bucket2:high_var"]

    # strict mode: запрещаем scenario_v4_* при forbid_scenario_v4_onehot
    if strict_feature_cols and forbid_scenario_v4_onehot:
        bad = [str(c) for c in cols if str(c).startswith("scenario_v4_")]
        if bad:
            ex = bad[0]
            raise ValueError(
                f"forbidden_feature_cols: scenario_v4_* не разрешены в strict mode "
                f"(n={len(bad)} ex={ex})"
            )

    # time one-hots: default = v3+ (ver_num >= 3)
    if include_time_onehot is None:
        include_time_onehot = ver_num >= 3
    if include_time_onehot:
        cols += [f"hour:{h}" for h in range(24)]
        cols += [f"dow:{d}" for d in range(7)]

    # session one-hots: default = v7+ (ver_num >= 7)
    if include_session_onehot is None:
        include_session_onehot = ver_num >= 7
    if include_session_onehot:
        cols += ["session_asia", "session_eu", "session_us", "session_off"]


    spec = _make_edge_stack_spec(ver, cols)
    # кэшируем только простой вызов (без доп. параметров) — с параметрами создаём свежий объект
    if (
        max_numeric == 128
        and not strict_feature_cols
        and not forbid_scenario_v4_onehot
        and not include_bucket2_onehot
        and include_time_onehot is not False
        and include_session_onehot is not False
    ):
        _EDGE_CACHE.setdefault(ver, spec)
    return spec


# ---------------------------------------------------------------------------
# Backward-compatible aliases
# ---------------------------------------------------------------------------

def get_schema(ver: str) -> FeatureSchemaInfo:
    """Алиас для get_schema_info() — backward compat."""
    return get_schema_info(ver)
