from __future__ import annotations

import hashlib
import json
import math
import os
from typing import Any

from signals.empirical_levels import EmpiricalLevels
from signals.empirical_time_levels import EmpiricalTimeLevelsConfig, RedisEmpiricalTimeLevelsProvider
from signals.risk_levels import compute_levels
import contextlib


def _env_float(name: str, default: float) -> float:
    try:
        v = os.getenv(name, "")
        if v is None or str(v).strip() == "":
            return default
        return float(v)
    except Exception:
        return default


def _norm_symbol(sym: str) -> str:
    return (sym or "").strip().upper().replace("/", "").replace("-", "")


def _sym_env_float(prefix: str, symbol: str, default: float) -> float:
    """
    Читает:
      1) <prefix>_<SYMBOL>  (например EDGE_LEVELS_MIN_STOP_BPS_BTCUSDT)
      2) <prefix>          (например EDGE_LEVELS_MIN_STOP_BPS)
    """
    s = _norm_symbol(symbol)
    v = os.getenv(f"{prefix}_{s}")
    if v is not None and str(v).strip() != "":
        try:
            return float(v)
        except Exception:
            pass
    return _env_float(prefix, default)


def _parse_csv_floats(s: Any) -> list[float]:
    """
    Парсит scalar/list/tuple/строку в список float.
    """
    if s is None:
        return []
    if isinstance(s, (list, tuple)):
        out = []
        for x in s:
            with contextlib.suppress(Exception):
                out.append(float(x))
        return out
    if isinstance(s, str):
        out = []
        for part in s.split(","):
            p = part.strip()
            if not p:
                continue
            with contextlib.suppress(Exception):
                out.append(float(p))
        return out
    try:
        return [float(s)]
    except Exception:
        return []


def _side_to_str(side: Any) -> str:
    """
    Нормализует сторону в 'LONG'/'SHORT' для compute_levels().

    Поддерживает:
      - 'LONG'/'SHORT'
      - 'BUY'/'SELL'
      - перечисления (enums) с .name
      - объекты с .value
    """
    if side is None:
        return "LONG"
    if isinstance(side, str):
        s = side.strip().upper()
    else:
        s = str(getattr(side, "name", None) or getattr(side, "value", None) or side).strip().upper()

    if s in {"LONG", "BUY"}:
        return "LONG"
    if s in {"SHORT", "SELL"}:
        return "SHORT"
    # fail-open по умолчанию (безопаснее, чем падение в рантайме)
    return "LONG"


def _cfg_hash(cfg: dict[str, Any]) -> str:
    """
    Stable hash for cfg. Deterministic across dict ordering.
    Used ONLY for per-ctx caching; not a global identifier.
    """
    try:
        s = json.dumps(cfg or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha1(s.encode("utf-8", "ignore")).hexdigest()
    except Exception:
        return "cfg:err"


def _levels_cache(ctx: Any) -> dict[tuple[Any, ...], dict[str, Any]] | None:
    """
    Per-ctx cache:
      key -> {"status": "attached"|"skipped", "reason": "..."}
    NOTE: ctx is per-signal in your pipeline, so no TTL is needed.
    """
    if ctx is None:
        return None
    try:
        c = getattr(ctx, "_levels_attach_cache", None)
        if isinstance(c, dict):
            return c
        c = {}
        ctx._levels_attach_cache = c
        return c
    except Exception:
        return None


def attach_trade_levels_to_ctx(
    ctx: Any,
    *,
    side: str,
    symbol: str,
    cfg: dict[str, Any],
    kind: str | None = None,
    regime: Any = None,
    empirical: EmpiricalLevels | None = None,
    overwrite: bool = False,
    logger: Any | None = None,
) -> None:
    """
    Обогатить SignalContext детерминированными торговыми уровнями (SL/TP) для фильтров ниже по потоку.

    Эта функция намеренно FAIL-OPEN:
      - Она никогда не должна ломать публикацию сигналов.
      - Если обязательные входные данные отсутствуют или неверны -> она просто возвращает управление без изменений.

    Что пишет в ctx (как динамические атрибуты):
      - entry_price: float
      - sl_price: float
      - tp_levels: list[float]
      - tp1_price: float  (tp_levels[0])
      - stop_dist: float
      - tp_rr: float | None (первый RR, если доступен)
      - levels_source: str (baseline_cfg | empirical_blend)
      - levels_samples: int (если empirical)
      - levels_ttd_tp1_ms: int (если empirical)

    Зачем:
      EdgeCostGate (анти-churn) становится точным только когда ctx имеет:
        ctx.entry_price + ctx.tp1_price (лучше всего)
      иначе он сработает fail-open и churn не будет уменьшен.

    # --------------------------------------------------------------------------
    # Strong anti-churn дополнение:
    #  - Мы не хотим "микро-уровни" (stop_dist в считанные bps),
    #    потому что они приводят к:
    #      a) постоянным veto по costs (edge ниже порога),
    #      b) дерганью/пересозданию сигналов (churn),
    #      c) ложным "супер-точным" TP1/SL на шуме.
    #
    # Поэтому добавлены floor-гейты (через ENV):
    #  - EDGE_LEVELS_MIN_STOP_BPS (и per-symbol EDGE_LEVELS_MIN_STOP_BPS_<SYM>)
    #  - EDGE_LEVELS_MIN_TP1_BPS  (и per-symbol EDGE_LEVELS_MIN_TP1_BPS_<SYM>)
    # Если уровень не проходит floor — НЕ обогащаем ctx (fail-open),
    # а дальше уже строгий cost gate решит (fail-closed при strict).
    # --------------------------------------------------------------------------
    """

    # 1) Сначала соберём все входные данные для ключа кэша
    side_norm = _side_to_str(side)
    kind_s = (kind or "").strip().lower()
    # regime may be enum/dict; keep it safe and bounded
    try:
        regime_s = str(getattr(regime, "name", None) or getattr(regime, "value", None) or regime or "")
    except Exception:
        regime_s = ""
    cfgd: dict[str, Any]
    try:
        cfgd = dict(cfg or {})
    except Exception:
        cfgd = {}

    # 2) Собрать entry & atr из ctx/of с консервативными фоллбэками.
    of = getattr(ctx, "of", None)

    # Inject spread and slippage into cfgd for adaptive SL floor
    cfgd["spread_bps"] = float(getattr(ctx, "spread_bps", None) or getattr(of, "spread_bps", 0.0) or 0.0)
    cfgd["slippage_ema_bps"] = float(getattr(ctx, "slippage_ema_bps", None) or getattr(of, "slippage_ema_bps", 0.0) or 0.0)

    entry = (
        getattr(ctx, "entry_price", None)
        or getattr(ctx, "entry", None)
        or getattr(ctx, "price", None)
        or (getattr(of, "price", None) if of is not None else None)
    )
    atr = (
        getattr(ctx, "atr", None)
        or getattr(ctx, "atr14", None)
        or getattr(ctx, "atr_1m", None)
        or (getattr(of, "atr", None) if of is not None else None)
    )

    try:
        entry_f = entry
        atr_f = atr if atr is not None else 0.0
    except Exception:
        return

    # entry должна быть положительной (вычисление bps этого требует)
    if entry_f <= 0.0:
        return

    # ----------------------------------------------------------------------
    # Cache check: if we already computed for these exact inputs, reuse result
    # ----------------------------------------------------------------------
    cache = _levels_cache(ctx)
    key = (
        "levels_v1",
        symbol,
        str(side_norm),
        str(kind_s),
        str(regime_s)[:64],
        _cfg_hash(cfgd),
        round(float(entry_f), 8),
        round(float(atr_f), 8),
        int(id(empirical)) if empirical is not None else 0,
    )
    if (not overwrite) and isinstance(cache, dict):
        hit = cache.get(key)
        if isinstance(hit, dict):
            st = (hit.get("status") or "")
            if st == "attached":
                # Ensure fields still exist; if not, fall through and recompute.
                try:
                    if getattr(ctx, "entry_price", None) is not None and getattr(ctx, "tp1_price", None) is not None:
                        return
                except Exception:
                    pass
            if st == "skipped":
                return

    # 3) Вычислить уровни:
    #   - baseline из конфига
    #   - опциональный эмпирический override для STOP/TP1 (MFE/MAE/TTD)
    try:
        baseline = compute_levels(entry_f, float(atr_f), side_norm, cfgd, symbol=symbol)

        base_sl = baseline.get("sl", None)
        base_tps = baseline.get("tp_levels", None)
        base_stop = baseline.get("stop_dist", None)
        if base_sl is None or not isinstance(base_tps, list) or len(base_tps) == 0 or base_stop is None:
            # Cache "skipped" only when we had valid entry/atr and stable inputs.
            if (not overwrite) and isinstance(cache, dict):
                cache[key] = {"status": "skipped", "reason": "compute_levels_failed"}
            return

        baseline_tp1_dist = abs(float(base_tps[0]) - float(entry_f))
        baseline_stop_dist = float(base_stop)

        sug = None
        if empirical is not None and kind is not None:
            sug = empirical.suggest(
                symbol=symbol,
                kind=str(kind),
                regime=regime,
                entry=float(entry_f),
                atr=float(atr_f),
                baseline_stop_dist=float(baseline_stop_dist),
                baseline_tp1_dist=float(baseline_tp1_dist),
            )

        if sug is not None:
            levels = compute_levels(
                entry_f,
                float(atr_f),
                _side_to_str(side),
                dict(cfg),
                stop_dist_override=float(sug.stop_dist),
                tp1_dist_override=float(sug.tp1_dist),
            )
            try:
                ctx.levels_source = str(sug.source)
                ctx.levels_samples = int(sug.samples)
                ctx.levels_ttd_tp1_ms = int(sug.ttd_tp1_ms)
            except Exception:
                pass
        else:
            levels = baseline
            with contextlib.suppress(Exception):
                ctx.levels_source = "baseline_cfg"

        # ──────────────────────────────────────────────────────────────────
        # AdaptiveTP1Policy v1 (Plan 3, 2026-05-29)
        #
        # Подменяет ТОЛЬКО TP1 distance на argmax_EV(TP1_R) кандидата из сетки.
        # SL/TP2/TP3 НЕ трогаются. Запускается через compute_levels(..., tp1_dist_override=...)
        # → все safety floors (EDGE_LEVELS_MIN_TP1_BPS, tp1_min_rr) применяются повторно.
        # SHADOW по умолчанию: метрики пишутся, но baseline TP1 остаётся.
        # Master switch: TP1_ADAPTIVE_ENABLED=0; mode: TP1_ADAPTIVE_MODE=shadow|paper|enforce.
        # ──────────────────────────────────────────────────────────────────
        try:
            _pre_sl = levels.get("sl", None)
            _pre_tps = levels.get("tp_levels", None)
            _pre_stop = levels.get("stop_dist", None)
            if (
                _pre_sl is not None
                and isinstance(_pre_tps, list)
                and len(_pre_tps) > 0
                and _pre_stop is not None
            ):
                # Phase 2: seed ctx.tp1_hit_prob_by_rr from publisher snapshot.
                # Fail-open: if reader disabled/unavailable or no matching bucket,
                # ctx.tp1_hit_prob_by_rr remains None → AdaptiveTP1Policy returns
                # skip_no_prob_curve and pipeline is unchanged.
                # Only seed if caller hasn't already provided a curve (e.g. tests).
                if getattr(ctx, "tp1_hit_prob_by_rr", None) is None:
                    try:
                        from services.tp1_hit_prob_reader import attach_tp1_phit_to_ctx

                        attach_tp1_phit_to_ctx(
                            ctx,
                            symbol=symbol,
                            kind=str(kind or ""),
                            regime=str(regime_s or ""),
                            direction=side_norm,
                        )
                    except Exception:
                        pass

                from core.adaptive_tp1_policy import choose_adaptive_tp1

                _cur_tp1_dist = abs(float(_pre_tps[0]) - float(entry_f))
                _cur_stop_dist = float(_pre_stop)
                _atp1 = choose_adaptive_tp1(
                    ctx=ctx,
                    entry=float(entry_f),
                    stop_dist=_cur_stop_dist,
                    baseline_tp1_dist=_cur_tp1_dist,
                    symbol=symbol,
                    kind=str(kind or ""),
                    regime=str(regime_s or ""),
                )
                # Always write telemetry (shadow-mode friendly).
                with contextlib.suppress(Exception):
                    ctx.tp1_adaptive_reason = _atp1.reason
                    ctx.tp1_adaptive_mode = _atp1.mode
                    ctx.tp1_adaptive_enabled = bool(_atp1.enabled)
                    ctx.tp1_adaptive_apply = bool(_atp1.apply)
                    ctx.tp1_adaptive_ev_baseline_r = float(_atp1.ev_baseline_r)
                    ctx.tp1_adaptive_ev_adaptive_r = float(_atp1.ev_adaptive_r)
                    ctx.tp1_adaptive_ev_delta_r = float(_atp1.ev_delta_r)
                    ctx.tp1_adaptive_cost_r = float(_atp1.cost_r)
                    ctx.tp1_adaptive_samples = int(_atp1.samples)
                    if _atp1.tp1_rr is not None:
                        ctx.tp1_adaptive_rr_selected = float(_atp1.tp1_rr)
                    if _atp1.p_hit is not None:
                        ctx.tp1_adaptive_p_hit = float(_atp1.p_hit)
                    if _atp1.p_hit_baseline is not None:
                        ctx.tp1_adaptive_p_hit_baseline = float(_atp1.p_hit_baseline)
                    if _atp1.baseline_rr is not None:
                        ctx.tp1_adaptive_baseline_rr = float(_atp1.baseline_rr)

                # Enforce only when policy explicitly approved AND mode in {paper, enforce}.
                if _atp1.apply and _atp1.tp1_dist is not None and _atp1.tp1_dist > 0.0:
                    try:
                        _adaptive_levels = compute_levels(
                            entry_f,
                            float(atr_f),
                            side_norm,
                            dict(cfgd),
                            symbol=symbol,
                            stop_dist_override=float(_cur_stop_dist),
                            tp1_dist_override=float(_atp1.tp1_dist),
                        )
                        _new_sl = _adaptive_levels.get("sl", None)
                        _new_tps = _adaptive_levels.get("tp_levels", None)
                        _new_stop = _adaptive_levels.get("stop_dist", None)
                        if (
                            _new_sl is not None
                            and isinstance(_new_tps, list)
                            and len(_new_tps) > 0
                            and _new_stop is not None
                        ):
                            levels = _adaptive_levels
                            with contextlib.suppress(Exception):
                                ctx.levels_source = "adaptive_tp1"
                    except Exception:
                        # fail-open: keep prior levels untouched
                        with contextlib.suppress(Exception):
                            ctx.tp1_adaptive_reason = "tp1_adaptive_skip_recompute_failed"

                # Phase 2: Prometheus counters + XADD shadow stream.
                # Never raises; obeys TP1_ADAPTIVE_EMIT_ENABLED master switch.
                try:
                    from core.tp1_adaptive_metrics import emit_decision

                    _adaptive_tp1_price = None
                    if _atp1.tp1_dist is not None:
                        side_dir = 1 if side_norm == "LONG" else -1
                        _adaptive_tp1_price = float(entry_f) + side_dir * float(_atp1.tp1_dist)
                    emit_decision(
                        decision=_atp1,
                        symbol=symbol,
                        kind=str(kind or ""),
                        side=side_norm,
                        regime=str(regime_s or ""),
                        entry_price=float(entry_f),
                        sl_price=float(_pre_sl),
                        baseline_tp1_price=float(_pre_tps[0]),
                        baseline_tp1_rr=_atp1.baseline_rr,
                        adaptive_tp1_price=_adaptive_tp1_price,
                        spread_bps=float(cfgd.get("spread_bps", 0.0) or 0.0),
                        slippage_bps=float(cfgd.get("slippage_ema_bps", 0.0) or 0.0),
                        fee_bps=float(os.getenv("TAKER_FEE_BPS", "4.0") or 4.0),
                        ts_ms=getattr(ctx, "ts_event_ms", None),
                        sid=getattr(ctx, "sid", None),
                    )
                except Exception:
                    pass
        except Exception:
            # AdaptiveTP1 is best-effort; never break enrichment
            with contextlib.suppress(Exception):
                ctx.tp1_adaptive_reason = "tp1_adaptive_skip_internal_error"
    except Exception:
        return

    sl = levels.get("sl", None)
    tps = levels.get("tp_levels", None)
    stop_dist = levels.get("stop_dist", None)
    rrs = levels.get("rr", None)

    if sl is None or not isinstance(tps, list) or len(tps) == 0:
        return
    if stop_dist is None:
        return

    # 3.1) Sanity floors в bps (поддерживается специфика символа).
    try:
        stop_bps = (stop_dist / float(entry_f)) * 10_000.0
        tp1_bps = (abs(float(tps[0]) - float(entry_f)) / float(entry_f)) * 10_000.0
    except Exception:
        return

    min_stop_bps = _sym_env_float("EDGE_LEVELS_MIN_STOP_BPS", symbol, 0.0)
    min_tp1_bps = _sym_env_float("EDGE_LEVELS_MIN_TP1_BPS", symbol, 0.0)
    if math.isfinite(min_stop_bps) and float(min_stop_bps) > 0.0:
        if not math.isfinite(stop_bps) or float(stop_bps) < float(min_stop_bps):
            if logger is not None:
                with contextlib.suppress(Exception):
                    logger.debug(
                        "attach_trade_levels_to_ctx: skip micro-stop: %s %s stop_bps=%.2f < min=%.2f",
                        symbol, side, float(stop_bps), float(min_stop_bps),
                    )
            if (not overwrite) and isinstance(cache, dict):
                cache[key] = {"status": "skipped", "reason": "floor_micro_stop"}
            return
    if math.isfinite(min_tp1_bps) and float(min_tp1_bps) > 0.0:
        if not math.isfinite(tp1_bps) or float(tp1_bps) < float(min_tp1_bps):
            if logger is not None:
                with contextlib.suppress(Exception):
                    logger.debug(
                        "attach_trade_levels_to_ctx: skip tiny-tp1: %s %s tp1_bps=%.2f < min=%.2f",
                        symbol, side, float(tp1_bps), float(min_tp1_bps),
                    )
            if (not overwrite) and isinstance(cache, dict):
                cache[key] = {"status": "skipped", "reason": "floor_tiny_tp1"}
            return

    # 4) Записать нормализованные поля, используемые гейтами/форматтерами.
    try:
        ctx.entry_price = float(entry_f)
        ctx.sl_price = sl
        ctx.tp_levels = [float(x) for x in tps]
        ctx.tp1_price = float(tps[0])
        if stop_dist is not None:
            ctx.stop_dist = stop_dist

        # Mode for telemetry
        tp_mode_used = levels.get("tp_mode_used", "ATR_LEGACY")
        ctx.tp_mode_used = tp_mode_used
        ctx.tp_mode = str(levels.get("mode", {}).get("tp", "ATR")).upper()

        # Опционально: единое значение RR (первый TP RR) помогает rr-mode fallback в гейтах.
        if isinstance(rrs, list) and len(rrs) > 0:
            with contextlib.suppress(Exception):
                ctx.tp_rr = float(rrs[0])

        # Backward-compat aliases (часто встречаются в старом коде)
        ctx.entry = float(entry_f)
        ctx.sl = sl
        ctx.tp1 = float(tps[0])

        # atr-mode support (если когда-то включите EDGE_EXPECTED_MOVE_MODE=atr)
        ms = _parse_csv_floats(cfg.get("TP_ATR_MULTS"))
        if ms:
            try:
                ctx.tp_atr_mults = ms
                ctx.tp1_atr_mult = ms[0]
            except Exception:
                pass

        # 8) Trailing Profile & Locks (for payload)
        ctx.trail_profile = _cfg_str(cfg, "trail_profile", "TRAIL_PROFILE", default="")
        ctx.trailing_min_lock_r = float(_cfg_get(cfg, "trailing_min_lock_r", "TRAILING_MIN_LOCK_R", default=0.0) or 0.0)

        # Опционально: сохранить symbol/side для отладки в логах ниже по потоку
        ctx.symbol = getattr(ctx, "symbol", None) or symbol
        ctx.side = getattr(ctx, "side", None) or side
    except Exception:
        # fail-open: никогда не прерывать публикацию сигналов
        if (not overwrite) and isinstance(cache, dict):
            cache[key] = {"status": "skipped", "reason": "write_ctx_failed"}
        return

    # Mark cache success (compute-once)
    if (not overwrite) and isinstance(cache, dict):
        cache[key] = {"status": "attached", "reason": "ok"}



def maybe_override_levels_from_empirical_time(
    ctx: Any,
    *,
    side: str,
    symbol: str,
    tf: str,
    kind: str,
    regime: str,
    redis_client: Any,
    overwrite: bool = True,
    logger: Any | None = None,
) -> None:
    """
    Optional strict empirical override:
      T = median(TTD_tp1)
      TP1_bps = quantile(MFE@T, q=0.6)
      SL_bps  = quantile(MAE@T, q=0.8)

    Writes (if enabled and enough data):
      ctx.tp1_price, ctx.sl_price, ctx.tp_levels[0], ctx.entry_price (kept), ctx.stop_dist

    Fail-open: never breaks publishing.
    """
    try:
        cfg = EmpiricalTimeLevelsConfig.from_env()
        if not cfg.enabled or redis_client is None:
            return
    except Exception:
        return

    # need entry
    try:
        entry = getattr(ctx, "entry_price", None) or getattr(ctx, "entry", None) or getattr(ctx, "price", None)
        entry_f = entry
        if entry_f <= 0:
            return
    except Exception:
        return

    try:
        prov = RedisEmpiricalTimeLevelsProvider(redis_client, cfg)
        res = prov.get_levels(kind=str(kind), symbol=symbol, tf=(tf or "1m"), regime=str(regime))
        if not res.ok:
            return
        tp1_bps = float(res.tp1_bps)
        sl_bps = float(res.sl_bps)
        if tp1_bps <= 0 or sl_bps <= 0:
            return
    except Exception:
        return

    s = (side or "").strip().upper()
    if s not in {"LONG", "SHORT"}:
        # keep compatible with existing _side_to_str
        s = "LONG" if s in {"BUY"} else ("SHORT" if s in {"SELL"} else "LONG")

    # Convert bps → prices
    try:
        tp_off = entry_f * (tp1_bps / 10_000.0)
        sl_off = entry_f * (sl_bps / 10_000.0)
        if s == "LONG":
            tp1_price = entry_f + tp_off
            sl_price = entry_f - sl_off
        else:
            tp1_price = entry_f - tp_off
            sl_price = entry_f + sl_off
        if overwrite or getattr(ctx, "tp1_price", None) is None:
            ctx.tp1_price = float(tp1_price)
        if overwrite or getattr(ctx, "sl_price", None) is None:
            ctx.sl_price = float(sl_price)
        # ensure tp_levels exists for other code paths
        try:
            tps = getattr(ctx, "tp_levels", None)
            if not isinstance(tps, list) or len(tps) == 0:
                ctx.tp_levels = [float(tp1_price)]
            else:
                tps[0] = float(tp1_price)
        except Exception:
            ctx.tp_levels = [float(tp1_price)]
        with contextlib.suppress(Exception):
            ctx.stop_dist = float(abs(entry_f - float(sl_price)))
        # useful for debugging / telemetry
        ctx.emp_time_bucket_ms = int(res.bucket_ms)
        ctx.emp_time_ttd_median_ms = int(res.ttd_median_ms)
        ctx.emp_time_n_alive = int(res.n_alive)
        if logger is not None:
            with contextlib.suppress(Exception):
                logger.debug(
                    "empirical_time_levels: %s %s kind=%s tf=%s regime=%s Tmed=%dms bucket=%dms n=%d tp1=%.2fbps sl=%.2fbps",
                    symbol, side, str(kind), tf, str(regime),
                    int(res.ttd_median_ms), int(res.bucket_ms), int(res.n_alive),
                    float(tp1_bps), float(sl_bps),
                )
    except Exception:
        return

