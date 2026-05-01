# tools/recommend_trailing_from_redis.py
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import sys
import time
from typing import Dict, List, Optional

import logging
import redis  # pip install redis

logger = logging.getLogger(__name__)

from analysis.trailing_recommender import (
    ClosedTradeSnapshot,
    TrailingSizeRecommendation,
    recommend_trailing_size,
    EPS,
)

# (опционально) если хочешь тянуть stop_atr_mult из symbol spec
try:
    from services.pnl_math import get_symbol_info, spec_from_symbol_info
except Exception:  # fallback, если модуль недоступен
    get_symbol_info = None
    spec_from_symbol_info = None

try:
    from domain.normalizers import canon_source, canon_symbol
except ImportError:
    # simple local fallback if domain.normalizers not found or circular import
    def canon_source(s: str) -> str:
        s = (s or "").strip()
        sl = s.lower()
        if sl in ("cryptoorderflow", "crypto-orderflow"): return "CryptoOrderFlow"
        if sl == "orderflow": return "OrderFlow"
        return s or "Unknown"

    def canon_symbol(s: str) -> str:
        return (s or "UNKNOWN").upper()

# Try to import hydrate_trade_closed for compact stream support
try:
    from services.trade_closed_hydrator import hydrate_trade_closed
    _HAS_HYDRATOR = True
except ImportError:
    _HAS_HYDRATOR = False
    hydrate_trade_closed = None


def _to_bool(v) -> bool:
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def _parse_trade(fields: Dict[str, str], debug: bool = False) -> ClosedTradeSnapshot:
    def f(name: str, default: float = 0.0) -> float:
        v = fields.get(name)
        if v is None or v == "":
            return float(default)
        try:
            return float(v)
        except Exception:
            try:
                return float(str(v).replace(",", "."))
            except Exception:
                return float(default)

    def b(name: str) -> bool:
        v = fields.get(name)
        if v is None:
            return False
        s = str(v).strip().lower()
        return s in ("1", "true", "yes", "y", "on")

    exit_ts_ms = int(f("exit_ts_ms", 0.0))
    entry_tag = str(
        fields.get("entry_tag")
        or fields.get("signal_flavor")
        or fields.get("detector")
        or fields.get("reason_tag")
        or ""
    )

    one_r = f("one_r_money")
    mfe_pnl = f("mfe_pnl")
    pnl_net = f("pnl_net")
    pnl_r = f("pnl_r")
    lot = f("lot") or f("qty") or 0.0
    entry_px = f("entry_price") or f("entry_px") or 0.0
    sl_px = f("sl") or f("sl_price") or f("stop_loss") or 0.0

    # Попытка восстановить one_r_money, если оно 0
    if one_r <= 1e-9:
        # 0. Из PnL_R (pnl_net / pnl_r = 1R_money)
        if abs(pnl_r) > 1e-6:
             one_r = pnl_net / pnl_r
        
        # 1. Проверяем signal_payload
        if one_r <= 1e-9:
            payload_raw = fields.get("signal_payload")
            if payload_raw:
                try:
                    p = json.loads(payload_raw)
                    p_sl = float(p.get("sl") or p.get("sl_price") or 0.0)
                    p_entry = float(p.get("entry") or p.get("entry_price") or entry_px)
                    p_lot = float(p.get("qty") or p.get("lot") or lot)
                    if p_sl > 0 and p_lot > 0:
                        one_r = abs(p_entry - p_sl) * p_lot
                except Exception:
                    pass
        
        # 2. Если все еще 0, пробуем через корневые поля sl/entry
        if one_r <= 1e-9 and sl_px > 0 and lot > 0:
             one_r = abs(entry_px - sl_px) * lot
        
        # 3. Крайний случай: ATR
        if one_r <= 1e-9:
            atr = 0.0
            payload_raw = fields.get("signal_payload")
            if payload_raw:
                try:
                    p = json.loads(payload_raw)
                    atr = float(p.get("atr") or 0.0)
                except Exception: pass
            if atr <= 0:
                try:
                    feats = json.loads(fields.get("features", "{}"))
                    atr = float(feats.get("atr") or 0.0)
                except Exception: pass
            
            if atr > 0 and lot > 0:
                one_r = atr * 1.0 * lot

    return ClosedTradeSnapshot(
        source=str(fields.get("source") or fields.get("strategy_source") or "Unknown"),
        symbol=str(fields.get("symbol") or "UNKNOWN").upper(),
        pnl_net=pnl_net,
        one_r_money=one_r,
        mfe_pnl=mfe_pnl,
        giveback=f("giveback"),
        trailing_started=b("trailing_started"),
        trailing_active=b("trailing_active"),
        exit_ts_ms=exit_ts_ms,
        entry_tag=entry_tag,
    )


def load_trades_from_stream(
    r: redis.Redis,
    stream: str,
    limit: int,
    sources: List[str],
    symbols: List[str],
    from_ts_ms: Optional[int] = None,
    to_ts_ms: Optional[int] = None,
) -> List[ClosedTradeSnapshot]:
    sources_u = {canon_source(s) for s in (sources or [])}
    symbols_u = {canon_symbol(s) for s in (symbols or [])}
    
    # Per-symbol accumulation buckets
    trades_by_symbol: Dict[str, List[ClosedTradeSnapshot]] = {s: [] for s in symbols_u}
    # Also keep a general list if no symbols specified (fallback)
    all_trades: List[ClosedTradeSnapshot] = []
    
    # We want to find at least `limit` trades for EACH symbol.
    # We will page backwards until we satisfy this or hit a hard safety limit.
    
    CHUNK_SIZE = 1000
    MAX_SCAN_DEPTH = 2_000_000  # Increased from 100k to 2M to cover 24h on high-volume days
    
    last_id = "+"
    min_id = "-"
    if from_ts_ms is not None:
        min_id = f"{from_ts_ms}-0"

    total_scanned = 0
    
    logger.info(f"Smart loading from {stream}: target={limit} trades/symbol, max_depth={MAX_SCAN_DEPTH}, min_id={min_id}")
    
    while total_scanned < MAX_SCAN_DEPTH:
        # Check if all requested symbols are satisfied
        if symbols_u:
            pending = [s for s in symbols_u if len(trades_by_symbol[s]) < limit]
            if not pending:
                logger.debug("All symbols satisfied their target limit.")
                break
        elif len(all_trades) >= limit:
            # If no symbols specified, just use global limit
            break
            
        try:
            entries = r.xrevrange(stream, max=last_id, min=min_id, count=CHUNK_SIZE)
        except redis.exceptions.BusyLoadingError:
            logger.warning(f"Redis is loading dataset, skipping analysis for {stream}")
            return []
        except Exception as e:
            logger.error(f"Redis error reading {stream}: {e}")
            return []
            
        if not entries:
            break
            
        # Prepare next cursor (exclusive)
        # Edge case: if we got fewer than CHUNK_SIZE, we reached end
        if len(entries) < CHUNK_SIZE:
             is_last_chunk = True
        else:
             is_last_chunk = False
             
        # Process entries
        count_in_chunk = 0
        ids_in_chunk = []
        
        for _id, fields in entries:
            ids_in_chunk.append(_id)
            if _id == last_id:
                # Skip the overlap if we are paging overlapping
                # On first iter last_id='+', so no overlap
                continue
                
            if not isinstance(fields, dict):
                continue
            
            count_in_chunk += 1
            
            # Hydrate trade if hydrator is available (for compact stream support)
            hydrated_fields = fields
            if _HAS_HYDRATOR and hydrate_trade_closed:
                try:
                    hydrated_fields = hydrate_trade_closed(
                        r,
                        fields,
                        require_closed=False,
                        merge_precedence="hash"
                    )
                except Exception as e:
                    # If hydration fails, use original fields
                    logger.debug(f"Failed to hydrate trade {_id}: {e}")
                    hydrated_fields = fields
            
            trade = _parse_trade(hydrated_fields)
            
            # NORMALIZATION APPLIED HERE
            t_source = canon_source(trade.source)
            t_symbol = canon_symbol(trade.symbol)
            
            # Global Filters
            # Check canonical source against set
            if sources_u and t_source not in sources_u:
                continue
            
            if from_ts_ms is not None and trade.exit_ts_ms and trade.exit_ts_ms < from_ts_ms:
                continue
            if to_ts_ms is not None and trade.exit_ts_ms and trade.exit_ts_ms > to_ts_ms:
                continue
            
            # Bucket distribution
            if symbols_u:
                if t_symbol in symbols_u:
                    # Only collect if we need more for this symbol
                    if len(trades_by_symbol[t_symbol]) < limit:
                        trades_by_symbol[t_symbol].append(trade)
            else:
                if len(all_trades) < limit:
                     all_trades.append(trade)

        total_scanned += len(entries)
        
        if is_last_chunk:
            break
            
        # Update last_id for next page. 
        # We need specific syntax for exclusive. 
        # If we use the raw ID, we get it again. 
        # To go 'backwards' from 1000-0, we need 1000-0 limit or 999...
        # A simpler way is to use '(' prefix which xrevrange supports since Redis 6.2
        # If not supported, we decrement sequence.
        # Let's assume '(' works or handle overlap manually (we did manual check above `if _id == last_id`).
        # But wait, if we manually skip `last_id`, and `last_id` was the *only* item, we might break.
        # Best reliable way: use `(` prefix for ID passed to max.
        oldest_id = ids_in_chunk[-1]
        last_id = f"({oldest_id}" 

    # Compile result
    res: List[ClosedTradeSnapshot] = []
    if symbols_u:
        for s in symbols_u:
            res.extend(trades_by_symbol[s])
            logger.debug(f"Collected {len(trades_by_symbol[s])} trades for {s}")
    else:
        res = all_trades

    # Sort chronologically (oldest first) as expected by caller
    res.sort(key=lambda t: t.exit_ts_ms or 0)
    return res


def _count_filtered_wins(
    trades: List[ClosedTradeSnapshot],
    source: str,
    symbol: str,
    winners_only: bool = True,
    trailing_only: bool = False,
) -> tuple[int, int]:
    """
    Подсчитывает wins и total trades с теми же фильтрами, что использует recommend_trailing_size.
    Возвращает (filtered_wins, count_total).
    """
    count_total = 0
    filtered_wins = 0
    
    for t in trades:
        if t.symbol != symbol or t.source != source:
            continue
        
        # фильтр по трейлингу
        if trailing_only and not (t.trailing_started or t.trailing_active):
            continue
        
        count_total += 1
        
        one_r = float(t.one_r_money or 0.0)
        if one_r <= EPS:
            continue
        
        pnl_net = float(t.pnl_net or 0.0)
        mfe = float(t.mfe_pnl or 0.0)
        
        # calculate normalized values
        current_mfe_r = mfe / one_r
        
        if winners_only and pnl_net <= 0.0:
            continue
        
        if current_mfe_r <= 0.0:
            continue
        
        # Suspicious data filter: MFE > 100R is likely a data error or extreme outlier
        if current_mfe_r > 100.0:
            continue
        
        filtered_wins += 1
        print("WIN TRADE:", t)
    
    return filtered_wins, count_total


def _get_stop_atr_mult(r: redis.Redis, symbol: str, default: float) -> float:
    """
    Пытается вытащить stop_atr_mult из symbol spec.
    Если недоступно – возвращает default.
    """
    symbol_up = symbol.upper()
    if get_symbol_info and spec_from_symbol_info:
        try:
            info = get_symbol_info(symbol_up, r) or {}
            spec = spec_from_symbol_info(info)
            val = float(getattr(spec, "stop_atr_mult", default) or default)
            return val
        except redis.exceptions.BusyLoadingError:
            logger.warning(f"Redis is loading dataset, using default stop_atr_mult for {symbol}")
        except redis.exceptions.ConnectionError as e:
            logger.error(f"Redis connection error getting stop_atr_mult: {e}")
        except Exception as e:
            logger.debug(f"Error getting stop_atr_mult for {symbol}: {e}")
    return default


def _format_rec_md(rec: TrailingSizeRecommendation, title_suffix: str) -> str:
    if not rec:
        return f"- {title_suffix}: недостаточно данных.\n"

    warn = ""
    if rec.wins_count >= 5 and rec.confidence < 1e-6:
        warn = " ⚠️ DATA_QUALITY_SUSPICIOUS (std=0)"

    return (
        f"- {title_suffix}: n_total={rec.sample_size}, n_wins={rec.wins_count}, "
        f"lock_r={rec.lock_r:.2f}R, TP1_OFFSET_ATR={rec.lock_offset_atr:.2f}\n"
        f"  MFE_R avg/median={rec.avg_mfe_r:.2f}/{rec.median_mfe_r:.2f}, "
        f"giveback_R={rec.avg_giveback_r:.2f}, ratio={rec.avg_giveback_ratio:.2f}\n"
        f"  std(MFE_R)={rec.std_mfe_r:.2f}, std(giveback_ratio)={rec.std_giveback_ratio:.2f}, "
        f"confidence={rec.confidence:.2f}{warn}\n"
    )


def _get_symbol_class(symbol: str) -> str:
    """
    Определяет класс инструмента для применения разных порогов confidence.
    Возвращает: 'major' (BTC/ETH), 'large_alt' (SOL/XRP/BNB/SUI/DOGE), 'meme' (остальные).
    """
    symbol_upper = symbol.upper()
    majors = {"BTCUSDT", "ETHUSDT"}
    large_alts = {"SOLUSDT", "XRPUSDT", "BNBUSDT", "SUIUSDT", "DOGEUSDT", "APTUSDT", "XAUUSDT"}
    
    if symbol_upper in majors:
        return "major"
    elif symbol_upper in large_alts:
        return "large_alt"
    else:
        return "meme"


def _get_confidence_thresholds(
    symbol: str,
    propose_threshold: float,
    auto_apply_threshold: float,
    use_symbol_class: bool = True,
) -> tuple[float, float]:
    """
    Возвращает (propose_threshold, auto_apply_threshold) с учетом класса инструмента.
    
    По классам инструментов (если use_symbol_class=True):
    - BTC/ETH (мажоры): AUTO_APPLY ≥ 0.68
    - SOL/XRP/BNB/SUI/DOGE (крупные альты): AUTO_APPLY ≥ 0.70
    - мемы/тонкие: AUTO_APPLY ≥ 0.75 (или только proposal)
    """
    if not use_symbol_class:
        return propose_threshold, auto_apply_threshold
    
    symbol_class = _get_symbol_class(symbol)
    
    if symbol_class == "major":
        # BTC/ETH: более низкий порог для авто-апплая
        auto_apply = max(0.68, auto_apply_threshold)
        propose = min(propose_threshold, auto_apply - 0.05)  # proposal всегда ниже
    elif symbol_class == "large_alt":
        # Крупные альты: стандартный порог
        auto_apply = max(0.70, auto_apply_threshold)
        propose = min(propose_threshold, auto_apply - 0.05)
    else:
        # Мемы/тонкие: более высокий порог
        auto_apply = max(0.75, auto_apply_threshold)
        propose = min(propose_threshold, auto_apply - 0.05)
    
    return propose, auto_apply


def _check_hold_down(
    r: redis.Redis,
    symbol: str,
    hold_down_hours: float,
) -> bool:
    """
    Проверяет, прошло ли достаточно времени с последнего применения (hold-down).
    Возвращает True, если можно применять (прошло >= hold_down_hours).
    """
    if hold_down_hours <= 0:
        return True  # hold-down отключен
    
    key = f"symbol:trailing_cfg:{symbol.upper()}"
    try:
        last_apply_ms = r.hget(key, "last_auto_apply_ms")
        if not last_apply_ms:
            return True  # никогда не применяли - можно применять
        
        last_apply_ms = int(last_apply_ms)
        now_ms = get_ny_time_millis()
        elapsed_hours = (now_ms - last_apply_ms) / (1000 * 3600)
        
        return elapsed_hours >= hold_down_hours
    except Exception as e:
        logger.debug(f"Error checking hold-down for {symbol}: {e}")
        return True  # при ошибке разрешаем (fail-open)


def _check_min_delta_change(
    r: redis.Redis,
    symbol: str,
    new_tp1_offset_atr: float,
    min_delta_change_pct: float,
) -> bool:
    """
    Проверяет, достаточно ли изменился параметр для применения (min_delta_change).
    Возвращает True, если изменение >= min_delta_change_pct.
    """
    if min_delta_change_pct <= 0:
        return True  # проверка отключена
    
    key = f"symbol:trailing_cfg:{symbol.upper()}"
    try:
        current_tp1_offset_atr_str = r.hget(key, "tp1_offset_atr")
        if not current_tp1_offset_atr_str:
            return True  # нет текущего значения - можно применять
        
        current_tp1_offset_atr = float(current_tp1_offset_atr_str)
        if current_tp1_offset_atr <= 0:
            return True  # некорректное значение - можно применять
        
        delta_pct = abs((new_tp1_offset_atr - current_tp1_offset_atr) / current_tp1_offset_atr) * 100.0
        return delta_pct >= min_delta_change_pct
    except Exception as e:
        logger.debug(f"Error checking min_delta_change for {symbol}: {e}")
        return True  # при ошибке разрешаем (fail-open)


def _choose_final_for_autowrite(
    rec_all: TrailingSizeRecommendation | None,
    rec_trailing: TrailingSizeRecommendation | None,
    conf_threshold: float,
    *,
    propose_threshold: float | None = None,
    auto_apply_threshold: float | None = None,
    symbol="",
    min_trades_for_apply: int = 0,
    r: redis.Redis | None = None,
    hold_down_hours: float = 0.0,
    min_delta_change_pct: float = 0.0,
) -> tuple[TrailingSizeRecommendation | None, str]:
    """
    Улучшенная логика с двумя порогами (proposal vs auto-apply) и защитами.
    
    Возвращает (recommendation, action), где action:
    - 'auto_apply': применить автоматически
    - 'proposal': только предложение (не применять)
    - 'none': не делать ничего
    
    Логика:
    1. Если есть трейлинговая рекомендация и confidence >= auto_apply_threshold → auto_apply
    2. Иначе, если есть общая с confidence >= auto_apply_threshold → auto_apply
    3. Иначе, если confidence >= propose_threshold → proposal
    4. Иначе → none
    
    Защиты для auto_apply:
    - min_trades_for_apply: минимальное количество сделок для auto-apply
    - hold_down_hours: не применять чаще, чем раз в N часов
    - min_delta_change_pct: применять только если изменение >= N%
    """
    # Определяем пороги
    if propose_threshold is None:
        propose_threshold = conf_threshold * 0.85  # fallback: 85% от основного порога
    if auto_apply_threshold is None:
        auto_apply_threshold = conf_threshold
    
    # Применяем пороги по классам инструментов, если задан symbol
    if symbol:
        propose_threshold, auto_apply_threshold = _get_confidence_thresholds(
            symbol, propose_threshold, auto_apply_threshold, use_symbol_class=True
        )
    
    # Выбираем лучшую рекомендацию
    best_rec: TrailingSizeRecommendation | None = None
    if rec_trailing and rec_trailing.confidence >= auto_apply_threshold:
        best_rec = rec_trailing
    elif rec_all and rec_all.confidence >= auto_apply_threshold:
        best_rec = rec_all
    elif rec_trailing and rec_trailing.confidence >= propose_threshold:
        best_rec = rec_trailing
    elif rec_all and rec_all.confidence >= propose_threshold:
        best_rec = rec_all
    
    if not best_rec:
        return None, "none"
    
    # Определяем действие: auto_apply или proposal
    confidence_high_enough = best_rec.confidence >= auto_apply_threshold
    
    if confidence_high_enough:
        # Проверяем защиты для auto_apply
        # 1. min_trades_for_apply
        if min_trades_for_apply > 0:
            sample_size = best_rec.sample_size if hasattr(best_rec, 'sample_size') else getattr(best_rec, 'wins_count', 0)
            if sample_size < min_trades_for_apply:
                logger.debug(
                    f"Auto-apply blocked for {symbol}: sample_size={sample_size} < min_trades_for_apply={min_trades_for_apply}"
                )
                # Переводим в proposal, если проходит propose_threshold
                if best_rec.confidence >= propose_threshold:
                    return best_rec, "proposal"
                return None, "none"
        
        # 2. hold_down
        if r and hold_down_hours > 0 and symbol:
            if not _check_hold_down(r, symbol, hold_down_hours):
                logger.debug(
                    f"Auto-apply blocked for {symbol}: hold-down period not elapsed (need {hold_down_hours}h)"
                )
                # Переводим в proposal
                if best_rec.confidence >= propose_threshold:
                    return best_rec, "proposal"
                return None, "none"
        
        # 3. min_delta_change
        if r and min_delta_change_pct > 0 and symbol:
            tp1_offset_atr = best_rec.lock_offset_atr if hasattr(best_rec, 'lock_offset_atr') else getattr(best_rec, 'trailing_tp1_offset_atr', 0.0)
            if not _check_min_delta_change(r, symbol, tp1_offset_atr, min_delta_change_pct):
                logger.debug(
                    f"Auto-apply blocked for {symbol}: delta change too small (need >= {min_delta_change_pct}%)"
                )
                # Переводим в proposal
                if best_rec.confidence >= propose_threshold:
                    return best_rec, "proposal"
                return None, "none"
        
        # Все проверки пройдены - можно auto_apply
        return best_rec, "auto_apply"
    else:
        # Confidence достаточен только для proposal
        return best_rec, "proposal"


def _autowrite_symbol_trailing_cfg(
    r: redis.Redis,
    symbol: str,
    final_rec: TrailingSizeRecommendation,
    rec_all: TrailingSizeRecommendation | None,
    rec_trailing: TrailingSizeRecommendation | None,
    action: str = "auto_apply",
) -> None:
    """
    Пишем рекомендуемые параметры в отдельный ключ:
        symbol:trailing_cfg:{SYMBOL}

    Дальше ты можешь в get_symbol_info() подмешивать этот хеш в общий spec.
    
    Args:
        action: 'auto_apply' (применить) или 'proposal' (только предложение)
    """
    key = f"symbol:trailing_cfg:{symbol.upper()}"
    now_ms = get_ny_time_millis()
    
    # Получаем текущее значение для отслеживания изменений
    tp1_offset_atr = final_rec.lock_offset_atr if hasattr(final_rec, 'lock_offset_atr') else getattr(final_rec, 'trailing_tp1_offset_atr', 0.0)

    mapping: Dict[str, str] = {
        "tp1_offset_atr": f"{tp1_offset_atr:.6f}",
        "lock_r": f"{final_rec.lock_r:.6f}",
        "confidence": f"{final_rec.confidence:.4f}",
        "stop_atr_mult": f"{final_rec.stop_atr_mult:.6f}",
        "updated_at_ms": str(now_ms),
        "action": action,  # 'auto_apply' или 'proposal'
    }
    
    # Если auto_apply - записываем timestamp последнего применения
    if action == "auto_apply":
        mapping["last_auto_apply_ms"] = str(now_ms)
        
        # --- Интеграция в основной symbol_specs ---
        # Патчим основной spec, чтобы tick_processor и executor тут же подхватили новое значение.
        try:
            import json
            spec_key = f"symbol_specs:{symbol.upper()}"
            raw_spec = r.get(spec_key)
            if raw_spec:
                spec = json.loads(raw_spec)
            else:
                spec = {}

            if "trailing" not in spec:
                spec["trailing"] = {}

            # Обновляем значение.
            spec["trailing"]["tp1_offset_atr"] = round(float(tp1_offset_atr), 3)

            r.set(spec_key, json.dumps(spec, separators=(",", ":"), sort_keys=True))
            logger.info(f"[{symbol.upper()}] Injected trailing_tp1_offset_atr={spec['trailing']['tp1_offset_atr']} into {spec_key}")
        except Exception as e:
            logger.error(f"[{symbol.upper()}] Failed to inject trailing offset to symbol_specs: {e}")

    if rec_all:
        mapping.update(
            {
                "all_tp1_offset_atr": f"{rec_all.lock_offset_atr:.6f}",
                "all_lock_r": f"{rec_all.lock_r:.6f}",
                "all_confidence": f"{rec_all.confidence:.4f}",
                "all_sample_size": str(rec_all.sample_size),
                "all_wins_count": str(rec_all.wins_count),
            }
        )
    if rec_trailing:
        mapping.update(
            {
                "trailing_tp1_offset_atr": f"{rec_trailing.lock_offset_atr:.6f}",
                "trailing_lock_r": f"{rec_trailing.lock_r:.6f}",
                "trailing_confidence": f"{rec_trailing.confidence:.4f}",
                "trailing_sample_size": str(rec_trailing.sample_size),
                "trailing_wins_count": str(rec_trailing.wins_count),
            }
        )

    try:
        r.hset(key, mapping=mapping)
    except redis.exceptions.BusyLoadingError:
        logger.warning(f"Redis is loading dataset, skipping write to {key}")
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Redis connection error during write: {e}")
    except Exception as e:
        logger.error(f"Unexpected Redis error during write: {e}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Рекомендатор размера трейлинг-стопа по trades:closed из Redis",
    )
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--stream", default=os.getenv("TRAILING_AUTOTUNE_STREAM", "trades:closed"))
    parser.add_argument("--limit", type=int, default=int(os.getenv("TRAILING_AUTOTUNE_LIMIT", "2000")))
    parser.add_argument("--source", default=os.getenv("TRAILING_AUTOTUNE_SOURCE", "CryptoOrderFlow"))
    parser.add_argument(
        "--symbols",
        default=os.getenv("TRAILING_AUTOTUNE_SYMBOLS", ""),
        help="Список символов через запятую (например, ETHUSDT,BTCUSDT)",
    )
    parser.add_argument("--min-trades", type=int, default=int(os.getenv("TRAILING_AUTOTUNE_MIN_TRADES", "50")))
    parser.add_argument(
        "--min-wins",
        type=int,
        default=int(os.getenv("TRAILING_AUTOTUNE_MIN_WINS", "0")),
        help="Minimum number of WIN trades required (0 = use min-trades value)",
    )
    parser.add_argument(
        "--mfe-quantile",
        type=float,
        default=float(os.getenv("TRAILING_AUTOTUNE_MFE_QUANTILE", "0.25")),
    )
    parser.add_argument(
        "--auto-write",
        action="store_true",
        default=_to_bool(os.getenv("TRAILING_AUTOTUNE_ENABLED")),
        help="При включении пишет рекомендуемые значения в symbol:trailing_cfg:{symbol}",
    )
    parser.add_argument(
        "--conf-threshold",
        type=float,
        default=float(os.getenv("TRAILING_AUTOTUNE_CONF_THRESHOLD", "0.6")),
        help="Минимальная confidence для автозаписи (legacy, используется как fallback)",
    )
    parser.add_argument(
        "--propose-conf-threshold",
        type=float,
        default=float(os.getenv("TRAILING_AUTOTUNE_PROPOSE_CONF_THRESHOLD", "0.60")),
        help="Минимальная confidence для proposal (предложения без авто-применения)",
    )
    parser.add_argument(
        "--auto-apply-conf-threshold",
        type=float,
        default=float(os.getenv("TRAILING_AUTOTUNE_AUTO_APPLY_CONF_THRESHOLD", "0.72")),
        help="Минимальная confidence для auto-apply (автоматического применения)",
    )
    parser.add_argument(
        "--min-trades-for-apply",
        type=int,
        default=int(os.getenv("TRAILING_AUTOTUNE_MIN_TRADES_FOR_APPLY", "80")),
        help="Минимальное количество сделок для auto-apply (защита от шума)",
    )
    parser.add_argument(
        "--hold-down-hours",
        type=float,
        default=float(os.getenv("TRAILING_AUTOTUNE_HOLD_DOWN_HOURS", "24.0")),
        help="Hold-down период: не применять чаще, чем раз в N часов (0 = отключено)",
    )
    parser.add_argument(
        "--min-delta-change-pct",
        type=float,
        default=float(os.getenv("TRAILING_AUTOTUNE_MIN_DELTA_CHANGE_PCT", "5.0")),
        help="Минимальное изменение параметра в %% для auto-apply (0 = отключено)",
    )
    parser.add_argument(
        "--from-ts",
        type=int,
        default=None,
        help="Фильтр: минимальный exit_ts_ms (epoch ms).",
    )
    parser.add_argument(
        "--to-ts",
        type=int,
        default=None,
        help="Фильтр: максимальный exit_ts_ms (epoch ms).",
    )
    parser.add_argument(
        "--group-by-entry-tag",
        action="store_true",
        default=_to_bool(os.getenv("TRAILING_AUTOTUNE_GROUP_BY_TAG")),
        help="Если включено — выводит рекомендации по каждому entry_tag внутри символа.",
    )

    args = parser.parse_args(argv)

    symbols = [canon_symbol(s) for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        print("Нет символов для анализа", file=sys.stderr)
        return 1

    r = redis.from_url(args.redis_url, decode_responses=True)

    # Конвертируем from_ts и to_ts из аргументов командной строки
    from_ts_ms = args.from_ts if args.from_ts is not None else None
    to_ts_ms = args.to_ts if args.to_ts is not None else None

    trades = load_trades_from_stream(
        r,
        stream=args.stream,
        limit=args.limit,
        sources=[args.source],
        symbols=symbols,
        from_ts_ms=from_ts_ms,
        to_ts_ms=to_ts_ms,
    )

    if not trades:
        print("Нет сделок в потоке trades:closed (по заданным фильтрам)", file=sys.stderr)
        return 1

    # Markdown-отчёт
    lines: List[str] = []
    lines.append(f"### 🔧 Trailing calibration: {args.source}")
    lines.append(
        f"_stream=`{args.stream}`, limit={args.limit}, min_trades={args.min_trades}, "
        f"from_ts={args.from_ts}, to_ts={args.to_ts}_"
    )
    lines.append("")

    for symbol in symbols:
        stop_atr_mult = _get_stop_atr_mult(r, symbol, default=1.0)

        # "Expert" Logic with Escape Hatch
        # 1. Base thresholds
        eff_min_trades = args.min_trades
        eff_min_wins = args.min_wins if args.min_wins > 0 else 0

        # We need to construct the call potentially twice or check manually?
        # Let's trust `recommend_trailing_size` to handle standard checks, but we might need to "force" it if escape hatch applies.
        # Actually, `recommend_trailing_size` usually returns None if checks fail.
        # If I want to support "Escape Hatch", I might need to implement it inside `recommend_trailing_size` OR pass lower limits and filter result.

        # Let's pass the "Escape Hatch" logic by trying with standard limits first,
        # and if that fails, try with "Escape Hatch" limits IF criteria are met?
        # No, `recommend_trailing_size` takes `min_trades`.

        # Plan:
        # 1. Update `analysis/trailing_recommender.py` to support `min_wins` explicitly if it doesn't already (it seems it does from the call signature in `recommend_trailing_from_redis.py`).
        # 2. Implement "Escape Hatch" logic.
        
        # Checking file content of `recommend_trailing_from_redis.py` (Step 49)...
        # It imports `recommend_trailing_size` from `analysis.trailing_recommender`.
        
        # I will READ `analysis/trailing_recommender.py` first before writing.


        rec_all = recommend_trailing_size(
            trades,
            source=args.source,
            symbol=symbol,
            stop_atr_mult=stop_atr_mult,
            min_trades=args.min_trades,
            min_wins=args.min_wins if args.min_wins > 0 else None,
            mfe_quantile=args.mfe_quantile,
            trailing_only=False,
        )
        rec_trailing = recommend_trailing_size(
            trades,
            source=args.source,
            symbol=symbol,
            stop_atr_mult=stop_atr_mult,
            min_trades=max(10, args.min_trades // 2),
            min_wins=max(10, args.min_wins // 2) if args.min_wins > 0 else None,
            mfe_quantile=args.mfe_quantile,
            trailing_only=True,
        )
        lines.append(f"{symbol}")
        if not rec_all and not rec_trailing:
            # Diagnostics: count wins using canon_source/canon_symbol for correct matching
            src_canon = canon_source(args.source)
            sym_trades = [
                t for t in trades
                if canon_symbol(t.symbol) == symbol and canon_source(t.source) == src_canon
            ]

            # Расширенная диагностика: проверяем все source для этого символа
            # (trades уже отфильтрован по source в load_trades_from_stream, поэтому
            #  all_sources_for_symbol всегда будет пустым или содержать только args.source;
            #  для честной проверки используем отдельный mini-scan)
            all_sources_for_symbol = {}
            for t in trades:
                if canon_symbol(t.symbol) == symbol:
                    src = canon_source(t.source)
                    all_sources_for_symbol[src] = all_sources_for_symbol.get(src, 0) + 1

            filtered_wins_all, count_total_all = _count_filtered_wins(
                sym_trades, args.source, symbol, winners_only=True, trailing_only=False
            )
            filtered_wins_trailing, count_total_trailing = _count_filtered_wins(
                sym_trades, args.source, symbol, winners_only=True, trailing_only=True
            )
            eff_wins = args.min_wins if args.min_wins > 0 else max(10, args.min_trades // 3)

            # Основное сообщение
            msg = (
                f"- недостаточно данных для рекомендаций "
                f"(found_trades={len(sym_trades)}, "
                f"filtered_wins_all={filtered_wins_all}, filtered_wins_trailing={filtered_wins_trailing}, "
                f"need_trades={args.min_trades}, need_wins~={eff_wins})"
            )

            # Дополнительная диагностика
            if len(sym_trades) == 0:
                msg += f"\n  ⚠️  ПРОБЛЕМА: Нет сделок для {symbol} с source={args.source}"

                # Символы из уже source-отфильтрованного trades (быстрая проверка)
                all_symbols_in_stream = {}
                for t in trades:
                    s = canon_symbol(t.symbol)
                    all_symbols_in_stream[s] = all_symbols_in_stream.get(s, 0) + 1

                if all_sources_for_symbol:
                    msg += f"\n  💡 Найдены сделки с другими source: {dict(all_sources_for_symbol)}"
                    msg += f"\n  💡 Попробуйте запустить с --source <другой_source>"
                elif symbol in all_symbols_in_stream:
                    msg += f"\n  💡 Найдены {all_symbols_in_stream[symbol]} сделок для {symbol}, но с другими source"
                    msg += f"\n  💡 Проверьте правильность source фильтра"
                else:
                    msg += f"\n  💡 В stream нет сделок для {symbol} вообще"
                    if all_symbols_in_stream:
                        msg += f"\n  💡 Найдены сделки для других символов: {dict(sorted(all_symbols_in_stream.items(), key=lambda x: x[1], reverse=True)[:5])}"
                    msg += f"\n  💡 Проверьте:"
                    msg += f"\n     - Генерируются ли сигналы для {symbol}?"
                    msg += f"\n     - Открываются ли позиции (orders:open)?"
                    msg += f"\n     - Закрываются ли позиции (trades:closed)?"
                    msg += f"\n     - Проверьте stream: redis-cli XLEN {args.stream}"
                    msg += f"\n     - Проверьте последние записи: redis-cli XREVRANGE {args.stream} + - COUNT 10"
            elif len(sym_trades) < args.min_trades:
                msg += f"\n  ⚠️  ПРОБЛЕМА: Мало сделок ({len(sym_trades)} < {args.min_trades})"
                if all_sources_for_symbol and src_canon not in all_sources_for_symbol:
                    msg += f"\n  💡 Возможно несоответствие source. Найдены: {dict(all_sources_for_symbol)}"

            lines.append(msg + "\n")
            continue

        lines.append(_format_rec_md(rec_all, "Все win-сделки") if rec_all else "- Все win-сделки: нет данных.\n")
        lines.append(
            _format_rec_md(rec_trailing, "Только трейлинговые win-сделки")
            if rec_trailing
            else "- Только трейлинговые win-сделки: нет данных.\n"
        )

        # автообновление symbol-spec (через trailing_cfg)
        if args.auto_write:
            final_rec, action = _choose_final_for_autowrite(
                rec_all, rec_trailing, args.conf_threshold,
                propose_threshold=args.propose_conf_threshold,
                auto_apply_threshold=args.auto_apply_conf_threshold,
                symbol=symbol,
                min_trades_for_apply=args.min_trades_for_apply,
                r=r,
                hold_down_hours=args.hold_down_hours,
                min_delta_change_pct=args.min_delta_change_pct,
            )
            if final_rec:
                _autowrite_symbol_trailing_cfg(r, symbol, final_rec, rec_all, rec_trailing, action=action)
                tp1_offset_atr = final_rec.lock_offset_atr if hasattr(final_rec, 'lock_offset_atr') else getattr(final_rec, 'trailing_tp1_offset_atr', 0.0)
                action_emoji = "🔄" if action == "auto_apply" else "💡"
                action_text = "Автообновление" if action == "auto_apply" else "Предложение (proposal)"
                lines.append(
                    f"- {action_emoji} {action_text}: выбрана рекомендация "
                    f"{'trailing_only' if final_rec.trailing_only else 'all'} "
                    f"(TP1_OFFSET_ATR≈{tp1_offset_atr:.3f}, lock_r≈{final_rec.lock_r:.3f}, "
                    f"confidence≈{final_rec.confidence:.2f})\n"
                )
            else:
                lines.append(
                    f"- ⚠️ Автообновление выключено: confidence ниже порога "
                    f"(propose={args.propose_conf_threshold:.2f}, auto_apply={args.auto_apply_conf_threshold:.2f}).\n"
                )

        # группировка по entry_tag (топ-10 по числу сделок)
        if args.group_by_entry_tag:
            # выбираем только сделки этого символа/сорса с непустым тегом
            tag_map: Dict[str, List[ClosedTradeSnapshot]] = {}
            for t in trades:
                if t.symbol != symbol or t.source != args.source:
                    continue
                if not t.entry_tag:
                    continue
                tag_map.setdefault(t.entry_tag, []).append(t)

            if tag_map:
                lines.append("_Per-entry_tag recommendations:_")
                # сортируем по размеру выборки, берём топ-10
                for entry_tag, tag_trades in sorted(
                    tag_map.items(), key=lambda kv: len(kv[1]), reverse=True
                )[:10]:
                    rec_tag_all = recommend_trailing_size(
                        tag_trades,
                        source=args.source,
                        symbol=symbol,
                        stop_atr_mult=stop_atr_mult,
                        min_trades=max(10, args.min_trades // 3),
                        min_wins=max(10, args.min_wins // 3) if args.min_wins > 0 else None,
                        mfe_quantile=args.mfe_quantile,
                        trailing_only=False,
                    )
                    rec_tag_trailing = recommend_trailing_size(
                        tag_trades,
                        source=args.source,
                        symbol=symbol,
                        stop_atr_mult=stop_atr_mult,
                        min_trades=max(5, args.min_trades // 4),
                        min_wins=max(5, args.min_wins // 4) if args.min_wins > 0 else None,
                        mfe_quantile=args.mfe_quantile,
                        trailing_only=True,
                    )

                    lines.append(f"- entry_tag = {entry_tag}")
                    if not rec_tag_all and not rec_tag_trailing:
                        lines.append("  - недостаточно данных.\n")
                        continue

                    if rec_tag_all:
                        lines.append(
                            "  " + _format_rec_md(rec_tag_all, "Все win-сделки по тегу").replace("\n", "\n  ")
                        )
                    else:
                        lines.append("  - Все win-сделки по тегу: нет данных.\n")

                    if rec_tag_trailing:
                        lines.append(
                            "  "
                            + _format_rec_md(
                                rec_tag_trailing,
                                "Только трейлинговые win-сделки по тегу",
                            ).replace("\n", "\n  ")
                        )
                    else:
                        lines.append("  - Только трейлинговые win-сделки по тегу: нет данных.\n")

        lines.append("")

    # Печатаем Markdown — можно сразу кидать в Telegram
    md = "\n".join(lines).strip()
    print(md)
    return 0


def build_trailing_report_markdown_from_env(r: redis.Redis | None = None) -> str:
    """
    Строит Markdown-отчёт по тем же правилам, что main(),
    но берёт настройки из ENV и возвращает строку, вместо print().
    Используется Telegram-воркером.
    """
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    stream = os.getenv("TRAILING_AUTOTUNE_STREAM", "trades:closed")
    limit = int(os.getenv("TRAILING_AUTOTUNE_LIMIT", "2000"))
    source = os.getenv("TRAILING_AUTOTUNE_SOURCE", "CryptoOrderFlow")
    symbols_env = os.getenv("TRAILING_AUTOTUNE_SYMBOLS", "ETHUSDT,BTCUSDT")
    min_trades = int(os.getenv("TRAILING_AUTOTUNE_MIN_TRADES", "50"))
    min_wins = int(os.getenv("TRAILING_AUTOTUNE_MIN_WINS", "0"))
    mfe_quantile = float(os.getenv("TRAILING_AUTOTUNE_MFE_QUANTILE", "0.25"))
    auto_write = _to_bool(os.getenv("TRAILING_AUTOTUNE_ENABLED"))
    conf_threshold = float(os.getenv("TRAILING_AUTOTUNE_CONF_THRESHOLD", "0.6"))
    propose_conf_threshold = float(os.getenv("TRAILING_AUTOTUNE_PROPOSE_CONF_THRESHOLD", "0.60"))
    auto_apply_conf_threshold = float(os.getenv("TRAILING_AUTOTUNE_AUTO_APPLY_CONF_THRESHOLD", "0.72"))
    min_trades_for_apply = int(os.getenv("TRAILING_AUTOTUNE_MIN_TRADES_FOR_APPLY", "80"))
    hold_down_hours = float(os.getenv("TRAILING_AUTOTUNE_HOLD_DOWN_HOURS", "24.0"))
    min_delta_change_pct = float(os.getenv("TRAILING_AUTOTUNE_MIN_DELTA_CHANGE_PCT", "5.0"))
    from_ts = os.getenv("TRAILING_AUTOTUNE_FROM_TS")
    to_ts = os.getenv("TRAILING_AUTOTUNE_TO_TS")
    window_hours_env = os.getenv("TRAILING_AUTOTUNE_WINDOW_HOURS")
    group_by_entry_tag = _to_bool(os.getenv("TRAILING_AUTOTUNE_GROUP_BY_TAG"))

    from_ts_ms = None
    to_ts_ms = None

    # 1) Если задано скользящее окно в часах — используем его
    if window_hours_env not in (None, ""):
        try:
            window_hours = float(window_hours_env)
        except Exception:
            window_hours = 0.0

        if window_hours > 0:
            now_ms = get_ny_time_millis()
            window_ms = int(window_hours * 3600_000)
            from_ts_ms = now_ms - window_ms
            to_ts_ms = now_ms
    # 2) Иначе — используем явные границы, если они заданы
    if from_ts_ms is None and from_ts not in (None, ""):
        from_ts_ms = int(from_ts)
    if to_ts_ms is None and to_ts not in (None, ""):
        to_ts_ms = int(to_ts)

    symbols = [s.strip().upper() for s in symbols_env.split(",") if s.strip()]
    if not symbols:
        return "Нет символов для анализа."

    if r is None:
        try:
            r = redis.from_url(redis_url, decode_responses=True)
        except Exception as e:
            logger.error(f"Failed to connect to Redis at {redis_url}: {e}")
            return "Ошибка подключения к Redis"

    trades = load_trades_from_stream(
        r,
        stream=stream,
        limit=limit,
        sources=[source],
        symbols=symbols,
        from_ts_ms=from_ts_ms,
        to_ts_ms=to_ts_ms,
    )

    if not trades:
        return ""

    lines: list[str] = []
    lines.append(f"Trailing calibration: {source} {os.getenv('TRAILING_AUTOTUNE_REPORT_TITLE_SUFFIX', '')}")
    lines.append(
        f"stream={stream}, limit={limit}, min_trades={min_trades}"
    )
    lines.append("")

    for symbol in symbols:
        stop_atr_mult = _get_stop_atr_mult(r, symbol, default=1.0)

        rec_all = recommend_trailing_size(
            trades,
            source=source,
            symbol=symbol,
            stop_atr_mult=stop_atr_mult,
            min_trades=min_trades,
            min_wins=min_wins if min_wins > 0 else None,
            mfe_quantile=mfe_quantile,
            trailing_only=False,
        )
        rec_trailing = recommend_trailing_size(
            trades,
            source=source,
            symbol=symbol,
            stop_atr_mult=stop_atr_mult,
            min_trades=max(10, min_trades // 2),
            min_wins=max(10, min_wins // 2) if min_wins > 0 else None,
            mfe_quantile=mfe_quantile,
            trailing_only=True,
        )

        lines.append(f"{symbol}")
        if not rec_all and not rec_trailing:
            # Diagnostics: use canon_source/canon_symbol for correct matching
            src_canon = canon_source(source)
            sym_trades = [
                t for t in trades
                if canon_symbol(t.symbol) == symbol and canon_source(t.source) == src_canon
            ]

            # Расширенная диагностика: проверяем все source для этого символа
            # (trades уже отфильтрован по source в load_trades_from_stream)
            all_sources_for_symbol = {}
            for t in trades:
                if canon_symbol(t.symbol) == symbol:
                    src = canon_source(t.source)
                    all_sources_for_symbol[src] = all_sources_for_symbol.get(src, 0) + 1

            filtered_wins_all, count_total_all = _count_filtered_wins(
                sym_trades, source, symbol, winners_only=True, trailing_only=False
            )
            filtered_wins_trailing, count_total_trailing = _count_filtered_wins(
                sym_trades, source, symbol, winners_only=True, trailing_only=True
            )
            eff_wins = min_wins if min_wins > 0 else max(10, min_trades // 3)

            # Основное сообщение
            msg = (
                f"- недостаточно данных для рекомендаций "
                f"(found_trades={len(sym_trades)}, "
                f"filtered_wins_all={filtered_wins_all}, filtered_wins_trailing={filtered_wins_trailing}, "
                f"need_trades={min_trades}, need_wins~={eff_wins})"
            )

            # Дополнительная диагностика
            if len(sym_trades) == 0:
                msg += f"\n  ⚠️  ПРОБЛЕМА: Нет сделок для {symbol} с source={source}"

                # Символы из уже source-отфильтрованного trades (быстрая проверка)
                all_symbols_in_stream = {}
                for t in trades:
                    s = canon_symbol(t.symbol)
                    all_symbols_in_stream[s] = all_symbols_in_stream.get(s, 0) + 1

                if all_sources_for_symbol:
                    msg += f"\n  💡 Найдены сделки с другими source: {dict(all_sources_for_symbol)}"
                    msg += f"\n  💡 Попробуйте запустить с другим source"
                elif symbol in all_symbols_in_stream:
                    msg += f"\n  💡 Найдены {all_symbols_in_stream[symbol]} сделок для {symbol}, но с другими source"
                    msg += f"\n  💡 Проверьте правильность source фильтра"
                else:
                    msg += f"\n  💡 В stream нет сделок для {symbol} вообще"
                    if all_symbols_in_stream:
                        msg += f"\n  💡 Найдены сделки для других символов: {dict(sorted(all_symbols_in_stream.items(), key=lambda x: x[1], reverse=True)[:5])}"
                    msg += f"\n  💡 Проверьте:"
                    msg += f"\n     - Генерируются ли сигналы для {symbol}?"
                    msg += f"\n     - Открываются ли позиции (orders:open)?"
                    msg += f"\n     - Закрываются ли позиции (trades:closed)?"
                    msg += f"\n     - Проверьте stream: redis-cli XLEN {stream}"
                    msg += f"\n     - Проверьте последние записи: redis-cli XREVRANGE {stream} + - COUNT 10"
            elif len(sym_trades) < min_trades:
                msg += f"\n  ⚠️  ПРОБЛЕМА: Мало сделок ({len(sym_trades)} < {min_trades})"
                if all_sources_for_symbol and src_canon not in all_sources_for_symbol:
                    msg += f"\n  💡 Возможно несоответствие source. Найдены: {dict(all_sources_for_symbol)}"

            lines.append(msg + "\n")
            continue
        else:
            lines.append(_format_rec_md(rec_all, "Все win-сделки") if rec_all else "- Все win-сделки: нет данных.\n")
            lines.append(
                _format_rec_md(rec_trailing, "Только трейлинговые win-сделки")
                if rec_trailing
                else "- Только трейлинговые win-сделки: нет данных.\n"
            )

        if auto_write:
            final_rec, action = _choose_final_for_autowrite(
                rec_all, rec_trailing, conf_threshold,
                propose_threshold=propose_conf_threshold,
                auto_apply_threshold=auto_apply_conf_threshold,
                symbol=symbol,
                min_trades_for_apply=min_trades_for_apply,
                r=r,
                hold_down_hours=hold_down_hours,
                min_delta_change_pct=min_delta_change_pct,
            )
            if final_rec:
                _autowrite_symbol_trailing_cfg(r, symbol, final_rec, rec_all, rec_trailing, action=action)
                tp1_offset_atr = final_rec.lock_offset_atr if hasattr(final_rec, 'lock_offset_atr') else getattr(final_rec, 'trailing_tp1_offset_atr', 0.0)
                action_emoji = "🔄" if action == "auto_apply" else "💡"
                action_text = "Автообновление" if action == "auto_apply" else "Предложение (proposal)"
                lines.append(
                    f"- {action_emoji} {action_text}: выбрана рекомендация "
                    f"{'trailing_only' if final_rec.trailing_only else 'all'} "
                    f"(TP1_OFFSET_ATR≈{tp1_offset_atr:.3f}, lock_r≈{final_rec.lock_r:.3f}, "
                    f"confidence≈{final_rec.confidence:.2f})\n"
                )
            else:
                lines.append(
                    f"- ⚠️ Автообновление выключено: confidence ниже порога "
                    f"(propose={propose_conf_threshold:.2f}, auto_apply={auto_apply_conf_threshold:.2f}).\n"
                )

        if group_by_entry_tag:
            tag_map: dict[str, list[ClosedTradeSnapshot]] = {}
            for t in trades:
                if t.symbol != symbol or t.source != source:
                    continue
                if not t.entry_tag:
                    continue
                tag_map.setdefault(t.entry_tag, []).append(t)

            if tag_map:
                lines.append("_Per-entry_tag recommendations:_")
                for entry_tag, tag_trades in sorted(
                    tag_map.items(), key=lambda kv: len(kv[1]), reverse=True
                )[:10]:
                    rec_tag_all = recommend_trailing_size(
                        tag_trades,
                        source=source,
                        symbol=symbol,
                        stop_atr_mult=stop_atr_mult,
                        min_trades=max(10, min_trades // 3),
                        min_wins=max(10, min_wins // 3) if min_wins > 0 else None,
                        mfe_quantile=mfe_quantile,
                        trailing_only=False,
                    )
                    rec_tag_trailing = recommend_trailing_size(
                        tag_trades,
                        source=source,
                        symbol=symbol,
                        stop_atr_mult=stop_atr_mult,
                        min_trades=max(5, min_trades // 4),
                        min_wins=max(5, min_wins // 4) if min_wins > 0 else None,
                        mfe_quantile=mfe_quantile,
                        trailing_only=True,
                    )

                    lines.append(f"- entry_tag = {entry_tag}")
                    if not rec_tag_all and not rec_tag_trailing:
                        lines.append("  - недостаточно данных.\n")
                        continue

                    if rec_tag_all:
                        lines.append(
                            "  " + _format_rec_md(rec_tag_all, "Все win-сделки по тегу").replace("\n", "\n  ")
                        )
                    else:
                        lines.append("  - Все win-сделки по тегу: нет данных.\n")

                    if rec_tag_trailing:
                        lines.append(
                            "  "
                            + _format_rec_md(
                                rec_tag_trailing,
                                "Только трейлинговые win-сделки по тегу",
                            ).replace("\n", "\n  ")
                        )
                    else:
                        lines.append("  - Только трейлинговые win-сделки по тегу: нет данных.\n")

        lines.append("")

    return "\n".join(lines).strip()


if __name__ == "__main__":
    raise SystemExit(main())
