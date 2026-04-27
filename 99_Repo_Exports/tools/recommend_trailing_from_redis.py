#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Trailing Size Recommender CLI

Читает trades:closed из Redis, считает рекомендации по трейлингу для символов,
выводит Markdown-отчёт и (опционально) пишет в symbol:trailing_cfg:{symbol}.

Использование:
python tools/recommend_trailing_from_redis.py --source CryptoOrderFlow

С автозаписью:
TRAILING_AUTOTUNE_ENABLED=true python tools/recommend_trailing_from_redis.py
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

try:
    import redis
except ImportError:
    redis = None

# Add scanner_infra to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.trailing_size_recommender import (
    ClosedTradeSnapshot,
    TrailingSizeRecommendation,
    recommend_trailing_size,
)

try:
    from services.pnl_math import get_symbol_info, spec_from_symbol_info
except Exception:
    get_symbol_info = None
    spec_from_symbol_info = None


def _to_bool(v) -> bool:
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def _parse_trade(fields: Dict[str, str]) -> ClosedTradeSnapshot:
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

    return ClosedTradeSnapshot(
        source=str(fields.get("source") or fields.get("strategy_source") or "Unknown"),
        symbol=str(fields.get("symbol") or "UNKNOWN").upper(),
        strategy=str(fields.get("strategy") or ""),
        entry_tag=str(fields.get("entry_tag") or ""),
        exit_ts_ms=int(f("exit_ts_ms", 0.0)),
        pnl_net=f("pnl_net"),
        pnl_if_fixed_exit=f("pnl_if_fixed_exit"),
        one_r_money=f("one_r_money"),
        mfe_pnl=f("mfe_pnl"),
        mae_pnl=f("mae_pnl"),
        giveback=f("giveback"),
        missed_profit=f("missed_profit"),
        fees_money=f("fees_money", f("fees", f("commission", 0.0))),
        trailing_started=b("trailing_started"),
        trailing_active=b("trailing_active"),
        close_reason=str(fields.get("close_reason") or ""),
        close_reason_raw=str(fields.get("close_reason_raw") or ""),
        close_reason_detail=str(fields.get("close_reason_detail") or ""),
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
    sources_u = {s for s in (sources or [])}
    symbols_u = {s.upper() for s in (symbols or [])}
    res: List[ClosedTradeSnapshot] = []

    entries = r.xrevrange(stream, count=limit)
    for _id, fields in entries:
        if not isinstance(fields, dict):
            continue

        trade = _parse_trade(fields)
        if sources_u and trade.source not in sources_u:
            continue
        if symbols_u and trade.symbol not in symbols_u:
            continue

        if from_ts_ms is not None and trade.exit_ts_ms and trade.exit_ts_ms < from_ts_ms:
            continue
        if to_ts_ms is not None and trade.exit_ts_ms and trade.exit_ts_ms > to_ts_ms:
            continue

        res.append(trade)

    res.reverse()
    return res


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
        except Exception:
            pass
    return default


def _format_rec_md(rec: TrailingSizeRecommendation, title_suffix: str) -> str:
    if not rec:
        return f"- {title_suffix}: недостаточно данных.\n"

    # Вычисляем avg_mfe_r_win если его нет
    avg_mfe_r_win = getattr(rec, 'avg_mfe_r_win', rec.median_mfe_r_win)

    return (
        f"- {title_suffix}: n_total={rec.sample_size_total}, n_wins={rec.sample_size_win}, "
        f"lock_r≈{rec.lock_r:.2f}R → TP1_OFFSET_ATR≈{rec.trailing_tp1_offset_atr:.2f}\n"
        f"  - MFE_R avg/median≈{avg_mfe_r_win:.2f}/{rec.median_mfe_r_win:.2f}, "
        f"giveback_R≈{rec.avg_giveback_r_win:.2f}, ratio≈{rec.avg_giveback_ratio_win:.2f}\n"
        f"  - σ(MFE_R)≈{rec.std_mfe_r:.2f}, σ(giveback_ratio)≈{rec.std_giveback_ratio:.2f}, "
        f"confidence≈{rec.confidence:.2f}\n"
    )


def _choose_final_for_autowrite(
    rec_all: TrailingSizeRecommendation | None,
    rec_trailing: TrailingSizeRecommendation | None,
    conf_threshold: float,
) -> TrailingSizeRecommendation | None:
    """
    Простейшая логика:
    - если есть трейлинговая рекомендация и confidence >= threshold → её используем;
    - иначе, если есть общая с достаточным confidence → её;
    - иначе не пишем ничего.
    """
    if rec_trailing and rec_trailing.confidence >= conf_threshold:
        return rec_trailing
    if rec_all and rec_all.confidence >= conf_threshold:
        return rec_all
    return None


def _autowrite_symbol_trailing_cfg(
    r: redis.Redis,
    symbol: str,
    final_rec: TrailingSizeRecommendation,
    rec_all: TrailingSizeRecommendation | None,
    rec_trailing: TrailingSizeRecommendation | None,
) -> None:
    """
    Пишем рекомендуемые параметры в отдельный ключ:
        symbol:trailing_cfg:{SYMBOL}

    Дальше ты можешь в get_symbol_info() подмешивать этот хеш в общий spec.
    """
    key = f"symbol:trailing_cfg:{symbol.upper()}"
    now_ms = int(time.time() * 1000)

    mapping: Dict[str, str] = {
        "tp1_offset_atr": f"{final_rec.trailing_tp1_offset_atr:.6f}",
        "lock_r": f"{final_rec.lock_r:.6f}",
        "confidence": f"{final_rec.confidence:.4f}",
        "stop_atr_mult": f"{final_rec.stop_atr_mult:.6f}",
        "trailing_after_tp1_enabled": "true",  # Включаем трейлинг когда есть рекомендация
        "updated_at_ms": str(now_ms),
    }

    if rec_all:
        mapping.update(
            {
                "all_tp1_offset_atr": f"{rec_all.trailing_tp1_offset_atr:.6f}",
                "all_lock_r": f"{rec_all.lock_r:.6f}",
                "all_confidence": f"{rec_all.confidence:.4f}",
                "all_sample_size": str(rec_all.sample_size_total),
                "all_wins_count": str(rec_all.sample_size_win),
            }
        )
    if rec_trailing:
        mapping.update(
            {
                "trailing_tp1_offset_atr": f"{rec_trailing.trailing_tp1_offset_atr:.6f}",
                "trailing_lock_r": f"{rec_trailing.lock_r:.6f}",
                "trailing_confidence": f"{rec_trailing.confidence:.4f}",
                "trailing_sample_size": str(rec_trailing.sample_size_total),
                "trailing_wins_count": str(rec_trailing.sample_size_win),
            }
        )

    r.hset(key, mapping=mapping)


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
        default=os.getenv("TRAILING_AUTOTUNE_SYMBOLS", "ETHUSDT,BTCUSDT"),
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
        help="Минимальная confidence для автозаписи",
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

    if not redis:
        print("redis package not available", file=sys.stderr)
        return 1

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        print("Нет символов для анализа", file=sys.stderr)
        return 1

    r = redis.from_url(args.redis_url, decode_responses=True)

    trades = load_trades_from_stream(
        r,
        stream=args.stream,
        limit=args.limit,
        sources=[args.source],
        symbols=symbols,
        from_ts_ms=args.from_ts,
        to_ts_ms=args.to_ts,
    )

    if not trades:
        print("Нет сделок в потоке trades:closed (по заданным фильтрам)", file=sys.stderr)
        return 1

    # Markdown-отчёт
    lines: List[str] = []
    lines.append(f"### 🔧 Trailing calibration: {args.source}")
    lines.append(f"_stream=`{args.stream}`, limit={args.limit}, min_trades={args.min_trades}_")
    if args.from_ts:
        lines.append(f"_from_ts={args.from_ts}, to_ts={args.to_ts}_")
    lines.append("")

    for symbol in symbols:
        stop_atr_mult = _get_stop_atr_mult(r, symbol, default=1.0)

        # Filter trades for the current symbol
        symbol_trades = [t for t in trades if t.symbol == symbol and t.source == args.source]

        # Calculate fees_in_r_median for the symbol trades (for Breakeven Guard)
        fees_in_r_list = []
        for t in symbol_trades:
            if t.one_r_money > 1e-9 and t.fees_money > 1e-9:
                fees_in_r_list.append(t.fees_money / t.one_r_money)
        
        fees_in_r_median = 0.0
        if fees_in_r_list:
            fees_in_r_median = float(sorted(fees_in_r_list)[len(fees_in_r_list)//2])

        rec_all = recommend_trailing_size(
            symbol_trades,
            stop_atr_mult=stop_atr_mult,
            min_trades=args.min_trades,
            min_wins=args.min_wins if args.min_wins > 0 else None,
            mfe_quantile=args.mfe_quantile,
            trailing_only=False,
            fees_in_r_median=fees_in_r_median,
        )

        rec_trailing = recommend_trailing_size(
            symbol_trades,
            stop_atr_mult=stop_atr_mult,
            min_trades=max(10, args.min_trades // 2),
            min_wins=max(10, args.min_wins // 2) if args.min_wins > 0 else None,
            mfe_quantile=args.mfe_quantile,
            trailing_only=True,
            fees_in_r_median=fees_in_r_median,
        )

        lines.append(f"**{symbol}**")
        if not rec_all and not rec_trailing:
            # Diagnostics
            wins = len([t for t in symbol_trades if t.pnl_net > 0])
            eff_wins = args.min_wins if args.min_wins > 0 else max(10, args.min_trades // 3)
            lines.append(f"- недостаточно данных для рекомендаций (found_trades={len(symbol_trades)}, wins={wins}, need_trades={args.min_trades}, need_wins~={eff_wins}).\n")
            continue

        lines.append(_format_rec_md(rec_all, "Все win-сделки") if rec_all else "- Все win-сделки: нет данных.\n")
        lines.append(
            _format_rec_md(rec_trailing, "Только трейлинговые win-сделки")
            if rec_trailing
            else "- Только трейлинговые win-сделки: нет данных.\n"
        )

        # автообновление symbol-spec (через trailing_cfg)
        if args.auto_write:
            final_rec = _choose_final_for_autowrite(rec_all, rec_trailing, args.conf_threshold)
            if final_rec:
                _autowrite_symbol_trailing_cfg(r, symbol, final_rec, rec_all, rec_trailing)
                lines.append(
                    f"- 🔄 Автообновление: выбрана рекомендация "
                    f"{'trailing_only' if final_rec.trailing_only else 'all'} "
                    f"(TP1_OFFSET_ATR≈{final_rec.trailing_tp1_offset_atr:.3f}, lock_r≈{final_rec.lock_r:.3f}, "
                    f"confidence≈{final_rec.confidence:.2f})\n"
                )
            else:
                lines.append(
                    f"- ⚠️ Автообновление выключено: confidence ниже порога ({args.conf_threshold:.2f}).\n"
                )

        # группировка по entry_tag (топ-10 по числу сделок)
        if args.group_by_entry_tag:
            # выбираем только сделки этого символа/сорса с непустым тегом
            tag_map: Dict[str, List[ClosedTradeSnapshot]] = {}
            for t in symbol_trades:
                if not t.entry_tag:
                    continue
                tag_map.setdefault(t.entry_tag, []).append(t)

            if tag_map:
                lines.append("_Per-entry_tag recommendations:_")
                # сортируем по размеру выборки, берём топ-10
                for entry_tag, tag_trades in sorted(
                    tag_map.items(), key=lambda kv: len(kv[1]), reverse=True
                )[:10]:
                    # Calculate fees_in_r_median for the tag
                    fees_in_r_tag_list = [t.fees_money / t.one_r_money for t in tag_trades if t.one_r_money > 1e-9 and t.fees_money > 1e-9]
                    fees_in_r_tag_median = float(sorted(fees_in_r_tag_list)[len(fees_in_r_tag_list)//2]) if fees_in_r_tag_list else 0.0

                    rec_tag_all = recommend_trailing_size(
                        tag_trades,
                        stop_atr_mult=stop_atr_mult,
                        min_trades=max(10, args.min_trades // 3),
                        min_wins=max(10, args.min_wins // 3) if args.min_wins > 0 else None,
                        mfe_quantile=args.mfe_quantile,
                        trailing_only=False,
                        fees_in_r_median=fees_in_r_tag_median,
                    )
                    rec_tag_trailing = recommend_trailing_size(
                        tag_trades,
                        stop_atr_mult=stop_atr_mult,
                        min_trades=max(5, args.min_trades // 4),
                        min_wins=max(5, args.min_wins // 4) if args.min_wins > 0 else None,
                        mfe_quantile=args.mfe_quantile,
                        trailing_only=True,
                        fees_in_r_median=fees_in_r_tag_median,
                    )

                    lines.append(f"- **entry_tag = `{entry_tag}`**")
                    if not rec_tag_all and not rec_tag_trailing:
                        wins_tag = len([t for t in tag_trades if t.pnl_net > 0])
                        lines.append(f"  - недостаточно данных (found={len(tag_trades)}, wins={wins_tag}).\n")
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
    from_ts = os.getenv("TRAILING_AUTOTUNE_FROM_TS")
    to_ts = os.getenv("TRAILING_AUTOTUNE_TO_TS")
    group_by_entry_tag = _to_bool(os.getenv("TRAILING_AUTOTUNE_GROUP_BY_TAG"))

    from_ts_ms = int(from_ts) if from_ts not in (None, "") else None
    to_ts_ms = int(to_ts) if to_ts not in (None, "") else None

    symbols = [s.strip().upper() for s in symbols_env.split(",") if s.strip()]
    if not symbols:
        return "Нет символов для анализа."

    if r is None:
        r = redis.from_url(redis_url, decode_responses=True)

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
        return "Нет сделок в потоке trades:closed (по заданным фильтрам)."

    lines: list[str] = []
    lines.append(f"### 🔧 Trailing calibration: {source}")
    lines.append(
        f"_stream=`{stream}`, limit={limit}, min_trades={min_trades}, "
        f"from_ts={from_ts_ms}, to_ts={to_ts_ms}_"
    )
    lines.append("")

    for symbol in symbols:
        stop_atr_mult = _get_stop_atr_mult(r, symbol, default=1.0)

        # Filter trades for the current symbol
        symbol_trades = [t for t in trades if t.symbol == symbol and t.source == source]

        rec_all = recommend_trailing_size(
            symbol_trades,
            stop_atr_mult=stop_atr_mult,
            min_trades=min_trades,
            min_wins=min_wins if min_wins > 0 else None,
            mfe_quantile=mfe_quantile,
            trailing_only=False,
        )
        rec_trailing = recommend_trailing_size(
            symbol_trades,
            stop_atr_mult=stop_atr_mult,
            min_trades=max(10, min_trades // 2),
            min_wins=max(10, min_wins // 2) if min_wins > 0 else None,
            mfe_quantile=mfe_quantile,
            trailing_only=True,
        )

        lines.append(f"**{symbol}**")
        if not rec_all and not rec_trailing:
            # Diagnostics
            wins = len([t for t in symbol_trades if t.pnl_net > 0])
            eff_wins = min_wins if min_wins > 0 else max(10, min_trades // 3)
            lines.append(f"- недостаточно данных для рекомендаций (found_trades={len(symbol_trades)}, wins={wins}, need_trades={min_trades}, need_wins~={eff_wins}).\n")
        else:
            lines.append(_format_rec_md(rec_all, "Все win-сделки") if rec_all else "- Все win-сделки: нет данных.\n")
            lines.append(
                _format_rec_md(rec_trailing, "Только трейлинговые win-сделки")
                if rec_trailing
                else "- Только трейлинговые win-сделки: нет данных.\n"
            )

        if auto_write:
            final_rec = _choose_final_for_autowrite(rec_all, rec_trailing, conf_threshold)
            if final_rec:
                _autowrite_symbol_trailing_cfg(r, symbol, final_rec, rec_all, rec_trailing)
                lines.append(
                    f"- 🔄 Автообновление: выбрана рекомендация "
                    f"{'trailing_only' if final_rec and final_rec.trailing_only else 'all'} "
                    f"(TP1_OFFSET_ATR≈{final_rec.trailing_tp1_offset_atr:.3f}, lock_r≈{final_rec.lock_r:.3f}, "
                    f"confidence≈{final_rec.confidence:.2f})\n"
                )
            else:
                lines.append(
                    f"- ⚠️ Автообновление выключено: confidence ниже порога ({conf_threshold:.2f}).\n"
                )

        if group_by_entry_tag:
            tag_map: dict[str, list[ClosedTradeSnapshot]] = {}
            for t in symbol_trades:
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
                        mfe_quantile=mfe_quantile,
                        trailing_only=False,
                    )
                    rec_tag_trailing = recommend_trailing_size(
                        tag_trades,
                        source=source,
                        symbol=symbol,
                        stop_atr_mult=stop_atr_mult,
                        min_trades=max(5, min_trades // 4),
                        mfe_quantile=mfe_quantile,
                        trailing_only=True,
                    )

                    lines.append(f"- **entry_tag = `{entry_tag}`**")
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

