import json
import os
import pytest


def _load_trades_jsonl(path: str) -> list[dict]:
    trades = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            trades.append(json.loads(line))
    return trades


def _get_close_bucket(t: dict) -> str:
    # после фикса можно хранить явно: close_bucket
    # пока используем close_reason (у вас это bucket из finalize_trade)
    return str(t.get("close_bucket") or t.get("close_reason") or "").strip().upper()


def test_replay_quality_contract():
    """
    Прогон 200–500 реальных сделок (replay) и проверка:
    - negative_duration == 0   (т.е. duration_ms > 0 у нормальных сделок; bad time уходит в quarantine/UNKNOWN)
    - close_reason_inconsistent падает на порядок (в терминах теста: доля <= 2%)
    - strict WR близок к обычному WR (расхождение только за счёт UNKNOWN)
    """
    path = os.getenv("REPLAY_TRADES_PATH")
    if not path:
        pytest.skip("Set REPLAY_TRADES_PATH=/abs/path/to/trades.jsonl")

    trades = _load_trades_jsonl(path)
    assert len(trades) >= 200, f"Need >=200 trades for this contract test, got {len(trades)}"
    assert len(trades) <= 5000, "Too many trades for CI; provide a 200–500 slice"

    # 1) negative_duration == 0 (для “валидных” сделок)
    # Правило: duration_ms должен быть >0, либо сделка должна быть помечена как UNKNOWN/quarantine
    neg_dur = 0
    for t in trades:
        dur = int(t.get("duration_ms") or 0)
        bucket = _get_close_bucket(t)
        quarantined = bool(t.get("time_quarantined") or t.get("quarantined") or False)
        if dur <= 0 and (bucket not in ("UNKNOWN", "") and not quarantined):
            neg_dur += 1
    assert neg_dur == 0, f"Found {neg_dur} trades with duration_ms<=0 without UNKNOWN/quarantine"

    # 2) close_reason_inconsistent падает (в терминах теста — <=2%)
    # Инконсистентность: close_bucket == INITIAL_SL/SL при pnl_net>0
    inconsistent = 0
    for t in trades:
        pnl = float(t.get("pnl_net") or 0.0)
        bucket = _get_close_bucket(t)
        if pnl > 0 and bucket in ("SL", "INITIAL_SL"):
            inconsistent += 1
    inconsistent_share = inconsistent / max(1, len(trades))
    assert inconsistent_share <= 0.02, f"close_reason_inconsistent_share too high: {inconsistent_share:.3%}"

    # 3) strict WR ~ обычный WR (diff <= 3pp), отличие только UNKNOWN
    wins = sum(1 for t in trades if float(t.get("pnl_net") or 0.0) > 0)
    wr = wins / max(1, len(trades))

    unknown = sum(1 for t in trades if _get_close_bucket(t) in ("UNKNOWN", ""))
    strict_den = max(1, len(trades) - unknown)
    strict_wins = sum(
        1 for t in trades
        if _get_close_bucket(t) not in ("UNKNOWN", "")
        and float(t.get("pnl_net") or 0.0) > 0
    )
    if strict_den > 0:
        strict_wr = strict_wins / strict_den

        assert abs(strict_wr - wr) <= 0.03, f"strict WR diverges: wr={wr:.3%}, strict={strict_wr:.3%}, unknown={unknown}"
