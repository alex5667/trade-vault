from typing import Any


def _f(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def compute_kelly_fraction_roll(trades: list[dict[str, Any]], n: int = 20) -> float:
    """Kelly criterion fraction: win_rate * (edge / odds).
    trades: list of dicts with 'pnl_ratio' (e.g. 0.01 = 1%, -0.005 = -0.5%).
    Output clamped to [0.0, 1.0].
    """
    if not trades:
        return 0.0

    recent = trades[-n:]
    wins = [t for t in recent if _f(t.get("pnl_ratio", 0)) > 0]
    losses = [t for t in recent if _f(t.get("pnl_ratio", 0)) <= 0]

    if not wins or not losses:
        return 0.0

    win_rate = len(wins) / len(recent)
    avg_win = sum(_f(t.get("pnl_ratio", 0)) for t in wins) / len(wins)
    avg_loss = abs(sum(_f(t.get("pnl_ratio", 0)) for t in losses) / len(losses))

    if avg_loss == 0:
        return 1.0

    # Kelly = W - (1-W)/R where R = avg_win / avg_loss
    # -> Kelly = W - (1-W)/(avg_win/abs(avg_loss))
    R = avg_win / avg_loss
    kelly = win_rate - ((1.0 - win_rate) / R)

    return max(0.0, min(1.0, kelly))

def compute_profit_factor_roll20(trades: list[dict[str, Any]]) -> float:
    """Gross profit / gross loss over last 20 trades.
    Trades are dicts with 'pnl_usd' or 'pnl_ratio'.
    """
    if not trades:
        return 1.0

    recent = trades[-20:]
    gross_profit = sum(_f(t.get("pnl_ratio", t.get("pnl_usd", 0))) for t in recent if _f(t.get("pnl_ratio", t.get("pnl_usd", 0))) > 0)
    gross_loss = abs(sum(_f(t.get("pnl_ratio", t.get("pnl_usd", 0))) for t in recent if _f(t.get("pnl_ratio", t.get("pnl_usd", 0))) < 0))

    if gross_loss == 0:
        return 999.0 if gross_profit > 0 else 1.0

    return min(999.0, gross_profit / gross_loss)

def compute_expectancy_bps(trades: list[dict[str, Any]], n: int = 50) -> float:
    """(win_rate * avg_win - (1-win_rate) * avg_loss) in bps."""
    if not trades:
        return 0.0

    recent = trades[-n:]
    wins = [t for t in recent if _f(t.get("pnl_ratio", 0)) > 0]
    losses = [t for t in recent if _f(t.get("pnl_ratio", 0)) <= 0]

    win_rate = len(wins) / len(recent)
    avg_win_bps = (sum(_f(t.get("pnl_ratio", 0)) for t in wins) / len(wins) * 10000) if wins else 0.0
    avg_loss_bps = (abs(sum(_f(t.get("pnl_ratio", 0)) for t in losses)) / len(losses) * 10000) if losses else 0.0

    exp_bps = (win_rate * avg_win_bps) - ((1.0 - win_rate) * avg_loss_bps)
    return min(max(exp_bps, -1000.0), 1000.0)

def compute_recovery_factor_roll(trades: list[dict[str, Any]], n: int = 20) -> float:
    """Net profit / max drawdown (last 20 trades)."""
    if not trades:
        return 0.0

    recent = trades[-n:]
    equity = 0.0
    peak = 0.0
    max_dd = 0.0

    for t in recent:
        equity += _f(t.get("pnl_ratio", 0))
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd

    if max_dd == 0:
        return 999.0 if equity > 0 else 0.0

    return min(999.0, equity / max_dd)

def compute_trade_freq_per_hr(trades: list[dict[str, Any]], current_ts_ms: int) -> float:
    """Trade frequency per hour in current session (looks back 1 hour max).
    Trades dict needs 'entry_ts_ms' or 'close_ts_ms'.
    """
    if not trades:
        return 0.0

    one_hour_ms = 3600_000
    cutoff = current_ts_ms - one_hour_ms

    count = 0
    for t in reversed(trades):
        ts = _f(t.get("close_ts_ms", t.get("entry_ts_ms", 0)))
        if ts >= cutoff:
            count += 1
        else:
            break

    return float(count)
