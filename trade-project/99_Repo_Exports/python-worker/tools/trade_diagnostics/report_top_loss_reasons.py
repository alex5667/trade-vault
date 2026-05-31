import os

import pandas as pd
import psycopg2
import redis

from utils.time_utils import get_ny_time_millis
import contextlib
from core.redis_keys import RedisStreams as RS

# Configuration (v1 Schema defaults)
REPORT_FEES_DOMINATES_BPS = float(os.getenv("REPORT_FEES_DOMINATES_BPS", "8.0"))
REPORT_GIVEBACK_PNL = float(os.getenv("REPORT_GIVEBACK_PNL", "0.0"))
REPORT_GIVEBACK_FRAC = float(os.getenv("REPORT_GIVEBACK_FRAC", "0.5"))
REPORT_L2_AGE_MS = float(os.getenv("REPORT_L2_AGE_MS", "250.0"))
REPORT_L2_STALE_RATIO = float(os.getenv("REPORT_L2_STALE_RATIO", "0.2"))

MIN_TRADES = int(os.getenv("REPORT_MIN_TRADES", "50"))
SEND_TELEGRAM = os.getenv("REPORT_SEND_TELEGRAM", "0") in ("1", "true", "True")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
NOTIFY_STREAM = os.getenv("NOTIFY_STREAM", RS.NOTIFY_TELEGRAM)

def fees_bps_roundtrip(row):
    try:
        fees = float(row.get("fees") or 0.0)
        notional = float(row.get("notional_usd") or 0.0)

        # calculate notional if missing but lot/entry_px exist
        if notional <= 0:
             lot = float(row.get("lot") or 0.0)
             px = float(row.get("entry_px") or 0.0)
             notional = lot * px

        if notional <= 1e-9: return 0.0
        return 10000.0 * fees / notional
    except Exception:
        return 0.0

def classify(row):
    if row["pnl_net"] >= 0:
        return "WIN_OR_BE"

    # 1. COST_DOMINATES: Fees are too high relative to standard
    fees_bps = row.get("fees_bps", 0.0)
    if fees_bps >= REPORT_FEES_DOMINATES_BPS:
        return "COST_DOMINATES"

    # 2. GIVEBACK_TRAIL: Significant giveback (missed profit)
    # Giveback is 'max_potential' - 'realized'.
    # If using mfe_pnl as reference: giveback ~ mfe_pnl - pnl_gross (roughly)
    # The row has explicit 'giveback' column from SQL now.
    giveback = row.get("giveback", 0.0)
    mfe_pnl = row.get("mfe_pnl", 0.0)

    if giveback > REPORT_GIVEBACK_PNL and mfe_pnl > 0:
        if giveback >= (REPORT_GIVEBACK_FRAC * mfe_pnl):
            return "GIVEBACK_TRAIL"

    # 3. L2_STALE: Execution environment issues
    l2_age = row.get("health_avg_l2_age_ms", 0.0)
    stale_now = row.get("health_l2_stale_ratio_now", 0.0)
    if l2_age > REPORT_L2_AGE_MS or stale_now > REPORT_L2_STALE_RATIO:
        return "L2_STALE"

    # 4. EARLY_STOP (NO FOLLOW THROUGH):
    # MAE is large (stopped out), MFE was small.
    # We use pnl (absolute) logic usually, or just absence of MFE.
    # If we are here, it's a loss.
    # If mfe_pnl was small or zero, it means price went straight to stop.
    # Let's consider Early Stop if we didn't have much MFE.
    # "EARLY_STOP: mae_pnl large, mfe_pnl small"
    if mfe_pnl < abs(row.get("pnl_net", 0.0)) * 0.5: # metric heuristic
         return "EARLY_STOP"

    return "OTHER"

def send_telegram_report(text_report):
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        message = {
            "type": "report",
            "text": f"📊 <b>Top 3 Loss Reasons Analysis (v1)</b>\n<pre>{text_report}</pre>",
            "parse_mode": "HTML",
            "source": "report_top_loss_reasons",
            "timestamp": str(get_ny_time_millis())
        }
        r.xadd(NOTIFY_STREAM, message, maxlen=20000)
        print(f"Report sent to Redis stream {NOTIFY_STREAM}")
    except Exception as e:
        print(f"Failed to send to Telegram: {e}")

def main():
    dsn = os.getenv("ANALYTICS_DB_DSN")
    if not dsn:
        print("ANALYTICS_DB_DSN not set")
        return

    # Defaults for report window: last 24h
    now_ms = get_ny_time_millis()
    default_from = now_ms - 24 * 3600 * 1000

    from_ms = int(os.getenv("REPORT_FROM_MS", default_from))
    to_ms = int(os.getenv("REPORT_TO_MS", now_ms))

    print(f"Fetching trades from {from_ms} to {to_ms}...")

    # Connect using psycopg2 directly
    try:
        conn = psycopg2.connect(dsn)
        # Assuming DSN is libpq connection string or URI.
        # If it uses 'postgresql+psycopg2://' prefix (sqlalchemy style), psycopg2 might complain.
        # We should strip "+psycopg2" if present.
        if dsn.startswith("postgresql+psycopg2://"):
             dsn = dsn.replace("postgresql+psycopg2://", "postgresql://")
             conn = psycopg2.connect(dsn)
    except Exception as e:
        print(f"DB Connection failed: {e}")
        return

    # Allow overriding via env var or fallback to local
    env_sql = os.getenv("REPORT_SQL_FILE")
    if env_sql and os.path.exists(env_sql):
        sql_path = env_sql
    else:
        # Fallback to local default if env var not set or not found
        sql_path = os.path.join(os.path.dirname(__file__), "sql_trades_window.sql")
        # Try finding the new default location if the old one is gone
        if not os.path.exists(sql_path):
             alt_path = os.path.join(os.path.dirname(__file__), "sql", "trades_window.sql")
             if os.path.exists(alt_path):
                 sql_path = alt_path

    if not os.path.exists(sql_path):
        print(f"SQL file not found: {sql_path}")
        with contextlib.suppress(Exception):
             conn.close()
        return

    try:
         with open(sql_path, encoding="utf-8") as f:
             query_str = f.read()
    except Exception as e:
         print(f"Error reading SQL file: {e}")
         with contextlib.suppress(Exception):
             conn.close()
         return

    # Use pandas read_sql with connection
    # Note: query_str uses :from_ms, :to_ms (named params). Psycopg2 uses %(name)s or %s.
    # Pandas read_sql might delegate to driver.
    # If the SQL uses :param, we might need to conform to psycopg2 style %(param)s.
    # Let's simple-replace :from_ms -> %(from_ms)s and :to_ms -> %(to_ms)s
    query_str = query_str.replace(":from_ms", "%(from_ms)s").replace(":to_ms", "%(to_ms)s")

    try:
        df = pd.read_sql(
            query_str,
            conn,
            params={"from_ms": from_ms, "to_ms": to_ms}
        )
    except Exception as e:
        print(f"Error reading SQL: {e}")
        with contextlib.suppress(Exception):
             conn.close()
        return
    finally:
        with contextlib.suppress(Exception):
             conn.close()

    if len(df) < MIN_TRADES:
        print(f"Too few trades: {len(df)} < {MIN_TRADES}")
        return

    # --- Preprocessing ---
    # 0. Normalize column aliases from SQL variants
    alias_map = {
        "fees_usd": "fees",
        "qty": "lot",
        "entry_price": "entry_px",
        "exit_price": "exit_px",
        "trade_id": "order_id",
        "side": "direction",
    }
    for src, dest in alias_map.items():
        if dest not in df.columns and src in df.columns:
            df[dest] = df[src]

    # 1. Basic numeric conversion
    cols_to_float = [
        "entry_px", "exit_px", "pnl_net", "pnl_gross", "fees", "lot", "notional_usd",
        "mfe_pnl", "mae_pnl", "giveback", "missed_profit",
        "health_avg_l2_age_ms", "health_l2_stale_ratio_now", "health_l2_stale_ratio_tick"
    ]
    missing_numeric = [c for c in cols_to_float if c not in df.columns]
    for c in missing_numeric:
        df[c] = 0.0
    for c in cols_to_float:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)
    if missing_numeric:
        print(f"WARN: missing numeric columns defaulted to 0.0: {', '.join(missing_numeric)}")

    # Sanitize data: remove corrupted or incomplete trades with 0 entry/exit price
    # These trades skew the PnL aggregations heavily (e.g. exit_px=0 causes massive artificial losses)
    initial_len = len(df)
    df = df[(df["entry_px"] > 0) & (df["exit_px"] > 0)].copy()
    if len(df) < initial_len:
        print(f"INFO: Dropped {initial_len - len(df)} corrupted trades (entry/exit px <= 0)")

    if df.empty:
        print("No valid trades found after dropping corrupted trades.")
        return

    # 2. Calculated Metrics
    df["fees_bps"] = df.apply(fees_bps_roundtrip, axis=1)

    # 3. Classification
    df["bucket"] = df.apply(classify, axis=1)

    # --- Aggregation ---

    # 1) Top buckets by negative PnL contribution
    neg = df[df["pnl_net"] < 0].copy()

    if neg.empty:
        print("No losing trades found.")
        return

    buckets = (neg.groupby("bucket")
                 .agg(trades=("order_id","count"),
                      pnl_sum=("pnl_net","sum"),
                      pnl_avg=("pnl_net","mean"),
                      fees_med=("fees_bps","median"),
                      l2_age_med=("health_avg_l2_age_ms", "median"))
                 .sort_values("pnl_sum", ascending=True))

    # 2) Toxic slices (Symbol x EntryTag / Direction)
    # Ensure columns exist
    for c in ["symbol", "entry_tag", "direction", "source"]:
        if c not in df.columns:
            df[c] = "n/a"

    # Slice 1: Symbol x EntryTag
    slice_tag = (df.groupby(["symbol", "entry_tag"])
                .agg(trades=("order_id","count"),
                     winrate=("pnl_net", lambda s: float((s>0).mean())),
                     pnl_sum=("pnl_net","sum"))
                .sort_values("pnl_sum", ascending=True))

    # Slice 2: Source x Symbol
    slice_src = (df.groupby(["source", "symbol"])
                .agg(trades=("order_id","count"),
                     winrate=("pnl_net", lambda s: float((s>0).mean())),
                     pnl_sum=("pnl_net","sum"))
                .sort_values("pnl_sum", ascending=True))

    output_lines = []
    output_lines.append(f"REPORT WINDOW: {pd.to_datetime(from_ms, unit='ms')} - {pd.to_datetime(to_ms, unit='ms')} (UTC)")
    output_lines.append(f"TRADES: {len(df)}")
    output_lines.append("\nTOP LOSS BUCKETS (by contribution):")
    output_lines.append(buckets.head(5).to_string())
    output_lines.append("\nTOP TOXIC TAGS (Symbol x EntryTag):")
    output_lines.append(slice_tag.head(5).to_string())
    output_lines.append("\nTOP TOXIC SOURCES (Source x Symbol):")
    output_lines.append(slice_src.head(5).to_string())

    report_text = "\n".join(output_lines)

    print(report_text)

    if SEND_TELEGRAM:
        send_telegram_report(report_text)

if __name__ == "__main__":
    main()
