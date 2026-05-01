from __future__ import annotations

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional, List, Dict
from dataclasses import dataclass

from common.log import setup_logger
from services.telegram.telegram_client import TelegramClient

logger = setup_logger("EdgeGateReporter")

@dataclass
class EdgeGateReportConfig:
    db_dsn: str
    lookback_hours: int = 6
    min_trades: int = 5

class EdgeGateReporter:
    def __init__(self, config: EdgeGateReportConfig, telegram: Optional[TelegramClient] = None):
        self.config = config
        self.telegram = telegram or TelegramClient.from_env()

    def generate_and_send(self) -> bool:
        """
        Generates the analytic report and sends it to Telegram.
        Returns True if sent successfully (or no data), False on error.
        """
        try:
            stats = self._fetch_stats()
            if not stats:
                logger.info("EdgeGateReporter: No stats found for report.")
                return True

            msg = self._format_message(stats)
            if self.telegram:
                self.telegram.send_text(msg)
                logger.info("EdgeGateReporter: Report sent to Telegram.")
            else:
                logger.warning("EdgeGateReporter: Telegram client not configured, printing report:\n" + msg)
            
            return True
        except Exception as e:
            logger.error(f"EdgeGateReporter: Error generating report: {e}", exc_info=True)
            return False

    def _fetch_stats(self) -> List[Dict]:
        query = """
            SELECT
                symbol,
                count(*) AS n,
                avg(CASE WHEN passed THEN 1.0 ELSE 0.0 END) AS pass_rate,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY margin_bps) as p50_margin,
                percentile_cont(0.9) WITHIN GROUP (ORDER BY margin_bps) as p90_margin,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY req_bps) as p50_req,
                avg(fees_bps) as avg_fees
            FROM edge_gate_events
            WHERE ts >= now() - interval '%s hours'
            GROUP BY symbol
            HAVING count(*) >= %s
            ORDER BY n DESC;
        """
        
        conn = None
        try:
            conn = psycopg2.connect(self.config.db_dsn)
            with conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, (self.config.lookback_hours, self.config.min_trades))
                    return cur.fetchall()
        finally:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
                conn.close()

    def _format_message(self, stats: List[Dict]) -> str:
        lines = [f"🛡️ <b>Edge Gate Analytics ({self.config.lookback_hours}h)</b>"]
        lines.append("")
        
        for row in stats:
            symbol = row['symbol']
            n = row['n']
            pass_rate = float(row['pass_rate']) * 100
            p50_margin = float(row['p50_margin'])
            p90_margin = float(row['p90_margin'])
            avg_fees = float(row['avg_fees'])
            
            # Icon logic
            icon = "✅"
            if pass_rate < 10.0: icon = "💀"
            elif pass_rate < 30.0: icon = "⚠️"
            
            # Margin health
            margin_str = f"M50: {p50_margin:+.1f}"
            if p50_margin < 0:
                margin_str = f"<b>{margin_str}</b>" # Bold negative median margin
            
            line = (
                f"{icon} <b>{symbol}</b> (n={n})\n"
                f"   Pass: {pass_rate:.1f}% | {margin_str} | M90: {p90_margin:+.1f}\n"
                f"   Fees: {avg_fees:.1f} bps"
            )
            lines.append(line)
            
        lines.append("")
        lines.append(f"<i>min_trades={self.config.min_trades}</i>")
        return "\n".join(lines)

def run_once():
    """Helper for ad-hoc execution"""
    dsn = (os.getenv("ANALYTICS_DB_DSN") or os.getenv("TRADES_DB_DSN"))
    if not dsn:
        print("Error: TRADES_DB_DSN not set")
        return
        
    cfg = EdgeGateReportConfig(db_dsn=dsn, lookback_hours=6)
    reporter = EdgeGateReporter(cfg)
    reporter.generate_and_send()

if __name__ == "__main__":
    run_once()
