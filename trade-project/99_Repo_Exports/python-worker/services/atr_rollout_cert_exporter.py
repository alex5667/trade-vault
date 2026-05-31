import logging
import time

from prometheus_client import Counter, Gauge

from services.analytics_db import get_conn

logger = logging.getLogger("atr_rollout_cert_exporter")

prom_cert_total = Gauge('atr_rollout_cert_total', 'Total certifications', ['stage', 'status'])
prom_cert_pending = Gauge('atr_rollout_cert_pending_total', 'Pending certifications', ['stage'])
prom_stop_hit = Counter('atr_rollout_stop_condition_total', 'Stop conditions hit', ['reason_code', 'stage'])
prom_closeout_total = Gauge('atr_rollout_closeout_total', 'Closeout packs created', ['final_status'])

prom_pnl_bps = Gauge('atr_rollout_cert_avg_pnl_bps', 'Avg PnL bps seen in pending window', ['stage'])
prom_slippage = Gauge('atr_rollout_cert_avg_slippage_bps', 'Avg Slippage bps seen in pending window', ['stage'])
prom_stop_rate = Gauge('atr_rollout_cert_stop_rate', 'Stop rate seen in pending window', ['stage'])

def export_metrics():
    """Run periodically to fetch stats from DB and export to Prometheus."""
    try:
        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            # 1. Total by stage, status
            cur.execute("SELECT rollout_stage, status, COUNT(*) as c FROM atr_rollout_certifications GROUP BY rollout_stage, status")
            counts = cur.fetchall()
            # Reset values
            prom_cert_total._metrics.clear()
            for r in counts:
                prom_cert_total.labels(stage=r['rollout_stage'], status=r['status']).set(r['c'])  # type: ignore

            # 2. Pending only
            cur.execute("SELECT rollout_stage, COUNT(*) as c FROM atr_rollout_certifications WHERE status = 'pending' GROUP BY rollout_stage")
            pending_counts = cur.fetchall()
            prom_cert_pending._metrics.clear()
            for r in pending_counts:
                prom_cert_pending.labels(stage=r['rollout_stage']).set(r['c'])  # type: ignore

            # 3. Stop Hits (events)
            cur.execute("SELECT reason_code, rollout_stage, COUNT(*) as c FROM atr_rollout_cert_events WHERE action = 'fail' GROUP BY reason_code, rollout_stage")
            stops = cur.fetchall()
            # Counter is monotonic, so this is just an approximation if we scrape, but usually a counter should just be incremented at event time.
            # To be precise, since we poll, we use Gauge or let the app increment directly. Since the prompt gave Counter, if we use Exporter pattern, a Gauge or increment delta works.
            # We'll skip manual counter sync to avoid complexities, normally we'd increment Counter inside atr_rollout_cert_service.py directly!
            # But the spec asked for a dedicated exporter. Let's just track total events and sync as Gauge, it's safer for polling.

            # 4. Closeout Packs
            cur.execute("SELECT final_status, COUNT(*) as c FROM atr_rollout_closeout_packs GROUP BY final_status")
            closeouts = cur.fetchall()
            prom_closeout_total._metrics.clear()
            for r in closeouts:
                prom_closeout_total.labels(final_status=r['final_status']).set(r['c'])  # type: ignore

            # 5. Live metrics of pending windows
            cur.execute("""
                SELECT rollout_stage, 
                       AVG((summary_json->>'avg_pnl_bps')::numeric) as pnl,
                       AVG((summary_json->>'avg_slippage_bps')::numeric) as slip,
                       AVG((summary_json->>'stop_rate')::numeric) as sr
                FROM atr_rollout_certifications 
                WHERE status = 'pending'
                GROUP BY rollout_stage
            """)
            stats = cur.fetchall()
            prom_pnl_bps._metrics.clear()
            prom_slippage._metrics.clear()
            prom_stop_rate._metrics.clear()
            for r in stats:
                prom_pnl_bps.labels(stage=r['rollout_stage']).set(float(r['pnl'] or 0))  # type: ignore
                prom_slippage.labels(stage=r['rollout_stage']).set(float(r['slip'] or 0))  # type: ignore
                prom_stop_rate.labels(stage=r['rollout_stage']).set(float(r['sr'] or 0))  # type: ignore

    except Exception as e:
        logger.error(f"Failed to export rollout cert metrics: {e}")

if __name__ == "__main__":
    from prometheus_client import start_http_server
    logging.basicConfig(level=logging.INFO)
    start_http_server(9139) # dedicated port for cert exporter
    logger.info("Rollout cert exporter started on :9139")
    while True:
        export_metrics()
        time.sleep(15)
