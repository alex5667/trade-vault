import sys

with open("python-worker/services/archivers/stream_archiver.py", "r") as f:
    content = f.read()

# 1. PgWriter methods 
pg_methods = """
    def ensure_trade_kpi_liqmap_v1_table(self) -> None:
        ddl = \"\"\"
        CREATE TABLE IF NOT EXISTS trade_kpi_liqmap_v1 (
          stream_id TEXT NOT NULL,
          ts_ms BIGINT NOT NULL,
          ts TIMESTAMPTZ NOT NULL,
          trade_id TEXT NOT NULL,
          symbol TEXT NOT NULL,
          side TEXT NOT NULL,
          regime TEXT NOT NULL,
          sl_hit_near_liqmap_peak SMALLINT,
          tp1_anchored SMALLINT,
          tp1_anchored_and_hit SMALLINT,
          sl_liqmap_peak_dist_bps DOUBLE PRECISION,
          sl_liqmap_peak_usd DOUBLE PRECISION,
          liqmap_kpi JSONB NOT NULL,
          payload_json JSONB NOT NULL,
          PRIMARY KEY (stream_id, ts)
        );
        \"\"\"
        idx = \"\"\"
        CREATE INDEX IF NOT EXISTS trade_kpi_liqmap_v1_symbol_ts_idx
          ON trade_kpi_liqmap_v1 (symbol, ts DESC);
        CREATE INDEX IF NOT EXISTS trade_kpi_liqmap_v1_trade_id_ts_idx
          ON trade_kpi_liqmap_v1 (trade_id, ts DESC);
        CREATE INDEX IF NOT EXISTS trade_kpi_liqmap_v1_liqmap_kpi_gin
          ON trade_kpi_liqmap_v1 USING GIN (liqmap_kpi jsonb_path_ops);
        \"\"\"
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
                try:
                    cur.execute("SELECT create_hypertable('trade_kpi_liqmap_v1','ts', if_not_exists => TRUE);")
                except Exception:
                    conn.rollback()
                cur.execute(idx)
            conn.commit()

    def insert_trade_kpi_liqmap_v1(self, rows) -> int:
        if not rows:
            return 0
        sql = \"\"\"
        INSERT INTO trade_kpi_liqmap_v1 (
          stream_id, ts_ms, ts,
          trade_id, symbol, side, regime,
          sl_hit_near_liqmap_peak, tp1_anchored, tp1_anchored_and_hit,
          sl_liqmap_peak_dist_bps, sl_liqmap_peak_usd,
          liqmap_kpi, payload_json
        ) VALUES %s
        ON CONFLICT (stream_id, ts) DO NOTHING
        \"\"\"
        with self._conn() as conn:
            with conn.cursor() as cur:
                from psycopg2.extras import execute_values
                execute_values(cur, sql, rows, page_size=2000)
        return len(rows)
"""
content = content.replace("class StreamArchiver:", pg_methods + "\nclass StreamArchiver:")

# 2. Add configs to __init__
init_configs = """
        self.post_sl_stream = env("POST_SL_STREAM", "trades:post_sl")
        self.post_sl_liqmap_enabled = env_int("POST_SL_LIQMAP_KPI_ARCHIVE_ENABLED", 0) == 1
        self.post_sl_liqmap_cg = env("POST_SL_LIQMAP_KPI_CG", "post_sl_liqmap_kpi_archiver")
        self.post_sl_liqmap_consumer = env("POST_SL_LIQMAP_KPI_CONSUMER", "archiver_post_sl_1")
        self.post_sl_liqmap_batch = env_int("POST_SL_LIQMAP_KPI_BATCH", 2000)
        self.post_sl_liqmap_block_ms = env_int("POST_SL_LIQMAP_KPI_BLOCK_MS", 1000)
        self.post_sl_liqmap_min_idle = env_int("POST_SL_LIQMAP_KPI_MIN_IDLE_MS", 60000)
        self.post_sl_liqmap_dlq = env("POST_SL_LIQMAP_KPI_DLQ_STREAM", "stream:dlq:post_sl_liqmap_kpi")
        self.post_sl_liqmap_auto_migrate = env_bool("POST_SL_LIQMAP_KPI_AUTO_MIGRATE", True)
        self.post_sl_liqmap_status_hash = env("POST_SL_LIQMAP_KPI_ARCHIVER_STATUS_HASH", "metrics:post_sl_liqmap_kpi_archiver")

"""
content = content.replace('self.conf_scores_stream = env("CONF_SCORES_STREAM", "signals:confidence:scores")', 'self.conf_scores_stream = env("CONF_SCORES_STREAM", "signals:confidence:scores")\n' + init_configs)

# 3. StreamArchiver methods
archiver_methods = """
    def post_sl_liqmap_kpi_row(self, stream_id, payload):
        ts_ms = coalesce_ts_ms(payload, stream_id)
        import datetime as dt
        import json
        from utils.type_utils import safe_int, safe_float
        ts = dt.datetime.fromtimestamp(ts_ms / 1000.0, tz=dt.timezone.utc)
        trade_id = str(payload.get("trade_id") or payload.get("id") or "").strip()
        symbol = str(payload.get("symbol") or "").strip().upper()
        side = str(payload.get("side") or "").strip().upper()
        regime = str(payload.get("regime") or payload.get("market_regime") or "unknown").strip().lower()
        if not trade_id or not symbol or not side:
            raise ValueError(f"missing_required_fields trade_id={trade_id} symbol={symbol} side={side}")
        liqmap_kpi = {}
        for k, v in payload.items():
            if isinstance(k, str) and k.startswith("liqmap_"):
                liqmap_kpi[k] = v
        for k in (
            "sl_hit_near_liqmap_peak", "sl_liqmap_peak_dist_bps", "sl_liqmap_peak_usd",
            "tp1_anchored", "tp1_anchored_and_hit", "liqmap_levels_applied",
            "liqmap_tp1_adj_bps", "liqmap_sl_adj_bps", "liqmap_levels_reason"
        ):
            if k in payload:
                liqmap_kpi[k] = payload.get(k)
        sl_hit = safe_int(payload.get("sl_hit_near_liqmap_peak"))
        tp1_anchored = safe_int(payload.get("tp1_anchored"))
        tp1_hit = safe_int(payload.get("tp1_anchored_and_hit"))
        sl_peak_dist_bps = safe_float(payload.get("sl_liqmap_peak_dist_bps"))
        sl_peak_usd = safe_float(payload.get("sl_liqmap_peak_usd"))
        return (
            stream_id, ts_ms, ts, trade_id, symbol, side, regime,
            sl_hit, tp1_anchored, tp1_hit, sl_peak_dist_bps, sl_peak_usd,
            json.dumps(liqmap_kpi, ensure_ascii=False), json.dumps(payload, ensure_ascii=False)
        )

    async def consume_post_sl_liqmap_kpi(self) -> None:
        import asyncio
        from services.archivers.stream_utils import parse_stream_payload, ts_ms_from_stream_id
        await self.ensure_group(self.post_sl_stream, self.post_sl_liqmap_cg)
        loop = asyncio.get_running_loop()
        while True:
            pending = await self._claim_pending(
                self.post_sl_stream, self.post_sl_liqmap_cg, self.post_sl_liqmap_consumer,
                self.post_sl_liqmap_min_idle, self.post_sl_liqmap_batch)
            if pending:
                msgs = pending
            else:
                resp = await self._read_new(
                    self.post_sl_stream, self.post_sl_liqmap_cg, self.post_sl_liqmap_consumer,
                    self.post_sl_liqmap_batch, self.post_sl_liqmap_block_ms)
                if not resp:
                    continue
                _, msgs = resp[0]
            rows = []
            ack_ids = []
            for mid, fields in msgs:
                try:
                    payload = parse_stream_payload(fields)
                    rows.append(self.post_sl_liqmap_kpi_row(mid, payload))
                    ack_ids.append(mid)
                except Exception as e:
                    await self.dlq(self.post_sl_liqmap_dlq, self.post_sl_stream, mid, f"parse_error:{e}", {"fields": str(fields)[:2000]})
                    await self.r.xack(self.post_sl_stream, self.post_sl_liqmap_cg, mid)
            if not rows:
                continue
            try:
                await loop.run_in_executor(None, self.pg.insert_trade_kpi_liqmap_v1, rows)
                await self.r.xack(self.post_sl_stream, self.post_sl_liqmap_cg, *ack_ids)
            except Exception as e:
                await self.dlq(self.post_sl_liqmap_dlq, self.post_sl_stream, ack_ids[0], f"pg_batch_error:{e}", {"batch_size": len(rows)})
                import asyncio
                await asyncio.sleep(1.0)
"""
content = content.replace("    async def run(self) -> None:", archiver_methods + "\n    async def run(self) -> None:")

# 4. Inject into run()
run_lines = """
        if self.post_sl_liqmap_enabled and self.post_sl_liqmap_auto_migrate:
            await loop.run_in_executor(None, self.pg.ensure_trade_kpi_liqmap_v1_table)
"""
tasks_append = """
        if self.post_sl_liqmap_enabled:
            tasks.append(asyncio.create_task(self.consume_post_sl_liqmap_kpi()))
"""
content = content.replace("        tasks = []", run_lines + "\n        tasks = []\n" + tasks_append)

with open("python-worker/services/archivers/stream_archiver.py", "w") as f:
    f.write(content)
