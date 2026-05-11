import hashlib
import json
import logging
import os
from datetime import UTC, datetime
from typing import Any

from services.analytics_db import get_conn

logger = logging.getLogger("atr_dataset_exporter")

def _calculate_sha256(filepath: str) -> str:
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def register_dataset(
    dataset_type: str,
    symbol: str,
    scenario: str,
    regime: str,
    venue: str,
    window_from: datetime,
    window_to: datetime,
    source_stream: str,
    row_count: int,
    storage_uri: str,
    sha256_hash: str,
    schema_ver: str,
    tags_json: dict[str, Any],
    is_golden: bool = False
) -> str:
    """Registers the dataset in the SQL registry and enforces immutability."""
    dataset_id = f"ds_{dataset_type}_{symbol.lower()}_{int(window_from.timestamp())}"

    with get_conn() as conn, conn.cursor() as cur:
        # Check if exists (immutable)
        cur.execute("SELECT sha256 FROM atr_replay_datasets WHERE dataset_id = %s", (dataset_id,))
        row = cur.fetchone()
        if row:
            if row[0] != sha256_hash:
                raise ValueError(f"Dataset {dataset_id} already exists with different sha256! Immutability violation.")
            logger.info(f"Dataset {dataset_id} already registered.")
            return dataset_id

        cur.execute("""
            INSERT INTO atr_replay_datasets (
                dataset_id, dataset_type, symbol, scenario, regime, venue,
                window_from, window_to, source_stream, row_count,
                storage_uri, sha256, schema_ver, tags_json, is_golden
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """, (
            dataset_id, dataset_type, symbol, scenario, regime, venue,
            window_from, window_to, source_stream, row_count,
            storage_uri, sha256_hash, schema_ver, json.dumps(tags_json), is_golden
        ))
        conn.commit()
    return dataset_id

def export_mixed_bundle(
    symbol: str,
    window_from: datetime,
    window_to: datetime,
    output_path: str,
    scenario: str = "default",
    regime: str = "auto",
    is_golden: bool = True
) -> str:
    """Exports a mixed bundle (signals, diagnostics, execution shadow, closed trades).
    For now, this uses placeholder selects. Implement concrete DB lookups per production schema.
    """
    logger.info(f"Exporting mixed bundle for {symbol} to {output_path}")

    # Ensure dir exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    row_count = 0
    with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
        with open(output_path, "w") as f:
            # 1. Closed Trades (post-trade truth)
            cur.execute("""
                SELECT * FROM trades_closed
                WHERE symbol = %s AND closed_at >= %s AND closed_at <= %s
                ORDER BY closed_at ASC
            """, (symbol, window_from, window_to))

            for row in cur.fetchall():
                # Serialize dates
                for k, v in row.items():  # type: ignore
                    if isinstance(v, datetime):
                        row[k] = v.isoformat()  # type: ignore
                bundle_row = {"_type": "closed_trades", "data": dict(row)}
                f.write(json.dumps(bundle_row) + "\n")
                row_count += 1

            # Note: For Phase 6.1 production, you also query:
            # - stream_diagnostics (veto reasons, gate outputs)
            # - signal_raw (original payloads)
            # - execution_shadow (risk_pct, tp/sl calculations)
            # using similar append logic.

    sha256_hash = _calculate_sha256(output_path)
    storage_uri = f"file://{os.path.abspath(output_path)}"

    dataset_id = register_dataset(
        dataset_type="mixed_bundle",
        symbol=symbol,
        scenario=scenario,
        regime=regime,
        venue="binance",
        window_from=window_from,
        window_to=window_to,
        source_stream="db_mixed",
        row_count=row_count,
        storage_uri=storage_uri,
        sha256_hash=sha256_hash,
        schema_ver="v1",
        tags_json={"exported_by": "atr_golden_dataset_exporter"},
        is_golden=is_golden
    )

    logger.info(f"Exported mixed_bundle {dataset_id} with {row_count} rows. SHA256: {sha256_hash}")
    return dataset_id

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from datetime import timedelta

    now = datetime.now(UTC)
    # Simple smoke test export
    try:
        ds_id = export_mixed_bundle(
            symbol="BTCUSDT",
            window_from=now - timedelta(hours=2),
            window_to=now,
            output_path="/tmp/atr_datasets/btc_smoke.ndjson"
        )
        print(f"Smoke test successful: {ds_id}")
    except Exception as e:
        print(f"Export failed: {e}")
