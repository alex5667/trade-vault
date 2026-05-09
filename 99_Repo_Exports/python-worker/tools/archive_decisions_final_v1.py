
import json
import logging
import os
from datetime import UTC, datetime
from typing import Any

import redis
import contextlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

class DecisionsArchiver:
    def __init__(self):
        self.redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
        self.r = redis.Redis.from_url(self.redis_url, decode_responses=True)

        self.stream_key = os.getenv("DECISIONS_FINAL_STREAM", "decisions:final")
        self.archive_dir = os.getenv("DECISIONS_FINAL_ARCHIVE_DIR", "/var/lib/trade/archives/decisions_final")
        self.state_key = os.getenv("DECISIONS_FINAL_ARCHIVER_STATE_KEY", "archiver:decisions_final:last_id")

        os.makedirs(self.archive_dir, exist_ok=True)

    def get_last_id(self) -> str:
        lid = self.r.get(self.state_key)
        return lid if lid else "0-0"

    def set_last_id(self, last_id: str):
        self.r.set(self.state_key, last_id)

    def parse_entry(self, entry_id: str, fields: dict[str, Any]) -> dict[str, Any] | None:
        """Parse stream entry into a clean dict for archiving."""
        try:
            # Try payload first
            if "payload" in fields:
                try:
                    data = json.loads(fields["payload"])
                    # Ensure metadata from stream is preserved if not in payload
                    if "id" not in data:
                        data["_stream_id"] = entry_id
                    if "ts" not in data:
                        ts_ms = int(entry_id.split("-")[0])
                        data["ts"] = ts_ms
                    return data
                except json.JSONDecodeError:
                    pass

            # Fallback
            data = fields.copy()
            data["_stream_id"] = entry_id
            if "ts" not in data:
                data["ts"] = int(entry_id.split("-")[0])
            else:
                with contextlib.suppress(Exception):
                    data["ts"] = int(data["ts"])
            return data
        except Exception as e:
            logger.warning(f"Failed to parse entry {entry_id}: {e}")
            return None

    def archive_batch(self, count: int = 1000):
        last_id = self.get_last_id()
        logger.info(f"Reading from {last_id}, count={count}")

        entries = self.r.xread({self.stream_key: last_id}, count=count, block=1000)

        if not entries:
            logger.info("No new entries.")
            return

        stream_data = entries[0][1]
        if not stream_data:
            return

        processed_count = 0
        current_file_handle = None
        current_file_path = None

        try:
            for eid, fields in stream_data:
                data = self.parse_entry(eid, fields)
                if not data:
                    continue

                # Determine date from timestamp
                ts_ms = data.get("ts", 0)
                dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=UTC)
                date_str = dt.strftime("%Y-%m-%d")

                target_path = os.path.join(self.archive_dir, f"{date_str}.ndjson")

                # Switch file handle if needed
                if current_file_path != target_path:
                    if current_file_handle:
                        current_file_handle.close()
                    current_file_handle = open(target_path, "a", encoding="utf-8")
                    current_file_path = target_path

                # Write NDJSON line
                json_line = json.dumps(data)
                current_file_handle.write(json_line + "\n")

                last_id = eid
                processed_count += 1

        finally:
            if current_file_handle:
                current_file_handle.close()

        if processed_count > 0:
            self.set_last_id(last_id)
            logger.info(f"Archived {processed_count} records. Last ID: {last_id}")

    def run_once(self):
        # In a real batch job, we might loop until no more data,
        # but to prevent infinite loops if stream is very fast, we might limit it.
        # But 'archive' usually implies catching up.
        # Let's loop while we get full batches.

        batch_size = 5000
        while True:
            last_id = self.get_last_id()
            entries = self.r.xread({self.stream_key: last_id}, count=batch_size, block=None)

            if not entries or not entries[0][1]:
                break

            stream_data = entries[0][1]

            # Logic duplication from archive_batch to avoid double read or complexity
            # Let's refactor simple: call archive_batch logic

            # Actually, standard pattern: read, process, update offset.
            # Rerolling the inner logic here for loop efficiency.

            # To keep it simple and safe, I will just call archive_batch in loop
            # until it returns < batch_size or 0.
            # But my archive_batch uses XREAD with block=1000.
            # Let's clarify: archive_batch reads ONCE.

            pass
            # Re-implementing loop here using helper is cleaner if helper captures return count.

            count = self._process_batch(stream_data)
            if count == 0:
                break

            if count < batch_size:
                break # Caught up

    def _process_batch(self, stream_data) -> int:
        processed_count = 0
        current_file_handle = None
        current_file_path = None
        last_id = None

        try:
            for eid, fields in stream_data:
                data = self.parse_entry(eid, fields)
                if not data:
                    last_id = eid # Skip but advance
                    continue

                ts_ms = data.get("ts", 0)
                dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=UTC)
                date_str = dt.strftime("%Y-%m-%d")

                target_path = os.path.join(self.archive_dir, f"{date_str}.ndjson")

                if current_file_path != target_path:
                    if current_file_handle:
                        current_file_handle.close()
                    current_file_handle = open(target_path, "a", encoding="utf-8")
                    current_file_path = target_path

                current_file_handle.write(json.dumps(data) + "\n")
                last_id = eid
                processed_count += 1
        finally:
            if current_file_handle:
                current_file_handle.close()

        if last_id:
            self.set_last_id(last_id)
            logger.info(f"Archived batch of {processed_count}. New Last ID: {last_id}")

        return len(stream_data)

if __name__ == "__main__":
    archiver = DecisionsArchiver()
    # Run until caught up
    logger.info("Starting archiver run...")
    archiver.run_once()
    logger.info("Archiver run complete.")
