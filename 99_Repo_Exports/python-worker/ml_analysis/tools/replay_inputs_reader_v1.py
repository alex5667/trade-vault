import gzip
import json
import logging
from collections.abc import Generator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

class ReplayInputsReader:
    """Utility to read replay inputs from archived files."""

    def __init__(self, archive_dir: str):
        self.archive_dir = Path(archive_dir)

    def _get_files(self, start_ts_ms: int | None = None, end_ts_ms: int | None = None) -> list[Path]:
        """Get list of relevant archive files, sorted by time."""
        if not self.archive_dir.exists():
            logger.warning(f"Archive directory {self.archive_dir} does not exist")
            return []

        # Support two naming conventions:
        #   ml_replay_inputs_v1_YYYYMMDD_HHMMSS.ndjson.gz  (chunked archiver)
        #   YYYY-MM-DD.ndjson                               (daily archiver)
        files = sorted(
            list(self.archive_dir.glob("ml_replay_inputs_v1_*.ndjson*"))
            + list(self.archive_dir.glob("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9].ndjson*"))
        )

        if not start_ts_ms and not end_ts_ms:
            return files

        relevant_files = []
        for f in files:
            try:
                name = f.name
                if name[:4].isdigit() and name[4] == '-':
                    # YYYY-MM-DD.ndjson — file covers the whole day; include if day overlaps range
                    file_dt = datetime.strptime(name[:10], "%Y-%m-%d").replace(tzinfo=UTC)
                    day_start_ms = int(file_dt.timestamp() * 1000)
                    day_end_ms = day_start_ms + 86_400_000
                    if start_ts_ms and day_end_ms < start_ts_ms:
                        continue
                    if end_ts_ms and day_start_ms > end_ts_ms:
                        continue
                else:
                    parts = name.split('_')
                    if len(parts) >= 6:
                        ts_str = parts[4] + "_" + parts[5].split('.')[0]
                        file_dt = datetime.strptime(ts_str, "%Y%m%d_%H%M%S").replace(tzinfo=UTC)
                        file_ts = int(file_dt.timestamp() * 1000)
                        if end_ts_ms and file_ts > end_ts_ms:
                            continue
                relevant_files.append(f)
            except Exception as e:
                logger.warning(f"Failed to parse timestamp from filename {f.name}: {e}")
                relevant_files.append(f)  # fallback: include

        return relevant_files

    def read_records(self, start_ts_ms: int | None = None, end_ts_ms: int | None = None) -> Generator[dict[str, Any], None, None]:
        """Stream records from archives within time range."""
        files = self._get_files(start_ts_ms, end_ts_ms)

        for file_path in files:
            logger.info(f"Reading from {file_path}")
            opener = gzip.open if file_path.suffix == '.gz' else open
            try:
                with opener(file_path, 'rt') as f:
                    for line in f:
                        if not line.strip():
                            continue
                        try:
                            record = json.loads(line)
                            ts = record.get('ts_ms') or record.get('ts')
                            if not ts:
                                yield record
                                continue

                            # Filter by time if requested
                            if start_ts_ms and ts < start_ts_ms:
                                continue
                            if end_ts_ms and ts > end_ts_ms:
                                break # Assuming chronological order in file

                            yield record
                        except json.JSONDecodeError:
                            logger.error(f"Malformed JSON in {file_path}: {line[:100]}...")
            except Exception as e:
                logger.error(f"Error reading {file_path}: {e}")
