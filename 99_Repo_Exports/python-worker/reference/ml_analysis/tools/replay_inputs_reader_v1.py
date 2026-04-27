import json
import gzip
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional, List

logger = logging.getLogger(__name__)

class ReplayInputsReader:
    """Utility to read replay inputs from archived files."""

    def __init__(self, archive_dir: str):
        self.archive_dir = Path(archive_dir)

    def _get_files(self, start_ts_ms: Optional[int] = None, end_ts_ms: Optional[int] = None) -> List[Path]:
        """Get list of relevant archive files, sorted by time."""
        if not self.archive_dir.exists():
            logger.warning(f"Archive directory {self.archive_dir} does not exist")
            return []

        # Assuming files are named like ml_replay_inputs_v1_YYYYMMDD_HHMMSS.ndjson.gz
        files = list(self.archive_dir.glob("ml_replay_inputs_v1_*.ndjson*"))
        files.sort()

        if not start_ts_ms and not end_ts_ms:
            return files

        relevant_files = []
        for f in files:
            # We use modification time or filename to approximate range
            # For robustness, we check the first record if possible, but here we just use name
            # filename format: ..._YYYYMMDD_HHMMSS.ndjson.gz
            try:
                parts = f.name.split('_')
                if len(parts) >= 6:
                    ts_str = parts[4] + "_" + parts[5].split('.')[0]
                    file_dt = datetime.strptime(ts_str, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
                    file_ts = int(file_dt.timestamp() * 1000)
                    
                    # If file_ts is after end_ts, and assuming files are discrete chunks,
                    # we might need the previous file too. But usually we keep a bit of overlap.
                    if end_ts_ms and file_ts > end_ts_ms:
                        continue
                    relevant_files.append(f)
            except Exception as e:
                logger.warning(f"Failed to parse timestamp from filename {f.name}: {e}")
                relevant_files.append(f) # Fallback to include it

        return relevant_files

    def read_records(self, start_ts_ms: Optional[int] = None, end_ts_ms: Optional[int] = None) -> Generator[Dict[str, Any], None, None]:
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
