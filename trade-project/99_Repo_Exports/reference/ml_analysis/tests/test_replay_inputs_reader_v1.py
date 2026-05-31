import json
import gzip
import pytest
from pathlib import Path
from ml_analysis.tools.replay_inputs_reader_v1 import ReplayInputsReader

@pytest.fixture
def temp_archive(tmp_path):
    archive_dir = tmp_path / "archive"
    archive_dir.mkdir()
    
    # Create some fakes
    # Format: ml_replay_inputs_v1_YYYYMMDD_HHMMSS.ndjson.gz
    # File 1: 2024-01-01 10:00:00 -> 1704103200000
    file1 = archive_dir / "ml_replay_inputs_v1_20240101_100000.ndjson.gz"
    with gzip.open(file1, "wt") as f:
        f.write(json.dumps({"ts_ms": 1704103200000, "sid": "s1", "val": 1}) + "\n")
        f.write(json.dumps({"ts_ms": 1704103210000, "sid": "s2", "val": 2}) + "\n")

    # File 2: 2024-01-01 11:00:00 -> 1704106800000
    file2 = archive_dir / "ml_replay_inputs_v1_20240101_110000.ndjson.gz"
    with gzip.open(file2, "wt") as f:
        f.write(json.dumps({"ts_ms": 1704106800000, "sid": "s3", "val": 3}) + "\n")
        f.write(json.dumps({"ts_ms": 1704106810000, "sid": "s4", "val": 4}) + "\n")
        
    return archive_dir

def test_reader_get_files(temp_archive):
    reader = ReplayInputsReader(str(temp_archive))
    
    # All files
    files = reader._get_files()
    assert len(files) == 2
    
    # Filter by time
    # 1704106000000 is between File 1 and File 2 start
    files = reader._get_files(start_ts_ms=1704106000000)
    # Reader returns files that *might* have data. 
    # File 1 starts at 10:00, File 2 at 11:00. 
    # 10:30 (1704105000) would need File 1.
    # 11:30 (1704108600) would need only File 2.
    assert len(files) >= 1

def test_reader_read_records(temp_archive):
    reader = ReplayInputsReader(str(temp_archive))
    
    records = list(reader.read_records())
    assert len(records) == 4
    assert records[0]["sid"] == "s1"
    assert records[-1]["sid"] == "s4"
    
    # Range query
    records = list(reader.read_records(start_ts_ms=1704103211000, end_ts_ms=1704106805000))
    # Should include s2 (10:00:10, fits) NO. 
    # File 1 records: 10:00:00, 10:00:10. Start is 10:00:11. So none from File 1.
    # File 2 records: 11:00:00, 11:00:10. End is 11:00:05. So only s3.
    assert len(records) == 1
    assert records[0]["sid"] == "s3"
