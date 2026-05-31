from __future__ import annotations

from orderflow_services.ofc_contextual_exporter_v1 import Exporter


class FakeRedis:
    def __init__(self, data):
        self.data = data

    def hgetall(self, key):
        return self.data.get(key, {})


def test_exporter_tick_reads_writer_and_ops_hashes(monkeypatch):
    ex = Exporter()
    ex.r = FakeRedis(
        {
            ex.writer_key: {
                b"last_run_ts_ms": b"1700000000000",
                b"written_total": b"12",
                b"db_fail_total": b"1",
                b"pending_count": b"5",
                b"last_ok": b"1",
                b"last_batch_rows": b"7",
            },
            ex.ops_key: {
                b"last_run_ts_ms": b"1700003600000",
                b"last_ok": b"1",
                b"last_exit_code": b"0",
                b"bundle_created_ts_ms": b"1700003600000",
            },
        }
    )
    ex.tick()
