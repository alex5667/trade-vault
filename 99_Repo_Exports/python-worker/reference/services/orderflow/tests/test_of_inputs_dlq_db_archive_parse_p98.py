import unittest


class TestOFInputsDLQDBArchiveParseP98(unittest.TestCase):
    def test_parse_dlq_payload_json(self):
        from orderflow_services.of_inputs_dlq_archive_to_db_p98 import parse_event

        stream = "stream:dlq:of_inputs"
        mid = "1700000000000-1"
        fields = {
            b"stream": b"signals:of:inputs"
            b"stream_id": b"1700000000000-9"
            b"err": b"missing required field: microprice"
            b"payload": b"{\"schema_version\":3,\"ts_ms\":1700000000000,\"symbol\":\"BTCUSDT\",\"dq_code\":\"missing_lob_fields\",\"missing_fields\":[\"microprice\",\"spread_bps\"]}"
        }

        row, last_id = parse_event(stream, mid, fields)
        self.assertEqual(last_id, mid)
        self.assertEqual(row[0], stream)
        self.assertEqual(row[1], mid)
        self.assertEqual(row[2], 1700000000000)
        self.assertEqual(row[7], "missing_lob_fields")
        self.assertIn("microprice", row[10])

    def test_parse_quarantine_fields_as_payload(self):
        from orderflow_services.of_inputs_dlq_archive_to_db_p98 import parse_event

        stream = "quarantine:signals:of:inputs"
        mid = "1700000001000-1"
        fields = {
            b"dq_code": b"book_state_degraded"
            b"attempt_version": b"3"
            b"published_version": b"2"
            b"missing_fields": b"microprice,obi_l1"
            b"ts_ms": b"1700000001000"
            b"symbol": b"ETHUSDT"
        }

        row, _ = parse_event(stream, mid, fields)
        self.assertEqual(row[0], stream)
        self.assertEqual(row[2], 1700000001000)
        self.assertEqual(row[7], "book_state_degraded")
        self.assertEqual(row[8], 3)
        self.assertEqual(row[9], 2)
        self.assertIn("microprice", row[10])


if __name__ == "__main__":
    unittest.main()
