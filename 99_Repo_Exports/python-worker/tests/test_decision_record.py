
import json

from core.decision_record import DecisionRecord
from core.decision_store import DecisionStore
from core.redis_keys import RedisStreams as RS


class TestDecisionRecord:
    def test_serialization_roundtrip(self):
        record = DecisionRecord(
            sid="TEST:LONG:100:123456",
            symbol="TEST",
            ts=123456,
            features={"active_arm": "A", "price": 100.5},
            rule_score=0.95,
            rule_ok=True,
            rule_reasons=["R1", "R2"],
            ml_allow=True,
            ml_prob=0.88,
            dq_state={"tick_age": 50},
            final_permit=True
        )

        # To Redis Dict
        redis_data = record.serialize_for_redis()
        assert redis_data["sid"] == "TEST:LONG:100:123456"
        assert redis_data["ts"] == "123456"
        assert redis_data["rule_ok"] == "true"  # json.dumps(True) -> "true"
        assert "\"active_arm\": \"A\"" in redis_data["features"]

        # From Redis Dict - verification that parse handles "true" correctly
        loaded = DecisionRecord.parse_from_redis(redis_data)
        assert loaded.sid == record.sid
        assert loaded.features == record.features
        assert loaded.rule_reasons == record.rule_reasons
        assert loaded.dq_state == record.dq_state
        assert loaded.rule_score == 0.95
        assert loaded.ml_allow is True

    def test_parse_legacy_or_broken(self):
        # Emulate partial data
        data = {
            "sid": "partial",
            "symbol": "BTC",
            "ts": "99999",
            "rule_score": "invalid_float",  # Should handle gracefully
            "rule_ok": "true"
        }
        record = DecisionRecord.parse_from_redis(data)
        assert record.sid == "partial"
        assert record.rule_score == 0.0  # Default/Fallback
        assert record.rule_ok is True

class TestDecisionStore:
    def test_save_load(self):
        from unittest.mock import MagicMock
        r = MagicMock()
        # Setup load return
        r.get.return_value = json.dumps({
            "sid": "S1", "symbol": "BTC", "ts": 1000,
            "features": {"f1": 1},
            "final_permit": True
        })

        store = DecisionStore(redis_client=r)

        record = DecisionRecord(
            sid="S1", symbol="BTC", ts=1000,
            features={"f1": 1},
            final_permit=True
        )

        store.save_decision(record)

        # Verify canonical JSON key write
        r.set.assert_called_once()
        args, kwargs = r.set.call_args
        assert args[0] == "decision:S1"
        saved = json.loads(args[1])
        assert saved["sid"] == "S1"
        assert kwargs["ex"] == 86400 * 3

        # Load back
        loaded = store.load_decision("S1")
        assert r.get.called
        assert loaded is not None
        assert loaded.sid == "S1"
        assert loaded.final_permit is True
        assert loaded.features == {"f1": 1}

    def test_publish(self):
        from unittest.mock import MagicMock
        r = MagicMock()
        store = DecisionStore(redis_client=r)

        record = DecisionRecord(
            sid="S2", symbol="ETH", ts=2000,
            final_permit=False
        )

        store.publish_decision(record)

        assert r.xadd.called
        args, kwargs = r.xadd.call_args
        assert args[0] == RS.DECISIONS_FINAL
        # payload is typically the second arg
        payload = args[1]
        assert payload["sid"] == "S2"
        assert "payload" in payload
        parsed = json.loads(payload["payload"])
        assert parsed["final_permit"] is False
