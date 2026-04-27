import os
import math
import random
import datetime as dt
from decimal import Decimal

import pytest

from common.json_safe import to_json_safe
from common.payload_policy import enforce_and_validate_payload, validate_tradeable_signal_payload, payload_max_bytes
from common.outbox_contract import assert_json_safe


class WeirdObj:
    def __init__(self, x):
        self.x = x
    def __str__(self):
        return f"WeirdObj({self.x})"


def gen_weird(depth: int = 4):
    if depth <= 0:
        choices = [
            None,
            True,
            False,
            0,
            1,
            -1,
            1.25,
            float("nan"),
            float("inf"),
            "ok",
            "x" * 5000,
            b"\xff\x00\x01",
            Decimal("1.2345"),
            dt.datetime.utcnow(),
            set([1, 2, 3]),
            tuple([1, 2]),
            WeirdObj("z"),
        ]
        return random.choice(choices)
    t = random.choice(["dict", "list", "scalar"])
    if t == "list":
        return [gen_weird(depth - 1) for _ in range(random.randint(0, 20))]
    if t == "dict":
        d = {}
        for i in range(random.randint(0, 20)):
            d[f"k{i}"] = gen_weird(depth - 1)
        # inject forbidden-ish keys sometimes
        if random.random() < 0.2:
            d["trace"] = {"events": [1, 2, 3]}
        if random.random() < 0.2:
            d["parts_full"] = {"big": "x" * 2000}
        return d
    return gen_weird(0)


def test_fuzz_to_json_safe_is_json_safe():
    random.seed(1337)
    for _ in range(200):
        obj = gen_weird(4)
        js = to_json_safe(obj)
        assert_json_safe(js, path="$")


def test_fuzz_payload_policy_keeps_budget_and_schema():
    os.environ["PAYLOAD_POLICY_MODE"] = "raise"
    os.environ["PAYLOAD_MAX_BYTES"] = "2048"
    os.environ["PAYLOAD_MAX_STRLEN"] = "256"

    random.seed(2025)
    for i in range(200):
        # Ensure parts is always a dict for validation
        parts_dict = gen_weird(3)
        if not isinstance(parts_dict, dict):
            parts_dict = {"weird": parts_dict}

        payload = {
            "sid": f"S{i}",
            "signal_id": f"S{i}",
            "kind": "breakout",
            "side": random.choice(["LONG", "SHORT"]),
            "symbol": "BTCUSDT",
            "ts": 1700000000000 + i,
            "price": 100.0,
            "confidence": 75.0,
            "conf_factor": 0.9,
            "raw_score": 1.0,
            "final_score": 0.9,
            "reasons": ["ok", "x" * 5000],  # intentionally huge
            "parts": parts_dict,          # intentionally dirty/heavy
        }
        meta = {"parts_full": gen_weird(3)}
        p2, m2 = enforce_and_validate_payload(payload=payload, payload_meta=meta, logger=None, where="fuzz")
        validate_tradeable_signal_payload(p2)
        # budget
        b = len(__import__("common.json_fast").json_fast.dumps1(p2).encode("utf-8", "ignore"))
        assert b <= payload_max_bytes()
