import logging
import types
from typing import Any

from core.ndjson_utils import read_concatenated_json
from core.of_confirm_engine import OFConfirmEngine
from utils.time_utils import get_ny_time_millis

logger = logging.getLogger(__name__)

class GoldenReplayRunner:
    def __init__(self):
        self.engine = OFConfirmEngine()

    def _make_runtime_snapshot(self, inp: dict[str, Any]) -> types.SimpleNamespace:
        """
        Reconstructs a runtime-like object from input snapshot.
        Fail-open: missing fields become None or defaults.
        """
        rt = types.SimpleNamespace()

        # Helper to convert dict to SimpleNamespace recursively if needed,
        # but for now we just stick to what Engine needs (dicts or specific objects).

        # Events
        rt.last_obi_event = inp.get("last_obi_event")
        rt.last_iceberg_event = inp.get("last_iceberg_event")
        rt.last_ofi_event = inp.get("last_ofi_event")
        rt.last_fp_edge = self._to_obj(inp.get("last_fp_edge")) # engine expects obj or dict, mostly dict logic adapted now but checks attributes

        # Complex objects needing attributes
        rt.last_sweep = self._to_obj(inp.get("last_sweep"))
        rt.last_reclaim = self._to_obj(inp.get("last_reclaim"))
        rt.last_wp = self._to_obj(inp.get("last_wp"), default_attrs={"weak_any": False})
        rt.last_bar = self._to_obj(inp.get("last_bar"))
        rt.last_div = self._to_obj(inp.get("last_div"))

        # Configs
        rt.dynamic_cfg = inp.get("dynamic_cfg", {}) or {}
        rt.last_regime = inp.get("last_regime", "na")

        # Pressure / Churn stub
        # Engine calls getattr(runtime, "pressure").is_pressure_hi(...)
        # We need a stub for pressure
        p_data = inp.get("pressure_state", {})
        class PressureStub:
            def is_pressure_hi(self, *args, **kwargs):
                return bool(p_data.get("is_pressure_hi", False))
        rt.pressure = PressureStub()

        rt.book_churn_hi = inp.get("book_churn_hi", 0)
        rt.cont_ctx_ts_ms = inp.get("cont_ctx_ts_ms", 0)

        return rt

    def _to_obj(self, data: Any, default_attrs: dict[str, Any] | None = None) -> Any:
        if data is None:
            return None
        if isinstance(data, dict):
            # Create object with attributes from dict
            obj = types.SimpleNamespace(**data)
            if default_attrs:
                for k, v in default_attrs.items():
                    if not hasattr(obj, k):
                        setattr(obj, k, v)
            return obj
        return data

    def run_case(self, case: dict[str, Any]) -> dict[str, Any]:
        """
        Runs a single replay case.
        input: case dict with "inputs" block (symbol, tf, indicators, etc.)
        output: result dict with "result" block (of_confirm)
        """
        inputs = case.get("inputs", {})
        if not inputs:
            return {"error": "no_inputs"}

        symbol = inputs.get("symbol", "TEST")
        tf = inputs.get("tf", "1m")
        direction = inputs.get("direction", "LONG")
        tick_ts_ms = inputs.get("tick_ts_ms", get_ny_time_millis())
        price = inputs.get("price", 100.0)
        delta_z = inputs.get("delta_z", 0.0)

        runtime = self._make_runtime_snapshot(inputs.get("runtime_snapshot", {}))
        cfg = inputs.get("cfg", {})
        indicators = inputs.get("indicators", {})
        absorption = inputs.get("absorption")

        try:
            ofc, dec = self.engine.build(
                symbol=symbol,
                tf=tf,
                direction=direction,
                tick_ts_ms=tick_ts_ms,
                price=price,
                delta_z=delta_z,
                runtime=runtime,
                cfg=cfg,
                indicators=indicators,
                absorption=absorption
            )

            # Serialize result
            res = ofc.to_dict()
            return {
                "case_id": case.get("id"),
                "inputs_summary": f"{symbol} {direction} {tf}",
                "result": res,
                "indicators_update": indicators # snapshot of updated indicators
            }
        except Exception as e:
            logger.exception(f"Replay failed for case {case.get('id')}")
            return {"case_id": case.get("id"), "error": str(e)}

    def run_file(self, path: str) -> list[dict[str, Any]]:
        with open(path, encoding='utf-8') as f:
            content = f.read()

        results = []
        for case in read_concatenated_json(content):
            if not isinstance(case, dict):
                continue
            # Assume standard format: { "id":..., "kind": "OF_INPUTS_V1", "inputs": {...}, "expect": {...} }
            if case.get("kind") != "OF_INPUTS_V1":
                continue

            res = self.run_case(case)

            # Compare with expect if present
            expect = case.get("expect")
            if expect:
                mismatch = []
                actual = res.get("result", {})

                # Check key fields
                for k in ["ok", "score", "scenario", "have", "need", "reason"]:
                    e_val = expect.get(k)
                    a_val = actual.get(k)
                    # loose comparison
                    if k == "score":
                        if abs(float(e_val or 0) - float(a_val or 0)) > 1e-4:
                            mismatch.append(f"score {e_val}!={a_val}")
                    elif str(e_val) != str(a_val):
                         mismatch.append(f"{k} {e_val}!={a_val}")

                res["mismatch"] = mismatch
                res["pass"] = len(mismatch) == 0

            results.append(res)

        return results





















