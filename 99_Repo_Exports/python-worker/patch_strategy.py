import os
from utils.task_manager import safe_create_task

from utils.task_manager import safe_create_task

import glob
import re

def patch_strategy_files():
    # 1. strategy.py and orderflow_strategy.py
    for pattern in ["/home/alex/front/trade/scanner_infra/python-worker/**/strategy.py",
                    "/home/alex/front/trade/scanner_infra/python-worker/**/orderflow_strategy.py"]:
        for path in glob.glob(pattern, recursive=True):
            if "aiogram" in path: continue
            with open(path, "r") as f:
                content = f.read()

            if "enrich_schema_fields" in content:
                continue

            # Identify if it's orderflow_strategy.py or strategy.py
            labels_src = "orderflow_strategy" if "orderflow_strategy" in path else "strategy"

            # Add imports
            new_content = content.replace("import json", "import json\nfrom common.time_utils import normalize_epoch_ms\nfrom common.of_gate_metrics_contract import enrich_schema_fields")
            
            # Add metrics import
            new_content = re.sub(
                r"(atr_gate_veto_total, tp1_net_margin_bps_gauge, tp1_zero_pnl_total, signals_total,?)",
                r"\1\n    ok_metrics_emitted_total, ok_metrics_skipped_total, ok_metrics_error_total,",
                new_content
            )

            # Replace ts_ms
            new_content = new_content.replace('"ts_ms": str(int(tick_ts)),', '"ts_ms": str(normalize_epoch_ms(tick_ts).ts_ms),')

            # Replace emission chunk
            emission_old = '''                            safe_create_task(self.redis.xadd(
                                OF_GATE_METRICS_STREAM,
                                payload,
                                maxlen=OF_GATE_METRICS_MAXLEN,
                                approximate=True,
                            ))'''
                            
            emission_new = f'''                            payload = enrich_schema_fields(payload)
                            async def _emit_ok_metrics(_payload: dict) -> None:
                                try:
                                    await self.redis.xadd(
                                        OF_GATE_METRICS_STREAM,
                                        {{k: str(v) for k, v in _payload.items()}},
                                        maxlen=OF_GATE_METRICS_MAXLEN,
                                        approximate=True,
                                    )
                                    ok_metrics_emitted_total.labels("{labels_src}").inc()
                                except Exception:
                                    ok_metrics_error_total.labels("{labels_src}", "xadd").inc()

                            safe_create_task(_emit_ok_metrics(payload))'''
            
            # Use regex for emission match due to indentations potentially varying slightly
            # We look for payload dictionary end, then asyncio.create_task
            
            # But wait, looking at the exact text replace might fail if spacing is different.
            # Let's use re.sub cautiously.
            pattern_xadd = r"asyncio\.create_task\(self\.redis\.xadd\(\s*OF_GATE_METRICS_STREAM,\s*payload,\s*maxlen=OF_GATE_METRICS_MAXLEN,\s*approximate=True,?\s*\)\)"
            
            if re.search(pattern_xadd, new_content):
                new_content = re.sub(pattern_xadd, emission_new, new_content)
                with open(path, "w") as f:
                    f.write(new_content)
                print(f"Patched {path}")

patch_strategy_files()
