import os
import glob
import re

def patch_file(path, src_label):
    if "aiogram" in path: return
    with open(path, "r") as f:
        content = f.read()

    if "enrich_schema_fields" in content:
        return

    # Add imports
    content = content.replace("import json", "import json\nfrom common.time_utils import normalize_epoch_ms\nfrom common.of_gate_metrics_contract import enrich_schema_fields")
            
    # Add metrics import
    content = re.sub(
        r"(atr_gate_veto_total, tp1_net_margin_bps_gauge, tp1_zero_pnl_total, signals_total,?)",
        r"\1\n    ok_metrics_emitted_total, ok_metrics_skipped_total, ok_metrics_error_total,",
        content
    )

    # Replace ts_ms
    content = content.replace('"ts_ms": str(int(tick_ts)),', '"ts_ms": str(normalize_epoch_ms(tick_ts).ts_ms),')

    # Replace emission chunk
    # Usually it looks like:
    # asyncio.create_task(self.redis.xadd(
    #     OF_GATE_METRICS_STREAM,
    #     payload,
    #     maxlen=OF_GATE_METRICS_MAXLEN,
    #     approximate=True,
    # ))
    pattern_xadd = r"asyncio\.create_task\(self\.redis\.xadd\(\s*OF_GATE_METRICS_STREAM,\s*payload,\s*maxlen=OF_GATE_METRICS_MAXLEN,\s*approximate=True,?\s*\)\)"
    
    emission_new = f'''payload = enrich_schema_fields(payload)
                            async def _emit_ok_metrics(_payload: dict) -> None:
                                try:
                                    await self.redis.xadd(
                                        OF_GATE_METRICS_STREAM,
                                        {{k: str(v) for k, v in _payload.items()}},
                                        maxlen=OF_GATE_METRICS_MAXLEN,
                                        approximate=True,
                                    )
                                    ok_metrics_emitted_total.labels("{src_label}").inc()
                                except Exception:
                                    ok_metrics_error_total.labels("{src_label}", "xadd").inc()

                            asyncio.create_task(_emit_ok_metrics(payload))'''
    
    if re.search(pattern_xadd, content):
        content = re.sub(pattern_xadd, emission_new, content)
        with open(path, "w") as f:
            f.write(content)
        print(f"Patched {path}")
    else:
        print(f"Could not find xadd pattern in {path}")

for pattern in ["/home/alex/front/trade/scanner_infra/python-worker/**/strategy.py"]:
    for path in glob.glob(pattern, recursive=True):
        patch_file(path, "strategy")

for pattern in ["/home/alex/front/trade/scanner_infra/python-worker/**/orderflow_strategy.py"]:
    for path in glob.glob(pattern, recursive=True):
        patch_file(path, "orderflow_strategy")

# And tick processor
for pattern in ["/home/alex/front/trade/scanner_infra/python-worker/**/tick_processor.py"]:
    for path in glob.glob(pattern, recursive=True):
        if "aiogram" in path: continue
        with open(path, "r") as f:
            content = f.read()
        if "enrich_schema_fields" in content: continue

        content = content.replace("import json", "import json\nfrom common.time_utils import normalize_epoch_ms\nfrom common.of_gate_metrics_contract import enrich_schema_fields")
        if "from services.orderflow.metrics import" in content:
             content = content.replace("from services.orderflow.metrics import (", "from services.orderflow.metrics import (\n    ok_metrics_emitted_total, ok_metrics_skipped_total, ok_metrics_error_total,")
        else:
             content = content.replace("import asyncio", "import asyncio\nfrom services.orderflow.metrics import ok_metrics_emitted_total, ok_metrics_skipped_total, ok_metrics_error_total")

        content = content.replace('"ts_ms": str(int(tick_ts)),', '"ts_ms": str(normalize_epoch_ms(tick_ts).ts_ms),')
        
        pattern_xadd = r"asyncio\.create_task\(self\.redis\.xadd\(\s*OF_GATE_METRICS_STREAM,\s*payload,\s*maxlen=OF_GATE_METRICS_MAXLEN,\s*approximate=True,?\s*\)\)"
        emission_new = f'''payload = enrich_schema_fields(payload)
                    async def _emit_ok_metrics(_payload: dict) -> None:
                        try:
                            await self.redis.xadd(
                                OF_GATE_METRICS_STREAM,
                                {{k: str(v) for k, v in _payload.items()}},
                                maxlen=OF_GATE_METRICS_MAXLEN,
                                approximate=True,
                            )
                            ok_metrics_emitted_total.labels("tick").inc()
                        except Exception:
                            ok_metrics_error_total.labels("tick", "xadd").inc()
                    asyncio.create_task(_emit_ok_metrics(payload))'''
        content = re.sub(pattern_xadd, emission_new, content)
        
        # Sampling code
        # if not _should_sample(...): return
        content = re.sub(r"(if not self\._should_sample_tick\([^)]+\):\s*return)", r"\1 # skip\n        ok_metrics_skipped_total.labels('tick', 'sample').inc()", content)

        with open(path, "w") as f:
            f.write(content)
        print(f"Patched {path}")
