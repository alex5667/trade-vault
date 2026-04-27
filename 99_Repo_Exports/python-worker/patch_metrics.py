import glob

METRICS_CODE = """
# --- OK/OF-gate metrics emission health (telemetry about telemetry) ---
ok_metrics_emitted_total = _get_or_create_prom_counter(
    "ok_metrics_emitted_total",
    "Total decision/ok metric rows emitted to Redis streams",
    ["src"],
)
ok_metrics_skipped_total = _get_or_create_prom_counter(
    "ok_metrics_skipped_total",
    "Total decision/ok metric rows skipped (sampling/disabled/invalid)",
    ["src", "why"],
)
ok_metrics_error_total = _get_or_create_prom_counter(
    "ok_metrics_error_total",
    "Total decision/ok metric emission errors",
    ["src", "where"],
)
"""

for path in glob.glob("/home/alex/front/trade/scanner_infra/python-worker/**/metrics.py", recursive=True):
    with open(path, "r") as f:
        content = f.read()
    if "ok_metrics_emitted_total" not in content and "fp_buckets_evicted_total" in content:
        idx = content.find("fp_buckets_evicted_total")
        if idx != -1:
            end_idx = content.find(")", idx) + 1
            new_content = content[:end_idx] + "\n" + METRICS_CODE + content[end_idx:]
            with open(path, "w") as f:
                f.write(new_content)
            print(f"Patched {path}")
