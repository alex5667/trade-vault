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

    if "ok_metrics_emitted_total" not in content:
        # Just append it to the end of the file. No need for complex regex.
        with open(path, "a") as f:
            f.write("\n" + METRICS_CODE + "\n")
        print(f"Appended to {path}")
