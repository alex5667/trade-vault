import os
import shutil

p_file = "monitoring/prometheus/prometheus.yml"
with open(p_file, "r") as f:
    p_content = f.read()

if "scanner-vertex-local-fallback-handoff-v3-1:9917" not in p_content:
    append_str = """
  - job_name: 'ml_phase3_v1_vertex_local_handoff'
    scrape_interval: 15s
    static_configs:
      - targets:
          - 'scanner-vertex-local-fallback-handoff-v3-1:9917'
"""
    with open(p_file, "a") as f:
        f.write(append_str)
        print("Updated prometheus.yml")

dc_file = "docker-compose-timers.yml"
with open(dc_file, "r") as f:
    dc_content = f.read()

frag1 = "orderflow_services/docker_compose_fragment_ml_phase3_local_fallback_plane_v1.yml"
frag2 = "orderflow_services/docker_compose_fragment_ml_phase3_1_vertex_local_handoff_v1.yml"

added = False
with open(dc_file, "a") as f:
    if "scanner-local-fallback-plane-v3-0" not in dc_content:
        with open(frag1, "r") as ff1:
            lines = ff1.readlines()
            for line in lines:
                if not line.startswith("services:"):
                    f.write(line)
        added = True
    if "scanner-vertex-local-fallback-handoff-v3-1" not in dc_content:
        with open(frag2, "r") as ff2:
            lines = ff2.readlines()
            for line in lines:
                if not line.startswith("services:"):
                    f.write(line)
        added = True

if added:
    print("Updated docker-compose-timers.yml")

os.makedirs("monitoring/prometheus/rules/orderflow_services", exist_ok=True)
shutil.copy("orderflow_services/prometheus_alerts_ml_phase3_local_fallback_plane_v1.yml", "monitoring/prometheus/rules/orderflow_services/")
shutil.copy("orderflow_services/prometheus_alerts_ml_phase3_1_vertex_local_handoff_v1.yml", "monitoring/prometheus/rules/orderflow_services/")
print("Copied Prometheus alerts.")
