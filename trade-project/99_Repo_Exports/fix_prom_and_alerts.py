import os
import re

# Fix Prom files
for yaml_file in [
    "prometheus.yml", 
    "monitoring/prometheus/prometheus.yml", 
    "monitoring/prometheus/prometheus.yml.tmpl"
]:
    if not os.path.exists(yaml_file): continue
    with open(yaml_file, "r") as f:
        content = f.read()
    
    # regex to replace targets under job_name: 'redis-workers' inside static_configs
    # We will just rewrite the whole redis-workers block if we can find it
    block_pattern = r"(job_name:\s*'redis-workers'[\s\S]*?static_configs:\s*\n)([\s\S]*?)(?=\s+# Python Worker|\s+- job_name:)"

    new_targets = """      - targets: ['redis-exporter-main:9121']
        labels:
          instance: 'redis'
      - targets: ['redis-exporter-worker-1:9121']
        labels:
          instance: 'redis-worker-1'
      - targets: ['redis-exporter-worker-2:9121']
        labels:
          instance: 'redis-worker-2'
      - targets: ['redis-exporter-worker-1b:9121']
        labels:
          instance: 'redis-worker-1b'
      - targets: ['redis-exporter-worker-2b:9121']
        labels:
          instance: 'redis-worker-2b'
      - targets: ['redis-exporter-ticks:9121']
        labels:
          instance: 'redis-ticks'
"""
    new_content = re.sub(block_pattern, r"\1" + new_targets, content)
    with open(yaml_file, "w") as f:
        f.write(new_content)
    print("Fixed", yaml_file)

# Add Redis Main Exporter to docker-compose-infrastructure.yml
compose_file = "docker-compose-infrastructure.yml"
with open(compose_file, "r") as f:
    cc = f.read()
if "redis-exporter-main:" not in cc and "redis-exporter-worker-1:" in cc:
    main_exporter = """  redis-exporter-main:
    image: oliver006/redis_exporter:v1.66.0
    container_name: redis-exporter-main
    restart: unless-stopped
    environment:
      - REDIS_ADDR=redis://redis:6379
      - REDIS_EXPORTER_LOG_FORMAT=txt
    expose:
      - "9121"
    depends_on:
      redis:
        condition: service_healthy
    networks:
      - scanner-infra

"""
    cc = cc.replace("  redis-exporter-worker-1:", main_exporter + "  redis-exporter-worker-1:")
    with open(compose_file, "w") as f:
        f.write(cc)
    print("Fixed docker-compose-infrastructure.yml")

