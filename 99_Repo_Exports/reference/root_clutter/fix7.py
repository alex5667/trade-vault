with open("docker-compose-timers.yml", "r") as f:
    lines = f.readlines()

out = []
for i, line in enumerate(lines):
    # The actual string is "networks:\n  scanner-network:\n    external: true\n"
    # We want to insert just before "networks:\n" if it's at the root level.
    # We can identify it because it's not indented.
    if line == "networks:\n" and lines[i+1] == "  scanner-network:\n":
        out.append("  # P96 DLQ Exporter\n")
        out.append("  of-inputs-dlq-exporter:\n")
        out.append("    build:\n")
        out.append("      context: .\n")
        out.append("      dockerfile: python-worker/Dockerfile.gpu\n")
        out.append("    container_name: scanner-of-inputs-dlq-exporter\n")
        out.append("    restart: unless-stopped\n")
        out.append("    environment:\n")
        out.append("      - REDIS_URL=redis://redis-worker-1:6379/0\n")
        out.append("      - OF_INPUTS_DLQ_EXPORTER_PORT=9158\n")
        out.append("      - PYTHONPATH=.:/app\n")
        out.append("    working_dir: /app\n")
        out.append("    ports:\n")
        out.append("      - \"9158:9158\"\n")
        out.append("    volumes:\n")
        out.append("      - ./python-worker:/app\n")
        out.append("    command: [\"python3\", \"-m\", \"orderflow_services.of_inputs_dlq_exporter_v1\"]\n")
        out.append("    networks:\n")
        out.append("      - scanner-network\n\n")
    out.append(line)

with open("docker-compose-timers.yml", "w") as f:
    f.writelines(out)
