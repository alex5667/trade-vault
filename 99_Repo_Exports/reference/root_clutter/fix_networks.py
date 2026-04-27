with open("docker-compose-python-workers.yml", "r") as f:
    text = f.read()

target = "    depends_on: [redis-worker-1]\n"
replacement = "    depends_on: [redis-worker-1]\n    networks:\n    - scanner-core\n    - scanner-infra\n"
if "scanner-apply-flow-experiment-incident-rca-bridge-v3-53:\n" in text:
    # Splitting the file to only target the newly added service block.
    parts = text.split("  scanner-apply-flow-experiment-incident-rca-bridge-v3-53:\n")
    if len(parts) == 2:
        parts[1] = parts[1].replace(target, replacement, 1)
        with open("docker-compose-python-workers.yml", "w") as f:
            f.write("  scanner-apply-flow-experiment-incident-rca-bridge-v3-53:\n".join(parts))
        print("Networks added.")
