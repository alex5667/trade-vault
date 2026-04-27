from ruamel.yaml import YAML
import sys

yaml = YAML()
yaml.preserve_quotes = True
with open("docker-compose-python-workers.yml") as f:
    data = yaml.load(f)

for service_name, service in data.get("services", {}).items():
    if "environment" in service:
        env = service["environment"]
        if isinstance(env, list):
            found = False
            for item in env:
                if isinstance(item, str) and item.startswith("OUTBOX_CONTRACT_MODE="):
                    found = True
                    break
            if not found:
                env.append("OUTBOX_CONTRACT_MODE=warn")
        elif isinstance(env, dict):
            if "OUTBOX_CONTRACT_MODE" not in env:
                env["OUTBOX_CONTRACT_MODE"] = "warn"

with open("docker-compose-python-workers.yml", "w") as f:
    yaml.dump(data, f)
