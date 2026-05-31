import os

with open("docker-compose-python-workers.yml", "r") as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    # For RCA series, if it starts with "      REDIS_URL:" we change it
    if line.startswith("      REDIS_URL: "):
        lines[i] = line.replace("      REDIS_URL: ", "      - REDIS_URL=")
    elif line.startswith("      DATABASE_URL: "):
        lines[i] = line.replace("      DATABASE_URL: ", "      - DATABASE_URL=")
    elif line.startswith("      ML_ROUTE_INCIDENT_RCA_"):
        # e.g. "      ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_SLO_PORT: ..."
        # we want to replace the first ': ' with '='
        if ": " in line:
            parts = line.split(": ", 1)
            lines[i] = parts[0][:6] + "- " + parts[0][6:] + "=" + parts[1]

with open("docker-compose-python-workers.yml", "w") as f:
    f.writelines(lines)
