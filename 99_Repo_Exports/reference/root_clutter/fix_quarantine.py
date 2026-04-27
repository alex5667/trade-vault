import re

files_to_fix = [
    "docker-compose-crypto-orderflow.yml",
    "compose-config.yaml",
    "python-worker/services/archivers/stream_archiver.py",
    "python-worker/services/redis_key_janitor.py",
    "python-worker/services/archivers/sql/20260224_of_gate_metrics_rollups_p2.sql",
    "python-worker/tests/test_of_gate_p3_archiver.py",
    "python-worker/core/redis_keys.py"
]

for file_path in files_to_fix:
    try:
        with open(file_path, "r") as f:
            content = f.read()
        
        # Replace the string occurrences
        new_content = content.replace("quarantine:metrics:of_gate", "quarantined:metrics:of_gate")
        
        # Remove the OF_GATE_DQ_QUARANTINE declaration line from redis_keys.py
        if "redis_keys.py" in file_path:
            lines = new_content.splitlines()
            new_lines = []
            for line in lines:
                if "OF_GATE_DQ_QUARANTINE:" not in line:
                    new_lines.append(line)
            new_content = "\n".join(new_lines) + "\n"
        
        with open(file_path, "w") as f:
            f.write(new_content)
        print(f"Fixed {file_path}")
    except Exception as e:
        print(f"Failed to fix {file_path}: {e}")

