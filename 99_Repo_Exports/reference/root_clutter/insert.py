import os

dc_file = "docker-compose-timers.yml"
frag1 = "orderflow_services/docker_compose_fragment_ml_phase3_local_fallback_plane_v1.yml"
frag2 = "orderflow_services/docker_compose_fragment_ml_phase3_1_vertex_local_handoff_v1.yml"

with open(dc_file, "r") as f:
    lines = f.readlines()

insert_index = len(lines)
for i, line in enumerate(lines):
    if line.startswith("volumes:") or line.startswith("networks:"):
        insert_index = i
        break

with open(frag1, "r") as f:
    frag1_lines = [l for l in f.readlines() if not l.startswith("services:")]

with open(frag2, "r") as f:
    frag2_lines = [l for l in f.readlines() if not l.startswith("services:")]

new_lines = lines[:insert_index] + ["\n"] + frag1_lines + ["\n"] + frag2_lines + ["\n"] + lines[insert_index:]

with open(dc_file, "w") as f:
    f.writelines(new_lines)

print("Insertion successful")
