import re

with open('docker-compose-timers.yml', 'r') as f:
    text = f.read()

# The previously appended text starts with "  scanner-local-fallback-plane-v3-0:"
# I need to find the top-level "networks:" block. Usually looks like:
# networks:
#   scanner-timers: ...

# Let's clean up the poorly appended stuff.
cleanup = text.split("  scanner-local-fallback-plane-v3-0:")
cleaned_text = cleanup[0]

# Now, cleaned_text probably ends with the networks section.
# We will insert the new services BEFORE the "networks:" section if it exists at the root,
# or just append if "services:" is the only root block.

frag1 = "orderflow_services/docker_compose_fragment_ml_phase3_local_fallback_plane_v1.yml"
frag2 = "orderflow_services/docker_compose_fragment_ml_phase3_1_vertex_local_handoff_v1.yml"

with open(frag1, "r") as f:
    lines1 = f.readlines()
    frag_1_text = "".join([l for l in lines1 if not l.startswith("services:")])

with open(frag2, "r") as f:
    lines2 = f.readlines()
    frag_2_text = "".join([l for l in lines2 if not l.startswith("services:")])

# Insert right before "^networks:"
import re
new_text = re.sub(r'(?m)^networks:', frag_1_text + frag_2_text + "\nnetworks:", cleaned_text)

with open('docker-compose-timers.yml', 'w') as f:
    f.write(new_text)

print("Fixed docker-compose-timers.yml")
