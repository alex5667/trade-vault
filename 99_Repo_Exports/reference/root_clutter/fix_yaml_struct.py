import os

with open("docker-compose-python-workers.yml", "r") as f:
    lines = f.readlines()

new_lines = []
network_lines = []
in_networks = False

for line in lines:
    if line.startswith("networks:"):
        in_networks = True
        network_lines.append(line)
        continue
    
    if in_networks:
        if line.startswith("  ") and not line.startswith("    "):
            # This is a ROOT level definition! If it is "  scanner-route-..." it is a service.
            if "scanner-" in line:
                in_networks = False
                new_lines.append(line)
            else:
                network_lines.append(line)
        else:
            if in_networks and line.strip() == "":
                network_lines.append(line)
            elif in_networks and line.startswith("    "):
                network_lines.append(line)
            else:
                # Should not happen unless we exited networks
                in_networks = False
                new_lines.append(line)
    else:
        new_lines.append(line)

# Now, append network_lines to the end of new_lines
final_lines = new_lines + network_lines

with open("docker-compose-python-workers.yml", "w") as f:
    f.writelines(final_lines)

print("done")
