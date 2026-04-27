def fix():
    with open("docker-compose-python-workers.yml", "r") as f:
        lines = f.readlines()
        
    frag_file = "orderflow_services/docker_compose_fragment_ml_phase3_53_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_bridge_v1.yml"
    with open(frag_file, "r") as f:
        frag_lines = f.readlines()
    if frag_lines[0].strip() == "services:":
        frag_lines = frag_lines[1:]
        
    out_lines = []
    for line in lines:
        if line.startswith("networks:"):
            out_lines.extend(frag_lines)
            out_lines.append(line)
        else:
            out_lines.append(line)
            
    # wait. But the block was already at the end?
    # I should also remove the appended lines from the end 
    # to be safe, I'm just gonna filter them.
    # Actually wait. The duplicate block might be exact match of frag_lines. 
    # Yes, we appended it. So let's delete the exact match at the end if it's there.
    out_text = "".join(out_lines)
    frag_text = "".join(frag_lines)
    if out_text.endswith(frag_text):
        out_text = out_text[:-len(frag_text)]

    with open("docker-compose-python-workers.yml", "w") as f:
        f.write(out_text)
    print("Fixed yaml")

fix()
