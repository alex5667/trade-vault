import os
import sys

def extract_patch(patch_file):
    if not os.path.exists(patch_file):
        print(f"File not found: {patch_file}")
        return
        
    with open(patch_file, "r") as f:
        lines = f.readlines()
        
    current_file = None
    content = []
    
    for line in lines:
        if line.startswith("diff --git "):
            if current_file and content:
                print(f"Writing {current_file}")
                os.makedirs(os.path.dirname(current_file), exist_ok=True)
                with open(current_file, "w") as out:
                    out.write("".join(content))
            parts = line.strip().split()
            current_file = parts[-1][2:]  # remove b/ prefix
            content = []
        elif line.startswith("+++ "):
            pass
        elif line.startswith("--- "):
            pass
        elif line.startswith("@@ "):
            pass
        elif line.startswith("+"):
            content.append(line[1:])
        elif line.startswith(" ") and not line.strip() == "":
            content.append(line[1:])
        elif line == " \n":
            content.append("\n")
            
    if current_file and content:
        print(f"Writing {current_file}")
        os.makedirs(os.path.dirname(current_file), exist_ok=True)
        with open(current_file, "w") as out:
            out.write("".join(content))

print("Extracting ml_phase3_local_fallback_plane_v1.patch")
extract_patch("/home/alex/front/trade/scanner_infra/ml_phase3_local_fallback_plane_v1.patch")
print("Extracting ml_phase3_1_vertex_local_handoff_v1.patch")
extract_patch("/home/alex/front/trade/scanner_infra/ml_phase3_1_vertex_local_handoff_v1.patch")
print("Done!")
