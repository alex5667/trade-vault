import re
import os

with open('ml_phase3_55_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_controller_v1.patch') as f:
    lines = f.readlines()

current_file = None
lines_to_write = []

for line in lines:
    header_match = re.match(r'^\+*\+\+\+ b/(.*)', line)
    
    if header_match:
        if current_file:
            os.makedirs(os.path.dirname(current_file), exist_ok=True)
            with open(current_file, 'w') as out:
                out.write("".join(lines_to_write))
            print("Wrote", current_file)
        current_file = header_match.group(1).strip()
        lines_to_write = []
        continue
    
    # skip metadata diff lines
    if re.match(r'^\+*diff --git', line) or \
       re.match(r'^\+*new file mode', line) or \
       re.match(r'^\+*index ', line) or \
       re.match(r'^\+*--- /dev/null', line) or \
       re.match(r'^\+*@@ ', line):
        continue
        
    if current_file:
        # the file is prefixed by some number of + signs for addition
        # we only remove ONE plus if the line starts with a plus, because that represents the diff addition
        # actually, if the line has multiple pluses like `++import`, the diff prefix is just one plus, so it becomes `+import`.
        # However, for subsequent files, it's `+diff`, `++import`, so the prefix is TWO pluses `++`. 
        # But wait, looking at `++++ b/..`, the LLM decided to increment the `+` prefix for each file?
        
        # safely strip ONE plus if the line starts with +, then strip another if the line starts with +?
        
        # let's just strip all leading + except one if it's meant to be code? No, python code doesn't start with `+`.
        # Let's just strip ALL leading pluses, because normal code doesn't start with + at the beginning of the line!
        # wait! `+` might be used in md or sql if there's math, but it has spaces before it usually.
        # let's use a simpler approach: extract the raw text and replace `^\++` with ``. 
        # WARNING! Python code might have `+ ` for line continuation? No.
        # But wait, what if the line is just spaces?
        
        clean_line = re.sub(r'^\++', '', line)
        
        # sometimes LLM patches have a space after the `+`. Let's just remove the first `+`. If it's `++`, remove `++`.
        # let's just remove the exact prefix string length as the `+diff` metadata had.
        # Or even simpler:
        lines_to_write.append(clean_line)

if current_file:
    os.makedirs(os.path.dirname(current_file), exist_ok=True)
    with open(current_file, 'w') as out:
        out.write("".join(lines_to_write))
    print("Wrote", current_file)
