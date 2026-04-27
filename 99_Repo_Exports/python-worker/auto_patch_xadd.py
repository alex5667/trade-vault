import os
import re
from tests.test_xadd_maxlen_lint import _find_unbounded_xadd

def patch_all():
    violations = _find_unbounded_xadd()
    files_to_patch = {}
    for rel, ln, _ in violations:
        files_to_patch.setdefault(rel, []).append(ln)
    
    for rel, lines in files_to_patch.items():
        if rel.startswith("tests/"):
            continue
        try:
            with open(rel, "r") as f:
                content = f.read()
        except Exception:
            continue
            
        # A simple but robust regex to insert maxlen=50000 inside xadd(...)
        # We look for .xadd( ... , maxlen=50000) handling nested parens up to some depth.
        # Actually it's easier to use a simple state machine to find the matching closing paren.
        
        patched_content = content
        
        # We iterate lines and patch
        lines_list = patched_content.splitlines()
        for i in range(len(lines_list)):
            if ".xadd(" in lines_list[i].lower() and "maxlen=" not in "".join(lines_list[i:i+6]).lower():
                # find start of .xadd(
                match = re.search(r'\.xadd\s*\(', lines_list[i], re.IGNORECASE)
                if match:
                    # find matching closing parenthesis
                    # join the rest of the file to find it
                    rest_of_file = "\n".join(lines_list[i:i+10])
                    start_idx = match.end()
                    paren_count = 1
                    end_idx = start_idx
                    for j, char in enumerate(rest_of_file[start_idx:]):
                        if char == '(':
                            paren_count += 1
                        elif char == ')':
                            paren_count -= 1
                        if paren_count == 0:
                            end_idx = start_idx + j
                            break
                    if paren_count == 0:
                        # insert maxlen=50000 before the closing paren
                        insertion = ", maxlen=50000"
                        if rest_of_file[end_idx-1] in ["(", " "]: # empty or spaced
                            # if empty fields dict? xadd expects fields
                            pass
                        
                        modified = rest_of_file[:end_idx] + insertion + rest_of_file[end_idx:]
                        # replace the lines_list
                        modified_lines = modified.splitlines()
                        for k in range(len(modified_lines)):
                            if i+k < len(lines_list):
                                lines_list[i+k] = modified_lines[k]
        
        # overwrite
        with open(rel, "w") as f:
            f.write("\n".join(lines_list) + "\n")
            
if __name__ == "__main__":
    patch_all()
    print("Done patching.")
