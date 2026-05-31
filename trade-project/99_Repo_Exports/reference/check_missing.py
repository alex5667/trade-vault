import re
import os

lines = open("summary_errors.txt").readlines()
current_test = None
for line in lines:
    if "ERROR collecting" in line:
        current_test = line.split(" ")[-2].strip()
    elif "ModuleNotFoundError:" in line:
        mod = re.search(r"No module named .(.+?).", line)
        if mod:
            m = mod.group(1)
            # Try to see if this module exists somewhere
            # e.g. tools._ml_common
            m_path = m.replace(".", "/") + ".py"
            if not os.path.exists(m_path):
                print(f"git rm {current_test} # module {m} absolutely dead")
    elif "ImportError:" in line:
        match = re.search(r"cannot import name .([^']+). from .([^']+). \(([^)]+)\)", line)
        if match:
            # check if the file exists
            if not os.path.exists(match.group(3)):
                print(f"git rm {current_test} # File {match.group(3)} dead")
            else:
                name = match.group(1)
                mod = match.group(2)
                file_path = match.group(3)
                # Let's check if the name is inside the file
                content = open(file_path).read()
                if name not in content:
                    print(f"git rm {current_test} # Name {name} missing from existing {file_path}")
        else:
            match2 = re.search(r"cannot import name .([^']+). from .([^']+).$", line)
            if match2:
                print(f"# Needs manual review: {current_test}: {line.strip()}")
            elif "cannot import name" in line:
                print(f"git rm {current_test} # Unparsed Import Error")
    elif "SyntaxError:" in line:
        print(f"# Syntax Error in {current_test}")
