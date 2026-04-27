import os
import glob
import re

target_dir = "/home/alex/front/trade/scanner_infra/python-worker"
files = glob.glob(target_dir + "/**/*.py", recursive=True)

pattern = re.compile(r'([\t ]*)if not batch:\n\1if len\(batch\) == 1 and batch\[0\]\[0\] == last_id:\n\1    break\n\1    break')

count = 0
for fpath in files:
    try:
        with open(fpath, "r") as f:
            content = f.read()
        
        new_content, subs = pattern.subn(r'\1if not batch:\n\1    break\n\1if len(batch) == 1 and batch[0][0] == last_id:\n\1    break', content)
        
        if subs > 0:
            with open(fpath, "w") as f:
                f.write(new_content)
            print(f"Fixed {subs} occurrences in {fpath}")
            count += 1
    except Exception as e:
        pass

print(f"Total files fixed: {count}")
