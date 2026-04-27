import re

ver_str = "v4_of_stable"
match = re.search(r"v(\d+)", ver_str)
if match:
    ver_num = int(match.group(1))
    print(ver_num)
