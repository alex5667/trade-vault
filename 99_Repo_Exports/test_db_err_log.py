with open("/home/alex/front/trade/scanner_infra/python-worker/services/analytics_db.py") as f:
    text = f.read()

import re
matches = re.findall(r'logger\.(error|exception|warning|critical|info|debug)\([^\)]*(%s|%d|%f)[^\)]*\)', text)
for m in re.finditer(r'logger\.(error|exception|warning|critical|info|debug)\((.*?)\)', text, re.DOTALL):
    args_str = m.group(2)
    s_count = args_str.count('%s') + args_str.count('%d') + args_str.count('%f') + args_str.count('%.')
    comma_count = args_str.count(',')
    
    # We roughly estimate the number of provided args vs placeholders
    # If s_count > 0 and comma_count == 0, it's definitely a fail!
    if s_count > 0 and comma_count == 0:
        print("FOUND DANGEROUS LOG:", m.group(0))
