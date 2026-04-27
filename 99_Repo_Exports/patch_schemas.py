import glob
import hashlib
import re

files = glob.glob('/home/alex/front/trade/scanner_infra/python-worker/**/*.py', recursive=True)
files = [f for f in files if "ml_feature_schema_" in f]

patterns_to_remove = [
    r'\s*"liqmap_[a-zA-Z0-9_]+_peak_up_usd",\n',
    r'\s*"liqmap_[a-zA-Z0-9_]+_peak_dn_usd",\n',
    r'\s*"liqmap_[a-zA-Z0-9_]+_peak_up_dist_bps",\n',
    r'\s*"liqmap_[a-zA-Z0-9_]+_peak_dn_dist_bps",\n',
    r'\s*"liqmap_[a-zA-Z0-9_]+_dist_up_bps",\n', # keep? The prompt said "двойная номенклатура liqmap_1h_peak_up_usd <-> peak_up1_usd"
]

patterns_to_remove_exact = [
    r'\s*"liqmap_[a-zA-Z0-9_]+_peak_up_usd"',
    r'\s*"liqmap_[a-zA-Z0-9_]+_peak_dn_usd"',
    r'\s*"liqmap_[a-zA-Z0-9_]+_peak_up_dist_bps"',
    r'\s*"liqmap_[a-zA-Z0-9_]+_peak_dn_dist_bps"',
]


count_patched = 0
for filepath in files:
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check if we need to remove double nomenclature
    original_content = content
    for pattern in patterns_to_remove:
        content = re.sub(pattern, '', content)

    for pattern in patterns_to_remove_exact:
        content = re.sub(pattern + r',?\n?', '', content)

    # Compute a schema hash based on the content (ignoring existing hash if any)
    content_for_hash = re.sub(r'SCHEMA_HASH\s*=\s*".*?"\n', '', content)
    schema_hash = hashlib.md5(content_for_hash.encode('utf-8')).hexdigest()[:12]
    
    # Inject SCHEMA_HASH at the top, after imports
    if 'SCHEMA_HASH' not in content:
        # Find a good place to inject: after the last import
        lines = content.split('\n')
        last_import = 0
        for i, line in enumerate(lines):
            if line.startswith('import ') or line.startswith('from '):
                last_import = max(last_import, i)
        
        insert_idx = last_import + 1 if last_import > 0 else 0
        lines.insert(insert_idx, f'\nSCHEMA_HASH = "{schema_hash}"\n')
        content = '\n'.join(lines)
    else:
        # Replace existing hash
        content = re.sub(r'SCHEMA_HASH\s*=\s*".*?"', f'SCHEMA_HASH = "{schema_hash}"', content)
        
    if content != original_content:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        count_patched += 1

print(f"Patched {count_patched} files.")
