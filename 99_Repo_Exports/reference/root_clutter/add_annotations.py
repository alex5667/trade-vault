#!/usr/bin/env python3
import glob
from ruamel.yaml import YAML

yaml = YAML()
yaml.preserve_quotes = True

REQUIRED_FOR = {"critical", "warning", "crit", "warn"}
FILES = glob.glob("**/*prometheus_alerts_*.yml", recursive=True) + glob.glob("prometheus_alerts_*.yml")

count = 0
for file in set(FILES):
    with open(file, 'r', encoding='utf-8') as f:
        data = yaml.load(f)
    if not data:
        continue
    
    changed = False
    groups = data.get('groups', [])
    if not groups:
        continue
        
    for group in groups:
        rules = group.get('rules', [])
        for rule in rules:
            if 'alert' not in rule:
                continue
            labels = rule.get('labels', {})
            severity = str(labels.get('severity', '')).lower()
            if severity in REQUIRED_FOR:
                ann = rule.get('annotations', {})
                # preserve existing keys, but ensure runbook_path and dashboard_path exist
                if 'runbook_path' not in ann or not str(ann.get('runbook_path')).strip():
                    ann['runbook_path'] = '/runbooks/tbd.md'
                    changed = True
                if 'dashboard_path' not in ann or not str(ann.get('dashboard_path')).strip():
                    ann['dashboard_path'] = '/d/tbd'
                    changed = True
                if changed:
                    rule['annotations'] = ann

    if changed:
        with open(file, 'w', encoding='utf-8') as f:
            yaml.dump(data, f)
        count += 1
        print(f"Updated {file}")

print(f"Modified {count} files")
