#!/usr/bin/env python3
"""Show the regex pattern and code for Entry Zone support in parse_utils.py."""
import re

# Add new pattern for "Entry Zone"
ENTRY_ZONE_RE = re.compile(r'Entry\s+Zone\s*:\s*(?P<p1>\d+(?:[.,]\d+)?)\s*[–\-]\s*(?P<p2>\d+(?:[.,]\d+)?)', re.I)

# Ищем место в коде, где обрабатывается entry
entry_processing_code = '''
    # Добавляем поддержку для "Entry Zone: 0.2158 – 0.2115"
    elif m := ENTRY_ZONE_RE.search(text):
        # Entry Zone format: "Entry Zone: 0.2158 – 0.2115" -> "0.2158 – 0.2115"
        p1 = m.group('p1')
        p2 = m.group('p2')
        entry = f"{p1} – {p2}"
        print(f"DEBUG: Entry found (Entry Zone format): {entry}")  # Отладка
'''

print("Новый паттерн для Entry Zone:")
print("ENTRY_ZONE_RE = re.compile(r'Entry\\s+Zone\\s*:\\s*(?P<p1>\\d+(?:[.,]\\d+)?)\\s*[–\\-]\\s*(?P<p2>\\d+(?:[.,]\\d+)?)', re.I)")
print("\nКод для обработки:")
print(entry_processing_code)
