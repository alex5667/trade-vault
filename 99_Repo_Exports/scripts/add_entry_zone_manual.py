#!/usr/bin/env python3
"""
Добавляем поддержку Entry Zone вручную
"""

# Читаем файл
with open('telegram-worker/app/parse_utils.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Находим строку с ENTRY_DIRECTION_RANGE_RE и добавляем после неё
new_lines = []
for i, line in enumerate(lines):  # noqa: B007
    new_lines.append(line)
    if 'ENTRY_DIRECTION_RANGE_RE = re.compile(r\'\\b(LONG|SHORT)\\s*:\\s*(?P<p1>\\d+(?:[.,]\\d+)?)\\s*-\\s*(?P<p2>\\d+(?:[.,]\\d+)?)\', re.I)' in line:
        # Добавляем новый паттерн
        new_lines.append('# New pattern for "Entry Zone: 0.2158 – 0.2115" format\n')
        new_lines.append('ENTRY_ZONE_RE = re.compile(r\'Entry\\s+Zone\\s*:\\s*(?P<p1>\\d+(?:[.,]\\d+)?)\\s*[–\\-]\\s*(?P<p2>\\d+(?:[.,]\\d+)?)\', re.I)\n')

# Находим место в коде для добавления обработки
for i, line in enumerate(new_lines):
    if 'elif m := ENTRY_DIRECTION_RANGE_RE.search(text):' in line:
        # Находим конец этого блока
        j = i + 1
        while j < len(new_lines) and (new_lines[j].startswith('        ') or new_lines[j].strip() == ''):
            j += 1

        # Добавляем обработку Entry Zone
        entry_zone_code = [
            '    elif m := ENTRY_ZONE_RE.search(text):\n',
            '        # Entry Zone format: "Entry Zone: 0.2158 – 0.2115" -> "0.2158 – 0.2115"\n',
            '        p1 = m.group(\'p1\')\n',
            '        p2 = m.group(\'p2\')\n',
            '        entry = f"{p1} – {p2}"\n',
            '        print(f"DEBUG: Entry found (Entry Zone format): {entry}")  # Отладка\n'
        ]

        # Вставляем код
        new_lines = new_lines[:j] + entry_zone_code + new_lines[j:]
        break

# Сохраняем файл
with open('telegram-worker/app/parse_utils.py', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print("✅ Добавлена поддержка Entry Zone")
