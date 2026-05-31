import os
import re

TARGET_DIR = "/home/alex/front/trade/scanner_infra/python-worker"

def process_file(file_path):
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()

    changed = False

    # Replace empty deque with deque(maxlen=4096)
    if "tick_uid_ring: Deque[str] = field(default_factory=lambda: deque(maxlen=4096), repr=False)" in content:
        content = content.replace("tick_uid_ring: Deque[str] = field(default_factory=lambda: deque(maxlen=4096), repr=False)",
                                  "tick_uid_ring: Deque[str] = field(default_factory=lambda: deque(maxlen=4096), repr=False)")
        changed = True
        
    # In _dedup_seen_uid function, the popping loop becomes redundant but can be left alone, or removed.
    # It has `while len(tick_uid_ring) > window:` and pops. 
    # That is perfectly fine and safe to leave.

    if changed:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"Refactored: {file_path}")

def main():
    for root, dirs, files in os.walk(TARGET_DIR):
        for file in files:
            if file.endswith(".py") and "runtime" in file.lower():
                file_path = os.path.join(root, file)
                process_file(file_path)

if __name__ == "__main__":
    main()
