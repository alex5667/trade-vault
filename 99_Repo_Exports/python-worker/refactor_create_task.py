import os
import re

TARGET_DIR = "/home/alex/front/trade/scanner_infra/python-worker"

IMPORT_STMT = "from utils.task_manager import safe_create_task\n"
ASYNCIO_CREATE_TASK_RE = re.compile(r'asyncio\.create_task\(')

def process_file(file_path):
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()

    # If the file doesn't have asyncio.create_task, skip
    if "asyncio.create_task" not in content:
        return

    # Skip files inside tests or venv
    if "tests/" in file_path or ".venv/" in file_path or "venv/" in file_path:
        return

    # Exclude utils/task_manager.py itself
    if "utils/task_manager.py" in file_path:
        return

    # Exclude the script itself
    if "refactor_create_task.py" in file_path:
        return

    new_content = ASYNCIO_CREATE_TASK_RE.sub("safe_create_task(", content)

    # Add import. 
    # Just add it after the `import asyncio` or `import logging`.
    if "import asyncio" in new_content:
        new_content = new_content.replace(
            "import asyncio",
            "import asyncio\n" + IMPORT_STMT,
            1
        )
    elif "import os" in new_content:
         new_content = new_content.replace(
            "import os",
            "import os\n" + IMPORT_STMT,
            1
        )
    else:
        # Just put at the top (after any sys.path imports)
        # Assuming the file isn't empty
        lines = new_content.split("\n")
        lines.insert(0, IMPORT_STMT.strip())
        new_content = "\n".join(lines)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(new_content)
    print(f"Refactored: {file_path}")

def main():
    for root, dirs, files in os.walk(TARGET_DIR):
        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)
                process_file(file_path)

if __name__ == "__main__":
    main()
