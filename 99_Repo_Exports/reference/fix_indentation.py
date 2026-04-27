import re

with open("services/crypto_orderflow_service.py", "r") as f:
    lines = f.readlines()

# Fix consume_ticks:
# 1. Remove the rogue except block I added at lines 1185-1188
start_remove = -1
for i, line in enumerate(lines):
    if "except Exception as parse_exc:" in line and "Error parsing tick msg" in lines[i+1]:
        start_remove = i
        break

if start_remove != -1:
    lines = lines[:start_remove] + lines[start_remove+4:]

# 2. Indent the trailing except and finally block for consume_ticks
# The except block starts with: "except Exception as exc:  # noqa: BLE001"
# It should end before: "except Exception as stream_exc:"
in_trailing = False
for i in range(len(lines)):
    if "except Exception as exc:  # noqa: BLE001" in lines[i] and lines[i].startswith("                    except"):
        if "consume_ticks" in "".join(lines[max(0, i-500):i]):
            # Verify we are in consume_ticks
            in_trailing = True
    
    if in_trailing:
        if lines[i].startswith("            except Exception as stream_exc:"):
            in_trailing = False
            break
        # Indent by 4 spaces
        if lines[i].startswith("                    "):
            lines[i] = "    " + lines[i]

# Fix consume_books:
# The `try:` block starts around line 1610 (now after changes).
# We need to find `for msg in batch:` in consume_books and indent everything after it up to `except Exception as stream_exc:`

in_books_for = False
for i in range(len(lines)):
    # Find the consume_books loop start
    if "def consume_books" in lines[i]:
        in_books_for = False
        
    if "for msg in batch:" in lines[i] and "msg_id = msg.msg_id" in lines[i+1] and "payload = msg.fields" in lines[i+2]:
        in_books_for = True
        continue # skip the `for msg in batch:` and the following 2 assignments, wait we don't indent them but we indent what's after them.
        
    if in_books_for and "def " in lines[i]:
        break # Safety
        
    if in_books_for and lines[i].startswith("            except Exception as stream_exc:"):
        in_books_for = False
        break
        
    if in_books_for and i > 1600:
        # Check if we are past the variable assignments of the loop
        if "msg_id = msg.msg_id" in lines[i] or "payload = msg.fields" in lines[i]:
            continue
        # Indent everything by 4 spaces!
        if lines[i].startswith("                    "):
            lines[i] = "    " + lines[i]

with open("services/crypto_orderflow_service.py", "w") as f:
    f.writelines(lines)
print("Indentation fixed.")
