import re

with open("/tmp/orig/binance_execution/binance_executor.py", "r") as f:
    text = f.read()

# We need to resolve the standard git conflict markers created by patch --merge
# The format is:
# <<<<<<<
# (original text from the file)
# =======
# (new text from the patch)
# >>>>>>>

# We can find all conflicts and replace the block with the NEW text only.
# Wait! Does `patch --merge` produce standard markers? YES: `<<<<<<< \n [original context] \n ======= \n [new text] \n >>>>>>> \n`
# Actually, the original text from the file was the text that was NOT matching the patch context.
# We just want the NEW text from the patch.

def resolve_ours(text):
    # This picks the new patch text (which is after =======)
    # patch --merge structure:
    # <<<<<<< /tmp/orig/binance_execution/...
    # text...
    # =======
    # new_text...
    # >>>>>>> /tmp/orig/binance_execution/...
    
    pattern = re.compile(r"<<<<<<<.*?\n(.*?)=======\n(.*?)\n>>>>>>>.*?\n", re.DOTALL)
    
    # Wait! the new text we want is in group 2! The patched text
    resolved_text = pattern.sub(r"\2\n", text)
    return resolved_text

resolved_text = resolve_ours(text)

with open("/tmp/orig/binance_execution/binance_executor.py.resolved", "w") as f:
    f.write(resolved_text)

print("Conflicts resolved. Validating syntax...")
import ast
try:
    ast.parse(resolved_text)
    print("Syntax OK")
except SyntaxError as e:
    print(f"Syntax error after resolution: {e}")
