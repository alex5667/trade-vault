import sys
import traceback
def trace_calls(frame, event, arg):
    if event == 'call' and frame.f_code.co_name == 'ping':
        print("\n=== PING TRACE ===")
        traceback.print_stack(frame)
    return trace_calls
sys.settrace(trace_calls)
try:
    import main
except Exception:
    pass
