import sys
import logging
logging.basicConfig(level=logging.DEBUG)

def trace_calls(frame, event, arg):
    if event == 'call':
        func_name = frame.f_code.co_name
        if func_name in ('get_dual_redis_client', 'from_url', 'ping'):
            print(f"TRACED CALL: {func_name} at {frame.f_code.co_filename}:{frame.f_lineno}")
    return trace_calls

sys.settrace(trace_calls)

print("Starting import main")
import main
print("Finished import main")
