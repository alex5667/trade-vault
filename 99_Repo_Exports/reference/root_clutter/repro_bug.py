import html
import json

def test_escaping():
    alerts_val = ["<0.2',"]
    alerts_str = json.dumps(alerts_val, ensure_ascii=False)
    print(f"alerts_str raw: {alerts_str}")
    
    escaped = html.escape(alerts_str, quote=False)
    print(f"escaped: {escaped}")
    
    message = f"alerts=<code>{escaped}</code>"
    print(f"message: {message}")

    # Simulate what Telegram sees
    # If escaped contains <, it's bad.
    if "<0.2" in escaped:
        print("FAIL: <0.2 found in escaped string")
    else:
        print("SUCCESS: <0.2 NOT found in escaped string")

    # Double check mixed quotes
    alerts_val_2 = ["<0.2',"]
    # If somehow we used str() instead of json.dumps
    str_val = str(alerts_val_2)
    print(f"str_val: {str_val}")
    escaped_str = html.escape(str_val, quote=False)
    print(f"escaped_str: {escaped_str}")

if __name__ == "__main__":
    test_escaping()
