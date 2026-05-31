import html
import json

alerts = ["meta_p50<0.2", "of_gate < 0.2"]
alerts_str = json.dumps(alerts, ensure_ascii=False)
escaped = html.escape(alerts_str, quote=False)

print(f"Original: {alerts_str}")
print(f"Escaped: {escaped}")

msg = f"alerts=<code>{escaped}</code>"
print(f"Final message: {msg}")
