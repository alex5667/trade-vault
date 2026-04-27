import json

def _b2s(x):
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", errors="ignore")
    return x if x is not None else ""

def _looks_like_json(s):
    s = s.strip()
    return (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]"))

def _maybe_json_load(v):
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, (bytes, bytearray)):
        v = v.decode("utf-8", errors="ignore")
    if not isinstance(v, str):
        return v
    if not _looks_like_json(v):
        return v
    try:
        return json.loads(v)
    except Exception:
        return v

JSON_FIELD_KEYS = {"signal_payload", "signal_settings", "risk", "metadata", "indicators", "confirmations", "buttons"}

def normalize_entry(entry):
    if not entry:
        return {}
    out = {}
    if isinstance(entry, dict):
        out = {_b2s(k): _b2s(v) for k, v in entry.items() if k is not None}
    elif isinstance(entry, (list, tuple)):
        try:
            d = dict(zip(entry[::2], entry[1::2]))
            out = {_b2s(k): _b2s(v) for k, v in d.items() if k is not None}
        except Exception:
            return {}
    for carrier in ("data", "payload"):
        if carrier in out:
            val = out.get(carrier)
            obj = _maybe_json_load(val)
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if k not in out:
                        out[k] = v
    for k in list(out.keys()):
        if k in JSON_FIELD_KEYS:
            out[k] = _maybe_json_load(out.get(k))
    return out

# Simulate redis raw response for xreadgroup
raw_entry = [b'type', b'report', b'subtype', b'taker_calibrator', b'ts', b'1773460786830', b'text', b'<b>Taker Flow Gate Calibrator</b>\n\n\xd0\x97\xd0\xb0 \xd0\xbf\xd0\xbe\xd1\x81\xd0\xbb\xd0\xb5\xd0\xb4\xd0\xbd\xd0\xb8\xd0\xb5 168.0 \xd1\x87\xd0\xb0\xd1\x81\xd0\xbe\xd0\xb2 `shadow_veto` \xd1\x81\xd1\x80\xd0\xb0\xd0\xb1\xd0\xbe\xd1\x82\xd0\xb0\xd0\xbb \xd0\xbd\xd0\xb0 0 \xd1\x82\xd1\x80\xd0\xb5\xd0\xb9\xd0\xb4\xd0\xb0\xd1\x85.\n\xd0\xa1\xd0\xbe\xd0\xb2\xd0\xbe\xd0\xba\xd1\x83\xd0\xbf\xd0\xbd\xd1\x8b\xd0\xb9 R-multiple \xd1\x8d\xd1\x82\xd0\xb8\xd1\x85 \xd1\x81\xd0\xb4\xd0\xb5\xd0\xbb\xd0\xbe\xd0\xba: <b>0.00R</b>.\n\xd0\xa2\xd0\xb5\xd0\xba\xd1\x83\xd1\x89\xd0\xb8\xd0\xb9 \xd0\xbe\xd0\xb1\xd1\x89\xd0\xb8\xd0\xb9 PnL (\xd0\xb7\xd0\xb0\xd0\xba\xd1\x80\xd1\x8b\xd1\x82\xd0\xb8\xd0\xb5 \xd0\xbf\xd0\xbe\xd0\xb7\xd0\xb8\xd1\x86\xd0\xb8\xd0\xb9) \xd0\xb1\xd1\x8b\xd0\xbb \xd0\xb1\xd1\x8b \xd0\xbd\xd0\xb0 <b>-0.00R</b> \xd0\xb2\xd1\x8b\xd1\x88\xd0\xb5 \xd0\xbf\xd1\x80\xd0\xb8 \xd1\x80\xd0\xb5\xd0\xb6\xd0\xb8\xd0\xbc\xd0\xb5 <code>enforce</code>.\n\n\xd0\x9f\xd1\x80\xd0\xb5\xd0\xb4\xd0\xbb\xd0\xb0\xd0\xb3\xd0\xb0\xd1\x8e \xd0\xb2\xd0\xba\xd0\xbb\xd1\x8e\xd1\x87\xd0\xb8\xd1\x82\xd1\x8c `taker_flow_gate_mode=enforce` (GLOBAL).', b'parse_mode', b'HTML', b'buttons', b'[[{"text": "\\u2705 Approve", "callback_data": "recs:confirm:taker_enforce_1773460786:b31d0bde"}, {"text": "\\u274c Reject", "callback_data": "recs:reject:taker_enforce_1773460786:b31d0bde"}]]']

norm = normalize_entry(raw_entry)
print("Normalized type:", norm.get('type'))
print("Normalized buttons type:", type(norm.get('buttons')))

msg_type = norm.get("type")
if msg_type in ("report", "alert"):
    text = norm.get("text", "")
    if not text:
        print("skipping empty")
    else:
        buttons = norm.get("buttons")
        if buttons:
            print(f"�� DEBUG: Report buttons type={type(buttons)}")
        print("SUCCESS! msg ready to send")
else:
     print(f"Skipped, type is {msg_type}")
