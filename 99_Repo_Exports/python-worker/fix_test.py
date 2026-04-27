with open("tests/core/test_tick_cvd_quarantine.py", "r") as f:
    text = f.read()

# Add a normal tick before the jump to establish baseline
text = text.replace('state.update({"ts": now_ms, "qty": 1000, "side": "BUY"})',
                    'state.update({"ts": now_ms - 100, "qty": 10, "side": "BUY"})\n        state.update({"ts": now_ms, "qty": 1000, "side": "BUY"})')

with open("tests/core/test_tick_cvd_quarantine.py", "w") as f:
    f.write(text)
