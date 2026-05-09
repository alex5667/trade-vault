

def test_meta_schema_compatibility():
    # Simulate what ABWinnerSuggesterLCB produces
    meta = {
        "sid": "abc1234",
        "ts_ms": 1700000000000,
        "symbol": "BTCUSD",
        "regime": "trend",
        "group": "default",
        "winner_arm": "B",
        "scenario": "continuation",
        "type": "ab_winner_lcb_v2",
        "decision": {"ok": 1},
    }

    # Simulate what ApplyRunner (patch 4) expects
    sym = (meta.get("symbol") or "").upper()
    rg = (meta.get("regime") or "na").lower()
    grp = (meta.get("group") or "default").lower()
    # Support both winner and winner_arm fallback
    win = str(meta.get("winner_arm") or meta.get("winner") or "").upper()
    scn = (meta.get("scenario") or "").lower()

    assert sym == "BTCUSD"
    assert rg == "trend"
    assert grp == "default"
    assert win == "B"
    assert scn == "continuation"

    # Simulate ApplyRunner Logic
    keys_written = {}

    # Base key
    key_base = f"cfg:entry_policy:active_arm:{sym}:{rg}:{grp}"
    keys_written[key_base] = win

    # Scenario key
    if scn in ("continuation", "reversal"):
        key_scn = f"cfg:entry_policy:active_arm:{sym}:{rg}:{grp}:{scn}"
        keys_written[key_scn] = win

    assert keys_written["cfg:entry_policy:active_arm:BTCUSD:trend:default"] == "B"
    assert keys_written["cfg:entry_policy:active_arm:BTCUSD:trend:default:continuation"] == "B"

    print("test_meta_schema_compatibility passed")

if __name__ == "__main__":
    test_meta_schema_compatibility()
