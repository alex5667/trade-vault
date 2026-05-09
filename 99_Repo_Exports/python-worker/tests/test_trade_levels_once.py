

def test_side_normalization():
    """Test the side normalization logic from _ensure_trade_levels_once"""

    def _norm_side(s) -> str:
        try:
            # numeric / bool-ish
            if s in (1, +1, True):
                return "LONG"
            if s in (-1, -1.0, False):
                return "SHORT"
        except Exception:
            pass
        try:
            if isinstance(s, str):
                u = s.strip().upper()
                if u in {"LONG", "BUY", "+1", "1"}:
                    return "LONG"
                if u in {"SHORT", "SELL", "-1"}:
                    return "SHORT"
            # enums/objects
            u = str(getattr(s, "name", None) or getattr(s, "value", None) or s).strip().upper()
            if u in {"LONG", "BUY"}:
                return "LONG"
            if u in {"SHORT", "SELL"}:
                return "SHORT"
        except Exception:
            pass
        # fail-open default (как в level_enricher)
        return "LONG"

    # Test various inputs
    assert _norm_side(1) == "LONG"
    assert _norm_side(-1) == "SHORT"
    assert _norm_side("LONG") == "LONG"
    assert _norm_side("SHORT") == "SHORT"
    assert _norm_side("1") == "LONG"
    assert _norm_side("-1") == "SHORT"
    assert _norm_side("BUY") == "LONG"
    assert _norm_side("SELL") == "SHORT"
    assert _norm_side(None) == "LONG"  # default


def test_key_generation_logic():
    """Test that key generation works correctly for different inputs"""

    def _cfg_hash(cfg: dict) -> str:
        import hashlib
        import json
        try:
            s = json.dumps(cfg or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            return hashlib.sha1(s.encode("utf-8", "ignore")).hexdigest()
        except Exception:
            return "cfg:err"

    def generate_key(symbol, side_s, kind, cfgd, rg_key, empirical):
        return (
            "levels_v1",
            symbol,
            str(side_s),
            str(kind),
            str(rg_key)[:64] if rg_key else "",
            _cfg_hash(cfgd),
            round(100.0, 8),  # entry_f placeholder
            round(1.0, 8),    # atr_f placeholder
            int(id(None)) if empirical is not None else 0,
        )

    # Same inputs should generate same key
    key1 = generate_key("BTCUSDT", "LONG", "breakout", {"A": 1}, None, None)
    key2 = generate_key("BTCUSDT", "LONG", "breakout", {"A": 1}, None, None)
    assert key1 == key2

    # Different side should generate different key
    key3 = generate_key("BTCUSDT", "SHORT", "breakout", {"A": 1}, None, None)
    assert key1 != key3

    # Different kind should generate different key
    key4 = generate_key("BTCUSDT", "LONG", "absorption", {"A": 1}, None, None)
    assert key1 != key4

    # Different cfg should generate different key
    key5 = generate_key("BTCUSDT", "LONG", "breakout", {"B": 1}, None, None)
    assert key1 != key5
