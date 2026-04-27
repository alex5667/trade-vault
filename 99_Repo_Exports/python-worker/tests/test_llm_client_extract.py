# python-worker/tests/test_llm_client.py
from news_pipeline.llm_client import GeminiHTTPClient

def test_sanitize():
    c = GeminiHTTPClient()
    out = c._sanitize({"risk": 2, "surprise": -2, "confidence": "0.7", "tags": ["CPI", 123], "summary": "x"*500})
    assert 0.0 <= out["risk"] <= 1.0
    assert -1.0 <= out["surprise"] <= 1.0
    assert out["confidence"] == 0.7
    assert out["tags"] == ["cpi"]
    assert len(out["summary"]) == 200