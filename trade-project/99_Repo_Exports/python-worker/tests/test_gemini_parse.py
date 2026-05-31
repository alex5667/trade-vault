from news_pipeline.llm_client import GeminiHTTPClient


def test_parse_response_candidates_text_json():
    c = GeminiHTTPClient()
    raw = {
        "candidates": [{
            "content": {"parts": [{"text": '{"risk":0.6,"surprise":-0.1,"tags":["cpi"],"confidence":0.7,"summary":"x"}'}]}
        }]
    }
    out = c._parse_gemini_response(__import__("json").dumps(raw))
    assert 0.0 <= out["risk"] <= 1.0
    assert out["tags_mask"] != 0
    assert out["primary_tag_id"] != 0
