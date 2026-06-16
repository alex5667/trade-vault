# package: policy (`social_policy`)

Shared, deterministic policy scoring — the social-domain port of the trade risk engine
(migration plan §4.11). Risk is an embedded, explainable control surface, not a report.

## Three-branch risk engine

`assess_risk(brief, commerce=None, signals=None)` returns a structured risk object:

```python
from social_policy import assess_risk

assess_risk(
    {"caption": "buy now, link in bio"},
    commerce={"product_margin": 0.05, "creator_product_fit": 0.2},
)
# {
#   "platform_risk": 0.42,      # policy violations, missing disclosure, spam, automation
#   "brand_risk": 0.02,         # off-brand tone, hallucinated claims, low synthetic quality, near-duplicate
#   "commercial_risk": 0.82,    # weak margin, no conversion evidence, creator mismatch, cost/outcome
#   "decision": "block",        # driven by the most conservative branch
#   "reason_codes": ["platform:disclosure_missing", "commercial:weak_margin_profile", ...],
#   "evidence": {"platform": {...}, "brand": {...}, "commercial": {...}},
# }
```

Invariants: deterministic + explainable (every contribution emits a `reason_code`); bad/missing
inputs are sanitized to conservative defaults (detect → sanitize → reason_code, never crash);
structured output only — the engine never decides "publish". The publish gate / `policy_critic`
worker consume this assessment. Rule surface (word lists, thresholds, weights) lives in `rules.py`
so it is tunable from policy config without touching the engine.

Run tests: `cd packages/policy/python && python3 -m pytest tests/ -q`
