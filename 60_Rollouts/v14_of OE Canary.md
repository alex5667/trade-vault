# v14_of Group OE Canary Rollout

**Schema:** `v14_of` (canary) vs `v13_of` (champion)
**New keys:** Group OE — 20 external-data features
**Status:** 🔴 **BLOCKED** by train/serve skew (Step 1 finding, 2026-05-16)
**Resume target:** ≥ 7 days after blocker fix lands AND OE keys observed in `signals:of:inputs`
**Owner:** alex5667

---

## 🔴 Pre-flight BLOCKER (must fix first)

**Symptom (verified 2026-05-16 via XREVRANGE signals:of:inputs):**
ALL Phase 7.8/7.9/8.1 deriv/external keys are **missing** from outbound `signals:of:inputs` payload — including `funding_rate`, `basis_bps`, `btc_ret_1m`, `leader_confidence`, and my new OE keys (`taker_buy_sell_imbalance`, `deribit_btc_iv_proxy`, `fear_greed_index`, etc.).

**Root cause:**
Populate code in `python-worker/core/of_confirm_engine.py` writes to a **local** `indicators_with_v4` dict used only for inline ML scoring. The **outbound** payload, however, comes from a different `indicators` dict — see how `build_og_payload(...)` is used:

```python
# of_confirm_engine.py:4528
indicators.update(build_og_payload(ofc=ofc, dec=dec, indicators=indicators))
```

So `og_*` keys flow correctly to `signals:of:inputs`, but **Phase 7.8/7.9/8.1** keys do not. This means:
- The v13_of champion was likely trained without 7.8/7.9 keys too (they vectorize to 0.0)
- v14_of canary with OE features will train on zeros → no signal
- This is **a multi-Phase train/serve skew bug**, not a v14_of-only issue

**Fix recipe (out of scope for this runbook, but required first):**
1. Create `python-worker/core/v14_of_features.py::build_oe_payload(ofc, indicators_with_v4, redis_client)` — analogous to `build_og_payload`.
2. Inside it: re-read the same Redis sources (`ctx:deriv:{symbol}`, `runtime:breadth`, `ctx:deribit:global`, `ctx:sentiment:global`) with same stale guards as in `of_confirm_engine.py` Phase 7.9b / 8.1 blocks, return dict of 20 OE keys.
3. Call `indicators.update(build_oe_payload(...))` next to the existing `indicators.update(build_og_payload(...))` at line ~4528.
4. (Bonus) Same pattern for Phase 7.8 (cross-context) and Phase 7.9 (deriv) keys — fix the underlying skew.
5. Sample N=10 fresh `signals:of:inputs` payloads — confirm all 20 OE keys present and non-zero for at least BTC/ETH/SOL after a few minutes.

---

## Acceptance gates (offline Phase 2)

After blocker fix lands and **≥ 7 days** of data with OE keys accumulated:

| Gate | Target | Source |
|---|---|---|
| EV/R uplift | ≥ +5% out-of-fold | `dataset_report.metrics.ev_r` |
| Precision@TopK (K=10%) | ≥ +2pp absolute | `dataset_report.metrics.precision_at_k` |
| Brier score | ≤ baseline (no calibration regression) | `dataset_report.metrics.brier` |
| ECE | ≤ baseline + 0.01 | `dataset_report.metrics.ece` |
| Ablation (v14_of without OE) | OE provides ≥ 80% of v14_of uplift | manual ablation run |
| Join rate (signals→trades_closed) | ≥ 95% | `dataset_report.join.rate` (watch SID join bug) |

---

## Commands

### Build dataset
```bash
docker exec scanner-python-worker python3 -m ml_analysis.tools.build_edge_stack_dataset_from_redis \
  --feature_schema_ver v14_of \
  --hours 168 \
  --out_parquet /tmp/v14_of_oe_canary.parquet \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT
```
Note: `v14_of` is now in [schema_choices_v1.py](python-worker/ml_analysis/tools/schema_choices_v1.py) (added 2026-05-16) — argparse choices accept it. Schema registry returns 318 feature_cols including all 20 OE `f_*` columns.

### Train baseline (v13_of replication)
```bash
docker exec scanner-python-worker python3 -m ml_analysis.tools.train_edge_stack_v1_oof \
  --feature_schema_ver v13_of \
  --dataset /tmp/v14_of_oe_canary.parquet \
  --out_model_dir /tmp/champion_v13_of_replication
```

### Train challenger (v14_of + OE)
```bash
docker exec scanner-python-worker python3 -m ml_analysis.tools.train_edge_stack_v1_oof \
  --feature_schema_ver v14_of \
  --dataset /tmp/v14_of_oe_canary.parquet \
  --out_model_dir /tmp/challenger_v14_of_oe
```

### Ablation (v14_of − OE keys)
Construct a feature_cols list of v14_of MINUS the 20 OE keys (`taker_buy_sell_imbalance`, `force_order_imbalance_1m`, `oi_confirmation_score`, `squeeze_risk_score`, `liq_impulse_score`, `market_breadth_*` (5), `deribit_*` (7), `fear_greed_*` (3)) and pass via `--feature_cols_json`.

### Compare
```bash
diff <(jq -S '.metrics' /tmp/champion_v13_of_replication/dataset_report.json) \
     <(jq -S '.metrics' /tmp/challenger_v14_of_oe/dataset_report.json)
```

---

## Phase 3: Shadow rollout (after offline gates pass)

### Enable shadow on canary services
Edit `docker-compose-crypto-orderflow.yml` for canary services (BTCUSDT/ETHUSDT/SOLUSDT path):
```yaml
- ML_FEATURE_SCHEMA_VER=v14_of
- ML_CONFIRM_MODE=SHADOW
```
Keep production services on `v13_of` until enforce gate passes.

Restart impacted services:
```bash
docker compose up -d <canary-services>
```

### Shadow observation period
**≥ 7 days minimum, 14 days preferred.** Watch:

| Metric | Source | Pass criterion |
|---|---|---|
| Live shadow EV/R | `metrics:ml_confirm` Prom | uplift ≥ +3% vs champion |
| Latency p99 | `histogram_quantile(0.99, ml_p_edge_latency_seconds_bucket)` | within 20% of champion |
| Abstain rate | `ml_abstain_total` | ≤ 2× baseline |
| Brier (live) | calibration tracker | within 5% of offline |
| ECE drift | calibration tracker | ≤ 0.05 |

### Enforce promotion gates
- Two consecutive 24h windows passing all shadow gates
- No prod incident attributable to OE features
- Schema parity check (`feature_registry_contract_check_v1`) green

When all green:
```yaml
- ML_FEATURE_SCHEMA_VER=v14_of
- ML_CONFIRM_MODE=ENFORCE   # was SHADOW
```

---

## Rollback (instant — keep this snippet handy)

```bash
# 1. Flip env back
sed -i 's/ML_FEATURE_SCHEMA_VER=v14_of/ML_FEATURE_SCHEMA_VER=v13_of/g' docker-compose-crypto-orderflow.yml
sed -i 's/ML_CONFIRM_MODE=ENFORCE/ML_CONFIRM_MODE=SHADOW/g' docker-compose-crypto-orderflow.yml

# 2. Restart impacted services (NOT all 326 — just signal-path services)
docker compose up -d \
  scanner-of-confirm-engine \
  scanner-signal-pipeline \
  scanner-ml-confirm-gate
```

### Rollback triggers (any of)
- Live calibration ECE drift > 0.05
- Abstain rate > 2× baseline for 1 hour
- p99 latency regression > 20%
- Any prod incident with `og_*`/`oe_*`/external features in root cause
- Schema parity check fails

---

## File inventory (already changed 2026-05-16)

- [ml_feature_schema_v14_of.py](../../../front/trade/scanner_infra/python-worker/core/ml_feature_schema_v14_of.py) — added Group OE (20 keys), SCHEMA_HASH=`v14of_og16_oe20_2026_05_16`, total 278 numeric keys
- [ml_feature_schema_v5_of.py](../../../front/trade/scanner_infra/python-worker/core/ml_feature_schema_v5_of.py) — Phase 8.1 same 18 num + 2 bool (for legacy v5_of consumers), SCHEMA_HASH=`3a08f83878e1`, total 195 num + 34 bool
- [of_confirm_engine.py](../../../front/trade/scanner_infra/python-worker/core/of_confirm_engine.py) — Phase 7.9b composites (5) + Phase 8.1 joiners (breadth/deribit/sentiment) populate **into `indicators_with_v4` only** (this is the blocker — see top of runbook)
- [schema_choices_v1.py](../../../front/trade/scanner_infra/python-worker/ml_analysis/tools/schema_choices_v1.py) — v14_of accepted in argparse choices + normalize_schema_ver(v14) → v14_of

## Related references
- [v14_of canary plan memory](file:///home/alex/.claude/projects/-home-alex-front-trade-scanner-infra/memory/project_v14_of_oe_canary_pending.md)
- [ML schema versions snapshot 2026-05-16](file:///home/alex/.claude/projects/-home-alex-front-trade-scanner-infra/memory/project_ml_schema_versions_2026_05_16.md)
- [ML Dataset SID Join Bug — watch join rate](file:///home/alex/.claude/projects/-home-alex-front-trade-scanner-infra/memory/feedback_ml_dataset_sid_join_bug.md)
- [orderflow_services dual-path gotcha](file:///home/alex/.claude/projects/-home-alex-front-trade-scanner-infra/memory/feedback_orderflow_services_dual_path.md)

## Change log
- 2026-05-16 — runbook created; documented train/serve skew blocker; v14_of schema accepted by dataset builder
