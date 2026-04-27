# P0–P3 gap report after patch

## Closed in this patch

- P0.1: canonical Binance algo/plain contract enforced in code and materialized state.
- P0.2: naked-position invariant hardened with timeout-driven emergency flatten.
- P0.3: unified reconcile waterfall for ambiguous 503/timeout paths.
- P0.4: local plain/algo validation before request dispatch.
- P1.3: explicit `PARTIAL_FILL_POLICY` with three branches.
- P1.5: user-stream liveness contract added; executor can hard-require live stream.
- P2.3: beta-cap + leader-stress tighten hooks added.
- P2.4: expected-edge / fee / slippage aware sizing inputs added.
- Ops: alert rules + safe env example added.

## Still not fully implemented

### P1 — execution determinism
- `orders:exec` is stronger now, but `orders:state:{sid}` is still used as hot-path mutable lookup. Full event-sourced rebuild-first execution is **not** complete.
- Execution event schema is typed, but not every downstream consumer has been migrated to the new canonical schema.
- User Data Stream is now enforceable, but there is still no separate supervisor / health controller that guarantees the worker is running before executor boot.

### P2 — risk engine
- Net beta is a coarse notional-beta approximation, not a covariance/basket beta model.
- Leader override is heuristic (drawdown threshold), not a full market-regime dependency graph.
- News blackout depends on upstream signal fields; there is still no direct integration with a dedicated news-state service.

### P3 — research / edge validation
- Walk-forward, embargo/purging, Reality Check and Deflated Sharpe are still not implemented in the research pipeline here.
- `shadow_only` flag exists in rollout contract/env example, but a full staged rollout controller (shadow -> paper -> small capital -> full) is still not wired end-to-end.
