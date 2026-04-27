---
name: trade-fast-test-gen
description: Generate unit tests for isolated functions without architectural drift.
---

1. Work in Fast mode with a low-cost model.
2. Limit scope to explicitly mentioned files and closest dependencies.
3. Do not redesign architecture.
4. Produce minimal diff.
5. Add unit tests if obvious.
6. Escalate instead of guessing if >2 subsystems are involved, contracts break, or risk policies change.
7. Output: Facts, Risks, Diff, Tests, Rollback
