# Optional OFC golden replay tooling targets (safe if file absent).
-include make.ofc.mk

# -----------------------------------------------------------------------
# lint-py: ruff F821 (undefined names) — CI-блокер.
# Exit non-zero = блокирует merge/deploy.
# -----------------------------------------------------------------------
.PHONY: lint-py
lint-py:
	@echo "==> ruff F821 (undefined names) on pipeline..."
	python -m ruff check handlers/crypto_orderflow/pipeline/ --select F821
	@echo "==> ruff F (all pyflakes) on common/..."
	python -m ruff check common/ --select F
	@echo "==> lint-py OK"

