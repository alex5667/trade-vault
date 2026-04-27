import sys
import os

# [AUTOGRAVITY CLEANUP] sys.path.append("/home/alex/front/trade/scanner_infra/python-worker")

# Set dummy env vars for DB
os.environ["POSTGRES_HOST"] = "localhost"
os.environ["POSTGRES_PORT"] = "5434"
os.environ["POSTGRES_USER"] = "postgres"
os.environ["POSTGRES_PASSWORD"] = ""
os.environ["POSTGRES_DB"] = "scanner_analytics"
os.environ["ATR_GRAPH_EFFECTIVE_STATE_MODE"] = "shadow_compare"

from services.atr_effective_state_equivalence_cert_service import ATREffectiveStateEquivalenceCertService

scope_val = "BTCUSDT|breakout|trend_up|short|stop_ttl|v17"
print(f"Running equivalence cert for {scope_val}")

result = ATREffectiveStateEquivalenceCertService.certify_equivalence("CryptoOrderFlow", scope_val)
print("Result:", result)
