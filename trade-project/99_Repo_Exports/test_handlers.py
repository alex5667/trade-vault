import sys
sys.path.append("/home/alex/front/trade/scanner_infra/python-worker")
from domain.models import PositionState
pos = PositionState(id="1", symbol="BTC", direction="long")
print("is_long:", pos.is_long())
print("is_short:", pos.is_short())
