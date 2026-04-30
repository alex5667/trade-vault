from utils.time_utils import get_ny_time_millis

import json
import logging
import time
from typing import Dict, Any

from core.decision_store import DecisionStore
from core.decision_record import DecisionRecord
from core.redis_client import get_redis
from services.stream_worker import StreamWorker, WorkerPolicy

logger = logging.getLogger(__name__)

class LabelJoinerService:
    """
    Joins POSITION_CLOSED events with DecisionRecords to create labeled examples.
    Uses StreamWorker for robust stream consumption.
    """
    def __init__(self):
        self.redis = get_redis()
        self.decision_store = DecisionStore(redis_client=self.redis)
        
        self.policy = WorkerPolicy(
            ack_mode="lossless"
            read_count=50
            block_ms=2000
            dlq_stream="dlq:label_joiner"
        )
        
        self.worker = StreamWorker(
            name="label-joiner"
            client=self.redis
            group="scanner-label-joiner"
            consumer="worker-1"
            build_streams=lambda: ["events:trades"]
            process=self.process_trade_event
            policy=self.policy
            logger=logger
        )

    def run(self):
        """Starts the worker loop."""
        # Simple running flag, in real app might be a signal handler
        self.worker.run_loop(lambda: True)

    def process_trade_event(self, stream: str, msg_id: str, fields: Dict[str, Any]) -> bool:
        """
        Callback for StreamWorker.
        Returns True if processed (ACK), False if failed (Retry).
        """
        try:
            event_type = fields.get("type")
            if event_type != "POSITION_CLOSED":
                # Not a trade closure, just ACK and ignore
                return True

            data_str = fields.get("data")
            if not data_str:
                return True
                
            if isinstance(data_str, str):
                try:
                    payload = json.loads(data_str)
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON in {msg_id}: {data_str}")
                    return True # Skip bad data
            else:
                payload = data_str

            sid = payload.get("sid")
            if not sid:
                return True # No SID, can't join

            # 1. Fetch Decision
            decision = self.decision_store.load_decision(sid)
            if not decision:
                # Decision might be expired or missing. 
                # If we want to retry later, return False.
                # But if it's gone, it's gone. Let's log warn and ACK.
                # Or maybe it's just slow to appear? Unlikely if trade is closed.
                logger.warning(f"Decision not found for sid={sid} (msg_id={msg_id})")
                return True

            # 2. Extract Trade Metrics
            metrics = self._calculate_metrics(payload, decision)

            # 3. Join & Publish
            self._publish_closed_trade(decision, metrics, payload)
            self._publish_ml_replay(decision, metrics, payload)
            
            return True

        except Exception as e:
            logger.exception(f"Error processing message {msg_id}: {e}")
            return False # Retry

    def _calculate_metrics(self, trade: Dict[str, Any], decision: DecisionRecord) -> Dict[str, Any]:
        """
        Calculates R-multiple, MFE/MDD ratios, result label.
        """
        entry = float(trade.get("entry_price", 0.0))
        exit_price = float(trade.get("exit_price", 0.0))
        pnl = float(trade.get("total_pnl", 0.0))
        side = trade.get("direction", "").upper()
        
        # SL/Risk
        sl = float(trade.get("sl", 0.0))
        
        r_mult = 0.0
        
        if side == "LONG":
            risk = entry - sl
            if risk > 1e-9:
                r_mult = (exit_price - entry) / risk
        elif side == "SHORT":
            risk = sl - entry
            if risk > 1e-9:
                r_mult = (entry - exit_price) / risk
                
        # Result Class
        if pnl > 0:
            result = "WIN"
        elif pnl < 0:
            result = "LOSS"
        else:
            result = "BE"
            
        return {
            "result": result
            "r_multiple": r_mult
            "pnl": pnl
            "entry_price": entry
            "exit_price": exit_price
            "close_ts": int(trade.get("exit_ts_ms", 0) or get_ny_time_millis())
        }

    def _publish_closed_trade(self, decision: DecisionRecord, metrics: Dict[str, Any], trade: Dict[str, Any]):
        """
        Publishes to trades:closed for monitoring.
        """
        out = {
            "sid": decision.sid
            "symbol": decision.symbol
            "result": metrics["result"]
            "r_multiple": str(metrics["r_multiple"])
            "pnl": str(metrics["pnl"])
            "rule_score": str(decision.rule_score)
            "ml_prob": str(decision.ml_prob)
            "final_permit": str(decision.final_permit)
            "ts_decision": str(decision.ts)
            "ts_close": str(metrics["close_ts"])
        }
        self.redis.xadd("trades:closed", out, maxlen=10000)

    def _publish_ml_replay(self, decision: DecisionRecord, metrics: Dict[str, Any], trade: Dict[str, Any]):
        """
        Publishes to ml_replay_inputs_v1 for dataset collection.
        """
        data = {
            "decision": decision.to_dict()
            "trade": trade
            "label": metrics
        }
        self.redis.xadd("ml_replay_inputs_v1", {"json": json.dumps(data)}, maxlen=200000)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    service = LabelJoinerService()
    try:
        service.run()
    except KeyboardInterrupt:
        pass
