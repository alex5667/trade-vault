# Confidence Calculation Files Inventory

## Copied: 2026-02-19 22:19

### Core Components (8 files)

1. **crypto_orderflow_detectors.py** (from `python-worker/core/`)
   - DeltaSpikeDetector - Z-score calculation
   - OBIDetector - Order book imbalance
   - AbsorptionDetector - Absorption detection
   - IcebergDetector - Iceberg order detection

2. **signal_confidence.py** (from `python-worker/services/`)
   - ConfidenceScorer - Main confidence calculation
   - ConfidenceConfig - Configuration dataclass
   - Confidence scoring logic with Z-score, OBI, spread

3. **tick_processor.py** (from `python-worker/services/orderflow/components/`)
   - TickProcessor - Processes each tick
   - DN-GATE implementation
   - Delta event handling

4. **orderflow_strategy.py** (from `python-worker/services/`)
   - OrderFlowStrategy - Main strategy coordinator

5. **delta_notional_calibrator.py** (from `python-worker/core/`)
   - DeltaNotionalCalibrator - Auto-tunes DN-GATE thresholds

6. **orderflow_runtime.py** (from `python-worker/core/`)
   - SymbolRuntime - Per-symbol state management

7. **confidence_utils.py** (from `python-worker/core/`)
   - Utility functions for confidence calculation

8. **unified_signal_formatter.py** (from `python-worker/core/`)
   - Formats final signals for Redis/Websocket

### Configuration (3 files)

9. **configuration.py** (from `python-worker/services/orderflow/`)
   - OrderFlowConfigLoader - Loads config from Redis + ENV

10. **instrument_config.py** (from `python-worker/core/`)
    - OrderFlowConfig - Per-symbol configuration dataclass

11. **docker-compose-crypto-orderflow.yml**
    - Full docker-compose configuration

### Helpers (3 files)

12. **quantile_p2.py** (from `python-worker/core/`)
    - P2Quantile - Online quantile estimation

13. **pnl_math.py** (from `python-worker/services/`)
    - Symbol specifications and PnL calculations

14. **test_zscore_calculation.py**
    - Unit tests for Z-score calculation

### Documentation (1 file)

15. **README.md**
    - Comprehensive documentation

## Related Artifacts (in brain folder)

- zscore_analysis.md - Z-score calculation analysis
- walkthrough.md - DN-GATE threshold adjustment
- implementation_plan.md - DN-GATE fix plan
- task.md - Task tracking
