"""
Основные компоненты Crypto OrderFlow.

Этот пакет содержит ключевые компоненты и функциональность
для CryptoOrderFlowHandler.
"""

from .crypto_orderflow_calibration import ConfidenceCalibratorCfg, RollingPercentileCalibrator
from .crypto_orderflow_components import (
    Emitter,
    L2ConfirmAbsorption,
    L2ConfirmBreakout,
    MicrostructureEngine,
    RealizedSpreadTracker,
    RegimeDetector,
    RegimeDetectorCfg,
    ScoreModel,
    TickParser,
    TouchFilter,
)
from .crypto_orderflow_confirmations import (
    L2ConfirmAbsorption as L2ConfirmAbsorptionCrypto,
)
from .crypto_orderflow_confirmations import (
    L2ConfirmBreakout as L2ConfirmBreakoutCrypto,
)
from .crypto_orderflow_confirmations import (
    L2ConfirmCfg,
)
from .crypto_orderflow_detector import CryptoEventDetector, DetectorCfg
from .crypto_orderflow_quality import (
    CompositeValidator,
    ExtremeOptionalFiltersValidator,
    L2AbsorptionValidator,
    L2BreakoutValidator,
    L3OptionalValidator,
    MinIntervalValidator,
    ModeValidator,
    OBIBreakoutValidator,
    OBIFadeValidator,
    PivotsPresentValidator,
    RegimeGateValidator,
    SpreadValidator,
    TouchVetoValidator,
)
from .crypto_orderflow_scoring import CryptoScoreModel, ScoreModelCfg

__all__ = [
    # Components
    'TickParser',
    'RealizedSpreadTracker',
    'MicrostructureEngine',
    'RegimeDetector',
    'RegimeDetectorCfg',
    'L2ConfirmBreakout',
    'L2ConfirmAbsorption',
    'TouchFilter',
    'ScoreModel',
    'Emitter',
    # Detector
    'CryptoEventDetector',
    'DetectorCfg',
    # Quality
    'CompositeValidator',
    'SpreadValidator',
    'MinIntervalValidator',
    'PivotsPresentValidator',
    'ModeValidator',
    'OBIBreakoutValidator',
    'OBIFadeValidator',
    'RegimeGateValidator',
    'L2BreakoutValidator',
    'L2AbsorptionValidator',
    'ExtremeOptionalFiltersValidator',
    'TouchVetoValidator',
    'L3OptionalValidator',
    # Confirmations
    'L2ConfirmBreakoutCrypto',
    'L2ConfirmAbsorptionCrypto',
    'L2ConfirmCfg',
    # Scoring
    'CryptoScoreModel',
    'ScoreModelCfg',
    # Calibration
    'RollingPercentileCalibrator',
    'ConfidenceCalibratorCfg',
]
