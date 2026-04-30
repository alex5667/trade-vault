"""
Основные компоненты Crypto OrderFlow.

Этот пакет содержит ключевые компоненты и функциональность
для CryptoOrderFlowHandler.
"""

from .crypto_orderflow_components import (
    TickParser
    RealizedSpreadTracker
    MicrostructureEngine
    RegimeDetector
    RegimeDetectorCfg
    L2ConfirmBreakout
    L2ConfirmAbsorption
    TouchFilter
    ScoreModel
    Emitter
)
from .crypto_orderflow_detector import CryptoEventDetector, DetectorCfg
from .crypto_orderflow_quality import (
    CompositeValidator
    SpreadValidator
    MinIntervalValidator
    PivotsPresentValidator
    ModeValidator
    OBIBreakoutValidator
    OBIFadeValidator
    RegimeGateValidator
    L2BreakoutValidator
    L2AbsorptionValidator
    ExtremeOptionalFiltersValidator
    TouchVetoValidator
    L3OptionalValidator
)
from .crypto_orderflow_confirmations import (
    L2ConfirmBreakout as L2ConfirmBreakoutCrypto
    L2ConfirmAbsorption as L2ConfirmAbsorptionCrypto
    L2ConfirmCfg
)
from .crypto_orderflow_scoring import CryptoScoreModel, ScoreModelCfg
from .crypto_orderflow_calibration import RollingPercentileCalibrator, ConfidenceCalibratorCfg

__all__ = [
    # Components
    'TickParser'
    'RealizedSpreadTracker'
    'MicrostructureEngine'
    'RegimeDetector'
    'RegimeDetectorCfg'
    'L2ConfirmBreakout'
    'L2ConfirmAbsorption'
    'TouchFilter'
    'ScoreModel'
    'Emitter'
    # Detector
    'CryptoEventDetector'
    'DetectorCfg'
    # Quality
    'CompositeValidator'
    'SpreadValidator'
    'MinIntervalValidator'
    'PivotsPresentValidator'
    'ModeValidator'
    'OBIBreakoutValidator'
    'OBIFadeValidator'
    'RegimeGateValidator'
    'L2BreakoutValidator'
    'L2AbsorptionValidator'
    'ExtremeOptionalFiltersValidator'
    'TouchVetoValidator'
    'L3OptionalValidator'
    # Confirmations
    'L2ConfirmBreakoutCrypto'
    'L2ConfirmAbsorptionCrypto'
    'L2ConfirmCfg'
    # Scoring
    'CryptoScoreModel'
    'ScoreModelCfg'
    # Calibration
    'RollingPercentileCalibrator'
    'ConfidenceCalibratorCfg'
]
