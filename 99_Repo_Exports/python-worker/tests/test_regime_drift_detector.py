import pytest
from services.regime_drift_detector import RegimeDriftDetector

def test_regime_drift_detector_clean():
    detector = RegimeDriftDetector(delta=0.01, lambda_=5.0)
    # Give a lot of wins (1.0), shouldn't trigger drift by default as performance is good
    for _ in range(50):
        drifted = detector.update(1.0)
        assert not drifted

def test_regime_drift_detector_detects_drop():
    detector = RegimeDriftDetector(delta=0.01, lambda_=1.0)
    # Establish high initial mean
    for _ in range(10):
        detector.update(1.0)
    
    # Simulate sudden persistent drop in win_rate
    drift_triggered = False
    for _ in range(50):
        if detector.update(0.0):
            drift_triggered = True
            break
            
    assert drift_triggered, "Drift detector failed to trigger on performance drop"
