#!/usr/bin/env python3
"""
Quick integration test for Cost Edge Gate + Enhanced Confidence Thresholds.

This script verifies that:
1. Modules can be imported
2. Configuration can be loaded from ENV
3. Filters work correctly
4. Symbol-specific thresholds apply

Run: python3 test_integration.py
"""

import os
import sys
from typing import Any


def setup_test_env():
    """Set up test environment variables."""
    os.environ.update({
        # Cost Edge Gate
        "EDGE_COST_GATE_ENABLED": "1",
        "EDGE_COST_K": "4.0",
        "EDGE_COST_K_BTCUSDT": "5.0",
        "EDGE_COST_K_ETHUSDT": "4.5",
        "EDGE_FEES_BPS_DEFAULT": "8.0",
        "EDGE_SLIPPAGE_BPS_DEFAULT": "4.0",
        "EDGE_SLIPPAGE_USE_SPREAD_HALF": "1",
        "EDGE_EXPECTED_MOVE_MODE": "tp1",
        "LOG_EDGE_VETO": "1",
        
        # Enhanced Confidence
        "MIN_CONF_DEFAULT": "70",
        "MIN_CONF_BTCUSDT": "75",
        "MIN_CONF_ETHUSDT": "72",
        "MIN_CONF_FACTOR_DEFAULT": "0.45",
        "MIN_CONF_FACTOR_BTCUSDT": "0.55",
        "MIN_CONF_FACTOR_ETHUSDT": "0.52",
    })


class MockContext:
    """Mock signal context for testing."""
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


def test_imports():
    """Test 1: Verify modules can be imported."""
    print("Test 1: Importing modules...")
    try:
        from handlers.crypto_orderflow.core.cost_edge_gate import (
            CostEdgeGate, CostEdgeConfig
        )
        from handlers.crypto_orderflow.core.confidence_threshold import (
            ConfidenceThresholdFilter, ConfidenceThresholdConfig
        )
        print("✅ All imports successful")
        return True
    except Exception as e:
        print(f"❌ Import failed: {e}")
        return False


def test_config_loading():
    """Test 2: Verify configuration loads from ENV."""
    print("\nTest 2: Loading configuration from ENV...")
    try:
        from handlers.crypto_orderflow.core.cost_edge_gate import CostEdgeConfig
        from handlers.crypto_orderflow.core.confidence_threshold import ConfidenceThresholdConfig
        
        # Load configs
        cost_cfg = CostEdgeConfig.from_env()
        conf_cfg = ConfidenceThresholdConfig.from_env()
        
        # Verify cost edge config
        assert cost_cfg.enabled == True, "Cost gate should be enabled"
        assert cost_cfg.default_cost_k == 4.0, "Default K should be 4.0"
        assert cost_cfg.symbol_cost_k.get("BTCUSDT") == 5.0, "BTC K should be 5.0"
        assert cost_cfg.symbol_cost_k.get("ETHUSDT") == 4.5, "ETH K should be 4.5"
        assert cost_cfg.fees_bps == 8.0, "Fees should be 8.0 bps"
        assert cost_cfg.slippage_bps == 4.0, "Slippage should be 4.0 bps"
        assert cost_cfg.edge_mode == "tp1", "Edge mode should be tp1"
        
        # Verify confidence config
        assert conf_cfg.min_conf_default == 70.0, "Default min conf should be 70"
        assert conf_cfg.min_conf_by_symbol.get("BTCUSDT") == 75.0, "BTC min conf should be 75"
        assert conf_cfg.min_conf_by_symbol.get("ETHUSDT") == 72.0, "ETH min conf should be 72"
        assert conf_cfg.min_conf_factor_default == 0.45, "Default factor should be 0.45"
        assert conf_cfg.min_conf_factor_by_symbol.get("BTCUSDT") == 0.55, "BTC factor should be 0.55"
        
        print("✅ Configuration loaded correctly")
        print(f"   Cost gate: enabled={cost_cfg.enabled}, K={cost_cfg.default_cost_k}")
        print(f"   BTC: K={cost_cfg.symbol_cost_k.get('BTCUSDT')}, min_conf={conf_cfg.min_conf_by_symbol.get('BTCUSDT')}")
        print(f"   ETH: K={cost_cfg.symbol_cost_k.get('ETHUSDT')}, min_conf={conf_cfg.min_conf_by_symbol.get('ETHUSDT')}")
        return True
    except AssertionError as e:
        print(f"❌ Config validation failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Config loading failed: {e}")
        return False


def test_cost_edge_gate():
    """Test 3: Verify cost edge gate filtering."""
    print("\nTest 3: Testing Cost Edge Gate...")
    try:
        from handlers.crypto_orderflow.core.cost_edge_gate import CostEdgeGate
        
        gate = CostEdgeGate.from_env()
        
        # Test case 1: Good signal (should pass)
        ctx_pass = MockContext(
            tp1=50200,  # 200 bps move
            entry=50000,
            side="LONG",
        )
        result_pass = gate.evaluate(ctx_pass, "BTCUSDT", entry_price=50000)
        assert result_pass.passed, "Signal with 200 bps edge should pass"
        print(f"✅ Good signal passed: edge={result_pass.expected_edge_bps:.1f}bps >= required={result_pass.required_edge_bps:.1f}bps")
        
        # Test case 2: Bad signal (should fail)
        ctx_fail = MockContext(
            tp1=50020,  # Only 40 bps move
            entry=50000,
            side="LONG",
        )
        result_fail = gate.evaluate(ctx_fail, "BTCUSDT", entry_price=50000)
        assert not result_fail.passed, "Signal with 40 bps edge should fail for BTC (needs 60 bps)"
        print(f"✅ Bad signal rejected: edge={result_fail.expected_edge_bps:.1f}bps < required={result_fail.required_edge_bps:.1f}bps")
        
        # Test case 3: Symbol-specific thresholds
        result_btc = gate.evaluate(ctx_fail, "BTCUSDT", entry_price=50000)
        result_other = gate.evaluate(ctx_fail, "ADAUSDT", entry_price=1.0)
        assert result_btc.cost_multiplier == 5.0, "BTC should use K=5.0"
        assert result_other.cost_multiplier == 4.0, "Other symbols should use K=4.0"
        print(f"✅ Symbol-specific K applied: BTC={result_btc.cost_multiplier}, ADA={result_other.cost_multiplier}")
        
        return True
    except AssertionError as e:
        print(f"❌ Cost edge gate test failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Cost edge gate test error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_confidence_threshold():
    """Test 4: Verify confidence threshold filtering."""
    print("\nTest 4: Testing Confidence Threshold Filter...")
    try:
        from handlers.crypto_orderflow.core.confidence_threshold import ConfidenceThresholdFilter
        
        filter = ConfidenceThresholdFilter.from_env()
        
        # Test case 1: Good BTC signal (should pass)
        result_pass = filter.evaluate(
            confidence_pct=80.0,
            conf_factor=0.60,
            symbol="BTCUSDT"
        )
        assert result_pass.passed, "BTC signal with conf=80, factor=0.60 should pass"
        print(f"✅ Good BTC signal passed: conf={result_pass.confidence_pct:.1f} >= {result_pass.min_conf_threshold:.1f}")
        
        # Test case 2: Bad BTC signal (should fail on confidence)
        result_fail_conf = filter.evaluate(
            confidence_pct=72.0,  # Below BTC threshold of 75
            conf_factor=0.60,
            symbol="BTCUSDT"
        )
        assert not result_fail_conf.passed, "BTC signal with conf=72 should fail"
        print(f"✅ Bad BTC signal rejected: conf={result_fail_conf.confidence_pct:.1f} < {result_fail_conf.min_conf_threshold:.1f}")
        
        # Test case 3: Bad BTC signal (should fail on factor)
        result_fail_factor = filter.evaluate(
            confidence_pct=80.0,
            conf_factor=0.50,  # Below BTC factor threshold of 0.55
            symbol="BTCUSDT"
        )
        assert not result_fail_factor.passed, "BTC signal with factor=0.50 should fail"
        print(f"✅ Bad BTC signal rejected: factor={result_fail_factor.conf_factor:.3f} < {result_fail_factor.min_conf_factor_threshold:.3f}")
        
        # Test case 4: Symbol-specific thresholds
        result_btc = filter.evaluate(confidence_pct=72.0, conf_factor=0.50, symbol="BTCUSDT")
        result_eth = filter.evaluate(confidence_pct=72.0, conf_factor=0.50, symbol="ETHUSDT")
        result_other = filter.evaluate(confidence_pct=72.0, conf_factor=0.50, symbol="ADAUSDT")
        
        assert not result_btc.passed, "BTC with conf=72 should fail"
        assert not result_eth.passed, "ETH with conf=72 should fail"
        assert result_other.passed, "ADA with conf=72 should pass"
        print(f"✅ Symbol-specific thresholds: BTC={result_btc.min_conf_threshold:.0f}, ETH={result_eth.min_conf_threshold:.0f}, ADA={result_other.min_conf_threshold:.0f}")
        
        return True
    except AssertionError as e:
        print(f"❌ Confidence threshold test failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Confidence threshold test error: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("=" * 70)
    print("Cost Edge Gate + Enhanced Confidence Integration Tests")
    print("=" * 70)
    
    # Setup
    setup_test_env()
    
    # Add project root to path
    project_root = os.path.dirname(os.path.abspath(__file__))
    python_worker = os.path.join(project_root, "python-worker")
    if python_worker not in sys.path:
        sys.path.insert(0, python_worker)
    
    # Run tests
    results = []
    results.append(("Imports", test_imports()))
    results.append(("Config Loading", test_config_loading()))
    results.append(("Cost Edge Gate", test_cost_edge_gate()))
    results.append(("Confidence Threshold", test_confidence_threshold()))
    
    # Summary
    print("\n" + "=" * 70)
    print("Test Summary")
    print("=" * 70)
    
    for test_name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} - {test_name}")
    
    total = len(results)
    passed = sum(1 for _, p in results if p)
    
    print("=" * 70)
    print(f"Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! Integration is ready for deployment.")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Please review errors above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())

