"""Unit tests for assert_latency_budget.py"""
import json
import os
import tempfile

import pytest

from tools.assert_latency_budget import main


def test_assert_latency_budget_pass():
    """Test that valid benchmark passes."""
    bench_data = {
        "n": 20000,
        "throughput_calls_per_s": 5000.0,
        "p50_us": 150.0,
        "p95_us": 200.0,
        "p99_us": 250.0,
        "max_us": 500.0,
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(bench_data, f)
        bench_path = f.name
    
    try:
        import sys
        old_argv = sys.argv
        sys.argv = [
            "test",
            "--bench-json", bench_path,
            "--p99-us-max", "3000",
            "--throughput-min", "2000",
        ]
        
        try:
            # Should not raise
            main()
        finally:
            sys.argv = old_argv
    finally:
        os.unlink(bench_path)


def test_assert_latency_budget_fail_p99():
    """Test that exceeding p99 threshold fails."""
    bench_data = {
        "n": 20000,
        "throughput_calls_per_s": 5000.0,
        "p99_us": 3500.0,  # exceeds 3000
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(bench_data, f)
        bench_path = f.name
    
    try:
        import sys
        old_argv = sys.argv
        sys.argv = [
            "test",
            "--bench-json", bench_path,
            "--p99-us-max", "3000",
            "--throughput-min", "2000",
        ]
        
        try:
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code != 0
        finally:
            sys.argv = old_argv
    finally:
        os.unlink(bench_path)


def test_assert_latency_budget_fail_throughput():
    """Test that low throughput fails."""
    bench_data = {
        "n": 20000,
        "throughput_calls_per_s": 1500.0,  # below 2000
        "p99_us": 2500.0,
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(bench_data, f)
        bench_path = f.name
    
    try:
        import sys
        old_argv = sys.argv
        sys.argv = [
            "test",
            "--bench-json", bench_path,
            "--p99-us-max", "3000",
            "--throughput-min", "2000",
        ]
        
        try:
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code != 0
        finally:
            sys.argv = old_argv
    finally:
        os.unlink(bench_path)

