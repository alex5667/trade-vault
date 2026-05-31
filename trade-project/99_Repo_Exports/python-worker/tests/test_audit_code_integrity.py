"""Test code integrity audit: detect duplicate def/class."""

import os
import tempfile

from tools.audit_code_integrity import scan_file


def test_scan_file_no_duplicates():
    """Test scanning file without duplicates."""
    code = """
def func1():
    pass

class Class1:
    pass

def func2():
    pass
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(code)
        fpath = f.name

    try:
        result = scan_file(fpath)
        assert "error" not in result
        assert len(result.get("dup_defs", {})) == 0
    finally:
        os.unlink(fpath)


def test_scan_file_with_duplicates():
    """Test scanning file with duplicate def."""
    code = """
def duplicate_func():
    pass

class MyClass:
    pass

def duplicate_func():
    pass
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(code)
        fpath = f.name

    try:
        result = scan_file(fpath)
        assert "error" not in result
        assert "duplicate_func" in result.get("dup_defs", {})
        dup_info = result["dup_defs"]["duplicate_func"]
        assert len(dup_info) == 2  # two definitions
    finally:
        os.unlink(fpath)


def test_scan_file_parse_error():
    """Test handling of syntax errors."""
    code = """
def broken_func(
    # missing closing paren
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(code)
        fpath = f.name

    try:
        result = scan_file(fpath)
        assert "error" in result
        assert "parse_error" in result["error"]
    finally:
        os.unlink(fpath)
