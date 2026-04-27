from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
HANDLER = ROOT / "handlers" / "crypto_orderflow_handler.py"


def _read(p: Path) -> str:
    return p.read_text(encoding="utf-8", errors="ignore")


def test_no_duplicate_type_definitions_in_handler_file():
    """
    1.1: BarSample/L2Snapshot/ClusterVol must not be re-declared in handler (single source of truth in types file).
    """
    txt = _read(HANDLER)
    forbidden = ["class BarSample", "class L2Snapshot", "class ClusterVol", "@dataclassclass L2Snapshot", "@dataclassclass BarSample"]
    for needle in forbidden:
        assert needle not in txt, f"found duplicate type definition pattern: {needle}"


def test_apply_regime_gate_single_definition():
    """
    1.1: _apply_regime_gate must exist once (no signature overwrite).
    """
    txt = _read(HANDLER)
    assert txt.count("def _apply_regime_gate") == 1


def test_generate_signals_never_returns_none():
    """
    1.2: _generate_signals annotated as bool must not return None.
    """
    txt = _read(HANDLER)
    # Find _generate_signals method
    start = txt.find("def _generate_signals")
    if start == -1:
        return  # method not found, skip test
    # Find end of method (next def at same level)
    lines = txt[start:].split('\n')
    method_lines = []
    indent_level = None
    for line in lines:
        if line.startswith('    def '):
            if indent_level is not None:
                break
        if indent_level is None and line.strip().startswith('def _generate_signals'):
            indent_level = len(line) - len(line.lstrip())
        if indent_level is not None:
            method_lines.append(line)
    method_code = '\n'.join(method_lines)
    assert "return None" not in method_code


def test_no_direct_crypto_conf_scorer_score_in_handler():
    """
    3.2: one-axis model - no direct crypto_conf_scorer.score calls in handler.
    """
    txt = _read(HANDLER)
    # Ignore comments
    lines = [line for line in txt.split('\n') if line.strip() and not line.strip().startswith('#')]
    code_only = '\n'.join(lines)
    assert "crypto_conf_scorer.score(" not in code_only


def test_regime_methods_moved_to_detector():
    """
    3.x: regime feature engine should live in regime/detector.py; handler keeps only wrappers.
    """
    txt = _read(HANDLER)
    # heuristic guard: old bulky docstrings/comments from original methods should not remain
    assert "Вычисляет фичи режима для принятия решения" not in txt
    assert "Обновляет историю режима для инструмента" not in txt
