import os
import sys

import pytest

# Ensure repo root is on sys.path for `services.*` imports when running tests from subfolders.
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from services.orderflow.probability_utils_v1 import extract_prob as extract_prob_main
from services.orderflow.probability_utils_v1 import extract_prob_with_source as extract_main
from tick_flow_full.services.orderflow.probability_utils_v1 import extract_prob as extract_prob_tick
from tick_flow_full.services.orderflow.probability_utils_v1 import extract_prob_with_source as extract_tick


@pytest.mark.parametrize(
    "decision, expected_p, expected_src",
    [
        ({"ml": {"p_edge": 0.72, "p": 0.55, "score": 0.33, "p_min": 0.95}}, 0.72, "p_edge"),
        ({"ml": {"p": 0.61, "score": 0.12, "p_min": 0.20}}, 0.61, "p"),
        ({"ml": {"score": 0.44, "p_min": 0.90}}, 0.44, "score"),
        ({"ml": {"score": 1.44, "p_min": 0.10}}, None, "none"),
        ({"ml": {"p_min": 0.55}}, None, "none"),
        ({}, None, "none"),
    ],
)
def test_extract_prob_with_source_main(decision, expected_p, expected_src):
    p, src = extract_main(decision)
    assert p == expected_p
    assert src == expected_src
    assert extract_prob_main(decision) == expected_p


@pytest.mark.parametrize(
    "decision, expected_p, expected_src",
    [
        ({"ml": {"p_edge": 0.72, "p": 0.55, "score": 0.33, "p_min": 0.95}}, 0.72, "p_edge"),
        ({"ml": {"p": 0.61, "score": 0.12, "p_min": 0.20}}, 0.61, "p"),
        ({"ml": {"score": 0.44, "p_min": 0.90}}, 0.44, "score"),
        ({"ml": {"score": 1.44, "p_min": 0.10}}, None, "none"),
        ({"ml": {"p_min": 0.55}}, None, "none"),
        ({}, None, "none"),
    ],
)
def test_extract_prob_with_source_tick(decision, expected_p, expected_src):
    p, src = extract_tick(decision)
    assert p == expected_p
    assert src == expected_src
    assert extract_prob_tick(decision) == expected_p
