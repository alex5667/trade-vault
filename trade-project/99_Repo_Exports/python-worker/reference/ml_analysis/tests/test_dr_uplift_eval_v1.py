import math
import random

# Ad-hoc path setup for testing if running directly
# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from ml_analysis.tools.dr_uplift_eval_v1 import DRInputs, _dr_pseudo


def test_dr_uplift_unbiased_simple():
    rng = random.Random(123)
    p1 = 0.3
    p0_true = 0.40
    p1_true = 0.45
    q0 = 0.40
    q1 = 0.45

    n = 20000
    samples = []
    for _ in range(n):
        a = 1 if rng.random() < p1 else 0
        y = 1.0 if rng.random() < (p1_true if a == 1 else p0_true) else 0.0
        pseudo = _dr_pseudo(DRInputs(y=y, a=a, p1=p1, q0=q0, q1=q1))
        assert math.isfinite(pseudo)
        samples.append(pseudo)

    est = sum(samples) / n
    # true ATE = 0.05
    assert abs(est - (p1_true - p0_true)) < 0.01
