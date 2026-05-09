import random

from core.quantile_p2 import P2Quantile


def test_p2_quantile_uniform_converges():
    # Test q75 for uniform(0, 1) -> should be close to 0.75
    q = P2Quantile(p=0.75)
    xs = [random.random() for _ in range(5000)]
    for x in xs:
        q.update(x)
    est = q.value()
    assert 0.70 <= est <= 0.80

def test_p2_quantile_warmup():
    q = P2Quantile(p=0.5)
    # Warmup with exactly 5 items
    data = [1.0, 5.0, 3.0, 2.0, 4.0]
    for x in data:
        q.update(x)

    assert q.ready()
    # For n=5, q3 (median) should be 3.0
    assert q.value() == 3.0

def test_p2_quantile_serialization():
    q = P2Quantile(p=0.95)
    for i in range(100):
        q.update(float(i))

    d = q.to_state()
    q2 = P2Quantile.from_state(d)

    assert q2.p == 0.95
    assert q2._count == 100
    assert abs(q.value() - q2.value()) < 1e-9
