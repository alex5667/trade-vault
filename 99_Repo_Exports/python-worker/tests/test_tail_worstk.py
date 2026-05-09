from core.tail_worstk import WorstK


def test_worstk_keeps_smallest():
    w = WorstK(k=3)
    for x in [5, 1, 4, 2, 3]:
        w.push(x)
    mu, sd = w.mean_std()
    # worst 3 are 1,2,3 => mean=2, var = ((1-2)^2 + (2-2)^2 + (3-2)^2)/3 = (1+0+1)/3 = 0.666
    assert abs(mu - 2.0) < 1e-9
    assert w.n() == 3

def test_worstk_serialization():
    w = WorstK(k=2)
    w.push(10.0)
    w.push(5.0)
    w.push(15.0) # dropped

    d = w.to_dict()
    w2 = WorstK.from_dict(d)

    assert w2.k == 2
    assert w2.n() == 2
    mu, _ = w2.mean_std()
    assert abs(mu - 7.5) < 1e-9
