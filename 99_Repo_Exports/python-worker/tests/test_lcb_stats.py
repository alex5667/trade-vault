from core.lcb_stats import mean_lcb


def test_mean_lcb_min_n_blocks():
    st = mean_lcb([1.0, 2.0, 3.0], min_n=30)
    assert st.n == 3
    assert st.lcb == float("-inf")


def test_mean_lcb_ok():
    xs = [1.0] * 50
    st = mean_lcb(xs, min_n=30)
    assert st.n == 50
    assert st.mean > 0
    assert st.lcb <= st.mean
