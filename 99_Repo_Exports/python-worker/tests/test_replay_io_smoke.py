from core.replay_io import topdiff

def test_topdiff_smoke():
    base = [{'decision': 1, 'score': 0.1}, {'decision': 0, 'score': 0.2}]
    cur  = [{'decision': 1, 'score': 0.1}, {'decision': 1, 'score': 0.2}]
    n, diffs = topdiff(base, cur, keys=['decision','score'])
    assert n == 1
    assert diffs[0].idx == 1

