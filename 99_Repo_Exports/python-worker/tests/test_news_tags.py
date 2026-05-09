from news_pipeline.tags import pick_primary_tag, tags_to_mask


def test_tags_mask():
    m = tags_to_mask(["cpi","risk_off"])
    assert (m & (1 << 0)) != 0
    assert (m & (1 << 7)) != 0

def test_primary_tag():
    tid = pick_primary_tag(["macro","cpi"])
    assert tid == 1
