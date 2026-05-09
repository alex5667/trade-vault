
from services.orderflow import metrics


def test_metrics_objects_exist():
    """
    Smoke test to ensure critical CVD Reclaim and OBI metrics are defined.
    """
    # Check CVD Reclaim metrics
    assert hasattr(metrics, "cvd_reclaim_eval_total"), "cvd_reclaim_eval_total missing"
    assert hasattr(metrics, "cvd_reclaim_ok_total"), "cvd_reclaim_ok_total missing"
    assert hasattr(metrics, "cvd_reclaim_applied_total"), "cvd_reclaim_applied_total missing"
    assert hasattr(metrics, "cvd_reclaim_no_data_total"), "cvd_reclaim_no_data_total missing"

    # Check Gauges
    assert hasattr(metrics, "cvd_reclaim_ratio_gauge"), "cvd_reclaim_ratio_gauge missing"
    assert hasattr(metrics, "cvd_reclaim_age_ms_gauge"), "cvd_reclaim_age_ms_gauge missing"
    assert hasattr(metrics, "cvd_reclaim_window_ms_gauge"), "cvd_reclaim_window_ms_gauge missing"

    # Check OBI metric
    assert hasattr(metrics, "obi_stability_score_gauge"), "obi_stability_score_gauge missing"
