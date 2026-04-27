from orderflow_services.route_incident_rca_mirror_bundle_rca_bridge_v3_12 import trigger_decision


def test_trigger_decision_vertex_healthy():
    decision = trigger_decision(
        severity="critical",
        mode="AUTO",
        vertex_degraded=False,
        bundle_size=1000,
        max_bytes=131072,
        require_vertex_degraded=True,
    )
    assert decision == "ROUTE_VERTEX"


def test_trigger_decision_vertex_degraded():
    decision = trigger_decision(
        severity="critical",
        mode="AUTO",
        vertex_degraded=True,
        bundle_size=1000,
        max_bytes=131072,
        require_vertex_degraded=True,
    )
    assert decision == "ROUTE_LOCAL"


def test_trigger_decision_too_large():
    decision = trigger_decision(
        severity="critical",
        mode="AUTO",
        vertex_degraded=False,
        bundle_size=200000,
        max_bytes=131072,
        require_vertex_degraded=True,
    )
    assert decision == "REJECT"


def test_trigger_decision_disabled():
    decision = trigger_decision(
        severity="critical",
        mode="DISABLED",
        vertex_degraded=False,
        bundle_size=1000,
        max_bytes=131072,
        require_vertex_degraded=True,
    )
    assert decision == "REJECT"


def test_trigger_decision_local_only():
    decision = trigger_decision(
        severity="critical",
        mode="LOCAL_ONLY",
        vertex_degraded=False,
        bundle_size=1000,
        max_bytes=131072,
        require_vertex_degraded=True,
    )
    assert decision == "ROUTE_LOCAL"


def test_trigger_decision_vertex_only():
    decision = trigger_decision(
        severity="critical",
        mode="VERTEX_ONLY",
        vertex_degraded=True,
        bundle_size=1000,
        max_bytes=131072,
        require_vertex_degraded=True,
    )
    assert decision == "ROUTE_VERTEX"
