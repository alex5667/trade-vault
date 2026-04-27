from pathlib import Path

import yaml


def _load_yaml(p: Path) -> dict:
    txt = p.read_text("utf-8", errors="ignore")
    data = yaml.safe_load(txt)
    assert isinstance(data, dict)
    return data


def test_liqmap_levels_overlay_alerts_yaml_v1_exists_and_parses():
    root = Path(__file__).resolve().parents[1]  # orderflow_services/
    for p in [
        root / "prometheus_alerts_liqmap_levels_overlay_v1.yml",
        root.parent / "tick_flow_full" / "orderflow_services" / "prometheus_alerts_liqmap_levels_overlay_v1.yml",
    ]:
        assert p.exists(), f"missing {p}"
        data = _load_yaml(p)
        assert "groups" in data and isinstance(data["groups"], list)
        assert data["groups"][0]["name"] == "liqmap_levels_overlay_v1"
        rules = data["groups"][0]["rules"]
        names = {r.get("alert") for r in rules}
        assert "LiqMapLevelsOverlayEnabledButNeverAppliedWarn" in names
        assert "LiqMapLevelsOverlayEnabledButNeverAppliedCrit" in names
