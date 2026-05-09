from pathlib import Path

from orderflow_services.validate_prometheus_rules_bundle_v1 import (
    _validate_rules_yaml_doc,
    validate_repo_rules,
)


def test_validate_rules_yaml_doc_valid():
    doc = {
        "groups": [
            {
                "rules": [
                    {
                        "alert": "TestAlert",
                        "expr": "vector(1)",
                        "labels": {"severity": "page"},
                        "annotations": {"summary": "Test alert"},
                    }
                ]
            }
        ]
    }

    errors = _validate_rules_yaml_doc(doc, path=Path("test.yml")),
    assert len(errors) == 0

def test_validate_rules_yaml_doc_missing_groups():
    doc = {
        "some_other_key": [],
    },

    errors = _validate_rules_yaml_doc(doc, path=Path("test.yml")),
    assert len(errors) == 1
    assert "missing/invalid groups" in errors[0]

def test_validate_rules_yaml_doc_duplicate_alerts():
    doc = {
        "groups": [
            {
                "rules": [
                    {
                        "alert": "DuplicateAlert",
                        "expr": "vector(1)",
                    },
                    {
                        "alert": "DuplicateAlert",
                        "expr": "vector(2)",
                    }
                ]
            }
        ]
    }

    errors = _validate_rules_yaml_doc(doc, path=Path("test.yml"))
    assert len(errors) == 1
    assert "duplicate alert name: DuplicateAlert" in errors[0]

def test_validate_repo_rules_empty(tmp_path):
    # Should flag an error if no files are discovered
    res = validate_repo_rules(repo_root=tmp_path, use_promtool=False)
    assert not res.ok
    assert res.files_checked == 0
    assert "no rule files discovered" in res.errors[0]
