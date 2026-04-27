from orderflow_services.ofc_contextual_runtime_health_exporter_v1 import export_once


def test_export_once_accepts_redis_summary_args(tmp_path):
    state = tmp_path / 'state.json'
    state.write_text('{}', encoding='utf-8')
    data = export_once(str(state), redis_url='', summary_key='')
    assert isinstance(data, dict)
