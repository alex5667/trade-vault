# python-worker/tests/test_pg_writer_sql.py
from news_pipeline.postgres_writer import NewsPostgresWriter

def test_writer_disabled_without_dsn(monkeypatch):
    monkeypatch.delenv("NEWS_PG_DSN", raising=False)
    w = NewsPostgresWriter()
    assert w.enabled is False
