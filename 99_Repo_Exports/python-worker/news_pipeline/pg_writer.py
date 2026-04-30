# news_pipeline/pg_writer.py
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List

log = logging.getLogger("news_pg_writer")

# Поддержим psycopg3 и psycopg2 (какой установлен — тот и используем)
_PG = None
try:
    import psycopg  # type: ignore
    _PG = "psycopg3"
except Exception:
    try:
        import psycopg2  # type: ignore
        import psycopg2.extras  # type: ignore
        _PG = "psycopg2"
    except Exception:
        _PG = None


class NewsPgWriter:
    """
    Fail-open writer для news_analysis и news_features_symbol.
    Включение: NEWS_PG_DSN и NEWS_PG_WRITE=1
    """

    def __init__(self) -> None:
        self.enabled = os.getenv("NEWS_PG_WRITE", "0").strip() == "1"
        self.dsn = os.getenv("NEWS_PG_DSN", "").strip()
        if not self.enabled or not self.dsn or _PG is None:
            self.enabled = False

    def write_news_analysis_rows(self, rows: List[Dict[str, Any]]) -> None:
        if not self.enabled or not rows:
            return
        try:
            if _PG == "psycopg3":
                self._write_news_analysis_psycopg3(rows)
            else:
                self._write_news_analysis_psycopg2(rows)
        except Exception as e:
            log.exception("write_news_analysis_rows failed: %s", e)

    def write_features_rows(self, rows: List[Dict[str, Any]]) -> None:
        if not self.enabled or not rows:
            return
        try:
            if _PG == "psycopg3":
                self._write_features_psycopg3(rows)
            else:
                self._write_features_psycopg2(rows)
        except Exception as e:
            log.exception("write_features_rows failed: %s", e)

    # ---------------- psycopg3 ----------------

    def _write_news_analysis_psycopg3(self, rows: List[Dict[str, Any]]) -> None:
        import psycopg  # type: ignore

        sql = """
        INSERT INTO news_analysis
          (uid, symbol, ts_ms, source, risk, surprise, confidence, tags_mask, primary_tag, payload_json)
        VALUES
          (%(uid)s, %(symbol)s, %(ts_ms)s, %(source)s, %(risk)s, %(surprise)s, %(confidence)s, %(tags_mask)s, %(primary_tag)s, %(payload_json)s)
        ON CONFLICT (uid, symbol) DO UPDATE SET
          risk=EXCLUDED.risk
          surprise=EXCLUDED.surprise
          confidence=EXCLUDED.confidence
          tags_mask=EXCLUDED.tags_mask
          primary_tag=EXCLUDED.primary_tag
          payload_json=EXCLUDED.payload_json
          inserted_at=now()
        """
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)

    def _write_features_psycopg3(self, rows: List[Dict[str, Any]]) -> None:
        import psycopg  # type: ignore

        sql = """
        INSERT INTO news_features_symbol
          (symbol, ts_ms, risk, surprise, tags_mask, primary_tag, ref, confidence, grade_id, horizon_sec)
        VALUES
          (%(symbol)s, %(ts_ms)s, %(risk)s, %(surprise)s, %(tags_mask)s, %(primary_tag)s, %(ref)s, %(confidence)s, %(grade_id)s, %(horizon_sec)s)
        ON CONFLICT (symbol, ts_ms) DO UPDATE SET
          risk=EXCLUDED.risk
          surprise=EXCLUDED.surprise
          tags_mask=EXCLUDED.tags_mask
          primary_tag=EXCLUDED.primary_tag
          ref=EXCLUDED.ref
          confidence=EXCLUDED.confidence
          grade_id=EXCLUDED.grade_id
          horizon_sec=EXCLUDED.horizon_sec
          inserted_at=now()
        """
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)

    # ---------------- psycopg2 ----------------

    def _write_news_analysis_psycopg2(self, rows: List[Dict[str, Any]]) -> None:
        import psycopg2  # type: ignore
        import psycopg2.extras  # type: ignore

        sql = """
        INSERT INTO news_analysis
          (uid, symbol, ts_ms, source, risk, surprise, confidence, tags_mask, primary_tag, payload_json)
        VALUES %s
        ON CONFLICT (uid, symbol) DO UPDATE SET
          risk=EXCLUDED.risk
          surprise=EXCLUDED.surprise
          confidence=EXCLUDED.confidence
          tags_mask=EXCLUDED.tags_mask
          primary_tag=EXCLUDED.primary_tag
          payload_json=EXCLUDED.payload_json
          inserted_at=now()
        """
        values = [
            (
                r["uid"], r["symbol"], r["ts_ms"], r["source"]
                r["risk"], r["surprise"], r["confidence"]
                r["tags_mask"], r["primary_tag"], json.dumps(r["payload_json"]) if isinstance(r["payload_json"], dict) else r["payload_json"]
            )
            for r in rows
        ]
        with psycopg2.connect(self.dsn) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, sql, values, page_size=200)

    def _write_features_psycopg2(self, rows: List[Dict[str, Any]]) -> None:
        import psycopg2  # type: ignore
        import psycopg2.extras  # type: ignore

        sql = """
        INSERT INTO news_features_symbol
          (symbol, ts_ms, risk, surprise, tags_mask, primary_tag, ref, confidence, grade_id, horizon_sec)
        VALUES %s
        ON CONFLICT (symbol, ts_ms) DO UPDATE SET
          risk=EXCLUDED.risk
          surprise=EXCLUDED.surprise
          tags_mask=EXCLUDED.tags_mask
          primary_tag=EXCLUDED.primary_tag
          ref=EXCLUDED.ref
          confidence=EXCLUDED.confidence
          grade_id=EXCLUDED.grade_id
          horizon_sec=EXCLUDED.horizon_sec
          inserted_at=now()
        """
        values = [
            (
                r["symbol"], r["ts_ms"], r["risk"], r["surprise"]
                r["tags_mask"], r["primary_tag"], r["ref"]
                r["confidence"], r["grade_id"], r["horizon_sec"]
            )
            for r in rows
        ]
        with psycopg2.connect(self.dsn) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, sql, values, page_size=200)
