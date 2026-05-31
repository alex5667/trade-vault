from __future__ import annotations
from core.redis_keys import RedisStreams as RS

"""
Тесты для tools/golden_replay_of_confirm_from_redis.py.

Проверяет детерминированный контроль OF golden replay с уведомлениями.
"""


import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tools.golden_replay_of_confirm_from_redis import (
    _notify,
    _safe_load_json,
    run,
)


class TestRun:
    """Тесты для функции run()."""

    def test_run_success(self):
        """Тест успешного выполнения команды."""
        # Команда, которая успешно завершается
        rc = run(["python", "-c", "print('test'); exit(0)"])
        assert rc == 0

    def test_run_failure(self):
        """Тест неуспешного выполнения команды."""
        # Команда, которая завершается с ошибкой
        rc = run(["python", "-c", "exit(1)"])
        assert rc == 1

    def test_run_captures_stdout(self, capsys):
        """Тест захвата stdout."""
        run(["python", "-c", "print('hello world')"])
        captured = capsys.readouterr()
        assert "hello world" in captured.out

    def test_run_captures_stderr(self, capsys):
        """Тест захвата stderr (перенаправляется в stdout)."""
        run(["python", "-c", "import sys; sys.stderr.write('error\\n')"])
        captured = capsys.readouterr()
        # stderr перенаправляется в stdout
        assert "error" in captured.out or "error" in captured.err


class TestSafeLoadJson:
    """Тесты для функции _safe_load_json()."""

    def test_safe_load_json_valid(self):
        """Тест загрузки валидного JSON."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
            json.dump({"key": "value", "number": 42}, f)
            path = f.name

        try:
            result = _safe_load_json(path)
            assert result == {"key": "value", "number": 42}
        finally:
            Path(path).unlink(missing_ok=True)

    def test_safe_load_json_invalid(self):
        """Тест загрузки невалидного JSON."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
            f.write("invalid json content")
            path = f.name

        try:
            result = _safe_load_json(path)
            assert result == {}
        finally:
            Path(path).unlink(missing_ok=True)

    def test_safe_load_json_not_dict(self):
        """Тест загрузки JSON, который не является словарём."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
            json.dump([1, 2, 3], f)
            path = f.name

        try:
            result = _safe_load_json(path)
            assert result == {}
        finally:
            Path(path).unlink(missing_ok=True)

    def test_safe_load_json_missing_file(self):
        """Тест загрузки несуществующего файла."""
        result = _safe_load_json("/nonexistent/path/to/file.json")
        assert result == {}

    def test_safe_load_json_empty_file(self):
        """Тест загрузки пустого файла."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
            path = f.name

        try:
            result = _safe_load_json(path)
            assert result == {}
        finally:
            Path(path).unlink(missing_ok=True)


class TestNotify:
    """Тесты для функции _notify()."""

    @patch("tools.golden_replay_of_confirm_from_redis.redis.Redis")
    def test_notify_success(self, mock_redis_class):
        """Тест успешной отправки уведомления."""
        mock_redis = MagicMock()
        mock_redis_class.from_url.return_value = mock_redis

        _notify("redis://localhost:6379/0", RS.NOTIFY_TELEGRAM, "test message")

        mock_redis_class.from_url.assert_called_once_with(
            "redis://localhost:6379/0", decode_responses=True
        )
        mock_redis.xadd.assert_called_once()
        call_args = mock_redis.xadd.call_args
        assert call_args[0][0] == RS.NOTIFY_TELEGRAM
        assert "type" in call_args[0][1]
        assert call_args[0][1]["type"] == "alert"
        assert call_args[0][1]["subtype"] == "of_confirm_replay"
        assert "text" in call_args[0][1]
        assert call_args[0][1]["text"] == "test message"
        assert call_args[1]["maxlen"] == 200000
        assert call_args[1]["approximate"] is True

    @patch("tools.golden_replay_of_confirm_from_redis.redis.Redis")
    def test_notify_redis_error(self, mock_redis_class):
        """Тест обработки ошибки Redis (fail-open)."""
        mock_redis_class.from_url.side_effect = Exception("Redis connection error")

        # Не должно падать
        _notify("redis://localhost:6379/0", RS.NOTIFY_TELEGRAM, "test message")

    @patch("tools.golden_replay_of_confirm_from_redis.redis.Redis")
    def test_notify_xadd_error(self, mock_redis_class):
        """Тест обработки ошибки xadd (fail-open)."""
        mock_redis = MagicMock()
        mock_redis_class.from_url.return_value = mock_redis
        mock_redis.xadd.side_effect = Exception("XADD error")

        # Не должно падать
        _notify("redis://localhost:6379/0", RS.NOTIFY_TELEGRAM, "test message")


class TestMainIntegration:
    """Интеграционные тесты для main() функции."""

    @patch("tools.golden_replay_of_confirm_from_redis.run")
    @patch("tools.golden_replay_of_confirm_from_redis._safe_load_json")
    @patch("tools.golden_replay_of_confirm_from_redis._notify")
    def test_main_no_baseline(self, mock_notify, mock_load_json, mock_run):
        """Тест main() без baseline (не должно быть diff и уведомлений)."""
        import sys

        from tools.golden_replay_of_confirm_from_redis import main

        # Мокаем успешные вызовы run
        mock_run.return_value = 0

        # Мокаем аргументы командной строки
        test_args = [
            "golden_replay_of_confirm_from_redis.py",
            "--out-dir",
            "/tmp/test_out",
            "--baseline",
            "",  # Пустой baseline
        ]

        with patch.object(sys, "argv", test_args):
            main()

        # Должно быть 2 вызова run (export + replay)
        assert mock_run.call_count == 2
        # Не должно быть вызовов _notify и _safe_load_json
        mock_notify.assert_not_called()
        mock_load_json.assert_not_called()

    @patch("tools.golden_replay_of_confirm_from_redis.run")
    @patch("tools.golden_replay_of_confirm_from_redis._safe_load_json")
    @patch("tools.golden_replay_of_confirm_from_redis._notify")
    def test_main_with_baseline_no_mismatch(self, mock_notify, mock_load_json, mock_run):
        """Тест main() с baseline, но без mismatches."""
        import sys

        from tools.golden_replay_of_confirm_from_redis import main

        # Мокаем успешные вызовы run
        mock_run.return_value = 0

        # Мокаем пустой report (нет mismatches)
        mock_load_json.return_value = {
            "missing_in_baseline": 0,
            "missing_in_candidate": 0,
            "mismatches": 0,
        }

        test_args = [
            "golden_replay_of_confirm_from_redis.py",
            "--out-dir",
            "/tmp/test_out",
            "--baseline",
            "/tmp/baseline.ndjson",
            "--fail-on-mismatch",
            "1",
            "--notify",
            "1",
        ]

        with patch.object(sys, "argv", test_args):
            main()

        # Должно быть 3 вызова run (export + replay + diff)
        assert mock_run.call_count == 3
        # Не должно быть уведомлений (нет mismatches)
        mock_notify.assert_not_called()

    @patch("tools.golden_replay_of_confirm_from_redis.run")
    @patch("tools.golden_replay_of_confirm_from_redis._safe_load_json")
    @patch("tools.golden_replay_of_confirm_from_redis._notify")
    def test_main_with_mismatch_and_notify(self, mock_notify, mock_load_json, mock_run):
        """Тест main() с mismatch и уведомлением."""
        import sys

        from tools.golden_replay_of_confirm_from_redis import main

        # Мокаем успешные вызовы run
        mock_run.return_value = 0

        # Мокаем report с mismatches
        mock_load_json.return_value = {
            "missing_in_baseline": 5,
            "missing_in_candidate": 3,
            "mismatches": 10,
            "mismatch_types": {"ok": 5, "score": 5},
            "top_groups": [("BTCUSDT|reversal", 3), ("ETHUSDT|breakout", 2)],
            "samples": [
                {"k": "key1", "diffs": ["ok"]},
                {"k": "key2", "diffs": ["score"]},
                {"k": "key3", "diffs": ["ok", "score"]},
            ],
        }

        test_args = [
            "golden_replay_of_confirm_from_redis.py",
            "--out-dir",
            "/tmp/test_out",
            "--baseline",
            "/tmp/baseline.ndjson",
            "--fail-on-mismatch",
            "1",
            "--notify",
            "1",
            "--notify-stream",
            RS.NOTIFY_TELEGRAM,
            "--redis-url",
            "redis://localhost:6379/0",
        ]

        with patch.object(sys, "argv", test_args):
            try:
                main()
            except SystemExit as e:
                # Ожидается exit(2) при fail_on_mismatch=1
                assert e.code == 2

        # Должно быть 3 вызова run
        assert mock_run.call_count == 3
        # Должно быть уведомление
        mock_notify.assert_called_once()
        call_args = mock_notify.call_args[0]
        assert "MISMATCH" in call_args[2]  # text содержит "MISMATCH"
        assert "missing_in_baseline=5" in call_args[2]
        assert "missing_in_candidate=3" in call_args[2]
        assert "mismatches=10" in call_args[2]

    @patch("tools.golden_replay_of_confirm_from_redis.run")
    @patch("tools.golden_replay_of_confirm_from_redis._safe_load_json")
    @patch("tools.golden_replay_of_confirm_from_redis._notify")
    def test_main_with_mismatch_no_fail(self, mock_notify, mock_load_json, mock_run):
        """Тест main() с mismatch, но fail_on_mismatch=0 (не падает)."""
        import sys

        from tools.golden_replay_of_confirm_from_redis import main

        # Мокаем успешные вызовы run
        mock_run.return_value = 0

        # Мокаем report с mismatches
        mock_load_json.return_value = {
            "missing_in_baseline": 1,
            "missing_in_candidate": 0,
            "mismatches": 0,
        }

        test_args = [
            "golden_replay_of_confirm_from_redis.py",
            "--out-dir",
            "/tmp/test_out",
            "--baseline",
            "/tmp/baseline.ndjson",
            "--fail-on-mismatch",
            "0",  # Не падать
            "--notify",
            "1",
        ]

        with patch.object(sys, "argv", test_args):
            # Не должно падать
            main()

        # Должно быть уведомление (есть mismatch)
        mock_notify.assert_called_once()

    @patch("tools.golden_replay_of_confirm_from_redis.run")
    def test_main_export_failure(self, mock_run):
        """Тест main() при ошибке экспорта (должно падать)."""
        import sys

        from tools.golden_replay_of_confirm_from_redis import main

        # Первый вызов (export) возвращает ошибку
        mock_run.side_effect = [1, 0, 0]

        test_args = [
            "golden_replay_of_confirm_from_redis.py",
            "--out-dir",
            "/tmp/test_out",
        ]

        with patch.object(sys, "argv", test_args):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1

    @patch("tools.golden_replay_of_confirm_from_redis.run")
    def test_main_replay_failure(self, mock_run):
        """Тест main() при ошибке replay (должно падать)."""
        import sys

        from tools.golden_replay_of_confirm_from_redis import main

        # Второй вызов (replay) возвращает ошибку
        mock_run.side_effect = [0, 1, 0]

        test_args = [
            "golden_replay_of_confirm_from_redis.py",
            "--out-dir",
            "/tmp/test_out",
        ]

        with patch.object(sys, "argv", test_args):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1

    @patch("tools.golden_replay_of_confirm_from_redis.run")
    def test_main_diff_failure(self, mock_run):
        """Тест main() при ошибке diff tool (должно падать)."""
        import sys

        from tools.golden_replay_of_confirm_from_redis import main

        # Третий вызов (diff) возвращает ошибку
        mock_run.side_effect = [0, 0, 2]

        test_args = [
            "golden_replay_of_confirm_from_redis.py",
            "--out-dir",
            "/tmp/test_out",
            "--baseline",
            "/tmp/baseline.ndjson",
        ]

        with patch.object(sys, "argv", test_args):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

