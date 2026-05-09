
import os
import sys
import unittest
from unittest.mock import patch

# Add parent directory to path to import tools
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools import run_tick_quality_gated_command


class TestRunTickQualityGatedCommand(unittest.TestCase):

    @patch('tools.run_tick_quality_gated_command.run_gate_check')
    @patch('subprocess.run')
    def test_gate_pass_run_command(self, mock_subprocess, mock_gate):
        """Test that command runs when gate passes."""
        # Setup
        mock_gate.return_value = 0  # PASS
        mock_subprocess.return_value.returncode = 0

        # Args
        test_args = [
            "script_name",
            "--metrics-url", "http://test:8000/metrics",
            "--", "echo", "hello"
        ]

        with patch.object(sys, 'argv', test_args):
            exit_code = run_tick_quality_gated_command.main()

        # Verify
        self.assertEqual(exit_code, 0)
        mock_subprocess.assert_called_once()
        args, _ = mock_subprocess.call_args
        self.assertEqual(args[0], ["echo", "hello"])

    @patch('tools.run_tick_quality_gated_command.run_gate_check')
    @patch('subprocess.run')
    def test_gate_fail_skip_command(self, mock_subprocess, mock_gate):
        """Test that command is skipped when gate fails."""
        # Setup
        mock_gate.return_value = 2  # FAIL

        # Args
        test_args = [
            "script_name",
            "--metrics-url", "http://test:8000/metrics",
            "--", "echo", "hello"
        ]

        with patch.object(sys, 'argv', test_args):
            exit_code = run_tick_quality_gated_command.main()

        # Verify
        self.assertEqual(exit_code, 20)
        mock_subprocess.assert_not_called()

    @patch('tools.run_tick_quality_gated_command.run_gate_check')
    @patch('subprocess.run')
    def test_gate_insufficient_fail_closed(self, mock_subprocess, mock_gate):
        """Test that command is skipped when gate insufficient and fail_closed."""
        # Setup
        mock_gate.return_value = 1  # INSUFFICIENT

        # Args: default fail-mode is fail_closed
        test_args = [
            "script_name",
            "--", "echo", "hello"
        ]

        with patch.object(sys, 'argv', test_args):
            exit_code = run_tick_quality_gated_command.main()

        # Verify
        self.assertEqual(exit_code, 21)
        mock_subprocess.assert_not_called()

    @patch('tools.run_tick_quality_gated_command.run_gate_check')
    @patch('subprocess.run')
    def test_gate_insufficient_fail_open(self, mock_subprocess, mock_gate):
        """Test that command runs when gate insufficient and fail_open."""
        # Setup
        mock_gate.return_value = 1  # INSUFFICIENT
        mock_subprocess.return_value.returncode = 0

        # Args
        test_args = [
            "script_name",
            "--fail-mode", "fail_open",
            "--", "echo", "hello"
        ]

        with patch.object(sys, 'argv', test_args):
            exit_code = run_tick_quality_gated_command.main()

        # Verify
        self.assertEqual(exit_code, 0)
        mock_subprocess.assert_called_once()

    @patch('tools.run_tick_quality_gated_command.run_gate_check')
    @patch('subprocess.run')
    def test_command_execution_failure(self, mock_subprocess, mock_gate):
        """Test return code when command fails."""
        # Setup
        mock_gate.return_value = 0  # PASS
        mock_subprocess.return_value.returncode = 127 # Command failed

        # Args
        test_args = [
            "script_name",
            "--", "echo", "hello"
        ]

        with patch.object(sys, 'argv', test_args):
            exit_code = run_tick_quality_gated_command.main()

        # Verify exit code 10 (Gate PASS, Command FAIL)
        self.assertEqual(exit_code, 10)

if __name__ == '__main__':
    unittest.main()
