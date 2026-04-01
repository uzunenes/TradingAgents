import os
from pathlib import Path
import sys
import threading
import unittest
from urllib.request import Request, urlopen
from unittest.mock import patch
import json

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import worker


class TestIntradayWorkerHelpers(unittest.TestCase):
    def test_schedule_times_env_creates_three_named_sessions(self):
        with patch.dict(os.environ, {"SCHEDULE_TIMES_UTC": "14:00,17:00,19:45"}, clear=False):
            sessions = worker._get_schedule_sessions()

        self.assertEqual([session.name for session in sessions], ["open", "midday", "close"])
        self.assertEqual([(session.hour, session.minute) for session in sessions], [(14, 0), (17, 0), (19, 45)])

    def test_legacy_schedule_env_falls_back_to_single_daily_session(self):
        with patch.dict(
            os.environ,
            {"SCHEDULE_TIMES_UTC": "", "SCHEDULE_HOUR_UTC": "21", "SCHEDULE_MINUTE_UTC": "5"},
            clear=False,
        ):
            sessions = worker._get_schedule_sessions()

        self.assertEqual(len(sessions), 1)
        self.assertEqual(sessions[0].name, "daily")
        self.assertEqual((sessions[0].hour, sessions[0].minute), (21, 5))

    def test_env_normalization_strips_quotes_and_whitespace(self):
        with patch.dict(os.environ, {"HTTP_ENABLED": '  "true"  '}, clear=False):
            self.assertTrue(worker._get_env_bool("HTTP_ENABLED", False))

    def test_env_normalization_strips_inline_comment(self):
        with patch.dict(
            os.environ,
            {"HTTP_PORT": ' "8080"   # local trigger port '},
            clear=False,
        ):
            self.assertEqual(worker._get_http_port(), 8080)

    def test_http_health_endpoint_reports_ok(self):
        server = worker._build_http_server("127.0.0.1", 0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = server.server_address[1]
            with urlopen(f"http://127.0.0.1:{port}/healthz") as response:
                payload = json.loads(response.read().decode("utf-8"))
        finally:
            server.shutdown()
            server.server_close()

        self.assertEqual(payload["status"], "ok")
        self.assertIn("run_state", payload)

    def test_http_trigger_endpoint_accepts_background_run(self):
        with patch.object(
            worker,
            "_start_background_job",
            return_value=(True, {"status": "accepted", "session_name": "smoke"}),
        ):
            server = worker._build_http_server("127.0.0.1", 0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                port = server.server_address[1]
                request = Request(
                    f"http://127.0.0.1:{port}/trigger?session=smoke",
                    method="POST",
                )
                with urlopen(request) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                    status_code = response.status
            finally:
                server.shutdown()
                server.server_close()

        self.assertEqual(status_code, 202)
        self.assertEqual(payload["status"], "accepted")