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
        self.assertIn("active", payload["run_state"])
        self.assertIn("last_status", payload["run_state"])

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

    def test_http_stop_endpoint_accepts_safe_stop(self):
        with patch.object(
            worker,
            "_request_stop",
            return_value=(True, {"status": "accepted", "reason": "dashboard_safe_stop"}),
        ):
            server = worker._build_http_server("127.0.0.1", 0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                port = server.server_address[1]
                request = Request(
                    f"http://127.0.0.1:{port}/stop?reason=dashboard_safe_stop",
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

    def test_pipeline_state_endpoint_reports_pipeline_snapshot(self):
        worker._reset_pipeline_state(
            run_id="run-1",
            session_name="smoke",
            trigger_source="http",
            llm_settings={
                "provider": "openrouter",
                "quick_model": "quick",
                "selection_model": "mini",
                "fundamentals_model": "fund",
                "deep_model": "deep",
            },
        )
        worker._set_pipeline_stage("analysis", "running", "Analysing test ticker.")
        worker._set_pipeline_tickers(["MSFT"])

        server = worker._build_http_server("127.0.0.1", 0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = server.server_address[1]
            with urlopen(f"http://127.0.0.1:{port}/pipeline-state") as response:
                payload = json.loads(response.read().decode("utf-8"))
        finally:
            server.shutdown()
            server.server_close()

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["pipeline"]["run_id"], "run-1")
        self.assertEqual(payload["pipeline"]["stages"]["analysis"]["status"], "running")
        self.assertEqual(payload["pipeline"]["llm_settings"]["selection_model"], "mini")
        self.assertIn("telemetry", payload["pipeline"])
        self.assertIn("completed_actual_cost_usd", payload["pipeline"]["telemetry"])

    def test_dashboard_route_serves_html(self):
        server = worker._build_http_server("127.0.0.1", 0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = server.server_address[1]
            with urlopen(f"http://127.0.0.1:{port}/dashboard") as response:
                body = response.read().decode("utf-8")
                status_code = response.status
                content_type = response.headers.get("Content-Type")
        finally:
            server.shutdown()
            server.server_close()

        self.assertEqual(status_code, 200)
        self.assertIn("text/html", content_type)
        self.assertIn("Realtime Pipeline Monitor", body)
        self.assertIn("Telemetry", body)

    def test_selection_model_defaults_to_quick_model_when_unset(self):
        with patch.dict(
            os.environ,
            {
                "LLM_PROVIDER": "openrouter",
                "QUICK_MODEL": "google/gemini-3.1-flash-lite-preview",
                "AGENTIC_SELECTION_MODEL": "",
            },
            clear=False,
        ):
            settings = worker._resolve_llm_settings()

        self.assertEqual(settings["selection_model"], settings["quick_model"])

    def test_openrouter_role_models_default_to_cheaper_split(self):
        with patch.dict(
            os.environ,
            {
                "LLM_PROVIDER": "openrouter",
                "QUICK_MODEL": "",
                "AGENTIC_SELECTION_MODEL": "",
                "ANALYST_MODEL": "",
                "FUNDAMENTALS_MODEL": "",
                "RESEARCH_MODEL": "",
                "TRADER_MODEL": "",
                "RISK_MODEL": "",
                "MANAGER_MODEL": "",
                "DEEP_MODEL": "",
            },
            clear=False,
        ):
            settings = worker._resolve_llm_settings()

        self.assertEqual(settings["quick_model"], "google/gemini-3.1-flash-lite-preview")
        self.assertEqual(settings["analyst_model"], "google/gemini-3.1-flash-lite-preview")
        self.assertEqual(settings["fundamentals_model"], "google/gemini-3.1-pro-preview")
        self.assertEqual(settings["research_model"], "openai/gpt-5.4-mini")
        self.assertEqual(settings["manager_model"], "openai/gpt-5.4-mini")
        self.assertEqual(settings["deep_model"], "openai/gpt-5.4")

    def test_build_ta_config_maps_role_specific_models(self):
        with patch.object(
            worker,
            "_resolve_llm_settings",
            return_value={
                "provider": "openrouter",
                "backend_url": "https://openrouter.ai/api/v1",
                "quick_model": "quick",
                "selection_model": "selection",
                "analyst_model": "analyst",
                "fundamentals_model": "fundamentals",
                "research_model": "research",
                "trader_model": "trader",
                "risk_model": "risk",
                "manager_model": "manager",
                "deep_model": "deep",
            },
        ):
            config = worker._build_ta_config()

        self.assertEqual(config["role_llm_models"]["market"], "analyst")
        self.assertEqual(config["role_llm_models"]["fundamentals"], "fundamentals")
        self.assertEqual(config["role_llm_models"]["bull_researcher"], "research")
        self.assertEqual(config["role_llm_models"]["trader"], "trader")
        self.assertEqual(config["role_llm_models"]["aggressive_analyst"], "risk")
        self.assertEqual(config["role_llm_models"]["portfolio_manager"], "manager")

    def test_build_pipeline_telemetry_exposes_actual_usage_and_live_pricing(self):
        pipeline_state = {
            "status": "running",
            "started_at": "2026-04-01T14:00:00+00:00",
            "finished_at": "2026-04-01T14:10:00+00:00",
            "tickers": [
                {
                    "symbol": "MSFT",
                    "status": "completed",
                    "started_at": "2026-04-01T14:01:00+00:00",
                    "finished_at": "2026-04-01T14:05:00+00:00",
                },
                {
                    "symbol": "NVDA",
                    "status": "running",
                    "started_at": "2026-04-01T14:05:00+00:00",
                    "finished_at": None,
                },
            ],
            "stages": {
                "analysis": {
                    "status": "completed",
                    "started_at": "2026-04-01T14:01:00+00:00",
                    "finished_at": "2026-04-01T14:08:00+00:00",
                },
                "selection": {
                    "status": "completed",
                    "started_at": "2026-04-01T14:00:10+00:00",
                    "finished_at": "2026-04-01T14:00:30+00:00",
                },
            },
            "llm_settings": {
                "provider": "openrouter",
                "selection_model": "openai/gpt-5.4-mini",
                "analyst_model": "google/gemini-3.1-flash-lite-preview",
                "fundamentals_model": "google/gemini-3.1-pro-preview",
                "research_model": "openai/gpt-5.4-mini",
                "trader_model": "openai/gpt-5.4-mini",
                "risk_model": "openai/gpt-5.4-mini",
                "manager_model": "openai/gpt-5.4-mini",
            },
            "llm_usage": {
                "calls": 3,
                "input_tokens": 600,
                "output_tokens": 150,
                "total_tokens": 750,
                "by_role": {
                    "selection": {
                        "model": "openai/gpt-5.4-mini",
                        "calls": 1,
                        "input_tokens": 100,
                        "output_tokens": 20,
                        "total_tokens": 120,
                    },
                    "market": {
                        "model": "google/gemini-3.1-flash-lite-preview",
                        "calls": 2,
                        "input_tokens": 500,
                        "output_tokens": 130,
                        "total_tokens": 630,
                    },
                },
                "by_ticker": {
                    "MSFT": {
                        "calls": 2,
                        "input_tokens": 500,
                        "output_tokens": 130,
                        "total_tokens": 630,
                        "roles": {
                            "market": {
                                "model": "google/gemini-3.1-flash-lite-preview",
                                "calls": 2,
                                "input_tokens": 500,
                                "output_tokens": 130,
                                "total_tokens": 630,
                            }
                        },
                    }
                },
            },
        }

        with patch.object(
            worker,
            "_fetch_openrouter_pricing_map",
            return_value={
                "openai/gpt-5.4-mini": {
                    "input_per_token_usd": 0.000003,
                    "output_per_token_usd": 0.000015,
                },
                "google/gemini-3.1-flash-lite-preview": {
                    "input_per_token_usd": 0.0000001,
                    "output_per_token_usd": 0.0000004,
                },
            },
        ):
            telemetry = worker._build_pipeline_telemetry(pipeline_state)

        self.assertEqual(telemetry["run_duration_seconds"], 600.0)
        self.assertEqual(telemetry["input_tokens"], 600)
        self.assertEqual(telemetry["output_tokens"], 150)
        self.assertGreater(telemetry["completed_actual_cost_usd"], 0.0)
        self.assertIsNone(telemetry["final_actual_cost_usd"])
        self.assertEqual(telemetry["stage_durations"]["analysis"]["duration_seconds"], 420.0)
        self.assertEqual(telemetry["ticker_durations"]["MSFT"]["duration_seconds"], 240.0)
        self.assertGreater(telemetry["ticker_durations"]["MSFT"]["actual_cost_usd"], 0.0)

    def test_health_payload_is_flat_and_stable(self):
        with patch.object(
            worker,
            "_snapshot_run_state",
            return_value={
                "active": True,
                "current": {
                    "session_name": "midday",
                    "trigger_source": "scheduler",
                    "started_at": "2026-04-01T17:00:00Z",
                },
                "last": {
                    "status": "completed",
                    "reason": None,
                    "session_name": "open",
                    "trigger_source": "scheduler",
                    "started_at": "2026-04-01T14:00:00Z",
                    "finished_at": "2026-04-01T14:20:00Z",
                },
            },
        ):
            payload = worker._build_health_payload()

        self.assertTrue(payload["run_state"]["active"])
        self.assertEqual(payload["run_state"]["current_session_name"], "midday")
        self.assertEqual(payload["run_state"]["last_status"], "completed")