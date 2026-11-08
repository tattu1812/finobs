"""
Test 5 tool agent trên dữ liệu giả + client lineage Marquez.

    python -m unittest tests.test_agent_tools -v        # hoặc: make test-agent
"""

from __future__ import annotations

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

from jsonschema import Draft202012Validator

from agent.tools import fake_world
from agent.tools.lineage_graph import render_mermaid, render_tree
from agent.tools.marquez import FakeTransport, HttpTransport, MarquezClient
from agent.tools.registry import TOOL_NAMES, Toolbox, load_spec

W0, W1 = "2026-10-27T03:00:00.000Z", "2026-10-27T03:20:00.000Z"

VALID_CALLS = {
    "list_alerts": {"start": W0, "end": W1},
    "query_metric": {"metric": "contract_violation_rate_1m", "start": W0, "end": W1},
    "get_lineage": {"asset": "finobs:gold.ohlcv_1m", "direction": "upstream"},
    "get_job_runs": {"job": "bronze_load", "start": W0, "end": W1},
    "get_contract_violations": {"source": "binance", "start": W0, "end": W1},
}

ERROR = Draft202012Validator(load_spec("_error"))


class ToolContractTest(unittest.TestCase):
    """Mỗi tool gọi được và trả JSON đúng output_schema."""

    @classmethod
    def setUpClass(cls):
        cls.tb = Toolbox.fake()

    def test_exactly_five_tools(self):
        self.assertEqual(len(TOOL_NAMES), 5)
        self.assertEqual(set(VALID_CALLS), set(TOOL_NAMES))

    def test_every_tool_returns_valid_json(self):
        for name, args in VALID_CALLS.items():
            with self.subTest(tool=name):
                result = self.tb.call(name, args)
                self.assertNotIn("error", result, result)
                json.dumps(result)  # phải serialise được, không có datetime lọt ra
                errors = list(Draft202012Validator(load_spec(name)["output_schema"]).iter_errors(result))
                self.assertEqual(errors, [], [e.message for e in errors])

    def test_schemas_are_valid_json_schema(self):
        for name in TOOL_NAMES + ["_error"]:
            spec = load_spec(name)
            for key in ("input_schema", "output_schema"):
                if key in spec:
                    Draft202012Validator.check_schema(spec[key])

    def test_definitions_for_llm_have_no_refs(self):
        defs = self.tb.definitions()
        self.assertEqual([d["name"] for d in defs], TOOL_NAMES)
        for d in defs:
            self.assertEqual(set(d), {"name", "description", "input_schema"})
            self.assertNotIn("$ref", json.dumps(d))

    def test_fake_world_is_deterministic(self):
        a, b = Toolbox.fake(), Toolbox.fake()
        for name, args in VALID_CALLS.items():
            self.assertEqual(a.call(name, args), b.call(name, args))


class ToolErrorTest(unittest.TestCase):
    """Lỗi không ném exception mà trả {"error": {code, message}}."""

    @classmethod
    def setUpClass(cls):
        cls.tb = Toolbox.fake()

    def assert_error(self, result, code):
        self.assertEqual(list(ERROR.iter_errors(result)), [], result)
        self.assertEqual(result["error"]["code"], code, result)

    def test_unknown_tool(self):
        self.assert_error(self.tb.call("drop_table", {}), "unknown_tool")

    def test_missing_and_extra_arguments(self):
        self.assert_error(self.tb.call("query_metric", {"metric": "trade_count_1m"}), "invalid_input")
        self.assert_error(self.tb.call("list_alerts", {"start": W0, "end": W1, "foo": 1}), "invalid_input")

    def test_bad_timestamp_and_reversed_window(self):
        self.assert_error(self.tb.call("list_alerts", {"start": "2026-10-27 03:00", "end": W1}), "invalid_input")
        self.assert_error(self.tb.call("list_alerts", {"start": W1, "end": W0}), "invalid_input")

    def test_not_found(self):
        self.assert_error(self.tb.call("query_metric", {"metric": "nope", "start": W0, "end": W1}), "not_found")
        self.assert_error(self.tb.call("get_job_runs", {"job": "nope", "start": W0, "end": W1}), "not_found")
        self.assert_error(self.tb.call("get_lineage", {"asset": "finobs:nope.table"}), "not_found")

    def test_marquez_unreachable(self):
        tb = Toolbox.fake(marquez_url="http://127.0.0.1:9")  # cổng discard, không ai nghe
        self.assert_error(tb.call("get_lineage", {"asset": "finobs:gold.ohlcv_1m"}), "backend_unavailable")


class FakeWorldStoryTest(unittest.TestCase):
    """Dữ liệu giả kể đúng câu chuyện S002 để agent có thể tìm ra gốc."""

    @classmethod
    def setUpClass(cls):
        cls.tb = Toolbox.fake()

    def test_alerts(self):
        res = self.tb.call("list_alerts", {"start": W0, "end": W1})
        self.assertEqual([a["alert_id"] for a in res["alerts"]], ["A001", "A002", "A003", "A004"])
        high = self.tb.call("list_alerts", {"start": W0, "end": W1, "min_severity": "high"})
        self.assertEqual([a["asset"] for a in high["alerts"]], ["finobs:raw.binance_trades"])

    def test_alert_values_match_metric(self):
        a001 = self.tb.call("list_alerts", {"start": W0, "end": W1})["alerts"][0]
        series = self.tb.call("query_metric", {"metric": a001["metric"], "group": a001["group"],
                                               "start": a001["detected_at"], "end": W1})["series"]
        self.assertEqual(series[0]["points"][0]["value"], a001["observed"])

    def test_violations_only_binance_ethusdt_from_0305(self):
        b = self.tb.call("get_contract_violations", {"source": "binance", "start": W0, "end": W1})
        self.assertEqual(b["first_violation_at"], "2026-10-27T03:05:00.000Z")
        self.assertEqual(set(b["by_symbol"]), {"ETHUSDT"})
        self.assertGreater(b["invalid"], 0)
        c = self.tb.call("get_contract_violations", {"source": "coinbase", "start": W0, "end": W1})
        self.assertEqual((c["invalid"], c["samples"], c["first_violation_at"]), (0, [], None))

    def test_bronze_load_fails_after_drift(self):
        r = self.tb.call("get_job_runs", {"job": "finobs:bronze_load", "start": W0, "end": W1, "limit": 3})
        self.assertEqual(r["first_failure_at"], "2026-10-27T03:06:05.000Z")
        self.assertEqual(len(r["runs"]), 3)
        self.assertEqual(r["state_counts"], {"COMPLETED": 6, "FAILED": 14})
        ok = self.tb.call("get_job_runs", {"job": "ingest_binance", "start": W0, "end": W1})
        self.assertIsNone(ok["first_failure_at"])

    def test_metric_group_filter(self):
        res = self.tb.call("query_metric", {"metric": "trade_count_1m", "group": {"source": "coinbase"},
                                            "start": W0, "end": W1})
        self.assertEqual({s["group"]["symbol_canonical"] for s in res["series"]}, {"BTC-USD", "ETH-USD"})
        self.assertTrue(all(len(s["points"]) == fake_world.MINUTES for s in res["series"]))


class LineageTest(unittest.TestCase):
    """get_lineage đọc đúng đồ thị Marquez và duyệt đúng chiều."""

    @classmethod
    def setUpClass(cls):
        cls.tb = Toolbox.fake()

    def test_upstream_of_gold_reaches_both_raw_sources(self):
        res = self.tb.call("get_lineage", {"asset": "finobs:gold.ohlcv_1m"})
        self.assertEqual(res["datasets"],
                         ["finobs:silver.trades_dedup", "finobs:bronze.trades",
                          "finobs:raw.binance_trades", "finobs:raw.coinbase_trades"])
        self.assertEqual(res["jobs"][:2], ["finobs:gold_ohlcv", "finobs:silver_dedup"])
        self.assertNotIn("finobs:gold.cross_source_spread", res["datasets"])  # nhánh anh em, không phải upstream
        dist = {n["name"]: n["distance"] for n in res["nodes"]}
        self.assertEqual(dist["gold.ohlcv_1m"], 0)
        self.assertEqual(dist["raw.binance_trades"], -6)

    def test_downstream_of_raw_binance_is_blast_radius(self):
        res = self.tb.call("get_lineage", {"asset": "finobs:raw.binance_trades", "direction": "downstream"})
        self.assertIn("finobs:gold.ohlcv_1m", res["datasets"])
        self.assertIn("finobs:gold.cross_source_spread", res["datasets"])
        self.assertNotIn("finobs:raw.coinbase_trades", res["datasets"])
        self.assertTrue(all(n["distance"] >= 0 for n in res["nodes"]))

    def test_depth_limits_walk(self):
        res = self.tb.call("get_lineage", {"asset": "finobs:gold.ohlcv_1m", "depth": 2})
        self.assertEqual(res["datasets"], ["finobs:silver.trades_dedup"])
        self.assertEqual(res["jobs"], ["finobs:gold_ohlcv"])

    def test_both_directions(self):
        res = self.tb.call("get_lineage", {"asset": "finobs:bronze.trades", "direction": "both"})
        dist = {n["name"]: n["distance"] for n in res["nodes"]}
        self.assertLess(dist["raw.coinbase_trades"], 0)
        self.assertGreater(dist["gold.ohlcv_1m"], 0)

    def test_http_transport_against_local_marquez_stub(self):
        """Đi qua đường HTTP thật: một server cục bộ trả JSON y hệt định dạng Marquez."""
        fake = FakeTransport()
        seen = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                url = urlparse(self.path)
                params = {k: v[0] for k, v in parse_qs(url.query).items()}
                seen.append((url.path, params))
                try:
                    body, code = json.dumps(fake.get(url.path, params)).encode(), 200
                except Exception:
                    body, code = b'{"code":404}', 404
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        server = HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            url = f"http://127.0.0.1:{server.server_port}"
            graph = MarquezClient(HttpTransport(url)).lineage("finobs:gold.ohlcv_1m", depth=5)
            self.assertIn("dataset:finobs:raw.binance_trades", graph.nodes)
            self.assertEqual(seen[0], ("/api/v1/lineage", {"nodeId": "dataset:finobs:gold.ohlcv_1m", "depth": "5"}))

            via_http = Toolbox.fake(marquez_url=url).call("get_lineage", {"asset": "finobs:gold.ohlcv_1m"})
            self.assertEqual(via_http, self.tb.call("get_lineage", {"asset": "finobs:gold.ohlcv_1m"}))

            missing = Toolbox.fake(marquez_url=url).call("get_lineage", {"asset": "finobs:nope.table"})
            self.assertEqual(missing["error"]["code"], "not_found")
        finally:
            server.shutdown()
            server.server_close()


class LineageScriptTest(unittest.TestCase):
    def test_tree_and_mermaid_render(self):
        tb = Toolbox.fake()
        res = tb.call("get_lineage", {"asset": "finobs:gold.ohlcv_1m"})
        tree = render_tree(res, "upstream", {"dataset:finobs:raw.binance_trades": ["A001"]})
        self.assertEqual(tree[0], "finobs:gold.ohlcv_1m")
        self.assertTrue(any("raw.binance_trades" in line and "A001" in line for line in tree))
        self.assertEqual(sum("raw.coinbase_trades" in line for line in tree), 1)
        mermaid = render_mermaid(res, {})
        self.assertEqual(mermaid[0], "flowchart LR")
        self.assertEqual(sum("-->" in line for line in mermaid), len(res["edges"]))


if __name__ == "__main__":
    unittest.main()
