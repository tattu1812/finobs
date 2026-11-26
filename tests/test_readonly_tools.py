"""Unit tests cho 5 tool chỉ-đọc (AI, Sprint 5): SQL injection, path traversal, allowlist."""

from __future__ import annotations

import unittest

from agent.tools.readonly_tools import ReadOnlyToolkit


class ReadOnlyToolsSecurityTest(unittest.TestCase):
    def setUp(self):
        self.tk = ReadOnlyToolkit()

    def test_query_table_allows_valid_select(self):
        res = self.tk.call("query_table", {"sql": "SELECT source, status FROM bronze.trades_raw LIMIT 5"})
        self.assertNotIn("error", res)
        self.assertEqual(res["table"], "bronze.trades_raw")
        self.assertGreaterEqual(res["row_count"], 1)

    def test_query_table_blocks_sql_injection_and_multistatement(self):
        bad_queries = [
            "DROP TABLE bronze.trades_raw",
            "SELECT * FROM bronze.trades_raw; DROP TABLE silver.trades",
            "SELECT * FROM bronze.trades_raw -- comment",
            "DELETE FROM metric_observations WHERE 1=1",
            "SELECT * FROM users_secret LIMIT 10",
            "SELECT * FROM bronze.trades_raw LIMIT 5000",
        ]
        for sql in bad_queries:
            res = self.tk.call("query_table", {"sql": sql})
            self.assertIn("error", res, f"Phải chặn SQL: {sql}")
            self.assertEqual(res["error"]["code"], "forbidden_sql")

    def test_read_config_allows_safe_yaml_and_blocks_env_traversal(self):
        ok = self.tk.call("read_config", {"path": "config/reconcile.yaml"})
        self.assertNotIn("error", ok)
        self.assertIn("content", ok)

        for bad in ("../.env", ".env", "config/../../.env", "infra/docker-compose.yml", "config/secret_token.yaml"):
            res = self.tk.call("read_config", {"path": bad})
            self.assertIn("error", res, f"Phải chặn path: {bad}")
            self.assertEqual(res["error"]["code"], "forbidden_path")

    def test_read_incident_and_lineage_return_expected_fields(self):
        inc = self.tk.call("read_incident", {
            "incident_id": "INC-1",
            "start": "2026-10-27T03:00:00.000Z",
            "end": "2026-10-27T03:20:00.000Z",
            "source": "binance",
        })
        self.assertIn("alerts", inc)
        self.assertIn("contract_violations", inc)

        lin = self.tk.call("read_lineage", {"asset": "finobs:gold.ohlcv_1m", "direction": "upstream"})
        self.assertIn("finobs:raw.binance.trade", lin.get("datasets", []))


if __name__ == "__main__":
    unittest.main()
