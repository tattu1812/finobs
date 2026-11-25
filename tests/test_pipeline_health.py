"""Lineage client, exporter pipeline health, metric schema/distribution, backend thật của agent."""

from __future__ import annotations

import json
import sys
import unittest
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "platform"), str(ROOT / "platform" / "streaming")]

from lineage import LineageEmitter  # noqa: E402
from progress_exporter import ProgressExporter  # noqa: E402

from agent.tools.live_backends import MarquezRuns, ViolationStore  # noqa: E402
from agent.tools.registry import Toolbox  # noqa: E402
from observability.collectors import table_metrics  # noqa: E402
from observability.collectors.run_loop import contract_rows  # noqa: E402

T0 = datetime(2026, 11, 25, 10, 0, tzinfo=timezone.utc)


class LineageTest(unittest.TestCase):
    def test_events_follow_naming_convention(self):
        sent = []
        lin = LineageEmitter("http://marquez:5000", post=lambda url, body: sent.append((url, json.loads(body))))
        run = lin.start("silver_trades", inputs=["bronze.trades_raw"], outputs=["silver.trades"])
        lin.fail(run, "boom")
        self.assertEqual(sent[0][0], "http://marquez:5000/api/v1/lineage")
        start, fail = sent[0][1], sent[1][1]
        self.assertEqual(start["eventType"], "START")
        self.assertEqual((start["job"]["namespace"], start["job"]["name"]), ("finobs", "silver_trades"))
        self.assertEqual(start["job"]["facets"]["jobType"]["processingType"], "BATCH")
        stream = lin.event("START", lin.start("bronze_trades", outputs=["bronze.trades_raw"], streaming=True))
        self.assertEqual(stream["job"]["facets"]["jobType"]["processingType"], "STREAMING")
        self.assertEqual(start["inputs"][0]["name"], "bronze.trades_raw")
        self.assertEqual(fail["run"]["runId"], start["run"]["runId"])
        self.assertEqual(fail["run"]["facets"]["errorMessage"]["message"], "boom")

    def test_unreachable_marquez_never_raises(self):
        def broken(url, body):
            raise OSError("down")
        lin = LineageEmitter("http://nowhere:5000", post=broken)
        self.assertFalse(lin.complete(lin.start("producer", outputs=["raw.binance.trade"])))


class ExporterTest(unittest.TestCase):
    def test_kafka_lag_and_batch_metrics(self):
        exp = ProgressExporter()
        exp.record_progress({"name": "bronze_trades", "batchDuration": 2300, "numInputRows": 4100,
                             "inputRowsPerSecond": 68.3, "processedRowsPerSecond": 1782.6,
                             "sources": [{"metrics": {"maxOffsetsBehindLatest": "12", "avgOffsetsBehindLatest": "4.0",
                                                      "minOffsetsBehindLatest": "0"}}]})
        exp.inc("finobs_silver_late_rows_total", 3, source="coinbase")
        text = exp.render()
        self.assertIn('finobs_stream_offsets_behind_latest{query="bronze_trades",stat="max"} 12.0', text)
        self.assertIn('finobs_stream_batch_duration_ms{query="bronze_trades"} 2300.0', text)
        self.assertIn('finobs_silver_late_rows_total{source="coinbase"} 3.0', text)
        self.assertIn("# TYPE finobs_stream_batches_total counter", text)

    def test_spark35_progress_uses_trigger_execution(self):
        """JSON tiến độ thật của Spark 3.5 không có batchDuration, chỉ có durationMs."""
        exp = ProgressExporter()
        exp.record_progress({"name": "silver_gold", "durationMs": {"triggerExecution": 8123, "addBatch": 7000},
                             "numInputRows": 10, "sources": [{"metrics": {}}]})
        self.assertIn('finobs_stream_batch_duration_ms{query="silver_gold"} 8123.0', exp.render())


class SchemaPillarTest(unittest.TestCase):
    def test_contract_rate_and_missing_source(self):
        minute = T0
        checked = Counter({("binance", minute): 200, ("coinbase", minute): 50})
        bad = Counter({("binance", minute): 10})
        rows = {(r["metric_name"], r["dimensions"]["source"]): r
                for r in contract_rows(checked, bad, minute + timedelta(minutes=1))}
        self.assertEqual(rows[("contract_violation_rate_1m", "binance")]["value"], 0.05)
        self.assertEqual(rows[("contract_violation_count_1m", "coinbase")]["value"], 0)
        self.assertEqual(rows[("contract_violation_rate_1m", "kraken")]["status"], "source_unavailable")


class FakeTrino:
    def __init__(self, answers):
        self.answers, self.sql = answers, []

    def query(self, sql):
        self.sql.append(sql)
        for key, rows in self.answers.items():
            if key in sql:
                return rows
        raise RuntimeError("bảng chưa có")


class TableMetricsTest(unittest.TestCase):
    def test_freshness_and_distribution(self):
        trino = FakeTrino({
            '"trades_raw$snapshots"': [{"last": T0.timestamp() - 30}],
            "approx_percentile": [{"source": "binance", "symbol_canonical": "BTC-USDT", "p95": 0.5, "n": 900}],
            "JOIN iceberg.gold.ohlcv_1m": [{"source": "coinbase", "symbol_std": "BTC/USD", "bps": 12.5, "n": 40}],
            "spread_bps, n_sources": [{"symbol_std": "BTC/USD", "spread_bps": 4.2, "n_sources": 3}],
        })
        rows = table_metrics.collect(trino, T0)
        by = {(r["metric_name"], json.dumps(r["dimensions"], sort_keys=True)): r for r in rows}
        self.assertEqual(by[("table_freshness_seconds", '{"asset": "finobs:bronze.trades_raw"}')]["value"], 30.0)
        self.assertEqual(by[("table_freshness_seconds", '{"asset": "finobs:silver.trades"}')]["status"], "no_data")
        self.assertEqual(by[("cross_source_spread_bps", '{"symbol_std": "BTC/USD"}')]["value"], 4.2)
        self.assertEqual(by[("trade_size_p95_5m", '{"source": "binance", "symbol_canonical": "BTC-USDT"}')]["sample_count"], 900)


class Cursor:
    def __init__(self, results):
        self.results, self.i = results, -1

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.i += 1

    def fetchall(self):
        return self.results[self.i]

    def cursor(self):
        return self


class LiveBackendsTest(unittest.TestCase):
    START, END = "2026-11-25T10:00:00.000Z", "2026-11-25T11:00:00.000Z"

    def test_contract_violations_from_store(self):
        metrics = [("contract_violation_count_1m", T0, 3, 100), ("contract_violation_rate_1m", T0, 0.03, 100),
                   ("contract_violation_count_1m", T0 + timedelta(minutes=1), 0, 90),
                   ("contract_violation_rate_1m", T0 + timedelta(minutes=1), 0, 90)]
        samples = [("kraken:BTC/USD:1", T0, ["payload/price: 1.5 is not of type 'string'"], "BTC/USD")]
        tb = Toolbox.fake()
        tb.violation_store = ViolationStore(connect=lambda: Cursor([metrics, samples]))
        out = tb.call("get_contract_violations", {"source": "kraken", "start": self.START, "end": self.END})
        self.assertNotIn("error", out, out)
        self.assertEqual((out["checked"], out["invalid"], out["invalid_rate"]), (190, 3, round(3 / 190, 6)))
        self.assertEqual(out["first_violation_at"], "2026-11-25T10:00:00.000Z")
        self.assertEqual(out["by_symbol"], {"BTC/USD": 1})
        from jsonschema import Draft202012Validator
        Draft202012Validator(tb.specs["get_contract_violations"]["output_schema"]).validate(out)

    def test_job_runs_from_marquez(self):
        class Transport:
            def get(self, path, params):
                assert path == "/api/v1/namespaces/finobs/jobs/silver_trades/runs"
                return {"runs": [
                    {"id": "r1", "state": "FAILED", "startedAt": "2026-11-25T10:05:00Z", "endedAt": "2026-11-25T10:06:00Z",
                     "facets": {"errorMessage": {"message": "OOM"}}},
                    {"id": "r2", "state": "RUNNING", "startedAt": "2026-11-25T10:10:00Z", "endedAt": None},
                    {"id": "r0", "state": "COMPLETED", "startedAt": "2026-11-24T10:00:00Z", "endedAt": None}]}
        tb = Toolbox.fake()
        tb.job_runs_backend = MarquezRuns(Transport())
        out = tb.call("get_job_runs", {"job": "silver_trades", "start": self.START, "end": self.END})
        self.assertEqual(out["total_in_window"], 2)
        self.assertEqual(out["first_failure_at"], "2026-11-25T10:05:00.000Z")
        self.assertEqual(out["runs"][1]["error_message"], "OOM")
        from jsonschema import Draft202012Validator
        Draft202012Validator(tb.specs["get_job_runs"]["output_schema"]).validate(out)


if __name__ == "__main__":
    unittest.main()
