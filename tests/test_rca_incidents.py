"""Unit test: AI-A (grouping), AI-B (BFS impact), AI-C (priority ranking) — Sprint 4."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from observability.rca.impact_bfs import bfs_downstream_impact  # noqa: E402
from observability.rca.incident_grouper import group_alerts  # noqa: E402
from observability.rca.priority_ranker import export_ranked_json, rank_incidents  # noqa: E402

SAMPLE_GRAPH = {
    "nodes": [
        {"id": "dataset:finobs:raw.binance.trade", "kind": "dataset", "name": "raw.binance.trade"},
        {"id": "job:finobs:bronze_trades", "kind": "job", "name": "bronze_trades"},
        {"id": "dataset:finobs:bronze.trades_raw", "kind": "dataset", "name": "bronze.trades_raw"},
        {"id": "dataset:finobs:silver.trades", "kind": "dataset", "name": "silver.trades"},
        {"id": "dataset:finobs:gold.ohlcv_1m", "kind": "dataset", "name": "gold.ohlcv_1m"},
        {"id": "dataset:finobs:raw.coinbase.trade", "kind": "dataset", "name": "raw.coinbase.trade"},
    ],
    "edges": [
        {"from": "dataset:finobs:raw.binance.trade", "to": "job:finobs:bronze_trades"},
        {"from": "job:finobs:bronze_trades", "to": "dataset:finobs:bronze.trades_raw"},
        {"from": "dataset:finobs:bronze.trades_raw", "to": "dataset:finobs:silver.trades"},
        {"from": "dataset:finobs:silver.trades", "to": "dataset:finobs:gold.ohlcv_1m"},
    ],
}


class TestBFSImpact(unittest.TestCase):
    def test_downstream_distances(self):
        impacted = bfs_downstream_impact(SAMPLE_GRAPH, "dataset:finobs:raw.binance.trade")
        names = [(n.name, n.distance, n.tier) for n in impacted]
        self.assertEqual(names[0], ("bronze_trades", 1, "bronze"))
        self.assertIn(("gold.ohlcv_1m", 4, "gold"), names)

    def test_handles_cycle_and_empty_graph(self):
        self.assertEqual(bfs_downstream_impact({}, "dataset:x"), [])
        cyclic = {
            "nodes": [
                {"id": "a", "kind": "dataset", "name": "raw.a"},
                {"id": "b", "kind": "dataset", "name": "bronze.b"},
            ],
            "edges": [{"from": "a", "to": "b"}, {"from": "b", "to": "a"}],
        }
        impacted = bfs_downstream_impact(cyclic, "a")
        self.assertEqual(len(impacted), 1)
        self.assertEqual(impacted[0].node_id, "b")


class TestGroupingAndPriority(unittest.TestCase):
    def test_group_related_and_separate_noise(self):
        alerts = [
            {
                "alert_id": "A001", "source": "binance", "symbol": "ETH-USDT",
                "asset": "finobs:raw.binance.trade", "severity": "high",
                "detected_at": "2026-11-26T03:05:00Z",
            },
            {
                "alert_id": "A002", "source": "binance", "symbol": "ETH-USDT",
                "asset": "finobs:bronze.trades_raw", "severity": "medium",
                "detected_at": "2026-11-26T03:07:00Z",
            },
            {
                "alert_id": "A003_NOISE", "source": "coinbase", "symbol": "BTC-USD",
                "asset": "finobs:raw.coinbase.trade", "severity": "low",
                "detected_at": "2026-11-26T03:08:00Z",
            },
        ]
        incidents = group_alerts(alerts, lineage_graph=SAMPLE_GRAPH, window_seconds=600)
        self.assertEqual(len(incidents), 2)

        ranked = rank_incidents(incidents)
        # Sự cố Binance ảnh hưởng tới gold + severity high phải xếp trên nhiễu Coinbase low
        self.assertEqual(ranked[0].sources, ["binance"])
        self.assertGreater(ranked[0].priority_score, ranked[1].priority_score)

        # Kiểm tra tính tất định (chạy lại cho cùng ID và thứ tự)
        incidents2 = group_alerts(list(reversed(alerts)), lineage_graph=SAMPLE_GRAPH, window_seconds=600)
        ranked2 = rank_incidents(incidents2)
        self.assertEqual([i.incident_id for i in ranked], [i.incident_id for i in ranked2])
        self.assertIn("INC-", export_ranked_json(ranked))


if __name__ == "__main__":
    unittest.main()
