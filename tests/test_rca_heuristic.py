"""RCA heuristic (AI-4): đúng gốc trên thế giới giả S002, xử lý nhánh và nhiễu."""

from __future__ import annotations

import unittest

from agent.tools.registry import Toolbox
from observability.rca.heuristic import dataset_edges, rank_candidates, rank_from_toolbox

T = "2026-10-27T03:{:02d}:00.000Z"


class HeuristicTest(unittest.TestCase):
    def test_fake_world_s002_root_is_raw_binance(self):
        ranked = rank_from_toolbox(Toolbox.fake(), T.format(0), T.format(20), k=3)
        self.assertEqual(ranked[0]["candidate_node"], "finobs:raw.binance.trade")
        # Coinbase cũng ở đầu nguồn và có alert (đỉnh tải thật) nhưng alert muộn hơn -> xếp sau
        self.assertEqual(ranked[1]["candidate_node"], "finobs:raw.coinbase.trade")
        self.assertEqual([r["rank"] for r in ranked], [1, 2, 3])

    def test_branching_picks_common_ancestor(self):
        # a -> b -> c, a -> d; a, b, c, d đều có alert: gốc là a
        edges = {("a", "b"), ("b", "c"), ("a", "d")}
        anomalies = {"a": T.format(5), "b": T.format(6), "c": T.format(7), "d": T.format(6)}
        self.assertEqual(rank_candidates(edges, anomalies)[0]["candidate_node"], "a")

    def test_child_without_alert_disqualifies(self):
        # a -> b, a -> c; a và b có alert, c bình thường: a không thỏa "mọi con đều bất thường"
        edges = {("a", "b"), ("a", "c")}
        ranked = rank_candidates(edges, {"a": T.format(1), "b": T.format(2)})
        self.assertEqual(ranked[0]["candidate_node"], "b")
        self.assertTrue(any("không bất thường" in e for e in ranked[1]["evidence"]))

    def test_downstream_only_symptom_ranked_after_root(self):
        edges = {("raw", "bronze"), ("bronze", "gold")}
        ranked = rank_candidates(edges, {"gold": T.format(1), "bronze": T.format(2), "raw": T.format(3)})
        self.assertEqual([r["candidate_node"] for r in ranked], ["raw", "bronze", "gold"])

    def test_dataset_edges_collapse_jobs(self):
        lineage = {("dataset:finobs:x", "job:finobs:j"), ("job:finobs:j", "dataset:finobs:y")}
        self.assertEqual(dataset_edges(lineage), {("finobs:x", "finobs:y")})


if __name__ == "__main__":
    unittest.main()
