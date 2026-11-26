"""Unit tests cho ReAct Agent v1 (AI, Sprint 5): trace, evidence verifier, limits."""

from __future__ import annotations

import json
import unittest

from agent.llm.provider import LLMResponse, ScriptedFakeLLM
from agent.react_agent import AgentConfig, ReActAgent
from agent.tools.readonly_tools import ReadOnlyToolkit


class ReActAgentTest(unittest.TestCase):
    def test_default_scripted_agent_succeeds_with_verified_evidence(self):
        agent = ReActAgent(toolkit=ReadOnlyToolkit(), llm=ScriptedFakeLLM())
        res = agent.diagnose({"incident_id": "INC-S002", "source": "binance"})
        self.assertEqual(res["status"], "completed")
        self.assertEqual(res["prompt_version"], "v1.0.0")
        self.assertEqual(len(res["hypotheses"]), 1)
        hyp = res["hypotheses"][0]
        self.assertEqual(hyp["suspected_asset"], "finobs:raw.binance.trade")
        self.assertTrue(hyp["verified"])

    def test_verifier_rejects_fabricated_call_id_or_excerpt(self):
        bad_llm = ScriptedFakeLLM([
            LLMResponse(
                content="Gọi read_incident",
                tool_calls=[{"name": "read_incident", "args": {"start": "2026-10-27T03:00:00.000Z", "end": "2026-10-27T03:20:00.000Z"}}],
                tokens_used=50,
            ),
            LLMResponse(
                content=json.dumps({
                    "hypotheses": [
                        {
                            "rank": 1,
                            "root_cause": "Bịa đặt",
                            "suspected_asset": "finobs:raw.binance.trade",
                            "confidence": 0.99,
                            "evidence": [
                                {"call_id": "call_999", "tool": "read_incident", "excerpt": "binance"},
                                {"call_id": "call_001", "tool": "read_incident", "excerpt": "khong_ton_tai_trong_obs_12345"},
                            ],
                            "suggested_remediation_text": "Không",
                        }
                    ]
                }),
                tool_calls=[],
                tokens_used=60,
            ),
        ])
        agent = ReActAgent(toolkit=ReadOnlyToolkit(), llm=bad_llm)
        res = agent.diagnose({"incident_id": "INC-FAKE"})
        self.assertEqual(res["status"], "rejected_unverified")
        self.assertEqual(res["hypotheses"], [])
        self.assertGreaterEqual(len(res["verification_errors"]), 2)

    def test_max_steps_limit_stops_infinite_loop(self):
        loop_llm = ScriptedFakeLLM([
            LLMResponse(
                content="Lặp",
                tool_calls=[{"name": "read_lineage", "args": {"asset": "finobs:gold.ohlcv_1m"}}],
                tokens_used=20,
            )
            for _ in range(10)
        ])
        agent = ReActAgent(
            toolkit=ReadOnlyToolkit(),
            llm=loop_llm,
            config=AgentConfig(max_steps=3),
        )
        res = agent.diagnose({"incident_id": "INC-LOOP"})
        self.assertEqual(res["status"], "max_steps_exceeded")
        self.assertEqual(res["steps_taken"], 3)


if __name__ == "__main__":
    unittest.main()
