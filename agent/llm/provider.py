"""LLM Provider interface, ScriptedFakeLLM (offline/test) và OpenAICompatibleProvider.

Không bao giờ hardcode API key. Đọc từ biến môi trường ``FINOBS_LLM_API_KEY``,
``FINOBS_LLM_BASE_URL``, ``FINOBS_LLM_MODEL``.
"""

from __future__ import annotations

import json
import os
import urllib.request
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class LLMResponse:
    content: str
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    tokens_used: int = 0


class LLMProvider(ABC):
    @abstractmethod
    def complete(self, messages: list[dict[str, Any]], tools: list[dict[str, Any]]) -> LLMResponse:
        raise NotImplementedError


class ScriptedFakeLLM(LLMProvider):
    """LLM giả lập tất định theo kịch bản (dùng cho unit test và offline demo)."""

    def __init__(self, script: list[LLMResponse] | None = None) -> None:
        self._script = list(script) if script is not None else self._default_script()
        self._cursor = 0

    @staticmethod
    def _default_script() -> list[LLMResponse]:
        return [
            LLMResponse(
                content="Bước 1: Kiểm tra thông tin sự cố và vi phạm contract.",
                tool_calls=[
                    {
                        "name": "read_incident",
                        "args": {
                            "incident_id": "INC-S002",
                            "start": "2026-10-27T03:00:00.000Z",
                            "end": "2026-10-27T03:20:00.000Z",
                            "source": "binance",
                        },
                    }
                ],
                tokens_used=120,
            ),
            LLMResponse(
                content="Bước 2: Truy vết lineage từ gold.ohlcv_1m ngược về nguồn.",
                tool_calls=[
                    {
                        "name": "read_lineage",
                        "args": {"asset": "finobs:gold.ohlcv_1m", "direction": "upstream", "depth": 5},
                    }
                ],
                tokens_used=110,
            ),
            LLMResponse(
                content=json.dumps({
                    "hypotheses": [
                        {
                            "rank": 1,
                            "root_cause": "FT07_schema_drift tại nguồn binance gây vi phạm data contract",
                            "suspected_asset": "finobs:raw.binance.trade",
                            "confidence": 0.9,
                            "evidence": [
                                {"call_id": "call_001", "tool": "read_incident", "excerpt": "binance"},
                                {"call_id": "call_002", "tool": "read_lineage", "excerpt": "finobs:raw.binance.trade"},
                            ],
                            "suggested_remediation_text": (
                                "Kiểm tra mapping schema nguồn Binance ở adapter trước khi cast vào bronze.trades_raw."
                            ),
                        }
                    ]
                }, ensure_ascii=False),
                tool_calls=[],
                tokens_used=160,
            ),
        ]

    def complete(self, messages: list[dict[str, Any]], tools: list[dict[str, Any]]) -> LLMResponse:
        if self._cursor >= len(self._script):
            return LLMResponse(content='{"hypotheses": []}', tool_calls=[], tokens_used=10)
        resp = self._script[self._cursor]
        self._cursor += 1
        return resp


class OpenAICompatibleProvider(LLMProvider):
    """Provider gọi endpoint OpenAI-compatible qua urllib (không thêm phụ thuộc nặng)."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        model: str | None = None,
        timeout_s: float = 30.0,
    ) -> None:
        self.api_key = api_key if api_key is not None else os.environ.get("FINOBS_LLM_API_KEY", "")
        self.base_url = (
            base_url or os.environ.get("FINOBS_LLM_BASE_URL", "https://api.openai.com/v1")
        ).rstrip("/")
        self.model = model or os.environ.get("FINOBS_LLM_MODEL", "gpt-4o-mini")
        self.timeout_s = timeout_s
        if not self.api_key:
            raise ValueError("Thiếu biến môi trường FINOBS_LLM_API_KEY")

    def complete(self, messages: list[dict[str, Any]], tools: list[dict[str, Any]]) -> LLMResponse:
        openai_tools = [
            {
                "type": "function",
                "function": {
                    "name": t["name"],
                    "description": t["description"],
                    "parameters": t["input_schema"],
                },
            }
            for t in tools
        ]
        payload = json.dumps({
            "model": self.model,
            "messages": messages,
            "tools": openai_tools,
            "temperature": 0.0,
        }).encode("utf-8")
        req = urllib.request.Request(
            f"{self.base_url}/chat/completions",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        choice = data["choices"][0]["message"]
        calls = []
        for tc in choice.get("tool_calls") or []:
            fn = tc.get("function", {})
            raw_args = fn.get("arguments", "{}")
            args = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
            calls.append({"name": fn.get("name"), "args": args})
        tokens = int(data.get("usage", {}).get("total_tokens", 0))
        return LLMResponse(content=choice.get("content") or "", tool_calls=calls, tokens_used=tokens)
