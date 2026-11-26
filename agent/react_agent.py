"""ReAct Agent v1 với Evidence Verifier & giới hạn tài nguyên (AI, Sprint 5)."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from agent.llm.provider import LLMProvider, ScriptedFakeLLM
from agent.tools.readonly_tools import ReadOnlyToolkit

PROMPT_PATH = Path(__file__).resolve().parent / "prompts" / "react_v1.md"
PROMPT_VERSION = "v1.0.0"


@dataclass
class AgentConfig:
    max_steps: int = 8
    timeout_s: float = 30.0
    max_tokens: int = 8000
    require_verified_evidence: bool = True


def _extract_json_block(text: str) -> dict[str, Any]:
    raw = text.strip()
    if raw.startswith("```"):
        lines = raw.splitlines()
        if len(lines) >= 3:
            raw = "\n".join(lines[1:-1]).strip()
            if raw.startswith("json"):
                raw = raw[4:].strip()
    return json.loads(raw)


def verify_evidence(
    hypotheses: list[dict[str, Any]],
    tool_observations: dict[str, str],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Kiểm tra mỗi hypothesis có call_id tồn tại và excerpt xuất hiện trong observation."""
    verified: list[dict[str, Any]] = []
    errors: list[str] = []

    for idx, hyp in enumerate(hypotheses):
        ev_list = hyp.get("evidence") or []
        if not ev_list:
            errors.append(f"Hypothesis #{idx + 1} thiếu mảng evidence")
            continue

        valid_evs = []
        for ev in ev_list:
            cid = str(ev.get("call_id", ""))
            excerpt = str(ev.get("excerpt", "")).strip()
            obs_text = tool_observations.get(cid)
            if obs_text is None:
                errors.append(f"Hypothesis #{idx + 1}: call_id '{cid}' không tồn tại trong trace")
                continue
            if not excerpt or excerpt.lower() not in obs_text.lower():
                errors.append(
                    f"Hypothesis #{idx + 1}: excerpt '{excerpt}' không khớp observation của '{cid}'"
                )
                continue
            valid_evs.append(ev)

        if valid_evs:
            clean_hyp = dict(hyp)
            clean_hyp["evidence"] = valid_evs
            clean_hyp["verified"] = True
            verified.append(clean_hyp)

    return verified, errors


class ReActAgent:
    """Agent chẩn đoán RCA v1 theo vòng lặp Thought -> Action -> Observation."""

    def __init__(
        self,
        toolkit: ReadOnlyToolkit | None = None,
        llm: LLMProvider | None = None,
        config: AgentConfig | None = None,
    ) -> None:
        self.toolkit = toolkit or ReadOnlyToolkit()
        self.llm = llm or ScriptedFakeLLM()
        self.config = config or AgentConfig()
        self.system_prompt = PROMPT_PATH.read_text(encoding="utf-8") if PROMPT_PATH.is_file() else ""

    def diagnose(self, incident_context: dict[str, Any]) -> dict[str, Any]:
        started = time.perf_counter()
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": self.system_prompt},
            {
                "role": "user",
                "content": json.dumps({"incident": incident_context}, ensure_ascii=False),
            },
        ]
        tools = self.toolkit.definitions()
        trace: list[dict[str, Any]] = []
        tool_observations: dict[str, str] = {}
        call_counter = 0
        total_tokens = 0
        final_hypotheses: list[dict[str, Any]] = []
        status = "completed"
        verification_errors: list[str] = []

        for step in range(1, self.config.max_steps + 1):
            if (time.perf_counter() - started) > self.config.timeout_s:
                status = "timeout"
                break
            if total_tokens >= self.config.max_tokens:
                status = "token_budget_exceeded"
                break

            resp = self.llm.complete(messages, tools)
            total_tokens += resp.tokens_used

            if resp.tool_calls:
                step_calls = []
                for tc in resp.tool_calls:
                    call_counter += 1
                    cid = f"call_{call_counter:03d}"
                    tname = str(tc.get("name", ""))
                    targs = tc.get("args") or {}
                    result = self.toolkit.call(tname, targs)
                    result_str = json.dumps(result, ensure_ascii=False)
                    tool_observations[cid] = result_str
                    step_calls.append({"call_id": cid, "tool": tname, "args": targs, "result": result})
                    messages.append({
                        "role": "tool",
                        "name": tname,
                        "content": json.dumps({"call_id": cid, "result": result}, ensure_ascii=False),
                    })
                trace.append({"step": step, "thought": resp.content, "actions": step_calls})
                continue

            # Không còn tool call -> parse kết luận cuối
            trace.append({"step": step, "thought": resp.content, "actions": []})
            try:
                parsed = _extract_json_block(resp.content)
                raw_hyps = parsed.get("hypotheses", [])
            except Exception as exc:  # noqa: BLE001
                status = "invalid_output"
                verification_errors.append(f"Không parse được JSON đầu ra: {exc}")
                break

            if self.config.require_verified_evidence:
                verified, errs = verify_evidence(raw_hyps, tool_observations)
                verification_errors.extend(errs)
                final_hypotheses = verified
                if not verified:
                    status = "rejected_unverified"
            else:
                final_hypotheses = raw_hyps
            break
        else:
            status = "max_steps_exceeded"

        elapsed_s = round(time.perf_counter() - started, 4)
        return {
            "incident_id": incident_context.get("incident_id", "INC-UNKNOWN"),
            "prompt_version": PROMPT_VERSION,
            "status": status,
            "steps_taken": len(trace),
            "total_tokens": total_tokens,
            "elapsed_seconds": elapsed_s,
            "hypotheses": final_hypotheses,
            "verification_errors": verification_errors,
            "trace": trace,
        }
