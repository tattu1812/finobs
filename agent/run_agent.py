"""CLI chạy ReAct Agent v1 (AI, Sprint 5)."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agent.llm.provider import OpenAICompatibleProvider, ScriptedFakeLLM  # noqa: E402
from agent.react_agent import AgentConfig, ReActAgent  # noqa: E402
from agent.tools.readonly_tools import ReadOnlyToolkit  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Chạy FinObs ReAct Agent v1 (chỉ-đọc)")
    parser.add_argument("--incident-id", default="INC-S002")
    parser.add_argument("--source", default="binance")
    parser.add_argument("--start", default="2026-10-27T03:00:00.000Z")
    parser.add_argument("--end", default="2026-10-27T03:20:00.000Z")
    parser.add_argument("--max-steps", type=int, default=8)
    parser.add_argument("--use-live-llm", action="store_true", help="Dùng FINOBS_LLM_API_KEY từ env")
    parser.add_argument("--out", default=None, help="File JSON ghi kết quả")
    args = parser.parse_args(argv)

    llm = (
        OpenAICompatibleProvider()
        if (args.use_live_llm and os.environ.get("FINOBS_LLM_API_KEY"))
        else ScriptedFakeLLM()
    )
    agent = ReActAgent(
        toolkit=ReadOnlyToolkit(),
        llm=llm,
        config=AgentConfig(max_steps=args.max_steps),
    )
    result = agent.diagnose({
        "incident_id": args.incident_id,
        "source": args.source,
        "start": args.start,
        "end": args.end,
    })

    if args.out:
        out_p = Path(args.out)
        out_p.parent.mkdir(parents=True, exist_ok=True)
        out_p.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Đã ghi chẩn đoán ra {out_p} (status={result['status']})")
    else:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
