"""
In đồ thị lineage của một dataset, đọc qua tool get_lineage (tức là qua Marquez).

    python -m agent.tools.lineage_graph                                   # Marquez giả
    python -m agent.tools.lineage_graph --asset finobs:raw.binance_trades --direction downstream
    python -m agent.tools.lineage_graph --marquez-url http://localhost:5000   # Marquez thật
    python -m agent.tools.lineage_graph --format mermaid > lineage.mmd
    python -m agent.tools.lineage_graph --alerts                         # đánh dấu node đang có alert

Chạy bằng "python -m" từ gốc repo để import được package agent.
"""

from __future__ import annotations

import argparse
import json
import sys

from agent.tools.registry import Toolbox

ARROW = {"upstream": "◀", "downstream": "▶"}


def label(node: dict, marks: dict[str, list[str]]) -> str:
    text = f"{node['namespace']}:{node['name']}"
    if node["kind"] == "job":
        text = f"[job] {text}"
    if marks.get(node["id"]):
        text += f"   ⚠ {', '.join(marks[node['id']])}"
    return text


def render_tree(result: dict, direction: str, marks: dict[str, list[str]]) -> list[str]:
    nodes = {n["id"]: n for n in result["nodes"]}
    children: dict[str, list[str]] = {}
    for e in result["edges"]:
        parent, child = (e["to"], e["from"]) if direction == "upstream" else (e["from"], e["to"])
        children.setdefault(parent, []).append(child)

    root = f"dataset:{result['asset']}"
    lines = [label(nodes[root], marks)]
    printed = {root}

    def visit(node_id: str, prefix: str):
        kids = sorted(children.get(node_id, []))
        for i, kid in enumerate(kids):
            last = i == len(kids) - 1
            seen = kid in printed
            suffix = "  (đã in ở trên)" if seen and children.get(kid) else ""
            lines.append(f"{prefix}{'└── ' if last else '├── '}{ARROW[direction]} {label(nodes[kid], marks)}{suffix}")
            if not seen:
                printed.add(kid)
                visit(kid, prefix + ("    " if last else "│   "))

    visit(root, "")
    return lines


def render_mermaid(result: dict, marks: dict[str, list[str]]) -> list[str]:
    ids = {n["id"]: f"n{i}" for i, n in enumerate(result["nodes"])}
    lines = ["flowchart LR"]
    for n in result["nodes"]:
        text = label(n, marks).replace('"', "'")
        shape = f'(["{text}"])' if n["kind"] == "job" else f'["{text}"]'
        lines.append(f"  {ids[n['id']]}{shape}")
    lines += [f"  {ids[e['from']]} --> {ids[e['to']]}" for e in result["edges"]]
    lines += [f"  style {ids[i]} stroke:#d33,stroke-width:3px" for i in marks if i in ids]
    return lines


def render_dot(result: dict, marks: dict[str, list[str]]) -> list[str]:
    lines = ["digraph lineage {", "  rankdir=LR;"]
    for n in result["nodes"]:
        attrs = ["shape=box" if n["kind"] == "dataset" else "shape=ellipse", f'label="{label(n, marks)}"']
        if n["id"] in marks:
            attrs.append("color=red")
        lines.append(f'  "{n["id"]}" [{", ".join(attrs)}];')
    lines += [f'  "{e["from"]}" -> "{e["to"]}";' for e in result["edges"]]
    lines.append("}")
    return lines


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--asset", default="finobs:gold.ohlcv_1m")
    ap.add_argument("--direction", choices=["upstream", "downstream", "both"], default="upstream")
    ap.add_argument("--depth", type=int, default=10)
    ap.add_argument("--marquez-url", help="bỏ trống để dùng Marquez giả")
    ap.add_argument("--format", choices=["tree", "mermaid", "dot", "json"], default="tree")
    ap.add_argument("--alerts", action="store_true", help="đánh dấu dataset có alert (dữ liệu giả, cả window)")
    args = ap.parse_args()

    tb = Toolbox.fake(marquez_url=args.marquez_url)
    result = tb.call("get_lineage", {"asset": args.asset, "direction": args.direction, "depth": args.depth})
    if "error" in result:
        print(f"Lỗi [{result['error']['code']}]: {result['error']['message']}", file=sys.stderr)
        return 1

    marks: dict[str, list[str]] = {}
    if args.alerts:
        start = "2026-10-27T03:00:00.000Z"
        end = "2026-10-27T03:20:00.000Z"
        for a in tb.call("list_alerts", {"start": start, "end": end})["alerts"]:
            marks.setdefault(f"dataset:{a['asset']}", []).append(a["alert_id"])

    if args.format == "json":
        print(json.dumps(result, ensure_ascii=False, indent=2))
    elif args.format == "mermaid":
        print("\n".join(render_mermaid(result, marks)))
    elif args.format == "dot":
        print("\n".join(render_dot(result, marks)))
    else:
        directions = ["upstream", "downstream"] if args.direction == "both" else [args.direction]
        for d in directions:
            print(f"# {d} của {args.asset}")
            print("\n".join(render_tree(result, d, marks)))
            print()
        print(f"{len(result['datasets'])} dataset, {len(result['jobs'])} job, {len(result['edges'])} cạnh")
    return 0


if __name__ == "__main__":
    sys.exit(main())
