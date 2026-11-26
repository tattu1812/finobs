"""RCA heuristic theo lineage (FR-18 phương pháp 1; Sprint 3, AI-4).

Luật (đặc tả 5.3): chọn TỔ TIÊN SỚM NHẤT có bất thường mà MỌI NÚT CON đều bất thường theo.

Cụ thể, với mỗi dataset đang có alert (ứng viên c):
  1. qualifies          mọi dataset con trực tiếp của c cũng có alert (lá thì đúng hiển nhiên)
  2. has_anomalous_anc  có tổ tiên nào của c đang có alert không (có -> c chỉ là nơi lỗi lan tới)
  3. explained          số dataset có alert nằm dưới c (c giải thích được bao nhiêu alert)
  4. first_alert        thời điểm alert sớm nhất của c (lỗi gốc thường lộ ra trước)
Xếp: qualifies trước, không có tổ tiên bất thường trước, explained nhiều trước, alert sớm trước.

Đồ thị dataset suy từ lineage: d1 -> job -> d2 thành cạnh d1 -> d2.

    from agent.tools.registry import Toolbox
    from observability.rca.heuristic import rank_from_toolbox
    rank_from_toolbox(Toolbox.fake(), "2026-10-27T03:00:00.000Z", "2026-10-27T03:20:00.000Z")
"""

from __future__ import annotations

from collections import defaultdict

METHOD = "heuristic_lineage"


def dataset_edges(lineage_edges) -> set[tuple[str, str]]:
    """Cạnh lineage (node id 'dataset:ns:name' / 'job:ns:name') -> cạnh dataset 'ns:name' -> 'ns:name'."""
    into_job, out_of_job = defaultdict(set), defaultdict(set)
    for src, dst in lineage_edges:
        if src.startswith("dataset:") and dst.startswith("job:"):
            into_job[dst].add(src.split(":", 1)[1])
        elif src.startswith("job:") and dst.startswith("dataset:"):
            out_of_job[src].add(dst.split(":", 1)[1])
    return {(a, b) for job in into_job for a in into_job[job] for b in out_of_job.get(job, ())}


def _reach(start: str, nxt: dict[str, set[str]]) -> set[str]:
    seen, stack = set(), [start]
    while stack:
        for node in nxt.get(stack.pop(), ()):
            if node not in seen:
                seen.add(node)
                stack.append(node)
    return seen


def rank_candidates(edges: set[tuple[str, str]], anomalies: dict[str, str], k: int = 3) -> list[dict]:
    """edges: cạnh dataset; anomalies: {asset: thời điểm alert sớm nhất (ISO)}. Trả top-k."""
    children, parents = defaultdict(set), defaultdict(set)
    for a, b in edges:
        children[a].add(b)
        parents[b].add(a)

    scored = []
    for asset, first_alert in anomalies.items():
        kids = children.get(asset, set())
        qualifies = all(kid in anomalies for kid in kids)
        anomalous_anc = sorted(_reach(asset, parents) & anomalies.keys())
        explained = sorted(_reach(asset, children) & anomalies.keys())
        key = (not qualifies, bool(anomalous_anc), -len(explained), first_alert)
        evidence = [f"alert sớm nhất lúc {first_alert}"]
        evidence.append(f"giải thích {len(explained)} alert phía sau: {', '.join(explained)}" if explained
                        else "không có alert nào phía sau")
        if kids and not qualifies:
            evidence.append(f"có dataset con không bất thường: {', '.join(sorted(k_ for k_ in kids if k_ not in anomalies))}")
        if anomalous_anc:
            evidence.append(f"có tổ tiên cũng bất thường: {', '.join(anomalous_anc)} (nhiều khả năng chỉ là nơi lỗi lan tới)")
        scored.append((key, asset, evidence))

    scored.sort()
    total = len(scored)
    return [
        {"method": METHOD, "rank": i + 1, "candidate_node": asset,
         "score": round((total - i) / total, 4), "evidence": evidence}
        for i, (_key, asset, evidence) in enumerate(scored[:k])
    ]


def rank_from_toolbox(toolbox, start: str, end: str, k: int = 3) -> list[dict]:
    """Lấy alert (list_alerts) và lineage (get_lineage) qua tool của agent, rồi xếp hạng."""
    alerts = toolbox.call("list_alerts", {"start": start, "end": end})
    if "error" in alerts:
        raise RuntimeError(alerts["error"])
    anomalies: dict[str, str] = {}
    for alert in alerts["alerts"]:
        asset = alert["asset"]
        anomalies[asset] = min(anomalies.get(asset, alert["detected_at"]), alert["detected_at"])

    lineage_edges = set()
    for asset in anomalies:
        graph = toolbox.call("get_lineage", {"asset": asset, "direction": "both", "depth": 20})
        if "error" not in graph:
            lineage_edges |= {(e["from"], e["to"]) for e in graph["edges"]}
    return rank_candidates(dataset_edges(lineage_edges), anomalies, k)
