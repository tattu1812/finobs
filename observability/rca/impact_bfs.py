"""Lan truyền tác động xuôi dòng bằng BFS trên đồ thị lineage (AI-B, Sprint 4).

Hỗ trợ cả:
  - Đồ thị giả (fixture dict ``{nodes, edges}``) để test nhanh.
  - Đồ thị thật từ Marquez qua ``agent.tools.registry.Toolbox``.
Xử lý được chu trình (cycle) và đồ thị rỗng.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass


@dataclass(frozen=True)
class ImpactedNode:
    node_id: str
    kind: str       # dataset | job | dashboard
    name: str
    tier: str       # raw | bronze | silver | gold | dashboard | unknown
    distance: int

    def as_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "kind": self.kind,
            "name": self.name,
            "tier": self.tier,
            "distance": self.distance,
        }


def infer_tier(name: str, kind: str = "dataset") -> str:
    lower = name.lower()
    if kind == "dashboard" or "dashboard" in lower or "grafana" in lower:
        return "dashboard"
    if "gold." in lower or lower.startswith("gold"):
        return "gold"
    if "silver." in lower or lower.startswith("silver"):
        return "silver"
    if "bronze." in lower or lower.startswith("bronze"):
        return "bronze"
    if "raw." in lower or lower.startswith("raw"):
        return "raw"
    return "unknown"


def bfs_downstream_impact(graph: dict, root_node_id: str) -> list[ImpactedNode]:
    """Duyệt BFS xuôi dòng từ ``root_node_id`` trên đồ thị ``{nodes, edges}``.

    Không bao gồm ``root_node_id`` trong kết quả (chỉ các node bị ảnh hưởng ở khoảng cách >= 1).
    """
    nodes_list = graph.get("nodes") or []
    edges_list = graph.get("edges") or []
    if not nodes_list:
        return []

    nodes_by_id = {n["id"]: n for n in nodes_list}
    if root_node_id not in nodes_by_id:
        # Thử khớp tiền tố dataset: nếu caller truyền tên asset trần
        candidate = f"dataset:{root_node_id}"
        if candidate in nodes_by_id:
            root_node_id = candidate
        else:
            return []

    adj: dict[str, list[str]] = {}
    for edge in edges_list:
        src = edge.get("from")
        dst = edge.get("to")
        if src and dst:
            adj.setdefault(src, []).append(dst)

    visited: set[str] = {root_node_id}
    queue: deque[tuple[str, int]] = deque([(root_node_id, 0)])
    impacted: list[ImpactedNode] = []

    while queue:
        curr, dist = queue.popleft()
        if dist > 0:
            meta = nodes_by_id.get(curr, {"kind": "dataset", "name": curr})
            kind = meta.get("kind", "dataset")
            name = meta.get("name", curr)
            impacted.append(ImpactedNode(
                node_id=curr,
                kind=kind,
                name=name,
                tier=infer_tier(name, kind),
                distance=dist,
            ))
        for nxt in sorted(adj.get(curr, [])):
            if nxt not in visited:
                visited.add(nxt)
                queue.append((nxt, dist + 1))

    return impacted


def are_lineage_connected(graph: dict, asset_a: str, asset_b: str) -> bool:
    """Kiểm tra hai asset có nằm trên cùng một đường xuôi/ngược dòng hay không."""
    if asset_a == asset_b:
        return True
    down_a = {n.name for n in bfs_downstream_impact(graph, f"dataset:{asset_a}")}
    down_b = {n.name for n in bfs_downstream_impact(graph, f"dataset:{asset_b}")}
    short_a = asset_a.split(":", 1)[-1]
    short_b = asset_b.split(":", 1)[-1]
    return (short_b in down_a) or (asset_b in down_a) or (short_a in down_b) or (asset_a in down_b)
