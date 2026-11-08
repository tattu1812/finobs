"""
Client đọc lineage từ Marquez (REST API v1).

    GET /api/v1/lineage?nodeId=dataset:<namespace>:<name>&depth=<n>
        -> {"graph": [{"id": "...", "type": "DATASET"|"JOB", "data": {...},
                       "inEdges": [{"origin": ..., "destination": ...}], "outEdges": [...]}]}

Marquez trả về cả thành phần liên thông quanh node. Client tự duyệt cạnh để lấy
thượng nguồn (upstream) / hạ nguồn (downstream).

Hai transport:
  - HttpTransport(base_url)  : gọi Marquez thật (mặc định http://localhost:5000)
  - FakeTransport()          : trả JSON CÙNG ĐỊNH DẠNG, dựng từ agent/tools/fake_world.py,
                               nên client chạy đúng một đường code ở cả hai chế độ.

Quy ước định danh: asset "finobs:raw.binance_trades" <-> Marquez node
"dataset:finobs:raw.binance_trades"; job "job:finobs:bronze_load".
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from agent.tools import fake_world

DEFAULT_URL = os.environ.get("MARQUEZ_URL", "http://localhost:5000")
PRODUCER = "https://github.com/tattu1812/finobs/agent/tools"
OL_SCHEMA = "https://openlineage.io/spec/2-0-2/OpenLineage.json"


class MarquezError(RuntimeError):
    pass


class Transport(Protocol):
    def get(self, path: str, params: dict) -> dict: ...


# ---------------------------------------------------------------- định danh
def dataset_node_id(asset: str) -> str:
    if asset.startswith(("dataset:", "job:")):
        return asset
    if ":" not in asset:
        raise ValueError(f"asset phải có dạng <namespace>:<tên>, nhận được '{asset}'")
    return f"dataset:{asset}"


def split_node_id(node_id: str) -> tuple[str, str, str]:
    """'dataset:finobs:raw.x' -> ('dataset', 'finobs', 'raw.x')."""
    kind, namespace, name = node_id.split(":", 2)
    return kind.lower(), namespace, name


# ---------------------------------------------------------------- transport
class HttpTransport:
    def __init__(self, base_url: str = DEFAULT_URL, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def get(self, path: str, params: dict) -> dict:
        url = f"{self.base_url}{path}?{urllib.parse.urlencode(params)}"
        try:
            with urllib.request.urlopen(url, timeout=self.timeout) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            exc.close()
            if exc.code == 404:
                raise MarquezError(f"Marquez không có node {params.get('nodeId')}") from exc
            raise MarquezError(f"Marquez trả HTTP {exc.code} cho {url}") from exc
        except urllib.error.URLError as exc:
            raise MarquezError(f"Không kết nối được Marquez ở {self.base_url}: {exc.reason}") from exc

    def post(self, path: str, body: dict) -> int:
        req = urllib.request.Request(
            f"{self.base_url}{path}", data=json.dumps(body).encode(), method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            return resp.status


class FakeTransport:
    """Giả lập GET /api/v1/lineage của Marquez từ topology trong fake_world.JOBS."""

    def __init__(self, jobs: dict[str, tuple[list[str], list[str]]] | None = None,
                 namespace: str = fake_world.NAMESPACE):
        self.jobs = jobs or fake_world.JOBS
        self.ns = namespace

    def _graph(self) -> dict[str, dict]:
        nodes: dict[str, dict] = {}

        def node(node_id: str, kind: str) -> dict:
            if node_id not in nodes:
                _, ns, name = split_node_id(node_id)
                nodes[node_id] = {"id": node_id, "type": kind,
                                  "data": {"namespace": ns, "name": name},
                                  "inEdges": [], "outEdges": []}
            return nodes[node_id]

        for job, (inputs, outputs) in self.jobs.items():
            jid = f"job:{self.ns}:{job}"
            node(jid, "JOB")
            for ds in inputs:
                edge = {"origin": f"dataset:{self.ns}:{ds}", "destination": jid}
                node(edge["origin"], "DATASET")["outEdges"].append(edge)
                nodes[jid]["inEdges"].append(edge)
            for ds in outputs:
                edge = {"origin": jid, "destination": f"dataset:{self.ns}:{ds}"}
                nodes[jid]["outEdges"].append(edge)
                node(edge["destination"], "DATASET")["inEdges"].append(edge)
        return nodes

    def get(self, path: str, params: dict) -> dict:
        if path != "/api/v1/lineage":
            raise MarquezError(f"FakeTransport chỉ hỗ trợ /api/v1/lineage, không hỗ trợ {path}")
        nodes = self._graph()
        if params["nodeId"] not in nodes:
            raise MarquezError(f"Marquez không có node {params['nodeId']}")
        return {"graph": list(nodes.values())}


# ---------------------------------------------------------------- đồ thị
@dataclass
class LineageGraph:
    nodes: dict[str, str]                 # node_id -> "dataset" | "job"
    edges: set[tuple[str, str]]           # (origin, destination)

    @classmethod
    def from_marquez(cls, payload: dict) -> LineageGraph:
        nodes, edges = {}, set()
        for n in payload.get("graph", []):
            nodes[n["id"]] = n["type"].lower()
            for e in n.get("inEdges", []) + n.get("outEdges", []):
                edges.add((e["origin"], e["destination"]))
        return cls(nodes, edges)

    def walk(self, start: str, direction: str, depth: int) -> tuple[dict[str, int], set[tuple[str, str]]]:
        """BFS từ start theo direction ('upstream'|'downstream'), tối đa depth cạnh.

        Trả về ({node_id: số bước từ start}, các cạnh đã đi qua)."""
        forward = direction == "downstream"
        dist, used, queue = {start: 0}, set(), deque([start])
        while queue:
            cur = queue.popleft()
            if dist[cur] >= depth:
                continue
            for o, t in sorted(self.edges):
                src, nxt = (o, t) if forward else (t, o)
                if src == cur:
                    used.add((o, t))
                    if nxt not in dist:
                        dist[nxt] = dist[cur] + 1
                        queue.append(nxt)
        return dist, used


class MarquezClient:
    def __init__(self, transport: Transport | None = None):
        self.transport = transport or HttpTransport()

    def lineage(self, asset: str, depth: int = 20) -> LineageGraph:
        payload = self.transport.get("/api/v1/lineage", {"nodeId": dataset_node_id(asset), "depth": depth})
        return LineageGraph.from_marquez(payload)


# ---------------------------------------------------------------- seed Marquez thật
def openlineage_events(jobs: dict[str, tuple[list[str], list[str]]] | None = None,
                       namespace: str = fake_world.NAMESPACE) -> list[dict]:
    """Một sự kiện COMPLETE cho mỗi job, đủ để Marquez dựng đúng đồ thị như FakeTransport."""
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return [
        {
            "eventType": "COMPLETE",
            "eventTime": now,
            "run": {"runId": str(uuid.uuid4())},
            "job": {"namespace": namespace, "name": job},
            "inputs": [{"namespace": namespace, "name": ds} for ds in inputs],
            "outputs": [{"namespace": namespace, "name": ds} for ds in outputs],
            "producer": PRODUCER,
            "schemaURL": OL_SCHEMA,
        }
        for job, (inputs, outputs) in (jobs or fake_world.JOBS).items()
    ]
