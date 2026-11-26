"""5 tool chỉ-đọc có kiểm soát chặt chẽ cho Agent v1 (AI, Sprint 5):
  1. read_metrics
  2. read_lineage
  3. query_table
  4. read_incident
  5. read_config
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from agent.tools.registry import Toolbox

ROOT = Path(__file__).resolve().parents[2]

ALLOWED_TABLES = frozenset({
    "bronze.trades_raw",
    "bronze.binance_official_trades",
    "silver.trades",
    "gold.ohlcv_1m",
    "gold.cross_exchange_spread",
    "metric_observations",
})

FORBIDDEN_SQL_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|EXEC|EXECUTE|COPY|CALL|MERGE|REPLACE|ATTACH|DETACH|PRAGMA)\b",
    re.IGNORECASE,
)
SQL_COMMENTS = re.compile(r"(--|/\*|\*/)")
FROM_TABLE_RE = re.compile(r"\bFROM\s+([a-zA-Z_][a-zA-Z0-9_.]*)", re.IGNORECASE)
JOIN_TABLE_RE = re.compile(r"\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_.]*)", re.IGNORECASE)
LIMIT_RE = re.compile(r"\bLIMIT\s+(\d+)", re.IGNORECASE)

ALLOWED_CONFIG_PREFIXES = (
    "config/",
    "observability/slo/slos.yaml",
    "observability/metric_catalog.yaml",
)

FORBIDDEN_CONFIG_NAMES = re.compile(r"(^|/)\.env(\.|$)|secret|token|password|credential", re.IGNORECASE)


class SecurityValidationError(ValueError):
    """Lỗi khi tham số đầu vào vi phạm quy tắc chỉ-đọc hoặc allowlist."""


def validate_readonly_sql(sql: str, max_limit: int = 100) -> dict[str, Any]:
    """Kiểm tra câu SQL chỉ được phép là 1 SELECT trên bảng nằm trong allowlist."""
    cleaned = sql.strip()
    if not cleaned:
        raise SecurityValidationError("Câu SQL rỗng")
    if SQL_COMMENTS.search(cleaned):
        raise SecurityValidationError("Không cho phép SQL comment (-- hoặc /* */)")

    # Chặn multi-statement: chỉ cho phép tối đa 1 dấu ; ở cuối cùng
    body = cleaned[:-1].strip() if cleaned.endswith(";") else cleaned
    if ";" in body:
        raise SecurityValidationError("Không cho phép nhiều câu lệnh SQL (multi-statement)")

    if not re.match(r"^\s*SELECT\b", body, re.IGNORECASE):
        raise SecurityValidationError("Chỉ cho phép câu lệnh SELECT")

    if FORBIDDEN_SQL_KEYWORDS.search(body):
        raise SecurityValidationError("Câu SQL chứa từ khóa thay đổi dữ liệu bị cấm")

    tables = FROM_TABLE_RE.findall(body) + JOIN_TABLE_RE.findall(body)
    if not tables:
        raise SecurityValidationError("Không xác định được bảng trong mệnh đề FROM")
    for tbl in tables:
        if tbl.lower() not in ALLOWED_TABLES:
            raise SecurityValidationError(
                f"Bảng '{tbl}' không nằm trong allowlist: {sorted(ALLOWED_TABLES)}"
            )

    limit_match = LIMIT_RE.search(body)
    limit_val = int(limit_match.group(1)) if limit_match else max_limit
    if limit_val > max_limit:
        raise SecurityValidationError(f"LIMIT {limit_val} vượt ngưỡng tối đa {max_limit}")

    return {"sql": body, "tables": [t.lower() for t in tables], "limit": limit_val}


def resolve_safe_config_path(rel_path: str, repo_root: Path = ROOT) -> Path:
    """Chặn path traversal, symlink thoát root và các file bí mật (.env)."""
    if not rel_path or "\x00" in rel_path:
        raise SecurityValidationError("Đường dẫn cấu hình không hợp lệ")
    norm = rel_path.replace("\\", "/").lstrip("/")
    if FORBIDDEN_CONFIG_NAMES.search(norm):
        raise SecurityValidationError(f"Từ chối truy cập file nhạy cảm: {rel_path}")
    if ".." in Path(norm).parts:
        raise SecurityValidationError("Không cho phép path traversal ('..')")
    if not any(norm == p or norm.startswith(p) for p in ALLOWED_CONFIG_PREFIXES):
        raise SecurityValidationError(
            f"Đường dẫn '{rel_path}' nằm ngoài phạm vi config cho phép"
        )
    root_resolved = repo_root.resolve()
    target = (root_resolved / norm).resolve()
    try:
        target.relative_to(root_resolved)
    except ValueError as exc:
        raise SecurityValidationError("Đường dẫn vượt ra ngoài thư mục repo") from exc
    if not target.is_file():
        raise FileNotFoundError(f"Không tìm thấy file cấu hình: {rel_path}")
    return target


class ReadOnlyToolkit:
    """Bộ 5 tool chỉ-đọc cho ReAct Agent v1 (Sprint 5)."""

    TOOL_NAMES = ("read_metrics", "read_lineage", "query_table", "read_incident", "read_config")

    def __init__(
        self,
        toolbox: Toolbox | None = None,
        *,
        incidents: list[dict[str, Any]] | None = None,
        table_rows: dict[str, list[dict[str, Any]]] | None = None,
        repo_root: Path = ROOT,
    ) -> None:
        self.toolbox = toolbox or Toolbox.fake()
        self.incidents = incidents or []
        self.table_rows = table_rows or self._default_fake_tables()
        self.repo_root = repo_root

    @staticmethod
    def _default_fake_tables() -> dict[str, list[dict[str, Any]]]:
        return {
            "bronze.trades_raw": [
                {"source": "binance", "symbol_native": "ETHUSDT", "status": "schema_drift", "count": 880},
                {"source": "coinbase", "symbol_native": "BTC-USD", "status": "ok", "count": 920},
            ],
            "silver.trades": [
                {"source": "binance", "symbol_canonical": "ETH-USDT", "null_price_count": 42},
            ],
            "gold.ohlcv_1m": [
                {"symbol_canonical": "ETH-USDT", "freshness_lag_s": 420},
            ],
            "metric_observations": [
                {"metric_name": "contract_violation_rate", "source": "binance", "value": 0.18},
            ],
        }

    def definitions(self) -> list[dict[str, Any]]:
        return [
            {
                "name": "read_metrics",
                "description": "Đọc chuỗi thời gian metric từ metric store (chỉ-đọc).",
                "input_schema": {
                    "type": "object",
                    "required": ["metric", "start", "end"],
                    "properties": {
                        "metric": {"type": "string"},
                        "start": {"type": "string"},
                        "end": {"type": "string"},
                        "group": {"type": "object"},
                    },
                },
            },
            {
                "name": "read_lineage",
                "description": "Đọc đồ thị lineage upstream/downstream của một asset.",
                "input_schema": {
                    "type": "object",
                    "required": ["asset"],
                    "properties": {
                        "asset": {"type": "string"},
                        "direction": {"type": "string", "enum": ["upstream", "downstream", "both"]},
                        "depth": {"type": "integer", "minimum": 1, "maximum": 10},
                    },
                },
            },
            {
                "name": "query_table",
                "description": "Truy vấn SELECT chỉ-đọc trên bảng allowlist (tối đa 100 dòng).",
                "input_schema": {
                    "type": "object",
                    "required": ["sql"],
                    "properties": {"sql": {"type": "string"}},
                },
            },
            {
                "name": "read_incident",
                "description": "Đọc thông tin sự cố, cảnh báo liên quan và vi phạm contract trong cửa sổ.",
                "input_schema": {
                    "type": "object",
                    "required": ["start", "end"],
                    "properties": {
                        "incident_id": {"type": "string"},
                        "start": {"type": "string"},
                        "end": {"type": "string"},
                        "source": {"type": "string"},
                    },
                },
            },
            {
                "name": "read_config",
                "description": "Đọc file cấu hình/data contract trong config/ (chặn .env và path traversal).",
                "input_schema": {
                    "type": "object",
                    "required": ["path"],
                    "properties": {"path": {"type": "string"}},
                },
            },
        ]

    def call(self, name: str, args: dict[str, Any] | None = None) -> dict[str, Any]:
        args = dict(args or {})
        if name == "read_metrics":
            return self.toolbox.call("query_metric", args)

        if name == "read_lineage":
            return self.toolbox.call("get_lineage", args)

        if name == "query_table":
            try:
                parsed = validate_readonly_sql(str(args.get("sql", "")))
            except SecurityValidationError as exc:
                return {"error": {"code": "forbidden_sql", "message": str(exc)}}
            tbl = parsed["tables"][0]
            rows = self.table_rows.get(tbl, [])[: parsed["limit"]]
            return {"table": tbl, "row_count": len(rows), "rows": rows}

        if name == "read_incident":
            inc_id = args.get("incident_id")
            if inc_id and self.incidents:
                matched = [i for i in self.incidents if i.get("incident_id") == inc_id]
                if matched:
                    return {"incident": matched[0]}
            start = args.get("start", "2026-10-27T03:00:00.000Z")
            end = args.get("end", "2026-10-27T03:20:00.000Z")
            alerts = self.toolbox.call("list_alerts", {"start": start, "end": end})
            source = args.get("source", "binance")
            violations = self.toolbox.call(
                "get_contract_violations", {"source": source, "start": start, "end": end}
            )
            return {
                "incident_id": inc_id or "INC-CURRENT",
                "alerts": alerts,
                "contract_violations": violations,
            }

        if name == "read_config":
            try:
                target = resolve_safe_config_path(str(args.get("path", "")), repo_root=self.repo_root)
            except SecurityValidationError as exc:
                return {"error": {"code": "forbidden_path", "message": str(exc)}}
            except FileNotFoundError as exc:
                return {"error": {"code": "not_found", "message": str(exc)}}
            content = target.read_text(encoding="utf-8")
            return {"path": str(args["path"]), "content": content[:4000]}

        return {"error": {"code": "unknown_tool", "message": f"Không có tool '{name}'"}}
