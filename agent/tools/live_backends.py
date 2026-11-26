"""
Backend thật cho get_contract_violations và get_job_runs (Sprint 3, AI-5).

  ViolationStore  đọc metric store: contract_violation_count_1m / _rate_1m (số đã kiểm, số vi phạm)
                  + bảng contract_violation_samples (lỗi cụ thể, mã, event_id) do collector ghi.
  MarquezRuns     đọc API runs của Marquez. Mọi job đều phát OpenLineage (producer, job Spark,
                  DAG Airflow) nên một nguồn này phủ cả job streaming lẫn batch.

Giới hạn: by_error và by_symbol đếm trên MẪU (collector giữ tối đa 20 mẫu mỗi nguồn mỗi phút),
còn checked / invalid là số đầy đủ từ metric.
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from urllib.parse import quote

NAMESPACE = "finobs"


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    value = value.astimezone(timezone.utc)
    return value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{value.microsecond // 1000:03d}Z"


def _parse(text: str | None) -> datetime | None:
    if not text:
        return None
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


class ViolationStore:
    def __init__(self, dsn: str | None = None, connect=None):
        if connect is None:
            import psycopg

            def connect():
                return psycopg.connect(dsn, connect_timeout=5)

        self._connect = connect

    def violations(self, source: str, start: datetime, end: datetime, sample_limit: int) -> dict:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT metric_name, window_start, value, sample_count
                    FROM metric_observations
                    WHERE metric_name IN ('contract_violation_count_1m', 'contract_violation_rate_1m')
                      AND dimensions @> %s::jsonb AND status = 'ok'
                      AND window_start >= %s AND window_start < %s""",
                               (json.dumps({"source": source}), start, end))
                metric_rows = cursor.fetchall()
                cursor.execute("""
                    SELECT event_id, checked_at, errors, symbol_native
                    FROM contract_violation_samples
                    WHERE source = %s AND ingest_minute >= %s AND ingest_minute < %s
                    ORDER BY ingest_minute, id""", (source, start, end))
                samples = cursor.fetchall()

        checked = sum(int(n or 0) for name, _, _, n in metric_rows if name == "contract_violation_rate_1m")
        invalid = sum(int(v or 0) for name, _, v, _ in metric_rows if name == "contract_violation_count_1m")
        bad_minutes = [ts for name, ts, v, _ in metric_rows if name == "contract_violation_count_1m" and v]
        by_error, by_symbol = Counter(), Counter()
        for _, _, errors, symbol in samples:
            errors = errors if isinstance(errors, list) else json.loads(errors)
            by_error.update(errors)
            by_symbol[symbol or "?"] += 1
        return {
            "source": source, "start": _iso(start), "end": _iso(end),
            "checked": checked, "invalid": invalid,
            "invalid_rate": round(invalid / checked, 6) if checked else None,
            "first_violation_at": _iso(min(bad_minutes)) if bad_minutes else None,
            "by_error": [{"error": e, "count": c} for e, c in by_error.most_common()],
            "by_symbol": dict(by_symbol),
            "samples": [
                {"event_id": event_id or "?", "checked_at": _iso(checked_at),
                 "errors": errors if isinstance(errors, list) else json.loads(errors)}
                for event_id, checked_at, errors, _ in samples[:sample_limit]
            ],
        }


class MarquezRuns:
    STATES = {"NEW": "RUNNING", "RUNNING": "RUNNING", "COMPLETED": "COMPLETED",
              "FAILED": "FAILED", "ABORTED": "ABORTED"}

    def __init__(self, transport):
        """transport: agent.tools.marquez.HttpTransport (có get(path, params))."""
        self.transport = transport

    def job_runs(self, job: str, start: datetime, end: datetime, limit: int) -> dict:
        payload = self.transport.get(f"/api/v1/namespaces/{NAMESPACE}/jobs/{quote(job, safe='')}/runs",
                                     {"limit": 200})
        runs = []
        for run in payload.get("runs", []):
            started = _parse(run.get("startedAt") or run.get("createdAt"))
            if started is None or not (start <= started < end):
                continue
            error = ((run.get("facets") or {}).get("errorMessage") or {}).get("message")
            runs.append({
                "run_id": run["id"],
                "state": self.STATES.get(run.get("state", ""), "RUNNING"),
                "started_at": _iso(started),
                "ended_at": _iso(_parse(run.get("endedAt"))),
                "error_message": error,
                "output_rows": None,
            })
        runs.sort(key=lambda r: r["started_at"], reverse=True)
        failures = [r["started_at"] for r in runs if r["state"] == "FAILED"]
        return {
            "job": f"{NAMESPACE}:{job}",
            "total_in_window": len(runs),
            "state_counts": dict(Counter(r["state"] for r in runs)),
            "first_failure_at": min(failures, default=None),
            "runs": runs[:limit],
        }
