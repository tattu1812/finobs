"""Client Trino tối giản (giao thức HTTP /v1/statement), chỉ dùng thư viện chuẩn.

    rows = TrinoClient("http://trino:8080").query("SELECT 1")   # -> [{'_col0': 1}]
"""

from __future__ import annotations

import json
import time
import urllib.request


class TrinoError(RuntimeError):
    pass


class TrinoClient:
    def __init__(self, url: str, user: str = "finobs", catalog: str = "iceberg", timeout: float = 60.0):
        self.url = url.rstrip("/")
        self.headers = {"X-Trino-User": user, "X-Trino-Catalog": catalog, "Content-Type": "text/plain"}
        self.timeout = timeout

    def _call(self, url: str, data: bytes | None = None) -> dict:
        request = urllib.request.Request(url, data=data, method="POST" if data is not None else "GET",
                                         headers=self.headers)
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            return json.load(response)

    def query(self, sql: str) -> list[dict]:
        deadline = time.monotonic() + self.timeout
        page = self._call(f"{self.url}/v1/statement", sql.encode())
        columns, rows = None, []
        while True:
            if "error" in page:
                raise TrinoError(page["error"].get("message", str(page["error"])))
            if page.get("columns") and columns is None:
                columns = [c["name"] for c in page["columns"]]
            for values in page.get("data", []):
                rows.append(dict(zip(columns, values)))
            next_uri = page.get("nextUri")
            if not next_uri:
                return rows
            if time.monotonic() > deadline:
                raise TrinoError(f"truy vấn quá {self.timeout} s")
            page = self._call(next_uri)
