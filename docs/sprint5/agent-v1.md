# Agent Chẩn đoán RCA v1 — Sprint 5 (AI)

## 1. Kiến trúc ReAct & 5 Tool Chỉ-Đọc

Agent v1 (`agent/react_agent.py`) chạy vòng lặp `Thought -> Action -> Observation` qua 5 tool chỉ-đọc trong [`agent/tools/readonly_tools.py`](../../agent/tools/readonly_tools.py):
1. `read_metrics`: đọc metric store (qua `Toolbox`).
2. `read_lineage`: đọc đồ thị lineage upstream/downstream.
3. `query_table`: chỉ cho phép duy nhất 1 lệnh `SELECT` trên allowlist bảng (`bronze.trades_raw`, `bronze.binance_official_trades`, `silver.trades`, `gold.ohlcv_1m`, `gold.cross_exchange_spread`, `metric_observations`), ép `LIMIT <= 100`, chặn comment SQL và multi-statement.
4. `read_incident`: đọc sự cố, cảnh báo và vi phạm contract.
5. `read_config`: chỉ đọc file trong `config/` hoặc `slos.yaml` / `metric_catalog.yaml`; chặn path traversal (`..`) và chặn `.env`/secrets.

## 2. Evidence Verifier & Giới hạn Tài nguyên

- Mọi giả thuyết đầu ra phải đi kèm mảng `evidence: [{call_id, tool, excerpt}]`.
- `verify_evidence()` kiểm tra `call_id` phải nằm trong trace thực tế và `excerpt` phải khớp với kết quả tool trả về. Kết luận bịa đặt bị gắn trạng thái `rejected_unverified`.
- Giới hạn an toàn: `max_steps=8`, `timeout_s=30.0`, `max_tokens=8000`.
- Prompt hệ thống được quản lý phiên bản tại [`agent/prompts/react_v1.md`](../../agent/prompts/react_v1.md) (`v1.0.0`).

## 3. Cấu hình LLM (Không hardcode API key)

- Mặc định offline/CI dùng `ScriptedFakeLLM` (tất định).
- Khi dùng LLM thật, đặt các biến trong `.env` (tham khảo `.env.example`): `FINOBS_LLM_API_KEY`, `FINOBS_LLM_BASE_URL`, `FINOBS_LLM_MODEL`.
