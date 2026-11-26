# Hợp đồng Giao tiếp giữa các Vai — Sprint 4 (`docs/sprint4-interfaces.md`)

Tài liệu này là hợp đồng giao tiếp chính thức cho Sprint 4 giữa **5 vai**: DE, OB, PL, AI, EV.

---

## 1. DE (Data Engineer)

- **Nguồn chi tiết**: [`docs/sprint4-de-handoff.md`](sprint4-de-handoff.md)
- **Metric Store (`metric_observations`)**:
  - Gap Detection (Binance & Coinbase): `trade_id_gap_count`, `trade_id_gap_missing_total`, `trade_id_duplicate_count`, `trade_id_out_of_order_count` — nhãn `source`, `symbol`.
  - Reconciliation D+1 (Binance): `reconciliation_match_ratio`, `reconciliation_missing_trades`, `reconciliation_extra_trades`, `reconciliation_value_mismatch` — nhãn `source="binance"`, `symbol`, `date="YYYY-MM-DD"`.
- **Data Contract YAML**:
  - [`config/contracts/gap_detection.contract.yaml`](../config/contracts/gap_detection.contract.yaml)
  - [`config/contracts/reconciliation.contract.yaml`](../config/contracts/reconciliation.contract.yaml)
- **Báo cáo đầu ra**: `reports/reconcile/YYYY-MM-DD.{md,json}`.

---

## 2. OB (Observability Engineer)

- **Sketch Profiling (`observability/collectors/sketch_profiler.py`)**:
  - Metric: `price_sketch_p50_{1m,5m}`, `price_sketch_p95_{1m,5m}`, `size_sketch_p95_{1m,5m}`, `trade_id_hll_distinct_{1m,5m}` — nhãn `source`, `symbol_canonical`.
  - Contract YAML: [`config/contracts/sketch_profiling.contract.yaml`](../config/contracts/sketch_profiling.contract.yaml).
- **SLO Engine (`observability/slo/slo_engine.py`, `slos.yaml`)**:
  - Metric: `slo_target`, `slo_sli_current`, `slo_error_budget_remaining`, `slo_burn_rate` — nhãn `slo`, `pillar`, `source`, `symbol`.
  - Prometheus Alert Rules sinh tự động tại: [`infra/configs/prometheus/rules/slo-burn-rate-alerts.yml`](../infra/configs/prometheus/rules/slo-burn-rate-alerts.yml).
  - Nhãn cảnh báo bắt buộc gửi sang PL & AI: `severity`, `pillar`, `slo`, `source`, `symbol`, `service`.

---

## 3. PL (Platform Engineer)

- **Alertmanager & Telegram**:
  - Service `alertmanager` (`prom/alertmanager:v0.28.1`, cổng `9093`), tự fallback về receiver `"null"` nếu thiếu `TELEGRAM_BOT_TOKEN` hoặc `TELEGRAM_CHAT_ID`.
  - Infra Alert Rules: [`infra/configs/prometheus/rules/infra-alerts.yml`](../infra/configs/prometheus/rules/infra-alerts.yml) (`Watchdog`, `TargetDown`, `HostLowMemory`, `HostDiskAlmostFull`, `KafkaConsumerLagGrowing`, `BronzeWriteLatencyHigh`, `ProducerSilentWindow`, `BackupStale`).
- **Định dạng Payload Cảnh báo chuyển sang AI**:
  ```json
  {
    "alert_id": "A001",
    "alertname": "SLOBurnRate_trade_id_completeness_fast_page",
    "severity": "high",
    "source": "binance",
    "symbol": "BTC-USDT",
    "pillar": "completeness",
    "asset": "finobs:raw.binance.trade",
    "detected_at": "2026-11-26T03:05:00Z",
    "summary": "..."
  }
  ```
- **Sao lưu & Runbook**: [`infra/scripts/backup.sh`](../infra/scripts/backup.sh), [`infra/scripts/restore.sh`](../infra/scripts/restore.sh), [`docs/runbook-infra.md`](runbook-infra.md).

---

## 4. AI (AI/RCA Engineer)

- **Đầu vào**: Danh sách cảnh báo chuẩn theo mục PL/OB ở trên (`observability/rca/incident_grouper.py`).
- **Đầu ra Incident JSON (`priority_ranker.export_ranked_json`)**:
  ```json
  [
    {
      "incident_id": "INC-1A2B3C4D",
      "alert_ids": ["A001", "A002"],
      "started_at": "2026-11-26T03:05:00Z",
      "ended_at": "2026-11-26T03:07:00Z",
      "affected_assets": ["finobs:raw.binance.trade", "finobs:bronze.trades_raw"],
      "sources": ["binance"],
      "symbols": ["ETH-USDT"],
      "severity": "high",
      "status": "open",
      "grouping_reasons": ["lineage_connected:...", "same_source:binance", "within_window:600s"],
      "impacted_nodes": [{"node_id": "...", "kind": "dataset", "name": "gold.ohlcv_1m", "tier": "gold", "distance": 4}],
      "priority_score": 138.0
    }
  ]
  ```
- **Tích hợp EV**: EV đọc `alerts` hoặc `incidents[].started_at` / `affected_assets` để chấm điểm MTTD và độ chính xác gộp.

---

## 5. EV (Evaluation & Documentation)

- **Injector v2 (`evaluation/injector/injector_v2.py`)**: Hỗ trợ 8 loại lỗi (`FT01`, `FT02`, `FT03`, `FT05`, `FT06`, `FT07`, `FT09`, `FT12`), mặc định `dry_run=True`, topic tiêm thử có tiền tố `test.`.
- **MTTD Harness (`evaluation/bench/mttd_harness.py`)**: Xuất kết quả benchmark ra `reports/bench/mttd_summary.{json,csv,md}`.
- **Phân tách kịch bản**: Nhóm chỉnh tham số (*tuning*) tách khỏi nhóm đánh giá (*eval*) theo `evaluation/scenarios/benchmark_split.yaml`.

---

# Hợp đồng Giao tiếp — Sprint 5

## DE (Sprint 5) — Dashboard Stream Completeness & Backfill C2

- **Dashboard Grafana**: `dashboards/finobs-stream-completeness.json` (`uid: finobs-stream-completeness`), tự động provision vào thư mục `FinObs`. Dùng biến `$source`, `$symbol` và recording rule `finobs:*` trên `trade_id_gap_count`, `trade_id_gap_missing_total`, `reconciliation_match_ratio`, `reconciliation_missing_trades`, `ingest_delay_p95_ms`.
- **Backfill C2 (`observability/reconcile/backfill.py`)**: xuất `reports/reconcile/YYYY-MM-DD.{md,json}` và `coverage_summary.{md,json}`.
- **Trạng thái ngày đối soát**: `OK`, `MISSING_IN_STREAM`, `EXTRA_IN_STREAM`, `VALUE_MISMATCH`, `INCOMPLETE_DAY` (ngày thu dở dang), `OFFICIAL_NOT_AVAILABLE` (chưa có file chính thức).
- **Đo M3 (`observability/reconcile/measure_m3.py`)**: trả về `m3_gap_detect_latency_seconds` (< 60 s) và `m3_report_rate` (= 1.0).
