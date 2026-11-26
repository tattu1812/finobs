# Sprint 4 — Bàn giao DE cho OB, AI và EV

Tài liệu này cung cấp schema đầu ra, danh sách metric và hướng dẫn tái lập lỗi giả từ phần DE (Sprint 4) để các nhóm **Observability (OB)**, **AI / RCA Agent (AI)** và **Evaluation / Fault Injector v2 (EV)** tích hợp.

---

## 1. Metric mới trong Metric Store (`metric_observations`)

Tất cả metric được ghi qua `upsert_metric_observation(...)` vào DB `finobs_metrics` (PostgreSQL). Nhóm OB/AI có thể đọc trực tiếp qua `query_metric` hoặc SQL.

### 1.1 Gap Detection (DE-A)

| `metric_name` | `dimensions` | Kiểu giá trị | Ghi chú cho SLO / Incident |
|---|---|---|---|
| `trade_id_gap_count` | `{"source": "binance\|coinbase", "symbol": "..."}` | Integer (`>= 0`) | `> 0` là dấu hiệu mất gói / reconnect hụt |
| `trade_id_gap_missing_total` | `{"source": "...", "symbol": "..."}` | Integer (`>= 0`) | Tổng số trade bị mất trong cửa sổ |
| `trade_id_duplicate_count` | `{"source": "...", "symbol": "..."}` | Integer (`>= 0`) | `> 0` khi producer/Kafka phát lại |
| `trade_id_out_of_order_count` | `{"source": "...", "symbol": "..."}` | Integer (`>= 0`) | Đảo thứ tự nhận theo `ingest_ts` |

### 1.2 Reconciliation D+1 (DE-B — Binance)

| `metric_name` | `dimensions` | Kiểu giá trị | Ghi chú cho SLO / Incident |
|---|---|---|---|
| `reconciliation_match_ratio` | `{"source": "binance", "symbol": "...", "date": "YYYY-MM-DD"}` | Float (`0.0` – `1.0`) | SLO độ đầy đủ dữ liệu D+1 (vd `>= 0.999`). `status = 'no_data'` nếu `OFFICIAL_NOT_AVAILABLE`. |
| `reconciliation_missing_trades` | `{"source": "binance", "symbol": "...", "date": "YYYY-MM-DD"}` | Integer (`>= 0`) | Dùng để gộp incident mất dữ liệu |
| `reconciliation_extra_trades` | `{"source": "binance", "symbol": "...", "date": "YYYY-MM-DD"}` | Integer (`>= 0`) | Lệch ranh giới ngày hoặc dữ liệu thừa |
| `reconciliation_value_mismatch` | `{"source": "binance", "symbol": "...", "date": "YYYY-MM-DD"}` | Integer (`>= 0`) | Sai giá, khối lượng hoặc thời gian |

---

## 2. Data Contract đầu ra (YAML)

- Gap Detection: [`config/contracts/gap_detection.contract.yaml`](../config/contracts/gap_detection.contract.yaml)
- Reconciliation: [`config/contracts/reconciliation.contract.yaml`](../config/contracts/reconciliation.contract.yaml)

Báo cáo D+1 được xuất tại `reports/reconcile/YYYY-MM-DD.json` và `.md`. Các trường chính trong JSON:
- `report_date`, `run_at`, `code_version`, `is_partial_day`, `window_from`, `window_to`
- `symbols[]`: `symbol`, `status`, `official_count`, `stream_count`, `matched`, `match_ratio`, `missing_in_stream_count`, `missing_in_stream` (rút gọn tối đa 100 phần tử), `extra_in_stream_count`, `value_mismatch_count`, `detection_coverage`, `gap_report`.

---

## 3. Hướng dẫn cho EV (Injector v2) — Phát lỗi giả để đo MTTD

Để kiểm thử detector và đo MTTD (Mean Time To Detect), nhóm EV có thể dùng các hàm thuần tuý hoặc tạo fixture giả:

### 3.1 Kiểm thử trực tiếp bằng Python API

```python
from decimal import Decimal
from observability.reconcile.gap_detection import analyze_trade_id_sequence
from observability.reconcile.reconcile import reconcile_symbol

# 1. Giả lập khoảng hở ID (mất trade 102, 103) + trùng (101) + đảo thứ tự (105 trước 104)
gap_report = analyze_trade_id_sequence([100, 101, 101, 105, 104], source="binance", symbol="BTCUSDT")
assert gap_report.gap_count == 1          # khoảng hở 102..103
assert gap_report.gap_missing_total == 2
assert gap_report.duplicate_count == 1
assert gap_report.out_of_order_count == 1

# 2. Giả lập đối soát thiếu trade & sai giá
stream = [
    {"trade_id": 100, "price": Decimal("90000"), "qty": Decimal("0.1"), "event_ts_ms": 1795737600000},
    {"trade_id": 102, "price": Decimal("89999"), "qty": Decimal("0.1"), "event_ts_ms": 1795737602000},
]
official = [
    {"trade_id": 100, "price": Decimal("90000"), "qty": Decimal("0.1"), "trade_ts_ms": 1795737600000},
    {"trade_id": 101, "price": Decimal("90001"), "qty": Decimal("0.1"), "trade_ts_ms": 1795737601000},
    {"trade_id": 102, "price": Decimal("90002"), "qty": Decimal("0.1"), "trade_ts_ms": 1795737602000},
]
res = reconcile_symbol(stream, official, "BTCUSDT")
# res.status == "MISSING_IN_STREAM", res.detection_coverage == 1.0, len(res.value_mismatches) == 1
```

### 3.2 Chèn lỗi vào file raw `.jsonl` / `.jsonl.gz`

- **Tạo khoảng hở**: Xoá 1 vài dòng liên tiếp (hoặc sửa `sequence` nhảy cóc) trong file raw rồi chạy `python observability/reconcile/run_gap_detection.py --in <dir> --dry-run`.
- **Tạo trùng lặp**: Nhân đôi 1 dòng JSONL giữ nguyên `event_id` và `sequence`.
- **Tạo đảo thứ tự**: Đổi chỗ 2 bản ghi có `sequence` ngược nhau nhưng `ingest_time` tăng dần.
