# Baseline Detectors — Sprint 5 (OB)

## 1. Kiến trúc & Giao diện chung

- Interface: [`observability/detectors/base.py`](../../observability/detectors/base.py)
  - `BaseDetector.detect(observations) -> list[DetectorAlert]`
  - `DetectorAlert`: `{detector, source, symbol, ts, detected_at, asset, score, severity, evidence, metric, labels}` — tương thích trực tiếp với `evaluation/bench/mttd_harness.py` và pipeline `incident_grouper.py`.

## 2. Hai thuật toán Baseline

### 2.1 `ZScoreSingleSourceDetector` (`zscore_single.py`)
- Tính rolling z-score độc lập cho từng bộ `(source, symbol, metric)`.
- Mặc định bật `robust_mad: true` (dùng median và $1.4826 \times \text{MAD}$) để hạn chế nhiễu do ngày thị trường biến động mạnh hợp lệ.
- Xử lý edge cases:
  - **Warm-up**: bỏ qua phát hiện khi số mẫu hợp lệ `< min_samples`.
  - **Missing data / NaN / Inf**: bỏ qua bản ghi `None`/`NaN` mà không làm hỏng cửa sổ.
  - **Chuỗi hằng số (`std == 0`)**: nếu giá trị mới bằng hằng số lịch sử → `score = 0` (không báo giả); nếu lệch hẳn → `score = 999.0` (cảnh báo).
  - **Chống ô nhiễm cửa sổ**: điểm bị gắn cờ bất thường không được đẩy vào buffer lịch sử.

### 2.2 `HistoricalBoundsDetector` (`historical_bounds.py`)
- Dùng `KLLSketch` (từ `observability/collectors/sketch_profiler.py`) để ước lượng phân vị `[q_low, q_high]` trên tập huấn luyện (`fit()`).
- Nới biên bằng `margin_factor * max(q_high - q_low, 0.01 * |q_high|)` nhằm giảm cảnh báo giả trên ngày biến động mạnh.
- Tách biệt hoàn toàn `fit(train)` và `detect(eval)` để tránh rò rỉ dữ liệu tương lai.

## 3. Cách chạy cục bộ

```bash
python observability/detectors/run_baseline.py \
  --input tests/fixtures/reconcile/stream_btcusdt.json \
  --detector both
```

## 4. Hướng dẫn triển khai lên VM (an toàn cho việc thu 14 ngày)

1. Các detector chỉ đọc dữ liệu/metric (read-only), **không** can thiệp `producer`, `bronze-stream`, hay Kafka topics.
2. Trên VM thu dữ liệu: chỉ `git pull` sau khi PR được duyệt vào `main`. Không chạy tải nặng hay thử nghiệm scalability trên VM đang thu.
3. Để quay lui: `git checkout <commit_trước>` trên nhánh triển khai; không có volume hay container nào bị thay đổi schema.
