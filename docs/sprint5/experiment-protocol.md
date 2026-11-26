# Quy trình Thí nghiệm & Định nghĩa Chỉ số (EV, Sprint 5)

## 1. Phân tách Tập Tuning và Tập Eval
- Danh sách phân tách lưu tại [`evaluation/bench/benchmark_split.yaml`](../../evaluation/bench/benchmark_split.yaml).
- **Tập `tuning`**: chỉ dùng để chọn tham số detector (ví dụ `window`, `threshold`, `margin_factor`).
- **Tập `eval`**: chỉ dùng để báo cáo kết quả cuối kỳ, tuyệt đối không tinh chỉnh tham số theo tập này.

## 2. Định nghĩa Chỉ số
- **MTTD**: `first_valid_alert_ts - ground_truth.start_ts` (giây UTC). Cảnh báo đến trước `start_ts` tính là `early_alert` / `false_positive`.
- **MTTDiag**: thời gian từ lúc agent nhận incident đến khi trả về `hypotheses` đã được `verify_evidence()` xác nhận.
- **AC@k**: tỷ lệ kịch bản sự cố mà `ground_truth.root_cause.asset` nằm trong top-$k$ `suspected_asset` do agent đề xuất.
- **False Positive Rate**: số cảnh báo không khớp bất kỳ `ground_truth` nào (đặc biệt trên kịch bản đối chứng `is_incident: false` và ngày biến động `evaluation/volatile_days.yaml`).
