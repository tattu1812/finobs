# Đề xuất Mục tiêu Định lượng M2–M6 (SƠ BỘ — Sprint 5)

> **Cảnh báo**: Các số liệu baseline trong tài liệu này được đo **sơ bộ trên dữ liệu mô phỏng/fixture** tại thời điểm Sprint 5 (chưa có đủ 14 ngày dữ liệu liên tục trên VM). Nhóm sẽ chốt mục tiêu chính thức sau khi đủ dữ liệu thật.

| Chỉ số | Định nghĩa ngắn gọn | Baseline sơ bộ (fixture) | Mục tiêu đề xuất | Ghi chú |
|---|---|---|---|---|
| **M2** (Độ chính xác phát hiện & FP) | Precision / False Positive Rate trên ngày biến động | FP = 0 (mô phỏng đơn giản) | Recall >= 90%, FP <= 1 / ngày biến động | Cần kiểm chứng lại trên 14 ngày thật |
| **M3** (Phát hiện gap & D+1) | Độ trễ phát hiện trade_id gap & tỷ lệ báo cáo D+1 | Latency < 1s (fixture), Report rate = 100% | Gap latency < 60s, Report rate = 100% | Đo bằng `measure_m3.py` |
| **M4** (MTTD) | Mean Time To Detect từ `ground_truth.start_ts` | ~0–30s (tập eval mô phỏng) | MTTD <= 120s (cảnh báo nhanh <= 60s) | Đo bằng `mttd_harness.py` |
| **M5** (RCA AC@k & MTTDiag) | Tỷ lệ đúng root cause trong top-k & thời gian chẩn đoán | AC@1 = 100% trên `ScriptedFakeLLM` (S002) | AC@3 >= 80%, MTTDiag <= 300s | Cần đo trên LLM thật ở Sprint 6 |
| **M6** (Evidence Grounding) | Tỷ lệ kết luận agent có `call_id` + `excerpt` hợp lệ | 100% (ép bởi Evidence Verifier) | 100% (không cho phép kết luận bịa) | Kiểm tra bởi `verify_evidence()` |
