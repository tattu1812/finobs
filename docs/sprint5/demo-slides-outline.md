# Đề cương Slide Demo Cuối Sprint 5 — FinObs

1. **Bối cảnh & Mục tiêu Sprint 5**: "C2 xong, có baseline" + đảm bảo an toàn luồng thu dữ liệu thật 14 ngày trên VM.
2. **PL — Công cụ Phát lại Tốc độ Cao (FR-23)**: Replay 1x/10x/100x, nhân bản mã tổng hợp (10 → 100 → 500 mã), cách ly namespace `replay.*`, pause/resume theo seed.
3. **DE — Hoàn thiện C2 & Dashboard**: Backfill D+1 idempotent, xử lý ngày dở dang (`INCOMPLETE_DAY`), đo M3 (< 60s, 100% ngày có báo cáo), dashboard Grafana `finobs-stream-completeness`.
4. **OB — Giao diện Detector & 2 Baseline**: `BaseDetector`, `ZScoreSingleSourceDetector` (robust MAD), `HistoricalBoundsDetector` (KLLSketch, tách `fit`/`detect`).
5. **AI — Agent Chẩn đoán RCA v1**: ReAct loop qua 5 tool chỉ-đọc, chặn SQL injection/path traversal, Evidence Verifier chống bịa bằng chứng.
6. **EV — Kết quả Baseline Sơ Bộ & Mở rộng FinObs-Bench**: 22 kịch bản (8 loại lỗi), tách `tuning`/`eval`, đề xuất ngưỡng M2–M6 sơ bộ.
7. **Kế hoạch Sprint 6**: Đánh giá trên dữ liệu thật 14 ngày từ VM, chốt ngưỡng M2–M6 chính thức, tích hợp multi-source & LLM thật.
