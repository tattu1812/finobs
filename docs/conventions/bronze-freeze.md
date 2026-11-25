# Khóa bronze trong thời gian thu 14 ngày

**Hiệu lực:** từ lúc bắt đầu thu liên tục (sau khi PR Sprint 3 được merge) tới hết đợt thu dùng cho thí nghiệm.

## Vì sao

Kế hoạch Sprint 3 dựa trên một giả định: *silver và gold làm xong lúc nào cũng tính lại được từ bronze*
(`make reprocess`). Giả định này chỉ đúng khi **nội dung bronze không đổi nghĩa** trong suốt đợt thu:
cùng nguồn, cùng mã, cùng cách đóng vỏ bản ghi, cùng schema bảng. Stream không thu bù được, nên một thay
đổi vô ý ở producer giữa đợt có thể làm hỏng cả 14 ngày dữ liệu.

## File bị khóa

| File | Quyết định gì |
|---|---|
| `platform/ingest/produce.py`, `sinks.py`, `common.py` | Đóng vỏ bản ghi, topic, kiểm contract |
| `platform/ingest/binance_ws.py`, `coinbase_ws.py`, `kraken_ws.py`, `vnstock_poll.py` | Đọc từng sàn |
| `platform/contracts/` | Contract (`SCHEMA_VERSION` 1.1.0) |
| `platform/streaming/bronze_trades.py` | Schema và cách ghi `bronze.trades_raw` |
| `config/symbols.json` | Danh sách mã thu |

Không khóa: silver/gold (`silver_gold.py`), collector, agent, evaluation, tài liệu. Đó chính là những thứ
có thể sửa rồi tính lại từ bronze.

## Muốn sửa file bị khóa

1. Mở PR riêng, chỉ chứa thay đổi đó, mô tả rõ ảnh hưởng tới dữ liệu đang thu.
2. Có **review riêng** của DE (Phạm Thành Trung) và nhóm trưởng; trả lời được: bản ghi trước và sau thay đổi
   còn cùng nghĩa không? silver có cần xử lý hai phiên bản không?
3. Sau khi duyệt, gắn nhãn **`bronze-change-approved`** cho PR. CI (bước *Bronze freeze*) chặn mọi PR đụng
   file bị khóa mà không có nhãn này.
4. Ghi thời điểm triển khai thay đổi vào `docs/sprint3/summary.md` (hoặc báo cáo sprint đang chạy), để khi
   phân tích biết dữ liệu trước/sau mốc đó.

Sửa lỗi khẩn cấp (producer chết, sàn đổi API) vẫn theo quy trình này, chỉ rút ngắn thời gian review.
