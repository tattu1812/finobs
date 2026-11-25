# Tóm tắt Sprint 1: Data Engineer

**Người thực hiện:** Trung · **Dữ liệu:** 60 phút thu thật, 2026-10-27 02:48–03:48 UTC

Đây là bản tóm tắt một trang cho cả nhóm. Chi tiết xem [báo cáo đầy đủ](report.md),
[ghi chú Dataflow Model](../papers/dataflow-model.md), [slide](slides.md) và
[README contract](../../platform/contracts/README.md).

## Đã làm được

| Hạng mục | Kết quả |
|---|---|
| Thu dữ liệu | 231 859 trade từ Binance và Coinbase (10 mã mỗi sàn), 0 reconnect, 0 mất gói |
| Kiểm tra đồng hồ | `clock_check.py` đo trước và sau khi thu; lệch quá 250 ms thì không cho thu |
| Recorder VN30 | `vnstock_poll.py` poll endpoint VCI, tự thoát ngoài giờ HOSE, tối đa 180 request/phút |
| Đo đạc | `reports/throughput.md` (theo nguồn, theo mã) cùng 2 biểu đồ |
| Data contract | 3 JSON Schema; 231 859/231 859 bản ghi hợp lệ; 22/22 ca lỗi cố ý bị bắt |
| CI | Đã thêm bước `make validate-contracts`, chạy trên fixtures |

## Số liệu chính

| | Binance | Coinbase | vnstock* |
|---|---:|---:|---:|
| Trade/s trung bình / max | 56,6 / 2 442 | 7,4 / 309 | — |
| Độ trễ p50 / p99 | 7 / 42 ms | 160 / 272 ms | 3,9 s (p50) |
| Băng thông | 32,6 MB/giờ | 5,5 MB/giờ | — |

\* vnstock: số liệu từ một lần chạy thử 2 phút, chỉ mang tính sơ bộ.

## Bốn phát hiện quan trọng

1. **Đồng hồ máy thu lệch rồi trôi tiếp.** Lúc đầu lệch +1,8 s. Sau khi chỉnh, đồng hồ vẫn trôi 26 ms trong giờ thu,
   và tới +626 ms sau 10 giờ. Hệ quả là 32 461 trade Binance có độ trễ âm.
2. **Coinbase gửi lại lịch sử giữa phiên** dù không có reconnect, dưới dạng message `update` không có cờ snapshot.
   Trong đó có trade cũ tới 19,7 giờ.
3. **Coinbase giới hạn 100 trade mỗi message và gửi trùng khi dồn tải** (7 trade BTC lặp lại lúc 02:57:01).
4. **Đỉnh tải xảy ra cùng một giây ở cả hai sàn** (02:57:01, gấp 43 lần mức trung bình của Binance).

## Các thành viên khác cần biết

- **Đọc dữ liệu raw:** luôn **khử trùng theo `event_id`**. Bỏ các bản ghi có `transport.is_snapshot = true`
  khi tính chỉ số realtime.
- **Giá và khối lượng là string.** Khi parse phải dùng `Decimal` / `DECIMAL(38,18)`, không dùng `float` / `double`.
- **Thời gian** luôn là UTC, ISO 8601, mili giây, hậu tố `Z`. Mã chuẩn hoá có dạng `BASE-QUOTE`;
  lưu ý `BTC-USDT` và `BTC-USD` là **hai cặp khác nhau**.
- **Watermark gợi ý cho từng nguồn:** Binance 0–200 ms, Coinbase 500 ms, vnstock khoảng 10 s (cần đo lại).
  Không gộp các nguồn dưới một watermark chung.
- **Sửa schema** thì phải tăng `SCHEMA_VERSION` trong cùng PR (quy tắc SemVer có trong README contract).

## Lệnh thường dùng

```bash
make record MINUTES=60           # chạy clock-check trước, rồi thu Binance + Coinbase
make record SOURCES=vnstock      # chỉ chạy được trong giờ HOSE
make analyze                     # bảng đo và biểu đồ vào reports/
make validate-contracts          # validate data/raw (nếu chưa có thì dùng fixtures) và các ca lỗi cố ý
```

Không dùng `python -m platform...`, vì thư mục `platform/` trùng tên với module chuẩn của Python.
Trước khi thu, phải đồng bộ giờ Windows bằng quyền Admin (xem báo cáo, mục 3).

## Việc tiếp theo

1. Đồng bộ giờ bền vững, và ghi offset đồng hồ vào log trong lúc thu.
2. Thu vnstock trọn một phiên HOSE, rồi viết `vnstock_trade.schema.json`.
3. Đưa dữ liệu vào Kafka: khử trùng ở tầng đầu, mỗi nguồn một watermark riêng.
4. Biến các phát hiện 1–3 thành detector observability.
