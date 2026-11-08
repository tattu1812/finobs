# Phân loại lỗi dữ liệu FinObs

Đây là bộ nhãn lỗi chung cho cả nhóm: kịch bản đánh giá (`evaluation/scenarios/`), injector,
detector và agent chẩn đoán đều dùng đúng các mã `FTxx_*` dưới đây. Kịch bản nào cũng phải
gắn một mã trong bảng (`scenario.schema.json` kiểm tra điều này).

**Cơ sở.** Khung phân loại bám theo Foidl et al. (2023), *Data Pipeline Quality: Influencing
Factors, Root Causes of Data-related Issues, and Processing Problem Areas for Developers*
(arXiv:2309.07067). Bài này phân tích issue trên GitHub và câu hỏi trên Stack Overflow để tìm
nguyên nhân gốc của lỗi dữ liệu trong pipeline. Hai kết luận ảnh hưởng trực tiếp tới bảng này:

1. Lỗi về **kiểu dữ liệu** là nhóm nguyên nhân gốc gặp nhiều nhất. FinObs vì vậy tách riêng
   lỗi đổi cấu trúc (FT07) và lỗi kiểu/độ chính xác (FT08).
2. Lập trình viên gặp khó nhiều nhất ở các giai đoạn **ingestion, integration và cleaning**.
   Phần lớn mã lỗi dưới đây rơi vào ba giai đoạn này.

Mỗi lỗi được gắn với giai đoạn xử lý nơi lỗi phát sinh, theo cách chia của Foidl.

> ⚠️ Tên nhóm và giai đoạn theo Foidl ở cột "Giai đoạn" được ghi theo cách hiểu của nhóm.
> Cần đối chiếu lại với bản gốc trước khi đưa vào báo cáo IEEE.

Cột "Đã gặp thật?" cho biết lỗi đã xuất hiện trong lần thu Sprint 1 (2026-10-27, 60 phút, xem
[`docs/sprint1/summary.md`](../docs/sprint1/summary.md)) hay chưa. Lỗi đã gặp thật được ưu tiên
viết kịch bản trước.

## Bảng tổng hợp

| Mã | Tên | Trụ cột (metric catalog) | Giai đoạn (Foidl) | Đã gặp thật? |
|---|---|---|---|---|
| `FT01_stale_feed` | Nguồn ngừng cập nhật | freshness | ingestion | Chưa |
| `FT02_volume_loss` | Mất một phần dữ liệu | volume | ingestion | Chưa |
| `FT03_duplicate_events` | Bản ghi trùng lặp | volume | ingestion / cleaning | **Có**: Coinbase, 02:57:01 |
| `FT04_history_replay` | Sàn phát lại lịch sử giữa phiên | freshness | ingestion | **Có**: Coinbase, trade cũ tới 19,7 giờ |
| `FT05_late_out_of_order` | Dữ liệu đến muộn quá watermark, sai thứ tự | freshness | ingestion / transformation | Một phần |
| `FT06_clock_skew` | Lệch đồng hồ máy thu | freshness | infrastructure | **Có**: +1,8 s, 32 461 độ trễ âm |
| `FT07_schema_drift` | Thay đổi cấu trúc bản ghi | schema | ingestion / integration | Chưa |
| `FT08_type_precision` | Sai kiểu, mất độ chính xác số | schema / distribution | cleaning / transformation | Chưa (đã phòng bằng contract) |
| `FT09_value_anomaly` | Giá trị bất thường, sai đơn vị | distribution | cleaning | Chưa |
| `FT10_cross_source_mismatch` | Lệch giữa các nguồn, ánh xạ mã sai | cross_source | integration | Chưa |
| `FT11_pipeline_failure` | Job/DAG lỗi, lan xuống hạ nguồn | pipeline_health | infrastructure / processing | Chưa (chờ Airflow) |

Có 11 loại. Ba loại FT01, FT02, FT11 là lỗi "kinh điển" của pipeline. Các loại còn lại đặc thù
cho dữ liệu tài chính nhiều nguồn, và phần lớn đã thấy ngay trong một giờ thu đầu tiên.

## Chi tiết từng loại

### FT01_stale_feed: Nguồn ngừng cập nhật
- **Hiện tượng:** một nguồn hoặc một mã không còn trade mới. Kết nối có thể vẫn "sống"
  (WebSocket không đóng) nên recorder không báo lỗi.
- **Nguyên nhân gốc điển hình:** sàn bảo trì, subscription bị huỷ âm thầm, bị rate-limit
  (VCI giới hạn khoảng 180 request/phút), tiến trình recorder bị treo.
- **Tín hiệu:** `last_live_trade_age_seconds` tăng, còn `trade_count_1m` giảm về 0.
- **Cách tiêm:** cắt toàn bộ bản ghi của một nguồn hoặc mã trong một khoảng thời gian.
- **Không phải lỗi:** HOSE đóng cửa (ngoài 9:00–11:30, 13:00–14:45 giờ VN), hoặc mã thanh khoản
  thấp không có giao dịch. Phải có kịch bản đối chứng cho các trường hợp này.

### FT02_volume_loss: Mất một phần dữ liệu
- **Hiện tượng:** vẫn có dữ liệu nhưng ít hơn thực tế. Ví dụ: mất một số mã, rơi một tỷ lệ
  message, một partition giờ không được ghi.
- **Nguyên nhân gốc:** lỗi subscribe, buffer đầy, consumer Kafka lag rồi bị retention xoá,
  ghi file thất bại.
- **Tín hiệu:** `trade_count_1m` giảm so với baseline cùng khung giờ. Đối soát với nguồn khác
  hoặc với sequence id cho thấy có khoảng trống.
- **Cách tiêm:** xoá ngẫu nhiên p% bản ghi, hoặc xoá toàn bộ một mã.

### FT03_duplicate_events: Bản ghi trùng lặp
- **Hiện tượng:** cùng một `event_id` xuất hiện nhiều lần.
- **Đã gặp:** Coinbase gửi trùng 7 trade BTC lúc 02:57:01, khi tải dồn và message bị giới hạn
  100 trade.
- **Nguyên nhân gốc:** sàn gửi lại khi dồn tải, reconnect rồi subscribe lại, producer retry mà
  không idempotent.
- **Tín hiệu:** số bản ghi lớn hơn số `event_id` khác nhau. `trade_count_1m` (đã khử trùng) không đổi,
  nhưng tổng khối lượng tầng sau bị thổi phồng nếu quên khử trùng.
- **Cách tiêm:** nhân bản k% bản ghi trong một khoảng thời gian.

### FT04_history_replay: Sàn phát lại lịch sử giữa phiên
- **Hiện tượng:** giữa phiên, không có reconnect, sàn đột ngột gửi một loạt trade cũ.
- **Đã gặp:** Coinbase gửi lại trade cũ tới 19,7 giờ dưới dạng message `update`, không có cờ snapshot.
- **Khác FT05 ở chỗ:** dữ liệu không chậm do mạng mà là lịch sử bị phát lại có chủ đích.
  Trade đó thường đã có từ trước, nên lỗi này hay đi kèm FT03.
- **Tín hiệu:** phân phối `ingest_time - event_time` có đuôi dài hàng giờ, và có nhiều `event_id` đã thấy.
- **Cách tiêm:** chèn lại một khối bản ghi cũ với `ingest_time` hiện tại.

### FT05_late_out_of_order: Đến muộn quá watermark, sai thứ tự
- **Hiện tượng:** bản ghi tới sau khi cửa sổ event-time đã đóng, hoặc thứ tự `event_time`
  khác thứ tự nhận.
- **Nguyên nhân gốc:** độ trễ mạng, backpressure, watermark đặt quá sát. Gợi ý watermark từ
  Sprint 1: Binance 0–200 ms, Coinbase 500 ms, vnstock khoảng 10 s.
- **Tín hiệu:** số bản ghi bị loại vì muộn tăng, và `ingest_delay_p95_ms` tăng.
- **Cách tiêm:** cộng thêm độ trễ ngẫu nhiên vào thời điểm phát lại của một phần bản ghi.

### FT06_clock_skew: Lệch đồng hồ máy thu
- **Hiện tượng:** `ingest_time` sai lệch so với giờ thật, khiến độ trễ đo được sai. Lệch âm
  thì xuất hiện độ trễ âm.
- **Đã gặp:** máy thu lệch +1,8 s lúc đầu. Sau khi chỉnh vẫn trôi 26 ms/giờ, tới +626 ms sau
  10 giờ. Kết quả là 32 461 trade Binance có độ trễ âm.
- **Nguyên nhân gốc:** WSL/Windows không đồng bộ NTP, máy ngủ rồi thức dậy.
- **Tín hiệu:** độ trễ âm (với Binance là bất khả về vật lý), độ trễ của *mọi* mã cùng dịch
  một lượng như nhau, và `clock_check` báo vượt 250 ms.
- **Cách tiêm:** cộng một offset cố định hoặc trôi dần vào `ingest_time`.

### FT07_schema_drift: Thay đổi cấu trúc bản ghi
- **Hiện tượng:** trường bị đổi tên, thêm, bỏ, hoặc đổi kiểu JSON. Ví dụ: `trade_id` của
  Coinbase đổi từ chuỗi sang số.
- **Nguyên nhân gốc:** sàn đổi API (thường có hoặc không có thông báo), recorder bị nâng cấp
  mà không tăng `SCHEMA_VERSION`.
- **Tín hiệu:** `contract_violation_rate_1m` tăng đột ngột, chỉ ở một nguồn.
- **Cách tiêm:** đổi tên, xoá hoặc đổi kiểu một trường payload từ một thời điểm trở đi.

### FT08_type_precision: Sai kiểu, mất độ chính xác số
- **Hiện tượng:** giá hoặc khối lượng bị parse thành float rồi làm tròn, hoặc số chữ số thập
  phân đổi (Binance luôn là 8 chữ số).
- **Nguyên nhân gốc:** tầng sau đọc chuỗi thập phân thành `double`, hoặc cast sai trong Spark/dbt.
  Theo Foidl đây là nhóm nguyên nhân phổ biến nhất.
- **Tín hiệu:** vi phạm contract (sai số chữ số thập phân). Đối soát tổng tiền giữa tầng raw
  và tầng sau lệch nhỏ nhưng khác 0.
- **Cách tiêm:** làm tròn giá về float32/float64, hoặc đổi số chữ số thập phân.

### FT09_value_anomaly: Giá trị bất thường, sai đơn vị
- **Hiện tượng:** giá bằng 0, giá nhảy vọt, khối lượng âm hoặc khổng lồ, giá VN30 sai đơn vị
  (nghìn đồng so với đồng, lệch 1 000 lần).
- **Nguyên nhân gốc:** lỗi phía nguồn, đổi đơn vị không báo trước, nhập nhầm ("fat finger").
- **Tín hiệu:** `price_p50_5m` hoặc `trade_size_p95_5m` lệch xa baseline. Chênh với nguồn khác (FT10).
- **Không phải lỗi:** biến động thị trường thật. Ví dụ: đỉnh tải 02:57:01 xảy ra ở cả hai sàn
  cùng lúc là sự kiện thị trường. Khi một sự kiện thấy ở nhiều nguồn cùng lúc, đó là dấu hiệu
  của thị trường chứ không phải lỗi.
- **Cách tiêm:** nhân giá một mã với hệ số, hoặc chèn vài trade giá ngoại lai.

### FT10_cross_source_mismatch: Lệch giữa các nguồn, ánh xạ mã sai
- **Hiện tượng:** cùng một tài sản nhưng hai nguồn cho giá hoặc biến động khác nhau quá ngưỡng,
  hoặc so nhầm hai cặp khác nhau (`BTC-USDT` và `BTC-USD` là hai cặp riêng).
- **Nguyên nhân gốc:** bảng ánh xạ mã sai, một nguồn bị lỗi FT01/FT09 mà không bị phát hiện riêng.
- **Tín hiệu:** `cross_source_return_gap_5m` vượt ngưỡng.
- **Cách tiêm:** đổi ánh xạ mã, hoặc dịch giá của một nguồn.
- Liên quan bài toán truth finding của Li et al. (2012).

### FT11_pipeline_failure: Job/DAG lỗi, lan xuống hạ nguồn
- **Hiện tượng:** một task Spark/Airflow/dbt thất bại, chạy lại hoặc chạy chậm. Bảng hạ nguồn
  cũ đi hoặc thiếu partition. Đây là loại lỗi cần **lineage** (Marquez) để lần ngược ra gốc.
- **Nguyên nhân gốc:** hết tài nguyên, lỗi code, phụ thuộc upstream không sẵn sàng.
- **Tín hiệu:** `recorder_error_count_5m`, trạng thái run trong Marquez, cảnh báo Spark từ
  Prometheus, và freshness của bảng hạ nguồn.
- **Cách tiêm:** làm job thất bại (kill container, cấu hình sai) ở một node của đồ thị lineage.

## Quy tắc khi thêm loại mới

1. Chỉ thêm loại mới khi không gộp được vào loại có sẵn. Mã lấy số kế tiếp, không dùng lại số cũ.
2. Cập nhật đồng thời bảng ở trên, mục chi tiết và `enum` trong
   [`scenarios/scenario.schema.json`](scenarios/scenario.schema.json).
   Lệnh `make validate-scenarios` sẽ báo lỗi nếu hai nơi này không khớp.
