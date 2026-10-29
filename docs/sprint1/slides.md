---
marp: true
paginate: true
size: 16:9
title: FinObs Sprint 1, Data Engineer
style: |
  section { font-size: 26px; }
  h1 { font-size: 44px; }
  table { font-size: 21px; }
  .small { font-size: 18px; color: #52514e; }
---

# FinObs Sprint 1
## Thu dữ liệu thật và đo đặc tính nguồn

Trung, Data Engineer
Phiên thu: 2026-10-27, 02:48 → 03:48 UTC, Binance + Coinbase, 10 mã mỗi sàn

<!--
~30 giây. Mục tiêu sprint: có dữ liệu thật, biết nó trông thế nào, viết contract dựa trên nó.
Mọi con số trong slide đều lấy từ dữ liệu thu thật, không có số ước lượng.
-->

---

# Ba nguồn, ba kiểu giao dữ liệu

| | Binance | Coinbase | vnstock (VCI) |
|---|---|---|---|
| Giao thức | WebSocket | WebSocket | REST poll 5 s |
| Một message | 1 trade | lô 250 ms, ≤ 100 trade | 100 trade mới nhất của 1 mã |
| Recorder | `binance_ws.py` | `coinbase_ws.py` | `vnstock_poll.py` *(mới)* |

- Lưu dạng NDJSON gzip, xoay theo giờ, **payload giữ nguyên văn**
- Mọi nguồn dùng chung một vỏ (envelope): `event_time` (giờ sàn), `ingest_time` (giờ máy thu), `transport` (thông tin vận chuyển)

<!--
~1 phút. Khác biệt giữa "message" và "trade" là lý do bảng đo phải tách hai cột này.
vnstock: gói vnstock và vnai đã bị gỡ khỏi PyPI, và intraday() chuyển giá sang float rồi chia 1000,
nên recorder gọi thẳng endpoint VCI mà vnstock vẫn dùng bên trong.
-->

---

# Trước khi thu: kiểm tra đồng hồ

Độ trễ = `ingest_time` (máy mình) − `event_time` (sàn), nên **đồng hồ máy thu sai thì độ trễ sai theo**

| UTC | Offset so với NTP | |
|---|---:|---|
| 02:29 | **+1779 ms** | ✗ dừng, không thu |
| 02:48 (bắt đầu thu) | +15,9 ms | ✓ |
| 03:48 (kết thúc thu) | +42,0 ms | ✓ |
| 14:23 (10,6 giờ sau) | **+626 ms** | ✗ |

<span class="small">5 tham chiếu độc lập (3 NTP, Binance, Coinbase) cho kết quả lệch nhau không quá vài ms. Lỗi nằm ở đồng hồ Windows, WSL kế thừa.</span>

<!--
~1 phút. Nếu không kiểm tra thì toàn bộ cột độ trễ đã lệch 1,8 giây. `make record` giờ luôn chạy
clock-check trước và tự dừng nếu lệch quá 250 ms. Lúc 14:23 nó đã chặn đúng một lần thu thử.
-->

---

# Một giờ dữ liệu

| | Binance | Coinbase |
|---|---:|---:|
| Bản ghi | 203 732 | 28 127 |
| Trade live/s: trung bình / max | 56,6 / 2 442 | 7,4 / 309 |
| Message/s: trung bình | 56,6 | 4,2 |
| Độ trễ p50 / p99 | 7 / 42 ms | 160 / 272 ms |
| Đứt seq / sai thứ tự / reconnect | 0 / 0 / 0 | 0 / 0 / 0 |
| Băng thông | 32,6 MB/giờ | 5,5 MB/giờ |

vnstock (chạy thử 2 phút): độ trễ p50 **3,9 s**, 119 request/phút, 0 trùng

<!--
~1 phút. Ba nguồn chênh nhau ba bậc độ lớn về độ trễ: khoảng 10 ms, 160 ms, khoảng 4 s.
Coinbase: trade/s gần gấp đôi message/s vì gom lô.
-->

---

# Độ trễ: hình dạng phân phối cho biết nguyên nhân

![w:1050](../../reports/latency_hist.png)

<!--
~1 phút. Binance: đỉnh nhọn quanh vài ms, chỉ có thời gian mạng.
Coinbase: gần như phẳng từ khoảng 100 đến 210 ms, đúng hình dạng của khoảng chờ phân bố đều 0–250 ms do gom lô,
cộng thêm thời gian mạng. Phần độ trễ âm bên trái của Binance là phát hiện ở slide sau.
-->

---

# Phát hiện 1: độ trễ âm do đồng hồ trôi

- **32 461 trade Binance (15,9%)** có `ingest_time < event_time`
- p50 theo khung 10 phút: Binance **22 → −2 ms**, Coinbase **176 → 147 ms**
- Cả hai sàn giảm cùng một mức, và mức đó khớp với 26 ms đồng hồ trôi trong giờ thu
  → nguyên nhân là **đồng hồ máy thu**, không phải mạng
- Dữ liệu được **giữ nguyên, không hiệu chỉnh**. Độ trễ âm là tín hiệu cảnh báo, không phải thứ cần làm sạch

<!--
~1 phút. Đây là phát hiện có thể kiểm chứng bằng hai nguồn độc lập. Bài học cho observability:
đồng hồ máy thu là một phụ thuộc phải được giám sát.
-->

---

# Phát hiện 2: Coinbase gửi lại lịch sử mà không báo

- Lúc 03:20, 03:21, 03:33: gửi lại lịch sử của ALGO, BAND, CTSI **dù không có reconnect**
- Mỗi lần là một `snapshot` kèm theo một message **`update` chứa 100 trade cũ, có trade từ tận 19,7 giờ trước**
- Phần lịch sử không có cờ snapshot, nên recorder không thể nhận ra

| Loại bản ghi Coinbase | Số lượng |
|---|---:|
| live | 26 520 |
| snapshot (có cờ) | 1 300 |
| trùng `event_id` | 219 |
| phát lại (không có cờ) | 88 |

<!--
~1 phút. Nếu không tách các loại này thì độ trễ p50 của BAND-USD sẽ ra khoảng 19 giờ.
Ngoài ra trong đợt dồn tải 02:57:01, message chạm giới hạn 100 trade và message kế tiếp lặp lại 7 trade.
Kết luận: phải khử trùng theo event_id ngay ở tầng đầu.
-->

---

# Phát hiện 3: đỉnh tải đồng thời ở hai sàn

![w:1050](../../reports/trades_per_second.png)

<span class="small">02:57:01 UTC: Binance 2 442 trade/s (gấp 43 lần trung bình), Coinbase 309 trade/s. Event time của các trade nằm gọn trong giây đó, nên đây là tải thật của thị trường.</span>

<!--
~45 giây. Hệ quả: ngưỡng cảnh báo throughput không thể là hằng số. Cửa sổ sliding sẽ làm mượt các đỉnh này.
-->

---

# Dataflow Model áp vào số liệu đo được

Tỷ lệ trade thành **dữ liệu muộn** theo độ trễ watermark W (watermark theo processing time):

| W | 50 ms | 100 ms | 200 ms | 300 ms | 500 ms |
|---|---:|---:|---:|---:|---:|
| Binance | 0,48% | 0,08% | **0%** | 0% | 0% |
| Coinbase | 100% | 99,2% | 11,2% | 0,17% | **0,004%** |

- Mỗi nguồn cần watermark riêng: Binance 0–200 ms, Coinbase 500 ms, vnstock khoảng 10 s
- Watermark tính theo tiến độ **event time** không bị ảnh hưởng bởi lệch đồng hồ. Với Binance, chỉ cần W = 0
- Dữ liệu muộn hàng giờ (phát hiện 2) thì **không watermark nào xử lý được**: cần khử trùng và giới hạn độ trễ cho phép

<!--
~1,5 phút. Gộp mọi nguồn dưới một watermark chung thì nguồn chậm nhất (vnstock) sẽ kéo cả hệ thống chậm theo.
Chi tiết trong docs/papers/dataflow-model.md.
-->

---

# Data contract

- JSON Schema 2020-12: `raw_event`, `binance_trade`, `coinbase_trade`
- Giá và khối lượng để **string** · thời gian **UTC ISO 8601, mili giây, hậu tố `Z`** · mã dạng **`BASE-QUOTE`**, giữ thêm `symbol_native`
- Ràng buộc rút ra từ toàn bộ dữ liệu thật, ví dụ: Binance luôn có 8 chữ số thập phân; `trade_id` của Coinbase là chuỗi
- **231 859 / 231 859 bản ghi hợp lệ** · **22 / 22 ca lỗi cố ý bị bắt**
- `make validate-contracts` đã được gắn vào CI, chạy trên fixtures

<!--
~1 phút. Schema chặt (additionalProperties: false): sàn thêm hoặc bớt trường thì ta biết ngay.
Đó chính là mục đích của observability.
-->

---

# Bài học và việc tiếp theo

**Bài học**
- Kiểm tra đồng hồ trước khi đo, và phải đo lại định kỳ
- Chỉ dựa vào cờ của sàn thì chưa đủ: Coinbase gửi lại lịch sử mà không gắn cờ
- Công cụ đo cũng có lỗi: bản đầu của `analyze.py` báo 49 682 "đứt seq" giả, do sort không ổn định

**Sprint sau**
1. Đồng bộ giờ bền vững và ghi offset vào log trong lúc thu
2. Thu vnstock trọn một phiên HOSE, viết schema cho vnstock
3. Đưa dữ liệu vào Kafka: khử trùng theo `event_id`, watermark riêng cho từng nguồn
4. Biến các phát hiện 1–3 thành detector observability

<!--
~1 phút. Tổng thời gian khoảng 10 phút.
-->
