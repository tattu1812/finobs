# Ghi chú bài báo The Dataflow Model, đối chiếu với dữ liệu FinObs

**Người thực hiện:** Trung, Data Engineer
**Sprint:** S1 (nghiên cứu, thu dữ liệu thật)
**Bài báo:** *The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing*
**Tác giả:** Tyler Akidau, Robert Bradshaw, Craig Chambers, Slava Chernyak, Rafael J. Fernández-Moctezuma, Reuven Lax, Sam McVeety, Daniel Mills, Frances Perry, Eric Schmidt, Sam Whittle (Google)
**Hội nghị:** VLDB 2015 (PVLDB tập 8, số 12)

> Mọi con số trong ghi chú này được tính từ phiên thu ngày **2026-10-27, 02:48:00 → 03:47:59 UTC**:
> 10 mã Binance và 10 mã Coinbase, tổng 231 859 bản ghi. Bảng gốc ở [`reports/throughput.md`](../../reports/throughput.md).
> Số liệu vnstock lấy từ một lần **chạy thử 2 phút** (02:52 UTC), chỉ mang tính sơ bộ.

## 1. Vấn đề bài báo đặt ra

Dữ liệu thực tế là **vô hạn** (không có điểm kết thúc), **đến không theo thứ tự**, và người dùng
muốn tự chọn điểm cân bằng giữa ba yếu tố: **đúng**, **nhanh** và **rẻ**. Các hệ thống trước đó buộc
phải chọn trước: batch (đúng nhưng chậm), micro-batch, hoặc streaming (nhanh nhưng xử lý kém dữ liệu
đến muộn). Bài báo đề xuất một mô hình trong đó ba yếu tố này là **các tham số cấu hình**, không phải
kiến trúc cố định. Mô hình xoay quanh bốn câu hỏi:

| Câu hỏi | Nghĩa | Cơ chế |
|---|---|---|
| **What** | Tính cái gì? | Phép biến đổi (ParDo, GroupByKey, …) |
| **Where** | Tính trên đoạn event time nào? | Windowing |
| **When** | Khi nào (theo processing time) thì phát kết quả? | Watermark + trigger |
| **How** | Các lần phát kết quả cho cùng một cửa sổ liên quan với nhau thế nào? | Accumulation mode |

Luận điểm cốt lõi: **không bao giờ biết chắc dữ liệu đã đủ**, nên đừng thiết kế hệ thống dựa trên
giả định "đủ rồi". Mục 2.4 dưới đây cho thấy dữ liệu của chúng ta xác nhận điều này một cách rất cụ thể.

## 2. Event time và processing time

**Bài báo:** *event time* là lúc sự kiện xảy ra; *processing time* là lúc hệ thống nhìn thấy nó.
Độ lệch giữa hai đại lượng này (skew) thay đổi liên tục và không thể đoán trước. Đây là nguồn gốc của mọi khó khăn.

**Ở FinObs:** `event_time` lấy từ đồng hồ sàn (`T` của Binance, `time` của Coinbase, `truncTime` của VCI),
`ingest_time` lấy từ đồng hồ máy thu. Độ trễ = `ingest_time − event_time`.

### 2.1 Phân phối độ trễ thực đo (chỉ tính trade live)

| Nguồn | Số trade | p50 | p90 | p99 | p99.9 | max |
|---|---:|---:|---:|---:|---:|---:|
| Binance | 203 732 | 7 ms | 19 ms | 42 ms | 96 ms | 118 ms |
| Coinbase | 26 520 | 160 ms | 202 ms | 272 ms | 332 ms | 504 ms |
| vnstock (chạy thử 2 phút) | 274 | 3 908 ms | — | p95 6 196 ms | — | 7 521 ms |

![Phân phối độ trễ](../../reports/latency_hist.png)

Ba nguồn chênh nhau **ba bậc độ lớn**, và mỗi chênh lệch đều có nguyên nhân rõ ràng:
- **Binance** gửi từng trade ngay khi khớp lệnh, nên độ trễ chỉ gồm thời gian truyền mạng.
- **Coinbase** gom trade thành lô 250 ms. Phân phối độ trễ có dạng gần như phẳng trong khoảng ~100–210 ms,
  đúng hình dạng của một khoảng chờ phân bố đều từ 0 đến 250 ms cộng thêm thời gian mạng.
  Đây là skew do *thiết kế của sàn*, không phải do mạng.
- **vnstock** là REST poll mỗi 5 giây cho mỗi mã, cộng thêm việc `truncTime` chỉ chính xác tới giây.
  Skew ở đây chủ yếu do *thiết kế của ta*.

### 2.2 Bản thân processing time cũng có sai số

Bài báo coi processing time là đồng hồ cục bộ của hệ thống. Thực tế, đồng hồ đó **lệch so với giờ chuẩn và
còn trôi theo thời gian**:

| Thời điểm (UTC) | Offset so với NTP |
|---|---:|
| 02:29 (trước khi chỉnh giờ) | +1779,3 ms |
| 02:48 (bắt đầu thu) | +15,9 ms |
| 03:48 (kết thúc thu) | +42,0 ms |
| 14:23 (10,6 giờ sau) | +626,1 ms |

Trong một giờ thu, p50 độ trễ của **cả hai sàn** giảm cùng một mức:
Binance từ 22 ms xuống −2 ms, Coinbase từ 176 ms xuống 147 ms (gom theo khung 10 phút).
Mức giảm này khớp với 26 ms đồng hồ trôi. Hệ quả: **32 461 trade Binance có độ trễ âm**,
một điều vô lý về mặt vật lý. Bài học: **mọi watermark xây trên processing time đều hấp thụ luôn sai số
của đồng hồ**. Watermark xây trên tiến độ event time (mục 4) thì không bị ảnh hưởng.

## 3. Windowing (Where)

**Bài báo:** chia dữ liệu theo event time thành các cửa sổ: *fixed/tumbling*, *sliding*, *session*.
Mô hình tổng quát hoá thành hai thao tác `AssignWindows` và `MergeWindows`. Nhờ thao tác merge,
cửa sổ session (khác nhau theo từng khoá) trở thành một trường hợp thông thường.

**Ở FinObs:**

| Cửa sổ (theo event time, chỉ trade live) | Binance | Coinbase |
|---|---|---|
| Tumbling 1 phút: số trade mỗi cửa sổ, min / trung vị / max | 1 425 / 3 258,5 / 7 116 | 293 / 427 / 1 090 |
| Tumbling 1 giây: tỷ lệ cửa sổ rỗng | 1/3600 (0,03%) | 52/3599 (1,4%) |
| Đỉnh 1 giây | 2 442 trade, lúc 02:57:01 | 309 trade, lúc 02:57:01 |

- **Fixed 1 phút** phù hợp cho các chỉ số observability ở mức nguồn (throughput, độ trễ).
  Khối lượng giữa các cửa sổ chênh nhau tới 5 lần (Binance 1 425–7 116), nên ngưỡng phát hiện bất thường
  không được là hằng số.
- **Ở mức từng mã**, cửa sổ cố định hoạt động kém với mã ít giao dịch: Coinbase BAND-USD chỉ có 5 trade live
  trong cả giờ, CTSI-USD có 4. Gần như mọi cửa sổ 1 phút của chúng đều rỗng. Với các mã này,
  **cửa sổ session** (đóng khi im lặng quá một khoảng gap) mô tả hoạt động giao dịch đúng hơn.
  Đây chính là ví dụ "cửa sổ khác nhau theo từng khoá" mà bài báo dùng để biện minh cho `MergeWindows`.
- **Đỉnh tải lúc 02:57:01 xảy ra đồng thời ở cả hai sàn** (xem
  [`trades_per_second.png`](../../reports/trades_per_second.png)). Một cửa sổ 1 giây đơn lẻ có thể chứa
  gấp 43 lần mức trung bình của Binance. Cửa sổ sliding (ví dụ dài 10 s, trượt mỗi 1 s) sẽ làm mượt đỉnh
  này nếu mục tiêu là cảnh báo về throughput.

## 4. Watermark (When, phần 1)

**Bài báo:** watermark là một mốc event time W sao cho hệ thống *tin rằng* mọi dữ liệu có event time nhỏ hơn W
đã đến. Watermark *hoàn hảo* hiếm khi có được; watermark *heuristic* có thể **quá chậm** (kết quả bị trễ)
hoặc **quá nhanh** (dữ liệu tới sau watermark thành dữ liệu muộn). Bài báo nhấn mạnh: chỉ có watermark thôi
là không đủ, cần thêm trigger.

Tôi thử hai kiểu watermark heuristic trên dữ liệu thật và đếm tỷ lệ trade trở thành **dữ liệu muộn**:

**(A) Theo processing time:** `watermark = ingest_time − W`. Một trade là muộn nếu độ trễ > W.

| W | 0 | 25 ms | 50 ms | 100 ms | 200 ms | 250 ms | 300 ms | 500 ms | 1 s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Binance (% muộn) | 79,51 | 3,91 | 0,484 | 0,084 | **0** | 0 | 0 | 0 | 0 |
| Coinbase (% muộn) | 100 | 100 | 100 | 99,20 | 11,15 | 1,95 | 0,170 | **0,0038** | 0 |

**(B) Theo tiến độ event time:** `watermark = max(event_time đã thấy của nguồn) − W`. Một trade là muộn nếu nó
đến sau một trade khác có event time lớn hơn nó quá W.

| W | 0 | 25 ms | 50 ms | 100 ms | 200 ms | 300 ms | 500 ms | Đi sau xa nhất |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Binance (% muộn) | **0** | 0 | 0 | 0 | 0 | 0 | 0 | 0 ms |
| Coinbase (% muộn) | 31,31 | 10,84 | 4,09 | 0,577 | 0,0075 | 0,0038 | **0** | 305 ms |

**Gợi ý đặt watermark, rút ra từ hai bảng trên:**
- **Binance:** luồng combined stream của 10 mã đến **đúng thứ tự event time tuyệt đối** (cột B toàn 0),
  nên watermark kiểu (B) có thể đặt W = 0. Nếu dùng kiểu (A), W = 200 ms là mức nhỏ nhất cho 0% dữ liệu muộn
  trong giờ đo. Nên cộng thêm biên cho đồng hồ trôi, vì riêng trong giờ thu đồng hồ đã trôi 26 ms.
- **Coinbase:** **W ≥ 300 ms, khuyến nghị 500 ms.** Mức sàn của W là 250 ms do Coinbase gom lô: với kiểu (A),
  W = 250 ms vẫn còn 1,95% dữ liệu muộn, W = 200 ms tới 11,15%, và W ≤ 50 ms thì 100% đều muộn. Kiểu (B) cũng cần W > 0 vì trong mỗi lô, trade được
  xếp từ mới đến cũ (31% trade "đi sau" ở W = 0). Với W = 500 ms, kiểu (B) cho 0 trade muộn và kiểu (A) cho
  đúng 1 trade muộn trên 26 520.
- **vnstock:** tối thiểu bằng chu kỳ poll cộng độ trễ lớn nhất, tức **khoảng 10 s** theo bản chạy thử
  (max 7,5 s). Con số này cần đo lại trên một phiên đầy đủ.
- **Không gộp các nguồn dưới một watermark chung.** Watermark của một luồng gộp là min của các đầu vào,
  nên sẽ bị nguồn chậm nhất kéo theo: vnstock (khoảng 10 s) sẽ làm chậm cả Binance (cần 0–200 ms).
  Mỗi nguồn nên có pipeline và watermark riêng.
- **Watermark theo từng mã sẽ bị đứng yên** ở các mã ít giao dịch (BAND-USD 5 trade/giờ). Watermark nên
  tính ở mức nguồn hoặc mức partition, không phải theo mã. Nếu vẫn tính theo mã thì cần cơ chế xử lý khi
  nguồn im lặng (idle).

Ghi chú: mọi số ở bảng (A) đều mang theo sai số đồng hồ (mục 2.2). Bảng (B) chỉ so event time với event time,
nên **không bị ảnh hưởng**. Đây là lý do thực tế để ưu tiên watermark kiểu (B).

## 5. Dữ liệu cực muộn: bằng chứng cho luận điểm "không bao giờ đủ"

Trong giờ thu, Coinbase gửi ba loại bản ghi **không phải dữ liệu thời gian thực**:

| Loại | Số bản ghi | Độ trễ trung vị | Độ trễ max | Cách nhận biết |
|---|---:|---:|---:|---|
| Snapshot (có cờ) | 1 300 | 824 194 ms (≈ 14 phút) | 234 584 028 ms (≈ 65 giờ) | `transport.is_snapshot` |
| Trùng lặp | 219 | 77 574 640 ms (≈ 21,5 giờ) | 229 894 280 ms (≈ 64 giờ) | `event_id` đã gặp |
| Phát lại (không có cờ) | 88 | 28 884 457 ms (≈ 8 giờ) | 70 765 589 ms (≈ 19,7 giờ) | event time trước lần kết nối đầu |

Đáng chú ý nhất: lúc 03:20:31, 03:21:21 và 03:33:00, **không có reconnect nào**, nhưng Coinbase vẫn gửi lại
lịch sử của ALGO-USD, BAND-USD và CTSI-USD. Mỗi lần là một message `snapshot` kèm theo một message
**`update`** chứa 100 trade cũ. Cờ snapshot không bắt được các trade này.
Không watermark heuristic nào xử lý được độ trễ tính bằng giờ. Theo đúng tinh thần bài báo, dữ liệu kiểu này
cần được xử lý **tường minh**:
1. **Khử trùng theo `event_id` trước khi chia cửa sổ.** Có thêm lý do: Coinbase còn gửi trùng cả trong luồng
   live (7 trade BTC bị lặp giữa hai message liên tiếp trong đợt dồn tải 02:57:01).
2. **Giới hạn độ trễ cho phép (allowed lateness)** để giải phóng trạng thái của các cửa sổ cũ.
   Bản ghi muộn hơn giới hạn này bị bỏ, **nhưng phải được đếm thành chỉ số**. Với FinObs, số bản ghi bị bỏ
   vì quá muộn chính là một tín hiệu observability.

## 6. Trigger (When, phần 2)

**Bài báo:** trigger quyết định *khi nào theo processing time* thì phát kết quả cho một cửa sổ. Có thể phát
sớm (early, ví dụ theo chu kỳ processing time), đúng hạn (on-time, khi watermark vượt qua cuối cửa sổ),
và muộn (late, mỗi khi có dữ liệu muộn), kết hợp với nhau thành trigger tổng hợp.

**Đối chiếu với dữ liệu:** trade cuối cùng của mỗi cửa sổ 1 phút đến muộn nhất **43 ms (Binance) và 169 ms
(Coinbase)** sau khi cửa sổ kết thúc (tính theo đồng hồ máy thu). Nghĩa là với W như khuyến nghị ở mục 4,
lần phát *on-time* đã chứa gần như toàn bộ dữ liệu trong giờ đo.

Đề xuất cho dashboard throughput và độ trễ của FinObs, cửa sổ 1 phút:
- **Early:** mỗi 1 s processing time, để dashboard không phải đợi hết phút.
- **On-time:** khi watermark (theo nguồn, mục 4) vượt qua cuối cửa sổ.
- **Late:** mỗi bản ghi muộn trong phạm vi allowed lateness. Theo bảng (A), Coinbase với W = 500 ms chỉ có
  khoảng 1 lần phát late mỗi giờ.

## 7. Accumulation mode (How)

**Bài báo:** khi một cửa sổ được phát nhiều lần, các lần phát quan hệ với nhau theo một trong ba cách:
*discarding* (mỗi lần chỉ chứa phần mới), *accumulating* (mỗi lần chứa toàn bộ, ghi đè lần trước),
và *accumulating & retracting* (phát kết quả mới kèm lệnh rút lại kết quả cũ).

**Ở FinObs:**
- Bảng chỉ số trên dashboard được **ghi đè** theo khoá (nguồn, cửa sổ), nên **accumulating** là đủ.
  Mỗi lần phát là giá trị đúng tại thời điểm đó.
- Nếu phía sau cộng dồn các lần phát (ví dụ đẩy qua Kafka sang một aggregator tính tổng) thì phải dùng
  **discarding**, nếu không sẽ đếm trùng.
- **Retraction** chỉ cần thiết nếu khử trùng diễn ra *sau* khi đếm. Ví dụ: một cửa sổ đã phát với 7 trade
  trùng của đợt dồn tải 02:57:01, sau đó mới phát hiện trùng. Vì vậy mục 5 đề xuất khử trùng **trước**
  windowing: khi đó không cần retraction.

## 8. Kết luận cho thiết kế FinObs

1. Mỗi nguồn có watermark riêng: Binance 0–200 ms, Coinbase 500 ms, vnstock khoảng 10 s (cần đo lại).
   Ưu tiên watermark theo tiến độ event time, vì kiểu này không bị ảnh hưởng bởi lệch đồng hồ máy thu.
2. Khử trùng theo `event_id` ở ngay tầng đầu vào. Đếm và giám sát số snapshot, số bản ghi trùng, số bản ghi
   phát lại và số bản ghi bị bỏ vì quá muộn.
3. Đồng hồ máy thu là một phụ thuộc cần được giám sát: chạy `clock_check` định kỳ. Độ trễ âm là dấu hiệu
   cảnh báo, không phải dữ liệu để làm sạch.
4. Mã ít giao dịch cần cửa sổ session hoặc chỉ số ở mức nguồn, không dùng cửa sổ cố định theo mã.
