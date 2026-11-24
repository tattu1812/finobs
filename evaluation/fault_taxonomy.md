# FinObs Fault Taxonomy (FT01–FT12)

## 1. Mục đích và cơ sở phân loại

Bộ phân loại lỗi (fault taxonomy) xác định **12 cơ chế lỗi dữ liệu và pipeline** có thể tạo ra có kiểm soát trong FinObs. Mỗi loại mô tả rõ bản chất lỗi, cơ chế phát sinh, nguyên nhân gốc, vị trí, hậu quả, tín hiệu quan sát dự kiến và ví dụ tiêm lỗi. Taxonomy hỗ trợ thiết kế kịch bản kiểm thử và đối chiếu kết quả chẩn đoán với Ground Truth.

Cách phân loại được FinObs xây dựng dựa trên kiến trúc thu thập, truyền, xử lý và đối chiếu dữ liệu tài chính, đồng thời tham khảo nghiên cứu **Foidl et al. (2023), _Data Pipeline Quality: Influencing Factors, Root Causes of Data-related Issues, and Processing Problem Areas for Developers_** ([arXiv:2309.07067](https://arxiv.org/abs/2309.07067)). **FT01–FT12 là taxonomy do FinObs đề xuất**, không phải 12 nhóm lỗi được trích nguyên văn từ bài báo. Các yếu tố ảnh hưởng chất lượng pipeline trong nghiên cứu không được xem là danh sách các lỗi tiêm trực tiếp.

## 2. Nguyên tắc phân loại và gắn nhãn

1. **Một scenario sự cố = một `fault_type` chính**, lấy từ **cơ chế được chủ động tiêm**; không suy ra mã lỗi chỉ từ biểu hiện.
2. **Cơ chế lỗi (mechanism)** là thao tác gây bất thường, ví dụ cố ý bỏ event. **Nguyên nhân gốc (root cause)** là thành phần/logic thực sự được xác định gây lỗi trong scenario. **Tín hiệu (signal)** là metric hoặc hiện tượng mà detector có thể quan sát; không phải luôn là nguyên nhân.
3. Trong scenario sự cố, `scenario.fault_type` và `ground_truth.fault_type` phải thống nhất. `affected_assets` có thể chứa nhiều đối tượng chịu ảnh hưởng nhưng không có nghĩa chúng đều là nguyên nhân gốc.
4. Nếu có một lỗi chính gây nhiều triệu chứng, giữ một nhãn chính và ghi triệu chứng trong `expected_signals`. Benchmark đa lỗi hoặc nhiều root cause hợp lệ phải được thiết kế riêng.
5. Đối chứng hợp lệ có `ground_truth.is_incident: false`, `injection: null`; trong schema hiện hành, trường `scenario.fault_type` vẫn bắt buộc, chỉ cho biết loại lỗi mà đối chứng hướng tới kiểm tra báo nhầm, **không khẳng định sự cố đã xảy ra**.
6. Các giá trị thời gian, tỷ lệ, mức biến động trong ví dụ đều là **tham số giả lập minh họa**, không phải ngưỡng thống nhất của hệ thống.

## 3. Bảng tổng hợp 12 loại lỗi

| Mã lỗi | Tên lỗi | Định nghĩa | Cơ chế phát sinh | Nguyên nhân gốc điển hình | Vị trí phát sinh | Hậu quả | Tín hiệu phát hiện dự kiến | Ví dụ tiêm lỗi |
|---|---|---|---|---|---|---|---|---|
| `FT01_stale_feed` | Stale Feed – Ngừng cập nhật | Không có event mới trong phạm vi đáng lẽ đang hoạt động | Chặn/ngừng toàn bộ event từ một feed | Producer treo, subscription ngừng, điểm chặn dữ liệu | Source / Ingestion | Dữ liệu lỗi thời; downstream không cập nhật | Freshness age ↑, số trade/phút về 0 | Dừng feed BTCUSDT 300 giây |
| `FT02_volume_loss` | Volume Loss – Mất một phần dữ liệu | Một phần event bị mất dù nguồn còn gửi | Bỏ sự kiện theo tỷ lệ hoặc partition | Buffer/consumer mất message, lỗi ghi | Kafka / Streaming / Ingestion | Thiếu giao dịch, sai tổng khối lượng | Volume ↓, trade-ID gap hoặc stream–batch mismatch khi có nguồn chuẩn | Bỏ 30% event trong 5 phút |
| `FT03_duplicate_events` | Duplicate Events – Trùng bản ghi | Một event logic được ghi nhận nhiều lần | Nhân bản bản ghi/ghi lại cùng ID | Retry không idempotent, dedup lỗi | Ingestion / Cleaning | Thổi phồng count/volume nếu không dedup | Duplicate ratio ↑; raw count > distinct count | Nhân đôi 10% event giữ cùng ID |
| `FT04_history_replay` | History Replay – Phát lại lịch sử | Event cũ được phát lại ngoài chính sách hợp lệ | Chèn một khối dữ liệu lịch sử vào luồng hiện tại | Replay/reconnect/snapshot bị xử lý sai | Ingestion | Event cũ, có thể trùng, sai cửa sổ thời gian | Tuổi event ↑, ID từng xuất hiện | Phát lại trade từ 6 giờ trước |
| `FT05_late_out_of_order` | Late / Out-of-order – Muộn/sai thứ tự | Event đến quá muộn hoặc sai thứ tự vượt dung sai xử lý | Delay/đảo thứ tự phân phối nhưng giữ event-time gốc | Backpressure, network delay, scheduling | Streaming / Transformation | Kết quả window sai hoặc bỏ event muộn | Ingest delay p95 ↑, late/drop count ↑ | Trì hoãn 20% event vượt watermark |
| `FT06_clock_skew` | Clock Skew – Lệch đồng hồ | Timestamp ghi nhận không khớp thời gian thực | Cộng/trừ offset hoặc tạo clock drift vào ingest-time | Đồng hồ host/NTP/logic timestamp sai | Host / Ingestion | Độ trễ đo sai, sai metric thời gian | Negative-delay ratio ↑, dịch chuyển đồng loạt latency | Trừ 2 giây khỏi `ingest_time` |
| `FT07_schema_drift` | Schema Drift – Đổi cấu trúc | Payload/contract thay đổi không tương thích | Đổi/xóa trường, đổi cấu trúc payload | Thay đổi API/schema không tương thích | Source / Integration | Bản ghi bị từ chối, parser lỗi | Contract violation ↑ | Đổi `price` thành `last_price` |
| `FT08_type_precision` | Type / Precision – Sai kiểu/độ chính xác | Phép cast/rounding làm thay đổi cách biểu diễn hoặc giá trị số | Ép kiểu hoặc làm tròn trong xử lý | Parser/cast trong transformation | Cleaning / Transformation | Sai kết quả số học, đối soát chênh | Sai khác raw–processed, vi phạm precision | Làm tròn giá xuống 2 chữ số |
| `FT09_value_anomaly` | Value Anomaly – Sai giá trị/đơn vị | Giá trị nghiệp vụ bị sửa sai ngoài quy tắc hợp lệ | Nhân giá, sai đơn vị hoặc chèn ngoại lai | Lỗi quy đổi / logic sửa giá | Ingestion / Transformation | Sai dữ liệu tài chính, cảnh báo sai | Distribution shift, outlier, cross-source gap có thể ↑ | Nhân giá BTC × 1000 |
| `FT10_cross_source_mismatch` | Cross-source Mismatch – Sai đối chiếu | Cơ chế ánh xạ/căn chỉnh hai nguồn bị sai | Thay symbol-pair mapping hoặc logic time alignment | Mapping/validation logic lỗi | Integration / Cross-source Validation | Kết luận đối chiếu sai, false alarm | Cross-source return gap ↑ | Ghép BTC với ETH khi so sánh |
| `FT11_pipeline_failure` | Pipeline Failure – Job lỗi | Job/DAG xử lý thất bại làm pipeline gián đoạn | Làm job thất bại trực tiếp tại runtime | Spark/Airflow task, lỗi cấu hình, tài nguyên | Processing / Infrastructure | Downstream trễ, thiếu dữ liệu | Job failure, run error, freshness downstream ↓ | Làm một job Spark thất bại trong test |
| `FT12_null_spike` | Null Spike – Null tăng đột biến | Tỷ lệ giá trị null ở trường quan trọng tăng bất thường | Thay giá trị hợp lệ thành null, vẫn giữ tên trường | Parser/transformation tạo mất giá trị | Ingestion / Cleaning / Transformation | Mất completeness, record bị reject/tổng hợp thiếu | Null ratio ↑; contract violation ↑ nếu non-null bắt buộc | Gán `price = null` cho 25% event |

**Lưu ý:** Các tín hiệu trên là dấu hiệu kỳ vọng về mặt thiết kế, không mặc định là metric đã được triển khai hoặc là ngưỡng phát hiện chuẩn.

## 4. Định nghĩa chi tiết từng loại lỗi

### FT01 – Stale Feed
- **Phạm vi:** Một nguồn, subscription hoặc symbol có dữ liệu kỳ vọng liên tục nhưng ngừng cập nhật.
- **Cơ chế tiêm:** Chặn toàn bộ event mới trên phạm vi được chọn, ví dụ 300 giây.
- **Nguyên nhân/vị trí:** Điểm dừng producer, subscription hoặc cổng thu dữ liệu. Root cause của scenario phải ghi đúng điểm được tiêm, không quy cho sàn khi chỉ dừng recorder của FinObs.
- **Hậu quả/tín hiệu:** Dữ liệu cũ đi; thời gian từ trade gần nhất tăng, count trên nguồn/symbol giảm về 0.
- **Ranh giới:** FT02 làm rơi một phần, FT01 làm dừng luồng trong phạm vi xác định; thị trường đóng cửa hoặc symbol không có trade tự nhiên là đối chứng, không tự động là sự cố.

### FT02 – Volume Loss
- **Phạm vi:** Dữ liệu vẫn tới nhưng bị mất một phần trên đường đi hoặc khi ghi.
- **Cơ chế tiêm:** Xóa p% event, rơi một partition, bỏ một nhóm message có nhãn xác định.
- **Nguyên nhân/vị trí:** Consumer/buffer/Kafka/điểm ghi; định nghĩa root cause theo nơi chủ động loại bỏ dữ liệu.
- **Hậu quả/tín hiệu:** Thiếu dữ liệu, trade count và tổng giao dịch giảm; đối soát với nguồn chuẩn hoặc ID có thể cho thấy khoảng trống. **ID gap chỉ có ý nghĩa nếu ID/sequence đảm bảo quy tắc liên tục tương ứng.**
- **Ranh giới:** Không chẩn đoán mất dữ liệu chỉ vì lượng giao dịch thấp tự nhiên hoặc hai nguồn có volume khác nhau.

### FT03 – Duplicate Events
- **Phạm vi:** Cùng event logic bị ghi lại nhiều lần (khóa định danh theo quy tắc mỗi nguồn).
- **Cơ chế tiêm:** Nhân bản k% event, giữ nguyên ID/khóa dedup thích hợp.
- **Nguyên nhân/vị trí:** Retry thiếu idempotency hoặc logic dedup bị lỗi.
- **Hậu quả/tín hiệu:** Raw count tăng; số event duy nhất không tăng; aggregate có thể bị thổi phồng nếu không dedup.
- **Ranh giới:** FT04 là phát lại dữ liệu lịch sử; hai loại có thể sinh cùng tín hiệu duplicate nhưng chọn nhãn theo thao tác tiêm.

### FT04 – History Replay
- **Phạm vi:** Một khối dữ liệu có event-time cũ bị đưa lại vào luồng hiện tại ngoài quy trình replay cho phép.
- **Cơ chế tiêm:** Chèn lại bản ghi từ nhiều giờ trước, có thể giữ ID/event-time gốc.
- **Nguyên nhân/vị trí:** Replay/snapshot xử lý sai hoặc không nhận diện rõ dữ liệu lịch sử.
- **Hậu quả/tín hiệu:** Tuổi event lớn bất thường, có thể duplicate, làm méo window.
- **Ranh giới:** Replay recovery có kiểm soát và dedup đúng không tự động là sự cố; FT05 là trễ vận chuyển event hiện hành, không chủ động phát lại kho lịch sử.

### FT05 – Late / Out-of-order
- **Phạm vi:** Sự kiện tới chậm/sai thứ tự tới mức vượt watermark hoặc làm kết quả theo event-time sai.
- **Cơ chế tiêm:** Đổi lịch phát hoặc trì hoãn một tập event; **không sửa timestamp gốc**.
- **Nguyên nhân/vị trí:** Delay/backpressure trong mạng hoặc streaming, watermark quá sát.
- **Hậu quả/tín hiệu:** Event late tăng, tổng hợp cửa sổ thiếu/sai; ingest-delay percentile có thể tăng.
- **Ranh giới:** Out-of-order trong dung sai bình thường không tự động là lỗi; FT06 làm sai thời gian được ghi chứ không thay đổi thời điểm đến thật.

### FT06 – Clock Skew
- **Phạm vi:** `ingest_time` sai vì đồng hồ hoặc quy tắc ghi thời gian lệch.
- **Cơ chế tiêm:** Dịch timestamp bằng offset hoặc drift có kiểm soát, không giả vờ rằng event bị chậm thật.
- **Nguyên nhân/vị trí:** Đồng hồ/NTP của host/collector hoặc timestamp assignment.
- **Hậu quả/tín hiệu:** Latency âm, percentile lệch đồng loạt và số đo freshness không đáng tin.
- **Ranh giới:** Latency âm chỉ là triệu chứng, chưa đủ chứng minh Clock Skew nếu chưa kiểm tra time semantics và đồng hồ nguồn.

### FT07 – Schema Drift
- **Phạm vi:** Dữ liệu nguồn đổi trường hoặc cấu trúc không tương thích với contract.
- **Cơ chế tiêm:** Rename/drop field, đổi object shape, thay đổi kiểu ở ranh giới payload contract.
- **Nguyên nhân/vị trí:** API/producer thay schema nhưng parser/contract chưa tương thích.
- **Hậu quả/tín hiệu:** Contract validation fail, bản ghi bị từ chối, parsing lỗi.
- **Ranh giới:** Thêm trường tùy chọn được contract cho phép không phải lỗi; FT08 là cast/precision sai khi vẫn dùng giao diện dữ liệu cũ.

### FT08 – Type / Precision
- **Phạm vi:** Giá/khối lượng bị sai do chuyển đổi kiểu hoặc giảm precision trong processing.
- **Cơ chế tiêm:** Cast, làm tròn số, cắt chữ số có nghĩa, thay decimal bởi kiểu thấp hơn độ chính xác yêu cầu.
- **Nguyên nhân/vị trí:** Parser/Spark transformation/cleaning.
- **Hậu quả/tín hiệu:** Giá trị giữa raw và tầng sau lệch vượt tolerance; tổng tiền và phép đối soát sai.
- **Ranh giới:** FT09 là sửa sai giá trị nghiệp vụ hoặc đơn vị có chủ đích; rounding hợp lệ trong tolerance thì không phải sự cố.

### FT09 – Value Anomaly
- **Phạm vi:** Giá trị nghiệp vụ bị sửa hoặc chuyển đơn vị sai dù tên trường/kiểu có thể hợp lệ.
- **Cơ chế tiêm:** Nhân giá ×1000, chèn giá/khối lượng sai, thay quy đổi đơn vị.
- **Nguyên nhân/vị trí:** Logic sửa giá, chuẩn hóa sai hoặc payload bị cố ý làm sai.
- **Hậu quả/tín hiệu:** Outlier, price distribution shift, một nguồn lệch nguồn khác.
- **Ranh giới:** Biến động giá mạnh tự nhiên không đủ chứng minh lỗi; nếu cơ chế sửa giá gây cross-source gap, nhãn chính vẫn là FT09, không phải FT10.

### FT10 – Cross-source Mismatch
- **Phạm vi:** Logic đối chiếu giữa hai nguồn sai vì mapping, time alignment hoặc chuyển đổi chuẩn so sánh.
- **Cơ chế tiêm:** Sửa bảng symbol-pair, ghép sai hai sản phẩm hoặc dịch thời gian căn chỉnh trong comparator.
- **Nguyên nhân/vị trí:** Cross-source mapping/validation component (C1).
- **Hậu quả/tín hiệu:** Sai khác không có ý nghĩa hoặc false positive tăng, return gap bất thường.
- **Ranh giới:** Chênh lệch thị trường thật, chênh khác quote currency hay đặc thù sàn không tự động là lỗi; sai giá trị một nguồn do tiêm thuộc FT09.

### FT11 – Pipeline Failure
- **Phạm vi:** Job hoặc task xử lý thất bại trực tiếp tại runtime, tác động downstream.
- **Cơ chế tiêm:** Mô phỏng job fail hoặc tạo cấu hình kiểm thử khiến một job không thể hoàn thành; chỉ thực hiện trong môi trường test được cho phép.
- **Nguyên nhân/vị trí:** Spark job/Airflow task/runtime/resource.
- **Hậu quả/tín hiệu:** Job status failed, run log lỗi, downstream stale hoặc thiếu partition.
- **Ranh giới:** Nếu chủ động đổi schema FT07 rồi job crash theo sau, nhãn chính là FT07 (lỗi đầu vào được tiêm), không phải FT11.

### FT12 – Null Spike
- **Phạm vi:** Tỷ lệ `null` ở một field quan trọng tăng vượt mức kỳ vọng, trong khi tên trường và cấu trúc payload vẫn giữ nguyên.
- **Cơ chế tiêm:** Chọn có kiểm soát một tỷ lệ bản ghi đang có giá trị hợp lệ và gán thành `null`; ví dụ `price` của 25% event trong 300 giây. Tỷ lệ 25% chỉ là ví dụ giả lập.
- **Nguyên nhân/vị trí:** Điểm xử lý dữ liệu được tiêm tại Ingestion/Cleaning/Transformation; Ground Truth phải xác định chính xác asset và layer thực hiện gán null.
- **Hậu quả/tín hiệu:** Completeness giảm, contract invalid tăng nếu trường non-null, aggregate thiếu hoặc reject record. Ví dụ metric có thể sử dụng: `price_null_ratio_1m` hoặc ratio theo field; metric `contract_violation_rate_1m` chỉ phản ánh lỗi nếu contract coi null là vi phạm và metric đo được.
- **Ranh giới:** `price` vẫn có mặt nhưng giá trị null → FT12; trường `price` biến mất/đổi tên → FT07; chuyển kiểu sai khiến giá trị mất sau cast → FT08 nếu đó là cơ chế được tiêm. Field optional được cho phép null không tự động tạo sự cố.
- **Điều kiện kiểm thử:** Phải xác nhận field mục tiêu tồn tại, dữ liệu gốc không null, điểm đo nhìn thấy giá trị hoặc bản ghi bị từ chối, và ghi số bản ghi thực sự bị can thiệp vào runtime injection evidence sau khi thực thi; Ground Truth chỉ mô tả phạm vi và tỷ lệ dự kiến trước thực thi.
- **Đặc trưng phân loại:** Null Spike đại diện trực tiếp cho sự suy giảm tính đầy đủ (completeness) mà không đòi hỏi thay đổi cấu trúc payload.

## 5. Phân biệt các loại lỗi chồng lấn

| Cặp lỗi | Chọn lỗi chính như thế nào? |
|---|---|
| FT01 / FT02 | Dừng toàn bộ feed trong phạm vi kiểm thử → FT01; vẫn có luồng nhưng mất một phần → FT02. |
| FT03 / FT04 | Nhân bản event → FT03; phát lại một đoạn lịch sử → FT04, dù có thể phát sinh duplicate. |
| FT04 / FT05 | Replay event cũ có chủ đích → FT04; delay event mới trong đường truyền → FT05. |
| FT05 / FT06 | Thay đổi thời điểm phân phối → FT05; sửa timestamp/clock → FT06. |
| FT07 / FT08 | Thay đổi contract/payload shape → FT07; cast/precision sai trong xử lý → FT08. |
| FT07 / FT12 | Bỏ/rename trường → FT07; giữ trường nhưng gán null → FT12. |
| FT08 / FT09 | Mất precision do cast → FT08; làm sai giá trị nghiệp vụ/đơn vị → FT09. |
| FT09 / FT10 | Sửa giá một nguồn gây cross-source gap → FT09; sửa mapping/alignment trực tiếp → FT10. |
| FT07 / FT11 | Tiêm schema lỗi khiến job crash → FT07; trực tiếp làm job fail → FT11. |