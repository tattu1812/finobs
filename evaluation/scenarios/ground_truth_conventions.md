# FinObs-Bench – Ground Truth Conventions

## 1. Mục đích và phạm vi

Ground Truth là tập thông tin mô tả đáp án chuẩn của mỗi kịch bản đánh giá FinObs-Bench. Thông tin này được xác định từ cơ chế tiêm lỗi có kiểm soát hoặc từ phạm vi đối chứng đã được xác minh, không được suy luận từ dự đoán của detector hay RCA Agent.

Ground Truth được sử dụng để:

- Xác định kịch bản có sự cố dữ liệu hay không.
- Xác định loại lỗi chính thuộc taxonomy FT01–FT12.
- Xác định khoảng thời gian xảy ra sự cố.
- Xác định asset và thành phần gây lỗi.
- Phân biệt nguyên nhân gốc với các asset bị ảnh hưởng.
- Mô tả tín hiệu quan sát kỳ vọng và các cảnh báo cần tránh.
- Cung cấp đáp án để đánh giá phát hiện và chẩn đoán sự cố.

Ground Truth là dữ liệu chuẩn phục vụ đánh giá, không phải kết quả do detector hoặc RCA Agent dự đoán.

## 2. Nguồn dữ liệu và nguyên tắc xác lập Ground Truth

### 2.1. Nguồn xác lập

| Loại kịch bản | Căn cứ xác lập | Giới hạn |
|---|---|---|
| Synthetic injection | Tham số và cơ chế tiêm được định nghĩa | Không chứng minh lỗi đã xảy ra trong hệ thống thực |
| Replay có tiêm lỗi | Dataset tham chiếu và cơ chế tiêm | Phân biệt lỗi được tiêm với hiện tượng có sẵn |
| Negative control | Dữ liệu và phạm vi tín hiệu được xác minh không có sự cố cần chấm | Không mở rộng kết luận sang metric ngoài phạm vi |
| Runtime execution | Log tiêm lỗi, kết quả chạy và timestamps thực tế | Cung cấp bằng chứng thực thi của kịch bản |

### 2.2. Nguyên tắc xác lập

1. Ground Truth được xác định trước khi đánh giá hệ thống.
2. Mỗi scenario sự cố có một `fault_type` chính theo cơ chế được tiêm.
3. Phải phân biệt dữ liệu quan sát, lỗi giả lập và nguyên nhân được xác định.
4. Không thay đổi Ground Truth sau khi xem kết quả Agent chỉ để cải thiện điểm đánh giá.
5. Mọi thay đổi Ground Truth sau khi nghiệm thu phải được ghi nhận để có thể truy vết.
6. Detector và RCA Agent không được sử dụng Ground Truth làm đầu vào dự đoán.
7. Kết quả validation cấu trúc không đồng nghĩa Ground Truth đã được xác minh bằng thực nghiệm.

## 3. Quy ước cấu trúc Ground Truth

### 3.1. Kịch bản có sự cố

Đối với `ground_truth.is_incident: true`, Ground Truth mô tả một sự cố có nhãn và nguyên nhân gốc.

| Trường | Bắt buộc | Ý nghĩa |
|---|---|---|
| `is_incident` | Có | Xác nhận scenario có sự cố |
| `fault_type` | Có | Mã loại lỗi FT01–FT12 |
| `start_offset_s` | Có | Thời điểm bắt đầu sự cố tính bằng giây |
| `end_offset_s` | Có | Thời điểm kết thúc sự cố tính bằng giây |
| `root_cause.asset` | Có | Asset gốc dùng làm đáp án RCA |
| `root_cause.component` | Có | Thành phần trực tiếp gây lỗi |
| `root_cause.explanation` | Có | Giải thích nguyên nhân |
| `affected_assets` | Có | Các asset chịu ảnh hưởng |
| `expected_signals` | Có | Các tín hiệu dự kiến thay đổi |
| `must_not_alert` | Không | Các cảnh báo không được xuất hiện trong phạm vi xác định |

Ví dụ Ground Truth cho FT12 Null Spike:

```yaml
ground_truth:
  is_incident: true
  fault_type: FT12_null_spike
  start_offset_s: 120
  end_offset_s: 300

  root_cause:
    asset: "finobs:raw.binance_trades"
    component: "raw-data mutation"
    explanation: >
      Co tinh gan null cho truong price cua
      mot phan giao dich trong pham vi tiem loi.

  affected_assets:
    - "finobs:raw.binance_trades"

  expected_signals:
    - metric: price_null_ratio_1m
      group: {source: binance}
      direction: up
      catalog_status: proposed
```

Đoạn trên minh họa phần Ground Truth, không phải một scenario hoàn chỉnh.

### 3.2. Kịch bản đối chứng

Đối với `ground_truth.is_incident: false`:

- `injection` bằng `null`.
- Ground Truth không chứa loại lỗi đã xảy ra.
- Không chứa khoảng thời gian sự cố.
- Không chứa root cause.
- Không chứa danh sách affected assets hoặc expected signals của sự cố.
- Có thể khai báo `must_not_alert` để xác định phạm vi kiểm tra false positive.

Ví dụ:

```yaml
injection: null

ground_truth:
  is_incident: false
  must_not_alert:
    - metric: trade_count_1m
      note: Bien dong hop le trong pham vi doi chung.
```

Theo cấu trúc schema v1, trường `fault_type` ở cấp scenario vẫn bắt buộc với đối chứng. Khi `is_incident: false`, giá trị này biểu thị loại lỗi mà đối chứng hướng đến kiểm tra báo nhầm, không khẳng định sự cố đã xảy ra.

## 4. Nguyên tắc lựa chọn Fault Type

### 4.1. Một loại lỗi chính

Mỗi scenario sự cố có một mã lỗi chính thuộc FT01–FT12.

Điều kiện nhất quán:

`scenario.fault_type == ground_truth.fault_type`

Mã lỗi được xác định theo cơ chế được chủ động tiêm, không suy luận chỉ từ triệu chứng.

Ví dụ:

| Cơ chế tiêm lỗi | Fault Type chính |
|---|---|
| Đổi tên trường giá làm job thất bại | `FT07_schema_drift` |
| Trực tiếp làm job xử lý thất bại | `FT11_pipeline_failure` |
| Dịch timestamp ghi nhận | `FT06_clock_skew` |
| Trì hoãn event nhưng giữ timestamp gốc | `FT05_late_out_of_order` |
| Giữ nguyên field nhưng gán giá trị null | `FT12_null_spike` |
| Nhân bản cùng một event ID | `FT03_duplicate_events` |
| Phát lại một đoạn dữ liệu lịch sử | `FT04_history_replay` |

### 4.2. Lỗi chính và triệu chứng thứ cấp

Nếu một lỗi gây ra nhiều biểu hiện, Ground Truth vẫn giữ một `fault_type` chính.

Các biểu hiện thứ cấp được mô tả trong `expected_signals`.

Ví dụ, FT07 Schema Drift có thể đồng thời gây ra:

- Contract violation tăng.
- Parser thất bại.
- Số bản ghi hợp lệ giảm.
- Dữ liệu downstream bị trễ.

Các biểu hiện này không tự động tạo thêm Ground Truth FT01, FT02 hoặc FT11.

Benchmark nhiều lỗi độc lập hoặc nhiều root cause phải sử dụng mô hình Ground Truth riêng.

## 5. Quy ước Root Cause và Asset

### 5.1. Phân biệt các đối tượng

| Đối tượng | Ý nghĩa |
|---|---|
| Injection target | Vị trí thực hiện thao tác gây lỗi |
| Root-cause asset | Asset đại diện cho nguyên nhân gốc |
| Root-cause component | Thành phần hoặc logic thực sự gây lỗi |
| Affected asset | Asset chịu ảnh hưởng trực tiếp hoặc gián tiếp |
| Observed metric | Tín hiệu đo biểu hiện hoặc hậu quả của lỗi |

Root-cause asset không nhất thiết trùng với tất cả affected assets.

Ví dụ FT06 Clock Skew:

- Root-cause asset: `finobs:host.collector`.
- Affected assets: `finobs:raw.binance_trades`, `finobs:raw.coinbase_trades`.
- Observed metric: `negative_delay_ratio_1m`.

Việc metric của cả hai nguồn bất thường không đủ để kết luận cả hai nguồn đều là nguyên nhân gốc.

### 5.2. Định dạng asset

Các asset trong YAML sử dụng định dạng:

`<namespace>:<name>`

Ví dụ:

- `finobs:raw.binance_trades`
- `finobs:raw.coinbase_trades`
- `finobs:bronze.trades`
- `finobs:host.collector`

Định dạng này được sử dụng thống nhất trong Ground Truth và các scenario hiện có.

### 5.3. Nguyên tắc ánh xạ định danh

Khi so sánh Ground Truth với kết quả RCA:

1. Mỗi scenario sự cố có một `root_cause.asset` chính.
2. Các ứng viên RCA được chuyển về định danh chuẩn trước khi so sánh.
3. Hai định danh được coi là tương đương nếu bằng nhau chính xác hoặc có ánh xạ tương đương hợp lệ.
4. Không xác định tương đương chỉ dựa trên tên gần giống.
5. Node cha và node con trong lineage không mặc nhiên tương đương.
6. Affected asset không tự động được chấp nhận thay cho root-cause asset.
7. Quy tắc chuẩn hóa phải nhất quán trên toàn bộ tập scenario.

Ví dụ hai cách biểu diễn có thể cần chuẩn hóa:

- `finobs:raw.binance_trades`
- `dataset:finobs:raw.binance_trades`

Chỉ được coi chúng tương đương khi quy tắc ánh xạ xác định cả hai cùng đại diện cho một asset.

## 6. Quy ước thời gian sự cố

### 6.1. Mốc thời gian

`input.window.start` là mốc thời gian gốc để tính các offset.

Thời điểm bắt đầu:

`t_start = input.window.start + ground_truth.start_offset_s`

Thời điểm kết thúc:

`t_end = input.window.start + ground_truth.end_offset_s`

Khoảng sự cố sử dụng quy ước:

`[t_start, t_end)`

Bao gồm thời điểm bắt đầu và không bao gồm thời điểm kết thúc.

Các điều kiện:

- `window.end > window.start`
- `start_offset_s >= 0`
- `end_offset_s > start_offset_s`
- Khoảng Ground Truth nằm trong input window.
- Khoảng injection hợp lệ và nằm trong input window.

Thông thường, thời gian Ground Truth trùng khoảng injection. Nếu sự ảnh hưởng kéo dài sau injection, phải phân biệt thời gian tiêm lỗi với thời gian hệ thống chịu ảnh hưởng.

### 6.2. Thời gian phát hiện

Thời gian phát hiện của một scenario:

`TTD = detected_at - incident_started_at`

Trong đó:

- `incident_started_at`: thời điểm sự cố bắt đầu.
- `detected_at`: thời điểm hệ thống xác nhận sự cố lần đầu.

Mean Time To Detect:

`MTTD = mean(TTD_i)`

Chỉ tính theo tập scenario và chính sách xử lý kết quả đã xác định.

### 6.3. Thời gian chẩn đoán

Quy ước thời gian chẩn đoán tính từ khi sự cố bắt đầu:

`TTDiag = diagnosed_at - incident_started_at`

Trong đó `diagnosed_at` là thời điểm hệ thống đưa ra kết quả chẩn đoán được bộ đánh giá chấp nhận.

Nếu cần đo thời gian xử lý riêng của RCA Agent sau khi có cảnh báo, sử dụng một đại lượng tách biệt:

`DiagnosisLatency = diagnosed_at - detected_at`

Không trộn hai định nghĩa trong cùng một phép tổng hợp.

Mean Time To Diagnose:

`MTTDiag = mean(TTDiag_i)`

### 6.4. Thời gian replay

Khi thực hiện replay tăng tốc, phải phân biệt:

- Thời gian logic của event.
- Thời gian thực của quá trình thực thi.
- Thời gian phát hiện và chẩn đoán theo đồng hồ của hệ thống.

Tất cả kết quả trong cùng một metric phải sử dụng chung quy tắc thời gian.

## 7. Expected Signals và Metric

### 7.1. Expected Signals

`expected_signals` mô tả những tín hiệu dự kiến xuất hiện sau khi lỗi được tiêm.

| Trường | Ý nghĩa |
|---|---|
| `metric` | Tên metric |
| `group` | Phạm vi nguồn hoặc label |
| `direction` | Chiều biến đổi dự kiến |
| `note` | Giải thích bổ sung |
| `catalog_status` | Trạng thái định nghĩa metric |

Các giá trị `direction`:

| Giá trị | Ý nghĩa |
|---|---|
| `up` | Tăng |
| `down` | Giảm |
| `shift` | Thay đổi phân phối hoặc giá trị |
| `violation` | Xuất hiện vi phạm |
| `non_zero` | Có giá trị khác 0 |
| `stable` | Duy trì ổn định |

Expected signal không phải bằng chứng detector đã phát hiện thành công.

### 7.2. Trạng thái metric

Hai giá trị:

- `in_catalog`: Metric được khai báo trong catalog.
- `proposed`: Metric được đề xuất, chưa được xác nhận triển khai.

Metric đã có trong catalog không tự động có nghĩa collector đã tạo được dữ liệu thực tế.

Metric chưa có dữ liệu quan sát không được đánh giá như một tín hiệu đã đo thành công.

### 7.3. Must-not-alert

`must_not_alert` xác định những cảnh báo không được xuất hiện trong phạm vi scenario.

Quy tắc:

1. Đối chiếu theo tên metric.
2. Áp dụng bộ lọc `group` nếu được khai báo.
3. Không mở rộng phạm vi cảnh báo vượt khỏi nội dung đã định nghĩa.
4. Không mặc định một metric ổn định nghĩa là mọi cảnh báo của metric đó đều sai.
5. Đối chứng có hiện tượng bất thường ngoài phạm vi chấm phải được xử lý riêng.

## 8. Quy ước kết quả RCA và đánh giá AC@k

### 8.1. Cấu trúc kết quả cần thiết cho đánh giá

Một kết quả RCA có thể được biểu diễn bằng các thông tin:

| Trường | Ý nghĩa |
|---|---|
| `scenario_id` | Scenario được đánh giá |
| `predictions` | Danh sách các nguyên nhân dự đoán |
| `rank` | Thứ hạng ứng viên |
| `asset` | Asset ID của ứng viên |
| `fault_type` | Loại lỗi dự đoán nếu có |
| `score` | Điểm xếp hạng nếu có |
| `timestamp` | Thời điểm đưa ra dự đoán |

Bộ đánh giá sử dụng danh sách ứng viên đã xếp hạng để so sánh với root-cause asset chuẩn.

### 8.2. AC@1

AC@1 đo tỷ lệ scenario có root-cause asset đúng ở vị trí dự đoán thứ nhất.

Đối với từng scenario:

- AC@1 = 1 nếu top-1 trùng root-cause asset chuẩn.
- AC@1 = 0 nếu top-1 không trùng.

Trên tập N scenario sự cố hợp lệ:

`AC@1 = (1/N) × Σ correct_top1(i)`

### 8.3. AC@3

AC@3 đo tỷ lệ scenario có root-cause asset đúng trong ba dự đoán đầu.

Đối với từng scenario:

- AC@3 = 1 nếu root-cause asset nằm trong top-3.
- AC@3 = 0 nếu không nằm trong top-3.

Trên tập N scenario sự cố hợp lệ:

`AC@3 = (1/N) × Σ correct_top3(i)`

### 8.4. Quy tắc chấm điểm

1. So sánh asset ID sau chuẩn hóa.
2. Giữ thứ hạng do RCA Agent trả về.
3. Không tự thay đổi thứ hạng dựa trên Ground Truth.
4. Một asset lặp lại không được tính thành nhiều ứng viên khác nhau.
5. Các ứng viên bằng điểm cần được sắp theo quy tắc xác định.
6. Không có dự đoán hợp lệ thì AC@k bằng 0 đối với scenario sự cố được chấm.
7. Affected asset không tự động được tính là root cause đúng.
8. Scenario đối chứng không có root cause được ghi AC@k là N/A.
9. Scenario có Ground Truth không hợp lệ phải được loại khỏi phép chấm và ghi nhận nguyên nhân.
10. Việc đúng loại lỗi và đúng root-cause asset là hai tiêu chí khác nhau.

AC@k ở đây được định nghĩa cho độ chính xác của **root-cause asset**. Nếu đánh giá đồng thời loại lỗi và asset, sử dụng tiêu chí bổ sung được định nghĩa riêng.

### 8.5. Ví dụ đánh giá

Ground Truth:

`finobs:host.collector`

Kết quả RCA:

| Rank | Asset |
|---|---|
| 1 | `finobs:raw.binance_trades` |
| 2 | `finobs:host.collector` |
| 3 | `finobs:raw.coinbase_trades` |

Kết quả:

- AC@1 = 0.
- AC@3 = 1.

## 9. Ground Truth của các scenario mẫu

### 9.1. Bảng tổng hợp

| Scenario | Incident | Fault Type | Root-cause asset |
|---|---|---|---|
| S001 – Coinbase Duplicate Burst | true | `FT03_duplicate_events` | `finobs:raw.coinbase_trades` |
| S002 – Binance Schema Drift | true | `FT07_schema_drift` | `finobs:raw.binance_trades` |
| S003 – Collector Clock Skew | true | `FT06_clock_skew` | `finobs:host.collector` |
| S100 – Control Market Burst | false | Không có Ground Truth sự cố | Không có |

### 9.2. S001 – Coinbase Duplicate Burst

- Cơ chế: nhân bản 5% trade Coinbase, giữ nguyên event ID.
- Khoảng sự cố: offset 600–720 giây.
- Root-cause asset: `finobs:raw.coinbase_trades`.
- Affected assets: raw Coinbase và bronze trades.
- Expected signal: `duplicate_ratio_1m`.
- Trạng thái metric: `proposed`.
- Must-not-alert: `trade_count_1m` đã được khử trùng theo event ID.

Nhãn chính là FT03 vì cơ chế được tiêm là nhân bản event.

### 9.3. S002 – Binance Schema Drift

- Cơ chế: đổi tên trường giá và thay đổi kiểu trade ID trong payload.
- Khoảng sự cố: offset 300–1200 giây.
- Root-cause asset: `finobs:raw.binance_trades`.
- Affected assets: raw Binance và bronze trades.
- Expected signals: `contract_violation_rate_1m`, `contract_violation_count_1m`.
- Must-not-alert: contract violation của Coinbase.

Nhãn chính là FT07 do thay đổi payload không tương thích với contract.

### 9.4. S003 – Collector Clock Skew

- Cơ chế: dịch `ingest_time` đi -500 ms.
- Khoảng sự cố: offset 600–1200 giây.
- Root-cause asset: `finobs:host.collector`.
- Affected assets: raw Binance và raw Coinbase.
- Expected signals: `ingest_delay_p95_ms`, `negative_delay_ratio_1m`.
- Must-not-alert: `trade_count_1m`.

Nhãn chính là FT06 vì timestamp được ghi nhận sai, không phải sự kiện bị trì hoãn thực tế.

### 9.5. S100 – Control Market Burst

- `is_incident: false`.
- `injection: null`.
- Không có root cause trong Ground Truth.
- Đánh giá false positive theo phạm vi volume/distribution được khai báo.

Dữ liệu nền S100 có hiện tượng trùng trade Coinbase được ghi nhận trong mô tả kịch bản. Vì vậy, S100 không được coi là bộ dữ liệu hoàn toàn không có bất thường theo mọi tiêu chí.

Không sử dụng S100 để tính false-positive rate toàn cục nếu chưa xác định rõ phạm vi cảnh báo được chấm.

## 10. Ground Truth và Runtime Evidence

Ground Truth của scenario mô tả thông tin đã xác định trước thực thi.

Runtime evidence ghi lại những gì thực tế xảy ra trong quá trình tiêm lỗi.

| Thuộc tính | Scenario Ground Truth | Runtime Evidence |
|---|---|---|
| Fault Type | Loại lỗi dự kiến | Loại lỗi được thực thi |
| Thời gian | Offset dự kiến | Thời gian thực tế |
| Asset | Asset mục tiêu và root cause | Asset thực sự bị can thiệp |
| Số event | Có thể có tham số tỷ lệ | Số event thực sự bị tác động |
| Trạng thái chạy | Chưa xác định | Thành công hoặc thất bại |

Các thông tin runtime cần ghi nhận gồm:

- Scenario ID.
- Thời gian bắt đầu và kết thúc thực thi.
- Injector được sử dụng.
- Asset được can thiệp.
- Số event được lựa chọn.
- Số event thực sự bị sửa, xóa, trì hoãn hoặc nhân bản.
- Trạng thái thực thi.
- Thông tin phục vụ tái lập.

Đối với FT12 Null Spike, tỷ lệ null dự kiến không đồng nghĩa với số bản ghi thực tế đã bị sửa. Hai giá trị phải được phân biệt.

Runtime evidence không được dùng để âm thầm thay đổi đáp án Ground Truth đã định nghĩa trước.

## 11. Kiểm tra tính hợp lệ và tính nhất quán

### 11.1. Kiểm tra tự động

Các thành phần kiểm tra:

- `evaluation/scenarios/scenario.schema.json`
- `evaluation/scenarios/validate.py`
- `evaluation/scenarios/test_validate.py`
- `tests/fixtures/scenarios_invalid/`

Các điều kiện cần kiểm tra:

1. YAML parse thành công.
2. JSON Schema hợp lệ.
3. Fault Type nằm trong FT01–FT12.
4. Fault Type ở cấp scenario khớp Ground Truth khi có incident.
5. Incident có injection object và root cause.
6. Control có `injection: null` và không có root cause.
7. Timestamp hợp lệ về định dạng và ngày lịch.
8. Khoảng thời gian không âm, không đảo ngược.
9. Khoảng injection và Ground Truth nằm trong input window.
10. Asset và metric tuân thủ quy tắc cú pháp.

### 11.2. Kiểm tra ngữ nghĩa

Ngoài kiểm tra cấu trúc, cần xác định:

- Root-cause asset tương ứng cơ chế tiêm lỗi.
- Root-cause component phản ánh thành phần gây lỗi.
- Affected assets phù hợp với phạm vi tác động.
- Expected signals có quan hệ hợp lý với fault type.
- Metric proposed không bị coi là metric đã đo được.
- Negative control có phạm vi kiểm tra cụ thể.
- Thời gian phát hiện và chẩn đoán sử dụng quy tắc nhất quán.
- So sánh RCA sử dụng định danh chuẩn.
- Ground Truth không bị đưa vào đầu vào dự đoán.

Validation thành công về cấu trúc không thay thế việc kiểm tra ngữ nghĩa hoặc đánh giá thực nghiệm.
