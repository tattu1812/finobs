# FinObs-Bench – Hướng dẫn kịch bản đánh giá (Scenario YAML)

## 1. Mục đích

Thư mục `evaluation/scenarios/` chứa các kịch bản **tiêm lỗi có kiểm soát** và **đối chứng** để đánh giá phát hiện bất thường và chẩn đoán nguyên nhân trong FinObs. Mỗi file `.yaml` mô tả dữ liệu đầu vào, thao tác tiêm lỗi (nếu có), đáp án chuẩn (*Ground Truth*) và tiêu chí đánh giá.

- **Fault Taxonomy:** [`../fault_taxonomy.md`](../fault_taxonomy.md) — định nghĩa FT01–FT12 và nguyên tắc lựa chọn loại lỗi chính.
- **JSON Schema:** [`scenario.schema.json`](scenario.schema.json) — định dạng và những ràng buộc cấu trúc của scenario.
- **Ground Truth:** [`ground_truth_conventions.md`](ground_truth_conventions.md) — quy tắc xác lập đáp án, đối chứng, thời gian, metric và AC@k.
- **Validator:** `validate.py` — kiểm tra schema và các điều kiện liên trường.

Một file **PASS validation** mới chứng minh cấu trúc và một số điều kiện ngữ nghĩa hợp lệ; **không** chứng minh Injector, Detector, RCA Agent hoặc benchmark đã chạy thành công. Đường dẫn dataset trong YAML chỉ là tham chiếu; JSON Schema không kiểm tra dataset có thực sự tồn tại.

## 2. Danh sách kịch bản mẫu

| Scenario | Vai trò | Fault Type | Cơ chế / phạm vi | Ground Truth chính |
|---|---|---|---|---|
| [`S001_coinbase_duplicate_burst.yaml`](S001_coinbase_duplicate_burst.yaml) | Sự cố | `FT03_duplicate_events` | Nhân bản 5% trade Coinbase, giữ event ID, trong 120 giây | `finobs:raw.coinbase_trades` |
| [`S002_binance_schema_drift.yaml`](S002_binance_schema_drift.yaml) | Sự cố | `FT07_schema_drift` | Đổi trường `p` thành `price` và kiểu trường `t` trên ETHUSDT, trong 900 giây | `finobs:raw.binance_trades` |
| [`S003_collector_clock_skew.yaml`](S003_collector_clock_skew.yaml) | Sự cố | `FT06_clock_skew` | Dịch `ingest_time` sớm 500 ms trên cả hai nguồn, trong 600 giây | `finobs:host.collector` |
| [`S100_control_market_burst.yaml`](S100_control_market_burst.yaml) | Đối chứng | `FT09_value_anomaly` ở cấp scenario (nhãn kiểm tra báo nhầm) | Phát lại dữ liệu không chủ động tiêm lỗi | `is_incident: false`; không có root cause |

Các kịch bản lỗi mô tả **nguyên nhân được chủ động giả lập**, không khẳng định nguyên nhân thực tế của các hiện tượng từng quan sát. S100 chỉ đánh giá báo nhầm trong phạm vi **volume/distribution** được khai báo; dữ liệu nền có một số trade Coinbase trùng nên không dùng S100 để kết luận false-positive rate cho mọi loại lỗi.

## 3. Bố cục một file YAML

| Nhóm trường | Nội dung | Vai trò khi thực thi |
|---|---|---|
| `scenario_version`, `id`, `title`, `description` | Phiên bản, mã duy nhất, tên, mô tả | Nhận diện và quản lý scenario |
| `fault_type`, `severity`, `tags` | Loại lỗi chính; mức độ, nhãn tùy chọn | Phân loại và lọc kịch bản |
| `input` | `mode`, `dataset` (khi replay), `sources`, `symbols`, `window`, `seed` | Chọn dữ liệu và xác định mốc thời gian |
| `injection` | `injector`, `target`, `start_offset_s`, `duration_s`, `params`; hoặc `null` | Mô tả cách gây lỗi |
| `ground_truth` | `is_incident`, loại lỗi, thời gian, root cause, affected assets, expected signals, must-not-alert | Đáp án để bộ đánh giá đối chiếu |
| `evaluation` | `metrics`, thời hạn phát hiện/chẩn đoán, `rca_top_k` | Xác định phạm vi và tiêu chí chấm |

Các trường bắt buộc **ở cấp cao nhất**: `scenario_version`, `id`, `title`, `fault_type`, `description`, `input`, `injection`, `ground_truth`, `evaluation`. `severity` và `tags` là tùy chọn. Chi tiết kiểu dữ liệu, enum và các trường con xem [`scenario.schema.json`](scenario.schema.json).

### 3.1. Quy tắc nhận diện

- `scenario_version: 1` là phiên bản cấu trúc đang sử dụng.
- `id` phải khớp **tên file không có phần mở rộng** và tuân theo dạng `S` + ít nhất 3 chữ số + `_` + tên chữ thường/số/dấu gạch dưới, ví dụ `S004_null_spike_demo`.
- `fault_type` ở cấp scenario phải thuộc đúng 12 mã FT01–FT12 trong taxonomy; trường này cũng có mặt ở **đối chứng** để chỉ loại lỗi được thử nghiệm báo nhầm.
- Theo quy ước tổ chức bộ mẫu: `S001–S099` dành cho lỗi, `S100–S199` dành cho đối chứng, `S900+` dùng cho fixture cố ý sai. Đây là **quy ước đặt tên**, không phải điều kiện phân loại do schema tự suy ra; số ID có thể được mở rộng khi bộ bench lớn hơn.

### 3.2. Dữ liệu đầu vào (`input`)

- `mode: replay` dùng dữ liệu tham chiếu, **bắt buộc** có `dataset` khác rỗng; `mode: synthetic` dùng dữ liệu tạo ra.
- `sources` là mảng không rỗng, không lặp, chọn từ `binance`, `coinbase`, `vnstock`.
- `symbols` và `seed` là tùy chọn; `seed` nguyên không âm giúp tái lập thao tác ngẫu nhiên.
- `window.start`, `window.end` là chuỗi **UTC có mili giây và hậu tố `Z`**; phải đặt trong dấu nháy kép trong YAML, ví dụ `"2026-10-27T03:00:00.000Z"`. `window.end` phải sau `window.start`.
- Mọi `*_offset_s` đều lấy `input.window.start` làm mốc 0; không trộn lẫn thời gian replay, event-time và wall-clock khi tính kết quả.

### 3.3. Tiêm lỗi (`injection`)

Với scenario sự cố, `injection` là object chứa `injector`, `target.layer`, `target.asset`, `start_offset_s`, `duration_s` và `params` tùy chọn. `start_offset_s >= 0`, `duration_s > 0`; đoạn tiêm lỗi phải nằm trong `input.window`. `target.layer` hỗ trợ `source`, `raw`, `kafka`, `bronze`, `silver`, `gold`, `host`.

`params` phụ thuộc vào cơ chế tiêm cụ thể (ví dụ `fraction`, `keep_event_id`, `rename_fields`, `offset_ms`). JSON Schema không chứng minh hàm injector có tồn tại hay các tham số này được backend thực thi. Đối chứng bắt buộc có `injection: null`.

### 3.4. Đáp án chuẩn (`ground_truth`)

Với `is_incident: true`, cần có `fault_type`, `start_offset_s`, `end_offset_s`, `root_cause` (`asset`, `component`, `explanation`), `affected_assets` và `expected_signals` không rỗng. `ground_truth.fault_type` phải khớp `fault_type` cấp scenario; thời gian sự cố phải hợp lệ và không được bắt đầu trước injection.

Với `is_incident: false`, không khai báo `fault_type` **bên trong `ground_truth`**, khoảng thời gian sự cố, `root_cause`, `affected_assets` hoặc `expected_signals`. Có thể có `must_not_alert` mô tả chính xác phạm vi cảnh báo cần tránh.

- `root_cause.asset`: nguyên nhân gốc cần chấm RCA; không mặc nhiên giống mọi `affected_assets`.
- `expected_signals`: tín hiệu kỳ vọng, gồm `metric`, `direction` và các trường tùy chọn như `group`, `note`, `catalog_status`.
- `catalog_status: proposed` là metric mới đề xuất; `in_catalog` là metric được khai báo trong catalog, **không bảo đảm đã thu được dữ liệu thực tế**.
- `must_not_alert`: chỉ được dùng để chấm báo nhầm đúng phạm vi metric/nhóm đã khai báo.
- Đáp án chuẩn chỉ được bộ đánh giá truy cập; không đưa `ground_truth` vào đầu vào của detector hoặc RCA Agent.

### 3.5. Tiêu chí đánh giá (`evaluation`)

`metrics` là mảng bắt buộc, chứa ít nhất một giá trị thuộc: `MTTD`, `MTTDiag`, `precision`, `recall`, `F1`, `AC@k`, `false_positive_count`.

`detect_within_s`, `diagnose_within_s` (số dương) và `rca_top_k` (số nguyên từ 1) là các trường tùy chọn. `rca_top_k` quy định độ sâu danh sách nguyên nhân để đánh giá, **không phải** danh sách kết quả do Agent tạo ra. Công thức, chính sách đánh giá và phân biệt runtime evidence xem [`ground_truth_conventions.md`](ground_truth_conventions.md).

## 4. Ví dụ scenario hợp lệ

Ví dụ mô tả cùng cơ chế như S001 và đủ các trường bắt buộc. Nếu lưu để kiểm tra, tên file phải là `S001_coinbase_duplicate_burst.yaml`; sử dụng thư mục riêng để không ghi đè scenario mẫu.

```yaml
scenario_version: 1
id: S001_coinbase_duplicate_burst
title: Mô phỏng trùng trade Coinbase
fault_type: FT03_duplicate_events
description: "Mô phỏng nhân bản trade trên luồng đầu vào Coinbase."
input:
  mode: replay
  dataset: data/raw/coinbase/2026-10-27
  sources: [coinbase]
  window:
    start: "2026-10-27T03:00:00.000Z"
    end: "2026-10-27T03:30:00.000Z"
injection:
  injector: duplicate_records
  target:
    layer: source
    asset: "finobs:raw.coinbase_trades"
  start_offset_s: 600
  duration_s: 120
ground_truth:
  is_incident: true
  fault_type: FT03_duplicate_events
  start_offset_s: 600
  end_offset_s: 720
  root_cause:
    asset: "finobs:raw.coinbase_trades"
    component: "điểm tiêm duplicate_records"
    explanation: "Scenario chủ động nhân bản trade trên luồng Coinbase."
  affected_assets: ["finobs:raw.coinbase_trades"]
  expected_signals:
    - metric: duplicate_ratio_1m
      direction: non_zero
      catalog_status: proposed
evaluation:
  metrics: [MTTD, AC@k]
```


## 5. Kiểm tra scenario từ thư mục gốc repository

Yêu cầu Python có thư viện `PyYAML` và `jsonschema`. Các lệnh dưới đây chỉ kiểm tra file, không thực hiện tiêm lỗi, không khởi động dịch vụ và không thay đổi Git.

```bash
# Kiểm tra tất cả YAML trong evaluation/scenarios/.
python evaluation/scenarios/validate.py

# Tám fixture S901–S908 phải bị từ chối (lệnh thành công khi bắt đủ lỗi).
python evaluation/scenarios/validate.py --in tests/fixtures/scenarios_invalid --expect-invalid

# Kiểm thử đơn vị cho validator nếu đã có file test_validate.py.
python -m unittest discover -s evaluation/scenarios -p 'test_validate.py' -v
```

Ý nghĩa kết quả:

| Trường hợp | Kết quả mong đợi |
|---|---|
| YAML hợp lệ | Thông báo `OK`, kết thúc với exit code 0 |
| YAML sai schema/semantic | Thông báo lỗi có file và nội dung, exit code khác 0 |
| `--expect-invalid` với S901–S908 | Tất cả ca sai bị phát hiện, exit code 0 |
| Fixture sai lọt validation | `LỌT`, exit code khác 0 |

Validator kiểm tra: ID trùng tên file, 12 mã FT nhất quán với taxonomy, cấu trúc incident/control, thứ tự và phạm vi thời gian, nhãn loại lỗi ở hai cấp, timestamp UTC hợp lệ (bao gồm ngày lịch thực tế trong bản validator Sprint 2), và ràng buộc schema. Validator **không** xác minh dữ liệu replay đã có, asset trong Marquez tồn tại, metric thực sự chạy, hay Ground Truth được Agent cách ly trong runtime.

## 6. Thêm một scenario mới

1. Chọn một cơ chế lỗi duy nhất từ [`fault_taxonomy.md`](../fault_taxonomy.md); xác định `fault_type` theo thao tác chủ động tiêm, không dựa vào triệu chứng.
2. Sao chép YAML mẫu gần nhất; đổi tên file và `id` trùng nhau, cập nhật `title`, `description` và phạm vi nguồn/symbol.
3. Xác định `input.mode`, dataset (nếu replay), window UTC có nháy kép và `seed` khi dùng ngẫu nhiên.
4. Khai báo `injection` tương ứng, đảm bảo offset/duration nằm trong window. Với đối chứng, đặt `injection: null`.
5. Khai báo `ground_truth` trước thực nghiệm: đúng mã lỗi, thời gian, root cause, affected assets, expected signals; với đối chứng chỉ giữ `is_incident: false` và phạm vi `must_not_alert` phù hợp.
6. Chọn `evaluation.metrics`, các hạn thời gian và `rca_top_k` nếu áp dụng.
7. Chạy validator và unit test. Kiểm tra thủ công nguyên nhân gốc, tính hợp lý của dữ liệu nền, metric và phạm vi đối chứng; không sửa YAML gốc chỉ để làm test PASS.

Không tự nhận một tên `injector` mới hoặc một metric `proposed` là đã được hỗ trợ tại runtime. Khi cần tạo fixture lỗi mới, sử dụng thư mục `tests/fixtures/scenarios_invalid/` và chế độ `--expect-invalid`; không đưa fixture cố ý sai vào `evaluation/scenarios/`.

## 7. Giới hạn của bộ kịch bản Sprint 2

- Ba YAML S001–S003 và đối chứng S100 là **đặc tả kịch bản**; kiểm thử schema/semantic không tương đương với chạy injector end-to-end.
- Đường dẫn `data/raw/...` tham chiếu dữ liệu replay, có thể không được đóng gói trong repository.
- Các ID asset như `finobs:raw.*`, `finobs:bronze.*` và `finobs:host.*` là định danh trong đặc tả; schema chỉ kiểm tra cú pháp, không xác nhận chúng tương đương với node runtime cụ thể.
- Metric `proposed` chưa được mặc nhiên tính vào metric đã triển khai; hướng thay đổi của tín hiệu là dự kiến thiết kế.
- S100 chỉ kiểm tra false positive theo phạm vi đã khai báo, không phải đối chứng sạch cho toàn bộ FT01–FT12.
- Số bản ghi thực sự bị sửa, thời điểm thực thi, trạng thái injector và kết quả Detector/RCA là **runtime evidence**; không được suy diễn từ việc scenario được validate thành công.
