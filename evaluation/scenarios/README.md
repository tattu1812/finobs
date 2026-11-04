# Kịch bản đánh giá FinObs

Mỗi file `*.yaml` ở đây mô tả **một** kịch bản lỗi có **ground truth**: lỗi gì, tiêm vào đâu,
lúc nào, gốc nằm ở node nào trong đồ thị lineage, và detector/agent phải thấy được gì. Các
module dùng chung bộ kịch bản này như sau:

- **injector** (`evaluation/injector/`) đọc mục `input` và `injection` để tái tạo lỗi;
- **detector** (`observability/`) chạy trên dữ liệu đã tiêm;
- **bench** (`evaluation/bench/`) so kết quả với `ground_truth` để tính MTTD, MTTDiag,
  precision/recall và AC@k.

Mã lỗi (`fault_type`) lấy từ [bảng phân loại lỗi](../fault_taxonomy.md).

```bash
make validate-scenarios       # kiểm mọi kịch bản + chứng minh các ca lỗi cố ý bị bắt
python evaluation/scenarios/validate.py
python tests/fixtures/make_scenario_fixtures.py   # sinh lại ca lỗi cố ý sau khi đổi format
```

## Kịch bản hiện có

| ID | Lỗi | Trụ cột | Dựa trên |
|---|---|---|---|
| [`S001_coinbase_duplicate_burst`](S001_coinbase_duplicate_burst.yaml) | FT03 trùng lặp | volume | Hiện tượng thật ngày 2026-10-27 |
| [`S002_binance_schema_drift`](S002_binance_schema_drift.yaml) | FT07 đổi cấu trúc | schema | Foidl et al.: lỗi kiểu dữ liệu phổ biến nhất |
| [`S003_collector_clock_skew`](S003_collector_clock_skew.yaml) | FT06 lệch đồng hồ | freshness | Hiện tượng thật: 32 461 độ trễ âm |
| [`S100_control_market_burst`](S100_control_market_burst.yaml) | *(đối chứng)* | | Đỉnh tải thật 02:57:01 |

Đánh số theo quy ước: `S001–S099` là kịch bản có lỗi, `S100–S199` là đối chứng (không có lỗi),
`S900+` dành cho ca lỗi cố ý trong `tests/fixtures/scenarios_invalid/`.

## Định dạng

Ví dụ dưới đây là bản rút gọn của S001. Kiểm tra chính thức do
[`scenario.schema.json`](scenario.schema.json) (JSON Schema 2020-12) thực hiện.

```yaml
scenario_version: 1                      # tăng khi đổi format
id: S001_coinbase_duplicate_burst        # = tên file, dạng S<3 số>_<snake_case>
title: Coinbase gửi trùng 5% trade ...
fault_type: FT03_duplicate_events        # mã trong fault_taxonomy.md
severity: medium                         # low | medium | high | critical
description: >                           # mô tả cho người đọc: lỗi gì, vì sao chọn
  ...
tags: [observed_in_sprint1, coinbase]

input:                                   # dữ liệu nền mà lỗi được tiêm vào
  mode: replay                           # replay (dữ liệu thu thật) | synthetic
  dataset: data/raw/coinbase/2026-10-27
  sources: [coinbase]
  symbols: [BTC-USD, ETH-USD]
  window:                                # mốc 0 của mọi *_offset_s
    start: "2026-10-27T03:00:00.000Z"    # PHẢI có ngoặc kép, xem lưu ý bên dưới
    end:   "2026-10-27T03:30:00.000Z"
  seed: 1                                # để tiêm ngẫu nhiên lặp lại được

injection:                               # null với kịch bản đối chứng
  injector: duplicate_records            # tên hàm trong evaluation/injector/
  target:
    layer: source                        # source | raw | kafka | bronze | silver | gold | host
    asset: "finobs:raw.coinbase_trades"  # node lineage <namespace>:<tên>
    source: coinbase
    symbols: [BTC-USD, ETH-USD]
  start_offset_s: 600                    # giây tính từ window.start
  duration_s: 120
  params: { fraction: 0.05, keep_event_id: true }   # tham số riêng của injector

ground_truth:                            # đáp án, detector/agent KHÔNG được đọc
  is_incident: true
  fault_type: FT03_duplicate_events      # phải trùng fault_type ở trên
  start_offset_s: 600                    # khoảng thời gian sự cố được tính là đang xảy ra
  end_offset_s: 720
  root_cause:                            # đáp án cho chẩn đoán (AC@k)
    asset: "finobs:raw.coinbase_trades"
    component: platform/ingest/coinbase_ws.py (dữ liệu từ sàn)
    explanation: >
      ...
  affected_assets: ["finobs:raw.coinbase_trades", "finobs:bronze.trades"]
  expected_signals:                      # metric cần đổi (để chấm recall)
    - metric: duplicate_ratio_1m
      group: { source: coinbase }
      direction: non_zero                # up | down | shift | violation | non_zero | stable
      catalog_status: proposed           # in_catalog | proposed
  must_not_alert:                        # metric KHÔNG được báo (để chấm false positive)
    - metric: trade_count_1m

evaluation:
  detect_within_s: 120                   # quá hạn này thì tính là phát hiện trễ
  diagnose_within_s: 600
  rca_top_k: 3                           # k cho AC@k
  metrics: [MTTD, MTTDiag, precision, recall, AC@k]
```

### Ý nghĩa từng phần

| Phần | Ai đọc | Nội dung |
|---|---|---|
| `input` | injector, bench | Dữ liệu nền và khoảng thời gian. `window.start` là mốc 0 của mọi offset. |
| `injection` | injector | Cách tạo lỗi. Tách khỏi `ground_truth` vì cùng một lỗi có thể tiêm theo nhiều cách. |
| `ground_truth` | **chỉ** bench | Đáp án. Detector và agent không bao giờ được đọc mục này. |
| `evaluation` | bench | Hạn thời gian và metric dùng để chấm kịch bản. |

### Ràng buộc mà `validate.py` kiểm tra

Phần schema kiểm tra kiểu, trường bắt buộc, mã lỗi và định dạng tên asset/thời gian. Ngoài ra
còn các kiểm tra liên trường:

1. `id` trùng tên file.
2. `window.end` sau `window.start`. Cả đoạn tiêm lỗi lẫn đoạn ground truth đều nằm trong window.
3. `ground_truth.fault_type` trùng `fault_type`.
4. `is_incident: false` khi và chỉ khi `injection: null`. Kịch bản đối chứng không được có
   `root_cause` hay `expected_signals`.
5. Ground truth không bắt đầu trước thời điểm tiêm lỗi.
6. Danh sách mã `FTxx` trong `fault_taxonomy.md` khớp đúng `enum` của schema.

Có 8 ca lỗi cố ý trong `tests/fixtures/scenarios_invalid/`, mỗi ca vi phạm một ràng buộc.
`make validate-scenarios` chỉ chạy xanh khi cả 8 ca đều bị bắt.

### Lưu ý

- **Thời gian phải đặt trong ngoặc kép.** YAML tự chuyển `2026-10-27T03:00:00.000Z` không có
  ngoặc thành kiểu datetime, và schema sẽ báo sai kiểu. Định dạng giống contract raw:
  UTC, mili giây, hậu tố `Z`.
- **Tên asset lineage hiện là tạm thời** (`finobs:raw.*`, `finobs:bronze.*`, `finobs:host.*`).
  Khi Marquez chạy thật, cần thống nhất namespace/tên dataset với phần agent (Khánh) và
  Airflow/Marquez (Tú), rồi sửa lại một lần cho mọi kịch bản.
- **Metric đánh dấu `proposed`** (`duplicate_ratio_1m`, `negative_delay_ratio_1m`) chưa có
  trong `observability/metric_catalog.yaml`, nhánh `Hanh-s1`. Cần đề xuất với Hạnh để thêm vào.
- **Ground truth phải "sạch".** Không chọn window chồng lên một lỗi thật mà kịch bản không
  khai báo. Ví dụ: S001 bắt đầu từ 03:00 để tránh lần Coinbase gửi trùng thật lúc 02:57:01.
  Nếu không tránh được (như S100), phải ghi rõ trong `description` và giới hạn metric được chấm.

## Thêm kịch bản mới

1. Chép một kịch bản gần giống, đổi `id` và tên file.
2. Chọn `fault_type` trong [bảng phân loại](../fault_taxonomy.md). Nếu không loại nào phù hợp,
   thêm loại mới theo quy tắc ở cuối bảng.
3. Với mỗi loại lỗi, nên có thêm một kịch bản đối chứng (`S1xx`) cho trường hợp "trông giống
   nhưng không phải lỗi". Ví dụ: HOSE đóng cửa trông giống FT01.
4. Chạy `make validate-scenarios`.
