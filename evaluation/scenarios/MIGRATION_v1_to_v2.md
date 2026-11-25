# Chuyển scenario từ version 1 sang version 2

Quyết định Sprint 2 (xem [SPRINT2_REVIEW_HANDOFF.md](../SPRINT2_REVIEW_HANDOFF.md), mục 7): **phương án C**.
Hợp đồng version 1 được giữ nguyên và vẫn được kiểm; mọi ràng buộc chặt hơn của Sprint 2 nằm trong version 2.

| `scenario_version` | Schema | Trạng thái |
|---|---|---|
| 1 | [`scenario.v1.schema.json`](scenario.v1.schema.json) | Đóng băng, đúng như `main` tại `6ae2034`. Không sửa. FT01–FT11. |
| 2 | [`scenario.schema.json`](scenario.schema.json) | Hiện hành. Chặt hơn ở 16 điểm, có thêm FT12. |

`validate.py` đọc `scenario_version` của từng file rồi chọn schema tương ứng. Phiên bản khác 1 và 2 bị từ chối
với thông báo "không được hỗ trợ". Kịch bản mới **phải** dùng version 2.

## Các bước chuyển một file

1. Đổi `scenario_version: 1` thành `scenario_version: 2`.
2. Chạy `python evaluation/scenarios/validate.py --in <thư mục>` và sửa theo bảng dưới cho tới khi đạt.
3. Đổi asset sang tên thật theo [quy ước đặt tên lineage](../../docs/conventions/lineage-naming.md)
   (ví dụ `finobs:raw.binance_trades` → `finobs:raw.binance.trade`). Version 2 không bắt buộc bước này về mặt
   schema, nhưng RCA chỉ chấm đúng khi asset trùng tên node trong Marquez.

## 16 điểm version 2 chặt hơn

Mỗi dòng đều có test trong [`test_validate.py`](test_validate.py) (`VersionPolicyTest.test_sixteen_breaking_cases`):
version 1 chấp nhận, version 2 từ chối.

| # | Version 1 chấp nhận | Sửa thế nào khi lên version 2 |
|---:|---|---|
| 1 | `input.mode: replay` nhưng không có `dataset` | Thêm `input.dataset` trỏ tới dữ liệu phát lại |
| 2 | `input.dataset: ""` | Ghi đường dẫn thật, hoặc đổi `mode` |
| 3 | `input.symbols: []` | Bỏ hẳn trường (= mọi mã) hoặc liệt kê mã |
| 4 | `input.symbols` có chuỗi rỗng | Xóa phần tử rỗng |
| 5 | `injection.target.symbols: []` | Bỏ trường hoặc liệt kê mã bị tiêm |
| 6 | `injection.target.symbols` có chuỗi rỗng | Xóa phần tử rỗng |
| 7 | `injection.target.symbols` bị trùng | Mỗi mã một lần |
| 8 | `tags` có chuỗi rỗng | Xóa phần tử rỗng |
| 9 | `ground_truth.affected_assets: []` | Liệt kê ít nhất asset gốc |
| 10 | `ground_truth.expected_signals: []` | Khai báo ít nhất một tín hiệu kỳ vọng |
| 11 | `root_cause.component: ""` | Mô tả thành phần gây lỗi |
| 12 | Tên metric trong `must_not_alert` sai mẫu `^[a-z][a-z0-9_]*$` | Dùng đúng tên metric trong catalog |
| 13–16 | Kịch bản đối chứng (`is_incident: false`) có `fault_type`, `start_offset_s`, `end_offset_s` hoặc `affected_assets` trong `ground_truth` | Xóa các trường đó: đối chứng không có sự cố để định vị |

Ngoài ra version 2 có thêm `FT12_null_spike`; dùng FT12 trong file version 1 sẽ bị từ chối.

## Ai cần làm gì

| Bên dùng scenario | Việc cần làm |
|---|---|
| EV (injector, FinObs-Bench) | Đọc `scenario_version`; chỉ chạy được kịch bản FT12 nếu hỗ trợ version 2 |
| AI (agent, RCA) | Asset trong `ground_truth.root_cause.asset` dùng tên thật, khớp node trong Marquez |
| OB (detector) | Tên metric trong `expected_signals` / `must_not_alert` phải có trong `observability/metric_catalog.yaml` hoặc ghi `catalog_status: proposed` |

Bốn kịch bản version 1 gốc được giữ trong [`tests/fixtures/scenarios_v1/`](../../tests/fixtures/scenarios_v1/)
làm regression: `make validate-scenarios` kiểm chúng vẫn hợp lệ với version 1.
