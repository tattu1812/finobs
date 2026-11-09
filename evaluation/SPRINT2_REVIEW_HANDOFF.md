# Sprint 2 – Evaluation Review & Integration Handoff

Task 1–5 đã được chuẩn bị trên nền Sprint 1: taxonomy mở rộng FT01–FT12, schema và Ground Truth rõ hơn, bốn scenario được rà soát, validator và hướng dẫn được bổ sung. Lần kiểm tra hiện tại đạt 4/4 scenario, 8/8 fixture sai và 7/7 unit test. **Schema mới vẫn ghi `scenario_version: 1` nhưng từ chối một số dữ liệu v1 cũ**; tài liệu này đề nghị nhóm trưởng chọn chính sách tương thích trước khi merge.

**Quyết định cần chốt:** D01 chọn A/B/C; D02 chọn version hỗ trợ FT12; D03 chọn có/không migration guide; D04 chọn có/không cross-team review; D05 phê duyệt hoặc yêu cầu sửa sau khi xử lý. Tất cả đều PENDING (xem mục 7).

| Nội dung | Trạng thái | Cần nhóm trưởng quyết định? |
|---|---|---|
| Fault Taxonomy FT01–FT12 | 12 mã nhất quán với enum; PASS kiểm tra cấu trúc | YES: phạm vi FT12 |
| Scenario Schema | Draft 2020-12 và tham chiếu nội bộ PASS | YES: chính sách version |
| Ground Truth | Đã tài liệu hóa; định danh runtime cần phối hợp | YES: tích hợp liên nhóm |
| YAML Scenarios | PASS 4/4 theo schema và semantic checks | NO cho cấu trúc; cần review ngữ nghĩa |
| Validator và Tests | PASS 8/8 fixture sai, 7/7 unit test | NO cho kết quả hiện tại |
| Schema Compatibility | REVIEW REQUIRED | YES |
| Task 6 – Introduction | Ngoài phạm vi | NO |

## 2 – Danh sách thay đổi so với main

Nền so sánh: commit `6ae20345bb1aa3419d5d1e94c8ec76a90c3f48f7`. Các đường dẫn tương đối tính từ thư mục `evaluation/`.

| File | Sprint 1 trên main | Sprint 2 | Loại thay đổi | Mục đích |
|---|---|---|---|---|
| [fault_taxonomy.md](fault_taxonomy.md) | FT01–FT11 | FT01–FT12 | MODIFIED | Định nghĩa Null Spike và ranh giới loại lỗi |
| [scenario.schema.json](scenarios/scenario.schema.json) | Schema v1 | Thêm FT12 và ràng buộc | MODIFIED, COMPATIBILITY RISK | Kiểm tra đầu vào và Ground Truth chặt hơn |
| [ground_truth_conventions.md](scenarios/ground_truth_conventions.md) | Chưa có | Quy ước Ground Truth | NEW | Thống nhất nhãn, thời gian và phạm vi chấm |
| [scenarios/README.md](scenarios/README.md) | Hướng dẫn ban đầu | Hướng dẫn cập nhật | MODIFIED | Giải thích scenario, lệnh kiểm tra và giới hạn |
| [validate.py](scenarios/validate.py) | Ngày không tồn tại có thể gây exception | Báo lỗi theo trường | MODIFIED | Kiểm tra ngày thực tế không gây traceback ngoài ý muốn |
| [test_validate.py](scenarios/test_validate.py) | Chưa có | Unit test validator | NEW | Kiểm tra regression, FT12 và timestamp |
| [S001](scenarios/S001_coinbase_duplicate_burst.yaml) | Duplicate Coinbase | Làm rõ nguyên nhân được tiêm | MODIFIED | Không quy nguyên nhân thực tế cho sàn |
| [S002](scenarios/S002_binance_schema_drift.yaml) | Schema drift Binance | Làm rõ payload giả lập | MODIFIED | Không khẳng định sàn đổi API |
| [S003](scenarios/S003_collector_clock_skew.yaml) | Clock skew | Làm rõ thao tác mô phỏng | MODIFIED | Phân biệt giả lập với nguyên nhân thực tế |
| [S100](scenarios/S100_control_market_burst.yaml) | Đối chứng đỉnh tải | Giới hạn phạm vi chấm | MODIFIED | Tránh xem window có duplicate là đối chứng sạch toàn cục |
| [S901–S908](../tests/fixtures/scenarios_invalid/) | Tám fixture sai | Giữ nguyên | UNCHANGED | Tái sử dụng regression |
| [SPRINT2_REVIEW_HANDOFF.md](SPRINT2_REVIEW_HANDOFF.md) | Chưa có | Tài liệu này | NEW | Tổng hợp nội dung và quyết định cần review |

## 3 – Những nâng cấp của Sprint 2

| STT | Hạng mục | Sprint 1 | Sprint 2 | Lợi ích | Ảnh hưởng tương thích |
|---:|---|---|---|---|---|
| 1 | Taxonomy | 11 mã | 12 mã | Bổ sung phân loại completeness | Consumer cũ không hiểu FT12 |
| 2 | FT12 Null Spike | Chưa có mã riêng | Tiêm null khi giữ trường | Tách mất giá trị khỏi đổi cấu trúc | Là mở rộng enum |
| 3 | Schema | Các ràng buộc v1 ban đầu | Siết một số điều kiện | Ngăn một số scenario thiếu thông tin | Có breaking cases tại mục 4 |
| 4 | Ground Truth Conventions | Nằm chủ yếu trong YAML/README | Có tài liệu riêng | Review cách gán nhãn và chấm | Asset/runtime cần thống nhất |
| 5 | Intent và runtime evidence | Chưa phân biệt rõ trong tài liệu riêng | Nêu riêng dự kiến và kết quả thực thi | Tránh suy diễn kết quả Injector từ YAML | Không thêm trường runtime bắt buộc |
| 6 | Timestamp | Ngày không tồn tại có thể gây exception | Validator kiểm tra ngày thực | Báo lỗi rõ mà không traceback ngoài ý muốn | Ngày sai không được coi là đầu vào hợp lệ |
| 7 | Incident/control | Semantic check xử lý mâu thuẫn injection | Thêm điều kiện vào schema | Báo lỗi sớm hơn | Không thay đổi kết quả validator đầy đủ đối với hai ca này |
| 8 | S001–S003 | Một số mô tả có thể hiểu là kết luận về sự cố thực | Mô tả cơ chế tiêm và giới hạn suy luận | Không quy lỗi chưa chứng minh cho sàn | Nội dung mô tả/Ground Truth được điều chỉnh |
| 9 | S100 | Đối chứng có lưu ý duplicate trong window | Giới hạn phạm vi false positive | Không coi window là đối chứng sạch toàn cục | Bench phải tôn trọng phạm vi chấm |
| 10 | Tests | Fixture và validator | Thêm unit test | Kiểm tra timestamp, FT12 và regression | Fixture S901–S908 giữ nguyên |
| 11 | README | Hướng dẫn ban đầu | Lệnh Python và giới hạn cụ thể | Tái kiểm chứng không cần chạy pipeline | Không khẳng định runtime đã hoạt động |

## 4 – So sánh schema v1 và Sprint 2

Bảng dưới so sánh **toàn bộ schema và semantic checks** trên main với bản Sprint 2. Mỗi ca được dựng trong bộ nhớ từ scenario mẫu: v1 chấp nhận và Sprint 2 từ chối. Bốn scenario hiện có vẫn hợp lệ ở cả hai bản.

| STT | Trường/quy tắc | V1 trên main | Sprint 2 | Phân loại |
|---:|---|---|---|---|
| 1 | Replay không có `dataset` | Chấp nhận | Từ chối | BREAKING |
| 2 | `dataset` là chuỗi rỗng | Chấp nhận | Từ chối | BREAKING |
| 3 | `input.symbols: []` | Chấp nhận | Từ chối | BREAKING |
| 4 | `input.symbols` chứa chuỗi rỗng | Chấp nhận | Từ chối | BREAKING |
| 5 | `injection.target.symbols: []` | Chấp nhận | Từ chối | BREAKING |
| 6 | Target symbols chứa chuỗi rỗng | Chấp nhận | Từ chối | BREAKING |
| 7 | Target symbols bị trùng | Chấp nhận | Từ chối | BREAKING |
| 8 | `tags` chứa chuỗi rỗng | Chấp nhận | Từ chối | BREAKING |
| 9 | `ground_truth.affected_assets: []` | Chấp nhận | Từ chối | BREAKING |
| 10 | `ground_truth.expected_signals: []` | Chấp nhận | Từ chối | BREAKING |
| 11 | `root_cause.component` rỗng | Chấp nhận | Từ chối | BREAKING |
| 12 | Tên metric trong `must_not_alert` không khớp pattern | Chấp nhận string | Từ chối | BREAKING |
| 13 | Control có `ground_truth.fault_type` | Chấp nhận | Từ chối | BREAKING |
| 14 | Control có `ground_truth.start_offset_s` | Chấp nhận | Từ chối | BREAKING |
| 15 | Control có `ground_truth.end_offset_s` | Chấp nhận | Từ chối | BREAKING |
| 16 | Control có `ground_truth.affected_assets` | Chấp nhận | Từ chối | BREAKING |

| Quy tắc khác | V1 trên main | Sprint 2 | Phân loại |
|---|---|---|---|
| Incident có `injection: null` | Semantic check từ chối | Schema từ chối sớm | NON-BREAKING đối với validator đầy đủ |
| Control có injection object | Semantic check từ chối | Schema từ chối sớm | NON-BREAKING đối với validator đầy đủ |
| Ngày không tồn tại | Có thể phát sinh `ValueError` | Trả lỗi validation theo trường | VALIDATION IMPROVEMENT |
| FT12 | Không có trong enum | Được hỗ trợ | EXTENSION; consumer cũ có thể từ chối |
| ID có bốn chữ số trở lên | Pattern không chấp nhận | Pattern cho phép từ ba chữ số | EXTENSION |

`scenario_version` là phiên bản hợp đồng khai báo trong YAML. `$schema` là phiên bản ngôn ngữ JSON Schema; cả hai bản đều dùng Draft 2020-12. Hiện cùng một `scenario_version: 1` có thể được kiểm theo hai tập ràng buộc khác nhau.

## 5 – Xung đột và rủi ro khi merge

| Vấn đề | Nguyên nhân | Tác động | Mức độ | Cần quyết định |
|---|---|---|---|---|
| R1 – Schema Compatibility | Vẫn là scenario v1 nhưng có 16 ca chặt hơn | Dữ liệu v1 hợp lệ cũ có thể bị từ chối | Cao | Chính sách A/B/C |
| R2 – FT12 Compatibility | Enum mới có FT12 | Consumer/validator cũ có thể từ chối FT12 | Trung bình | Version hỗ trợ FT12 |
| R3 – Scenario Migration | Version mới có thể yêu cầu chuyển dữ liệu | Cần sửa scenario, validator và test nhất quán | Trung bình | Phạm vi và hướng dẫn migration |
| R4 – Cross-team Integration | Consumer schema khác chưa được xác nhận | Có thể lệch hợp đồng giữa các thành phần | Cao | Review liên nhóm |
| R5 – Runtime Dependencies | Asset, metric và pipeline thực chưa được xác minh bởi schema | PASS validation không chứng minh chạy end-to-end | Trung bình | Ai xác nhận asset và metric |

Không có Git merge conflict được quan sát trong worktree chuẩn bị. R1–R4 là rủi ro **hợp đồng dữ liệu**; R5 là phụ thuộc tích hợp chưa được xác minh.

## 6 – Phương án giải quyết

| Tiêu chí | A – Chấp nhận schema hiện tại | B – Giữ tương thích v1 | C – Phát hành scenario v2 |
|---|---|---|---|
| Tương thích dữ liệu v1 | Có thể từ chối dữ liệu cũ | Giữ dữ liệu v1 hợp lệ | Giữ bằng schema/validator v1 |
| Giữ validation mới | Có | Có thể đưa vào strict mode | Có trong v2 |
| Chi phí điều chỉnh | Thấp trước mắt; cần thông báo | Cần tách quy tắc/strict mode | Cần version dispatch, test và migration |
| Tác động consumer | Consumer phải chấp nhận breaking changes | Consumer v1 ít bị ảnh hưởng | Consumer chọn version; cần cập nhật để dùng v2 |
| Khả năng mở rộng | Có rủi ro cùng version mang hai nghĩa | Cần duy trì hai chế độ | Hợp đồng mỗi version rõ ràng |

- **A:** Giữ `scenario_version: 1`, chấp nhận breaking changes, thông báo và thống nhất với các consumer.
- **B:** Khôi phục khả năng chấp nhận dữ liệu v1; các kiểm tra mới có thể là strict mode, sau khi kiểm tra tác động tới test S2.
- **C:** Giữ hợp đồng v1 và phát hành hợp đồng chặt hơn dưới version mới; validator chọn theo version, có migration guide và regression cho cả hai.

**Đề xuất kỹ thuật:** ưu tiên C nếu nhóm muốn đồng thời duy trì hợp đồng v1 và các ràng buộc nghiêm ngặt của Sprint 2. Đây là đề xuất, chưa phải quyết định được phê duyệt.

## 7 – Các quyết định cần nhóm trưởng xác nhận

| ID | Câu hỏi cần quyết định | Các lựa chọn | Trạng thái |
|---|---|---|---|
| D01 | Chọn chính sách schema compatibility nào? | A/B/C | PENDING |
| D02 | FT12 được hỗ trợ trong version nào? | V1/V2 | PENDING |
| D03 | Có cần migration guide không? | YES/NO | PENDING |
| D04 | Có cần cross-team review với consumer schema không? | YES/NO | PENDING |
| D05 | Có thể merge PR sau khi giải quyết các vấn đề trên không? | APPROVE/REQUEST CHANGES | PENDING |

## 8 – Kết quả kiểm thử

Kết quả chạy lại trên nội dung bàn giao; compatibility được giữ ở trạng thái cần review.

| Kiểm thử | Kỳ vọng | Kết quả thực tế | Trạng thái |
|---|---|---|---|
| Taxonomy FT01–FT12 | 12 mã, 12 hàng, 9 cột, khớp enum | 12/12; khớp | PASS |
| JSON Schema Draft 2020-12 | Schema hợp lệ | Kiểm tra meta-schema thành công | PASS |
| Schema reference resolution | Các `$ref` nội bộ phân giải | 4 tham chiếu phân giải | PASS |
| Scenario S001–S100 | 4 scenario hợp lệ | 4/4 | PASS |
| Invalid fixtures S901–S908 | 8 fixture bị từ chối | 8/8 | PASS |
| Unit tests | Tất cả thành công | 7/7 | PASS |
| Timestamp | Ngày sai bị từ chối, ngày nhuận đúng được chấp nhận | Unit test thành công, không traceback ngoài ý muốn | PASS |
| Incident/control | Cấu trúc không nhất quán bị từ chối | Unit test thành công | PASS |
| README | Liên kết và ví dụ hợp lệ; lệnh chạy được | 10 liên kết nội bộ và 1 ví dụ hợp lệ; 3 lệnh dưới đây chạy thành công | PASS |
| Git diff check | Không lỗi whitespace | `git diff --check` thành công; file mới được kiểm tra whitespace riêng | PASS |
| Schema compatibility | Nhóm thống nhất chính sách | 16 trường hợp v1 chấp nhận nhưng S2 từ chối | REVIEW REQUIRED |

Tái kiểm chứng từ gốc repository, với Python có `PyYAML` và `jsonschema`:

```bash
python -B evaluation/scenarios/validate.py
python -B evaluation/scenarios/validate.py --in tests/fixtures/scenarios_invalid --expect-invalid
python -B -m unittest discover -s evaluation/scenarios -p 'test_validate.py' -v
git diff --check
```

## 9 – Hướng dẫn review và merge

1. Review các file Task 1–5 tại mục 2.
2. Đọc 16 trường hợp tương thích tại mục 4.
3. Chọn phương án A, B hoặc C và ghi quyết định D01–D05.
4. Yêu cầu chỉnh sửa khi cần; chạy lại validation sau khi thống nhất.
5. Chỉ merge khi nhóm trưởng phê duyệt.

HANDOFF_STATUS: READY_FOR_REVIEW
SCHEMA_COMPATIBILITY: REVIEW_REQUIRED
MERGE_APPROVAL: PENDING
TASK6_INCLUDED: NO
