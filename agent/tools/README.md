# Tool cho agent chẩn đoán FinObs

Gồm 5 tool mà agent (LLM) gọi để điều tra một sự cố dữ liệu. Hiện tool chạy trên **dữ liệu giả**,
riêng lineage có thể đọc từ **Marquez thật**.

```bash
make test-agent                                  # 21 test: 5 tool, schema, lỗi, client Marquez
make lineage                                     # in đồ thị lineage (Marquez giả) kèm alert
python -m agent.tools.lineage_graph --asset finobs:raw.binance_trades --direction downstream
python -m agent.tools.lineage_graph --format mermaid     # hoặc dot, json
```

Luôn chạy bằng `python -m ...` từ gốc repo, để Python import được package `agent`.

## 5 tool

| Tool | Trả lời câu hỏi | Nguồn dữ liệu hiện tại |
|---|---|---|
| `list_alerts` | Detector đang báo gì, trong khoảng thời gian nào? | dữ liệu giả |
| `query_metric` | Metric thay đổi thế nào so với trước, các nguồn khác có thay đổi giống vậy không? | dữ liệu giả |
| `get_lineage` | Dataset này phụ thuộc vào đâu (upstream) và ảnh hưởng tới đâu (downstream)? | **Marquez** (thật hoặc giả) |
| `get_job_runs` | Job nằm giữa hai dataset có đang lỗi không, lỗi từ lúc nào? | dữ liệu giả |
| `get_contract_violations` | Nguồn nào gửi bản ghi sai contract, sai ở trường nào, mã nào? | dữ liệu giả |

Mỗi tool có định nghĩa riêng trong [`schemas/<tên>.json`](schemas/), gồm 4 phần:

- `name` và `description`: mô tả bằng tiếng Anh, vì đây là phần LLM đọc.
- `input_schema`: cùng định dạng tool definition của Claude API.
- `output_schema`: dùng trong test để đảm bảo tool luôn trả JSON đúng cấu trúc.

```python
from agent.tools.registry import Toolbox

tb = Toolbox.fake()                                       # dữ liệu giả + Marquez giả
tb = Toolbox.fake(marquez_url="http://localhost:5000")    # dữ liệu giả + Marquez thật
tools = tb.definitions()            # [{name, description, input_schema}], $ref đã được inline
result = tb.call("get_lineage", {"asset": "finobs:gold.ohlcv_1m", "direction": "upstream"})
```

`call()` luôn kiểm input theo schema và **không ném exception**. Khi lỗi, kết quả có dạng
`{"error": {"code": ..., "message": ...}}` (xem [`schemas/_error.json`](schemas/_error.json)),
để agent đọc được lỗi và tự sửa lời gọi. Có 4 mã lỗi: `unknown_tool`, `invalid_input`,
`not_found`, `backend_unavailable`.

## Dữ liệu giả

[`fake_world.py`](fake_world.py) dựng một sự cố hoàn chỉnh, lấy kịch bản
[`S002`](../../evaluation/scenarios/S002_binance_schema_drift.yaml) làm gốc rồi cho lỗi lan
xuống hạ nguồn. Window là 2026-10-27, 03:00–03:20 UTC.

| Thời điểm | Sự kiện | Thấy qua |
|---|---|---|
| 03:05 | Binance đổi schema payload của ETHUSDT | `get_contract_violations`, alert A001 |
| 03:06 | Job `bronze_load` bắt đầu FAILED vì cast lỗi | `get_job_runs` |
| 03:08–03:09 | Bảng `bronze.trades`, `gold.ohlcv_1m` cũ dần | `query_metric table_freshness_seconds`, alert A002, A003 |
| 03:12 | Coinbase BTC-USD có đỉnh tải thật (đây là **nhiễu**, không phải lỗi) | alert A004 |

**Đáp án:** gốc của sự cố là `finobs:raw.binance_trades`, tức nguồn Binance đổi API.
- Job `bronze_load` không phải gốc: đó chỉ là nơi lỗi lộ ra.
- Coinbase cũng không phải gốc: nó nằm trên cùng nhánh upstream và có alert, nhưng alert đó
  là đỉnh tải thật của thị trường.

Agent tốt phải loại được cả hai nhánh sai này. Mọi số liệu đều sinh tất định từ `SEED`.

## Marquez

[`marquez.py`](marquez.py) gọi `GET /api/v1/lineage?nodeId=dataset:<ns>:<name>&depth=N` rồi tự
duyệt cạnh để lấy upstream/downstream. Quy ước tên khớp với `asset` trong kịch bản đánh giá:
dataset `finobs:raw.binance_trades` là node `dataset:finobs:raw.binance_trades` trong Marquez.

`FakeTransport` trả JSON **cùng định dạng** response của Marquez, nên chế độ giả và chế độ thật
đi qua cùng một đường code. Test cũng dựng một HTTP server cục bộ để chạy qua `HttpTransport`.

Để chạy với Marquez thật (cần nhánh `tattu/6-airflow-marquez`, xem `docs/airflow-marquez.md`):

```bash
docker compose --env-file .env -f infra/docker-compose.yml up -d marquez-postgres marquez-api
python -m agent.tools.seed_marquez --marquez-url http://localhost:5000   # nạp topology giả qua OpenLineage
make lineage MARQUEZ_URL=http://localhost:5000
```

## Việc tiếp theo

- **Kiểm tra với Marquez thật.** Phần này chưa chạy được, vì compose có Marquez chưa được merge
  vào `main`. Định dạng response dựa trên tài liệu API của Marquez, nên cần xác nhận lại khi chạy thật.
- Khi Airflow phát sự kiện OpenLineage thật, đổi `fake_world.JOBS` theo tên DAG/task thật, rồi
  thống nhất tên dataset với `evaluation/scenarios/*.yaml`.
- Thay dữ liệu giả của 4 tool còn lại bằng backend thật mà không đổi schema:
  - `query_metric`, `list_alerts`: metric store của Hạnh (`observability/store`).
  - `get_contract_violations`: kết quả của `platform/contracts/validate.py`.
  - `get_job_runs`: API runs của Marquez hoặc Airflow.
- Metric `table_freshness_seconds` hiện chỉ có trong dữ liệu giả. Cần đề xuất thêm vào metric catalog.
- Viết vòng lặp agent (prompt trong `agent/prompts/`) dùng `tb.definitions()` và `tb.call()`,
  rồi chấm bằng ground truth của kịch bản.
