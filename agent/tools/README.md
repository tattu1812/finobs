# Tool cho agent chẩn đoán FinObs

Gồm 5 tool mà agent (LLM) gọi để điều tra một sự cố dữ liệu. Mặc định tool chạy trên **dữ liệu giả**;
lineage đọc được từ **Marquez thật**, và `query_metric` đọc được từ **metric store thật** (Sprint 2).

```bash
make test-agent                                  # 5 tool, schema, lỗi, client Marquez, metric store
make lineage                                     # in đồ thị lineage (Marquez giả) kèm alert
python -m agent.tools.lineage_graph --asset finobs:raw.binance.trade --direction downstream
python -m agent.tools.lineage_graph --format mermaid     # hoặc dot, json
```

Luôn chạy bằng `python -m ...` từ gốc repo, để Python import được package `agent`.

## 5 tool

| Tool | Trả lời câu hỏi | Nguồn dữ liệu hiện tại |
|---|---|---|
| `list_alerts` | Detector đang báo gì, trong khoảng thời gian nào? | dữ liệu giả |
| `query_metric` | Metric thay đổi thế nào so với trước, các nguồn khác có thay đổi giống vậy không? | dữ liệu giả hoặc **metric store** |
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
| 03:06 | Job `bronze_trades` bắt đầu FAILED vì cast lỗi | `get_job_runs` |
| 03:08–03:09 | Bảng `bronze.trades_raw`, `gold.ohlcv_1m` cũ dần | `query_metric table_freshness_seconds`, alert A002, A003 |
| 03:12 | Coinbase BTC-USD có đỉnh tải thật (đây là **nhiễu**, không phải lỗi) | alert A004 |

**Đáp án:** gốc của sự cố là `finobs:raw.binance.trade`, tức nguồn Binance đổi API.
- Job `bronze_trades` không phải gốc: đó chỉ là nơi lỗi lộ ra.
- Coinbase cũng không phải gốc: nó nằm trên cùng nhánh upstream và có alert, nhưng alert đó
  là đỉnh tải thật của thị trường.

Agent tốt phải loại được cả hai nhánh sai này. Mọi số liệu đều sinh tất định từ `SEED`.

## Marquez

[`marquez.py`](marquez.py) gọi `GET /api/v1/lineage?nodeId=dataset:<ns>:<name>&depth=N` rồi tự
duyệt cạnh để lấy upstream/downstream. Quy ước tên khớp với `asset` trong kịch bản đánh giá:
dataset `finobs:raw.binance.trade` là node `dataset:finobs:raw.binance.trade` trong Marquez.

`FakeTransport` trả JSON **cùng định dạng** response của Marquez, nên chế độ giả và chế độ thật
đi qua cùng một đường code. Test cũng dựng một HTTP server cục bộ để chạy qua `HttpTransport`.

Để chạy với Marquez thật (xem `docs/airflow-marquez.md`):

```bash
docker compose --env-file .env -f infra/docker-compose.yml up -d marquez-postgres marquez-api
python -m agent.tools.seed_marquez --marquez-url http://localhost:5000   # nạp topology giả qua OpenLineage
make lineage MARQUEZ_URL=http://localhost:5000
```

## Metric store

[`metric_store.py`](metric_store.py) đọc bảng `metric_observations` mà service `metrics-collector`
ghi mỗi phút (`trade_count_1m`, `ingest_delay_p95_ms`, `last_live_trade_age_seconds`). Đơn vị lấy
từ `observability/metric_catalog.yaml`. Giá trị null (ví dụ `source_unavailable`) giữ nguyên là null.

```python
from agent.tools.registry import Toolbox
tb = Toolbox.with_metric_store("postgresql://finobs:<mật khẩu>@localhost:5435/finobs_metrics")
tb.call("query_metric", {"metric": "trade_count_1m", "group": {"source": "binance"},
                         "start": "2026-11-25T05:00:00.000Z", "end": "2026-11-25T06:00:00.000Z"})
```

DB không kết nối được thì tool trả `backend_unavailable`; không có dòng nào khớp thì `not_found`.
Test tích hợp (`tests/test_metric_store.py`) chỉ chạy khi có biến `FINOBS_METRIC_DSN`.

Tên dataset theo [quy ước đặt tên lineage](../../docs/conventions/lineage-naming.md): trùng tên topic
Kafka / bảng Iceberg thật (`raw.binance.trade`, `bronze.trades_raw`...).

## Backend thật khác (Sprint 3)

| Tool | Backend thật | File |
|---|---|---|
| `get_job_runs` | API runs của Marquez: mọi job (producer, Spark, DAG) đều phát OpenLineage | [`live_backends.py`](live_backends.py) `MarquezRuns` |
| `get_contract_violations` | Metric `contract_violation_*` + bảng `contract_violation_samples` do collector ghi | [`live_backends.py`](live_backends.py) `ViolationStore` |
| `get_lineage` | Marquez thật: đồ thị raw → bronze → silver → gold do job tự phát (cần facet `jobType: STREAMING`) | [`marquez.py`](marquez.py) |

```python
tb = Toolbox.live(dsn="postgresql://finobs:<mật khẩu>@localhost:5435/finobs_metrics",
                  marquez_url="http://localhost:5000")
```

`list_alerts` vẫn đọc dữ liệu giả tới khi có detector (S4–S5).

## RCA heuristic

[`observability/rca/heuristic.py`](../../observability/rca/heuristic.py) (phương pháp 1 của đặc tả 5.3) lấy alert
qua `list_alerts` và đồ thị qua `get_lineage`, rồi xếp hạng ứng viên. Đúng top-1 trên thế giới giả S002 và trên đồ
thị Marquez thật (ca giả lập alert ở `raw.coinbase.trade` lan xuống bronze, silver, gold).

## Việc tiếp theo

- Khi job Spark/Airflow phát OpenLineage thật (S3), chạy lại `make lineage MARQUEZ_URL=...` để xác
  nhận đồ thị thật khớp tên trong `fake_world.JOBS`.
- Thay dữ liệu giả của 3 tool còn lại bằng backend thật mà không đổi schema:
  - `list_alerts`: khi có detector (S3–S4).
  - `get_contract_violations`: đếm từ topic `raw.dlq` (header `finobs.errors`) hoặc metric Prometheus
    `finobs_producer_records_total{result="invalid"}`.
  - `get_job_runs`: API runs của Marquez hoặc Airflow.
- Metric `table_freshness_seconds` hiện chỉ có trong dữ liệu giả. Cần đề xuất thêm vào metric catalog.
- Viết vòng lặp agent (prompt trong `agent/prompts/`) dùng `tb.definitions()` và `tb.call()`,
  rồi chấm bằng ground truth của kịch bản.
