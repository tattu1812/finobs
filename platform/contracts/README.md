# Data contracts: dữ liệu raw

Contract cho tầng raw: các file NDJSON nén gzip do `platform/ingest/*` ghi vào
`data/raw/<source>/<YYYY-MM-DD>/<HH>.jsonl.gz`. Mỗi dòng là một trade.

| File | Phạm vi |
|---|---|
| `raw_event.schema.json` | Vỏ (envelope) chung. Mọi nguồn đều phải tuân thủ. |
| `binance_trade.schema.json` | Bản ghi `source = "binance"`: vỏ chung (`allOf`) cộng thêm ràng buộc cho payload stream `<symbol>@trade`. |
| `coinbase_trade.schema.json` | Bản ghi `source = "coinbase"`: vỏ chung cộng thêm ràng buộc cho payload kênh `market_trades`. |
| `validate.py` | Kiểm tra mọi bản ghi theo schema của nguồn tương ứng, và chạy thêm 3 kiểm tra chéo trường. |

Chuẩn dùng: **JSON Schema draft 2020-12**. Các schema tham chiếu nhau qua `$id`
(`https://finobs.example/contracts/...`). Domain này chỉ dùng làm định danh,
không cần phải truy cập được. `validate.py` nạp tất cả schema vào một registry nội bộ.

```bash
make validate-contracts                                   # data/raw nếu có, không thì fixtures
python platform/contracts/validate.py --in data/raw
python platform/contracts/validate.py --in tests/fixtures/invalid --expect-invalid
```

## Các schema được viết dựa trên dữ liệu thật

Mọi ràng buộc đều rút ra từ **toàn bộ** dữ liệu thu ngày 2026-10-27, từ 02:48 đến 03:48 UTC:
203 732 bản ghi Binance và 28 127 bản ghi Coinbase, kiểm đủ từng bản ghi chứ không lấy mẫu.

| Điều quan sát được | Binance | Coinbase |
|---|---|---|
| Mọi trường của vỏ có mặt | 100% | 100% |
| Trường trong payload | `e E s t p q T m M`, cả 9 trường đều có ở 100% bản ghi | `trade_id product_id price size side time`, cả 6 trường đều có ở 100% |
| Số chữ số thập phân của giá | luôn đúng 8 | 0–6 (sàn cắt bỏ số 0 ở cuối) |
| Số chữ số thập phân của khối lượng | luôn đúng 8 | 0–8 |
| Kiểu của trade id | integer | **chuỗi** số |
| Giờ gốc trong payload | epoch ms (integer) | ISO 8601, 3–6 chữ số lẻ giây, hậu tố `Z` |
| Số trade trong một message | luôn 1 | 1–100 (tối đa 100) |
| `payload.id` == `sequence`, `payload.symbol` == `symbol_native` | 100% | 100% |

Kết quả validate trên dữ liệu đó: **231 859/231 859 hợp lệ**.

## Ba quyết định bắt buộc

### 1. Giá và khối lượng để kiểu string, không dùng number

**Quy tắc:** `payload.p` và `payload.q` (Binance), `payload.price` và `payload.size` (Coinbase)
đều là chuỗi số thập phân `^(0|[1-9][0-9]*)(\.[0-9]+)?$`. Không cho phép số mũ, số âm,
hay số 0 thừa ở đầu.

**Lý do:**
- **Số float nhị phân không biểu diễn chính xác được số thập phân.** Ví dụ `0.1 + 0.2 = 0.30000000000000004`.
  Tiền, phí, và việc đối soát giữa hai nguồn đều cần so sánh bằng chính xác tuyệt đối.
  Chỉ cần sai ở chữ số thứ 16 là phép so sánh "khớp / không khớp" cho kết quả sai.
- **Độ chính xác đã chạm sát giới hạn của float64.** Một giá trị `q` của Binance trong dữ liệu là
  `934737.00000000`, tức 14 chữ số có nghĩa. Float64 chỉ đảm bảo khoảng 15–17 chữ số.
  Coinbase có `size` nhỏ tới `0.00000001`.
- **Bản thân các sàn gửi giá dạng chuỗi** (`"p":"79707.83000000"`, `"price":"79707.83"`).
  Nếu parse sang number thì đã là *biến đổi dữ liệu*, trái với nguyên tắc giữ payload nguyên văn.
  Phép biến đổi này còn làm mất thông tin: `"1.10000000"` và `"1.1"` trở thành cùng một số,
  nên không còn biết được độ chính xác mà sàn công bố.
- **Bộ parse JSON mặc định (Python `json`, JavaScript `JSON.parse`) đọc mọi number thành float64.**
  Để dạng chuỗi thì lỗi này không thể xảy ra một cách âm thầm.
- **Tầng sau** sẽ chuyển sang `Decimal` (Python) hoặc `DECIMAL(38,18)` (Spark/SQL), không bao giờ dùng `double`.

Ngoài ra, Binance bị ràng buộc **đúng 8 chữ số thập phân** (khớp 100% mẫu). Nếu sàn đổi độ
chính xác, đó là thay đổi contract phía sàn và cần bị phát hiện (xem ca lỗi `binance_price_2_decimals`).

### 2. Thời gian: UTC, ISO 8601, chính xác mili giây, luôn có hậu tố `Z`

**Quy tắc:** `event_time` và `ingest_time` có dạng `YYYY-MM-DDTHH:MM:SS.mmmZ`, đúng 3 chữ số
mili giây, chỉ dùng `Z`, không dùng offset kiểu `+07:00`.

**Lý do:**
- **Ba nguồn dùng ba định dạng khác nhau.** Binance dùng epoch ms (integer). Coinbase dùng chuỗi ISO
  có 3–6 chữ số lẻ giây, độ dài thay đổi. VCI dùng epoch *giây*, lại viết dạng chuỗi. Nếu không
  thống nhất, mỗi phép so sánh event time / processing time sẽ phải tự xử lý ba kiểu.
- **UTC và `Z` để không bao giờ nhầm múi giờ.** Máy thu chạy WSL ở UTC, máy Windows bên ngoài ở UTC+7,
  sàn HOSE tính giờ VN. Offset kiểu `+07:00` hợp lệ theo ISO 8601 nhưng bị cấm ở đây:
  với một định dạng duy nhất, so sánh chuỗi cũng chính là so sánh thời gian.
- **Chọn mili giây vì đây là mức chính xác cao nhất mà mọi nguồn đều có** (Binance chỉ có ms),
  **và vì đồng hồ máy thu không chính xác hơn mức đó.** Lệch đồng hồ đo được trong phiên thu
  dao động từ +16 đến +42 ms (`reports/clock_check.jsonl`). Ghi tới micro giây sẽ tạo cảm giác
  chính xác hơn thực tế.
- **Đúng 3 chữ số, không cắt số 0 ở cuối:** chuỗi có độ dài cố định thì sắp xếp theo thứ tự
  chữ cái cũng chính là sắp xếp theo thời gian.
- Micro giây của Coinbase được **cắt bớt** (không làm tròn) xuống mili giây trong `event_time`.
  Giá trị gốc vẫn còn nguyên trong `payload.time`, nên không mất thông tin.

### 3. Mã chuẩn hoá về `BASE-QUOTE`, giữ mã gốc ở `symbol_native`

**Quy tắc:** `symbol_canonical` khớp `^[A-Z0-9]+-[A-Z0-9]+$`, ví dụ `BTC-USDT`, `BTC-USD`, `FPT-VND`.
`symbol_native` là mã đúng như sàn viết: `BTCUSDT`, `BTC-USD`, `FPT`.

**Lý do:**
- **Mỗi sàn viết mã một kiểu:** Binance viết liền (`BTCUSDT`), Coinbase có gạch ngang (`BTC-USD`),
  VCI chỉ ghi mã cổ phiếu (`FPT`). Một dạng chung giúp tầng sau group và join theo mã mà không cần
  biết dữ liệu đến từ sàn nào.
- **Có dấu `-` thì tách base/quote không bị mơ hồ.** Với `BTCUSDT`, phải có sẵn danh sách quote
  mới tách được (`common._QUOTE_ASSETS`), và cách này chỉ là heuristic.
- **Phải giữ `symbol_native`** vì: (a) gọi lại API của sàn hay đối chiếu với tài liệu sàn đều cần mã gốc;
  (b) phép chuẩn hoá có thể sai hoặc thay đổi theo thời gian, nên còn mã gốc thì luôn chạy lại được;
  (c) `event_id` được ghép từ mã gốc.
- **Lưu ý:** `BTC-USDT` và `BTC-USD` là **hai cặp khác nhau**. Chuẩn hoá chỉ thống nhất *cách viết*,
  không khẳng định USDT tương đương USD. Khi so sánh chéo hai sàn, phải tự khai báo rõ ánh xạ quote.

## Các quyết định khác

| Quyết định | Lý do |
|---|---|
| **Chặt chẽ:** `additionalProperties: false` cho vỏ, `transport` và payload | finobs là đồ án *data observability*. Khi sàn thêm hoặc bớt trường (Binance từng bỏ `b`/`a` khỏi stream trade), ta muốn **biết ngay** chứ không âm thầm bỏ qua. Nhận thêm trường mới là một thay đổi contract có chủ đích, cần tăng MINOR. |
| Tên trường trong vỏ dùng `snake_case`; tên trường trong payload **giữ nguyên tên của sàn** (`E`, `T`, `trade_id`…) | Payload là dữ liệu nguyên văn. Đổi tên tức là biến đổi dữ liệu. |
| `event_id = <source>:<symbol_native>:<sequence>` | Khoá khử trùng toàn cục, đọc được bằng mắt. Coinbase thật sự có gửi trùng (219 bản ghi trong phiên thu), nên cần một khoá ổn định. |
| `sequence` là integer (kể cả khi Coinbase gửi `trade_id` dạng chuỗi) | Để so sánh và tìm khoảng hở theo số. Trade id lớn nhất quan sát được là khoảng 1,1×10⁹, nhỏ hơn rất nhiều so với 2⁵³. |
| `transport.is_snapshot` **bắt buộc** với Coinbase | Mỗi lần subscribe, Coinbase gửi 100 trade cũ **cho mỗi mã**. Nếu không có cờ này, các số đo độ trễ và throughput sẽ sai. |
| `transport.batch_size` ≤ 100 với Coinbase, = 1 với Binance | Đúng như quan sát: Coinbase giới hạn 100 trade mỗi message. Lô lớn hơn nghĩa là hành vi của sàn đã thay đổi. |
| `payload.time` của Coinbase cho phép 0–6 chữ số lẻ giây | Quan sát được 3–6. Vì sàn cắt số 0 ở cuối nên 1–2 chữ số, hoặc không có phần lẻ, cũng hợp lệ về mặt logic. |
| `source` là `enum` | Thêm nguồn mới là một quyết định có chủ đích (tăng MINOR), không phải lỗi gõ nhầm. |

Ba **kiểm tra chéo trường** trong `validate.py` (JSON Schema 2020-12 không so sánh được hai trường với nhau):
`event_id` khớp với các thành phần của nó; id trong payload bằng `sequence`; mã trong payload bằng `symbol_native`.

## Contract KHÔNG đảm bảo những gì

Nói rõ để không hiểu nhầm "100% hợp lệ" là dữ liệu "sạch":
- **Không khử trùng.** Một trade có thể xuất hiện nhiều lần (snapshot, phát lại, các lô chồng nhau).
  Phải khử trùng theo `event_id`.
- **Không phân biệt được trade cũ bị gửi lại không có cờ.** Giữa phiên, Coinbase gửi các message `update`
  chứa trade từ nhiều giờ trước, và những message này không có cờ snapshot.
  Xem `docs/sprint1/report.md`.
- **Không đảm bảo đồng hồ đúng.** `ingest_time` theo đồng hồ máy thu, độ trễ có thể âm.

## Versioning

Phiên bản contract được ghi trong **mỗi bản ghi**: `schema_version` (SemVer). Recorder lấy giá trị này từ
`platform/ingest/common.py::SCHEMA_VERSION`. Phiên bản hiện tại: **1.0.0**.

| Loại thay đổi | Ví dụ | Tăng |
|---|---|---|
| Làm cho dữ liệu đang hợp lệ trở thành không hợp lệ: bỏ trường, đổi tên, đổi kiểu, bắt buộc thêm trường, siết pattern | `price` chuyển sang number; bỏ `symbol_native` | **MAJOR** |
| Nới lỏng, thêm vào: thêm trường không bắt buộc, thêm nguồn hoặc giá trị enum, nới pattern | thêm `transport.foo`; thêm `source: "kraken"` | **MINOR** |
| Chỉ sửa mô tả, tài liệu | sửa `description` | **PATCH** |

Quy tắc:
1. Sửa schema và `SCHEMA_VERSION` **trong cùng một PR**. CI sẽ chạy `validate-contracts`.
2. `raw_event.schema.json` chỉ nhận một major (`^1\.`). Khi lên major mới, giữ schema cũ để còn đọc được
   dữ liệu cũ, và `validate.py` phải chọn schema theo major của `schema_version`. Hiện chỉ có major 1,
   nên `validate.py` chưa làm việc này.
3. **Không bao giờ ghi đè dữ liệu raw cũ** để "nâng cấp" lên version mới.
4. Fixture trong `tests/fixtures/` được sinh lại bằng `python tests/fixtures/make_fixtures.py --in data/raw`.
   Mỗi ràng buộc mới nên có thêm một ca trong `invalid/`.

| Phiên bản | Ngày | Thay đổi |
|---|---|---|
| 1.0.0 | 2026-10-27 | Bản đầu tiên: vỏ, Binance trade, Coinbase trade. `transport` gồm `is_snapshot`, `poll_interval_s`, `rows_returned`. |

## Việc còn mở

- **Chưa có `vnstock_trade.schema.json`.** Bản ghi vnstock hiện chỉ được kiểm phần vỏ. Mới có 2 phút
  dữ liệu thử (1274 bản ghi), chưa đủ để rút ra ràng buộc cho payload.
