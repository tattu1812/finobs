# Tóm tắt Sprint 3: hoàn thiện và khóa hạ tầng

**Ngày:** 25/11/2026 · **Mục tiêu (Đề tài, Bảng 8):** *khóa hạ tầng, bắt đầu chạy liên tục* ·
**Kiểm chứng trên:** Docker Desktop, laptop 8 GB (dữ liệu live 25/11 + dữ liệu Sprint 1)

Quyết định Q1–Q8 của kế hoạch được chốt theo đề xuất, cộng 5 góp ý của nhóm trưởng: thêm sàn thứ ba trước ngày
thu, khóa bronze, tách hai ý trong DoD gold, trigger bronze 1 phút, thêm metric pipeline health.

## Đã làm được

| Vai | Task | Kết quả |
|---|---|---|
| DE | DE-11 sàn thứ ba | Adapter **Kraken** (WS v2), contract 1.1.0 (`kraken_trade.schema.json`), 4 fixture lỗi mới |
| DE | DE-6 silver | `silver.trades`: DECIMAL(38,18), phía taker, `symbol_std`, khử trùng bằng MERGE, bỏ bản ghi trễ > 10 phút |
| DE | DE-7 gold | `gold.ohlcv_1m` (OHLC, volume, VWAP) và `gold.cross_exchange_spread` (3 sàn), tính lại đúng các phút bị ảnh hưởng |
| DE | DE-8 lineage | `platform/lineage.py`: producer, bronze, silver, gold, DAG đều phát OpenLineage theo quy ước tên |
| DE | DE-9 vnstock | Poll VCI 24/7 trong producer (ngủ ngoài phiên HOSE), topic `raw.vnstock.trade`, ≤ 60 request/phút |
| DE | DE-10 backfill | `make load-official DATE=` / DAG; dùng cho mọi ngày đã thu |
| PL | PL-6 đồng hồ | Service `timesync` (ntpd, quyền `SYS_TIME`) cho Docker Desktop; tìm ra nguyên nhân gốc là giờ Windows (phát hiện 3) |
| PL | PL-7 profile | Lõi (16 service) / `ops` / `timesync`; `mem_limit` từng service; `make up-core` |
| PL | PL-8 bảo trì | DAG `iceberg_maintenance`: optimize, expire_snapshots 7 ngày, remove_orphan_files qua Trino |
| PL | PL-9 alert | Alertmanager → Telegram (đặt `TELEGRAM_*`); 6 alert pipeline mới, tổng 12 |
| OB | OB-4 5 trụ cột | + schema (`contract_violation_*`), distribution (`trade_size_p95_5m`, `return_1m_bps`, `cross_source_spread_bps`), lineage (`table_freshness_seconds`) |
| OB | Pipeline health | Kafka lag, thời gian micro-batch, job đứng, gold cũ, DAG lỗi (Prometheus) |
| OB | OB-5 lịch giao dịch | `trading_freshness_seconds`: tính giây **giao dịch**, nghỉ trưa / cuối tuần / lễ không tính |
| OB | OB-6 | Metric cấp bảng đọc Iceberg qua Trino |
| AI | AI-3 | `get_lineage` + RCA chạy trên Marquez thật |
| AI | AI-4 | RCA heuristic theo lineage (`observability/rca/heuristic.py`) |
| AI | AI-5 | `get_job_runs` (Marquez runs), `get_contract_violations` (metric store + mẫu vi phạm) |
| EV | EV-4 | Injector v1: 5 loại lỗi FT02, FT03, FT06, FT07, FT09; S004, S005 mới |
| EV | EV-5 | Đáp án vào `bench.scenarios` (thời điểm tiêm thật) |
| — | Khóa bronze | [Quy định](../conventions/bronze-freeze.md) + bước CI *bronze-freeze* chặn PR thiếu nhãn `bronze-change-approved` |

## Đối chiếu tiêu chí khóa hạ tầng

| # | Tiêu chí | Kết quả | |
|---|---|---|---|
| 1 | Pipeline chạy 24 giờ không can thiệp trên máy 24/7 | Chạy ổn định nhiều giờ trên laptop; **chưa có máy 24/7 (Q2)** | 🟡 |
| 2 | Lệch đồng hồ < 100 ms suốt 24 giờ | `timesync` đưa về +0,3 ms nhưng bị kéo lại ~2 s: **đồng hồ Windows chậm 1 949 ms** (xem phát hiện 3) | ❌ tới khi đồng bộ giờ Windows |
| 3 | Marquez có lineage thật đủ job, đúng tên | 6 dataset, 4 job, 13 cạnh + DAG `binance_official_daily` | ✅ |
| 4 | Đủ 5 trụ cột; freshness VN theo lịch | 10 metric chạy thật; lịch HOSE có test | ✅ |
| 5a | Gold tính đúng (đếm lại từ silver) | **0 / 2 072 phút lệch** trên 993 912 trade | ✅ |
| 5b | Stream đủ so với file chính thức | Cần file D+1 của một ngày đã thu đủ (`make completeness-check`), chỉ số C2 của S4 | ⏳ |
| 6 | Injector 5 loại lỗi; RCA top-1 đúng S001–S003 | 5/5 loại, tất định; RCA đúng top-1 trên S002 (giả) và đồ thị Marquez thật | ✅ |
| 7 | Đủ ≥ 9 ngày dữ liệu tới 06/12 | Phụ thuộc Q2 | ⏳ |

Kiểm chứng thêm theo góp ý: **rebuild từ đầu bronze** (`--mode rebuild`, 199 giây cho 1,6 triệu dòng) cho silver,
gold, spread **giống hệt** chế độ stream (cùng số dòng, cùng tổng khối lượng, cùng tổng giá trị).

## Năm phát hiện quan trọng

1. **Hai "ONE" khác nhau.** `cross_source_spread_bps` của ONE/USD = 19 704 bps: ONE của Kraken giá ~0,12 USD, ONE
   (Harmony) của Binance ~0,0009 USD, tức là hai tài sản trùng ký hiệu (đúng loại lỗi FT10). Đã thay bằng CELR/USD
   (lệch 0,16% so với Binance) **trước khi khóa danh sách mã**; BAND, CTSI kiểm tra khớp.
2. **Catalog SQLite bị khóa khi nhiều job commit đồng thời** (`SQLITE_BUSY` → commit lỗi 500, cả rebuild và bảo trì
   hỏng). Đã bật `journal_mode=WAL` + `busy_timeout=30000`; sau đó bảo trì chạy song song với stream bình thường.
   Nếu tải tăng tiếp, chuyển catalog sang PostgreSQL.
3. **Đồng hồ Windows chậm 1 949 ms, và Hyper-V kéo máy ảo Docker theo nó.** `timesync` (ntpd) chỉnh máy ảo về
   +0,3 ms, nhưng vài chục giây sau Hyper-V lại đồng bộ máy ảo theo giờ Windows, nên ntpd phải chỉnh +2 s lặp đi lặp lại.
   Dịch vụ thời gian của Windows (`w32time`) đang không chạy. Đây là cơ chế của phát hiện 1 Sprint 1. **Cách sửa (quyền
   Admin, trên máy chạy 24/7):**
   ```powershell
   net start w32time
   w32tm /config /syncfromflags:manual /manualpeerlist:"time.google.com time.cloudflare.com" /update
   w32tm /resync
   ```
   Sau đó `make timesync` chỉ còn phải bù phần trôi nhỏ của máy ảo. Trên máy chủ Linux thì dùng chrony của host, không cần `timesync`.
4. **Marquez không dựng lineage cho job streaming** nếu thiếu facet `jobType.processingType = STREAMING` (chỉ dựng
   khi run COMPLETE). Đã thêm facet; đồ thị đủ 13 cạnh.
5. **Ba lỗi chỉ lộ khi chạy thật**, đã sửa: Iceberg làm nguồn streaming cần `ResolvingFileIO`; temp view trong
   `foreachBatch` phải dùng session của micro-batch; Python trong image Spark là 3.8 (không có `str.removeprefix`).

## Số liệu

| | Binance | Coinbase | Kraken |
|---|---:|---:|---:|
| Silver ngày 25/11 (sau rebuild) | 1 605 427 dòng cả 3 sàn | | |
| Bỏ vì trễ > 10 phút | 0 | 2 651 (snapshot gửi lại khi kết nối lại) | 0 |
| Spread BTC/USD (bps, có chênh USDT/USD) | khoảng 15 | | |

| Thành phần | Số đo |
|---|---|
| Micro-batch bronze / silver-gold | ~9 s / ~35 s (trigger 60 s) |
| Kafka lag của bronze | 0 offset |
| Bảo trì Iceberg | bronze 909 → 26 file; silver 6 → 3 file |
| RAM phần lõi | ~5,2 GB; thêm `ops` ~1,3–2 GB |
| Unit test | 84 (venv) + 5 Spark (image) + 11 scenario |

## Các thành viên khác cần biết

- **Cập nhật `.env`**: `PRODUCER_SOURCES=binance,coinbase,kraken,vnstock`, `BRONZE_TRIGGER_SECONDS=60`, thêm
  `SILVER_TRIGGER_SECONDS`, `ALERTMANAGER_PORT`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`.
- **Bronze bị khóa** khi bắt đầu thu: đọc [bronze-freeze.md](../conventions/bronze-freeze.md) trước khi sửa producer,
  adapter, contract, job bronze hay `config/symbols.json`.
- **Silver/gold sửa thoải mái**, sau đó `make reprocess` để tính lại từ bronze.
- Mã so liên sàn dùng `symbol_std` (BTC/USD); `symbol_canonical` vẫn phân biệt USDT/USD.
- Lượt chạy job streaming bị kill đột ngột còn nằm ở trạng thái RUNNING trong Marquez (job không kịp gửi
  COMPLETE/FAIL); `get_job_runs` sẽ thấy nhiều lượt RUNNING. Cần dọn khi phân tích.

## Việc cần làm ngay

1. **Chốt Q2 (máy 24/7, ≥ 16 GB RAM)**, bật đồng bộ giờ của hệ điều hành (phát hiện 3), rồi `make up` (+ `make timesync`
   nếu là Docker Desktop) và bắt đầu thu 14 ngày. Laptop 8 GB không chạy đủ `ops` cùng phần lõi.
2. Đặt `TELEGRAM_*` để nhận cảnh báo; đối chiếu `config/trading_calendar.yaml` với lịch nghỉ HOSE 2026.
3. Sau ngày thu đầu tiên: `make load-official` + `make completeness-check` (tiêu chí 5b).

## Ngoài phạm vi S3 (sang S4)

Khoảng hở trade ID và đối soát D+1 (C2), profiling KLL/HLL, SLO burn rate, gộp sự cố, injector v2 (≥ 8 loại) và
harness đo MTTD, dashboard pipeline health trên Grafana, catalog Iceberg trên PostgreSQL nếu cần.
