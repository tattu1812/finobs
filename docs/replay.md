# Hướng dẫn Công cụ Phát lại Tốc độ cao (`replay`) — PL-A / PL-B

Công cụ `infra/replay/replayer.py` phát lại dữ liệu lịch sử (từ file `make record` hoặc file chính thức Binance Public Data) vào pipeline với tốc độ điều chỉnh được (`1x`, `10x`, `100x`, `max`), phục vụ:
- **EV**: dựng các ngày biến động mạnh để đo False Positive & MTTD của các detector.
- **PL / OB**: thử nghiệm scalability `10 → 100 → 500` mã (tự động nhân bản mã tổng hợp `SYNTH001USDT`, gắn cờ `transport.is_synthetic=true`).

---

## 1. Quy tắc an toàn bắt buộc

- **KHÔNG chạy trên VM đang thu dữ liệu thật** (VM Azure): thử tải sẽ tranh chấp RAM/CPU/đĩa với `producer` và `bronze-stream`. Chạy trên máy cục bộ hoặc máy benchmark tách biệt.
- **Cách ly namespace**: mặc định công cụ chạy `dry-run` với namespace `replay` (topic `replay.<source>.trade`). Nếu truyền namespace trùng hoặc chạm tới dữ liệu thật (`raw`, `bronze`, `silver`, `gold`, `live`, `prod`, `finobs`), công cụ **từ chối chạy ngay lập tức** (`ReplayNamespaceError`).

---

## 2. Cách chạy

```bash
# Dry-run trên fixtures cục bộ, tốc độ tối đa
python infra/replay/replayer.py --in tests/fixtures/raw --speed max --namespace replay

# Xuất ra file JSONL để OB/EV chấm điểm, mở rộng lên 100 mã với seed cố định
python infra/replay/replayer.py --in tests/fixtures/raw --num-symbols 100 --speed max --seed 42 --out bench/replay_100.jsonl

# Qua Makefile
make replay IN=tests/fixtures/raw SPEED=100x NAMESPACE=replay NUM_SYMBOLS=100
```

### Tạm dừng & tiếp tục (Pause / Resume)
```bash
python infra/replay/replayer.py --in tests/fixtures/raw --checkpoint bench/replay.ckpt --stop-after 20
python infra/replay/replayer.py --in tests/fixtures/raw --checkpoint bench/replay.ckpt --resume
```

---

## 3. Đọc kết quả thông lượng & tài nguyên

Output JSON gồm:
- `metrics.emitted_events` / `total_events` / `progress_pct`: tiến độ phát lại.
- `metrics.throughput_eps`: thông lượng (sự kiện/giây).
- `metrics.avg_detect_latency_ms`, `p95_detect_latency_ms`: độ trễ xử lý mỗi sự kiện qua sink.
- `metrics.cpu_seconds`, `peak_ram_mb`: tài nguyên tiêu thụ trong quá trình chạy.
- `metrics.synthetic_symbols`: số mã tổng hợp được nhân bản thêm.

---

## 4. Ước lượng sơ bộ tài nguyên (10 → 100 → 500 mã)

Giả sử trung bình mỗi mã crypto có ~5 trade/giây (~432.000 trade/ngày, ~250 byte/event JSON nén hoặc ~400 byte JSON thô):

| Quy mô | Thông lượng 1x | Thông lượng 100x | RAM ước tính (replayer + buffer) | Đĩa ước tính (1 giờ JSONL thô) |
|---|---|---|---|---|
| **10 mã** | ~50 events/s | ~5.000 events/s | ~120–200 MB | ~70 MB |
| **100 mã** | ~500 events/s | ~50.000 events/s | ~350–600 MB | ~700 MB |
| **500 mã** | ~2.500 events/s | ~250.000 events/s (giới hạn CPU 1 core) | ~1.2–2.0 GB | ~3.5 GB |

*(Số liệu ước lượng sơ bộ dựa trên kích thước envelope `raw_event.schema.json`; khi chạy full stack Spark + Kafka cần cộng thêm ~3–4 GB RAM cho JVM).*

---

## 5. Dọn dẹp namespace `replay.` sau khi thử nghiệm

Chỉ xoá các topic/file/bảng thuộc namespace `replay.` — **tuyệt đối không chạm** `raw.*`, `bronze.*`, `silver.*`, `gold.*`:

```bash
# Xoá file output replay cục bộ
rm -rf bench/replay_*.jsonl bench/replay.ckpt

# Nếu đã tạo topic Kafka replay.* trên môi trường test cục bộ:
docker exec finobs-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --delete --topic "replay\..*"
```
