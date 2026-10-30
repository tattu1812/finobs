"""
Đo lệch đồng hồ máy thu so với giờ chuẩn, kiểu NTP.

    python clock_check.py                          # in kết quả
    python clock_check.py --label before --out reports/clock_check.jsonl

Vì sao cần: cột độ trễ = ingest_time (đồng hồ máy mình) - event_time (đồng
hồ sàn). Nếu máy mình lệch X ms thì mọi giá trị trễ đều lệch X ms theo.
Lệch quá ngưỡng (mặc định 250 ms) thì số đo độ trễ vô nghĩa -> exit code 2.

Hai loại tham chiếu:
  - NTP (SNTP qua UDP 123): có 4 mốc t0..t3, tính được offset chuẩn
        offset = ((t1 - t0) + (t2 - t3)) / 2
        delay  = (t3 - t0) - (t2 - t1)
  - HTTP time API của chính các sàn: chỉ có 1 mốc server, giả định nó nằm
    giữa khoảng đi-về, sai số tối đa ±RTT/2. Đây là đồng hồ mà event_time
    thực sự dựa vào, nên vẫn đáng đo dù kém chính xác hơn NTP.

Mỗi tham chiếu lấy nhiều mẫu, giữ mẫu có RTT nhỏ nhất (ít nhiễu mạng nhất).
"""

from __future__ import annotations

import argparse
import http.client
import json
import socket
import statistics
import struct
import time
from datetime import datetime
from pathlib import Path

from common import utc_now_iso

NTP_SERVERS = ("time.google.com", "time.cloudflare.com", "pool.ntp.org")
NTP_EPOCH_DELTA = 2208988800  # giây từ 1900-01-01 tới 1970-01-01

HTTP_REFS = {
    "binance": ("api.binance.com", "/api/v3/time"),
    "coinbase": ("api.coinbase.com", "/api/v3/brokerage/time"),
}


def _now_ms() -> float:
    return time.time_ns() / 1e6


def _ntp_to_ms(seconds: int, fraction: int) -> float:
    return (seconds - NTP_EPOCH_DELTA) * 1000 + fraction * 1000 / 2**32


def ntp_sample(host: str, timeout: float = 2.0) -> dict:
    # LI=0, VN=4, Mode=3 (client)
    packet = b"\x23" + 47 * b"\0"
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(timeout)
        t0 = _now_ms()
        sock.sendto(packet, (host, 123))
        data, _ = sock.recvfrom(512)
        t3 = _now_ms()
    t1 = _ntp_to_ms(*struct.unpack("!II", data[32:40]))  # receive timestamp
    t2 = _ntp_to_ms(*struct.unpack("!II", data[40:48]))  # transmit timestamp
    return {
        "offset_ms": ((t1 - t0) + (t2 - t3)) / 2,
        "rtt_ms": (t3 - t0) - (t2 - t1),
    }


def _server_ms(name: str, body: dict) -> float:
    if name == "binance":
        return float(body["serverTime"])
    if name == "coinbase":
        return float(body["epochMillis"])
    raise ValueError(name)


def http_samples(name: str, host: str, path: str, n: int) -> list[dict]:
    conn = http.client.HTTPSConnection(host, timeout=5)
    try:
        # Lượt đầu để dựng TCP + TLS, không tính
        conn.request("GET", path)
        conn.getresponse().read()
        out = []
        for _ in range(n):
            t0 = _now_ms()
            conn.request("GET", path)
            resp = conn.getresponse()
            body = json.loads(resp.read())
            t3 = _now_ms()
            server = _server_ms(name, body)
            out.append({"offset_ms": server - (t0 + t3) / 2, "rtt_ms": t3 - t0})
            time.sleep(0.2)
        return out
    finally:
        conn.close()


def best(samples: list[dict]) -> dict:
    pick = min(samples, key=lambda s: s["rtt_ms"])
    return {
        "offset_ms": round(pick["offset_ms"], 1),
        "rtt_ms": round(pick["rtt_ms"], 1),
        "uncertainty_ms": round(pick["rtt_ms"] / 2, 1),
        "samples": len(samples),
        "offset_spread_ms": round(
            max(s["offset_ms"] for s in samples) - min(s["offset_ms"] for s in samples), 1
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--samples", type=int, default=8)
    parser.add_argument("--threshold-ms", type=float, default=250.0)
    parser.add_argument("--label", default="")
    parser.add_argument("--out", default="", help="nối kết quả (1 dòng JSON) vào file này")
    args = parser.parse_args()

    refs: dict = {}
    for host in NTP_SERVERS:
        samples = []
        for _ in range(args.samples):
            try:
                samples.append(ntp_sample(host))
            except OSError:
                pass
            time.sleep(0.1)
        refs[f"ntp:{host}"] = best(samples) if samples else {"error": "no reply"}

    for name, (host, path) in HTTP_REFS.items():
        try:
            refs[f"http:{name}"] = best(http_samples(name, host, path, args.samples))
        except Exception as exc:  # noqa: BLE001 — mất một tham chiếu vẫn in phần còn lại
            refs[f"http:{name}"] = {"error": repr(exc)}

    ntp_offsets = [r["offset_ms"] for k, r in refs.items() if k.startswith("ntp:") and "offset_ms" in r]
    if not ntp_offsets:
        raise SystemExit("Không NTP server nào trả lời, không kết luận được.")
    # offset > 0 nghĩa là đồng hồ chuẩn đi TRƯỚC máy mình (máy mình chậm)
    offset = statistics.median(ntp_offsets)
    ok = abs(offset) <= args.threshold_ms

    result = {
        "ts": utc_now_iso(),
        "label": args.label,
        "host_tz": datetime.now().astimezone().tzname(),
        "ntp_offset_median_ms": round(offset, 1),
        "threshold_ms": args.threshold_ms,
        "ok": ok,
        "refs": refs,
    }

    print(f"{'tham chiếu':<28}{'offset ms':>11}{'rtt ms':>9}{'±ms':>8}{'spread':>9}")
    for key, r in refs.items():
        if "error" in r:
            print(f"{key:<28}  LỖI {r['error']}")
            continue
        print(
            f"{key:<28}{r['offset_ms']:>11.1f}{r['rtt_ms']:>9.1f}"
            f"{r['uncertainty_ms']:>8.1f}{r['offset_spread_ms']:>9.1f}"
        )
    print(f"\nOffset trung vị NTP: {offset:+.1f} ms  (ngưỡng ±{args.threshold_ms:.0f} ms)")
    print("ĐẠT" if ok else "KHÔNG ĐẠT — đồng hồ lệch quá ngưỡng, số đo độ trễ sẽ sai")

    if args.out:
        path = Path(args.out)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(result, ensure_ascii=False) + "\n")

    raise SystemExit(0 if ok else 2)


if __name__ == "__main__":
    main()
