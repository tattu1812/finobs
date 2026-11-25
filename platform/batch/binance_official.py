"""
Nạp "bản chính thức" D+1 của Binance Public Data (FR-05, NFR-07).

    python platform/batch/binance_official.py --date 2026-11-24
    python platform/batch/binance_official.py --date 2026-11-24 --symbols BTCUSDT,ETHUSDT --dry-run

Với mỗi mã: tải <SYMBOL>-trades-<ngày>.zip và file .CHECKSUM đi kèm từ
data.binance.vision, kiểm SHA-256, **chỉ khi khớp** mới đọc CSV và ghi vào
iceberg bronze.binance_official_trades. Sai checksum -> ChecksumMismatch, không ghi gì.

Chạy lại cùng ngày + mã là an toàn: dữ liệu cũ của đúng (file_date, symbol) bị thay
thế (overwrite theo partition), không sinh bản trùng.

Vì sao dùng pyiceberg thay cho Spark batch: đây là bước "tải - kiểm - nạp" không
có phép biến đổi nặng, chạy ngay trong task Airflow mà không cần dựng Spark trong
image Airflow. Phép biến đổi nặng (silver/gold, đối soát C2) vẫn dùng Spark.

Định dạng CSV (tài liệu binance-public-data): id, price, qty, quoteQty, time,
isBuyerMaker, isBestMatch. Từ 2025-01-01 cột time của spot tính bằng micro giây
(trước đó là mili giây); hàm read_trades tự nhận biết theo độ lớn.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

BASE_URL = "https://data.binance.vision/data/spot/daily/trades"
TABLE = "bronze.binance_official_trades"
CSV_COLUMNS = ["trade_id", "price", "qty", "quote_qty", "time", "is_buyer_maker", "is_best_match"]
MICROS_THRESHOLD = 10**14  # epoch ms hiện nay ~1.7e12, epoch µs ~1.7e15
BLOCK_SIZE = 16 << 20       # 16 MB CSV mỗi khối, khoảng 150-200 nghìn trade
COMMIT_RETRIES = 5          # Iceberg dùng optimistic concurrency: commit đồng thời thì thử lại


class ChecksumMismatch(Exception):
    pass


# --------------------------------------------------------------------------
# Tải và kiểm checksum
# --------------------------------------------------------------------------

def file_name(symbol: str, day: date) -> str:
    return f"{symbol}-trades-{day.isoformat()}.zip"


def file_url(symbol: str, day: date) -> str:
    return f"{BASE_URL}/{symbol}/{file_name(symbol, day)}"


def download(url: str, dest: Path, timeout: float = 120.0) -> Path:
    request = urllib.request.Request(url, headers={"User-Agent": "finobs-batch/1.0"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response, open(dest, "wb") as out:
            while chunk := response.read(1 << 20):
                out.write(chunk)
    except urllib.error.HTTPError as exc:
        raise FileNotFoundError(f"{url}: HTTP {exc.code} (file chưa được công bố hoặc mã không tồn tại)") from exc
    return dest


def expected_sha256(checksum_text: str, name: str) -> str:
    """File .CHECKSUM có dạng '<hex>  <tên file>'."""
    for line in checksum_text.splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[1].lstrip("*") == name:
            return parts[0].lower()
        if len(parts) == 1 and len(parts[0]) == 64:
            return parts[0].lower()
    raise ValueError(f"Không tìm thấy checksum cho {name} trong: {checksum_text[:120]!r}")


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def verify(zip_path: Path, checksum_path: Path) -> str:
    expected = expected_sha256(checksum_path.read_text(encoding="utf-8"), zip_path.name)
    actual = sha256_of(zip_path)
    if actual != expected:
        raise ChecksumMismatch(f"{zip_path.name}: SHA-256 {actual} khác {expected} trong .CHECKSUM")
    return actual


# --------------------------------------------------------------------------
# Đọc CSV
# --------------------------------------------------------------------------

def arrow_schema():
    import pyarrow as pa

    money = pa.decimal128(38, 18)
    return pa.schema([
        pa.field("trade_id", pa.int64(), nullable=False),
        pa.field("price", money, nullable=False),
        pa.field("qty", money, nullable=False),
        pa.field("quote_qty", money, nullable=False),
        pa.field("trade_ts", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("is_buyer_maker", pa.bool_(), nullable=False),
        pa.field("is_best_match", pa.bool_()),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("file_date", pa.date32(), nullable=False),
        pa.field("checksum", pa.string(), nullable=False),
        pa.field("loaded_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ])


def iter_trade_batches(zip_path: Path, symbol: str, day: date, checksum: str,
                       block_size: int = BLOCK_SIZE):
    """Đọc CSV trong zip theo từng khối (không giải nén cả file vào RAM: file BTCUSDT
    một ngày lên tới hàng trăm MB), mỗi khối -> pyarrow.Table đúng arrow_schema()."""
    import pyarrow as pa
    import pyarrow.compute as pc
    from pyarrow import csv

    with zipfile.ZipFile(zip_path) as archive:
        inner = [n for n in archive.namelist() if n.endswith(".csv")]
        if len(inner) != 1:
            raise ValueError(f"{zip_path.name}: cần đúng 1 file CSV, thấy {inner}")
        with archive.open(inner[0]) as probe:
            has_header = not probe.read(1).isdigit()

        money = pa.decimal128(38, 18)
        loaded_at = datetime.now(timezone.utc)
        scale = None  # 1000 nếu file tính bằng mili giây; quyết định theo khối đầu tiên
        with archive.open(inner[0]) as stream:
            reader = csv.open_csv(
                stream,
                read_options=csv.ReadOptions(column_names=CSV_COLUMNS, skip_rows=1 if has_header else 0,
                                             block_size=block_size),
                convert_options=csv.ConvertOptions(column_types={
                    "trade_id": pa.int64(), "price": pa.string(), "qty": pa.string(),
                    "quote_qty": pa.string(), "time": pa.int64(),
                    "is_buyer_maker": pa.bool_(), "is_best_match": pa.bool_(),
                }),
            )
            for batch in reader:
                n = batch.num_rows
                if n == 0:
                    continue
                times = batch.column("time")
                if scale is None:
                    scale = 1000 if pc.max(times).as_py() < MICROS_THRESHOLD else 1
                if scale != 1:
                    times = pc.multiply(times, scale)  # mili giây -> micro giây
                yield pa.table({
                    "trade_id": batch.column("trade_id"),
                    "price": pc.cast(batch.column("price"), money),
                    "qty": pc.cast(batch.column("qty"), money),
                    "quote_qty": pc.cast(batch.column("quote_qty"), money),
                    "trade_ts": pc.cast(times, pa.timestamp("us", tz="UTC")),
                    "is_buyer_maker": batch.column("is_buyer_maker"),
                    "is_best_match": batch.column("is_best_match"),
                    "symbol": pa.array([symbol] * n, pa.string()),
                    "file_date": pa.array([day] * n, pa.date32()),
                    "checksum": pa.array([checksum] * n, pa.string()),
                    "loaded_at": pa.array([loaded_at] * n, pa.timestamp("us", tz="UTC")),
                }, schema=arrow_schema())


def read_trades(zip_path: Path, symbol: str, day: date, checksum: str, block_size: int = BLOCK_SIZE):
    """Cả file thành một bảng; chỉ dùng cho file nhỏ và test."""
    import pyarrow as pa

    batches = list(iter_trade_batches(zip_path, symbol, day, checksum, block_size))
    return pa.concat_tables(batches) if batches else arrow_schema().empty_table()


# --------------------------------------------------------------------------
# Iceberg
# --------------------------------------------------------------------------

def load_catalog_from_env():
    from pyiceberg.catalog import load_catalog

    return load_catalog("finobs", **{
        "type": "rest",
        "uri": os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181"),
        "warehouse": os.environ.get("ICEBERG_WAREHOUSE", "s3://finobs-warehouse/"),
        "s3.endpoint": os.environ.get("S3_ENDPOINT", "http://minio:9000"),
        "s3.access-key-id": os.environ["MINIO_ROOT_USER"],
        "s3.secret-access-key": os.environ["MINIO_ROOT_PASSWORD"],
        "s3.region": "us-east-1",
        "s3.path-style-access": "true",
    })


def ensure_table(catalog):
    from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError, TableAlreadyExistsError
    from pyiceberg.transforms import IdentityTransform

    namespace = TABLE.split(".")[0]
    try:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        pass
    try:
        return catalog.load_table(TABLE)
    except NoSuchTableError:
        pass

    try:
        table = catalog.create_table(
            TABLE, schema=arrow_schema(),
            properties={"comment": "Binance Public Data trades D+1, chỉ nạp file khớp SHA-256"},
        )
    except TableAlreadyExistsError:  # task khác vừa tạo xong
        return catalog.load_table(TABLE)
    # Partition theo ngày file (đặc tả, Bảng 9) và mã, để chạy lại 1 mã chỉ thay đúng partition đó.
    with table.update_spec() as spec:
        spec.add_field("file_date", IdentityTransform(), "file_date")
        spec.add_field("symbol", IdentityTransform(), "symbol")
    return catalog.load_table(TABLE)


def write_batches(table, batches, symbol: str, day: date, on_batch=None) -> None:
    """Thay dữ liệu của đúng (file_date, symbol) trong MỘT transaction: xóa partition cũ
    rồi append từng khối. Lỗi giữa chừng thì không commit gì (không để lại dữ liệu nửa vời)."""
    from pyiceberg.expressions import And, EqualTo

    with table.transaction() as tx:
        tx.delete(And(EqualTo("file_date", day.isoformat()), EqualTo("symbol", symbol)))
        for batch in batches:
            tx.append(batch)
            if on_batch:
                on_batch(batch)


# --------------------------------------------------------------------------
# Một mã, một ngày
# --------------------------------------------------------------------------

def fetch_verified(symbol: str, day: date, workdir: Path, tamper: bool = False) -> tuple[Path, str]:
    """Tải zip + checksum và kiểm. tamper=True làm hỏng 1 byte để thử nhánh lỗi (demo/test)."""
    name = file_name(symbol, day)
    zip_path = download(file_url(symbol, day), workdir / name)
    checksum_path = download(file_url(symbol, day) + ".CHECKSUM", workdir / f"{name}.CHECKSUM")
    if tamper:
        with open(zip_path, "r+b") as fh:
            first = fh.read(1)
            fh.seek(0)
            fh.write(bytes([first[0] ^ 0xFF]))
    return zip_path, verify(zip_path, checksum_path)


def load_day(symbol: str, day: date, catalog=None, dry_run: bool = False, tamper: bool = False) -> dict:
    import pyarrow.compute as pc

    summary = {"symbol": symbol, "file_date": day.isoformat(), "rows": 0, "sha256": None,
               "first_trade_id": None, "last_trade_id": None, "written": False}

    def track(batch) -> None:
        ids = batch.column("trade_id")
        summary["rows"] += batch.num_rows
        low, high = pc.min(ids).as_py(), pc.max(ids).as_py()
        summary["first_trade_id"] = low if summary["first_trade_id"] is None else min(summary["first_trade_id"], low)
        summary["last_trade_id"] = high if summary["last_trade_id"] is None else max(summary["last_trade_id"], high)

    with tempfile.TemporaryDirectory(prefix="finobs-binance-") as tmp:
        zip_path, checksum = fetch_verified(symbol, day, Path(tmp), tamper=tamper)
        summary["sha256"] = checksum
        if dry_run:
            for batch in iter_trade_batches(zip_path, symbol, day, checksum):
                track(batch)
            return summary

        from pyiceberg.exceptions import CommitFailedException

        catalog = catalog or load_catalog_from_env()
        for attempt in range(1, COMMIT_RETRIES + 1):
            summary.update(rows=0, first_trade_id=None, last_trade_id=None)
            try:
                # Nạp lại metadata mỗi lần: job khác (mã khác) có thể vừa commit vào cùng bảng.
                write_batches(ensure_table(catalog), iter_trade_batches(zip_path, symbol, day, checksum),
                              symbol, day, on_batch=track)
                break
            except CommitFailedException:
                if attempt == COMMIT_RETRIES:
                    raise
                time.sleep(2 * attempt)
        summary["written"] = True
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date", required=True, help="ngày UTC của file, YYYY-MM-DD")
    parser.add_argument("--symbols", default="config/symbols.json",
                        help="file symbols.json (lấy danh sách binance) hoặc danh sách cách nhau dấu phẩy")
    parser.add_argument("--dry-run", action="store_true", help="tải + kiểm + đọc, không ghi Iceberg")
    parser.add_argument("--tamper", action="store_true", help="làm hỏng file sau khi tải để thử nhánh sai checksum")
    args = parser.parse_args()

    day = date.fromisoformat(args.date)
    if Path(args.symbols).is_file():
        symbols = json.loads(Path(args.symbols).read_text(encoding="utf-8"))["binance"]
    else:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    catalog = None if args.dry_run else load_catalog_from_env()
    for symbol in symbols:
        print(json.dumps(load_day(symbol, day, catalog, args.dry_run, args.tamper), ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
