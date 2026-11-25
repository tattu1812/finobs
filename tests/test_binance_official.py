"""Bộ nạp Binance Public Data: kiểm SHA-256 và đọc CSV, không cần mạng hay Iceberg."""

from __future__ import annotations

import hashlib
import sys
import tempfile
import unittest
import zipfile
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "platform" / "batch"))

import binance_official as bo  # noqa: E402

try:
    import pyarrow  # noqa: F401

    HAS_ARROW = True
except ImportError:
    HAS_ARROW = False

DAY = date(2026, 11, 24)
ROWS_MS = "4235710000,91234.56000000,0.00120000,109.48147200,1795478400123,True,True\n" \
          "4235710001,91234.57000000,0.50000000,45617.28500000,1795478400456,False,True\n"
ROWS_US = "4235710000,91234.56000000,0.00120000,109.48147200,1795478400123456,True,True\n"
HEADER = "id,price,qty,quote_qty,time,is_buyer_maker,is_best_match\n"


def make_zip(folder: Path, csv_text: str, symbol: str = "BTCUSDT") -> Path:
    path = folder / bo.file_name(symbol, DAY)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(path.name.replace(".zip", ".csv"), csv_text)
    return path


def write_checksum(zip_path: Path, digest: str | None = None) -> Path:
    digest = digest or hashlib.sha256(zip_path.read_bytes()).hexdigest()
    path = zip_path.with_name(zip_path.name + ".CHECKSUM")
    path.write_text(f"{digest}  {zip_path.name}\n", encoding="utf-8")
    return path


class ChecksumTest(unittest.TestCase):
    def test_url_layout(self):
        self.assertEqual(
            bo.file_url("BTCUSDT", DAY),
            "https://data.binance.vision/data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2026-11-24.zip",
        )

    def test_matching_checksum_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            zip_path = make_zip(Path(tmp), ROWS_MS)
            digest = bo.verify(zip_path, write_checksum(zip_path))
        self.assertEqual(len(digest), 64)

    def test_tampered_file_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            zip_path = make_zip(Path(tmp), ROWS_MS)
            checksum = write_checksum(zip_path)
            data = bytearray(zip_path.read_bytes())
            data[0] ^= 0xFF
            zip_path.write_bytes(bytes(data))
            with self.assertRaises(bo.ChecksumMismatch):
                bo.verify(zip_path, checksum)

    def test_checksum_line_for_other_file_is_an_error(self):
        with self.assertRaises(ValueError):
            bo.expected_sha256("ab" * 32 + "  OTHER.zip\n", "BTCUSDT-trades-2026-11-24.zip")


@unittest.skipUnless(HAS_ARROW, "cần pyarrow")
class ReadTradesTest(unittest.TestCase):
    def read(self, csv_text: str):
        with tempfile.TemporaryDirectory() as tmp:
            return bo.read_trades(make_zip(Path(tmp), csv_text), "BTCUSDT", DAY, "f" * 64)

    def test_millisecond_file(self):
        table = self.read(ROWS_MS)
        self.assertEqual(table.num_rows, 2)
        self.assertEqual(table.schema, bo.arrow_schema())
        first = table.slice(0, 1).to_pylist()[0]
        self.assertEqual(first["trade_id"], 4235710000)
        self.assertEqual(first["price"], Decimal("91234.56"))
        self.assertEqual(first["trade_ts"], datetime(2026, 11, 24, 0, 0, 0, 123000, tzinfo=timezone.utc))
        self.assertEqual((first["symbol"], first["file_date"]), ("BTCUSDT", DAY))

    def test_microsecond_file_and_header(self):
        table = self.read(HEADER + ROWS_US)
        self.assertEqual(table.num_rows, 1)
        ts = table.slice(0, 1).to_pylist()[0]["trade_ts"]
        self.assertEqual(ts, datetime(2026, 11, 24, 0, 0, 0, 123456, tzinfo=timezone.utc))

    def test_small_blocks_give_same_rows_and_unit(self):
        """Đọc theo khối nhỏ (nhiều batch) phải cho đúng dữ liệu như đọc một lần,
        và đơn vị thời gian quyết định ở khối đầu áp dụng cho mọi khối."""
        rows = "".join(f"{4235710000 + i},1.00000000,2.00000000,2.00000000,{1795478400000 + i},True,True\n"
                       for i in range(5000))
        with tempfile.TemporaryDirectory() as tmp:
            zip_path = make_zip(Path(tmp), rows)
            batches = list(bo.iter_trade_batches(zip_path, "BTCUSDT", DAY, "f" * 64, block_size=4096))
            whole = bo.read_trades(zip_path, "BTCUSDT", DAY, "f" * 64)
        self.assertGreater(len(batches), 1)
        self.assertEqual(sum(b.num_rows for b in batches), 5000)
        self.assertEqual(whole.column("trade_ts")[-1].as_py(),
                         datetime(2026, 11, 24, 0, 0, 4, 999000, tzinfo=timezone.utc))

    def test_decimal_keeps_eight_places(self):
        table = self.read(ROWS_MS)
        self.assertEqual(table.slice(1, 1).to_pylist()[0]["quote_qty"], Decimal("45617.285"))


if __name__ == "__main__":
    unittest.main()
