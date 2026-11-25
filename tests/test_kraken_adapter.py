"""Adapter Kraken: giữ nguyên văn giá/khối lượng dạng số JSON, đóng vỏ đúng contract 1.1.0."""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "platform" / "ingest"))

import kraken_ws  # noqa: E402
from common import canonical_kraken  # noqa: E402
from sinks import ContractGate  # noqa: E402

# Message thật dạng Kraken v2 (2 trade cùng lô), giá/khối lượng là SỐ JSON.
RAW = ('{"channel":"trade","type":"update","data":['
       '{"symbol":"BTC/USD","side":"buy","price":58905.6,"qty":0.00100000,"ord_type":"limit",'
       '"trade_id":117871607,"timestamp":"2026-11-25T10:12:55.296465Z"},'
       '{"symbol":"BTC/USD","side":"sell","price":58906,"qty":0.10000000,"ord_type":"market",'
       '"trade_id":117871608,"timestamp":"2026-11-25T10:12:55.3Z"}]}')


class ListWriter:
    def __init__(self):
        self.rows = []

    def write(self, record):
        self.rows.append(record)


class KrakenAdapterTest(unittest.TestCase):
    def test_decimals_kept_verbatim_as_strings(self):
        trades = kraken_ws.parse(RAW)["data"]
        self.assertEqual(trades[0]["price"], "58905.6")
        self.assertEqual(trades[0]["qty"], "0.00100000")  # json.loads thường sẽ cho 0.001
        self.assertEqual(trades[1]["price"], "58906")      # số nguyên cũng thành chuỗi
        self.assertEqual(trades[1]["qty"], "0.10000000")
        self.assertIsInstance(trades[0]["trade_id"], int)  # trade_id vẫn là số nguyên

    def test_envelope_passes_contract(self):
        writer = ListWriter()
        kraken_ws._handle(RAW, writer)
        self.assertEqual(len(writer.rows), 2)
        first = writer.rows[0]
        self.assertEqual(first["event_id"], "kraken:BTC/USD:117871607")
        self.assertEqual(first["symbol_canonical"], "BTC-USD")
        self.assertEqual(first["event_time"], "2026-11-25T10:12:55.296Z")
        self.assertEqual(first["transport"], {"batch_size": 2, "raw_bytes": len(RAW), "msg_seq": None,
                                              "is_snapshot": False})
        gate = ContractGate()
        for row in writer.rows:
            self.assertEqual(gate.errors(json.loads(json.dumps(row))), [])

    def test_control_messages_ignored(self):
        writer = ListWriter()
        for raw in ('{"channel":"heartbeat"}',
                    '{"channel":"status","type":"update","data":[{"system":"online"}]}',
                    '{"method":"subscribe","result":{"channel":"trade","symbol":"BTC/USD"},"success":true}'):
            kraken_ws._handle(raw, writer)
        self.assertEqual(writer.rows, [])

    def test_canonical(self):
        self.assertEqual(canonical_kraken("doge/usd"), "DOGE-USD")


if __name__ == "__main__":
    unittest.main()
