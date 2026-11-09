"""Regression tests for scenario validation; generated cases stay in a temp directory."""

import copy
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator

import validate


HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
INVALID = ROOT / "tests" / "fixtures" / "scenarios_invalid"


class ScenarioValidationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = json.loads((HERE / "scenario.schema.json").read_text(encoding="utf-8"))
        cls.validator = Draft202012Validator(cls.schema)
        cls.example = yaml.safe_load(
            (HERE / "S002_binance_schema_drift.yaml").read_text(encoding="utf-8")
        )

    def check(self, doc):
        with tempfile.TemporaryDirectory(prefix="finobs-validation-") as directory:
            path = Path(directory) / (doc["id"] + ".yaml")
            path.write_text(yaml.safe_dump(doc, allow_unicode=True), encoding="utf-8")
            return validate.check_file(path, self.validator)

    def ft12(self):
        doc = copy.deepcopy(self.example)
        doc["id"] = "S012_null_spike"
        doc["title"] = "Synthetic null spike"
        doc["description"] = "Insert null values into a valid price field."
        doc["fault_type"] = "FT12_null_spike"
        doc["input"]["mode"] = "synthetic"
        doc["input"].pop("dataset", None)
        doc["injection"]["injector"] = "inject_null"
        doc["injection"]["params"] = {"field": "price", "fraction": 0.25}
        truth = doc["ground_truth"]
        truth["fault_type"] = "FT12_null_spike"
        truth["root_cause"]["component"] = "test ingestion"
        truth["root_cause"]["explanation"] = "Null values are inserted intentionally."
        truth["expected_signals"] = [
            {"metric": "price_null_ratio_1m", "direction": "up", "catalog_status": "proposed"}
        ]
        truth.pop("must_not_alert", None)
        return doc

    def test_existing_scenarios_and_taxonomy(self):
        self.assertEqual(validate.taxonomy_errors(self.schema), [])
        names = [
            "S001_coinbase_duplicate_burst.yaml",
            "S002_binance_schema_drift.yaml",
            "S003_collector_clock_skew.yaml",
            "S100_control_market_burst.yaml",
        ]
        for name in names:
            with self.subTest(name=name):
                self.assertEqual(validate.check_file(HERE / name, self.validator), [])

    def test_all_reference_invalid_fixtures_rejected(self):
        files = sorted(INVALID.glob("S9*.yaml"))
        self.assertEqual(len(files), 8)
        for path in files:
            with self.subTest(name=path.name):
                self.assertTrue(validate.check_file(path, self.validator))

    def test_ft12_and_invalid_incident_cases(self):
        valid = self.ft12()
        self.assertEqual(self.check(valid), [])
        cases = {}
        doc = copy.deepcopy(valid)
        doc["fault_type"] = "FT12_null_spik"
        cases["wrong fault code"] = doc
        doc = copy.deepcopy(valid)
        del doc["ground_truth"]
        cases["missing ground truth"] = doc
        doc = copy.deepcopy(valid)
        doc["injection"] = None
        cases["incident without injection"] = doc
        doc = copy.deepcopy(valid)
        doc["ground_truth"]["fault_type"] = "FT07_schema_drift"
        cases["mismatched fault type"] = doc
        doc = copy.deepcopy(valid)
        doc["injection"]["duration_s"] = 1300
        cases["injection outside window"] = doc
        doc = copy.deepcopy(valid)
        doc["injection"]["start_offset_s"] = -1
        cases["negative offset"] = doc
        doc = copy.deepcopy(valid)
        doc["injection"]["start_offset_s"] = "300"
        cases["wrong offset type"] = doc
        doc = copy.deepcopy(valid)
        doc["input"]["mode"] = "replay"
        cases["replay missing dataset"] = doc
        for name, invalid in cases.items():
            with self.subTest(name=name):
                self.assertTrue(self.check(invalid))

    def test_control_with_root_cause_rejected(self):
        doc = yaml.safe_load(
            (HERE / "S100_control_market_burst.yaml").read_text(encoding="utf-8")
        )
        doc["ground_truth"]["root_cause"] = self.ft12()["ground_truth"]["root_cause"]
        self.assertTrue(self.check(doc))

    def test_real_calendar_dates_for_both_fields(self):
        for field in ("start", "end"):
            for value in ("2026-02-30T03:00:00.000Z", "2025-02-29T03:00:00.000Z"):
                with self.subTest(field=field, value=value):
                    doc = self.ft12()
                    doc["input"]["window"][field] = value
                    errors = self.check(doc)
                    self.assertTrue(any(
                        f"input/window/{field}" in error and value in error for error in errors
                    ), errors)

    def test_leap_day_and_invalid_format(self):
        doc = self.ft12()
        doc["input"]["window"] = {
            "start": "2024-02-29T03:00:00.000Z",
            "end": "2024-02-29T03:20:00.000Z",
        }
        self.assertEqual(self.check(doc), [])
        for value in ("2026-13-01T03:00:00.000Z", "2026-10-27T03:00:00Z"):
            with self.subTest(value=value):
                invalid = self.ft12()
                invalid["input"]["window"]["start"] = value
                errors = self.check(invalid)
                self.assertTrue(any("input/window/start" in error for error in errors), errors)

    def test_cli_rejects_invalid_date_without_traceback(self):
        doc = self.ft12()
        doc["input"]["window"]["end"] = "2026-02-30T03:00:00.000Z"
        with tempfile.TemporaryDirectory(prefix="finobs-validation-") as directory:
            path = Path(directory) / (doc["id"] + ".yaml")
            path.write_text(yaml.safe_dump(doc, allow_unicode=True), encoding="utf-8")
            result = subprocess.run(
                [sys.executable, "-B", str(HERE / "validate.py"), "--in", directory],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("Traceback", result.stdout + result.stderr)
        self.assertIn("input/window/end", result.stdout)
        self.assertIn("2026-02-30T03:00:00.000Z", result.stdout)


if __name__ == "__main__":
    unittest.main()
