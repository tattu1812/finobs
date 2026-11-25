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


V1_DIR = ROOT / "tests" / "fixtures" / "scenarios_v1"


def _set(path: str, value):
    def mutate(doc):
        *parents, leaf = path.split(".")
        node = doc
        for key in parents:
            node = node[key]
        node[leaf] = value
    return mutate


def _drop(path: str):
    def mutate(doc):
        *parents, leaf = path.split(".")
        node = doc
        for key in parents:
            node = node[key]
        node.pop(leaf, None)
    return mutate


# 16 ca BREAKING trong SPRINT2_REVIEW_HANDOFF.md mục 4: version 1 chấp nhận, version 2 từ chối.
# (tên, kịch bản gốc, phép sửa)
BREAKING = [
    ("1 replay không có dataset", "S002", _drop("input.dataset")),
    ("2 dataset rỗng", "S002", _set("input.dataset", "")),
    ("3 input.symbols rỗng", "S002", _set("input.symbols", [])),
    ("4 input.symbols có chuỗi rỗng", "S002", _set("input.symbols", ["BTCUSDT", ""])),
    ("5 target.symbols rỗng", "S002", _set("injection.target.symbols", [])),
    ("6 target.symbols có chuỗi rỗng", "S002", _set("injection.target.symbols", [""])),
    ("7 target.symbols trùng", "S002", _set("injection.target.symbols", ["ETHUSDT", "ETHUSDT"])),
    ("8 tags có chuỗi rỗng", "S002", _set("tags", [""])),
    ("9 affected_assets rỗng", "S002", _set("ground_truth.affected_assets", [])),
    ("10 expected_signals rỗng", "S002", _set("ground_truth.expected_signals", [])),
    ("11 root_cause.component rỗng", "S002", _set("ground_truth.root_cause.component", "")),
    ("12 must_not_alert metric sai pattern", "S002",
     _set("ground_truth.must_not_alert", [{"metric": "Bad-Metric"}])),
    ("13 đối chứng có fault_type", "S100", _set("ground_truth.fault_type", "FT09_value_anomaly")),
    ("14 đối chứng có start_offset_s", "S100", _set("ground_truth.start_offset_s", 0)),
    ("15 đối chứng có end_offset_s", "S100", _set("ground_truth.end_offset_s", 60)),
    ("16 đối chứng có affected_assets", "S100",
     _set("ground_truth.affected_assets", ["finobs:raw.binance.trade"])),
]


class VersionPolicyTest(unittest.TestCase):
    """Quyết định D01 = C: version 1 đóng băng vẫn hợp lệ, version 2 chặt hơn và có FT12."""

    @classmethod
    def setUpClass(cls):
        cls.validators = validate.load_validators()

        def load(folder: Path, prefix: str):
            path = next(folder.glob(f"{prefix}_*.yaml"))
            return yaml.safe_load(path.read_text(encoding="utf-8"))

        cls.v1 = {key: load(V1_DIR, key) for key in ("S002", "S100")}
        cls.v2 = {key: load(HERE, key) for key in ("S002", "S100")}

    def check(self, doc):
        with tempfile.TemporaryDirectory(prefix="finobs-validation-") as directory:
            path = Path(directory) / (doc["id"] + ".yaml")
            path.write_text(yaml.safe_dump(doc, allow_unicode=True), encoding="utf-8")
            return validate.check_file(path, self.validators)

    def test_versions_and_bases(self):
        self.assertEqual(sorted(self.validators), [1, 2])
        self.assertEqual(validate.LATEST_VERSION, 2)
        for key in ("S002", "S100"):
            self.assertEqual(self.v1[key]["scenario_version"], 1)
            self.assertEqual(self.check(self.v1[key]), [], f"v1 {key}")
            self.assertEqual(self.v2[key]["scenario_version"], 2)
            self.assertEqual(self.check(self.v2[key]), [], f"v2 {key}")

    def test_sixteen_breaking_cases(self):
        self.assertEqual(len(BREAKING), 16)
        for name, base, mutate in BREAKING:
            with self.subTest(case=name):
                old, new = copy.deepcopy(self.v1[base]), copy.deepcopy(self.v2[base])
                mutate(old)
                mutate(new)
                self.assertEqual(self.check(old), [], f"version 1 phải chấp nhận: {name}")
                self.assertTrue(self.check(new), f"version 2 phải từ chối: {name}")

    def test_ft12_only_in_version_2(self):
        doc = copy.deepcopy(self.v1["S002"])
        doc["fault_type"] = doc["ground_truth"]["fault_type"] = "FT12_null_spike"
        self.assertTrue(any("fault_type" in e for e in self.check(doc)))

    def test_unsupported_version(self):
        doc = copy.deepcopy(self.v2["S002"])
        doc["scenario_version"] = 3
        errors = self.check(doc)
        self.assertEqual(len(errors), 1)
        self.assertIn("không được hỗ trợ", errors[0])


if __name__ == "__main__":
    unittest.main()
