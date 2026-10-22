import json
import shutil
import tempfile
import unittest
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.build_common_parquet import build_dataset
from pipelines.filter_profiles import query_dataset
from pipelines.travel_analysis.criteria import select_profiles, validate_config
from pipelines.travel_analysis.inputs import discover


def fixture(directory):
    definitions = json.loads(
        (Path(__file__).with_name("travel_analysis") / "test_schemas.json").read_text()
    )
    for table, fields in definitions.items():

        def dtype(value):
            return pa.date32() if value == "date32[day]" else pa.type_for_alias(value)

        schema = pa.schema([(v["name"], dtype(v["type"])) for v in fields])
        rows = []
        if table == "json_profiles":
            for i in range(2):
                row = {f.name: None for f in schema}
                row.update(
                    record_id=f"profile-00{i}",
                    profile_index=i,
                    source_sha256="a" * 64,
                    rule_version="fixture.v1",
                    profile_status="ok",
                    metadata_json=json.dumps(
                        [
                            [
                                "Real Name",
                                {"First Name": "IVAN", "Last Name": "TESTOV"},
                            ],
                            ["Travel Documents", []],
                        ]
                    ),
                )
                rows.append(row)
        if table == "json_flights":
            for i in range(5):
                row = {f.name: None for f in schema}
                row.update(
                    record_id=f"flight-000{i}",
                    profile_record_id="profile-000",
                    source_sha256="a" * 64,
                    rule_version="fixture.v1",
                    parse_status="ok",
                    Date=f"2017-01-0{i + 1}",
                    Flight=f"SU00{i + 1}",
                    Departure_Airport="SVO",
                    Arrival_Airport="CDG",
                )
                rows.append(row)
        pq.write_table(
            pa.Table.from_pylist(rows, schema=schema), directory / (table + ".parquet")
        )


class TwoStageIntegrationTests(unittest.TestCase):
    def test_common_file_alone_is_enough_for_second_stage(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inputs = root / "parsed"
            inputs.mkdir()
            fixture(inputs)
            path = root / "travel.parquet"
            report = build_dataset(inputs, path)
            self.assertEqual(
                (report["observations"], report["source_entities"], report["rows"]),
                (5, 2, 6),
            )
            shutil.rmtree(inputs)
            output = root / "selected.parquet"
            summary = query_dataset(
                path,
                output,
                {"criteria": {"high_frequency": {"minimum": 2}}},
                root / "selected.json",
            )
            self.assertEqual(summary["matched_units"], 1)
            selected = pq.read_table(output).to_pylist()
            self.assertEqual(selected[0]["flight_days"], 5)
            self.assertEqual(selected[0]["matched_criteria"], ["high_frequency"])
            self.assertEqual(len(json.loads(selected[0]["names_json"])), 1)
            self.assertIn("threshold", json.loads(selected[0]["reasons_json"])[0])
            ui = json.loads((root / "selected.json").read_text())
            self.assertEqual(ui["profiles"][0]["names"][0]["first_name"], "IVAN")
            self.assertEqual(
                ui["profiles"][0]["reasons"][0]["criterion"], "high_frequency"
            )
            empty = root / "none.parquet"
            query_dataset(
                path, empty, {"criteria": {"high_frequency": {"minimum": 99}}}
            )
            self.assertEqual(pq.ParquetFile(empty).metadata.num_rows, 0)
            self.assertEqual(
                pq.ParquetFile(empty).schema_arrow.field("criteria_count").type,
                pa.int64(),
            )
            with self.assertRaises(FileExistsError):
                restored = root / "inputs_again"
                restored.mkdir()
                build_dataset(restored, path)

    def test_duplicate_exports_and_numeric_ids_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture(root)
            original = pq.read_table(root / "json_flights.parquet")
            pq.write_table(original, root / "duplicate.parquet", compression="gzip")
            # Different encoding of the same IDs must not be silently counted twice.
            import duckdb

            from pipelines.travel_analysis.inputs import register

            files, _ = discover(root)
            with duckdb.connect() as db:
                with self.assertRaisesRegex(ValueError, "duplicate"):
                    register(db, files)
            (root / "duplicate.parquet").unlink()
            ix = original.schema.get_field_index("record_id")
            pq.write_table(
                original.set_column(ix, "record_id", pa.array(range(5))),
                root / "json_flights.parquet",
            )
            with self.assertRaisesRegex(ValueError, "must be a string"):
                discover(root)


class CriterionTests(unittest.TestCase):
    def frame(self):
        return pd.DataFrame(
            [
                dict(
                    unit_id="a",
                    unit_kind="source_profile",
                    cohort="source_profile:json",
                    flight_days=12,
                    routed_flight_days=12,
                    distinct_routes=8,
                    route_coverage=1.0,
                    reference_eligible=False,
                    reference_units=1,
                    top_route_share=0.5,
                    top_route_flight_days=6,
                    peak7_share=0.6,
                    peak7_flight_days=7,
                    peak_day_flight_bins=3,
                    route_rarity=2.0,
                    candidate_document_count=2,
                    route_conflict_flight_days=0,
                )
            ]
        )

    def test_absolute_thresholds_do_not_need_large_reference_cohort(self):
        frame = self.frame()
        result, _, _ = select_profiles(
            frame,
            {
                "criteria": {
                    "high_frequency": {"minimum": 12},
                    "busy_day": {"minimum": 3},
                },
                "match": "all",
            },
        )
        self.assertEqual(len(result), 1)
        empty, _, _ = select_profiles(frame, {"criteria": {"high_frequency": {}}})
        self.assertEqual(len(empty), 0)
        quality, reasons, _ = select_profiles(
            frame, {"criteria": {"document_discrepancy": {}}}
        )
        self.assertEqual(len(quality), 1)
        self.assertEqual(reasons[0][0]["category"], "data_quality")

    def test_invalid_parameters_fail_before_reading_input(self):
        for config in [
            {"criteria": {"not_a_rule": {}}},
            {"criteria": {"repeated_route": {"min_share": 1.1}}},
            {"criteria": {"busy_day": {"minimum": True}}},
            {"criteria": {"high_frequency": {"minimum": float("nan")}}},
        ]:
            with self.assertRaises(ValueError):
                validate_config(config)


if __name__ == "__main__":
    unittest.main()
