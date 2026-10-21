import csv
import io
import json
import tempfile
import unittest
from pathlib import Path

import json_parser as jp
import pyarrow.parquet as pq


class LoyaltyTests(unittest.TestCase):
    def test_multiple_programs_strings_and_parquet_roundtrip(self):
        obj = {
            "Registered Flights": [],
            "Loyality Programm": [
                {"Status": "Gold", "programm": "SU", "Number": " 000123"},
                {"Status": "Silver", "programm": "KE", "Number": "000001"},
            ],
        }
        with tempfile.TemporaryDirectory() as d:
            base = Path(d)
            source = base / "source.json"
            source.write_text(json.dumps({"Forum Profiles": [obj]}))
            stats = jp.process_file(source, base, chunk_size=1)
            rows = [
                row
                for part in sorted(base.glob("json_loyalty_part_*.parquet"))
                for row in pq.read_table(part).to_pylist()
            ]
            self.assertEqual(stats["loyalties_written"], 2)
            self.assertTrue(stats["integrity_passed"])
            self.assertEqual([r["Number"] for r in rows], [" 000123", "000001"])
            self.assertEqual([r["loyalty_index"] for r in rows], [0, 1])
            self.assertEqual(len({r["record_id"] for r in rows}), 2)
            self.assertEqual(rows[0]["profile_record_id"], rows[1]["profile_record_id"])
            self.assertEqual(rows[0]["rule_version"], jp.VERSION)

    def test_duplicate_groups_and_conflicting_fields(self):
        raw = (
            '{"Loyality Programm":[{"Status":"G","programm":"SU",'
            '"Number":"001","Number":"002"}],'
            '"Loyality Programm":[{"Status":"S","programm":"KE","Number":"003"}]}'
        )
        obj = json.loads(raw, object_pairs_hook=jp.PairsDict)
        rows, issues = jp.extract_profile_loyalties(obj, "test", 0)
        self.assertEqual([r["loyalty_group_index"] for r in rows], [0, 1])
        self.assertIsNone(rows[0]["Number"])
        self.assertEqual(rows[0]["raw_loyalty_json"].count('"Number"'), 2)
        self.assertEqual(issues[0]["issue_type"], "duplicate_json_key")

    def test_malformed_elements_not_dropped(self):
        obj = {"Loyality Programm": [None, "bad", {"Number": 123, "extra": [1]}]}
        rows, issues = jp.extract_profile_loyalties(obj, "test", 0)
        self.assertEqual(len(rows), 3)
        self.assertTrue(all(r["parse_status"] == "review" for r in rows))
        self.assertIsNone(rows[2]["Number"])
        self.assertIn("extra", rows[2]["extra_fields_json"])
        self.assertTrue(issues)
        rows, issues = jp.extract_profile_loyalties({"Loyality Programm": {}}, "t", 0)
        self.assertEqual(rows, [])
        self.assertEqual(issues[0]["issue_type"], "invalid_loyalty_list")
        self.assertEqual(jp.extract_profile_loyalties({}, "t", 0), ([], []))

    def test_after_line_300_and_multiple_output_chunks(self):
        payload = json.dumps(
            {
                "Forum Profiles": [
                    {
                        "Registered Flights": [None] * 350,
                        "Loyality Programm": [
                            {"Status": "G", "programm": "SU", "Number": "0001"}
                        ],
                    },
                    {"Registered Flights": [], "Loyality Programm": []},
                ]
            },
            indent=2,
        )
        self.assertGreater(
            payload.index('"Loyality Programm"'), payload.index("\n", 300)
        )
        self.assertGreater(
            payload[: payload.index('"Loyality Programm"')].count("\n"), 300
        )
        self.assertEqual(
            len(
                list(
                    jp.iter_profiles(
                        jp.StreamReader(io.StringIO(payload), block_size=31)
                    )
                )
            ),
            2,
        )
        with tempfile.TemporaryDirectory() as d:
            base = Path(d)
            source = base / "source.json"
            source.write_text(payload)
            stats = jp.process_file(source, base, fmt="csv", chunk_size=100)
            self.assertEqual(stats["flights_written"], 350)
            self.assertEqual(stats["loyalties_written"], 1)
            self.assertEqual(stats["profiles_written"], 2)
            with (base / "json_loyalty.csv").open(encoding="utf-8-sig") as f:
                self.assertEqual(list(csv.DictReader(f))[0]["Number"], "0001")


if __name__ == "__main__":
    unittest.main()
