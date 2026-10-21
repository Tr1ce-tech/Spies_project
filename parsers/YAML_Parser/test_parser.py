"""Tests for streaming boundaries, strings, nested data and explicit failures."""

import io
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

import parse_yaml as parser
import pyarrow.parquet as pq

SAMPLE = """'2017-02-03':
  SU0001:
    FF:
      SU 000001: {CLASS: Y, FARE: '001ABC'}
      FB 000002: {CLASS: J, FARE: 'NULL'}
    FROM: svo
    STATUS: LANDED
    TO: N/A
  SU0002:
    FF: {}
    FROM: LED
    STATUS: 'ON'
    TO: SVO
'2017-02-04':
  AF0003:
    FROM: CDG
    STATUS: LANDED
    TO: SVO
    EXTRA: {name: 'ИВАН ЁЛКИН', notes: [one, two]}
"""


class ParserTests(unittest.TestCase):
    def test_iso_dates_no_ambiguous_guess(self):
        self.assertEqual(parser.normalise("2017-02-03", "date")[0], date(2017, 2, 3))
        self.assertIsNone(parser.normalise("03/02/2017", "date")[0])
        self.assertIsNone(parser.normalise("2017-02-29", "date")[0])
        self.assertEqual(parser.normalise("2016-02-29", "date")[0], date(2016, 2, 29))

    def test_missing_and_airport_syntax(self):
        self.assertEqual(parser.normalise(None)[1], "absent")
        self.assertEqual(parser.normalise("")[1], "empty_scalar")
        self.assertEqual(parser.normalise("N/A")[1], "missing_marker")
        self.assertEqual(
            parser.normalise("rea", "airport"),
            ("REA", "syntax_only_not_historically_verified"),
        )

    def test_event_stream_preserves_scalar_lexemes(self):
        rows = list(parser.source_records(io.StringIO(SAMPLE)))
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0][2]["FF"]["SU 000001"]["FARE"], "001ABC")
        self.assertEqual(rows[1][2]["STATUS"], "ON")
        self.assertEqual(rows[2][2]["EXTRA"]["name"], "ИВАН ЁЛКИН")
        self.assertEqual(rows[2][2]["EXTRA"]["notes"], ["one", "two"])

    def test_duplicate_date_rejected(self):
        with self.assertRaisesRegex(ValueError, "Repeated date"):
            list(
                parser.source_records(
                    io.StringIO("'2017-01-01': {}\n'2017-01-01': {}\n")
                )
            )

    def test_duplicate_flight_rejected(self):
        text = "'2017-01-01':\n  SU1: {}\n  SU1: {}\n"
        with self.assertRaisesRegex(ValueError, "Repeated flight"):
            list(parser.source_records(io.StringIO(text)))

    def test_duplicate_loyalty_key_rejected(self):
        text = "'2017-01-01':\n  SU1:\n    FF:\n      SU 001: {}\n      SU 001: {}\n"
        with self.assertRaisesRegex(ValueError, "Duplicate mapping key"):
            list(parser.source_records(io.StringIO(text)))

    def test_duplicate_attribute_rejected(self):
        text = "'2017-01-01':\n  SU1: {FROM: SVO, FROM: LED}\n"
        with self.assertRaisesRegex(ValueError, "Duplicate mapping key"):
            list(parser.source_records(io.StringIO(text)))

    def test_aliases_and_tags_rejected(self):
        for text in [
            "'2017-01-01': {SU1: &a {FF: {}}}\n",
            "'2017-01-01': {SU1: !!python/object:os.system {}}\n",
        ]:
            with self.assertRaises(ValueError):
                list(parser.source_records(io.StringIO(text)))

    def test_malformed_yaml_rejected(self):
        import yaml

        with self.assertRaises(yaml.YAMLError):
            list(parser.source_records(io.StringIO("'2017-01-01': {SU1: [\n")))

    def test_multiple_documents_rejected(self):
        with self.assertRaises(ValueError):
            list(parser.source_records(io.StringIO("{}\n---\n{}\n")))

    def test_full_roundtrip_provenance_unknown_fields_and_empty_ff(self):
        for ending in ["\n", ""]:
            with (
                self.subTest(final_newline=bool(ending)),
                tempfile.TemporaryDirectory() as folder,
            ):
                root = Path(folder)
                source = root / "source.yaml"
                source.write_text(SAMPLE.rstrip("\n") + ending, encoding="utf8")
                result = parser.run(source, root / "result")
                flights = pq.read_table(root / "result/flights.parquet").to_pylist()
                entries = pq.read_table(
                    root / "result/loyalty_entries.parquet"
                ).to_pylist()
                self.assertEqual(result["physical_lines"], len(SAMPLE.splitlines()))
                self.assertEqual((len(flights), len(entries)), (3, 2))
                self.assertEqual(entries[0]["loyalty_number"], "000001")
                self.assertEqual(entries[0]["fare_raw"], "001ABC")
                self.assertEqual(entries[0]["date"], date(2017, 2, 3))
                self.assertIsNone(entries[0]["destination"])
                self.assertEqual(flights[1]["loyalty_entry_count"], 0)
                self.assertEqual(flights[2]["loyalty_entry_count"], 0)
                self.assertIn("EXTRA", json.loads(flights[2]["raw_flight_json"]))
                for flight in flights:
                    restored = __import__("yaml").load(
                        flight["raw_flight_yaml"], Loader=__import__("yaml").BaseLoader
                    )
                    self.assertEqual(
                        restored[flight["flight_number_raw"]],
                        json.loads(flight["raw_flight_json"]),
                    )
                self.assertEqual(len({f["flight_record_id"] for f in flights}), 3)
                self.assertEqual(
                    entries[0]["flight_record_id"], flights[0]["flight_record_id"]
                )
                self.assertTrue(result["complete"])
                with self.assertRaises(FileExistsError):
                    parser.run(source, root / "result")

    def test_bad_run_never_publishes_success(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = root / "source.yaml"
            source.write_text("'2017-01-01': {SU1: {FF: []}}", encoding="utf8")
            with self.assertRaises(ValueError):
                parser.run(source, root / "result")
            self.assertFalse((root / "result").exists())
            failure = json.loads(
                (root / "result.partial/FAILED.json").read_text("utf8")
            )
            self.assertFalse(failure["complete"])

    def test_pilot_never_marked_complete(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = root / "source.yaml"
            source.write_text(SAMPLE, encoding="utf8")
            result = parser.run(source, root / "pilot", max_flights=1)
            self.assertFalse(result["complete"])
            self.assertEqual(result["flight_rows"], 1)

    def test_independent_reconciliation_and_source_change_detection(self):
        from verify_yaml import verify

        text = """'2017-01-01':
  SU001:
    FF:
      SU 000001: {CLASS: Y, FARE: Y001}
    FROM: SVO
    STATUS: LANDED
    TO: LED
  AF002:
    FF:
      FB 000002: {CLASS: J, FARE: J002}
    FROM: CDG
    STATUS: LANDED
    TO: SVO
"""
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = root / "source.yaml"
            source.write_text(text, encoding="utf8")
            parser.run(source, root / "result")
            checks = verify(source, root / "result")
            self.assertTrue(checks["all_checks_passed"])
            self.assertEqual(checks["leading_zero_card_numbers"], 2)
            source.write_text(text.replace("LED", "VKO"), encoding="utf8")
            with self.assertRaisesRegex(ValueError, "Source hash"):
                verify(source, root / "result")


if __name__ == "__main__":
    unittest.main(verbosity=2)
