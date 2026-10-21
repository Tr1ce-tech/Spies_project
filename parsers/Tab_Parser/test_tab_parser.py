"""Tests for field alignment, no-drop behavior, and diagnostics."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import tab_parser
from tab_parser import parse_line, process_file, records

SAMPLE = Path(__file__).with_name("sample.tab")


class TestFixedWidthParser(unittest.TestCase):
    def test_supplied_first_row_fields(self):
        line = SAMPLE.read_text(encoding="utf-8").splitlines()[1]
        row, issues = parse_line(line, 2)
        self.assertEqual(
            (row["TrvCls"], row["Fare"], row["Baggage"]), ("J", "JGRPGN", "0PC")
        )
        self.assertEqual(row["FrequentFlyer"], "FF#SU 38116280")
        self.assertEqual(row["PaxName"], "ОЗЕРОВ ИЛЬДАР ДАНИИЛОВИЧ")
        self.assertEqual(row["source_line"], 2)
        self.assertEqual(row["parse_status"], "ok")
        self.assertFalse(issues)

    def test_missing_marker_stays_as_original(self):
        line = SAMPLE.read_text(encoding="utf-8").splitlines()[2]
        row, issues = parse_line(line, 3)
        self.assertEqual(row["PaxBirthDate"], "N/A")
        self.assertEqual(row["Baggage"], "")
        self.assertEqual(row["TrvCls"], "Y")
        self.assertEqual(row["Fare"], "YRSTUQ")
        self.assertEqual(row["parse_status"], "ok")
        self.assertFalse(issues)

    def test_invalid_width_not_dropped(self):
        line = SAMPLE.read_text(encoding="utf-8").splitlines()[1][:-1]
        row, issues = parse_line(line, 2)
        self.assertEqual(row["parse_status"], "invalid_width")
        self.assertEqual(row["raw_line"], line)
        self.assertEqual(issues[0]["error_type"], "invalid_width")

    def test_invalid_date_preserved_and_reported(self):
        line = SAMPLE.read_text(encoding="utf-8").splitlines()[1]
        line = line[:72] + "2017-99-30  " + line[84:]
        row, issues = parse_line(line, 2)
        self.assertEqual(row["DepartDate"], "2017-99-30")
        self.assertEqual(row["parse_status"], "review")
        self.assertIn("invalid_date", [x["error_type"] for x in issues])

    def test_every_sample_record_survives(self):
        result = list(records(SAMPLE))
        self.assertEqual(len(result), 37)
        self.assertTrue(all(row["parse_status"] == "ok" for row, _ in result))
        self.assertTrue(all(not issues for _, issues in result))

    def test_config_only_run_writes_next_to_script(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            (directory / "flights.tab").write_bytes(SAMPLE.read_bytes())
            with (
                patch.object(tab_parser, "__file__", str(directory / "tab_parser.py")),
                patch.object(tab_parser, "INPUT_FILE", "flights.tab"),
                patch.object(tab_parser, "OUTPUT_FORMAT", "csv"),
                patch.object(tab_parser, "OUTPUT_DIRECTORY", "."),
                patch.object(tab_parser, "CHUNK_SIZE", 10),
            ):
                with patch("sys.argv", ["tab_parser.py"]):
                    tab_parser.main()
            self.assertTrue((directory / "flights.csv").exists())
            self.assertTrue((directory / "issues.csv").exists())
            self.assertTrue((directory / "summary.json").exists())
            import pandas as pd

            self.assertEqual(len(pd.read_csv(directory / "flights.csv")), 37)

    def test_malformed_and_invalid_encoding_survive_in_output(self):
        header, normal = SAMPLE.read_bytes().splitlines()[:2]
        wrong_width = normal[:-10]
        broken_encoding = normal[:61] + b"\xff" + normal[62:]
        with tempfile.TemporaryDirectory() as temp:
            inp = Path(temp) / "bad.tab"
            inp.write_bytes(
                b"\n".join([header, normal, wrong_width, broken_encoding]) + b"\n"
            )
            result = process_file(inp, Path(temp) / "out", "csv", chunk_size=2)
            self.assertEqual(result["rows_read"], 3)
            self.assertEqual(result["rows_written"], 3)
            self.assertEqual(result["statuses"]["invalid_width"], 1)
            self.assertEqual(result["statuses"]["invalid_encoding"], 1)
            self.assertIn("invalid_encoding", result["issues_by_type"])
            self.assertIn("invalid_width", result["issues_by_type"])


if __name__ == "__main__":
    unittest.main()
