"""Boundary tests and a synthetic two-column PDF; no private fixtures required."""

import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pymupdf
from parse_schedule import (
    header_value,
    load_schedule,
    parse_row,
    parse_schedule,
    resolve_period,
)

START, END = date(2018, 11, 1), date(2019, 1, 31)
ROW = "26 Dec - 06 Jan 1 34567 18:15 19:35 KL1336 73W 1H20M"


class ScheduleTests(unittest.TestCase):
    def test_year_boundary_and_day_codes(self):
        row = parse_row(ROW, START, END, {"73W"})
        self.assertEqual(
            (row["valid_from"], row["valid_to"]), ("2018-12-26", "2019-01-06")
        )
        self.assertEqual(row["days_codes"], "134567")
        self.assertEqual(row["duration_minutes"], 80)

    def test_unsupported_marker_not_silently_repaired(self):
        row = parse_row(
            ROW.replace("19:35 KL1336", "19:35+-1 MF9869*"), START, END, {"73W"}
        )
        self.assertIsNone(row["arrival_day_offset"])
        self.assertEqual(row["arrival_offset_raw"], "+-1")
        self.assertIn("ambiguous_arrival_day_marker", row["field_issues"])
        self.assertEqual(row["flight_number"], "MF9869")
        self.assertEqual(row["flight_marker"], "*")

    def test_next_day_and_invalid_clock(self):
        row = parse_row(ROW.replace("19:35", "24:99+1"), START, END, {"73W"})
        self.assertIsNone(row["arrival_local"])
        self.assertEqual(row["arrival_day_offset"], 1)
        self.assertIn("invalid_local_clock", row["field_issues"])

    def test_invalid_period_and_unknown_equipment(self):
        self.assertEqual(
            resolve_period("31", "Nov", "06", "Jan", START, END)[2], "invalid_date"
        )
        row = parse_row(ROW.replace("73W", "CS3"), START, END, {"73W"})
        self.assertEqual(row["aircraft_code"], "CS3")
        self.assertEqual(row["aircraft_dictionary_status"], "not_listed_in_intro")
        self.assertIsNone(parse_row("not a flight row", START, END, set()))

    def test_geographical_label_not_guessed(self):
        self.assertEqual(
            header_value("Amsterdam , Netherlands AMS"),
            ("Amsterdam , Netherlands", "AMS"),
        )
        self.assertEqual(
            header_value("Some place REA/rea"), ("Some place REA/rea", None)
        )

    def test_columns_continuation_reset_and_lineage(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "fixture.pdf"
            doc = pymupdf.open()
            for _ in range(4):
                doc.new_page(width=595, height=842)
            page = doc.new_page(width=595, height=842)
            page.insert_text(
                (18, 65),
                "FROM: Aalborg, Denmark AAL\nTO: Amsterdam, Netherlands AMS\n" + ROW,
                fontsize=6,
            )
            page.insert_text(
                (308, 65),
                "FROM: Amsterdam, Netherlands AMS\nTO: Aalborg, Denmark AAL\n"
                + ROW
                + "\nOperated by: Explicit operator",
                fontsize=6,
            )
            page = doc.new_page(width=595, height=842)
            page.insert_text(
                (18, 65),
                "Operated by: Previous page operator\n"
                + ROW
                + "\nFROM: Paris, France CDG\n"
                + ROW
                + "\nunexpected heading",
                fontsize=6,
            )
            page.insert_text((308, 65), ROW, fontsize=6)
            doc.save(path)
            doc.close()
            fake_intro = {
                "metadata": pd.DataFrame(
                    {
                        "key": ["coverage_start", "coverage_end"],
                        "value": [START.isoformat(), END.isoformat()],
                    }
                ),
                "aircraft_types": pd.DataFrame({"aircraft_code": ["73W"]}),
                "pages": pd.DataFrame({"source_sha256": ["fixture-sha"]}),
            }
            out = Path(temp) / "parsed"
            with patch("parse_schedule.parse_intro", return_value=fake_intro):
                manifest = parse_schedule(path, out)
            f = load_schedule(out)
            self.assertEqual(len(f), 5)
            left = f[(f.page == 6) & (f.column == 0)].reset_index(drop=True)
            self.assertEqual(left.loc[0, "origin_code"], "AAL")
            self.assertEqual(left.loc[0, "route_status"], "continued_same_column")
            self.assertEqual(left.loc[1, "origin_code"], "CDG")
            self.assertTrue(pd.isna(left.loc[1, "destination_code"]))
            right = f[(f.page == 6) & (f.column == 1)].iloc[0]
            self.assertEqual(
                (right.origin_code, right.destination_code), ("AMS", "AAL")
            )
            raw = pd.read_parquet(out / "raw_lines.parquet")
            self.assertTrue(set(f.raw_id) <= set(raw.raw_id))
            self.assertTrue(set(f.origin_header_id.dropna()) <= set(raw.raw_id))
            self.assertEqual(len(raw), raw.raw_id.nunique())
            self.assertEqual(manifest["tables"]["quarantine"], 1)
            notes = pd.read_parquet(out / "operator_notes.parquet")
            self.assertEqual(len(notes), 2)
            self.assertEqual(notes.loc[1, "link_status"], "preceding_row_previous_page")
            self.assertTrue(set(notes.schedule_id) <= set(f.schedule_id))
            self.assertEqual(notes.loc[1, "schedule_id"], f.iloc[0].schedule_id)
            with self.assertRaises(FileExistsError):
                parse_schedule(path, out)


if __name__ == "__main__":
    unittest.main()
