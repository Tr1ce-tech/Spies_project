import base64
import csv
import io
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

import pandas as pd

if __package__:
    from .parse_csv import FIELDS, birth_candidates, load_records, normalize, parse_csv
else:
    from parse_csv import FIELDS, birth_candidates, load_records, normalize, parse_csv


def record(**changes):
    row = dict(
        zip(
            FIELDS,
            [
                "Имя",
                "Отчество",
                "Фамилия",
                "Female",
                "03/04/1980",
                "001234",
                "00ABC",
                "0001234567890",
                "None",
                "2017-01-01",
                "06:05",
                "SU001",
                "Own",
                "Moscow",
            ],
        )
    )
    row.update(changes)
    return [row[k] for k in FIELDS]


def csv_bytes(rows, blank=True):
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, delimiter=";", lineterminator="\r\n")
    writer.writerow(FIELDS)
    if blank:
        writer.writerow([])
    writer.writerows(rows)
    return stream.getvalue().encode()


class CSVTests(unittest.TestCase):
    def test_ambiguous_and_unique_birth_dates(self):
        values, status = birth_candidates("03/04/1980")
        self.assertEqual(status, "ambiguous_date")
        self.assertEqual(values, [date(1980, 3, 4), date(1980, 4, 3)])
        self.assertEqual(
            birth_candidates("10/27/1991"), ([date(1991, 10, 27)], "calendar_unique")
        )
        self.assertEqual(birth_candidates("31/02/1980"), ([], "invalid_date"))
        self.assertEqual(
            birth_candidates("05/05/1980"), ([date(1980, 5, 5)], "calendar_unique")
        )

    def test_field_aware_missing_values(self):
        self.assertEqual(normalize("Baggage", "None")[:2], ("None", "present"))
        self.assertEqual(
            normalize("PassengerSecondName", "None")[:2], (None, "missing_marker")
        )
        self.assertEqual(normalize("PassengerFirstName", "")[:2], (None, "empty"))
        self.assertEqual(
            normalize("PassengerFirstName", "N/A")[:2], (None, "missing_marker")
        )

    def test_iso_date_and_local_time(self):
        self.assertEqual(normalize("FlightDate", "01/02/2017")[1], "invalid_date")
        self.assertEqual(normalize("FlightDate", "2017-02-30")[1], "invalid_date")
        self.assertEqual(normalize("FlightTime", "24:00")[1], "invalid_time")

    def run_fixture(self, payload):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        source = Path(temp.name) / "input.csv"
        source.write_bytes(payload)
        out = Path(temp.name) / "result"
        manifest = parse_csv(source, out)
        return source, out, manifest

    def test_quotes_multiline_blank_and_raw_bytes(self):
        payload = csv_bytes([record(PassengerFirstName='Имя; "тест"\nпродолжение')])
        source, out, m = self.run_fixture(payload)
        f = load_records(out)
        self.assertEqual(len(f), 1)
        self.assertEqual(f.loc[0, "passenger_first_name"], 'Имя; "тест"\nпродолжение')
        self.assertEqual(f.loc[0, "ticket_number"], "0001234567890")
        self.assertEqual(f.loc[0, "passenger_document"], "001234")
        self.assertTrue(pd.isna(f.loc[0, "passenger_birth_date"]))
        self.assertEqual(f.loc[0, "birth_year"], 1980)
        raw = pd.read_parquet(out / "raw_records.parquet")
        reconstructed = base64.b64decode(m["header_raw_bytes_b64"]) + b"".join(
            base64.b64decode(x) for x in raw.raw_bytes_b64
        )
        self.assertEqual(reconstructed, payload)
        self.assertEqual(raw.loc[0, "record_status"], "blank_record")
        self.assertEqual(
            (f.loc[0, "source_line_start"], f.loc[0, "source_line_end"]), (3, 4)
        )
        with self.assertRaises(FileExistsError):
            parse_csv(source, out)

    def test_wrong_width_preserves_extra_fields(self):
        _, out, m = self.run_fixture(csv_bytes([record() + ["extra"]], blank=False))
        f = load_records(out)
        self.assertEqual(f.loc[0, "record_status"], "needs_review")
        self.assertTrue(pd.isna(f.loc[0, "ticket_number"]))
        raw = pd.read_parquet(out / "raw_records.parquet")
        self.assertEqual(json.loads(raw.loc[0, "raw_fields_json"])[-1], "extra")
        self.assertFalse(m["structurally_valid"])

    def test_invalid_utf8_retained(self):
        data = csv_bytes([record()], blank=False).replace("Имя".encode(), b"\xffbad")
        _, out, m = self.run_fixture(data)
        raw = pd.read_parquet(out / "raw_records.parquet")
        self.assertEqual(raw.loc[0, "record_status"], "invalid_utf8")
        self.assertIn(b"\xffbad", base64.b64decode(raw.loc[0, "raw_bytes_b64"]))
        self.assertFalse(m["structurally_valid"])

    def test_unclosed_quote_retained(self):
        payload = csv_bytes([], blank=False) + b'"broken\nline'
        _, out, m = self.run_fixture(payload)
        self.assertEqual(len(load_records(out)), 1)
        raw = pd.read_parquet(out / "raw_records.parquet")
        self.assertEqual(raw.loc[0, "record_status"], "csv_syntax_error")
        self.assertEqual(
            base64.b64decode(raw.loc[0, "raw_bytes_b64"]), b'"broken\nline'
        )
        self.assertFalse(m["structurally_valid"])

    def test_duplicates_not_removed(self):
        _, out, m = self.run_fixture(csv_bytes([record(), record()], blank=False))
        f = load_records(out)
        self.assertEqual(len(f), 2)
        self.assertEqual(f.record_id.nunique(), 2)
        self.assertEqual(m["exact_field_duplicate_excess"], 1)

    def test_unknown_header_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            source, out = Path(temp) / "wrong.csv", Path(temp) / "out"
            source.write_bytes(b"unknown;header\n1;2\n")
            with self.assertRaises(ValueError):
                parse_csv(source, out)
            self.assertEqual(
                json.loads((out / "manifest.json").read_text())["status"], "failed"
            )


if __name__ == "__main__":
    unittest.main()
