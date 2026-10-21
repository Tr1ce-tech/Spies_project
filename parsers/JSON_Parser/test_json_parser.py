import csv
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import json_parser as jp


def sample_flight():
    return {
        "Date": "2017-03-04",
        "Codeshare": False,
        "Arrival": {
            "City": "Pittsburgh PA",
            "Airport": "PIT",
            "Country": "United States of",
        },
        "Flight": "DL3377",
        "Departure": {
            "City": "Boston MA",
            "Airport": "BOS",
            "Country": "United States of",
        },
    }


class JSONParserTests(unittest.TestCase):
    def test_write_failure_does_not_report_completed(self):
        original = jp.OutputWriter.close

        def failed_close(writer):
            original(writer)
            if writer.prefix == "json_flights":
                raise OSError("write failed")

        with patch.object(jp.OutputWriter, "close", failed_close):
            stats, *_ = self.parse('{"Forum Profiles": []}')
        self.assertFalse(stats["completed"])
        self.assertFalse(stats["integrity_passed"])

    def test_stream_buffer_discards_previous_profiles(self):
        payload = json.dumps(
            {
                "Forum Profiles": [
                    {"Registered Flights": [], "note": "я" * 300} for _ in range(100)
                ]
            },
            ensure_ascii=False,
        )
        reader = jp.StreamReader(io.StringIO(payload), block_size=64)
        sizes = []
        for index, obj, raw in jp.iter_profiles(reader):
            sizes.append(len(reader.buf))
            self.assertEqual(json.loads(raw), obj)
        self.assertEqual(len(sizes), 100)
        self.assertLess(max(sizes), 1000)

    def parse(self, payload: str, chunk_size: int = 2):
        with tempfile.TemporaryDirectory() as d:
            base = Path(d)
            inp = base / "example.json"
            inp.write_text(payload, encoding="utf-8")
            try:
                stats = jp.process_file(
                    inp, output_dir=base, fmt="csv", chunk_size=chunk_size
                )
            except RuntimeError:
                summary = json.loads((base / "json_summary.json").read_text())
                return summary, None, None, None

            def records(name):
                with (base / name).open(encoding="utf-8-sig", newline="") as f:
                    return list(csv.DictReader(f))

            return (
                stats,
                records("json_flights.csv"),
                records("json_profiles.csv"),
                records("json_issues.csv"),
            )

    def test_two_profiles_multiple_flights(self):
        payload = json.dumps(
            {
                "Forum Profiles": [
                    {
                        "Nickname": "xx",
                        "Registered Flights": [sample_flight(), sample_flight()],
                    },
                    {"Registered Flights": [sample_flight()]},
                ]
            },
            ensure_ascii=False,
        )
        stats, flights, profiles, issues = self.parse(payload)
        self.assertTrue(stats["integrity_passed"])
        self.assertEqual((stats["profiles"], stats["flights_written"]), (2, 3))
        self.assertEqual(
            (flights[0]["Departure_Airport"], flights[0]["Arrival_Airport"]),
            ("BOS", "PIT"),
        )
        self.assertIn("Nickname", profiles[0]["metadata_json"])
        self.assertFalse(issues)

    def test_missing_flights_list_is_kept_in_profiles(self):
        stats, flights, profiles, issues = self.parse(
            '{"Forum Profiles":[{"Name":"a"}]}'
        )
        self.assertTrue(stats["integrity_passed"])
        self.assertEqual(len(profiles), 1)
        self.assertEqual(len(flights), 0)
        self.assertEqual(issues[0]["issue_type"], "missing_flights_list")

    def test_non_object_flight_kept_as_row(self):
        stats, flights, profiles, issues = self.parse(
            '{"Forum Profiles":[{"Registered Flights":[null,"broken",12]}]}'
        )
        self.assertEqual(len(flights), 3)
        self.assertEqual(len(issues), 3)
        self.assertTrue(all(row["parse_status"] == "review" for row in flights))
        self.assertTrue(stats["integrity_passed"])

    def test_missing_nested_fields_keeps_flight(self):
        stats, flights, profiles, issues = self.parse(
            '{"Forum Profiles":[{"Registered Flights":[{"Date":"BAD", "Departure":[] }]}]}'
        )
        self.assertEqual(len(flights), 1)
        self.assertEqual(flights[0]["Date"], "BAD")
        types = {x["issue_type"] for x in issues}
        self.assertIn("invalid_date", types)
        self.assertIn("invalid_nested_object", types)
        self.assertIn("missing_field", types)

    def test_duplicate_registered_flights_is_not_dropped(self):
        flight = json.dumps(sample_flight(), ensure_ascii=False)
        payload = (
            '{"Forum Profiles":[{"Registered Flights":['
            + flight
            + '],"Registered Flights":['
            + flight
            + "]}]}".replace(
                '"Registered Flights":[' + flight + "]}]}",
                '"Registered Flights":[' + flight + "]}]}",
            )
        )
        stats, flights, profiles, issues = self.parse(payload)
        self.assertEqual(stats["flights_written"], 2)
        self.assertTrue(any(e["issue_type"] == "duplicate_json_key" for e in issues))
        self.assertEqual([x["flight_group_index"] for x in flights], ["0", "1"])
        self.assertEqual(
            profiles[0]["raw_profile_json"].count('"Registered Flights"'), 2
        )

    def test_duplicate_flight_key_flagged(self):
        payload = '{"Forum Profiles":[{"Registered Flights":[{"Flight":"SU1","Flight":"SU2", "Date":"2017-01-01", "Codeshare":true, "Arrival":{}, "Departure":{}}]}]}'
        stats, flights, profiles, issues = self.parse(payload)
        self.assertEqual(flights[0]["Flight"], "")
        self.assertIn('"Flight":"SU1"', flights[0]["raw_flight_json"])
        self.assertTrue(
            any(
                e["issue_type"] == "duplicate_json_key" and e["field"] == "Flight"
                for e in issues
            )
        )
        self.assertIn('"Flight":"SU1"', profiles[0]["raw_profile_json"])

    def test_invalid_json_report_incomplete(self):
        stats, *_ = self.parse(
            '{"Forum Profiles":[{"Registered Flights":[]}, INVALID ]}'
        )
        self.assertFalse(stats["completed"])
        self.assertFalse(stats["integrity_passed"])
        self.assertEqual(stats["profiles"], 1)

    def test_extra_fields_preserved(self):
        item = sample_flight()
        item["seat_count"] = 20
        stats, flights, profiles, issues = self.parse(
            json.dumps({"Forum Profiles": [{"Registered Flights": [item]}]})
        )
        self.assertIn("seat_count", flights[0]["extra_fields_json"])

    def test_empty_array_still_complete(self):
        stats, flights, profiles, issues = self.parse('{"Forum Profiles":[]}')
        self.assertTrue(stats["integrity_passed"])
        self.assertEqual(stats["profiles"], 0)

    def test_root_extra_preserved(self):
        stats, flights, profiles, issues = self.parse(
            '{"Extra metadata":{"version":2},"Forum Profiles":[]}'
        )
        self.assertTrue(stats["integrity_passed"])
        self.assertEqual(stats["root_extra_members"], 1)

    def test_unknown_root_key_does_not_success_silently(self):
        stats, *_ = self.parse('{"Other Profiles":[]}')
        self.assertFalse(stats["completed"])

    def test_codeshare_invalid_preserved(self):
        item = sample_flight()
        item["Codeshare"] = "false"
        stats, flights, profiles, issues = self.parse(
            json.dumps({"Forum Profiles": [{"Registered Flights": [item]}]})
        )
        self.assertEqual(flights[0]["Codeshare"], "")
        self.assertTrue(any(e["field"] == "Codeshare" for e in issues))
        self.assertIn('"Codeshare":"false"', flights[0]["raw_flight_json"])


if __name__ == "__main__":
    unittest.main()
