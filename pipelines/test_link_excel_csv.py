import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

import pandas as pd
from link_excel_csv import compare_names, run, sha256


class ObservationLinkTests(unittest.TestCase):
    def test_name_order_and_incomplete_name_are_distinguished(self):
        row = {
            "excel_name_key": "PERSON TEST",
            "csv_name_key": "TEST PERSON",
            "csv_first_last_key": "PERSON TEST",
        }
        self.assertEqual(compare_names(row), "same_full_name_tokens")
        row["csv_name_key"] = "TEST PERSON MIDDLE"
        self.assertEqual(compare_names(row), "only_first_and_last_name_agree")
        row["excel_name_key"] = "OTHER PERSON"
        self.assertEqual(compare_names(row), "full_name_components_unresolved")

    def test_exact_links_conflicts_and_missing_keys(self):
        day = date(2017, 1, 1)
        excel_base = dict(
            departure_date=day,
            flight_number="SU100",
            passenger_name="TEST PERSON",
            booking_reference="ABC123",
            departure_time="10:00",
            origin_airport="SVO",
            destination_airport="LED",
            parse_status="ok",
        )
        csv_base = dict(
            flight_date=day,
            flight_number="SU100",
            passenger_last_name="TEST",
            passenger_first_name="PERSON",
            passenger_second_name=None,
            booking_code="ABC123",
            flight_time="10:00",
            passenger_document="00001",
        )
        excel = [
            dict(excel_base, record_id="e1", ticket_number="00001"),
            dict(excel_base, record_id="e2", ticket_number="00002"),
            dict(excel_base, record_id="e3", ticket_number=None),
            dict(excel_base, record_id="e4", ticket_number="00003"),
        ]
        csv = [
            dict(csv_base, record_id="c1", ticket_number="00001"),
            dict(csv_base, record_id="c2", ticket_number="00002"),
            dict(
                csv_base,
                record_id="c3",
                ticket_number="00002",
                passenger_document="00002",
            ),
            dict(csv_base, record_id="c4", ticket_number=None),
            dict(
                csv_base,
                record_id="c5",
                ticket_number="00003",
                booking_code="DIFFERENT",
            ),
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ep, cp = root / "excel.parquet", root / "csv.parquet"
            pd.DataFrame(excel).to_parquet(ep, index=False)
            pd.DataFrame(csv).to_parquet(cp, index=False)
            ep.with_suffix(".manifest.json").write_text(
                json.dumps(
                    {
                        "complete": True,
                        "output_sha256": sha256(ep),
                        "input_sha256": "synthetic",
                    }
                )
            )
            report = run(ep, cp, root / "links")
            output = pd.read_parquet(root / "links/excel_csv_candidates.parquet")
            self.assertEqual(report["candidate_pairs"], 4)
            self.assertEqual(report["statuses"]["supported_observation_link"], 1)
            self.assertEqual(report["statuses"]["review"], 3)
            self.assertNotIn("e3", output.excel_record_id.to_list())
            self.assertEqual(len(output.loc[output.excel_record_id == "e2"]), 2)
            self.assertTrue(
                all(
                    "conflicting_documents_in_block" in r
                    for r in output.loc[output.excel_record_id == "e2", "reasons_json"]
                )
            )
            self.assertIn(
                "different_known_pnr_key",
                output.loc[output.excel_record_id == "e4", "reasons_json"].iloc[0],
            )
            self.assertNotIn("passenger_document", output.columns)
            self.assertIsNone(report["precision"])
            self.assertIsNone(report["recall"])


if __name__ == "__main__":
    unittest.main()
