import unittest

import duckdb

from pipelines.normalize_sources import build


class NormalizationTests(unittest.TestCase):
    def setUp(self):
        self.db = duckdb.connect()
        self.db.execute("""CREATE TABLE observations (
          source VARCHAR, flight_raw VARCHAR, ticket_number VARCHAR,
          booking_reference VARCHAR, local_time VARCHAR, origin VARCHAR,
          destination VARCHAR, date DATE, date_raw VARCHAR,
          airport_marker_ambiguity BOOLEAN)""")
        self.db.execute("""CREATE TABLE identifier_assertions (
          identifier_type VARCHAR, namespace VARCHAR, value VARCHAR, usable BOOLEAN)""")

    def tearDown(self):
        self.db.close()

    def test_source_specific_flag_split_and_preserved_raw(self):
        for source, flight in [
            ("tab", "SU1181NO"),
            ("json", "SU1181NO"),
            ("csv", "su 0012"),
        ]:
            self.db.execute(
                "INSERT INTO observations VALUES (?,?, '00 0123', ' abc12 ', '03:10', 'SVO', 'CDG', '2017-01-01', '2017-01-01', false)",
                [source, flight],
            )
        build(self.db)
        rows = self.db.execute(
            "SELECT source,flight_raw,flight_key,source_flight_flag,ticket_key FROM normalized_observations ORDER BY source"
        ).fetchall()
        self.assertEqual(
            rows,
            [
                ("csv", "su 0012", "SU0012", None, "000123"),
                ("json", "SU1181NO", None, None, "000123"),
                ("tab", "SU1181NO", "SU1181", "NO", "000123"),
            ],
        )

    def test_uncertainty_is_not_repaired_by_guessing(self):
        self.db.execute(
            "INSERT INTO observations VALUES ('tab','SU12MAYBE','12-X','N/A','25:00','NAN','CDG',NULL,'01/02/2017',true)"
        )
        build(self.db)
        self.assertEqual(
            self.db.execute(
                "SELECT flight_key,ticket_key,booking_key,local_time_key,origin_key,date_key,route_status FROM normalized_observations"
            ).fetchone(),
            (None, None, None, None, None, None, "literal_marker_ambiguous"),
        )

    def test_card_namespace_and_upstream_rejection_remain_separate(self):
        self.db.execute(
            "INSERT INTO identifier_assertions VALUES ('loyalty','su','0001',true),('loyalty','dt','0001',true),('loyalty','su','0001',false)"
        )
        build(self.db)
        rows = self.db.execute(
            "SELECT namespace_key,value_key,key_usable,identifier_key FROM normalized_identifiers ORDER BY namespace_key,key_usable"
        ).fetchall()
        self.assertEqual(
            [(r[0], r[1], r[2]) for r in rows],
            [("DT", "0001", True), ("SU", "0001", False), ("SU", "0001", True)],
        )
        self.assertIsNone(rows[1][3])
        self.assertNotEqual(rows[0][3], rows[2][3])


if __name__ == "__main__":
    unittest.main()
