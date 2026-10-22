import unittest

import duckdb

from pipelines.profile_patterns import build_tables


class ProfilePatternTests(unittest.TestCase):
    def setUp(self):
        self.db = duckdb.connect()
        self.db.execute("""CREATE TABLE entities AS SELECT * FROM (VALUES
          ('a','json','native_profile'),('b','xml','native_user'),
          ('c','json','native_profile'),('account','yaml','account_reference'))
          t(entity_id,source,entity_kind)""")
        self.db.execute("""CREATE TABLE assignments AS SELECT * FROM (VALUES
          ('a','g','provisional_consistent'),('b','g','provisional_consistent'),
          ('c',NULL,'held_for_review'),('account',NULL,'unlinked'))
          t(entity_id,provisional_group_id,status)""")
        self.db.execute("""CREATE TABLE observations(entity_id VARCHAR,source VARCHAR,
          date_key DATE,flight_key VARCHAR,origin_key VARCHAR,destination_key VARCHAR,local_time_key VARCHAR)""")
        self.db.execute("""INSERT INTO observations VALUES
          ('a','json','2017-01-01','SU1','SVO','CDG',NULL),
          ('a','json','2017-01-01','SU1','SVO','CDG',NULL),
          ('b','xml','2017-01-01','SU1','SVO','CDG',NULL),
          ('a','json','2017-01-08','SU2','CDG','SVO',NULL),
          ('c','json','2017-01-01','SU1','AER','SVO',NULL),
          ('account','yaml','2017-01-01','SU1','AER','SVO',NULL)""")

    def tearDown(self):
        self.db.close()

    def test_duplicates_and_source_overlap_are_not_extra_flight_days(self):
        build_tables(self.db)
        row = self.db.execute(
            "SELECT flight_days,raw_observations,peak7_flight_days FROM all_features WHERE unit_id='group:g'"
        ).fetchone()
        self.assertEqual(row, (2, 4, 1))
        self.assertEqual(
            self.db.execute(
                "SELECT count(*) FROM unit_observations WHERE unit_id='group:g'"
            ).fetchone()[0],
            4,
        )

    def test_held_profile_remains_local_and_accounts_are_not_people(self):
        build_tables(self.db)
        self.assertEqual(self.db.execute("SELECT count(*) FROM units").fetchone()[0], 4)
        self.assertEqual(
            self.db.execute(
                "SELECT count(*) FROM units WHERE unit_id='profile:c'"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(
            self.db.execute(
                "SELECT count(*) FROM unit_members WHERE entity_id='c' AND unit_kind='preliminary_group'"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.db.execute(
                "SELECT count(*) FROM units WHERE unit_id='profile:account'"
            ).fetchone()[0],
            0,
        )
        self.assertTrue(
            self.db.execute(
                "SELECT bool_and(NOT model_eligible) FROM all_features"
            ).fetchone()[0]
        )
        self.assertIsNone(
            self.db.execute(
                "SELECT rare_route_share FROM all_features WHERE unit_id='profile:c'"
            ).fetchone()[0]
        )

    def test_conflicting_routes_are_not_resolved_by_selecting_one(self):
        self.db.execute("UPDATE observations SET origin_key='AER' WHERE entity_id='b'")
        build_tables(self.db)
        row = self.db.execute(
            "SELECT route_variants,origin,destination FROM slots WHERE unit_id='group:g' AND flight_key='SU1'"
        ).fetchone()
        self.assertEqual(row, (2, None, None))
        self.assertEqual(
            self.db.execute(
                "SELECT routed_flight_days,route_conflict_flight_days FROM all_features WHERE unit_id='group:g'"
            ).fetchone(),
            (1, 1),
        )


if __name__ == "__main__":
    unittest.main()
