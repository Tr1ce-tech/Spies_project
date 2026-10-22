import json
import tempfile
import unittest
from pathlib import Path

import duckdb
import pyarrow as pa
from research.common import airport, card, clean, document, install_functions
from research.matching import assess_components, build_blocks, connected_components


class ResearchTests(unittest.TestCase):
    def test_identifiers_preserve_zeroes_and_program_namespace(self):
        self.assertEqual(document(" 0012 000034 "), "0012000034")
        self.assertEqual(document("AB-0012"), "AB-0012")
        self.assertEqual(card("FF#DT 000123"), ("DT", "000123"))
        self.assertEqual(card("Delta SkyMiles 000123"), (None, None))
        self.assertIsNone(clean("N/A"))
        self.assertIsNone(clean(None))
        self.assertEqual(airport("NAN"), "NAN")
        self.assertEqual(airport(" rea "), "REA")
        self.assertIsNone(airport(None))

    def analyze(self, entries, keys):
        with tempfile.TemporaryDirectory() as directory:
            db = duckdb.connect()
            install_functions(db)
            fields = [
                "entity_id",
                "source",
                "entity_kind",
                "source_record_id",
                "source_sha256",
                "first_name",
                "last_name",
                "middle_name",
                "full_name",
                "sex",
                "birth_dates_json",
                "document_key",
            ]
            rows = []
            for entry in entries:
                row = dict.fromkeys(fields)
                row.update(
                    entity_kind="native_profile",
                    source_record_id=entry["entity_id"],
                    source_sha256="test",
                    first_name="TEST",
                    last_name="PERSON",
                    birth_dates_json="[]",
                )
                row.update(entry)
                rows.append(row)
            schema = pa.schema([(f, pa.string()) for f in fields])
            db.register("input_entities", pa.Table.from_pylist(rows, schema=schema))
            db.execute("CREATE TABLE entities AS SELECT * FROM input_entities")
            identifiers = []
            for n, (entity, kind, namespace, value) in enumerate(keys):
                source = next(x["source"] for x in entries if x["entity_id"] == entity)
                identifiers.append(
                    dict(
                        entity_id=entity,
                        source=source,
                        identifier_type=kind,
                        namespace=namespace,
                        value=value,
                        usable=True,
                        assertion_id=str(n),
                        source_record_id=entity,
                        source_sha256="test",
                    )
                )
            db.register("input_ids", pa.Table.from_pylist(identifiers))
            db.execute("CREATE TABLE identifiers AS SELECT * FROM input_ids")
            build_blocks(db, Path(directory))
            connected_components(db)
            assess_components(db, Path(directory))
            groups = db.execute(
                "SELECT status,reasons_json,entities FROM preliminary_groups"
            ).fetchall()
            assignments = db.execute(
                "SELECT entity_id,status,provisional_group_id FROM assignments"
            ).fetchall()
            db.close()
            return groups, assignments

    def test_consistent_group_and_unknowns_remain_separate(self):
        entries = [
            dict(entity_id="a", source="csv", document_key="00123"),
            dict(entity_id="b", source="tab", document_key="00123"),
            dict(entity_id="unknown1", source="csv"),
            dict(entity_id="unknown2", source="tab"),
        ]
        groups, assignments = self.analyze(
            entries,
            [
                ("a", "document", "untyped_document", "00123"),
                ("b", "document", "untyped_document", "00123"),
            ],
        )
        self.assertEqual(groups, [("provisional_consistent", "[]", 2)])
        unknowns = [r for r in assignments if r[0].startswith("unknown")]
        self.assertEqual(len(unknowns), 2)
        self.assertTrue(all(r[1] == "unlinked" and r[2] is None for r in unknowns))

    def test_bridge_cannot_choose_a_convenient_subgroup(self):
        entries = [
            dict(entity_id="a", source="csv", document_key="00123"),
            dict(entity_id="b", source="json", document_key="00999"),
            dict(entity_id="c", source="tab", document_key="00999"),
        ]
        keys = [
            ("a", "loyalty", "SU", "0001"),
            ("b", "loyalty", "SU", "0001"),
            ("b", "document", "untyped_document", "00999"),
            ("c", "document", "untyped_document", "00999"),
        ]
        groups, assignments = self.analyze(entries, keys)
        self.assertEqual(groups[0][0], "held_for_review")
        self.assertIn("different_known_documents", json.loads(groups[0][1]))
        self.assertTrue(all(r[2] is None for r in assignments))
        self.assertEqual(groups[0][2], 3)

    def test_date_constraints_intersect_across_entire_group(self):
        entries = [
            dict(
                entity_id="a",
                source="csv",
                birth_dates_json='["2000-01-02","2000-02-01"]',
            ),
            dict(entity_id="b", source="json", birth_dates_json='["2000-02-01"]'),
            dict(entity_id="c", source="tab", birth_dates_json='["2000-01-02"]'),
        ]
        keys = [(e["entity_id"], "loyalty", "SU", "0001") for e in entries]
        groups, _ = self.analyze(entries, keys)
        self.assertEqual(groups[0][0], "held_for_review")
        self.assertIn("disjoint_birth_date_constraints", json.loads(groups[0][1]))
        groups, _ = self.analyze(entries[:2], keys[:2])
        self.assertEqual(groups[0][0], "provisional_consistent")

    def test_different_structured_names_hold_the_complete_group(self):
        entries = [
            dict(entity_id="a", source="csv"),
            dict(entity_id="b", source="json", first_name="OTHER"),
        ]
        keys = [(e["entity_id"], "loyalty", "SU", "0001") for e in entries]
        groups, _ = self.analyze(entries, keys)
        self.assertIn("different_structured_names", json.loads(groups[0][1]))
        self.assertEqual(groups[0][0], "held_for_review")

    def test_known_given_name_conflict_is_checked_even_without_family_name(self):
        entries = [
            dict(entity_id="a", source="csv"),
            dict(entity_id="b", source="json", first_name="OTHER", last_name=None),
        ]
        keys = [(e["entity_id"], "loyalty", "SU", "0001") for e in entries]
        groups, _ = self.analyze(entries, keys)
        self.assertEqual(groups[0][0], "held_for_review")
        self.assertIn("different_structured_names", json.loads(groups[0][1]))


if __name__ == "__main__":
    unittest.main()
