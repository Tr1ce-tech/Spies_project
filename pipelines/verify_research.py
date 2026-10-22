"""Independent read-back, source-ID reconciliation and group invariant checks."""

import argparse
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from research.common import dump, literal, sha

ROOT = Path(__file__).resolve().parents[1]


def verify(output, root=ROOT):
    output = Path(output)
    manifest = json.loads((output / "manifest.json").read_text())
    assert manifest["complete"]
    for path, digest in manifest["input_hashes"].items():
        assert sha(root / path) == digest, path
    for path, digest in manifest["code_hashes"].items():
        assert sha(root / path) == digest, path
    db = duckdb.connect()
    db.execute("SET memory_limit='2GB'")
    files = {
        "entities": "source_entities",
        "ids": "identifier_assertions",
        "obs": "observations",
        "assignments": "entity_assignments",
        "candidate_groups": "preliminary_groups",
        "members": "candidate_block_members",
        "blocks": "candidate_identifier_blocks",
        "coverage": "source_coverage",
        "daily": "daily_counts",
        "routes": "routes",
        "labels": "review_labels",
    }
    for table, filename in files.items():
        db.execute(
            f"CREATE VIEW {table} AS SELECT * FROM read_parquet({literal(output / (filename + '.parquet'))})"
        )
    counts = {}
    for table, key in [
        ("entities", "entity_id"),
        ("ids", "assertion_id"),
        ("obs", "observation_id"),
        ("assignments", "entity_id"),
        ("candidate_groups", "candidate_component_id"),
    ]:
        total, distinct, present = db.execute(
            f"SELECT count(*),count(distinct {key}),count({key}) FROM {table}"
        ).fetchone()
        assert total == distinct == present, table
        assert pq.ParquetFile(output / (files[table] + ".parquet")).schema_arrow.field(
            key
        ).type in (pa.string(), pa.large_string())
        counts[table] = total
    assert counts["entities"] == counts["assignments"] == manifest["counts"]["entities"]
    assert (
        db.execute(
            "SELECT count(*) FROM obs ANTI JOIN entities USING(entity_id)"
        ).fetchone()[0]
        == 0
    )
    assert (
        db.execute(
            "SELECT count(*) FROM ids ANTI JOIN entities USING(entity_id)"
        ).fetchone()[0]
        == 0
    )
    assert (
        db.execute(
            "SELECT count(*) FROM assignments WHERE status!='provisional_consistent' AND provisional_group_id IS NOT NULL"
        ).fetchone()[0]
        == 0
    )
    assert (
        db.execute(
            "SELECT count(*) FROM candidate_groups WHERE provisional_group_id IS NOT NULL AND reasons_json!='[]'"
        ).fetchone()[0]
        == 0
    )
    assert (
        db.execute(
            "SELECT count(*) FROM (SELECT block_id,count(distinct a.candidate_component_id) n FROM members m JOIN assignments a USING(entity_id) GROUP BY block_id HAVING n!=1)"
        ).fetchone()[0]
        == 0
    )
    expected = [
        ("csv", "CSV_Parser/result/records.parquet", "record_id", "source_sha256"),
        (
            "tab",
            "Tab_Parser/result/flights_part_*.parquet",
            "record_id",
            "source_sha256",
        ),
        (
            "excel",
            "Excel_Tickets/result/tickets.parquet",
            "record_id",
            "source_archive_sha256",
        ),
        (
            "json",
            "JSON_Parser/result/json_flights_part_*.parquet",
            "record_id",
            "source_sha256",
        ),
        ("xml", "XML_Parser/result/activities.parquet", "record_id", "source_sha256"),
        (
            "yaml",
            "YAML_Parser/result/loyalty_entries.parquet",
            "entry_id",
            "source_sha256",
        ),
    ]
    queries = []
    expected_counts = {}
    fields = {
        "csv": ("flight_date", "flight_number", None, None),
        "tab": ("DepartDate", "FlightCode", "From", "Dest"),
        "excel": (
            "departure_date",
            "flight_number",
            "origin_airport",
            "destination_airport",
        ),
        "json": ("Date", "Flight", "Departure_Airport", "Arrival_Airport"),
        "xml": ("flight_date", "flight_number", "departure_iata", "arrival_iata"),
        "yaml": ("date", "flight_number", "origin", "destination"),
    }
    for source, pattern, key, fingerprint in expected:
        paths = sorted((root / "parsers").glob(pattern))
        expected_counts[source] = sum(
            pq.ParquetFile(p).metadata.num_rows for p in paths
        )
        query = "read_parquet([" + ",".join(literal(p) for p in paths) + "])"
        date, flight, origin, destination = fields[source]

        def column(value):
            return '"' + value + '"' if value else "NULL::VARCHAR"

        queries.append(
            f"""SELECT '{source}' AS source_name,{key} raw_id,{fingerprint} source_hash,
            try_cast({column(date)} AS DATE) expected_date,
            regexp_replace(upper(trim({column(flight)})),'[[:space:]]+','','g') expected_flight,
            upper(trim({column(origin)})) expected_origin,upper(trim({column(destination)})) expected_destination
            FROM {query}"""
        )
    db.execute("CREATE VIEW originals AS " + " UNION ALL ".join(queries))
    condition = "o.source=s.source_name AND o.source_record_id=s.raw_id AND o.source_sha256=s.source_hash"
    assert (
        db.execute(
            f"SELECT count(*) FROM obs o ANTI JOIN originals s ON {condition}"
        ).fetchone()[0]
        == 0
    )
    assert (
        db.execute(
            f"SELECT count(*) FROM originals s ANTI JOIN obs o ON {condition}"
        ).fetchone()[0]
        == 0
    )
    actual_counts = dict(
        db.execute("SELECT source,count(*) FROM obs GROUP BY source").fetchall()
    )
    assert actual_counts == expected_counts
    mismatches = db.execute(f"""SELECT count(*) FROM obs o JOIN originals s ON {condition}
      WHERE (s.expected_date IS NOT NULL AND o.date IS DISTINCT FROM s.expected_date)
      OR (s.expected_flight IS NOT NULL AND o.flight IS DISTINCT FROM s.expected_flight)
      OR (regexp_full_match(s.expected_origin,'[A-Z]{{3}}') AND o.origin IS DISTINCT FROM s.expected_origin)
      OR (regexp_full_match(s.expected_destination,'[A-Z]{{3}}') AND o.destination IS DISTINCT FROM s.expected_destination)
      """).fetchone()[0]
    assert mismatches == 0, (
        "Canonical fields differ from known parser fields",
        mismatches,
    )
    assert db.execute("SELECT sum(rows) FROM coverage").fetchone()[0] == counts["obs"]
    for source, total in expected_counts.items():
        assert (
            db.execute(
                "SELECT sum(observations) FROM daily WHERE source=?", [source]
            ).fetchone()[0]
            == total
        )
        known = db.execute(
            "SELECT count(*) FROM obs WHERE source=? AND valid_airport_pair", [source]
        ).fetchone()[0]
        route_total = db.execute(
            "SELECT coalesce(sum(observations),0) FROM routes WHERE source=?", [source]
        ).fetchone()[0]
        assert known == route_total
    xml_pattern = r"^([A-Z0-9]{2,8})\s+([0-9]+)$"
    bad_xml = db.execute(
        """SELECT count(*) FROM ids WHERE source='xml' AND usable AND
      (namespace!=regexp_extract(trim(raw_value),?,1) OR value!=regexp_extract(trim(raw_value),?,2))""",
        [xml_pattern, xml_pattern],
    ).fetchone()[0]
    assert bad_xml == 0
    assert (
        db.execute(
            "SELECT count(*) FROM ids WHERE identifier_type='loyalty' AND usable AND (namespace IS NULL OR value IS NULL)"
        ).fetchone()[0]
        == 0
    )
    accepted = db.execute("""SELECT a.provisional_group_id,e.document_key,e.birth_dates_json
      FROM assignments a JOIN entities e USING(entity_id) WHERE a.status='provisional_consistent'
      ORDER BY a.provisional_group_id""").to_arrow_reader(8192)
    current, documents, allowed = None, set(), None
    checked_groups = 0
    for batch in accepted:
        for group, doc, dates in zip(*(batch.column(i).to_pylist() for i in range(3))):
            if group != current:
                if current is not None:
                    assert len(documents) <= 1 and (allowed is None or allowed)
                    checked_groups += 1
                current, documents, allowed = group, set(), None
            if doc:
                documents.add(doc)
            values = set(json.loads(dates))
            if values:
                allowed = values if allowed is None else allowed.intersection(values)
    if current is not None:
        assert len(documents) <= 1 and (allowed is None or allowed)
        checked_groups += 1
    assert (
        db.execute(
            "SELECT count(*) FROM labels WHERE reviewer_label IS NOT NULL"
        ).fetchone()[0]
        == 0
    )
    review = pq.ParquetFile(output / "review_case_members.parquet").metadata.num_rows
    db.close()
    result = {
        "all_checks_passed": True,
        "counts": counts,
        "observations_by_source": actual_counts,
        "all_source_observation_ids_and_hashes_preserved": True,
        "all_identifier_and_observation_foreign_keys_valid": True,
        "daily_and_route_aggregates_reconciled": True,
        "canonical_dates_flights_and_known_airport_codes_reconciled": True,
        "explicit_xml_program_and_number_rechecked": True,
        "accepted_components_checked_for_document_and_birth_constraints": checked_groups,
        "review_member_rows": review,
        "independent_truth_labels": 0,
        "scope": "Technical read-back and independent invariant checks; not empirical linkage accuracy",
        "output_parquet_hashes": {
            p.name: sha(p) for p in sorted(output.glob("*.parquet"))
        },
    }
    dump(output / "verification.json", result)
    print(
        json.dumps(
            {k: v for k, v in result.items() if k != "output_parquet_hashes"}, indent=2
        )
    )
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "output", type=Path, nargs="?", default=ROOT / "proceed_data/research_v1"
    )
    args = parser.parse_args()
    verify(args.output)
