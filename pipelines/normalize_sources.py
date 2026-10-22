import argparse
import json
import time
from pathlib import Path

import duckdb

from pipelines.research.common import dump, export, literal, sha

VERSION = "cross-source-normalization.v1"
INPUTS = ("observations", "source_entities", "identifier_assertions")


def build(db):
    db.execute("""CREATE VIEW normalized_observations AS WITH prepared AS (
      SELECT *, upper(regexp_replace(trim(nfc_normalize(flight_raw)),
        '[[:space:]]+', '', 'g')) compact_flight,
        regexp_replace(trim(ticket_number),'[[:space:]]+','','g') compact_ticket,
        upper(trim(nfc_normalize(booking_reference))) compact_booking,
        source='tab' AND regexp_full_match(compact_flight,
          '[A-Z0-9]{2}[0-9]{1,4}(YES|NO)') tab_compound
      FROM observations), keys AS (
      SELECT *, CASE WHEN tab_compound THEN
          regexp_replace(compact_flight,'(YES|NO)$','')
        WHEN regexp_full_match(compact_flight,'[A-Z0-9]{2}[0-9]{1,4}[A-Z]?')
          THEN compact_flight END flight_key,
        CASE WHEN tab_compound THEN regexp_extract(compact_flight,'(YES|NO)$',1) END source_flight_flag,
        CASE WHEN regexp_full_match(compact_ticket,'[0-9]+') THEN compact_ticket END ticket_key,
        CASE WHEN regexp_full_match(compact_booking,'[A-Z0-9]+') THEN compact_booking END booking_key,
        CASE WHEN regexp_full_match(trim(local_time),'([01][0-9]|2[0-3]):[0-5][0-9]')
          THEN trim(local_time) END local_time_key,
        CASE WHEN regexp_full_match(origin,'[A-Z]{3}') AND
          origin NOT IN ('NAN') THEN origin END origin_key,
        CASE WHEN regexp_full_match(destination,'[A-Z]{3}') AND
          destination NOT IN ('NAN') THEN destination END destination_key
      FROM prepared)
      SELECT * EXCLUDE(compact_flight,compact_ticket,compact_booking,tab_compound),
        date date_key,
        CASE WHEN date IS NOT NULL THEN 'iso_calendar_valid'
          WHEN date_raw IS NULL OR trim(date_raw)='' THEN 'missing' ELSE 'unresolved' END date_status,
        CASE WHEN tab_compound THEN 'source_compound_split'
          WHEN flight_key IS NOT NULL THEN 'syntax_only_unverified'
          WHEN flight_raw IS NULL OR trim(flight_raw)='' THEN 'missing' ELSE 'unresolved' END flight_status,
        CASE WHEN ticket_key IS NOT NULL THEN 'digits_only_unverified'
          WHEN ticket_number IS NULL THEN 'missing' ELSE 'unresolved' END ticket_status,
        CASE WHEN booking_key IS NOT NULL THEN 'source_scoped_only'
          WHEN booking_reference IS NULL THEN 'missing' ELSE 'unresolved' END booking_status,
        CASE WHEN local_time_key IS NOT NULL THEN 'local_clock_only'
          WHEN local_time IS NULL THEN 'missing' ELSE 'unresolved' END local_time_status,
        CASE WHEN airport_marker_ambiguity THEN 'literal_marker_ambiguous'
          WHEN origin_key IS NOT NULL AND destination_key IS NOT NULL
          THEN 'syntax_only_unverified' ELSE 'missing_or_nonstandard' END route_status,
        'cross-source-normalization.v1' normalization_rule
      FROM keys""")
    db.execute("""CREATE VIEW normalized_identifiers AS WITH keys AS (
      SELECT *, CASE WHEN identifier_type='loyalty' THEN upper(trim(nfc_normalize(namespace)))
        ELSE namespace END namespace_key,
        CASE WHEN identifier_type='loyalty' AND
          regexp_full_match(regexp_replace(trim(value),'[[:space:]]+','','g'),'[0-9]+')
          THEN regexp_replace(trim(value),'[[:space:]]+','','g')
          WHEN identifier_type='document' THEN value END value_key
      FROM identifier_assertions)
      SELECT *, usable AND value_key IS NOT NULL AND namespace_key IS NOT NULL AND
        (identifier_type='document' OR regexp_full_match(namespace_key,'[A-Z0-9]{2,8}')) key_usable,
        CASE WHEN key_usable THEN sha256(to_json([identifier_type,namespace_key,value_key])) END identifier_key,
        CASE WHEN key_usable THEN 'upstream_eligible_syntax_only' ELSE 'unresolved_or_upstream_ineligible' END key_status,
        'cross-source-normalization.v1' normalization_rule
      FROM keys""")


def block_summary(db, table, key, condition):
    return db.execute(f"""WITH blocks AS (
      SELECT {key}, count(DISTINCT source) sources, count(*) members
      FROM {table} WHERE {condition} GROUP BY {key})
      SELECT count(*) FILTER(WHERE sources>=2),
        coalesce(sum(members) FILTER(WHERE sources>=2),0)
      FROM blocks""").fetchone()


def run(root, input_dir, output):
    if output.exists() or output.with_name(output.name + ".partial").exists():
        raise FileExistsError(f"Use a new output directory: {output}")
    partial = output.with_name(output.name + ".partial")
    partial.mkdir(parents=True)
    started = time.perf_counter()
    paths = {label: input_dir / f"{label}.parquet" for label in INPUTS}
    tab_paths = sorted(
        (root / "parsers/Tab_Parser/result").glob("flights_part_*.parquet")
    )
    if not tab_paths:
        raise FileNotFoundError(
            "TAB parser output is required to verify the split rule"
        )
    paths.update({f"tab_part_{n}": p for n, p in enumerate(tab_paths)})
    hashes = {str(p.relative_to(root)): sha(p) for p in paths.values()}
    db = duckdb.connect()
    db.execute("SET memory_limit='2GB'")
    db.execute("SET threads=4")
    db.execute(f"SET temp_directory={literal(partial / 'temp')}")
    for label, path in paths.items():
        db.execute(
            f"CREATE VIEW {label} AS SELECT * FROM read_parquet({literal(path)})"
        )
    build(db)
    checks = {}
    tab_union = " UNION ALL ".join(
        f"SELECT * FROM tab_part_{n}" for n in range(len(tab_paths))
    )
    db.execute(f"CREATE VIEW tab_evidence AS {tab_union}")
    checks["tab_split_matches_source_region"] = db.execute("""SELECT
      count(*)=(SELECT count(*) FROM normalized_observations WHERE source='tab') AND
      count(*) FILTER(WHERE t.Sh='' AND t.source_sha256=o.source_sha256 AND
        regexp_full_match(trim(substring(t.raw_line,121,12)),
          '[A-Z0-9]{2}[0-9]{1,4}(YES|NO)') AND
        trim(substring(t.raw_line,121,12))=o.flight_raw AND
        regexp_replace(trim(substring(t.raw_line,121,12)),'(YES|NO)$','')=o.flight_key AND
        regexp_extract(trim(substring(t.raw_line,121,12)),'(YES|NO)$',1)=o.source_flight_flag)=count(*)
      FROM normalized_observations o JOIN tab_evidence t ON o.source_record_id=t.record_id
      WHERE o.source='tab'""").fetchone()[0]
    for source, target, identifier in [
        ("observations", "normalized_observations", "observation_id"),
        ("identifier_assertions", "normalized_identifiers", "assertion_id"),
    ]:
        export(db, f"SELECT * FROM {target}", partial / f"{target}.parquet")
        reread = f"read_parquet({literal(partial / (target + '.parquet'))})"
        checks[target] = db.execute(f"""SELECT
          (SELECT count(*) FROM {source})=count(*) AND count(*)=count(DISTINCT {identifier})
          AND count(*) FILTER(WHERE normalization_rule IS NULL)=0 FROM {reread}""").fetchone()[0]
        checks[target + "_payload_preserved"] = db.execute(f"""SELECT count(*)=0 FROM (
          (SELECT * FROM {source} EXCEPT ALL SELECT COLUMNS(*) FROM
            (SELECT {",".join(literal_column(r[0]) for r in db.execute("DESCRIBE " + source).fetchall())} FROM {reread})))""").fetchone()[0]
    export(db, "SELECT * FROM source_entities", partial / "source_entities.parquet")
    export(
        db,
        """SELECT source,flight_status,route_status,ticket_status,local_time_status,
      count(*) observations FROM normalized_observations GROUP BY ALL""",
        partial / "quality_summary.parquet",
    )
    summary = {}
    for label, table, key, where in [
        (
            "before",
            "observations",
            "ticket_number,date,flight",
            "ticket_number IS NOT NULL AND date IS NOT NULL AND flight IS NOT NULL",
        ),
        (
            "after",
            "normalized_observations",
            "ticket_key,date_key,flight_key",
            "ticket_key IS NOT NULL AND date_key IS NOT NULL AND flight_key IS NOT NULL",
        ),
    ]:
        blocks, members = block_summary(db, table, key, where)
        summary[label] = {
            "cross_source_ticket_date_flight_blocks": blocks,
            "observation_members": members,
        }
    export(
        db,
        """SELECT ticket_key,date_key,flight_key,count(*) observations,
      count(DISTINCT source) sources, list(DISTINCT source ORDER BY source) source_list,
      count(DISTINCT to_json([origin_key,destination_key])) FILTER(
        WHERE origin_key IS NOT NULL AND destination_key IS NOT NULL) known_route_variants,
      count(DISTINCT local_time_key) known_local_time_variants,
      'candidate_only_not_identity_or_completed_trip' status,
      'cross-source-normalization.v1' normalization_rule
      FROM normalized_observations WHERE ticket_key IS NOT NULL AND date_key IS NOT NULL
      AND flight_key IS NOT NULL GROUP BY ticket_key,date_key,flight_key
      HAVING count(DISTINCT source)>=2""",
        partial / "trip_candidate_blocks.parquet",
    )
    summary["tab_split_rows"] = db.execute(
        "SELECT count(*) FROM normalized_observations WHERE flight_status='source_compound_split'"
    ).fetchone()[0]
    summary["candidate_conflicts"] = db.execute(
        f"""SELECT count(*) FILTER(WHERE known_route_variants>1),
        count(*) FILTER(WHERE known_local_time_variants>1)
        FROM read_parquet({literal(partial / "trip_candidate_blocks.parquet")})"""
    ).fetchone()
    summary["identifier_key_changes"] = (
        db.execute("""SELECT count(*) FROM normalized_identifiers
      WHERE namespace_key IS DISTINCT FROM namespace OR value_key IS DISTINCT FROM value""").fetchone()[
            0
        ]
    )
    checks["input_hashes_unchanged"] = all(
        sha(p) == hashes[str(p.relative_to(root))] for p in paths.values()
    )
    checks["all_passed"] = all(checks.values())
    dump(partial / "verification.json", checks)
    if not checks["all_passed"]:
        raise ValueError(checks)
    dump(
        partial / "manifest.json",
        {
            "normalization_rule": VERSION,
            "input_hashes": hashes,
            "code_sha256": sha(Path(__file__)),
            "elapsed_seconds": time.perf_counter() - started,
            "summary": summary,
            "source_entities": "unchanged upstream normalization; original values via source_record_id",
            "labels": "No independent labels; candidate counts are not precision or recall.",
        },
    )
    db.close()
    partial.rename(output)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def literal_column(value):
    return '"' + value.replace('"', '""') + '"'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=Path("proceed_data/research_v1"))
    parser.add_argument(
        "--output", type=Path, default=Path("proceed_data/normalized_v1")
    )
    args = parser.parse_args()
    run(Path.cwd(), args.input.resolve(), args.output.resolve())


if __name__ == "__main__":
    main()
