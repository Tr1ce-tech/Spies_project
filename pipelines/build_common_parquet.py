import argparse
import json
import sys
import tempfile
import time
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from pipelines.normalize_sources import build as normalize
from pipelines.research.adapters import (
    build_entities,
    build_identifiers,
    build_observations,
)
from pipelines.research.common import install_functions, literal, sha
from pipelines.research.matching import (
    assess_components,
    build_blocks,
    connected_components,
)
from pipelines.travel_analysis.inputs import discover, forum_fields, register

SCHEMA_VERSION = "travel-common.v1"


def build_dataset(input_dir, output):
    input_dir = Path(input_dir).resolve()
    output = Path(output).resolve()
    if not input_dir.is_dir():
        raise ValueError(f"Input directory does not exist: {input_dir}")
    if output.suffix.lower() != ".parquet":
        raise ValueError("Output must have .parquet extension")
    if output.is_relative_to(input_dir):
        raise ValueError("Keep the output outside the scanned input directory")
    if output.exists():
        raise FileExistsError(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    files, inventory = discover(input_dir)
    input_hashes = {
        str(p.relative_to(input_dir)): sha(p) for paths in files.values() for p in paths
    }
    with tempfile.TemporaryDirectory(
        prefix=".common-build-", dir=output.parent
    ) as directory:
        work = Path(directory)
        db = duckdb.connect()
        db.execute("SET memory_limit='2GB'")
        db.execute("SET threads=4")
        db.execute("SET temp_directory='" + str(work / "temp").replace("'", "''") + "'")
        install_functions(db)
        register(db, files)
        print(
            "Reading parser tables and building source entities",
            file=sys.stderr,
            flush=True,
        )
        forum_fields(db, files["json_profiles"], work / "forum.parquet")
        build_entities(db, work)
        build_identifiers(db, work)
        build_observations(db, work)
        db.execute("CREATE VIEW identifier_assertions AS SELECT * FROM identifiers")
        normalize(db)
        db.execute(
            "CREATE TABLE normalized_id_storage AS SELECT *, namespace namespace_raw, value key_value_raw FROM normalized_identifiers"
        )
        db.execute("DROP VIEW normalized_identifiers")
        db.execute("DROP VIEW identifier_assertions")
        db.execute("DROP TABLE identifiers")
        db.execute("""CREATE TABLE identifiers AS SELECT * EXCLUDE(namespace,value,usable),
          namespace_key AS namespace,value_key AS "value",key_usable AS usable FROM normalized_id_storage""")
        print(
            "Checking candidate components; conflicting components remain unresolved",
            file=sys.stderr,
            flush=True,
        )
        build_blocks(db, work)
        connected_components(db)
        assess_components(db, work)
        keep = {
            "entities",
            "entity_raw",
            "observations",
            "identifiers",
            "assignments",
            "preliminary_groups",
        }
        for (table,) in db.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE'"
        ).fetchall():
            if table not in keep:
                db.execute('DROP TABLE "' + table.replace('"', '""') + '"')
        db.execute("SET threads=2")
        db.execute("SET preserve_insertion_order=false")
        identity_paths = []
        for (source,) in db.execute(
            "SELECT DISTINCT source FROM identifiers ORDER BY source"
        ).fetchall():
            identity_path = work / f"identifiers_{source}.parquet"
            db.execute(f"""COPY (SELECT entity_id,to_json(list(i ORDER BY assertion_id)) identifier_assertions_json
              FROM identifiers i WHERE source={literal(source)} GROUP BY entity_id)
              TO {literal(identity_path)} (FORMAT PARQUET,COMPRESSION ZSTD)""")
            identity_paths.append(identity_path)
        if identity_paths:
            db.execute(
                f"CREATE VIEW entity_identifiers AS SELECT * FROM read_parquet([{','.join(literal(p) for p in identity_paths)}])"
            )
        else:
            db.execute(
                "CREATE VIEW entity_identifiers AS SELECT NULL::VARCHAR entity_id,NULL::VARCHAR identifier_assertions_json WHERE false"
            )
        db.execute("""CREATE VIEW common_rows AS SELECT
          coalesce(o.observation_id,sha256('entity-only:'||e.entity_id)) record_id,
          CASE WHEN o.observation_id IS NULL THEN 'entity_only' ELSE 'observation' END record_kind,
          e.source,e.entity_id,e.entity_kind,e.source_record_id entity_record_id,
          e.source_locator entity_locator,e.source_sha256 entity_source_sha256,
          e.source_rule_version entity_source_rule_version,e.source_status entity_source_status,e.source_records_represented,
          e.first_name,e.last_name,e.middle_name,e.full_name,e.sex,e.birth_dates_json,e.document_raw,e.document_key,
          r.first_name first_name_raw,r.last_name last_name_raw,r.middle_name middle_name_raw,r.full_name full_name_raw,
          a.candidate_component_id,a.provisional_group_id,a.status linkage_status,
          coalesce(g.known_documents,0) candidate_document_count,
          coalesce(g.reasons_json,'[]') component_reasons_json,
          o.observation_id,coalesce(o.source_record_id,e.source_record_id) source_record_id,
          coalesce(o.source_sha256,e.source_sha256) source_sha256,
          coalesce(o.source_rule_version,e.source_rule_version) source_rule_version,
          o.unit,o.source_status,o.date_raw,o.date_key,o.flight_raw,o.flight_key,o.source_flight_flag,
          o.origin_raw,o.origin_key,o.destination_raw,o.destination_key,o.destination_text,
          o.local_time local_time_raw,o.local_time_key,o.ticket_number ticket_raw,o.ticket_key,
          o.booking_reference booking_raw,o.booking_key,
          o.date_status,o.flight_status,o.route_status,o.local_time_status,o.ticket_status,
          o.airport_marker_ambiguity,
          CASE WHEN row_number() OVER(PARTITION BY e.entity_id ORDER BY o.observation_id)=1
            THEN coalesce(i.identifier_assertions_json,'[]') END identifier_assertions_json,
          'source-research.v1' entity_processing_rule,'cross-source-normalization.v1' normalization_rule,
          'travel-common.v1' processing_rule
          FROM entities e LEFT JOIN normalized_observations o USING(entity_id)
          JOIN entity_raw r USING(entity_id) JOIN assignments a USING(entity_id)
          LEFT JOIN preliminary_groups g USING(candidate_component_id)
          LEFT JOIN entity_identifiers i USING(entity_id)""")
        observations = db.execute("SELECT count(*) FROM observations").fetchone()[0]
        entities = db.execute("SELECT count(*) FROM entities").fetchone()[0]
        rows = db.execute("SELECT count(*) FROM common_rows").fetchone()[0]
        assert db.execute(
            "SELECT count(*)=count(DISTINCT record_id) FROM common_rows"
        ).fetchone()[0]
        assert (
            db.execute("SELECT count(DISTINCT entity_id) FROM common_rows").fetchone()[
                0
            ]
            == entities
        )
        assert (
            db.execute("SELECT count(observation_id) FROM common_rows").fetchone()[0]
            == observations
        )
        assert (
            db.execute(
                "SELECT coalesce(sum(json_array_length(identifier_assertions_json)),0) FROM common_rows"
            ).fetchone()[0]
            == db.execute("SELECT count(*) FROM identifiers").fetchone()[0]
        )
        assert (
            db.execute(
                "SELECT count(*) FROM common_rows WHERE provisional_group_id IS NOT NULL AND component_reasons_json!='[]'"
            ).fetchone()[0]
            == 0
        )
        manifest = dict(
            schema_version=SCHEMA_VERSION,
            input_directory=str(input_dir),
            inputs=input_hashes,
            inventory=inventory,
            rows=rows,
            observations=observations,
            source_entities=entities,
            schedules_excluded=True,
            completed_flights_confirmed=False,
            code_hashes={
                str(p.relative_to(Path(__file__).resolve().parents[1])): sha(p)
                for p in [
                    Path(__file__),
                    Path(__file__).with_name("travel_analysis") / "inputs.py",
                    Path(__file__).with_name("normalize_sources.py"),
                    Path(__file__).with_name("research") / "adapters.py",
                    Path(__file__).with_name("research") / "common.py",
                    Path(__file__).with_name("research") / "matching.py",
                ]
            },
        )
        print(f"Writing {rows:,} common records", file=sys.stderr, flush=True)
        reader = db.execute("SELECT * FROM common_rows").to_arrow_reader(32768)
        metadata = {
            b"travel.schema_version": SCHEMA_VERSION.encode(),
            b"travel.manifest": json.dumps(manifest, ensure_ascii=False).encode(),
        }
        schema = reader.schema.with_metadata(metadata)
        temporary = work / "dataset.parquet"
        with pq.ParquetWriter(temporary, schema, compression="zstd") as writer:
            for batch in reader:
                writer.write_batch(batch)
        saved = pq.ParquetFile(temporary)
        assert saved.metadata.num_rows == rows
        for label in ["document_key", "ticket_key", "entity_id", "source_record_id"]:
            assert str(saved.schema_arrow.field(label).type) == "string"
        for path, digest in input_hashes.items():
            if sha(input_dir / path) != digest:
                raise ValueError("Input changed during build: " + path)
        db.close()
        temporary.rename(output)
    return dict(
        output=str(output),
        rows=rows,
        observations=observations,
        source_entities=entities,
        elapsed_seconds=round(time.perf_counter() - started, 3),
        schema_version=SCHEMA_VERSION,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Combine passenger parser Parquet tables into one portable dataset"
    )
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            build_dataset(args.input_dir, args.output), ensure_ascii=False, indent=2
        )
    )


if __name__ == "__main__":
    main()
