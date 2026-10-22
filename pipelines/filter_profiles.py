import argparse
import json
import tempfile
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.build_common_parquet import SCHEMA_VERSION
from pipelines.profile_patterns import build_tables
from pipelines.research.common import literal, sha
from pipelines.travel_analysis.criteria import (
    CRITERIA,
    DEFAULT_CRITERIA,
    UNIT_KINDS,
    select_profiles,
    validate_config,
)


def load_features(input_file):
    path = Path(input_file).resolve()
    schema = pq.ParquetFile(path).schema_arrow
    if (schema.metadata or {}).get(b"travel.schema_version") != SCHEMA_VERSION.encode():
        raise ValueError(
            "Input is not a supported common dataset; run build_common_parquet first"
        )
    manifest = json.loads(schema.metadata[b"travel.manifest"])
    with tempfile.TemporaryDirectory(prefix="profile-query-") as directory:
        db = duckdb.connect()
        db.execute("SET memory_limit='2GB'")
        db.execute("SET threads=4")
        db.execute(f"SET temp_directory={literal(directory)}")
        db.execute(f"CREATE VIEW common AS SELECT * FROM read_parquet({literal(path)})")
        db.execute("""CREATE TABLE entities AS SELECT DISTINCT entity_id,source,entity_kind,
          entity_record_id source_record_id,entity_locator source_locator,
          entity_source_sha256 source_sha256,entity_source_rule_version source_rule_version,
          entity_source_status source_status,first_name,last_name,middle_name,full_name,sex,
          birth_dates_json,document_raw,document_key,entity_processing_rule processing_rule
          FROM common""")
        db.execute("""CREATE TABLE assignments AS SELECT DISTINCT entity_id,source,entity_kind,
          candidate_component_id,provisional_group_id,linkage_status status FROM common""")
        db.execute("""CREATE VIEW observations AS SELECT source,entity_id,observation_id,source_record_id,
          source_sha256,date_key,flight_key,origin_key,destination_key,local_time_key FROM common
          WHERE observation_id IS NOT NULL""")
        build_tables(db)
        db.execute("""CREATE TABLE names AS SELECT m.unit_id,to_json(list(struct_pack(
          source:=e.source,entity_id:=e.entity_id,first_name:=e.first_name,last_name:=e.last_name,
          middle_name:=e.middle_name,full_name:=e.full_name,source_record_id:=e.source_record_id,
          source_sha256:=e.source_sha256) ORDER BY e.source,e.entity_id)) names_json
          FROM unit_members m JOIN entities e USING(entity_id) GROUP BY m.unit_id""")
        db.execute("""CREATE TABLE context_entities AS SELECT DISTINCT entity_id,candidate_component_id,
          candidate_document_count FROM common""")
        db.execute("""CREATE TABLE context AS SELECT m.unit_id,max(c.candidate_document_count) candidate_document_count,
          coalesce(to_json(list(DISTINCT c.candidate_component_id ORDER BY c.candidate_component_id)
            FILTER(WHERE c.candidate_document_count>1)),'[]') document_context_components_json,
          count(DISTINCT e.document_key) own_document_count
          FROM unit_members m JOIN context_entities c USING(entity_id) JOIN entities e USING(entity_id)
          GROUP BY m.unit_id""")
        db.execute("""CREATE TABLE peak_day AS WITH days AS (
          SELECT unit_id,date_key,count(*) n FROM slots GROUP BY unit_id,date_key)
          SELECT unit_id,max(n) peak_day_flight_bins FROM days GROUP BY unit_id""")
        frame = db.execute("""SELECT f.* EXCLUDE(model_eligible,eligible_units),
          f.model_eligible reference_eligible,f.eligible_units reference_units,
          n.names_json,c.candidate_document_count,c.document_context_components_json,c.own_document_count,
          coalesce(p.peak_day_flight_bins,0) peak_day_flight_bins
          FROM all_features f JOIN names n USING(unit_id) JOIN context c USING(unit_id)
          LEFT JOIN peak_day p USING(unit_id) ORDER BY f.unit_kind,f.unit_id""").df()
        assert frame.unit_id.is_unique
        db.close()
    return frame, manifest


def query_dataset(input_file, output, config, json_output=None):
    output = Path(output).resolve()
    if output.suffix.lower() != ".parquet":
        raise ValueError("Output must have .parquet extension")
    json_output = Path(json_output).resolve() if json_output else None
    if output.exists() or json_output and json_output.exists():
        raise FileExistsError("Output already exists")
    if json_output == output:
        raise ValueError("Parquet and JSON outputs must be different")
    config = validate_config(config)
    started = time.perf_counter()
    input_file = Path(input_file).resolve()
    input_hash = sha(input_file)
    features, manifest = load_features(input_file)
    result, reasons, config = select_profiles(features, config)
    result["reasons_json"] = [
        json.dumps(reasons[idx], ensure_ascii=False) for idx in result.index
    ]
    result["selection_rule"] = "profile-selection.v1"
    table = pa.Table.from_pandas(result, preserve_index=False)
    for label, values, dtype in [
        ("criteria_count", result.criteria_count, pa.int64()),
        ("reasons_json", result.reasons_json, pa.string()),
        ("selection_rule", result.selection_rule, pa.string()),
    ]:
        table = table.set_column(
            table.schema.get_field_index(label), label, pa.array(values, type=dtype)
        )
    if not len(result):
        index = table.schema.get_field_index("matched_criteria")
        table = table.set_column(
            index, "matched_criteria", pa.array([], type=pa.list_(pa.string()))
        )
    summary = dict(
        schema_version="profile-selection.v1",
        input_sha256=input_hash,
        common_schema_version=manifest["schema_version"],
        config=config,
        units_considered={
            kind: int(features.unit_kind.eq(kind).sum())
            for kind in config["unit_kinds"]
        },
        reference_eligible_units={
            kind: int((features.unit_kind.eq(kind) & features.reference_eligible).sum())
            for kind in config["unit_kinds"]
        },
        matched_units=len(result),
        unit_counts=result.unit_kind.value_counts().to_dict(),
        elapsed_seconds=round(time.perf_counter() - started, 3),
        interpretation="Observed patterns or data-quality review candidates; not established people or criminal involvement",
        probability=None,
        models_used=False,
        code_hashes={
            str(p.name): sha(p)
            for p in [
                Path(__file__),
                Path(__file__).with_name("travel_analysis") / "criteria.py",
                Path(__file__).with_name("profile_patterns.py"),
            ]
        },
    )
    if sha(input_file) != input_hash:
        raise ValueError("Input changed during query")
    table = table.replace_schema_metadata(
        {
            **(table.schema.metadata or {}),
            b"travel.selection": json.dumps(summary, ensure_ascii=False).encode(),
        }
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".profile-selection-", dir=output.parent
    ) as directory:
        temporary = Path(directory) / "result.parquet"
        pq.write_table(table, temporary, compression="zstd")
        assert pq.ParquetFile(temporary).metadata.num_rows == len(result)
        temporary.rename(output)
    if json_output:
        json_output.parent.mkdir(parents=True, exist_ok=True)
        records = table.to_pylist()
        for row in records:
            for label in ["names", "reasons", "document_context_components"]:
                row[label] = json.loads(row.pop(label + "_json"))
        json_output.write_text(
            json.dumps(
                dict(summary=summary, profiles=records),
                ensure_ascii=False,
                default=str,
                allow_nan=False,
            )
        )
    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Select observed profile patterns using configurable criteria"
    )
    parser.add_argument("--input", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--list-criteria", action="store_true")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--criteria", help="Comma-separated criterion IDs")
    parser.add_argument("--match", choices=["any", "all"])
    parser.add_argument(
        "--unit-kinds",
        help="source_profile, preliminary_group, or both separated by commas",
    )
    parser.add_argument(
        "--set", action="append", default=[], metavar="CRITERION.PARAM=JSON_VALUE"
    )
    args = parser.parse_args()
    if args.list_criteria:
        print(
            json.dumps(
                dict(
                    criteria=CRITERIA,
                    match_options=["any", "all"],
                    unit_kinds=UNIT_KINDS,
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return
    if not args.input or not args.output:
        parser.error("--input and --output are required")
    if args.config and any([args.criteria, args.match, args.unit_kinds, args.set]):
        parser.error("Use --config or criterion options; mixing them is not allowed")
    if args.config:
        config = json.loads(args.config.read_text())
    else:
        config = dict(
            criteria={
                key.strip(): {}
                for key in (
                    args.criteria.split(",") if args.criteria else DEFAULT_CRITERIA
                )
            },
            match=args.match or "any",
            unit_kinds=(args.unit_kinds or "source_profile").split(","),
        )
        for setting in args.set:
            try:
                key, value = setting.split("=", 1)
                criterion, param = key.split(".", 1)
                if criterion not in config["criteria"]:
                    raise ValueError("Set a parameter only for a selected criterion")
                config["criteria"][criterion][param] = json.loads(value)
            except (ValueError, KeyError) as exc:
                parser.error(str(exc))
    print(
        json.dumps(
            query_dataset(args.input, args.output, config, args.json_output),
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
