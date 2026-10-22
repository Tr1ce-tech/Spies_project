import hashlib
import json
from collections import Counter

import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.research.common import clean, literal, sha

SIGNATURES = {
    "csv": {
        "passenger_document_raw",
        "source_record_number",
        "birth_date_candidates_json",
    },
    "tab": {"PaxName", "TravelDoc", "source_line"},
    "excel": {
        "passenger_name",
        "ticket_number",
        "source_sheet",
        "source_archive_sha256",
    },
    "json_profiles": {"metadata_json", "profile_index", "profile_status"},
    "json_cards": {"profile_record_id", "programm", "Number", "Status"},
    "json_flights": {
        "profile_record_id",
        "Departure_Airport",
        "Arrival_Airport",
        "Date",
    },
    "xml_users": {"user_record_id", "user_first_name", "user_last_name"},
    "xml_cards": {"card_record_id", "user_record_id", "card_program", "card_number"},
    "xml_activities": {"user_record_id", "activity_type", "flight_date_raw"},
    "yaml_entries": {
        "entry_id",
        "loyalty_program",
        "loyalty_number",
        "flight_record_id",
    },
}
STRING_IDS = {
    "csv": ["record_id", "passenger_document_raw", "ticket_number"],
    "tab": ["record_id", "TravelDoc", "e-Ticket", "FrequentFlyer"],
    "excel": ["record_id", "ticket_number", "loyalty_card"],
    "json_profiles": ["record_id"],
    "json_cards": ["record_id", "profile_record_id", "Number", "programm"],
    "json_flights": ["record_id", "profile_record_id"],
    "xml_users": ["user_record_id"],
    "xml_cards": ["card_record_id", "user_record_id", "card_number"],
    "xml_activities": ["record_id", "user_record_id"],
    "yaml_entries": ["entry_id", "loyalty_number", "loyalty_program"],
}


def discover(directory):
    found = {table: [] for table in SIGNATURES}
    inventory = []
    seen = set()
    for path in sorted(directory.rglob("*.parquet")):
        if set(path.relative_to(directory).parts[:-1]) & {
            "legacy",
            "legacy_pdf",
            ".venv",
            ".git",
            "__pycache__",
        }:
            inventory.append(
                {
                    "path": str(path.relative_to(directory)),
                    "bytes": path.stat().st_size,
                    "status": "excluded_archive_directory",
                }
            )
            continue
        meta = pq.ParquetFile(path)
        columns = set(meta.schema_arrow.names)
        matches = [table for table, keys in SIGNATURES.items() if keys <= columns]
        if "xml_activities" in matches:
            matches = [m for m in matches if m not in ["xml_users", "xml_cards"]]
        elif "xml_cards" in matches:
            matches = [m for m in matches if m != "xml_users"]
        item = {
            "path": str(path.relative_to(directory)),
            "bytes": path.stat().st_size,
            "rows": meta.metadata.num_rows,
        }
        if len(matches) > 1:
            raise ValueError(f"Ambiguous parser schema: {path}")
        if not matches:
            item["status"] = (
                "excluded_schedule"
                if {"days_codes", "valid_from", "valid_to"} <= columns
                else "auxiliary_or_unrecognized"
            )
            inventory.append(item)
            continue
        table = matches[0]
        for label in STRING_IDS[table]:
            if label not in columns:
                raise ValueError(f"{path}: missing identifier column {label}")
            dtype = meta.schema_arrow.field(label).type
            if pa.types.is_dictionary(dtype):
                dtype = dtype.value_type
            if not (pa.types.is_string(dtype) or pa.types.is_large_string(dtype)):
                raise ValueError(
                    f"{path}: {label} must be a string, got {dtype}; leading zeroes cannot be recovered"
                )
        digest = sha(path)
        item.update(table=table, sha256=digest, status="included")
        if (table, digest) in seen:
            item["status"] = "identical_file_duplicate"
        else:
            found[table].append(path)
            seen.add((table, digest))
        inventory.append(item)
    missing = [table for table, files in found.items() if not files]
    if missing:
        raise ValueError(
            "Missing parser tables: "
            + ", ".join(missing)
            + ". Supply one folder containing all passenger parser outputs; subfolders are scanned recursively."
        )
    return found, inventory


def register(db, files):
    for table, paths in files.items():
        db.execute(
            f"CREATE VIEW src_{table} AS SELECT * FROM read_parquet([{','.join(literal(p) for p in paths)}])"
        )
        key = {
            "xml_users": "user_record_id",
            "xml_cards": "card_record_id",
            "yaml_entries": "entry_id",
        }.get(table, "record_id")
        if not db.execute(
            f"SELECT count(*)=count({key}) AND count(*)=count(DISTINCT {key}) FROM src_{table}"
        ).fetchone()[0]:
            raise ValueError(
                f"{table}: duplicate or missing {key}; provide only one non-overlapping parser export"
            )


def forum_fields(db, files, path):
    labels = [
        "entity_id",
        "source_record_id",
        "source_locator",
        "source_sha256",
        "source_rule_version",
        "source_status",
        "first_name",
        "last_name",
        "sex",
    ]
    schema = pa.schema([(label, pa.string()) for label in labels])
    with pq.ParquetWriter(path, schema, compression="zstd") as writer:
        for p in files:
            for batch in pq.ParquetFile(p).iter_batches(batch_size=2048):
                rows = []
                for row in batch.to_pylist():
                    pairs = json.loads(row["metadata_json"])
                    counts = Counter(key for key, _ in pairs)
                    data = {key: value for key, value in pairs if counts[key] == 1}
                    real = data.get("Real Name") or {}
                    if not isinstance(real, dict):
                        real = {}
                    for doc in data.get("Travel Documents", []) or []:
                        if isinstance(doc, dict) and doc.get("Passports") is not None:
                            raise ValueError(
                                "Nonempty JSON passports require a documented adapter; refusing to discard them"
                            )
                    rows.append(
                        dict(
                            entity_id=hashlib.sha256(
                                ("json:" + row["record_id"]).encode()
                            ).hexdigest(),
                            source_record_id=row["record_id"],
                            source_locator=f"profile[{row['profile_index']}]",
                            source_sha256=row["source_sha256"],
                            source_rule_version=row["rule_version"],
                            source_status=row["profile_status"],
                            first_name=clean(real.get("First Name")),
                            last_name=clean(real.get("Last Name")),
                            sex=clean(data.get("Sex")),
                        )
                    )
                writer.write_table(pa.Table.from_pylist(rows, schema=schema))
    db.execute(
        f"CREATE VIEW forum_fields AS SELECT * FROM read_parquet({literal(path)})"
    )
