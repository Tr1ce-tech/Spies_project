"""Full byte preservation and independent pandas C-engine source verification."""

import argparse
import base64
import hashlib
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

if __package__:
    from .parse_csv import DEFAULT_INPUT, DEFAULT_OUTPUT, FIELDS, VERSION, sha256_file
else:
    from parse_csv import DEFAULT_INPUT, DEFAULT_OUTPUT, FIELDS, VERSION, sha256_file


def verify(source=DEFAULT_INPUT, output=DEFAULT_OUTPUT):
    out = Path(output)
    m = json.loads((out / "manifest.json").read_text())
    assert m["status"] == "complete" and m["structurally_valid"], (
        "Run needs structural review first"
    )
    checks = {"source_sha256_matches": sha256_file(source) == m["source_sha256"]}
    checks["parquet_counts"] = all(
        pq.ParquetFile(out / f"{k}.parquet").metadata.num_rows == v
        for k, v in m["tables"].items()
    )
    digest = hashlib.sha256(base64.b64decode(m["header_raw_bytes_b64"]))
    raw_ids, raw_positions, raw_statuses = [], [], []
    previous_end = m["header_line_end"]
    positions_ok = True
    for batch in pq.ParquetFile(out / "raw_records.parquet").iter_batches():
        for row in batch.to_pylist():
            digest.update(base64.b64decode(row["raw_bytes_b64"]))
            positions_ok &= row["source_line_start"] == previous_end + 1
            previous_end = row["source_line_end"]
            expected_id = hashlib.sha256(
                f"{m['source_sha256']}:{row['source_record_number']}:{row['source_line_start']}:{row['source_line_end']}".encode()
            ).hexdigest()
            positions_ok &= (
                expected_id == row["record_id"]
                and row["source_sha256"] == m["source_sha256"]
                and row["rule_version"] == VERSION
            )
            raw_ids.append(row["record_id"])
            raw_statuses.append(row["record_status"])
            if row["record_status"] != "blank_record":
                raw_positions.append(
                    (
                        row["record_id"],
                        row["source_record_number"],
                        row["source_line_start"],
                        row["source_line_end"],
                    )
                )
    checks["all_source_bytes_reconstructed"] = digest.hexdigest() == m["source_sha256"]
    checks["raw_ids_unique_and_positions_contiguous"] = (
        positions_ok
        and len(set(raw_ids)) == len(raw_ids)
        and previous_end == m["physical_lines"]
    )
    f = pd.read_parquet(out / "records.parquet")
    checks["records_lineage"] = raw_positions == list(
        f[
            [
                "record_id",
                "source_record_number",
                "source_line_start",
                "source_line_end",
            ]
        ].itertuples(index=False, name=None)
    )
    expected = pd.read_csv(
        source,
        sep=";",
        dtype="string",
        keep_default_na=False,
        encoding="utf-8-sig",
        skip_blank_lines=True,
        engine="c",
    )
    actual = f[[x + "_raw" for x in FIELDS.values()]].astype("string")
    actual.columns = list(FIELDS)
    pd.testing.assert_frame_equal(
        expected.reset_index(drop=True), actual.reset_index(drop=True)
    )
    checks["all_14_raw_fields_equal_independent_C_reader"] = True
    schema = pq.ParquetFile(out / "records.parquet").schema_arrow
    checks["identifier_types_and_leading_zeros"] = all(
        str(schema.field(name).type) == "string"
        and f[name].astype("string").equals(expected[src].str.strip())
        for src, name in FIELDS.items()
        if name
        in ["passenger_document", "booking_code", "ticket_number", "flight_number"]
    )
    checks["date32_types"] = all(
        str(schema.field(name).type) == "date32[day]"
        for name in ["flight_date", "passenger_birth_date"]
    )
    iso_dates = pd.to_datetime(
        expected.FlightDate, format="%Y-%m-%d", errors="raise"
    ).dt.date
    checks["all_flight_dates_equal_ISO_source"] = f.flight_date.equals(iso_dates)
    options = {}
    for raw in expected.PassengerBirthDate.unique():
        candidates = set()
        for fmt in ["%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d"]:
            try:
                candidates.add(datetime.strptime(raw, fmt).date())
            except ValueError:
                pass
        options[raw] = sorted(candidates)
    birth_ok = True
    for raw, normalized, candidates, year in zip(
        f.passenger_birth_date_raw,
        f.passenger_birth_date,
        f.birth_date_candidates_json,
        f.birth_year,
    ):
        wanted = options[raw]
        birth_ok &= json.loads(candidates) == [d.isoformat() for d in wanted]
        birth_ok &= (
            (normalized == wanted[0]) if len(wanted) == 1 else pd.isna(normalized)
        )
        birth_ok &= (year == wanted[0].year) if wanted else pd.isna(year)
    checks["all_birth_candidates_and_unresolved_dates"] = bool(birth_ok)
    checks["baggage_None_category_retained"] = bool(
        (f.loc[f.baggage_raw == "None", "baggage"] == "None").all()
    )
    checks["blank_records_separate"] = len(f) + raw_statuses.count(
        "blank_record"
    ) == len(raw_ids)
    result = dict(
        checks={k: bool(v) for k, v in checks.items()},
        records=len(f),
        raw_records=len(raw_ids),
        blank_records=raw_statuses.count("blank_record"),
        independent_reader="pandas read_csv engine=c; all raw values compared",
        limitation="Technical parsing verification; not passenger identity or actual-flight validation",
    )
    (out / "verification.json").write_text(json.dumps(result, indent=2))
    if not all(checks.values()):
        raise AssertionError(result)
    return result


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("input", nargs="?", type=Path, default=DEFAULT_INPUT)
    p.add_argument("--out", type=Path, default=DEFAULT_OUTPUT)
    args = p.parse_args()
    print(json.dumps(verify(args.input, args.out), indent=2))
