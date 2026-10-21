"""Standalone parser for BoardingData.csv. No passenger matching or imputation."""

from __future__ import annotations

import argparse
import base64
import collections
import csv
import hashlib
import json
import re
import time
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

BASE = Path(__file__).resolve().parent
DEFAULT_INPUT = BASE.parents[1] / "raw_data/BoardingData.csv"
DEFAULT_OUTPUT = BASE / "result"
VERSION = "boarding.csv.v1"
FIELDS = {
    "PassengerFirstName": "passenger_first_name",
    "PassengerSecondName": "passenger_second_name",
    "PassengerLastName": "passenger_last_name",
    "PassengerSex": "passenger_sex",
    "PassengerBirthDate": "passenger_birth_date",
    "PassengerDocument": "passenger_document",
    "BookingCode": "booking_code",
    "TicketNumber": "ticket_number",
    "Baggage": "baggage",
    "FlightDate": "flight_date",
    "FlightTime": "flight_time",
    "FlightNumber": "flight_number",
    "CodeShare": "code_share",
    "Destination": "destination_text",
}
MARKERS = {"N/A", "NA", "NULL", "NONE", "NAN", "-"}
RULES = {name: "trim_outer_whitespace.v1" for name in FIELDS.values()}
RULES.update(
    passenger_birth_date="calendar_candidates_without_date_order_preference.v1",
    flight_date="strict_iso_calendar.v1",
    flight_time="local_HH_MM_without_timezone.v1",
    baggage="literal_baggage_category_None_is_not_missing.v1",
    code_share="literal_source_category_not_operator.v1",
    destination_text="literal_geographic_text_no_airport_lookup.v1",
)


def dumps(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def sha256_file(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def birth_candidates(text):
    """No file-wide date-order inference; unique calendar candidates only."""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        try:
            return [date.fromisoformat(text)], "valid_iso"
        except ValueError:
            return [], "invalid_date"
    match = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", text)
    if not match:
        return [], "unsupported_date_format"
    a, b, year = map(int, match.groups())
    options = set()
    for month, day in [(a, b), (b, a)]:
        try:
            options.add(date(year, month, day))
        except ValueError:
            pass
    options = sorted(options)
    return options, "ambiguous_date" if len(
        options
    ) > 1 else "calendar_unique" if options else "invalid_date"


def normalize(source, raw):
    text = raw.strip()
    if text == "":
        return None, "empty", "Empty decoded CSV field", []
    # None is an actual baggage category in this source, not a universal NA token.
    if text.upper() in MARKERS and not (source == "Baggage" and text == "None"):
        return None, "missing_marker", "Literal missing marker retained in _raw", []
    if source == "PassengerBirthDate":
        options, status = birth_candidates(text)
        return (
            (options[0] if len(options) == 1 else None),
            status,
            (
                "Date order unresolved; retain all calendar candidates"
                if len(options) > 1
                else "Calendar validation only; source date order not assumed"
            ),
            options,
        )
    if source == "FlightDate":
        try:
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
                raise ValueError
            return date.fromisoformat(text), "valid_iso", "Explicit YYYY-MM-DD", []
        except ValueError:
            return None, "invalid_date", "Not a valid explicit ISO calendar date", []
    if source == "FlightTime":
        if re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", text):
            return text, "valid_local_time", "No timezone or UTC inferred", []
        return None, "invalid_time", "Expected HH:MM between 00:00 and 23:59", []
    categories = {
        "PassengerSex": {"Male", "Female"},
        "Baggage": {"None", "Registered", "Transit", "Delayed"},
        "CodeShare": {"Own", "Operated"},
    }
    if source in categories and text not in categories[source]:
        return text, "unexpected_category", "Literal value retained; no correction", []
    return text, "present", "Whitespace trim only; no transliteration or imputation", []


PROVENANCE = [
    ("record_id", pa.string()),
    ("source_file", pa.string()),
    ("source_sha256", pa.string()),
    ("source_record_number", pa.int64()),
    ("source_line_start", pa.int64()),
    ("source_line_end", pa.int64()),
    ("rule_version", pa.string()),
]
RAW_SCHEMA = pa.schema(
    PROVENANCE
    + [
        (n, pa.string())
        for n in ["raw_text", "raw_bytes_b64", "raw_fields_json", "record_status"]
    ]
    + [("field_count", pa.int32())]
)
RECORD_SCHEMA = pa.schema(
    PROVENANCE
    + [(name + "_raw", pa.string()) for name in FIELDS.values()]
    + [
        (
            name,
            pa.date32()
            if name in {"passenger_birth_date", "flight_date"}
            else pa.string(),
        )
        for name in FIELDS.values()
    ]
    + [
        ("birth_date_candidates_json", pa.string()),
        ("birth_year", pa.int32()),
        ("field_status_json", pa.string()),
        ("record_status", pa.string()),
        ("source_fields_sha256", pa.string()),
    ]
)
ISSUE_SCHEMA = pa.schema(
    PROVENANCE + [(n, pa.string()) for n in ["field", "status", "reason"]]
)


class Sink:
    def __init__(self, path, schema):
        self.schema, self.buffer, self.count = schema, [], 0
        self.writer = pq.ParquetWriter(path, schema, compression="zstd")

    def add(self, row):
        self.buffer.append(row)
        self.count += 1
        if len(self.buffer) >= 10000:
            self.flush()

    def flush(self):
        if self.buffer:
            self.writer.write_table(
                pa.Table.from_pylist(self.buffer, schema=self.schema)
            )
            self.buffer.clear()

    def close(self):
        self.flush()
        self.writer.close()


class CapturedLines:
    """Track exact physical input bytes across quoted multiline CSV records."""

    def __init__(self, stream):
        self.stream, self.line_number = stream, 0
        self.digest = hashlib.sha256()
        self.reset()

    def reset(self):
        self.parts, self.encoding_error = [], False
        self.start = self.line_number + 1

    def __iter__(self):
        return self

    def __next__(self):
        data = self.stream.readline()
        if not data:
            raise StopIteration
        self.line_number += 1
        self.parts.append(data)
        self.digest.update(data)
        try:
            text = data.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            self.encoding_error = True
            text = data.decode("utf-8", errors="replace")
        return text.removeprefix("\ufeff") if self.line_number == 1 else text


def parse_csv(input_path=DEFAULT_INPUT, output_dir=DEFAULT_OUTPUT):
    source, out = Path(input_path).resolve(), Path(output_dir).resolve()
    source_sha = sha256_file(source)
    out.mkdir(parents=True, exist_ok=False)
    started = time.perf_counter()
    manifest = dict(
        rule_version=VERSION,
        source_file=source.name,
        source_sha256=source_sha,
        source_bytes=source.stat().st_size,
        encoding="utf-8",
        delimiter=";",
        quotechar='"',
        status="running",
        unit="source passenger-flight/boarding observation; not a verified completed flight",
    )
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2))
    sinks = {
        "records": Sink(out / "records.parquet", RECORD_SCHEMA),
        "raw_records": Sink(out / "raw_records.parquet", RAW_SCHEMA),
        "issues": Sink(out / "issues.parquet", ISSUE_SCHEMA),
    }
    statuses, field_stats, duplicates = (
        collections.Counter(),
        collections.Counter(),
        collections.Counter(),
    )
    min_date = max_date = None
    try:
        with source.open("rb") as stream:
            lines = CapturedLines(stream)
            reader = csv.reader(
                lines, delimiter=";", quotechar='"', doublequote=True, strict=True
            )
            header = next(reader)
            manifest.update(
                header=header,
                header_raw_bytes_b64=base64.b64encode(b"".join(lines.parts)).decode(
                    "ascii"
                ),
                header_line_end=lines.line_number,
            )
            if lines.encoding_error or header != list(FIELDS):
                raise ValueError(
                    "Unsupported or invalid UTF-8 header; no schema guessed"
                )
            record_number = 0
            while True:
                lines.reset()
                syntax_error = False
                try:
                    values = next(reader)
                except StopIteration:
                    break
                except csv.Error:
                    values, syntax_error = [], True
                record_number += 1
                raw_bytes = b"".join(lines.parts)
                rid = hashlib.sha256(
                    f"{source_sha}:{record_number}:{lines.start}:{lines.line_number}".encode()
                ).hexdigest()
                provenance = dict(
                    record_id=rid,
                    source_file=source.name,
                    source_sha256=source_sha,
                    source_record_number=record_number,
                    source_line_start=lines.start,
                    source_line_end=lines.line_number,
                    rule_version=VERSION,
                )
                structural_status = (
                    "csv_syntax_error"
                    if syntax_error
                    else "invalid_utf8"
                    if lines.encoding_error
                    else "blank_record"
                    if not values
                    else "wrong_field_count"
                    if len(values) != len(FIELDS)
                    else "parsed"
                )
                sinks["raw_records"].add(
                    dict(
                        provenance,
                        raw_text=raw_bytes.decode("utf-8", errors="replace"),
                        raw_bytes_b64=base64.b64encode(raw_bytes).decode("ascii"),
                        raw_fields_json=dumps(values),
                        record_status=structural_status,
                        field_count=len(values),
                    )
                )
                if structural_status == "blank_record":
                    statuses["blank_record"] += 1
                    continue
                row = dict(provenance)
                field_status = {}
                bad = structural_status != "parsed"
                uncertain = False
                options = []
                if bad:
                    sinks["issues"].add(
                        dict(
                            provenance,
                            field="__record__",
                            status=structural_status,
                            reason="Raw bytes retained; field alignment or decoding cannot be trusted",
                        )
                    )
                for i, (src, dst) in enumerate(FIELDS.items()):
                    raw = values[i] if i < len(values) else None
                    row[dst + "_raw"] = raw
                    if structural_status == "parsed":
                        value, status, reason, candidates = normalize(src, raw)
                    else:
                        value, status, reason, candidates = (
                            None,
                            "unavailable_due_to_record_error",
                            structural_status,
                            [],
                        )
                    row[dst] = value
                    field_status[dst] = dict(
                        status=status, rule=RULES[dst], reason=reason
                    )
                    field_stats[dst + ":" + status] += 1
                    if src == "PassengerBirthDate":
                        options = candidates
                    if status in {
                        "ambiguous_date",
                        "invalid_date",
                        "invalid_time",
                        "unsupported_date_format",
                        "unexpected_category",
                    }:
                        sinks["issues"].add(
                            dict(provenance, field=dst, status=status, reason=reason)
                        )
                        uncertain |= status == "ambiguous_date"
                        bad |= status != "ambiguous_date"
                row["birth_date_candidates_json"] = dumps(
                    [x.isoformat() for x in options]
                )
                row["birth_year"] = options[0].year if options else None
                row["field_status_json"] = dumps(field_status)
                row["record_status"] = (
                    "needs_review"
                    if bad
                    else "parsed_with_uncertainty"
                    if uncertain
                    else "parsed"
                )
                row["source_fields_sha256"] = hashlib.sha256(
                    dumps(values).encode()
                ).hexdigest()
                duplicates[row["source_fields_sha256"]] += 1
                sinks["records"].add(row)
                statuses[row["record_status"]] += 1
                d = row["flight_date"]
                if d is not None:
                    min_date = d if min_date is None else min(min_date, d)
                    max_date = d if max_date is None else max(max_date, d)
            manifest.update(
                physical_lines=lines.line_number,
                consumed_bytes_sha256=lines.digest.hexdigest(),
            )
        if (
            sha256_file(source) != source_sha
            or manifest["consumed_bytes_sha256"] != source_sha
        ):
            raise ValueError("Source changed or not all bytes accounted for")
        manifest.update(
            status="complete",
            structurally_valid=statuses["needs_review"] == 0,
            tables={name: sink.count for name, sink in sinks.items()},
            record_statuses=dict(statuses),
            field_status_counts=dict(field_stats),
            exact_field_duplicate_excess=sum(n - 1 for n in duplicates.values()),
            flight_date_min=min_date.isoformat() if min_date else None,
            flight_date_max=max_date.isoformat() if max_date else None,
            seconds=round(time.perf_counter() - started, 3),
            limitation="Date ambiguity retained; no identity matching, timezone, airport or actual-flight inference",
        )
    except BaseException as exc:
        manifest.update(status="failed", error_type=type(exc).__name__)
        raise
    finally:
        for sink in sinks.values():
            sink.close()
        (out / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2)
        )
    fields = {
        src: dict(
            field=dst,
            raw_field=dst + "_raw",
            rule=RULES[dst],
            parquet_type=str(RECORD_SCHEMA.field(dst).type),
        )
        for src, dst in FIELDS.items()
    }
    (out / "fields.json").write_text(
        json.dumps(dict(rule_version=VERSION, fields=fields), indent=2)
    )
    return manifest


def load_records(output_dir=DEFAULT_OUTPUT, columns=None, filters=None):
    out = Path(output_dir)
    if json.loads((out / "manifest.json").read_text())["status"] != "complete":
        raise ValueError("Incomplete CSV parsing run")
    return pd.read_parquet(
        out / "records.parquet",
        columns=columns,
        filters=filters,
        dtype_backend="pyarrow",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", nargs="?", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    print(json.dumps(parse_csv(args.input, args.out), ensure_ascii=False, indent=2))
