"""Bounded streaming YAML -> two Parquet tables; no full-file load."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
import unicodedata
from collections import Counter
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from yaml.events import (
    AliasEvent,
    DocumentEndEvent,
    DocumentStartEvent,
    MappingEndEvent,
    MappingStartEvent,
    ScalarEvent,
    SequenceEndEvent,
    SequenceStartEvent,
    StreamEndEvent,
    StreamStartEvent,
)

VERSION = "yaml-exchange-1.0.0"
MARKERS = {"N/A", "NA", "NULL", "NONE", "NAN", "-", "~"}
LIMITS = {
    "depth": 16,
    "nodes_per_flight": 100_000,
    "scalar_characters": 1_000_000,
    "raw_flight_characters": 8_000_000,
}


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def stable_id(*parts):
    return hashlib.sha256(canonical(parts).encode("utf8")).hexdigest()


def update_digest(digest, values):
    digest.update((canonical(values) + "\n").encode("utf8"))


def normalise(raw, kind="text"):
    if raw is None:
        return None, "absent"
    if not isinstance(raw, str):
        return None, "unexpected_nested_value"
    value = unicodedata.normalize("NFC", raw).strip()
    if not value:
        return None, "empty_scalar"
    if value.upper() in MARKERS:
        return None, "missing_marker"
    if kind == "date":
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            return None, "ambiguous_or_unsupported_date"
        try:
            return date.fromisoformat(value), "valid_iso_date"
        except ValueError:
            return None, "invalid_calendar_date"
    if kind == "airport":
        value = value.upper()
        if not re.fullmatch(r"[A-Z]{3}", value):
            return None, "invalid_airport_syntax"
        return value, "syntax_only_not_historically_verified"
    if kind == "flight":
        value = re.sub(r"\s+", "", value.upper())
        if not re.fullmatch(r"[A-Z0-9]{2,3}\d{1,5}[A-Z]?", value):
            return None, "invalid_flight_syntax"
        return value, "syntax_only_operator_unknown"
    return value, "trim_nfc_only"


class EventReader:
    """A single-flight tree builder on top of LibYAML's incremental event API."""

    def __init__(self, handle):
        self.events = iter(yaml.parse(handle, Loader=yaml.CBaseLoader))
        self.node_count = 0
        self.text_characters = 0
        self.locations = {}
        self.styles = []

    def take(self, expected=None):
        event = next(self.events)
        if isinstance(event, AliasEvent) or getattr(event, "anchor", None):
            raise ValueError(
                f"Anchors/aliases unsupported, line {event.start_mark.line + 1}"
            )
        tag = getattr(event, "tag", None)
        if tag and tag not in {
            "tag:yaml.org,2002:str",
            "tag:yaml.org,2002:map",
            "tag:yaml.org,2002:seq",
        }:
            raise ValueError(
                f"Explicit tag unsupported: {tag}, line {event.start_mark.line + 1}"
            )
        if (
            isinstance(event, ScalarEvent)
            and len(event.value) > LIMITS["scalar_characters"]
        ):
            raise ValueError("Scalar exceeds configured memory bound")
        if expected is not None and not isinstance(event, expected):
            raise ValueError(
                f"Expected {expected.__name__}, got {type(event).__name__} "
                f"at line {event.start_mark.line + 1}"
            )
        return event

    def key(self, event, path):
        if not isinstance(event, ScalarEvent):
            raise ValueError("Complex YAML keys are unsupported")
        self.text_characters += len(event.value)
        if self.text_characters > LIMITS["raw_flight_characters"]:
            raise ValueError("Flight scalar text exceeds configured memory bound")
        self.locations[path] = (
            event.start_mark.line + 1,
            event.start_mark.column + 1,
            event.style,
        )
        if event.style or event.tag:
            self.styles.append(
                {
                    "path": list(path),
                    "role": "key",
                    "style": event.style,
                    "tag": event.tag,
                }
            )

    def value(self, event, path=(), depth=0):
        self.node_count += 1
        if depth > LIMITS["depth"] or self.node_count > LIMITS["nodes_per_flight"]:
            raise ValueError("Flight subtree exceeds configured depth/node bound")
        if isinstance(event, ScalarEvent):
            self.text_characters += len(event.value)
            if self.text_characters > LIMITS["raw_flight_characters"]:
                raise ValueError("Flight scalar text exceeds configured memory bound")
            if event.style or event.tag:
                self.styles.append(
                    {
                        "path": list(path),
                        "role": "value",
                        "style": event.style,
                        "tag": event.tag,
                    }
                )
            return event.value, event.end_mark
        if isinstance(event, MappingStartEvent):
            result = {}
            while True:
                key_event = self.take()
                if isinstance(key_event, MappingEndEvent):
                    return result, key_event.end_mark
                if not isinstance(key_event, ScalarEvent):
                    raise ValueError("Complex mapping key")
                key = key_event.value
                if key in result:
                    raise ValueError(
                        f"Duplicate mapping key {key!r}, line "
                        f"{key_event.start_mark.line + 1}"
                    )
                self.key(key_event, path + (key,))
                result[key], _ = self.value(self.take(), path + (key,), depth + 1)
        elif isinstance(event, SequenceStartEvent):
            result = []
            while True:
                child = self.take()
                if isinstance(child, SequenceEndEvent):
                    return result, child.end_mark
                value, _ = self.value(child, path + (len(result),), depth + 1)
                result.append(value)
        else:
            raise ValueError(f"Unsupported event {type(event).__name__}")


class SourceFragments:
    """Second sequential reader retaining original flight YAML, without full-file text."""

    def __init__(self, path):
        self.handle = Path(path).open(encoding="utf-8-sig", newline="")
        self.line = ""
        self.number = 0
        self.eof = False

    def advance(self):
        self.line = self.handle.readline()
        if self.line:
            self.number += 1
        else:
            self.eof = True

    def fragment(self, start_line, end_mark):
        while self.number < start_line and not self.eof:
            self.advance()
        stop_line = end_mark.line + 1
        parts = []
        size = 0
        while not self.eof and self.number <= stop_line:
            if self.number == stop_line:
                # A block-end mark may point at the indentation of the next key.
                prefix = self.line[: end_mark.column]
                if prefix.strip():
                    parts.append(prefix)
                break
            parts.append(self.line)
            size += len(self.line)
            if size > LIMITS["raw_flight_characters"]:
                raise ValueError("Raw flight fragment exceeds configured memory bound")
            self.advance()
        text = "".join(parts)
        return text, start_line + len(text.splitlines()) - 1

    def finish(self):
        while not self.eof:
            self.advance()
        return self.number

    def close(self):
        self.handle.close()


class Sink:
    def __init__(self, path, schema, batch_size=2048):
        self.writer = pq.ParquetWriter(path, schema, compression="zstd")
        self.schema = schema
        self.batch_size = batch_size
        self.rows = []
        self.buffer_characters = 0
        self.count = 0

    def add(self, row):
        self.rows.append(row)
        self.buffer_characters += sum(
            len(v) for v in row.values() if isinstance(v, str)
        )
        self.count += 1
        if len(self.rows) >= self.batch_size or self.buffer_characters >= 4_000_000:
            self.flush()

    def flush(self):
        if self.rows:
            self.writer.write_table(pa.Table.from_pylist(self.rows, schema=self.schema))
            self.rows.clear()
            self.buffer_characters = 0

    def close(self):
        self.flush()
        self.writer.close()


def schemas():
    common = [
        pa.field(name, pa.string())
        for name in [
            "source_file",
            "source_sha256",
            "rule_version",
            "date_raw",
            "flight_number_raw",
            "flight_number",
            "origin_raw",
            "origin",
            "destination_raw",
            "destination",
            "status_raw",
            "status",
        ]
    ] + [pa.field("date", pa.date32()), pa.field("source_date_line", pa.int64())]
    flight = (
        common
        + [
            pa.field(name, pa.string())
            for name in [
                "flight_record_id",
                "date_key_style",
                "flight_key_style",
                "raw_flight_yaml",
                "raw_flight_json",
                "scalar_styles_json",
                "field_status_json",
            ]
        ]
        + [
            pa.field(name, pa.int64())
            for name in [
                "source_start_line",
                "source_end_line",
                "loyalty_entry_count",
            ]
        ]
    )
    entry = (
        common
        + [
            pa.field(name, pa.string())
            for name in [
                "entry_id",
                "flight_record_id",
                "loyalty_key_raw",
                "loyalty_program",
                "loyalty_number",
                "class_raw",
                "travel_class",
                "fare_raw",
                "fare",
                "raw_entry_json",
                "loyalty_key_style",
                "field_status_json",
            ]
        ]
        + [pa.field("source_line", pa.int64()), pa.field("source_column", pa.int32())]
    )
    return (
        pa.schema(
            flight,
            metadata={
                b"rule_version": VERSION.encode(),
                b"unit": b"source date-flight mapping",
            },
        ),
        pa.schema(
            entry,
            metadata={
                b"rule_version": VERSION.encode(),
                b"unit": b"FF loyalty mapping entry within date-flight",
            },
        ),
    )


def source_records(handle):
    """Yield one (date key, flight key, subtree) at a time, detecting duplicate keys."""
    reader = EventReader(handle)
    reader.take(StreamStartEvent)
    reader.take(DocumentStartEvent)
    reader.take(MappingStartEvent)
    seen_dates = set()
    while True:
        day = reader.take()
        if isinstance(day, MappingEndEvent):
            break
        if not isinstance(day, ScalarEvent):
            raise ValueError("Date key must be scalar")
        if day.value in seen_dates:
            raise ValueError(f"Repeated date key {day.value!r}")
        seen_dates.add(day.value)
        reader.take(MappingStartEvent)
        seen_flights = set()
        while True:
            flight = reader.take()
            if isinstance(flight, MappingEndEvent):
                break
            if not isinstance(flight, ScalarEvent):
                raise ValueError("Flight key must be scalar")
            if flight.value in seen_flights:
                raise ValueError(f"Repeated flight {day.value}/{flight.value}")
            seen_flights.add(flight.value)
            reader.node_count, reader.text_characters = 0, 0
            reader.locations, reader.styles = {}, []
            record, end_mark = reader.value(reader.take())
            if not isinstance(record, dict):
                raise ValueError("Flight value must be a mapping")
            yield day, flight, record, end_mark, reader.locations, reader.styles
    reader.take(DocumentEndEvent)
    reader.take(StreamEndEvent)


def run(source, output, max_flights=None):
    source, output = Path(source), Path(output)
    partial = output.with_name(output.name + ".partial")
    if output.exists() or partial.exists():
        raise FileExistsError(
            "Use a fresh output directory; results are not overwritten"
        )
    partial.mkdir(parents=True)
    start = time.perf_counter()
    digest = sha_file(source)
    flight_schema, entry_schema = schemas()
    flights = Sink(partial / "flights.parquet", flight_schema)
    entries = Sink(partial / "loyalty_entries.parquet", entry_schema)
    fragments = SourceFragments(source)
    by_day, field_statuses = {}, Counter()
    flight_digest, entry_digest = hashlib.sha256(), hashlib.sha256()
    truncated = False
    max_raw_chars = 0
    try:
        with source.open(encoding="utf-8-sig", newline="") as handle:
            records = source_records(handle)
            for day, flight, record, end_mark, locations, styles in records:
                if max_flights is not None and flights.count >= max_flights:
                    truncated = True
                    break
                cards = record.get("FF", {})
                if not isinstance(cards, dict):
                    raise ValueError(
                        f"FF must be a mapping at {day.value}/{flight.value}"
                    )
                base = {
                    "source_file": source.name,
                    "source_sha256": digest,
                    "rule_version": VERSION,
                    "date_raw": day.value,
                    "flight_number_raw": flight.value,
                    "source_date_line": day.start_mark.line + 1,
                }
                statuses = {}
                for field, raw, kind in [
                    ("date", day.value, "date"),
                    ("flight_number", flight.value, "flight"),
                    ("origin", record.get("FROM"), "airport"),
                    ("destination", record.get("TO"), "airport"),
                    ("status", record.get("STATUS"), "text"),
                ]:
                    if not isinstance(raw, (str, type(None))):
                        raise ValueError(
                            f"Expected scalar {field} at {day.value}/{flight.value}"
                        )
                    if field not in {"date", "flight_number"}:
                        base[field + "_raw"] = raw
                    base[field], statuses[field] = normalise(raw, kind)
                    field_statuses["flight." + field + ":" + statuses[field]] += 1
                raw_yaml, end_line = fragments.fragment(
                    flight.start_mark.line + 1, end_mark
                )
                max_raw_chars = max(max_raw_chars, len(raw_yaml))
                flight_id = stable_id(digest, day.value, flight.value)
                flights.add(
                    {
                        **base,
                        "flight_record_id": flight_id,
                        "date_key_style": day.style,
                        "flight_key_style": flight.style,
                        "source_start_line": flight.start_mark.line + 1,
                        "source_end_line": end_line,
                        "loyalty_entry_count": len(cards),
                        "raw_flight_yaml": raw_yaml,
                        "raw_flight_json": canonical(record),
                        "scalar_styles_json": canonical(styles),
                        "field_status_json": canonical(statuses),
                    }
                )
                update_digest(
                    flight_digest,
                    [
                        day.value,
                        flight.value,
                        record.get("FROM"),
                        record.get("TO"),
                        record.get("STATUS"),
                        len(cards),
                    ],
                )
                for card_key, detail in cards.items():
                    if not isinstance(detail, dict):
                        raise ValueError(f"FF detail must be a mapping: {card_key!r}")
                    raw_class, raw_fare = detail.get("CLASS"), detail.get("FARE")
                    if not all(
                        isinstance(x, (str, type(None))) for x in [raw_class, raw_fare]
                    ):
                        raise ValueError("CLASS/FARE must be scalar")
                    travel_class, class_status = normalise(raw_class)
                    fare, fare_status = normalise(raw_fare)
                    match = re.fullmatch(
                        r"([A-Z0-9]{2,3})\s+([0-9]+)", card_key.strip()
                    )
                    entry_status = {
                        "loyalty_key": "program_plus_string_number"
                        if match
                        else "unparsed_key_preserved",
                        "travel_class": class_status,
                        "fare": fare_status,
                    }
                    for field, status in entry_status.items():
                        field_statuses["entry." + field + ":" + status] += 1
                    line, column, style = locations[("FF", card_key)]
                    entries.add(
                        {
                            **base,
                            "entry_id": stable_id(flight_id, card_key),
                            "flight_record_id": flight_id,
                            "loyalty_key_raw": card_key,
                            "loyalty_program": match.group(1) if match else None,
                            "loyalty_number": match.group(2) if match else None,
                            "source_line": line,
                            "source_column": column,
                            "loyalty_key_style": style,
                            "class_raw": raw_class,
                            "travel_class": travel_class,
                            "fare_raw": raw_fare,
                            "fare": fare,
                            "raw_entry_json": canonical(detail),
                            "field_status_json": canonical(entry_status),
                        }
                    )
                    update_digest(
                        entry_digest,
                        [day.value, flight.value, card_key, raw_class, raw_fare],
                    )
                daily = by_day.setdefault(
                    day.value,
                    {"flights": 0, "entries": 0, "flights_without_entries": 0},
                )
                daily["flights"] += 1
                daily["entries"] += len(cards)
                daily["flights_without_entries"] += not cards
                if flights.count == 1 or flights.count % 100_000 == 0:
                    print(
                        f"Flights {flights.count:,}; FF entries {entries.count:,}; "
                        f"date {day.value}",
                        flush=True,
                    )
            records.close()
        flights.close()
        entries.close()
        lines = fragments.finish() if not truncated else None
        if sha_file(source) != digest:
            raise ValueError("Source changed during parsing")
        assert (
            pq.ParquetFile(partial / "flights.parquet").metadata.num_rows
            == flights.count
        )
        assert (
            pq.ParquetFile(partial / "loyalty_entries.parquet").metadata.num_rows
            == entries.count
        )
        result = {
            "rule_version": VERSION,
            "source_file": source.name,
            "source_bytes": source.stat().st_size,
            "source_sha256": digest,
            "physical_lines": lines,
            "complete": max_flights is None,
            "limited_run": max_flights is not None,
            "flight_rows": flights.count,
            "loyalty_entry_rows": entries.count,
            "days_with_flights": len(by_day),
            "by_day": by_day,
            "field_status_counts": dict(field_statuses),
            "read_errors": 0,
            "flight_content_digest": flight_digest.hexdigest(),
            "entry_content_digest": entry_digest.hexdigest(),
            "max_raw_flight_characters": max_raw_chars,
            "limits": LIMITS,
            "buffer_rows_per_table": 2048,
            "seconds": round(time.perf_counter() - start, 3),
            "buffer_character_limit_per_table": 4_000_000,
            "versions": {"pyarrow": pa.__version__, "PyYAML": yaml.__version__},
        }
        result["outputs"] = {
            p.name: {"bytes": p.stat().st_size, "sha256": sha_file(p)}
            for p in partial.glob("*.parquet")
        }
        (partial / "manifest.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2), encoding="utf8"
        )
        (partial / "schema.txt").write_text(
            "flights.parquet\n"
            + str(flight_schema)
            + "\n\nloyalty_entries.parquet\n"
            + str(entry_schema),
            encoding="utf8",
        )
        partial.rename(output)
        print(
            canonical(
                {
                    k: result[k]
                    for k in [
                        "complete",
                        "flight_rows",
                        "loyalty_entry_rows",
                        "physical_lines",
                        "seconds",
                    ]
                }
            ),
            flush=True,
        )
        return result
    except BaseException as error:
        # Partial files remain explicitly marked; no successful result directory is published.
        failure = {
            "complete": False,
            "error": f"{type(error).__name__}: {error}",
            "flight_rows_buffered_or_written": flights.count,
            "entry_rows_buffered_or_written": entries.count,
        }
        (partial / "FAILED.json").write_text(
            json.dumps(failure, ensure_ascii=False, indent=2), encoding="utf8"
        )
        for sink in [flights, entries]:
            try:
                sink.writer.close()
            except Exception:
                pass
        raise
    finally:
        fragments.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--output", type=Path, default=Path("result"))
    parser.add_argument(
        "--max-flights", type=int, help="Pilot only; never marked complete"
    )
    args = parser.parse_args()
    if args.max_flights is not None and args.max_flights < 1:
        parser.error("--max-flights must be positive")
    run(args.source, args.output, args.max_flights)
