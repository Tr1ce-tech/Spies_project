"""Parse pages 5+ of the SkyTeam edition into provenance-linked Parquet tables."""

from __future__ import annotations

import argparse
import collections
import json
import re
import time
from datetime import date
from functools import lru_cache
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pymupdf

if __package__:
    from .parse_intro import MONTHS, parse_intro
else:
    from parse_intro import MONTHS, parse_intro

VERSION = "skyteam.schedule.v1"
BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT = BASE_DIR / "data/pdf_schedule"
ROW = re.compile(
    r"^(?P<validity_raw>(?P<start_day>\d{2})\s+(?P<start_month>[A-Za-z]{3})\s*-\s*"
    r"(?P<end_day>\d{2})\s+(?P<end_month>[A-Za-z]{3}))\s+"
    r"(?P<days_raw>[1-7 ]+?)\s+(?P<departure_raw>\d{2}:\d{2})\s+"
    r"(?P<arrival_raw>\d{2}:\d{2})(?P<arrival_offset_raw>\s*\+-?\d+)?\s+"
    r"(?P<flight_raw>[A-Z0-9]{2}\d+[A-Z]?\*?)\s+"
    r"(?P<aircraft_code>[A-Z0-9]{3})\s+(?P<duration_raw>\d+H\d+M)$"
)
TABLE_HEADERS = re.compile(
    r"^(?:Validity|Time|Dep|Arr|Days|Flight|Aircraft|Travel)(?:\s|$)"
)
NO_DETAILS = "Consult your travel agent for details"


@lru_cache(maxsize=2048)
def resolve_period(start_day, start_month, end_day, end_month, cover_start, cover_end):
    """Assign years only if exactly one interval fits explicit cover boundaries."""
    try:
        pairs = []
        for y1 in range(cover_start.year, cover_end.year + 1):
            for y2 in range(cover_start.year, cover_end.year + 1):
                a = date(y1, MONTHS[start_month], int(start_day))
                b = date(y2, MONTHS[end_month], int(end_day))
                if cover_start <= a <= b <= cover_end:
                    pairs.append((a.isoformat(), b.isoformat()))
        if len(pairs) == 1:
            return (*pairs[0], "unique_within_cover_period")
        return None, None, "ambiguous_or_outside_cover"
    except (KeyError, ValueError):
        return None, None, "invalid_date"


def header_value(raw):
    """Retain geographical label literally; never infer city/country/timezone."""
    match = re.fullmatch(r"(.+?)\s+([A-Z0-9]{3})", raw.strip())
    return (match[1], match[2]) if match else (raw.strip(), None)


def valid_clock(raw):
    h, m = map(int, raw.split(":"))
    return h < 24 and m < 60


def parse_row(text, cover_start, cover_end, equipment_codes):
    match = ROW.fullmatch(text.strip())
    if not match:
        return None
    g = match.groupdict()
    a, b, date_status = resolve_period(
        g["start_day"],
        g["start_month"],
        g["end_day"],
        g["end_month"],
        cover_start,
        cover_end,
    )
    issues = []
    if date_status != "unique_within_cover_period":
        issues.append(date_status)
    offset_raw = (g["arrival_offset_raw"] or "").strip()
    if offset_raw.startswith("+-"):
        offset = None
        issues.append("ambiguous_arrival_day_marker")
    else:
        offset = int(offset_raw[1:]) if offset_raw else 0
    departure_ok, arrival_ok = (
        valid_clock(g["departure_raw"]),
        valid_clock(g["arrival_raw"]),
    )
    if not departure_ok or not arrival_ok:
        issues.append("invalid_local_clock")
    hours, minutes = map(int, re.fullmatch(r"(\d+)H(\d+)M", g["duration_raw"]).groups())
    duration = hours * 60 + minutes if minutes < 60 else None
    if duration is None:
        issues.append("invalid_duration")
    days = "".join(g["days_raw"].split())
    if list(days) != sorted(set(days)):
        issues.append("noncanonical_weekday_codes")
    aircraft_status = (
        "listed_in_intro"
        if g["aircraft_code"] in equipment_codes
        else "not_listed_in_intro"
    )
    return dict(
        validity_raw=g["validity_raw"],
        valid_from=a,
        valid_to=b,
        date_status=date_status,
        days_raw=g["days_raw"],
        days_codes=days,
        departure_raw=g["departure_raw"],
        departure_local=g["departure_raw"] if departure_ok else None,
        arrival_raw=g["arrival_raw"],
        arrival_local=g["arrival_raw"] if arrival_ok else None,
        arrival_offset_raw=offset_raw,
        arrival_day_offset=offset,
        flight_raw=g["flight_raw"],
        flight_number=g["flight_raw"].rstrip("*"),
        flight_marker="*" if g["flight_raw"].endswith("*") else "",
        aircraft_code=g["aircraft_code"],
        aircraft_dictionary_status=aircraft_status,
        duration_raw=g["duration_raw"],
        duration_minutes=duration,
        field_issues=";".join(issues),
    )


BASE = [
    ("raw_id", pa.string()),
    ("source_file", pa.string()),
    ("source_sha256", pa.string()),
    ("page", pa.int32()),
    ("column", pa.int8()),
    ("line", pa.int32()),
    ("raw_text", pa.string()),
    ("parser_version", pa.string()),
]
RAW_SCHEMA = pa.schema(BASE + [("line_kind", pa.string())])
SCHEDULE_SCHEMA = pa.schema(
    BASE
    + [
        (n, pa.string())
        for n in [
            "schedule_id",
            "origin_label_raw",
            "origin_code",
            "destination_label_raw",
            "destination_code",
            "origin_header_id",
            "destination_header_id",
            "route_status",
            "route_rule",
            "validity_raw",
            "valid_from",
            "valid_to",
            "date_status",
            "days_raw",
            "days_codes",
            "departure_raw",
            "departure_local",
            "arrival_raw",
            "arrival_local",
            "arrival_offset_raw",
            "flight_raw",
            "flight_number",
            "flight_marker",
            "aircraft_code",
            "aircraft_dictionary_status",
            "duration_raw",
            "field_issues",
            "parse_status",
            "verification_status",
            "normalization_rule",
        ]
    ]
    + [("arrival_day_offset", pa.int32()), ("duration_minutes", pa.int32())]
)
ISSUE_SCHEMA = pa.schema(BASE + [("reason", pa.string())])
NOTE_SCHEMA = pa.schema(
    BASE
    + [
        ("schedule_id", pa.string()),
        ("operator_raw", pa.string()),
        ("link_status", pa.string()),
        ("processing_rule", pa.string()),
    ]
)
PAGE_SCHEMA = pa.schema(
    [
        ("page", pa.int32()),
        ("column", pa.int8()),
        ("nonempty_lines", pa.int32()),
        ("parsed_rows", pa.int32()),
        ("row_like_lines", pa.int32()),
        ("quarantined_lines", pa.int32()),
    ]
)


class Sink:
    """Bounded buffers; the full PDF need not fit in a Python DataFrame at once."""

    def __init__(self, path, schema):
        self.schema = schema
        self.writer = pq.ParquetWriter(path, schema, compression="zstd")
        self.buffer = []
        self.count = 0

    def add(self, row):
        self.buffer.append(row)
        self.count += 1
        if len(self.buffer) >= 20000:
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


def parse_schedule(pdf_path, out=DEFAULT_OUTPUT, start_page=5, end_page=None):
    path, out = Path(pdf_path).resolve(), Path(out)
    # Refuse silent overwrite: interrupted and completed runs both remain inspectable.
    out.mkdir(parents=True, exist_ok=False)
    started = time.perf_counter()
    intro = parse_intro(path)
    cover = intro["metadata"].set_index("key")["value"]
    cover_start, cover_end = (
        date.fromisoformat(cover["coverage_start"]),
        date.fromisoformat(cover["coverage_end"]),
    )
    equipment = frozenset(intro["aircraft_types"]["aircraft_code"])
    source_sha = intro["pages"]["source_sha256"].iloc[0]
    manifest = dict(
        parser_version=VERSION,
        source_file=path.name,
        source_sha256=source_sha,
        coverage_start=cover_start.isoformat(),
        coverage_end=cover_end.isoformat(),
        status="running",
        start_page=start_page,
        end_page=end_page,
        unit="recurring schedule rule; not an individual or completed flight",
    )
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2))
    sinks = {
        "raw_lines": Sink(out / "raw_lines.parquet", RAW_SCHEMA),
        "schedule": Sink(out / "schedule.parquet", SCHEDULE_SCHEMA),
        "quarantine": Sink(out / "quarantine.parquet", ISSUE_SCHEMA),
        "operator_notes": Sink(out / "operator_notes.parquet", NOTE_SCHEMA),
        "pages": Sink(out / "pages.parquet", PAGE_SCHEMA),
    }
    contexts = [{}, {}]
    last_rows = [None, None]
    counts = collections.Counter()
    issue_counts = collections.Counter()
    try:
        with pymupdf.open(path) as doc:
            end_page = len(doc) if end_page is None else end_page
            if not 5 <= start_page <= end_page <= len(doc):
                raise ValueError(
                    "Expected 5 <= start_page <= end_page <= document length"
                )
            manifest["end_page"] = end_page
            for number in range(start_page, end_page + 1):
                page = doc[number - 1]
                for col in (0, 1):
                    context = contexts[col]
                    clip = pymupdf.Rect(
                        col * page.rect.width / 2,
                        0,
                        (col + 1) * page.rect.width / 2,
                        page.rect.height,
                    )
                    text = page.get_text("text", clip=clip, sort=True)
                    tally = collections.Counter()
                    for line_number, raw in enumerate(text.splitlines(), 1):
                        stripped = raw.strip()
                        if not stripped:
                            continue
                        tally["nonempty_lines"] += 1
                        raw_id = f"{source_sha}:{number}:{col}:{line_number}"
                        base = dict(
                            raw_id=raw_id,
                            source_file=path.name,
                            source_sha256=source_sha,
                            page=number,
                            column=col,
                            line=line_number,
                            raw_text=raw,
                            parser_version=VERSION,
                        )
                        reason = None
                        if stripped.startswith("FROM:"):
                            # A new origin invalidates the old destination, including on the same page.
                            context.clear()
                            last_rows[col] = None
                            label, code = header_value(stripped[5:])
                            context.update(
                                origin_label_raw=label,
                                origin_code=code,
                                origin_header_id=raw_id,
                                origin_page=number,
                            )
                            kind = "origin_header"
                        elif stripped.startswith("TO:"):
                            last_rows[col] = None
                            label, code = header_value(stripped[3:])
                            context.update(
                                destination_label_raw=label,
                                destination_code=code,
                                destination_header_id=raw_id,
                                destination_page=number,
                            )
                            kind = "destination_header"
                        elif stripped == NO_DETAILS:
                            last_rows[col] = None
                            kind = "no_schedule_details_notice"
                            counts["no_details_notices"] += 1
                        elif stripped.startswith("Operated by:"):
                            previous = last_rows[col]
                            sinks["operator_notes"].add(
                                dict(
                                    base,
                                    schedule_id=previous[0] if previous else None,
                                    operator_raw=stripped[
                                        len("Operated by:") :
                                    ].strip(),
                                    link_status=(
                                        "preceding_row_same_page"
                                        if previous[1] == number
                                        else "preceding_row_previous_page"
                                    )
                                    if previous
                                    else "unassigned",
                                    processing_rule="explicit_note_after_row_same_column.v1",
                                )
                            )
                            kind = "operator_note"
                        elif TABLE_HEADERS.match(stripped):
                            kind = "table_header"
                        elif re.match(r"^\d{1,2}\s+[A-Za-z]{3}", stripped) or re.search(
                            r"\d{2}:\d{2}", stripped
                        ):
                            tally["row_like_lines"] += 1
                            fields = parse_row(
                                stripped, cover_start, cover_end, equipment
                            )
                            if fields is None:
                                last_rows[col] = None
                                kind, reason = "quarantined", "row_pattern_not_matched"
                            else:
                                inherited = (
                                    context.get("origin_page") != number
                                    or context.get("destination_page") != number
                                )
                                known = all(
                                    context.get(k)
                                    for k in [
                                        "origin_code",
                                        "destination_code",
                                        "origin_header_id",
                                        "destination_header_id",
                                    ]
                                )
                                status = (
                                    "explicit_headers"
                                    if known and not inherited
                                    else "continued_same_column"
                                    if known
                                    else "incomplete_route"
                                )
                                if not known:
                                    fields["field_issues"] = ";".join(
                                        filter(
                                            None,
                                            [
                                                fields["field_issues"],
                                                "incomplete_route",
                                            ],
                                        )
                                    )
                                row = dict(
                                    base,
                                    **fields,
                                    schedule_id=raw_id,
                                    **{
                                        k: context.get(k)
                                        for k in [
                                            "origin_label_raw",
                                            "origin_code",
                                            "destination_label_raw",
                                            "destination_code",
                                            "origin_header_id",
                                            "destination_header_id",
                                        ]
                                    },
                                    route_status=status,
                                    route_rule="latest_explicit_headers_same_column.v1",
                                    parse_status="parsed_with_issues"
                                    if fields["field_issues"]
                                    else "parsed",
                                    verification_status="not_independently_verified",
                                    normalization_rule="unique_cover_years;local_clock;explicit_weekday_digits.v1",
                                )
                                sinks["schedule"].add(row)
                                last_rows[col] = (raw_id, number)
                                tally["parsed_rows"] += 1
                                counts["route_" + status] += 1
                                counts[
                                    "equipment_" + fields["aircraft_dictionary_status"]
                                ] += 1
                                for issue in filter(
                                    None, fields["field_issues"].split(";")
                                ):
                                    issue_counts[issue] += 1
                                kind = "schedule_row"
                        else:
                            kind, reason = "quarantined", "unclassified_text"
                            # Unknown text may be a broken route header; stop carrying that context.
                            context.clear()
                            last_rows[col] = None
                        if reason:
                            sinks["quarantine"].add(dict(base, reason=reason))
                            tally["quarantined_lines"] += 1
                            issue_counts[reason] += 1
                        sinks["raw_lines"].add(dict(base, line_kind=kind))
                        counts["line_" + kind] += 1
                    sinks["pages"].add(
                        dict(
                            page=number,
                            column=col,
                            **{
                                k: tally[k]
                                for k in [
                                    "nonempty_lines",
                                    "parsed_rows",
                                    "row_like_lines",
                                    "quarantined_lines",
                                ]
                            },
                        )
                    )
                    counts["row_like_lines"] += tally["row_like_lines"]
                if (number - start_page + 1) % 1000 == 0:
                    print(
                        f"page {number}/{end_page}; rows {sinks['schedule'].count:,}; seconds {time.perf_counter() - started:.1f}",
                        flush=True,
                    )
        manifest.update(
            status="complete",
            seconds=round(time.perf_counter() - started, 3),
            tables={k: v.count for k, v in sinks.items()},
            counters=dict(counts),
            field_issues=dict(issue_counts),
            verification="Technical parse only; selected pages require visual validation; inherited context explicitly flagged.",
        )
    except BaseException as exc:
        manifest.update(status="failed", error=repr(exc))
        raise
    finally:
        for sink in sinks.values():
            sink.close()
        (out / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def load_schedule(out=DEFAULT_OUTPUT, columns=None, filters=None):
    """Return a pandas DataFrame; column selection/predicate pushdown reduce RAM use."""
    manifest = json.loads((Path(out) / "manifest.json").read_text())
    if manifest["status"] != "complete":
        raise ValueError("Schedule extraction is incomplete; inspect manifest.json")
    return pd.read_parquet(
        Path(out) / "schedule.parquet",
        columns=columns,
        filters=filters,
        dtype_backend="pyarrow",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pdf", type=Path)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--start-page", type=int, default=5)
    parser.add_argument("--end-page", type=int)
    args = parser.parse_args()
    print(
        json.dumps(
            parse_schedule(args.pdf, args.out, args.start_page, args.end_page), indent=2
        )
    )
