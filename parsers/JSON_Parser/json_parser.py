"""Stream forum profiles, flight observations and loyalty entries into tables."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any, Iterator

INPUT_FILE = "../../raw_data/FrequentFlyerForum-Profiles.json"
OUTPUT_FORMAT = "parquet"  # "csv" или "parquet"
CHUNK_SIZE = 50_000
ENCODING = "utf-8-sig"  # UTF-8 с BOM и без BOM
READ_BLOCK_SIZE = 64 * 1024  # Размер чтения входного файла (символы)
MAX_JSON_VALUE_CHARS = 32_000_000  # Защита от бесконечного роста буфера


BASE_DIR = Path(__file__).resolve().parent
VERSION = "forum.json.v3"
PROVENANCE = ["source_sha256", "rule_version", "profile_record_id", "record_id"]

FLIGHT_COLUMNS = [
    "source_file",
    "profile_index",
    "flight_index",
    "flight_group_index",
    "Date",
    "Flight",
    "Codeshare",
    "Departure_City",
    "Departure_Airport",
    "Departure_Country",
    "Arrival_City",
    "Arrival_Airport",
    "Arrival_Country",
    "parse_status",
    "extra_fields_json",
    "raw_flight_json",
]
PROFILE_COLUMNS = [
    "source_file",
    "profile_index",
    "flight_rows",
    "loyalty_rows",
    "profile_status",
    "metadata_json",
    "raw_profile_json",
]
LOYALTY_COLUMNS = [
    "source_file",
    "profile_index",
    "loyalty_index",
    "loyalty_group_index",
    "loyalty_key",
    "Status",
    "programm",
    "Number",
    "parse_status",
    "extra_fields_json",
    "raw_loyalty_json",
]
LOYALTY_COLUMNS += PROVENANCE
FLIGHT_COLUMNS += PROVENANCE
PROFILE_COLUMNS += PROVENANCE


class PairsDict(dict):
    """Dictionary that also remembers repeated keys and their *all* values."""

    def __init__(self, pairs: list[tuple[str, Any]]):
        super().__init__()
        self.pairs = pairs
        self.repeated_keys = []
        for key, value in pairs:
            if key in self and key not in self.repeated_keys:
                self.repeated_keys.append(key)
            self[key] = value


def refuse_nonstandard_constant(value: str) -> None:
    raise ValueError(f"Invalid JSON constant: {value}")


class StreamReader:
    """Incremental JSON reader. Decodes one top-level member/profile at a time."""

    def __init__(
        self, stream, block_size=READ_BLOCK_SIZE, max_value_chars=MAX_JSON_VALUE_CHARS
    ):
        self.stream = stream
        self.block_size = block_size
        self.max_value_chars = max_value_chars
        self.buf = ""
        self.pos = 0
        self.absolute = 0
        self.eof = False
        self.decoder = json.JSONDecoder(
            object_pairs_hook=PairsDict, parse_constant=refuse_nonstandard_constant
        )

    def _read_more(self):
        if self.pos:
            self.absolute += self.pos
            self.buf = self.buf[self.pos :]
            self.pos = 0
        data = self.stream.read(self.block_size)
        if not data:
            self.eof = True
        else:
            self.buf += data

    def peek(self):
        while True:
            while self.pos < len(self.buf) and self.buf[self.pos] in " \t\r\n":
                self.pos += 1
            if self.pos < len(self.buf):
                return self.buf[self.pos]
            if self.eof:
                return None
            self._read_more()

    def expect(self, char):
        found = self.peek()
        if found != char:
            raise ValueError(
                f"Expected {char!r}, got {found!r} at character {self.absolute + self.pos}"
            )
        self.pos += 1

    def value(self):
        self.peek()
        start_abs = self.absolute + self.pos
        while True:
            if self.absolute + self.pos != start_abs:
                raise RuntimeError("Internal reader position error")
            try:
                obj, end = self.decoder.raw_decode(self.buf, self.pos)
                # Number/string may end at a block boundary; read more to verify.
                if end == len(self.buf) and not self.eof:
                    self._grow(start_abs)
                    continue
                raw = self.buf[self.pos : end]
                self.pos = end
                return obj, raw
            except json.JSONDecodeError as exc:
                if self.eof:
                    raise ValueError(
                        f"Invalid or incomplete JSON near character {start_abs}: {exc.msg}"
                    ) from exc
                self._grow(start_abs)
            except ValueError as exc:
                # Includes non-standard NaN/Infinity from parse_constant.
                raise ValueError(
                    f"Invalid JSON near character {start_abs}: {exc}"
                ) from exc

    def _grow(self, start_abs):
        chars_in_value = self.absolute + len(self.buf) - start_abs
        if chars_in_value > self.max_value_chars:
            raise ValueError(
                f"JSON value exceeds MAX_JSON_VALUE_CHARS={self.max_value_chars}; "
                f"started at character {start_abs}. Nothing is discarded."
            )
        self._read_more_preserve()

    def _read_more_preserve(self):
        if self.pos:
            self.absolute += self.pos
            self.buf = self.buf[self.pos :]
            self.pos = 0
        data = self.stream.read(self.block_size)
        if not data:
            self.eof = True
        else:
            self.buf += data


def iter_profiles(
    reader: StreamReader, on_extra=None
) -> Iterator[tuple[int, Any, str]]:
    """Iterate every element in Forum Profiles while validating the root structure."""
    reader.expect("{")
    found_profiles = False
    root_keys = set()
    if reader.peek() == "}":
        reader.expect("}")
    else:
        while True:
            key, _ = reader.value()
            if not isinstance(key, str):
                raise ValueError("Root JSON member name is not a string")
            if key in root_keys:
                raise ValueError(
                    f"Duplicate root key {key!r}: cannot choose a value safely"
                )
            root_keys.add(key)
            reader.expect(":")
            if key == "Forum Profiles":
                found_profiles = True
                reader.expect("[")
                idx = 0
                if reader.peek() != "]":
                    while True:
                        obj, raw = reader.value()
                        yield idx, obj, raw
                        idx += 1
                        separator = reader.peek()
                        if separator == ",":
                            reader.expect(",")
                            continue
                        if separator == "]":
                            break
                        raise ValueError(f"Expected ',' or ']' after profile {idx - 1}")
                reader.expect("]")
            else:
                # Preserve unexpected root metadata as raw JSON, without losing it.
                _, raw = reader.value()
                if on_extra:
                    on_extra(key, raw)
            sep = reader.peek()
            if sep == ",":
                reader.expect(",")
                continue
            if sep == "}":
                reader.expect("}")
                break
            raise ValueError("Expected ',' or '}' after a root member")
    if not found_profiles:
        raise ValueError("Root key 'Forum Profiles' not found")
    if reader.peek() is not None:
        raise ValueError("Non-whitespace data after the root JSON object")


def compact_json(value: Any) -> str:
    if isinstance(value, dict):
        pairs = value.pairs if isinstance(value, PairsDict) else value.items()
        return (
            "{"
            + ",".join(compact_json(k) + ":" + compact_json(v) for k, v in pairs)
            + "}"
        )
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(compact_json(v) for v in value) + "]"
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def all_duplicate_paths(value: Any, prefix: str = "") -> Iterator[str]:
    if isinstance(value, PairsDict):
        for key in value.repeated_keys:
            yield f"{prefix}.{key}" if prefix else key
        for key, child in value.pairs:
            yield from all_duplicate_paths(child, f"{prefix}.{key}" if prefix else key)
    elif isinstance(value, list):
        for i, child in enumerate(value):
            yield from all_duplicate_paths(child, f"{prefix}[{i}]")


def parse_flight(
    value: Any,
    source_name: str,
    profile_index: int,
    flight_index: int,
    group_index: int,
) -> tuple[dict, list[dict]]:
    errors = []
    row = dict.fromkeys(FLIGHT_COLUMNS)
    row.update(
        source_file=source_name,
        profile_index=profile_index,
        flight_index=flight_index,
        flight_group_index=group_index,
        raw_flight_json=compact_json(value),
        parse_status="ok",
    )

    def issue(name, detail, field=""):
        errors.append(
            dict(
                source_file=source_name,
                profile_index=profile_index,
                flight_index=flight_index,
                issue_type=name,
                field=field,
                detail=str(detail),
            )
        )

    if not isinstance(value, dict):
        row["parse_status"] = "review"
        issue("invalid_flight_type", f"Expected object; got {type(value).__name__}")
        return row, errors

    for field in ("Date", "Flight"):
        val = value.get(field)
        if val is None:
            issue("missing_field", "Missing/null field", field)
        elif not isinstance(val, str):
            issue("invalid_field_type", type(val).__name__, field)
        else:
            row[field] = val  # No normalization here.
    if isinstance(row["Date"], str):
        try:
            if date.fromisoformat(row["Date"]).isoformat() != row["Date"]:
                raise ValueError("Expected YYYY-MM-DD")
        except ValueError:
            issue("invalid_date", row["Date"], "Date")
    codeshare = value.get("Codeshare")
    if isinstance(codeshare, bool):
        row["Codeshare"] = codeshare
    elif codeshare is None:
        issue("missing_field", "Missing/null field", "Codeshare")
    else:
        issue("invalid_field_type", type(codeshare).__name__, "Codeshare")

    for side in ("Departure", "Arrival"):
        place = value.get(side)
        if not isinstance(place, dict):
            issue(
                "invalid_nested_object",
                f"Expected object; got {type(place).__name__}",
                side,
            )
            continue
        for key in ("City", "Airport", "Country"):
            field = f"{side}_{key}"
            item = place.get(key)
            if item is None:
                issue("missing_field", "Missing/null field", field)
            elif not isinstance(item, str):
                issue("invalid_field_type", type(item).__name__, field)
            else:
                row[field] = item
                if key == "Airport" and not re.fullmatch(r"[A-Z]{3}", item):
                    issue("unexpected_airport_code", item, field)
    extras = [
        (k, v)
        for k, v in (value.pairs if isinstance(value, PairsDict) else value.items())
        if k not in {"Date", "Flight", "Codeshare", "Departure", "Arrival"}
    ]
    row["extra_fields_json"] = compact_json(extras) if extras else None
    duplicate_paths = list(all_duplicate_paths(value))
    for path in duplicate_paths:
        issue(
            "duplicate_json_key",
            "All original occurrences preserved in raw_profile_json",
            path,
        )
    if duplicate_paths:
        for field in [
            "Date",
            "Flight",
            "Codeshare",
            "Departure_City",
            "Departure_Airport",
            "Departure_Country",
            "Arrival_City",
            "Arrival_Airport",
            "Arrival_Country",
        ]:
            row[field] = None
    if errors:
        row["parse_status"] = "review"
    return row, errors


def parse_loyalty(
    value: Any,
    source_name: str,
    profile_index: int,
    loyalty_index: int,
    group_index: int,
    key: str,
):
    """Preserve each loyalty element, including malformed values."""
    errors = []
    row = dict.fromkeys(LOYALTY_COLUMNS)
    row.update(
        source_file=source_name,
        profile_index=profile_index,
        loyalty_index=loyalty_index,
        loyalty_group_index=group_index,
        loyalty_key=key,
        raw_loyalty_json=compact_json(value),
        parse_status="ok",
    )

    def issue(name, detail, field=""):
        errors.append(
            dict(
                source_file=source_name,
                profile_index=profile_index,
                flight_index="",
                loyalty_index=loyalty_index,
                issue_type=name,
                field=field,
                detail=str(detail),
            )
        )

    if not isinstance(value, dict):
        issue(
            "invalid_loyalty_type", f"Expected object; got {type(value).__name__}", key
        )
        row["parse_status"] = "review"
        return row, errors

    for field in ("Status", "programm", "Number"):
        if field not in value or value[field] is None:
            issue("missing_loyalty_field", "Missing or null", field)
        elif not isinstance(value[field], str):
            issue("invalid_loyalty_field_type", type(value[field]).__name__, field)
        else:
            row[field] = value[field]
    pairs = value.pairs if isinstance(value, PairsDict) else list(value.items())
    extras = [(k, v) for k, v in pairs if k not in {"Status", "programm", "Number"}]
    row["extra_fields_json"] = compact_json(extras) if extras else None
    duplicate_paths = list(all_duplicate_paths(value))
    for path in duplicate_paths:
        issue(
            "duplicate_json_key", "All occurrences retained in raw_loyalty_json", path
        )
    if duplicate_paths:
        for field in ("Status", "programm", "Number"):
            row[field] = None
    if errors:
        row["parse_status"] = "review"
    return row, errors


def extract_profile_loyalties(value: Any, source_name: str, profile_index: int):
    """Extract all occurrences of the literal 'Loyality Programm' profile key."""
    loyalties, issues = [], []
    if not isinstance(value, dict):
        return loyalties, issues
    pairs = value.pairs if isinstance(value, PairsDict) else list(value.items())
    groups = [(k, v) for k, v in pairs if k == "Loyality Programm"]
    for group_index, (key, group) in enumerate(groups):
        if not isinstance(group, list):
            issues.append(
                dict(
                    source_file=source_name,
                    profile_index=profile_index,
                    flight_index="",
                    loyalty_index="",
                    issue_type="invalid_loyalty_list",
                    field=key,
                    detail=f"Expected list; got {type(group).__name__}",
                )
            )
            continue
        for element in group:
            row, errors = parse_loyalty(
                element, source_name, profile_index, len(loyalties), group_index, key
            )
            loyalties.append(row)
            issues.extend(errors)
    return loyalties, issues


def parse_profile(value: Any, raw: str, source_name: str, profile_index: int):
    """Return profile row, flight rows, issues. No profile or flight element is dropped."""
    issues, flights = [], []
    profile = dict(
        source_file=source_name,
        profile_index=profile_index,
        flight_rows=0,
        profile_status="ok",
        metadata_json=None,
        raw_profile_json=raw,
    )

    def issue(name, detail, field=""):
        issues.append(
            dict(
                source_file=source_name,
                profile_index=profile_index,
                flight_index="",
                issue_type=name,
                field=field,
                detail=str(detail),
            )
        )

    if not isinstance(value, dict):
        profile["profile_status"] = "review"
        issue("invalid_profile_type", f"Expected object; got {type(value).__name__}")
        return profile, flights, issues

    pairs = value.pairs if isinstance(value, PairsDict) else list(value.items())
    metadata = [(key, item) for key, item in pairs if key != "Registered Flights"]
    profile["metadata_json"] = compact_json(metadata)
    flight_groups = [(key, item) for key, item in pairs if key == "Registered Flights"]
    for key in value.repeated_keys if isinstance(value, PairsDict) else []:
        issue(
            "duplicate_json_key",
            "Original occurrences retained in raw_profile_json",
            key,
        )
    for key, item in metadata:
        for path in all_duplicate_paths(item, key):
            issue(
                "duplicate_json_key",
                "Original occurrences retained in raw_profile_json",
                path,
            )
    if not flight_groups:
        issue(
            "missing_flights_list",
            "Registered Flights key absent",
            "Registered Flights",
        )
    for group_index, (_, group) in enumerate(flight_groups):
        if not isinstance(group, list):
            issue(
                "invalid_flights_list",
                f"Expected list; got {type(group).__name__}",
                "Registered Flights",
            )
            continue
        for item in group:
            row, flight_issues = parse_flight(
                item, source_name, profile_index, len(flights), group_index
            )
            flights.append(row)
            issues.extend(flight_issues)
    profile["flight_rows"] = len(flights)
    if issues:
        profile["profile_status"] = "review"
    elif not flights:
        profile["profile_status"] = "no_flights"
    return profile, flights, issues


class OutputWriter:
    def __init__(
        self, base: Path, prefix: str, columns: list[str], fmt: str, chunk_size: int
    ):
        self.base, self.prefix, self.columns = base, prefix, columns
        self.fmt, self.chunk_size = fmt, chunk_size
        self.buffer: list[dict] = []
        self.parts, self.rows = 0, 0
        self.csv_file = None
        self.csv_writer = None
        if fmt == "csv":
            self.csv_file = (base / f"{prefix}.csv").open(
                "w", encoding="utf-8-sig", newline=""
            )
            self.csv_writer = csv.DictWriter(self.csv_file, fieldnames=columns)
            self.csv_writer.writeheader()

    def add(self, row):
        self.buffer.append(row)
        if len(self.buffer) >= self.chunk_size:
            self.flush()

    def flush(self):
        if not self.buffer:
            return
        if self.fmt == "csv":
            self.csv_writer.writerows(self.buffer)
        else:
            import pandas as pd

            df = pd.DataFrame.from_records(self.buffer, columns=self.columns)
            if self.prefix == "json_flights":
                df["Codeshare"] = pd.array(df["Codeshare"], dtype="boolean")
            for c in df.columns:
                if c not in (
                    "Codeshare",
                    "profile_index",
                    "flight_index",
                    "flight_group_index",
                    "flight_rows",
                    "loyalty_rows",
                    "loyalty_index",
                    "loyalty_group_index",
                ):
                    df[c] = pd.array(df[c], dtype="string")
            df.to_parquet(
                self.base / f"{self.prefix}_part_{self.parts:05d}.parquet",
                index=False,
                engine="pyarrow",
            )
        self.rows += len(self.buffer)
        self.parts += 1
        self.buffer.clear()

    def close(self):
        try:
            self.flush()
        finally:
            if self.csv_file:
                self.csv_file.close()


def process_file(
    path: Path,
    output_dir=BASE_DIR,
    fmt=OUTPUT_FORMAT,
    chunk_size=CHUNK_SIZE,
    encoding=ENCODING,
) -> dict:
    if fmt not in ("parquet", "csv"):
        raise ValueError("OUTPUT_FORMAT must be 'parquet' or 'csv'")
    if chunk_size < 1:
        raise ValueError("CHUNK_SIZE must be positive")
    if not path.is_file():
        raise FileNotFoundError(f"File not found: {path}")
    output_dir.mkdir(parents=True, exist_ok=True)
    if fmt == "parquet":
        try:
            import pyarrow  # noqa: F401
        except ImportError as exc:
            raise RuntimeError("Install dependency: pip install pyarrow") from exc
    # Do not overwrite existing results, or mix old and new Parquet chunks.
    output_names = ["json_issues.csv", "json_summary.json", "json_root_extras.csv"]
    for prefix in ("json_flights", "json_profiles", "json_loyalty"):
        if fmt == "csv":
            output_names.append(prefix + ".csv")
        else:
            output_names.extend(
                p.name for p in output_dir.glob(prefix + "_part_*.parquet")
            )
    collisions = [name for name in output_names if (output_dir / name).exists()]
    if collisions:
        raise FileExistsError(
            "Existing JSON parser results would be overwritten: "
            + ", ".join(collisions[:8])
            + ". Move old outputs before rerunning."
        )

    flight_writer = profile_writer = loyalty_writer = None
    issue_file = root_extras_file = None
    stats = {
        "source_file": str(path),
        "completed": False,
        "profiles": 0,
        "root_extra_members": 0,
        "flight_elements": 0,
        "flights_written": 0,
        "profiles_written": 0,
        "loyalty_elements": 0,
        "loyalties_written": 0,
        "issues": 0,
        "issues_by_type": {},
        "format": fmt,
    }
    with path.open("rb") as handle:
        source_sha = hashlib.file_digest(handle, "sha256").hexdigest()
    stats.update(source_sha256=source_sha, rule_version=VERSION)
    issue_counts = Counter()
    run_error = None
    try:
        flight_writer = OutputWriter(
            output_dir, "json_flights", FLIGHT_COLUMNS, fmt, chunk_size
        )
        profile_writer = OutputWriter(
            output_dir, "json_profiles", PROFILE_COLUMNS, fmt, chunk_size
        )
        loyalty_writer = OutputWriter(
            output_dir, "json_loyalty", LOYALTY_COLUMNS, fmt, chunk_size
        )
        issue_file = (output_dir / "json_issues.csv").open(
            "w", encoding="utf-8-sig", newline=""
        )
        issues_csv = csv.DictWriter(
            issue_file,
            fieldnames=[
                "source_file",
                "profile_index",
                "flight_index",
                "loyalty_index",
                "issue_type",
                "field",
                "detail",
            ],
        )
        issues_csv.writeheader()
        root_extras_file = (output_dir / "json_root_extras.csv").open(
            "w", encoding="utf-8-sig", newline=""
        )
        extra_csv = csv.DictWriter(
            root_extras_file, fieldnames=["source_file", "field", "raw_value_json"]
        )
        extra_csv.writeheader()

        def keep_root_extra(key, raw):
            extra_csv.writerow(
                {"source_file": path.name, "field": key, "raw_value_json": raw}
            )
            stats["root_extra_members"] += 1

        with path.open("r", encoding=encoding, errors="strict", newline="") as src:
            reader = StreamReader(src)
            for profile_index, obj, raw in iter_profiles(
                reader, on_extra=keep_root_extra
            ):
                profile, flights, issues = parse_profile(
                    obj, raw, path.name, profile_index
                )
                loyalties, loyalty_issues = extract_profile_loyalties(
                    obj, path.name, profile_index
                )
                issues.extend(loyalty_issues)
                profile["loyalty_rows"] = len(loyalties)
                if loyalty_issues:
                    profile["profile_status"] = "review"
                profile_id = hashlib.sha256(
                    f"{source_sha}:{profile_index}".encode()
                ).hexdigest()
                profile.update(
                    source_sha256=source_sha,
                    rule_version=VERSION,
                    profile_record_id=profile_id,
                    record_id=profile_id,
                )
                stats["profiles"] += 1
                for row in flights:
                    row.update(
                        source_sha256=source_sha,
                        rule_version=VERSION,
                        profile_record_id=profile_id,
                        record_id=hashlib.sha256(
                            f"{profile_id}:{row['flight_group_index']}:{row['flight_index']}".encode()
                        ).hexdigest(),
                    )
                    flight_writer.add(row)
                for row in loyalties:
                    row.update(
                        source_sha256=source_sha,
                        rule_version=VERSION,
                        profile_record_id=profile_id,
                        record_id=hashlib.sha256(
                            f"{profile_id}:loyalty:{row['loyalty_group_index']}:{row['loyalty_index']}".encode()
                        ).hexdigest(),
                    )
                    loyalty_writer.add(row)
                stats["loyalty_elements"] += len(loyalties)
                stats["flight_elements"] += len(flights)
                profile_writer.add(profile)
                for issue in issues:
                    issues_csv.writerow(issue)
                    issue_counts[issue["issue_type"]] += 1
                    stats["issues"] += 1
        with path.open("rb") as handle:
            if hashlib.file_digest(handle, "sha256").hexdigest() != source_sha:
                raise ValueError("Source changed during parsing")
        stats["completed"] = True
    except Exception as exc:
        run_error = exc
        stats["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        for writer, count_key in [
            (flight_writer, "flights_written"),
            (profile_writer, "profiles_written"),
            (loyalty_writer, "loyalties_written"),
        ]:
            if writer is None:
                continue
            try:
                writer.close()
            except Exception as exc:
                run_error = run_error or exc
                stats["completed"] = False
                stats["error"] = f"{type(exc).__name__}: {exc}"
            stats[count_key] = writer.rows
        if issue_file:
            issue_file.close()
        if root_extras_file:
            root_extras_file.close()
        stats["issues_by_type"] = dict(issue_counts)
        stats["integrity_passed"] = bool(
            stats["completed"]
            and stats["flight_elements"] == stats["flights_written"]
            and stats["profiles"] == stats["profiles_written"]
            and stats["loyalty_elements"] == stats["loyalties_written"]
        )
        (output_dir / "json_summary.json").write_text(
            json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    if run_error:
        raise RuntimeError(
            f"Input JSON was not fully parsed. See json_summary.json. Error: {run_error}"
        ) from run_error
    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Parse forum JSON profiles, flights and loyalty entries"
    )
    parser.add_argument("source", nargs="?", type=Path, default=BASE_DIR / INPUT_FILE)
    parser.add_argument("--output", type=Path, default=BASE_DIR / "result")
    args = parser.parse_args()
    path = args.source
    try:
        result = process_file(path, output_dir=args.output)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
