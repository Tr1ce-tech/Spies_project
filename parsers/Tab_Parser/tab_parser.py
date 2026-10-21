"""Loss-aware streaming parser for Yoyoflot fixed-width (.tab) flight records."""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Iterator

import pandas as pd

INPUT_FILE = "../../raw_data/Sirena-export-fixed.tab"
OUTPUT_FORMAT = "parquet"  # "csv" или "parquet"
CHUNK_SIZE = 50_000
ENCODING = "utf-8"
VERSION = "sirena.fwf.v2"
OUTPUT_DIRECTORY = "result"


# Python string indices, end exclusive; established against the supplied sample.
FIELDS = (
    ("PaxName", 0, 60),
    ("PaxBirthDate", 60, 72),
    ("DepartDate", 72, 84),
    ("DepartTime", 84, 96),
    ("ArrivalDate", 96, 108),
    ("ArrivalTime", 108, 120),
    ("FlightCode", 120, 130),
    ("Sh", 130, 132),
    ("From", 132, 138),
    ("Dest", 138, 144),
    ("Code", 144, 150),
    ("e-Ticket", 150, 168),
    ("TravelDoc", 168, 180),
    ("Seat", 180, 186),
    ("Meal", 186, 192),
    ("TrvCls", 192, 198),
    ("Fare", 198, 204),
    ("Baggage", 204, 216),
    ("PaxAdditionalInfo", 216, 240),
    ("FrequentFlyer", 240, 276),  # Column exists but has no header.
    ("AgentInfo", 276, 336),
)
WIDTH = 336
DATA_COLUMNS = [name for name, _, _ in FIELDS]
META_COLUMNS = [
    "source_file",
    "source_line",
    "parse_status",
    "raw_line",
    "raw_bytes_b64",
    "record_id",
    "source_sha256",
    "rule_version",
]
OUTPUT_COLUMNS = DATA_COLUMNS + META_COLUMNS
ISSUE_COLUMNS = [
    "source_file",
    "source_line",
    "field",
    "error_type",
    "raw_value",
    "details",
]

REQUIRED = {
    "PaxName",
    "DepartDate",
    "DepartTime",
    "ArrivalDate",
    "ArrivalTime",
    "FlightCode",
    "From",
    "Dest",
}
DATE_COLUMNS = {"PaxBirthDate", "DepartDate", "ArrivalDate"}
TIME_COLUMNS = {"DepartTime", "ArrivalTime"}
HEADER_TOKENS = {
    0: "PaxName",
    60: "PaxBirthDate",
    72: "DepartDate",
    84: "DepartTime",
    96: "ArrivalDate",
    108: "ArrivalTime",
    120: "FlightCode",
    130: "Sh",
    132: "From",
    138: "Dest",
    144: "Code",
    150: "e-Ticket",
    168: "TravelDoc",
    180: "Seat",
    186: "Meal",
    192: "TrvCls",
    198: "Fare",
    204: "Baggage",
    216: "PaxAdditionalInfo",
    276: "AgentInfo",
}


def _is_missing(value: str) -> bool:
    return value == "" or value.upper() == "N/A"


def _is_date_or_time(value: str, fmt: str) -> bool:
    try:
        return datetime.strptime(value, fmt).strftime(fmt) == value
    except ValueError:
        return False


def parse_line(
    line: str, source_line: int, source_file: str = "input.tab"
) -> tuple[dict, list[dict]]:
    """Parse exactly one decoded physical line, retaining even damaged rows."""
    row = {name: "" for name in DATA_COLUMNS}
    row.update(
        source_file=source_file,
        source_line=source_line,
        parse_status="ok",
        raw_line=line,
        raw_bytes_b64="",
    )
    issues: list[dict] = []

    def issue(field: str, category: str, raw_value: str, details: str) -> None:
        issues.append(
            dict(
                source_file=source_file,
                source_line=source_line,
                field=field,
                error_type=category,
                raw_value=raw_value,
                details=details,
            )
        )

    if len(line) != WIDTH:
        # Don't assume the columns remain aligned once the width is wrong.
        row["parse_status"] = "invalid_width"
        row["raw_line"] = line
        issue(
            "__row__",
            "invalid_width",
            line,
            f"Expected {WIDTH} characters; got {len(line)}",
        )
        return row, issues

    for name, begin, end in FIELDS:
        row[name] = line[begin:end].strip()

    for name in REQUIRED:
        if _is_missing(row[name]):
            issue(name, "missing_required", row[name], "Required field is empty or N/A")

    for name in DATE_COLUMNS:
        value = row[name]
        if not _is_missing(value) and not _is_date_or_time(value, "%Y-%m-%d"):
            issue(name, "invalid_date", value, "Expected YYYY-MM-DD")

    for name in TIME_COLUMNS:
        value = row[name]
        if not _is_missing(value) and not _is_date_or_time(value, "%H:%M"):
            issue(name, "invalid_time", value, "Expected HH:MM")

    for name in ("From", "Dest"):
        value = row[name]
        if not _is_missing(value) and not re.fullmatch(r"[A-Z]{3}", value):
            issue(name, "invalid_airport_code", value, "Expected 3 uppercase letters")

    if row["Baggage"] and not re.fullmatch(r"\d+PC", row["Baggage"]):
        issue("Baggage", "unusual_baggage", row["Baggage"], "Expected e.g. 0PC or 2PC")

    if issues:
        row["parse_status"] = "review"
    return row, issues


def _without_line_ending(raw: bytes) -> bytes:
    # Remove one line terminator, but never trim meaningful padding spaces.
    if raw.endswith(b"\n"):
        raw = raw[:-1]
    if raw.endswith(b"\r"):
        raw = raw[:-1]
    return raw


def _decoded_record(raw: bytes, encoding: str) -> tuple[str, bool]:
    try:
        return raw.decode(encoding), True
    except UnicodeDecodeError:
        # Store reversible original bytes (base64) as well as readable fallback.
        return raw.decode(encoding, errors="replace"), False


def _validate_header(header: bytes, encoding: str) -> None:
    text, valid = _decoded_record(header, encoding)
    text = text.removeprefix("\ufeff")
    if not valid or len(text) != WIDTH:
        raise ValueError(
            f"Unrecognized header: expected {WIDTH} decoded characters, got {len(text)}"
        )
    for pos, token in HEADER_TOKENS.items():
        if not text.startswith(token, pos):
            raise ValueError(
                f"Header mismatch at character {pos}: expected {token!r}; check format/encoding"
            )


def records(path: Path, encoding: str = "utf-8") -> Iterator[tuple[dict, list[dict]]]:
    """Read fixed-width records without loading the complete source."""
    path = Path(path)
    with path.open("rb") as handle:
        source_sha = hashlib.file_digest(handle, "sha256").hexdigest()
    with path.open("rb") as stream:
        header = stream.readline()
        if not header:
            raise ValueError("File is empty")
        _validate_header(_without_line_ending(header), encoding)

        for line_number, physical_line in enumerate(stream, start=2):
            raw = _without_line_ending(physical_line)
            decoded, encoding_ok = _decoded_record(raw, encoding)
            row, issues = parse_line(decoded, line_number, path.name)
            row.update(
                source_sha256=source_sha,
                rule_version=VERSION,
                record_id=hashlib.sha256(
                    f"{source_sha}:{line_number}".encode()
                ).hexdigest(),
            )
            row["raw_bytes_b64"] = base64.b64encode(physical_line).decode("ascii")
            if not encoding_ok:
                row["raw_line"] = decoded
                row["parse_status"] = "invalid_encoding"
                issues.append(
                    dict(
                        source_file=path.name,
                        source_line=line_number,
                        field="__row__",
                        error_type="invalid_encoding",
                        raw_value="",
                        details="Raw bytes preserved in raw_bytes_b64",
                    )
                )
            yield row, issues


def process_file(
    path: Path,
    output_dir: Path,
    output_format: str = "csv",
    chunk_size: int = 50_000,
    encoding: str = "utf-8",
) -> dict:
    """Write parsed rows in chunks and save diagnostic summaries."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    if output_format not in {"csv", "parquet"}:
        raise ValueError("output_format must be 'csv' or 'parquet'")
    if output_format == "parquet":
        try:
            import pyarrow  # noqa: F401
        except ImportError as exc:
            raise RuntimeError("Parquet output requires: pip install pyarrow") from exc

    path, output_dir = Path(path), Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    if any(output_dir.glob("flights_part_*.parquet")) or any(
        (output_dir / name).exists()
        for name in ["flights.csv", "summary.json", "issues.csv"]
    ):
        raise FileExistsError("Choose a fresh output directory")
    with path.open("rb") as handle:
        source_sha = hashlib.file_digest(handle, "sha256").hexdigest()
    rows: list[dict] = []
    counters = Counter()
    nulls = Counter()
    written = 0
    chunk_index = 0
    csv_path = output_dir / "flights.csv"

    def flush() -> None:
        nonlocal written, chunk_index
        if not rows:
            return
        df = pd.DataFrame.from_records(rows, columns=OUTPUT_COLUMNS)
        if output_format == "csv":
            df.to_csv(
                csv_path,
                mode="w" if chunk_index == 0 else "a",
                index=False,
                header=(chunk_index == 0),
                encoding="utf-8",
            )
        else:
            df.to_parquet(
                output_dir / f"flights_part_{chunk_index:05d}.parquet", index=False
            )
        written += len(df)
        chunk_index += 1
        rows.clear()

    with (output_dir / "issues.csv").open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=ISSUE_COLUMNS)
        writer.writeheader()
        for row, issues in records(path, encoding):
            rows.append(row)
            counters[f"status:{row['parse_status']}"] += 1
            # Unparseable width means fields are unknown, not truly missing.
            if not any(x["error_type"] == "invalid_width" for x in issues):
                for col in DATA_COLUMNS:
                    if _is_missing(row[col]):
                        nulls[col] += 1
            for item in issues:
                writer.writerow(item)
                counters[f"issue:{item['error_type']}"] += 1
            if len(rows) >= chunk_size:
                flush()
        flush()

    # If a previous run produced more Parquet parts, remove only obsolete
    # parser-generated parts after the current run has finished successfully.
    if output_format == "parquet":
        for old_part in output_dir.glob("flights_part_*.parquet"):
            suffix = old_part.stem.removeprefix("flights_part_")
            if suffix.isdigit() and int(suffix) >= chunk_index:
                old_part.unlink()

    with path.open("rb") as handle:
        if hashlib.file_digest(handle, "sha256").hexdigest() != source_sha:
            raise ValueError("Source changed during parsing")
    result = {
        "rule_version": VERSION,
        "source_sha256": source_sha,
        "complete": True,
        "input_file": str(path),
        "encoding": encoding,
        "expected_width_characters": WIDTH,
        "fields": len(DATA_COLUMNS),
        "rows_read": written,
        "rows_written": written,
        "chunks_written": chunk_index,
        "statuses": {k[7:]: v for k, v in counters.items() if k.startswith("status:")},
        "issues_by_type": {
            k[6:]: v for k, v in counters.items() if k.startswith("issue:")
        },
        "missing_or_NA_by_column": dict(nulls),
        "output_format": output_format,
    }
    (output_dir / "summary.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return result


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description="Parse fixed-width Sirena records")
    parser.add_argument("source", nargs="?", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    input_path = (args.source or Path(INPUT_FILE)).expanduser()
    if args.source is None and not input_path.is_absolute():
        input_path = script_dir / input_path
    if not input_path.is_file():
        raise FileNotFoundError(
            f"Не найден исходный файл: {input_path}. "
            "Положите .tab рядом со скриптом или измените INPUT_FILE."
        )

    result = process_file(
        path=input_path,
        output_dir=args.output or script_dir / OUTPUT_DIRECTORY,
        output_format=OUTPUT_FORMAT,
        chunk_size=CHUNK_SIZE,
        encoding=ENCODING,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
