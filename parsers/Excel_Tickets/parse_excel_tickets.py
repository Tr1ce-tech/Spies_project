"""Parse the supplied boarding-pass XLSX archive: one worksheet = one Parquet row."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import posixpath
import re
import time
import unicodedata
import xml.etree.ElementTree as ET
import zipfile
from collections import Counter
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

VERSION = "excel-tickets-1.0.1"
NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
REL_ID = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
MISSING = {"N/A", "NA", "NULL", "NONE", "NAN", "-"}

# Every named value also has an untouched *_raw column; the complete cell map is retained.
FIELDS = {
    "passenger_title": ("A3", "text"),
    "passenger_name": ("B3", "text"),
    "loyalty_card": ("F3", "identifier"),
    "travel_class": ("H3", "text"),
    "flight_number": ("A5", "flight"),
    "origin_city": ("D5", "text"),
    "destination_city": ("H5", "text"),
    "gate": ("B7", "identifier"),
    "origin_airport": ("D7", "airport"),
    "destination_airport": ("H7", "airport"),
    "departure_date": ("A9", "date"),
    "departure_time": ("C9", "time"),
    "operator_text": ("E9", "text"),
    "boarding_note": ("A11", "text"),
    "seat": ("H11", "identifier"),
    "booking_reference": ("B13", "identifier"),
    "ticket_number": ("E13", "identifier"),
    "sequence_number": ("H1", "sequence"),
}
TEMPLATE = {"A1": "BOARDING PASS", "A13": "PNR:", "D13": "E-TICKET"}


def json_text(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def rich_text(element):
    """Read text/rich runs, excluding phonetic annotations and formatting metadata."""
    if element is None:
        return ""
    return "".join(t.text or "" for t in element.findall("m:t", NS)) + "".join(
        t.text or "" for t in element.findall("m:r/m:t", NS)
    )


def read_cells(xml_bytes, shared_strings):
    cells, types, formulas = {}, {}, {}
    root = ET.fromstring(xml_bytes)
    for cell in root.findall("m:sheetData/m:row/m:c", NS):
        address = cell.attrib["r"]
        if address in cells:
            raise ValueError(f"Repeated cell coordinate: {address}")
        kind = cell.get("t", "n")
        value_node = cell.find("m:v", NS)
        value = value_node.text if value_node is not None else None
        if kind == "s":
            value = shared_strings[int(value)]
        elif kind == "inlineStr":
            value = rich_text(cell.find("m:is", NS))
        formula = cell.find("m:f", NS)
        if formula is not None:
            formulas[address] = formula.text or ""
        # Preserve explicit empty cells as well as absent cells (which have no key).
        cells[address] = value
        types[address] = kind
    return cells, types, formulas


def normalise(raw, kind, cell_type="s", is_formula=False):
    if is_formula:
        return None, "formula_not_evaluated"
    if raw is None:
        return None, "empty"
    value = unicodedata.normalize("NFC", raw).strip()
    if value == "":
        return None, "empty_string"
    if value.upper() in MISSING:
        return None, "missing_marker"
    if kind in {"identifier", "date", "time", "flight", "airport"}:
        if cell_type not in {"s", "inlineStr", "str"}:
            return None, "nontext_value_not_inferred"
    if kind == "date":
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            return None, "unsupported_or_ambiguous_date_format"
        try:
            return date.fromisoformat(value), "valid_iso_date"
        except ValueError:
            return None, "invalid_calendar_date"
    if kind == "time":
        if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", value):
            return None, "invalid_local_time"
        return value, "valid_local_hhmm_timezone_unknown"
    if kind == "airport":
        value = value.upper()
        if not re.fullmatch(r"[A-Z]{3}", value):
            return None, "invalid_airport_code_format"
        return value, "syntax_only_not_historically_verified"
    if kind == "flight":
        value = re.sub(r"\s+", "", value.upper())
        if not re.fullmatch(r"[A-Z0-9]{2,3}\d{1,5}[A-Z]?", value):
            return None, "invalid_flight_format"
        return value, "syntax_only_operator_unknown"
    return value, "trim_nfc_only"


def schema():
    columns = [
        (name, pa.string())
        for name in [
            "record_id",
            "source_archive",
            "source_archive_sha256",
            "source_file",
            "source_file_sha256",
            "source_sheet",
            "source_sheet_xml",
            "rule_version",
            "parse_status",
            "errors_json",
            "raw_cells_json",
            "cell_types_json",
            "formulas_json",
            "field_status_json",
        ]
    ]
    columns.append(("source_sheet_index", pa.int32()))
    for name, (_, kind) in FIELDS.items():
        columns.extend(
            [
                (name + "_raw", pa.string()),
                (name, pa.date32() if kind == "date" else pa.string()),
            ]
        )
    metadata = {
        b"rule_version": VERSION.encode(),
        b"unit": b"one source worksheet, not proof of a completed flight",
        b"field_mapping": json_text(FIELDS).encode("utf8"),
    }
    return pa.schema(columns, metadata=metadata)


def make_row(
    archive,
    archive_sha,
    member,
    member_sha,
    sheet_name,
    sheet_index,
    sheet_xml,
    cells,
    types,
    formulas,
    read_error=None,
):
    identity = json_text([archive_sha, member, sheet_name, sheet_index])
    errors = [read_error] if read_error else []
    if not read_error:
        for address, label in TEMPLATE.items():
            if cells.get(address) != label or address in formulas:
                errors.append(f"template_mismatch:{address}")
    template_valid = not errors
    if formulas:
        errors.append("formula_cells_present")
    row = {
        "record_id": hashlib.sha256(identity.encode("utf8")).hexdigest(),
        "source_archive": archive,
        "source_archive_sha256": archive_sha,
        "source_file": member,
        "source_file_sha256": member_sha,
        "source_sheet": sheet_name,
        "source_sheet_index": sheet_index,
        "source_sheet_xml": sheet_xml,
        "rule_version": VERSION,
        "parse_status": "ok" if not errors else "needs_review",
        "errors_json": json_text(errors),
        "raw_cells_json": json_text(cells),
        "cell_types_json": json_text(types),
        "formulas_json": json_text(formulas),
    }
    statuses = {}
    for name, (address, kind) in FIELDS.items():
        raw = cells.get(address)
        value, status = normalise(
            raw, kind, types.get(address, "n"), address in formulas
        )
        if not template_valid:
            value, status = None, "unrecognized_template_or_unreadable_sheet"
        row[name + "_raw"] = raw
        row[name] = value
        statuses[name] = status
    row["field_status_json"] = json_text(statuses)
    return row


def worksheet_list(book):
    root = ET.fromstring(book.read("xl/workbook.xml"))
    rels = ET.fromstring(book.read("xl/_rels/workbook.xml.rels"))
    targets = {r.get("Id"): (r.get("Target"), r.get("TargetMode")) for r in rels}
    for index, sheet in enumerate(root.findall("m:sheets/m:sheet", NS), 1):
        target, mode = targets[sheet.attrib[REL_ID]]
        if mode == "External":
            raise ValueError("External worksheet relationship")
        target = posixpath.normpath(
            target.lstrip("/")
            if target.startswith("/")
            else posixpath.join("xl", target)
        )
        if not target.startswith("xl/"):
            raise ValueError("Worksheet relationship outside xl/")
        yield index, sheet.attrib["name"], target


def is_workbook_member(entry):
    parts = Path(entry.filename).parts
    return (
        not entry.is_dir()
        and entry.filename.lower().endswith(".xlsx")
        and "__MACOSX" not in parts
        and not Path(entry.filename).name.startswith("._")
    )


def parse_archive(input_path, output_path, max_books=None, batch_size=4096):
    input_path, output_path = Path(input_path), Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path = output_path.with_suffix(".manifest.json")
    # Refuse stale results and make a partial run visibly different from a finished table.
    temporary = output_path.with_suffix(".parquet.partial")
    for candidate in [output_path, manifest_path, temporary]:
        if candidate.exists():
            raise FileExistsError(f"Choose a fresh output path: {candidate}")
    start = time.perf_counter()
    archive_sha = sha256_file(input_path)
    arrow_schema = schema()
    pending, books, errors = [], [], []
    statuses, field_statuses = Counter(), Counter()
    count = 0
    with zipfile.ZipFile(input_path) as outer:
        entries = outer.infolist()
        if any(
            e.header_offset < 0 or e.header_offset >= outer.start_dir for e in entries
        ):
            raise zipfile.BadZipFile(
                "Invalid central-directory offsets; restore the source archive"
            )
        names = [e.filename for e in entries]
        if len(names) != len(set(names)):
            raise ValueError("Duplicate archive member names")
        members = sorted(e.filename for e in entries if is_workbook_member(e))
        selected = members if max_books is None else members[:max_books]
        if not selected:
            raise ValueError("No XLSX workbooks found")
        with pq.ParquetWriter(temporary, arrow_schema, compression="zstd") as writer:
            for book_index, member in enumerate(selected, 1):
                info = {"source_file": member, "sheets": 0, "rows": 0}
                try:
                    binary = outer.read(member)
                    member_sha = hashlib.sha256(binary).hexdigest()
                    info.update(sha256=member_sha, bytes=len(binary))
                    with zipfile.ZipFile(io.BytesIO(binary)) as book:
                        if len(book.namelist()) != len(set(book.namelist())):
                            raise ValueError("Duplicate workbook ZIP entries")
                        strings = []
                        if "xl/sharedStrings.xml" in book.namelist():
                            strings = [
                                rich_text(e)
                                for e in ET.fromstring(
                                    book.read("xl/sharedStrings.xml")
                                )
                            ]
                        sheets = list(worksheet_list(book))
                        info["sheets"] = len(sheets)
                        for index, sheet_name, target in sheets:
                            read_error = None
                            try:
                                cells, types, formulas = read_cells(
                                    book.read(target), strings
                                )
                            except (
                                KeyError,
                                ValueError,
                                IndexError,
                                ET.ParseError,
                                zipfile.BadZipFile,
                            ) as error:
                                cells, types, formulas = {}, {}, {}
                                read_error = f"{type(error).__name__}:{error}"
                                errors.append(
                                    {
                                        "source_file": member,
                                        "source_sheet": sheet_name,
                                        "error": read_error,
                                    }
                                )
                            row = make_row(
                                input_path.name,
                                archive_sha,
                                member,
                                member_sha,
                                sheet_name,
                                index,
                                target,
                                cells,
                                types,
                                formulas,
                                read_error,
                            )
                            statuses[row["parse_status"]] += 1
                            for field, status in json.loads(
                                row["field_status_json"]
                            ).items():
                                field_statuses[field + ":" + status] += 1
                            pending.append(row)
                            count += 1
                            info["rows"] += 1
                            if len(pending) >= batch_size:
                                writer.write_table(
                                    pa.Table.from_pylist(pending, schema=arrow_schema)
                                )
                                pending.clear()
                except (
                    KeyError,
                    ValueError,
                    IndexError,
                    ET.ParseError,
                    zipfile.BadZipFile,
                    OSError,
                ) as error:
                    info["error"] = f"{type(error).__name__}:{error}"
                    errors.append({"source_file": member, "error": info["error"]})
                books.append(info)
                if (
                    book_index == 1
                    or book_index % 10 == 0
                    or book_index == len(selected)
                ):
                    print(
                        f"Workbooks {book_index}/{len(selected)}; rows {count:,}",
                        flush=True,
                    )
            if pending:
                writer.write_table(pa.Table.from_pylist(pending, schema=arrow_schema))
    if sha256_file(input_path) != archive_sha:
        raise ValueError("Source archive changed during parsing")
    assert pq.ParquetFile(temporary).metadata.num_rows == count
    temporary.rename(output_path)
    complete = max_books is None and not errors and statuses["needs_review"] == 0
    result = {
        "rule_version": VERSION,
        "input_filename": input_path.name,
        "input_bytes": input_path.stat().st_size,
        "input_sha256": archive_sha,
        "xlsx_members_total": len(members),
        "workbooks_selected": len(selected),
        "workbooks_read": sum("error" not in b for b in books),
        "sheet_count": sum(b["sheets"] for b in books),
        "row_count": count,
        "parse_status_counts": dict(statuses),
        "field_status_counts": dict(field_statuses),
        "complete": complete,
        "limited_run": max_books is not None,
        "errors": errors,
        "workbooks": books,
        "other_archive_members": [
            e.filename for e in entries if not e.is_dir() and not is_workbook_member(e)
        ],
        "seconds": round(time.perf_counter() - start, 3),
        "output_filename": output_path.name,
        "output_bytes": output_path.stat().st_size,
        "output_sha256": sha256_file(output_path),
    }
    manifest_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf8"
    )
    output_path.with_suffix(".fields.json").write_text(
        json.dumps(
            {"rule_version": VERSION, "fields": FIELDS, "schema": str(arrow_schema)},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf8",
    )
    print(
        json_text(
            {
                k: result[k]
                for k in [
                    "complete",
                    "row_count",
                    "sheet_count",
                    "workbooks_read",
                    "seconds",
                ]
            }
        ),
        flush=True,
    )
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--output", type=Path, default=Path("tickets.parquet"))
    parser.add_argument(
        "--max-books", type=int, help="Pilot only; never marked complete"
    )
    args = parser.parse_args()
    if args.max_books is not None and args.max_books < 1:
        parser.error("--max-books must be positive")
    summary = parse_archive(args.archive, args.output, args.max_books)
    if summary["errors"] or summary["parse_status_counts"].get("needs_review"):
        raise SystemExit(2)
