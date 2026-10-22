"""Full Parquet read-back plus independent openpyxl checks of actual source sheets."""

import argparse
import hashlib
import io
import json
import time
import unicodedata
import zipfile
from collections import Counter
from datetime import date
from pathlib import Path

import openpyxl
import pyarrow as pa
import pyarrow.parquet as pq
from parse_excel_tickets import FIELDS, sha256_file


def verify(archive_path, parquet_path):
    start = time.perf_counter()
    manifest = json.loads(parquet_path.with_suffix(".manifest.json").read_text("utf8"))
    archive_sha = sha256_file(archive_path)
    assert archive_sha == manifest["input_sha256"], "Archive changed after parsing"
    assert sha256_file(parquet_path) == manifest["output_sha256"]
    table = pq.ParquetFile(parquet_path)
    assert table.schema_arrow.field("ticket_number").type == pa.string()
    assert table.schema_arrow.field("booking_reference").type == pa.string()
    assert table.schema_arrow.field("departure_date").type == pa.date32()
    books = manifest["workbooks"]
    selected_books = [books[i] for i in sorted({0, len(books) // 2, len(books) - 1})]
    sample_keys = {
        (b["source_file"], i)
        for b in selected_books
        for i in sorted({1, b["sheets"] // 2 + 1, b["sheets"]})
    }
    samples = {}
    additional = {}
    counts, leading_zeros, field_status = Counter(), Counter(), Counter()
    dates = []
    current_book, seen_sheets = None, set()
    rows = 0
    for batch in table.iter_batches(batch_size=2048):
        for row in batch.to_pylist():
            rows += 1
            member = row["source_file"]
            if member != current_book:
                assert counts[member] == 0, "Workbook rows unexpectedly noncontiguous"
                current_book, seen_sheets = member, set()
            assert row["source_sheet"] not in seen_sheets, "Duplicate worksheet row"
            seen_sheets.add(row["source_sheet"])
            counts[member] += 1
            assert row["source_sheet_index"] == counts[member]
            assert row["source_archive_sha256"] == archive_sha
            identity = json.dumps(
                [archive_sha, member, row["source_sheet"], row["source_sheet_index"]],
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            assert (
                row["record_id"] == hashlib.sha256(identity.encode("utf8")).hexdigest()
            )
            cells = json.loads(row["raw_cells_json"])
            types = json.loads(row["cell_types_json"])
            statuses = json.loads(row["field_status_json"])
            for name, (address, kind) in FIELDS.items():
                assert row[name + "_raw"] == cells.get(address)
                field_status[name + ":" + statuses[name]] += 1
                raw = row[name + "_raw"]
                if kind == "identifier" and raw and raw.startswith("0"):
                    leading_zeros[name] += 1
                    if statuses[name] == "trim_nfc_only":
                        assert row[name] == unicodedata.normalize("NFC", raw).strip()
                        additional.setdefault("leading_zero", row)
                if kind == "identifier" and raw and types.get(address) == "n":
                    assert row[name] is None, (
                        "Numeric ID silently treated as complete text ID"
                    )
            raw_date = row["departure_date_raw"]
            if statuses["departure_date"] == "valid_iso_date":
                parsed = date.fromisoformat(raw_date.strip())
                assert row["departure_date"] == parsed
                dates.append(parsed)
            if "missing_marker" in statuses.values():
                additional.setdefault("missing_marker", row)
            if any(ord(c) > 127 for c in (row["passenger_name_raw"] or "")):
                additional.setdefault("unicode_name", row)
            if (member, row["source_sheet_index"]) in sample_keys:
                samples[(member, row["source_sheet_index"])] = row
        if rows % 102400 == 0:
            print(f"Read-back validation: {rows:,} rows", flush=True)
    assert rows == manifest["row_count"] == manifest["sheet_count"]
    assert dict(field_status) == manifest["field_status_counts"]
    assert counts == Counter({b["source_file"]: b["sheets"] for b in books})
    for row in additional.values():
        samples[(row["source_file"], row["source_sheet_index"])] = row
    by_book = {}
    for (member, index), row in samples.items():
        by_book.setdefault(member, {})[index] = row
    independent_checks = []
    with zipfile.ZipFile(archive_path) as outer:
        xlsx_members = [
            e.filename
            for e in outer.infolist()
            if not e.is_dir()
            and e.filename.lower().endswith(".xlsx")
            and "__MACOSX" not in Path(e.filename).parts
            and not Path(e.filename).name.startswith("._")
        ]
        assert len(xlsx_members) == manifest["xlsx_members_total"]
        if not manifest["limited_run"]:
            assert set(xlsx_members) == set(counts)
        for member, selected in sorted(by_book.items()):
            binary = outer.read(member)
            book_info = next(b for b in books if b["source_file"] == member)
            assert hashlib.sha256(binary).hexdigest() == book_info["sha256"]
            workbook = openpyxl.load_workbook(
                io.BytesIO(binary), read_only=True, data_only=False
            )
            assert len(workbook.worksheets) == book_info["sheets"]
            for index, row in sorted(selected.items()):
                sheet = workbook.worksheets[index - 1]
                assert sheet.title == row["source_sheet"]
                actual = {
                    c.coordinate: str(c.value)
                    for cells in sheet.iter_rows()
                    for c in cells
                    if c.value is not None
                }
                stored = {
                    k: v
                    for k, v in json.loads(row["raw_cells_json"]).items()
                    if v is not None and v != ""
                }
                assert actual == stored, (member, sheet.title, "cell mismatch")
                # Independently compare the named columns to their original cells.
                for name, (address, _) in FIELDS.items():
                    value = sheet[address].value
                    expected = str(value) if value is not None else None
                    assert row[name + "_raw"] == expected
                independent_checks.append(
                    {
                        "source_file": member,
                        "source_sheet": sheet.title,
                        "source_sheet_index": index,
                        "cells_compared": len(actual),
                        "equal": True,
                        "values": {
                            name: row[name + "_raw"]
                            for name in [
                                "passenger_name",
                                "ticket_number",
                                "booking_reference",
                                "departure_date",
                                "flight_number",
                                "origin_airport",
                                "destination_airport",
                            ]
                        },
                    }
                )
            workbook.close()
    result = {
        "all_checks_passed": True,
        "full_dataset_complete": manifest["complete"],
        "rows_read_back": rows,
        "workbooks": len(counts),
        "date_min": min(dates).isoformat() if dates else None,
        "date_max": max(dates).isoformat() if dates else None,
        "leading_zero_raw_identifiers": dict(leading_zeros),
        "field_status_counts": dict(field_status),
        "independent_reader": "openpyxl (all populated cells of selected source worksheets)",
        "independent_checks": independent_checks,
        "scope": "All Parquet rows checked; source spot checks are not a second full extraction",
        "seconds": round(time.perf_counter() - start, 3),
    }
    target = parquet_path.with_suffix(".verification.json")
    target.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf8")
    print(
        json.dumps(
            {
                k: result[k]
                for k in [
                    "all_checks_passed",
                    "rows_read_back",
                    "workbooks",
                    "date_min",
                    "date_max",
                    "seconds",
                ]
            }
        ),
        flush=True,
    )
    print(
        f"Independent source sheet comparisons: {len(independent_checks)}", flush=True
    )
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("parquet", type=Path)
    args = parser.parse_args()
    verify(args.archive, args.parquet)
