"""Technical integrity checks; not independent validation of historical schedules."""

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pymupdf


def validate(out, pdf):
    out = Path(out)
    manifest = json.loads((out / "manifest.json").read_text())
    assert manifest["status"] == "complete", "Incomplete run"
    checks = {}
    with Path(pdf).open("rb") as stream:
        checks["source_sha256_matches"] = (
            hashlib.file_digest(stream, "sha256").hexdigest()
            == manifest["source_sha256"]
        )
    checks["parquet_row_counts"] = all(
        pq.ParquetFile(out / f"{name}.parquet").metadata.num_rows == count
        for name, count in manifest["tables"].items()
    )
    pages = pd.read_parquet(out / "pages.parquet")
    checks["page_column_coverage"] = (
        len(pages) == (manifest["end_page"] - manifest["start_page"] + 1) * 2
        and not pages.duplicated(["page", "column"]).any()
    )
    checks["page_sums"] = (
        int(pages.parsed_rows.sum()) == manifest["tables"]["schedule"]
        and int(pages.nonempty_lines.sum()) == manifest["tables"]["raw_lines"]
        and int(pages.quarantined_lines.sum()) == manifest["tables"]["quarantine"]
    )
    raw_kinds = (
        pd.read_parquet(out / "raw_lines.parquet", columns=["line_kind"])["line_kind"]
        .value_counts()
        .to_dict()
    )
    checks["all_native_lines_accounted"] = (
        sum(raw_kinds.values()) == manifest["tables"]["raw_lines"]
    )
    checks["row_like_conservation"] = manifest["counters"]["row_like_lines"] == (
        manifest["tables"]["schedule"]
        + manifest["field_issues"].get("row_pattern_not_matched", 0)
    )
    header_df = pd.read_parquet(
        out / "raw_lines.parquet",
        columns=["raw_id", "page", "column", "line", "line_kind"],
        filters=[("line_kind", "in", ["origin_header", "destination_header"])],
    )
    headers = header_df.set_index("raw_id").to_dict("index")
    schedule_ids = set()
    headers_ok = True
    dates_ok = True
    col_types_ok = True
    schema = pq.ParquetFile(out / "schedule.parquet").schema_arrow
    for name in [
        "origin_code",
        "destination_code",
        "flight_number",
        "aircraft_code",
        "days_codes",
    ]:
        col_types_ok &= str(schema.field(name).type) == "string"
    columns = [
        "raw_id",
        "page",
        "column",
        "line",
        "origin_header_id",
        "destination_header_id",
        "valid_from",
        "valid_to",
    ]
    for batch in pq.ParquetFile(out / "schedule.parquet").iter_batches(
        batch_size=20000, columns=columns
    ):
        for row in batch.to_pylist():
            if row["raw_id"] in schedule_ids:
                raise AssertionError("Repeated source locator")
            schedule_ids.add(row["raw_id"])
            for field, kind in [
                ("origin_header_id", "origin_header"),
                ("destination_header_id", "destination_header"),
            ]:
                if row[field] is None:
                    continue
                h = headers.get(row[field])
                headers_ok &= (
                    h is not None
                    and h["column"] == row["column"]
                    and h["line_kind"] == kind
                    and (h["page"], h["line"]) < (row["page"], row["line"])
                )
            if row["valid_from"] is not None and row["valid_to"] is not None:
                dates_ok &= (
                    manifest["coverage_start"]
                    <= row["valid_from"]
                    <= row["valid_to"]
                    <= manifest["coverage_end"]
                )
    checks.update(
        unique_schedule_locators=len(schedule_ids) == manifest["tables"]["schedule"],
        header_lineage_same_column=headers_ok,
        normalized_periods_within_cover=dates_ok,
        identifier_string_types=col_types_ok,
    )
    notes_ok = True
    orphan_notes = 0
    for batch in pq.ParquetFile(out / "operator_notes.parquet").iter_batches(
        columns=["schedule_id"]
    ):
        for row in batch.to_pylist():
            if row["schedule_id"] is None:
                orphan_notes += 1
            else:
                notes_ok &= row["schedule_id"] in schedule_ids
    checks["assigned_operator_note_lineage"] = notes_ok
    # Separate extraction of complete selected pages, including lines that are not rows.
    sample_pages = [
        n
        for n in [5, 6, 100, 200, 1000, 13094, 15000, 27514]
        if manifest["start_page"] <= n <= manifest["end_page"]
    ]
    sample_raw = pd.read_parquet(
        out / "raw_lines.parquet", filters=[("page", "in", sample_pages)]
    )
    samples_ok = True
    with pymupdf.open(pdf) as doc:
        for number in sample_pages:
            page = doc[number - 1]
            for col in (0, 1):
                clip = pymupdf.Rect(
                    col * page.rect.width / 2,
                    0,
                    (col + 1) * page.rect.width / 2,
                    page.rect.height,
                )
                extracted = [
                    (i, text)
                    for i, text in enumerate(
                        page.get_text("text", clip=clip, sort=True).splitlines(), 1
                    )
                    if text.strip()
                ]
                actual = sample_raw[
                    (sample_raw.page == number) & (sample_raw.column == col)
                ]
                samples_ok &= extracted == list(
                    zip(actual.line.tolist(), actual.raw_text.tolist())
                )
    checks["selected_pages_exact_text_reextraction"] = samples_ok
    result = dict(
        checks=checks,
        raw_line_kinds=raw_kinds,
        unassigned_operator_notes=orphan_notes,
        text_rechecked_pages=sample_pages,
        limitation="Technical integrity only; repeated source rows retained; inherited routes and source claims require separate review.",
    )
    (out / "validation.json").write_text(json.dumps(result, indent=2))
    if not all(checks.values()):
        raise AssertionError(result)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pdf", type=Path)
    parser.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).resolve().parent / "data/pdf_schedule",
    )
    args = parser.parse_args()
    print(json.dumps(validate(args.out, args.pdf), indent=2))
