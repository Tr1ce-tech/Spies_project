"""Step 1: parse only the first four pages of the supplied SkyTeam edition."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import date
from pathlib import Path

import pandas as pd
import pymupdf

PARSER_VERSION = "skyteam.intro.v1"
BASE_DIR = Path(__file__).resolve().parent
FIELDS = (
    "From",
    "To",
    "Validity",
    "Days",
    "DepTime",
    "ArrTime",
    "Flight",
    "Aircraft",
    "Travel Time",
)
MONTHS = {
    name: n
    for n, name in enumerate(
        (
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dec",
        ),
        1,
    )
}


def parse_intro(pdf_path: str | Path) -> dict[str, pd.DataFrame]:
    """Return provenance-linked DataFrames. Pages are one-based; boxes are PDF points."""
    path = Path(pdf_path).resolve()
    with path.open("rb") as stream:
        sha = hashlib.file_digest(stream, "sha256").hexdigest()
    lines, images, pages = [], [], []
    with pymupdf.open(path) as doc:
        if len(doc) < 4:
            raise ValueError("Expected at least four pages")
        for index in range(4):
            page = doc[index]
            page_lines = []
            # Native text lines retain the source column boundaries. Global sort=True
            # can interleave left and right columns; do not parse that flattened text.
            for block_index, block in enumerate(page.get_text("dict")["blocks"]):
                if "lines" not in block:
                    continue
                for line_index, line in enumerate(block["lines"]):
                    raw = "".join(span["text"] for span in line["spans"])
                    if not raw.strip():
                        continue
                    x0, y0, x1, y1 = map(float, line["bbox"])
                    record_id = hashlib.sha256(
                        f"{sha}:{index + 1}:{block_index}:{line_index}".encode()
                    ).hexdigest()
                    row = dict(
                        raw_record_id=record_id,
                        source_file=path.name,
                        source_sha256=sha,
                        page=index + 1,
                        block=block_index,
                        line=line_index,
                        x0=x0,
                        y0=y0,
                        x1=x1,
                        y1=y1,
                        raw_text=raw,
                        text=raw.strip(),
                        parser_version=PARSER_VERSION,
                        normalization_rule="trim_outer_whitespace.v1",
                        parse_status="extracted_native_text",
                        verification_status="not_independently_verified",
                    )
                    lines.append(row)
                    page_lines.append(row)
            for image_index, info in enumerate(page.get_image_info()):
                images.append(
                    dict(
                        page=index + 1,
                        image_index=image_index,
                        bbox_json=json.dumps(list(info["bbox"])),
                        width=info["width"],
                        height=info["height"],
                        source_file=path.name,
                        source_sha256=sha,
                        parser_version=PARSER_VERSION,
                        parse_status="image_not_transcribed",
                    )
                )
            pages.append(
                dict(
                    page=index + 1,
                    width=page.rect.width,
                    height=page.rect.height,
                    extracted_lines=len(page_lines),
                    source_file=path.name,
                    source_sha256=sha,
                    parser_version=PARSER_VERSION,
                )
            )
        total_pages = len(doc)

    def on(page: int):
        return sorted(
            (r for r in lines if r["page"] == page), key=lambda r: (r["y0"], r["x0"])
        )

    def derived(rows, rule, **values):
        return dict(
            **values,
            source_file=path.name,
            source_sha256=sha,
            page=rows[0]["page"],
            source_record_ids=json.dumps([r["raw_record_id"] for r in rows]),
            parser_version=PARSER_VERSION,
            processing_rule=rule,
            parse_status="parsed",
            verification_status="not_independently_verified",
        )

    p1, p2, p3, p4 = (on(n) for n in range(1, 5))
    if not any(r["text"] == "SkyTeam Timetable" for r in p1):
        raise ValueError("Unrecognized cover")
    metadata = []
    for row in p1:
        match = re.fullmatch(
            r"Covers period:\s*(\d{2}) (\w{3}) (\d{4}) through (\d{2}) (\w{3}) (\d{4})",
            row["text"],
        )
        if match:
            d1, m1, y1, d2, m2, y2 = match.groups()
            for key, value in [
                ("coverage_start", date(int(y1), MONTHS[m1], int(d1)).isoformat()),
                ("coverage_end", date(int(y2), MONTHS[m2], int(d2)).isoformat()),
            ]:
                metadata.append(
                    derived(
                        [row],
                        "cover_english_month_date.v1",
                        key=key,
                        raw_value=row["raw_text"],
                        value=value,
                    )
                )
        else:
            metadata.append(
                derived(
                    [row],
                    "cover_literal.v1",
                    key="cover_text",
                    raw_value=row["raw_text"],
                    value=row["text"],
                )
            )
    if {r["key"] for r in metadata} != {"coverage_start", "coverage_end", "cover_text"}:
        raise ValueError("Cover period not recognized")

    contacts, disclaimer = [], []
    # The actual edition has column origins at x=38 and x=309.5 on an A4 page.
    midpoint = pages[1]["width"] / 2
    for row in p2:
        if row["x0"] < midpoint:
            match = re.fullmatch(r"(.+?)\s+(www\.\S+)", row["text"])
            if match:
                contacts.append(
                    derived(
                        [row],
                        "contact_name_website.v1",
                        airline_name_raw=match[1],
                        website_raw=match[2],
                        raw_text=row["raw_text"],
                    )
                )
            elif row["text"] not in {
                "Contact",
                "To book, contact any SkyTeam member airline.",
            }:
                raise ValueError(f"Unrecognized contact line: {row['text']}")
        elif row["text"] != "Disclaimer":
            disclaimer.append(row)
    if not contacts or not disclaimer:
        raise ValueError("Missing contacts or disclaimer")

    definitions, weekdays = [], []
    seen = set()
    for i, row in enumerate(p3):
        if row["text"] not in {f + ":" for f in FIELDS}:
            continue
        field = row["text"][:-1]
        following = []
        for candidate in p3[i + 1 :]:
            if candidate["text"] in {f + ":" for f in FIELDS}:
                break
            following.append(candidate)
        if not following:
            raise ValueError(f"Empty definition: {field}")
        description = "\n".join(r["raw_text"] for r in following)
        definitions.append(
            derived(
                [row, *following],
                "label_following_lines.v1",
                field=field,
                definition_raw=description,
            )
        )
        seen.add(field)
        if field == "Days":
            for code, name in re.findall(r"([1-7])\s*-\s*([A-Za-z]+)", description):
                weekdays.append(
                    derived(
                        following,
                        "explicit_weekday_mapping.v1",
                        day_code=code,
                        weekday_name=name,
                    )
                )
    if seen != set(FIELDS) or len(weekdays) != 7:
        raise ValueError("Field dictionary or weekday mapping incomplete")

    aircraft = []
    for row in p4:
        if row["text"] == "Aircraft Types:":
            continue
        match = re.fullmatch(r"([A-Z0-9]{3})\s*:\s*(.+)", row["text"])
        if not match:
            raise ValueError(f"Unrecognized equipment line: {row['text']}")
        aircraft.append(
            derived(
                [row],
                "equipment_code_description.v1",
                aircraft_code=match[1],
                description_raw=match[2],
                raw_text=row["raw_text"],
                column="left" if row["x0"] < pages[3]["width"] / 2 else "right",
            )
        )
    if len({r["aircraft_code"] for r in aircraft}) != len(aircraft):
        raise ValueError("Duplicate equipment codes")

    first_field = min(r["y0"] for r in p3 if r["text"] == "From:")
    guide = [r for r in p3 if r["y0"] < first_field]
    sections = [
        derived(
            disclaimer,
            "column_ordered_text.v1",
            section="disclaimer",
            text_raw="\n".join(r["raw_text"] for r in disclaimer),
        ),
        derived(
            guide,
            "native_text_before_field_dictionary.v1",
            section="usage_and_coverage",
            text_raw="\n".join(r["raw_text"] for r in guide),
        ),
    ]
    frames = {
        name: pd.DataFrame(records)
        for name, records in dict(
            raw_lines=lines,
            pages=pages,
            images=images,
            metadata=metadata,
            contacts=contacts,
            sections=sections,
            field_definitions=definitions,
            weekdays=weekdays,
            aircraft_types=aircraft,
        ).items()
    }
    for frame in frames.values():
        for col in frame:
            if all(isinstance(value, str) for value in frame[col]):
                frame[col] = frame[col].astype("string")
    frames["pages"]["document_total_pages"] = total_pages
    return frames


def save_frames(frames: dict[str, pd.DataFrame], out: str | Path):
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    for name, frame in frames.items():
        frame.to_parquet(out / f"{name}.parquet", index=False)
        frame.to_csv(out / f"{name}.csv", index=False, encoding="utf-8")
        restored = pd.read_parquet(out / f"{name}.parquet")
        pd.testing.assert_frame_equal(frame, restored)
    manifest = dict(
        parser_version=PARSER_VERSION,
        tables={k: len(v) for k, v in frames.items()},
        source_sha256=frames["pages"]["source_sha256"].iloc[0],
        scope="Pages 1-4 native text only; illustrations/logos inventoried, not transcribed.",
        verification="Roundtrip checked; independent historical/source correctness not established.",
    )
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pdf", type=Path)
    parser.add_argument("--out", type=Path, default=BASE_DIR / "data/pdf_intro")
    args = parser.parse_args()
    tables = parse_intro(args.pdf)
    save_frames(tables, args.out)
    print(json.dumps({k: len(v) for k, v in tables.items()}, indent=2))
