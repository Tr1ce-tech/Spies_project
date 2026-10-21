"""Re-read every schedule page and compare source text and structured fields."""

import argparse
import hashlib
import json
import re
from pathlib import Path

import pyarrow.parquet as pq
import pymupdf

BASE_DIR = Path(__file__).resolve().parent


def rows(path, columns=None):
    for batch in pq.ParquetFile(path).iter_batches(batch_size=2048, columns=columns):
        yield from batch.to_pylist()


def verify(source, output):
    source, output = Path(source), Path(output)
    manifest = json.loads((output / "manifest.json").read_text())
    assert manifest["status"] == "complete"
    with source.open("rb") as handle:
        sha = hashlib.file_digest(handle, "sha256").hexdigest()
    assert sha == manifest["source_sha256"]
    raw = iter(
        rows(
            output / "raw_lines.parquet",
            ["raw_id", "page", "column", "line", "raw_text", "line_kind"],
        )
    )
    schedules = iter(rows(output / "schedule.parquet"))
    notes = iter(
        rows(
            output / "operator_notes.parquet", ["raw_id", "schedule_id", "operator_raw"]
        )
    )
    contexts = [{}, {}]
    previous = [None, None]
    counts = {"raw_lines": 0, "schedule": 0, "operator_notes": 0}
    with pymupdf.open(source) as doc:
        for page_number in range(manifest["start_page"], manifest["end_page"] + 1):
            page = doc[page_number - 1]
            for col in (0, 1):
                clip = pymupdf.Rect(
                    col * page.rect.width / 2,
                    0,
                    (col + 1) * page.rect.width / 2,
                    page.rect.height,
                )
                source_lines = page.get_text("text", clip=clip, sort=True).splitlines()
                context = contexts[col]
                for line_number, text in enumerate(source_lines, 1):
                    s = text.strip()
                    if not s:
                        continue
                    r = next(raw)
                    locator = f"{sha}:{page_number}:{col}:{line_number}"
                    assert (
                        r["raw_id"],
                        r["page"],
                        r["column"],
                        r["line"],
                        r["raw_text"],
                    ) == (locator, page_number, col, line_number, text)
                    counts["raw_lines"] += 1
                    if s.startswith("FROM:"):
                        context.clear()
                        label, code = s[5:].strip().rsplit(None, 1)
                        context.update(
                            origin_label_raw=label,
                            origin_code=code,
                            origin_header_id=locator,
                        )
                    elif s.startswith("TO:"):
                        label, code = s[3:].strip().rsplit(None, 1)
                        context.update(
                            destination_label_raw=label,
                            destination_code=code,
                            destination_header_id=locator,
                        )
                    elif s.startswith("Operated by:"):
                        note = next(notes)
                        assert (
                            note["raw_id"],
                            note["schedule_id"],
                            note["operator_raw"],
                        ) == (locator, previous[col], s[len("Operated by:") :].strip())
                        counts["operator_notes"] += 1
                    elif re.match(r"^\d{2} [A-Za-z]{3}", s):
                        row = next(schedules)
                        assert row["raw_id"] == locator
                        tokens = s.split()
                        assert tokens[2] == "-"
                        dep_index = next(
                            i
                            for i, t in enumerate(tokens[5:], 5)
                            if re.fullmatch(r"\d{2}:\d{2}", t)
                        )
                        rest = tokens[dep_index:]
                        assert len(rest) == 5
                        assert row["days_codes"] == "".join(tokens[5:dep_index])
                        assert row["departure_raw"] == rest[0]
                        assert row["arrival_raw"] == rest[1][:5]
                        assert row["arrival_offset_raw"] == rest[1][5:]
                        assert (
                            row["flight_raw"],
                            row["aircraft_code"],
                            row["duration_raw"],
                        ) == tuple(rest[2:])
                        for field, value in context.items():
                            assert row[field] == value, (page_number, col, field)
                        counts["schedule"] += 1
                        previous[col] = locator
            if page_number % 5000 == 0:
                print("Verified source page", page_number, flush=True)
    assert (
        next(raw, None) is None
        and next(schedules, None) is None
        and next(notes, None) is None
    )
    assert all(counts[k] == manifest["tables"][k] for k in counts)
    result = {
        "all_checks_passed": True,
        "source_sha256": sha,
        "pages": manifest["end_page"] - manifest["start_page"] + 1,
        "counts": counts,
        "scope": "Full native-text re-extraction; all schedule fields and explicit operators compared. Uses the same PDF text engine, not an independent historical or rendering verification.",
    }
    (output / "source_verification.json").write_text(json.dumps(result, indent=2))
    return result


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "source",
        nargs="?",
        type=Path,
        default=BASE_DIR.parents[1] / "raw_data/Skyteam_Timetable.pdf",
    )
    p.add_argument("--output", type=Path, default=BASE_DIR / "data/pdf_schedule")
    args = p.parse_args()
    print(json.dumps(verify(args.source, args.output), indent=2))
