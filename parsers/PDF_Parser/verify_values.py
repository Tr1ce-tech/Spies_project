"""Check normalized schedule values against retained source fields."""

import argparse
import json
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
MONTHS = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split()


def same(actual, expected):
    return bool(
        (
            actual.astype("string").fillna("<NULL>")
            == expected.astype("string").fillna("<NULL>")
        ).all()
    )


def verify(output):
    output = Path(output)
    manifest = json.loads((output / "manifest.json").read_text())
    columns = [
        "validity_raw",
        "valid_from",
        "valid_to",
        "days_raw",
        "days_codes",
        "departure_raw",
        "departure_local",
        "arrival_raw",
        "arrival_local",
        "arrival_offset_raw",
        "arrival_day_offset",
        "flight_raw",
        "flight_number",
        "flight_marker",
        "duration_raw",
        "duration_minutes",
    ]
    f = pd.read_parquet(
        output / "schedule.parquet", columns=columns, dtype_backend="pyarrow"
    )
    calendar = {}
    for timestamp in pd.date_range(
        manifest["coverage_start"], manifest["coverage_end"]
    ):
        d = timestamp.date()
        key = f"{d.day:02d} {MONTHS[d.month - 1]}"
        calendar.setdefault(key, []).append(d.isoformat())
    periods = {}
    for raw in f.validity_raw.unique():
        parts = raw.split()
        assert len(parts) == 5 and parts[2] == "-"
        options = [
            (a, b)
            for a in calendar.get(" ".join(parts[:2]), [])
            for b in calendar.get(" ".join(parts[-2:]), [])
            if a <= b
        ]
        periods[raw] = options[0] if len(options) == 1 else (None, None)
    expected_from = f.validity_raw.map({k: v[0] for k, v in periods.items()})
    expected_to = f.validity_raw.map({k: v[1] for k, v in periods.items()})
    offsets = {
        raw: (None if raw.startswith("+-") else int(raw[1:]) if raw else 0)
        for raw in f.arrival_offset_raw.unique()
    }
    expected_offset = f.arrival_offset_raw.map(offsets).astype("Int32")
    duration = f.duration_raw.str.extract(r"^(?P<hours>\d+)H(?P<minutes>\d+)M$").astype(
        "Int32"
    )
    expected_duration = (duration["hours"] * 60 + duration["minutes"]).where(
        duration["minutes"] < 60
    )
    flight_ids = {x: x[:-1] if x.endswith("*") else x for x in f.flight_raw.unique()}
    markers = {x: "*" if x.endswith("*") else "" for x in f.flight_raw.unique()}
    checks = {
        "all_normalized_periods": same(f.valid_from, expected_from)
        and same(f.valid_to, expected_to),
        "all_weekday_codes": same(
            f.days_codes, f.days_raw.str.replace(r"\s+", "", regex=True)
        ),
        "departure_local_clocks": same(
            f.departure_local,
            f.departure_raw.where(
                f.departure_raw.str.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d")
            ),
        ),
        "arrival_local_clocks": same(
            f.arrival_local,
            f.arrival_raw.where(
                f.arrival_raw.str.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d")
            ),
        ),
        "arrival_offsets_and_ambiguity": same(f.arrival_day_offset, expected_offset),
        "flight_identifiers_and_markers": same(
            f.flight_number, f.flight_raw.map(flight_ids)
        )
        and same(f.flight_marker, f.flight_raw.map(markers)),
        "duration_minutes": same(f.duration_minutes, expected_duration),
    }
    result = {
        "all_checks_passed": all(checks.values()),
        "rows": len(f),
        "checks": checks,
        "scope": "All normalized fields compared to their raw source fields; not historical schedule verification",
    }
    (output / "value_verification.json").write_text(json.dumps(result, indent=2))
    assert result["all_checks_passed"], result
    return result


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", type=Path, default=BASE_DIR / "data/pdf_schedule")
    args = p.parse_args()
    print(json.dumps(verify(args.output), indent=2))
