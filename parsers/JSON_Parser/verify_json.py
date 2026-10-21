"""Compare every stored profile and flight against independent ijson reading."""

import argparse
import hashlib
import json
from itertools import zip_longest
from pathlib import Path

import ijson
import pyarrow.parquet as pq

BASE_DIR = Path(__file__).resolve().parent


def rows(output, prefix):
    for part in sorted(output.glob(prefix + "_part_*.parquet")):
        for batch in pq.ParquetFile(part).iter_batches(batch_size=512):
            yield from batch.to_pylist()


def verify(source, output):
    source, output = Path(source), Path(output)
    manifest = json.loads((output / "json_summary.json").read_text())
    assert manifest["integrity_passed"]
    with source.open("rb") as handle:
        sha = hashlib.file_digest(handle, "sha256").hexdigest()
    assert sha == manifest["source_sha256"]
    stored_profiles = rows(output, "json_profiles")
    stored_flights = iter(rows(output, "json_flights"))
    stored_loyalties = iter(rows(output, "json_loyalty"))
    profile_count = flight_count = loyalty_count = 0
    columns = {
        "Date": ("Date",),
        "Flight": ("Flight",),
        "Codeshare": ("Codeshare",),
        **{
            f"{side}_{field}": (side, field)
            for side in ["Departure", "Arrival"]
            for field in ["City", "Airport", "Country"]
        },
    }
    with source.open("rb") as handle:
        independent = ijson.items(handle, "Forum Profiles.item", use_float=True)
        for index, (obj, profile) in enumerate(
            zip_longest(independent, stored_profiles)
        ):
            assert obj is not None and profile is not None
            assert json.loads(profile["raw_profile_json"]) == obj
            assert profile["profile_index"] == index and profile["source_sha256"] == sha
            pid = hashlib.sha256(f"{sha}:{index}".encode()).hexdigest()
            assert profile["profile_record_id"] == profile["record_id"] == pid
            flights = obj.get("Registered Flights", [])
            assert len(flights) == profile["flight_rows"]
            for flight_index, flight in enumerate(flights):
                row = next(stored_flights)
                assert json.loads(row["raw_flight_json"]) == flight
                assert (
                    row["profile_record_id"] == pid
                    and row["profile_index"] == index
                    and row["flight_index"] == flight_index
                )
                assert (
                    row["record_id"]
                    == hashlib.sha256(f"{pid}:0:{flight_index}".encode()).hexdigest()
                )
                for column, path in columns.items():
                    value = flight
                    for key in path:
                        value = value.get(key) if isinstance(value, dict) else None
                    assert row[column] == value, (index, flight_index, column)
                flight_count += 1
            loyalties = obj.get("Loyality Programm", [])
            assert len(loyalties) == profile["loyalty_rows"]
            for loyalty_index, loyalty in enumerate(loyalties):
                row = next(stored_loyalties)
                assert json.loads(row["raw_loyalty_json"]) == loyalty
                assert row["profile_record_id"] == pid
                assert row["profile_index"] == index
                assert row["loyalty_index"] == loyalty_index
                assert row["loyalty_group_index"] == 0
                assert row["loyalty_key"] == "Loyality Programm"
                assert row["source_sha256"] == sha
                assert row["rule_version"] == manifest["rule_version"]
                assert (
                    row["record_id"]
                    == hashlib.sha256(
                        f"{pid}:loyalty:0:{loyalty_index}".encode()
                    ).hexdigest()
                )
                for field in ("Status", "programm", "Number"):
                    value = loyalty.get(field)
                    expected = value if isinstance(value, str) else None
                    assert row[field] == expected, (index, loyalty_index, field)
                loyalty_count += 1
            profile_count += 1
    assert next(stored_flights, None) is None
    assert next(stored_loyalties, None) is None
    assert (
        loyalty_count == manifest["loyalties_written"] == manifest["loyalty_elements"]
    )
    assert (
        profile_count == manifest["profiles"]
        and flight_count == manifest["flights_written"]
    )
    result = dict(
        all_checks_passed=True,
        profiles=profile_count,
        flight_observations=flight_count,
        loyalty_entries=loyalty_count,
        all_loyalty_fields_and_ids_equal=True,
        all_raw_profile_objects_equal=True,
        all_flattened_flight_fields_equal=True,
        source_sha256=sha,
        independent_reader="ijson over the entire source; current file has no duplicate keys",
    )
    (output / "verification.json").write_text(json.dumps(result, indent=2))
    return result


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "source",
        nargs="?",
        type=Path,
        default=BASE_DIR.parents[1] / "raw_data/FrequentFlyerForum-Profiles.json",
    )
    p.add_argument("output", nargs="?", type=Path, default=BASE_DIR / "result")
    args = p.parse_args()
    print(json.dumps(verify(args.source, args.output), indent=2))
