"""Independent fixed-layout source reconciliation + full streaming Parquet join audit."""

import argparse
import hashlib
import json
import re
import time
import unicodedata
from collections import Counter
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from parse_yaml import canonical, sha_file, stable_id, update_digest


def expected_text(raw):
    if raw is None:
        return None
    value = unicodedata.normalize("NFC", raw).strip()
    return (
        None
        if not value or value.upper() in {"N/A", "NA", "NULL", "NONE", "NAN", "-", "~"}
        else value
    )


def independent_scan(source, max_flights=None, sample_ordinals=()):
    day_pattern = re.compile(r"^'(\d{4}-\d{2}-\d{2})':$")
    flight_pattern = re.compile(r"^  ([A-Z0-9]+):$")
    attribute_pattern = re.compile(r"^    (FROM|STATUS|TO): (.*)$")
    card_pattern = re.compile(
        r"^      ([A-Z0-9]{2,3} [0-9]+): \{CLASS: ([^,{}]+), FARE: ([^,{}]+)\}$"
    )
    by_day, samples = {}, {}
    flight_hash, entry_hash = hashlib.sha256(), hashlib.sha256()
    day, current, day_line = None, None, None
    flight_count, entry_count, line_count = 0, 0, 0
    seen_dates, seen_flights = set(), set()
    special_chosen = set()

    def finish():
        nonlocal flight_count, entry_count
        if current is None:
            return
        if (
            set(current["fields"]) != {"FROM", "STATUS", "TO"}
            or current["ff_headers"] != 1
        ):
            raise ValueError("Independent scanner: missing/extra flight fields")
        record = {
            **current["fields"],
            "FF": {c[0]: {"CLASS": c[1], "FARE": c[2]} for c in current["cards"]},
        }
        flight_count += 1
        entry_count += len(current["cards"])
        update_digest(
            flight_hash,
            [
                current["day"],
                current["flight"],
                record["FROM"],
                record["TO"],
                record["STATUS"],
                len(current["cards"]),
            ],
        )
        for card, travel_class, fare in current["cards"]:
            update_digest(
                entry_hash,
                [current["day"], current["flight"], card, travel_class, fare],
            )
        daily = by_day.setdefault(
            current["day"], {"flights": 0, "entries": 0, "flights_without_entries": 0}
        )
        daily["flights"] += 1
        daily["entries"] += len(current["cards"])
        daily["flights_without_entries"] += not current["cards"]
        special = []
        if any(
            card.split(" ", 1)[1].startswith("0") for card, _, _ in current["cards"]
        ):
            special.append("leading_zero_card")
        if len(current["cards"]) > 1:
            special.append("multiple_cards")
        if record["FROM"].upper() in {
            "N/A",
            "NA",
            "NULL",
            "NONE",
            "NAN",
            "-",
            "~",
        } or record["TO"].upper() in {"N/A", "NA", "NULL", "NONE", "NAN", "-", "~"}:
            special.append("missing_airport")
        needed = flight_count in sample_ordinals or any(
            x not in special_chosen for x in special
        )
        if needed:
            samples[(current["day"], current["flight"])] = {
                "ordinal": flight_count,
                "record": record,
                "start_line": current["start_line"],
                "end_line": current["end_line"],
                "date_line": current["date_line"],
                "card_lines": current["card_lines"],
                "reasons": special,
            }
            special_chosen.update(special)

    with source.open(encoding="utf-8-sig", newline="") as handle:
        for line_count, raw in enumerate(handle, 1):
            line = raw.rstrip("\r\n")
            if match := day_pattern.fullmatch(line):
                finish()
                current = None
                if max_flights is not None and flight_count >= max_flights:
                    break
                day = match.group(1)
                if day in seen_dates:
                    raise ValueError("Independent scanner: duplicate date")
                seen_dates.add(day)
                seen_flights = set()
                day_line = line_count
            elif match := flight_pattern.fullmatch(line):
                finish()
                current = None
                if max_flights is not None and flight_count >= max_flights:
                    break
                flight = match.group(1)
                if day is None or flight in seen_flights:
                    raise ValueError("Independent scanner: duplicate or orphan flight")
                seen_flights.add(flight)
                current = {
                    "day": day,
                    "flight": flight,
                    "date_line": day_line,
                    "start_line": line_count,
                    "end_line": line_count,
                    "fields": {},
                    "cards": [],
                    "card_lines": {},
                    "ff_headers": 0,
                }
            elif current is not None and line == "    FF:":
                current["ff_headers"] += 1
                current["end_line"] = line_count
            elif current is not None and (match := attribute_pattern.fullmatch(line)):
                field, value = match.groups()
                if field in current["fields"]:
                    raise ValueError("Independent scanner: duplicate flight field")
                current["fields"][field] = value
                current["end_line"] = line_count
            elif current is not None and (match := card_pattern.fullmatch(line)):
                card, travel_class, fare = match.groups()
                if card in current["card_lines"]:
                    raise ValueError("Independent scanner: duplicate card")
                current["cards"].append((card, travel_class, fare))
                current["card_lines"][card] = line_count
                current["end_line"] = line_count
            else:
                raise ValueError(
                    f"Independent scanner: unsupported line {line_count}: {line[:100]!r}"
                )
        else:
            finish()
    return {
        "flights": flight_count,
        "entries": entry_count,
        "physical_lines": line_count,
        "by_day": by_day,
        "flight_digest": flight_hash.hexdigest(),
        "entry_digest": entry_hash.hexdigest(),
        "samples": samples,
    }


def parquet_rows(path):
    for batch in pq.ParquetFile(path).iter_batches(batch_size=2048):
        yield from batch.to_pylist()


def verify(source, output):
    start = time.perf_counter()
    manifest = json.loads((output / "manifest.json").read_text("utf8"))
    source_sha = sha_file(source)
    if source_sha != manifest["source_sha256"]:
        raise ValueError("Source hash differs from parser input")
    count = manifest["flight_rows"]
    ordinals = {1, 2, min(100, count), max(1, count // 2), max(1, count - 1), count}
    scan = independent_scan(source, None if manifest["complete"] else count, ordinals)
    assert (scan["flights"], scan["entries"]) == (count, manifest["loyalty_entry_rows"])
    assert scan["by_day"] == manifest["by_day"]
    if manifest["complete"]:
        assert scan["physical_lines"] == manifest["physical_lines"]
    print(
        f"Independent source scan: {scan['flights']:,} flights, {scan['entries']:,} entries",
        flush=True,
    )
    for name, info in manifest["outputs"].items():
        assert sha_file(output / name) == info["sha256"]
    assert (
        pq.ParquetFile(output / "loyalty_entries.parquet")
        .schema_arrow.field("loyalty_number")
        .type
        == pa.string()
    )
    assert (
        pq.ParquetFile(output / "flights.parquet").schema_arrow.field("date").type
        == pa.date32()
    )
    entries = iter(parquet_rows(output / "loyalty_entries.parquet"))
    common = [
        "source_file",
        "source_sha256",
        "rule_version",
        "date_raw",
        "date",
        "flight_number_raw",
        "flight_number",
        "origin_raw",
        "origin",
        "destination_raw",
        "destination",
        "status_raw",
        "status",
        "source_date_line",
    ]
    day_counts, statuses = {}, Counter()
    flight_hash, entry_hash = hashlib.sha256(), hashlib.sha256()
    flight_count, entry_count, zero_cards = 0, 0, 0
    sample_results = []
    min_date, max_date = None, None
    last_day, seen_flights = None, set()
    seen_days = set()
    for row in parquet_rows(output / "flights.parquet"):
        flight_count += 1
        day, flight = row["date_raw"], row["flight_number_raw"]
        if day != last_day:
            assert day not in seen_days
            seen_days.add(day)
            last_day, seen_flights = day, set()
        assert flight not in seen_flights
        seen_flights.add(flight)
        assert row["source_sha256"] == source_sha
        assert row["flight_record_id"] == stable_id(source_sha, day, flight)
        assert row["date"] == date.fromisoformat(day)
        min_date = row["date"] if min_date is None else min(min_date, row["date"])
        max_date = row["date"] if max_date is None else max(max_date, row["date"])
        raw = json.loads(row["raw_flight_json"])
        cards = raw.get("FF", {})
        assert row["loyalty_entry_count"] == len(cards)
        for field, key in [
            ("origin", "FROM"),
            ("destination", "TO"),
            ("status", "STATUS"),
        ]:
            assert row[field + "_raw"] == raw.get(key)
            expected = expected_text(raw.get(key))
            if field != "status" and expected is not None:
                expected = expected.upper()
                expected = expected if re.fullmatch(r"[A-Z]{3}", expected) else None
            assert row[field] == expected
        assert row["flight_number"] == re.sub(r"\s+", "", flight.upper())
        for field, status in json.loads(row["field_status_json"]).items():
            statuses["flight." + field + ":" + status] += 1
        assert row["source_end_line"] - row["source_start_line"] + 1 == len(
            row["raw_flight_yaml"].splitlines()
        )
        update_digest(
            flight_hash,
            [
                day,
                flight,
                row["origin_raw"],
                row["destination_raw"],
                row["status_raw"],
                len(cards),
            ],
        )
        seen_cards = set()
        for _ in range(len(cards)):
            entry = next(entries, None)
            assert entry is not None, "Entry table ended prematurely"
            entry_count += 1
            assert entry["flight_record_id"] == row["flight_record_id"]
            assert all(entry[k] == row[k] for k in common), (
                "Shared flight fields differ"
            )
            card = entry["loyalty_key_raw"]
            assert card in cards and card not in seen_cards
            seen_cards.add(card)
            assert entry["entry_id"] == stable_id(row["flight_record_id"], card)
            assert (
                row["source_start_line"]
                <= entry["source_line"]
                <= row["source_end_line"]
            )
            assert json.loads(entry["raw_entry_json"]) == cards[card]
            assert entry["class_raw"] == cards[card].get("CLASS")
            assert entry["fare_raw"] == cards[card].get("FARE")
            assert entry["travel_class"] == expected_text(entry["class_raw"])
            assert entry["fare"] == expected_text(entry["fare_raw"])
            match = re.fullmatch(r"([A-Z0-9]{2,3})\s+([0-9]+)", card.strip())
            assert match and entry["loyalty_program"] == match.group(1)
            assert entry["loyalty_number"] == match.group(2)
            zero_cards += entry["loyalty_number"].startswith("0")
            for field, status in json.loads(entry["field_status_json"]).items():
                statuses["entry." + field + ":" + status] += 1
            update_digest(
                entry_hash, [day, flight, card, entry["class_raw"], entry["fare_raw"]]
            )
        assert seen_cards == set(cards)
        daily = day_counts.setdefault(
            day, {"flights": 0, "entries": 0, "flights_without_entries": 0}
        )
        daily["flights"] += 1
        daily["entries"] += len(cards)
        daily["flights_without_entries"] += not cards
        if sample := scan["samples"].get((day, flight)):
            assert raw == sample["record"]
            assert (
                row["source_start_line"],
                row["source_end_line"],
                row["source_date_line"],
            ) == (sample["start_line"], sample["end_line"], sample["date_line"])
            # Use the independent pure-Python YAML reader on only the sampled fragments.
            restored = yaml.load(row["raw_flight_yaml"], Loader=yaml.BaseLoader)
            assert restored == {flight: raw}
            sample_results.append(
                {
                    "date": day,
                    "flight": flight,
                    "source_start_line": row["source_start_line"],
                    "source_end_line": row["source_end_line"],
                    "entries": len(cards),
                    "equal": True,
                    "source_fields": raw,
                }
            )
        if flight_count % 200_000 == 0:
            print(
                f"Parquet join audit: {flight_count:,} flights, {entry_count:,} entries",
                flush=True,
            )
    assert next(entries, None) is None, "Extra entries with no corresponding flight"
    assert (flight_count, entry_count) == (
        manifest["flight_rows"],
        manifest["loyalty_entry_rows"],
    )
    assert day_counts == manifest["by_day"]
    assert dict(statuses) == manifest["field_status_counts"]
    assert (
        flight_hash.hexdigest()
        == scan["flight_digest"]
        == manifest["flight_content_digest"]
    )
    assert (
        entry_hash.hexdigest()
        == scan["entry_digest"]
        == manifest["entry_content_digest"]
    )
    assert len(sample_results) == len(scan["samples"])
    result = {
        "all_checks_passed": True,
        "full_dataset_complete": manifest["complete"],
        "physical_lines": scan["physical_lines"] if manifest["complete"] else None,
        "flights_read_back": flight_count,
        "entries_read_back": entry_count,
        "days": len(day_counts),
        "date_min": min_date.isoformat(),
        "date_max": max_date.isoformat(),
        "leading_zero_card_numbers": zero_cards,
        "full_source_flight_digest_matches": True,
        "full_source_entry_digest_matches": True,
        "foreign_keys_and_shared_fields_checked_for_every_entry": True,
        "independent_methods": [
            "strict line grammar over all source lines",
            "pure-Python YAML BaseLoader over sampled source fragments",
        ],
        "source_samples": sample_results,
        "seconds": round(time.perf_counter() - start, 3),
    }
    (output / "verification.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf8"
    )
    print(
        canonical(
            {
                k: result[k]
                for k in [
                    "all_checks_passed",
                    "flights_read_back",
                    "entries_read_back",
                    "date_min",
                    "date_max",
                    "seconds",
                ]
            }
        ),
        flush=True,
    )
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    verify(args.source, args.output)
