"""Read Pointz XML into separate users, cards and activity tables."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = BASE_DIR.parents[1] / "raw_data/PointzAggregator-AirlinesData.xml"
VERSION = "pointz.xml.v2"
ACTIVITY_FIELDS = {
    "Code": "flight_number",
    "Date": "flight_date",
    "Departure": "departure_iata",
    "Arrival": "arrival_iata",
    "Fare": "fare_class",
}


def text_json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def source_hash(path):
    digest = hashlib.sha256()
    tail = b""
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            if b"<!DOCTYPE" in tail + block or b"<!ENTITY" in tail + block:
                raise ValueError("DTD and entity declarations are unsupported")
            digest.update(block)
            tail = block[-16:]
    return digest.hexdigest()


def local_name(tag):
    return tag.rsplit("}", 1)[-1]


def children(element, name):
    return [node for node in element if local_name(node.tag) == name]


def child_text(element, name):
    found = children(element, name)
    if len(found) > 1:
        raise ValueError(f"Repeated scalar child: {name}")
    return found[0].text if found else None


def iter_users(source):
    context = ET.iterparse(source, events=("start", "end"))
    _, root = next(context)
    if local_name(root.tag) != "PointzAggregatorUsers":
        raise ValueError("Unsupported XML root")
    index = 0
    for event, element in context:
        if event == "end" and local_name(element.tag) == "user":
            index += 1
            yield index, element
            root.remove(element)
            element.clear()


def identity(*parts):
    return hashlib.sha256(text_json(parts).encode()).hexdigest()


def schemas():
    common = [
        ("source_file", pa.string()),
        ("source_sha256", pa.string()),
        ("rule_version", pa.string()),
        ("source_locator", pa.string()),
        ("user_record_id", pa.string()),
        ("user_id", pa.string()),
    ]
    users = common + [
        (name, pa.string())
        for name in [
            "user_first_name",
            "user_last_name",
            "user_attrs_json",
            "name_attrs_json",
            "raw_user_xml",
        ]
    ]
    cards = common + [
        (name, pa.string())
        for name in [
            "card_record_id",
            "card_number",
            "card_program_raw",
            "card_program",
            "card_attrs_json",
        ]
    ]
    activities = common + [
        (name, pa.string())
        for name in [
            "record_id",
            "card_record_id",
            "card_number",
            "card_program",
            "activity_type",
            "activity_attrs_json",
            "field_status_json",
        ]
    ]
    for name in ACTIVITY_FIELDS.values():
        activities.extend(
            [
                (name + "_raw", pa.string()),
                (name, pa.date32() if name == "flight_date" else pa.string()),
            ]
        )
    return {
        "users": pa.schema(users),
        "cards": pa.schema(cards),
        "activities": pa.schema(activities),
    }


class Sink:
    def __init__(self, path, schema):
        self.schema, self.rows, self.count = schema, [], 0
        self.writer = pq.ParquetWriter(path, schema, compression="zstd")

    def add(self, row):
        self.rows.append(row)
        self.count += 1
        if len(self.rows) >= 1024:
            self.flush()

    def flush(self):
        if self.rows:
            self.writer.write_table(pa.Table.from_pylist(self.rows, schema=self.schema))
            self.rows.clear()

    def close(self):
        self.flush()
        self.writer.close()


def run(source=DEFAULT_INPUT, output=BASE_DIR / "result"):
    source, output = Path(source), Path(output)
    digest = source_hash(source)
    partial = output.with_name(output.name + ".partial")
    if output.exists() or partial.exists():
        raise FileExistsError("Choose a fresh output directory")
    partial.mkdir(parents=True)
    sinks = {
        name: Sink(partial / f"{name}.parquet", schema)
        for name, schema in schemas().items()
    }
    counters = Counter()
    try:
        for user_index, user in iter_users(source):
            user_locator = f"/PointzAggregatorUsers/user[{user_index}]"
            user_record_id = identity(digest, user_locator)
            base = dict(
                source_file=source.name,
                source_sha256=digest,
                rule_version=VERSION,
                user_record_id=user_record_id,
                user_id=user.get("uid"),
            )
            names = children(user, "name")
            if len(names) > 1:
                raise ValueError("Repeated name element")
            attrs = names[0].attrib if names else {}
            sinks["users"].add(
                dict(
                    base,
                    source_locator=user_locator,
                    user_first_name=attrs.get("first"),
                    user_last_name=attrs.get("last"),
                    user_attrs_json=text_json(user.attrib),
                    name_attrs_json=text_json(attrs),
                    raw_user_xml=ET.tostring(user, encoding="unicode"),
                )
            )
            card_count = 0
            for container_index, container in enumerate(children(user, "cards"), 1):
                for card_index, card in enumerate(children(container, "card"), 1):
                    card_count += 1
                    locator = (
                        f"{user_locator}/cards[{container_index}]/card[{card_index}]"
                    )
                    card_id = identity(digest, locator)
                    program_raw = child_text(card, "bonusprogramm")
                    program = program_raw.strip() if program_raw is not None else None
                    card_base = dict(
                        base,
                        card_record_id=card_id,
                        card_number=card.get("number"),
                        card_program=program,
                    )
                    sinks["cards"].add(
                        dict(
                            card_base,
                            source_locator=locator,
                            card_program_raw=program_raw,
                            card_attrs_json=text_json(card.attrib),
                        )
                    )
                    activity_count = 0
                    for group_index, group in enumerate(
                        children(card, "activities"), 1
                    ):
                        for activity_index, activity in enumerate(
                            children(group, "activity"), 1
                        ):
                            activity_count += 1
                            activity_locator = f"{locator}/activities[{group_index}]/activity[{activity_index}]"
                            row = dict(
                                card_base,
                                source_locator=activity_locator,
                                record_id=identity(digest, activity_locator),
                                activity_type=activity.get("type"),
                                activity_attrs_json=text_json(activity.attrib),
                            )
                            statuses = {}
                            for xml_name, name in ACTIVITY_FIELDS.items():
                                raw = child_text(activity, xml_name)
                                row[name + "_raw"] = raw
                                value = raw.strip() if raw is not None else None
                                status = (
                                    "absent"
                                    if raw is None
                                    else "empty"
                                    if value == ""
                                    else "literal_text"
                                )
                                if name == "flight_date" and value:
                                    try:
                                        if not re.fullmatch(
                                            r"\d{4}-\d{2}-\d{2}", value
                                        ):
                                            raise ValueError
                                        value = date.fromisoformat(value)
                                        status = "valid_iso_date"
                                    except ValueError:
                                        value, status = (
                                            None,
                                            "invalid_or_ambiguous_date",
                                        )
                                elif name == "flight_date":
                                    value = None
                                row[name] = value
                                statuses[name] = status
                                counters[f"field:{name}:{status}"] += 1
                            row["field_status_json"] = text_json(statuses)
                            sinks["activities"].add(row)
                            counters[f"activity_type:{activity.get('type')}"] += 1
                    counters["cards_without_activities"] += activity_count == 0
            counters["users_without_cards"] += card_count == 0
        for sink in sinks.values():
            sink.close()
        if source_hash(source) != digest:
            raise ValueError("Source changed during parsing")
        result = dict(
            complete=True,
            rule_version=VERSION,
            source_file=source.name,
            source_sha256=digest,
            tables={name: sink.count for name, sink in sinks.items()},
            counters=dict(counters),
            raw_xml_scope="Parsed XML serialization; original lexical bytes remain in the immutable source",
            unit="Separate source users, cards and loyalty activities; empty cards do not create flights",
        )
        (partial / "manifest.json").write_text(json.dumps(result, indent=2))
        partial.rename(output)
        return result
    except BaseException as error:
        for sink in sinks.values():
            try:
                sink.writer.close()
            except Exception:
                pass
        (partial / "FAILED.json").write_text(
            json.dumps(dict(complete=False, error_type=type(error).__name__))
        )
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", nargs="?", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=BASE_DIR / "result")
    args = parser.parse_args()
    print(json.dumps(run(args.source, args.output), indent=2))
