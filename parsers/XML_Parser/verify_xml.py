"""Compare all XML fields with an independent SAX pass."""

import argparse
import hashlib
import json
import xml.sax
from collections import Counter
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

BASE_DIR = Path(__file__).resolve().parent


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def update(digest, value):
    digest.update((canonical(value) + "\n").encode())


class SourceHandler(xml.sax.ContentHandler):
    def __init__(self):
        self.digests = {
            name: hashlib.sha256() for name in ["users", "cards", "activities"]
        }
        self.counts = Counter()
        self.user = self.card = self.activity = None
        self.current_field = None
        self.text = []

    def startElement(self, name, attrs):
        name = name.split(":")[-1]
        if name == "user":
            self.user = {"attrs": dict(attrs), "name": {}, "cards": []}
        elif name == "name":
            self.user["name"] = dict(attrs)
        elif name == "card":
            self.card = {"attrs": dict(attrs), "program": None, "activities": []}
            self.user["cards"].append(self.card)
        elif name == "activity":
            self.activity = {"attrs": dict(attrs), "fields": {}}
            self.card["activities"].append(self.activity)
        elif name in ["bonusprogramm", "Code", "Date", "Departure", "Arrival", "Fare"]:
            self.current_field = name
            self.text = []

    def characters(self, text):
        if self.current_field:
            self.text.append(text)

    def endElement(self, name):
        name = name.split(":")[-1]
        if name == self.current_field:
            value = "".join(self.text) if self.text else None
            if name == "bonusprogramm":
                self.card["program"] = value
            else:
                self.activity["fields"][name] = value
            self.current_field = None
        elif name == "user":
            u = self.user
            uid = u["attrs"].get("uid")
            update(
                self.digests["users"],
                [
                    uid,
                    u["name"].get("first"),
                    u["name"].get("last"),
                    u["attrs"],
                    u["name"],
                ],
            )
            self.counts["users"] += 1
            for card in u["cards"]:
                number = card["attrs"].get("number")
                program = card["program"]
                update(self.digests["cards"], [uid, number, program, card["attrs"]])
                self.counts["cards"] += 1
                for a in card["activities"]:
                    update(
                        self.digests["activities"],
                        [
                            uid,
                            number,
                            program.strip() if program is not None else None,
                            a["attrs"],
                            *[
                                a["fields"].get(n)
                                for n in [
                                    "Code",
                                    "Date",
                                    "Departure",
                                    "Arrival",
                                    "Fare",
                                ]
                            ],
                        ],
                    )
                    self.counts["activities"] += 1


def verify(source, output):
    source, output = Path(source), Path(output)
    manifest = json.loads((output / "manifest.json").read_text())
    assert manifest["complete"]
    with source.open("rb") as f:
        sha = hashlib.file_digest(f, "sha256").hexdigest()
    assert sha == manifest["source_sha256"]
    handler = SourceHandler()
    parser = xml.sax.make_parser()
    parser.setFeature(xml.sax.handler.feature_external_ges, False)
    parser.setContentHandler(handler)
    parser.parse(str(source))
    hashes = {name: hashlib.sha256() for name in handler.digests}
    counts = Counter()
    user_ids = set()
    card_ids = {}
    ids = set()
    for name in ["users", "cards", "activities"]:
        table = pq.ParquetFile(output / f"{name}.parquet")
        assert table.schema_arrow.field("user_id").type == pa.string()
        for batch in table.iter_batches(batch_size=1024):
            for row in batch.to_pylist():
                assert row["source_sha256"] == sha
                uid = row["user_id"]
                counts[name] += 1
                if name == "users":
                    update(
                        hashes[name],
                        [
                            uid,
                            row["user_first_name"],
                            row["user_last_name"],
                            json.loads(row["user_attrs_json"]),
                            json.loads(row["name_attrs_json"]),
                        ],
                    )
                    user_ids.add(row["user_record_id"])
                    assert "<" in row["raw_user_xml"]
                elif name == "cards":
                    assert row["user_record_id"] in user_ids
                    update(
                        hashes[name],
                        [
                            uid,
                            row["card_number"],
                            row["card_program_raw"],
                            json.loads(row["card_attrs_json"]),
                        ],
                    )
                    card_ids[row["card_record_id"]] = (
                        row["user_record_id"],
                        row["card_number"],
                        row["card_program"],
                    )
                else:
                    assert card_ids[row["card_record_id"]] == (
                        row["user_record_id"],
                        row["card_number"],
                        row["card_program"],
                    )
                    update(
                        hashes[name],
                        [
                            uid,
                            row["card_number"],
                            row["card_program"],
                            json.loads(row["activity_attrs_json"]),
                            *[
                                row[n + "_raw"]
                                for n in [
                                    "flight_number",
                                    "flight_date",
                                    "departure_iata",
                                    "arrival_iata",
                                    "fare_class",
                                ]
                            ],
                        ],
                    )
                    assert row["record_id"] not in ids
                    ids.add(row["record_id"])
                    raw = row["flight_date_raw"]
                    status = json.loads(row["field_status_json"])["flight_date"]
                    if status == "valid_iso_date":
                        assert row["flight_date"] == date.fromisoformat(raw.strip())
        assert counts[name] == handler.counts[name] == manifest["tables"][name]
        assert hashes[name].hexdigest() == handler.digests[name].hexdigest(), name
    assert len(user_ids) == counts["users"] and len(card_ids) == counts["cards"]
    result = {
        "all_checks_passed": True,
        "counts": dict(counts),
        "source_sha256": sha,
        "independent_reader": "xml.sax over all users, cards and activity fields",
        "foreign_keys_and_ID_types_checked": True,
    }
    (output / "verification.json").write_text(json.dumps(result, indent=2))
    return result


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "source",
        nargs="?",
        type=Path,
        default=BASE_DIR.parents[1] / "raw_data/PointzAggregator-AirlinesData.xml",
    )
    p.add_argument("output", nargs="?", type=Path, default=BASE_DIR / "result")
    args = p.parse_args()
    print(json.dumps(verify(args.source, args.output), indent=2))
