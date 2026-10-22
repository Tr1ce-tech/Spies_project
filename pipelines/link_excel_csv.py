"""Produce provisional observation links; do not merge people or assign passports."""

import argparse
import hashlib
import json
import time
import unicodedata
from collections import Counter
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parents[1]
VERSION = "excel-csv-observation-links.v1"
KEYS = ["ticket_key", "date_key", "flight_key"]


def text(value):
    if value is None or pd.isna(value):
        return ""
    return " ".join(unicodedata.normalize("NFC", str(value)).upper().split())


def compare_names(row):
    excel = row["excel_name_key"].split()
    csv = row["csv_name_key"].split()
    if not excel or not csv:
        return "missing_full_name"
    if sorted(excel) == sorted(csv):
        return "same_full_name_tokens"
    if sorted(excel) == sorted(row["csv_first_last_key"].split()):
        return "only_first_and_last_name_agree"
    return "full_name_components_unresolved"


def assess(row):
    reasons = []
    if row["excel_block_rows"] != 1 or row["csv_block_rows"] != 1:
        reasons.append("nonunique_observation_key")
    if row["known_documents_in_block"] > 1:
        reasons.append("conflicting_documents_in_block")
    if row["known_routes_in_block"] > 1:
        reasons.append("conflicting_excel_routes_in_block")
    name_evidence = compare_names(row)
    if name_evidence != "same_full_name_tokens":
        reasons.append(name_evidence)
    for field in ("pnr_key", "time_key"):
        a, b = row["excel_" + field], row["csv_" + field]
        if a and b and a != b:
            reasons.append("different_known_" + field)
    if row["excel_parse_status"] == "needs_review":
        reasons.append("excel_record_needs_review")
    return (
        "review" if reasons else "supported_observation_link",
        json.dumps(reasons, separators=(",", ":")),
    )


def sha256(path):
    with Path(path).open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def run(excel_path, csv_path, output):
    started = time.perf_counter()
    output = Path(output)
    if output.exists():
        raise FileExistsError(f"Choose a fresh output directory: {output}")
    excel_manifest = json.loads(
        Path(excel_path).with_suffix(".manifest.json").read_text()
    )
    if not excel_manifest["complete"]:
        raise ValueError("Excel extraction is incomplete")
    assert sha256(excel_path) == excel_manifest["output_sha256"]
    excel_columns = [
        "record_id",
        "ticket_number",
        "departure_date",
        "flight_number",
        "passenger_name",
        "booking_reference",
        "departure_time",
        "origin_airport",
        "destination_airport",
        "parse_status",
    ]
    csv_columns = [
        "record_id",
        "ticket_number",
        "flight_date",
        "flight_number",
        "passenger_first_name",
        "passenger_second_name",
        "passenger_last_name",
        "booking_code",
        "flight_time",
        "passenger_document",
    ]
    input_hashes = {
        "excel_parquet": sha256(excel_path),
        "csv_parquet": sha256(csv_path),
    }
    excel = pd.read_parquet(excel_path, columns=excel_columns)
    csv = pd.read_parquet(csv_path, columns=csv_columns)
    assert excel.record_id.is_unique and csv.record_id.is_unique
    for table, date_field in ((excel, "departure_date"), (csv, "flight_date")):
        table["ticket_key"] = table.ticket_number.map(text)
        table["flight_key"] = table.flight_number.map(text)
        table["date_key"] = table[date_field].map(
            lambda d: "" if pd.isna(d) else d.isoformat()
        )
    excel["excel_name_key"] = excel.passenger_name.map(text)
    csv["csv_name_key"] = csv.apply(
        lambda r: " ".join(
            text(r[c])
            for c in (
                "passenger_last_name",
                "passenger_first_name",
                "passenger_second_name",
            )
            if text(r[c])
        ),
        axis=1,
    )
    csv["csv_first_last_key"] = csv.apply(
        lambda r: " ".join(
            text(r[c])
            for c in ("passenger_first_name", "passenger_last_name")
            if text(r[c])
        ),
        axis=1,
    )
    for table, prefix, pnr, clock in (
        (excel, "excel", "booking_reference", "departure_time"),
        (csv, "csv", "booking_code", "flight_time"),
    ):
        table[prefix + "_pnr_key"] = table[pnr].map(text)
        table[prefix + "_time_key"] = table[clock].map(text)
    excel["route_key"] = excel.apply(
        lambda r: (
            f"{text(r.origin_airport)}>{text(r.destination_airport)}"
            if text(r.origin_airport) and text(r.destination_airport)
            else None
        ),
        axis=1,
    )
    # Documents are counted literally after outer whitespace removal; no type or country is inferred.
    csv["document_key"] = csv.passenger_document.map(
        lambda v: None if pd.isna(v) else v.strip()
    )
    all_excel, all_csv = len(excel), len(csv)
    excel = excel.loc[excel[KEYS].ne("").all(axis=1)].copy()
    csv = csv.loc[csv[KEYS].ne("").all(axis=1)].copy()
    excel["excel_block_rows"] = excel.groupby(KEYS).record_id.transform("size")
    csv["csv_block_rows"] = csv.groupby(KEYS).record_id.transform("size")
    excel["known_routes_in_block"] = excel.groupby(KEYS).route_key.transform("nunique")
    csv["known_documents_in_block"] = csv.groupby(KEYS).document_key.transform(
        "nunique"
    )
    csv["csv_document_available"] = csv.document_key.notna() & csv.document_key.ne("")
    excel = excel.rename(
        columns={"record_id": "excel_record_id", "parse_status": "excel_parse_status"}
    )
    csv = csv.rename(columns={"record_id": "csv_record_id"})
    links = excel[
        KEYS
        + [
            "excel_record_id",
            "excel_name_key",
            "excel_pnr_key",
            "excel_time_key",
            "excel_block_rows",
            "known_routes_in_block",
            "excel_parse_status",
        ]
    ].merge(
        csv[
            KEYS
            + [
                "csv_record_id",
                "csv_name_key",
                "csv_first_last_key",
                "csv_pnr_key",
                "csv_time_key",
                "csv_block_rows",
                "known_documents_in_block",
                "csv_document_available",
            ]
        ],
        on=KEYS,
        how="inner",
        validate="many_to_many",
    )
    records = links.to_dict("records")
    links["name_evidence"] = [compare_names(row) for row in records]
    decisions = [assess(row) for row in records]
    links["status"] = [x[0] for x in decisions]
    links["reasons_json"] = [x[1] for x in decisions]
    links["rule_version"] = VERSION
    links["link_id"] = [
        hashlib.sha256(f"{VERSION}:{a}:{b}".encode()).hexdigest()
        for a, b in zip(links.excel_record_id, links.csv_record_id)
    ]
    assert links.link_id.is_unique
    output_columns = [
        "link_id",
        "excel_record_id",
        "csv_record_id",
        "status",
        "reasons_json",
        "name_evidence",
        "excel_block_rows",
        "csv_block_rows",
        "known_routes_in_block",
        "known_documents_in_block",
        "csv_document_available",
        "rule_version",
    ]
    report = {
        "rule_version": VERSION,
        "inputs": input_hashes,
        "excel_source_archive_sha256": excel_manifest["input_sha256"],
        "excel_rows_total": all_excel,
        "csv_rows_total": all_csv,
        "excel_rows_with_complete_key": len(excel),
        "csv_rows_with_complete_key": len(csv),
        "candidate_pairs": len(links),
        "statuses": dict(Counter(links.status)),
        "excel_records_with_candidates": links.excel_record_id.nunique(),
        "csv_records_with_candidates": links.csv_record_id.nunique(),
        "candidate_pairs_with_csv_document": int(links.csv_document_available.sum()),
        "name_evidence": dict(Counter(links.name_evidence)),
        "reasons": dict(
            Counter(
                reason for _, reasons in decisions for reason in json.loads(reasons)
            )
        ),
        "scope": "Exact ticket + date + flight candidates only; not person identities or completed flights",
        "name_rule": "NFC, uppercase, whitespace; compare full token multisets regardless of order; first+last-only agreement stays review; no transliteration",
        "passport_rule": "Document remains an attribute of the CSV source observation; not assigned to an Excel passenger",
        "precision": None,
        "recall": None,
        "limitation": "No independent labels; candidates outside the exact-key block are not searched",
        "seconds": round(time.perf_counter() - started, 3),
    }
    assert sha256(excel_path) == input_hashes["excel_parquet"]
    assert sha256(csv_path) == input_hashes["csv_parquet"]
    output.mkdir(parents=True)
    links[output_columns].to_parquet(
        output / "excel_csv_candidates.parquet", index=False
    )
    assert len(pd.read_parquet(output / "excel_csv_candidates.parquet")) == len(links)
    (output / "manifest.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2)
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--excel",
        type=Path,
        default=BASE / "parsers/Excel_Tickets/result/tickets.parquet",
    )
    parser.add_argument(
        "--csv", type=Path, default=BASE / "parsers/CSV_Parser/result/records.parquet"
    )
    parser.add_argument(
        "--output", type=Path, default=BASE / "proceed_data/excel_csv_v1"
    )
    args = parser.parse_args()
    run(args.excel, args.csv, args.output)
