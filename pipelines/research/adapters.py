import hashlib
import json

import pyarrow as pa
import pyarrow.parquet as pq

from .common import VERSION, clean, dump, export, literal, sha

TABLES = {
    "csv": ("CSV_Parser/result/records.parquet", "boarding observation"),
    "tab": (
        "Tab_Parser/result/flights_part_*.parquet",
        "agency passenger-flight record",
    ),
    "excel": ("Excel_Tickets/result/tickets.parquet", "boarding-pass worksheet"),
    "json_profiles": (
        "JSON_Parser/result/json_profiles_part_*.parquet",
        "forum profile",
    ),
    "json_cards": (
        "JSON_Parser/result/json_loyalty_part_*.parquet",
        "loyalty array element",
    ),
    "json_flights": (
        "JSON_Parser/result/json_flights_part_*.parquet",
        "forum flight observation",
    ),
    "xml_users": ("XML_Parser/result/users.parquet", "source user"),
    "xml_cards": ("XML_Parser/result/cards.parquet", "source loyalty card"),
    "xml_activities": ("XML_Parser/result/activities.parquet", "loyalty activity"),
    "yaml_flights": (
        "YAML_Parser/result/flights.parquet",
        "source date-flight mapping",
    ),
    "yaml_entries": (
        "YAML_Parser/result/loyalty_entries.parquet",
        "FF mapping element",
    ),
    "pdf": ("PDF_Parser/data/pdf_schedule/schedule.parquet", "recurring schedule rule"),
}


def register_sources(db, root, output):
    inventory = []
    hashes = {}
    for table, (pattern, unit) in TABLES.items():
        files = sorted((root / "parsers").glob(pattern))
        if not files:
            raise FileNotFoundError(pattern)
        paths = ",".join(literal(p) for p in files)
        db.execute(f"CREATE VIEW src_{table} AS SELECT * FROM read_parquet([{paths}])")
        for p in files:
            hashes[str(p.relative_to(root))] = sha(p)
        inventory.append(
            {
                "table": table,
                "unit": unit,
                "rows": db.execute(f"SELECT count(*) FROM src_{table}").fetchone()[0],
                "files": [str(p.relative_to(root)) for p in files],
                "bytes": sum(p.stat().st_size for p in files),
                "fields": [
                    {"name": f.name, "type": str(f.type)}
                    for f in pq.ParquetFile(files[0]).schema_arrow
                ],
                "processing_rule": VERSION,
            }
        )
    dump(output / "input_inventory.json", inventory)
    dump(output / "input_hashes.json", hashes)
    return inventory, hashes


def expand_forum(db, root, output):
    schema = pa.schema(
        [
            (n, pa.string())
            for n in [
                "entity_id",
                "source_record_id",
                "source_locator",
                "source_sha256",
                "source_rule_version",
                "source_status",
                "first_name",
                "last_name",
                "sex",
            ]
        ]
    )
    path = output / "forum_profile_fields.parquet"
    passport_values = 0
    with pq.ParquetWriter(path, schema, compression="zstd") as writer:
        for p in sorted(
            (root / "parsers/JSON_Parser/result").glob("json_profiles_part_*.parquet")
        ):
            for batch in pq.ParquetFile(p).iter_batches(
                columns=[
                    "record_id",
                    "profile_index",
                    "source_sha256",
                    "rule_version",
                    "profile_status",
                    "metadata_json",
                ],
                batch_size=2048,
            ):
                rows = []
                for obj in batch.to_pylist():
                    pairs = json.loads(obj["metadata_json"])
                    keys = [key for key, _ in pairs]
                    metadata = {
                        key: value for key, value in pairs if keys.count(key) == 1
                    }
                    real_name = metadata.get("Real Name") or {}
                    if not isinstance(real_name, dict):
                        real_name = {}
                    for entry in metadata.get("Travel Documents", []) or []:
                        if (
                            isinstance(entry, dict)
                            and entry.get("Passports") is not None
                        ):
                            passport_values += 1
                    rows.append(
                        {
                            "entity_id": hashlib.sha256(
                                ("json:" + obj["record_id"]).encode()
                            ).hexdigest(),
                            "source_record_id": obj["record_id"],
                            "source_locator": f"profile[{obj['profile_index']}]",
                            "source_sha256": obj["source_sha256"],
                            "source_rule_version": obj["rule_version"],
                            "source_status": obj["profile_status"],
                            "first_name": clean(real_name.get("First Name")),
                            "last_name": clean(real_name.get("Last Name")),
                            "sex": clean(metadata.get("Sex")),
                        }
                    )
                writer.write_table(pa.Table.from_pylist(rows, schema=schema))
    if passport_values:
        raise ValueError(
            "JSON documents are no longer all null; add an explicit schema adapter"
        )
    db.execute(
        f"CREATE VIEW forum_fields AS SELECT * FROM read_parquet({literal(path)})"
    )


def build_entities(db, output):
    # An observation anchor is deliberately not called a native passenger profile.
    db.execute("""CREATE TABLE entity_raw AS
    SELECT 'csv' AS "source", 'observation_anchor' entity_kind, sha256('csv:'||record_id) entity_id,
      record_id source_record_id, cast(source_record_number as varchar) source_locator,
      source_sha256,rule_version source_rule_version,record_status source_status,
      passenger_first_name first_name,passenger_last_name last_name,passenger_second_name middle_name,
      NULL::VARCHAR full_name,passenger_sex sex,birth_date_candidates_json birth_dates_json,
      passenger_document_raw document_raw,1::BIGINT source_records_represented
    FROM src_csv
    UNION ALL SELECT 'tab','observation_anchor',sha256('tab:'||record_id),record_id,
      cast(source_line as varchar),source_sha256,rule_version,parse_status,
      NULL,NULL,NULL,PaxName,NULL,
      CASE WHEN regexp_full_match(PaxBirthDate,'[0-9]{4}-[0-9]{2}-[0-9]{2}')
        AND try_cast(PaxBirthDate AS DATE) IS NOT NULL THEN to_json([PaxBirthDate]) ELSE '[]' END,
      TravelDoc,1 FROM src_tab
    UNION ALL SELECT 'excel','observation_anchor',sha256('excel:'||record_id),record_id,
      source_file||'/'||source_sheet,source_archive_sha256,rule_version,parse_status,
      NULL,NULL,NULL,passenger_name,NULL,'[]',NULL,1 FROM src_excel
    UNION ALL SELECT 'json','native_profile',entity_id,source_record_id,source_locator,
      source_sha256,source_rule_version,source_status,first_name,last_name,NULL,NULL,sex,'[]',NULL,1 FROM forum_fields
    UNION ALL SELECT 'xml','native_user',sha256('xml:'||user_record_id),user_record_id,source_locator,
      source_sha256,rule_version,'ok',user_first_name,user_last_name,NULL,NULL,NULL,'[]',NULL,1 FROM src_xml_users
    UNION ALL SELECT 'yaml','account_reference',
      sha256('yaml:'||source_sha256||':'||loyalty_program||':'||loyalty_number),
      min(entry_id),'account-reference; see observations for all entry_ids',source_sha256,
      rule_version,'ok',NULL,NULL,NULL,NULL,NULL,'[]',NULL,count(*)
      FROM src_yaml_entries GROUP BY source_sha256,rule_version,loyalty_program,loyalty_number
    """)
    db.execute(f"""CREATE TABLE entities AS SELECT * EXCLUDE(first_name,last_name,middle_name,full_name,sex),
      name_text(first_name) first_name,name_text(last_name) last_name,name_text(middle_name) middle_name,
      name_text(full_name) full_name,clean_text(sex) sex,
      doc_key(document_raw) document_key,{literal(VERSION)} processing_rule
      FROM entity_raw""")
    assert db.execute(
        "SELECT count(*)=count(distinct entity_id) FROM entities"
    ).fetchone()[0]
    export(
        db,
        "SELECT * FROM entities ORDER BY source,entity_id",
        output / "source_entities.parquet",
    )


def build_identifiers(db, output):
    db.execute("""CREATE TABLE identifier_raw AS
      SELECT source,entity_id,source_record_id,source_sha256,'document' identifier_type,
        'untyped_document' namespace,document_raw raw_value,document_key AS "value",
        document_key IS NOT NULL usable FROM entities WHERE document_raw IS NOT NULL
      UNION ALL SELECT 'excel',sha256('excel:'||record_id),record_id,source_archive_sha256,
        'loyalty',card_program_key(loyalty_card),loyalty_card_raw,card_number_key(loyalty_card),
        card_program_key(loyalty_card) IS NOT NULL FROM src_excel WHERE loyalty_card_raw IS NOT NULL
      UNION ALL SELECT 'tab',sha256('tab:'||record_id),record_id,source_sha256,
        'loyalty',card_program_key(FrequentFlyer),FrequentFlyer,card_number_key(FrequentFlyer),
        card_program_key(FrequentFlyer) IS NOT NULL FROM src_tab WHERE clean_text(FrequentFlyer) IS NOT NULL
      UNION ALL SELECT 'json',sha256('json:'||profile_record_id),record_id,source_sha256,
        'loyalty',clean_text(programm),Number,clean_text(Number),parse_status='ok' AND clean_text(Number) IS NOT NULL
        FROM src_json_cards
      UNION ALL SELECT 'xml',sha256('xml:'||user_record_id),card_record_id,source_sha256,
        'loyalty',card_program_key(card_number),card_number,card_number_key(card_number),
        card_program_key(card_number) IS NOT NULL AND card_number_key(card_number) IS NOT NULL FROM src_xml_cards
      UNION ALL SELECT 'yaml',sha256('yaml:'||source_sha256||':'||loyalty_program||':'||loyalty_number),
        min(entry_id),source_sha256,'loyalty',loyalty_program,min(loyalty_key_raw),loyalty_number,
        loyalty_program IS NOT NULL AND loyalty_number IS NOT NULL FROM src_yaml_entries
        GROUP BY source_sha256,loyalty_program,loyalty_number
    """)
    db.execute(f"""CREATE TABLE identifiers AS SELECT i.*,x.card_program raw_program_label,
      sha256(i.entity_id||':'||i.source_record_id||':'||identifier_type||':'||coalesce(namespace,'')||':'||coalesce(value,'')) assertion_id,
      {literal(VERSION)} processing_rule FROM identifier_raw i LEFT JOIN src_xml_cards x
      ON i.source='xml' AND i.source_record_id=x.card_record_id""")
    assert db.execute(
        "SELECT count(*)=count(distinct assertion_id) FROM identifiers"
    ).fetchone()[0]
    assert (
        db.execute(
            "SELECT count(*) FROM identifiers i ANTI JOIN entities e USING(entity_id)"
        ).fetchone()[0]
        == 0
    )
    export(
        db,
        "SELECT * FROM identifiers ORDER BY source,entity_id",
        output / "identifier_assertions.parquet",
    )
    export(
        db,
        f"""SELECT card_program raw_program_label,card_program_key(card_number) explicit_program_code,
      count(*) card_assertions,count(distinct card_number_key(card_number)) distinct_numbers,
      {literal(VERSION)} processing_rule FROM src_xml_cards GROUP BY card_program,card_program_key(card_number)""",
        output / "xml_program_labels.parquet",
    )


def build_observations(db, output):
    db.execute("""CREATE TABLE obs_raw AS
      SELECT 'csv' AS "source",'boarding observation' unit,record_id observation_id,record_id source_record_id,
        sha256('csv:'||record_id) entity_id,source_sha256,rule_version source_rule_version,
        flight_date_raw date_raw,flight_number_raw flight_raw,NULL::VARCHAR origin_raw,
        NULL::VARCHAR destination_raw,flight_time local_time,booking_code booking_reference,
        ticket_number, destination_text,record_status source_status,
        to_json([ticket_number,passenger_document,passenger_first_name,passenger_last_name,
          passenger_second_name,cast(flight_date as varchar),flight_number,flight_time,destination_text]) duplicate_key
        FROM src_csv
      UNION ALL SELECT 'tab','agency passenger-flight record',record_id,record_id,sha256('tab:'||record_id),
        source_sha256,rule_version,DepartDate,FlightCode,"From",Dest,DepartTime,Code,"e-Ticket",NULL,parse_status,
        to_json(["e-Ticket",TravelDoc,PaxName,DepartDate,FlightCode,"From",Dest,DepartTime]) FROM src_tab
      UNION ALL SELECT 'excel','boarding-pass worksheet',record_id,record_id,sha256('excel:'||record_id),
        source_archive_sha256,rule_version,departure_date_raw,flight_number_raw,origin_airport_raw,
        destination_airport_raw,departure_time,booking_reference,ticket_number,NULL,parse_status,
        to_json([ticket_number,passenger_name,cast(departure_date as varchar),flight_number,
          origin_airport,destination_airport,departure_time]) FROM src_excel
      UNION ALL SELECT 'json','forum flight observation',record_id,record_id,sha256('json:'||profile_record_id),
        source_sha256,rule_version,Date,Flight,Departure_Airport,Arrival_Airport,NULL,NULL,NULL,NULL,parse_status,
        to_json([profile_record_id,Date,Flight,Departure_Airport,Arrival_Airport]) FROM src_json_flights
      UNION ALL SELECT 'xml','loyalty activity',record_id,record_id,sha256('xml:'||user_record_id),
        source_sha256,rule_version,flight_date_raw,flight_number_raw,departure_iata_raw,arrival_iata_raw,
        NULL,NULL,NULL,NULL,'ok',to_json([user_record_id,card_record_id,cast(flight_date as varchar),
          flight_number,departure_iata,arrival_iata,activity_type]) FROM src_xml_activities
      UNION ALL SELECT 'yaml','FF mapping element',entry_id,entry_id,
        sha256('yaml:'||source_sha256||':'||loyalty_program||':'||loyalty_number),source_sha256,
        rule_version,date_raw,flight_number_raw,origin_raw,destination_raw,NULL,NULL,NULL,NULL,'ok',
        to_json([flight_record_id,loyalty_program,loyalty_number]) FROM src_yaml_entries
    """)
    db.execute(f"""CREATE TABLE observations AS SELECT * EXCLUDE(date_raw,flight_raw,origin_raw,destination_raw),
      date_raw,flight_raw,origin_raw,destination_raw,
      CASE WHEN regexp_full_match(clean_text(date_raw),'[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}') THEN try_cast(clean_text(date_raw) AS DATE) END date,
      regexp_replace(upper(clean_text(flight_raw)),'[[:space:]]+','','g') flight,
      airport_text(origin_raw) origin,airport_text(destination_raw) destination,
      regexp_full_match(coalesce(airport_text(origin_raw),''),'[A-Z]{{3}}') AND
      regexp_full_match(coalesce(airport_text(destination_raw),''),'[A-Z]{{3}}') valid_airport_pair,
      coalesce(airport_text(origin_raw) IN ('NA','N/A','NULL','NONE','NAN','-'),false) OR
        coalesce(airport_text(destination_raw) IN ('NA','N/A','NULL','NONE','NAN','-'),false) airport_marker_ambiguity,
      CASE WHEN airport_marker_ambiguity THEN 'literal_marker_ambiguous'
        WHEN valid_airport_pair THEN 'syntax_only_unverified' ELSE 'missing_or_nonstandard' END airport_pair_status,
      {literal(VERSION)} processing_rule FROM obs_raw""")
    assert db.execute(
        "SELECT count(*)=count(distinct observation_id) FROM observations"
    ).fetchone()[0]
    assert (
        db.execute(
            "SELECT count(*) FROM observations o ANTI JOIN entities e USING(entity_id)"
        ).fetchone()[0]
        == 0
    )
    export(
        db,
        "SELECT * FROM observations ORDER BY source,observation_id",
        output / "observations.parquet",
    )
