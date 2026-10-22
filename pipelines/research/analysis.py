import json

from .common import VERSION, dump, export, literal

PAYLOAD = {
    "csv": [
        "passenger_first_name",
        "passenger_second_name",
        "passenger_last_name",
        "passenger_document",
        "passenger_birth_date_raw",
        "ticket_number",
        "booking_code",
        "flight_date",
        "flight_time",
        "flight_number",
        "destination_text",
        "baggage",
    ],
    "tab": [
        "PaxName",
        "PaxBirthDate",
        "DepartDate",
        "DepartTime",
        "ArrivalDate",
        "ArrivalTime",
        "FlightCode",
        "From",
        "Dest",
        "e-Ticket",
        "TravelDoc",
        "FrequentFlyer",
    ],
    "excel": [
        "passenger_name",
        "ticket_number",
        "departure_date",
        "departure_time",
        "flight_number",
        "origin_airport",
        "destination_airport",
        "booking_reference",
        "loyalty_card",
    ],
    "json_profiles": ["metadata_json", "flight_rows", "loyalty_rows"],
    "json_cards": ["profile_record_id", "Status", "programm", "Number"],
    "json_flights": [
        "profile_record_id",
        "Date",
        "Flight",
        "Departure_Airport",
        "Arrival_Airport",
        "Codeshare",
    ],
    "xml_users": ["user_id", "user_first_name", "user_last_name"],
    "xml_cards": ["user_record_id", "card_program", "card_number"],
    "xml_activities": [
        "user_record_id",
        "card_record_id",
        "activity_type",
        "flight_date",
        "flight_number",
        "departure_iata",
        "arrival_iata",
    ],
    "yaml_flights": ["date", "flight_number", "origin", "destination", "status"],
    "yaml_entries": [
        "date",
        "flight_number",
        "loyalty_program",
        "loyalty_number",
        "travel_class",
        "fare",
    ],
    "pdf": [
        "origin_code",
        "destination_code",
        "valid_from",
        "valid_to",
        "days_codes",
        "departure_local",
        "arrival_local",
        "flight_number",
        "aircraft_code",
        "arrival_day_offset",
    ],
}


def quoted(column):
    return '"' + column.replace('"', '""') + '"'


def profile_tables(db, inventory, output):
    quality, duplicates = [], []
    for table in inventory:
        label, total = table["table"], table["rows"]
        columns = table["fields"]
        for field in columns:
            column = quoted(field["name"])
            huge = any(
                word in field["name"]
                for word in ["json", "xml", "yaml", "bytes_b64", "raw_line", "raw_text"]
            )
            check_marker = "string" in field["type"] and not huge
            marker_sql = (
                f"count(*) FILTER(WHERE upper(trim({column})) IN ('N/A','NA','NULL','NONE','NAN','-'))"
                if check_marker
                else "NULL"
            )
            empty_sql = (
                f"count(*) FILTER(WHERE trim({column})='')" if check_marker else "NULL"
            )
            missing, empty, markers = db.execute(
                f"SELECT count(*) FILTER(WHERE {column} IS NULL),{empty_sql},{marker_sql} FROM src_{label}"
            ).fetchone()
            quality.append(
                {
                    "table": label,
                    "field": field["name"],
                    "type": field["type"],
                    "unit": table["unit"],
                    "rows": total,
                    "nulls": missing,
                    "empty_strings": empty,
                    "literal_marker_like_strings": markers,
                    "marker_note": "literal counts only; e.g. baggage None can be a category",
                    "processing_rule": VERSION,
                }
            )
        keys = ",".join(quoted(c) for c in PAYLOAD[label])
        unique = db.execute(
            f"SELECT count(DISTINCT ({keys})) FROM src_{label}"
        ).fetchone()[0]
        duplicates.append(
            {
                "table": label,
                "unit": table["unit"],
                "rows": total,
                "distinct_selected_payloads": unique,
                "duplicate_excess": total - unique,
                "fields_json": json.dumps(PAYLOAD[label]),
                "interpretation": "Repeated selected payload, not proven duplicate trip or person",
                "processing_rule": VERSION,
            }
        )
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(
        pa.Table.from_pylist(quality),
        output / "field_quality.parquet",
        compression="zstd",
    )
    pq.write_table(
        pa.Table.from_pylist(duplicates),
        output / "selected_payload_duplicates.parquet",
        compression="zstd",
    )
    dump(output / "selected_payload_duplicates.json", duplicates)
    return duplicates


def aggregate(db, output):
    queries = {
        "source_coverage": """SELECT source,unit,count(*) AS "rows",count(date) dated_rows,
          min(date) date_min,max(date) date_max,count(*) FILTER(WHERE valid_airport_pair) known_airport_pair_rows,
          count(*) FILTER(WHERE airport_marker_ambiguity) airport_marker_like_rows,
          count(distinct duplicate_key) distinct_selected_observations,
          count(distinct entity_id) source_entity_references FROM observations GROUP BY source,unit""",
        "daily_counts": """SELECT source,unit,date,count(*) observations,
          count(distinct duplicate_key) selected_payloads FROM observations WHERE date IS NOT NULL GROUP BY source,unit,date
          UNION ALL SELECT 'yaml_flights','source date-flight mapping',date,count(*),count(*)
          FROM src_yaml_flights WHERE date IS NOT NULL GROUP BY date""",
        "routes": """SELECT source,unit,origin,destination,count(*) observations,count(distinct duplicate_key) selected_payloads,
          count(distinct date) distinct_dates,count(distinct strftime(date,'%Y-%m')) distinct_months
          FROM observations WHERE valid_airport_pair GROUP BY source,unit,origin,destination
          UNION ALL SELECT 'yaml_flights','source date-flight mapping',origin,destination,count(*),
          count(*),count(distinct date),count(distinct strftime(date,'%Y-%m')) FROM src_yaml_flights
          WHERE regexp_full_match(coalesce(origin,''),'[A-Z]{3}') AND regexp_full_match(coalesce(destination,''),'[A-Z]{3}')
          GROUP BY origin,destination
          UNION ALL SELECT 'pdf','recurring schedule rule',origin_code,destination_code,count(*),
          count(distinct (origin_code,destination_code,valid_from,valid_to,days_codes,departure_local,arrival_local,flight_number,aircraft_code,arrival_day_offset)),
          NULL,NULL FROM src_pdf
          WHERE regexp_full_match(coalesce(origin_code,''),'[A-Z]{3}') AND regexp_full_match(coalesce(destination_code,''),'[A-Z]{3}')
          GROUP BY origin_code,destination_code""",
        "route_day_support": """SELECT date,origin,destination,count(distinct source) sources,
          string_agg(distinct source,',' ORDER BY source) source_names,
          count(*) observations FROM observations WHERE valid_airport_pair AND date IS NOT NULL
          GROUP BY date,origin,destination""",
        "pdf_coverage": """SELECT min(try_cast(valid_from AS DATE)) validity_min,max(try_cast(valid_to AS DATE)) validity_max,
          count(*) schedule_rules,count(*) FILTER(WHERE route_status='continued_same_column') inherited_routes,
          count(*) FILTER(WHERE field_issues IS NOT NULL AND field_issues NOT IN ('','[]')) rules_with_field_issues
          FROM src_pdf""",
    }
    for label, query in queries.items():
        db.execute(f"CREATE TABLE {label} AS {query}")
        export(
            db,
            f"SELECT *,{literal(VERSION)} processing_rule FROM {label}",
            output / f"{label}.parquet",
        )
    for label, query in {
        "monthly_counts": """SELECT source,unit,strftime(date,'%Y-%m') AS "month",sum(observations) observations,sum(selected_payloads) selected_payloads,count(*) observed_dates,sum(observations)/count(*) observations_per_observed_date FROM daily_counts GROUP BY 1,2,3""",
        "weekday_counts": "SELECT source,unit,isodow(date) weekday_iso,sum(observations) observations,sum(selected_payloads) selected_payloads,count(*) observed_dates,sum(observations)/count(*) observations_per_observed_date FROM daily_counts GROUP BY source,unit,weekday_iso",
        "airport_hubs": "SELECT source,unit,airport,sum(n) endpoint_observations,sum(u) endpoint_selected_payloads FROM (SELECT source,unit,origin airport,observations n,selected_payloads u FROM routes UNION ALL SELECT source,unit,destination,observations,selected_payloads FROM routes) GROUP BY source,unit,airport",
        "route_repeatability": """SELECT source,unit,count(*) routes_with_known_airports,
          count(*) FILTER(WHERE distinct_dates>=2) routes_on_multiple_dates,
          sum(observations) known_route_observations,
          sum(CASE WHEN distinct_dates>=2 THEN observations ELSE 0 END) observations_on_repeated_routes
          FROM routes WHERE source!='pdf' GROUP BY source,unit""",
    }.items():
        db.execute(f"CREATE TABLE {label} AS {query}")
        export(
            db,
            f"SELECT *,{literal(VERSION)} processing_rule FROM {label}",
            output / f"{label}.parquet",
        )
    db.execute("""CREATE TABLE ambiguous_route_keys AS SELECT date,flight,
      count(distinct (origin,destination)) known_routes,count(distinct source) sources,
      count(*) observations FROM observations WHERE valid_airport_pair AND date IS NOT NULL AND flight IS NOT NULL
      GROUP BY date,flight HAVING count(distinct (origin,destination))>1""")
    export(
        db,
        f"SELECT *,{literal(VERSION)} processing_rule FROM ambiguous_route_keys",
        output / "ambiguous_route_keys.parquet",
    )
    export(
        db,
        f"""SELECT o.source,o.unit,count(*) observations,
      count(*) FILTER(WHERE a.flight IS NOT NULL) observations_on_multiroute_dateflight,
      count(*) FILTER(WHERE a.flight IS NULL) observations_without_multiroute_flag,
      count(*) FILTER(WHERE a.flight IS NULL AND o.valid_airport_pair) known_route_observations_without_flag,
      {literal(VERSION)} processing_rule FROM observations o LEFT JOIN ambiguous_route_keys a
      ON o.date=a.date AND o.flight=a.flight GROUP BY o.source,o.unit""",
        output / "route_conflict_sensitivity.parquet",
    )
    export(
        db,
        f"""SELECT o.source,o.unit,o.origin,o.destination,count(*) observations,
      {literal(VERSION)} processing_rule FROM observations o ANTI JOIN ambiguous_route_keys a
      ON o.date=a.date AND o.flight=a.flight WHERE o.valid_airport_pair
      GROUP BY o.source,o.unit,o.origin,o.destination""",
        output / "routes_without_multiroute_keys.parquet",
    )
    export(
        db,
        f"""SELECT a.source source_a,b.source source_b,count(*) common_observed_dates,
      corr(a.observations,b.observations) daily_count_pearson,
      {literal(VERSION)} processing_rule FROM daily_counts a JOIN daily_counts b
      ON a.date=b.date AND a.source<b.source GROUP BY a.source,b.source""",
        output / "daily_source_similarity.parquet",
    )
    export(
        db,
        f"""SELECT source,unit,origin,destination,observations,selected_payloads,
      rank() OVER(PARTITION BY source ORDER BY observations DESC) raw_count_rank,
      rank() OVER(PARTITION BY source ORDER BY selected_payloads DESC) projection_count_rank,
      {literal(VERSION)} processing_rule FROM routes""",
        output / "route_rank_sensitivity.parquet",
    )
    coverage = (
        db.execute("SELECT * FROM source_coverage ORDER BY source")
        .fetchdf()
        .to_dict("records")
    )
    dump(output / "source_coverage.json", coverage)
    return coverage


def plot(db, output):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    directory = output / "figures"
    directory.mkdir(exist_ok=True)
    labels = {
        "excel": "Excel: лист талона",
        "csv": "CSV: запись о посадке",
        "tab": "TAB: запись агентства",
        "json": "JSON: наблюдение профиля",
        "xml": "XML: операция карты",
        "yaml": "YAML: элемент FF",
        "yaml_flights": "YAML: запись дата–рейс",
        "pdf": "PDF: правило расписания",
    }
    daily = db.execute("SELECT * FROM daily_counts ORDER BY source,date").fetchdf()
    fig, axes = plt.subplots(4, 2, figsize=(14, 13))
    for axis, (source, frame) in zip(axes.flat, daily.groupby("source", sort=True)):
        axis.plot(
            frame.date, frame.observations, label="Исходные наблюдения", linewidth=0.8
        )
        axis.plot(
            frame.date,
            frame.selected_payloads,
            label="Различные выбранные поля",
            linewidth=0.8,
            linestyle="--",
        )
        axis.set_title(labels[source])
        axis.set_ylabel("Записей в день")
        axis.tick_params(axis="x", rotation=30)
        axis.legend(fontsize=7)
    axes.flat[-1].axis("off")
    fig.suptitle("Покрытие источников; линии не являются числом совершённых перелётов")
    fig.tight_layout(rect=(0, 0.04, 1, 0.96))
    fig.text(
        0.02,
        0.01,
        "Дубли определяются выбранными полями. Состав источников и единицы наблюдения различаются.",
        fontsize=9,
    )
    fig.savefig(directory / "daily_coverage.png", dpi=150)
    plt.close(fig)
    fig, axes = plt.subplots(4, 2, figsize=(14, 14))
    for axis, source in zip(axes.flat, sorted(labels)):
        frame = db.execute(
            "SELECT origin||' → '||destination route,observations FROM routes WHERE source=? ORDER BY observations DESC,route LIMIT 10",
            [source],
        ).fetchdf()
        if frame.empty:
            axis.text(
                0.5,
                0.5,
                "В источнике нет пары кодов аэропортов",
                ha="center",
                va="center",
                transform=axis.transAxes,
                fontsize=10,
            )
            axis.set_title(labels[source])
            axis.set_axis_off()
            continue
        axis.barh(frame.route[::-1], frame.observations[::-1])
        axis.set_title(labels[source])
        axis.set_xlabel("Исходных записей с двумя кодами аэропортов")
    fig.suptitle("Чаще представленные направления отдельно по каждому источнику")
    fig.tight_layout(rect=(0, 0.04, 1, 0.96))
    fig.text(
        0.02,
        0.01,
        "CSV не содержит пары аэропортов. PDF — другое время и повторяющиеся правила, а не отдельные вылеты.",
        fontsize=9,
    )
    fig.savefig(directory / "top_routes.png", dpi=150)
    plt.close(fig)
    monthly = db.execute("SELECT * FROM monthly_counts ORDER BY source,month").fetchdf()
    fig, axis = plt.subplots(figsize=(12, 6))
    for source, frame in monthly.groupby("source"):
        values = frame.observations / frame.observations.sum()
        axis.plot(frame.month, values, marker=".", label=labels[source])
    axis.set_ylabel("Доля записей источника за весь доступный период")
    axis.set_xlabel("Месяц")
    axis.tick_params(axis="x", rotation=35)
    axis.legend(fontsize=8)
    axis.set_title(
        "Временной профиль покрытия; не оценка сезонности всех авиапассажиров"
    )
    fig.tight_layout()
    fig.savefig(directory / "monthly_shares.png", dpi=150)
    plt.close(fig)
