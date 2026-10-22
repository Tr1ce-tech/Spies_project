import json
from pathlib import Path

import duckdb

from pipelines.research.common import dump, export, literal, sha

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "proceed_data/anomaly_workbook_v1"
VERSION = "anomaly-workbook.v1"


def main():
    OUT.mkdir(exist_ok=False)
    db = duckdb.connect()
    db.execute("SET memory_limit='2GB'")
    db.execute("SET threads=4")
    inputs = {
        "o": ROOT / "proceed_data/normalized_v1/normalized_observations.parquet",
        "e": ROOT / "proceed_data/normalized_v1/source_entities.parquet",
        "g": ROOT / "proceed_data/research_v1/preliminary_groups.parquet",
    }
    for label, path in inputs.items():
        db.execute(
            f"CREATE VIEW {label} AS SELECT * FROM read_parquet({literal(path)})"
        )
    db.execute("""CREATE TABLE daily AS WITH counts AS (
      SELECT source,unit,date_key date,count(*) n FROM o GROUP BY ALL),
      baseline AS (SELECT source,isodow(date) weekday,median(n) med FROM counts GROUP BY ALL),
      deviations AS (SELECT c.*,b.weekday,b.med,abs(n-med) deviation FROM counts c JOIN baseline b
        ON c.source=b.source AND isodow(c.date)=b.weekday),
      dispersion AS (SELECT source,weekday,median(deviation) mad FROM deviations GROUP BY ALL)
      SELECT d.*,s.mad,CASE WHEN s.mad>0 THEN (d.n-d.med)/(1.4826*s.mad) END robust_z
      FROM deviations d JOIN dispersion s USING(source,weekday)""")
    db.execute("""CREATE TABLE trips AS SELECT ticket_key,date_key,flight_key,
      count(*) observations,count(DISTINCT o.source) sources,
      string_agg(DISTINCT o.source,', ' ORDER BY o.source) source_list,
      min(origin_key) FILTER(WHERE origin_key IS NOT NULL) origin,
      min(destination_key) FILTER(WHERE destination_key IS NOT NULL) destination,
      count(DISTINCT to_json([origin_key,destination_key])) FILTER(
        WHERE origin_key IS NOT NULL AND destination_key IS NOT NULL) route_variants,
      count(DISTINCT local_time_key) time_variants,
      count(DISTINCT e.document_key) document_variants,
      count(DISTINCT o.entity_id) source_entities
      FROM o LEFT JOIN e USING(entity_id) WHERE ticket_key IS NOT NULL AND
        date_key IS NOT NULL AND flight_key IS NOT NULL GROUP BY ticket_key,date_key,flight_key
        HAVING count(DISTINCT o.source)>=2""")
    # Singleton route fields are exposed only when the whole block agrees.
    db.execute("""CREATE VIEW trip_view AS SELECT ticket_key,date_key,flight_key,observations,sources,source_list,
      CASE WHEN route_variants=1 THEN origin END origin,
      CASE WHEN route_variants=1 THEN destination END destination,
      route_variants,time_variants,document_variants,source_entities FROM trips""")
    db.execute("""CREATE TABLE routes AS SELECT source,unit,origin_key origin,destination_key destination,
      count(*) observations,count(DISTINCT date_key) dates,
      count(DISTINCT date_trunc('month',date_key)) month_count,
      count(DISTINCT flight_key) flight_codes FROM o WHERE origin_key IS NOT NULL
      AND destination_key IS NOT NULL GROUP BY ALL""")
    db.execute("""CREATE TABLE route_conflicts AS SELECT date_key,flight_key,count(*) observations,
      count(DISTINCT to_json([origin_key,destination_key])) route_variants,
      count(DISTINCT source) sources,
      string_agg(DISTINCT origin_key||' → '||destination_key, '; ' ORDER BY origin_key||' → '||destination_key) routes
      FROM o WHERE origin_key IS NOT NULL AND destination_key IS NOT NULL AND date_key IS NOT NULL
        AND flight_key IS NOT NULL GROUP BY date_key,flight_key HAVING route_variants>1""")
    specs = []

    def add(
        name, query, headers, note, limit=None, dates=(), text=(), excel_query=None
    ):
        table = "sheet_" + str(len(specs))
        db.execute(f"CREATE TABLE {table} AS {query}")
        total = db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        export(db, f"SELECT * FROM {table}", OUT / f"{table}.parquet")
        selected = (
            excel_query.replace("{table}", table)
            if excel_query
            else f"SELECT * FROM {table}"
        )
        cursor = db.execute(selected + (f" LIMIT {limit}" if limit else ""))
        rows = cursor.fetchall()
        assert len(headers) == len(cursor.description)
        specs.append(
            dict(
                name=name,
                headers=headers,
                rows=rows,
                note=note,
                total_rows=total,
                exported_rows=len(rows),
                date_cols=dates,
                text_cols=text,
                parquet=f"proceed_data/anomaly_workbook_v1/{table}.parquet",
            )
        )

    add(
        "Источники",
        """SELECT source,min(unit),count(*),min(date_key),max(date_key),
      count(*) FILTER(WHERE origin_key IS NOT NULL AND destination_key IS NOT NULL),
      count(*) FILTER(WHERE ticket_key IS NOT NULL),count(*) FILTER(WHERE local_time_key IS NOT NULL),
      count(*) FILTER(WHERE airport_marker_ambiguity) FROM o GROUP BY source ORDER BY source""",
        [
            "Источник",
            "Единица записи",
            "Наблюдения",
            "Первая дата",
            "Последняя дата",
            "Есть маршрут",
            "Есть билет",
            "Есть время",
            "Маркер аэропорта",
        ],
        "Полный массив. Единицы источников различаются. Коды аэропортов проверены только по форме.",
        dates=(3, 4),
    )
    add(
        "По месяцам",
        """SELECT source,cast(date_trunc('month',date_key) AS DATE),count(*),count(DISTINCT date_key),
      count(*)::DOUBLE/sum(count(*)) OVER(PARTITION BY source) FROM o GROUP BY source,date_trunc('month',date_key) ORDER BY source,2""",
        ["Источник", "Месяц", "Наблюдения", "Дат с записями", "Доля источника"],
        "Все строки, без дедупликации. Неполные месяцы нельзя сравнивать напрямую.",
        dates=(1,),
    )
    add(
        "По дням",
        """SELECT source,date,n,weekday,med,mad,robust_z FROM daily ORDER BY source,date""",
        [
            "Источник",
            "Дата",
            "Наблюдения",
            "День ISO",
            "Медиана этого дня недели",
            "MAD",
            "Отклонение robust z",
        ],
        "Baseline: все даты того же дня недели внутри источника. Диагностика покрытия, не прогноз и не вероятность.",
        dates=(1,),
    )
    add(
        "Маршруты",
        "SELECT * FROM routes ORDER BY source,observations DESC,origin,destination",
        [
            "Источник",
            "Единица записи",
            "Откуда",
            "Куда",
            "Наблюдения",
            "Разных дат",
            "Разных месяцев",
            "Кодов рейса",
        ],
        "До 1000 направлений каждого источника по объёму. Полный список в Parquet. Повторяемость по датам не означает поездки одного человека.",
        excel_query="SELECT * FROM {table} QUALIFY row_number() OVER(PARTITION BY source ORDER BY observations DESC,origin,destination)<=1000 ORDER BY source,observations DESC,origin,destination",
    )
    add(
        "Общие поездки",
        "SELECT * FROM trip_view ORDER BY document_variants DESC,sources DESC,date_key,flight_key,ticket_key",
        [
            "Билет",
            "Дата",
            "Рейс",
            "Наблюдения",
            "Источников",
            "Источники",
            "Откуда",
            "Куда",
            "Вариантов маршрута",
            "Вариантов времени",
            "Разных документов",
            "Сущностей источников",
        ],
        "Первые 5000: разные документы, затем число источников, дата/рейс/билет. Все блоки в Parquet. Кандидаты, не подтверждённые люди или перелёты.",
        limit=5000,
        dates=(1,),
        text=(0,),
    )
    add(
        "Сигналы по датам",
        """SELECT source,date,n,weekday,med,mad,robust_z FROM daily
      WHERE abs(robust_z)>3.5 ORDER BY abs(robust_z) DESC,source,date""",
        [
            "Источник",
            "Дата",
            "Наблюдения",
            "День ISO",
            "Медиана этого дня недели",
            "MAD",
            "Отклонение robust z",
        ],
        "Фиксированный диагностический порог |robust z| > 3.5. Зависимость дат и перебор тестов не учтены; статистическая значимость не заявляется.",
        dates=(1,),
    )
    add(
        "Несколько маршрутов",
        "SELECT * FROM route_conflicts ORDER BY route_variants DESC,observations DESC,date_key,flight_key",
        ["Дата", "Рейс", "Наблюдения", "Вариантов маршрута", "Источников", "Маршруты"],
        "Первые 2000 по числу маршрутов и объёму. Многоучастковый рейс и codeshare — возможные объяснения; полный список в Parquet.",
        limit=2000,
        dates=(0,),
    )
    add(
        "Одинаковые аэропорты",
        """SELECT source,date_key,flight_key,origin_key,count(*) FROM o
      WHERE origin_key=destination_key GROUP BY ALL ORDER BY count(*) DESC,source,date_key,flight_key""",
        ["Источник", "Дата", "Рейс", "Аэропорт", "Наблюдения"],
        "Первые 2000 сгруппированных случаев по объёму. Совпадение кодов требует проверки исходников, не доказывает невозможное перемещение.",
        limit=2000,
        dates=(1,),
    )
    add(
        "Повторы записей",
        """SELECT source,duplicate_key,count(*) n,min(observation_id) example_id
      FROM o GROUP BY source,duplicate_key HAVING n>1 ORDER BY n DESC,source,duplicate_key""",
        ["Источник", "Проекция записи JSON", "Число повторов", "Пример observation_id"],
        "Первые 2000 повторов выбранной проекции. Поля проекции различаются по источникам, строки не удалялись.",
        limit=2000,
    )
    add(
        "Качество полей",
        """SELECT source,'flight' field,flight_status status,count(*) FROM o GROUP BY ALL
      UNION ALL SELECT source,'date',date_status,count(*) FROM o GROUP BY ALL
      UNION ALL SELECT source,'route',route_status,count(*) FROM o GROUP BY ALL
      UNION ALL SELECT source,'ticket',ticket_status,count(*) FROM o GROUP BY ALL
      UNION ALL SELECT source,'local_time',local_time_status,count(*) FROM o GROUP BY ALL ORDER BY 1,2,3""",
        ["Источник", "Поле", "Статус", "Наблюдения"],
        "Статусы нормализации. Синтаксически допустимое значение ещё не подтверждено внешним справочником.",
    )
    add(
        "Конфликты профилей",
        """SELECT candidate_component_id,entities,sources,known_documents,
      known_birth_entities,reasons_json,status FROM g WHERE status='held_for_review'
      ORDER BY entities DESC,candidate_component_id""",
        [
            "Компонент",
            "Сущностей",
            "Источников",
            "Разных документов",
            "С датой рождения",
            "Причины",
            "Статус",
        ],
        "Первые 2000 крупнейших компонентов прежнего сопоставления. Новые связи по билетам в эти группы не включались.",
        limit=2000,
    )
    add(
        "Документы в блоках",
        "SELECT * FROM trip_view WHERE document_variants>1 ORDER BY document_variants DESC,date_key,flight_key,ticket_key",
        [
            "Билет",
            "Дата",
            "Рейс",
            "Наблюдения",
            "Источников",
            "Источники",
            "Откуда",
            "Куда",
            "Вариантов маршрута",
            "Вариантов времени",
            "Разных документов",
            "Сущностей источников",
        ],
        "Все блоки с несколькими известными документами. Разные документы могут принадлежать одному человеку; автоматического объединения нет.",
        dates=(1,),
        text=(0,),
    )
    checks = {
        "source_total": db.execute("SELECT count(*) FROM o").fetchone()[0],
        "daily_total": db.execute("SELECT sum(n) FROM daily").fetchone()[0],
        "trip_blocks": db.execute("SELECT count(*) FROM trips").fetchone()[0],
        "trip_members": db.execute("SELECT sum(observations) FROM trips").fetchone()[0],
        "route_rows": db.execute("SELECT count(*) FROM routes").fetchone()[0],
        "route_observations": db.execute(
            "SELECT sum(observations) FROM routes"
        ).fetchone()[0],
        "known_route_observations": db.execute(
            "SELECT count(*) FROM o WHERE origin_key IS NOT NULL AND destination_key IS NOT NULL"
        ).fetchone()[0],
    }
    assert checks["source_total"] == checks["daily_total"]
    assert checks["route_observations"] == checks["known_route_observations"]
    checks["pdf_excluded"] = True
    checks["all_passed"] = True
    dump(OUT / "verification.json", checks)
    dump(OUT / "workbook_data.json", dict(sheets=specs, checks=checks, version=VERSION))
    dump(
        OUT / "manifest.json",
        dict(
            version=VERSION,
            code_sha256=sha(Path(__file__)),
            inputs={str(p.relative_to(ROOT)): sha(p) for p in inputs.values()},
        ),
    )
    print(
        json.dumps(
            {
                "checks": checks,
                "sheets": [
                    (s["name"], s["exported_rows"], s["total_rows"]) for s in specs
                ],
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
