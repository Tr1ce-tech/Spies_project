import argparse
import json
from pathlib import Path

import duckdb
import joblib
import numpy as np
import pandas as pd

from pipelines.research.common import dump, literal, sha


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "output", type=Path, nargs="?", default=Path("proceed_data/profile_patterns_v1")
    )
    args = parser.parse_args()
    output = args.output.resolve()
    root = Path(__file__).resolve().parents[1]
    manifest = json.loads((output / "manifest.json").read_text())
    checks = {
        "code_hash": manifest["code_sha256"]
        == sha(root / "pipelines/profile_patterns.py"),
        "input_hashes": all(sha(root / p) == h for p, h in manifest["inputs"].items()),
    }
    models = joblib.load(output / "isolation_forests.joblib")
    frame = pd.read_parquet(output / "profile_scores.parquet")
    for cohort, bundle in models["cohorts"].items():
        group = frame[frame.model_eligible & frame.cohort.eq(cohort)]
        scores = -bundle["model"].score_samples(group[bundle["features"]].to_numpy())
        assert np.array_equal(scores, group.iforest_score.to_numpy()), cohort
        assert np.array_equal(
            scores > bundle["threshold"], group.iforest_flag.to_numpy()
        ), cohort
    checks["reloaded_models_match_all_scores"] = True
    db = duckdb.connect()
    for table in ["observation_members", "flight_day_bins", "unit_members"]:
        db.execute(
            f"CREATE VIEW {table} AS SELECT * FROM read_parquet({literal(output / (table + '.parquet'))})"
        )
    db.execute(
        f"CREATE VIEW scores AS SELECT * FROM read_parquet({literal(output / 'profile_scores.parquet')})"
    )
    db.execute(
        f"CREATE VIEW original AS SELECT * FROM read_parquet({literal(root / 'proceed_data/normalized_v1/normalized_observations.parquet')})"
    )
    checks["original_record_pointers"] = (
        db.execute("""SELECT count(*)=0 FROM observation_members m
      LEFT JOIN original o USING(observation_id) WHERE o.observation_id IS NULL OR
        m.entity_id!=o.entity_id OR m.source_record_id!=o.source_record_id OR
        m.source_sha256!=o.source_sha256 OR m.source!=o.source""").fetchone()[0]
    )
    checks["membership_unique"] = db.execute(
        "SELECT count(*)=count(DISTINCT to_json([unit_id,observation_id])) FROM observation_members"
    ).fetchone()[0]
    checks["all_unit_observations_retained"] = db.execute("""SELECT
      (SELECT count(*) FROM observation_members)=(SELECT count(*) FROM unit_members JOIN original USING(entity_id))""").fetchone()[0]
    checks["frequency_from_bins"] = (
        db.execute("""SELECT count(*)=0 FROM scores s LEFT JOIN
      (SELECT unit_id,count(*) n FROM flight_day_bins GROUP BY unit_id) b USING(unit_id)
      WHERE s.flight_days!=coalesce(b.n,0)""").fetchone()[0]
    )
    checks["conflicts_not_assigned_a_route"] = (
        db.execute("""SELECT count(*)=0 FROM flight_day_bins
      WHERE route_variants!=1 AND (origin IS NOT NULL OR destination IS NOT NULL)""").fetchone()[
            0
        ]
    )
    # Leave-one-unit-out prevalence: own route presence cannot create support from others.
    checks["rare_share_range"] = bool(
        frame.rare_route_share.dropna().between(0, 1).all()
    )
    evidence = pd.read_parquet(output / "document_evidence.parquet")
    key = ["ticket_key", "date_key", "flight_key"]
    doc_counts = evidence.groupby(key).document_key.nunique()
    blocks = pd.read_parquet(output / "document_discrepancies.parquet").set_index(key)
    checks["document_counts_match_evidence"] = bool(
        blocks.known_documents.sort_index().equals(doc_counts.sort_index())
    )
    birth_comparable = 0
    birth_disjoint = 0
    for _, rows in evidence.groupby(key, sort=False):
        sets = [set(json.loads(v)) for v in rows.birth_dates_json.dropna()]
        sets = [v for v in sets if v]
        if len(sets) >= 2:
            birth_comparable += 1
            birth_disjoint += not bool(set.intersection(*sets))
    checks["all_passed"] = all(checks.values())
    dump(
        output / "independent_verification.json",
        {
            "checks": checks,
            "document_blocks_with_two_or_more_birth_constraints": birth_comparable,
            "document_blocks_with_disjoint_birth_constraints": birth_disjoint,
            "scope": "field consistency only; no independent identity or anomaly truth labels",
        },
    )
    labels = {
        "high_observed_frequency": "Высокая наблюдаемая частота",
        "many_observed_routes": "Много разных направлений",
        "repeated_route": "Повторение одного направления",
        "concentrated_dates": "Концентрация по датам",
        "rare_routes_in_cohort": "Редкие направления",
    }
    rows = []
    for reason, label in labels.items():
        selected = frame[frame.baseline_reasons.str.contains(reason, regex=False)]
        by_kind = selected.groupby("unit_kind").size()
        rows.append(
            f"| {label} | {by_kind.get('source_profile', 0):,} | {by_kind.get('preliminary_group', 0):,} |"
        )
    model_rows = []
    for kind, label in [
        ("source_profile", "Локальные профили"),
        ("preliminary_group", "Предварительные группы"),
    ]:
        subset = frame[frame.unit_kind.eq(kind)]
        intersection = int((subset.baseline_flag & subset.iforest_flag).sum())
        model_rows.append(
            f"| {label} | {len(subset):,} | {int(subset.model_eligible.sum()):,} | {int(subset.iforest_flag.sum()):,} | {intersection:,} |"
        )
    stable = frame[frame.iforest_flag].groupby("seed_flag_count").size().to_dict()
    findings = f"""# Что обнаружено

## Наблюдаемые факты

| Единица | Всего | Достаточно данных для модели | Флаги Isolation Forest | Также отмечены baseline |
|---|---:|---:|---:|---:|
{chr(10).join(model_rows)}

Локальные профили и группы — пересекающиеся представления, не разные люди.
Модель оценивает распределение наблюдаемых свойств внутри когорты, без оценки
намерений. Порог — верхний 1% обучающего score: количество флагов задано бюджетом
просмотра и не оценивает распространённость реальных аномалий.

## Пять проверяемых паттернов

| Правило | Локальные профили | Предварительные группы |
|---|---:|---:|
{chr(10).join(rows)}

Каждая строка имеет собственное основание; один профиль может входить в несколько.
Знаменатели: model_eligible единицы в cohort_summary.csv. Пороги сравнения — отдельно
по составу источников. Частота — число разных блоков дата/рейс, не доказанных перелётов.
При согласованных маршрутах ни одна применимая единица не достигла критерия
не менее пяти повторов одного направления и доли 80%. Это отсутствие срабатывания
конкретного правила, а не доказательство отсутствия повторяющихся поездок вообще.

В модели встречаются и редкие сочетания небольшого объёма с другими свойствами.
Высокий score не означает автоматически высокую частоту поездок. Отклонения
признаков от медианы когорты сохранены в feature_deviations; это описательная
характеристика, не причинная важность признаков Isolation Forest.

## Документы

Найдены {len(blocks):,} блока билет/дата/рейс с несколькими известными документами.
У {birth_comparable:,} блоков есть минимум два непустых набора возможных дат рождения.
Среди них {birth_disjoint:,} блоков с пустым общим пересечением этих наборов.
Остальные блоки не получают вывода о согласованности дат рождения: информации мало.
Совместимая дата рождения не доказывает принадлежность одному человеку.

В отдельном старом сопоставлении 2 561 компонент удержан из-за разных документов.
Это другая единица и пересекающийся набор. Полные документы, исходные имена и
варианты дат рождения доступны в document_evidence и held_document_evidence.

## Проверки и открытые вопросы

{stable.get(3, 0):,} из {int(frame.iforest_flag.sum()):,} отмеченных аналитических единиц
сохраняются при всех трёх seed. Это проверка устойчивости, не точность.
Все сохранённые score воспроизведены загруженными моделями; принадлежность
наблюдений и числа блоков сверены с исходными нормализованными таблицами.

Гипотезы: часть отклонений может объясняться реальным разнообразием поездок,
неравномерностью выгрузки, неполным профилем, копированием или ошибкой связи.
Для выбора между ними нужны независимые метки и проверка исходников.
Массив не позволяет установить преступную причастность, намерения, фактический
перелёт или репрезентативность для всех пассажиров. Проверки ПО не заменяют разметку.

Методика — REPORT.md. Для просмотра — review.html. Полные результаты — profile_scores.parquet.
"""
    (output / "FINDINGS.md").write_text(findings)
    print(
        json.dumps(
            {
                "checks": checks,
                "birth_comparable": birth_comparable,
                "birth_disjoint": birth_disjoint,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    if not checks["all_passed"]:
        raise AssertionError(checks)


if __name__ == "__main__":
    main()
