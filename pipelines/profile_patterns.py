import argparse
import json
import platform
import time
from pathlib import Path

import duckdb
import joblib
import numpy as np
import sklearn
from sklearn.ensemble import IsolationForest

from pipelines.research.common import dump, export, literal, sha

VERSION = "profile-patterns.v1"
SEEDS = (42, 43, 44)
MIN_COHORT = 200
FEATURES = [
    "log_flight_days",
    "log_routes",
    "top_route_share",
    "peak7_share",
    "max_month_share",
    "route_rarity",
]


def build_tables(db):
    db.execute("""CREATE TABLE unit_members AS
      SELECT 'profile:'||e.entity_id unit_id,e.entity_id,'source_profile' unit_kind,
        e.source profile_source,a.status linkage_status
      FROM entities e JOIN assignments a USING(entity_id)
      WHERE e.entity_kind IN ('native_profile','native_user')
      UNION ALL
      SELECT 'group:'||a.provisional_group_id,a.entity_id,'preliminary_group',NULL,a.status
      FROM assignments a WHERE a.status='provisional_consistent'
        AND a.provisional_group_id IS NOT NULL""")
    db.execute("""CREATE TABLE units AS SELECT unit_id,unit_kind,
      coalesce(min(profile_source),string_agg(DISTINCT e.source,'+' ORDER BY e.source)) source_signature,
      count(*) entity_members, min(linkage_status) linkage_status
      FROM unit_members m JOIN entities e USING(entity_id) GROUP BY unit_id,unit_kind""")
    db.execute("""CREATE TABLE unit_observations AS SELECT m.unit_id,o.*,
      CASE WHEN date_key IS NOT NULL AND flight_key IS NOT NULL THEN
        sha256(to_json([m.unit_id,cast(date_key AS VARCHAR),flight_key])) END slot_id
      FROM observations o JOIN unit_members m USING(entity_id)""")
    db.execute("""CREATE TABLE slots AS SELECT unit_id,slot_id,date_key,flight_key,
      count(*) raw_observations,count(DISTINCT source) sources,
      count(DISTINCT to_json([origin_key,destination_key])) FILTER(
        WHERE origin_key IS NOT NULL AND destination_key IS NOT NULL) route_variants,
      CASE WHEN route_variants=1 THEN min(origin_key) FILTER(
        WHERE origin_key IS NOT NULL AND destination_key IS NOT NULL) END origin,
      CASE WHEN route_variants=1 THEN min(destination_key) FILTER(
        WHERE origin_key IS NOT NULL AND destination_key IS NOT NULL) END destination,
      count(DISTINCT local_time_key) known_time_variants
      FROM unit_observations WHERE slot_id IS NOT NULL GROUP BY unit_id,slot_id,date_key,flight_key""")
    db.execute("""CREATE TABLE unit_routes AS SELECT unit_id,origin,destination,
      count(*) flight_days,count(DISTINCT date_key) dates
      FROM slots WHERE route_variants=1 GROUP BY ALL""")
    db.execute("""CREATE TABLE pattern_features AS WITH counts AS (
      SELECT unit_id,count(*) flight_days,count(DISTINCT date_key) active_dates,
        min(date_key) first_date,max(date_key) last_date,sum(raw_observations) raw_observations,
        count(*) FILTER(WHERE route_variants=1) routed_flight_days,
        count(*) FILTER(WHERE route_variants>1) route_conflict_flight_days,
        count(*) FILTER(WHERE known_time_variants>1) different_known_times
      FROM slots GROUP BY unit_id),
      by_day AS (SELECT unit_id,date_key,count(*) n FROM slots GROUP BY ALL),
      rolling AS (SELECT unit_id,sum(n) OVER(PARTITION BY unit_id ORDER BY date_key
        RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW) peak FROM by_day),
      peaks AS (SELECT unit_id,max(peak) peak7_flight_days FROM rolling GROUP BY unit_id),
      by_month AS (SELECT unit_id,date_trunc('month',date_key) period_month,count(*) n FROM slots GROUP BY ALL),
      months AS (SELECT unit_id,max(n) max_month_flight_days FROM by_month GROUP BY unit_id),
      routes AS (SELECT unit_id,count(*) distinct_routes,max(flight_days) top_route_flight_days
        FROM unit_routes GROUP BY unit_id)
      SELECT u.*,coalesce(c.flight_days,0) flight_days,coalesce(c.active_dates,0) active_dates,
        c.first_date,c.last_date,coalesce(c.raw_observations,0) raw_observations,
        coalesce(c.routed_flight_days,0) routed_flight_days,
        coalesce(c.route_conflict_flight_days,0) route_conflict_flight_days,
        coalesce(c.different_known_times,0) different_known_times,
        coalesce(r.distinct_routes,0) distinct_routes,coalesce(r.top_route_flight_days,0) top_route_flight_days,
        coalesce(p.peak7_flight_days,0) peak7_flight_days,coalesce(m.max_month_flight_days,0) max_month_flight_days,
        c.routed_flight_days::DOUBLE/nullif(c.flight_days,0) route_coverage,
        r.top_route_flight_days::DOUBLE/nullif(c.routed_flight_days,0) top_route_share,
        p.peak7_flight_days::DOUBLE/nullif(c.flight_days,0) peak7_share,
        m.max_month_flight_days::DOUBLE/nullif(c.flight_days,0) max_month_share,
        u.unit_kind||':'||u.source_signature cohort,
        coalesce(c.flight_days>=5 AND c.active_dates>=3 AND
          c.routed_flight_days::DOUBLE/c.flight_days>=0.8,false) eligible
      FROM units u LEFT JOIN counts c USING(unit_id) LEFT JOIN peaks p USING(unit_id)
      LEFT JOIN months m USING(unit_id) LEFT JOIN routes r USING(unit_id)""")
    db.execute("""CREATE TABLE cohort_routes AS SELECT f.cohort,r.origin,r.destination,
      count(*) units_with_route FROM unit_routes r JOIN pattern_features f USING(unit_id)
      WHERE f.eligible GROUP BY ALL""")
    db.execute("""CREATE TABLE route_rarity AS WITH cohorts AS (
      SELECT cohort,count(*) eligible_units FROM pattern_features WHERE eligible GROUP BY cohort)
      SELECT f.unit_id,sum(r.flight_days * -ln(c.units_with_route::DOUBLE/(n.eligible_units+1)))/sum(r.flight_days) route_rarity,
        sum(r.flight_days) FILTER(WHERE (c.units_with_route-1)::DOUBLE/nullif(n.eligible_units-1,0)<0.01)::DOUBLE/
          sum(r.flight_days) rare_route_share,n.eligible_units
      FROM pattern_features f JOIN unit_routes r USING(unit_id)
      JOIN cohort_routes c ON f.cohort=c.cohort AND r.origin=c.origin AND r.destination=c.destination
      JOIN cohorts n ON f.cohort=n.cohort WHERE f.eligible GROUP BY f.unit_id,n.eligible_units""")
    db.execute("""CREATE TABLE all_features AS SELECT f.*,r.route_rarity,
      CASE WHEN f.eligible THEN coalesce(r.rare_route_share,0) END rare_route_share,r.eligible_units,
      coalesce(f.eligible AND r.eligible_units>=200,false) model_eligible,
      ln(1+f.flight_days) log_flight_days,ln(1+f.distinct_routes) log_routes
      FROM pattern_features f LEFT JOIN route_rarity r USING(unit_id)""")


def baseline_flags(frame):
    result = frame.copy()
    result["baseline_reasons"] = ""
    thresholds = []
    for cohort, group in result[result.model_eligible].groupby("cohort", sort=True):
        cuts = group[
            ["flight_days", "distinct_routes", "peak7_share", "route_rarity"]
        ].quantile(0.99)
        thresholds.append({"cohort": cohort, "units": len(group), **cuts.to_dict()})
        for idx, row in group.iterrows():
            reasons = []
            if row.flight_days > cuts.flight_days:
                reasons.append("high_observed_frequency")
            if row.distinct_routes > cuts.distinct_routes:
                reasons.append("many_observed_routes")
            if row.top_route_share >= 0.8 and row.top_route_flight_days >= 5:
                reasons.append("repeated_route")
            if row.peak7_share > cuts.peak7_share and row.flight_days >= 10:
                reasons.append("concentrated_dates")
            if row.route_rarity > cuts.route_rarity and row.distinct_routes >= 3:
                reasons.append("rare_routes_in_cohort")
            result.at[idx, "baseline_reasons"] = json.dumps(reasons)
    return result, thresholds


def forest_scores(frame, output):
    result = frame.copy()
    result["iforest_score"] = np.nan
    result["iforest_flag"] = False
    result["seed_flag_count"] = 0
    result["feature_deviations"] = ""
    summaries = []
    models = {}
    reference = {}
    for cohort, group in result[result.model_eligible].groupby("cohort", sort=True):
        x = group[FEATURES].to_numpy(dtype=float)
        assert np.isfinite(x).all()
        quartiles = np.quantile(x, [0.25, 0.5, 0.75], axis=0)
        widths = quartiles[2] - quartiles[0]
        nonconstant = widths > 0
        deviations = np.zeros_like(x)
        deviations[:, nonconstant] = (
            x[:, nonconstant] - quartiles[1, nonconstant]
        ) / widths[nonconstant]
        explanations = []
        for values, delta in zip(x, deviations, strict=True):
            strongest = np.argsort(-np.abs(delta), kind="stable")[:2]
            explanations.append(
                json.dumps(
                    [
                        {
                            "feature": FEATURES[n],
                            "value": float(values[n]),
                            "cohort_median": float(quartiles[1, n]),
                            "signed_iqr_deviation": float(delta[n]),
                        }
                        for n in strongest
                        if nonconstant[n]
                    ],
                    ensure_ascii=False,
                )
            )
        result.loc[group.index, "feature_deviations"] = explanations
        reference[cohort] = {
            feature: {
                "q25": float(quartiles[0, n]),
                "median": float(quartiles[1, n]),
                "q75": float(quartiles[2, n]),
            }
            for n, feature in enumerate(FEATURES)
        }
        flags = []
        scores = []
        cuts = []
        for seed in SEEDS:
            model = IsolationForest(
                n_estimators=200,
                max_samples=min(256, len(x)),
                contamination="auto",
                random_state=seed,
                n_jobs=1,
            )
            model.fit(x)
            score = -model.score_samples(x)
            cut = float(np.quantile(score, 0.99))
            flags.append(score > cut)
            scores.append(score)
            cuts.append(cut)
            if seed == 42:
                models[cohort] = {
                    "model": model,
                    "threshold": cut,
                    "features": FEATURES,
                }
        result.loc[group.index, "iforest_score"] = scores[0]
        result.loc[group.index, "iforest_flag"] = flags[0]
        result.loc[group.index, "seed_flag_count"] = np.sum(flags, axis=0)
        agreement = []
        for n in (1, 2):
            union = np.logical_or(flags[0], flags[n]).sum()
            agreement.append(
                float(np.logical_and(flags[0], flags[n]).sum() / union)
                if union
                else None
            )
        summaries.append(
            {
                "cohort": cohort,
                "units": len(group),
                "threshold_seed42": cuts[0],
                "flags_seed42": int(flags[0].sum()),
                "jaccard_42_43": agreement[0],
                "jaccard_42_44": agreement[1],
            }
        )
    joblib.dump(
        {"version": VERSION, "cohorts": models},
        output / "isolation_forests.joblib",
        compress=3,
    )
    dump(output / "feature_reference.json", reference)
    return result, summaries


def document_tables(db, output):
    db.execute("""CREATE TABLE document_blocks AS SELECT ticket_key,date_key,flight_key,
      count(*) observations,count(DISTINCT o.source) sources,count(DISTINCT e.document_key) known_documents,
      list(DISTINCT e.document_key ORDER BY e.document_key) FILTER(WHERE e.document_key IS NOT NULL) document_keys,
      count(DISTINCT o.entity_id) source_entities
      FROM observations o LEFT JOIN entities e USING(entity_id)
      WHERE ticket_key IS NOT NULL AND date_key IS NOT NULL AND flight_key IS NOT NULL
      GROUP BY ticket_key,date_key,flight_key HAVING known_documents>1""")
    export(
        db,
        "SELECT *, 'review_not_proven_identity_error' status FROM document_blocks",
        output / "document_discrepancies.parquet",
    )
    export(
        db,
        """SELECT b.ticket_key,b.date_key,b.flight_key,o.observation_id,o.source,o.source_record_id,
      o.source_sha256,o.entity_id,e.document_raw,e.document_key,e.birth_dates_json,
      e.first_name,e.last_name,e.middle_name,e.full_name,o.origin_key,o.destination_key,o.local_time_key,
      'profile-patterns.v1' processing_rule
      FROM document_blocks b JOIN observations o USING(ticket_key,date_key,flight_key)
      LEFT JOIN entities e USING(entity_id)""",
        output / "document_evidence.parquet",
    )
    export(
        db,
        """SELECT g.* FROM groups g WHERE known_documents>1""",
        output / "held_document_components.parquet",
    )
    export(
        db,
        """SELECT a.candidate_component_id,e.* FROM assignments a JOIN entities e USING(entity_id)
      JOIN groups g ON a.candidate_component_id=g.candidate_component_id
      WHERE g.known_documents>1""",
        output / "held_document_evidence.parquet",
    )


def review_page(frame, document_rows, output):
    labels = {
        "high_observed_frequency": "Высокая наблюдаемая частота",
        "many_observed_routes": "Много направлений",
        "repeated_route": "Повторение маршрута",
        "concentrated_dates": "Концентрация по датам",
        "rare_routes_in_cohort": "Редкие направления",
    }
    fields = [
        "unit_id",
        "cohort",
        "flight_days",
        "distinct_routes",
        "top_route_share",
        "peak7_share",
        "rare_route_share",
        "iforest_score",
        "seed_flag_count",
        "feature_deviations",
    ]
    parts = [
        "<h1>Паттерны наблюдаемых поездок</h1><p>Профили источников и предварительные группы показаны отдельно. "
        "Это исторический поиск отклонений; оценок намерений и преступной причастности нет.</p>"
    ]
    parts.append(
        "<p>feature_deviations показывает два наиболее отличающихся признака относительно медианы когорты "
        "в единицах межквартильного размаха. Это описание данных, не причинное объяснение модели.</p>"
    )
    for key, label in labels.items():
        selected = frame[frame.baseline_reasons.str.contains(key, regex=False)]
        parts.append(
            f"<h2>{label}: {len(selected):,} единиц</h2><p>Первые 25, порядок: "
            "стабильность Isolation Forest, затем его score. Полный список — pattern_candidates.parquet.</p>"
        )
        rows = selected.sort_values(
            ["seed_flag_count", "iforest_score", "unit_id"],
            ascending=[False, False, True],
        ).head(25)
        parts.append(
            rows[fields].to_html(
                index=False, float_format=lambda x: f"{x:.3f}", escape=True
            )
        )
    parts.append(
        "<h2>Isolation Forest</h2><p>Первые 50 отмеченных единиц. Score не является вероятностью.</p>"
    )
    selected = (
        frame[frame.iforest_flag]
        .sort_values(
            ["seed_flag_count", "iforest_score", "unit_id"],
            ascending=[False, False, True],
        )
        .head(50)
    )
    parts.append(
        selected[fields].to_html(
            index=False, float_format=lambda x: f"{x:.3f}", escape=True
        )
    )
    parts.append(
        "<h2>Разные документы в одном блоке билета</h2><p>Первые 50 по числу документов. "
        "Значения документов доступны только в локальных Parquet с исходными наблюдениями.</p>"
    )
    parts.append(document_rows.to_html(index=False, escape=True))
    css = "body{font:14px system-ui;margin:28px;color:#213449}table{border-collapse:collapse;font-size:12px}th,td{padding:7px;border:1px solid #d6dde5}th{background:#e8eef6}h2{margin-top:35px}"
    (output / "review.html").write_text(
        "<!doctype html><html lang='ru'><meta charset='utf-8'><title>Паттерны поездок</title><style>"
        + css
        + "</style>"
        + "\n".join(parts)
        + "</html>"
    )


def plot_results(frame, output):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(2, 2, figsize=(13, 9), layout="constrained")
    eligible = frame[frame.model_eligible]
    group_cohorts = (
        eligible[eligible.unit_kind == "preliminary_group"]
        .cohort.value_counts()
        .head(2)
        .index.tolist()
    )
    selected = ["source_profile:json", "source_profile:xml", *group_cohorts]
    for axis, cohort in zip(axes.flat, selected, strict=False):
        rows = eligible[eligible.cohort == cohort].sort_values("unit_id")
        step = max(1, int(np.ceil(len(rows) / 6000)))
        normal = rows.iloc[::step]
        flagged = rows[rows.iforest_flag]
        axis.scatter(
            normal.flight_days,
            normal.distinct_routes,
            s=9,
            c="#9BAABC",
            alpha=0.3,
            label=f"Срез фона: {len(normal):,}",
        )
        axis.scatter(
            flagged.flight_days,
            flagged.distinct_routes,
            s=14,
            c="#C86C35",
            alpha=0.7,
            label=f"Флаги IF: {len(flagged):,}",
        )
        axis.set_title(
            cohort.replace("preliminary_group:", "Группа: ").replace(
                "source_profile:", "Профили: "
            )
            + f"\nn={len(rows):,}"
        )
        axis.set_xlabel("Разных блоков дата/рейс")
        axis.set_ylabel("Разных направлений")
        axis.legend(fontsize=8)
        axis.spines[["top", "right"]].set_visible(False)
    fig.suptitle("Наблюдаемые паттерны; флаги модели требуют проверки", fontsize=14)
    fig.savefig(output / "patterns.png", dpi=150)
    plt.close(fig)


def run(root, output):
    started = time.perf_counter()
    partial = output.with_name(output.name + ".partial")
    if output.exists() or partial.exists():
        raise FileExistsError("Choose a new output directory")
    partial.mkdir(parents=True)
    paths = {
        "observations": root
        / "proceed_data/normalized_v1/normalized_observations.parquet",
        "entities": root / "proceed_data/normalized_v1/source_entities.parquet",
        "assignments": root / "proceed_data/research_v1/entity_assignments.parquet",
        "groups": root / "proceed_data/research_v1/preliminary_groups.parquet",
    }
    hashes = {str(p.relative_to(root)): sha(p) for p in paths.values()}
    db = duckdb.connect()
    db.execute("SET memory_limit='2GB'")
    db.execute("SET threads=4")
    db.execute(f"SET temp_directory={literal(partial / 'temp')}")
    for name, path in paths.items():
        db.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet({literal(path)})")
    build_tables(db)
    export(
        db,
        "SELECT *, 'profile-patterns.v1' processing_rule FROM all_features",
        partial / "profile_features.parquet",
    )
    export(db, "SELECT * FROM unit_members", partial / "unit_members.parquet")
    export(db, "SELECT * FROM slots", partial / "flight_day_bins.parquet")
    export(db, "SELECT * FROM unit_routes", partial / "profile_routes.parquet")
    export(db, "SELECT * FROM cohort_routes", partial / "cohort_routes.parquet")
    export(
        db,
        """SELECT unit_id,slot_id,observation_id,entity_id,source,source_record_id,
      source_sha256,normalization_rule, 'profile-patterns.v1' processing_rule FROM unit_observations""",
        partial / "observation_members.parquet",
    )
    frame = db.execute("SELECT * FROM all_features ORDER BY cohort,unit_id").df()
    frame, thresholds = baseline_flags(frame)
    frame, summaries = forest_scores(frame, partial)
    frame["processing_rule"] = VERSION
    frame["baseline_flag"] = frame.baseline_reasons.str.len().gt(2)
    frame["any_pattern"] = frame.baseline_flag | frame.iforest_flag
    frame.to_parquet(partial / "profile_scores.parquet", index=False)
    candidates = frame[frame.any_pattern]
    candidates.to_parquet(partial / "pattern_candidates.parquet", index=False)
    candidates.to_csv(partial / "pattern_candidates.csv", index=False)
    dump(partial / "baseline_thresholds.json", thresholds)
    dump(partial / "model_cohorts.json", summaries)
    document_tables(db, partial)
    docs = db.execute(
        "SELECT * EXCLUDE(document_keys) FROM document_blocks ORDER BY known_documents DESC,date_key,flight_key,ticket_key LIMIT 50"
    ).df()
    review_page(frame, docs, partial)
    plot_results(frame, partial)
    checks = {
        "unit_ids_unique": len(frame) == frame.unit_id.nunique(),
        "all_native_profiles_retained": db.execute(
            "SELECT count(*) FROM units WHERE unit_kind='source_profile'"
        ).fetchone()[0]
        == db.execute(
            "SELECT count(*) FROM entities WHERE entity_kind IN ('native_profile','native_user')"
        ).fetchone()[0],
        "held_groups_not_merged": db.execute(
            "SELECT count(*) FROM unit_members WHERE unit_kind='preliminary_group' AND linkage_status!='provisional_consistent'"
        ).fetchone()[0]
        == 0,
        "no_group_with_multiple_documents": db.execute(
            "SELECT count(*) FROM groups WHERE status='provisional_consistent' AND known_documents>1"
        ).fetchone()[0]
        == 0,
        "bins_no_more_than_observations": bool(
            (frame.flight_days <= frame.raw_observations).all()
        ),
        "all_scored_features_finite": bool(
            np.isfinite(frame.loc[frame.model_eligible, FEATURES].to_numpy()).all()
        ),
        "unscored_have_no_forest_flag": bool(
            (~frame.loc[~frame.model_eligible, "iforest_flag"]).all()
        ),
        "inputs_unchanged": all(
            sha(p) == hashes[str(p.relative_to(root))] for p in paths.values()
        ),
    }
    checks["all_passed"] = all(checks.values())
    dump(partial / "verification.json", checks)
    if not checks["all_passed"]:
        raise ValueError(checks)
    counts = {
        "units": len(frame),
        "model_eligible": int(frame.model_eligible.sum()),
        "isolation_forest_flags": int(frame.iforest_flag.sum()),
        "baseline_flags": int(frame.baseline_flag.sum()),
        "any_pattern": len(candidates),
        "document_blocks": db.execute(
            "SELECT count(*) FROM document_blocks"
        ).fetchone()[0],
        "held_document_components": db.execute(
            "SELECT count(*) FROM groups WHERE known_documents>1"
        ).fetchone()[0],
    }
    dump(
        partial / "manifest.json",
        {
            "version": VERSION,
            "inputs": hashes,
            "code_sha256": sha(Path(__file__)),
            "counts": counts,
            "features": FEATURES,
            "seeds": SEEDS,
            "minimum_cohort": MIN_COHORT,
            "elapsed_seconds": time.perf_counter() - started,
            "python": platform.python_version(),
            "sklearn": sklearn.__version__,
            "precision": None,
            "recall": None,
            "evaluation": "cross-sectional exploratory fit; no labeled or temporal test accuracy",
        },
    )
    reason_counts = {
        key: int(frame.baseline_reasons.str.contains(key, regex=False).sum())
        for key in (
            "high_observed_frequency",
            "many_observed_routes",
            "repeated_route",
            "concentrated_dates",
            "rare_routes_in_cohort",
        )
    }
    cohort_counts = (
        frame.groupby(["unit_kind", "source_signature"])
        .agg(
            units=("unit_id", "size"),
            eligible=("model_eligible", "sum"),
            forest_flags=("iforest_flag", "sum"),
            baseline_flags=("baseline_flag", "sum"),
        )
        .reset_index()
    )
    cohort_counts.to_csv(partial / "cohort_summary.csv", index=False)
    report(partial, counts, reason_counts, cohort_counts, summaries)
    db.close()
    partial.rename(output)
    print(json.dumps(counts, ensure_ascii=False, indent=2))


def report(output, counts, reasons, cohorts, summaries):
    rows = "\n".join(f"| {k} | {v:,} |" for k, v in counts.items())
    patterns = "\n".join(f"| {k} | {v:,} |" for k, v in reasons.items())
    text = f"""# Паттерны наблюдаемых поездок

Единицы: локальный профиль JSON/XML и предварительная группа связей, отдельно.
Один профиль может присутствовать в обеих аналитических версиях. Их количества
нельзя складывать как число людей. PDF не используется.

| Величина | Количество |
|---|---:|
{rows}

| Прозрачный признак | Отмеченные единицы |
|---|---:|
{patterns}

Единицы с несколькими признаками входят в несколько строк. Полный знаменатель
каждой группы сравнения — cohort_summary.csv; модель применяется только при
минимум 5 блоках дата/рейс, 3 разных датах, 80% блоков с согласованным маршрутом
и 200 сопоставимых единицах. Не прошедшие условия остаются в profile_features.

## Наблюдаемые факты и определения

Частота — количество разных сочетаний дата/рейс внутри единицы анализа.
Повторы источников такого сочетания схлопываются только для расчёта; все наблюдения
остаются в observation_members. Несколько маршрутов внутри такого сочетания
удерживаются как конфликт. Это не подтверждённые сегменты и не количество перелётов.
Разные локальные времена подсчитаны, но их физическая несовместимость не выводится.

Разнообразие — число согласованных направлений; повторение — доля самого частого
направления среди блоков с маршрутом (не менее 5 повторов и доля не ниже 80%).
Концентрация — максимальная доля блоков в любом окне 7 календарных дней.
Редкость — среднее −log сглаженной представленности направления в той же когорте,
взвешенное числом блоков. В rare_route_share присутствие самого профиля исключено;
редким считается направление менее чем у 1% остальных единиц когорты.

Для частоты, разнообразия, концентрации и редкости используются строгие превышения
99-го процентиля когорты; ограничения малых чисел записаны в baseline_thresholds
и коде. Эти пороги описательные, не вероятностные и не калиброваны на экспертах.

## Модель и проверка

Isolation Forest: 200 деревьев, максимум 256 обучающих единиц на дерево,
6 признаков, отдельная модель для каждой комбинации типа единицы и состава
источников. Порог — 99-й процентиль собственного обучающего score. Score не
вероятность; доля отмеченных единиц не является точностью. Все признаки относятся
к наблюдаемым паттернам, без имён, документов, гражданства и номеров карт.

Это поперечный поиск отклонений на историческом массиве. Временная проверка
предсказания будущего не проводилась; фиктивная тестовая точность не рассчитывается.
Для контроля воспроизводимости использованы seed 42, 43, 44; Jaccard списков
кандидатов каждой когорты записан в model_cohorts.json. Устойчивость не заменяет
независимую разметку и не доказывает превосходство над прозрачными правилами.

## Документы

document_discrepancies — блоки билет/дата/рейс с несколькими известными документами,
включая случаи внутри одного источника. held_document_components — ранее удержанные
компоненты с разными документами. Это разные единицы и пересекающиеся основания;
их количества нельзя суммировать как число людей или ошибок.
В document_evidence и held_document_evidence сохранены все строки, документы,
имена, наборы дат рождения и происхождение для ручной проверки. Несколько документов
могут быть законными; смена документа, ошибка номера билета или ошибочное сопоставление
остаются гипотезами. Паспорт не присваивается источнику, где его не было.

## Ограничения вывода

Покрытие источников неизвестно и может зависеть от активности профиля. Даже после
схлопывания повторов высокая частота не доказывает частые фактические перелёты.
Профили разных источников сравниваются раздельно; состав когорты не обеспечивает
равного наблюдения за людьми. Предварительные группы могут содержать ошибки.
Нет независимых меток аномалий или личностей, precision/recall недоступны.
Нельзя определить преступную причастность, намерение, шпионаж, истинную личность,
невозможность перемещения или репрезентативность для всех авиапассажиров.

Открыть review.html для просмотра кандидатов и profile_scores.parquet для полного
набора измерений. unit_id → unit_members/observation_members → исходные таблицы.
Версия правил {VERSION}; входные SHA-256 и версии среды находятся в manifest.json.
"""
    (output / "REPORT.md").write_text(text)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output", type=Path, default=Path("proceed_data/profile_patterns_v1")
    )
    args = parser.parse_args()
    run(Path(__file__).resolve().parents[1], args.output.resolve())


if __name__ == "__main__":
    main()
