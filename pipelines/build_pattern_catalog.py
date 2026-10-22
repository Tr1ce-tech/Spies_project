import argparse
import collections
import json
from pathlib import Path

import duckdb
import pandas as pd

from pipelines.research.common import dump, literal, sha

VERSION = "pattern-catalog.v1"
LABELS = {
    "high_observed_frequency": "Высокая наблюдаемая частота",
    "many_observed_routes": "Много направлений",
    "repeated_route": "Сильное повторение маршрута",
    "concentrated_dates": "Концентрация поездок по датам",
    "rare_routes_in_cohort": "Редкие направления",
}
FIELDS = [
    "unit_id",
    "unit_kind",
    "source_signature",
    "entity_members",
    "linkage_status",
    "flight_days",
    "active_dates",
    "first_date",
    "last_date",
    "raw_observations",
    "routed_flight_days",
    "route_conflict_flight_days",
    "distinct_routes",
    "top_route_flight_days",
    "peak7_flight_days",
    "route_coverage",
    "top_route_share",
    "peak7_share",
    "route_rarity",
    "rare_route_share",
    "eligible_units",
    "cohort",
    "baseline_reasons",
]


def rules(row, cut):
    result = []
    if row["flight_days"] > cut["flight_days"]:
        result.append("high_observed_frequency")
    if row["distinct_routes"] > cut["distinct_routes"]:
        result.append("many_observed_routes")
    if row["top_route_share"] >= 0.8 and row["top_route_flight_days"] >= 5:
        result.append("repeated_route")
    if row["peak7_share"] > cut["peak7_share"] and row["flight_days"] >= 10:
        result.append("concentrated_dates")
    if row["route_rarity"] > cut["route_rarity"] and row["distinct_routes"] >= 3:
        result.append("rare_routes_in_cohort")
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output", type=Path, default=Path("proceed_data/pattern_catalog_v1")
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    if output.exists():
        raise FileExistsError("Choose a new output directory")
    output.mkdir(parents=True)
    base = root / "proceed_data/profile_patterns_v1"
    paths = {
        label: base / (label + ".parquet")
        for label in [
            "profile_scores",
            "unit_members",
            "flight_day_bins",
            "profile_routes",
            "cohort_routes",
            "observation_members",
        ]
    }
    paths["entities"] = root / "proceed_data/normalized_v1/source_entities.parquet"
    thresholds_path = base / "baseline_thresholds.json"
    hashes = {
        str(p.relative_to(root)): sha(p) for p in [*paths.values(), thresholds_path]
    }
    db = duckdb.connect()
    db.execute("SET memory_limit='2GB'")
    db.execute("SET threads=4")
    for label, p in paths.items():
        db.execute(f"CREATE VIEW {label} AS SELECT * FROM read_parquet({literal(p)})")
    db.execute("CREATE VIEW chosen AS SELECT * FROM profile_scores WHERE baseline_flag")
    candidates = (
        db.execute("SELECT " + ",".join(FIELDS) + " FROM chosen ORDER BY unit_id")
        .to_arrow_table()
        .to_pylist()
    )
    cuts = {r["cohort"]: r for r in json.loads(thresholds_path.read_text())}
    for row in candidates:
        assert rules(row, cuts[row["cohort"]]) == json.loads(row["baseline_reasons"])
        row["patterns"] = json.loads(row.pop("baseline_reasons"))
        row["thresholds"] = cuts[row["cohort"]]
        row["processing_rule"] = VERSION
    names = (
        db.execute("""SELECT m.unit_id,e.entity_id,e.source,e.first_name,e.last_name,e.middle_name,
      e.full_name,e.source_record_id,e.source_locator,e.source_sha256,e.source_rule_version,
      e.processing_rule FROM unit_members m JOIN chosen c USING(unit_id) JOIN entities e USING(entity_id)
      ORDER BY m.unit_id,e.source,e.entity_id""")
        .to_arrow_table()
        .to_pylist()
    )
    bins = (
        db.execute("""SELECT b.* FROM flight_day_bins b JOIN chosen c USING(unit_id)
      ORDER BY unit_id,date_key,flight_key""")
        .to_arrow_table()
        .to_pylist()
    )
    routes = (
        db.execute("""SELECT r.*,p.units_with_route,c.eligible_units,
      (p.units_with_route-1)::DOUBLE/(c.eligible_units-1) other_units_share
      FROM profile_routes r JOIN chosen c USING(unit_id) JOIN cohort_routes p
        ON c.cohort=p.cohort AND r.origin=p.origin AND r.destination=p.destination
      ORDER BY unit_id,r.flight_days DESC,r.origin,r.destination""")
        .to_arrow_table()
        .to_pylist()
    )
    links = (
        db.execute("""SELECT m.* FROM observation_members m JOIN chosen c USING(unit_id)
      ORDER BY unit_id,observation_id""")
        .to_arrow_table()
        .to_pylist()
    )
    for name, rows in [
        ("names", names),
        ("flight_day_bins", bins),
        ("routes", routes),
        ("observation_members", links),
    ]:
        # Raw source references stay in the private analytical layer.
        db.register("rows_to_save", pd.DataFrame(rows))
        db.execute(
            f"COPY rows_to_save TO {literal(output / (name + '.parquet'))} (FORMAT PARQUET,COMPRESSION ZSTD)"
        )
        db.unregister("rows_to_save")

    def grouped(rows):
        result = collections.defaultdict(list)
        for row in rows:
            result[row["unit_id"]].append(row)
        return result

    by_names, by_bins, by_routes, by_links = map(grouped, [names, bins, routes, links])
    for row in candidates:
        uid = row["unit_id"]
        row["names"] = by_names[uid]
        row["flights"] = by_bins[uid]
        row["routes"] = by_routes[uid]
        row["observations"] = by_links[uid]
        assert len(row["flights"]) == row["flight_days"]
        assert len(row["observations"]) == row["raw_observations"]
        assert (
            sum(r["raw_observations"] for r in row["flights"])
            == row["raw_observations"]
        )
    pd.DataFrame(
        [
            {
                k: v
                for k, v in r.items()
                if k not in ["names", "flights", "routes", "observations", "thresholds"]
            }
            for r in candidates
        ]
    ).to_parquet(output / "catalog.parquet", index=False)
    counts = {
        kind: {
            key: sum(
                r["unit_kind"] == kind and key in r["patterns"] for r in candidates
            )
            for key in LABELS
        }
        for kind in ["source_profile", "preliminary_group"]
    }
    display_candidates = []
    for row in candidates:
        shown = {k: v for k, v in row.items() if k != "observations"}
        shown["observation_count"] = len(row["observations"])
        shown["flights"] = [
            {k: v for k, v in r.items() if k not in ["unit_id", "slot_id"]}
            for r in row["flights"]
        ]
        display_candidates.append(shown)
    payload = {
        "version": VERSION,
        "labels": LABELS,
        "candidates": display_candidates,
        "counts": counts,
    }
    encoded = json.dumps(
        payload, ensure_ascii=False, default=str, allow_nan=False, separators=(",", ":")
    )
    (output / "catalog.json").write_text(encoded)
    template = Path(__file__).with_name("pattern_catalog.html").read_text()
    (output / "index.html").write_text(
        template.replace("__CATALOG_DATA__", encoded.replace("<", "\\u003c"))
    )
    verified = dict(
        no_isolation_forest_filter=True,
        rules_recomputed=True,
        all_matching_units_retained=len(candidates)
        == db.execute("SELECT count(*) FROM chosen").fetchone()[0],
        all_bin_counts_reconciled=True,
        all_observations_preserved=True,
        input_hashes_unchanged=all(sha(root / p) == v for p, v in hashes.items()),
    )
    verified["all_passed"] = all(verified.values())
    assert verified["all_passed"]
    dump(output / "verification.json", verified)
    dump(
        output / "manifest.json",
        dict(
            version=VERSION,
            inputs=hashes,
            code_sha256=sha(Path(__file__)),
            template_sha256=sha(Path(__file__).with_name("pattern_catalog.html")),
            counts=counts,
            source_profiles=sum(r["unit_kind"] == "source_profile" for r in candidates),
            preliminary_groups=sum(
                r["unit_kind"] == "preliminary_group" for r in candidates
            ),
            independent_identity_labels=False,
        ),
    )
    print(json.dumps(counts, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
