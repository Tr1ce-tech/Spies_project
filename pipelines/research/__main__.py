import argparse
import importlib.metadata
import json
import platform
import time
from pathlib import Path

import duckdb

from .adapters import (
    build_entities,
    build_identifiers,
    build_observations,
    expand_forum,
    register_sources,
)
from .analysis import aggregate, plot, profile_tables
from .common import VERSION, dump, install_functions, sha
from .matching import run_matching
from .report import write_report

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(
        description="Analyze sources and build provisional, auditable profile candidates"
    )
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument(
        "--output", type=Path, default=ROOT / "proceed_data/research_v1"
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Continue completed stages of a partial run",
    )
    args = parser.parse_args()
    root = args.root.resolve()
    target = args.output.resolve()
    output = target.with_name(target.name + ".partial")
    if target.exists() or (output.exists() and not args.resume):
        raise FileExistsError(
            "Choose a fresh output path; existing results are preserved"
        )
    output.mkdir(parents=True, exist_ok=args.resume)
    run_config = {
        "root": str(root),
        "target": str(target),
        "processing_rule": VERSION,
        "code_hashes": {
            str(p.relative_to(root)): sha(p)
            for p in sorted((root / "pipelines/research").glob("*.py"))
        },
    }
    config_path = output / "run_config.json"
    if args.resume:
        if (
            not config_path.exists()
            or json.loads(config_path.read_text()) != run_config
        ):
            raise ValueError("Resume requires exactly the same code, root and target")
    else:
        dump(config_path, run_config)
    db = duckdb.connect(str(output / "work.db"))
    db.execute("SET memory_limit='2GB'")
    db.execute("SET threads=4")
    install_functions(db)
    cache_path = output / "stage_cache.json"
    cache = json.loads(cache_path.read_text()) if cache_path.exists() else {}
    timings_path = output / "performance.json"
    timings = json.loads(timings_path.read_text()) if timings_path.exists() else {}
    started = time.perf_counter()

    def stage(label, action):
        if label in cache:
            print(label + " (completed stage reused)", flush=True)
            return cache[label]
        print(label, flush=True)
        begin = time.perf_counter()
        result = action()
        timings[label] = round(time.perf_counter() - begin, 3)
        cache[label] = result
        dump(cache_path, cache)
        dump(output / "performance.json", timings)
        return result

    try:
        inventory, hashes = stage(
            "1. Input inventory and hashes", lambda: register_sources(db, root, output)
        )
        stage("2. JSON profile fields", lambda: expand_forum(db, root, output))
        stage("3. Source entities", lambda: build_entities(db, output))
        stage("4. Identifier assertions", lambda: build_identifiers(db, output))
        stage("5. Source observations", lambda: build_observations(db, output))
        stage(
            "6. Source field quality and repeats",
            lambda: profile_tables(db, inventory, output),
        )
        coverage = stage(
            "7. Aggregated routes and coverage", lambda: aggregate(db, output)
        )
        matching = stage(
            "8. Profile candidates and complete-component checks",
            lambda: run_matching(db, output),
        )
        stage("9. Figures", lambda: plot(db, output))
        stage(
            "10. Input stability",
            lambda: [
                (_ for _ in ()).throw(ValueError("Input changed: " + path))
                for path, digest in hashes.items()
                if sha(root / path) != digest
            ],
        )
        for path, digest in run_config["code_hashes"].items():
            if sha(root / path) != digest:
                raise ValueError("Code changed during execution: " + path)
        timings.pop("total_seconds", None)
        timings["last_attempt_seconds"] = round(time.perf_counter() - started, 3)
        timings["sum_completed_stage_seconds"] = round(
            sum(v for k, v in timings.items() if k[0].isdigit()), 3
        )
        timings["timing_scope"] = (
            "Per completed stage; cached stages retain prior times; failed attempts excluded"
        )
        write_report(db, output, inventory, coverage, matching, timings)
        counts = {
            name: db.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
            for name in [
                "entities",
                "identifiers",
                "observations",
                "assignments",
                "preliminary_groups",
            ]
        }
        assert counts["entities"] == counts["assignments"]
        dump(
            output / "manifest.json",
            {
                "complete": True,
                "processing_rule": VERSION,
                "input_hashes": hashes,
                "counts": counts,
                "python": platform.python_version(),
                "platform": platform.platform(),
                "libraries": {
                    name: importlib.metadata.version(name)
                    for name in ["duckdb", "pyarrow", "pandas", "matplotlib"]
                },
                "code_hashes": run_config["code_hashes"],
                "precision": None,
                "recall": None,
                "technical_checks_passed": True,
            },
        )
        db.close()
        (output / "work.db").unlink()
        if (output / "FAILED.json").exists():
            (output / "FAILED.json").unlink()
        output.rename(target)
        print(
            json.dumps(
                {"output": str(target), "counts": counts, "matching": matching},
                indent=2,
            ),
            flush=True,
        )
    except Exception as error:
        dump(
            output / "FAILED.json",
            {
                "complete": False,
                "error": f"{type(error).__name__}: {error}",
                "timings": timings,
            },
        )
        db.close()
        raise


if __name__ == "__main__":
    main()
