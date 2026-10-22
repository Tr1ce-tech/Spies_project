"""Build a local read-only review page and an empty CSV label template."""

import argparse
import html
import json
from pathlib import Path

import pandas as pd


def build(directory):
    directory = Path(directory)
    assert json.loads((directory / "verification.json").read_text())[
        "all_checks_passed"
    ]
    page, labels_path = directory / "review.html", directory / "review_labels.csv"
    if page.exists() or labels_path.exists():
        raise FileExistsError("Existing review files are preserved")
    labels = pd.read_parquet(directory / "review_labels.parquet")
    members = pd.read_parquet(directory / "review_case_members.parquet")
    columns = [
        "source",
        "entity_kind",
        "first_name",
        "last_name",
        "middle_name",
        "full_name",
        "birth_dates_json",
        "document_raw",
        "source_locator",
        "source_record_id",
    ]
    parts = [
        '<!doctype html><html lang="ru"><meta charset="utf-8"><title>Проверка соответствий</title>',
        "<style>body{font:16px system-ui;margin:32px;max-width:1600px;color:#182028}"
        "summary{cursor:pointer;padding:12px;background:#eef2f5;margin-top:12px}"
        ".scroll{overflow-x:auto}table{border-collapse:collapse;font-size:13px;margin:12px 0}"
        "td,th{border:1px solid #ccd4dc;padding:8px;text-align:left;vertical-align:top}"
        "td{white-space:pre-wrap;max-width:350px;overflow-wrap:anywhere}h1{font-size:26px}</style>",
        "<h1>Ручная проверка соответствий</h1>",
        f"<p>Подготовлено случаев: {len(labels)}. Решения алгоритма здесь скрыты. "
        "Сравните атрибуты и указанные исходные записи; внесите вывод в review_labels.csv.</p>",
        "<p>Для пары: same_person, different_people или uncertain. Для компонента/блока: "
        "one_person, multiple_people или uncertain. Укажите проверяющего, основания и источник "
        "независимых сведений. Проверка согласованности полей сама по себе не устанавливает истинную принадлежность.</p>",
    ]
    for case in labels.itertuples(index=False):
        frame = members.loc[members.case_id == case.case_id, columns].fillna("")
        parts.append(
            f"<details><summary>{html.escape(case.case_id)} · "
            f"{html.escape(case.case_unit)} · записей: {len(frame)}</summary>"
            '<div class="scroll">'
            + frame.to_html(index=False, escape=True, border=0)
            + "</div></details>"
        )
    parts.append("</html>")
    page.write_text("\n".join(parts), encoding="utf-8")
    labels.to_csv(labels_path, index=False, encoding="utf-8-sig")
    print(f"Review cases: {len(labels)}; source members: {len(members)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "directory",
        type=Path,
        nargs="?",
        default=Path(__file__).resolve().parents[1] / "proceed_data/research_v1",
    )
    build(parser.parse_args().directory)
