import argparse
import csv
import datetime
import hashlib
import html
import json
import re
from pathlib import Path

import duckdb
import pandas as pd

from pipelines.research.common import document, dump, literal, name, sha

VERSION = "document-review.v1"
KEY = ["ticket_key", "date_key", "flight_key"]


def case_id(kind, value):
    return hashlib.sha256(
        json.dumps([kind, value], default=str, ensure_ascii=False).encode()
    ).hexdigest()


def birth_options(raw):
    value = raw.strip()
    patterns = [(value, "%Y-%m-%d")]
    if re.fullmatch(r"[0-9]{2}/[0-9]{2}/[0-9]{4}", value):
        patterns += [(value, "%d/%m/%Y"), (value, "%m/%d/%Y")]
    result = set()
    for text, fmt in patterns:
        try:
            parsed = datetime.datetime.strptime(text, fmt).date()
            if parsed.strftime(fmt) == text:
                result.add(parsed.isoformat())
        except ValueError:
            pass
    return result


def choose(frame, quota):
    frame = frame.copy()
    frame["selection_hash"] = frame.case_id.map(
        lambda v: hashlib.sha256((v + ":" + VERSION).encode()).hexdigest()
    )
    return (
        frame.sort_values(["stratum", "selection_hash"])
        .groupby("stratum", sort=True)
        .head(quota)
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output", type=Path, default=Path("proceed_data/document_review_v1")
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    if output.exists():
        raise FileExistsError(
            "Choose a new output directory; reviewer work is never overwritten"
        )
    output.mkdir(parents=True)
    source = root / "proceed_data/profile_patterns_v1"
    paths = {
        n: source / (n + ".parquet")
        for n in [
            "document_discrepancies",
            "document_evidence",
            "held_document_components",
            "held_document_evidence",
        ]
    }
    hashes = {str(p.relative_to(root)): sha(p) for p in paths.values()}
    blocks = pd.read_parquet(paths["document_discrepancies"])
    evidence = pd.read_parquet(paths["document_evidence"])
    components = pd.read_parquet(paths["held_document_components"])
    members = pd.read_parquet(paths["held_document_evidence"])
    cases = []
    evidence_groups = evidence.groupby(KEY, sort=False)
    for key, rows in evidence_groups:
        known_birth = sum(bool(json.loads(v)) for v in rows.birth_dates_json.dropna())
        sources = "+".join(sorted(rows.source.unique()))
        cases.append(
            dict(
                case_id=case_id("ticket_block", list(key)),
                case_kind="ticket_block",
                stratum=f"ticket:{sources}:birth_constraints_{'two_plus' if known_birth >= 2 else 'insufficient'}",
                ticket_key=key[0],
                date_key=key[1],
                flight_key=key[2],
                candidate_component_id=None,
            )
        )
    ticket_frame = pd.DataFrame(cases)
    assert len(ticket_frame) == len(blocks)
    all_document_rows = evidence[evidence.document_raw.notna()]
    assert all_document_rows.source.isin(["csv", "tab"]).all()
    assert (
        all_document_rows.loc[all_document_rows.source.eq("csv"), "document_raw"]
        .str.fullmatch(r"[0-9]{4} [0-9]{6}")
        .all()
    )
    assert (
        all_document_rows.loc[all_document_rows.source.eq("tab"), "document_raw"]
        .str.fullmatch(r"[0-9]{2} [0-9]{7}")
        .all()
    )
    assert all(
        set(len(v) for v in group.document_key.dropna().unique()) == {9, 10}
        for _, group in members.groupby("candidate_component_id")
    )
    group_cases = pd.DataFrame(
        [
            dict(
                case_id=case_id("held_component", r.candidate_component_id),
                case_kind="held_component",
                stratum=f"component:names_{bool(r.conflicting_structured_names)}:native_{r.native_profiles}",
                ticket_key=None,
                date_key=None,
                flight_key=None,
                candidate_component_id=r.candidate_component_id,
            )
            for r in components.itertuples()
        ]
    )
    selected = pd.concat(
        [choose(ticket_frame, 6), choose(group_cases, 4)], ignore_index=True
    )
    assert len(selected) == 40, "Review strata changed: revise quotas explicitly"
    selected["review_number"] = range(1, len(selected) + 1)
    frame = pd.concat([ticket_frame, group_cases], ignore_index=True)
    selected["frame_cases"] = selected.stratum.map(frame.groupby("stratum").size())
    selected.to_parquet(output / "sampling.parquet", index=False)
    selection = []
    for row in selected.itertuples():
        if row.case_kind == "ticket_block":
            subset = evidence_groups.get_group(
                (row.ticket_key, row.date_key, row.flight_key)
            ).copy()
        else:
            subset = members[
                members.candidate_component_id.eq(row.candidate_component_id)
            ].copy()
        subset["case_id"] = row.case_id
        subset["case_kind"] = row.case_kind
        selection.append(subset)
    selected_evidence = pd.concat(selection, ignore_index=True)
    db = duckdb.connect()
    db.register(
        "wanted",
        selected_evidence[
            ["entity_id", "source_record_id", "source"]
        ].drop_duplicates(),
    )
    db.execute(
        f"CREATE VIEW csv_source AS SELECT * FROM read_parquet({literal(root / 'parsers/CSV_Parser/result/records.parquet')})"
    )
    db.execute(
        f"CREATE VIEW tab_source AS SELECT * FROM read_parquet({literal(str(root / 'parsers/Tab_Parser/result/flights_part_*.parquet'))})"
    )
    csv_meta = db.execute(
        "SELECT p.* FROM csv_source p JOIN wanted w ON w.source='csv' AND w.source_record_id=p.record_id"
    ).df()
    tab_meta = db.execute(
        "SELECT p.* FROM tab_source p JOIN wanted w ON w.source='tab' AND w.source_record_id=p.record_id"
    ).df()
    raw_csv_path = root / "raw_data/BoardingData.csv"
    raw_tab_path = root / "raw_data/Sirena-export-fixed.tab"
    for p in [raw_csv_path, raw_tab_path]:
        hashes[str(p.relative_to(root))] = sha(p)
    for p in [
        root / "parsers/CSV_Parser/result/records.parquet",
        *sorted((root / "parsers/Tab_Parser/result").glob("flights_part_*.parquet")),
        root / "proceed_data/research_v1/candidate_block_assertions.parquet",
        root / "proceed_data/research_v1/candidate_identifier_blocks.parquet",
    ]:
        hashes[str(p.relative_to(root))] = sha(p)
    csv_records = {}
    wanted_csv = set(csv_meta.source_record_number)
    with raw_csv_path.open(encoding="utf-8", newline="") as handle:
        for n, record in enumerate(csv.DictReader(handle, delimiter=";"), 1):
            if n in wanted_csv:
                csv_records[n] = record
    tab_records = {}
    wanted_tab = set(tab_meta.source_line)
    with raw_tab_path.open(encoding="utf-8", newline="") as handle:
        for n, line in enumerate(handle, 1):
            if n in wanted_tab:
                tab_records[n] = line.rstrip("\r\n")
    audits = []
    for r in csv_meta.itertuples():
        raw = csv_records[r.source_record_number]
        checks = {
            "document": raw["PassengerDocument"] == r.passenger_document_raw,
            "first_name": raw["PassengerFirstName"] == r.passenger_first_name_raw,
            "last_name": raw["PassengerLastName"] == r.passenger_last_name_raw,
            "middle_name": raw["PassengerSecondName"] == r.passenger_second_name_raw,
            "birth_constraints": birth_options(raw["PassengerBirthDate"])
            == set(json.loads(r.birth_date_candidates_json)),
            "ticket": raw["TicketNumber"] == r.ticket_number_raw,
            "date": raw["FlightDate"] == r.flight_date_raw,
            "flight": raw["FlightNumber"] == r.flight_number_raw,
            "source_sha": r.source_sha256
            == hashes[str(raw_csv_path.relative_to(root))],
        }
        audits.append(
            dict(
                source="csv",
                source_record_id=r.record_id,
                document_raw=raw["PassengerDocument"],
                source_name_raw=" | ".join(
                    raw[v]
                    for v in [
                        "PassengerLastName",
                        "PassengerFirstName",
                        "PassengerSecondName",
                    ]
                ),
                birth_raw=raw["PassengerBirthDate"],
                source_locator=f"logical record {r.source_record_number}",
                checked_fields_json=json.dumps(checks),
                passed=all(checks.values()),
            )
        )
    for r in tab_meta.to_dict("records"):
        line = tab_records[r["source_line"]]
        checks = {
            label: line[a:b].strip() == r[label]
            for label, a, b in [
                ("TravelDoc", 168, 180),
                ("PaxName", 0, 60),
                ("PaxBirthDate", 60, 72),
                ("e-Ticket", 150, 168),
                ("DepartDate", 72, 84),
                ("FlightCode", 120, 130),
            ]
        }
        checks["source_sha"] = (
            r["source_sha256"] == hashes[str(raw_tab_path.relative_to(root))]
        )
        audits.append(
            dict(
                source="tab",
                source_record_id=r["record_id"],
                document_raw=line[168:180].strip(),
                source_name_raw=line[:60].strip(),
                birth_raw=line[60:72].strip(),
                source_locator=f"physical line {r['source_line']}",
                checked_fields_json=json.dumps(checks),
                passed=all(checks.values()),
            )
        )
    audit = pd.DataFrame(audits)
    assert audit.passed.all()
    enriched = selected_evidence.merge(
        audit, on=["source", "source_record_id"], how="left", suffixes=("", "_original")
    )
    document_rows = enriched[enriched.document_raw.notna()]
    assert document_rows.passed.fillna(False).all(), "Unverified document source"
    assert (document_rows.document_raw == document_rows.document_raw_original).all()
    assert all(
        document(v) == k
        for v, k in zip(
            document_rows.document_raw, document_rows.document_key, strict=True
        )
    )
    assert all(
        name(r.source_name_raw) == r.full_name
        for r in enriched[enriched.source.eq("tab")].itertuples()
    )
    assert all(
        birth_options(r.birth_raw) == set(json.loads(r.birth_dates_json))
        for r in document_rows.itertuples()
    )
    enriched.to_parquet(output / "case_evidence.parquet", index=False)
    audit.to_parquet(output / "source_audit.parquet", index=False)
    case_facts = []
    for cid, rows in enriched.groupby("case_id", sort=False):
        births = [
            set(json.loads(v)) for v in rows.birth_dates_json.dropna() if json.loads(v)
        ]
        formats = sorted(
            set(re.sub("[0-9]", "d", v) for v in rows.document_raw.dropna())
        )
        case_facts.append(
            dict(
                case_id=cid,
                known_documents=rows.document_key.nunique(),
                document_formats_json=json.dumps(formats),
                birth_constraint_assertions=len(births),
                birth_status="compatible_candidates"
                if len(births) >= 2 and set.intersection(*births)
                else "disjoint_candidates"
                if len(births) >= 2
                else "insufficient",
                source_document_fields_verified=True,
                identity_status="unresolved_without_independent_evidence",
                processing_rule=VERSION,
            )
        )
    facts = pd.DataFrame(case_facts)
    facts.to_parquet(output / "technical_assessment.parquet", index=False)
    labels = [
        dict(
            case_id=r.case_id,
            reviewer="",
            label="",
            evidence_notes="",
            independent_evidence_reference="",
            reviewed_at="",
        )
        for r in selected.itertuples()
    ]
    (output / "review_labels.json").write_text(
        json.dumps(labels, ensure_ascii=False, indent=2) + "\n"
    )
    # Trace why each selected held component was connected, preserving every assertion.
    db.register(
        "chosen_entities",
        members[
            members.candidate_component_id.isin(
                selected.candidate_component_id.dropna()
            )
        ][["entity_id", "candidate_component_id"]],
    )
    db.execute(
        f"CREATE VIEW assertions AS SELECT * FROM read_parquet({literal(root / 'proceed_data/research_v1/candidate_block_assertions.parquet')})"
    )
    db.execute(
        f"CREATE VIEW blocks AS SELECT * FROM read_parquet({literal(root / 'proceed_data/research_v1/candidate_identifier_blocks.parquet')})"
    )
    links = db.execute("""SELECT c.candidate_component_id,a.*,b.identifier_type,b.namespace,b.value
      FROM chosen_entities c JOIN assertions a USING(entity_id) JOIN blocks b USING(block_id)""").df()
    links.to_parquet(output / "link_evidence.parquet", index=False)
    write_page(output, selected, enriched, facts, labels)
    checks = dict(
        cases=len(selected),
        ticket_cases=int(selected.case_kind.eq("ticket_block").sum()),
        held_components=int(selected.case_kind.eq("held_component").sum()),
        unique_source_records_checked=len(audit),
        all_source_field_checks_passed=bool(audit.passed.all()),
        identity_labels_blank=all(not r["label"] for r in labels),
        input_hashes_unchanged=all(sha(root / p) == v for p, v in hashes.items()),
    )
    checks["all_passed"] = all(
        v
        for k, v in checks.items()
        if k
        not in [
            "cases",
            "ticket_cases",
            "held_components",
            "unique_source_records_checked",
        ]
    )
    dump(output / "verification.json", checks)
    dump(
        output / "manifest.json",
        dict(
            processing_rule=VERSION,
            code_sha256=sha(Path(__file__)),
            inputs=hashes,
            sampling="6 per ticket source/birth-completeness stratum; 4 per component name-conflict/native-profile-count stratum; hash order",
            warning="Diagnostic sample; categories overlap in original entities; no precision/recall estimate",
        ),
    )
    write_report(output, selected, facts, checks)
    print(json.dumps(checks, ensure_ascii=False, indent=2))


def write_page(output, cases, evidence, facts, labels):
    parts = [
        "<h1>Проверка разных документов</h1><p>40 случаев: 24 блока билета и 16 компонентов связей. "
        "Все документные поля выбранных CSV/TAB сверены с исходниками. Принадлежность одному человеку не установлена.</p>"
        "<p>Незаполненная метка означает отсутствие решения. Подтверждённые решения требуют основания; совместимые поля не являются независимым доказательством.</p>"
        '<label>Проверяющий: <input id="reviewer"></label> <button onclick="downloadLabels()">Скачать решения JSON</button>'
    ]
    for case in cases.itertuples():
        rows = evidence[evidence.case_id.eq(case.case_id)]
        fact = facts[facts.case_id.eq(case.case_id)].iloc[0]
        title = f"Случай {case.review_number}: " + (
            "билет / дата / рейс"
            if case.case_kind == "ticket_block"
            else "компонент связей"
        )
        parts.append(
            f"<details><summary>{html.escape(title)}</summary><p>ID: {case.case_id}</p>"
        )
        if case.case_kind == "ticket_block":
            parts.append(
                f"<p>Билет: {html.escape(str(case.ticket_key))}; дата: {case.date_key}; рейс: {html.escape(case.flight_key)}</p>"
            )
        else:
            parts.append(
                f"<p>Компонент: {html.escape(case.candidate_component_id)}. Основания соединения — link_evidence.parquet.</p>"
            )
        parts.append(
            f"<p>Разных документов: {fact.known_documents}; сравнение дат рождения: {html.escape(fact.birth_status)}. "
            "Типы документов не назначались, транслитерация имён не применялась.</p>"
        )
        columns = [
            "source",
            "source_name_raw",
            "first_name",
            "last_name",
            "middle_name",
            "full_name",
            "document_raw",
            "document_key",
            "birth_raw",
            "birth_dates_json",
            "source_locator",
            "source_record_id",
        ]
        parts.append(
            rows.reindex(columns=columns).fillna("").to_html(index=False, escape=True)
        )
        parts.append(
            f'<p><label>Решение <select id="label-{case.case_id}"><option value="">Не проверено</option>'
            '<option value="unresolved">Недостаточно оснований</option>'
            '<option value="confirmed_same_person_multiple_documents">Подтверждён один человек, разные документы</option>'
            '<option value="confirmed_source_data_error">Подтверждена ошибка данных источника</option>'
            '<option value="confirmed_linkage_error">Подтверждена ошибочная связь</option></select></label></p>'
            f'<p>Основание / замечания<br><textarea id="notes-{case.case_id}" rows="3" cols="100"></textarea></p>'
            f'<p>Ссылка на независимое основание <input id="reference-{case.case_id}" size="70"></p></details>'
        )
    encoded = json.dumps(labels, ensure_ascii=False).replace("<", "\\u003c")
    script = """const initial=JSON.parse(document.getElementById('initial').textContent);
function downloadLabels(){const reviewer=document.getElementById('reviewer').value;const rows=initial.map(r=>({...r,
reviewer,label:document.getElementById('label-'+r.case_id).value,evidence_notes:document.getElementById('notes-'+r.case_id).value,
independent_evidence_reference:document.getElementById('reference-'+r.case_id).value,reviewed_at:document.getElementById('label-'+r.case_id).value?new Date().toISOString():''}));
const blob=new Blob([JSON.stringify(rows,null,2)],{type:'application/json'});const link=document.createElement('a');link.href=URL.createObjectURL(blob);link.download='document_review_labels.json';link.click();URL.revokeObjectURL(link.href);}
"""
    css = "body{font:15px system-ui;margin:24px;color:#24374d}details{border:1px solid #cad3df;padding:12px;margin:14px 0;overflow:auto}summary{cursor:pointer;font-weight:bold}table{border-collapse:collapse;font-size:12px}td,th{padding:7px;border:1px solid #ddd;max-width:260px;overflow-wrap:anywhere}th{background:#e9eef5}input,select,textarea,button{font:inherit;padding:6px}"
    (output / "review.html").write_text(
        '<!doctype html><html lang="ru"><meta charset="utf-8"><title>Проверка документов</title><style>'
        + css
        + "</style>"
        + "".join(parts)
        + "<p>Решения не отправляются в сеть. Скачайте JSON перед закрытием: значения формы при обновлении страницы не сохраняются.</p>"
        + '<script type="application/json" id="initial">'
        + encoded
        + "</script><script>"
        + script
        + "</script></html>"
    )


def write_report(output, cases, facts, checks):
    sample = cases.groupby("stratum").agg(
        selected=("case_id", "size"), frame=("frame_cases", "first")
    )
    table = "\n".join(
        f"| {idx} | {r.selected} | {r.frame} |" for idx, r in sample.iterrows()
    )
    text = f"""# Проверка расхождений документов

Проверены {checks["unique_source_records_checked"]} уникальных исходных записей CSV/TAB
в 40 выбранных случаях: 24 блока билет/дата/рейс и 16 компонентов связей.
Документы, исходные имена, даты рождения, билеты и поля рейса сверены непосредственно
с raw_data; начальные нули сохранены. Различия документов не возникли при парсинге
проверенных полей. Это технический вывод, не подтверждение принадлежности человеку.

| Диагностическая категория | Выбрано | В категории |
|---|---:|---:|
{table}

Отбор: сортировка SHA-256(case_id + версия), фиксированная квота на категорию.
Случаи двух типов могут пересекаться по исходным профилям. Выборка не предназначена
для оценки общей точности, доли ошибок или recall.

Наблюдаемые факты полного массива, проверенные до отбора: во всех 3782 блоках
разные документы относятся к форматам «4 цифры + 6» в CSV и «2 цифры + 7» в TAB.
У всех 2561 удержанных компонентов два известных нормализованных документа:
один длиной 10 цифр и один длиной 9. Формы могут соответствовать разным видам
документов, но источник не устанавливает их тип, страну или владельца.

В выбранных случаях совместимые наборы дат рождения: {int(facts.birth_status.eq("compatible_candidates").sum())};
противоречащие: {int(facts.birth_status.eq("disjoint_candidates").sum())};
недостаточно сведений: {int(facts.birth_status.eq("insufficient").sum())}.
Имена разных алфавитов не объявлялись совпадающими автоматически. Противоречия
известных имён внутри одного алфавита в компонентах требуют отдельной проверки.

Гипотезы: разные законные документы одного человека; неоднозначная карта лояльности;
ошибка связи; ошибка исходного номера. Эти версии не различить только проверкой
срезов и совместимости даты рождения. Ни один случай не получил истинную метку.
Автоматически ослаблять удержание групп из-за разных документов пока нельзя.

review.html — просмотр всех 40 случаев и ввод решений. Скачайте JSON с решениями
перед закрытием: исходный шаблон review_labels.json остаётся пустым. Метки
«подтверждено» требуют независимого основания. Если его нет, используйте
«Недостаточно оснований». technical_assessment.parquet содержит только технические
проверки, отдельно от человеческих меток. case_evidence/source_audit/link_evidence
сохраняют исходные значения и цепочку происхождения. Исходники и группы не менялись.
"""
    (output / "REPORT.md").write_text(text)


if __name__ == "__main__":
    main()
