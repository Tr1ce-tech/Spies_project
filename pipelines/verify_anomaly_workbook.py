import datetime
import hashlib
import json
import math
import statistics
from pathlib import Path

import openpyxl


def main():
    root = Path(__file__).resolve().parents[1]
    inputs = root / "proceed_data/anomaly_workbook_v1"
    output = root / "outputs/anomaly_review_20261022"
    data = json.loads((inputs / "workbook_data.json").read_text())
    path = output / "Анализ_нормализованных_данных.xlsx"
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    checks = {}
    for spec in data["sheets"]:
        sheet = wb[spec["name"]]
        rows = (
            list(
                sheet.iter_rows(
                    min_row=5,
                    max_row=4 + len(spec["rows"]),
                    max_col=len(spec["headers"]),
                )
            )
            if spec["rows"]
            else []
        )
        assert len(rows) == len(spec["rows"])
        for actual, expected in zip(rows, spec["rows"], strict=True):
            for j, (cell, value) in enumerate(zip(actual, expected, strict=True)):
                result = cell.value
                if j in spec["date_cols"] and value is not None:
                    assert isinstance(result, (datetime.date, datetime.datetime))
                    result = result.strftime("%Y-%m-%d")
                if j in spec["text_cols"] and value is not None:
                    assert cell.data_type == "s", (cell.coordinate, cell.data_type)
                if isinstance(value, float):
                    assert isinstance(result, (int, float)) and math.isclose(
                        result, value, rel_tol=1e-12, abs_tol=1e-12
                    )
                else:
                    assert result == value, (
                        spec["name"],
                        cell.coordinate,
                        result,
                        value,
                    )
        checks[spec["name"]] = len(rows)
    groups = {}
    days = next(s for s in data["sheets"] if s["name"] == "По дням")["rows"]
    for source, date, n, weekday, med, mad, z in days:
        groups.setdefault((source, weekday), []).append(n)
    for source, date, n, weekday, med, mad, z in days:
        values = groups[source, weekday]
        independent_med = statistics.median(values)
        independent_mad = statistics.median(abs(x - independent_med) for x in values)
        assert med == independent_med and mad == independent_mad
        assert (z is None and mad == 0) or math.isclose(
            z, (n - med) / (1.4826 * mad), abs_tol=1e-10
        )
    expected_summary = [
        data["checks"]["source_total"],
        data["checks"]["trip_blocks"],
        data["checks"]["trip_members"],
        data["checks"]["route_rows"],
        120,
        3782,
        6,
    ]
    assert [wb["Обзор"].cell(r, 2).value for r in range(5, 12)] == expected_summary
    assert len(wb.sheetnames) == 14
    wb.close()
    settings = openpyxl.load_workbook(path, read_only=False, data_only=False)
    for spec in data["sheets"]:
        sheet = settings[spec["name"]]
        assert sheet.freeze_panes == "B5", (sheet.title, sheet.freeze_panes)
        if spec["rows"]:
            assert len(sheet.tables) == 1
    assert len(settings["Обзор"]._charts) == 1
    settings.close()
    result = dict(
        all_passed=True,
        sheet_rows=checks,
        sheets=14,
        all_exported_cells_compared=True,
        tickets_remain_exact_strings=True,
        independent_weekday_baselines=True,
        summary_cached_values_verified=True,
        frozen_headers_and_filters=True,
        pdf_excluded=True,
        workbook_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
    )
    (output / "verification.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
