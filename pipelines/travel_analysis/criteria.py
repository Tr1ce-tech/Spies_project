import math

import numpy as np
import pandas as pd


def parameter(kind, default, minimum=0, maximum=None, nullable=False):
    return dict(
        type=kind, default=default, minimum=minimum, maximum=maximum, nullable=nullable
    )


CRITERIA = {
    "high_frequency": dict(
        label="Высокая наблюдаемая частота",
        category="travel_pattern",
        parameters=dict(
            minimum=parameter("integer", None, 1, nullable=True),
            quantile=parameter("number", 0.99, 0.5, 1),
        ),
    ),
    "many_routes": dict(
        label="Много направлений",
        category="travel_pattern",
        parameters=dict(
            minimum=parameter("integer", None, 1, nullable=True),
            quantile=parameter("number", 0.99, 0.5, 1),
        ),
    ),
    "repeated_route": dict(
        label="Сильное повторение маршрута",
        category="travel_pattern",
        parameters=dict(
            min_count=parameter("integer", 5, 1),
            min_share=parameter("number", 0.8, 0, 1),
        ),
    ),
    "date_concentration": dict(
        label="Концентрация в окне 7 дней",
        category="travel_pattern",
        parameters=dict(
            min_count=parameter("integer", 10, 1),
            min_share=parameter("number", None, 0, 1, True),
            quantile=parameter("number", 0.99, 0.5, 1),
        ),
    ),
    "rare_routes": dict(
        label="Редкие направления относительно когорты",
        category="travel_pattern",
        parameters=dict(
            min_routes=parameter("integer", 3, 1),
            minimum=parameter("number", None, 0, nullable=True),
            quantile=parameter("number", 0.99, 0.5, 1),
        ),
    ),
    "document_discrepancy": dict(
        label="Связь с неразрешённым компонентом с разными документами",
        category="data_quality",
        parameters=dict(minimum=parameter("integer", 2, 2)),
    ),
    "route_conflict": dict(
        label="Несколько маршрутов внутри даты и рейса",
        category="data_quality",
        parameters=dict(minimum=parameter("integer", 1, 1)),
    ),
    "busy_day": dict(
        label="Несколько наблюдаемых рейсов в одну дату",
        category="travel_pattern",
        parameters=dict(minimum=parameter("integer", 3, 2)),
    ),
}
DEFAULT_CRITERIA = list(CRITERIA)[:5]
UNIT_KINDS = ["source_profile", "preliminary_group"]
HELP = {
    "high_frequency": "Разные блоки дата/рейс. minimum задаёт абсолютный порог; null означает процентиль когорты.",
    "many_routes": "Разные согласованные направления, покрытие маршрутами от 80%. minimum либо процентиль.",
    "repeated_route": "Доля главного направления: min_share; одновременно минимум min_count повторов.",
    "date_concentration": "Доля блоков в окне 7 календарных дней. min_count блоков всего; min_share либо процентиль.",
    "rare_routes": "Средняя редкость направлений в когорте. minimum либо процентиль. Это не вероятность.",
    "document_discrepancy": "Участие в неразрешённом компоненте с разными документами; принадлежность документов профилю не установлена.",
    "route_conflict": "Блоки дата/рейс с несколькими известными маршрутами; это не доказанная ошибка.",
    "busy_day": "Максимум разных рейсов с одной календарной датой; без проверки часовых поясов и фактических перелётов.",
}
for _key, _help in HELP.items():
    CRITERIA[_key]["help"] = _help


def validate_config(config):
    if not isinstance(config, dict) or set(config) - {
        "criteria",
        "match",
        "unit_kinds",
    }:
        raise ValueError("Config supports only criteria, match, unit_kinds")
    chosen = config.get("criteria")
    if not isinstance(chosen, dict) or not chosen:
        raise ValueError("Choose at least one criterion")
    mode = config.get("match", "any")
    if mode not in ["any", "all"]:
        raise ValueError("match must be any or all")
    kinds = config.get("unit_kinds", ["source_profile"])
    if (
        not isinstance(kinds, list)
        or not kinds
        or any(k not in UNIT_KINDS for k in kinds)
    ):
        raise ValueError(
            "unit_kinds must contain source_profile and/or preliminary_group"
        )
    result = dict(criteria={}, match=mode, unit_kinds=list(dict.fromkeys(kinds)))
    for key, values in chosen.items():
        if key not in CRITERIA:
            raise ValueError("Unknown criterion: " + key)
        definitions = CRITERIA[key]["parameters"]
        if not isinstance(values, dict) or set(values) - set(definitions):
            raise ValueError("Unknown parameters for " + key)
        params = {}
        for label, definition in definitions.items():
            value = values.get(label, definition["default"])
            if value is None and definition["nullable"]:
                params[label] = value
                continue
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
            ):
                raise ValueError(f"{key}.{label} must be a finite number")
            if definition["type"] == "integer" and not isinstance(value, int):
                raise ValueError(f"{key}.{label} must be an integer")
            if (
                value < definition["minimum"]
                or definition["maximum"] is not None
                and value > definition["maximum"]
            ):
                raise ValueError(f"{key}.{label} is out of range")
            params[label] = value
        result["criteria"][key] = params
    return result


def select_profiles(features, config):
    config = validate_config(config)
    frame = features[features.unit_kind.isin(config["unit_kinds"])].copy()
    reasons = {idx: [] for idx in frame.index}
    reference = features[features.reference_eligible]
    eligible = frame.reference_eligible
    for key, params in config["criteria"].items():
        metric = {
            "high_frequency": "flight_days",
            "many_routes": "distinct_routes",
            "repeated_route": "top_route_share",
            "date_concentration": "peak7_share",
            "rare_routes": "route_rarity",
            "document_discrepancy": "candidate_document_count",
            "route_conflict": "route_conflict_flight_days",
            "busy_day": "peak_day_flight_bins",
        }[key]
        minimum = (
            params.get("min_share")
            if key in ["repeated_route", "date_concentration"]
            else params.get("minimum")
        )
        if minimum is None:
            cut = reference.groupby("cohort")[metric].quantile(params["quantile"])
            threshold = frame.cohort.map(cut)
            mask = eligible & frame[metric].gt(threshold)
            operator = ">"
        else:
            threshold = pd.Series(minimum, index=frame.index, dtype=float)
            mask = frame[metric].ge(minimum)
            operator = ">="
        if key in ["many_routes", "repeated_route"]:
            mask &= frame.route_coverage.ge(0.8)
        if key == "repeated_route":
            mask &= frame.top_route_flight_days.ge(params["min_count"])
        if key == "date_concentration":
            mask &= frame.flight_days.ge(params["min_count"])
        if key == "rare_routes":
            mask &= eligible & frame.distinct_routes.ge(params["min_routes"])
        if key == "document_discrepancy":
            note = "Участие в кандидатном компоненте с разными документами; принадлежность документов профилю не установлена."
        elif key == "route_conflict":
            note = "Разные известные маршруты в одном блоке дата/рейс; возможен многоучастковый рейс или codeshare."
        elif key == "busy_day":
            note = "Разные блоки рейсов с одной календарной датой; часовые пояса и фактическое выполнение не установлены."
        else:
            note = "Отклонение наблюдаемых записей; покрытие источника неизвестно."
        for idx in frame.index[mask.fillna(False)]:
            value = frame.at[idx, metric]
            reasons[idx].append(
                dict(
                    criterion=key,
                    label=CRITERIA[key]["label"],
                    category=CRITERIA[key]["category"],
                    metric=metric,
                    observed=float(value),
                    operator=operator,
                    threshold=float(threshold.at[idx]),
                    params=params,
                    threshold_mode="cohort_quantile" if minimum is None else "absolute",
                    cohort=frame.at[idx, "cohort"],
                    evidence={
                        field: int(frame.at[idx, field])
                        if pd.notna(frame.at[idx, field])
                        else None
                        for field in [
                            "flight_days",
                            "routed_flight_days",
                            "top_route_flight_days",
                            "peak7_flight_days",
                            "peak_day_flight_bins",
                        ]
                    },
                    reference_units=int(frame.at[idx, "reference_units"])
                    if pd.notna(frame.at[idx, "reference_units"])
                    else None,
                    explanation=note,
                )
            )
    sizes = np.array([len(reasons[idx]) for idx in frame.index])
    selected = (
        sizes > 0 if config["match"] == "any" else sizes == len(config["criteria"])
    )
    result = frame.loc[selected].copy()
    result["matched_criteria"] = [
        [r["criterion"] for r in reasons[idx]] for idx in result.index
    ]
    result["criteria_count"] = [len(reasons[idx]) for idx in result.index]
    return result, reasons, config
