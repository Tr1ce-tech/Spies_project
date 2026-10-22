# Catch The Spies: парсеры источников

Исходники находятся в `raw_data/`, отдельные парсеры — в `parsers/`. Они не объединяют людей и не считают записи доказанными перелётами. Итоги проверки: [audit/2026-10-22/REPORT.md](audit/2026-10-22/REPORT.md).

| Формат | Папка | Основной результат |
|---|---|---|
| CSV | CSV_Parser | result/records.parquet |
| Фиксированная ширина | Tab_Parser | result/flights_part_*.parquet |
| JSON | JSON_Parser | result/json_profiles_part_*.parquet, json_flights_part_*.parquet и json_loyalty_part_*.parquet |
| XML | XML_Parser | result/users.parquet, cards.parquet, activities.parquet |
| YAML | YAML_Parser | result/flights.parquet и loyalty_entries.parquet |
| PDF | PDF_Parser | data/pdf_intro/ и data/pdf_schedule/schedule.parquet |
| Excel | Excel_Tickets | result/tickets.parquet; исправный ZIP проверен, 615 771 строка |

Python-код отформатирован Ruff: 88 символов, 4 пробела, упорядоченные импорты. Правила находятся в pyproject.toml. Форматирование и тесты не заменяют сверку данных с источником.

```bash
.venv/bin/python -m pip install -r requirements-dev.txt
.venv/bin/ruff check parsers
.venv/bin/ruff format --check parsers
```

Команды разбора и независимой проверки приведены в README соответствующей папки. Существующие актуальные результаты не перезаписываются молча; для повторного разбора нужен новый путь. Таблицы разных единиц наблюдения нельзя читать как один общий Parquet dataset. Git на данном этапе не используется.

Дополнение по JSON: [полнота чтения и новая таблица лояльности](audit/2026-10-22/JSON_UPDATE.md).

Excel: исходник заменён и повторно проверен; подробности в [CURRENT_AUDIT.md](parsers/Excel_Tickets/CURRENT_AUDIT.md). Предварительные связи с CSV описаны в [pipelines/README.md](pipelines/README.md).

## Анализ источников и сопоставление

Первичный анализ семи источников и предварительные группы записей: [отчёт](reports/source_analysis_v1/REPORT.md). Код, команды запуска и ограничения: [pipelines/research/README.md](pipelines/research/README.md). Локальные таблицы и материалы ручной проверки находятся в proceed_data/research_v1 и исключены из Git. Предварительные группы не являются установленными людьми; независимых меток точности нет.

## Два этапа для будущего интерфейса

`pipelines.build_common_parquet` собирает единый файл из пассажирских Parquet
в указанном каталоге. `pipelines.filter_profiles` читает только этот файл и
отбирает профили по выбранным критериям и порогам. Вывод включает имена по
источникам, измеряемые показатели и причины срабатывания, без оценки преступной
причастности. PDF не включается. Команды, формат конфигурации и API для UI:
[TWO_STAGE_ANALYSIS.md](pipelines/TWO_STAGE_ANALYSIS.md).
