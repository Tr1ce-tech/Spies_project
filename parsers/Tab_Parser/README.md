# Sirena: фиксированная ширина

`tab_parser.py` читает `raw_data/Sirena-export-fixed.tab`: 21 поле, 336 символов на запись. Позиции задаются в символах Unicode, не в байтах и не по количеству пробелов.

```bash
.venv/bin/python parsers/Tab_Parser/tab_parser.py
.venv/bin/python parsers/Tab_Parser/verify_tab.py
.venv/bin/python -m unittest discover -s parsers/Tab_Parser -p test_tab_parser.py -v
```

Результаты: `result/flights_part_*.parquet`, `issues.csv`, `summary.json`, `verification.json`. Повторный запуск требует новой папки: `--output parsers/Tab_Parser/result_new`.

Сохранены 155 757 исходных записей. Поля выделяются по FIELDS и обрезаются только по краям; `N/A` остаётся строковым маркером. Исходная строка с выравниванием хранится в `raw_line`; точные байты, включая перевод строки, — в `raw_bytes_b64`. Есть SHA-256 источника, версия и устойчивый `record_id` по хэшу/номеру физической строки. Идентификаторы не переводятся в числа.

Строки неверной ширины или кодировки не удаляются. Даты проверяются только как ISO, время — местное HH:MM; никакие часовые пояса не назначаются. Справочник аэропортов не подставляется. Данные исходных записей не доказывают состоявшийся перелёт.

`verify_tab.py` сравнивает все 21 поле каждой записи с независимым pandas.read_fwf, проверяет ID и полностью восстанавливает исходный SHA-256 из сохранённых байтов. sample.tab используется только тестами. Константы INPUT_FILE, OUTPUT_DIRECTORY, OUTPUT_FORMAT, CHUNK_SIZE, ENCODING остаются доступными для настройки.
