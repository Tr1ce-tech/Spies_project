"""Synthetic fixtures: no source XLSX files are modified or created."""

import io
import json
import struct
import tempfile
import unittest
import xml.etree.ElementTree as ET
import zipfile
from datetime import date
from pathlib import Path

import parse_excel_tickets as parser
import pyarrow.parquet as pq


def workbook_bytes(sheets):
    buffer = io.BytesIO()
    main_ns = parser.NS["m"]
    rel_ns = "http://schemas.openxmlformats.org/package/2006/relationships"
    book = ET.Element("workbook", xmlns=main_ns)
    sheet_list = ET.SubElement(book, "sheets")
    relations = ET.Element("Relationships", xmlns=rel_ns)
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for index, (name, values) in enumerate(sheets, 1):
            ET.SubElement(
                sheet_list,
                "sheet",
                name=name,
                sheetId=str(index),
                attrib={parser.REL_ID: f"rId{index}"},
            )
            ET.SubElement(
                relations,
                "Relationship",
                Id=f"rId{index}",
                Target=f"worksheets/sheet{index}.xml",
            )
            root = ET.Element("worksheet", xmlns=main_ns)
            data = ET.SubElement(root, "sheetData")
            row = ET.SubElement(data, "row")
            for address, value in values.items():
                cell = ET.SubElement(row, "c", r=address, t="inlineStr")
                inline = ET.SubElement(cell, "is")
                ET.SubElement(inline, "t").text = value
            archive.writestr(f"xl/worksheets/sheet{index}.xml", ET.tostring(root))
        archive.writestr("xl/workbook.xml", ET.tostring(book))
        archive.writestr("xl/_rels/workbook.xml.rels", ET.tostring(relations))
    return buffer.getvalue()


class ParserTests(unittest.TestCase):
    def test_corrupt_directory_offsets_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "input.zip"
            with zipfile.ZipFile(source, "w") as archive:
                archive.writestr("a.xlsx", b"placeholder")
            data = bytearray(source.read_bytes())
            offset = data.rfind(b"PK\x05\x06")
            stored = struct.unpack_from("<I", data, offset + 16)[0]
            struct.pack_into("<I", data, offset + 16, stored + 1)
            source.write_bytes(data)
            with self.assertRaises(zipfile.BadZipFile):
                parser.parse_archive(source, Path(directory) / "out.parquet")
            self.assertFalse((Path(directory) / "out.parquet").exists())

    def test_repeated_cell_not_chosen(self):
        data = f'<worksheet xmlns="{parser.NS["m"]}"><sheetData><row><c r="A1"/><c r="A1"/></row></sheetData></worksheet>'
        with self.assertRaises(ValueError):
            parser.read_cells(data.encode(), [])

    def test_date_order_and_ambiguity(self):
        self.assertEqual(parser.normalise("2017-02-03", "date")[0], date(2017, 2, 3))
        self.assertIsNone(parser.normalise("03/02/2017", "date")[0])
        self.assertIsNone(parser.normalise("2017-02-29", "date")[0])
        self.assertEqual(parser.normalise("2016-02-29", "date")[0], date(2016, 2, 29))

    def test_identifier_zeros_and_numeric_cell(self):
        self.assertEqual(
            parser.normalise("0000123456789", "identifier")[0], "0000123456789"
        )
        self.assertIsNone(parser.normalise("12345", "identifier", "n")[0])

    def test_unicode_and_missing(self):
        self.assertEqual(
            parser.normalise("  ИВАН Е\u0308ЛКИН  ", "text")[0], "ИВАН ЁЛКИН"
        )
        self.assertEqual(parser.normalise("", "text")[1], "empty_string")
        self.assertEqual(parser.normalise(None, "text")[1], "empty")
        self.assertEqual(parser.normalise("N/A", "text")[1], "missing_marker")

    def test_local_time_and_unverified_airport(self):
        self.assertIsNone(parser.normalise("24:01", "time")[0])
        self.assertEqual(parser.normalise("00:05", "time")[0], "00:05")
        self.assertEqual(
            parser.normalise("rea", "airport"),
            ("REA", "syntax_only_not_historically_verified"),
        )

    def test_shared_rich_inline_and_formula(self):
        xml = (
            f'<worksheet xmlns="{parser.NS["m"]}"><sheetData><row>'
            '<c r="E13" t="s"><v>0</v></c>'
            '<c r="B3" t="inlineStr"><is><r><t>ИВАН </t></r>'
            "<r><t>ПЕТРОВ</t></r></is></c>"
            '<c r="A9" t="str"><f>TEXT(TODAY(),"yyyy-mm-dd")</f>'
            '<v>2017-01-01</v></c><c r="F3" t="inlineStr"><is><t></t></is></c>'
            "</row></sheetData></worksheet>"
        ).encode("utf8")
        cells, types, formulas = parser.read_cells(xml, ["0000001234"])
        self.assertEqual(cells["E13"], "0000001234")
        self.assertEqual(cells["B3"], "ИВАН ПЕТРОВ")
        self.assertEqual(cells["F3"], "")
        self.assertIsNone(parser.normalise(cells["A9"], "date", types["A9"], True)[0])
        self.assertIn("A9", formulas)

    def test_bad_template_keeps_raw_without_assuming_fields(self):
        cells = {"B3": "IVAN PETROV", "A9": "2017-01-01"}
        row = parser.make_row(
            "a.zip",
            "sha",
            "b.xlsx",
            "booksha",
            "S",
            1,
            "xl/worksheets/sheet1.xml",
            cells,
            {"B3": "s"},
            {},
        )
        self.assertEqual(row["passenger_name_raw"], "IVAN PETROV")
        self.assertIsNone(row["passenger_name"])
        self.assertEqual(row["parse_status"], "needs_review")

    def test_end_to_end_one_row_per_sheet_roundtrip(self):
        values = dict(
            parser.TEMPLATE,
            B3="ПЁТР IVANOV",
            E13="0000123456789012",
            B13="001ABC",
            A9="2017-02-03",
            C9="00:05",
            A5="SU0001",
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, output = root / "input.zip", root / "tickets.parquet"
            with zipfile.ZipFile(source, "w") as archive:
                archive.writestr(
                    "folder/a.xlsx",
                    workbook_bytes([("Билет 1", values), ("Билет 2", values)]),
                )
                archive.writestr("b.xlsx", workbook_bytes([("First", values)]))
            result = parser.parse_archive(source, output, batch_size=2)
            rows = pq.read_table(output).to_pylist()
            self.assertTrue(result["complete"])
            self.assertEqual(result["row_count"], 3)
            self.assertEqual(len({row["record_id"] for row in rows}), 3)
            self.assertEqual(rows[0]["ticket_number"], "0000123456789012")
            self.assertEqual(rows[0]["departure_date"], date(2017, 2, 3))
            self.assertEqual(rows[0]["passenger_name"], "ПЁТР IVANOV")
            self.assertIn("B3", json.loads(rows[0]["raw_cells_json"]))
            with self.assertRaises(FileExistsError):
                parser.parse_archive(source, output)

    def test_sheet_read_failure_keeps_a_row_and_is_incomplete(self):
        binary = workbook_bytes([("broken", dict(parser.TEMPLATE))])
        corrupted = io.BytesIO()
        with zipfile.ZipFile(io.BytesIO(binary)) as original:
            with zipfile.ZipFile(corrupted, "w") as target:
                for name in original.namelist():
                    if not name.endswith("sheet1.xml"):
                        target.writestr(name, original.read(name))
        with tempfile.TemporaryDirectory() as directory:
            source, output = Path(directory) / "a.zip", Path(directory) / "out.parquet"
            with zipfile.ZipFile(source, "w") as archive:
                archive.writestr("a.xlsx", corrupted.getvalue())
            result = parser.parse_archive(source, output)
            self.assertFalse(result["complete"])
            self.assertEqual(result["row_count"], 1)
            self.assertEqual(
                pq.read_table(output)["parse_status"][0].as_py(), "needs_review"
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
