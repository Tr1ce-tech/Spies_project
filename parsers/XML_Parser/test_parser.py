import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq
from parse_ds_parquet import run


class XMLTests(unittest.TestCase):
    def test_empty_users_cards_and_string_IDs(self):
        text = '<PointzAggregatorUsers><user uid="0007"><name first="Имя" last="Тест"/><cards/></user><user uid="0008"><cards><card number="0012"><bonusprogramm>AA</bonusprogramm><activities/></card><card number="0013"><bonusprogramm>BB</bonusprogramm><activities><activity type="Flight"><Code>SU001</Code><Date>2017-01-02</Date><Departure>AAA</Departure><Arrival>BBB</Arrival><Fare>Y</Fare></activity></activities></card></cards></user></PointzAggregatorUsers>'
        with tempfile.TemporaryDirectory() as temp:
            source = Path(temp) / "input.xml"
            source.write_text(text)
            out = Path(temp) / "out"
            m = run(source, out)
            self.assertEqual(m["tables"], {"users": 2, "cards": 2, "activities": 1})
            self.assertEqual(
                pq.read_table(out / "users.parquet")["user_id"].to_pylist(),
                ["0007", "0008"],
            )
            self.assertEqual(
                pq.read_table(out / "cards.parquet")["card_number"].to_pylist(),
                ["0012", "0013"],
            )
            with self.assertRaises(FileExistsError):
                run(source, out)

    def test_truncated_XML_fails(self):
        with tempfile.TemporaryDirectory() as temp:
            source = Path(temp) / "input.xml"
            source.write_text('<PointzAggregatorUsers><user uid="1">')
            out = Path(temp) / "out"
            with self.assertRaises(Exception):
                run(source, out)
            self.assertFalse(out.exists())
            self.assertTrue(
                out.with_name("out.partial").joinpath("FAILED.json").exists()
            )

    def test_DTD_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            source = Path(temp) / "input.xml"
            source.write_text("<!DOCTYPE x><PointzAggregatorUsers/>")
            with self.assertRaises(ValueError):
                run(source, Path(temp) / "out")

    def test_repeated_scalar_not_chosen(self):
        with tempfile.TemporaryDirectory() as temp:
            source = Path(temp) / "input.xml"
            source.write_text(
                "<PointzAggregatorUsers><user><cards><card><bonusprogramm>A</bonusprogramm><bonusprogramm>B</bonusprogramm></card></cards></user></PointzAggregatorUsers>"
            )
            with self.assertRaises(ValueError):
                run(source, Path(temp) / "out")


if __name__ == "__main__":
    unittest.main()
