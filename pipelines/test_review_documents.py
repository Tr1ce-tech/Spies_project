import unittest

import pandas as pd

from pipelines.review_documents import birth_options, case_id, choose


class DocumentReviewTests(unittest.TestCase):
    def test_date_order_remains_unresolved(self):
        self.assertEqual(birth_options("01/02/2000"), {"2000-01-02", "2000-02-01"})
        self.assertEqual(birth_options("13/02/2000"), {"2000-02-13"})
        self.assertEqual(birth_options("2000-02-30"), set())
        self.assertEqual(birth_options("N/A"), set())

    def test_sampling_is_independent_of_input_row_order(self):
        frame = pd.DataFrame(
            [
                dict(
                    case_id=case_id("ticket_block", [str(i), "2017-01-01", "SU1"]),
                    stratum=s,
                )
                for i, s in enumerate(["a"] * 30 + ["b"] * 12)
            ]
        )
        first = choose(frame, 6)
        second = choose(frame.sample(frac=1, random_state=13), 6)
        self.assertEqual(first.case_id.tolist(), second.case_id.tolist())
        self.assertEqual(first.groupby("stratum").size().to_dict(), {"a": 6, "b": 6})

    def test_case_keys_do_not_drop_leading_zeroes_or_unit_kind(self):
        self.assertNotEqual(
            case_id("ticket_block", ["0012"]), case_id("ticket_block", ["12"])
        )
        self.assertNotEqual(
            case_id("ticket_block", ["0012"]), case_id("held_component", ["0012"])
        )


if __name__ == "__main__":
    unittest.main()
