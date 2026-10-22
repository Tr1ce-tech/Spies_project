import unittest

from pipelines.build_pattern_catalog import rules


class PatternCatalogTests(unittest.TestCase):
    def test_equal_quantile_does_not_trigger_strict_exceedance(self):
        row = dict(
            flight_days=30,
            distinct_routes=20,
            top_route_share=0.1,
            top_route_flight_days=3,
            peak7_share=0.2,
            route_rarity=7,
        )
        cut = dict(flight_days=30, distinct_routes=20, peak7_share=0.2, route_rarity=7)
        self.assertEqual(rules(row, cut), [])

    def test_repeat_boundary_and_small_sample_guards(self):
        row = dict(
            flight_days=5,
            distinct_routes=2,
            top_route_share=0.8,
            top_route_flight_days=5,
            peak7_share=1,
            route_rarity=9,
        )
        cut = dict(flight_days=30, distinct_routes=20, peak7_share=0.2, route_rarity=7)
        self.assertEqual(rules(row, cut), ["repeated_route"])
        row["top_route_flight_days"] = 4
        self.assertEqual(rules(row, cut), [])


if __name__ == "__main__":
    unittest.main()
