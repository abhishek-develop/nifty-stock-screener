"""
Unit Tests for Institutional-Grade Fundamental Scoring Engine.
"""

import unittest
from fundamental_scoring_engine import (
    InstitutionalFundamentalScoringEngine,
    ScoringConfig,
    compute_percentile,
    compute_proximity_score
)


class TestFundamentalScoringEngine(unittest.TestCase):

    def setUp(self):
        self.engine = InstitutionalFundamentalScoringEngine()

    def test_percentile_calculation(self):
        arr = [10.0, 20.0, 30.0, 40.0, 50.0]
        # Higher is better
        self.assertAlmostEqual(compute_percentile(50.0, arr, lower_is_better=False), 90.0)
        self.assertAlmostEqual(compute_percentile(10.0, arr, lower_is_better=False), 10.0)

        # Lower is better
        self.assertAlmostEqual(compute_percentile(10.0, arr, lower_is_better=True), 90.0)
        self.assertAlmostEqual(compute_percentile(50.0, arr, lower_is_better=True), 10.0)

    def test_proximity_score(self):
        # Current ratio ideal ~ 2.0
        self.assertEqual(compute_proximity_score(2.0, ideal_val=2.0, max_diff=2.0), 100.0)
        self.assertEqual(compute_proximity_score(1.0, ideal_val=2.0, max_diff=2.0), 50.0)

    def test_weight_redistribution_for_missing_data(self):
        stock_sparse = {
            "ticker": "TEST_SPARSE",
            "sector": "IT",
            "roe": 25.0,
            # Missing roce, operating_margin, etc.
        }
        scored = self.engine.evaluate_universe([stock_sparse])[0]
        self.assertIn("profitability_score", scored)
        self.assertGreater(scored["profitability_score"], 0.0)

    def test_bonus_and_penalty_caps(self):
        stock_star = {
            "ticker": "STAR",
            "sector": "Tech",
            "debt_to_equity": 0.0,
            "roe": 25.0,
            "roce": 25.0,
            "free_cash_flow": 100.0,
            "sales_cagr_5y": 25.0,
            "profit_cagr_5y": 25.0,
            "promoter_holding_trend": 2.0,
            "dividend_growth_trend": True
        }
        scored = self.engine.evaluate_universe([stock_star])[0]
        self.assertLessEqual(scored["bonus"], 20.0)
        self.assertEqual(scored["bonus"], 20.0)  # Capped at 20

    def test_tier_and_color_assignment(self):
        self.assertEqual(self.engine._get_score_tier_and_color(95), ("Excellent Business", "#047857"))
        self.assertEqual(self.engine._get_score_tier_and_color(85), ("Very Strong", "#10b981"))
        self.assertEqual(self.engine._get_score_tier_and_color(75), ("Good", "#34d399"))
        self.assertEqual(self.engine._get_score_tier_and_color(65), ("Average", "#f59e0b"))
        self.assertEqual(self.engine._get_score_tier_and_color(55), ("Weak", "#f97316"))
        self.assertEqual(self.engine._get_score_tier_and_color(40), ("Poor Fundamentals", "#ef4444"))

    def test_universe_ranking(self):
        stocks = [
            {"ticker": "WEAK", "sector": "Auto", "roe": 4.0, "eps": -5.0, "debt_to_equity": 200.0},
            {"ticker": "STRONG", "sector": "Auto", "roe": 28.0, "eps": 50.0, "debt_to_equity": 0.0, "free_cash_flow": 500.0},
        ]
        results = self.engine.evaluate_universe(stocks)
        self.assertEqual(results[0]["ticker"], "STRONG")
        self.assertEqual(results[0]["fundamental_rank"], 1)
        self.assertEqual(results[1]["ticker"], "WEAK")
        self.assertEqual(results[1]["fundamental_rank"], 2)


if __name__ == "__main__":
    unittest.main()
