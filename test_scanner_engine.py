import os
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd

os.environ["VCP_DISABLE_SCHEDULER"] = "1"
os.environ["VCP_SKIP_INITIAL_SCAN"] = "1"
os.environ["VCP_SKIP_UNIVERSE_SYNC"] = "1"

import vcp_scanner_web as scanner


def make_setup(final_close=106.8, final_high=107.0, final_volume=2_000_000, base_volume=650_000):
    """Create an uptrend followed by three contracting ranges and a signal bar."""
    dates = pd.bdate_range(end=pd.Timestamp.today().normalize() - pd.offsets.BDay(1), periods=241)
    trend = np.linspace(60.0, 100.0, 180)
    w1 = 101.0 + np.sin(np.linspace(0, 3 * np.pi, 20)) * 4.0
    w2 = 103.0 + np.sin(np.linspace(0, 3 * np.pi, 20)) * 2.2
    w3 = 104.5 + np.sin(np.linspace(0, 3 * np.pi, 20)) * 0.9
    close = np.concatenate([trend, w1, w2, w3, [final_close]])
    spread = np.concatenate([
        np.full(180, 1.0), np.full(20, 1.4), np.full(20, 0.8), np.full(20, 0.35), [0.45]
    ])
    volume = np.concatenate([
        np.full(180, 1_200_000), np.full(20, 1_100_000),
        np.full(20, 900_000), np.full(20, base_volume), [final_volume]
    ])
    frame = pd.DataFrame({
        "open": close - 0.15,
        "high": close + spread,
        "low": close - spread,
        "close": close,
        "volume": volume,
    }, index=dates)
    frame.iloc[-1, frame.columns.get_loc("high")] = final_high
    return frame


def make_benchmark():
    dates = pd.bdate_range(end=pd.Timestamp.today().normalize(), periods=241)
    close = np.linspace(100.0, 112.0, len(dates))
    return pd.DataFrame({
        "open": close - 0.2, "high": close + 0.4, "low": close - 0.4,
        "close": close, "volume": np.full(len(dates), 10_000_000),
    }, index=dates)


class TestScannerEngine(unittest.TestCase):
    def test_close_and_volume_are_required_for_confirmed_breakout(self):
        result = scanner.calculate_vcp_score(make_setup(), make_benchmark())
        self.assertEqual(result["status"], "breakout")
        self.assertEqual(result["setup_type"], "VCP Breakout")
        self.assertGreaterEqual(result["volume_ratio"], 1.3)

    def test_intraday_wick_is_not_mislabeled_as_breakout(self):
        result = scanner.calculate_vcp_score(
            make_setup(final_close=104.8, final_high=108.0, final_volume=2_000_000),
            make_benchmark(),
        )
        self.assertNotEqual(result["status"], "breakout")
        self.assertTrue(any("Pivot rejection" in reason for reason in result["rejection_reasons"]))

    def test_near_pivot_dry_up_is_a_watch_not_a_confirmation(self):
        result = scanner.calculate_vcp_score(
            make_setup(final_close=105.0, final_high=105.3, final_volume=500_000),
            make_benchmark(),
        )
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["setup_type"], "VCP Watch")

    def test_low_liquidity_setup_is_not_actionable(self):
        result = scanner.calculate_vcp_score(
            make_setup(final_volume=2_000, base_volume=1_000), make_benchmark()
        )
        self.assertFalse(result["actionable"])
        self.assertTrue(any("Low liquidity" in reason for reason in result["rejection_reasons"]))

    @patch.object(scanner, "_daily_signal_bar_complete", return_value=False)
    def test_intraday_breakout_waits_for_daily_close(self, _mock_complete):
        result = scanner.calculate_vcp_score(make_setup(), make_benchmark())
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["setup_type"], "Intraday Breakout Watch")
        self.assertFalse(result["signal_bar_complete"])

    @patch.object(scanner, "calculate_vcp_score", return_value=None)
    @patch.object(scanner, "fetch_stock_data")
    def test_watchlist_symbol_is_normalized_to_nse(self, mock_fetch, _mock_score):
        mock_fetch.return_value = make_setup()
        self.assertIsNone(scanner.scan_stock("SBIN"))
        mock_fetch.assert_called_once_with("SBIN.NS")

    def test_manual_scan_rejects_unknown_universe(self):
        response = scanner.app.test_client().post("/api/scan", json={"universe": "not-a-real-index"})
        self.assertEqual(response.status_code, 400)

    def test_recent_complete_cache_skips_duplicate_startup_scan(self):
        universes = {name: [{}] for name in ["nifty500", "ipo"]}
        with patch.dict(scanner.scan_cache, {"last_scan": datetime.now().isoformat(), "results": universes}, clear=True):
            self.assertTrue(scanner.scan_cache_is_fresh())
        old_scan = (datetime.now() - timedelta(hours=2)).isoformat()
        with patch.dict(scanner.scan_cache, {"last_scan": old_scan, "results": universes}, clear=True):
            self.assertFalse(scanner.scan_cache_is_fresh())

    @patch.object(scanner.requests, "get")
    def test_official_index_file_drives_membership(self, mock_get):
        response = Mock()
        response.text = "Company Name,Industry,Symbol\nAlpha Ltd,IT,ALPHA\nBeta Ltd,Bank,BETA\n"
        response.raise_for_status.return_value = None
        mock_get.return_value = response
        result = scanner.fetch_official_index_constituents()
        self.assertEqual(result["nifty50"], ["ALPHA.NS", "BETA.NS"])
        self.assertEqual(result["nifty500"], ["ALPHA.NS", "BETA.NS"])

    def test_lowest_volume_is_an_alert_not_an_automatic_entry(self):
        frame = make_setup(final_close=104.6, final_high=104.8, final_volume=50_000)
        frame.iloc[-1, frame.columns.get_loc("open")] = 104.55
        frame.iloc[-1, frame.columns.get_loc("low")] = 104.3
        result = scanner.calculate_vcp_score(frame, make_benchmark())
        self.assertTrue(result["dry_up_alert"])
        self.assertEqual(result["trade_state"], "watch_trigger")
        self.assertEqual(result["setup_family"], "Volume Dry-Up Alert")

    def test_breakout_after_prior_dry_up_requires_price_and_volume_confirmation(self):
        frame = make_setup(final_volume=2_000_000)
        frame.iloc[-2, frame.columns.get_loc("volume")] = 50_000
        frame.iloc[-2, frame.columns.get_loc("high")] = frame.iloc[-2]["close"] + 0.08
        frame.iloc[-2, frame.columns.get_loc("low")] = frame.iloc[-2]["close"] - 0.08
        result = scanner.calculate_vcp_score(frame, make_benchmark())
        self.assertTrue(result["dry_up_confirmed"])
        self.assertIn("Volume Dry-Up", result["setup_families"])

    def test_recent_listing_is_classified_only_with_official_listing_date(self):
        listing_date = (pd.Timestamp.today() - pd.Timedelta(days=180)).date().isoformat()
        result = scanner.calculate_vcp_score(
            make_setup(final_volume=4_000_000, base_volume=1_500_000),
            make_benchmark(),
            listing_date=listing_date,
        )
        self.assertTrue(result["is_recent_listing"])
        self.assertIn("IPO Base", result["setup_families"])

    def test_long_base_breakout_is_a_distinct_setup_family(self):
        dates = pd.bdate_range(end=pd.Timestamp.today().normalize() - pd.offsets.BDay(1), periods=270)
        close = 100 + np.sin(np.linspace(0, 10 * np.pi, 270)) * 3
        close[-60:-1] = 103 + np.sin(np.linspace(0, 5 * np.pi, 59)) * 1.2
        close[-1] = 107.0
        volume = np.full(270, 1_500_000.0)
        volume[-1] = 3_500_000
        frame = pd.DataFrame({
            "open": close - 0.15, "high": close + 0.35, "low": close - 0.35,
            "close": close, "volume": volume,
        }, index=dates)
        result = scanner.calculate_vcp_score(frame, make_benchmark())
        self.assertEqual(result["setup_family"], "Long Base Breakout")
        self.assertIn("Long Base", result["setup_families"])

    def test_daily_focus_applies_market_and_concentration_caps(self):
        rows = []
        sectors = ["IT", "IT", "IT", "Bank", "Auto", "Energy", "Consumer"]
        for index, sector in enumerate(sectors):
            rows.append({
                "ticker": f"S{index}", "sector": sector, "market_regime": "favorable",
                "trend_score": 22, "rs_3m_pct": 10, "trade_state": "trade_now",
                "status": "breakout", "actionable": True, "swing_score": 90,
                "fundamental_score": 80, "eps": 10, "roe_pct": 18,
                "avg_turnover_cr": 50, "risk_pct": 4, "data_age_business_days": 0,
                "volume_ratio": 1.8, "breakout_distance_pct": -1,
                "signal_bar_complete": True, "setup_family": "VCP Breakout",
                "setup_families": ["VCP"], "evidence": ["Confirmed"],
                "is_recent_listing": index in {4, 5},
            })
        focus = scanner.build_daily_focus(rows)
        self.assertEqual(len(focus["trade_now"]), 5)
        self.assertLessEqual(sum(r["sector"] == "IT" for r in focus["trade_now"]), 2)
        self.assertLessEqual(sum(bool(r["is_recent_listing"]) for r in focus["trade_now"]), 1)

    def test_risk_off_market_returns_no_new_trades(self):
        row = {
            "ticker": "RISK", "sector": "IT", "market_regime": "risk_off",
            "trend_score": 22, "rs_3m_pct": 10, "trade_state": "trade_now",
            "status": "breakout", "actionable": True, "swing_score": 95,
            "avg_turnover_cr": 100, "risk_pct": 3, "data_age_business_days": 0,
            "volume_ratio": 2, "signal_bar_complete": True,
        }
        self.assertEqual(scanner.build_daily_focus([row])["trade_now"], [])


class TestFrontendRegression(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = Path("templates/index.html").read_text(encoding="utf-8")

    def test_watchlist_is_the_active_navigation_dataset(self):
        self.assertIn("if (activeTab === 'watchlist') return watchlistResults;", self.html)

    def test_daily_focus_is_the_default_decision_view(self):
        self.assertIn("let activeTab = 'daily_focus';", self.html)
        self.assertIn("/api/results/daily_focus", self.html)

    def test_old_pattern_tabs_are_removed_from_primary_navigation(self):
        self.assertNotIn("switchTab('ipo_breakouts'", self.html)
        self.assertNotIn("switchTab('forming'", self.html)

    def test_broken_timeframe_controls_are_removed(self):
        self.assertNotIn("changeTimeframe", self.html)
        self.assertNotIn("timeframeBar", self.html)

    def test_volume_panel_has_readable_height(self):
        self.assertIn("scaleMargins: { top: 0.68, bottom: 0 }", self.html)


if __name__ == "__main__":
    unittest.main()
