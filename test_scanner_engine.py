import os
import unittest
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
        self.assertEqual(result["setup_type"], "Fresh Breakout")
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
        self.assertEqual(result["setup_type"], "Breakout Watch")

    def test_low_liquidity_setup_is_not_actionable(self):
        result = scanner.calculate_vcp_score(
            make_setup(final_volume=2_000, base_volume=1_000), make_benchmark()
        )
        self.assertFalse(result["actionable"])
        self.assertTrue(any("Low liquidity" in reason for reason in result["rejection_reasons"]))

    @patch.object(scanner.requests, "get")
    def test_official_index_file_drives_membership(self, mock_get):
        response = Mock()
        response.text = "Company Name,Industry,Symbol\nAlpha Ltd,IT,ALPHA\nBeta Ltd,Bank,BETA\n"
        response.raise_for_status.return_value = None
        mock_get.return_value = response
        result = scanner.fetch_official_index_constituents()
        self.assertEqual(result["nifty50"], ["ALPHA.NS", "BETA.NS"])
        self.assertEqual(result["nifty500"], ["ALPHA.NS", "BETA.NS"])


if __name__ == "__main__":
    unittest.main()
