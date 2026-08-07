# 🇮🇳 Indian Stock VCP Scanner - Full-Stack Web App

A complete **web-based VCP scanner** for Indian stocks (NSE/BSE) that runs on your local machine. It fetches real data from Yahoo Finance, calculates VCP scores, and presents results in a beautiful dark-themed dashboard.

![Scanner Preview](https://via.placeholder.com/800x400/0a0e1a/0ea5e9?text=VCP+Scanner+Dashboard)

---

## ✨ Features

- **Real-time scanning** of NSE stocks via Yahoo Finance
- **Breakout Radar (0-100)** based on observable daily-market evidence:
  - Confirmed close above the pivot (a wick alone does not count)
  - Volume expansion for breakouts / volume dry-up before breakouts
  - Stage-2 moving-average trend, including a rising 200-day average
  - Three-window volatility contraction and price tightness
  - 3/6-month relative strength versus NIFTY 50
  - Liquidity, data freshness, extension, and defined-stop risk gates
- **Auto-scan** every 30 minutes during market hours (9:15 AM - 3:30 PM IST)
- **Multiple universes**: IPOs, Nifty 500, Nifty 200, Smallcap 100, Custom
- **Visual dashboard** with mini charts, volume bars, and score rings
- **Detail modal** with trade setup (entry, stop loss, position sizing)
- **CSV export** for further analysis
- **Keyboard shortcuts**: `Ctrl+Enter` to scan, `Escape` to close modal

---

## 🚀 Quick Start

### Option 1: One-Click Setup (Recommended)

**Linux/Mac:**
```bash
bash setup.sh
```

**Windows:**
```cmd
setup.bat
```

### Option 2: Manual Setup

```bash
# 1. Install Python 3.8+ if not already installed

# 2. Create virtual environment (optional but recommended)
python -m venv venv

# 3. Activate it
# Linux/Mac:
source venv/bin/activate
# Windows:
venv\Scripts\activate.bat

# 4. Install dependencies
pip install -r requirements_web.txt

# 5. Run the scanner
python vcp_scanner_web.py

# 6. Open browser
curl http://localhost:5000
```

---

## 📁 File Structure

```
vcp-scanner/
├── vcp_scanner_web.py      ← Main application (Flask backend + embedded frontend)
├── requirements_web.txt    ← Python dependencies
├── setup.sh                ← Linux/Mac setup script
├── setup.bat               ← Windows setup script
├── scan_cache.json         ← Auto-created cache file
└── README.md               ← This file
```

**Single file deployment!** Everything is in `vcp_scanner_web.py`. No template folders needed.

---

## 🖥️ Usage

### Web Interface

1. **Select Universe**: Choose from IPOs, Nifty 500, Nifty 200, Smallcap, or enter custom tickers
2. **Set Filters**: Min VCP score (70+ recommended), price range
3. **Click "Scan Now"**: Fetches fresh data and calculates VCP scores
4. **View Results**: Start with Breakout Radar, then inspect Confirmed Breakouts and Near Pivot setups
5. **Click "Detail"**: See full analysis with trade setup
6. **Export CSV**: Download results for Excel/further analysis

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Main dashboard |
| `/api/scan` | POST | Run scan with filters |
| `/api/results/<universe>` | GET | Get cached results |
| `/api/results/top_picks` | GET | Get up to 15 actionable breakout-radar candidates |
| `/api/stock/<ticker>` | GET | Single stock detail |
| `/api/universes` | GET | List available universes |
| `/api/status` | GET | Scanner status |
| `/api/export/<universe>` | GET | Export CSV |

### Example API Call

```bash
curl -X POST http://localhost:5000/api/scan \
  -H "Content-Type: application/json" \
  -d '{"universe": "ipo", "min_score": 70}'
```

---

## ⏰ Auto-Scan Schedule

The scanner automatically runs:
- **Every 30 minutes** between 9:15 AM - 3:30 PM IST (Mon-Fri)
- **At 9:20 AM** for market open scan
- **On startup** for initial data load

To customize the schedule, edit the `scheduler.add_job()` calls in `vcp_scanner_web.py`.

---

## 🎯 Signal Definitions

- **Fresh Breakout:** daily close at least 0.3% above the prior 20-session high, at least 1.3× baseline volume, valid contracting base, strong trend, and no excessive extension.
- **Breakout Watch:** within 5% of the pivot with a valid base and Stage-2 trend. It is not a buy trigger.
- **Actionable:** score of at least 65, fresh data, average turnover of at least ₹5 Cr/day, price of at least ₹20, and stop risk no greater than 8%.
- **Rejected:** the API exposes explicit reasons such as stale data, low liquidity, weak trend, pivot rejection, excessive extension, or wide stop risk.

---

## 🔧 Customization

### Add Your Own Stocks

Edit the `STOCK_UNIVERSES` dict in `vcp_scanner_web.py`:

```python
STOCK_UNIVERSES["my_watchlist"] = [
    "RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS"
]
```

### Change Scan Frequency

```python
# Scan every 15 minutes instead of 30
scheduler.add_job(auto_scan, 'cron', hour='9-15', minute='*/15', day_of_week='mon-fri')
```

### Adjust VCP Scoring

Modify the scoring logic in `calculate_vcp_score()` to match your trading style.

---

## 🔄 Integration with Broker APIs

For real-time data (instead of Yahoo Finance's ~15min delay), integrate with:

- **Zerodha Kite Connect**: Replace `fetch_stock_data()` with Kite API calls
- **Upstox API**: Similar integration pattern
- **Angel One SmartAPI**: Real-time tick data

Example pseudocode:
```python
def fetch_stock_data_kite(ticker):
    kite = KiteConnect(api_key="your_key")
    data = kite.historical_data(instrument_token, "day", "3month")
    return pd.DataFrame(data)
```

---

## 📱 Mobile Access

Since the app runs on `0.0.0.0:5000`, you can access it from any device on your network:

1. Find your computer's IP: `ipconfig` (Windows) or `ifconfig` (Mac/Linux)
2. On your phone, open: `http://YOUR_IP:5000`

---

## ⚠️ Important Notes

- **Universes**: Index membership comes from official NSE constituent CSVs; arbitrary equity-master slices are never labeled as Nifty indexes.
- **Data Source**: Yahoo Finance fallback data may be delayed. Zerodha credentials are preferred when configured. Not suitable for intraday execution or high-frequency trading.
- **Corporate actions**: Yahoo OHLC data is adjusted for splits/dividends before technical calculations.
- **Research only**: A high score is a probability-ranked setup, not a forecast or assurance of returns.
- **Rate Limits**: Yahoo may throttle requests. For large universes, add `time.sleep()` between calls.
- **NSE Tickers**: Must end with `.NS` (e.g., `RELIANCE.NS`)
- **Not Financial Advice**: This is a research tool. Always do your own analysis.

---

## 🐛 Troubleshooting

### "Module not found" error
```bash
pip install -r requirements_web.txt
```

### "Port 5000 already in use"
Change port in `vcp_scanner_web.py`:
```python
app.run(host='0.0.0.0', port=5001)  # Use 5001 instead
```

### Yahoo Finance rate limited
Add delay between requests in `scan_universe()`:
```python
import time
time.sleep(0.5)  # 500ms delay between stocks
```

### Slow scanning for Nifty 500
The full Nifty 500 scan takes ~5-10 minutes. Use IPO or custom universe for faster results.

---

## 🗺️ Roadmap

- [ ] Telegram/Email breakout alerts
- [ ] Zerodha Kite integration for real-time data
- [ ] Backtesting module
- [ ] Multi-timeframe analysis (daily + weekly)
- [ ] Sector rotation detection
- [ ] Earnings date filtering
- [ ] Docker container for easy deployment

---

## 📜 License

MIT License — free to use, modify, and distribute.

Built for Indian traders who believe in **buying right and sitting tight** 🎯

---

## 💬 Support

If you find this useful, star the repo and share with fellow traders!

Happy scanning! 🇮🇳📈
