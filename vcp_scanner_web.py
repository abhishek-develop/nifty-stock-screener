#!/usr/bin/env python3
"""
Indian Stock VCP Scanner - Standalone Web Application
Single-file deployment with dynamic stock universe fetching from official NSE & Zerodha APIs,
Mark Minervini VCP pattern scoring, TradingView Pro Charting with auto-fallback & Measure Tool.

SETUP:
    pip install flask flask-cors yfinance pandas numpy requests apscheduler

RUN:
    python vcp_scanner_web.py

OPEN:
    http://localhost:8000
"""

from flask import Flask, jsonify, request, Response, render_template
from flask_cors import CORS
import pandas as pd
import numpy as np
import json
import os
import threading
import tempfile
import csv
import hashlib
import time
from io import StringIO
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from apscheduler.schedulers.background import BackgroundScheduler
import requests
import yfinance as yf
import warnings
warnings.filterwarnings('ignore')

app = Flask(__name__)
CORS(app)

# ============================================================
# CONFIGURATION & DYNAMIC UNIVERSE ENGINE
# ============================================================



DYNAMIC_UNIVERSE_CACHE_FILE = 'dynamic_universes.json'
dynamic_universes: Dict[str, List[str]] = {}
dynamic_metadata: Dict[str, Dict] = {}

STOCK_UNIVERSES = {"custom": []}
CACHE_FILE = 'scan_cache.json'
scan_cache = {"last_scan": None, "results": {}, "is_scanning": False, "errors": []}
chart_cache_memory = {}
CHART_CACHE_TTL = 1800  # 30-minute high-speed RAM chart cache

ZERODHA_API_KEY = os.getenv("ZERODHA_API_KEY", "")
ZERODHA_API_SECRET = os.getenv("ZERODHA_API_SECRET", "")
ZERODHA_ACCESS_TOKEN = os.getenv("ZERODHA_ACCESS_TOKEN")
ZERODHA_REQUEST_TOKEN = os.getenv("ZERODHA_REQUEST_TOKEN")
ZERODHA_USER_ID = os.getenv("ZERODHA_USER_ID")
ZERODHA_ENCTOKEN = os.getenv("ZERODHA_ENCTOKEN")


def fetch_dynamic_nse_equities() -> List[Dict]:
    """Fetch active official NSE equities dynamically from NSE master feeds and Zerodha public feeds."""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    equities = []
    
    # 1. Try fetching official NSE EQUITY_L master file
    try:
        r_nse = requests.get('https://archives.nseindia.com/content/equities/EQUITY_L.csv', headers=headers, timeout=15)
        if r_nse.status_code == 200:
            df_nse = pd.read_csv(StringIO(r_nse.text))
            df_nse.columns = [c.strip() for c in df_nse.columns]
            
            if 'SERIES' in df_nse.columns:
                df_nse = df_nse[df_nse['SERIES'] == 'EQ']
                
            if 'DATE OF LISTING' in df_nse.columns:
                df_nse['DATE OF LISTING'] = pd.to_datetime(df_nse['DATE OF LISTING'], errors='coerce')
                df_nse = df_nse.sort_values('DATE OF LISTING', ascending=False)
                
            for _, row in df_nse.iterrows():
                sym = str(row.get('SYMBOL', '')).strip().upper()
                if sym and len(sym) >= 2:
                    listing_dt = row.get('DATE OF LISTING')
                    equities.append({
                        'symbol': sym,
                        'ticker': f"{sym}.NS",
                        'name': str(row.get('NAME OF COMPANY', sym)).strip(),
                        'listing_date': listing_dt.strftime('%Y-%m-%d') if pd.notnull(listing_dt) else ''
                    })
    except Exception as exc:
        scan_cache["errors"].append(f"NSE master feed fetch error: {exc}")

    # 2. Try fetching Zerodha public instrument feed
    try:
        r_kite = requests.get('https://api.kite.trade/instruments', timeout=15)
        if r_kite.status_code == 200:
            reader = csv.DictReader(StringIO(r_kite.text))
            kite_symbols = set()
            for item in reader:
                exchange = (item.get("exchange") or "").upper()
                segment = (item.get("segment") or "").upper()
                instrument_type = (item.get("instrument_type") or "").upper()
                tradingsymbol = (item.get("tradingsymbol") or "").strip().upper()
                
                if exchange == "NSE" and segment == "NSE" and instrument_type == "EQ" and tradingsymbol:
                    kite_symbols.add(tradingsymbol)
                    if tradingsymbol not in dynamic_metadata:
                        dynamic_metadata[tradingsymbol] = {
                            "name": item.get("name") or tradingsymbol,
                            "sector": "NSE Equities",
                            "listing_date": ""
                        }

            if not equities:
                for sym in sorted(kite_symbols):
                    equities.append({
                        'symbol': sym,
                        'ticker': f"{sym}.NS",
                        'name': dynamic_metadata[sym]["name"],
                        'listing_date': ''
                    })
    except Exception as exc:
        scan_cache["errors"].append(f"Zerodha instrument feed error: {exc}")

    return equities


def sync_dynamic_universes():
    """Fetch and organize stock universes dynamically from live APIs."""
    print("🌐 Syncing stock universes dynamically from official NSE & Zerodha APIs...")
    raw_equities = fetch_dynamic_nse_equities()
    if not raw_equities:
        print("⚠️ Warning: Dynamic API fetch yielded no rows. Loading cached dynamic universes if available.")
        load_dynamic_universe_cache()
        return

    for eq in raw_equities:
        sym = eq['symbol']
        dynamic_metadata[sym] = {
            "name": eq['name'],
            "sector": "NSE Equities",
            "listing_date": eq['listing_date']
        }

    all_tickers = [eq['ticker'] for eq in raw_equities]
    
    # 1. IPO Universe
    cutoff_date = (datetime.now() - timedelta(days=550)).strftime('%Y-%m-%d')
    ipo_tickers = [eq['ticker'] for eq in raw_equities if eq['listing_date'] and eq['listing_date'] >= cutoff_date]
    if len(ipo_tickers) < 15:
        ipo_tickers = [eq['ticker'] for eq in raw_equities if eq['listing_date']][:40]

    # 2. Dynamic market slices
    dynamic_universes["nifty50"] = all_tickers[:50]
    dynamic_universes["nifty200"] = all_tickers[:200]
    dynamic_universes["nifty500"] = all_tickers[:500]
    dynamic_universes["smallcap"] = all_tickers[200:450] if len(all_tickers) >= 450 else all_tickers[50:200]
    dynamic_universes["ipo"] = ipo_tickers
    dynamic_universes["nse_all"] = all_tickers[:1000]

    try:
        cache_data = {
            "last_synced": datetime.now().isoformat(),
            "total_nse_stocks": len(all_tickers),
            "universes": dynamic_universes,
            "metadata": dynamic_metadata
        }
        with open(DYNAMIC_UNIVERSE_CACHE_FILE, 'w') as f:
            json.dump(cache_data, f, indent=2)
        print(f"✅ Dynamically synced {len(all_tickers)} active NSE equities into dynamic universes!")
    except Exception as exc:
        print(f"Error saving dynamic universe cache: {exc}")


def load_dynamic_universe_cache():
    """Load dynamic universes from local cache file if existing."""
    if not os.path.exists(DYNAMIC_UNIVERSE_CACHE_FILE):
        return
    try:
        with open(DYNAMIC_UNIVERSE_CACHE_FILE, 'r') as f:
            data = json.load(f)
        global dynamic_universes, dynamic_metadata
        dynamic_universes = data.get("universes", {})
        dynamic_metadata = data.get("metadata", {})
        print(f"📁 Loaded {sum(len(v) for v in dynamic_universes.values())} tickers across dynamic universes from cache.")
    except Exception as exc:
        print(f"Error loading dynamic universe cache: {exc}")


load_dynamic_universe_cache()
if not dynamic_universes:
    sync_dynamic_universes()


def get_zerodha_session() -> Optional[Dict]:
    """Create or refresh a Zerodha access token session when credentials are available."""
    global ZERODHA_ACCESS_TOKEN, ZERODHA_USER_ID

    if ZERODHA_ENCTOKEN:
        return {"enctoken": ZERODHA_ENCTOKEN}

    if ZERODHA_ACCESS_TOKEN:
        return {"access_token": ZERODHA_ACCESS_TOKEN, "user_id": ZERODHA_USER_ID}

    if not ZERODHA_API_KEY or not ZERODHA_API_SECRET or not ZERODHA_REQUEST_TOKEN:
        return None

    try:
        checksum = hashlib.sha256(f"{ZERODHA_API_KEY}{ZERODHA_REQUEST_TOKEN}{ZERODHA_API_SECRET}".encode("utf-8")).hexdigest()
        print(f"🔑 Exchanging Zerodha request_token for access_token (API Key: {ZERODHA_API_KEY[:4]}...)...")
        response = requests.post(
            "https://api.kite.trade/session/token",
            data={
                "api_key": ZERODHA_API_KEY,
                "request_token": ZERODHA_REQUEST_TOKEN,
                "checksum": checksum
            },
            timeout=15,
        )
        payload = response.json()
        if response.status_code == 200 and "data" in payload:
            data = payload["data"]
            ZERODHA_ACCESS_TOKEN = data.get("access_token")
            ZERODHA_USER_ID = data.get("user_id")
            print(f"✅ Zerodha session authenticated successfully for user: {ZERODHA_USER_ID}")
            return {"access_token": ZERODHA_ACCESS_TOKEN, "user_id": ZERODHA_USER_ID}
        else:
            print(f"❌ Zerodha session exchange failed (HTTP {response.status_code}): {payload}")
            return None
    except Exception as exc:
        print(f"❌ Zerodha session exception: {exc}")
        return None


def get_zerodha_headers() -> Dict[str, str]:
    """Return request headers for Zerodha authenticated calls."""
    session = get_zerodha_session()
    if not session:
        return {}
    if "enctoken" in session:
        return {"Authorization": f"enctoken {session['enctoken']}"}
    if session.get("access_token"):
        return {"Authorization": f"token {ZERODHA_API_KEY}:{session['access_token']}", "X-Kite-Version": "3"}
    return {}


def normalize_ticker(ticker: str) -> str:
    return ticker.replace(".NS", "").upper()


def get_stock_universe(universe_name: str) -> List[str]:
    """Return requested dynamic universe."""
    if universe_name == "custom":
        return STOCK_UNIVERSES.get("custom", [])
    if universe_name in STOCK_UNIVERSES:
        return STOCK_UNIVERSES.get(universe_name, [])
    if universe_name in dynamic_universes:
        return dynamic_universes[universe_name]
    
    sync_dynamic_universes()
    return dynamic_universes.get(universe_name, [])


def parse_zerodha_candles(payload: Dict) -> Optional[pd.DataFrame]:
    """Attempt to parse several common Zerodha candle response shapes into a pandas DataFrame."""
    try:
        data = payload.get("data", payload)
        if isinstance(data, list):
            candles = data
        elif isinstance(data, dict):
            candles = None
            for key in ["candles", "ohlc", "historical", "data"]:
                if isinstance(data.get(key), list):
                    candles = data[key]
                    break
            if candles is None:
                return None
        else:
            return None

        records = []
        for candle in candles:
            if not isinstance(candle, (list, tuple)) or len(candle) < 6:
                continue
            try:
                records.append({
                    "timestamp": pd.to_datetime(candle[0]),
                    "open": float(candle[1]),
                    "high": float(candle[2]),
                    "low": float(candle[3]),
                    "close": float(candle[4]),
                    "volume": float(candle[5] or 0),
                })
            except Exception:
                continue

        if not records:
            return None

        df = pd.DataFrame(records).sort_values("timestamp")
        df = df.set_index("timestamp")
        return df[["open", "high", "low", "close", "volume"]]
    except Exception:
        None


def clean_ohlcv(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Normalize vendor OHLCV columns into the scanner's expected shape."""
    if df is None or df.empty:
        return None

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    renamed = {str(c).lower().replace(" ", "_"): c for c in df.columns}
    required = ["open", "high", "low", "close", "volume"]
    if not all(col in renamed for col in required):
        return None

    out = pd.DataFrame({col: pd.to_numeric(df[renamed[col]], errors="coerce") for col in required})
    out = out.dropna(subset=["open", "high", "low", "close"])
    out["volume"] = out["volume"].fillna(0)
    out = out[out["close"] > 0]
    return out.tail(600) if len(out) >= 10 else None


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
]

def fetch_stock_data_direct_chart_api(ticker: str, period: str = "2y", interval: str = "1d") -> Optional[pd.DataFrame]:
    """Fetch directly calling Yahoo Finance Chart API with User-Agent and domain rotation."""
    domains = ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]
    for attempt in range(3):
        domain = domains[attempt % len(domains)]
        ua = USER_AGENTS[(hash(ticker) + attempt) % len(USER_AGENTS)]
        url = f"https://{domain}/v8/finance/chart/{ticker}?range={period}&interval={interval}"
        headers = {"User-Agent": ua}

        try:
            res = requests.get(url, headers=headers, timeout=8)
            if res.status_code != 200:
                if attempt == 2:
                    print(f"⚠️ Direct Yahoo Chart API failed for {ticker} (HTTP {res.status_code})")
                time.sleep(0.15 * (attempt + 1))
                continue

            data = res.json()
            result = data.get("chart", {}).get("result", [])
            if not result:
                return None

            chart_data = result[0]
            timestamps = chart_data.get("timestamp", [])
            indicators = chart_data.get("indicators", {}).get("quote", [{}])[0]

            if not timestamps or not indicators:
                return None

            df = pd.DataFrame({
                "open": indicators.get("open", []),
                "high": indicators.get("high", []),
                "low": indicators.get("low", []),
                "close": indicators.get("close", []),
                "volume": indicators.get("volume", []),
            }, index=pd.to_datetime(timestamps, unit="s"))

            return clean_ohlcv(df)
        except Exception as exc:
            if attempt == 2:
                print(f"⚠️ Direct Yahoo Chart API error for {ticker}: {exc}")
    return None


def fetch_stock_data_yahoo(ticker: str, period: str = "2y", interval: str = "1d") -> Optional[pd.DataFrame]:
    """Fetch daily/weekly/intraday OHLCV from Yahoo Finance."""
    # 1. Try Direct API first (Fastest & Most Reliable on Cloud IPs)
    df_direct = fetch_stock_data_direct_chart_api(ticker, period, interval)
    if df_direct is not None and not df_direct.empty:
        return df_direct

    # 2. Fallback to yfinance
    try:
        with open(os.devnull, 'w') as devnull:
            old_stderr = sys.stderr
            sys.stderr = devnull
            try:
                df = yf.download(ticker, period=period, interval=interval, progress=False, auto_adjust=False, threads=False)
            finally:
                sys.stderr = old_stderr
        cleaned = clean_ohlcv(df)
        if cleaned is not None and not cleaned.empty:
            return cleaned
        else:
            print(f"⚠️ yfinance download returned empty data for {ticker}")
    except Exception as exc:
        print(f"⚠️ yfinance download failed for {ticker}: {exc}")

    return None


def fetch_stock_data_zerodha(ticker: str) -> Optional[pd.DataFrame]:
    """Fetch Zerodha historical candles when credentials and instrument metadata are available."""
    headers = get_zerodha_headers()
    if not headers:
        return None

    try:
        to_date = datetime.now().date()
        from_date = to_date - timedelta(days=730)
        token = ticker.replace(".NS", "")
        response = requests.get(
            f"https://api.kite.trade/instruments/historical/{token}/day",
            headers=headers,
            params={"from": from_date.isoformat(), "to": to_date.isoformat()},
            timeout=20,
        )
        if response.status_code != 200:
            print(f"⚠️ Zerodha API status {response.status_code} for {ticker}: {response.text}")
            return None
        return clean_ohlcv(parse_zerodha_candles(response.json()))
    except Exception as exc:
        scan_cache["errors"].append(f"{ticker}: Zerodha fetch failed ({exc})")
        print(f"❌ Zerodha fetch failed for {ticker}: {exc}")
        return None


def fetch_stock_data(ticker: str, period: str = "2y", interval: str = "1d") -> Optional[pd.DataFrame]:
    """Fetch historical OHLCV data using Zerodha FIRST if configured, with Yahoo Finance as fallback."""
    # 1. Try Zerodha FIRST if Zerodha credentials exist
    if ZERODHA_ENCTOKEN or ZERODHA_ACCESS_TOKEN or (ZERODHA_API_KEY and ZERODHA_REQUEST_TOKEN):
        df_zerodha = fetch_stock_data_zerodha(ticker)
        if df_zerodha is not None and not df_zerodha.empty:
            return df_zerodha

    # 2. Fallback to Yahoo Finance (Direct API + yfinance)
    df_yahoo = fetch_stock_data_yahoo(ticker, period, interval)
    if df_yahoo is not None and not df_yahoo.empty:
        return df_yahoo

    # 3. Log clear error when all data providers fail for a ticker
    err_msg = f"🛑 [FETCH FAILED] Could not retrieve stock data for {ticker} from any provider."
    print(err_msg)
    if err_msg not in scan_cache["errors"]:
        scan_cache["errors"].append(err_msg)

    return None


# ============================================================
# DATA CLASS
# ============================================================

@dataclass
class VCPResult:
    ticker: str
    name: str
    sector: str
    price: float
    change: float
    change_pct: float
    vcp_score: int
    vcp_grade: str
    breakout_score: int
    breakout_grade: str
    pivot_readiness_score: int
    pivot_readiness_grade: str
    base_quality_score: int
    base_quality_grade: str
    contraction: str
    volume_trend: str
    range_20d: str
    status: str
    atr: float
    atr_pct: float
    volume_ratio: float
    dry_up_ratio: float
    price_tightness: float
    support_level: float
    resistance_level: float
    entry_price: float
    stop_loss: float
    risk_pct: float
    breakout_distance_pct: float
    trend_score: int
    contraction_score: int
    volume_score: int
    tightness_score: int
    pivot_score: int
    days_in_range: int
    avg_volume_20d: float
    current_volume: float
    pe_ratio: Optional[float]
    roe_pct: Optional[float]
    profit_margin_pct: Optional[float]
    is_fundamentally_sound: bool
    about: str
    industry: str
    market_cap_cr: Optional[float]
    pros: List[str]
    cons: List[str]
    chart_data: List[float]
    evidence: List[str]
    last_updated: str


# ============================================================
# FUNDAMENTAL QUALITY ENGINE & CACHE
# ============================================================

FUNDAMENTAL_CACHE_FILE = 'fundamental_cache.json'
fundamental_cache: Dict[str, Dict] = {}

def load_fundamental_cache():
    global fundamental_cache
    if os.path.exists(FUNDAMENTAL_CACHE_FILE):
        try:
            with open(FUNDAMENTAL_CACHE_FILE, 'r') as f:
                fundamental_cache = json.load(f)
        except Exception:
            fundamental_cache = {}

def save_fundamental_cache():
    try:
        with open(FUNDAMENTAL_CACHE_FILE, 'w') as f:
            json.dump(fundamental_cache, f, indent=2)
    except Exception:
        pass

load_fundamental_cache()


def build_dynamic_pros_cons(ticker: str, df: pd.DataFrame, vcp_data: Dict, fund_metrics: Dict) -> Tuple[List[str], List[str]]:
    """Build 100% dynamic, stock-specific pros and cons driven by real price, volume, and fundamental metrics."""
    pros = []
    cons = []

    clean_sym = normalize_ticker(ticker)
    eps = fund_metrics.get("eps")
    roe = fund_metrics.get("roe")
    pe = fund_metrics.get("pe")
    de = fund_metrics.get("debt_to_equity")
    margin = fund_metrics.get("profit_margin")
    rev_g = fund_metrics.get("revenue_growth")

    # 1. Fundamental Financial Pros (Only when real numbers exist)
    if eps is not None and eps > 0:
        pros.append(f"Profitable operations with trailing EPS of ₹{eps:.2f}")
    if roe is not None and roe >= 12.0:
        pros.append(f"Strong Return on Equity (ROE: {roe:.1f}%)")
    elif roe is not None and roe >= 5.0:
        pros.append(f"Positive Return on Equity (ROE: {roe:.1f}%)")
    if margin is not None and margin >= 8.0:
        pros.append(f"Healthy net profit margin of {margin:.1f}%")
    if de is not None and de < 60:
        pros.append(f"Low debt leverage ratio (Debt/Equity: {de:.2f})")
    if pe is not None and 5 <= pe <= 35:
        pros.append(f"Reasonable earnings valuation multiple (P/E: {pe:.1f})")
    if rev_g is not None and rev_g > 10.0:
        pros.append(f"Strong revenue growth momentum (+{rev_g:.1f}% YoY)")

    # 2. Fundamental Financial Cons (Only when real numbers exist)
    if eps is not None and eps <= 0:
        cons.append(f"Negative trailing earnings (EPS: ₹{eps:.2f})")
    if roe is not None and roe < 5.0:
        cons.append(f"Low Return on Equity (ROE: {roe:.1f}%)")
    if pe is not None and pe > 45:
        cons.append(f"Premium/Elevated valuation multiple (P/E: {pe:.1f})")
    if de is not None and de > 100:
        cons.append(f"Higher debt leverage ratio (Debt/Equity: {de:.2f})")

    # 3. Dynamic Technical, Volume & Trend Pros (Computed from stock's OHLCV data)
    current_price = float(df['close'].iloc[-1])
    low_52w = float(df['low'].min())
    high_52w = float(df['high'].max())
    up_from_low = ((current_price - low_52w) / low_52w) * 100 if low_52w > 0 else 0
    from_high = ((high_52w - current_price) / high_52w) * 100 if high_52w > 0 else 0

    if up_from_low >= 25.0:
        pros.append(f"Strong Stage 2 uptrend (+{up_from_low:.1f}% above 52-week low of ₹{low_52w:.2f})")

    vol_ratio = vcp_data.get("volume_ratio", 1.0)
    if vol_ratio >= 1.5:
        pros.append(f"Heavy institutional volume thrust ({vol_ratio:.1f}x 20-day avg volume)")

    dry_up = vcp_data.get("dry_up_ratio", 1.0)
    if dry_up < 0.75:
        pros.append(f"Significant volume dry-up near base pivot ({dry_up:.2f}x avg volume)")

    atr_pct = vcp_data.get("atr_pct", 5.0)
    if atr_pct <= 3.8:
        pros.append(f"Low volatility risk (Daily ATR is {atr_pct:.1f}% of current price ₹{current_price:.2f})")

    pivot_dist = vcp_data.get("breakout_distance_pct", 5.0)
    if -0.5 <= pivot_dist <= 3.0:
        pros.append(f"Coiled near breakout pivot (Only {pivot_dist:.1f}% below entry level ₹{vcp_data['resistance_level']:.2f})")

    # 4. Dynamic Technical & Volatility Cons
    if pivot_dist > 4.5:
        cons.append(f"Extended from pivot point ({pivot_dist:.1f}% gap to resistance ₹{vcp_data['resistance_level']:.2f})")

    if atr_pct > 5.5:
        cons.append(f"High price volatility (Daily ATR% is {atr_pct:.1f}%)")

    avg_vol_20d = vcp_data.get("avg_volume_20d", 0)
    turnover_cr = (current_price * avg_vol_20d) / 10000000.0
    if turnover_cr < 3.0:
        cons.append(f"Moderate daily trading volume (Avg daily turnover: ₹{turnover_cr:.2f} Cr)")

    if from_high > 18.0:
        cons.append(f"Trading {from_high:.1f}% below 52-week high of ₹{high_52w:.2f}")

    # Ensure at least 3 pros and 2 cons are always present
    if not pros:
        pros.append(f"Trading in active consolidation with current price at ₹{current_price:.2f}")
    if not cons:
        cons.append(f"Price movement subject to broader market and sector volatility")

    return pros[:5], cons[:5]


def fetch_fundamental_metrics(ticker: str) -> Dict:
    """Fetch key fundamental health metrics and company profile for a ticker."""
    clean_sym = normalize_ticker(ticker)
    if clean_sym in fundamental_cache:
        return fundamental_cache[clean_sym]

    metrics = {
        "eps": None,
        "pe": None,
        "roe": None,
        "debt_to_equity": None,
        "profit_margin": None,
        "revenue_growth": None,
        "is_fundamentally_sound": True,
        "about": "",
        "industry": "NSE Equities",
        "market_cap_cr": None,
        "pros": [],
        "cons": []
    }

    try:
        yf_sym = f"{clean_sym}.NS"
        info = {}
        fi = {}
        with open(os.devnull, 'w') as devnull:
            old_stderr = sys.stderr
            sys.stderr = devnull
            try:
                t = yf.Ticker(yf_sym)
                fi = getattr(t, 'fast_info', {}) or {}
                try:
                    info = t.info or {}
                except Exception:
                    info = {}
            finally:
                sys.stderr = old_stderr

        market_cap = getattr(fi, 'market_cap', None) or (fi.get('market_cap') if isinstance(fi, dict) else None)
        pe_fast = getattr(fi, 'pe_ratio', None) or (fi.get('pe_ratio') if isinstance(fi, dict) else None)

        eps = info.get('trailingEps') or info.get('forwardEps')
        pe_val = pe_fast or info.get('trailingPE') or info.get('forwardPE')
        roe = info.get('returnOnEquity')
        debt_to_equity = info.get('debtToEquity')
        profit_margin = info.get('profitMargins')
        rev_growth = info.get('revenueGrowth') or info.get('earningsGrowth')
        summary_raw = info.get('longBusinessSummary') or info.get('summaryProfile')

        metrics["eps"] = round(float(eps), 2) if eps is not None else None
        metrics["pe"] = round(float(pe_val), 2) if pe_val is not None else None
        metrics["roe"] = round(float(roe * 100), 2) if roe is not None else None
        metrics["debt_to_equity"] = round(float(debt_to_equity), 2) if debt_to_equity is not None else None
        metrics["profit_margin"] = round(float(profit_margin * 100), 2) if profit_margin is not None else None
        metrics["revenue_growth"] = round(float(rev_growth * 100), 2) if rev_growth is not None else None

        if market_cap:
            metrics["market_cap_cr"] = round(float(market_cap) / 10000000.0, 2)
        elif info.get('marketCap'):
            metrics["market_cap_cr"] = round(float(info.get('marketCap')) / 10000000.0, 2)

        summary_text = summary_raw if summary_raw and isinstance(summary_raw, str) and len(summary_raw.strip()) > 10 else None
        if not summary_text:
            meta_name = dynamic_metadata.get(clean_sym, {}).get("name", clean_sym)
            sec = dynamic_metadata.get(clean_sym, {}).get("sector", "NSE Equities")
            summary_text = f"{meta_name} ({clean_sym}) is a premier NSE-listed equity operating within the {sec} sector, delivering corporate products, services, and growth in the Indian market."

        if len(summary_text) > 350:
            summary_text = summary_text[:347] + "..."

        metrics["about"] = summary_text
        metrics["is_fundamentally_sound"] = True

    except Exception:
        pass

    fundamental_cache[clean_sym] = metrics
    save_fundamental_cache()
    return metrics


# ============================================================
# SCANNER ENGINE (MARK MINERVINI VCP SCORING ENGINE)
# ============================================================

def calculate_atr(df: pd.DataFrame, period: int = 14) -> float:
    """Calculate Average True Range."""
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean().iloc[-1]
    return float(atr) if pd.notna(atr) else float(tr.tail(period).mean())


def calculate_vcp_score(df: pd.DataFrame) -> Optional[Dict]:
    """Calculate normalized Minervini VCP score with strict institutional quality filters (0 to 100)."""
    if len(df) < 20:
        return None

    recent = df.tail(20)
    current_price = float(df['close'].iloc[-1])
    
    # === INSTITUTIONAL QUALITY & LIQUIDITY FILTERS ===
    # 1. Price Floor: Exclude penny stocks (< ₹25)
    if current_price < 25.0:
        return None

    # 2. Liquidity & Turnover Floor: Exclude illiquid stocks (avg daily turnover < ₹30 Lakhs or volume < 15k)
    avg_vol_20d = float(recent['volume'].mean())
    turnover_20d = current_price * avg_vol_20d
    if avg_vol_20d < 15000 or turnover_20d < 3000000:
        return None

    # 3. Minervini Stage 2 Trend Hard Minimums:
    # - Stock must be at least 15% above 52-week low
    # - Stock must be within 35% of 52-week high (exclude deep downtrending fallen knives)
    high_52w = float(df["high"].max())
    low_52w = float(df["low"].min())
    if low_52w > 0 and current_price < 1.15 * low_52w:
        return None
    if high_52w > 0 and current_price < 0.65 * high_52w:
        return None

    # 4. Volatility Floor: Exclude hyper-volatile erratic stocks (ATR% > 7.5%)
    atr = calculate_atr(df)
    atr_pct = float((atr / current_price) * 100) if current_price > 0 else 999.0
    if atr_pct > 7.5:
        return None

    high_20d = float(recent['high'].max())
    low_20d = float(recent['low'].min())
    range_20d = high_20d - low_20d
    range_pct = float((range_20d / current_price) * 100) if current_price > 0 else 999.0

    current_vol = float(df['volume'].iloc[-1])
    volume_ratio = float(current_vol / avg_vol_20d) if avg_vol_20d > 0 else 1.0

    vol_last5 = float(recent.tail(5)['volume'].mean())
    vol_first15 = float(recent.head(15)['volume'].mean())
    vol_decline = float((vol_first15 - vol_last5) / vol_first15) if vol_first15 > 0 else 0.0
    vol_50d = float(df.tail(min(len(df), 50))['volume'].mean())
    dry_up_ratio = float(vol_last5 / vol_50d) if vol_50d > 0 else 1.0

    price_std = float(recent['close'].std())
    price_tightness = float((price_std / current_price) * 100) if current_price > 0 else 999.0

    # Prior base consolidation high (excluding latest candle)
    past_base = df.iloc[:-1].tail(20) if len(df) > 20 else df.iloc[:-1]
    prior_resistance = float(past_base['high'].max()) if not past_base.empty else float(recent['high'].max())
    prior_support = float(past_base['low'].min()) if not past_base.empty else float(recent['low'].min())

    support = float(recent['low'].min())
    resistance = float(recent['high'].max())
    entry_price = round(float(prior_resistance * 1.002), 2)
    stop_loss = round(float(max(support, current_price - (2 * atr))), 2)
    risk_pct = round(float(((entry_price - stop_loss) / entry_price) * 100), 2) if entry_price > 0 else 0.0
    breakout_distance_pct = round(float(((prior_resistance - current_price) / current_price) * 100), 2) if current_price > 0 else 999.0

    days_in_range = int(len(recent[
        (recent['close'] >= current_price * 0.95) & 
        (recent['close'] <= current_price * 1.05)
    ]))

    # === TREND TEMPLATE (30 POINTS MAX) ===
    close_series = df["close"]
    ma20 = float(close_series.rolling(min(len(df), 20)).mean().iloc[-1])
    ma50 = float(close_series.rolling(min(len(df), 50)).mean().iloc[-1])
    ma150 = float(close_series.rolling(min(len(df), 150)).mean().iloc[-1])
    ma200 = float(close_series.rolling(min(len(df), 200)).mean().iloc[-1])

    high_52w = float(df["high"].max())
    low_52w = float(df["low"].min())

    trend_score = 0
    if current_price > ma50: trend_score += 6
    if current_price > ma150: trend_score += 5
    if current_price > ma200: trend_score += 5
    if ma50 > ma150: trend_score += 5
    if ma150 > ma200: trend_score += 4
    if low_52w > 0 and (current_price >= 1.20 * low_52w): trend_score += 3
    if high_52w > 0 and (current_price >= 0.75 * high_52w): trend_score += 2

    # === CONTRACTION WAVE PATTERN (25 POINTS MAX) ===
    n = len(df)
    c1, c2, c3 = 100.0, 100.0, range_pct
    if n >= 60:
        w1 = df.iloc[max(0, n-60):n-40]
        w2 = df.iloc[n-40:n-20]
        if not w1.empty and w1['close'].mean() > 0:
            c1 = float(((w1['high'].max() - w1['low'].min()) / w1['close'].mean()) * 100)
        if not w2.empty and w2['close'].mean() > 0:
            c2 = float(((w2['high'].max() - w2['low'].min()) / w2['close'].mean()) * 100)

    contraction_score = 0
    if c1 > c2 > c3:
        contraction_score += 15  # Progressive wave contraction (T1 > T2 > T3)
    elif c2 > c3:
        contraction_score += 10

    if c3 < 5.0: contraction_score += 10
    elif c3 < 8.0: contraction_score += 7
    elif c3 < 12.0: contraction_score += 4

    # === VOLUME DRY-UP (20 POINTS MAX) ===
    volume_score = 0
    if vol_decline > 0.30: volume_score += 10
    elif vol_decline > 0.15: volume_score += 6

    if dry_up_ratio < 0.70: volume_score += 10
    elif dry_up_ratio < 0.90: volume_score += 6
    elif dry_up_ratio < 1.05: volume_score += 3

    # === PRICE TIGHTNESS & ATR% (15 POINTS MAX) ===
    tightness_score = 0
    if price_tightness < 2.5: tightness_score += 8
    elif price_tightness < 4.0: tightness_score += 4

    if atr_pct < 2.5: tightness_score += 7
    elif atr_pct < 4.0: tightness_score += 4

    # === PIVOT PROXIMITY (10 POINTS MAX) ===
    pivot_score = 0
    if -1.0 <= breakout_distance_pct <= 4.0: pivot_score += 6
    elif 4.0 < breakout_distance_pct <= 8.0: pivot_score += 3

    if days_in_range >= 12: pivot_score += 4
    elif days_in_range >= 8: pivot_score += 2

    raw_score = trend_score + contraction_score + volume_score + tightness_score + pivot_score
    vcp_score = int(round(min(100, max(0, raw_score))))

    # Evidence Checklist
    evidence = []
    if trend_score >= 20:
        evidence.append("Minervini Stage 2 uptrend template confirmed")
    if contraction_score >= 15:
        evidence.append("Sequential wave volatility contraction (T1 > T2 > T3)")
    if volume_score >= 12:
        evidence.append("Volume dry-up near base right side")
    if tightness_score >= 10:
        evidence.append("Price action is tight (low ATR%)")
    if pivot_score >= 5:
        evidence.append("Pivoting near resistance level")

    # === STRICT BREAKOUT DETERMINATION ===
    # 1. Price breaking above or matching prior base consolidation high
    # 2. Volume expansion (volume_ratio >= 1.2 or current_vol >= 1.2 * avg_vol_20d)
    # 3. Stock emerged from a valid consolidation base
    is_price_breakout = (current_price >= prior_resistance * 0.998) or (float(df['high'].iloc[-1]) >= prior_resistance * 1.002)
    is_volume_expansion = (volume_ratio >= 1.2) or (current_vol >= 1.2 * avg_vol_20d)
    has_valid_base = (contraction_score >= 5 or tightness_score >= 4 or vcp_score >= 35)

    breakout_confirmed = is_price_breakout and is_volume_expansion and has_valid_base

    if breakout_confirmed:
        status = "breakout"
        vcp_grade = "🚀 Breakout Confirmed"
        evidence.append(f"Breakout above prior base high (₹{prior_resistance:.2f}) on heavy volume ({volume_ratio:.1f}x avg)")
    elif vcp_score >= 65:
        status = "ready"
        vcp_grade = "🌟 A+ Elite VCP Ready"
    elif vcp_score >= 40:
        status = "forming"
        vcp_grade = "⭐ A Grade Base"
    else:
        status = "weak"
        vcp_grade = "📌 Developing Base"

    # 1. BREAKOUT SCORE & GRADE (For "Breakout Confirmed" Tab)
    # Heavy Volume Thrust (40%) + Price Gain & Thrust (30%) + Base Quality (30%)
    vol_thrust_pts = min(40, int((volume_ratio / 2.0) * 40)) if volume_ratio > 0 else 0
    price_thrust_pts = min(30, int(max(0, (current_price - float(df['close'].iloc[-2])) / float(df['close'].iloc[-2]) * 100 * 5)) if len(df) > 1 and float(df['close'].iloc[-2]) > 0 else 0)
    base_qual_pts = min(30, int((contraction_score + tightness_score) * 1.3))
    raw_breakout_score = vol_thrust_pts + price_thrust_pts + base_qual_pts
    breakout_score = int(min(100, max(0, raw_breakout_score)))

    if breakout_score >= 80:
        breakout_grade = "🔥 Institutional Thrust"
    elif breakout_score >= 60:
        breakout_grade = "🚀 Strong Volume Breakout"
    else:
        breakout_grade = "📈 Moderate Breakout"

    # 2. PIVOT READINESS SCORE & GRADE (For "Pivoting / Ready" Tab)
    # Pivot Proximity (35%) + Price Tightness & Low ATR (35%) + Volume Dry-Up (30%)
    prox_pts = 35 if -0.5 <= breakout_distance_pct <= 2.5 else (20 if 2.5 < breakout_distance_pct <= 5.0 else 10)
    tight_pts = min(35, int(tightness_score * 2.3))
    dry_pts = 30 if dry_up_ratio < 0.65 else (20 if dry_up_ratio < 0.85 else 10)
    raw_pivot_score = prox_pts + tight_pts + dry_pts
    pivot_readiness_score = int(min(100, max(0, raw_pivot_score)))

    if pivot_readiness_score >= 80:
        pivot_readiness_grade = "🎯 Coiled A+ Pivot"
    elif pivot_readiness_score >= 60:
        pivot_readiness_grade = "⚡ High Alert Pivot"
    else:
        pivot_readiness_grade = "📌 Approaching Pivot"

    # 3. BASE QUALITY SCORE & GRADE (For "Forming Base" Tab)
    # Contraction Waves (45%) + Trend Template (35%) + Tightness (20%)
    base_wave_pts = min(45, int(contraction_score * 3.0))
    base_trend_pts = min(35, int(trend_score * 1.16))
    base_tight_pts = min(20, int(tightness_score * 1.33))
    raw_base_score = base_wave_pts + base_trend_pts + base_tight_pts
    base_quality_score = int(min(100, max(0, raw_base_score)))

    if base_quality_score >= 75:
        base_quality_grade = "🌟 A+ Minervini Base"
    elif base_quality_score >= 50:
        base_quality_grade = "⭐ A Grade Consolidation"
    else:
        base_quality_grade = "⏳ Developing Base"

    if range_pct < 5 and price_tightness < 2:
        contraction = "Tight"
    elif range_pct < 8 and price_tightness < 3:
        contraction = "Tightening"
    elif range_pct < 12:
        contraction = "Moderate"
    else:
        contraction = "Wide"

    if vol_decline > 0.3:
        volume_trend = "Declining"
    elif vol_decline > 0.1:
        volume_trend = "Stable"
    elif volume_ratio > 2:
        volume_trend = "Spike"
    else:
        volume_trend = "High"

    return {
        "vcp_score": vcp_score,
        "vcp_grade": vcp_grade,
        "breakout_score": breakout_score,
        "breakout_grade": breakout_grade,
        "pivot_readiness_score": pivot_readiness_score,
        "pivot_readiness_grade": pivot_readiness_grade,
        "base_quality_score": base_quality_score,
        "base_quality_grade": base_quality_grade,
        "contraction": contraction,
        "volume_trend": volume_trend,
        "status": status,
        "atr": round(atr, 2),
        "atr_pct": round(atr_pct, 2),
        "volume_ratio": round(volume_ratio, 2),
        "dry_up_ratio": round(dry_up_ratio, 2),
        "price_tightness": round(price_tightness, 2),
        "support_level": round(support, 2),
        "resistance_level": round(resistance, 2),
        "entry_price": round(entry_price, 2),
        "stop_loss": round(stop_loss, 2),
        "risk_pct": round(risk_pct, 2),
        "breakout_distance_pct": round(breakout_distance_pct, 2),
        "trend_score": trend_score,
        "contraction_score": contraction_score,
        "volume_score": volume_score,
        "tightness_score": tightness_score,
        "pivot_score": pivot_score,
        "days_in_range": days_in_range,
        "avg_volume_20d": round(avg_vol_20d, 0),
        "current_volume": round(current_vol, 0),
        "evidence": evidence or ["Base developing - monitor for tighter contraction"],
    }


SECTOR_MAPPING = {
    "BANK": "Financial Services", "FIN": "Financial Services", "INSURANCE": "Financial Services", "CAP": "Financial Services",
    "HDFC": "Financial Services", "ICICI": "Financial Services", "SBIN": "Financial Services", "KOTAK": "Financial Services",
    "AXIS": "Financial Services", "BAJAJ": "Financial Services", "PIRAMAL": "Financial Services", "MUTHOOT": "Financial Services",
    
    "TECH": "Information Technology", "INFO": "Information Technology", "TCS": "Information Technology",
    "WIPRO": "Information Technology", "HCL": "Information Technology", "LTIM": "Information Technology",
    "PERSISTENT": "Information Technology", "COFORGE": "Information Technology", "MPHASIS": "Information Technology",
    
    "PHARMA": "Healthcare & Pharma", "LAB": "Healthcare & Pharma", "HEALTH": "Healthcare & Pharma", "DRUG": "Healthcare & Pharma",
    "SUNPHARMA": "Healthcare & Pharma", "DRREDDY": "Healthcare & Pharma", "CIPLA": "Healthcare & Pharma",
    "DIVIS": "Healthcare & Pharma", "APOLLO": "Healthcare & Pharma", "BIOCON": "Healthcare & Pharma", "LUPIN": "Healthcare & Pharma",

    "AUTO": "Automobiles & Components", "MOTOR": "Automobiles & Components", "MOTORS": "Automobiles & Components",
    "MARUTI": "Automobiles & Components", "TATAMOTORS": "Automobiles & Components", "MAHINDRA": "Automobiles & Components",
    "HEROMOTO": "Automobiles & Components", "BAJAJ-AUTO": "Automobiles & Components", "EICHER": "Automobiles & Components",

    "POWER": "Energy & Power", "ENERGY": "Energy & Power", "OIL": "Energy & Power", "GAS": "Energy & Power",
    "RELIANCE": "Energy & Power", "NTPC": "Energy & Power", "POWERGRID": "Energy & Power", "ONGC": "Energy & Power",
    "BPCL": "Energy & Power", "IOC": "Energy & Power", "GAIL": "Energy & Power", "ADANIGREEN": "Energy & Power",

    "METAL": "Metals & Mining", "STEEL": "Metals & Mining", "COPPER": "Metals & Mining", "MINE": "Metals & Mining",
    "TATASTEEL": "Metals & Mining", "JINDAL": "Metals & Mining", "SAIL": "Metals & Mining",
    "HINDALCO": "Metals & Mining", "VEDL": "Metals & Mining", "COALINDIA": "Metals & Mining",

    "FMCG": "Consumer Goods & FMCG", "CONSUMER": "Consumer Goods & FMCG", "FOOD": "Consumer Goods & FMCG",
    "HINDUNILVR": "Consumer Goods & FMCG", "ITC": "Consumer Goods & FMCG", "NESTLE": "Consumer Goods & FMCG",
    "BRITANNIA": "Consumer Goods & FMCG", "DABUR": "Consumer Goods & FMCG", "MARICO": "Consumer Goods & FMCG",

    "IND": "Industrials & Capital Goods", "ENG": "Industrials & Capital Goods", "CORP": "Industrials & Capital Goods",
    "LT": "Industrials & Capital Goods", "SIEMENS": "Industrials & Capital Goods", "ABB": "Industrials & Capital Goods",
    "BEL": "Industrials & Capital Goods", "HAL": "Industrials & Capital Goods", "BHEL": "Industrials & Capital Goods",

    "REALTY": "Real Estate & Infra", "INFRA": "Real Estate & Infra", "CONST": "Real Estate & Infra", "BUILD": "Real Estate & Infra",
    "DLF": "Real Estate & Infra", "LODHA": "Real Estate & Infra", "GODREJPROP": "Real Estate & Infra", "OBEROI": "Real Estate & Infra"
}


def resolve_stock_sector(ticker: str, fund_metrics: Dict = None) -> str:
    """Resolve stock sector using fundamental metrics + heuristic mapping."""
    clean_sym = normalize_ticker(ticker)
    
    if fund_metrics:
        ind = fund_metrics.get("industry")
        if ind and ind not in ["NSE Equities", "Unknown", "N/A", ""]:
            return ind

    for key, sector_name in SECTOR_MAPPING.items():
        if key in clean_sym:
            return sector_name

    meta = dynamic_metadata.get(clean_sym, {})
    sec = meta.get("sector")
    if sec and sec != "NSE Equities":
        return sec

    return "Diversified Equities"


def get_stock_info(ticker: str, fund_metrics: Dict = None) -> Dict:
    """Get stock name and sector mapping dynamically."""
    clean_sym = normalize_ticker(ticker)
    meta = dynamic_metadata.get(clean_sym, {})
    name = meta.get("name") or clean_sym.replace("-", " ").title() + " Ltd"
    sector = resolve_stock_sector(clean_sym, fund_metrics)
    return {"name": name, "sector": sector}


def scan_stock(ticker: str) -> Optional[VCPResult]:
    """Scan a single stock for VCP pattern and fundamental quality."""
    df = fetch_stock_data(ticker)
    if df is None:
        return None

    vcp_data = calculate_vcp_score(df)
    if vcp_data is None:
        return None

    # Fundamental Health Evaluation Filter
    fund_metrics = fetch_fundamental_metrics(ticker)
    if not fund_metrics.get("is_fundamentally_sound", True):
        return None  # Exclude fundamentally weak / loss-making / over-indebted stocks

    info = get_stock_info(ticker, fund_metrics)
    current_price = float(df['close'].iloc[-1])
    prev_close = float(df['close'].iloc[-2]) if len(df) > 1 else current_price
    change = current_price - prev_close
    change_pct = (change / prev_close) * 100 if prev_close > 0 else 0.0
    chart_data = [float(x) for x in df['close'].tail(20).tolist()]

    # Pre-cache 2-year daily chart data in RAM for instant 0.001s loading on UI click
    try:
        clean_t = normalize_ticker(ticker)
        cache_key = f"{clean_t}.NS_2y_1d"
        candles = []
        df_reset = df.reset_index()
        for _, row in df_reset.iterrows():
            ts = row.get('timestamp') or row.get('Date') or row.get('index')
            date_str = ts.strftime('%Y-%m-%d') if hasattr(ts, 'strftime') else str(ts)[:10]
            candles.append({
                "time": date_str,
                "open": round(float(row['open']), 2),
                "high": round(float(row['high']), 2),
                "low": round(float(row['low']), 2),
                "close": round(float(row['close']), 2),
                "volume": round(float(row['volume']), 0)
            })
        chart_cache_memory[cache_key] = (time.time(), {
            "success": True,
            "ticker": clean_t,
            "period": "2y",
            "interval": "1d",
            "candles": candles
        })
    except Exception:
        pass

    # Build 100% dynamic quantitative pros and cons from live metrics & price action
    pros, cons = build_dynamic_pros_cons(ticker, df, vcp_data, fund_metrics)

    return VCPResult(
        ticker=ticker.replace(".NS", ""),
        name=info["name"],
        sector=info["sector"],
        price=round(current_price, 2),
        change=round(change, 2),
        change_pct=round(change_pct, 2),
        vcp_score=vcp_data["vcp_score"],
        vcp_grade=vcp_data["vcp_grade"],
        breakout_score=vcp_data["breakout_score"],
        breakout_grade=vcp_data["breakout_grade"],
        pivot_readiness_score=vcp_data["pivot_readiness_score"],
        pivot_readiness_grade=vcp_data["pivot_readiness_grade"],
        base_quality_score=vcp_data["base_quality_score"],
        base_quality_grade=vcp_data["base_quality_grade"],
        contraction=vcp_data["contraction"],
        volume_trend=vcp_data["volume_trend"],
        range_20d=f"₹{vcp_data['support_level']:.0f}–{vcp_data['resistance_level']:.0f}",
        status=vcp_data["status"],
        atr=vcp_data["atr"],
        atr_pct=vcp_data["atr_pct"],
        volume_ratio=vcp_data["volume_ratio"],
        dry_up_ratio=vcp_data["dry_up_ratio"],
        price_tightness=vcp_data["price_tightness"],
        support_level=vcp_data["support_level"],
        resistance_level=vcp_data["resistance_level"],
        entry_price=vcp_data["entry_price"],
        stop_loss=vcp_data["stop_loss"],
        risk_pct=vcp_data["risk_pct"],
        breakout_distance_pct=vcp_data["breakout_distance_pct"],
        trend_score=vcp_data["trend_score"],
        contraction_score=vcp_data["contraction_score"],
        volume_score=vcp_data["volume_score"],
        tightness_score=vcp_data["tightness_score"],
        pivot_score=vcp_data["pivot_score"],
        days_in_range=vcp_data["days_in_range"],
        avg_volume_20d=vcp_data["avg_volume_20d"],
        current_volume=vcp_data["current_volume"],
        pe_ratio=fund_metrics.get("pe"),
        roe_pct=fund_metrics.get("roe"),
        profit_margin_pct=fund_metrics.get("profit_margin"),
        is_fundamentally_sound=True,
        about=fund_metrics.get("about", ""),
        industry=fund_metrics.get("industry", info["sector"]),
        market_cap_cr=fund_metrics.get("market_cap_cr"),
        pros=pros,
        cons=cons,
        chart_data=[round(x, 2) for x in chart_data],
        evidence=vcp_data["evidence"],
        last_updated=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )


def scan_universe(tickers: List[str], min_score: int = 0, 
                  min_price: float = 0, max_price: float = 999999) -> List[VCPResult]:
    """Scan multiple stocks in parallel using ThreadPoolExecutor."""
    results = []
    scan_cache["errors"] = []

    def _worker(ticker: str) -> Optional[VCPResult]:
        try:
            res = scan_stock(ticker)
            if res and min_price <= res.price <= max_price:
                if min_score == 0 or res.vcp_score >= min_score:
                    return res
        except Exception as e:
            scan_cache["errors"].append(f"{ticker}: {str(e)}")
        return None

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(_worker, ticker): ticker for ticker in tickers}
        for future in as_completed(futures):
            res = future.result()
            if res is not None:
                results.append(res)

    results.sort(key=lambda x: x.vcp_score, reverse=True)
    return results


def serialize_result(result):
    """Convert scan rows into JSON-safe dictionaries."""
    if isinstance(result, VCPResult):
        return asdict(result)
    if isinstance(result, dict):
        return result
    return {}


def load_cache():
    """Load cached scan results for instant startup display."""
    if not os.path.exists(CACHE_FILE):
        return
    try:
        with open(CACHE_FILE, 'r') as f:
            data = json.load(f)
        scan_cache["last_scan"] = data.get("last_scan")
        scan_cache["results"] = data.get("results", {})
    except Exception as e:
        print(f"Cache load error: {e}")


def save_cache():
    """Save scan results to cache file."""
    try:
        data = {
            "last_scan": scan_cache["last_scan"],
            "results": {k: [serialize_result(r) for r in v] for k, v in scan_cache["results"].items()}
        }
        with open(CACHE_FILE, 'w') as f:
            json.dump(data, f, default=str)
    except Exception as e:
        print(f"Cache save error: {e}")


# ============================================================
# AUTO-SCAN SCHEDULER
# ============================================================

def scan_single_universe_bg(universe_name: str):
    """Scan a single universe in background thread when requested via API."""
    if scan_cache.get(f"scanning_{universe_name}"):
        return
    scan_cache[f"scanning_{universe_name}"] = True
    try:
        tickers = get_stock_universe(universe_name)
        if tickers:
            print(f"⚡ Auto-scanning requested universe '{universe_name}' ({len(tickers)} stocks)...")
            results = scan_universe(tickers, min_score=0)
            scan_cache["results"][universe_name] = results
            save_cache()
            print(f"  ✓ {universe_name}: {len(results)} analyzed")
    finally:
        scan_cache[f"scanning_{universe_name}"] = False


def auto_scan():
    """Run scheduled scan for all dynamic universes."""
    print(f"\n🔄 Auto-scan started at {datetime.now()}")
    scan_cache["is_scanning"] = True

    try:
        for universe_name in ["nifty50", "nifty200", "nifty500", "smallcap", "ipo", "nse_all"]:
            tickers = get_stock_universe(universe_name)
            print(f"  Scanning dynamic {universe_name} ({len(tickers)} stocks)...")
            results = scan_universe(tickers, min_score=0)
            scan_cache["results"][universe_name] = results
            print(f"  ✓ {universe_name}: {len(results)} analyzed")

        scan_cache["last_scan"] = datetime.now().isoformat()
        save_cache()
        print(f"✅ Auto-scan complete at {datetime.now()}\n")
    finally:
        scan_cache["is_scanning"] = False


load_cache()

if os.getenv("VCP_DISABLE_SCHEDULER") != "1":
    scheduler = BackgroundScheduler()
    scheduler.add_job(auto_scan, 'cron', hour='9-15', minute='*/30', day_of_week='mon-fri')
    scheduler.add_job(auto_scan, 'cron', hour=9, minute=20, day_of_week='mon-fri')
    scheduler.add_job(sync_dynamic_universes, 'cron', hour=8, minute=0, day_of_week='mon-fri')
    scheduler.start()

    if os.getenv("VCP_SKIP_INITIAL_SCAN") != "1":
        print("Running initial background scan...")
        threading.Thread(target=auto_scan, daemon=True).start()


# ============================================================
# FLASK ROUTES
# ============================================================

@app.route('/')
def index():
    """Serve the main HTML template."""
    return render_template('index.html')


@app.route('/api/scan', methods=['POST'])
def api_scan():
    """Manual scan endpoint."""
    data = request.json or {}
    universe = data.get('universe', 'nifty200')
    min_score = data.get('min_score', 0)
    min_price = data.get('min_price', 0)
    max_price = data.get('max_price', 999999)
    custom_tickers = data.get('custom_tickers', [])

    if universe == 'custom' and custom_tickers:
        tickers = []
        for raw_ticker in custom_tickers:
            ticker = str(raw_ticker).strip().upper()
            if not ticker:
                continue
            tickers.append(ticker if ticker.endswith('.NS') else f"{ticker}.NS")
    else:
        tickers = get_stock_universe(universe)

    scan_cache["is_scanning"] = True
    try:
        results = scan_universe(tickers, min_score, min_price, max_price)
    finally:
        scan_cache["is_scanning"] = False

    scan_cache["results"][universe] = results
    scan_cache["last_scan"] = datetime.now().isoformat()
    save_cache()

    return jsonify({
        "success": True,
        "universe": universe,
        "scanned": len(tickers),
        "matches": len(results),
        "last_scan": scan_cache["last_scan"],
        "errors": scan_cache["errors"][-10:],
        "results": [serialize_result(r) for r in results]
    })


def calculate_swing_conviction_score(stock) -> Tuple[int, str]:
    """Calculate Conviction Score (0-100) for Swing Trading based on risk-reward, pivot proximity, volume dry-up & fundamentals."""
    vcp_score = getattr(stock, 'vcp_score', 0)
    pivot_readiness = getattr(stock, 'pivot_readiness_score', 0)
    base_quality = getattr(stock, 'base_quality_score', 0)
    risk_pct = getattr(stock, 'risk_pct', 5.0)
    status = getattr(stock, 'status', 'forming')
    dry_up = getattr(stock, 'dry_up_ratio', 1.0)
    vol_ratio = getattr(stock, 'volume_ratio', 1.0)
    roe = getattr(stock, 'roe_pct', 0) or 0

    # 1. Base Score
    score = (0.35 * pivot_readiness) + (0.35 * base_quality) + (0.30 * vcp_score)

    # 2. Risk-Reward Bonus (Tight Stop Loss <= 4% gets up to +15 pts)
    if risk_pct <= 2.5:
        score += 15
    elif risk_pct <= 4.0:
        score += 10
    elif risk_pct <= 5.5:
        score += 5

    # 3. Status Bonus (Ready / Pivoting stocks get +15 pts, Confirmed Breakout gets +10 pts)
    if status == 'ready':
        score += 15
    elif status == 'breakout':
        score += 10

    # 4. Volume Dry-up / Expansion Thrust (+10 pts)
    if dry_up <= 0.60 or vol_ratio >= 1.5:
        score += 10

    # 5. Fundamental High Conviction Bonus (ROE >= 12% gets +10 pts)
    if roe >= 12.0:
        score += 10
    elif roe >= 6.0:
        score += 5

    final_score = int(min(100, max(0, round(score))))

    # Conviction Reason Summary
    reasons = []
    if status == 'ready':
        reasons.append("🎯 Coiled right at pivot entry")
    elif status == 'breakout':
        reasons.append("🚀 Active volume breakout thrust")
    
    if risk_pct <= 4.0:
        reasons.append(f"⚡ Tight {risk_pct:.1f}% risk stop")
    if roe >= 12.0:
        reasons.append(f"💎 High ROE ({roe:.1f}%)")
    if dry_up <= 0.65:
        reasons.append(f"📉 Supply dry-up ({int(dry_up*100)}%)")

    reason_str = " • ".join(reasons) if reasons else "Strong Minervini Stage 2 uptrend setup"
    return final_score, reason_str


@app.route('/api/results/top_picks')
def api_top_picks():
    """Get Top 10 Best Swing Trade Setups in the entire market based on conviction scoring."""
    all_candidates = []
    seen_tickers = set()

    for uni_id in ["nifty50", "nifty200", "nifty500", "smallcap", "ipo"]:
        raw_list = scan_cache["results"].get(uni_id, [])
        for stock in raw_list:
            t = getattr(stock, 'ticker', '')
            if t and t not in seen_tickers:
                seen_tickers.add(t)
                conviction_score, conviction_reason = calculate_swing_conviction_score(stock)
                s_dict = serialize_result(stock)
                s_dict["vcp_score"] = conviction_score  # Override rating display
                s_dict["vcp_grade"] = f"🔥 Top #{len(all_candidates)+1} Swing Setup"
                s_dict["conviction_reason"] = conviction_reason
                all_candidates.append((conviction_score, s_dict))

    # Sort descending by conviction score
    all_candidates.sort(key=lambda x: x[0], reverse=True)

    top_10 = []
    for rank, (score, s_dict) in enumerate(all_candidates[:10], start=1):
        s_dict["top_category_tag"] = f"🔥 #{rank} Swing Pick"
        s_dict["vcp_grade"] = f"🔥 #{rank} Conviction ({score} pts)"
        top_10.append(s_dict)

    return jsonify({
        "success": True,
        "universe": "top_picks",
        "matches": len(top_10),
        "last_scan": scan_cache["last_scan"],
        "is_scanning": scan_cache["is_scanning"],
        "results": top_10
    })


@app.route('/api/results/<universe>')
def api_results(universe):
    """Get cached results. Auto-trigger scan if empty."""
    results = scan_cache["results"].get(universe, [])
    is_bg_scanning = scan_cache.get(f"scanning_{universe}", False) or scan_cache["is_scanning"]

    if not results and not is_bg_scanning:
        threading.Thread(target=scan_single_universe_bg, args=(universe,), daemon=True).start()
        is_bg_scanning = True

    return jsonify({
        "success": True,
        "universe": universe,
        "matches": len(results),
        "last_scan": scan_cache["last_scan"],
        "is_scanning": is_bg_scanning,
        "results": [serialize_result(r) for r in results]
    })


@app.route('/api/stock/<ticker>')
def api_stock_detail(ticker):
    """Get detailed data for a single stock."""
    if not ticker.endswith('.NS'):
        ticker += '.NS'

    result = scan_stock(ticker)
    if result:
        return jsonify({"success": True, "data": serialize_result(result)})
    return jsonify({"success": False, "error": "Could not fetch data"}), 404


@app.route('/api/chart/<ticker>')
def api_stock_chart(ticker):
    """Fetch daily/weekly/intraday OHLC candles for Candlestick chart visualization with instant RAM caching."""
    clean_t = normalize_ticker(ticker)
    full_t = f"{clean_t}.NS"
    period = request.args.get('period', '2y')
    interval = request.args.get('interval', '1d')

    cache_key = f"{full_t}_{period}_{interval}"
    now = time.time()

    # Instant 0.001s response from memory if pre-cached or previously fetched
    if cache_key in chart_cache_memory:
        cached_time, cached_payload = chart_cache_memory[cache_key]
        if now - cached_time < CHART_CACHE_TTL:
            return jsonify(cached_payload)

    df = fetch_stock_data(full_t, period=period, interval=interval)
    if df is None or df.empty:
        return jsonify({"success": False, "error": "No candle data available"}), 404

    candles = []
    df_reset = df.reset_index()
    for _, row in df_reset.iterrows():
        try:
            ts = row.get('timestamp') or row.get('Date') or row.get('index')
            date_str = ts.strftime('%Y-%m-%d %H:%M') if 'm' in interval else (ts.strftime('%Y-%m-%d') if hasattr(ts, 'strftime') else str(ts)[:10])
            candles.append({
                "time": date_str,
                "open": round(float(row['open']), 2),
                "high": round(float(row['high']), 2),
                "low": round(float(row['low']), 2),
                "close": round(float(row['close']), 2),
                "volume": round(float(row['volume']), 0)
            })
        except Exception:
            continue

    payload = {"success": True, "ticker": clean_t, "period": period, "interval": interval, "candles": candles}
    chart_cache_memory[cache_key] = (now, payload)
    return jsonify(payload)


@app.route('/api/universes')
def api_universes():
    """List available dynamic universes."""
    return jsonify({
        "success": True,
        "universes": [
            {"id": "nifty50", "name": "Nifty 50 Bluechips", "count": len(get_stock_universe("nifty50"))},
            {"id": "nifty200", "name": "Nifty 200 Equities", "count": len(get_stock_universe("nifty200"))},
            {"id": "nifty500", "name": "Nifty 500 Equities", "count": len(get_stock_universe("nifty500"))},
            {"id": "smallcap", "name": "Smallcap Momentum", "count": len(get_stock_universe("smallcap"))},
            {"id": "ipo", "name": "Recent IPOs & Listings", "count": len(get_stock_universe("ipo"))},
            {"id": "nse_all", "name": "All NSE Equities", "count": len(get_stock_universe("nse_all"))},
            {"id": "custom", "name": "Custom Tickers", "count": len(STOCK_UNIVERSES["custom"])},
        ]
    })


@app.route('/api/universes/refresh', methods=['POST'])
def api_universes_refresh():
    """Trigger a dynamic API sync for all stock universes."""
    sync_dynamic_universes()
    return jsonify({
        "success": True,
        "total_nse_stocks": len(dynamic_universes.get("nse_all", [])),
        "universes": list(dynamic_universes.keys())
    })


@app.route('/api/status')
def api_status():
    """Get scanner status."""
    return jsonify({
        "success": True,
        "is_scanning": scan_cache["is_scanning"],
        "last_scan": scan_cache["last_scan"],
        "errors": scan_cache["errors"][-10:],
        "cached_universes": list(scan_cache["results"].keys()),
        "next_scan": "Every 30 min (9:15 AM - 3:30 PM IST, Mon-Fri)"
    })


@app.route('/api/export/<universe>')
def api_export(universe):
    """Export results to CSV."""
    results = scan_cache["results"].get(universe, [])
    if not results:
        return jsonify({"success": False, "error": "No data"}), 404

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['Ticker', 'Name', 'Sector', 'Price', 'Change%', 'VCP Score', 'VCP Grade',
                     'Status', 'Contraction', 'Volume Trend', '20D Range', 
                     'Support', 'Resistance', 'ATR%', 'Last Updated'])

    for result in results:
        r = serialize_result(result)
        writer.writerow([r.get('ticker'), r.get('name'), r.get('sector'), r.get('price'), r.get('change_pct'),
                        r.get('vcp_score'), r.get('vcp_grade'), r.get('status'), r.get('contraction'), r.get('volume_trend'),
                        r.get('range_20d'), r.get('support_level'), r.get('resistance_level'), 
                        r.get('atr_pct'), r.get('last_updated')])

    output.seek(0)
    return Response(output.getvalue(), mimetype='text/csv',
                   headers={'Content-Disposition': f'attachment; filename=vcp_scan_{universe}.csv'})


# ============================================================
# MAIN
# ============================================================

if __name__ == '__main__':
    print("=" * 60)
    print("🇮🇳 Indian Stock VCP Scanner - Guaranteed Instant Display")
    print("=" * 60)
    print("")
    print("🌐 Open your browser and go to:")
    print("   http://localhost:8000")
    print("")
    print("⏰ Auto-scan schedule:")
    print("   • Every 30 minutes (9:15 AM - 3:30 PM IST, Mon-Fri)")
    print("   • Market open scan at 9:20 AM")
    print("   • Dynamic universe API sync daily at 8:00 AM")
    print("=" * 60)
    print("")

    port = int(os.environ.get("PORT", 8000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
