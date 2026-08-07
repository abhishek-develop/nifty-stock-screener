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
import math
import os
import sys
import threading
import csv
import hashlib
import time
from io import StringIO
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dataclasses import dataclass, asdict, field
from typing import List, Dict, Optional, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from apscheduler.schedulers.background import BackgroundScheduler
import requests
import yfinance as yf
import werkzeug.serving
import warnings
from fundamental_scoring_engine import InstitutionalFundamentalScoringEngine

fundamental_engine = InstitutionalFundamentalScoringEngine()
warnings.filterwarnings('ignore')

app = Flask(__name__)
CORS(app)
_app_start_time = time.time()

# ============================================================
# CONFIGURATION & DYNAMIC UNIVERSE ENGINE
# ============================================================



DYNAMIC_UNIVERSE_CACHE_FILE = 'dynamic_universes.json'
UNIVERSE_SOURCE_VERSION = 2
dynamic_universes: Dict[str, List[str]] = {}
dynamic_metadata: Dict[str, Dict] = {}
dynamic_universe_source_version = 0

OFFICIAL_INDEX_FILES = {
    "nifty50": "ind_nifty50list.csv",
    "nifty200": "ind_nifty200list.csv",
    "nifty500": "ind_nifty500list.csv",
    "smallcap": "ind_niftysmallcap100list.csv",
}

STOCK_UNIVERSES = {"custom": []}
CACHE_FILE = 'scan_cache.json'
SCAN_SCHEMA_VERSION = 3
scan_cache = {"last_scan": None, "results": {}, "is_scanning": False, "errors": []}
scan_cache_lock = threading.Lock()
chart_cache_memory = {}
CHART_CACHE_TTL = 1800  # 30-minute high-speed RAM chart cache

# OHLCV DataFrame memory cache — avoids re-fetching same ticker across overlapping universes
ohlcv_cache: Dict[str, Tuple[float, pd.DataFrame]] = {}
OHLCV_CACHE_TTL = 1800  # 30 minutes

# Global rate limiter for Yahoo Finance API calls
yahoo_rate_semaphore = threading.Semaphore(2)  # Max 2 concurrent Yahoo requests

# Determine worker count based on environment
MAX_SCAN_WORKERS = 2 if os.getenv("RENDER") else 4
FOCUS_SOURCE_UNIVERSES = ("nifty500", "ipo")

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
                    existing = dynamic_metadata.get(tradingsymbol, {})
                    dynamic_metadata[tradingsymbol] = {
                        **existing,
                        "name": item.get("name") or existing.get("name") or tradingsymbol,
                        "sector": existing.get("sector") or "NSE Equities",
                        "listing_date": existing.get("listing_date", ""),
                        "instrument_token": item.get("instrument_token") or existing.get("instrument_token", ""),
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


def fetch_official_index_constituents() -> Dict[str, List[str]]:
    """Fetch genuine NSE index members instead of inventing indexes from master-file order."""
    headers = {'User-Agent': 'Mozilla/5.0 (compatible; NSE-Swing-Scanner/2.0)'}
    result: Dict[str, List[str]] = {}
    for universe, filename in OFFICIAL_INDEX_FILES.items():
        url = f"https://nsearchives.nseindia.com/content/indices/{filename}"
        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            index_df = pd.read_csv(StringIO(response.text))
            symbol_col = next((c for c in index_df.columns if str(c).strip().lower() == "symbol"), None)
            if not symbol_col:
                raise ValueError("Symbol column missing")
            symbols = [str(s).strip().upper() for s in index_df[symbol_col].dropna()]
            symbols = list(dict.fromkeys(s for s in symbols if s))
            if symbols:
                result[universe] = [f"{symbol}.NS" for symbol in symbols]
                for _, row in index_df.iterrows():
                    symbol = str(row.get(symbol_col, "")).strip().upper()
                    if not symbol:
                        continue
                    name = row.get("Company Name") or row.get("Company_Name") or symbol
                    industry = row.get("Industry") or "NSE Equities"
                    dynamic_metadata.setdefault(symbol, {})
                    dynamic_metadata[symbol].update({"name": str(name).strip(), "sector": str(industry).strip()})
        except Exception as exc:
            scan_cache["errors"].append(f"Official {universe} constituent fetch failed: {exc}")
    return result


def sync_dynamic_universes():
    """Fetch and organize stock universes dynamically from live APIs."""
    global dynamic_universe_source_version
    print("🌐 Syncing stock universes dynamically from official NSE & Zerodha APIs...")
    raw_equities = fetch_dynamic_nse_equities()
    official_indexes = fetch_official_index_constituents()
    if not raw_equities:
        print("⚠️ Warning: Dynamic API fetch yielded no rows. Loading cached dynamic universes if available.")
        load_dynamic_universe_cache()
        for universe, tickers in official_indexes.items():
            dynamic_universes[universe] = tickers
        return

    for eq in raw_equities:
        sym = eq['symbol']
        existing_meta = dynamic_metadata.get(sym, {})
        dynamic_metadata[sym] = {
            "name": eq['name'],
            "sector": "NSE Equities",
            "listing_date": eq['listing_date'],
            "instrument_token": existing_meta.get("instrument_token", ""),
        }

    all_tickers = [eq['ticker'] for eq in raw_equities]
    
    # 1. IPO Universe
    cutoff_date = (datetime.now() - timedelta(days=550)).strftime('%Y-%m-%d')
    ipo_tickers = [eq['ticker'] for eq in raw_equities if eq['listing_date'] and eq['listing_date'] >= cutoff_date]
    if len(ipo_tickers) < 15:
        ipo_tickers = [eq['ticker'] for eq in raw_equities if eq['listing_date']][:40]

    # 2. Official index universes. Never label arbitrary equity-master slices as indexes.
    for universe in ["nifty50", "nifty200", "nifty500", "smallcap"]:
        if official_indexes.get(universe):
            dynamic_universes[universe] = official_indexes[universe]
        elif not dynamic_universes.get(universe):
            # Honest fallback: expose a broad NSE set without pretending it is a real index.
            dynamic_universes[universe] = all_tickers
    dynamic_universes["ipo"] = ipo_tickers
    dynamic_universes["nse_all"] = all_tickers[:1000]

    try:
        cache_data = {
            "source_version": UNIVERSE_SOURCE_VERSION,
            "source": "Official NSE constituent files + NSE equity master",
            "last_synced": datetime.now().isoformat(),
            "total_nse_stocks": len(all_tickers),
            "universes": dynamic_universes,
            "metadata": dynamic_metadata
        }
        with open(DYNAMIC_UNIVERSE_CACHE_FILE, 'w') as f:
            json.dump(cache_data, f, indent=2)
        dynamic_universe_source_version = UNIVERSE_SOURCE_VERSION
        print(f"✅ Synced {len(all_tickers)} NSE equities and official index constituents!")
    except Exception as exc:
        print(f"Error saving dynamic universe cache: {exc}")


def load_dynamic_universe_cache():
    """Load dynamic universes from local cache file if existing."""
    if not os.path.exists(DYNAMIC_UNIVERSE_CACHE_FILE):
        return
    try:
        with open(DYNAMIC_UNIVERSE_CACHE_FILE, 'r') as f:
            data = json.load(f)
        global dynamic_universes, dynamic_metadata, dynamic_universe_source_version
        dynamic_universes = data.get("universes", {})
        dynamic_metadata = data.get("metadata", {})
        dynamic_universe_source_version = int(data.get("source_version", 0) or 0)
        print(f"📁 Loaded {sum(len(v) for v in dynamic_universes.values())} tickers across dynamic universes from cache.")
    except Exception as exc:
        print(f"Error loading dynamic universe cache: {exc}")


load_dynamic_universe_cache()
if (not dynamic_universes or dynamic_universe_source_version < UNIVERSE_SOURCE_VERSION) and os.getenv("VCP_SKIP_UNIVERSE_SYNC") != "1":
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
        return None


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
    # Prefer query2 first — it works reliably from cloud IPs (Render)
    domains = ["query2.finance.yahoo.com", "query1.finance.yahoo.com", "query2.finance.yahoo.com"]
    for attempt in range(3):
        domain = domains[attempt]
        ua = USER_AGENTS[(hash(ticker) + attempt) % len(USER_AGENTS)]
        url = f"https://{domain}/v8/finance/chart/{ticker}?range={period}&interval={interval}"
        headers = {"User-Agent": ua}

        try:
            yahoo_rate_semaphore.acquire()
            try:
                res = requests.get(url, headers=headers, timeout=8)
            finally:
                yahoo_rate_semaphore.release()

            if res.status_code == 429:
                # Rate limit hit — exponential backoff before retry
                time.sleep(0.6 * (attempt + 1))
                continue

            if res.status_code != 200:
                if attempt == 2:
                    print(f"⚠️ Direct Yahoo Chart API failed for {ticker} (HTTP {res.status_code})")
                time.sleep(0.2 * (attempt + 1))
                continue

            data = res.json()
            result = data.get("chart", {}).get("result", [])
            if not result:
                return None

            chart_data = result[0]
            timestamps = chart_data.get("timestamp", [])
            indicators = chart_data.get("indicators", {}).get("quote", [{}])[0]
            adjusted = chart_data.get("indicators", {}).get("adjclose", [{}])[0].get("adjclose", [])

            if not timestamps or not indicators:
                return None

            df = pd.DataFrame({
                "open": indicators.get("open", []),
                "high": indicators.get("high", []),
                "low": indicators.get("low", []),
                "close": indicators.get("close", []),
                "volume": indicators.get("volume", []),
            }, index=pd.to_datetime(timestamps, unit="s"))

            # Adjust all OHLC prices for splits/dividends. Unadjusted histories create
            # artificial gaps, ranges and moving-average crosses around corporate actions.
            if adjusted and len(adjusted) == len(df):
                adj = pd.to_numeric(pd.Series(adjusted, index=df.index), errors="coerce")
                raw_close = pd.to_numeric(df["close"], errors="coerce")
                factor = (adj / raw_close).replace([np.inf, -np.inf], np.nan).fillna(1.0)
                for price_col in ["open", "high", "low", "close"]:
                    df[price_col] = pd.to_numeric(df[price_col], errors="coerce") * factor

            return clean_ohlcv(df)
        except Exception as exc:
            if attempt == 2:
                print(f"⚠️ Direct Yahoo Chart API error for {ticker}: {exc}")
            time.sleep(0.3 * (attempt + 1))
    return None


def fetch_stock_data_yahoo(ticker: str, period: str = "2y", interval: str = "1d") -> Optional[pd.DataFrame]:
    """Fetch daily/weekly/intraday OHLCV from Yahoo Finance."""
    # 1. Try Direct API first (Fastest & Most Reliable on Cloud IPs)
    df_direct = fetch_stock_data_direct_chart_api(ticker, period, interval)
    if df_direct is not None and not df_direct.empty:
        return df_direct

    # 2. Fallback to yfinance
    try:
        # Never redirect process-wide stderr here: scans run concurrently and one
        # worker can otherwise close the stream while another worker is using it.
        df = yf.download(ticker, period=period, interval=interval, progress=False, auto_adjust=True, threads=False)
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
        clean_symbol = normalize_ticker(ticker)
        token = dynamic_metadata.get(clean_symbol, {}).get("instrument_token")
        if not token:
            scan_cache["errors"].append(f"{clean_symbol}: Zerodha instrument token unavailable")
            return None
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
    """Fetch historical OHLCV data with in-memory caching to avoid redundant fetches across overlapping universes."""
    # 0. Check OHLCV memory cache first (avoids re-fetching nifty50 stocks when scanning nifty200)
    cache_key = f"{ticker}_{period}_{interval}"
    if cache_key in ohlcv_cache:
        cached_time, cached_df = ohlcv_cache[cache_key]
        if time.time() - cached_time < OHLCV_CACHE_TTL:
            return cached_df

    # 1. Try Zerodha FIRST if Zerodha credentials exist
    if ZERODHA_ENCTOKEN or ZERODHA_ACCESS_TOKEN or (ZERODHA_API_KEY and ZERODHA_REQUEST_TOKEN):
        df_zerodha = fetch_stock_data_zerodha(ticker)
        if df_zerodha is not None and not df_zerodha.empty:
            ohlcv_cache[cache_key] = (time.time(), df_zerodha)
            return df_zerodha

    # 2. Fallback to Yahoo Finance (Direct API + yfinance)
    df_yahoo = fetch_stock_data_yahoo(ticker, period, interval)
    if df_yahoo is not None and not df_yahoo.empty:
        ohlcv_cache[cache_key] = (time.time(), df_yahoo)
        return df_yahoo

    # 3. Log clear error when all data providers fail for a ticker
    err_msg = f"🛑 [FETCH FAILED] Could not retrieve stock data for {ticker} from any provider."
    print(err_msg)
    with scan_cache_lock:
        if len(scan_cache["errors"]) < 100:
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
    max_entry_price: float
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
    avg_turnover_cr: float
    rs_3m_pct: Optional[float]
    rs_6m_pct: Optional[float]
    relative_strength_score: int
    swing_score: int
    setup_type: str
    setup_quality: str
    actionable: bool
    rejection_reasons: List[str]
    data_date: str
    data_age_business_days: int
    signal_bar_complete: bool
    market_regime: str
    pe_ratio: Optional[float]
    roe_pct: Optional[float]
    profit_margin_pct: Optional[float]
    is_fundamentally_sound: bool
    fundamental_score: int
    fundamental_rank: int = 1
    fundamental_tier: str = "Good"
    fundamental_color: str = "#34d399"
    profitability_score: float = 50.0
    growth_score: float = 50.0
    financial_strength_score: float = 50.0
    valuation_score: float = 50.0
    cash_flow_score: float = 50.0
    management_score: float = 50.0
    stability_score: float = 50.0
    bonus: float = 0.0
    penalty: float = 0.0
    bonus_reasons: List[str] = field(default_factory=list)
    penalty_reasons: List[str] = field(default_factory=list)
    metric_percentiles: Dict[str, float] = field(default_factory=dict)
    about: str = ""
    industry: str = ""
    market_cap_cr: Optional[float] = None
    pros: List[str] = field(default_factory=list)
    cons: List[str] = field(default_factory=list)
    chart_data: List[float] = field(default_factory=list)
    evidence: List[str] = field(default_factory=list)
    fund_metrics: Dict[str, Any] = field(default_factory=dict)
    last_updated: str = ""
    trade_state: str = "avoid"
    setup_family: str = "Developing Base"
    setup_families: List[str] = field(default_factory=list)
    focus_score: int = 0
    entry_condition: str = ""
    invalidation: str = ""
    volume_percentile: float = 100.0
    range_percentile: float = 100.0
    inside_bar_count: int = 0
    dry_up_alert: bool = False
    dry_up_confirmed: bool = False
    long_base_days: int = 0
    long_base_depth_pct: Optional[float] = None
    listing_date: str = ""
    sessions_since_listing: Optional[int] = None
    is_recent_listing: bool = False


# ============================================================
# FUNDAMENTAL QUALITY ENGINE & CACHE
# ============================================================

FUNDAMENTAL_CACHE_FILE = 'fundamental_cache.json'
FUNDAMENTAL_CACHE_TTL = 7 * 24 * 60 * 60
fundamental_cache: Dict[str, Dict] = {}
fundamental_cache_file_mtime = 0.0

def load_fundamental_cache():
    global fundamental_cache, fundamental_cache_file_mtime
    if os.path.exists(FUNDAMENTAL_CACHE_FILE):
        try:
            with open(FUNDAMENTAL_CACHE_FILE, 'r') as f:
                fundamental_cache = json.load(f)
            fundamental_cache_file_mtime = os.path.getmtime(FUNDAMENTAL_CACHE_FILE)
        except Exception:
            fundamental_cache = {}

def save_fundamental_cache():
    try:
        with open(FUNDAMENTAL_CACHE_FILE, 'w') as f:
            json.dump(fundamental_cache, f, indent=2)
    except Exception:
        pass

load_fundamental_cache()

# ============================================================
# WATCHLIST PERSISTENCE
# ============================================================

WATCHLIST_FILE = 'watchlist.json'
watchlist_tickers: List[str] = []

def load_watchlist():
    global watchlist_tickers
    if os.path.exists(WATCHLIST_FILE):
        try:
            with open(WATCHLIST_FILE, 'r') as f:
                data = json.load(f)
                watchlist_tickers = data if isinstance(data, list) else data.get("tickers", [])
        except Exception:
            watchlist_tickers = []

def save_watchlist():
    try:
        with open(WATCHLIST_FILE, 'w') as f:
            json.dump(watchlist_tickers, f, indent=2)
    except Exception:
        pass

load_watchlist()


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
    """Fetch comprehensive fundamental metrics and company profile for a ticker.
    Extracts ~20+ fields from yfinance to feed the Institutional Scoring Engine."""
    clean_sym = normalize_ticker(ticker)
    cached_metrics = fundamental_cache.get(clean_sym)
    if cached_metrics:
        fetched_at = cached_metrics.get("_fetched_at")
        try:
            cache_time = datetime.fromisoformat(fetched_at).timestamp() if fetched_at else fundamental_cache_file_mtime
        except (TypeError, ValueError):
            cache_time = fundamental_cache_file_mtime
        if cache_time and time.time() - cache_time < FUNDAMENTAL_CACHE_TTL:
            return cached_metrics

    metrics = {
        "eps": None, "pe": None, "roe": None, "roce": None,
        "debt_to_equity": None, "profit_margin": None, "operating_margin": None,
        "net_margin": None, "gross_margin": None,
        "revenue_growth": None, "earnings_growth": None,
        "current_ratio": None, "quick_ratio": None, "interest_coverage": None,
        "peg": None, "price_to_book": None, "ev_ebitda": None, "price_to_sales": None,
        "industry_pe": None,
        "free_cash_flow": None, "operating_cashflow": None, "fcf_margin": None,
        "promoter_holding": None, "promoter_pledge": None,
        "is_fundamentally_sound": True,
        "about": "", "industry": "NSE Equities", "sector": "General",
        "market_cap_cr": None, "pros": [], "cons": []
    }

    try:
        yf_sym = f"{clean_sym}.NS"
        info = {}
        fi = {}
        t = yf.Ticker(yf_sym)
        fi = getattr(t, 'fast_info', {}) or {}
        try:
            info = t.info or {}
        except Exception:
            info = {}

        market_cap = getattr(fi, 'market_cap', None) or (fi.get('market_cap') if isinstance(fi, dict) else None)
        pe_fast = getattr(fi, 'pe_ratio', None) or (fi.get('pe_ratio') if isinstance(fi, dict) else None)

        # === CORE METRICS ===
        eps = info.get('trailingEps') or info.get('forwardEps')
        pe_val = pe_fast or info.get('trailingPE') or info.get('forwardPE')
        roe = info.get('returnOnEquity')
        roce = info.get('returnOnCapital')
        debt_to_equity = info.get('debtToEquity')
        profit_margin = info.get('profitMargins')
        operating_margin = info.get('operatingMargins')
        gross_margin = info.get('grossMargins')
        rev_growth = info.get('revenueGrowth')
        earnings_growth = info.get('earningsGrowth')
        summary_raw = info.get('longBusinessSummary') or info.get('summaryProfile')

        metrics["eps"] = round(float(eps), 2) if eps is not None else None
        metrics["pe"] = round(float(pe_val), 2) if pe_val is not None else None
        metrics["roe"] = round(float(roe * 100), 2) if roe is not None else None
        metrics["debt_to_equity"] = round(float(debt_to_equity), 2) if debt_to_equity is not None else None
        metrics["profit_margin"] = round(float(profit_margin * 100), 2) if profit_margin is not None else None
        metrics["net_margin"] = round(float(profit_margin * 100), 2) if profit_margin is not None else None
        metrics["operating_margin"] = round(float(operating_margin * 100), 2) if operating_margin is not None else None
        metrics["gross_margin"] = round(float(gross_margin * 100), 2) if gross_margin is not None else None
        metrics["revenue_growth"] = round(float(rev_growth * 100), 2) if rev_growth is not None else None
        metrics["earnings_growth"] = round(float(earnings_growth * 100), 2) if earnings_growth is not None else None
        metrics["roce"] = round(float(roce * 100), 2) if roce is not None else None

        # Never manufacture ROCE or interest coverage from broad assumptions. Missing
        # vendor fields stay missing and the scoring engine redistributes their weight.
        ebitda_val = info.get('ebitda')
        total_revenue = info.get('totalRevenue')

        # === FINANCIAL STRENGTH ===
        metrics["current_ratio"] = round(float(info.get('currentRatio')), 2) if info.get('currentRatio') is not None else None
        metrics["quick_ratio"] = round(float(info.get('quickRatio')), 2) if info.get('quickRatio') is not None else None

        if info.get('interestCoverage') is not None:
            metrics["interest_coverage"] = round(float(info.get('interestCoverage')), 2)

        # === VALUATION ===
        metrics["peg"] = round(float(info.get('pegRatio')), 2) if info.get('pegRatio') is not None else None
        metrics["price_to_book"] = round(float(info.get('priceToBook')), 2) if info.get('priceToBook') is not None else None
        metrics["ev_ebitda"] = round(float(info.get('enterpriseToEbitda')), 2) if info.get('enterpriseToEbitda') is not None else None
        metrics["price_to_sales"] = round(float(info.get('priceToSalesTrailing12Months')), 2) if info.get('priceToSalesTrailing12Months') is not None else None

        # Industry PE from sector metadata or use forward PE as proxy
        ind_pe = info.get('industryPE')
        if ind_pe is not None:
            metrics["industry_pe"] = round(float(ind_pe), 2)

        # === CASH FLOW ===
        fcf = info.get('freeCashflow')
        ocf = info.get('operatingCashflow')
        if fcf is not None:
            metrics["free_cash_flow"] = round(float(fcf) / 10000000.0, 2)  # Convert to Cr
        if ocf is not None:
            metrics["operating_cashflow"] = round(float(ocf) / 10000000.0, 2)
        # FCF margin = FCF / Revenue
        if fcf is not None and total_revenue and total_revenue > 0:
            metrics["fcf_margin"] = round(float(fcf / total_revenue * 100), 2)

        # === MANAGEMENT / OWNERSHIP ===
        insiders_pct = info.get('heldPercentInsiders')
        if insiders_pct is not None:
            metrics["promoter_holding"] = round(float(insiders_pct * 100), 2)

        # === MARKET CAP ===
        if market_cap:
            metrics["market_cap_cr"] = round(float(market_cap) / 10000000.0, 2)
        elif info.get('marketCap'):
            metrics["market_cap_cr"] = round(float(info.get('marketCap')) / 10000000.0, 2)

        # === SECTOR/INDUSTRY ===
        metrics["sector"] = info.get('sector') or dynamic_metadata.get(clean_sym, {}).get("sector", "General")
        metrics["industry"] = info.get('industry') or "NSE Equities"

        # === ABOUT ===
        summary_text = summary_raw if summary_raw and isinstance(summary_raw, str) and len(summary_raw.strip()) > 10 else None
        if not summary_text:
            meta_name = dynamic_metadata.get(clean_sym, {}).get("name", clean_sym)
            sec = dynamic_metadata.get(clean_sym, {}).get("sector", "NSE Equities")
            summary_text = f"{meta_name} ({clean_sym}) is NSE-listed. Detailed company profile data is unavailable; verify filings and exchange disclosures before trading. Sector metadata: {sec}."
        if len(summary_text) > 350:
            summary_text = summary_text[:347] + "..."
        metrics["about"] = summary_text

        # === FUNDAMENTAL QUALITY EVALUATION ===
        eps_val = metrics.get("eps")
        roe_val = metrics.get("roe")
        de_val = metrics.get("debt_to_equity")
        margin_val = metrics.get("profit_margin")
        pe_check = metrics.get("pe")

        is_sound = True
        if eps_val is None or eps_val <= 0:
            is_sound = False
        if roe_val is None or roe_val < 10.0:
            is_sound = False
        if margin_val is not None and margin_val < 3.0:
            is_sound = False
        if de_val is not None and de_val > 100.0:
            is_sound = False
        if pe_check is not None and (pe_check <= 0 or pe_check > 75.0):
            is_sound = False

        metrics["is_fundamentally_sound"] = is_sound
        metrics["fundamental_score"] = calculate_fundamental_score(metrics)

    except Exception:
        pass

    metrics["_fetched_at"] = datetime.now().isoformat()
    fundamental_cache[clean_sym] = metrics
    return metrics


def calculate_fundamental_score(metrics: Dict) -> int:
    """Calculate normalized Fundamental Quality Score (0 to 100)."""
    if not metrics:
        return 0
        
    eps = metrics.get("eps")
    roe = metrics.get("roe")
    margin = metrics.get("profit_margin")
    de = metrics.get("debt_to_equity")
    pe = metrics.get("pe")
    
    score = 0
    
    # 1. ROE % (Up to 40 pts)
    if roe is not None:
        if roe >= 25.0: score += 40
        elif roe >= 18.0: score += 32
        elif roe >= 12.0: score += 22
        elif roe >= 5.0: score += 12
        elif roe > 0: score += 5
        
    # 2. Net Profit Margin % (Up to 25 pts)
    if margin is not None:
        if margin >= 20.0: score += 25
        elif margin >= 12.0: score += 18
        elif margin >= 5.0: score += 12
        elif margin > 0: score += 5

    # 3. Debt to Equity % (Up to 20 pts)
    if de is not None:
        if de <= 30.0: score += 20
        elif de <= 80.0: score += 15
        elif de <= 120.0: score += 8
    else:
        score += 10  # Moderate score for missing debt data
        
    # 4. Earnings & Valuation (Up to 15 pts)
    if eps is not None and eps > 0:
        score += 5
    if pe is not None and 8.0 <= pe <= 55.0:
        score += 10
    elif pe is not None and 0 < pe <= 75.0:
        score += 5

    return int(min(100, max(0, score)))


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


def _period_return(close: pd.Series, sessions: int) -> Optional[float]:
    if len(close) <= sessions or float(close.iloc[-sessions - 1]) <= 0:
        return None
    return float((close.iloc[-1] / close.iloc[-sessions - 1] - 1.0) * 100.0)


def _business_day_age(index_value: Any, reference_date: Optional[Any] = None) -> Tuple[str, int]:
    stamp = pd.Timestamp(index_value)
    data_date = stamp.date()
    today = pd.Timestamp(reference_date).date() if reference_date is not None else datetime.now().date()
    if data_date >= today:
        return data_date.isoformat(), 0
    return data_date.isoformat(), int(np.busday_count(data_date, today))


def _market_regime(benchmark_df: Optional[pd.DataFrame]) -> str:
    if benchmark_df is None or len(benchmark_df) < 50:
        return "unknown"
    close = benchmark_df["close"]
    price = float(close.iloc[-1])
    ma50 = float(close.tail(50).mean())
    ma200 = float(close.tail(min(200, len(close))).mean())
    if price > ma50 > ma200:
        return "favorable"
    if price > ma200:
        return "neutral"
    return "risk_off"


def _daily_signal_bar_complete(index_value: Any) -> bool:
    """Daily vendor candles are provisional until shortly after the NSE close."""
    now_ist = datetime.now(ZoneInfo("Asia/Kolkata"))
    data_date = pd.Timestamp(index_value).date()
    if data_date != now_ist.date() or now_ist.weekday() >= 5:
        return True
    market_open = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
    finalization_buffer = now_ist.replace(hour=15, minute=40, second=0, microsecond=0)
    return not (market_open <= now_ist < finalization_buffer)


def _percentile_rank(history: pd.Series, value: float) -> float:
    clean = pd.to_numeric(history, errors="coerce").dropna()
    if clean.empty:
        return 100.0
    return float((clean <= value).mean() * 100.0)


def _range_pct(frame: pd.DataFrame) -> float:
    if frame is None or frame.empty:
        return 999.0
    mean_close = float(frame["close"].mean())
    return float((frame["high"].max() - frame["low"].min()) / mean_close * 100.0) if mean_close > 0 else 999.0


def calculate_vcp_score(
    df: pd.DataFrame,
    benchmark_df: Optional[pd.DataFrame] = None,
    listing_date: Optional[str] = None,
    as_of_date: Optional[Any] = None,
) -> Optional[Dict]:
    """Classify VCP, dry-up, IPO-base and long-base setups from completed OHLCV evidence."""
    if df is None or len(df) < 60:
        return None

    df = clean_ohlcv(df)
    if df is None or len(df) < 60:
        return None

    current_price = float(df["close"].iloc[-1])
    previous_close = float(df["close"].iloc[-2])
    latest_high = float(df["high"].iloc[-1])
    current_vol = float(df["volume"].iloc[-1])
    prior_20 = df.iloc[-21:-1]
    prior_50 = df.iloc[-51:-1]
    recent_10 = df.tail(10)

    # The signal bar is excluded from baselines. Median volume is more robust for
    # IPOs and event spikes; the arithmetic mean remains exposed for display.
    avg_vol_20d = float(prior_20["volume"].mean())
    median_vol_20d = float(prior_20["volume"].median())
    volume_ratio = float(current_vol / median_vol_20d) if median_vol_20d > 0 else 0.0
    avg_turnover_cr = float(current_price * median_vol_20d / 10_000_000.0)
    vol_last5 = float(df.iloc[-6:-1]["volume"].mean())
    vol_50d = float(prior_50["volume"].mean()) if not prior_50.empty else avg_vol_20d
    dry_up_ratio = float(vol_last5 / vol_50d) if vol_50d > 0 else 1.0
    vol_first15 = float(prior_20.head(15)["volume"].mean())
    vol_decline = float((vol_first15 - vol_last5) / vol_first15) if vol_first15 > 0 else 0.0

    true_range = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift()).abs(),
        (df["low"] - df["close"].shift()).abs(),
    ], axis=1).max(axis=1)
    volume_percentile = _percentile_rank(df["volume"].iloc[-51:-1], current_vol)
    range_percentile = _percentile_rank(true_range.iloc[-51:-1], float(true_range.iloc[-1]))
    previous_volume_percentile = _percentile_rank(df["volume"].iloc[-52:-2], float(df["volume"].iloc[-2]))
    previous_range_percentile = _percentile_rank(true_range.iloc[-52:-2], float(true_range.iloc[-2]))
    inside_bar_count = sum(
        1 for i in range(max(1, len(df) - 6), len(df))
        if float(df["high"].iloc[i]) <= float(df["high"].iloc[i - 1])
        and float(df["low"].iloc[i]) >= float(df["low"].iloc[i - 1])
    )

    atr = calculate_atr(df)
    atr_pct = float(atr / current_price * 100.0) if current_price > 0 else 999.0
    base_high = float(prior_20["high"].max())
    base_low = float(prior_20["low"].min())
    range_pct = _range_pct(prior_20)
    price_tightness = float(prior_20["close"].std() / prior_20["close"].mean() * 100.0)
    days_in_range = int(((prior_20["close"] >= current_price * 0.95) &
                         (prior_20["close"] <= current_price * 1.05)).sum())

    close = df["close"]
    ma50 = float(close.tail(50).mean())
    ma150 = float(close.tail(min(150, len(close))).mean())
    ma200 = float(close.tail(min(200, len(close))).mean())
    ma200_month_ago = float(close.iloc[-220:-20].mean()) if len(close) >= 220 else ma200
    year = df.tail(min(252, len(df)))
    high_52w = float(year["high"].max())
    low_52w = float(year["low"].min())

    trend_score = 0
    if current_price > ma50: trend_score += 5
    if current_price > ma150: trend_score += 4
    if current_price > ma200: trend_score += 4
    if ma50 > ma150: trend_score += 4
    if ma150 > ma200: trend_score += 3
    if ma200 >= ma200_month_ago: trend_score += 2
    if low_52w > 0 and current_price >= 1.25 * low_52w: trend_score += 2
    if high_52w > 0 and current_price >= 0.80 * high_52w: trend_score += 1
    if len(df) < 200:
        trend_score = min(trend_score, 17)

    c1 = _range_pct(df.iloc[-61:-41])
    c2 = _range_pct(df.iloc[-41:-21])
    c3 = _range_pct(prior_20)
    contraction_score = 20 if c1 > c2 > c3 else 15 if c2 > c3 else 10 if c3 <= 12 else 5 if c3 <= 20 else 0
    tightness_score = (
        (8 if price_tightness <= 2.0 else 5 if price_tightness <= 3.5 else 2 if price_tightness <= 5.0 else 0)
        + (7 if atr_pct <= 2.5 else 4 if atr_pct <= 4.0 else 1 if atr_pct <= 6.0 else 0)
    )
    has_valid_base = contraction_score >= 10 and tightness_score >= 5
    volume_expansion = volume_ratio >= 1.50
    signal_bar_complete = _daily_signal_bar_complete(df.index[-1])

    # Recent IPOs are explicitly identified from official listing metadata. Short
    # price history alone is not enough to label an ordinary stock an IPO.
    listing_date_value = ""
    sessions_since_listing = None
    is_recent_listing = False
    try:
        if listing_date:
            listing_stamp = pd.Timestamp(listing_date).date()
            data_stamp = pd.Timestamp(df.index[-1]).date()
            listing_age_days = (data_stamp - listing_stamp).days
            listing_date_value = listing_stamp.isoformat()
            sessions_since_listing = int(len(df))
            is_recent_listing = 45 <= listing_age_days <= 730
    except (TypeError, ValueError):
        pass

    ipo_frame = df.iloc[-61:-1]
    ipo_base_depth = float((ipo_frame["high"].max() - ipo_frame["low"].min()) / ipo_frame["high"].max() * 100.0)
    ipo_base_valid = (
        is_recent_listing and sessions_since_listing is not None and sessions_since_listing >= 60
        and ipo_base_depth <= 30.0 and has_valid_base and trend_score >= 10 and avg_turnover_cr >= 10.0
    )

    # Long-base candidates must contain roughly one trading year of history, be
    # reasonably contained overall, and tighten on the right side of the base.
    long_window = df.iloc[-253:-1] if len(df) >= 253 else df.iloc[:-1]
    long_base_days = int(len(long_window)) if len(df) >= 240 else 0
    long_pivot = float(long_window["high"].max()) if long_base_days else base_high
    long_base_depth = (
        float((long_window["high"].max() - long_window["low"].min()) / long_window["high"].max() * 100.0)
        if long_base_days and float(long_window["high"].max()) > 0 else None
    )
    recent_60_depth = _range_pct(df.iloc[-61:-1])
    long_distance_pct = float((long_pivot - current_price) / current_price * 100.0) if long_pivot > 0 else 999.0
    long_base_valid = bool(
        long_base_days >= 239 and long_base_depth is not None and long_base_depth <= 40.0
        and recent_60_depth <= 20.0 and current_price > ma200 and ma200 >= ma200_month_ago
    )
    long_near_pivot = long_base_valid and -0.75 <= long_distance_pct <= 5.0
    long_breakout = long_base_valid and current_price >= long_pivot * 1.003 and volume_expansion

    base_distance_pct = float((base_high - current_price) / current_price * 100.0)
    price_breakout = current_price >= base_high * 1.003
    near_base_pivot = -0.75 <= base_distance_pct <= 5.0
    base_breakout = price_breakout and volume_expansion and has_valid_base and trend_score >= (10 if is_recent_listing else 15)

    dry_up_alert = bool(
        volume_percentile <= 15.0 and range_percentile <= 25.0 and near_base_pivot
        and has_valid_base and (inside_bar_count >= 1 or price_tightness <= 3.5)
    )
    dry_up_confirmed = bool(
        previous_volume_percentile <= 15.0 and previous_range_percentile <= 25.0
        and base_breakout and current_price > float(df["high"].iloc[-2])
    )

    setup_families = []
    if has_valid_base and trend_score >= (10 if is_recent_listing else 15): setup_families.append("VCP")
    if dry_up_alert or dry_up_confirmed: setup_families.append("Volume Dry-Up")
    if long_near_pivot or long_breakout: setup_families.append("Long Base")
    if ipo_base_valid and (near_base_pivot or base_breakout): setup_families.append("IPO Base")
    if not setup_families and has_valid_base: setup_families.append("Tight Base")

    any_breakout = base_breakout or long_breakout
    confirmed_breakout = any_breakout and signal_bar_complete
    intraday_breakout_watch = any_breakout and not signal_bar_complete

    if long_breakout:
        setup_family, selected_pivot = "Long Base Breakout", long_pivot
    elif ipo_base_valid and base_breakout:
        setup_family, selected_pivot = "IPO Base Breakout", base_high
    elif dry_up_confirmed:
        setup_family, selected_pivot = "Dry-Up Breakout", base_high
    elif base_breakout:
        setup_family, selected_pivot = "VCP Breakout", base_high
    elif long_near_pivot:
        setup_family, selected_pivot = "Long Base Watch", long_pivot
    elif ipo_base_valid and near_base_pivot:
        setup_family, selected_pivot = "IPO Base Watch", base_high
    elif dry_up_alert:
        setup_family, selected_pivot = "Volume Dry-Up Alert", base_high
    elif near_base_pivot and has_valid_base:
        setup_family, selected_pivot = "VCP Watch", base_high
    else:
        setup_family, selected_pivot = "Developing Base", base_high

    breakout_distance_pct = float((selected_pivot - current_price) / current_price * 100.0)
    extension_pct = float((current_price / selected_pivot - 1.0) * 100.0) if selected_pivot > 0 else 999.0
    overextended = extension_pct > 5.0
    entry_price = float(selected_pivot * 1.003)
    max_entry_price = float(selected_pivot * 1.05)
    raw_stop = max(float(recent_10["low"].min()), current_price - (2.0 * atr))
    stop_loss = min(raw_stop, current_price * 0.995)
    risk_reference = current_price if current_price > selected_pivot else entry_price
    risk_pct = float((risk_reference - stop_loss) / risk_reference * 100.0) if risk_reference > 0 else 999.0
    rejected_at_pivot = latest_high >= selected_pivot * 1.003 and current_price < selected_pivot

    if confirmed_breakout:
        volume_score = 15 if volume_ratio >= 2.0 else 12 if volume_ratio >= 1.5 else 9
    else:
        volume_score = 15 if dry_up_alert else 12 if dry_up_ratio <= 0.75 else 8 if dry_up_ratio <= 0.90 else 4 if dry_up_ratio <= 1.05 else 0
    pivot_score = 10 if -0.75 <= breakout_distance_pct <= 2.0 else 7 if breakout_distance_pct <= 5.0 else 3 if breakout_distance_pct <= 8.0 else 0
    if rejected_at_pivot:
        pivot_score = max(0, pivot_score - 4)

    stock_3m = _period_return(close, 63)
    stock_6m = _period_return(close, 126)
    benchmark_3m = _period_return(benchmark_df["close"], 63) if benchmark_df is not None else None
    benchmark_6m = _period_return(benchmark_df["close"], 126) if benchmark_df is not None else None
    rs_3m = stock_3m - benchmark_3m if stock_3m is not None and benchmark_3m is not None else None
    rs_6m = stock_6m - benchmark_6m if stock_6m is not None and benchmark_6m is not None else None
    rs_blend = rs_3m if rs_3m is not None else rs_6m
    relative_strength_score = 7 if rs_blend is None else 15 if rs_blend >= 10 else 12 if rs_blend >= 5 else 9 if rs_blend >= 0 else 5 if rs_blend >= -5 else 0

    vcp_score = int(min(100, round(
        trend_score + contraction_score + tightness_score + volume_score + pivot_score + relative_strength_score
    )))
    regime = _market_regime(benchmark_df)
    liquidity_points = 5 if avg_turnover_cr >= 25 else 3 if avg_turnover_cr >= 10 else 1 if avg_turnover_cr >= 5 else 0
    risk_points = 5 if 0 < risk_pct <= 5 else 3 if risk_pct <= 7 else 0
    confirmed_families = [family for family in setup_families if family != "Volume Dry-Up"]
    pattern_points = min(4, max(0, len(confirmed_families) - 1) * 2)
    swing_score = int(round(
        trend_score / 25 * 20 +
        (contraction_score + tightness_score) / 35 * 25 +
        pivot_score / 10 * 15 +
        volume_score / 15 * 15 +
        relative_strength_score + liquidity_points + risk_points + pattern_points
    ))
    if regime == "risk_off": swing_score -= 12
    if rejected_at_pivot: swing_score -= 8
    if overextended: swing_score -= 15
    swing_score = int(min(100, max(0, swing_score)))

    data_date, data_age = _business_day_age(df.index[-1], reference_date=as_of_date)
    rejection_reasons = []
    if current_price < 20: rejection_reasons.append("Price below ₹20")
    if avg_turnover_cr < 5: rejection_reasons.append(f"Low liquidity (₹{avg_turnover_cr:.1f} Cr/day)")
    if data_age > 3: rejection_reasons.append(f"Stale data ({data_age} business days old)")
    if trend_score < (10 if is_recent_listing else 15): rejection_reasons.append("Leadership trend not confirmed")
    if risk_pct <= 0 or risk_pct > 8: rejection_reasons.append(f"Unfavorable stop risk ({risk_pct:.1f}%)")
    if overextended: rejection_reasons.append(f"Already {extension_pct:.1f}% above pivot")
    if rejected_at_pivot: rejection_reasons.append("Pivot rejection: high crossed, close failed")

    if confirmed_breakout:
        status, setup_type, vcp_grade = "breakout", setup_family, "Confirmed close + volume"
    elif intraday_breakout_watch:
        status, setup_type, vcp_grade = "ready", "Intraday Breakout Watch", "Awaiting daily close"
    elif setup_family.endswith("Watch") or dry_up_alert:
        status, setup_type, vcp_grade = "ready", setup_family, "Watch for exact trigger"
    elif has_valid_base:
        status, setup_type, vcp_grade = "forming", "Base Forming", "Constructive but not ready"
    else:
        status, setup_type, vcp_grade = "weak", "Developing / Avoid", "Insufficient confirmation"

    critical_rejections = [reason for reason in rejection_reasons if not reason.startswith("Pivot rejection")]
    trade_state = "avoid"
    if (
        status == "breakout" and swing_score >= 72 and not critical_rejections
        and regime in {"favorable", "neutral"} and avg_turnover_cr >= 10
    ):
        trade_state = "trade_now"
    elif status == "ready" and swing_score >= 65 and not critical_rejections:
        trade_state = "watch_trigger"
    actionable = trade_state in {"trade_now", "watch_trigger"}
    setup_quality = "A" if trade_state == "trade_now" and swing_score >= 80 else "B" if actionable else "Watch" if status in {"ready", "forming"} else "Reject"

    base_quality_score = int(min(100, round((contraction_score + tightness_score + trend_score) / 60 * 100)))
    pivot_readiness_score = int(min(100, round((pivot_score + tightness_score + volume_score) / 40 * 100)))
    daily_gain_pct = (current_price / previous_close - 1.0) * 100.0 if previous_close > 0 else 0.0
    breakout_score = int(min(100, round(
        (min(volume_ratio, 2.5) / 2.5 * 40) +
        (min(max(daily_gain_pct, 0), 6.0) / 6.0 * 20) +
        ((contraction_score + tightness_score) / 35 * 25) +
        (trend_score / 25 * 15)
    )))

    evidence = []
    if trend_score >= 20: evidence.append("Stage-2 price and moving-average trend")
    if c1 > c2 > c3: evidence.append(f"Contracting ranges: {c1:.1f}% → {c2:.1f}% → {c3:.1f}%")
    if dry_up_alert: evidence.append(f"Volume/range dry-up: {volume_percentile:.0f}th/{range_percentile:.0f}th percentile")
    elif dry_up_ratio <= 0.75: evidence.append(f"Supply dry-up: {dry_up_ratio:.2f}x 50-day volume")
    if long_base_valid: evidence.append(f"{long_base_days}-session base; {long_base_depth:.1f}% depth")
    if ipo_base_valid: evidence.append(f"Liquid IPO base ({sessions_since_listing} trading sessions)")
    if confirmed_breakout: evidence.append(f"Closed above ₹{selected_pivot:.2f} pivot on {volume_ratio:.2f}x median volume")
    elif intraday_breakout_watch: evidence.append(f"Above ₹{selected_pivot:.2f}; confirmation waits for the daily close")
    elif status == "ready": evidence.append(f"Price is {breakout_distance_pct:.1f}% from ₹{selected_pivot:.2f} trigger")
    if rs_3m is not None: evidence.append(f"3-month relative strength vs NIFTY: {rs_3m:+.1f}%")

    entry_condition = (
        f"Buy only after a completed daily close above ₹{entry_price:.2f} with at least 1.5x normal volume"
        if trade_state != "trade_now" else
        f"Valid only while price is between ₹{entry_price:.2f} and ₹{max_entry_price:.2f}; avoid chasing"
    )
    invalidation = f"Exit thesis below ₹{stop_loss:.2f}; planned chart risk {risk_pct:.1f}%"
    contraction = "Tight" if range_pct < 6 and price_tightness < 2.5 else "Tightening" if range_pct < 10 else "Moderate" if range_pct < 15 else "Wide"
    volume_trend = "Spike" if volume_ratio >= 2 else "Expansion" if volume_ratio >= 1.3 else "Dry-Up" if dry_up_alert else "Declining" if vol_decline > 0.2 else "Normal"
    breakout_grade = "Institutional Thrust" if breakout_score >= 80 else "Strong Breakout" if breakout_score >= 65 else "Unconfirmed"
    pivot_grade = "Coiled A+ Pivot" if pivot_readiness_score >= 80 else "High Alert Pivot" if pivot_readiness_score >= 60 else "Approaching Pivot"
    base_grade = "A+ Base" if base_quality_score >= 80 else "Constructive Base" if base_quality_score >= 60 else "Developing Base"

    return {
        "vcp_score": vcp_score, "vcp_grade": vcp_grade,
        "breakout_score": breakout_score, "breakout_grade": breakout_grade,
        "pivot_readiness_score": pivot_readiness_score, "pivot_readiness_grade": pivot_grade,
        "base_quality_score": base_quality_score, "base_quality_grade": base_grade,
        "contraction": contraction, "volume_trend": volume_trend, "status": status,
        "atr": round(atr, 2), "atr_pct": round(atr_pct, 2),
        "volume_ratio": round(volume_ratio, 2), "dry_up_ratio": round(dry_up_ratio, 2),
        "price_tightness": round(price_tightness, 2), "support_level": round(base_low, 2),
        "resistance_level": round(selected_pivot, 2), "entry_price": round(entry_price, 2),
        "max_entry_price": round(max_entry_price, 2),
        "stop_loss": round(stop_loss, 2), "risk_pct": round(risk_pct, 2),
        "breakout_distance_pct": round(breakout_distance_pct, 2), "trend_score": trend_score,
        "contraction_score": contraction_score, "volume_score": volume_score,
        "tightness_score": tightness_score, "pivot_score": pivot_score,
        "days_in_range": days_in_range, "avg_volume_20d": round(avg_vol_20d, 0),
        "current_volume": round(current_vol, 0), "avg_turnover_cr": round(avg_turnover_cr, 2),
        "rs_3m_pct": round(rs_3m, 2) if rs_3m is not None else None,
        "rs_6m_pct": round(rs_6m, 2) if rs_6m is not None else None,
        "relative_strength_score": relative_strength_score, "swing_score": swing_score,
        "focus_score": swing_score, "trade_state": trade_state,
        "setup_type": setup_type, "setup_family": setup_family, "setup_families": setup_families,
        "setup_quality": setup_quality, "actionable": actionable,
        "entry_condition": entry_condition, "invalidation": invalidation,
        "volume_percentile": round(volume_percentile, 1), "range_percentile": round(range_percentile, 1),
        "inside_bar_count": inside_bar_count, "dry_up_alert": dry_up_alert, "dry_up_confirmed": dry_up_confirmed,
        "long_base_days": long_base_days, "long_base_depth_pct": round(long_base_depth, 2) if long_base_depth is not None else None,
        "listing_date": listing_date_value, "sessions_since_listing": sessions_since_listing,
        "is_recent_listing": is_recent_listing,
        "rejection_reasons": rejection_reasons, "data_date": data_date,
        "data_age_business_days": data_age, "signal_bar_complete": signal_bar_complete,
        "market_regime": regime,
        "evidence": evidence or ["No high-conviction breakout evidence yet"],
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


def scan_stock(ticker: str, benchmark_df: Optional[pd.DataFrame] = None) -> Optional[VCPResult]:
    """Scan a single stock for VCP pattern and fundamental quality."""
    ticker = ticker if ticker.startswith("^") else f"{normalize_ticker(ticker)}.NS"
    df = fetch_stock_data(ticker)
    if df is None:
        return None

    clean_ticker = normalize_ticker(ticker)
    listing_date = dynamic_metadata.get(clean_ticker, {}).get("listing_date")
    vcp_data = calculate_vcp_score(df, benchmark_df=benchmark_df, listing_date=listing_date)
    if vcp_data is None:
        return None

    # Fundamental Health Evaluation — Filter out chronic loss-makers & over-indebted firms
    fund_metrics = fetch_fundamental_metrics(ticker)
    eps_val = fund_metrics.get("eps")
    roe_val = fund_metrics.get("roe")
    de_val = fund_metrics.get("debt_to_equity")

    if eps_val is not None and eps_val <= 0 and roe_val is not None and roe_val < 0:
        return None  # Exclude chronic loss-making companies
    if de_val is not None and de_val > 150.0:
        return None  # Exclude over-leveraged companies

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
        max_entry_price=vcp_data["max_entry_price"],
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
        avg_turnover_cr=vcp_data["avg_turnover_cr"],
        rs_3m_pct=vcp_data["rs_3m_pct"],
        rs_6m_pct=vcp_data["rs_6m_pct"],
        relative_strength_score=vcp_data["relative_strength_score"],
        swing_score=vcp_data["swing_score"],
        setup_type=vcp_data["setup_type"],
        setup_quality=vcp_data["setup_quality"],
        actionable=vcp_data["actionable"],
        rejection_reasons=vcp_data["rejection_reasons"],
        data_date=vcp_data["data_date"],
        data_age_business_days=vcp_data["data_age_business_days"],
        signal_bar_complete=vcp_data["signal_bar_complete"],
        market_regime=vcp_data["market_regime"],
        pe_ratio=fund_metrics.get("pe"),
        roe_pct=fund_metrics.get("roe"),
        profit_margin_pct=fund_metrics.get("profit_margin"),
        is_fundamentally_sound=fund_metrics.get("is_fundamentally_sound", True),
        fundamental_score=fund_metrics.get("fundamental_score", 0),
        about=fund_metrics.get("about", ""),
        industry=fund_metrics.get("industry", info["sector"]),
        market_cap_cr=fund_metrics.get("market_cap_cr"),
        pros=pros,
        cons=cons,
        chart_data=[round(x, 2) for x in chart_data],
        evidence=vcp_data["evidence"],
        fund_metrics=fund_metrics,
        last_updated=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        trade_state=vcp_data["trade_state"],
        setup_family=vcp_data["setup_family"],
        setup_families=vcp_data["setup_families"],
        focus_score=vcp_data["focus_score"],
        entry_condition=vcp_data["entry_condition"],
        invalidation=vcp_data["invalidation"],
        volume_percentile=vcp_data["volume_percentile"],
        range_percentile=vcp_data["range_percentile"],
        inside_bar_count=vcp_data["inside_bar_count"],
        dry_up_alert=vcp_data["dry_up_alert"],
        dry_up_confirmed=vcp_data["dry_up_confirmed"],
        long_base_days=vcp_data["long_base_days"],
        long_base_depth_pct=vcp_data["long_base_depth_pct"],
        listing_date=vcp_data["listing_date"],
        sessions_since_listing=vcp_data["sessions_since_listing"],
        is_recent_listing=vcp_data["is_recent_listing"],
    )


def scan_universe(tickers: List[str], min_score: int = 0, 
                  min_price: float = 0, max_price: float = 999999) -> List[VCPResult]:
    """Scan multiple stocks in parallel using ThreadPoolExecutor."""
    results = []
    scan_cache["errors"] = []
    benchmark_df = fetch_stock_data("^NSEI")

    def _worker(ticker: str) -> Optional[VCPResult]:
        try:
            res = scan_stock(ticker, benchmark_df=benchmark_df)
            if res and min_price <= res.price <= max_price:
                if min_score == 0 or res.vcp_score >= min_score:
                    return res
        except Exception as e:
            scan_cache["errors"].append(f"{ticker}: {str(e)}")
        return None

    with ThreadPoolExecutor(max_workers=MAX_SCAN_WORKERS) as executor:
        futures = {executor.submit(_worker, ticker): ticker for ticker in tickers}
        for future in as_completed(futures):
            res = future.result()
            if res is not None:
                results.append(res)

    results.sort(key=lambda x: (x.actionable, x.swing_score, x.vcp_score), reverse=True)
    serialized = [serialize_result(r) for r in results]
    return process_universe_fundamental_scores(serialized)


def clean_json_value(val):
    if isinstance(val, float):
        if math.isinf(val) or math.isnan(val):
            return None
        return val
    elif isinstance(val, dict):
        return {k: clean_json_value(v) for k, v in val.items()}
    elif isinstance(val, list):
        return [clean_json_value(x) for x in val]
    return val


def serialize_result(result):
    """Convert scan rows into JSON-safe dictionaries without NaN/Infinity.
    Flattens fund_metrics into top-level dict so scoring engine can access all metric keys."""
    d = asdict(result) if isinstance(result, VCPResult) else (dict(result) if isinstance(result, dict) else {})
    # Flatten fund_metrics into top-level for scoring engine access
    fm = d.pop("fund_metrics", None)
    if fm and isinstance(fm, dict):
        for k, v in fm.items():
            if k not in d:  # Don't overwrite existing fields
                d[k] = v
    return clean_json_value(d)


def process_universe_fundamental_scores(result_dicts: List[Dict]) -> List[Dict]:
    """Add fundamental ranks without destroying the scanner's technical ranking order."""
    if not result_dicts:
        return []
    original_order = {
        str(row.get("ticker", "")): position for position, row in enumerate(result_dicts)
    }
    evaluated = fundamental_engine.evaluate_universe(result_dicts)
    evaluated.sort(key=lambda row: original_order.get(str(row.get("ticker", "")), len(original_order)))
    return evaluated


def load_cache():
    """Load cached scan results for instant startup display."""
    for filepath in [CACHE_FILE, f"{CACHE_FILE}.bak"]:
        if os.path.exists(filepath) and os.path.getsize(filepath) > 10:
            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)
                if int(data.get("schema_version", 0) or 0) != SCAN_SCHEMA_VERSION:
                    print(f"Ignoring legacy scan cache from {filepath}; a fresh scan is required.")
                    continue
                scan_cache["last_scan"] = data.get("last_scan")
                res = data.get("results", {})
                if res and any(len(v) > 0 for v in res.values()):
                    scan_cache["results"] = res
                    print(f"📁 Loaded cached results from {filepath}")
                    return
            except Exception as e:
                print(f"Cache load error from {filepath}: {e}")


def save_cache():
    """Save scan results atomically to prevent file corruption."""
    try:
        data = {
            "schema_version": SCAN_SCHEMA_VERSION,
            "last_scan": scan_cache["last_scan"],
            "results": {k: [serialize_result(r) for r in v] for k, v in scan_cache["results"].items() if v}
        }
        tmp_file = f"{CACHE_FILE}.tmp"
        bak_file = f"{CACHE_FILE}.bak"
        with open(tmp_file, 'w') as f:
            json.dump(data, f, default=str)
        
        if os.path.exists(CACHE_FILE) and os.path.getsize(CACHE_FILE) > 10:
            try:
                os.replace(CACHE_FILE, bak_file)
            except Exception:
                pass
        os.replace(tmp_file, CACHE_FILE)
    except Exception as e:
        print(f"Cache save error: {e}")


# ============================================================
# AUTO-SCAN SCHEDULER
# ============================================================

def scan_single_universe_bg(universe_name: str):
    """Scan a single universe in background thread when requested via API."""
    flag = f"scanning_{universe_name}"
    with scan_cache_lock:
        if scan_cache.get(flag):
            return
        scan_cache[flag] = True
    try:
        tickers = get_stock_universe(universe_name)
        if tickers:
            print(f"⚡ Scanning universe '{universe_name}' ({len(tickers)} stocks)...")
            results = scan_universe(tickers, min_score=0)
            scan_cache["results"][universe_name] = results
            scan_cache["last_scan"] = datetime.now().isoformat()
            save_cache()
            print(f"  ✓ {universe_name}: {len(results)} analyzed")
    finally:
        with scan_cache_lock:
            scan_cache[flag] = False


def auto_scan():
    """Run scheduled scan for all dynamic universes."""
    print(f"\n🔄 Auto-scan started at {datetime.now()}")
    with scan_cache_lock:
        scan_cache["is_scanning"] = True
        scan_cache["errors"] = []

    try:
        # Nifty 500 already contains the smaller index universes. Scan the two
        # decision sources only; explorer universes remain available on demand.
        for universe_name in FOCUS_SOURCE_UNIVERSES:
            scan_single_universe_bg(universe_name)

        with scan_cache_lock:
            scan_cache["last_scan"] = datetime.now().isoformat()
        save_cache()
        save_fundamental_cache()  # Batch write after full scan
        print(f"✅ Auto-scan complete at {datetime.now()}\n")
    finally:
        with scan_cache_lock:
            scan_cache["is_scanning"] = False


load_cache()


def scan_cache_is_fresh(max_age_seconds: int = OHLCV_CACHE_TTL) -> bool:
    """Avoid an expensive duplicate startup scan when a recent complete cache exists."""
    last_scan = scan_cache.get("last_scan")
    required = set(FOCUS_SOURCE_UNIVERSES)
    if not last_scan or not required.issubset(scan_cache.get("results", {})):
        return False
    try:
        age = (datetime.now() - datetime.fromisoformat(last_scan)).total_seconds()
        return 0 <= age < max_age_seconds
    except (TypeError, ValueError):
        return False

if os.getenv("VCP_DISABLE_SCHEDULER") != "1":
    scheduler = BackgroundScheduler()
    scheduler.add_job(auto_scan, 'cron', hour='9-15', minute='*/30', day_of_week='mon-fri')
    scheduler.add_job(auto_scan, 'cron', hour=9, minute=20, day_of_week='mon-fri')
    scheduler.add_job(sync_dynamic_universes, 'cron', hour=8, minute=0, day_of_week='mon-fri')
    scheduler.start()

    if os.getenv("VCP_SKIP_INITIAL_SCAN") != "1" and not scan_cache_is_fresh():
        def staggered_initial_scan():
            """Scan priority universes first, then defer larger ones."""
            print("Running initial background scan (priority universes first)...")
            with scan_cache_lock:
                scan_cache["is_scanning"] = True
                scan_cache["errors"] = []
            try:
                for uni in FOCUS_SOURCE_UNIVERSES:
                    scan_single_universe_bg(uni)

                save_fundamental_cache()  # Batch write after full scan
                print(f"✅ Initial scan complete at {datetime.now()}")
            finally:
                with scan_cache_lock:
                    scan_cache["is_scanning"] = False

        threading.Thread(target=staggered_initial_scan, daemon=True).start()
    elif os.getenv("VCP_SKIP_INITIAL_SCAN") != "1":
        print("Recent scan cache found; skipping duplicate startup scan.")


# ============================================================
# FLASK ROUTES
# ============================================================

@app.route('/')
def index():
    """Serve the main HTML template."""
    return render_template('index.html')


@app.route('/api/scan', methods=['POST'])
def api_scan():
    """Manual scan endpoint - launches background scan thread and returns immediately."""
    data = request.json or {}
    universe = data.get('universe', 'nifty200')
    allowed_universes = set(dynamic_universes) | set(STOCK_UNIVERSES)
    if universe not in allowed_universes:
        return jsonify({"success": False, "error": "Unknown stock universe"}), 400

    # Launch background scan thread so HTTP response returns instantly
    threading.Thread(target=scan_single_universe_bg, args=(universe,), daemon=True).start()

    return jsonify({
        "success": True,
        "message": f"Scan launched for {universe}. Auto-refreshing UI...",
        "universe": universe,
        "is_scanning": True
    })


def calculate_swing_conviction_score(stock) -> Tuple[int, str]:
    """Calculate Conviction Score (0-100) for Swing Trading based on risk-reward, pivot proximity, volume dry-up & fundamentals."""
    stored_score = stock.get('swing_score') if isinstance(stock, dict) else getattr(stock, 'swing_score', None)
    if stored_score is not None:
        evidence = stock.get('evidence', []) if isinstance(stock, dict) else getattr(stock, 'evidence', [])
        return int(stored_score), " • ".join(evidence[:3]) if evidence else "Quantified breakout setup"
    vcp_score = stock.get('vcp_score', 0) if isinstance(stock, dict) else getattr(stock, 'vcp_score', 0)
    pivot_readiness = stock.get('pivot_readiness_score', 0) if isinstance(stock, dict) else getattr(stock, 'pivot_readiness_score', 0)
    base_quality = stock.get('base_quality_score', 0) if isinstance(stock, dict) else getattr(stock, 'base_quality_score', 0)
    risk_pct = float(stock.get('risk_pct', 5.0) or 5.0) if isinstance(stock, dict) else float(getattr(stock, 'risk_pct', 5.0) or 5.0)
    status = stock.get('status', 'forming') if isinstance(stock, dict) else getattr(stock, 'status', 'forming')
    dry_up = float(stock.get('dry_up_ratio', 1.0) or 1.0) if isinstance(stock, dict) else float(getattr(stock, 'dry_up_ratio', 1.0) or 1.0)
    vol_ratio = float(stock.get('volume_ratio', 1.0) or 1.0) if isinstance(stock, dict) else float(getattr(stock, 'volume_ratio', 1.0) or 1.0)
    roe = float(stock.get('roe_pct', 0) or 0) if isinstance(stock, dict) else float(getattr(stock, 'roe_pct', 0) or 0)

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


def _unique_scanned_candidates(universes: Tuple[str, ...] = FOCUS_SOURCE_UNIVERSES) -> List[Dict]:
    """Deduplicate overlapping index/IPO scans while preferring the freshest row."""
    by_ticker: Dict[str, Dict] = {}
    for universe in universes:
        for raw in scan_cache.get("results", {}).get(universe, []):
            row = serialize_result(raw)
            row["source_universe"] = universe
            ticker = normalize_ticker(str(row.get("ticker", "")))
            if not ticker:
                continue
            current = by_ticker.get(ticker)
            row_age = row.get("data_age_business_days")
            current_age = current.get("data_age_business_days") if current else None
            if current is None or int(row_age if row_age is not None else 999) < int(current_age if current_age is not None else 999):
                by_ticker[ticker] = row
    return list(by_ticker.values())


def _market_snapshot(rows: List[Dict]) -> Dict:
    """Turn the benchmark regime and cross-sectional leadership into an exposure gate."""
    benchmark_regimes = [str(row.get("market_regime")) for row in rows if row.get("market_regime") not in {None, "unknown"}]
    regime = max(set(benchmark_regimes), key=benchmark_regimes.count) if benchmark_regimes else "unknown"
    index_rows = [row for row in rows if row.get("source_universe") in {None, "nifty500"}]
    eligible = [row for row in index_rows if int(row.get("trend_score", 0) or 0) > 0]
    leaders = [
        row for row in eligible
        if int(row.get("trend_score", 0) or 0) >= 15
        and (row.get("rs_3m_pct") is None or float(row.get("rs_3m_pct") or 0) >= 0)
    ]
    breadth_pct = round(len(leaders) / len(eligible) * 100.0, 1) if eligible else None

    if regime == "risk_off" or (breadth_pct is not None and breadth_pct < 25):
        risk_mode, max_new_positions, label = "risk_off", 0, "Capital protection"
    elif regime == "favorable" and (breadth_pct is None or breadth_pct >= 45):
        risk_mode, max_new_positions, label = "normal", 5, "Favorable"
    else:
        risk_mode, max_new_positions, label = "selective", 2, "Selective"

    return {
        "benchmark_regime": regime,
        "breadth_pct": breadth_pct,
        "risk_mode": risk_mode,
        "label": label,
        "max_new_positions": max_new_positions,
        "message": (
            "No new swing entries: preserve capital until the index and leadership breadth recover."
            if risk_mode == "risk_off" else
            "Take only the very best setups and use reduced total exposure."
            if risk_mode == "selective" else
            "New positions are permitted, but every trade still requires its trigger and stop."
        ),
    }


def _effective_trade_state(row: Dict) -> str:
    state = str(row.get("trade_state") or "")
    if state in {"trade_now", "watch_trigger", "avoid"}:
        return state
    if row.get("status") == "breakout" and row.get("actionable"):
        return "trade_now"
    if row.get("status") == "ready" and row.get("actionable"):
        return "watch_trigger"
    return "avoid"


def _focus_score(row: Dict, snapshot: Dict, sector_rs: Dict[str, float]) -> int:
    """Comparable 0-100 quality score; deliberately not presented as a probability."""
    swing = float(row.get("swing_score", row.get("vcp_score", 0)) or 0)
    fundamental_raw = row.get("fundamental_score")
    fundamentals_available = any(row.get(key) is not None for key in ("eps", "roe_pct", "revenue_growth", "earnings_growth"))
    fundamental = float(fundamental_raw or 0) if fundamentals_available else 50.0
    turnover = float(row.get("avg_turnover_cr", 0) or 0)
    liquidity = min(100.0, turnover / 50.0 * 100.0)
    rs = row.get("rs_3m_pct")
    relative_strength = 50.0 if rs is None else min(100.0, max(0.0, 50.0 + float(rs) * 2.5))
    confirmation = 100.0 if _effective_trade_state(row) == "trade_now" else 72.0
    regime_points = 100.0 if snapshot["risk_mode"] == "normal" else 65.0 if snapshot["risk_mode"] == "selective" else 0.0
    family_count = len([family for family in (row.get("setup_families") or []) if family != "Volume Dry-Up"])
    alignment_bonus = min(4.0, max(0, family_count - 1) * 2.0)
    sector_bonus = 3.0 if sector_rs.get(str(row.get("sector") or ""), -999.0) >= 5.0 else 0.0
    score = (
        swing * 0.50 + fundamental * 0.12 + liquidity * 0.10
        + relative_strength * 0.10 + confirmation * 0.10 + regime_points * 0.08
        + alignment_bonus + sector_bonus
    )
    return int(min(100, max(0, round(score))))


def _candidate_has_red_flag(row: Dict) -> bool:
    eps = row.get("eps")
    roe = row.get("roe_pct")
    debt = row.get("debt_to_equity")
    return bool(
        (eps is not None and roe is not None and float(eps) <= 0 and float(roe) < 0)
        or (debt is not None and float(debt) > 150)
    )


def build_daily_focus(rows: List[Dict], limit: int = 5) -> Dict:
    """Produce one constrained decision list plus a trigger-only preparation list."""
    snapshot = _market_snapshot(rows)
    sector_values: Dict[str, List[float]] = {}
    for row in rows:
        if row.get("rs_3m_pct") is not None:
            sector_values.setdefault(str(row.get("sector") or "Other"), []).append(float(row["rs_3m_pct"]))
    sector_rs = {sector: float(np.mean(values)) for sector, values in sector_values.items() if values}

    trade_pool = []
    watch_pool = []
    for original in rows:
        row = dict(original)
        state = _effective_trade_state(row)
        turnover = float(row.get("avg_turnover_cr", 0) or 0)
        risk_value = row.get("risk_pct")
        data_age_value = row.get("data_age_business_days")
        risk = float(risk_value if risk_value is not None else 999)
        data_age = int(data_age_value if data_age_value is not None else 999)
        volume_ratio = float(row.get("volume_ratio", 0) or 0)
        extension = max(0.0, -float(row.get("breakout_distance_pct", 0) or 0))
        if turnover < 10 or not (0 < risk <= 7) or data_age > 2 or _candidate_has_red_flag(row):
            continue

        row["focus_score"] = _focus_score(row, snapshot, sector_rs)
        row["trade_state"] = state
        row["selection_reason"] = " • ".join((row.get("evidence") or [])[:3])
        row["focus_grade"] = "A+" if row["focus_score"] >= 85 else "A" if row["focus_score"] >= 78 else "B+"
        if state == "trade_now" and volume_ratio >= 1.5 and extension <= 5 and row.get("signal_bar_complete", True):
            if row["focus_score"] >= 74:
                trade_pool.append(row)
        elif state == "watch_trigger" and row["focus_score"] >= (62 if snapshot["risk_mode"] == "risk_off" else 68):
            watch_pool.append(row)

    trade_pool.sort(key=lambda row: (row["focus_score"], row.get("swing_score", 0), row.get("avg_turnover_cr", 0)), reverse=True)
    watch_pool.sort(key=lambda row: (
        row["focus_score"],
        -abs(float(row["breakout_distance_pct"])) if row.get("breakout_distance_pct") is not None else -999.0,
    ), reverse=True)

    selected = []
    sector_counts: Dict[str, int] = {}
    ipo_count = 0
    allowed = min(limit, int(snapshot["max_new_positions"]))
    for row in trade_pool:
        if len(selected) >= allowed:
            break
        sector = str(row.get("sector") or "Other")
        is_ipo = bool(row.get("is_recent_listing")) or "IPO" in str(row.get("setup_family", ""))
        if sector_counts.get(sector, 0) >= 2 or (is_ipo and ipo_count >= 1):
            continue
        selected.append(row)
        sector_counts[sector] = sector_counts.get(sector, 0) + 1
        ipo_count += int(is_ipo)

    watch_selected = []
    watch_sector_counts: Dict[str, int] = {}
    for row in watch_pool:
        sector = str(row.get("sector") or "Other")
        if watch_sector_counts.get(sector, 0) >= 3:
            continue
        watch_selected.append(row)
        watch_sector_counts[sector] = watch_sector_counts.get(sector, 0) + 1
        if len(watch_selected) >= 10:
            break

    for rank, row in enumerate(selected, start=1):
        row["focus_rank"] = rank
        row["top_category_tag"] = f"#{rank} {row.get('setup_family') or row.get('setup_type') or 'Breakout'}"
    for rank, row in enumerate(watch_selected, start=1):
        row["watch_rank"] = rank
        row["top_category_tag"] = f"Watch #{rank} {row.get('setup_family') or row.get('setup_type') or 'Setup'}"

    return {"market": snapshot, "trade_now": selected, "watch_for_trigger": watch_selected, "evaluated": len(rows)}


@app.route('/api/watchlist')
def api_get_watchlist():
    """Return watchlist tickers and their scan results instantly (0.001s)."""
    results = []
    missing_tickers = []
    
    for ticker in watchlist_tickers:
        clean_w = normalize_ticker(ticker)
        found = None
        for uni_key, uni_results in scan_cache["results"].items():
            for r in uni_results:
                r_ticker = r.get("ticker", "") if isinstance(r, dict) else getattr(r, "ticker", "")
                if normalize_ticker(r_ticker) == clean_w:
                    found = r if isinstance(r, dict) else serialize_result(r)
                    break
            if found:
                break
        
        if found:
            results.append(found)
        else:
            missing_tickers.append(clean_w)

    # If there are missing watchlist tickers, scan them in background thread
    if missing_tickers and not scan_cache.get("scanning_watchlist", False):
        def _bg_scan_watchlist(tickers):
            scan_cache["scanning_watchlist"] = True
            try:
                scanned_items = []
                benchmark_df = fetch_stock_data("^NSEI")
                for t_sym in tickers:
                    res = scan_stock(t_sym, benchmark_df=benchmark_df)
                    if res:
                        scanned_items.append(serialize_result(res))
                if scanned_items:
                    scored = process_universe_fundamental_scores(scanned_items)
                    existing = scan_cache["results"].get("watchlist_extra", [])
                    refreshed = {normalize_ticker(row.get("ticker", "")) for row in scored}
                    scan_cache["results"]["watchlist_extra"] = scored + [
                        row for row in existing if normalize_ticker(row.get("ticker", "")) not in refreshed
                    ]
                    save_cache()
            except Exception as e:
                print(f"Background watchlist scan error: {e}")
            finally:
                scan_cache["scanning_watchlist"] = False

        threading.Thread(target=_bg_scan_watchlist, args=(missing_tickers,), daemon=True).start()

    return jsonify({
        "success": True,
        "tickers": watchlist_tickers,
        "matches": len(results),
        "is_scanning": scan_cache.get("scanning_watchlist", False),
        "results": results
    })


@app.route('/api/watchlist/add', methods=['POST'])
def api_watchlist_add():
    """Add a ticker to the watchlist."""
    data = request.get_json() or {}
    ticker = data.get("ticker", "").strip().upper().replace(".NS", "")
    if not ticker:
        return jsonify({"success": False, "error": "No ticker provided"}), 400
    if ticker not in watchlist_tickers:
        watchlist_tickers.append(ticker)
        save_watchlist()
    return jsonify({"success": True, "ticker": ticker, "watchlist": watchlist_tickers})


@app.route('/api/watchlist/remove', methods=['POST'])
def api_watchlist_remove():
    """Remove a ticker from the watchlist."""
    data = request.get_json() or {}
    ticker = data.get("ticker", "").strip().upper().replace(".NS", "")
    if ticker in watchlist_tickers:
        watchlist_tickers.remove(ticker)
        save_watchlist()
    return jsonify({"success": True, "ticker": ticker, "watchlist": watchlist_tickers})


def _daily_focus_payload() -> Dict:
    rows = _unique_scanned_candidates()
    focus = build_daily_focus(rows)
    return {
        "success": True,
        "universe": "daily_focus",
        "matches": len(focus["trade_now"]),
        "last_scan": scan_cache["last_scan"],
        "is_scanning": any(scan_cache.get(f"scanning_{name}", False) for name in FOCUS_SOURCE_UNIVERSES),
        "market": focus["market"],
        "evaluated": focus["evaluated"],
        "trade_now": focus["trade_now"],
        "watch_for_trigger": focus["watch_for_trigger"],
        "results": focus["trade_now"],
        "methodology": "Maximum five close-confirmed, liquid, non-extended leaders selected from Nifty 500 and recent mainboard listings; sector and IPO concentration caps apply.",
        "disclaimer": "A quality score is not a probability or guarantee. Enter only at the displayed trigger and size from the stop; a zero-stock day is valid.",
    }


@app.route('/api/results/daily_focus')
def api_daily_focus():
    """Return the small decision list used for new trades today."""
    return jsonify(_daily_focus_payload())


@app.route('/api/results/watch_candidates')
def api_watch_candidates():
    """Return preparation candidates that have not yet confirmed an entry."""
    payload = _daily_focus_payload()
    payload["universe"] = "watch_candidates"
    payload["results"] = payload["watch_for_trigger"]
    payload["matches"] = len(payload["results"])
    return jsonify(payload)


@app.route('/api/results/top_picks')
def api_top_picks():
    """Backward-compatible alias for clients that previously used Breakout Radar."""
    return jsonify(_daily_focus_payload())


def calculate_ipo_breakout_analysis(stock) -> Tuple[int, str, str, str]:
    """
    Evaluate Cup & Handle, Rounding Bottom, Listing High Breakout, and IPO VCP patterns for an IPO stock.
    Returns: (ipo_score, pattern_tag, pattern_grade, conviction_reason)
    """
    ticker = getattr(stock, 'ticker', '') if hasattr(stock, 'ticker') else stock.get('ticker', '')
    full_ticker = f"{ticker}.NS" if not ticker.endswith('.NS') else ticker
    
    # Read pre-fetched DataFrame from memory cache to ensure instant 0.0001s response
    cache_key = f"{full_ticker}_2y_1d"
    cached_tuple = ohlcv_cache.get(cache_key)
    df = cached_tuple[1] if cached_tuple else None
    
    current_price = getattr(stock, 'price', 0.0) if hasattr(stock, 'price') else float(stock.get('price', 0.0))
    res_level = getattr(stock, 'resistance_level', 0.0) if hasattr(stock, 'resistance_level') else float(stock.get('resistance_level', 0.0))
    dist_pct = getattr(stock, 'breakout_distance_pct', 999.0) if hasattr(stock, 'breakout_distance_pct') else float(stock.get('breakout_distance_pct', 999.0))
    vol_ratio = getattr(stock, 'volume_ratio', 1.0) if hasattr(stock, 'volume_ratio') else float(stock.get('volume_ratio', 1.0))
    dry_up = getattr(stock, 'dry_up_ratio', 1.0) if hasattr(stock, 'dry_up_ratio') else float(stock.get('dry_up_ratio', 1.0))
    status = getattr(stock, 'status', 'forming') if hasattr(stock, 'status') else stock.get('status', 'forming')
    risk_pct = getattr(stock, 'risk_pct', 5.0) if hasattr(stock, 'risk_pct') else float(stock.get('risk_pct', 5.0))
    vcp_score = getattr(stock, 'vcp_score', 0) if hasattr(stock, 'vcp_score') else stock.get('vcp_score', 0)

    listing_high = current_price
    is_rounding_bottom = False
    is_cup_and_handle = False
    is_listing_breakout = False

    if df is not None and not df.empty:
        listing_high = float(df['high'].max())
        n = len(df)
        
        # 1. Listing High / All-Time High Breakout check
        if current_price >= 0.96 * listing_high:
            is_listing_breakout = True

        # 2. Cup & Handle check (U-shape drop & recovery + tight right handle)
        if n >= 30:
            peak_price = float(df.head(max(10, int(n * 0.4)))['high'].max())
            trough_price = float(df['low'].min())
            cup_depth = ((peak_price - trough_price) / peak_price * 100) if peak_price > 0 else 0
            
            recent_15 = df.tail(15)
            handle_tightness = float((recent_15['close'].std() / current_price) * 100) if current_price > 0 else 999
            
            if 10.0 <= cup_depth <= 48.0 and current_price >= 0.86 * peak_price and (handle_tightness < 4.0 or dry_up < 0.85):
                is_cup_and_handle = True

        # 3. Rounding Bottom check (smooth curve recovery from post-listing low)
        if n >= 20 and not is_cup_and_handle:
            past_low = float(df['low'].min())
            recovery_pct = ((current_price - past_low) / past_low * 100) if past_low > 0 else 0
            if recovery_pct >= 15.0 and current_price >= 0.82 * listing_high:
                is_rounding_bottom = True

    # Pattern Tag & Grade Determination
    if is_listing_breakout and (status == 'breakout' or vol_ratio >= 1.2 or current_price >= listing_high * 0.99):
        pattern_tag = "🚀 Listing High Breakout"
        pattern_grade = "🚀 IPO ATH Breakout"
        reason = f"Breaking out past listing high (₹{listing_high:.2f}) on {vol_ratio:.1f}x volume"
        pattern_score = 35
    elif is_cup_and_handle:
        pattern_tag = "☕ Cup & Handle"
        pattern_grade = "☕ Cup & Handle Setup"
        reason = f"Cup & Handle base • Coiled {dist_pct:.1f}% below pivot resistance (₹{res_level:.2f})"
        pattern_score = 32
    elif is_rounding_bottom:
        pattern_tag = "🔄 Rounding Bottom"
        pattern_grade = "🔄 Rounding Bottom Base"
        reason = f"Rounding U-shape bottom • Recovery towards upper resistance (₹{res_level:.2f})"
        pattern_score = 28
    else:
        pattern_tag = "⚡ IPO VCP Base"
        pattern_grade = "⚡ IPO Consolidation"
        reason = f"Tight IPO base consolidation • Pivot resistance at ₹{res_level:.2f}"
        pattern_score = 22

    # Total Score Calculation (0-100)
    score = pattern_score
    if status == 'breakout': score += 25
    elif status == 'ready' or dist_pct <= 3.0: score += 20
    elif dist_pct <= 6.0: score += 12
    
    if vol_ratio >= 1.5 or dry_up <= 0.65: score += 20
    elif dry_up <= 0.85: score += 10
    
    if risk_pct <= 4.0: score += 15
    elif risk_pct <= 6.0: score += 10
    
    if vcp_score >= 60: score += 10

    final_score = int(min(100, max(0, round(score))))
    return final_score, pattern_tag, pattern_grade, reason


@app.route('/api/results/ipo_breakouts')
def api_ipo_breakouts():
    """Backward-compatible strict IPO list using the same focus gates as every other setup."""
    rows = _unique_scanned_candidates(("ipo",))
    focus = build_daily_focus(rows)
    results = focus["trade_now"] + focus["watch_for_trigger"]
    if not rows and not scan_cache.get("scanning_ipo", False):
        threading.Thread(target=scan_single_universe_bg, args=("ipo",), daemon=True).start()

    return jsonify({
        "success": True,
        "universe": "ipo_breakouts",
        "matches": len(results),
        "last_scan": scan_cache["last_scan"],
        "is_scanning": scan_cache.get("scanning_ipo", False) or scan_cache["is_scanning"],
        "market": focus["market"],
        "results": results
    })


@app.route('/api/results/<universe>')
def get_results(universe):
    """Get pre-scored cached results for requested universe with instant response time."""
    results = scan_cache["results"].get(universe, [])
    is_universe_scanning = scan_cache.get(f"scanning_{universe}", False)

    if not results and not is_universe_scanning:
        threading.Thread(target=scan_single_universe_bg, args=(universe,), daemon=True).start()
        is_universe_scanning = True

    return jsonify({
        "success": True,
        "universe": universe,
        "matches": len(results),
        "last_scan": scan_cache["last_scan"],
        "is_scanning": is_universe_scanning,
        "results": results
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
            {"id": "nse_all", "name": "Recent NSE 1000 (On Demand)", "count": len(get_stock_universe("nse_all"))},
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


@app.route('/api/health')
def api_health():
    """Health check endpoint for Render monitoring."""
    import resource
    mem_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)  # macOS gives bytes
    if sys.platform == 'linux':
        mem_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # Linux gives KB

    cached_counts = {k: len(v) for k, v in scan_cache["results"].items()}
    return jsonify({
        "status": "healthy",
        "uptime_seconds": round(time.time() - _app_start_time, 0),
        "memory_mb": round(mem_mb, 1),
        "is_scanning": scan_cache["is_scanning"],
        "last_scan": scan_cache["last_scan"],
        "ohlcv_cache_size": len(ohlcv_cache),
        "chart_cache_size": len(chart_cache_memory),
        "fundamental_cache_size": len(fundamental_cache),
        "cached_universe_counts": cached_counts,
        "error_count": len(scan_cache["errors"]),
    })


@app.route('/api/export/<universe>')
def api_export(universe):
    """Export results to CSV."""
    results = scan_cache["results"].get(universe, [])
    if not results:
        return jsonify({"success": False, "error": "No data"}), 404

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['Ticker', 'Name', 'Sector', 'Price', 'Change%', 'Focus Score', 'Swing Score', 'VCP Score',
                     'Decision', 'Setup Family', 'Setup Type', 'Actionable', 'Status', 'Contraction', 'Volume Ratio',
                     'Volume Dry Up', 'Pivot Gap%', 'RS vs Nifty 3M%', 'Avg Turnover Cr',
                     'Pivot Trigger', '5% Chase Limit', 'Stop', 'Risk%', 'Data Date', 'Rejection Reasons'])

    for result in results:
        r = serialize_result(result)
        writer.writerow([r.get('ticker'), r.get('name'), r.get('sector'), r.get('price'), r.get('change_pct'),
                        r.get('focus_score'), r.get('swing_score'), r.get('vcp_score'), r.get('trade_state'),
                        r.get('setup_family'), r.get('setup_type'), r.get('actionable'),
                        r.get('status'), r.get('contraction'), r.get('volume_ratio'), r.get('dry_up_ratio'),
                        r.get('breakout_distance_pct'), r.get('rs_3m_pct'), r.get('avg_turnover_cr'),
                        r.get('entry_price'), r.get('max_entry_price'), r.get('stop_loss'), r.get('risk_pct'), r.get('data_date'),
                        '; '.join(r.get('rejection_reasons') or [])])

    output.seek(0)
    return Response(output.getvalue(), mimetype='text/csv',
                   headers={'Content-Disposition': f'attachment; filename=vcp_scan_{universe}.csv'})


# ============================================================
# MAIN
# ============================================================

if __name__ == '__main__':
    print("=" * 60)
    print("🇮🇳 Swing Focus India - Rules-Based Research Scanner")
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
