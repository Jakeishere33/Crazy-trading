import os
import sys
import time
import math
import random
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderType, AssetClass

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("options_only_engine")

# ============================================================
# CONFIG
# ============================================================

EASTERN_TZ = ZoneInfo("America/New_York")
TRADING_DAYS = 252
RISK_FREE_ANNUAL = 0.04

MIN_TRADES_PER_DAY = 30
MAX_OPTION_CONTRACTS_PER_TICKER = 4  # high risk, but still capped

ALPACA_DATA_URL = "https://data.alpaca.markets"
ALPACA_STOCK_FEED = os.getenv("ALPACA_STOCK_FEED", "iex")
ALPACA_OPTION_FEED = os.getenv("ALPACA_OPTION_FEED", "indicative")

DATA_BATCH_SIZE = 50
DATA_MAX_RETRIES = 5
DATA_RETRY_BASE_DELAY = 1.5
HTTP_TIMEOUT = 15
DATA_CACHE_TTL_SECONDS = 60

OPTIONS_MIN_DTE = 20
OPTIONS_MAX_DTE = 45
MIN_OPTION_VOLUME = 50
MAX_OPTION_SPREAD_PCT = 0.15

REGIME_BENCHMARK = "VOO"

LOOP_SLEEP_SECONDS = 60

# ============================================================
# UNIVERSE (SEMIS, MINING, INDUSTRIALS)
# ============================================================

SEMICONDUCTOR_TICKERS = [
    "NVDA", "AMD", "INTC", "TSM", "AVGO",
    "QCOM", "TXN", "MU", "LRCX", "AMAT",
    "ADI", "KLAC", "MRVL", "ON", "MCHP",
    "SWKS", "QRVO", "NXPI", "TER", "ENTG",
    "MPWR", "CRUS", "SLAB", "POWI", "DIOD",
    "RMBS", "ALGM", "WOLF", "ONTO", "COHU",
]

MINING_TICKERS = [
    "FCX", "NEM", "GOLD", "SCCO", "AEM",
    "TECK", "RIO", "BHP", "VALE", "MOS",
    "AA", "CLF", "X", "NUE", "STLD",
    "MP", "CDE", "HL", "PAAS", "AG",
    "SSRM", "EGO", "KGC", "AU", "WPM",
    "FNV", "RGLD", "ALB", "LAC", "SQM",
]

INDUSTRIALS_TICKERS = [
    "WAB", "JBHT", "ODFL", "XPO", "CHRW",
    "LSTR", "EXPD", "WERN", "SAIA", "RXO",
    "TXT", "HEI", "TDG", "CW", "WWD",
    "AXON", "LII", "WSO", "JCI", "CSL",
    "MAS", "VMC", "MLM", "WM", "RSG",
    "CTAS", "ROL", "PWR", "FIX", "EME",
]

UNIVERSE = sorted(set(
    SEMICONDUCTOR_TICKERS
    + MINING_TICKERS
    + INDUSTRIALS_TICKERS
))

# ============================================================
# ALPACA CREDENTIALS / HTTP
# ============================================================

ALPACA_API_KEY = ""
ALPACA_API_SECRET = ""

_HTTP = requests.Session()
_HTTP.headers.update({
    "User-Agent": "OptionsOnlyEngine/1.1",
    "Accept": "application/json",
    "Connection": "keep-alive",
})

def initialize_credentials():
    global ALPACA_API_KEY, ALPACA_API_SECRET
    ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "").strip()
    ALPACA_API_SECRET = os.getenv("ALPACA_API_SECRET", "").strip()
    if not ALPACA_API_KEY or not ALPACA_API_SECRET:
        raise RuntimeError("ALPACA_API_KEY / ALPACA_API_SECRET are not configured.")

def alpaca_headers():
    return {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_API_SECRET,
        "Accept": "application/json",
    }

# ============================================================
# CACHE
# ============================================================

_BARS_CACHE = {}
_OPTIONS_CACHE = {}
_PRICE_CACHE = {}

def cache_get(cache, key):
    item = cache.get(key)
    if not item:
        return None
    ts, value = item
    if time.time() - ts > DATA_CACHE_TTL_SECONDS:
        return None
    return value

def cache_put(cache, key, value):
    cache[key] = (time.time(), value)

# ============================================================
# TIME HELPERS
# ============================================================

def now_et():
    return datetime.now(EASTERN_TZ)

def utc_now():
    return datetime.now(timezone.utc)

def iso_utc(dt):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

# ============================================================
# GENERIC ALPACA DATA GET
# ============================================================

def alpaca_data_get(path, params=None, label=""):
    url = f"{ALPACA_DATA_URL}{path}"
    last_error = None

    for attempt in range(1, DATA_MAX_RETRIES + 1):
        try:
            resp = _HTTP.get(
                url,
                params=params or {},
                headers=alpaca_headers(),
                timeout=HTTP_TIMEOUT,
            )
            status = resp.status_code

            if 200 <= status < 300:
                try:
                    return resp.json()
                except ValueError as exc:
                    raise RuntimeError(f"Invalid JSON returned by Alpaca: {exc}")

            if status == 429:
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        delay = float(retry_after)
                    except Exception:
                        delay = DATA_RETRY_BASE_DELAY * (2 ** (attempt - 1))
                else:
                    delay = DATA_RETRY_BASE_DELAY * (2 ** (attempt - 1))
                delay += random.uniform(0.2, 0.8)
                log.warning(
                    "Rate limit (%s) %s attempt %d/%d. Sleeping %.1fs.",
                    label, path, attempt, DATA_MAX_RETRIES, delay,
                )
                time.sleep(delay)
                continue

            if status >= 500:
                delay = DATA_RETRY_BASE_DELAY * (2 ** (attempt - 1))
                delay += random.uniform(0.2, 0.8)
                log.warning(
                    "Server error %s (%s) attempt %d/%d. Retrying in %.1fs.",
                    status, label, attempt, DATA_MAX_RETRIES, delay,
                )
                time.sleep(delay)
                continue

            try:
                body = resp.json()
            except Exception:
                body = resp.text[:500]
            raise RuntimeError(f"Alpaca HTTP {status}: {body}")

        except Exception as exc:
            last_error = exc
            if attempt < DATA_MAX_RETRIES:
                delay = DATA_RETRY_BASE_DELAY * (2 ** (attempt - 1))
                delay += random.uniform(0.2, 0.8)
                log.warning(
                    "Request failed (%s) attempt %d/%d: %s. Retrying in %.1fs.",
                    label, attempt, DATA_MAX_RETRIES, exc, delay,
                )
                time.sleep(delay)
            else:
                log.error(
                    "Request failed (%s) after %d attempts: %s",
                    label, DATA_MAX_RETRIES, exc,
                )

    raise last_error or RuntimeError(f"Unknown Alpaca data failure: {label}")

# ============================================================
# HISTORICAL BARS / MOMENTUM / VOL
# ============================================================

def alpaca_bars_many(symbols, days=60):
    symbols = sorted(set(s.upper() for s in symbols))
    if not symbols:
        return {}

    days = min(int(days), 220)
    cache_key = (tuple(symbols), days, ALPACA_STOCK_FEED)
    cached = cache_get(_BARS_CACHE, cache_key)
    if cached is not None:
        return cached

    calendar_days = int(days * 1.55) + 15
    end_dt = utc_now()
    start_dt = end_dt - timedelta(days=calendar_days)

    result = {s: [] for s in symbols}

    for start_idx in range(0, len(symbols), DATA_BATCH_SIZE):
        batch = symbols[start_idx:start_idx + DATA_BATCH_SIZE]
        params = {
            "symbols": ",".join(batch),
            "timeframe": "1Day",
            "start": iso_utc(start_dt),
            "end": iso_utc(end_dt),
            "feed": ALPACA_STOCK_FEED,
            "adjustment": "split",
            "sort": "asc",
            "limit": 10000,
        }
        try:
            data = alpaca_data_get("/v2/stocks/bars", params=params, label="bars")
            bars = data.get("bars", {})
            if not isinstance(bars, dict):
                log.warning("Malformed Alpaca bars response for batch.")
                continue
            for symbol in batch:
                closes = []
                for bar in bars.get(symbol, []):
                    try:
                        c = float(bar["c"])
                        if c > 0:
                            closes.append(c)
                    except Exception:
                        continue
                result[symbol] = closes[-days:]
        except Exception as exc:
            log.error("Historical data batch failed: %s", exc)
            continue

    cache_put(_BARS_CACHE, cache_key, result)
    return result

def volatility_from_bars(bars, window=14):
    if not bars or len(bars) < window + 1:
        return None
    returns = []
    for i in range(1, len(bars)):
        prev, curr = bars[i - 1], bars[i]
        if prev <= 0:
            continue
        try:
            returns.append((curr / prev) - 1.0)
        except Exception:
            continue
    if len(returns) < window:
        return None
    sample = returns[-window:]
    mean = sum(sample) / len(sample)
    var = sum((r - mean) ** 2 for r in sample) / len(sample)
    return math.sqrt(var) * math.sqrt(TRADING_DAYS)

def momentum_from_bars(bars, lookback=20):
    if not bars or len(bars) < lookback + 1:
        return None
    try:
        return (bars[-1] / bars[-lookback]) - 1.0
    except Exception:
        return None

# ============================================================
# REGIME (BULL / BEAR)
# ============================================================

class RegimeManager:
    def __init__(self, market_data=None):
        self.market_data = market_data or {}

    def regime_risk_score(self):
        bars = self.market_data.get(REGIME_BENCHMARK, [])
        if len(bars) < 200:
            log.warning(
                "Not enough %s history for regime calculation. Using neutral regime.",
                REGIME_BENCHMARK,
            )
            return 0.5  # neutral 

        sma200 = sum(bars[-200:]) / 200
        last = bars[-1]
        below_sma = 0.0 if last > sma200 else 1.0

        last_year = bars[-252:] if len(bars) >= 252 else bars
        highest = max(last_year) if last_year else 0
        if highest <= 0:
            dd = 0.0
        else:
            dd = 1.0 - (last / highest)

        dd_score = max(0.0, min(dd / 0.20, 1.0))
        return (below_sma + dd_score) / 2.0

    def regime_label(self):
        score = self.regime_risk_score()
        if score < 0.3:
            return "bull", score
        elif score > 0.7:
            return "bear", score
        else:
            return "neutral", score

# ============================================================
# BLACK–SCHOLES
# ============================================================

def _cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def bs_price(S, K, T, r, sigma, opt_type):
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if opt_type == "call":
        return S * _cdf(d1) - K * math.exp(-r * T) * _cdf(d2)
    else:
        return K * math.exp(-r * T) * _cdf(-d2) - S * _cdf(-d1)

def bs_delta(S, K, T, r, sigma, opt_type):
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    if opt_type == "call":
        return _cdf(d1)
    else:
        return _cdf(d1) - 1.0

# ============================================================
# MONTE CARLO
# ============================================================

def monte_carlo_paths(S0, mu, sigma, T, steps=20, n_paths=500):
    dt = T / steps
    paths = []
    for _ in range(n_paths):
        S = S0
        for _ in range(steps):
            z = random.gauss(0, 1)
            S *= math.exp((mu - 0.5 * sigma * sigma) * dt + sigma * math.sqrt(dt) * z)
        paths.append(S)
    return paths

def mc_expected_move(S0, mu, sigma, T):
    paths = monte_carlo_paths(S0, mu, sigma, T)
    if not paths:
        return 0.0
    avg = sum(paths) / len(paths)
    return (avg / S0) - 1.0

# ============================================================
# OPTION CHAIN + SANITY CHECKS
# ============================================================

def parse_occ_option_symbol(symbol):
    try:
        root = symbol[:-15]
        tail = symbol[-15:]
        date_str = tail[:6]
        opt_type = tail[6]
        strike_str = tail[7:]

        year = int(date_str[0:2])
        month = int(date_str[2:4])
        day = int(date_str[4:6])
        expiration = datetime(2000 + year, month, day).date()
        strike = int(strike_str) / 1000.0
        return root, expiration, opt_type, strike
    except Exception:
        return None

def option_chain_calls(underlying):
    underlying = underlying.upper()
    today = now_et().date()
    min_exp = today + timedelta(days=OPTIONS_MIN_DTE)
    max_exp = today + timedelta(days=OPTIONS_MAX_DTE)

    cache_key = (underlying, OPTIONS_MIN_DTE, OPTIONS_MAX_DTE, ALPACA_OPTION_FEED)
    cached = cache_get(_OPTIONS_CACHE, cache_key)
    if cached is not None:
        return cached

    params = {
        "feed": ALPACA_OPTION_FEED,
        "type": "call",
        "expiration_date_gte": min_exp.isoformat(),
        "expiration_date_lte": max_exp.isoformat(),
        "limit": 1000,
    }

    data = alpaca_data_get(
        f"/v1beta1/options/snapshots/{underlying}",
        params=params,
        label=f"chain:{underlying}",
    )

    snapshots = data.get("snapshots", {}) or {}
    contracts = []

    if isinstance(snapshots, dict):
        for contract_symbol, snapshot in snapshots.items():
            parts = parse_occ_option_symbol(contract_symbol)
            if not parts:
                continue
            root, expiration, opt_type, strike = parts
            if opt_type != "C":
                continue
            if expiration < min_exp or expiration > max_exp:
                continue

            quote = snapshot.get("latestQuote", {}) or {}
            trade = snapshot.get("latestTrade", {}) or {}
            greeks = snapshot.get("greeks", {}) or {}

            bid = float(quote.get("bp", 0) or 0)
            ask = float(quote.get("ap", 0) or 0)
            volume = int(trade.get("s", 0) or 0)
            iv = float(snapshot.get("impliedVolatility", 0) or 0)
            delta = float(greeks.get("delta", 0) or 0)

            if bid <= 0 or ask <= 0 or ask < bid:
                continue
            mid = (bid + ask) / 2.0
            if mid <= 0:
                continue
            spread_pct = (ask - bid) / mid
            if spread_pct > MAX_OPTION_SPREAD_PCT:
                continue
            if volume < MIN_OPTION_VOLUME:
                continue

            contracts.append({
                "symbol": contract_symbol,
                "strike": strike,
                "expiration": expiration,
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "volume": volume,
                "delta": delta,
                "iv": iv,
            })

    cache_put(_OPTIONS_CACHE, cache_key, contracts)
    return contracts

# ============================================================
# LATEST PRICES
# ============================================================

def latest_prices_many(symbols):
    symbols = sorted(set(s.upper() for s in symbols))
    if not symbols:
        return {}
    cache_key = (tuple(symbols), ALPACA_STOCK_FEED)
    cached = cache_get(_PRICE_CACHE, cache_key)
    if cached is not None:
        return cached

    result = {}
    for start_idx in range(0, len(symbols), DATA_BATCH_SIZE):
        batch = symbols[start_idx:start_idx + DATA_BATCH_SIZE]
        params = {"symbols": ",".join(batch), "feed": ALPACA_STOCK_FEED}
        try:
            data = alpaca_data_get("/v2/stocks/trades/latest", params=params, label="latest")
            trades = data.get("trades", {}) or {}
            if not isinstance(trades, dict):
                continue
            for symbol in batch:
                trade = trades.get(symbol)
                if not trade:
                    continue
                try:
                    p = float(trade["p"])
                    if p > 0:
                        result[symbol] = p
                except Exception:
                    continue
        except Exception as exc:
            log.warning("Latest-price batch failed: %s", exc)

    cache_put(_PRICE_CACHE, cache_key, result)
    return result

# ============================================================
# TRADING ENGINE (OPTIONS ONLY, HIGH RISK)
# ============================================================

class OptionsOnlyEngine:

    def __init__(self, trading_client):
        self.trading = trading_client
        self.trades_today = 0
        self.last_trade_day = None
        self.daily_log = []  # for CSV summary

    def reset_if_new_day(self):
        today = now_et().date()
        if self.last_trade_day != today:
            self.trades_today = 0
            self.last_trade_day = today
            self.daily_log = []

    def can_trade_today(self):
        return self.trades_today < MIN_TRADES_PER_DAY

    def positions(self):
        try:
            positions = self.trading.get_all_positions()
            return {p.symbol: p for p in positions}
        except Exception as exc:
            log.warning("Failed to fetch positions: %s", exc)
            return {}

    def existing_option_symbols(self):
        pos = self.positions()
        return {s for s in pos.keys() if len(s) > 5}  # crude: OCC symbols

    def submit_option_buy(self, contract, qty, regime_label, mc_move, bs_val_ratio):
        symbol = contract["symbol"]
        try:
            order = MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=OrderSide.BUY,
                time_in_force=TimeInForce.DAY,
                type=OrderType.MARKET,
                asset_class=AssetClass.OPTION,
            )
            self.trading.submit_order(order)
            self.trades_today += 1
            log.info(
                "BUY %s x%d | regime=%s mc_move=%.2f%% bs_ratio=%.2f",
                symbol, qty, regime_label, mc_move * 100, bs_val_ratio,
            )
            self.daily_log.append({
                "time": now_et().isoformat(),
                "contract": symbol,
                "qty": qty,
                "regime": regime_label,
                "mc_move_pct": mc_move * 100,
                "bs_ratio": bs_val_ratio,
                "mid": contract["mid"],
                "delta": contract["delta"],
                "iv": contract["iv"],
            })
        except Exception as exc:
            log.error("Order failed %s x%d: %s", symbol, qty, exc)

    def pick_contracts_for_symbol(self, symbol, bars, regime_label):
        if len(bars) < 40:
            return []

        mom = momentum_from_bars(bars, lookback=20)
        vol = volatility_from_bars(bars, window=14)
        if mom is None or vol is None or vol <= 0:
            return []

        S0 = bars[-1]
        T_years = 30 / TRADING_DAYS
        mu = mom

        # Bull vs bear tilt
        if regime_label == "bull":
            mu *= 1.3
        elif regime_label == "bear":
            mu *= 0.5

        mc_move = mc_expected_move(S0, mu, vol, T_years)
        if mc_move < 0.01:  # require at least +1% expected move
            return []

        chain = option_chain_calls(symbol)
        if not chain:
            return []

        existing_opts = self.existing_option_symbols()
        candidates = []

        for c in chain:
            if c["symbol"] in existing_opts:
                continue  # position tracking: no rebuy of same contract

            K = c["strike"]
            T = (c["expiration"] - now_et().date()).days / TRADING_DAYS
            if T <= 0:
                continue

            iv = c["iv"] if c["iv"] > 0 else vol
            model_price = bs_price(S0, K, T, RISK_FREE_ANNUAL, iv, "call")
            if model_price <= 0 or c["mid"] <= 0:
                continue

            bs_ratio = c["mid"] / model_price
            if bs_ratio > 1.8:  # sanity: don't pay >80% over BS
                continue

            moneyness = abs(K - S0) / S0
            score = (mc_move * 100) - (moneyness * 40) + (c["delta"] * 25)
            candidates.append((score, c, mc_move, bs_ratio))

        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[:MAX_OPTION_CONTRACTS_PER_TICKER]

    def run_cycle(self):
        self.reset_if_new_day()
        if not self.can_trade_today():
            log.info("Daily trade target already met (%d)", self.trades_today)
            return

        log.info("Loading market data for %d symbols...", len(UNIVERSE))
        bars_map = alpaca_bars_many(UNIVERSE + [REGIME_BENCHMARK], days=60)

        regime_mgr = RegimeManager(market_data=bars_map)
        regime_label, regime_score = regime_mgr.regime_label()
        log.info("Regime: %s (score=%.2f)", regime_label, regime_score)

        for symbol in UNIVERSE:
            if not self.can_trade_today():
                break
            bars = bars_map.get(symbol, [])
            picks = self.pick_contracts_for_symbol(symbol, bars, regime_label)
            for score, contract, mc_move, bs_ratio in picks:
                if not self.can_trade_today():
                    break
                self.submit_option_buy(contract, qty=1, regime_label=regime_label,
                                       mc_move=mc_move, bs_val_ratio=bs_ratio)

        log.info("Cycle complete, trades today: %d", self.trades_today)
        self.write_daily_summary()

    def write_daily_summary(self):
        if not self.daily_log:
            return
        date_str = now_et().strftime("%Y-%m-%d")
        filename = f"options_summary_{date_str}.csv"
        try:
            import csv
            with open(filename, "w", newline="") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "time", "contract", "qty", "regime",
                        "mc_move_pct", "bs_ratio", "mid", "delta", "iv",
                    ],
                )
                writer.writeheader()
                for row in self.daily_log:
                    writer.writerow(row)
            log.info("Daily summary written to %s (open in Excel or convert to PDF).", filename)
        except Exception as exc:
            log.error("Failed to write daily summary: %s", exc)

# ============================================================
# MAIN LOOP
# ============================================================

def main():
    initialize_credentials()
    trading_client = TradingClient(ALPACA_API_KEY, ALPACA_API_SECRET, paper=True)
    engine = OptionsOnlyEngine(trading_client)

    while True:
        try:
            engine.run_cycle()
        except Exception as exc:
            log.error("Engine cycle error: %s", exc)
        time.sleep(LOOP_SLEEP_SECONDS)

if __name__ == "__main__":
    main()
