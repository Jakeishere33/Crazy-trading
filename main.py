import os
import sys
import gc
import time
import random
import logging
import threading
from math import sqrt, log as ln, exp, erf
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from flask import Flask

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderType, AssetClass


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout,
)

log = logging.getLogger("options_bs_engine")


# ============================================================
# FLASK / ENGINE STATE
# ============================================================

app = Flask(__name__)

ENGINE_STATE = {
    "thread_started": False,
    "thread_alive": False,
    "last_cycle_started": None,
    "last_cycle_finished": None,
    "last_error": None,
    "trades_today": 0,
    "market_open": None,
    "data_provider": "Alpaca",
    "data_failures": 0,
    "candidate_count": 0,
}


@app.route("/")
def home():
    return "Black-Scholes Options-Only Engine Online - Alpaca", 200


@app.route("/health")
def health():
    return ENGINE_STATE, 200


# ============================================================
# CONFIG
# ============================================================

EASTERN_TZ = ZoneInfo("America/New_York")

RISK_FREE_ANNUAL = 0.04
TRADING_DAYS = 252

# ------------------------------------------------------------
# DAILY TRADE TARGET
# ------------------------------------------------------------

# Target and minimum number of OPTION ORDERS submitted per day.
TARGET_TRADES_PER_DAY = 100
MIN_TRADES_PER_DAY = 100

# Hard upper ceiling.
MAX_TRADES_PER_DAY = 125

TRADES_TODAY = 0
LAST_TRADE_DAY = None


# ------------------------------------------------------------
# OPTION PARAMETERS
# ------------------------------------------------------------

MIN_OPTION_DTE = 14
MAX_OPTION_DTE = 45

MIN_OPTION_VOLUME = 10
MIN_OPEN_INTEREST = 50

MAX_OPTION_SPREAD_PCT = 0.12

# Minimum Black-Scholes discount required.
#
# Example:
# BS value = $2.00
# Market mid = $1.50
#
# Discount = 25%
#
# This would qualify if MIN_BS_EDGE_PCT <= 25%.
MIN_BS_EDGE_PCT = 0.08

# Do not buy options where theoretical value is only
# a few cents higher than the market.
MIN_BS_EDGE_DOLLARS = 0.10

# Maximum premium paid per contract.
MAX_OPTION_PREMIUM = 15.00

# Maximum contracts for any one underlying.
MAX_CONTRACTS_PER_UNDERLYING = 2

# Maximum total capital deployed through the strategy
# during a single cycle.
MAX_CYCLE_OPTION_BUDGET_PCT = 0.20

# Maximum percentage of account equity allocated to
# one option position.
MAX_POSITION_BUDGET_PCT = 0.01

# Do not buy options with extremely low or extremely high IV.
MIN_IV = 0.10
MAX_IV = 2.50

# Delta ranges.
CALL_MIN_DELTA = 0.20
CALL_MAX_DELTA = 0.70

PUT_MIN_ABS_DELTA = 0.20
PUT_MAX_ABS_DELTA = 0.70


# ------------------------------------------------------------
# SIGNAL PARAMETERS
# ------------------------------------------------------------

MOMENTUM_LOOKBACK = 20
VOLATILITY_WINDOW = 20

# Directional threshold.
#
# Momentum > +2%:
#     prefer calls
#
# Momentum < -2%:
#     prefer puts
#
# Between those:
#     both sides may qualify.
DIRECTION_THRESHOLD = 0.02


# ------------------------------------------------------------
# MARKET WINDOW
# ------------------------------------------------------------

NO_TRADE_BEFORE = (9, 35)
NO_TRADE_AFTER = (15, 55)


# ------------------------------------------------------------
# DATA
# ------------------------------------------------------------

ALPACA_DATA_URL = "https://data.alpaca.markets"

ALPACA_STOCK_FEED = os.getenv(
    "ALPACA_STOCK_FEED",
    "iex",
)

ALPACA_OPTION_FEED = os.getenv(
    "ALPACA_OPTION_FEED",
    "indicative",
)

DATA_BATCH_SIZE = 50
OPTION_SNAPSHOT_BATCH_SIZE = 100

DATA_MAX_RETRIES = 5
DATA_RETRY_BASE_DELAY = 1.5

DATA_CACHE_TTL_SECONDS = 30

HTTP_TIMEOUT = 15

MAX_BARS_DAYS = 220

LOOP_SLEEP_SECONDS = 30


# ============================================================
# OPTIONS-ONLY UNIVERSE
# ============================================================

SEMICONDUCTOR_TICKERS = [
    "NVDA",
    "AMD",
    "INTC",
    "TSM",
    "AVGO",
    "QCOM",
    "TXN",
    "MU",
    "LRCX",
    "AMAT",
    "ADI",
    "KLAC",
    "MRVL",
    "ON",
    "MCHP",
    "SWKS",
    "QRVO",
    "NXPI",
    "TER",
    "ENTG",
    "MPWR",
    "CRUS",
    "SLAB",
    "POWI",
    "DIOD",
    "RMBS",
    "ALGM",
    "WOLF",
    "ONTO",
    "COHU",
]


MINING_TICKERS = [
    "FCX",
    "NEM",
    "GOLD",
    "SCCO",
    "AEM",
    "TECK",
    "RIO",
    "BHP",
    "VALE",
    "MOS",
    "AA",
    "CLF",
    "X",
    "NUE",
    "STLD",
    "MP",
    "CDE",
    "HL",
    "PAAS",
    "AG",
    "SSRM",
    "EGO",
    "KGC",
    "AU",
    "WPM",
    "FNV",
    "RGLD",
    "ALB",
    "LAC",
    "SQM",
]


PHARMA_TICKERS = [
    "LLY",
    "JNJ",
    "MRK",
    "ABBV",
    "PFE",
    "BMY",
    "GILD",
    "AMGN",
    "REGN",
    "VRTX",
    "BIIB",
    "MRNA",
    "GSK",
    "AZN",
    "NVS",
    "SNY",
    "BNTX",
    "TEVA",
    "TAK",
    "ELAN",
    "UTHR",
    "INCY",
    "ALNY",
    "HALO",
    "JAZZ",
    "SRPT",
    "IONS",
    "EXEL",
    "BMRN",
    "RPRX",
]


def _dedupe(seq):
    seen = set()
    result = []

    for symbol in seq:
        symbol = symbol.upper().strip()

        if symbol and symbol not in seen:
            seen.add(symbol)
            result.append(symbol)

    return result


OPTIONS_UNIVERSE = _dedupe(
    SEMICONDUCTOR_TICKERS
    + MINING_TICKERS
    + PHARMA_TICKERS
)


# ============================================================
# HTTP SESSION
# ============================================================

_HTTP = requests.Session()

_HTTP.headers.update({
    "User-Agent": "BlackScholesOptionsEngine/1.0",
    "Accept": "application/json",
    "Connection": "keep-alive",
})


# ============================================================
# ALPACA CREDENTIALS
# ============================================================

ALPACA_API_KEY = ""
ALPACA_API_SECRET = ""


def initialize_credentials():

    global ALPACA_API_KEY
    global ALPACA_API_SECRET

    ALPACA_API_KEY = os.getenv(
        "ALPACA_API_KEY",
        "",
    ).strip()

    ALPACA_API_SECRET = os.getenv(
        "ALPACA_API_SECRET",
        "",
    ).strip()

    if not ALPACA_API_KEY or not ALPACA_API_SECRET:
        raise RuntimeError(
            "ALPACA_API_KEY / ALPACA_API_SECRET "
            "are not configured."
        )


def alpaca_headers():

    return {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_API_SECRET,
        "Accept": "application/json",
    }


# ============================================================
# ALPACA DATA REQUEST
# ============================================================

def alpaca_data_get(
    path,
    params=None,
    label="",
):

    url = f"{ALPACA_DATA_URL}{path}"

    last_error = None

    for attempt in range(
        1,
        DATA_MAX_RETRIES + 1,
    ):

        try:

            response = _HTTP.get(
                url,
                params=params or {},
                headers=alpaca_headers(),
                timeout=HTTP_TIMEOUT,
            )

            status = response.status_code

            if 200 <= status < 300:

                try:
                    return response.json()

                except ValueError as exc:

                    raise RuntimeError(
                        f"Invalid JSON from Alpaca: {exc}"
                    )

            if status == 429:

                retry_after = response.headers.get(
                    "Retry-After"
                )

                if retry_after:

                    try:
                        delay = float(retry_after)

                    except Exception:
                        delay = (
                            DATA_RETRY_BASE_DELAY
                            * (2 ** (attempt - 1))
                        )

                else:

                    delay = (
                        DATA_RETRY_BASE_DELAY
                        * (2 ** (attempt - 1))
                    )

                delay += random.uniform(
                    0.2,
                    0.8,
                )

                log.warning(
                    "Rate limit | %s | retry %d/%d | %.1fs",
                    label,
                    attempt,
                    DATA_MAX_RETRIES,
                    delay,
                )

                time.sleep(delay)

                continue

            if status >= 500:

                delay = (
                    DATA_RETRY_BASE_DELAY
                    * (2 ** (attempt - 1))
                )

                delay += random.uniform(
                    0.2,
                    0.8,
                )

                log.warning(
                    "Server error | %s | retry %d/%d",
                    label,
                    attempt,
                    DATA_MAX_RETRIES,
                )

                time.sleep(delay)

                continue

            try:
                body = response.json()

            except Exception:
                body = response.text[:500]

            raise RuntimeError(
                f"Alpaca HTTP {status}: {body}"
            )

        except Exception as exc:

            last_error = exc

            if attempt < DATA_MAX_RETRIES:

                delay = (
                    DATA_RETRY_BASE_DELAY
                    * (2 ** (attempt - 1))
                )

                delay += random.uniform(
                    0.2,
                    0.8,
                )

                log.warning(
                    "Request failed | %s | "
                    "retry %d/%d | %s",
                    label,
                    attempt,
                    DATA_MAX_RETRIES,
                    exc,
                )

                time.sleep(delay)

            else:

                log.error(
                    "Request failed permanently | %s | %s",
                    label,
                    exc,
                )

    raise last_error or RuntimeError(
        f"Unknown Alpaca data failure: {label}"
    )


# ============================================================
# CACHE
# ============================================================

_BARS_CACHE = {}
_PRICE_CACHE = {}
_CONTRACT_CACHE = {}
_SNAPSHOT_CACHE = {}


def cache_get(
    cache,
    key,
):

    item = cache.get(key)

    if not item:
        return None

    timestamp, value = item

    if (
        time.time()
        - timestamp
        > DATA_CACHE_TTL_SECONDS
    ):
        return None

    return value


def cache_put(
    cache,
    key,
    value,
):

    cache[key] = (
        time.time(),
        value,
    )


# ============================================================
# DATE HELPERS
# ============================================================

def now_et():
    return datetime.now(
        EASTERN_TZ
    )


def utc_now():
    return datetime.now(
        timezone.utc
    )


def iso_utc(dt):

    return (
        dt.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


# ============================================================
# STOCK DATA
# ============================================================

def alpaca_bars_many(
    symbols,
    days=60,
):

    symbols = _dedupe(symbols)

    if not symbols:
        return {}

    days = min(
        int(days),
        MAX_BARS_DAYS,
    )

    cache_key = (
        tuple(sorted(symbols)),
        days,
        ALPACA_STOCK_FEED,
    )

    cached = cache_get(
        _BARS_CACHE,
        cache_key,
    )

    if cached is not None:
        return cached

    calendar_days = (
        int(days * 1.55) + 15
    )

    end_dt = utc_now()

    start_dt = (
        end_dt
        - timedelta(
            days=calendar_days
        )
    )

    result = {
        symbol: []
        for symbol in symbols
    }

    for start_index in range(
        0,
        len(symbols),
        DATA_BATCH_SIZE,
    ):

        batch = symbols[
            start_index:
            start_index + DATA_BATCH_SIZE
        ]

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

            data = alpaca_data_get(
                "/v2/stocks/bars",
                params=params,
                label="bars",
            )

            bars = data.get(
                "bars",
                {},
            )

            if not isinstance(
                bars,
                dict,
            ):
                continue

            for symbol in batch:

                closes = []

                for bar in bars.get(
                    symbol,
                    [],
                ):

                    try:

                        close = float(
                            bar["c"]
                        )

                        if close > 0:
                            closes.append(
                                close
                            )

                    except Exception:
                        continue

                result[symbol] = closes[-days:]

        except Exception as exc:

            ENGINE_STATE[
                "data_failures"
            ] += 1

            log.error(
                "Historical data batch failed: %s",
                exc,
            )

    cache_put(
        _BARS_CACHE,
        cache_key,
        result,
    )

    return result


def latest_prices_many(symbols):

    symbols = _dedupe(symbols)

    if not symbols:
        return {}

    cache_key = (
        tuple(sorted(symbols)),
        ALPACA_STOCK_FEED,
    )

    cached = cache_get(
        _PRICE_CACHE,
        cache_key,
    )

    if cached is not None:
        return cached

    result = {}

    for start_index in range(
        0,
        len(symbols),
        DATA_BATCH_SIZE,
    ):

        batch = symbols[
            start_index:
            start_index + DATA_BATCH_SIZE
        ]

        params = {
            "symbols": ",".join(batch),
            "feed": ALPACA_STOCK_FEED,
        }

        try:

            data = alpaca_data_get(
                "/v2/stocks/trades/latest",
                params=params,
                label="latest prices",
            )

            trades = data.get(
                "trades",
                {},
            )

            for symbol in batch:

                trade = trades.get(
                    symbol
                )

                if not trade:
                    continue

                try:

                    price = float(
                        trade["p"]
                    )

                    if price > 0:
                        result[symbol] = price

                except Exception:
                    continue

        except Exception as exc:

            ENGINE_STATE[
                "data_failures"
            ] += 1

            log.warning(
                "Latest prices failed: %s",
                exc,
            )

    cache_put(
        _PRICE_CACHE,
        cache_key,
        result,
    )

    return result


# ============================================================
# MOMENTUM
# ============================================================

def momentum_from_bars(
    bars,
    lookback=MOMENTUM_LOOKBACK,
):

    if len(bars) < lookback + 1:
        return None

    try:

        return (
            bars[-1]
            / bars[-lookback]
        ) - 1.0

    except Exception:
        return None


# ============================================================
# HISTORICAL VOLATILITY
# ============================================================

def volatility_from_bars(
    bars,
    window=VOLATILITY_WINDOW,
):

    if len(bars) < window + 1:
        return None

    returns = []

    for i in range(
        1,
        len(bars),
    ):

        previous = bars[i - 1]
        current = bars[i]

        if previous <= 0:
            continue

        try:

            returns.append(
                (current / previous)
                - 1.0
            )

        except Exception:
            continue

    if len(returns) < window:
        return None

    sample = returns[-window:]

    mean = (
        sum(sample)
        / len(sample)
    )

    variance = (
        sum(
            (r - mean) ** 2
            for r in sample
        )
        / len(sample)
    )

    return sqrt(
        variance
    ) * sqrt(TRADING_DAYS)


# ============================================================
# BLACK-SCHOLES
# ============================================================

def normal_cdf(x):

    return 0.5 * (
        1.0
        + erf(
            x / sqrt(2.0)
        )
    )


def bs_price(
    S,
    K,
    T,
    r,
    sigma,
    option_type,
):

    if (
        S <= 0
        or K <= 0
        or T <= 0
        or sigma <= 0
    ):
        return 0.0

    d1 = (
        ln(S / K)
        + (
            r
            + 0.5 * sigma * sigma
        ) * T
    ) / (
        sigma * sqrt(T)
    )

    d2 = (
        d1
        - sigma * sqrt(T)
    )

    if option_type == "call":

        return (
            S * normal_cdf(d1)
            - K
            * exp(-r * T)
            * normal_cdf(d2)
        )

    return (
        K
        * exp(-r * T)
        * normal_cdf(-d2)
        - S
        * normal_cdf(-d1)
    )


def bs_delta(
    S,
    K,
    T,
    r,
    sigma,
    option_type,
):

    if (
        S <= 0
        or K <= 0
        or T <= 0
        or sigma <= 0
    ):
        return 0.0

    d1 = (
        ln(S / K)
        + (
            r
            + 0.5 * sigma * sigma
        ) * T
    ) / (
        sigma * sqrt(T)
    )

    if option_type == "call":
        return normal_cdf(d1)

    return (
        normal_cdf(d1)
        - 1.0
    )


# ============================================================
# OPTION CONTRACT DATA
# ============================================================

def get_option_contracts(
    underlying,
):

    underlying = (
        underlying.upper()
    )

    cache_key = (
        underlying,
        MIN_OPTION_DTE,
        MAX_OPTION_DTE,
    )

    cached = cache_get(
        _CONTRACT_CACHE,
        cache_key,
    )

    if cached is not None:
        return cached

    today = now_et().date()

    min_expiration = (
        today
        + timedelta(
            days=MIN_OPTION_DTE
        )
    )

    max_expiration = (
        today
        + timedelta(
            days=MAX_OPTION_DTE
        )
    )

    params = {
        "underlying_symbols": underlying,
        "status": "active",
        "expiration_date_gte":
            min_expiration.isoformat(),
        "expiration_date_lte":
            max_expiration.isoformat(),
        "limit": 1000,
    }

    try:

        data = alpaca_data_get(
            "/v2/options/contracts",
            params=params,
            label=f"contracts:{underlying}",
        )

        contracts = data.get(
            "option_contracts",
            [],
        )

        clean = []

        for contract in contracts:

            try:

                if not contract.get(
                    "tradable",
                    False,
                ):
                    continue

                expiration = datetime.strptime(
                    contract[
                        "expiration_date"
                    ],
                    "%Y-%m-%d",
                ).date()

                if (
                    expiration
                    < min_expiration
                    or expiration
                    > max_expiration
                ):
                    continue

                option_type = (
                    contract.get(
                        "type",
                        ""
                    ).lower()
                )

                if option_type not in (
                    "call",
                    "put",
                ):
                    continue

                clean.append({
                    "symbol":
                        contract["symbol"],

                    "underlying":
                        underlying,

                    "expiration":
                        expiration,

                    "type":
                        option_type,

                    "strike":
                        float(
                            contract[
                                "strike_price"
                            ]
                        ),

                    "open_interest":
                        int(
                            contract.get(
                                "open_interest",
                                0
                            )
                            or 0
                        ),

                    "size":
                        int(
                            contract.get(
                                "size",
                                100
                            )
                            or 100
                        ),

                    "tradable":
                        bool(
                            contract.get(
                                "tradable",
                                False
                            )
                        ),
                })

            except Exception:
                continue

        cache_put(
            _CONTRACT_CACHE,
            cache_key,
            clean,
        )

        return clean

    except Exception as exc:

        ENGINE_STATE[
            "data_failures"
        ] += 1

        log.warning(
            "Contract lookup failed for %s: %s",
            underlying,
            exc,
        )

        return []


# ============================================================
# OPTION SNAPSHOTS
# ============================================================

def get_option_snapshots(
    contract_symbols,
):

    symbols = _dedupe(
        contract_symbols
    )

    if not symbols:
        return {}

    result = {}

    for start_index in range(
        0,
        len(symbols),
        OPTION_SNAPSHOT_BATCH_SIZE,
    ):

        batch = symbols[
            start_index:
            start_index
            + OPTION_SNAPSHOT_BATCH_SIZE
        ]

        params = {
            "symbols": ",".join(batch),
            "feed": ALPACA_OPTION_FEED,
            "limit": 1000,
        }

        try:

            data = alpaca_data_get(
                "/v1beta1/options/snapshots",
                params=params,
                label="option snapshots",
            )

            snapshots = data.get(
                "snapshots",
                {},
            )

            if not isinstance(
                snapshots,
                dict,
            ):
                continue

            for symbol in batch:

                snapshot = snapshots.get(
                    symbol
                )

                if not snapshot:
                    continue

                quote = (
                    snapshot.get(
                        "latestQuote",
                        {}
                    )
                    or {}
                )

                trade = (
                    snapshot.get(
                        "latestTrade",
                        {}
                    )
                    or {}
                )

                greeks = (
                    snapshot.get(
                        "greeks",
                        {}
                    )
                    or {}
                )

                try:

                    bid = float(
                        quote.get(
                            "bp",
                            0
                        )
                        or 0
                    )

                    ask = float(
                        quote.get(
                            "ap",
                            0
                        )
                        or 0
                    )

                    volume = int(
                        trade.get(
                            "s",
                            0
                        )
                        or 0
                    )

                    iv = float(
                        snapshot.get(
                            "impliedVolatility",
                            0
                        )
                        or 0
                    )

                    delta = float(
                        greeks.get(
                            "delta",
                            0
                        )
                        or 0
                    )

                    result[symbol] = {
                        "bid": bid,
                        "ask": ask,
                        "volume": volume,
                        "iv": iv,
                        "delta": delta,
                    }

                except Exception:
                    continue

        except Exception as exc:

            ENGINE_STATE[
                "data_failures"
            ] += 1

            log.warning(
                "Option snapshot batch failed: %s",
                exc,
            )

    return result


# ============================================================
# OPTION LIQUIDITY
# ============================================================

def option_liquidity_ok(
    snapshot,
):

    bid = snapshot["bid"]
    ask = snapshot["ask"]

    if bid <= 0:
        return False

    if ask <= 0:
        return False

    if ask < bid:
        return False

    mid = (
        bid + ask
    ) / 2.0

    if mid <= 0:
        return False

    spread_pct = (
        ask - bid
    ) / mid

    if (
        spread_pct
        > MAX_OPTION_SPREAD_PCT
    ):
        return False

    if (
        snapshot["volume"]
        < MIN_OPTION_VOLUME
    ):
        return False

    return True


# ============================================================
# OPTION CANDIDATE SCORING
# ============================================================

def build_option_candidates(
    market_data,
    prices,
):

    candidates = []

    for underlying in OPTIONS_UNIVERSE:

        bars = market_data.get(
            underlying,
            [],
        )

        spot = prices.get(
            underlying,
            0.0,
        )

        if spot <= 0:
            continue

        if len(bars) < 60:
            continue

        momentum = (
            momentum_from_bars(
                bars,
                MOMENTUM_LOOKBACK,
            )
        )

        if momentum is None:
            continue

        historical_vol = (
            volatility_from_bars(
                bars,
                VOLATILITY_WINDOW,
            )
        )

        if (
            historical_vol is None
            or historical_vol <= 0
        ):
            continue

        historical_vol = max(
            MIN_IV,
            min(
                historical_vol,
                MAX_IV,
            ),
        )

        contracts = (
            get_option_contracts(
                underlying
            )
        )

        if not contracts:
            continue

        contract_symbols = [
            c["symbol"]
            for c in contracts
        ]

        snapshots = (
            get_option_snapshots(
                contract_symbols
            )
        )

        for contract in contracts:

            snapshot = snapshots.get(
                contract["symbol"]
            )

            if not snapshot:
                continue

            if not option_liquidity_ok(
                snapshot
            ):
                continue

            bid = snapshot["bid"]
            ask = snapshot["ask"]

            mid = (
                bid + ask
            ) / 2.0

            if mid <= 0:
                continue

            if (
                mid
                > MAX_OPTION_PREMIUM
            ):
                continue

            iv = snapshot["iv"]

            if iv <= 0:
                iv = historical_vol

            iv = max(
                MIN_IV,
                min(
                    iv,
                    MAX_IV,
                ),
            )

            expiration = (
                contract[
                    "expiration"
                ]
            )

            dte = (
                expiration
                - now_et().date()
            ).days

            if dte <= 0:
                continue

            T = (
                dte
                / TRADING_DAYS
            )

            option_type = (
                contract["type"]
            )

            strike = (
                contract["strike"]
            )

            theoretical = bs_price(
                spot,
                strike,
                T,
                RISK_FREE_ANNUAL,
                historical_vol,
                option_type,
            )

            if theoretical <= 0:
                continue

            edge_dollars = (
                theoretical
                - mid
            )

            edge_pct = (
                edge_dollars
                / theoretical
            )

            if (
                edge_dollars
                < MIN_BS_EDGE_DOLLARS
            ):
                continue

            if (
                edge_pct
                < MIN_BS_EDGE_PCT
            ):
                continue

            bs_delta_value = bs_delta(
                spot,
                strike,
                T,
                RISK_FREE_ANNUAL,
                historical_vol,
                option_type,
            )

            # ------------------------------------------------
            # DIRECTION FILTER
            # ------------------------------------------------

            if option_type == "call":

                if (
                    bs_delta_value
                    < CALL_MIN_DELTA
                    or bs_delta_value
                    > CALL_MAX_DELTA
                ):
                    continue

                # Strongly bearish stocks should not
                # automatically create call trades.
                if (
                    momentum
                    < -DIRECTION_THRESHOLD
                ):
                    continue

            else:

                abs_delta = abs(
                    bs_delta_value
                )

                if (
                    abs_delta
                    < PUT_MIN_ABS_DELTA
                    or abs_delta
                    > PUT_MAX_ABS_DELTA
                ):
                    continue

                # Strongly bullish stocks should not
                # automatically create put trades.
                if (
                    momentum
                    > DIRECTION_THRESHOLD
                ):
                    continue

            spread_pct = (
                ask - bid
            ) / mid

            # ------------------------------------------------
            # SCORE
            # ------------------------------------------------

            liquidity_score = (
                min(
                    snapshot["volume"],
                    10000,
                )
                / 10000.0
            )

            oi_score = (
                min(
                    contract[
                        "open_interest"
                    ],
                    10000,
                )
                / 10000.0
            )

            score = (
                edge_pct * 5.0
                + liquidity_score
                + oi_score
                - spread_pct * 2.0
            )

            candidates.append({
                "symbol":
                    contract["symbol"],

                "underlying":
                    underlying,

                "type":
                    option_type,

                "strike":
                    strike,

                "expiration":
                    expiration,

                "dte":
                    dte,

                "spot":
                    spot,

                "bid":
                    bid,

                "ask":
                    ask,

                "mid":
                    mid,

                "iv":
                    iv,

                "historical_vol":
                    historical_vol,

                "delta":
                    bs_delta_value,

                "theoretical":
                    theoretical,

                "edge_dollars":
                    edge_dollars,

                "edge_pct":
                    edge_pct,

                "momentum":
                    momentum,

                "volume":
                    snapshot["volume"],

                "open_interest":
                    contract[
                        "open_interest"
                    ],

                "spread_pct":
                    spread_pct,

                "score":
                    score,
            })

    candidates.sort(
        key=lambda x: x["score"],
        reverse=True,
    )

    return candidates


# ============================================================
# ACCOUNT
# ============================================================

def get_equity(
    trading_client,
):

    try:

        account = (
            trading_client.get_account()
        )

        return float(
            account.equity
        )

    except Exception as exc:

        log.error(
            "Failed to fetch equity: %s",
            exc,
        )

        return 0.0


# ============================================================
# POSITIONS
# ============================================================

def get_positions(
    trading_client,
):

    try:

        positions = (
            trading_client
            .get_all_positions()
        )

        return {
            p.symbol: p
            for p in positions
        }

    except Exception as exc:

        log.warning(
            "Failed to fetch positions: %s",
            exc,
        )

        return {}


# ============================================================
# OPEN ORDERS
# ============================================================

def get_open_orders(
    trading_client,
):

    try:

        return (
            trading_client
            .get_orders(
                filter=None
            )
        )

    except Exception as exc:

        log.warning(
            "Failed to fetch open orders: %s",
            exc,
        )

        return []


# ============================================================
# TRADE COUNTER
# ============================================================

def reset_trade_counter_if_new_day():

    global TRADES_TODAY
    global LAST_TRADE_DAY

    today = now_et().date()

    if LAST_TRADE_DAY != today:

        LAST_TRADE_DAY = today

        TRADES_TODAY = 0

        ENGINE_STATE[
            "trades_today"
        ] = 0

        log.info(
            "New trading day. "
            "Option trade counter reset."
        )


# ============================================================
# MARKET WINDOW
# ============================================================

def in_trade_window():

    current = now_et()

    current_minutes = (
        current.hour * 60
        + current.minute
    )

    before = (
        NO_TRADE_BEFORE[0] * 60
        + NO_TRADE_BEFORE[1]
    )

    after = (
        NO_TRADE_AFTER[0] * 60
        + NO_TRADE_AFTER[1]
    )

    return (
        current_minutes >= before
        and current_minutes < after
    )


def market_is_open(
    trading_client,
):

    try:

        clock = (
            trading_client
            .get_clock()
        )

        is_open = bool(
            clock.is_open
        )

        ENGINE_STATE[
            "market_open"
        ] = is_open

        if not is_open:
            return False

        if not in_trade_window():
            return False

        return True

    except Exception as exc:

        log.warning(
            "Alpaca clock failed: %s",
            exc,
        )

        return in_trade_window()


# ============================================================
# SAFE OPTION ORDER
# ============================================================

def assert_option_order(
    symbol,
):

    if not symbol:
        raise ValueError(
            "Missing option symbol."
        )

    # OCC-style option symbols contain
    # a call/put marker in the contract.
    #
    # We also explicitly restrict the
    # trading asset class to US_OPTION.
    return True


def submit_option_order(
    trading_client,
    candidate,
    equity,
):

    global TRADES_TODAY

    reset_trade_counter_if_new_day()

    if (
        TRADES_TODAY
        >= MAX_TRADES_PER_DAY
    ):
        return False

    if not market_is_open(
        trading_client
    ):
        return False

    symbol = candidate[
        "symbol"
    ]

    assert_option_order(
        symbol
    )

    # --------------------------------------------------------
    # POSITION SIZING
    # --------------------------------------------------------

    budget_per_trade = (
        equity
        * MAX_POSITION_BUDGET_PCT
    )

    contract_cost = (
        candidate["mid"]
        * 100
    )

    if contract_cost <= 0:
        return False

    max_qty_by_budget = int(
        budget_per_trade
        / contract_cost
    )

    qty = min(
        max_qty_by_budget,
        MAX_CONTRACTS_PER_UNDERLYING,
    )

    if qty <= 0:
        return False

    # --------------------------------------------------------
    # LIMIT PRICE
    # --------------------------------------------------------

    bid = candidate["bid"]
    ask = candidate["ask"]
    theoretical = candidate[
        "theoretical"
    ]

    mid = (
        bid + ask
    ) / 2.0

    # Do not chase the ask if the option is
    # materially above Black-Scholes value.
    max_price = min(
        ask,
        theoretical * 0.98,
    )

    if max_price <= 0:
        return False

    # Never submit below the bid.
    limit_price = max(
        bid,
        min(
            mid,
            max_price,
        ),
    )

    limit_price = round(
        limit_price,
        2,
    )

    if limit_price <= 0:
        return False

    # --------------------------------------------------------
    # OPTION ORDER ONLY
    # --------------------------------------------------------

    order = LimitOrderRequest(
        symbol=symbol,
        qty=qty,
        side=OrderSide.BUY,
        type=OrderType.LIMIT,
        time_in_force=TimeInForce.DAY,
        limit_price=limit_price,
    )

    try:

        result = (
            trading_client
            .submit_order(
                order_data=order
            )
        )

        TRADES_TODAY += 1

        ENGINE_STATE[
            "trades_today"
        ] = TRADES_TODAY

        log.info(
            "OPTION ORDER #%d | "
            "%s | %s | "
            "strike=%.2f | "
            "DTE=%d | "
            "mid=%.2f | "
            "limit=%.2f | "
            "BS=%.2f | "
            "edge=%.1f%% | "
            "delta=%.3f | "
            "qty=%d | "
            "id=%s",
            TRADES_TODAY,
            candidate["underlying"],
            candidate["type"].upper(),
            candidate["strike"],
            candidate["dte"],
            candidate["mid"],
            limit_price,
            candidate["theoretical"],
            candidate["edge_pct"] * 100,
            candidate["delta"],
            qty,
            getattr(
                result,
                "id",
                "unknown",
            ),
        )

        return True

    except Exception as exc:

        log.warning(
            "Option order failed | %s | %s",
            symbol,
            exc,
        )

        return False


# ============================================================
# MAIN OPTIONS ENGINE
# ============================================================

class BlackScholesOptionsEngine:

    def __init__(
        self,
        trading_client,
    ):

        self.trading = (
            trading_client
        )

    # --------------------------------------------------------
    # PRELOAD DATA
    # --------------------------------------------------------

    def preload_market_data(self):

        log.info(
            "Loading historical data "
            "for %d option underlyings.",
            len(OPTIONS_UNIVERSE),
        )

        data = alpaca_bars_many(
            OPTIONS_UNIVERSE,
            days=MAX_BARS_DAYS,
        )

        successful = sum(
            1
            for symbol in OPTIONS_UNIVERSE
            if len(
                data.get(
                    symbol,
                    []
                )
            ) >= 60
        )

        log.info(
            "Usable underlyings: "
            "%d/%d",
            successful,
            len(OPTIONS_UNIVERSE),
        )

        return data

    # --------------------------------------------------------
    # EXISTING OPTION SYMBOLS
    # --------------------------------------------------------

    def existing_option_symbols(self):

        positions = get_positions(
            self.trading
        )

        open_orders = get_open_orders(
            self.trading
        )

        blocked = set()

        for symbol in positions:
            blocked.add(
                symbol
            )

        for order in open_orders:

            symbol = getattr(
                order,
                "symbol",
                None,
            )

            if symbol:
                blocked.add(
                    symbol
                )

        return blocked

    # --------------------------------------------------------
    # EXECUTE CANDIDATES
    # --------------------------------------------------------

    def execute_candidates(
        self,
        candidates,
    ):

        reset_trade_counter_if_new_day()

        if (
            TRADES_TODAY
            >= TARGET_TRADES_PER_DAY
        ):
            return 0

        equity = get_equity(
            self.trading
        )

        if equity <= 0:
            return 0

        blocked = (
            self.existing_option_symbols()
        )

        # Track underlying exposure
        # so we do not dump dozens of
        # contracts into one ticker.
        underlying_counts = {}

        for symbol in blocked:

            # OCC roots are variable length.
            # Use candidate matching instead
            # of trying to parse roots here.
            for candidate in candidates:

                if (
                    candidate["symbol"]
                    == symbol
                ):

                    underlying = (
                        candidate[
                            "underlying"
                        ]
                    )

                    underlying_counts[
                        underlying
                    ] = (
                        underlying_counts.get(
                            underlying,
                            0
                        )
                        + 1
                    )

                    break

        placed = 0

        cycle_budget = (
            equity
            * MAX_CYCLE_OPTION_BUDGET_PCT
        )

        estimated_spend = 0.0

        for candidate in candidates:

            if (
                TRADES_TODAY
                >= TARGET_TRADES_PER_DAY
            ):
                break

            symbol = candidate[
                "symbol"
            ]

            underlying = candidate[
                "underlying"
            ]

            if symbol in blocked:
                continue

            if (
                underlying_counts.get(
                    underlying,
                    0
                )
                >= MAX_CONTRACTS_PER_UNDERLYING
            ):
                continue

            estimated_order_cost = (
                candidate["mid"]
                * 100
            )

            if (
                estimated_spend
                + estimated_order_cost
                > cycle_budget
            ):
                continue

            log.info(
                "CANDIDATE | "
                "%s | %s | "
                "spot=%.2f | "
                "strike=%.2f | "
                "dte=%d | "
                "mid=%.2f | "
                "BS=%.2f | "
                "edge=%.1f%% | "
                "momentum=%.2f%%",
                underlying,
                candidate["type"].upper(),
                candidate["spot"],
                candidate["strike"],
                candidate["dte"],
                candidate["mid"],
                candidate["theoretical"],
                candidate["edge_pct"] * 100,
                candidate["momentum"] * 100,
            )

            if submit_option_order(
                self.trading,
                candidate,
                equity,
            ):

                placed += 1

                estimated_spend += (
                    estimated_order_cost
                )

                underlying_counts[
                    underlying
                ] = (
                    underlying_counts.get(
                        underlying,
                        0
                    )
                    + 1
                )

        return placed

    # --------------------------------------------------------
    # ONE CYCLE
    # --------------------------------------------------------

    def run_once(self):

        reset_trade_counter_if_new_day()

        if not market_is_open(
            self.trading
        ):
            return

        if (
            TRADES_TODAY
            >= TARGET_TRADES_PER_DAY
        ):
            log.info(
                "Daily target already reached: "
                "%d/%d",
                TRADES_TODAY,
                TARGET_TRADES_PER_DAY,
            )

            return

        # ----------------------------------------------------
        # HISTORICAL DATA
        # ----------------------------------------------------

        market_data = (
            self.preload_market_data()
        )

        # ----------------------------------------------------
        # PRICES
        # ----------------------------------------------------

        usable_symbols = [
            symbol
            for symbol in OPTIONS_UNIVERSE
            if len(
                market_data.get(
                    symbol,
                    []
                )
            ) >= 60
        ]

        prices = latest_prices_many(
            usable_symbols
        )

        # ----------------------------------------------------
        # BLACK-SCHOLES CANDIDATES
        # ----------------------------------------------------

        candidates = (
            build_option_candidates(
                market_data,
                prices,
            )
        )

        ENGINE_STATE[
            "candidate_count"
        ] = len(candidates)

        log.info(
            "Black-Scholes candidates: %d",
            len(candidates),
        )

        # ----------------------------------------------------
        # TRADE
        # ----------------------------------------------------

        placed = (
            self.execute_candidates(
                candidates
            )
        )

        # ----------------------------------------------------
        # DAILY MINIMUM
        # ----------------------------------------------------

        reset_trade_counter_if_new_day()

        if (
            TRADES_TODAY
            < MIN_TRADES_PER_DAY
        ):

            log.warning(
                "DAILY MINIMUM NOT YET REACHED | "
                "%d/%d option orders submitted.",
                TRADES_TODAY,
                MIN_TRADES_PER_DAY,
            )

        else:

            log.info(
                "DAILY MINIMUM REACHED | "
                "%d option orders submitted.",
                TRADES_TODAY,
            )

        log.info(
            "Cycle complete | "
            "candidates=%d | "
            "new_orders=%d | "
            "trades_today=%d",
            len(candidates),
            placed,
            TRADES_TODAY,
        )

        gc.collect()


# ============================================================
# ENVIRONMENT HELPERS
# ============================================================

def _env_bool(
    name,
    default=True,
):

    value = os.getenv(
        name
    )

    if value is None:
        return default

    return (
        value.strip().lower()
        in (
            "1",
            "true",
            "yes",
            "on",
        )
    )


# ============================================================
# TRADING LOOP
# ============================================================

def trading_loop():

    ENGINE_STATE[
        "thread_started"
    ] = True

    ENGINE_STATE[
        "thread_alive"
    ] = True

    try:

        initialize_credentials()

    except Exception as exc:

        log.error(
            "Credential initialization failed: %s",
            exc,
        )

        ENGINE_STATE[
            "last_error"
        ] = str(exc)

        ENGINE_STATE[
            "thread_alive"
        ] = False

        return

    paper = _env_bool(
        "ALPACA_PAPER",
        True,
    )

    log.info(
        "=================================================="
    )

    log.info(
        "BLACK-SCHOLES OPTIONS-ONLY ENGINE"
    )

    log.info(
        "=================================================="
    )

    log.info(
        "Paper trading: %s",
        paper,
    )

    log.info(
        "Stock feed: %s",
        ALPACA_STOCK_FEED,
    )

    log.info(
        "Option feed: %s",
        ALPACA_OPTION_FEED,
    )

    log.info(
        "Universe size: %d",
        len(OPTIONS_UNIVERSE),
    )

    log.info(
        "Daily target: %d",
        TARGET_TRADES_PER_DAY,
    )

    # --------------------------------------------------------
    # ALPACA
    # --------------------------------------------------------

    try:

        trading_client = TradingClient(
            ALPACA_API_KEY,
            ALPACA_API_SECRET,
            paper=paper,
        )

    except Exception as exc:

        log.exception(
            "Failed to initialize Alpaca: %s",
            exc,
        )

        ENGINE_STATE[
            "last_error"
        ] = str(exc)

        ENGINE_STATE[
            "thread_alive"
        ] = False

        return

    # --------------------------------------------------------
    # OPTIONS-ONLY ENGINE
    # --------------------------------------------------------

    engine = (
        BlackScholesOptionsEngine(
            trading_client
        )
    )

    # --------------------------------------------------------
    # MAIN LOOP
    # --------------------------------------------------------

    while True:

        ENGINE_STATE[
            "last_cycle_started"
        ] = now_et().isoformat()

        try:

            engine.run_once()

            ENGINE_STATE[
                "last_error"
            ] = None

        except Exception as exc:

            log.exception(
                "Engine cycle error: %s",
                exc,
            )

            ENGINE_STATE[
                "last_error"
            ] = str(exc)

        ENGINE_STATE[
            "last_cycle_finished"
        ] = now_et().isoformat()

        gc.collect()

        log.info(
            "Cycle finished. "
            "Sleeping %ss.",
            LOOP_SLEEP_SECONDS,
        )

        time.sleep(
            LOOP_SLEEP_SECONDS
        )


# ============================================================
# THREAD WRAPPER
# ============================================================

def _thread_wrapper():

    try:

        trading_loop()

    except Exception as exc:

        log.exception(
            "Trading thread crashed: %s",
            exc,
        )

        ENGINE_STATE[
            "last_error"
        ] = (
            f"Thread crashed: {exc}"
        )

        ENGINE_STATE[
            "thread_alive"
        ] = False


# ============================================================
# START ENGINE
# ============================================================

threading.Thread(
    target=_thread_wrapper,
    daemon=True,
).start()

log.info(
    "Black-Scholes options trading "
    "thread launched."
)


# ============================================================
# FLASK SERVER
# ============================================================

if __name__ == "__main__":

    port = int(
        os.getenv(
            "PORT",
            10000,
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
    )
