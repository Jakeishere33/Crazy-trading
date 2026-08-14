import os
import time
import math
import random
import logging
import threading

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from flask import Flask, jsonify

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import LimitOrderRequest
from alpaca.trading.enums import (
    OrderSide,
    OrderType,
    TimeInForce,
)


# ============================================================
# FLASK APPLICATION
# ============================================================

app = Flask(__name__)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

log = logging.getLogger("OPTIONS_BS_ENGINE")


# ============================================================
# TIME ZONE
# ============================================================

EASTERN_TZ = ZoneInfo(
    "America/New_York"
)


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
# ALPACA CREDENTIALS
# ============================================================

ALPACA_API_KEY = os.getenv(
    "ALPACA_API_KEY",
    ""
).strip()

ALPACA_API_SECRET = os.getenv(
    "ALPACA_API_SECRET",
    ""
).strip()

ALPACA_PAPER = (
    os.getenv(
        "ALPACA_PAPER",
        "true",
    )
    .lower()
    == "true"
)


if not ALPACA_API_KEY:
    raise RuntimeError(
        "ALPACA_API_KEY is not configured."
    )


if not ALPACA_API_SECRET:
    raise RuntimeError(
        "ALPACA_API_SECRET is not configured."
    )


# ============================================================
# ALPACA CLIENT
# ============================================================

trading_client = TradingClient(
    ALPACA_API_KEY,
    ALPACA_API_SECRET,
    paper=ALPACA_PAPER,
)


# ============================================================
# ALPACA DATA CONFIGURATION
# ============================================================

ALPACA_DATA_URL = (
    "https://data.alpaca.markets"
)

ALPACA_STOCK_FEED = os.getenv(
    "ALPACA_STOCK_FEED",
    "iex",
)

ALPACA_OPTION_FEED = os.getenv(
    "ALPACA_OPTION_FEED",
    "indicative",
)

DATA_BATCH_SIZE = 50

DATA_MAX_RETRIES = 5

DATA_RETRY_BASE_DELAY = 1.5

DATA_CACHE_TTL_SECONDS = 60

HTTP_TIMEOUT = 15


# ============================================================
# HTTP SESSION
# ============================================================

_HTTP = requests.Session()

_HTTP.headers.update({
    "User-Agent":
        "BlackScholesOptionsEngine/1.0",

    "Accept":
        "application/json",

    "Connection":
        "keep-alive",
})


def alpaca_headers():

    return {
        "APCA-API-KEY-ID":
            ALPACA_API_KEY,

        "APCA-API-SECRET-KEY":
            ALPACA_API_SECRET,

        "Accept":
            "application/json",
    }


# ============================================================
# RESILIENT ALPACA DATA REQUEST
# ============================================================

def alpaca_data_get(
    path,
    params=None,
    label="",
):

    url = (
        f"{ALPACA_DATA_URL}{path}"
    )

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

            # ------------------------------------------------
            # SUCCESS
            # ------------------------------------------------

            if 200 <= status < 300:

                try:

                    return response.json()

                except ValueError as exc:

                    raise RuntimeError(
                        "Invalid JSON returned "
                        f"by Alpaca: {exc}"
                    )

            # ------------------------------------------------
            # RATE LIMIT
            # ------------------------------------------------

            if status == 429:

                retry_after = (
                    response.headers.get(
                        "Retry-After"
                    )
                )

                if retry_after:

                    try:
                        delay = float(
                            retry_after
                        )

                    except Exception:
                        delay = (
                            DATA_RETRY_BASE_DELAY
                            * (
                                2
                                ** (
                                    attempt - 1
                                )
                            )
                        )

                else:

                    delay = (
                        DATA_RETRY_BASE_DELAY
                        * (
                            2
                            ** (
                                attempt - 1
                            )
                        )
                    )

                delay += random.uniform(
                    0.2,
                    0.8,
                )

                log.warning(
                    "Rate limit | %s | "
                    "attempt %d/%d | "
                    "sleep %.1fs",
                    label,
                    attempt,
                    DATA_MAX_RETRIES,
                    delay,
                )

                time.sleep(
                    delay
                )

                continue

            # ------------------------------------------------
            # SERVER ERROR
            # ------------------------------------------------

            if status >= 500:

                delay = (
                    DATA_RETRY_BASE_DELAY
                    * (
                        2
                        ** (
                            attempt - 1
                        )
                    )
                )

                delay += random.uniform(
                    0.2,
                    0.8,
                )

                log.warning(
                    "Server error %s | "
                    "%s | "
                    "attempt %d/%d | "
                    "sleep %.1fs",
                    status,
                    label,
                    attempt,
                    DATA_MAX_RETRIES,
                    delay,
                )

                time.sleep(
                    delay
                )

                continue

            # ------------------------------------------------
            # CLIENT ERROR
            # ------------------------------------------------

            try:

                body = (
                    response.json()
                )

            except Exception:

                body = (
                    response.text[:500]
                )

            raise RuntimeError(
                f"Alpaca HTTP {status}: "
                f"{body}"
            )

        except Exception as exc:

            last_error = exc

            if attempt < DATA_MAX_RETRIES:

                delay = (
                    DATA_RETRY_BASE_DELAY
                    * (
                        2
                        ** (
                            attempt - 1
                        )
                    )
                )

                delay += random.uniform(
                    0.2,
                    0.8,
                )

                log.warning(
                    "Alpaca request failed | "
                    "%s | "
                    "attempt %d/%d | "
                    "%s | "
                    "retry %.1fs",
                    label,
                    attempt,
                    DATA_MAX_RETRIES,
                    exc,
                    delay,
                )

                time.sleep(
                    delay
                )

            else:

                log.error(
                    "Alpaca request failed | "
                    "%s | "
                    "after %d attempts | "
                    "%s",
                    label,
                    DATA_MAX_RETRIES,
                    exc,
                )

    raise (
        last_error
        or RuntimeError(
            f"Unknown Alpaca data failure: {label}"
        )
    )


# ============================================================
# DATA CACHE
# ============================================================

_BARS_CACHE = {}

_PRICE_CACHE = {}

_OPTIONS_CACHE = {}


def cache_get(
    cache,
    key,
):

    item = cache.get(
        key
    )

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
# SYMBOL HELPERS
# ============================================================

def dedupe(
    symbols,
):

    seen = set()

    output = []

    for symbol in symbols:

        symbol = (
            symbol
            .upper()
            .strip()
        )

        if (
            symbol
            and symbol not in seen
        ):

            seen.add(
                symbol
            )

            output.append(
                symbol
            )

    return output


# ============================================================
# SEMICONDUCTORS
# ============================================================

SEMICONDUCTOR_TICKERS = [

    "NVDA",
    "AMD",
    "AVGO",
    "INTC",
    "MU",
    "QCOM",
    "TXN",
    "AMAT",
    "LRCX",
    "KLAC",
    "ADI",
    "MRVL",
    "MCHP",
    "ON",
    "NXPI",
    "SWKS",
    "QRVO",
    "MPWR",
    "TER",
    "WOLF",
    "ARM",
    "ASML",
    "TSM",
    "ENTG",
    "ONTO",
    "CRDO",
    "ALGM",
    "SMTC",
    "SITM",
    "POWI",
]


# ============================================================
# MINING / METALS
# ============================================================

MINING_TICKERS = [

    "FCX",
    "NEM",
    "GOLD",
    "AEM",
    "SCCO",
    "TECK",
    "RIO",
    "BHP",
    "VALE",
    "AA",
    "CLF",
    "NUE",
    "STLD",
    "MP",
    "CDE",
    "HL",
    "PAAS",
    "AG",
    "WPM",
    "FNV",
    "ATI",
    "CMC",
    "MOS",
    "CF",
    "ARCH",
    "BTU",
]


# ============================================================
# PHARMA / BIOTECH
# ============================================================

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
    "INCY",
    "ALNY",
    "EXEL",
    "BMRN",
    "UTHR",
    "ARGX",
    "IONS",
    "SRPT",
    "RARE",
    "HALO",
    "NBIX",
    "JAZZ",
]


# ============================================================
# FINAL OPTION UNIVERSE
# ============================================================

OPTION_UNIVERSE = dedupe(
    SEMICONDUCTOR_TICKERS
    + MINING_TICKERS
    + PHARMA_TICKERS
)


OPTIONABLE_SYMBOLS = set(
    OPTION_UNIVERSE
)


# ============================================================
# STRATEGY CONFIGURATION
# ============================================================

TARGET_TRADES_PER_DAY = 100

MAX_TRADES_PER_DAY = 110

MIN_TRADES_PER_DAY = 100


# ------------------------------------------------------------
# OPTION EXPIRATION
# ------------------------------------------------------------

OPTIONS_MIN_DTE = 14

OPTIONS_MAX_DTE = 45


# ------------------------------------------------------------
# BLACK-SCHOLES
# ------------------------------------------------------------

RISK_FREE_RATE = 0.04


# ------------------------------------------------------------
# OPTION QUALITY FILTERS
# ------------------------------------------------------------

MIN_OPTION_VOLUME = 1

MIN_OPEN_INTEREST = 10

MAX_OPTION_SPREAD_PCT = 0.15

MAX_OPTION_PRICE = 15.00


# ------------------------------------------------------------
# EDGE REQUIREMENTS
# ------------------------------------------------------------

MIN_BS_EDGE = 0.10

MIN_BS_EDGE_PERCENT = 0.08


# ------------------------------------------------------------
# RISK
# ------------------------------------------------------------

ACCOUNT_RISK_PERCENT = 0.005

MAX_OPTION_CONTRACTS_PER_TRADE = 1


# ------------------------------------------------------------
# ENGINE
# ------------------------------------------------------------

LOOP_SLEEP_SECONDS = 30


# ============================================================
# DAILY STATE
# ============================================================

TRADES_TODAY = 0

LAST_TRADE_DAY = None

ENGINE_LOCK = threading.Lock()


STATE = {

    "running":
        False,

    "market_open":
        False,

    "trades_today":
        0,

    "target_trades":
        TARGET_TRADES_PER_DAY,

    "candidates":
        0,

    "last_cycle":
        None,

    "last_error":
        None,

    "paper":
        ALPACA_PAPER,

    "universe_size":
        len(OPTION_UNIVERSE),
}


# ============================================================
# DAILY RESET
# ============================================================

def reset_daily_counter():

    global TRADES_TODAY

    global LAST_TRADE_DAY

    today = (
        now_et().date()
    )

    if (
        LAST_TRADE_DAY
        != today
    ):

        LAST_TRADE_DAY = today

        TRADES_TODAY = 0

        STATE[
            "trades_today"
        ] = 0

        log.info(
            "New trading day | "
            "%s",
            today,
        )


# ============================================================
# MARKET CLOCK
# ============================================================

def market_is_open():

    try:

        clock = (
            trading_client
            .get_clock()
        )

        is_open = bool(
            clock.is_open
        )

        STATE[
            "market_open"
        ] = is_open

        return is_open

    except Exception as exc:

        log.error(
            "Unable to read Alpaca clock: %s",
            exc,
        )

        STATE[
            "market_open"
        ] = False

        return False


# ============================================================
# TRADING WINDOW
# ============================================================

def valid_trade_time():

    current = now_et()

    current_minutes = (
        current.hour * 60
        + current.minute
    )

    start_minutes = (
        9 * 60
        + 35
    )

    end_minutes = (
        15 * 60
        + 45
    )

    return (
        start_minutes
        <= current_minutes
        <= end_minutes
    )


# ============================================================
# BLACK-SCHOLES NORMAL CDF
# ============================================================

def normal_cdf(
    x,
):

    return (
        1.0
        + math.erf(
            x
            / math.sqrt(2.0)
        )
    ) / 2.0


# ============================================================
# BLACK-SCHOLES PRICING
# ============================================================

def black_scholes_price(
    stock_price,
    strike_price,
    time_to_expiry,
    volatility,
    risk_free_rate,
    option_type,
):

    if stock_price <= 0:
        return 0.0

    if strike_price <= 0:
        return 0.0

    if time_to_expiry <= 0:
        return 0.0

    if volatility <= 0:
        return 0.0

    try:

        d1 = (
            math.log(
                stock_price
                / strike_price
            )
            + (
                risk_free_rate
                + (
                    volatility
                    ** 2
                )
                / 2.0
            )
            * time_to_expiry
        ) / (
            volatility
            * math.sqrt(
                time_to_expiry
            )
        )

        d2 = (
            d1
            - volatility
            * math.sqrt(
                time_to_expiry
            )
        )

        if (
            option_type
            == "call"
        ):

            value = (
                stock_price
                * normal_cdf(d1)
                - strike_price
                * math.exp(
                    -risk_free_rate
                    * time_to_expiry
                )
                * normal_cdf(d2)
            )

        else:

            value = (
                strike_price
                * math.exp(
                    -risk_free_rate
                    * time_to_expiry
                )
                * normal_cdf(-d2)
                - stock_price
                * normal_cdf(-d1)
            )

        return max(
            float(value),
            0.0,
        )

    except Exception:

        return 0.0


# ============================================================
# HISTORICAL STOCK BARS
# ============================================================

def alpaca_bars_many(
    symbols,
    days=60,
):

    symbols = dedupe(
        symbols
    )

    if not symbols:
        return {}

    days = min(
        int(days),
        220,
    )

    cache_key = (
        tuple(
            sorted(
                symbols
            )
        ),
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
        int(
            days * 1.55
        )
        + 15
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
            start_index
            + DATA_BATCH_SIZE
        ]

        params = {

            "symbols":
                ",".join(batch),

            "timeframe":
                "1Day",

            "start":
                iso_utc(
                    start_dt
                ),

            "end":
                iso_utc(
                    end_dt
                ),

            "feed":
                ALPACA_STOCK_FEED,

            "adjustment":
                "split",

            "sort":
                "asc",

            "limit":
                10000,
        }

        try:

            data = alpaca_data_get(
                "/v2/stocks/bars",
                params=params,
                label="stock bars",
            )

            bars = (
                data.get(
                    "bars",
                    {}
                )
            )

            if not isinstance(
                bars,
                dict,
            ):

                bars = {}

            for symbol in batch:

                symbol_bars = (
                    bars.get(
                        symbol,
                        []
                    )
                )

                closes = []

                for bar in symbol_bars:

                    close = bar.get(
                        "c"
                    )

                    if close is not None:

                        try:

                            closes.append(
                                float(close)
                            )

                        except Exception:
                            pass

                result[
                    symbol
                ] = closes[-days:]

        except Exception as exc:

            log.warning(
                "Historical bars failed | "
                "%s | %s",
                ",".join(batch),
                exc,
            )

    cache_put(
        _BARS_CACHE,
        cache_key,
        result,
    )

    return result


# ============================================================
# HISTORICAL VOLATILITY
# ============================================================

def calculate_historical_volatility(
    prices,
):

    if len(prices) < 21:
        return None

    returns = []

    for i in range(
        1,
        len(prices),
    ):

        previous = prices[
            i - 1
        ]

        current = prices[
            i
        ]

        if previous <= 0:
            continue

        try:

            returns.append(
                math.log(
                    current
                    / previous
                )
            )

        except Exception:
            pass

    if len(returns) < 20:
        return None

    returns = returns[
        -20:
    ]

    mean = (
        sum(returns)
        / len(returns)
    )

    variance = (
        sum(
            (
                value
                - mean
            ) ** 2
            for value in returns
        )
        / (
            len(returns) - 1
        )
    )

    volatility = (
        math.sqrt(
            variance
        )
        * math.sqrt(252)
    )

    return volatility


# ============================================================
# CURRENT STOCK PRICES
# ============================================================

def get_current_stock_prices(
    symbols,
):

    symbols = dedupe(
        symbols
    )

    if not symbols:
        return {}

    cache_key = (
        tuple(
            sorted(
                symbols
            )
        ),
        ALPACA_STOCK_FEED,
    )

    cached = cache_get(
        _PRICE_CACHE,
        cache_key,
    )

    if cached is not None:
        return cached

    prices = {}

    for start_index in range(
        0,
        len(symbols),
        DATA_BATCH_SIZE,
    ):

        batch = symbols[
            start_index:
            start_index
            + DATA_BATCH_SIZE
        ]

        params = {

            "symbols":
                ",".join(batch),

            "feed":
                ALPACA_STOCK_FEED,
        }

        try:

            data = alpaca_data_get(
                "/v2/stocks/snapshots",
                params=params,
                label="stock snapshots",
            )

            snapshots = (
                data.get(
                    "snapshots",
                    {}
                )
            )

            for symbol, snapshot in (
                snapshots.items()
            ):

                latest_trade = (
                    snapshot.get(
                        "latestTrade"
                    )
                    or {}
                )

                price = (
                    latest_trade.get(
                        "p"
                    )
                )

                if price is None:

                    daily_bar = (
                        snapshot.get(
                            "dailyBar"
                        )
                        or {}
                    )

                    price = (
                        daily_bar.get(
                            "c"
                        )
                    )

                if price is not None:

                    try:

                        prices[
                            symbol
                        ] = float(
                            price
                        )

                    except Exception:
                        pass

        except Exception as exc:

            log.warning(
                "Price request failed | "
                "%s | %s",
                ",".join(batch),
                exc,
            )

    cache_put(
        _PRICE_CACHE,
        cache_key,
        prices,
    )

    return prices


# ============================================================
# OPTION CONTRACTS
# ============================================================

def get_option_contracts(
    underlying,
):

    today = (
        now_et().date()
    )

    expiration_min = (
        today
        + timedelta(
            days=OPTIONS_MIN_DTE
        )
    )

    expiration_max = (
        today
        + timedelta(
            days=OPTIONS_MAX_DTE
        )
    )

    cache_key = (
        underlying,
        expiration_min,
        expiration_max,
    )

    cached = cache_get(
        _OPTIONS_CACHE,
        cache_key,
    )

    if cached is not None:
        return cached

    params = {

        "underlying_symbols":
            underlying,

        "status":
            "active",

        "expiration_date_gte":
            expiration_min.isoformat(),

        "expiration_date_lte":
            expiration_max.isoformat(),

        "limit":
            1000,
    }

    try:

        data = alpaca_data_get(
            "/v2/options/contracts",
            params=params,
            label=f"contracts {underlying}",
        )

        contracts = (
            data.get(
                "option_contracts",
                []
            )
        )

        cache_put(
            _OPTIONS_CACHE,
            cache_key,
            contracts,
        )

        return contracts

    except Exception as exc:

        log.warning(
            "Option contracts failed | "
            "%s | %s",
            underlying,
            exc,
        )

        return []


# ============================================================
# OPTION SNAPSHOTS
# ============================================================

def get_option_snapshots(
    underlying,
):

    params = {

        "feed":
            ALPACA_OPTION_FEED,

        "limit":
            1000,
    }

    try:

        data = alpaca_data_get(
            f"/v1beta1/options/snapshots/{underlying}",
            params=params,
            label=f"option snapshots {underlying}",
        )

        return (
            data.get(
                "snapshots",
                {}
            )
        )

    except Exception as exc:

        log.warning(
            "Option snapshots failed | "
            "%s | %s",
            underlying,
            exc,
        )

        return {}


# ============================================================
# OPTION CANDIDATE BUILDER
# ============================================================

def build_option_candidates():

    candidates = []

    prices = (
        get_current_stock_prices(
            OPTION_UNIVERSE
        )
    )

    if not prices:

        log.warning(
            "No underlying prices available."
        )

        return []

    bars = (
        alpaca_bars_many(
            OPTION_UNIVERSE,
            days=60,
        )
    )

    for underlying in OPTION_UNIVERSE:

        stock_price = prices.get(
            underlying
        )

        if not stock_price:
            continue

        historical_prices = (
            bars.get(
                underlying,
                []
            )
        )

        volatility = (
            calculate_historical_volatility(
                historical_prices
            )
        )

        if not volatility:
            continue

        # Avoid absurd values caused by
        # bad or extremely thin data.
        volatility = max(
            volatility,
            0.15,
        )

        volatility = min(
            volatility,
            1.50,
        )

        contracts = (
            get_option_contracts(
                underlying
            )
        )

        if not contracts:
            continue

        snapshots = (
            get_option_snapshots(
                underlying
            )
        )

        if not snapshots:
            continue

        for contract in contracts:

            try:

                if not contract.get(
                    "tradable",
                    False,
                ):

                    continue

                option_symbol = (
                    contract.get(
                        "symbol"
                    )
                )

                if not option_symbol:
                    continue

                snapshot = (
                    snapshots.get(
                        option_symbol
                    )
                )

                if not snapshot:
                    continue

                quote = (
                    snapshot.get(
                        "latestQuote"
                    )
                    or {}
                )

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

                if bid <= 0:
                    continue

                if ask <= 0:
                    continue

                if ask < bid:
                    continue

                mid = (
                    bid
                    + ask
                ) / 2.0

                if mid <= 0:
                    continue

                if (
                    mid
                    > MAX_OPTION_PRICE
                ):
                    continue

                spread_pct = (
                    ask
                    - bid
                ) / mid

                if (
                    spread_pct
                    > MAX_OPTION_SPREAD_PCT
                ):
                    continue

                option_type = (
                    str(
                        contract.get(
                            "type",
                            ""
                        )
                    )
                    .lower()
                )

                if option_type not in (
                    "call",
                    "put",
                ):

                    continue

                strike = float(
                    contract.get(
                        "strike_price",
                        0
                    )
                )

                if strike <= 0:
                    continue

                expiration_string = (
                    contract.get(
                        "expiration_date"
                    )
                )

                if not expiration_string:
                    continue

                expiration = (
                    datetime.strptime(
                        expiration_string,
                        "%Y-%m-%d",
                    ).date()
                )

                dte = (
                    expiration
                    - now_et().date()
                ).days

                if (
                    dte
                    < OPTIONS_MIN_DTE
                ):
                    continue

                if (
                    dte
                    > OPTIONS_MAX_DTE
                ):
                    continue

                time_to_expiry = (
                    dte
                    / 365.0
                )

                theoretical_value = (
                    black_scholes_price(
                        stock_price=stock_price,
                        strike_price=strike,
                        time_to_expiry=time_to_expiry,
                        volatility=volatility,
                        risk_free_rate=RISK_FREE_RATE,
                        option_type=option_type,
                    )
                )

                if (
                    theoretical_value
                    <= 0
                ):
                    continue

                edge = (
                    theoretical_value
                    - mid
                )

                edge_percent = (
                    edge
                    / theoretical_value
                )

                if (
                    edge
                    < MIN_BS_EDGE
                ):
                    continue

                if (
                    edge_percent
                    < MIN_BS_EDGE_PERCENT
                ):
                    continue

                trade = (
                    snapshot.get(
                        "latestTrade"
                    )
                    or {}
                )

                volume = int(
                    trade.get(
                        "s",
                        0
                    )
                    or 0
                )

                open_interest = int(
                    contract.get(
                        "open_interest",
                        0
                    )
                    or 0
                )

                if (
                    volume
                    < MIN_OPTION_VOLUME
                ):
                    continue

                if (
                    open_interest
                    < MIN_OPEN_INTEREST
                ):
                    continue

                greeks = (
                    snapshot.get(
                        "greeks"
                    )
                    or {}
                )

                delta = float(
                    greeks.get(
                        "delta",
                        0
                    )
                    or 0
                )

                candidates.append({

                    "symbol":
                        option_symbol,

                    "underlying":
                        underlying,

                    "option_type":
                        option_type,

                    "strike":
                        strike,

                    "expiration":
                        expiration_string,

                    "dte":
                        dte,

                    "stock_price":
                        stock_price,

                    "bid":
                        bid,

                    "ask":
                        ask,

                    "mid":
                        mid,

                    "theoretical":
                        theoretical_value,

                    "edge":
                        edge,

                    "edge_percent":
                        edge_percent,

                    "spread_percent":
                        spread_pct,

                    "volume":
                        volume,

                    "open_interest":
                        open_interest,

                    "delta":
                        delta,

                    "volatility":
                        volatility,
                })

            except Exception:
                continue

    # Highest Black-Scholes discount first.
    candidates.sort(
        key=lambda item:
        item[
            "edge_percent"
        ],
        reverse=True,
    )

    return candidates


# ============================================================
# EXISTING POSITIONS
# ============================================================

def get_existing_positions():

    symbols = set()

    try:

        positions = (
            trading_client
            .get_all_positions()
        )

        for position in positions:

            symbol = getattr(
                position,
                "symbol",
                None,
            )

            if symbol:

                symbols.add(
                    symbol
                )

    except Exception as exc:

        log.warning(
            "Could not retrieve positions: %s",
            exc,
        )

    return symbols


# ============================================================
# OPEN ORDERS
# ============================================================

def get_open_order_symbols():

    symbols = set()

    try:

        orders = (
            trading_client
            .get_orders()
        )

        for order in orders:

            symbol = getattr(
                order,
                "symbol",
                None,
            )

            if symbol:

                symbols.add(
                    symbol
                )

    except Exception as exc:

        log.warning(
            "Could not retrieve open orders: %s",
            exc,
        )

    return symbols


# ============================================================
# ACCOUNT EQUITY
# ============================================================

def get_account_equity():

    try:

        account = (
            trading_client
            .get_account()
        )

        equity = float(
            account.equity
        )

        return equity

    except Exception as exc:

        log.error(
            "Account lookup failed: %s",
            exc,
        )

        return None


# ============================================================
# ORDER SIZE
# ============================================================

def calculate_quantity(
    option_price,
):

    equity = (
        get_account_equity()
    )

    if equity is None:
        return 0

    if option_price <= 0:
        return 0

    maximum_dollars = (
        equity
        * ACCOUNT_RISK_PERCENT
    )

    contract_cost = (
        option_price
        * 100.0
    )

    if contract_cost <= 0:
        return 0

    quantity = int(
        maximum_dollars
        / contract_cost
    )

    quantity = min(
        quantity,
        MAX_OPTION_CONTRACTS_PER_TRADE,
    )

    return max(
        quantity,
        0,
    )


# ============================================================
# SUBMIT OPTION BUY
# ============================================================

def submit_option_order(
    candidate,
):

    global TRADES_TODAY

    if (
        TRADES_TODAY
        >= MAX_TRADES_PER_DAY
    ):

        return False

    symbol = candidate[
        "symbol"
    ]

    bid = candidate[
        "bid"
    ]

    ask = candidate[
        "ask"
    ]

    if bid <= 0 or ask <= 0:
        return False

    # --------------------------------------------------------
    # START AT MIDPOINT
    # --------------------------------------------------------

    limit_price = (
        bid
        + ask
    ) / 2.0

    limit_price = round(
        limit_price,
        2,
    )

    if limit_price < bid:
        limit_price = bid

    if limit_price > ask:
        limit_price = ask

    # --------------------------------------------------------
    # POSITION SIZE
    # --------------------------------------------------------

    quantity = (
        calculate_quantity(
            limit_price
        )
    )

    if quantity < 1:

        log.info(
            "Skipping %s | "
            "insufficient account "
            "size for 1 contract.",
            symbol,
        )

        return False

    # --------------------------------------------------------
    # OPTIONS ONLY
    #
    # BUY means buy-to-open for a
    # new option position.
    # --------------------------------------------------------

    order = LimitOrderRequest(

        symbol=symbol,

        qty=quantity,

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

        STATE[
            "trades_today"
        ] = TRADES_TODAY

        order_id = getattr(
            result,
            "id",
            "unknown",
        )

        log.info(
            "OPTION TRADE #%d | "
            "BUY | "
            "%s | "
            "%s | "
            "strike %.2f | "
            "expiration %s | "
            "DTE %d | "
            "bid %.2f | "
            "ask %.2f | "
            "limit %.2f | "
            "BS %.2f | "
            "edge %.2f | "
            "edge %.1f%% | "
            "delta %.3f | "
            "qty %d | "
            "order %s",
            TRADES_TODAY,
            candidate[
                "underlying"
            ],
            candidate[
                "option_type"
            ].upper(),
            candidate[
                "strike"
            ],
            candidate[
                "expiration"
            ],
            candidate[
                "dte"
            ],
            candidate[
                "bid"
            ],
            candidate[
                "ask"
            ],
            limit_price,
            candidate[
                "theoretical"
            ],
            candidate[
                "edge"
            ],
            candidate[
                "edge_percent"
            ] * 100,
            candidate[
                "delta"
            ],
            quantity,
            order_id,
        )

        return True

    except Exception as exc:

        log.warning(
            "OPTION ORDER FAILED | "
            "%s | %s",
            symbol,
            exc,
        )

        return False


# ============================================================
# MAIN STRATEGY
# ============================================================

def run_strategy():

    global TRADES_TODAY

    reset_daily_counter()

    if (
        TRADES_TODAY
        >= TARGET_TRADES_PER_DAY
    ):

        log.info(
            "Daily target already reached | "
            "%d/%d",
            TRADES_TODAY,
            TARGET_TRADES_PER_DAY,
        )

        return

    if not market_is_open():

        log.info(
            "Market is closed."
        )

        return

    if not valid_trade_time():

        log.info(
            "Outside strategy trading window."
        )

        return

    if not ENGINE_LOCK.acquire(
        blocking=False
    ):

        log.info(
            "Previous strategy cycle "
            "is still running."
        )

        return

    try:

        log.info(
            "===================================================="
        )

        log.info(
            "BLACK-SCHOLES OPTIONS SCAN"
        )

        log.info(
            "===================================================="
        )

        log.info(
            "Universe: %d symbols",
            len(
                OPTION_UNIVERSE
            ),
        )

        log.info(
            "Trades today: %d/%d",
            TRADES_TODAY,
            TARGET_TRADES_PER_DAY,
        )

        log.info(
            "Paper trading: %s",
            ALPACA_PAPER,
        )

        candidates = (
            build_option_candidates()
        )

        STATE[
            "candidates"
        ] = len(candidates)

        log.info(
            "Qualified candidates: %d",
            len(candidates),
        )

        if not candidates:

            log.warning(
                "No qualified option "
                "candidates found."
            )

            return

        # ----------------------------------------------------
        # BLOCK DUPLICATES
        # ----------------------------------------------------

        blocked = (
            get_existing_positions()
        )

        blocked.update(
            get_open_order_symbols()
        )

        submitted_this_cycle = 0

        # ----------------------------------------------------
        # SUBMIT OPTIONS
        # ----------------------------------------------------

        for candidate in candidates:

            if (
                TRADES_TODAY
                >= TARGET_TRADES_PER_DAY
            ):

                break

            symbol = candidate[
                "symbol"
            ]

            if symbol in blocked:

                continue

            success = (
                submit_option_order(
                    candidate
                )
            )

            if success:

                submitted_this_cycle += 1

                blocked.add(
                    symbol
                )

            # Small delay so we do not
            # hammer the trading endpoint.
            time.sleep(
                0.15
            )

        log.info(
            "Cycle complete | "
            "new orders=%d | "
            "total today=%d/%d",
            submitted_this_cycle,
            TRADES_TODAY,
            TARGET_TRADES_PER_DAY,
        )

    finally:

        ENGINE_LOCK.release()


# ============================================================
# TRADING LOOP
# ============================================================

def trading_loop():

    STATE[
        "running"
    ] = True

    log.info(
        "===================================================="
    )

    log.info(
        "BLACK-SCHOLES OPTIONS ENGINE STARTED"
    )

    log.info(
        "===================================================="
    )

    log.info(
        "Paper trading: %s",
        ALPACA_PAPER,
    )

    log.info(
        "Option universe: %d symbols",
        len(
            OPTION_UNIVERSE
        ),
    )

    while True:

        try:

            reset_daily_counter()

            STATE[
                "last_cycle"
            ] = now_et().isoformat()

            run_strategy()

            STATE[
                "last_error"
            ] = None

        except Exception as exc:

            STATE[
                "last_error"
            ] = str(exc)

            log.exception(
                "Strategy error: %s",
                exc,
            )

        time.sleep(
            LOOP_SLEEP_SECONDS
        )


# ============================================================
# FLASK ROUTES
# ============================================================

@app.route("/")
def home():

    return jsonify({

        "status":
            "online",

        "engine":
            "Black-Scholes Options Only",

        "paper":
            ALPACA_PAPER,

        "trades_today":
            STATE[
                "trades_today"
            ],

        "target":
            TARGET_TRADES_PER_DAY,

        "candidates":
            STATE[
                "candidates"
            ],

        "universe_size":
            len(
                OPTION_UNIVERSE
            ),

        "sectors":
            [
                "Semiconductors",
                "Mining",
                "Pharma",
            ],
    })


# ============================================================
# HEALTH CHECK
# ============================================================

@app.route("/health")
def health():

    return jsonify(
        STATE
    )


# ============================================================
# STATUS
# ============================================================

@app.route("/status")
def status():

    return jsonify({

        "running":
            STATE[
                "running"
            ],

        "market_open":
            STATE[
                "market_open"
            ],

        "trades_today":
            STATE[
                "trades_today"
            ],

        "target_trades":
            TARGET_TRADES_PER_DAY,

        "minimum_target":
            MIN_TRADES_PER_DAY,

        "candidates":
            STATE[
                "candidates"
            ],

        "last_cycle":
            STATE[
                "last_cycle"
            ],

        "last_error":
            STATE[
                "last_error"
            ],

        "paper":
            ALPACA_PAPER,

        "universe_size":
            len(
                OPTION_UNIVERSE
            ),
    })


# ============================================================
# START ENGINE
# ============================================================

def start_engine():

    thread = threading.Thread(
        target=trading_loop,
        daemon=True,
        name="options-trading-engine",
    )

    thread.start()

    return thread


# ============================================================
# IMPORTANT:
#
# Gunicorn imports this file.
#
# We start ONE engine thread.
#
# DO NOT run Gunicorn with multiple workers.
# ============================================================

_ENGINE_STARTED = False


def ensure_engine_started():

    global _ENGINE_STARTED

    if _ENGINE_STARTED:
        return

    _ENGINE_STARTED = True

    start_engine()


# Start when imported by Gunicorn.
ensure_engine_started()


# ============================================================
# LOCAL DEVELOPMENT
# ============================================================

if __name__ == "__main__":

    port = int(
        os.getenv(
            "PORT",
            "10000",
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
    )
