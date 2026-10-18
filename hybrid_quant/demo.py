"""Deterministic synthetic scenarios for engineering verification, never market evidence."""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
import math
import random

from .models import (ENGINE_TRAILING_WINDOW_SESSIONS, AssetType, DailyBar,
                     DataSnapshot, Security, StockFactorObservation)

CHINA_TIME = timezone(timedelta(hours=8))
# The engine reads at most 121 known sessions per security (the stock
# strategy's lookback) and never older bars, so a snapshot only has to carry a
# trailing window of observable history. Supplying the whole growing prefix
# instead makes a replay cost quadratic time and memory in the session count;
# the window keeps every engine-visible value identical to the full prefix.
HISTORY_WINDOW_SESSIONS = ENGINE_TRAILING_WINDOW_SESSIONS


def synthetic_snapshots(config: dict) -> tuple[DataSnapshot, ...]:
    if config.get("purpose") != "synthetic_engineering_demo":
        raise ValueError("Demo accepts only an explicitly synthetic configuration")
    total, evaluation_start, stock_count = (config[key] for key in ("history_sessions", "evaluation_start_session", "stock_count"))
    if not all(isinstance(value, int) and not isinstance(value, bool) for value in (total, evaluation_start, stock_count)):
        raise ValueError("Fixture sizes must be integers")
    if evaluation_start < 250 or total < evaluation_start + 2 or total > 3000 or not 25 <= stock_count <= 200:
        raise ValueError("Demo requires at least 250 warmup sessions, two evaluation sessions, and 25–200 stocks")
    window = config.get("history_window_sessions", HISTORY_WINDOW_SESSIONS)
    if not isinstance(window, int) or isinstance(window, bool) or not 121 <= window <= total:
        raise ValueError("history_window_sessions must be an integer of at least 121 and at most history_sessions")
    rng = random.Random(config["seed"])
    dates = []
    day = date(2021, 1, 4)
    while len(dates) < total:
        if day.weekday() < 5:
            dates.append(day)
        day += timedelta(days=1)
    sectors = tuple(f"synthetic_industry_{index}" for index in range(6))
    securities, parameters, factors = {}, {}, {}
    for index in range(stock_count):
        symbol = f"SYN_ST_{index + 1:02d}"
        securities[symbol] = Security(
            symbol, AssetType.STOCK, dates[0], industry_exposure={sectors[index % len(sectors)]: 1.0},
            historical_metadata_verified=True, lookthrough_verified=True, eligible_for_new_positions=True,
        )
        parameters[symbol] = (10 + index * 0.65, 0.00035 + (index % 7) * 0.00003, 0.0025 + (index % 5) * 0.0005)
        first = StockFactorObservation(symbol, 0.06 + rng.random() * 0.2, 0.025 + rng.random() * 0.1,
                                       datetime.combine(dates[249], time(16, 5), CHINA_TIME), True)
        later = StockFactorObservation(symbol, 0.06 + rng.random() * 0.2, 0.025 + rng.random() * 0.1,
                                       datetime.combine(dates[min(335, total - 1)], time(16, 5), CHINA_TIME), True)
        factors[symbol] = (first, later)
    for index in range(4):
        symbol = f"SYN_EQ_ETF_{index + 1}"
        securities[symbol] = Security(
            symbol, AssetType.EQUITY_ETF, dates[0],
            industry_exposure={sector: 1 / len(sectors) for sector in sectors},
            tracking_index=f"synthetic_index_{index if index < 3 else 0}",
            historical_metadata_verified=True, lookthrough_verified=True, eligible_for_new_positions=True,
        )
        parameters[symbol] = (3 + index * 0.4, 0.0005 + index * 0.00004, 0.004 + index * 0.0005)
    bond = "SYN_BOND_ETF"
    securities[bond] = Security(bond, AssetType.BOND_ETF, dates[0], settlement_sessions=0,
                               historical_metadata_verified=True, lookthrough_verified=True, eligible_for_new_positions=True)
    parameters[bond] = (100.0, 0.00008, 0.000025)
    histories = {symbol: [] for symbol in securities}
    previous = {symbol: values[0] for symbol, values in parameters.items()}
    for session, day in enumerate(dates):
        common = rng.gauss(0, 0.0012) + 0.00012 * math.sin(session / 17)
        for symbol, (_, drift, deviation) in parameters.items():
            market = 0 if symbol == bond else common
            overnight = rng.gauss(0, deviation / 4)
            opening = previous[symbol] * (1 + overnight)
            closing = opening * (1 + drift + market + rng.gauss(0, deviation))
            if min(opening, closing) <= 0:
                raise ValueError("Synthetic price generator produced invalid prices")
            bar = DailyBar(symbol, day, opening, max(opening, closing) * 1.001,
                           min(opening, closing) * 0.999, closing, 500_000_000.0,
                           datetime.combine(day, time(16), CHINA_TIME), adjusted_close=closing,
                           tradable=True, buyable=True, sellable=True)
            histories[symbol].append(bar)
            previous[symbol] = closing
    return tuple(DataSnapshot(
        datetime.combine(day, time(16, 30), CHINA_TIME), securities,
        {symbol: tuple(history[max(0, session + 1 - window):session + 1]) for symbol, history in histories.items()},
        factors, {symbol: session + 1 for symbol in securities},
    ) for session, day in enumerate(dates) if session >= evaluation_start)
