#!/usr/bin/env python3
"""
Market Flow Indicators Service for AI Prompt Variables

Provides aggregated market flow data formatted for AI prompt injection.
Unlike the chart API which returns time series, this returns:
- Current value
- Last N period values
- Relevant context (e.g., averages for comparison)

Supported variables:
- {SYMBOL}_CVD_{PERIOD} - Cumulative Volume Delta
- {SYMBOL}_TAKER_{PERIOD} - Taker Buy/Sell Volume and Ratio
- {SYMBOL}_OI_{PERIOD} - Open Interest
- {SYMBOL}_FUNDING_{PERIOD} - Funding Rate
- {SYMBOL}_DEPTH_{PERIOD} - Order Book Depth Ratio
"""

import logging
from decimal import Decimal
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func

from database.models import (
    MarketTradesAggregated,
    MarketOrderbookSnapshots,
    MarketAssetMetrics
)

logger = logging.getLogger(__name__)

# Timeframe to milliseconds mapping
TIMEFRAME_MS = {
    "1m": 60 * 1000,
    "3m": 3 * 60 * 1000,
    "5m": 5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "30m": 30 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "2h": 2 * 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
}


def floor_timestamp(ts_ms: int, interval_ms: int) -> int:
    """Floor timestamp to interval boundary"""
    return (ts_ms // interval_ms) * interval_ms


def decimal_to_float(val) -> Optional[float]:
    """Convert Decimal to float, handling None"""
    if val is None:
        return None
    return float(val)


def format_volume(value: float) -> str:
    """Format volume with appropriate unit (K, M, B)"""
    abs_val = abs(value)
    sign = "+" if value >= 0 else "-"
    if abs_val >= 1_000_000_000:
        return f"{sign}${abs_val/1_000_000_000:.2f}B"
    elif abs_val >= 1_000_000:
        return f"{sign}${abs_val/1_000_000:.2f}M"
    elif abs_val >= 1_000:
        return f"{sign}${abs_val/1_000:.2f}K"
    else:
        return f"{sign}${abs_val:.2f}"


def get_flow_indicators_for_prompt(
    db: Session,
    symbol: str,
    period: str,
    indicators: List[str],
    current_time_ms: Optional[int] = None
) -> Dict[str, Any]:
    """
    Get market flow indicator data formatted for AI prompt injection.

    Args:
        db: Database session
        symbol: Trading symbol (e.g., "BTC")
        period: Time period (e.g., "15m", "1h")
        indicators: List of indicators to calculate ["CVD", "TAKER", "OI", "FUNDING", "DEPTH"]
        current_time_ms: Current timestamp in ms (defaults to now)

    Returns:
        Dict with indicator name as key and raw data dict as value
    """
    if period not in TIMEFRAME_MS:
        logger.warning(f"Unsupported period: {period}")
        return {}

    interval_ms = TIMEFRAME_MS[period]

    if current_time_ms is None:
        from datetime import datetime
        current_time_ms = int(datetime.utcnow().timestamp() * 1000)

    results = {}

    for indicator in indicators:
        indicator_upper = indicator.upper()
        try:
            if indicator_upper == "CVD":
                results["CVD"] = _get_cvd_data(db, symbol, period, interval_ms, current_time_ms)
            elif indicator_upper == "TAKER":
                results["TAKER"] = _get_taker_data(db, symbol, period, interval_ms, current_time_ms)
            elif indicator_upper == "OI":
                results["OI"] = _get_oi_data(db, symbol, period, interval_ms, current_time_ms)
            elif indicator_upper == "OI_DELTA":
                results["OI_DELTA"] = _get_oi_delta_data(db, symbol, period, interval_ms, current_time_ms)
            elif indicator_upper == "FUNDING":
                results["FUNDING"] = _get_funding_data(db, symbol, period, interval_ms, current_time_ms)
            elif indicator_upper == "DEPTH":
                results["DEPTH"] = _get_depth_data(db, symbol, period, interval_ms, current_time_ms)
            elif indicator_upper == "IMBALANCE":
                results["IMBALANCE"] = _get_imbalance_data(db, symbol, period, interval_ms, current_time_ms)
            else:
                logger.warning(f"Unknown flow indicator: {indicator}")
        except Exception as e:
            logger.error(f"Error calculating flow indicator {indicator}: {e}")
            results[indicator_upper] = None

    return results


def _get_cvd_data(
    db: Session, symbol: str, period: str, interval_ms: int, current_time_ms: int
) -> Optional[Dict[str, Any]]:
    """
    Get CVD (Cumulative Volume Delta) data.

    CVD = Cumulative(Taker Buy Notional - Taker Sell Notional)
    """
    lookback_ms = interval_ms * 10
    start_time = current_time_ms - lookback_ms

    records = db.query(
        MarketTradesAggregated.timestamp,
        MarketTradesAggregated.taker_buy_notional,
        MarketTradesAggregated.taker_sell_notional
    ).filter(
        MarketTradesAggregated.symbol == symbol.upper(),
        MarketTradesAggregated.timestamp >= start_time,
        MarketTradesAggregated.timestamp <= current_time_ms
    ).order_by(MarketTradesAggregated.timestamp).all()

    if not records:
        from datetime import datetime
        logger.warning(
            f"CVD insufficient data: symbol={symbol}, period={period}, "
            f"query_range=[{datetime.utcfromtimestamp(start_time/1000)} - "
            f"{datetime.utcfromtimestamp(current_time_ms/1000)}], records_found=0"
        )
        return None

    # Aggregate by period
    buckets = {}
    for ts, buy_notional, sell_notional in records:
        bucket_ts = floor_timestamp(ts, interval_ms)
        if bucket_ts not in buckets:
            buckets[bucket_ts] = {"buy": Decimal("0"), "sell": Decimal("0")}
        buckets[bucket_ts]["buy"] += buy_notional or Decimal("0")
        buckets[bucket_ts]["sell"] += sell_notional or Decimal("0")

    # Calculate CVD for each period
    sorted_times = sorted(buckets.keys())
    period_deltas = []

    for ts in sorted_times:
        bucket = buckets[ts]
        delta = float(bucket["buy"] - bucket["sell"])
        period_deltas.append(delta)

    if not period_deltas:
        from datetime import datetime
        logger.warning(
            f"CVD insufficient data: symbol={symbol}, period={period}, "
            f"records_found={len(records)}, buckets=0"
        )
        return None

    last_5 = period_deltas[-5:] if len(period_deltas) >= 5 else period_deltas
    current_delta = period_deltas[-1]
    cumulative = sum(period_deltas)

    return {
        "current": current_delta,
        "last_5": last_5,
        "cumulative": cumulative,
        "period": period
    }


def _get_taker_data(
    db: Session, symbol: str, period: str, interval_ms: int, current_time_ms: int
) -> Optional[Dict[str, Any]]:
    """
    Get Taker Buy/Sell Volume data.

    Returns buy volume, sell volume, and buy/sell ratio.
    """
    lookback_ms = interval_ms * 10
    start_time = current_time_ms - lookback_ms

    records = db.query(
        MarketTradesAggregated.timestamp,
        MarketTradesAggregated.taker_buy_notional,
        MarketTradesAggregated.taker_sell_notional
    ).filter(
        MarketTradesAggregated.symbol == symbol.upper(),
        MarketTradesAggregated.timestamp >= start_time,
        MarketTradesAggregated.timestamp <= current_time_ms
    ).order_by(MarketTradesAggregated.timestamp).all()

    if not records:
        return None

    # Aggregate by period
    buckets = {}
    for ts, buy_notional, sell_notional in records:
        bucket_ts = floor_timestamp(ts, interval_ms)
        if bucket_ts not in buckets:
            buckets[bucket_ts] = {"buy": Decimal("0"), "sell": Decimal("0")}
        buckets[bucket_ts]["buy"] += buy_notional or Decimal("0")
        buckets[bucket_ts]["sell"] += sell_notional or Decimal("0")

    sorted_times = sorted(buckets.keys())
    ratios = []

    for ts in sorted_times:
        bucket = buckets[ts]
        buy = float(bucket["buy"])
        sell = float(bucket["sell"])
        ratio = buy / sell if sell > 0 else 1.0
        ratios.append(ratio)

    if not ratios:
        return None

    # Current period data
    current_bucket = buckets[sorted_times[-1]]
    current_buy = float(current_bucket["buy"])
    current_sell = float(current_bucket["sell"])
    current_ratio = current_buy / current_sell if current_sell > 0 else 1.0

    last_5_ratios = ratios[-5:] if len(ratios) >= 5 else ratios

    return {
        "buy": current_buy,
        "sell": current_sell,
        "ratio": current_ratio,
        "ratio_last_5": last_5_ratios,
        "period": period
    }


def _get_oi_data(
    db: Session, symbol: str, period: str, interval_ms: int, current_time_ms: int
) -> Optional[Dict[str, Any]]:
    """
    Get Open Interest absolute value data.

    Returns current OI and last 5 values.
    """
    lookback_ms = interval_ms * 10
    start_time = current_time_ms - lookback_ms

    records = db.query(
        MarketAssetMetrics.timestamp,
        MarketAssetMetrics.open_interest
    ).filter(
        MarketAssetMetrics.symbol == symbol.upper(),
        MarketAssetMetrics.timestamp >= start_time,
        MarketAssetMetrics.timestamp <= current_time_ms
    ).order_by(MarketAssetMetrics.timestamp).all()

    if not records:
        from datetime import datetime
        logger.warning(
            f"OI insufficient data: symbol={symbol}, period={period}, "
            f"query_range=[{datetime.utcfromtimestamp(start_time/1000)} - "
            f"{datetime.utcfromtimestamp(current_time_ms/1000)}], records_found=0"
        )
        return None

    # Aggregate by period - take last value in each bucket
    buckets = {}
    for ts, oi in records:
        bucket_ts = floor_timestamp(ts, interval_ms)
        buckets[bucket_ts] = oi

    sorted_times = sorted(buckets.keys())
    if not sorted_times:
        from datetime import datetime
        logger.warning(
            f"OI insufficient data: symbol={symbol}, period={period}, "
            f"records_found={len(records)}, buckets=0"
        )
        return None

    # Get OI values
    oi_values = [decimal_to_float(buckets[ts]) for ts in sorted_times]
    oi_values = [v for v in oi_values if v is not None]

    if not oi_values:
        from datetime import datetime
        logger.warning(
            f"OI insufficient data: symbol={symbol}, period={period}, "
            f"records_found={len(records)}, buckets={len(sorted_times)}, valid_values=0"
        )
        return None

    current_oi = oi_values[-1]
    last_5 = oi_values[-5:] if len(oi_values) >= 5 else oi_values

    return {
        "current": current_oi,
        "last_5": last_5,
        "period": period
    }


def _get_oi_delta_data(
    db: Session, symbol: str, period: str, interval_ms: int, current_time_ms: int
) -> Optional[Dict[str, Any]]:
    """
    Get Open Interest Delta (change percentage) data.

    Returns current OI change % and last 5 changes.
    """
    lookback_ms = interval_ms * 10
    start_time = current_time_ms - lookback_ms

    records = db.query(
        MarketAssetMetrics.timestamp,
        MarketAssetMetrics.open_interest
    ).filter(
        MarketAssetMetrics.symbol == symbol.upper(),
        MarketAssetMetrics.timestamp >= start_time,
        MarketAssetMetrics.timestamp <= current_time_ms
    ).order_by(MarketAssetMetrics.timestamp).all()

    if not records:
        from datetime import datetime
        logger.warning(
            f"OI_DELTA insufficient data: symbol={symbol}, period={period}, "
            f"query_range=[{datetime.utcfromtimestamp(start_time/1000)} - "
            f"{datetime.utcfromtimestamp(current_time_ms/1000)}], records_found=0"
        )
        return None

    # Aggregate by period - take last value in each bucket
    buckets = {}
    for ts, oi in records:
        bucket_ts = floor_timestamp(ts, interval_ms)
        buckets[bucket_ts] = oi

    sorted_times = sorted(buckets.keys())
    if len(sorted_times) < 2:
        from datetime import datetime
        logger.warning(
            f"OI_DELTA insufficient data: symbol={symbol}, period={period}, "
            f"records_found={len(records)}, buckets={len(sorted_times)}, need_min=2"
        )
        return None

    # Calculate OI changes
    oi_values = [decimal_to_float(buckets[ts]) for ts in sorted_times]
    oi_changes = []
    for i in range(1, len(oi_values)):
        if oi_values[i] and oi_values[i-1] and oi_values[i-1] != 0:
            change_pct = ((oi_values[i] - oi_values[i-1]) / oi_values[i-1]) * 100
            oi_changes.append(change_pct)

    if not oi_changes:
        from datetime import datetime
        logger.warning(
            f"OI_DELTA insufficient data: symbol={symbol}, period={period}, "
            f"records_found={len(records)}, buckets={len(sorted_times)}, valid_changes=0"
        )
        return None

    current_change = oi_changes[-1]
    last_5 = oi_changes[-5:] if len(oi_changes) >= 5 else oi_changes

    return {
        "current": current_change,
        "last_5": last_5,
        "period": period
    }


def _get_funding_data(
    db: Session, symbol: str, period: str, interval_ms: int, current_time_ms: int
) -> Optional[Dict[str, Any]]:
    """
    Get Funding Rate data.

    Returns current funding rate and last 5 values.
    """
    lookback_ms = interval_ms * 10
    start_time = current_time_ms - lookback_ms

    records = db.query(
        MarketAssetMetrics.timestamp,
        MarketAssetMetrics.funding_rate
    ).filter(
        MarketAssetMetrics.symbol == symbol.upper(),
        MarketAssetMetrics.timestamp >= start_time,
        MarketAssetMetrics.timestamp <= current_time_ms
    ).order_by(MarketAssetMetrics.timestamp).all()

    if not records:
        return None

    # Aggregate by period - take last value in each bucket
    buckets = {}
    for ts, funding in records:
        bucket_ts = floor_timestamp(ts, interval_ms)
        buckets[bucket_ts] = funding

    sorted_times = sorted(buckets.keys())
    if not sorted_times:
        return None

    # Get funding rate values (convert to percentage)
    funding_values = []
    for ts in sorted_times:
        fr = buckets[ts]
        if fr is not None:
            funding_values.append(float(fr) * 100)  # Convert to percentage

    if not funding_values:
        return None

    current_funding = funding_values[-1]
    last_5 = funding_values[-5:] if len(funding_values) >= 5 else funding_values

    # Calculate annualized rate (assuming 8-hour funding periods, 3 per day)
    annualized = current_funding * 3 * 365

    return {
        "current": current_funding,
        "last_5": last_5,
        "annualized": annualized,
        "period": period
    }


def _get_depth_data(
    db: Session, symbol: str, period: str, interval_ms: int, current_time_ms: int
) -> Optional[Dict[str, Any]]:
    """
    Get Order Book Depth data.

    Returns bid/ask depth ratio and last 5 values.
    """
    lookback_ms = interval_ms * 10
    start_time = current_time_ms - lookback_ms

    records = db.query(
        MarketOrderbookSnapshots.timestamp,
        MarketOrderbookSnapshots.bid_depth_5,
        MarketOrderbookSnapshots.ask_depth_5,
        MarketOrderbookSnapshots.spread
    ).filter(
        MarketOrderbookSnapshots.symbol == symbol.upper(),
        MarketOrderbookSnapshots.timestamp >= start_time,
        MarketOrderbookSnapshots.timestamp <= current_time_ms
    ).order_by(MarketOrderbookSnapshots.timestamp).all()

    if not records:
        return None

    # Aggregate by period - take last value in each bucket
    buckets = {}
    for ts, bid_depth, ask_depth, spread in records:
        bucket_ts = floor_timestamp(ts, interval_ms)
        buckets[bucket_ts] = {
            "bid": bid_depth,
            "ask": ask_depth,
            "spread": spread
        }

    sorted_times = sorted(buckets.keys())
    if not sorted_times:
        return None

    # Calculate depth ratios
    ratios = []
    for ts in sorted_times:
        bucket = buckets[ts]
        bid = decimal_to_float(bucket["bid"]) or 0
        ask = decimal_to_float(bucket["ask"]) or 0
        ratio = bid / ask if ask > 0 else 1.0
        ratios.append(ratio)

    current_bucket = buckets[sorted_times[-1]]
    current_bid = decimal_to_float(current_bucket["bid"]) or 0
    current_ask = decimal_to_float(current_bucket["ask"]) or 0
    current_ratio = current_bid / current_ask if current_ask > 0 else 1.0
    current_spread = decimal_to_float(current_bucket["spread"])

    last_5_ratios = ratios[-5:] if len(ratios) >= 5 else ratios

    return {
        "bid": current_bid,
        "ask": current_ask,
        "ratio": current_ratio,
        "ratio_last_5": last_5_ratios,
        "spread": current_spread,
        "period": period
    }


def _get_imbalance_data(
    db: Session, symbol: str, period: str, interval_ms: int, current_time_ms: int
) -> Optional[Dict[str, Any]]:
    """
    Get Order Book Imbalance data.

    Imbalance = (Bid - Ask) / (Bid + Ask), range -1 to 1
    Positive = more bid support, Negative = more ask pressure
    """
    lookback_ms = interval_ms * 10
    start_time = current_time_ms - lookback_ms

    records = db.query(
        MarketOrderbookSnapshots.timestamp,
        MarketOrderbookSnapshots.bid_depth_5,
        MarketOrderbookSnapshots.ask_depth_5
    ).filter(
        MarketOrderbookSnapshots.symbol == symbol.upper(),
        MarketOrderbookSnapshots.timestamp >= start_time,
        MarketOrderbookSnapshots.timestamp <= current_time_ms
    ).order_by(MarketOrderbookSnapshots.timestamp).all()

    if not records:
        return None

    # Aggregate by period - take last value in each bucket
    buckets = {}
    for ts, bid_depth, ask_depth in records:
        bucket_ts = floor_timestamp(ts, interval_ms)
        buckets[bucket_ts] = {"bid": bid_depth, "ask": ask_depth}

    sorted_times = sorted(buckets.keys())
    if not sorted_times:
        return None

    # Calculate imbalance values
    imbalances = []
    for ts in sorted_times:
        bucket = buckets[ts]
        bid = decimal_to_float(bucket["bid"]) or 0
        ask = decimal_to_float(bucket["ask"]) or 0
        total = bid + ask
        imbalance = (bid - ask) / total if total > 0 else 0.0
        imbalances.append(imbalance)

    current_imbalance = imbalances[-1]
    last_5 = imbalances[-5:] if len(imbalances) >= 5 else imbalances

    return {
        "current": current_imbalance,
        "last_5": last_5,
        "period": period
    }
