"""
Helper utilities for the pipeline.
"""
from datetime import datetime, timedelta
from typing import List
import pandas as pd


def parse_date(date_str: str) -> datetime:
    """Parse date string in YYYYMMDD format."""
    return datetime.strptime(date_str, '%Y%m%d')


def format_date(date_obj: datetime) -> str:
    """Format datetime object to YYYYMMDD string."""
    return date_obj.strftime('%Y%m%d')


def get_trading_dates(start_date: str, end_date: str) -> List[str]:
    """
    Generate list of trading dates between start and end (inclusive).
    Excludes weekends and holidays.
    
    This is a convenience wrapper around TradingCalendar.
    For more control, use TradingCalendar directly.
    
    Args:
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        
    Returns:
        List of trading dates in YYYYMMDD format
    """
    from trading_calendar import TradingCalendar
    calendar = TradingCalendar()
    return calendar.get_trading_days(start_date, end_date)


def normalize_symbol(symbol: str) -> str:
    """Normalize symbol (uppercase, strip whitespace)."""
    return symbol.strip().upper()


def validate_symbol_format(symbol: str) -> bool:
    """Validate that symbol has correct format for v3 API."""
    if not symbol:
        return False
    if not all(c.isalnum() or c in ['.', '-', '/'] for c in symbol):
        return False
    if len(symbol) < 1 or len(symbol) > 15:  # V3 supports longer symbols
        return False
    return True


def validate_ohlc_relationships(df: pd.DataFrame) -> List[str]:
    """
    Validate OHLC price relationships.
    Returns list of error messages.
    """
    errors = []
    
    # Check high >= low
    invalid_high_low = df[df['high'] < df['low']]
    if len(invalid_high_low) > 0:
        errors.append(f"Found {len(invalid_high_low)} rows where high < low")
    
    # Check high >= open
    invalid_high_open = df[df['high'] < df['open']]
    if len(invalid_high_open) > 0:
        errors.append(f"Found {len(invalid_high_open)} rows where high < open")
    
    # Check high >= close
    invalid_high_close = df[df['high'] < df['close']]
    if len(invalid_high_close) > 0:
        errors.append(f"Found {len(invalid_high_close)} rows where high < close")
    
    # Check low <= open
    invalid_low_open = df[df['low'] > df['open']]
    if len(invalid_low_open) > 0:
        errors.append(f"Found {len(invalid_low_open)} rows where low > open")
    
    # Check low <= close
    invalid_low_close = df[df['low'] > df['close']]
    if len(invalid_low_close) > 0:
        errors.append(f"Found {len(invalid_low_close)} rows where low > close")
    
    # Check volume >= 0
    invalid_volume = df[df['volume'] < 0]
    if len(invalid_volume) > 0:
        errors.append(f"Found {len(invalid_volume)} rows where volume < 0")
    
    return errors


def calculate_file_size_mb(file_path: str) -> float:
    """Calculate file size in MB."""
    import os
    if os.path.exists(file_path):
        return os.path.getsize(file_path) / (1024 * 1024)
    return 0.0
