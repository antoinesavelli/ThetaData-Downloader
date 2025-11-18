"""
Data parser for processing ThetaData API responses.
"""
import pandas as pd
import io
import json
import logging
from typing import Optional, Union, Dict, Any, List
from datetime import datetime, timedelta
from pathlib import Path
from utils.helpers import validate_ohlc_relationships
from utils.diagnostics import ErrorTracer
from utils.null_data_tracker import NullDataTracker


class DataParser:
    """Parse and validate data from ThetaData API."""
    
    def __init__(self, logger: logging.Logger, output_dir: str = None):
        """
        Initialize parser with logger.
        
        Args:
            logger: Logger instance
            output_dir: Output directory for reading previous day's data (optional)
        """
        self.logger = logger
        self.output_dir = output_dir
        
        # Initialize null data tracker
        if output_dir:
            self.null_tracker = NullDataTracker(output_dir, logger)
        else:
            self.null_tracker = None
    
    def _get_date_folder_path(self, base_dir: str, date: str) -> Path:
        """
        Create folder path structure: base_dir/YYYY/MM/DD
        
        Args:
            base_dir: Base output directory
            date: Date in YYYYMMDD format
            
        Returns:
            Path object with year/month/day structure
        """
        # Parse date string YYYYMMDD
        year = date[:4]
        month = date[4:6]
        day = date[6:8]
        
        # Create path structure
        date_path = Path(base_dir) / year / month / day
        return date_path
    
    def _get_previous_day_close(self, symbol: str, current_date: str) -> Optional[float]:
        """
        Get the closing price from the previous trading day.
        Uses year/month/day folder structure and trading calendar.
        
        Args:
            symbol: Stock symbol
            current_date: Current date in YYYYMMDD format
            
        Returns:
            Previous day's closing price, or None if not found
        """
        if not self.output_dir:
            return None
        
        try:
            # Import trading calendar
            from utils.trading_calendar import TradingCalendar
            calendar = TradingCalendar(logger=self.logger)
            
            # Get previous trading day (skips weekends & holidays)
            previous_date = calendar.get_previous_trading_day(current_date)
            
            # Use date folder structure to find previous day's file
            prev_date_path = self._get_date_folder_path(self.output_dir, previous_date)
            prev_file = prev_date_path / f"{symbol}_{previous_date}.parquet"
            
            # Check if file exists before attempting to read
            if not prev_file.exists():
                self.logger.debug(f"[{symbol}] Previous trading day file not found: {prev_file}")
                return None
            
            # Additional check: verify it's a file (not a directory)
            if not prev_file.is_file():
                self.logger.debug(f"[{symbol}] Path exists but is not a file: {prev_file}")
                return None
            
            # Read the parquet file
            prev_df = pd.read_parquet(prev_file)
            
            if prev_df is None or len(prev_df) == 0:
                return None
            
            # Get the last bar's closing price
            last_close = prev_df['close'].iloc[-1]
            self.logger.debug(f"[{symbol}] Found previous trading day close: {last_close} from {previous_date}")
            
            return float(last_close)
            
        except Exception as e:
            self.logger.debug(f"[{symbol}] Could not get previous trading day close: {e}")
            return None
    
    def parse_market_data(self, data: Union[str, Dict, List], symbol: str = None) -> Optional[pd.DataFrame]:
        """Parse market data from various formats (JSON, CSV, dict, list)."""
        if not data:
            self.logger.debug(f"[{symbol}] No data received")
            return None
        
        self.logger.debug(f"[{symbol}] Parsing data of type: {type(data)}")
        
        try:
            # Handle JSON array (from MCP)
            if isinstance(data, list):
                self.logger.debug(f"[{symbol}] Detected list with {len(data)} items")
                result = self._parse_json_list(data, symbol)
                
                if result is not None:
                    # ✅ EXTRACT DATE from DataFrame before validation
                    date = result['date'].iloc[0] if 'date' in result.columns and len(result) > 0 else None
                    
                    # Validate data quality with date parameter
                    is_valid, reason = self.validate_data_quality(result, symbol, date)
                    
                    if not is_valid:
                        self.logger.warning(f"[{symbol}] ✗ Data rejected: {reason}")
                        return None
                    
                    self.logger.info(f"[{symbol}] ✓ Parsed {len(result)} records (validated)")
                else:
                    self.logger.warning(f"[{symbol}] Failed to parse JSON list")
                
                return result
            
            # Handle JSON dict
            elif isinstance(data, dict):
                self.logger.debug(f"[{symbol}] Detected dict with keys: {list(data.keys())}")
                # Check if it's a wrapper with data key
                if 'data' in data:
                    result = self._parse_json_list(data['data'], symbol)
                # Check if it contains OHLC fields directly
                elif all(k in data for k in ['open', 'high', 'low', 'close']):
                    result = self._parse_json_list([data], symbol)
                else:
                    self.logger.warning(f"[{symbol}] Unexpected dict format: {list(data.keys())}")
                    return None
                
                # Validate data quality with date
                if result is not None:
                    # ✅ EXTRACT DATE
                    date = result['date'].iloc[0] if 'date' in result.columns and len(result) > 0 else None
                    
                    is_valid, reason = self.validate_data_quality(result, symbol, date)
                    
                    if not is_valid:
                        self.logger.warning(f"[{symbol}] ✗ Data rejected: {reason}")
                        return None
            
                return result
            
            # Handle CSV string
            elif isinstance(data, str):
                self.logger.debug(f"[{symbol}] Detected string, attempting CSV parse")
                # Check if it might be JSON string that wasn't parsed
                if data.strip().startswith('[') or data.strip().startswith('{'):
                    self.logger.warning(f"[{symbol}] String looks like JSON but wasn't parsed: {data[:100]}")
                
                result = self.parse_csv_response(data, symbol)
                
                # Validate data quality with date
                if result is not None:
                    # ✅ EXTRACT DATE
                    date = result['date'].iloc[0] if 'date' in result.columns and len(result) > 0 else None
                    
                    is_valid, reason = self.validate_data_quality(result, symbol, date)
                    
                    if not is_valid:
                        self.logger.warning(f"[{symbol}] ✗ Data rejected: {reason}")
                        return None
            
                return result
            
            else:
                self.logger.error(f"[{symbol}] Unsupported data format: {type(data)}")
                return None
            
        except Exception as e:
            self.logger.error(f"[{symbol}] Error parsing market data: {e}", exc_info=True)
            return None
    
    def _parse_json_list(self, data: List[Dict[str, Any]], symbol: str = None) -> Optional[pd.DataFrame]:
        """
        Parse JSON list of OHLC records into DataFrame.
        
        Args:
            data: List of OHLC dictionaries
            symbol: Stock symbol
            
        Returns:
            DataFrame with parsed data
        """
        if not data or len(data) == 0:
            return None
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Check if dataframe is empty
            if len(df) == 0:
                return None
            
            # Add symbol column if provided
            if symbol:
                df['symbol'] = symbol
            
            # Standardize column names (lowercase)
            df.columns = [col.lower() for col in df.columns]
            
            # Verify required columns exist
            required_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                self.logger.error(f"Missing columns for {symbol}: {missing_cols}")
                return None
            
            # Convert data types
            df = self._convert_types_json(df)
            
            # Parse timestamps (ISO format from MCP)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Extract date and ms_of_day from timestamp for compatibility
            df['date'] = df['timestamp'].dt.strftime('%Y%m%d')
            df['ms_of_day'] = (
                df['timestamp'].dt.hour * 3600000 +
                df['timestamp'].dt.minute * 60000 +
                df['timestamp'].dt.second * 1000 +
                df['timestamp'].dt.microsecond // 1000
            ).astype('Int64')
            
            self.logger.debug(f"[{symbol}] Added date and ms_of_day columns from timestamp")
            
            # Fill missing bars with extrapolation
            df = self._fill_missing_bars(df, symbol)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error parsing JSON list for {symbol}: {e}", exc_info=True)
            return None
    
    def _fill_missing_bars(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        Fill missing minute bars with extrapolated data.

        Creates bars with:
        - Volume = 0
        - Count = 0 (if column exists)
        - OHLC = previous bar's close price
        - VWAP = previous bar's close price (if column exists)

        Args:
            df: DataFrame with existing bars
            symbol: Stock symbol for logging
        
        Returns:
            DataFrame with missing bars filled
        """
        if df is None or len(df) == 0:
            return df

        try:
            # Sort by timestamp
            df = df.sort_values('timestamp').reset_index(drop=True)
        
            # Get the date from the first record
            first_timestamp = df['timestamp'].iloc[0]
            trade_date = df['date'].iloc[0]
        
            # Define FULL trading session (4:00 AM to 8:00 PM ET)
            session_start = first_timestamp.replace(hour=4, minute=0, second=0, microsecond=0)
            session_end = first_timestamp.replace(hour=20, minute=0, second=0, microsecond=0)
        
            # Get previous day's close for proper gap filling
            previous_close_price = self._get_previous_day_close(symbol, trade_date)
        
            if previous_close_price is None:
                # Use first bar's open as fallback
                previous_close_price = df['open'].iloc[0]
                self.logger.debug(f"[{symbol}] Using first bar's open price ({previous_close_price}) for gap filling")
            else:
                self.logger.debug(f"[{symbol}] Using previous day's close ({previous_close_price}) for gap filling")
        
            # Create complete minute range for FULL session (4 AM - 8 PM)
            complete_range = pd.date_range(
                start=session_start,
                end=session_end,
                freq='1min'
            )
        
            # Verify we're creating 961 bars
            expected_bars = 16 * 60 + 1  # 961 bars
            if len(complete_range) != expected_bars:
                self.logger.warning(f"[{symbol}] Expected {expected_bars} bars, generated {len(complete_range)}")
        
            # Create a complete dataframe with all minute timestamps
            complete_df = pd.DataFrame({'timestamp': complete_range})
        
            # Merge with existing data - mark which bars are new
            df = complete_df.merge(df, on='timestamp', how='left', indicator=True)
        
            # Identify bars that need filling (new bars only)
            missing_mask = df['_merge'] == 'left_only'
        
            if missing_mask.sum() > 0:
                self.logger.debug(f"[{symbol}] Filling {missing_mask.sum()} missing bars")
                
                # Fill missing OHLC and VWAP values
                df.loc[missing_mask, 'open'] = previous_close_price
                df.loc[missing_mask, 'high'] = previous_close_price
                df.loc[missing_mask, 'low'] = previous_close_price
                df.loc[missing_mask, 'close'] = previous_close_price
                
                # Fill missing volume as 0
                df.loc[missing_mask, 'volume'] = 0
                
                # If 'count' column exists, fill with 0
                if 'count' in df.columns:
                    df.loc[missing_mask, 'count'] = 0
                
                # If 'vwap' column exists, fill with previous close price
                if 'vwap' in df.columns:
                    df.loc[missing_mask, 'vwap'] = previous_close_price
            
            # Drop the merge indicator column
            df = df.drop(columns=['_merge'])
        
            return df
            
        except Exception as e:
            self.logger.error(f"Error filling missing bars for {symbol}: {e}", exc_info=True)
            return df
    
    def parse_csv_response(self, raw_csv: str, symbol: str) -> Optional[pd.DataFrame]:
        """Parse CSV response from ThetaData API."""
        if not raw_csv or len(raw_csv.strip()) == 0:
            return None
        
        try:
            # Parse CSV
            df = pd.read_csv(io.StringIO(raw_csv))
            
            # Check if dataframe is empty
            if len(df) == 0:
                return None
            
            # Add symbol column if provided
            if symbol:
                df['symbol'] = symbol
            
            # Standardize column names (lowercase)
            df.columns = [col.lower() for col in df.columns]
            
            # Verify required columns exist
            required_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                self.logger.error(f"Missing columns for {symbol}: {missing_cols}")
                return None
            
            # Convert data types
            df = self._convert_types_csv(df)
            
            # Parse timestamps (UTC format)
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S', utc=True)
            
            # Convert to user's local time zone
            # df['timestamp'] = df['timestamp'].dt.tz_convert('America/New_York')
            
            # Extract date and ms_of_day from timestamp for compatibility
            df['date'] = df['timestamp'].dt.strftime('%Y%m%d')
            df['ms_of_day'] = (
                df['timestamp'].dt.hour * 3600000 +
                df['timestamp'].dt.minute * 60000 +
                df['timestamp'].dt.second * 1000 +
                df['timestamp'].dt.microsecond // 1000
            ).astype('Int64')
            
            self.logger.debug(f"[{symbol}] Added date and ms_of_day columns from timestamp")
            
            # Fill missing bars
            df = self._fill_missing_bars(df, symbol)
            
            # ✅ EXTRACT DATE before validation
            date = df['date'].iloc[0] if 'date' in df.columns and len(df) > 0 else None
            
            # Validate data quality WITH DATE
            is_valid, reason = self.validate_data_quality(df, symbol, date)
            
            if not is_valid:
                self.logger.warning(f"[{symbol}] ✗ CSV data rejected: {reason}")
                return None
            
            return df

        except Exception as e:
            self.logger.error(f"Error parsing CSV response for {symbol}: {e}", exc_info=True)
            return None
    
    def _convert_types_json(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert column data types for JSON parsed DataFrame.
        
        Args:
            df: DataFrame with parsed JSON data
            
        Returns:
            DataFrame with converted types
        """
        try:
            df['open'] = df['open'].astype(float)
            df['high'] = df['high'].astype(float)
            df['low'] = df['low'].astype(float)
            df['close'] = df['close'].astype(float)
            df['volume'] = df['volume'].astype(float)
            
            # Debug output for verification
            self.logger.debug(f"Converted types for JSON data: {df.dtypes.to_dict()}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error converting types for JSON data: {e}", exc_info=True)
            return df
    
    def _convert_types_csv(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert column data types for CSV parsed DataFrame.
        
        Args:
            df: DataFrame with parsed CSV data
            
        Returns:
            DataFrame with converted types
        """
        try:
            df['open'] = df['open'].astype(float)
            df['high'] = df['high'].astype(float)
            df['low'] = df['low'].astype(float)
            df['close'] = df['close'].astype(float)
            df['volume'] = df['volume'].astype(float)
            
            # Debug output for verification
            self.logger.debug(f"Converted types for CSV data: {df.dtypes.to_dict()}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error converting types for CSV data: {e}", exc_info=True)
            return df
    
    def validate_data_quality(self, df: pd.DataFrame, symbol: str, date: Optional[str] = None) -> tuple[bool, str]:
        """
        Validate the quality of the parsed data.
        
        Args:
            df: DataFrame with parsed data
            symbol: Stock symbol
            date: Date string in YYYYMMDD format (optional)
        
        Returns:
            Tuple of (is_valid: bool, reason: str)
        """
        if df is None or len(df) == 0:
            reason = "Empty dataframe"
            
            # Log to null data tracker if date is provided
            if self.null_tracker and date:
                self.null_tracker.add_null_symbol(symbol, date, reason)
            
            return False, reason

        # Check for required columns
        required_cols = ['open', 'high', 'low', 'close']
        missing_cols = [col for col in required_cols if col not in df.columns]

        if missing_cols:
            reason = f"Missing columns: {', '.join(missing_cols)}"
            
            if self.null_tracker and date:
                self.null_tracker.add_null_symbol(symbol, date, reason)
            
            return False, reason

        ohlc_cols = ['open', 'high', 'low', 'close']

        # Check if all OHLC values are zero or null
        all_ohlc_zero = all(
            (df[col] == 0).all() or df[col].isna().all()
            for col in ohlc_cols
        )

        if all_ohlc_zero:
            reason = f"All OHLC zeros ({len(df)} bars)"
            
            if self.null_tracker and date:
                self.null_tracker.add_null_symbol(symbol, date, reason)
            
            return False, reason

        # Check if all values are null
        all_null = all(df[col].isna().all() for col in ohlc_cols)

        if all_null:
            reason = f"All OHLC null ({len(df)} bars)"
            
            if self.null_tracker and date:
                self.null_tracker.add_null_symbol(symbol, date, reason)
            
            return False, reason

        # Check for constant price (no price movement)
        constant_price = (
            (df['open'] == df['high']).all() and
            (df['high'] == df['low']).all() and
            (df['low'] == df['close']).all()
        )

        if constant_price:
            price = df['close'].iloc[0]
        
            has_volume = 'volume' in df.columns
            all_zero_volume = False
        
            if has_volume:
                all_zero_volume = (df['volume'] == 0).all() or df['volume'].isna().all()
        
            if price == 0.0:
                reason = f"Constant price at $0.00 ({len(df)} bars)"
                
                if self.null_tracker and date:
                    self.null_tracker.add_null_symbol(symbol, date, reason)
                
                return False, reason
            elif all_zero_volume:
                reason = f"Zero volume + constant price ${price:.2f} ({len(df)} bars)"
                
                if self.null_tracker and date:
                    self.null_tracker.add_null_symbol(symbol, date, reason)
                
                return False, reason

        # Check for zero volume throughout
        if 'volume' in df.columns:
            all_zero_volume = (df['volume'] == 0).all() or df['volume'].isna().all()
        
            if all_zero_volume:
                all_prices_zero = all(
                    (df[col] == 0).all() for col in ohlc_cols
                )
            
                if all_prices_zero:
                    reason = f"Zero volume + all prices zero ({len(df)} bars)"
                    
                    if self.null_tracker and date:
                        self.null_tracker.add_null_symbol(symbol, date, reason)
                    
                    return False, reason

        # Check for negative prices
        negative_prices = any((df[col] < 0).any() for col in ohlc_cols if col in df.columns)
        if negative_prices:
            reason = "Negative prices detected"
            
            if self.null_tracker and date:
                self.null_tracker.add_null_symbol(symbol, date, reason)
            
            return False, reason

        # Check for OHLC relationship violations
        violations = (
            (df['high'] < df['low']).any() or
            (df['high'] < df['open']).any() or
            (df['high'] < df['close']).any() or
            (df['low'] > df['open']).any() or
            (df['low'] > df['close']).any()
        )

        if violations:
            reason = "OHLC violations (high<low, etc.)"
            
            if self.null_tracker and date:
                self.null_tracker.add_null_symbol(symbol, date, reason)
            
            return False, reason

        # All checks passed
        return True, "Valid data"