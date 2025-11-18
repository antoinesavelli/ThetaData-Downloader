"""
Check if a specific symbol was downloaded correctly by comparing with ThetaData API.
"""
import asyncio
import pandas as pd
import sys
from pathlib import Path
import json

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import Config
from connectors.thetadata_api import ThetaDataAPI
from processors.data_parser import DataParser
from utils.logger import setup_logger


async def check_symbol_data(symbol: str, date: str):
    """
    Check if symbol data was downloaded correctly.
    
    Args:
        symbol: Stock symbol
        date: Date in YYYYMMDD format
    """
    config = Config()
    logger = setup_logger(
        name="check_download",
        log_file="logs/check_download.log",
        log_level="INFO"
    )
    
    print("=" * 80)
    print(f"CHECKING DOWNLOAD: {symbol} on {date}")
    print("=" * 80)
    print()
    
    # 1. Check the saved file
    year = date[:4]
    month = date[4:6]
    day = date[6:8]
    
    file_path = Path(config.OUTPUT_DIR) / year / month / day / f"{symbol}_{date}.parquet"
    
    print(f"Saved file: {file_path}")
    print(f"File exists: {file_path.exists()}")
    print()
    
    df_saved = None
    if file_path.exists():
        df_saved = pd.read_parquet(file_path)
        print("=" * 80)
        print("SAVED FILE DATA:")
        print("=" * 80)
        print(f"  Total rows: {len(df_saved)}")
        
        if 'volume' in df_saved.columns:
            total_volume = df_saved['volume'].sum()
            non_zero_bars = (df_saved['volume'] > 0).sum()
            zero_bars = (df_saved['volume'] == 0).sum()
            
            print(f"  Total volume: {total_volume:,.0f}")
            print(f"  Non-zero volume bars: {non_zero_bars}")
            print(f"  Zero volume bars: {zero_bars}")
            print(f"  Volume percentage: {(non_zero_bars/len(df_saved)*100):.1f}%")
        
        if 'close' in df_saved.columns:
            print(f"  Price range: ${df_saved['close'].min():.2f} - ${df_saved['close'].max():.2f}")
        
        # Show first 5 bars with volume
        if 'volume' in df_saved.columns:
            volume_bars = df_saved[df_saved['volume'] > 0]
            if len(volume_bars) > 0:
                print(f"\n  First 5 bars WITH volume (from saved file):")
                display_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                available_cols = [col for col in display_cols if col in volume_bars.columns]
                for idx, row in volume_bars[available_cols].head(5).iterrows():
                    print(f"    {row['timestamp']}: Close=${row['close']:.2f}, Vol={row['volume']:,}")
        print()
    
    # 2. Fetch fresh data from API
    print("=" * 80)
    print("FETCHING FRESH DATA FROM API")
    print("=" * 80)
    api = ThetaDataAPI(config, logger)
    
    try:
        # Check if terminal is running
        if not await api.check_terminal_running():
            print("❌ ThetaData Terminal is not running!")
            return
        
        # Fetch raw data
        print(f"Requesting: {symbol} for {date} from {config.START_TIME} to {config.END_TIME}")
        data = await api.fetch_market_data(
            symbol,
            date,
            start_time=config.START_TIME,
            end_time=config.END_TIME
        )
        
        if not data:
            print("❌ No data returned from API!")
            return
        
        print(f"✓ Data fetched from API")
        print(f"  Data type: {type(data)}")
        
        # ========================================================================
        # BEFORE PARSING - RAW JSON DATA
        # ========================================================================
        print()
        print("=" * 80)
        print("RAW JSON DATA (BEFORE PARSING)")
        print("=" * 80)
        
        if isinstance(data, list):
            print(f"  Total records received: {len(data)}")
            
            # Analyze volume in raw API data
            if len(data) > 0 and isinstance(data[0], dict):
                volumes = [record.get('volume', 0) for record in data]
                total_volume_raw = sum(volumes)
                non_zero_volume_raw = sum(1 for v in volumes if v > 0)
                
                print(f"  Total volume (raw): {total_volume_raw:,.0f}")
                print(f"  Non-zero volume bars (raw): {non_zero_volume_raw}")
                print(f"  Zero volume bars (raw): {len(volumes) - non_zero_volume_raw}")
                
                # Show first 10 raw records
                print(f"\n  First 10 RAW records from API:")
                for i, record in enumerate(data[:10]):
                    vol = record.get('volume', 0)
                    close_price = record.get('close', 0)
                    timestamp = record.get('timestamp', 'N/A')
                    print(f"    {i+1}. {timestamp}: Close=${close_price:.4f}, Vol={vol:,}")
                
                # Show first 10 records WITH volume
                print(f"\n  First 10 RAW records WITH volume > 0:")
                count = 0
                for record in data:
                    if record.get('volume', 0) > 0:
                        vol = record.get('volume', 0)
                        close_price = record.get('close', 0)
                        timestamp = record.get('timestamp', 'N/A')
                        print(f"    {timestamp}: Close=${close_price:.4f}, Vol={vol:,}")
                        count += 1
                        if count >= 10:
                            break
                
                if count == 0:
                    print("    ❌ No records with volume > 0 found in raw API data!")
                
                # Show a sample of the raw JSON structure
                print(f"\n  Sample raw record structure (first record):")
                sample_record = data[0]
                print(f"    Keys: {list(sample_record.keys())}")
                print(f"    Full record: {json.dumps(sample_record, indent=4)}")
        
        # ========================================================================
        # AFTER PARSING - PARSED DATAFRAME
        # ========================================================================
        print()
        print("=" * 80)
        print("PARSED DATA (AFTER PARSING)")
        print("=" * 80)
        
        # Parse the data using DataParser
        parser = DataParser(logger, output_dir=config.OUTPUT_DIR)
        
        # Log parsing step by step
        print("  Step 1: Creating DataFrame from raw data...")
        if isinstance(data, list) and len(data) > 0:
            df_before_fill = pd.DataFrame(data)
            df_before_fill.columns = [col.lower() for col in df_before_fill.columns]
            
            print(f"    Rows before gap-filling: {len(df_before_fill)}")
            if 'volume' in df_before_fill.columns:
                vol_sum_before = df_before_fill['volume'].sum()
                non_zero_before = (df_before_fill['volume'] > 0).sum()
                print(f"    Total volume (before fill): {vol_sum_before:,.0f}")
                print(f"    Non-zero volume bars (before fill): {non_zero_before}")
                
                # Show first 5 records with volume before gap-filling
                print(f"\n    First 5 records WITH volume (before gap-filling):")
                volume_records = df_before_fill[df_before_fill['volume'] > 0]
                for idx, row in volume_records.head(5).iterrows():
                    print(f"      {row.get('timestamp', 'N/A')}: Close={row['close']:.4f}, Vol={row['volume']:,}")
        
        print("\n  Step 2: Running full parser (with gap-filling)...")
        df_parsed = parser.parse_market_data(data, symbol)
        
        if df_parsed is not None:
            print(f"    ✓ Parsing successful")
            print(f"    Total rows (after fill): {len(df_parsed)}")
            
            if 'volume' in df_parsed.columns:
                vol_sum_after = df_parsed['volume'].sum()
                non_zero_after = (df_parsed['volume'] > 0).sum()
                zero_after = (df_parsed['volume'] == 0).sum()
                
                print(f"    Total volume (after fill): {vol_sum_after:,.0f}")
                print(f"    Non-zero volume bars (after fill): {non_zero_after}")
                print(f"    Zero volume bars (after fill): {zero_after}")
                print(f"    Volume percentage: {(non_zero_after/len(df_parsed)*100):.1f}%")
                
                # Show first 5 bars with volume after parsing
                print(f"\n    First 5 bars WITH volume (after parsing):")
                volume_bars_parsed = df_parsed[df_parsed['volume'] > 0]
                if len(volume_bars_parsed) > 0:
                    display_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                    available_cols = [col for col in display_cols if col in volume_bars_parsed.columns]
                    for idx, row in volume_bars_parsed[available_cols].head(5).iterrows():
                        print(f"      {row['timestamp']}: Close=${row['close']:.4f}, Vol={row['volume']:,}")
                else:
                    print("      ❌ No bars with volume after parsing!")
                
                # Show time range
                if 'timestamp' in df_parsed.columns:
                    df_parsed['timestamp'] = pd.to_datetime(df_parsed['timestamp'])
                    print(f"\n    Time range:")
                    print(f"      First: {df_parsed['timestamp'].min()}")
                    print(f"      Last:  {df_parsed['timestamp'].max()}")
        else:
            print("    ❌ Parsing returned None!")
        
        # ========================================================================
        # COMPARISON
        # ========================================================================
        print()
        print("=" * 80)
        print("COMPARISON SUMMARY")
        print("=" * 80)
        
        if isinstance(data, list) and df_parsed is not None:
            # Volume comparison
            raw_total_volume = sum(record.get('volume', 0) for record in data)
            parsed_total_volume = df_parsed['volume'].sum() if 'volume' in df_parsed.columns else 0
            
            print(f"Raw API total volume:     {raw_total_volume:,}")
            print(f"Parsed total volume:      {parsed_total_volume:,}")
            
            if df_saved is not None:
                saved_total_volume = df_saved['volume'].sum() if 'volume' in df_saved.columns else 0
                print(f"Saved file total volume:  {saved_total_volume:,}")
            
            # Bar count comparison
            print(f"\nBar counts:")
            print(f"  Raw records from API:     {len(data)}")
            print(f"  Parsed (after fill):      {len(df_parsed)}")
            if df_saved is not None:
                print(f"  Saved file:               {len(df_saved)}")
            
            # Diagnose the issue
            print()
            if raw_total_volume > 0 and parsed_total_volume == 0:
                print("❌ CRITICAL ISSUE: Raw API has volume but parsed data has ZERO volume!")
                print("   → Problem is in the parsing/gap-filling logic")
            elif raw_total_volume > 0 and parsed_total_volume > 0 and parsed_total_volume < raw_total_volume:
                diff = raw_total_volume - parsed_total_volume
                pct_loss = (diff / raw_total_volume * 100)
                print(f"⚠️  WARNING: Volume lost during parsing!")
                print(f"   → Lost: {diff:,} ({pct_loss:.1f}%)")
            elif raw_total_volume == 0:
                print("ℹ️  API returned zero volume - no trading activity on this day")
            elif raw_total_volume == parsed_total_volume:
                print("✓ Volume preserved correctly during parsing")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await api.close()
    
    print()
    print("=" * 80)


def main():
    """Main entry point."""
    symbol = "ABTC"
    date = "20240102"
    
    # Allow command line arguments
    if len(sys.argv) >= 2:
        symbol = sys.argv[1].upper()
    if len(sys.argv) >= 3:
        date = sys.argv[2]
    
    asyncio.run(check_symbol_data(symbol, date))


if __name__ == "__main__":
    main()
