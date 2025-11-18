"""
Inspect a specific parquet file to diagnose data quality issues.
"""
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import Config


def inspect_file(symbol: str, date: str, base_dir: str):
    """
    Inspect a specific symbol/date file in detail.
    
    Args:
        symbol: Stock symbol (e.g., 'ABTC')
        date: Date in YYYYMMDD format (e.g., '20240102')
        base_dir: Base directory containing parquet files
    """
    # Construct file path using YYYY/MM/DD structure
    year = date[:4]
    month = date[4:6]
    day = date[6:8]
    
    file_path = Path(base_dir) / year / month / day / f"{symbol}_{date}.parquet"
    
    print("=" * 80)
    print(f"INSPECTING FILE: {symbol} on {date}")
    print("=" * 80)
    print(f"File path: {file_path}")
    print()
    
    if not file_path.exists():
        print(f"❌ File does not exist: {file_path}")
        return
    
    # Read the file
    try:
        df = pd.read_parquet(file_path)
        print(f"✓ File loaded successfully")
        print(f"  Rows: {len(df)}")
        print(f"  Columns: {list(df.columns)}")
        print()
        
        # Basic statistics
        print("=" * 80)
        print("BASIC STATISTICS")
        print("=" * 80)
        
        if 'volume' in df.columns:
            print(f"Volume statistics:")
            print(f"  Total volume: {df['volume'].sum():,.0f}")
            print(f"  Non-zero bars: {(df['volume'] > 0).sum()}")
            print(f"  Zero bars: {(df['volume'] == 0).sum()}")
            print(f"  Null bars: {df['volume'].isna().sum()}")
            print(f"  Min volume: {df['volume'].min()}")
            print(f"  Max volume: {df['volume'].max()}")
            print(f"  Mean volume: {df['volume'].mean():.2f}")
            print()
        
        print(f"Price statistics:")
        for col in ['open', 'high', 'low', 'close']:
            if col in df.columns:
                print(f"  {col.capitalize()}:")
                print(f"    Min: ${df[col].min():.4f}")
                print(f"    Max: ${df[col].max():.4f}")
                print(f"    Mean: ${df[col].mean():.4f}")
                print(f"    Zeros: {(df[col] == 0).sum()}")
                print(f"    Nulls: {df[col].isna().sum()}")
        print()
        
        # Show first 20 bars with volume > 0
        print("=" * 80)
        print("BARS WITH VOLUME > 0 (First 20)")
        print("=" * 80)
        
        if 'volume' in df.columns:
            volume_bars = df[df['volume'] > 0]
            if len(volume_bars) > 0:
                print(f"Found {len(volume_bars)} bars with volume")
                print()
                display_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                available_cols = [col for col in display_cols if col in volume_bars.columns]
                print(volume_bars[available_cols].head(20).to_string())
            else:
                print("❌ No bars with volume > 0 found!")
        print()
        
        # Show first 10 bars overall
        print("=" * 80)
        print("FIRST 10 BARS (Overall)")
        print("=" * 80)
        display_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        available_cols = [col for col in display_cols if col in df.columns]
        print(df[available_cols].head(10).to_string())
        print()
        
        # Show last 10 bars overall
        print("=" * 80)
        print("LAST 10 BARS (Overall)")
        print("=" * 80)
        print(df[available_cols].tail(10).to_string())
        print()
        
        # Check for filled bars (where volume is 0 but prices exist)
        print("=" * 80)
        print("FILLED BARS ANALYSIS")
        print("=" * 80)
        
        if 'volume' in df.columns:
            filled_bars = df[(df['volume'] == 0) & (df['close'] > 0)]
            print(f"Bars with zero volume but non-zero prices: {len(filled_bars)}")
            
            if len(filled_bars) > 0:
                print("\nThese are likely gap-filled bars (market closed or no trading).")
                print("Sample of filled bars:")
                print(filled_bars[available_cols].head(10).to_string())
        print()
        
        # Time range analysis
        print("=" * 80)
        print("TIME RANGE ANALYSIS")
        print("=" * 80)
        
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            print(f"First timestamp: {df['timestamp'].min()}")
            print(f"Last timestamp:  {df['timestamp'].max()}")
            print(f"Time range: {(df['timestamp'].max() - df['timestamp'].min()).total_seconds() / 3600:.2f} hours")
            
            # Count bars by hour
            df['hour'] = df['timestamp'].dt.hour
            bars_by_hour = df.groupby('hour').size()
            print("\nBars per hour:")
            for hour, count in bars_by_hour.items():
                print(f"  {hour:02d}:00 - {count} bars")
            
            # Volume by hour
            if 'volume' in df.columns:
                volume_by_hour = df.groupby('hour')['volume'].sum()
                print("\nVolume per hour:")
                for hour, vol in volume_by_hour.items():
                    if vol > 0:
                        print(f"  {hour:02d}:00 - {vol:,.0f}")
        
        print()
        print("=" * 80)
        
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        import traceback
        traceback.print_exc()


def main():
    """Main entry point."""
    config = Config()
    
    # Default: inspect ABTC on 2024-01-02
    symbol = "ABTC"
    date = "20240102"
    
    # Allow command line arguments
    if len(sys.argv) >= 2:
        symbol = sys.argv[1].upper()
    if len(sys.argv) >= 3:
        date = sys.argv[2]
    
    inspect_file(symbol, date, config.OUTPUT_DIR)


if __name__ == "__main__":
    main()
