"""
Test script for consolidated daily download mode.
Downloads 2-3 days of data to verify the system works correctly.
"""
import asyncio
import pandas as pd
from pathlib import Path
import sys

# Add parent directory to path so we can import from config, connectors, etc.
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import Config
from connectors.thetadata_api import ThetaDataAPI
from processors.data_parser import DataParser
from storage.parquet_writer import ParquetWriter
from downloaders.coordinator import DownloadCoordinator
from utils.logger import setup_logger


async def test_consolidated_download():
    """Test consolidated daily download with a small sample."""
    
    print("=" * 60)
    print("TESTING CONSOLIDATED DAILY DOWNLOAD")
    print("=" * 60)
    
    # Initialize with test configuration
    config = Config()
    config.START_DATE = "20240529"
    config.END_DATE = "20240531"  # Just 3 days for testing
    config.OUTPUT_DIR = "D:/trading_data_test"  # Test directory
    config.validate()
    
    # Setup logger
    logger = setup_logger('test_consolidated', 'logs/test_consolidated.log', 'INFO')
    
    # Load symbols (limit to 10 for testing)
    symbols_df = pd.read_csv(config.SYMBOL_FILE)
    if 'symbol' in symbols_df.columns:
        all_symbols = symbols_df['symbol'].tolist()
    else:
        all_symbols = symbols_df.iloc[:, 0].tolist()
    
    # Use only first 10 symbols for testing
    test_symbols = [str(s).strip().upper() for s in all_symbols[:10] if pd.notna(s)]
    
    print(f"\nTest configuration:")
    print(f"  Dates: {config.START_DATE} to {config.END_DATE}")
    print(f"  Symbols: {len(test_symbols)} ({test_symbols})")
    print(f"  Output: {config.OUTPUT_DIR}")
    print(f"  Mode: Consolidated (one file per day)")
    print()
    
    # Initialize components
    api = ThetaDataAPI(config, logger)
    parser = DataParser(logger, output_dir=config.OUTPUT_DIR)
    writer = ParquetWriter(config, logger)
    coordinator = DownloadCoordinator(config, api, parser, writer, logger)
    
    # Check connection
    print("Checking ThetaData Terminal connection...")
    if not await api.check_terminal_running():
        print("❌ ThetaData Terminal not accessible")
        return
    
    print("✓ Connection verified\n")
    
    # Run download
    try:
        print("Starting download...\n")
        summary = await coordinator.download_date_range(
            start_date=config.START_DATE,
            end_date=config.END_DATE,
            symbols=test_symbols,
            output_dir=config.OUTPUT_DIR
        )
        
        # Display results
        print("\n" + "=" * 60)
        print("TEST RESULTS")
        print("=" * 60)
        print(f"Dates processed: {summary['dates_processed']}")
        print(f"Dates successful: {summary['dates_successful']}")
        print(f"Dates failed: {summary['dates_failed']}")
        print(f"Total records: {summary['total_records']:,}")
        print(f"Total file size: {summary['total_file_size_mb']:.2f} MB")
        print()
        
        # List created files
        output_path = Path(config.OUTPUT_DIR)
        parquet_files = sorted(output_path.rglob("*.parquet"))
        
        if parquet_files:
            print(f"Files created ({len(parquet_files)}):")
            for f in parquet_files:
                file_size = f.stat().st_size / (1024*1024)
                rel_path = f.relative_to(output_path)
                print(f"  {rel_path} ({file_size:.2f} MB)")
                
                # Read and show sample data
                df = pd.read_parquet(f)
                print(f"    - {len(df):,} rows, {df['symbol'].nunique()} symbols")
                print(f"    - Symbols: {sorted(df['symbol'].unique())}")
                print()
        else:
            print("❌ No files were created")
        
        print("=" * 60)
        print("✓ Test complete!")
        print("=" * 60)
        
    finally:
        await api.close()


if __name__ == "__main__":
    asyncio.run(test_consolidated_download())