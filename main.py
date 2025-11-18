"""
Main entry point for ThetaData pipeline - CONSOLIDATED DAILY MODE.
Creates ONE Parquet file per day containing ALL symbols.
"""
import asyncio
import logging
from pathlib import Path
from config.settings import Config
from connectors.thetadata_api import ThetaDataAPI
from processors.data_parser import DataParser
from storage.parquet_writer import ParquetWriter
from downloaders.coordinator import DownloadCoordinator
from utils.logger import setup_logger
from utils.diagnostics import ConnectionDiagnostics
import pandas as pd


async def main():
    """Main execution function."""
    # Initialize configuration
    config = Config()
    
    # Validate configuration
    try:
        config.validate()
    except Exception as e:
        print(f"Configuration validation failed: {e}")
        return
    
    # Setup logging
    logger = setup_logger(
        'thetadata_pipeline',
        config.LOG_FILE,
        config.LOG_LEVEL
    )
    
    logger.info("="*80)
    logger.info("ThetaData Pipeline - CONSOLIDATED DAILY MODE")
    logger.info("="*80)
    logger.info(f"Mode: ONE parquet file per day with ALL symbols")
    logger.info(f"Folder structure: YYYY/MM/YYYYMMDD.parquet")
    logger.info(f"Date Range: {config.START_DATE} to {config.END_DATE}")
    logger.info(f"Time Range: {config.START_TIME} to {config.END_TIME}")
    logger.info(f"  ⏰ Premarket: 04:00:00 - 09:30:00")
    logger.info(f"  📈 Regular Hours: 09:30:00 - 16:00:00")
    logger.info(f"  🌙 After Hours: 16:00:00 - 20:00:00")
    logger.info(f"Output Directory: {config.OUTPUT_DIR}")
    logger.info(f"Symbol File: {config.SYMBOL_FILE}")
    logger.info(f"Max Concurrent Requests: {config.MAX_CONCURRENT_REQUESTS}")
    
    # Load symbols from CSV
    try:
        symbols_df = pd.read_csv(config.SYMBOL_FILE)
        
        # Handle different CSV formats
        if 'symbol' in symbols_df.columns:
            symbols = symbols_df['symbol'].tolist()
        elif 'Symbol' in symbols_df.columns:
            symbols = symbols_df['Symbol'].tolist()
        else:
            # Assume first column contains symbols
            symbols = symbols_df.iloc[:, 0].tolist()
        
        # Clean symbols (remove whitespace, convert to uppercase)
        symbols = [str(s).strip().upper() for s in symbols if pd.notna(s)]
        
        logger.info(f"Loaded {len(symbols)} symbols from {config.SYMBOL_FILE}")
        logger.info(f"First 10 symbols: {symbols[:10]}")
        
    except Exception as e:
        logger.error(f"Failed to load symbols: {e}")
        return
    
    # Initialize components
    api = ThetaDataAPI(config, logger)
    parser = DataParser(logger, output_dir=config.OUTPUT_DIR)
    writer = ParquetWriter(config, logger)
    coordinator = DownloadCoordinator(config, api, parser, writer, logger)
    
    # Check terminal connection
    logger.info("\nChecking ThetaData Terminal connection...")
    diagnostics = ConnectionDiagnostics(logger)
    
    if not await api.check_terminal_running():
        logger.error("❌ ThetaData Terminal is not running or not accessible")
        logger.error("Please ensure:")
        logger.error("  1. Theta Terminal is running")
        logger.error("  2. MCP server is enabled on port 25503")
        logger.error("  3. Terminal is properly authenticated")
        return
    
    logger.info("✓ ThetaData Terminal connection verified")
    
    # Download data in consolidated mode
    logger.info(f"\nStarting CONSOLIDATED download...")
    logger.info(f"Download strategy:")
    logger.info(f"  1. Process one date at a time")
    logger.info(f"  2. Download all {len(symbols)} symbols concurrently for each date")
    logger.info(f"  3. Combine into ONE parquet file per date")
    logger.info(f"  4. Skip dates with existing files")
    
    try:
        summary = await coordinator.download_date_range(
            start_date=config.START_DATE,
            end_date=config.END_DATE,
            symbols=symbols,
            output_dir=config.OUTPUT_DIR
        )
        
        # Print summary
        logger.info("\n" + "="*80)
        logger.info("Download Summary:")
        logger.info(f"Dates processed: {summary['dates_processed']}")
        logger.info(f"Dates successful: {summary['dates_successful']}")
        logger.info(f"Dates failed: {summary['dates_failed']}")
        logger.info(f"Total symbols downloaded: {summary['total_symbols_downloaded']}")
        logger.info(f"Total records: {summary['total_records']:,}")
        logger.info(f"Total file size: {summary['total_file_size_mb']:.2f} MB")
        logger.info(f"Files created: {summary['dates_successful']}")
        logger.info("="*80)
        
        # List output files
        output_path = Path(config.OUTPUT_DIR)
        parquet_files = list(output_path.rglob("*.parquet"))
        logger.info(f"\nTotal parquet files in output directory: {len(parquet_files)}")
        
        if parquet_files:
            total_size = sum(f.stat().st_size for f in parquet_files)
            logger.info(f"Total data size: {total_size / (1024*1024):.2f} MB")
            
            # Show sample files
            logger.info("\nSample files created:")
            for f in sorted(parquet_files)[:5]:
                file_size = f.stat().st_size / (1024*1024)
                logger.info(f"  {f.relative_to(output_path)} ({file_size:.2f} MB)")
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Download interrupted by user")
    except Exception as e:
        logger.error(f"Download failed: {e}", exc_info=True)
    finally:
        # Cleanup
        await api.close()
        logger.info("\n✓ Pipeline completed")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user")
    except Exception as e:
        print(f"\n\nFatal error: {e}")
    
    input("\nPress any key to exit...")