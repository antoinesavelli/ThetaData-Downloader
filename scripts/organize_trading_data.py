"""
Script to organize existing trading data files into hierarchical structure.
Run this once to reorganize your D:\trading_data folder.
"""
import logging
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path to import modules
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Clear all cached modules to ensure fresh imports
modules_to_clear = [
    'utils.logger',
    'utils.trading_calendar', 
    'storage.parquet_writer',
    'config.settings'
]

for module_name in modules_to_clear:
    if module_name in sys.modules:
        del sys.modules[module_name]

from config.settings import Config
from storage.parquet_writer import ParquetWriter
from utils.logger import setup_logger


def main():
    """Main function to organize trading data."""
    # Setup
    config = Config()
    
    # Create log file for this organization run
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"organize_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logger = setup_logger(
        name="organize_data",
        log_file=str(log_file),
        log_level="INFO"
    )
    
    writer = ParquetWriter(config, logger)
    
    # Configuration
    data_dir = r"D:\trading_data"
    
    logger.info("=" * 60)
    logger.info("Trading Data Organization Tool")
    logger.info("=" * 60)
    logger.info(f"Target directory: {data_dir}")
    logger.info(f"Log file: {log_file}")
    logger.info("")
    
    # Execute organization immediately
    logger.info("Organizing files...")
    logger.info("-" * 60)
    stats = writer.organize_existing_files(data_dir, dry_run=False)
    
    if stats.get("error"):
        logger.error(f"Error: {stats['error']}")
        return
    
    # Validation
    logger.info("\nValidating organized structure...")
    logger.info("-" * 60)
    validation_stats = writer.validate_organized_structure(data_dir)
    
    # Final summary
    logger.info("\n" + "=" * 60)
    logger.info("ORGANIZATION COMPLETE!")
    logger.info("=" * 60)
    logger.info(f"✓ Moved {stats['files_moved']} files")
    logger.info(f"✓ Deleted {stats['files_deleted_non_trading']} non-trading day files")
    logger.info(f"✓ Organized into {validation_stats['unique_dates']} date folders")
    logger.info(f"✓ Covering {validation_stats['unique_symbols']} unique symbols")
    
    if stats["errors"] > 0:
        logger.warning(f"⚠️  {stats['errors']} errors occurred (check logs above)")
    
    logger.info("=" * 60)
    logger.info(f"Full log saved to: {log_file}")
    
    print("\n" + "=" * 60)
    print("DONE! Check the log file for details:")
    print(f"  {log_file}")
    print("=" * 60)


if __name__ == "__main__":
    main()