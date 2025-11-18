"""
Combine fragmented symbol files into consolidated daily parquet files.

This script reads individual symbol parquet files (one per symbol per day)
and combines them into consolidated daily files (one file per day with all symbols).

Input structure:  YYYY/MM/SYMBOL_YYYYMMDD.parquet (one symbol per file)
Output structure: YYYY/MM/YYYYMMDD.parquet (all symbols for the day)
"""
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set
import sys
from datetime import datetime
import time
from collections import defaultdict

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import Config
from storage.parquet_writer import ParquetWriter
from utils.logger import setup_logger
from utils.helpers import calculate_file_size_mb
from utils.trading_calendar import TradingCalendar


class FragmentedFileCombiner:
    """Combine fragmented symbol files into consolidated daily files."""
    
    def __init__(self, config: Config, logger: logging.Logger):
        """Initialize combiner."""
        self.config = config
        self.logger = logger
        self.writer = ParquetWriter(config, logger)
        self.calendar = TradingCalendar(logger=logger)
        
    def find_fragmented_files(self, input_dir: str) -> tuple[Dict[str, List[Path]], Set[str]]:
        """
        Find all fragmented symbol files grouped by date.
        
        Args:
            input_dir: Directory to scan for fragmented files
            
        Returns:
            Tuple of (files_by_date dict, consolidated_files set)
        """
        input_path = Path(input_dir)
        
        if not input_path.exists():
            self.logger.error(f"Input directory does not exist: {input_dir}")
            return {}, set()
        
        # Group files by date
        files_by_date = defaultdict(list)
        consolidated_files = set()  # Track already consolidated files
        
        self.logger.info(f"Scanning {input_dir} for fragmented files...")
        
        # Scan all parquet files
        for file_path in input_path.rglob("*.parquet"):
            filename = file_path.stem
            
            # Check if it's already a consolidated file (YYYYMMDD.parquet without symbol)
            if '_' not in filename and len(filename) == 8 and filename.isdigit():
                consolidated_files.add(filename)
                continue
            
            # Check if it's a fragmented symbol file (SYMBOL_YYYYMMDD.parquet)
            if '_' in filename:
                parts = filename.rsplit('_', 1)
                if len(parts) == 2:
                    symbol, date = parts
                    
                    # Validate date format
                    if len(date) == 8 and date.isdigit():
                        files_by_date[date].append(file_path)
        
        self.logger.info(f"Found {len(files_by_date)} dates with fragmented files")
        self.logger.info(f"Found {len(consolidated_files)} dates already consolidated")
        
        return dict(files_by_date), consolidated_files
    
    def combine_date(self, date: str, file_paths: List[Path], 
                    output_dir: str, overwrite: bool = False,
                    skip_existing: bool = True) -> Dict:
        """
        Combine all fragmented files for a specific date into one consolidated file.
        
        Args:
            date: Date in YYYYMMDD format
            file_paths: List of fragmented file paths for this date
            output_dir: Output directory for consolidated file
            overwrite: Whether to overwrite existing files
            skip_existing: Whether to skip if consolidated file already exists
            
        Returns:
            Dictionary with combination statistics
        """
        stats = {
            "date": date,
            "success": False,
            "files_read": 0,
            "files_failed": 0,
            "symbols_found": 0,
            "total_rows": 0,
            "input_size_mb": 0.0,
            "output_size_mb": 0.0,
            "duration_seconds": 0.0,
            "skipped": False
        }
        
        start_time = time.time()
        
        try:
            # Create output directory structure
            year, month = date[:4], date[4:6]
            output_path = Path(output_dir) / year / month
            output_path.mkdir(parents=True, exist_ok=True)
            
            # Define output file
            output_file = output_path / f"{date}.parquet"
            
            # Check if file exists
            if output_file.exists():
                if skip_existing and not overwrite:
                    self.logger.info(f"  ⊗ Skipping {date} (consolidated file already exists)")
                    stats["skipped"] = True
                    stats["success"] = True
                    stats["duration_seconds"] = time.time() - start_time
                    return stats
                elif not overwrite:
                    self.logger.warning(f"  ⊗ Consolidated file exists: {date}.parquet (use --overwrite to replace)")
                    stats["skipped"] = True
                    stats["duration_seconds"] = time.time() - start_time
                    return stats
                else:
                    self.logger.info(f"  ⚠ Overwriting existing consolidated file: {date}.parquet")
            
            # Read all fragmented files
            dataframes = []
            symbols_found = set()
            
            for file_path in file_paths:
                try:
                    df = pd.read_parquet(file_path)
                    
                    if len(df) > 0:
                        dataframes.append(df)
                        stats["files_read"] += 1
                        stats["input_size_mb"] += calculate_file_size_mb(str(file_path))
                        
                        # Track symbols
                        if 'symbol' in df.columns:
                            symbols_found.update(df['symbol'].unique())
                        
                except Exception as e:
                    self.logger.debug(f"    Failed to read {file_path.name}: {e}")
                    stats["files_failed"] += 1
            
            if not dataframes:
                self.logger.warning(f"  ✗ No valid data found for date {date}")
                stats["duration_seconds"] = time.time() - start_time
                return stats
            
            # Combine all dataframes
            combined_df = pd.concat(dataframes, ignore_index=True)
            
            # Sort by symbol and timestamp
            combined_df = combined_df.sort_values(['symbol', 'timestamp'])
            
            stats["symbols_found"] = len(symbols_found)
            stats["total_rows"] = len(combined_df)
            
            # Write consolidated file using the writer
            file_path_str = self.writer.write_date_parquet(
                date=date,
                df=combined_df,
                output_dir=output_dir,
                overwrite=overwrite
            )
            
            if file_path_str:
                stats["output_size_mb"] = calculate_file_size_mb(file_path_str)
                stats["success"] = True
                
                compression_ratio = (stats["input_size_mb"] / stats["output_size_mb"]) if stats["output_size_mb"] > 0 else 1.0
                
                self.logger.info(
                    f"  ✓ Combined {date}: "
                    f"{stats['symbols_found']} symbols, "
                    f"{stats['total_rows']:,} rows, "
                    f"{stats['output_size_mb']:.2f} MB "
                    f"(from {stats['files_read']} files, "
                    f"{compression_ratio:.1f}x compression)"
                )
            else:
                self.logger.error(f"  ✗ Failed to write consolidated file for {date}")
            
            stats["duration_seconds"] = time.time() - start_time
            
        except Exception as e:
            self.logger.error(f"  ✗ Failed to combine {date}: {e}", exc_info=True)
            stats["duration_seconds"] = time.time() - start_time
        
        return stats
    
    def combine_all(self, input_dir: str, output_dir: str,
                   start_date: Optional[str] = None,
                   end_date: Optional[str] = None,
                   overwrite: bool = False,
                   skip_existing: bool = True,
                   dry_run: bool = False) -> Dict:
        """
        Combine all fragmented files into consolidated daily files.
        
        Args:
            input_dir: Directory containing fragmented files
            output_dir: Output directory for consolidated files
            start_date: Optional start date filter (YYYYMMDD)
            end_date: Optional end date filter (YYYYMMDD)
            overwrite: Whether to overwrite existing consolidated files
            skip_existing: Whether to skip dates with existing consolidated files
            dry_run: If True, only simulate without writing files
            
        Returns:
            Dictionary with overall statistics
        """
        overall_stats = {
            "dates_found": 0,
            "dates_processed": 0,
            "dates_successful": 0,
            "dates_failed": 0,
            "dates_skipped": 0,
            "dates_already_consolidated": 0,
            "total_symbols": 0,
            "total_files_read": 0,
            "total_files_failed": 0,
            "total_rows": 0,
            "total_input_size_mb": 0.0,
            "total_output_size_mb": 0.0,
            "total_duration_seconds": 0.0,
            "start_time": datetime.now().isoformat(),
            "end_time": None
        }
        
        start_time = time.time()
        
        # Find all fragmented files grouped by date
        self.logger.info(f"\n{'='*70}")
        self.logger.info(f"{'[DRY RUN] ' if dry_run else ''}COMBINE FRAGMENTED FILES INTO CONSOLIDATED DAILY FILES")
        self.logger.info(f"{'='*70}")
        self.logger.info(f"Input directory:    {input_dir}")
        self.logger.info(f"Output directory:   {output_dir}")
        if start_date:
            self.logger.info(f"Start date:         {start_date}")
        if end_date:
            self.logger.info(f"End date:           {end_date}")
        self.logger.info(f"Overwrite:          {overwrite}")
        self.logger.info(f"Skip existing:      {skip_existing}")
        self.logger.info(f"{'='*70}\n")
        
        files_by_date, already_consolidated = self.find_fragmented_files(input_dir)
        overall_stats["dates_found"] = len(files_by_date)
        overall_stats["dates_already_consolidated"] = len(already_consolidated)
        
        if not files_by_date:
            self.logger.warning("No fragmented files found to combine")
            return overall_stats
        
        self.logger.info(f"Found fragmented files for {len(files_by_date)} dates")
        if already_consolidated:
            self.logger.info(f"Found {len(already_consolidated)} dates already consolidated\n")
        
        # Filter by date range if specified
        if start_date or end_date:
            filtered_dates = {}
            for date, files in files_by_date.items():
                if start_date and date < start_date:
                    continue
                if end_date and date > end_date:
                    continue
                filtered_dates[date] = files
            
            skipped_count = len(files_by_date) - len(filtered_dates)
            if skipped_count > 0:
                self.logger.info(f"Filtered to {len(filtered_dates)} dates (skipped {skipped_count} outside date range)\n")
            
            files_by_date = filtered_dates
            overall_stats["dates_skipped"] += skipped_count
        
        # Process each date
        dates = sorted(files_by_date.keys())
        
        self.logger.info(f"Processing {len(dates)} dates...\n")
        
        for idx, date in enumerate(dates, 1):
            file_paths = files_by_date[date]
            
            # Check if already consolidated
            if date in already_consolidated and skip_existing and not overwrite:
                self.logger.info(f"[{idx}/{len(dates)}] {date}: Already consolidated, skipping")
                overall_stats["dates_skipped"] += 1
                continue
            
            self.logger.info(f"[{idx}/{len(dates)}] Processing {date} ({len(file_paths)} files)...")
            
            if dry_run:
                # Just count without processing
                total_rows = 0
                total_size = 0.0
                symbols = set()
                valid_files = 0
                
                for file_path in file_paths:
                    try:
                        df = pd.read_parquet(file_path)
                        total_rows += len(df)
                        total_size += calculate_file_size_mb(str(file_path))
                        if 'symbol' in df.columns:
                            symbols.update(df['symbol'].unique())
                        valid_files += 1
                    except Exception:
                        pass
                
                self.logger.info(
                    f"  [DRY RUN] Would combine {len(symbols)} symbols, "
                    f"{total_rows:,} rows, {total_size:.2f} MB from {valid_files} files"
                )
                overall_stats["dates_processed"] += 1
                overall_stats["dates_successful"] += 1
            else:
                stats = self.combine_date(
                    date, 
                    file_paths, 
                    output_dir, 
                    overwrite,
                    skip_existing
                )
                
                overall_stats["dates_processed"] += 1
                
                if stats["success"]:
                    if stats["skipped"]:
                        overall_stats["dates_skipped"] += 1
                    else:
                        overall_stats["dates_successful"] += 1
                        overall_stats["total_symbols"] += stats["symbols_found"]
                        overall_stats["total_files_read"] += stats["files_read"]
                        overall_stats["total_files_failed"] += stats["files_failed"]
                        overall_stats["total_rows"] += stats["total_rows"]
                        overall_stats["total_input_size_mb"] += stats["input_size_mb"]
                        overall_stats["total_output_size_mb"] += stats["output_size_mb"]
                else:
                    overall_stats["dates_failed"] += 1
                
                overall_stats["total_duration_seconds"] += stats["duration_seconds"]
        
        overall_stats["end_time"] = datetime.now().isoformat()
        overall_stats["total_duration_seconds"] = time.time() - start_time
        
        # Print summary
        self.logger.info(f"\n{'='*70}")
        self.logger.info(f"{'[DRY RUN] ' if dry_run else ''}COMBINATION SUMMARY")
        self.logger.info(f"{'='*70}")
        self.logger.info(f"Dates with fragments:       {overall_stats['dates_found']}")
        self.logger.info(f"Dates already consolidated: {overall_stats['dates_already_consolidated']}")
        self.logger.info(f"Dates processed:            {overall_stats['dates_processed']}")
        self.logger.info(f"Dates successful:           {overall_stats['dates_successful']}")
        self.logger.info(f"Dates failed:               {overall_stats['dates_failed']}")
        self.logger.info(f"Dates skipped:              {overall_stats['dates_skipped']}")
        
        if not dry_run and overall_stats['dates_successful'] > 0:
            avg_symbols = overall_stats['total_symbols'] / overall_stats['dates_successful'] if overall_stats['dates_successful'] > 0 else 0
            compression_ratio = (overall_stats['total_input_size_mb'] / overall_stats['total_output_size_mb']) if overall_stats['total_output_size_mb'] > 0 else 1.0
            
            self.logger.info(f"\nData Statistics:")
            self.logger.info(f"  Avg symbols per day:      {avg_symbols:.0f}")
            self.logger.info(f"  Files read:               {overall_stats['total_files_read']:,}")
            self.logger.info(f"  Files failed:             {overall_stats['total_files_failed']:,}")
            self.logger.info(f"  Total rows:               {overall_stats['total_rows']:,}")
            self.logger.info(f"  Input size:               {overall_stats['total_input_size_mb']:.2f} MB")
            self.logger.info(f"  Output size:              {overall_stats['total_output_size_mb']:.2f} MB")
            self.logger.info(f"  Compression ratio:        {compression_ratio:.2f}x")
        
        self.logger.info(f"\nDuration:                   {overall_stats['total_duration_seconds']:.1f}s ({overall_stats['total_duration_seconds']/60:.1f} min)")
        self.logger.info(f"{'='*70}\n")
        
        if dry_run:
            self.logger.info("⚠ This was a DRY RUN. No files were actually combined.")
            self.logger.info("Run with --no-dry-run to perform the actual combination.\n")
        elif overall_stats['dates_successful'] > 0:
            self.logger.info("✓ Combination complete! Your data is now in consolidated daily files.")
            self.logger.info(f"Location: {output_dir}\n")
        
        return overall_stats


def main():
    """Main execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Combine fragmented symbol parquet files into consolidated daily files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run (safe, shows what would happen)
  python scripts/convert_combine.py
  
  # Actually combine files (SAME directory, consolidated + fragments coexist)
  python scripts/convert_combine.py --no-dry-run
  
  # Combine to a different directory
  python scripts/convert_combine.py --no-dry-run --output-dir "D:/trading_data_consolidated"
  
  # Combine specific date range
  python scripts/convert_combine.py --no-dry-run --start-date 20240101 --end-date 20240131
  
  # Force overwrite existing consolidated files
  python scripts/convert_combine.py --no-dry-run --overwrite
        """
    )
    parser.add_argument(
        '--input-dir',
        type=str,
        help='Input directory containing fragmented files (default: from config OUTPUT_DIR)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        help='Output directory for consolidated files (default: same as input-dir)'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date filter (YYYYMMDD format)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date filter (YYYYMMDD format)'
    )
    parser.add_argument(
        '--overwrite',
        action='store_true',
        help='Overwrite existing consolidated files'
    )
    parser.add_argument(
        '--no-skip-existing',
        action='store_true',
        help='Do not skip dates that already have consolidated files'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        default=True,
        help='Perform a dry run without actually combining files (default: True)'
    )
    parser.add_argument(
        '--no-dry-run',
        action='store_true',
        help='Actually perform the combination (disables dry-run)'
    )
    
    args = parser.parse_args()
    
    # Initialize configuration
    config = Config()
    
    # Setup logger with correct parameters (name, log_file, log_level)
    logger = setup_logger(
        name="FragmentCombiner",
        log_file=config.LOG_FILE,
        log_level=config.LOG_LEVEL
    )
    
    # Determine input directory
    input_dir = args.input_dir if args.input_dir else config.OUTPUT_DIR
    
    # Determine output directory (default: same as input)
    output_dir = args.output_dir if args.output_dir else input_dir
    
    # Handle dry-run flag
    dry_run = args.dry_run and not args.no_dry_run
    
    # Handle skip-existing flag
    skip_existing = not args.no_skip_existing
    
    logger.info("Starting fragmented file combiner...")
    logger.info(f"Strategy: Combine SYMBOL_YYYYMMDD.parquet → YYYYMMDD.parquet")
    
    if input_dir == output_dir:
        logger.info("⚠ Writing to SAME directory - fragmented and consolidated files will coexist")
    
    try:
        # Create combiner
        combiner = FragmentedFileCombiner(config, logger)
        
        # Run combination
        stats = combiner.combine_all(
            input_dir=input_dir,
            output_dir=output_dir,
            start_date=args.start_date,
            end_date=args.end_date,
            overwrite=args.overwrite,
            skip_existing=skip_existing,
            dry_run=dry_run
        )
        
        # Exit with appropriate code
        if stats["dates_failed"] > 0:
            logger.warning(f"Combination completed with {stats['dates_failed']} failures")
            sys.exit(1)
        elif stats["dates_successful"] == 0 and not dry_run:
            logger.info("No files were combined (all skipped or no data)")
            sys.exit(0)
        else:
            logger.info("Combination completed successfully!")
            sys.exit(0)
        
    except KeyboardInterrupt:
        logger.warning("\nCombination interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Combination failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()