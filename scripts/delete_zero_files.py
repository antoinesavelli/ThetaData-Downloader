"""
Delete parquet files that contain only zeros in all OHLC columns.

This script identifies and optionally deletes files where all data is zero or null,
indicating no valid trading data.
"""
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, List
from datetime import datetime
import sys
import re

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import Config
from utils.logger import setup_logger


class ZeroFileDeleter:
    """Delete parquet files with all zeros."""
    
    def __init__(self, base_dir: str, logger: logging.Logger):
        """
        Initialize deleter.
        
        Args:
            base_dir: Base directory containing parquet files
            logger: Logger instance
        """
        self.base_dir = Path(base_dir)
        self.logger = logger
        
        # Pattern to identify special securities (warrants, units, rights)
        self.special_pattern = re.compile(r'[A-Z]+[UWRZ]$')
        
    def is_special_security(self, symbol: str) -> bool:
        """Check if symbol is a special security (warrant, unit, right)."""
        if not symbol:
            return False
        return bool(self.special_pattern.match(symbol))
    
    def _extract_symbol(self, filename: str) -> str:
        """Extract symbol from filename (e.g., AAPL_20240102.parquet -> AAPL)."""
        try:
            return filename.split('_')[0]
        except:
            return filename.replace('.parquet', '')
    
    def is_all_zeros(self, file_path: Path) -> tuple[bool, str]:
        """
        Check if a file should be deleted based on data quality issues.
        Matches the criteria from validate_data_quality.py
        
        Args:
            file_path: Path to parquet file
            
        Returns:
            Tuple of (should_delete, reason)
        """
        try:
            df = pd.read_parquet(file_path)
            
            # Check if dataframe is empty
            if len(df) == 0:
                return True, "Empty file (0 rows)"
            
            # Check for required columns
            required_cols = ['open', 'high', 'low', 'close']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                return True, f"Missing columns: {', '.join(missing_cols)}"
            
            ohlc_cols = ['open', 'high', 'low', 'close']
            
            # 1. Check if all OHLC values are zero or null
            all_ohlc_zero = all(
                (df[col] == 0).all() or df[col].isna().all()
                for col in ohlc_cols
            )
            
            if all_ohlc_zero:
                return True, f"All OHLC zeros ({len(df)} bars)"
            
            # 2. Check if all values are null (separate from zero check)
            all_null = all(df[col].isna().all() for col in ohlc_cols)
            
            if all_null:
                return True, f"All OHLC null ({len(df)} bars)"
            
            # 3. Check for constant price (no price movement)
            # This catches files where prices are constant (including constant at 0)
            constant_price = (
                (df['open'] == df['high']).all() and
                (df['high'] == df['low']).all() and
                (df['low'] == df['close']).all()
            )
            
            if constant_price:
                price = df['close'].iloc[0]
                
                # Check if volume is also zero
                has_volume = 'volume' in df.columns
                all_zero_volume = False
                
                if has_volume:
                    all_zero_volume = (df['volume'] == 0).all() or df['volume'].isna().all()
                
                # Delete if:
                # - Constant price at $0 (regardless of volume)
                # - OR constant price with zero volume (any price)
                if price == 0.0:
                    return True, f"Constant price at $0.00 ({len(df)} bars)"
                elif all_zero_volume:
                    return True, f"Zero volume + constant price ${price:.2f} ({len(df)} bars)"
            
            # 4. Check for zero volume throughout (even if prices vary)
            if 'volume' in df.columns:
                all_zero_volume = (df['volume'] == 0).all() or df['volume'].isna().all()
                
                if all_zero_volume:
                    # Check if prices are all zero too
                    all_prices_zero = all(
                        (df[col] == 0).all() for col in ohlc_cols
                    )
                    
                    if all_prices_zero:
                        return True, f"Zero volume + all prices zero ({len(df)} bars)"
            
            # Return False for valid data
            return False, "Contains valid data"
            
        except Exception as e:
            return False, f"Error reading file: {e}"
    
    def scan_and_delete(self, dry_run: bool = True) -> Dict:
        """
        Scan for files with all zeros and optionally delete them.
        Always includes special securities (warrants/units).
        
        Args:
            dry_run: If True, only simulate deletion without actually deleting
            
        Returns:
            Dictionary with deletion statistics
        """
        self.logger.info("=" * 80)
        self.logger.info(f"{'DRY RUN: ' if dry_run else ''}SCANNING FOR ZERO-ONLY FILES")
        self.logger.info("=" * 80)
        self.logger.info(f"Base directory: {self.base_dir}")
        self.logger.info(f"Including special securities: YES (always)")
        self.logger.info("")
        
        stats = {
            "total_files": 0,
            "files_checked": 0,
            "files_to_delete": 0,
            "files_deleted": 0,
            "specials_deleted": 0,
            "regular_deleted": 0,
            "errors": 0,
            "deleted_files": [],
        }
        
        # Get all parquet files
        parquet_files = sorted(self.base_dir.rglob("*.parquet"))
        stats["total_files"] = len(parquet_files)
        
        self.logger.info(f"Found {len(parquet_files)} parquet files")
        self.logger.info("")
        
        # Process each file
        for idx, file_path in enumerate(parquet_files, 1):
            relative_path = file_path.relative_to(self.base_dir)
            stats["files_checked"] += 1
            
            # Progress indicator
            if idx % 500 == 0:
                self.logger.info(f"Progress: {idx}/{len(parquet_files)} ({idx/len(parquet_files)*100:.1f}%)")
            
            try:
                # Extract symbol
                symbol = self._extract_symbol(file_path.name)
                is_special = self.is_special_security(symbol)
                
                # Check if file has all zeros
                all_zeros, reason = self.is_all_zeros(file_path)
                
                if all_zeros:
                    # Mark for deletion (always include specials)
                    stats["files_to_delete"] += 1
                    
                    if is_special:
                        stats["specials_deleted"] += 1
                    else:
                        stats["regular_deleted"] += 1
                    
                    if dry_run:
                        special_tag = " [SPECIAL]" if is_special else ""
                        self.logger.info(f"[DRY RUN] Would delete: {relative_path}{special_tag} ({reason})")
                        stats["deleted_files"].append({
                            'file': str(relative_path),
                            'symbol': symbol,
                            'reason': reason,
                            'size_bytes': file_path.stat().st_size,
                            'is_special': is_special
                        })
                    else:
                        # Actually delete the file
                        file_size = file_path.stat().st_size
                        file_path.unlink()
                        stats["files_deleted"] += 1
                        stats["deleted_files"].append({
                            'file': str(relative_path),
                            'symbol': symbol,
                            'reason': reason,
                            'size_bytes': file_size,
                            'is_special': is_special
                        })
                        special_tag = " [SPECIAL]" if is_special else ""
                        self.logger.info(f"✓ Deleted: {relative_path}{special_tag} ({reason})")
                
            except Exception as e:
                stats["errors"] += 1
                self.logger.error(f"Error processing {relative_path}: {e}")
        
        # Report results
        self._report_results(stats, dry_run)
        
        return stats
    
    def _report_results(self, stats: Dict, dry_run: bool):
        """Generate detailed report of deletion results."""
        self.logger.info("")
        self.logger.info("=" * 80)
        self.logger.info(f"{'DRY RUN ' if dry_run else ''}DELETION SUMMARY")
        self.logger.info("=" * 80)
        self.logger.info(f"Total files found:          {stats['total_files']}")
        self.logger.info(f"Files checked:              {stats['files_checked']}")
        self.logger.info(f"Files with all zeros:       {stats['files_to_delete']}")
        
        if dry_run:
            self.logger.info(f"Files that would be deleted: {stats['files_to_delete']}")
        else:
            self.logger.info(f"Files deleted:              {stats['files_deleted']}")
        
        self.logger.info(f"  Regular securities:       {stats['regular_deleted']}")
        self.logger.info(f"  Special securities:       {stats['specials_deleted']}")
        self.logger.info(f"Errors:                     {stats['errors']}")
        
        # Calculate space saved
        if stats['deleted_files']:
            total_bytes = sum(f['size_bytes'] for f in stats['deleted_files'])
            total_mb = total_bytes / (1024 * 1024)
            self.logger.info(f"Space {'to be freed' if dry_run else 'freed'}: {total_mb:.2f} MB")
        
        self.logger.info("")
        
        # List deleted files by symbol
        if stats['deleted_files']:
            # Group by symbol
            by_symbol = {}
            for item in stats['deleted_files']:
                symbol = item['symbol']
                if symbol not in by_symbol:
                    by_symbol[symbol] = []
                by_symbol[symbol].append(item)
            
            self.logger.info(f"{'Files to delete' if dry_run else 'Deleted files'} by symbol:")
            
            # Separate regular and special
            regular_symbols = {k: v for k, v in by_symbol.items() if not v[0]['is_special']}
            special_symbols = {k: v for k, v in by_symbol.items() if v[0]['is_special']}
            
            if regular_symbols:
                self.logger.info(f"\n  Regular securities ({len(regular_symbols)} symbols):")
                for symbol in sorted(regular_symbols.keys()):
                    files = regular_symbols[symbol]
                    self.logger.info(f"    {symbol}: {len(files)} files")
                    
                    # Show first 2 files for this symbol
                    for file_info in files[:2]:
                        self.logger.info(f"      - {file_info['file']} ({file_info['reason']})")
                    
                    if len(files) > 2:
                        self.logger.info(f"      ... and {len(files) - 2} more")
            
            if special_symbols:
                self.logger.info(f"\n  Special securities ({len(special_symbols)} symbols):")
                for symbol in sorted(special_symbols.keys())[:10]:
                    files = special_symbols[symbol]
                    self.logger.info(f"    {symbol}: {len(files)} files")
                
                if len(special_symbols) > 10:
                    self.logger.info(f"    ... and {len(special_symbols) - 10} more special symbols")
        
        self.logger.info("")
        self.logger.info("=" * 80)
        
        if dry_run:
            self.logger.warning("\n⚠️  THIS WAS A DRY RUN - NO FILES WERE DELETED")
            self.logger.warning("Run with --execute flag to actually delete files")
        else:
            self.logger.info(f"\n✓ Deletion complete: {stats['files_deleted']} files removed")
    
    def export_deletion_log(self, stats: Dict, output_file: str):
        """
        Export deletion log to CSV file.
        
        Args:
            stats: Statistics dictionary from scan_and_delete
            output_file: Path to output CSV file
        """
        try:
            import csv
            
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['File', 'Symbol', 'Reason', 'Size (bytes)', 'Is Special Security'])
                
                # Write all deleted files
                for item in stats['deleted_files']:
                    writer.writerow([
                        item['file'],
                        item['symbol'],
                        item['reason'],
                        item['size_bytes'],
                        'Yes' if item['is_special'] else 'No'
                    ])
            
            self.logger.info(f"Deletion log exported to: {output_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to export log: {e}")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Delete parquet files with all zeros (always includes special securities)')
    parser.add_argument('--execute', action='store_true', 
                       help='Actually delete files (default is dry run)')
    parser.add_argument('--export-log', type=str,
                       help='Export deletion log to CSV file')
    
    args = parser.parse_args()
    
    # Load configuration
    config = Config()
    
    # Setup logger
    logger = setup_logger(
        name="zero_file_deleter",
        log_file="logs/delete_zero_files.log",
        log_level="INFO"
    )
    
    # Create deleter
    deleter = ZeroFileDeleter(
        base_dir=config.OUTPUT_DIR,
        logger=logger
    )
    
    # Confirm with user if executing
    if args.execute:
        logger.warning("=" * 80)
        logger.warning("⚠️  WARNING: YOU ARE ABOUT TO DELETE FILES!")
        logger.warning("=" * 80)
        logger.warning(f"Directory: {config.OUTPUT_DIR}")
        logger.warning(f"Includes special securities: YES (always)")
        logger.warning("")
        
        response = input("Are you sure you want to continue? (type 'yes' to confirm): ")
        if response.lower() != 'yes':
            logger.info("Deletion cancelled by user")
            return
    
    # Scan and delete (always includes special securities)
    stats = deleter.scan_and_delete(dry_run=not args.execute)
    
    # Export log if requested
    if args.export_log:
        deleter.export_deletion_log(stats, args.export_log)
    else:
        # Auto-generate log filename
        log_file = f"logs/deletion_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        deleter.export_deletion_log(stats, log_file)
    
    logger.info("\nOperation complete!")


if __name__ == "__main__":
    main()
