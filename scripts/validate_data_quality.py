"""
Data quality validation script for parquet files.

Scans all parquet files to identify:
1. Files with all zeros in OHLC columns
2. Files with all missing/null values
3. Files with zero volume throughout
4. Files with constant prices (no movement)
5. Files with insufficient data (< minimum bars expected)
"""
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Set, Optional
from datetime import datetime
import sys
import re

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import Config
from utils.logger import setup_logger


class DataQualityValidator:
    """Validate data quality in parquet files."""
    
    def __init__(self, base_dir: str, logger: logging.Logger):
        """
        Initialize validator.
        
        Args:
            base_dir: Base directory containing parquet files (with YYYY/MM/DD structure)
            logger: Logger instance
        """
        self.base_dir = Path(base_dir)
        self.logger = logger
        self.issues_found = []
        
        # Pattern to identify special securities (warrants, units, rights)
        self.special_pattern = re.compile(r'[A-Z]+[UWRZ]$')  # Ends with U, W, R, or Z
        
    def is_special_security(self, symbol: str) -> bool:
        """
        Check if symbol is a special security (warrant, unit, right).
        These often have zero/low trading activity.
        
        Args:
            symbol: Stock symbol
            
        Returns:
            True if special security
        """
        if not symbol:
            return False
        return bool(self.special_pattern.match(symbol))
    
    def _extract_date_from_path(self, file_path: Path) -> Optional[str]:
        """
        Extract date from file path (YYYY/MM/DD structure) or filename.
        
        Args:
            file_path: Path to parquet file
            
        Returns:
            Date in YYYYMMDD format, or None if not found
        """
        try:
            # Try extracting from filename first (e.g., AAPL_20240102.parquet)
            filename = file_path.stem  # Remove .parquet extension
            if '_' in filename:
                date_part = filename.split('_')[-1]
                if len(date_part) == 8 and date_part.isdigit():
                    return date_part
            
            # Try extracting from path structure (YYYY/MM/DD)
            parts = file_path.parts
            if len(parts) >= 3:
                # Look for year/month/day pattern
                for i in range(len(parts) - 2):
                    year = parts[i]
                    month = parts[i + 1]
                    day = parts[i + 2]
                    
                    if (len(year) == 4 and year.isdigit() and
                        len(month) == 2 and month.isdigit() and
                        len(day) == 2 and day.isdigit()):
                        return f"{year}{month}{day}"
            
            return None
        except Exception as e:
            self.logger.debug(f"Could not extract date from {file_path}: {e}")
            return None
    
    def _is_in_date_range(self, file_date: str, start_date: Optional[str], end_date: Optional[str]) -> bool:
        """
        Check if file date is within the specified date range.
        
        Args:
            file_date: Date in YYYYMMDD format
            start_date: Start date in YYYYMMDD format (inclusive), or None for no start limit
            end_date: End date in YYYYMMDD format (inclusive), or None for no end limit
            
        Returns:
            True if file is within date range
        """
        if not file_date:
            return False
        
        if start_date and file_date < start_date:
            return False
        
        if end_date and file_date > end_date:
            return False
        
        return True
        
    def scan_all_files(
        self, 
        min_expected_bars: int = 961, 
        categorize_specials: bool = True,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, any]:
        """
        Scan all parquet files and identify quality issues.
        
        Args:
            min_expected_bars: Minimum number of bars expected per file (default 961 for 16 hours)
            categorize_specials: If True, separate special securities (warrants/units) in report
            start_date: Start date filter in YYYYMMDD format (inclusive), or None for all dates
            end_date: End date filter in YYYYMMDD format (inclusive), or None for all dates
            
        Returns:
            Dictionary with scan results and statistics
        """
        self.logger.info("=" * 80)
        self.logger.info("STARTING DATA QUALITY SCAN")
        self.logger.info("=" * 80)
        self.logger.info(f"Base directory: {self.base_dir}")
        self.logger.info(f"Minimum expected bars: {min_expected_bars}")
        self.logger.info(f"Categorizing special securities: {categorize_specials}")
        
        # Log date range filter
        if start_date or end_date:
            date_range_str = f"{start_date or 'beginning'} to {end_date or 'end'}"
            self.logger.info(f"Date range filter: {date_range_str}")
        else:
            self.logger.info("Date range filter: None (scanning all dates)")
        
        self.logger.info("")
        
        stats = {
            "total_files": 0,
            "valid_files": 0,
            "skipped_files": 0,
            "all_zeros": [],
            "all_nulls": [],
            "zero_volume": [],
            "constant_price": [],
            "insufficient_bars": [],
            "read_errors": [],
            "other_issues": [],
            # Special security tracking
            "special_securities": {
                "all_zeros": [],
                "zero_volume": [],
                "constant_price": []
            }
        }
        
        # Get all parquet files recursively
        all_parquet_files = sorted(self.base_dir.rglob("*.parquet"))
        
        # Filter by date range if specified
        parquet_files = []
        for file_path in all_parquet_files:
            file_date = self._extract_date_from_path(file_path)
            
            if self._is_in_date_range(file_date, start_date, end_date):
                parquet_files.append(file_path)
            else:
                stats["skipped_files"] += 1
        
        stats["total_files"] = len(parquet_files)
        
        self.logger.info(f"Found {len(all_parquet_files)} total parquet files")
        if start_date or end_date:
            self.logger.info(f"Filtered to {len(parquet_files)} files in date range")
            self.logger.info(f"Skipped {stats['skipped_files']} files outside date range")
        self.logger.info("")
        
        # Process each file
        for idx, file_path in enumerate(parquet_files, 1):
            relative_path = file_path.relative_to(self.base_dir)
            
            # Progress indicator every 500 files
            if idx % 500 == 0:
                self.logger.info(f"Progress: {idx}/{len(parquet_files)} files scanned ({idx/len(parquet_files)*100:.1f}%)")
            
            try:
                # Extract symbol from filename
                symbol = self._extract_symbol(file_path.name)
                is_special = self.is_special_security(symbol)
                
                # Read the file
                df = pd.read_parquet(file_path)
                
                # Validate the file
                issues = self._validate_file(df, file_path, min_expected_bars)
                
                if issues:
                    # Categorize issues
                    for issue in issues:
                        issue_type = issue['type']
                        issue_data = {
                            'file': str(relative_path),
                            'symbol': symbol,
                            'details': issue['details'],
                            'is_special': is_special
                        }
                        
                        # Separate special securities if enabled
                        if categorize_specials and is_special and issue_type in ['all_zeros', 'zero_volume', 'constant_price']:
                            stats["special_securities"][issue_type].append(issue_data)
                        else:
                            if issue_type not in stats:
                                stats[issue_type] = []
                            stats[issue_type].append(issue_data)
                else:
                    stats["valid_files"] += 1
                    
            except Exception as e:
                self.logger.error(f"Error reading {relative_path}: {e}")
                stats["read_errors"].append({
                    'file': str(relative_path),
                    'symbol': self._extract_symbol(file_path.name),
                    'error': str(e),
                    'is_special': False
                })
        
        # Report results
        self._report_results(stats, categorize_specials)
        
        return stats
    
    def _extract_symbol(self, filename: str) -> str:
        """Extract symbol from filename (e.g., AAPL_20240102.parquet -> AAPL)."""
        try:
            return filename.split('_')[0]
        except:
            return filename.replace('.parquet', '')
    
    def _validate_file(self, df: pd.DataFrame, file_path: Path, min_bars: int) -> List[Dict]:
        """
        Validate a single file for data quality issues.
        
        Returns:
            List of issues found (empty if file is valid)
        """
        issues = []
        
        # Check if dataframe is empty
        if len(df) == 0:
            issues.append({
                'type': 'all_nulls',
                'details': 'File is empty (0 rows)'
            })
            return issues
        
        # Check for required columns
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            issues.append({
                'type': 'other_issues',
                'details': f'Missing columns: {", ".join(missing_cols)}'
            })
            return issues
        
        # 1. Check if all OHLC values are zero
        ohlc_cols = ['open', 'high', 'low', 'close']
        all_ohlc_zero = all(
            (df[col] == 0).all() or df[col].isna().all()
            for col in ohlc_cols
        )
        
        if all_ohlc_zero:
            issues.append({
                'type': 'all_zeros',
                'details': f'{len(df)} bars, all zero'
            })
        
        # 2. Check if all values are null
        all_null = all(df[col].isna().all() for col in ohlc_cols)
        
        if all_null and not all_ohlc_zero:  # Don't double-count
            issues.append({
                'type': 'all_nulls',
                'details': f'{len(df)} bars, all null'
            })
        
        # 3. Check if volume is zero throughout
        if 'volume' in df.columns:
            all_zero_volume = (df['volume'] == 0).all() or df['volume'].isna().all()
            
            if all_zero_volume:
                issues.append({
                    'type': 'zero_volume',
                    'details': f'{len(df)} bars, zero volume'
                })
        
        # 4. Check for constant prices (no price movement)
        if not all_ohlc_zero and not all_null:
            # Check if open == high == low == close for all rows
            constant_price = (
                (df['open'] == df['high']).all() and
                (df['high'] == df['low']).all() and
                (df['low'] == df['close']).all()
            )
            
            if constant_price:
                # Get the constant price value
                price = df['close'].iloc[0]
                issues.append({
                    'type': 'constant_price',
                    'details': f'{len(df)} bars, constant at ${price:.2f}'
                })
        
        # 5. Check for insufficient bars (only if not other critical issues)
        if not all_ohlc_zero and not all_null and len(df) < min_bars:
            issues.append({
                'type': 'insufficient_bars',
                'details': f'{len(df)}/{min_bars} bars'
            })
        
        # 6. Check for negative prices
        if not all_ohlc_zero:
            negative_prices = any((df[col] < 0).any() for col in ohlc_cols)
            if negative_prices:
                issues.append({
                    'type': 'other_issues',
                    'details': 'Negative prices detected'
                })
        
        # 7. Check for OHLC relationship violations
        if not all_ohlc_zero and not all_null:
            violations = (
                (df['high'] < df['low']).any() or
                (df['high'] < df['open']).any() or
                (df['high'] < df['close']).any() or
                (df['low'] > df['open']).any() or
                (df['low'] > df['close']).any()
            )
            
            if violations:
                issues.append({
                    'type': 'other_issues',
                    'details': 'OHLC violations (high<low, etc.)'
                })
        
        return issues
    
    def _report_results(self, stats: Dict[str, any], show_specials: bool = True):
        """Generate detailed report of validation results."""
        self.logger.info("");
        self.logger.info("=" * 80);
        self.logger.info("DATA QUALITY SCAN RESULTS");
        self.logger.info("=" * 80);
        self.logger.info(f"Total files scanned:        {stats['total_files']}");
        if stats.get('skipped_files', 0) > 0:
            self.logger.info(f"Files skipped (date filter): {stats['skipped_files']}");
        self.logger.info(f"Valid files:                {stats['valid_files']}");
        
        total_issues = sum(len(stats.get(k, [])) for k in ['all_zeros', 'all_nulls', 'zero_volume', 
                                                            'constant_price', 'insufficient_bars', 
                                                            'read_errors', 'other_issues'])
        self.logger.info(f"Files with issues:          {total_issues}");
        
        # Report special securities separately
        if show_specials:
            special_count = sum(len(v) for v in stats['special_securities'].values())
            if special_count > 0:
                self.logger.info(f"Special securities (W/U/R): {special_count} (expected low activity)");
        
        self.logger.info("");
        
        # Report each issue type
        issue_types = {
            'all_zeros': 'All OHLC values are zero',
            'zero_volume': 'All volume values are zero',
            'constant_price': 'No price movement (constant)',
            'all_nulls': 'All OHLC values are null',
            'insufficient_bars': 'Insufficient data bars',
            'other_issues': 'Other data quality issues',
            'read_errors': 'File read errors'
        }
        
        for issue_key, issue_label in issue_types.items():
            items = stats.get(issue_key, [])
            count = len(items)
            
            if count > 0:
                self.logger.warning(f"\n{issue_label}: {count} files");
                
                # Show first 10 files for this issue type
                for item in items[:10]:
                    symbol_info = f" [{item['symbol']}]" if 'symbol' in item else ""
                    if 'error' in item:
                        self.logger.warning(f"  • {item['file']}{symbol_info}: {item['error']}")
                    else:
                        self.logger.warning(f"  • {item['file']}{symbol_info}: {item['details']}")
                
                if count > 10:
                    self.logger.warning(f"  ... and {count - 10} more")
        
        # Report special securities
        if show_specials:
            self.logger.info("");
            self.logger.info("=" * 80);
            self.logger.info("SPECIAL SECURITIES (Warrants/Units/Rights)");
            self.logger.info("=" * 80);
            self.logger.info("These securities often have zero/low trading activity - this is expected.");
            
            for issue_key in ['all_zeros', 'zero_volume', 'constant_price']:
                items = stats['special_securities'].get(issue_key, [])
                if items:
                    self.logger.info(f"\n{issue_types[issue_key]}: {len(items)} special securities");
                    # Show first 5
                    for item in items[:5]:
                        self.logger.info(f"  • {item['symbol']}: {item['details']}")
                    if len(items) > 5:
                        self.logger.info(f"  ... and {len(items) - 5} more")
        
        self.logger.info("");
        self.logger.info("=" * 80);
        
        # Summary recommendation
        critical_regular = (
            len([x for x in stats.get('all_zeros', []) if not x.get('is_special', False)]) +
            len(stats.get('all_nulls', [])) +
            len(stats.get('read_errors', []))
        )
        
        if critical_regular > 0:
            self.logger.error(f"\n⚠️  CRITICAL: {critical_regular} regular securities have no valid data!");
            self.logger.error("These files should be investigated or re-downloaded.");
        else:
            self.logger.info("\n✓ No critical data quality issues in regular securities.");
    
    def export_report(self, stats: Dict[str, any], output_file: str):
        """
        Export detailed report to CSV file.
        
        Args:
            stats: Statistics dictionary from scan_all_files
            output_file: Path to output CSV file
        """
        try:
            import csv
            
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['File', 'Symbol', 'Issue Type', 'Details', 'Is Special Security'])
                
                # Write all issues
                issue_types = ['all_zeros', 'all_nulls', 'zero_volume', 'constant_price',
                             'insufficient_bars', 'read_errors', 'other_issues']
                
                for issue_type in issue_types:
                    for item in stats.get(issue_type, []):
                        symbol = item.get('symbol', 'UNKNOWN')
                        is_special = item.get('is_special', False)
                        
                        if 'error' in item:
                            writer.writerow([item['file'], symbol, issue_type, item['error'], is_special])
                        else:
                            writer.writerow([item['file'], symbol, issue_type, item['details'], is_special])
                
                # Write special securities
                for issue_type in ['all_zeros', 'zero_volume', 'constant_price']:
                    for item in stats['special_securities'].get(issue_type, []):
                        writer.writerow([item['file'], item['symbol'], f"{issue_type}_special", item['details'], True])
            
            self.logger.info(f"\nDetailed report exported to: {output_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to export report: {e}")


def main():
    """Main entry point for data quality validation."""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Validate data quality of parquet files')
    parser.add_argument('--start-date', type=str, help='Start date in YYYYMMDD format (inclusive)')
    parser.add_argument('--end-date', type=str, help='End date in YYYYMMDD format (inclusive)')
    parser.add_argument('--min-bars', type=int, default=961, help='Minimum expected bars per file (default: 961)')
    
    args = parser.parse_args()
    
    # Load configuration
    config = Config()
    
    # Setup logger
    logger = setup_logger(
        name="data_quality_validator",
        log_file="logs/data_quality_validation.log",
        log_level="INFO"
    )
    
    # Log date range if provided
    if args.start_date or args.end_date:
        logger.info(f"Date range filter: {args.start_date or 'beginning'} to {args.end_date or 'end'}")
    
    # Create validator
    validator = DataQualityValidator(
        base_dir=config.OUTPUT_DIR,
        logger=logger
    )
    
    # Scan all files with date range filter
    stats = validator.scan_all_files(
        min_expected_bars=args.min_bars,
        categorize_specials=True,
        start_date=args.start_date,
        end_date=args.end_date
    )
    
    # Check if insufficient_bars issues are being logged
    print(f"\nInsufficient bars found: {len(stats['insufficient_bars'])}")
    
    # Export detailed report
    report_file = f"logs/data_quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    validator.export_report(stats, report_file)
    
    logger.info("\nValidation complete!")


if __name__ == "__main__":
    main()