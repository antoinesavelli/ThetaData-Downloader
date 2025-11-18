
"""
Track stocks with null/invalid data to skip in future downloads.
"""
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Set, Dict, List
from threading import Lock


class NullDataTracker:
    """Track and persist stocks with null/invalid data."""
    
    def __init__(self, output_dir: str, logger: logging.Logger):
        """
        Initialize tracker.
        
        Args:
            output_dir: Base output directory
            logger: Logger instance
        """
        self.output_dir = Path(output_dir)
        self.logger = logger
        self.lock = Lock()
        
        # Cache of null symbols by date (in memory)
        self._cache: Dict[str, Set[str]] = {}
    
    def _get_null_data_file(self, date: str) -> Path:
        """
        Get path to null data tracking file for a specific date.
        Uses same YYYY/MM/DD structure as data files.
        
        Args:
            date: Date in YYYYMMDD format
            
        Returns:
            Path to null data JSON file
        """
        year = date[:4]
        month = date[4:6]
        day = date[6:8]
        
        date_path = self.output_dir / year / month / day
        date_path.mkdir(parents=True, exist_ok=True)
        
        return date_path / f"_null_data_{date}.json"
    
    def load_null_symbols(self, date: str) -> Set[str]:
        """
        Load list of symbols with null data for a specific date.
        
        Args:
            date: Date in YYYYMMDD format
            
        Returns:
            Set of symbols to skip
        """
        # Check cache first
        if date in self._cache:
            return self._cache[date].copy()
        
        null_file = self._get_null_data_file(date)
        
        if not null_file.exists():
            self._cache[date] = set()
            return set()
        
        try:
            with open(null_file, 'r') as f:
                data = json.load(f)
                
                # Extract symbols from the data structure
                symbols = set(data.get('symbols', []))
                
                self._cache[date] = symbols
                self.logger.info(f"Loaded {len(symbols)} null data symbols for {date}")
                
                return symbols.copy()
                
        except Exception as e:
            self.logger.error(f"Error loading null data file for {date}: {e}")
            return set()
    
    def add_null_symbol(self, symbol: str, date: str, reason: str):
        """
        Add a symbol to the null data list for a specific date.
        
        Args:
            symbol: Stock symbol
            date: Date in YYYYMMDD format
            reason: Reason for null data
        """
        with self.lock:
            null_file = self._get_null_data_file(date)
            
            # Load existing data
            if null_file.exists():
                try:
                    with open(null_file, 'r') as f:
                        data = json.load(f)
                except Exception as e:
                    self.logger.warning(f"Could not read existing null data file: {e}")
                    data = {
                        'date': date,
                        'created_at': datetime.now().isoformat(),
                        'symbols': [],
                        'details': {}
                    }
            else:
                data = {
                    'date': date,
                    'created_at': datetime.now().isoformat(),
                    'symbols': [],
                    'details': {}
                }
            
            # Add symbol if not already present
            if symbol not in data['symbols']:
                data['symbols'].append(symbol)
                data['details'][symbol] = {
                    'reason': reason,
                    'logged_at': datetime.now().isoformat()
                }
                
                # Sort symbols alphabetically
                data['symbols'].sort()
                
                # Update metadata
                data['last_updated'] = datetime.now().isoformat()
                data['total_count'] = len(data['symbols'])
                
                # Write back to file
                try:
                    with open(null_file, 'w') as f:
                        json.dump(data, f, indent=2)
                    
                    # Update cache
                    if date in self._cache:
                        self._cache[date].add(symbol)
                    else:
                        self._cache[date] = set(data['symbols'])
                    
                    self.logger.debug(f"Added {symbol} to null data list for {date}: {reason}")
                    
                except Exception as e:
                    self.logger.error(f"Error writing null data file: {e}")
            else:
                self.logger.debug(f"{symbol} already in null data list for {date}")
    
    def should_skip_symbol(self, symbol: str, date: str) -> tuple[bool, str]:
        """
        Check if a symbol should be skipped for a specific date.
        
        Args:
            symbol: Stock symbol
            date: Date in YYYYMMDD format
            
        Returns:
            Tuple of (should_skip, reason)
        """
        null_symbols = self.load_null_symbols(date)
        
        if symbol in null_symbols:
            # Load reason from file
            null_file = self._get_null_data_file(date)
            
            try:
                with open(null_file, 'r') as f:
                    data = json.load(f)
                    reason = data.get('details', {}).get(symbol, {}).get('reason', 'Null data')
                    return True, reason
            except:
                return True, "Null data (previously logged)"
        
        return False, ""
    
    def get_statistics(self, date: str) -> Dict:
        """
        Get statistics about null data for a specific date.
        
        Args:
            date: Date in YYYYMMDD format
            
        Returns:
            Dictionary with statistics
        """
        null_file = self._get_null_data_file(date)
        
        if not null_file.exists():
            return {
                'date': date,
                'total_null_symbols': 0,
                'file_exists': False
            }
        
        try:
            with open(null_file, 'r') as f:
                data = json.load(f)
                
                # Count by reason
                reason_counts = {}
                for symbol, details in data.get('details', {}).items():
                    reason = details.get('reason', 'Unknown')
                    reason_counts[reason] = reason_counts.get(reason, 0) + 1
                
                return {
                    'date': date,
                    'total_null_symbols': len(data.get('symbols', [])),
                    'file_exists': True,
                    'created_at': data.get('created_at'),
                    'last_updated': data.get('last_updated'),
                    'reason_breakdown': reason_counts
                }
        except Exception as e:
            self.logger.error(f"Error reading null data statistics: {e}")
            return {
                'date': date,
                'error': str(e)
            }
    
    def remove_symbol(self, symbol: str, date: str):
        """
        Remove a symbol from the null data list (e.g., if data becomes available).
        
        Args:
            symbol: Stock symbol
            date: Date in YYYYMMDD format
        """
        with self.lock:
            null_file = self._get_null_data_file(date)
            
            if not null_file.exists():
                return
            
            try:
                with open(null_file, 'r') as f:
                    data = json.load(f)
                
                if symbol in data.get('symbols', []):
                    data['symbols'].remove(symbol)
                    if symbol in data.get('details', {}):
                        del data['details'][symbol]
                    
                    data['last_updated'] = datetime.now().isoformat()
                    data['total_count'] = len(data['symbols'])
                    
                    with open(null_file, 'w') as f:
                        json.dump(data, f, indent=2)
                    
                    # Update cache
                    if date in self._cache and symbol in self._cache[date]:
                        self._cache[date].remove(symbol)
                    
                    self.logger.info(f"Removed {symbol} from null data list for {date}")
            
            except Exception as e:
                self.logger.error(f"Error removing symbol from null data: {e}")
