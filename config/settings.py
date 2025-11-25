"""
Configuration settings for ThetaData pipeline.
"""
import os
from pathlib import Path
from typing import Dict, Any


class Config:
    """Configuration management for the pipeline."""
    
    def __init__(self):
        # ThetaData API Configuration
        self.THETADATA_BASE_URL = "http://localhost:25503"
        self.THETADATA_ENDPOINT = "/v3/stock/history/ohlc"
        
        # MCP Configuration
        self.USE_MCP = True  # Use MCP protocol
        self.MCP_HOST = "localhost"
        self.MCP_PORT = 25503
        self.MCP_URL = f"ws://{self.MCP_HOST}:{self.MCP_PORT}/mcp/sse"
        
        # Data Configuration
        self.START_DATE = "20200101"
        self.END_DATE = "20201231"  # ✅ UPDATED: Test with 3 days first
        self.INTERVAL = "1m"
        self.VENUE = None

        # ✅ Response format configuration
        self.RESPONSE_FORMAT = "csv"  # ✅ OPTIMIZED: CSV may parse faster than JSON
        # Options: None (JSON default), "csv"
        # To use JSON format: self.RESPONSE_FORMAT = None
        
        # Time Range Configuration - INCLUDES PREMARKET
        self.START_TIME = "04:00:00"  # 4:00 AM ET - Premarket start
        self.END_TIME = "20:00:00"    # 8:00 PM ET - After hours end
        # This covers:
        # - Premarket: 4:00 AM - 9:30 AM
        # - Regular: 9:30 AM - 4:00 PM
        # - After hours: 4:00 PM - 8:00 PM
        
        # Symbol List - Full CSV path
        self.SYMBOL_FILE = "data/symbols.csv"  # Full production list
        
        # Output Configuration
        self.OUTPUT_DIR = "D:/trading_data"  # Production output directory
        
        # ✅ CONSOLIDATED MODE: Download all symbols per day into ONE file
        self.CONSOLIDATED_DAILY_MODE = True  # ✅ ONE file per day (faster analysis)
        # Structure: YYYY/MM/YYYYMMDD.parquet
        # Each file contains ALL symbols for that day
        
        # Request Configuration
        # ✅ FIXED: Increased timeout to 120s to handle slower API responses
        # With 3 retries, this gives up to 360s (6 minutes) total per symbol
        self.REQUEST_TIMEOUT = 120

        # ✅ FIXED: ThetaData Terminal has HTTP_CONCURRENCY limit (default=4, max=24)
        # Sending more concurrent requests than this causes timeouts as requests queue up
        # See: https://http-docs.thetadata.us/Articles/Performance-And-Tuning/Concurrent-Requests.html
        self.MAX_CONCURRENT_REQUESTS = 10  # Reduced from 20 to test if SSE connection is bottleneck
        self.RATE_LIMIT_DELAY = 0.1  # Small delay between requests to prevent queue buildup

        # ✅ DIAGNOSTIC: Refresh MCP connection before each day to test if staleness causes slowdowns
        self.REFRESH_CONNECTION_PER_DAY = True  # Set to False after testing
        
        # Parquet Configuration
        self.PARQUET_COMPRESSION = "snappy"  # Fast compression
        self.PARQUET_ROW_GROUP_SIZE = 100000  # Rows per row group
        
        # Logging Configuration
        self.LOG_LEVEL = "INFO"
        self.LOG_FILE = "logs/thetadata_pipeline.log"
        self.LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key."""
        return getattr(self, key, default)
    
    def get_parquet_settings(self) -> Dict[str, Any]:
        """Get parquet-specific settings."""
        return {
            'compression': self.PARQUET_COMPRESSION,
            'row_group_size': self.PARQUET_ROW_GROUP_SIZE
        }
    
    def validate(self) -> bool:
        """Validate configuration settings."""
        # Ensure symbol file exists
        if not Path(self.SYMBOL_FILE).exists():
            raise FileNotFoundError(f"Symbol file not found: {self.SYMBOL_FILE}")
        
        # Ensure output directory exists or can be created
        output_path = Path(self.OUTPUT_DIR)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Ensure logs directory exists
        log_path = Path(self.LOG_FILE).parent
        log_path.mkdir(parents=True, exist_ok=True)
        
        return True
