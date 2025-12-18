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
        self.START_DATE = "20251121"
        self.END_DATE = "20251218"
        self.INTERVAL = "1m"
        self.VENUE = None

        # ✅ Response format configuration
        self.RESPONSE_FORMAT = "csv"  # ✅ OPTIMIZED: CSV may parse faster than JSON
        
        # Time Range Configuration - INCLUDES PREMARKET
        self.START_TIME = "04:00:00"  # 4:00 AM ET - Premarket start
        self.END_TIME = "20:00:00"    # 8:00 PM ET - After hours end
        
        # Symbol List - Full CSV path
        self.SYMBOL_FILE = "data/symbols.csv"  # Full production list
        
        # Output Configuration
        self.OUTPUT_DIR = "D:/trading_data"  # Production output directory
        
        # ✅ CONSOLIDATED MODE: Download all symbols per day into ONE file
        self.CONSOLIDATED_DAILY_MODE = True
        # Structure: YYYY/MM/YYYYMMDD.parquet
        
        # ✅ CRITICAL: ULTRA-CONSERVATIVE settings to prevent 500 errors
        self.REQUEST_TIMEOUT = 300  # ✅ 5 minutes per attempt (very long timeout)
        self.MAX_CONCURRENT_REQUESTS = 100  # ✅ ONLY 2 concurrent requests (prevents server crash)
        self.RATE_LIMIT_DELAY = 1.0  # ✅ 1 second delay between requests (maximum breathing room)
        # Rationale for ULTRA-LOW settings:
        # - 500 Server Error = Theta Terminal backend is CRASHING
        # - Must be EXTREMELY gentle to keep server alive
        # - Better to go VERY slow than crash the server repeatedly
        # - Can increase after confirming stability
        
        # Parquet Configuration
        self.PARQUET_COMPRESSION = "snappy"
        self.PARQUET_ROW_GROUP_SIZE = 100000
        
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
        from datetime import datetime
        
        # Ensure symbol file exists
        if not Path(self.SYMBOL_FILE).exists():
            raise FileNotFoundError(f"Symbol file not found: {self.SYMBOL_FILE}")
        
        # Ensure output directory exists or can be created
        output_path = Path(self.OUTPUT_DIR)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Ensure logs directory exists
        log_path = Path(self.LOG_FILE).parent
        log_path.mkdir(parents=True, exist_ok=True)
        
        # ✅ Validate date formats to catch errors early
        try:
            datetime.strptime(self.START_DATE, '%Y%m%d')
        except ValueError as e:
            raise ValueError(f"Invalid START_DATE '{self.START_DATE}': {e}")
        
        try:
            datetime.strptime(self.END_DATE, '%Y%m%d')
        except ValueError as e:
            raise ValueError(f"Invalid END_DATE '{self.END_DATE}': {e}")
        
        return True
