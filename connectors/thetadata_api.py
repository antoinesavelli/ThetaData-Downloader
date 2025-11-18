"""
ThetaData API connector for making requests to Theta Terminal.
"""
import logging
import asyncio
from typing import Optional, Dict, Any, Union
from config.settings import Config
import httpx

from .thetadata_mcp import ThetaDataMCP, HAS_MCP


class ThetaDataAPI:
    """
    High-level connector for ThetaData API, supporting both REST and MCP protocols.
    Acts as a facade for both REST API and MCP functionality.
    """
    
    def __init__(self, config: Config, logger: logging.Logger):
        """Initialize API connector with configuration."""
        self.config = config
        self.logger = logger
        self.base_url = config.THETADATA_BASE_URL
        self.endpoint = config.THETADATA_ENDPOINT
        self.timeout = config.REQUEST_TIMEOUT
        
        # ✅ OPTIMIZED: Configure httpx with connection pooling and limits
        limits = httpx.Limits(
            max_connections=200,  # Total connection pool size
            max_keepalive_connections=100,  # Keep-alive connections
            keepalive_expiry=30.0  # Keep connections alive for 30s
        )
        
        # Session for REST API with optimized settings
        self.session = httpx.AsyncClient(
            timeout=self.timeout,
            limits=limits,
            http2=True  # Enable HTTP/2 for multiplexing
        )
        
        # MCP client
        self.use_mcp = config.USE_MCP
        self.mcp_client = ThetaDataMCP(config, logger) if self.use_mcp else None

    async def check_terminal_running(self) -> bool:
        """
        Check if the Theta Terminal is running and accessible.
        
        Returns:
            bool: True if terminal is running and accessible, False otherwise
        """
        try:
            async with asyncio.timeout(self.timeout):
                if self.use_mcp:
                    if not HAS_MCP:
                        self.logger.error("MCP support not available - check if mcp package is installed")
                        return False
                    return await self.mcp_client.check_connection()
                
                # For REST API, attempt a basic health check
                response = await self.session.get(f"{self.base_url}/health")
                return response.status_code == 200
                
        except asyncio.TimeoutError:
            self.logger.error("Terminal connection check timed out")
            return False
        except Exception as e:
            self.logger.error(f"Terminal connection check failed: {e}")
            return False

    async def fetch_market_data(self, symbol: str, date: str, start_time: str = None, end_time: str = None) -> Optional[Union[Dict[str, Any], str]]:
        """
        Fetch market data using either MCP or REST API.
        
        Args:
            symbol: Stock symbol
            date: Date in YYYYMMDD format
            start_time: Optional start time (HH:MM:SS)
            end_time: Optional end time (HH:MM:SS)
            
        Returns:
            Optional[Union[Dict[str, Any], str]]: Market data (JSON dict/list or CSV string) if successful, None otherwise
        """
        try:
            if self.use_mcp and self.mcp_client:
                return await self.mcp_client.fetch_market_data(symbol, date, start_time, end_time)
            
            # Implement REST API fetch if needed
            # TODO: Implement REST API data fetching
            self.logger.error("REST API fetch not yet implemented")
            return None
            
        except Exception as e:
            self.logger.error(f"Error fetching market data for {symbol}: {e}")
            return None

    def _build_request_params(self, symbol: str, date: str, start_time: str = None, end_time: str = None) -> Dict[str, str]:
        """Build request parameters."""
        params = {
            "symbol": symbol,
            "date": date,
            "interval": self.config.INTERVAL,
        }
        
        # ✅ Add format parameter
        if hasattr(self.config, 'RESPONSE_FORMAT') and self.config.RESPONSE_FORMAT:
            params["format"] = self.config.RESPONSE_FORMAT
        
        if start_time:
            params["start_time"] = start_time
        
        if end_time:
            params["end_time"] = end_time
        
        if self.config.VENUE:
            params["venue"] = self.config.VENUE
        
        return params

    async def close(self):
        """
        Cleanup resources and close all connections.
        """
        # Close REST API session
        if self.session:
            try:
                await asyncio.wait_for(self.session.aclose(), timeout=2.0)
            except asyncio.TimeoutError:
                self.logger.warning("REST API session cleanup timed out")
            except Exception as e:
                self.logger.debug(f"Error closing REST session: {e}")
        
        # Close MCP client
        if self.mcp_client:
            try:
                await asyncio.wait_for(self.mcp_client.close(), timeout=2.0)
            except asyncio.TimeoutError:
                self.logger.warning("MCP client cleanup timed out")
            except Exception as e:
                self.logger.debug(f"Error closing MCP client: {e}")