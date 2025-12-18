"""
ThetaData MCP protocol connector for Theta Terminal v3.
OPTIMIZED: Hardcoded tool name - NO tool discovery overhead.
FIXED: Properly handles ExceptionGroup errors from Python 3.11+.
FIXED: Detects and handles 500 Server Error with extended cooldown.
"""
import logging
import asyncio
from typing import Optional, Dict, Any, List, Union
from datetime import datetime, timedelta
from config.settings import Config
from utils.diagnostics import MCPResponseHandler, ErrorTracer
import uuid
import sys

# MCP imports
try:
    from mcp import ClientSession
    from mcp.client.sse import sse_client
    HAS_MCP = True
except ImportError:
    HAS_MCP = False
    ClientSession = None  # type: ignore
    
    def sse_client(url: str):  # type: ignore
        raise ImportError("MCP package not installed")


class ThetaDataMCP:
    """MCP client for ThetaData API - OPTIMIZED with hardcoded tool name.""" 
    
    # ✅ HARDCODED TOOL NAME - No discovery needed!
    _TOOL_NAME = "stock_history_ohlc"  # Standard ThetaData MCP tool name
    
    def __init__(self, config: Config, logger: logging.Logger):
        """Initialize MCP client with configuration.""" 
        self.config = config
        self.logger = logger
        self.mcp_url = self.config.get('MCP_URL')
        if not self.mcp_url:
            host = self.config.get('MCP_HOST', 'localhost')
            port = self.config.get('MCP_PORT', 25503)
            self.mcp_url = f"ws://{host}:{port}/mcp/sse"
        
        self._session = None
        self._lock = asyncio.Lock()
        self._request_timeout = getattr(config, 'REQUEST_TIMEOUT', 30)
        self._sse_context = None
        self._session_context = None
        
        # ✅ Enhanced error rate limiting with server crash detection
        self._error_count = 0
        self._error_threshold = 15  # Lower threshold for server errors
        self._cooldown_duration = 120  # 2 minutes base cooldown
        self._last_error_reset = datetime.now()
        self._error_lock = asyncio.Lock()
        
        # ✅ Track server errors specifically
        self._server_error_count = 0  # Track 500 errors
        self._last_500_time = None
        
        # Initialize diagnostic helper - DISABLE debug file saving
        self.response_handler = MCPResponseHandler(logger, save_debug_files=False)
        
        self.logger.info(f"Initializing MCP client with URL: {self.mcp_url}")
        self.logger.info(f"Using hardcoded tool: {self._TOOL_NAME}")

        # ✅ Cache logger level check
        self._debug_enabled = logger.isEnabledFor(logging.DEBUG)

    def _extract_real_exception(self, e: Exception) -> Exception:
        """
        Extract the actual exception from an ExceptionGroup (Python 3.11+).
        
        ExceptionGroups wrap multiple exceptions. We extract the first one
        to get the real error message.
        
        Args:
            e: Exception (might be ExceptionGroup or regular exception)
            
        Returns:
            The first nested exception, or the original exception
        """
        # Check if it's an ExceptionGroup
        exception_type_name = type(e).__name__
        
        if exception_type_name in ('ExceptionGroup', 'BaseExceptionGroup'):
            # ExceptionGroup has 'exceptions' attribute
            if hasattr(e, 'exceptions') and e.exceptions:
                # Recursively extract from first exception
                first_exception = e.exceptions[0]
                return self._extract_real_exception(first_exception)
        
        # Return the exception as-is if not an ExceptionGroup
        return e

    async def check_connection(self) -> bool:
        """Check if connection to MCP server can be established.""" 
        try:
            if not HAS_MCP:
                self.logger.error("MCP package not installed")
                return False
            
            # Quick port check
            try:
                _, writer = await asyncio.open_connection(
                    self.config.get('MCP_HOST', 'localhost'),
                    self.config.get('MCP_PORT', 25503)
                )
                writer.close()
                await writer.wait_closed()
                return True
            except Exception as e:
                self.logger.error(f"Port check failed: {e}")
                return False
                
        except asyncio.TimeoutError:
            self.logger.error("Connection check timed out")
            return False
        except Exception as e:
            self.logger.error(f"MCP connection check failed: {e}")
            return False

    async def connect(self) -> bool:
        """Legacy compatibility method - checks connection availability."""
        return await self.check_connection()

    async def _cleanup(self):
        """Clean up all resources properly.""" 
        self.logger.debug("cleanup: start")
        try:
            if self._session_context:
                self.logger.debug("cleanup: closing session context")
                try:
                    await self._session_context.__aexit__(None, None, None)
                except Exception as e:
                    self.logger.debug(f"Session context cleanup: {e}")
                finally:
                    self._session_context = None
                    self._session = None
            
            if self._sse_context:
                self.logger.debug("cleanup: closing SSE context")
                try:
                    await self._sse_context.__aexit__(None, None, None)
                except Exception as e:
                    self.logger.debug(f"SSE context cleanup: {e}")
                finally:
                    self._sse_context = None
                    
        except Exception as e:
            self.logger.debug(f"Cleanup error: {e}")
        self.logger.debug("cleanup: end")

    async def _increment_error_count(self, is_server_error: bool = False) -> bool:
        """
        Increment error count and check if cooldown is needed.
        
        Args:
            is_server_error: True if this is a 500/502 server error
        
        Returns:
            True if cooldown should be triggered, False otherwise
        """ 
        async with self._error_lock:
            self._error_count += 1
            
            # Track server errors separately
            if is_server_error:
                self._server_error_count += 1
                self._last_500_time = datetime.now()
                
                # ✅ AGGRESSIVE: Trigger cooldown after just 2 consecutive 500 errors
                if self._server_error_count >= 2:
                    self.logger.error(
                        f"🚨 CRITICAL: Server crashing! {self._server_error_count} consecutive 500 errors. "
                        f"Triggering EXTENDED cooldown to let server recover..."
                    )
                    return True
            
            if self._error_count >= self._error_threshold:
                self.logger.warning(
                    f"⚠️ Error threshold reached ({self._error_count} errors). "
                    f"Pausing for {self._cooldown_duration}s..."
                )
                return True
            
            self.logger.debug(f"Error count: {self._error_count}/{self._error_threshold}, 500s: {self._server_error_count}")
            return False

    async def _reset_error_count(self):
        """Reset error count after successful request.""" 
        async with self._error_lock:
            if self._error_count > 0:
                self.logger.debug(f"Resetting error count from {self._error_count} to 0")
                self._error_count = 0
                self._server_error_count = 0  # Also reset server error counter
                self._last_error_reset = datetime.now()

    async def _apply_cooldown(self):
        """Apply cooldown period and reset error counter.""" 
        async with self._error_lock:
            # ✅ EXTENDED cooldown for server crashes
            cooldown_time = self._cooldown_duration
            if self._server_error_count >= 2:
                cooldown_time = 600  # 10 MINUTES for severe server crashes!
                self.logger.error(
                    f"💥 SERVER CRASH DETECTED! "
                    f"Entering EXTENDED cooldown ({cooldown_time}s = {cooldown_time//60} minutes) "
                    f"to allow Theta Terminal to recover..."
                )
            
            self.logger.warning(
                f"🕐 Entering cooldown period ({cooldown_time}s) "
                f"after {self._error_count} errors ({self._server_error_count} server crashes)"
            )
            error_count_before = self._error_count
            server_errors_before = self._server_error_count
            self._error_count = 0
            self._server_error_count = 0
            self._last_error_reset = datetime.now()
        
        try:
            await asyncio.sleep(cooldown_time)
            self.logger.info(
                f"✓ Cooldown period completed. Resuming operations "
                f"(had {error_count_before} errors, {server_errors_before} server crashes)"
            )
        except asyncio.CancelledError:
            self.logger.debug("Cooldown cancelled")
            return

    async def fetch_market_data(self, symbol: str, date: str, start_time: str = None, end_time: str = None, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """
        Fetch market data using fresh SSE session per request.
        NO TOOL DISCOVERY - uses hardcoded tool name.
        HANDLES ExceptionGroup properly.
        DETECTS 500 Server Errors and triggers extended cooldown.
        """
        req_id = self._make_req_id(symbol, date)
        
        for attempt in range(max_retries):
            session_ctx = None
            sse_ctx = None
            
            try:
                args = self._build_request_args(symbol, date, start_time, end_time)
                if not args:
                    return None
                
                if self._debug_enabled:
                    self.logger.debug(f"[{req_id}] Creating fresh SSE session (attempt {attempt + 1})")
                
                async with asyncio.timeout(self._request_timeout):
                    # Create new SSE connection
                    sse_ctx = sse_client(self.mcp_url)
                    reader, writer = await sse_ctx.__aenter__()
                    
                    # Create new session
                    session_ctx = ClientSession(reader, writer)
                    session = await session_ctx.__aenter__()
                    
                    # Initialize and call tool (using hardcoded name)
                    await session.initialize()
                    response = await session.call_tool(self._TOOL_NAME, arguments=args)
                    
                    # Process response
                    result = await self._handle_rpc_response(response, symbol)
                    if result is not None:
                        await self._reset_error_count()
                        return result
                    return None
                    
            except asyncio.CancelledError:
                self.logger.debug(f"[{req_id}] Cancelled by coordinator")
                raise
                
            except BaseException as e:  # ✅ Catch BaseException to handle ExceptionGroup
                # ✅ Extract the real exception from ExceptionGroup
                real_exception = self._extract_real_exception(e)
                error_type = type(real_exception).__name__
                error_msg = str(real_exception)[:400]  # Increased to 400 chars for full error
                
                # ✅ Detect server errors (500/502)
                is_server_error = (
                    "500" in error_msg or 
                    "502" in error_msg or 
                    "Server Error" in error_msg or
                    "Bad Gateway" in error_msg or
                    "HTTPStatusError" in error_type
                )
                
                # ✅ Check if it's a cancellation
                is_cancelled = isinstance(real_exception, asyncio.CancelledError)
                
                # Re-raise CancelledError immediately
                if is_cancelled:
                    raise
                
                # ✅ Log server errors at ERROR level always
                if is_server_error:
                    self.logger.error(
                        f"[{req_id}] 🚨 SERVER ERROR on attempt {attempt + 1}/{max_retries}: "
                        f"{error_type} - {error_msg}"
                    )
                elif attempt == 0 or attempt == max_retries - 1:
                    self.logger.warning(f"[{req_id}] {error_type} on attempt {attempt + 1}/{max_retries}: {error_msg}")
                else:
                    self.logger.debug(f"[{req_id}] {error_type} on attempt {attempt + 1}/{max_retries}")
                
                if await self._increment_error_count(is_server_error=is_server_error):
                    await self._apply_cooldown()
                
                if attempt < max_retries - 1:
                    # ✅ LONGER backoff for server errors
                    backoff = (2 ** attempt) * (5 if is_server_error else 1)
                    self.logger.debug(f"[{req_id}] Retrying after {backoff}s backoff")
                    await asyncio.sleep(backoff)
                    continue
                else:
                    self.logger.error(f"[{req_id}] Failed after {max_retries} attempts: {error_type}")
                    return None
                    
            finally:
                # CRITICAL: Always cleanup session immediately
                if session_ctx:
                    try:
                        await session_ctx.__aexit__(None, None, None)
                    except Exception:
                        pass
                if sse_ctx:
                    try:
                        await sse_ctx.__aexit__(None, None, None)
                    except Exception:
                        pass

    def _build_request_args(self, symbol: str, date: str, start_time: str = None, end_time: str = None) -> Optional[Dict[str, str]]:
        """Build and validate request arguments.""" 
        try:
            args = {
                "symbol": str(symbol).strip().upper(),
                "date": str(date).strip(),
                "interval": "1m"
            }
            
            # ✅ Add format parameter if configured
            if hasattr(self.config, 'RESPONSE_FORMAT') and self.config.RESPONSE_FORMAT:
                args["format"] = self.config.RESPONSE_FORMAT
            
            if start_time:
                args["start_time"] = str(start_time).strip()
            
            if end_time:
                args["end_time"] = str(end_time).strip()
            
            if hasattr(self.config, 'VENUE') and self.config.VENUE:
                args["venue"] = str(self.config.VENUE).strip()
            
            return args
            
        except Exception as e:
            self.logger.error(f"Error building request args for {symbol}: {e}")
            return None

    async def _handle_rpc_response(self, response: Dict[str, Any], symbol: str) -> Optional[Union[Dict[str, Any], str]]:
        """Handle and validate JSON-RPC response (supports both JSON and CSV).""" 
        try:
            # Check for errors first
            error_message = self.response_handler.check_error_response(response, symbol)
            if error_message:
                # ✅ Log server errors at ERROR level
                if "500" in error_message or "502" in error_message:
                    self.logger.error(f"MCP tool error for {symbol}: {error_message}")
                else:
                    self.logger.warning(f"MCP tool error for {symbol}: {error_message}")
                return None
        
            # Extract text content
            text_content = self.response_handler.extract_text_content(response)
        
            if text_content is not None:
                # ✅ Detect if response is CSV format
                is_csv = self._detect_csv_format(text_content)
            
                if is_csv:
                    self.logger.info(f"[{symbol}] ✓ Successfully received CSV ({len(text_content)} chars)")
                    return text_content
                else:
                    # Parse as JSON
                    result = self.response_handler.parse_json_response(text_content, symbol)
                
                    if result is not None:
                        if isinstance(result, list):
                            self.logger.info(f"[{symbol}] ✓ Successfully parsed {len(result)} records")
                        elif isinstance(result, dict):
                            self.logger.info(f"[{symbol}] ✓ Successfully parsed dict")
                        return result
                    return None
        
            # Fallback to dict-based response
            if isinstance(response, dict) and 'result' in response:
                result = response['result']
                if result and result != {} and result != []:
                    return result
            
            return None
        
        except Exception as e:
            ErrorTracer.log_exception(self.logger, e, f"Error processing RPC response for {symbol}")
            return None

    def _detect_csv_format(self, text: str) -> bool:
        """Detect if text content is CSV format."""
        if not text or len(text.strip()) == 0:
            return False
        
        first_line = text.split('\n')[0].strip()
        
        csv_indicators = [
            'timestamp' in first_line.lower(),
            'open' in first_line.lower(),
            'high' in first_line.lower(),
            'low' in first_line.lower(),
            'close' in first_line.lower(),
            'volume' in first_line.lower(),
            ',' in first_line,
            not first_line.startswith('['),
            not first_line.startswith('{')
        ]
        
        return sum(csv_indicators) >= 5

    async def close(self):
        """Close the MCP session.""" 
        try:
            await asyncio.wait_for(self._cleanup(), timeout=2.0)
        except asyncio.TimeoutError:
            self.logger.warning("MCP cleanup timed out")
        except Exception as e:
            self.logger.debug(f"Error during MCP cleanup: {e}")
    
    async def reset(self, force_reconnect: bool = False) -> bool:
        """Perform a full client reset - resets error counters."""
        self.logger.info("Performing MCP client reset (daily cleanup)")
        try:
            async with self._error_lock:
                self._error_count = 0
                self._server_error_count = 0
                self._last_error_reset = datetime.now()
            return True
        except Exception as e:
            self.logger.error(f"MCP reset failed: {e}")
            return False

    def _make_req_id(self, symbol: str, date: str) -> str:
        """Generate unique request ID for logging."""
        return f"{symbol}-{date}-{uuid.uuid4().hex[:8]}"