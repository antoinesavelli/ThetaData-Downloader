"""
ThetaData MCP protocol connector for Theta Terminal v3.
"""
import logging
import asyncio
from typing import Optional, Dict, Any, List, Union
from datetime import datetime, timedelta
from config.settings import Config
from utils.diagnostics import MCPResponseHandler, MCPToolExtractor, ErrorTracer
import orjson  # Replace standard json with orjson

# anyio may be used by httpx/httpcore/mcp stack for cancellation semantics
try:
    import anyio
except Exception:
    anyio = None

# Optional runtime imports for explicit timeout exception mapping
try:
    import httpx
except Exception:
    httpx = None
try:
    import httpcore
except Exception:
    httpcore = None

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
    """MCP client for ThetaData API.""" 
    
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
        self._tool_name = None
        self._lock = asyncio.Lock()
        self._request_timeout = getattr(config, 'REQUEST_TIMEOUT', 30)
        self._sse_context = None
        self._session_context = None
        
        # ✅ Error rate limiting
        self._error_count = 0
        self._error_threshold = 20
        self._cooldown_duration = 60  # 1 minute in seconds
        self._last_error_reset = datetime.now()
        self._error_lock = asyncio.Lock()  # Protect error counter in concurrent environment
        
        # Initialize diagnostic helpers - DISABLE debug file saving
        self.response_handler = MCPResponseHandler(logger, save_debug_files=False)
        self.tool_extractor = MCPToolExtractor(logger)
        
        self.logger.info(f"Initializing MCP client with URL: {self.mcp_url}")

    async def check_connection(self) -> bool:
        """Check if connection to MCP server can be established.""" 
        try:
            return await self.connect()
        except asyncio.TimeoutError:
            self.logger.error("Connection check timed out")
            return False
        except Exception as e:
            self.logger.error(f"MCP connection check failed: {e}")
            return False

    async def connect(self) -> bool:
        """Establish connection to MCP server.""" 
        if not HAS_MCP:
            self.logger.error("MCP package not installed")
            return False
            
        async with self._lock:
            if self._session and not self._session.closed:
                return True
            
            try:
                self.logger.debug(f"Attempting connection to {self.mcp_url}")
                
                # Check if port is accessible
                try:
                    _, writer = await asyncio.open_connection(
                        self.config.get('MCP_HOST', 'localhost'),
                        self.config.get('MCP_PORT', 25503)
                    )
                    writer.close()
                    await writer.wait_closed()
                except Exception as e:
                    self.logger.error(f"ThetaTerminal not available at {self.mcp_url}: {str(e)}")
                    return False
                
                async with asyncio.timeout(self._request_timeout):
                    # Create SSE client connection with extended timeouts
                    # Default sse_client timeouts are too short (5s request, 300s read)
                    # When Theta Terminal queue backs up, reads take longer
                    self._sse_context = sse_client(
                        self.mcp_url,
                        timeout=self._request_timeout,  # HTTP request timeout (default was 5s)
                        sse_read_timeout=self._request_timeout * 3  # SSE read timeout (default was 300s)
                    )
                    reader, writer = await self._sse_context.__aenter__()
                    self.logger.debug("SSE client connection established")
                    
                    # Create and initialize client session
                    self._session_context = ClientSession(reader, writer)
                    self._session = await self._session_context.__aenter__()
                    
                    # Initialize session and get tools
                    await self._session.initialize()
                    self.logger.debug("Session initialized")
                    
                    tools_response = await self._session.list_tools()
                    
                    # TEMPORARY: Log tool schema for stock_history_ohlc
                    if hasattr(tools_response, 'tools'):
                        for tool in tools_response.tools:
                            if hasattr(tool, 'name') and 'stock_history_ohlc' in tool.name:
                                self.logger.info(f"=== TOOL SCHEMA FOR {tool.name} ===")
                                if hasattr(tool, 'inputSchema'):
                                    self.logger.info(f"Input Schema: {tool.inputSchema}")
                                self.logger.info(f"=== END TOOL SCHEMA ===")
                    
                    # Process tools using diagnostic helper
                    available_tools = await self._extract_available_tools(tools_response)
                    if not available_tools:
                        self.logger.error("No tools found in MCP response")
                        await self._cleanup()
                        return False
                    
                    self._tool_name = await self._find_required_tool(available_tools)
                    if not self._tool_name:
                        await self._cleanup()
                        return False
                        
                    self.logger.info("✓ MCP connection established successfully")
                    return True
                    
            except asyncio.TimeoutError:
                self.logger.error(f"Connection attempt timed out after {self._request_timeout}s")
                await self._cleanup()
                return False
            except Exception as e:
                root_cause = ErrorTracer.get_root_cause(e)
                self.logger.error(f"Connection failed: {type(root_cause).__name__}: {str(root_cause)}")
                await self._cleanup()
                return False

    async def _extract_available_tools(self, tools_response: Any) -> List[str]:
        """Extract available tools from response.""" 
        return self.tool_extractor.extract_tools(tools_response)
        
    async def _find_required_tool(self, available_tools: List[str]) -> Optional[str]:
        """Find required tool from available tools.""" 
        required_tools = [
            "stock_history_ohlc",
            "stock.history.ohlc",
            "stock/history/ohlc",
            "stock_history_minute",
            "stock.history.minute"
        ]
        return self.tool_extractor.find_required_tool(available_tools, required_tools)

    async def _cleanup(self):
        """Clean up all resources properly.""" 
        try:
            if self._session_context:
                try:
                    await self._session_context.__aexit__(None, None, None)
                except Exception as e:
                    self.logger.debug(f"Session context cleanup: {e}")
                finally:
                    self._session_context = None
                    self._session = None
            
            if self._sse_context:
                try:
                    await self._sse_context.__aexit__(None, None, None)
                except Exception as e:
                    self.logger.debug(f"SSE context cleanup: {e}")
                finally:
                    self._sse_context = None
                    
        except Exception as e:
            self.logger.debug(f"Cleanup error: {e}")

    async def _increment_error_count(self) -> bool:
        """
        Increment error count and check if cooldown is needed.
        
        Returns:
            True if cooldown should be triggered, False otherwise
        """ 
        async with self._error_lock:
            self._error_count += 1
            
            if self._error_count >= self._error_threshold:
                self.logger.warning(
                    f"⚠️ Error threshold reached ({self._error_count} errors). "
                    f"Pausing for {self._cooldown_duration}s..."
                )
                return True
            
            self.logger.debug(f"Error count: {self._error_count}/{self._error_threshold}")
            return False

    async def _reset_error_count(self):
        """Reset error count after successful request.""" 
        async with self._error_lock:
            if self._error_count > 0:
                self.logger.debug(f"Resetting error count from {self._error_count} to 0")
                self._error_count = 0
                self._last_error_reset = datetime.now()

    async def _apply_cooldown(self):
        """Apply cooldown period and reset error counter.""" 
        async with self._error_lock:
            self.logger.warning(
                f"🕐 Entering cooldown period ({self._cooldown_duration}s) "
                f"after {self._error_count} consecutive errors"
            )
            error_count_before = self._error_count
            self._error_count = 0
            self._last_error_reset = datetime.now()
        
        try:
            # Sleep outside the lock to allow other operations
            await asyncio.sleep(self._cooldown_duration)
            self.logger.info(
                f"✓ Cooldown period completed. Resuming operations "
                f"(had {error_count_before} errors)"
            )
        except asyncio.CancelledError:
            self.logger.debug("Cooldown cancelled")
            raise

    def _is_cancellation(self, exc: BaseException) -> bool:
        """
        Detect cancellation exceptions coming from asyncio/anyio/httpx/httpcore layers.

        Returns True if exc should be treated as a cancellation (and re-raised).
        Behaviour:
         - If the current asyncio Task has been explicitly cancelled, treat as cancellation.
         - Do NOT treat internal anyio/httpx "cancel scope" cancellations or WouldBlock as
           coordinator cancellations unless the current Task is cancelled.
        """
        # If this is a native asyncio CancelledError, only propagate when the current task is cancelled.
        if isinstance(exc, asyncio.CancelledError):
            try:
                task = asyncio.current_task()
                if task is not None and task.cancelled():
                    return True
            except Exception:
                # defensive: if we can't determine task state, fall back to propagating
                return True
            return False

        # anyio exposes the concrete cancellation exception class via get_cancelled_exc()
        if anyio is not None:
            try:
                cancel_exc = anyio.get_cancelled_exc()
                if cancel_exc is not None and isinstance(exc, cancel_exc):
                    try:
                        task = asyncio.current_task()
                        if task is not None and task.cancelled():
                            return True
                    except Exception:
                        return True
                    return False
            except Exception:
                # defensive: if anyio API differs, fall back to message checks below
                pass

        # Heuristic: only treat explicit cancel scopes/cancellation messages as cancellation
        # when the current task was cancelled. This avoids treating internal HTTP/IO read
        # timeouts (which sometimes raise "cancel scope" messages) as coordinator cancellations.
        try:
            text = str(exc)
            if "cancel scope" in text.lower() or "cancelled via cancel scope" in text.lower():
                try:
                    task = asyncio.current_task()
                    if task is not None and task.cancelled():
                        return True
                except Exception:
                    return True
                return False
        except Exception:
            pass

        return False

    async def fetch_market_data(self, symbol: str, date: str, start_time: str = None, end_time: str = None, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """Fetch market data using MCP protocol with retry logic and error rate limiting.""" 
        
        for attempt in range(max_retries):
            try:
                # Check if session is valid, reconnect if needed
                if not self._session or (hasattr(self._session, 'closed') and self._session.closed):
                    self.logger.warning(f"[{symbol}] Session closed, attempting reconnect (attempt {attempt + 1}/{max_retries})")
                    
                    try:
                        if not await self.connect():
                            if attempt < max_retries - 1:
                                await asyncio.sleep(2 ** attempt)  # Exponential backoff
                                continue
                            else:
                                self.logger.error(f"[{symbol}] Failed to reconnect after {max_retries} attempts")
                                # Count connection failure as error
                                if await self._increment_error_count():
                                    await self._apply_cooldown()
                                return None
                    except asyncio.CancelledError:
                        # Cancelled during reconnection
                        self.logger.debug(f"[{symbol}] Cancelled during reconnection")
                        raise
    
                args = self._build_request_args(symbol, date, start_time, end_time)
                if not args:
                    return None
                
                self.logger.debug(f"[{symbol}] Requesting data (attempt {attempt + 1}/{max_retries}) with args: {args}")
                
                # Add timeout to tool call - handle anyio/httpx cancellations explicitly
                try:
                    async with asyncio.timeout(self._request_timeout):
                        response = await self._session.call_tool(self._tool_name, arguments=args)
                except asyncio.CancelledError as ce:
                    # Distinguish between:
                    #  - explicit task cancellation triggered by coordinator/user (propagate)
                    #  - internal anyio/httpx "cancel scope" used for timeouts (map to transient read timeout)
                    text = str(ce).lower() if ce is not None else ""
                    try:
                        task = asyncio.current_task()
                        explicitly_cancelled = task is not None and task.cancelled()
                    except Exception:
                        explicitly_cancelled = False

                    if explicitly_cancelled:
                        self.logger.debug(f"[{symbol}] Request cancelled during API call (explicit task cancel)")
                        raise  # propagate real coordinator/user cancellation

                    # Heuristic: internal cancel scopes include "cancel scope" / "cancelled via cancel scope"
                    if "cancel scope" in text or "cancelled via cancel scope" in text:
                        # Map internal cancel-scope to transient read timeout so outer logic retries
                        self.logger.debug(f"[{symbol}] Internal cancel-scope CancelledError mapped to transient read timeout")
                        raise RuntimeError("ReadTimeout (mapped from cancel-scope CancelledError)") from ce

                    # Otherwise be conservative and propagate
                    self.logger.debug(f"[{symbol}] Request cancelled during API call (asyncio.CancelledError) - propagating")
                    raise

                self.logger.debug(f"[{symbol}] Raw response type: {type(response)}")
                
                result = await self._handle_rpc_response(response, symbol)
                
                # If successful, return result
                if result is not None:
                    # ✅ Reset error count on success
                    await self._reset_error_count()
                    return result
                
                # If no data but no error, return None (don't retry)
                return None
                
            except asyncio.CancelledError:
                # ✅ CRITICAL: Propagate cancellation immediately - DO NOT RETRY
                self.logger.debug(f"[{symbol}] Request cancelled by coordinator")
                raise  # Re-raise to propagate cancellation
                
            except asyncio.TimeoutError:
                # ✅ Handle timeout separately from cancellation
                self.logger.warning(f"[{symbol}] Request timeout (attempt {attempt + 1}/{max_retries})")
                
                # Count timeout as error
                if await self._increment_error_count():
                    await self._apply_cooldown()
                
                if attempt < max_retries - 1:
                    try:
                        await asyncio.sleep(2 ** attempt)
                    except asyncio.CancelledError:
                        # If cancelled during sleep, propagate immediately
                        self.logger.debug(f"[{symbol}] Cancelled during retry backoff")
                        raise
                    continue
                else:
                    self.logger.error(f"[{symbol}] Failed after {max_retries} timeout attempts")
                    return None
        
            except Exception as e:
                # If this exception is actually an anyio/httpx cancellation, propagate immediately.
                if self._is_cancellation(e):
                    self.logger.debug(f"[{symbol}] Detected cancellation in exception handler ({type(e).__name__}), propagating")
                    raise

                error_type = type(e).__name__
                error_message = str(e)

                # Check if it's a connection error that should trigger retry
                # Includes transient errors mapped from internal cancel-scope timeouts
                connection_errors = ['ClosedResourceError', 'McpError', 'ConnectionError', 'BrokenPipeError', 'ReadTimeout', 'RuntimeError']
                is_connection_error = any(err in error_type or err in error_message for err in connection_errors)

                # Specifically handle mapped cancel-scope timeouts as connection errors
                if 'ReadTimeout' in error_message and 'cancel-scope' in error_message:
                    is_connection_error = True
                
                if is_connection_error:
                    self.logger.warning(f"[{symbol}] Connection error: {error_type} (attempt {attempt + 1}/{max_retries})")
                    
                    # Count connection error
                    if await self._increment_error_count():
                        await self._apply_cooldown()
                    
                    # Force cleanup and reconnect
                    await self._cleanup()
                    
                    if attempt < max_retries - 1:
                        backoff = 2 ** attempt
                        self.logger.info(f"[{symbol}] Waiting {backoff}s before retry...")
                        try:
                            await asyncio.sleep(backoff)
                        except asyncio.CancelledError:
                            # If cancelled during sleep, propagate immediately
                            self.logger.debug(f"[{symbol}] Cancelled during retry backoff")
                            raise
                        continue
                    else:
                        self.logger.error(f"[{symbol}] Failed after {max_retries} attempts: {e}")
                        ErrorTracer.log_exception(self.logger, e, f"MCP data fetch error for {symbol}")
                        return None
                else:
                    # Non-connection error, don't retry but still count
                    self.logger.error(f"[{symbol}] Non-retryable error: {error_type}")
                    ErrorTracer.log_exception(self.logger, e, f"MCP data fetch error for {symbol}")
                    
                    # Count non-retryable errors too
                    if await self._increment_error_count():
                        await self._apply_cooldown()
                    
                    return None

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
                self.logger.debug(f"[{symbol}] Using response format: {self.config.RESPONSE_FORMAT}")
            
            if start_time:
                args["start_time"] = str(start_time).strip()
                self.logger.debug(f"[{symbol}] Adding start_time: {start_time}")
            
            if end_time:
                args["end_time"] = str(end_time).strip()
                self.logger.debug(f"[{symbol}] Adding end_time: {end_time}")
            
            if hasattr(self.config, 'VENUE') and self.config.VENUE:
                args["venue"] = str(self.config.VENUE).strip()
            
            return args
            
        except Exception as e:
            self.logger.error(f"Error building request args for {symbol}: {e}")
            return None

    async def _handle_rpc_response(self, response: Dict[str, Any], symbol: str) -> Optional[Union[Dict[str, Any], str]]:
        """Handle and validate JSON-RPC response (supports both JSON and CSV).""" 
        try:
            # Log detailed response for debugging specific symbols
            if symbol in ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']:
                self.response_handler.log_detailed_response(response, symbol)
        
            # Check for errors first
            error_message = self.response_handler.check_error_response(response, symbol)
            if error_message:
                self.logger.error(f"MCP tool error for {symbol}: {error_message}")
                return None
        
            # Extract text content
            text_content = self.response_handler.extract_text_content(response)
            self.logger.debug(f"[{symbol}] Text content extracted: {text_content is not None}")
        
            if text_content is not None:
                self.logger.debug(f"[{symbol}] Text length: {len(text_content)}, starts with: {text_content[:50] if text_content else 'N/A'}")
            
                # ✅ NEW: Detect if response is CSV format
                is_csv = self._detect_csv_format(text_content)
            
                if is_csv:
                    self.logger.info(f"[{symbol}] ✓ Detected CSV format, returning raw text for parser")
                    # Return raw CSV string so parse_market_data can handle it
                    return text_content
                else:
                    # Parse as JSON
                    result = self.response_handler.parse_json_response(text_content, symbol)
                
                    self.logger.debug(f"[{symbol}] Parse result type: {type(result)}, is None: {result is None}")
                
                    if result is not None:
                        if isinstance(result, list):
                            self.logger.info(f"[{symbol}] ✓ Successfully parsed {len(result)} records")
                        elif isinstance(result, dict):
                            self.logger.info(f"[{symbol}] ✓ Successfully parsed dict with keys: {list(result.keys())}")
                        return result
                    else:
                        self.logger.warning(f"[{symbol}] parse_json_response returned None")
                        return None
            else:
                self.logger.warning(f"[{symbol}] No text content extracted")
        
            # Fallback to dict-based response
            if isinstance(response, dict):
                if 'result' in response:
                    result = response['result']
                    if result and result != {} and result != []:
                        self.logger.info(f"[{symbol}] Using fallback dict result")
                        return result
                    self.logger.debug(f"[{symbol}] Empty result in fallback")
                else:
                    self.logger.debug(f"[{symbol}] No 'result' key in fallback")
                return None
        
            self.logger.warning(f"[{symbol}] Unexpected response format: {type(response)}")
            return None
        
        except Exception as e:
            ErrorTracer.log_exception(self.logger, e, f"Error processing RPC response for {symbol}")
            return None

    def _detect_csv_format(self, text: str) -> bool:
        """
        Detect if text content is CSV format.
        
        Args:
            text: Text content to check
            
        Returns:
            True if CSV format, False otherwise
        """
        if not text or len(text.strip()) == 0:
            return False
        
        # Check first line for CSV indicators
        first_line = text.split('\n')[0].strip()
        
        # CSV typically has:
        # 1. Column headers with commas
        # 2. Common OHLC column names
        csv_indicators = [
            'timestamp' in first_line.lower(),
            'open' in first_line.lower(),
            'high' in first_line.lower(),
            'low' in first_line.lower(),
            'close' in first_line.lower(),
            'volume' in first_line.lower(),
            ',' in first_line,  # Has commas
            not first_line.startswith('['),  # Doesn't start with JSON array
            not first_line.startswith('{')   # Doesn't start with JSON object
        ]
        
        # If at least 5 indicators are true, it's likely CSV
        return sum(csv_indicators) >= 5

    async def close(self):
        """Close the MCP session.""" 
        try:
            await asyncio.wait_for(self._cleanup(), timeout=2.0)
        except asyncio.TimeoutError:
            self.logger.warning("MCP cleanup timed out")
        except Exception as e:
            self.logger.debug(f"Error during MCP cleanup: {e}")