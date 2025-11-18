"""
Diagnostic utilities for connection troubleshooting.
"""
import socket
import requests
import psutil
import logging
import json
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Any
import os


class MCPResponseHandler:
    """Handle and parse MCP responses with detailed diagnostics."""
    
    def __init__(self, logger: logging.Logger, save_debug_files: bool = False):
        """
        Initialize response handler.
        
        Args:
            logger: Logger instance
            save_debug_files: If True, save debug JSON files to debug_output folder
        """
        self.logger = logger
        self.save_debug_files = save_debug_files
    
    def check_error_response(self, response: Any, symbol: str) -> Optional[str]:
        """
        Check if response indicates an error.
        
        Returns:
            Optional[str]: Error message if error detected, None otherwise
        """
        # Check isError flag
        if hasattr(response, 'isError') and response.isError:
            error_text = ""
            if hasattr(response, 'content') and isinstance(response.content, list):
                for item in response.content:
                    if hasattr(item, 'text'):
                        error_text = item.text
                        break
            return error_text
        
        # Check dict-based error
        if isinstance(response, dict) and 'error' in response:
            error = response['error']
            if isinstance(error, dict):
                code = error.get('code')
                message = error.get('message', 'Unknown error')
                return f"[{code}] {message}"
            return str(error)
        
        return None
    
    def extract_text_content(self, response: Any) -> Optional[str]:
        """
        Extract text content from MCP response.
        Concatenates all text items in the content array.
        
        Returns:
            Optional[str]: Extracted text or None
        """
        if hasattr(response, 'content'):
            content = response.content
            if isinstance(content, list) and len(content) > 0:
                text_parts = []
                for item in content:
                    if hasattr(item, 'type') and item.type == 'text':
                        if hasattr(item, 'text') and item.text:
                            # Skip whitespace-only items
                            if item.text.strip():
                                text_parts.append(item.text)
                
                if text_parts:
                    # Concatenate all text parts
                    combined_text = ''.join(text_parts)
                    return combined_text.strip()
        
        return None
    
    def parse_json_response(self, text: str, symbol: str) -> Optional[Any]:
        """
        Parse JSON from text content with robust error handling.
        
        Returns:
            Optional[Any]: Parsed JSON (list, dict, etc.) or None
        """
        if not text:
            self.logger.debug(f"[{symbol}] Empty text content")
            return None
        
        try:
            self.logger.debug(f"[{symbol}] Attempting to parse JSON, length: {len(text)}")
            result = json.loads(text)
            
            self.logger.debug(f"[{symbol}] JSON parsed successfully, type: {type(result)}")
            
            # Check if result is empty
            if result is None:
                self.logger.debug(f"[{symbol}] Result is None")
                return None
            
            if isinstance(result, list):
                if len(result) == 0:
                    self.logger.debug(f"[{symbol}] Empty list result")
                    return None
                self.logger.debug(f"[{symbol}] List with {len(result)} items")
                return result
            
            if isinstance(result, dict):
                if len(result) == 0:
                    self.logger.debug(f"[{symbol}] Empty dict result")
                    return None
                self.logger.debug(f"[{symbol}] Dict with {len(result)} keys")
                return result
            
            # For other types (string, number, etc.)
            self.logger.debug(f"[{symbol}] Returning result of type {type(result)}")
            return result
            
        except json.JSONDecodeError as e:
            # Try to fix the JSON issue first
            fixed_result = self._attempt_json_fix(text, symbol, e)
            
            # Only log error if fix failed
            if fixed_result is None:
                self.logger.error(f"[{symbol}] JSON decode error: {str(e)}")
                self.logger.debug(f"[{symbol}] Error location: line {e.lineno}, column {e.colno}, position {e.pos}")
                
                # Save malformed JSON to file for inspection (only if enabled)
                if self.save_debug_files:
                    try:
                        from pathlib import Path
                        debug_dir = Path("debug_output")
                        debug_dir.mkdir(exist_ok=True)
                        
                        json_file = debug_dir / f"{symbol}_malformed.json"
                        with open(json_file, 'w', encoding='utf-8') as f:
                            f.write(text)
                        self.logger.info(f"[{symbol}] Saved malformed JSON to {json_file}")
                        
                    except Exception as save_error:
                        self.logger.debug(f"[{symbol}] Could not save debug file: {save_error}")
                
                # Show context around the error
                if e.pos:
                    start = max(0, e.pos - 100)
                    end = min(len(text), e.pos + 100)
                    context = text[start:end]
                    self.logger.debug(f"[{symbol}] Context around error position {e.pos}:")
                    self.logger.debug(f"  ...{context}...")
            
            return fixed_result
            
        except Exception as e:
            self.logger.error(f"[{symbol}] Unexpected parse error: {str(e)}")
            return None
    
    def _attempt_json_fix(self, text: str, symbol: str, error: json.JSONDecodeError) -> Optional[Any]:
        """
        Attempt to fix common JSON formatting issues.
        
        Args:
            text: Original JSON text
            symbol: Stock symbol for logging
            error: The JSONDecodeError that occurred
            
        Returns:
            Parsed JSON if fix successful, None otherwise
        """
        try:
            import re
            
            original_text = text
            fixed_text = text.strip()
            
            self.logger.debug(f"[{symbol}] Attempting JSON fix...")
            self.logger.debug(f"[{symbol}] Text length: {len(fixed_text)}")
            self.logger.debug(f"[{symbol}] Starts with: '{fixed_text[:10]}'")
            self.logger.debug(f"[{symbol}] Ends with: '{fixed_text[-10:]}'")
            
            # MOST COMMON ISSUE: Array is missing closing bracket
            # Check if it starts with [ but ends with } instead of ]
            if fixed_text.startswith('['):
                if fixed_text.endswith('}'):
                    # Array is not closed - add the closing bracket
                    fixed_text += ']'
                    self.logger.debug(f"[{symbol}] Added missing array closing bracket ]")
                elif not fixed_text.endswith(']'):
                    # Ends with something else - find last } and close array there
                    last_brace = fixed_text.rfind('}')
                    if last_brace != -1:
                        fixed_text = fixed_text[:last_brace + 1] + ']'
                        self.logger.debug(f"[{symbol}] Truncated and added closing bracket ] at position {last_brace + 1}")
            
            # Fix missing commas between objects: }whitespace{ becomes },{
            before_comma_fix = fixed_text
            fixed_text = re.sub(r'}(\s+)\{', r'},\1{', fixed_text)
            if fixed_text != before_comma_fix:
                comma_count = before_comma_fix.count('}{')+before_comma_fix.count('}\n')+before_comma_fix.count('}\t')
                self.logger.debug(f"[{symbol}] Added missing commas between objects")
            
            # Remove double commas that might have been created
            fixed_text = re.sub(r',\s*,', ',', fixed_text)
            
            # Remove trailing commas before ] or }
            fixed_text = re.sub(r',(\s*])', r'\1', fixed_text)
            fixed_text = re.sub(r',(\s*})', r'\1', fixed_text)
            
            self.logger.debug(f"[{symbol}] After fixes, ends with: '{fixed_text[-10:]}'")
            
            # Try to parse the fixed JSON
            if fixed_text != original_text.strip():
                self.logger.debug(f"[{symbol}] Attempting to parse fixed JSON...")
                try:
                    result = json.loads(fixed_text)
                    
                    if isinstance(result, list) and len(result) > 0:
                        # Removed: Success message - data recovery is normal operation
                        self.logger.debug(f"[{symbol}] Successfully fixed and parsed {len(result)} records")
                        
                        # Save the fixed version (only if enabled)
                        if self.save_debug_files:
                            try:
                                debug_dir = Path("debug_output")
                                fixed_file = debug_dir / f"{symbol}_fixed.json"
                                with open(fixed_file, 'w', encoding='utf-8') as f:
                                    json.dump(result, f, indent=2)
                                self.logger.debug(f"[{symbol}] Saved fixed JSON to {fixed_file}")
                            except:
                                pass
                        
                        return result
                    
                except json.JSONDecodeError as parse_error:
                    self.logger.warning(f"[{symbol}] Fixed JSON still has errors: {parse_error}")
                    # Fall through to regex extraction
            else:
                self.logger.warning(f"[{symbol}] No changes made to text")
            
            # Last resort: Extract valid objects using regex
            self.logger.debug(f"[{symbol}] Attempting regex extraction as final fallback...")
            return self._extract_valid_objects(original_text, symbol)
            
        except Exception as e:
            self.logger.error(f"[{symbol}] Unexpected error in JSON fix: {type(e).__name__}: {str(e)}")
            return self._extract_valid_objects(original_text, symbol)
    
    def _extract_valid_objects(self, text: str, symbol: str) -> Optional[List[Dict]]:
        """
        Last resort: Extract all valid OHLC objects using regex.
        
        This method tries to salvage data when JSON is severely malformed.
        """
        try:
            import re
            self.logger.debug(f"[{symbol}] Attempting to extract valid objects with regex...")
            
            # Pattern to match complete OHLC objects
            # This matches: {"volume":...,"high":...,"low":...,"vwap":...,"count":...,"close":...,"open":...,"timestamp":"..."}
            pattern = r'\{"volume":\d+,"high":[\d.]+,"low":[\d.]+,"vwap":[\d.]+,"count":\d+,"close":[\d.]+,"open":[\d.]+,"timestamp":"[^"]+"\}'
            
            matches = re.findall(pattern, text)
            
            if matches:
                self.logger.debug(f"[{symbol}] Found {len(matches)} potential valid objects")
                
                # Parsing each matched object
                valid_objects = []
                for i, match in enumerate(matches):
                    try:
                        obj = json.loads(match)
                        valid_objects.append(obj)
                    except json.JSONDecodeError:
                        self.logger.debug(f"[{symbol}] Object {i} failed to parse, skipping")
                        continue
                
                if valid_objects:
                    # Removed: Success message - regex extraction is normal fallback operation
                    self.logger.debug(f"[{symbol}] Extracted {len(valid_objects)} valid records using regex")
                    
                    # Save extracted data (only if enabled)
                    if self.save_debug_files:
                        try:
                            debug_dir = Path("debug_output")
                            extracted_file = debug_dir / f"{symbol}_extracted.json"
                            with open(extracted_file, 'w', encoding='utf-8') as f:
                                json.dump(valid_objects, f, indent=2)
                            self.logger.debug(f"[{symbol}] Saved extracted data to {extracted_file}")
                        except:
                            pass
                    
                    return valid_objects
            
            self.logger.debug(f"[{symbol}] No valid objects found with regex")
            return None
            
        except Exception as e:
            self.logger.debug(f"[{symbol}] Regex extraction failed: {str(e)}")
            return None
    
    def log_detailed_response(self, response: Any, symbol: str):
        """Log detailed response information for debugging."""
        self.logger.info(f"=== FULL RESPONSE for {symbol} ===")
        self.logger.info(f"Response type: {type(response)}")
        self.logger.info(f"Response: {response}")
        
        if hasattr(response, '__dict__'):
            self.logger.info(f"Response attributes: {response.__dict__}")
        
        if hasattr(response, 'content'):
            self.logger.info(f"Content: {response.content}")
            if isinstance(response.content, list):
                for i, item in enumerate(response.content):
                    self.logger.info(f"Content item {i}: {item}")
                    if hasattr(item, '__dict__'):
                        self.logger.info(f"Content item {i} attributes: {item.__dict__}")
        
        self.logger.info(f"=== END RESPONSE for {symbol} ===")


class MCPToolExtractor:
    """Extract and validate MCP tools from responses."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def extract_tools(self, tools_response: Any) -> List[str]:
        """
        Extract available tools from MCP list_tools response.
        
        Returns:
            List[str]: List of available tool names
        """
        available_tools = []
        self.logger.debug(f"Tools response type: {type(tools_response)}")
        
        try:
            # Format 1: Object with tools attribute
            if hasattr(tools_response, 'tools'):
                available_tools = self._extract_from_tools_attribute(tools_response.tools)
            
            # Format 2: Dictionary with 'tools' key
            elif isinstance(tools_response, dict) and 'tools' in tools_response:
                available_tools = self._extract_from_dict(tools_response['tools'])
            
            # Format 3: List/tuple response
            elif isinstance(tools_response, (list, tuple)):
                available_tools = self._extract_from_list(tools_response)
            
            if not available_tools:
                self.logger.warning("No tools extracted from response")
                
        except Exception as e:
            self.logger.error(f"Error extracting tools: {type(e).__name__}: {str(e)}")
        
        return available_tools
    
    def _extract_from_tools_attribute(self, tools_list: Any) -> List[str]:
        """Extract tools from tools attribute."""
        tools = []
        self.logger.debug(f"Found tools attribute with {len(tools_list)} tools")
        
        if isinstance(tools_list, list):
            for tool in tools_list:
                if hasattr(tool, 'name'):
                    tools.append(tool.name)
                    self.logger.info(f"  • {tool.name}")
        
        return tools
    
    def _extract_from_dict(self, tools_list: Any) -> List[str]:
        """Extract tools from dictionary."""
        tools = []
        
        if isinstance(tools_list, list):
            for tool in tools_list:
                if isinstance(tool, str):
                    tools.append(tool)
                elif hasattr(tool, 'name'):
                    tools.append(tool.name)
                elif isinstance(tool, dict) and 'name' in tool:
                    tools.append(tool['name'])
        
        return tools
    
    def _extract_from_list(self, tools_response: Any) -> List[str]:
        """Extract tools from list/tuple response."""
        tools = []
        
        for item in tools_response:
            if isinstance(item, tuple) and len(item) > 1:
                if item[0] == 'tools' and isinstance(item[1], list):
                    for tool in item[1]:
                        if hasattr(tool, 'name'):
                            tools.append(tool.name)
            elif hasattr(item, 'name'):
                tools.append(item.name)
        
        return tools
    
    def find_required_tool(self, available_tools: List[str], required_tools: List[str]) -> Optional[str]:
        """
        Find first matching required tool from available tools.
        
        Args:
            available_tools: List of available tool names
            required_tools: List of required tool names in priority order
            
        Returns:
            Optional[str]: First matching tool name or None
        """
        for tool in required_tools:
            if tool in available_tools:
                self.logger.info(f"✓ Using tool: {tool}")
                return tool
        
        self.logger.error("❌ Required MCP tools not found")
        self.logger.error(f"Available tools: {', '.join(available_tools)}")
        return None


class ErrorTracer:
    """Trace and log errors with detailed context."""
    
    @staticmethod
    def get_root_cause(exception: Exception) -> Exception:
        """Get the root cause of an exception chain."""
        root_cause = exception
        while getattr(root_cause, '__cause__', None):
            root_cause = root_cause.__cause__
        return root_cause
    
    @staticmethod
    def format_exception(exception: Exception) -> str:
        """Format exception with full traceback."""
        return traceback.format_exc()
    
    @staticmethod
    def log_exception(logger: logging.Logger, exception: Exception, context: str = ""):
        """Log exception with context and traceback."""
        root_cause = ErrorTracer.get_root_cause(exception)
        logger.error(f"{context}: {type(root_cause).__name__}: {str(root_cause)}")
        logger.debug(f"Traceback: {ErrorTracer.format_exception(exception)}")


class ConnectionDiagnostics:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        
    def check_process(self, process_name: str = "java") -> bool:
        """Check if Theta Terminal service is accessible."""
        # 1. First check if port 25503 is open and get process info
        port_check = self.test_port('127.0.0.1', 25503)
        if not port_check['open']:
            self.logger.error("   Port 25503 is not accessible")
            self.logger.error("   Please ensure Theta Terminal is running on port 25503")
            return False
        
        self.logger.info("   ✓ Port 25503 is open and listening")
        
        # Get process info for port 25503
        try:
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    connections = proc.connections()
                    for conn in connections:
                        if conn.laddr.port == 25503:
                            self.logger.info(f"   ✓ Found process using port 25503:")
                            self.logger.info(f"     - PID: {proc.pid}")
                            self.logger.info(f"     - Name: {proc.name()}")
                            self.logger.info(f"     - Command: {' '.join(proc.cmdline())}")
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
        except Exception as e:
            self.logger.warning(f"   Unable to get process details: {e}")

        # 2. Try a basic API request with more detailed error reporting
        test_url = "http://localhost:25503/v3/stock/list/symbols"
        self.logger.info(f"\n   Testing API endpoint: {test_url}")
        api_result = self.test_api_endpoint(test_url)
        
        if api_result['accessible']:
            self.logger.info("   ✓ API is responding successfully")
            return True
        
        # 3. If API test fails, provide detailed error information
        self.logger.error("\n   API endpoint test failed:")
        if api_result['status_code']:
            self.logger.error(f"     - Status Code: {api_result['status_code']}")
        if api_result['error']:
            self.logger.error(f"     - Error: {api_result['error']}")
        if api_result['response']:
            self.logger.error(f"     - Response: {api_result['response']}")
        
        self.logger.error("\n   Troubleshooting steps:")
        self.logger.error("   1. Verify Theta Terminal is properly initialized")
        self.logger.error("   2. Check if authentication is required")
        self.logger.error("   3. Try accessing the endpoint in a browser: http://localhost:25503/v3/stock/list/symbols")
        
        return False
    
    def test_port(self, host: str, port: int, timeout: float = 2.0) -> Dict:
        """Test port connectivity with detailed diagnostics."""
        result = {
            'open': False,
            'error': None,
            'details': None
        }
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        
        try:
            conn_result = sock.connect_ex((host, port))
            if conn_result == 0:
                result['open'] = True
                result['details'] = 'Port is open and accepting connections'
            else:
                result['error'] = f'Connection failed with error code: {conn_result}'
        except socket.timeout:
            result['error'] = 'Connection timed out'
        except socket.gaierror:
            result['error'] = 'Address resolution error'
        except socket.error as e:
            result['error'] = f'Socket error: {str(e)}'
        finally:
            sock.close()
            
        return result
    
    def test_api_endpoint(self, url: str, timeout: float = 5.0) -> Dict:
        """Test API endpoint with detailed diagnostics."""
        result = {
            'accessible': False,
            'status_code': None,
            'error': None,
            'response': None
        }
        
        try:
            response = requests.get(url, timeout=timeout)
            result['status_code'] = response.status_code
            result['accessible'] = response.status_code == 200
            try:
                result['response'] = response.json()
            except:
                result['response'] = response.text[:100]  # First 100 chars
        except requests.exceptions.Timeout:
            result['error'] = 'Request timed out'
        except requests.exceptions.ConnectionError:
            result['error'] = 'Connection error'
        except requests.exceptions.RequestException as e:
            result['error'] = f'Request failed: {str(e)}'
        
        return result

    def check_file_permissions(self, path: Path) -> Dict:
        """Check if we have necessary file permissions."""
        result = {
            'exists': False,
            'readable': False,
            'writable': False,
            'error': None
        }
        
        try:
            result['exists'] = path.exists()
            if result['exists']:
                result['readable'] = os.access(path, os.R_OK)
                result['writable'] = os.access(path, os.W_OK)
        except Exception as e:
            result['error'] = str(e)
            
        return result