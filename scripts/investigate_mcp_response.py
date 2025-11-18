"""
Deep investigation of MCP response to check if volume data is being returned.
"""
import asyncio
import sys
from pathlib import Path
import json

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import Config
from utils.logger import setup_logger


async def investigate_mcp_tool(symbol: str, date: str):
    """
    Investigate the raw MCP tool response to check for volume data.
    
    Args:
        symbol: Stock symbol
        date: Date in YYYYMMDD format
    """
    config = Config()
    logger = setup_logger(
        name="mcp_investigation",
        log_file="logs/mcp_investigation.log",
        log_level="DEBUG"  # Set to DEBUG for maximum detail
    )
    
    print("=" * 80)
    print(f"INVESTIGATING MCP TOOL RESPONSE: {symbol} on {date}")
    print("=" * 80)
    print()
    
    # Import MCP client
    from connectors.thetadata_mcp import ThetaDataMCP
    
    mcp = ThetaDataMCP(config, logger)
    
    try:
        # Step 1: Connect to MCP
        print("Step 1: Connecting to ThetaData MCP...")
        if not await mcp.connect():
            print("❌ Failed to connect to MCP server")
            return
        print("✓ Connected successfully")
        print()
        
        # Step 2: Check the tool schema
        print("=" * 80)
        print("Step 2: Checking MCP Tool Schema")
        print("=" * 80)
        
        # The tool schema was logged during connection (check logs)
        print("Tool name being used:", mcp._tool_name)
        print()
        print("⚠️  Check the log file for the full tool schema:")
        print(f"   {config.LOG_FILE}")
        print()
        print("Look for: '=== TOOL SCHEMA FOR stock_history_ohlc ==='")
        print()
        
        # Step 3: Build request arguments
        print("=" * 80)
        print("Step 3: Building Request Arguments")
        print("=" * 80)
        
        args = mcp._build_request_args(symbol, date, config.START_TIME, config.END_TIME)
        print("Request arguments:")
        print(json.dumps(args, indent=2))
        print()
        
        # Step 4: Make the raw MCP call
        print("=" * 80)
        print("Step 4: Making Raw MCP Tool Call")
        print("=" * 80)
        
        print(f"Calling tool: {mcp._tool_name}")
        print(f"With arguments: {args}")
        print()
        
        # Make the call and get raw response
        response = await mcp._session.call_tool(mcp._tool_name, arguments=args)
        
        print("✓ Received response")
        print(f"Response type: {type(response)}")
        print()
        
        # Step 5: Analyze the raw response structure
        print("=" * 80)
        print("Step 5: Analyzing Raw Response Structure")
        print("=" * 80)
        
        print(f"Response attributes: {dir(response)}")
        print()
        
        if hasattr(response, 'content'):
            print(f"Response has 'content' attribute")
            print(f"Content type: {type(response.content)}")
            print(f"Content length: {len(response.content) if hasattr(response.content, '__len__') else 'N/A'}")
            print()
            
            if isinstance(response.content, list):
                print(f"Content is a list with {len(response.content)} items")
                for i, item in enumerate(response.content):
                    print(f"\n  Content item {i}:")
                    print(f"    Type: {type(item)}")
                    print(f"    Attributes: {dir(item)}")
                    
                    if hasattr(item, 'type'):
                        print(f"    Item type: {item.type}")
                    
                    if hasattr(item, 'text'):
                        text = item.text
                        print(f"    Text length: {len(text)}")
                        print(f"    First 500 characters:")
                        print(f"    {text[:500]}")
                        print()
                        
                        # Try to parse as JSON
                        try:
                            parsed = json.loads(text)
                            print(f"    ✓ Text is valid JSON")
                            print(f"    JSON type: {type(parsed)}")
                            
                            if isinstance(parsed, list) and len(parsed) > 0:
                                print(f"    JSON list with {len(parsed)} records")
                                print(f"\n    First record:")
                                print(f"    {json.dumps(parsed[0], indent=6)}")
                                
                                # Check for volume in first record
                                if 'volume' in parsed[0]:
                                    print(f"\n    ✓ VOLUME field exists in data!")
                                    print(f"      First record volume: {parsed[0]['volume']}")
                                else:
                                    print(f"\n    ❌ NO VOLUME field in data!")
                                    print(f"      Available fields: {list(parsed[0].keys())}")
                                
                                # Check a few more records for volume
                                print(f"\n    Checking first 5 records for volume:")
                                for idx, record in enumerate(parsed[:5]):
                                    vol = record.get('volume', 'MISSING')
                                    ts = record.get('timestamp', 'N/A')
                                    print(f"      {idx+1}. {ts}: volume={vol}")
                                
                        except json.JSONDecodeError as e:
                            print(f"    ❌ Text is NOT valid JSON: {str(e)}")
                            print(f"    First error at position {e.pos}")
        
        # Step 6: Check if the tool call was successful
        print()
        print("=" * 80)
        print("Step 6: Response Status Check")
        print("=" * 80)
        
        if hasattr(response, 'isError'):
            print(f"Is error: {response.isError}")
            if response.isError:
                print("❌ Tool call returned an error!")
        else:
            print("No 'isError' attribute (assuming success)")
        print()
        
        # Step 7: Compare with what the handler extracts
        print("=" * 80)
        print("Step 7: What the Response Handler Extracts")
        print("=" * 80)
        
        # Use the actual handler
        from utils.diagnostics import MCPResponseHandler
        handler = MCPResponseHandler(logger, save_debug_files=True)
        
        text_content = handler.extract_text_content(response)
        print(f"Extracted text content length: {len(text_content) if text_content else 0}")
        
        if text_content:
            print(f"First 200 chars: {text_content[:200]}")
            print()
            
            parsed_result = handler.parse_json_response(text_content, symbol)
            if parsed_result:
                print(f"✓ Handler successfully parsed the response")
                print(f"  Result type: {type(parsed_result)}")
                if isinstance(parsed_result, list):
                    print(f"  Records: {len(parsed_result)}")
                    if len(parsed_result) > 0:
                        print(f"  First record keys: {list(parsed_result[0].keys())}")
                        print(f"  First record: {json.dumps(parsed_result[0], indent=4)}")
                        
                        # Check volume
                        if 'volume' in parsed_result[0]:
                            total_vol = sum(r.get('volume', 0) for r in parsed_result)
                            non_zero = sum(1 for r in parsed_result if r.get('volume', 0) > 0)
                            print(f"\n  Volume analysis:")
                            print(f"    Total volume: {total_vol:,}")
                            print(f"    Non-zero bars: {non_zero}")
            else:
                print("❌ Handler failed to parse the response")
        else:
            print("❌ No text content extracted")
        
        # Step 8: Summary and recommendations
        print()
        print("=" * 80)
        print("INVESTIGATION SUMMARY")
        print("=" * 80)
        print()
        print("Key findings:")
        print("1. Check if the MCP tool schema includes a 'volume' field")
        print("2. Check if the raw JSON response contains volume data")
        print("3. Check if the response handler is extracting the data correctly")
        print()
        print("Action items:")
        print("- Review the tool schema in the log file")
        print("- Check if you need to add 'volume' to the tool request parameters")
        print("- Verify ThetaData subscription includes volume data")
        print()
        
    except Exception as e:
        print(f"❌ Error during investigation: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await mcp.close()


def main():
    """Main entry point."""
    symbol = "ABTC"
    date = "20240102"
    
    # Allow command line arguments
    if len(sys.argv) >= 2:
        symbol = sys.argv[1].upper()
    if len(sys.argv) >= 3:
        date = sys.argv[2]
    
    asyncio.run(investigate_mcp_tool(symbol, date))


if __name__ == "__main__":
    main()