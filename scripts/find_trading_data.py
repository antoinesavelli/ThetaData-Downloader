"""
Search for dates and times when a stock has actual trading data.
"""
import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import Config
from connectors.thetadata_mcp import ThetaDataMCP
from utils.logger import setup_logger
from utils.diagnostics import MCPResponseHandler


async def find_trading_data(symbol: str, start_date: str, num_days: int = 5):
    """
    Search for trading data across multiple dates.
    
    Args:
        symbol: Stock symbol
        start_date: Starting date in YYYYMMDD format
        num_days: Number of trading days to check
    """
    config = Config()
    logger = setup_logger(
        name="find_trading_data",
        log_file="logs/find_trading_data.log",
        log_level="INFO"
    )
    
    print("=" * 80)
    print(f"SEARCHING FOR TRADING DATA: {symbol}")
    print("=" * 80)
    print(f"Starting from: {start_date}")
    print(f"Checking {num_days} days")
    print()
    
    mcp = ThetaDataMCP(config, logger)
    handler = MCPResponseHandler(logger, save_debug_files=False)
    
    try:
        if not await mcp.connect():
            print("❌ Failed to connect to MCP")
            return
        
        # Convert start date to datetime
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        
        results = []
        
        # Check each day
        for day_offset in range(num_days):
            current_dt = start_dt + timedelta(days=day_offset)
            current_date = current_dt.strftime('%Y%m%d')
            day_name = current_dt.strftime('%A, %B %d, %Y')
            
            # Skip weekends
            if current_dt.weekday() >= 5:  # Saturday=5, Sunday=6
                print(f"\n📅 {day_name} ({current_date})")
                print(f"   ⊗ Skipped (Weekend)")
                continue
            
            print(f"\n📅 {day_name} ({current_date})")
            print(f"   Checking...")
            
            # Try regular market hours first (9:30 AM - 4:00 PM)
            args = {
                "symbol": symbol,
                "date": current_date,
                "interval": "1m",
                "start_time": "09:30:00",
                "end_time": "16:00:00"
            }
            
            try:
                response = await mcp._session.call_tool(mcp._tool_name, arguments=args)
                text_content = handler.extract_text_content(response)
                
                if text_content:
                    parsed = handler.parse_json_response(text_content, symbol)
                    
                    if parsed and isinstance(parsed, list):
                        # Analyze the data
                        total_bars = len(parsed)
                        total_volume = sum(r.get('volume', 0) for r in parsed)
                        non_zero_volume = sum(1 for r in parsed if r.get('volume', 0) > 0)
                        non_zero_price = sum(1 for r in parsed if r.get('close', 0) != 0.0)
                        
                        # Get price range
                        prices = [r.get('close', 0) for r in parsed if r.get('close', 0) != 0.0]
                        price_min = min(prices) if prices else 0
                        price_max = max(prices) if prices else 0
                        
                        # Get first and last bars with data
                        first_trade = None
                        last_trade = None
                        for r in parsed:
                            if r.get('volume', 0) > 0:
                                if first_trade is None:
                                    first_trade = r.get('timestamp', 'N/A')
                                last_trade = r.get('timestamp', 'N/A')
                        
                        result = {
                            'date': current_date,
                            'day_name': day_name,
                            'total_bars': total_bars,
                            'total_volume': total_volume,
                            'non_zero_volume': non_zero_volume,
                            'non_zero_price': non_zero_price,
                            'price_min': price_min,
                            'price_max': price_max,
                            'first_trade': first_trade,
                            'last_trade': last_trade,
                            'has_data': total_volume > 0 or non_zero_price > 0
                        }
                        results.append(result)
                        
                        # Display result
                        if result['has_data']:
                            print(f"   ✓ HAS TRADING DATA!")
                            print(f"      Total volume: {total_volume:,}")
                            print(f"      Bars with volume: {non_zero_volume}/{total_bars} ({non_zero_volume/total_bars*100:.1f}%)")
                            print(f"      Price range: ${price_min:.2f} - ${price_max:.2f}")
                            if first_trade:
                                print(f"      First trade: {first_trade}")
                                print(f"      Last trade: {last_trade}")
                        else:
                            print(f"   ✗ No trading data (all zeros)")
                            print(f"      {total_bars} bars, volume={total_volume}, prices={non_zero_price}")
                    else:
                        print(f"   ✗ Failed to parse response")
                else:
                    print(f"   ✗ No data returned")
                    
            except Exception as e:
                print(f"   ❌ Error: {e}")
            
            await asyncio.sleep(0.3)  # Small delay between requests
        
        # Summary
        print()
        print("=" * 80)
        print("SUMMARY")
        print("=" * 80)
        
        days_with_data = [r for r in results if r['has_data']]
        days_without_data = [r for r in results if not r['has_data']]
        
        print(f"\nTotal days checked: {len(results)}")
        print(f"Days WITH trading data: {len(days_with_data)}")
        print(f"Days WITHOUT trading data: {len(days_without_data)}")
        
        if days_with_data:
            print(f"\n✓ DAYS WITH TRADING DATA:")
            for r in days_with_data:
                print(f"\n   {r['day_name']} ({r['date']})")
                print(f"      Volume: {r['total_volume']:,}")
                print(f"      Price: ${r['price_min']:.2f} - ${r['price_max']:.2f}")
                print(f"      First trade: {r['first_trade']}")
                print(f"      Last trade: {r['last_trade']}")
        else:
            print(f"\n❌ NO TRADING DATA FOUND in any of the checked dates!")
            print(f"\nPossible reasons:")
            print(f"   1. The stock was not actively trading during this period")
            print(f"   2. The stock symbol changed or was delisted")
            print(f"   3. ThetaData doesn't have historical data for this stock")
            print(f"   4. The stock is very illiquid and only trades occasionally")
        
        if days_without_data:
            print(f"\n✗ Days without data:")
            for r in days_without_data:
                print(f"   {r['date']} ({r['day_name'].split(',')[0]})")
        
        print()
        
    except Exception as e:
        print(f"❌ Error during search: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await mcp.close()


def main():
    """Main entry point."""
    symbol = "ABTC"
    start_date = "20240102"  # Start from January 2, 2024
    num_days = 10  # Check 10 days
    
    # Allow command line arguments
    if len(sys.argv) >= 2:
        symbol = sys.argv[1].upper()
    if len(sys.argv) >= 3:
        start_date = sys.argv[2]
    if len(sys.argv) >= 4:
        num_days = int(sys.argv[3])
    
    print(f"Searching for {symbol} starting from {start_date} for {num_days} days...")
    print()
    
    asyncio.run(find_trading_data(symbol, start_date, num_days))


if __name__ == "__main__":
    main()
