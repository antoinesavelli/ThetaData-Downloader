"""
Script to fetch historical quote data for ABTC on 2024-02-02.
"""
import requests
import json
from pathlib import Path
from datetime import datetime
 

def fetch_abtc_history():
    """Fetch historical quote data for ABTC on 2024-02-02."""
    
    # Configuration
    base_url = "http://localhost:25503"
    endpoint = "/v3/stock/history/quote"
    symbol = "ABTC"
    date = "20240201"
    
    # Build request parameters (API v3)
    # NOTE: Quote endpoint returns tick data - no interval parameter needed
    params = {
        "symbol": symbol,
        "date": date,
        # ✓ REMOVED interval parameter - quote endpoint is always tick-by-tick
    }
    
    # Optional parameters (uncomment if needed)
    # params["start_time"] = "04:00:00"  # Start time
    # params["end_time"] = "20:00:00"    # End time
    # params["rth"] = "true"             # Regular trading hours only
    
    try:
        print(f"Fetching {symbol} quote history for {date}...")
        print(f"URL: {base_url}{endpoint}")
        print(f"Parameters: {params}")
        
        # Make the request
        response = requests.get(
            f"{base_url}{endpoint}",
            params=params,
            timeout=60
        )
        
        # Check response status
        response.raise_for_status()
        
        # Parse response
        data = response.json()
        
        # Save to file
        output_dir = Path("data/manual_queries")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = output_dir / f"{symbol}_{date}_quote_history.json"
        
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"\n✓ Success! Data saved to: {output_file}")
        print(f"Response preview:")
        print(json.dumps(data, indent=2)[:500] + "...")
        
        # Print summary statistics
        if isinstance(data, dict) and 'response' in data:
            records = data.get('response', [])
            print(f"\nTotal records: {len(records)}")
        
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching data: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response status: {e.response.status_code}")
            print(f"Response body: {e.response.text[:500]}")
        return None
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return None


if __name__ == "__main__":
    print("=" * 60)
    print("ABTC Historical Quote Data Fetcher")
    print("=" * 60)
    
    result = fetch_abtc_history()
    
    if result:
        print("\n" + "=" * 60)
        print("Fetch complete!")
        print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("Fetch failed. Please check the error messages above.")
        print("=" * 60)