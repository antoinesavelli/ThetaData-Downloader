import pandas as pd
from pathlib import Path

# Pick any file
file = Path("D:/trading_data/2024/01/02/AACG_20240102.parquet")
df = pd.read_parquet(file)

print(f"Total bars: {len(df)}")
print(f"Bars with volume > 0: {(df['volume'] > 0).sum()}")
print(f"Bars with volume = 0: {(df['volume'] == 0).sum()}")

# Check timestamp distribution
if 'timestamp' in df.columns:
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Group by hour to see distribution
    df['hour'] = df['timestamp'].dt.hour
    hourly_counts = df.groupby('hour').size()
    
    print("\nBars per hour:")
    print(hourly_counts)
    
    print("\nBars with volume per hour:")
    volume_by_hour = df[df['volume'] > 0].groupby('hour').size()
    print(volume_by_hour)