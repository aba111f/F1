import pandas as pd
import os


script_dir = os.path.dirname(os.path.abspath(__file__))


parquet_location = os.path.join(script_dir, 'raw_cleaned.parquet')

print(f"📂 Looking for file at: {parquet_location}")


if not os.path.exists(parquet_location):
    print("❌ Error: File not found at the calculated path.")
    print("   Current folder contents:", os.listdir(script_dir)) 
else:
    print("✅ File found! Loading...")
    
    # --- STEP 3: Load and Inspect ---
    df = pd.read_parquet(parquet_location)

    print("\n--- HEAD (First 5 rows) ---")
    pd.set_option('display.max_columns', None)
    print(df.head())

    print("\n--- INFO ---")
    print(df.info())

    # Check Weather columns
    weather_cols = ['AirTemp', 'TrackTemp', 'Humidity']
    print("\n--- Weather Data Check ---")
    if all(col in df.columns for col in weather_cols):
        print(df[weather_cols].head())
    else:
        print("⚠️ Weather columns missing!")